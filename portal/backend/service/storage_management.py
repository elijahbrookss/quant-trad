"""Server-owned storage enrollment, revisioned policy plans and status.

HTTP and CLI adapters share this service. A plan is never an applied policy:
activation belongs to the worker after its physical operations are verified.
"""
from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

from core.settings import get_settings
from core.storage_inventory import read_storage_inventory
from core.storage_mounts import StorageMountError
from core.storage_targets import STORAGE_ROLES, StoragePolicy, StorageTarget
from portal.backend.db import db
from portal.backend.db.storage_target_models import StoragePlanRecord, StoragePolicyRecord, StorageTargetRecord


logger = logging.getLogger(__name__)


class StorageConflict(ValueError):
    """A reviewed configuration is stale or an idempotency key was reused."""


def _target(record: StorageTargetRecord) -> StorageTarget:
    return StorageTarget(record.id, record.label, record.filesystem_uuid, record.root,
                         record.medium, tuple(record.roles), record.state)


def _plan(record: StoragePlanRecord) -> dict[str, Any]:
    return {"id": record.id, "base_revision": record.base_revision, "policy": record.policy,
            "policy_hash": record.policy_hash, "state": record.state, "impact": record.impact,
            "progress": record.progress, "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat()}


def _lock(session) -> None:
    session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('qt.storage.management.v1', 0))"))


class StorageManagementService:
    def __init__(self, database=None, *, inventory_path: Path | None = None):
        self.database = database if database is not None else db
        self.inventory_path = inventory_path

    def _inventory(self) -> tuple[StorageTarget, ...]:
        path = self.inventory_path or Path(get_settings().storage.inventory_path)
        return read_storage_inventory(path)

    @staticmethod
    def _inspect(target: StorageTarget) -> dict[str, Any]:
        try:
            evidence = target.inspect()
            status = "read_only" if evidence.read_only else "available"
            return {"status": status, "capacity": asdict(evidence), "error": None}
        except StorageMountError as exc:
            return {"status": "unavailable", "capacity": None, "error": str(exc)}

    def snapshot(self) -> dict[str, Any]:
        inventory = self._inventory()
        with self.database.session() as session:
            registered = session.scalars(select(StorageTargetRecord).order_by(StorageTargetRecord.id)).all()
            config = session.get(StoragePolicyRecord, 1)
            plans = session.scalars(select(StoragePlanRecord).order_by(StoragePlanRecord.created_at.desc()).limit(10)).all()
            policy = config.policy if config else None
            rows = []
            for record in registered:
                target = _target(record)
                observed = self._inspect(target)
                rows.append({**asdict(target), **observed, "reserved_bytes": record.reserved_bytes})
            registered_ids = {record.id for record in registered}
            candidates = [{**asdict(target), **self._inspect(target)}
                          for target in inventory if target.target_id not in registered_ids]
            active_plan = session.scalar(select(StoragePlanRecord).where(
                StoragePlanRecord.state.in_(("queued", "running", "blocked"))).limit(1))
            if not registered or policy is None:
                health = "unconfigured"
            elif any(row["status"] != "available" for row in rows):
                health = "needs_attention"
            elif active_plan:
                health = "changing"
            else:
                health = "available"
            return {
                "schema_version": "qt.storage_status.v1", "observed_at": datetime.now(UTC).isoformat(),
                "revision": config.revision if config else 0, "policy": policy,
                "targets": rows, "candidates": candidates, "plans": [_plan(record) for record in plans],
                "health": health, "active_plan": _plan(active_plan) if active_plan else None,
                # Filesystem availability cannot assert lifecycle or backup success.
                "movement": {"state": "unconfigured" if policy is None else "unknown", "last_completed_at": None},
                "backup": {"state": "unconfigured" if policy is None else "unknown", "last_completed_at": None},
            }

    def register(self, target_id: str) -> dict[str, Any]:
        candidate = next((target for target in self._inventory() if target.target_id == target_id), None)
        if candidate is None:
            raise ValueError("storage_target_unprepared: select an administrator-prepared drive")
        candidate.inspect(require_writable=True)
        with self.database.session() as session:
            _lock(session)
            existing = session.get(StorageTargetRecord, target_id)
            if existing is not None:
                if _target(existing) != candidate:
                    raise StorageConflict("storage_target_identity_changed: existing target cannot be repointed")
                return {"target_id": target_id, "registered": True, "reused": True}
            same_device = session.scalar(select(StorageTargetRecord.id).where(
                StorageTargetRecord.filesystem_uuid == candidate.filesystem_uuid))
            if same_device:
                raise StorageConflict("storage_target_already_registered: filesystem capacity cannot be counted twice")
            session.add(StorageTargetRecord(
                id=candidate.target_id, label=candidate.label, filesystem_uuid=candidate.filesystem_uuid,
                root=candidate.root, medium=candidate.medium, roles=list(candidate.roles), state="active",
            ))
        logger.info("storage_target_enrolled | target_id=%s filesystem_uuid=%s", target_id, candidate.filesystem_uuid)
        return {"target_id": target_id, "registered": True, "reused": False}

    def plan(self, *, policy: dict[str, Any], base_revision: int, request_id: str) -> dict[str, Any]:
        desired = StoragePolicy.from_dict(policy)
        if type(base_revision) is not int or base_revision < 0:
            raise ValueError("storage_plan_invalid: base_revision must be nonnegative")
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 80:
            raise ValueError("storage_plan_invalid: request_id required")
        with self.database.session() as session:
            _lock(session)
            existing = session.scalar(select(StoragePlanRecord).where(StoragePlanRecord.request_id == request_id))
            if existing is not None:
                if existing.policy_hash != desired.fingerprint or existing.base_revision != base_revision:
                    raise StorageConflict("storage_plan_request_conflict")
                return _plan(existing)
            session.execute(insert(StoragePolicyRecord).values(id=1, revision=0).on_conflict_do_nothing())
            config = session.get(StoragePolicyRecord, 1)
            if config.revision != base_revision:
                raise StorageConflict("storage_policy_changed: refresh and review again")
            targets = [_target(row) for row in session.scalars(select(StorageTargetRecord)).all()]
            desired.validate_targets(targets)
            previous = config.policy or {}
            changes = [{"role": role, "before": previous.get(role, []), "after": list(getattr(desired, role))}
                       for role in STORAGE_ROLES if previous.get(role, []) != list(getattr(desired, role))]
            checks = []
            for target in targets:
                if any(target.target_id in getattr(desired, role) for role in STORAGE_ROLES):
                    checks.append({"target_id": target.target_id, **self._inspect(target)})
            blockers = [{"code": "storage_target_unavailable", "target_id": item["target_id"],
                         "detail": item["error"] or item["status"]}
                        for item in checks if item["status"] != "available"]
            blockers.append({"code": "storage_execution_unavailable",
                             "detail": "Storage movement and activation are not implemented in this build."})
            warnings = []
            if any(t.medium == "hdd" and t.target_id in desired.recent for t in targets):
                warnings.append("Recent data on HDD requires a measured latency check.")
            if set(desired.backups) & set(desired.history + desired.archives):
                warnings.append("Local backups share a drive with historical data.")
            impact = {
                "changes": changes, "blockers": blockers, "warnings": warnings,
                "setting_changes": [{"setting": key, "before": previous.get(key), "after": value}
                                    for key, value in desired.to_dict().items()
                                    if key not in (*STORAGE_ROLES, "schema_version") and previous.get(key) != value],
                "existing_data_automatically_relocated": False,
                "requires_migration": any(item["role"] in {"recent", "history"} for item in changes),
                "requires_restart": any(item["role"] in {"recent", "history"} for item in changes),
                "estimated_seconds": None,
                "target_evidence": checks,
            }
            now = datetime.now(UTC)
            record = StoragePlanRecord(id="storage_" + uuid4().hex, request_id=request_id,
                base_revision=base_revision, policy=desired.to_dict(), policy_hash=desired.fingerprint,
                state="planned", impact=impact, progress={}, created_at=now, updated_at=now)
            session.add(record)
            session.flush()
            result = _plan(record)
        logger.info("storage_plan_created | plan_id=%s base_revision=%s policy_hash=%s",
                    result["id"], base_revision, desired.fingerprint)
        return result

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        with self.database.session() as session:
            record = session.get(StoragePlanRecord, plan_id)
            if record is None:
                raise KeyError("storage_plan_not_found")
            return _plan(record)

    def queue_plan(self, plan_id: str, *, policy_hash: str) -> dict[str, Any]:
        with self.database.session() as session:
            _lock(session)
            record = session.get(StoragePlanRecord, plan_id)
            if record is None:
                raise KeyError("storage_plan_not_found")
            if record.policy_hash != policy_hash:
                raise StorageConflict("storage_plan_review_mismatch")
            if record.state in {"queued", "running", "completed"}:
                return _plan(record)
            if record.state != "planned":
                raise StorageConflict("storage_plan_not_applicable")
            config = session.get(StoragePolicyRecord, 1)
            if config is None or config.revision != record.base_revision:
                raise StorageConflict("storage_policy_changed: refresh and review again")
            competing = session.scalar(select(StoragePlanRecord.id).where(
                StoragePlanRecord.id != plan_id, StoragePlanRecord.state.in_(("queued", "running", "blocked"))).limit(1))
            if competing:
                raise StorageConflict("storage_change_in_progress")
            desired = StoragePolicy.from_dict(record.policy)
            targets = [_target(row) for row in session.scalars(select(StorageTargetRecord)).all()]
            desired.validate_targets(targets)
            for target in targets:
                if any(target.target_id in getattr(desired, role) for role in STORAGE_ROLES):
                    target.inspect(require_writable=True)
            raise StorageConflict("storage_execution_unavailable: movement and activation are not implemented")


storage_management_service = StorageManagementService()

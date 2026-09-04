"""Scheduled, pin-safe storage lifecycle for continuous market data."""

from __future__ import annotations

import hashlib
import logging
import socket
import threading
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from market_data.archive import (
    FilesystemRawArchiveObjectStore,
    publish_compacted_raw_archives,
)
from core.market_storage_lifecycle import (
    MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
    MarketStorageLifecyclePolicy,
)

from ..storage.repos.market_lifecycle import (
    MarketStorageLifecycleBusyError,
    PostgresMarketStorageLifecycleRepository,
    lifecycle_operation_id,
    market_storage_lifecycle_repository,
)
from ..storage.repos.market_structure import (
    PostgresMarketStructureRepository,
    market_structure_repository,
)
from ..storage.repos.fact_retention import canonical_fact_retention_repository
from .market_structure_service import DEFAULT_STORAGE_ROOT


logger = logging.getLogger(__name__)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class MarketStorageLifecycleService:
    """Plan first, then execute bounded lifecycle work under one global fence."""

    def __init__(
        self,
        *,
        lifecycle_repository: PostgresMarketStorageLifecycleRepository = (
            market_storage_lifecycle_repository
        ),
        market_repository: PostgresMarketStructureRepository = (
            market_structure_repository
        ),
        canonical_repository=canonical_fact_retention_repository,
    ) -> None:
        self.lifecycle_repository = lifecycle_repository
        self.market_repository = market_repository
        self.canonical_repository = canonical_repository

    def plan(
        self,
        *,
        policy: MarketStorageLifecyclePolicy,
        now: Optional[datetime] = None,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        canonical_after_storage_day: date | None = None,
    ) -> dict[str, Any]:
        observed_at = _utc(now or datetime.now(UTC))
        canonical = self.canonical_repository.plan(
            policy=policy.canonical_retention, storage_root=storage_root,
            after_storage_day=canonical_after_storage_day,
        )
        return self._assemble_plan(policy=policy, observed_at=observed_at, canonical=canonical)

    def _assemble_plan(self, *, policy, observed_at, canonical):
        compactions = (
            self._plan_compactions(policy=policy, now=observed_at)
            if policy.archive_compaction_enabled
            else []
        )
        archive_expirations = (
            self._plan_archive_expirations(policy=policy, now=observed_at)
            if policy.archive_expiration_enabled
            else []
        )
        chunk_compressions: list[dict[str, Any]] = []
        chunk_expirations: list[dict[str, Any]] = []
        actions = [
            *compactions,
            *archive_expirations,
            *chunk_compressions,
            *chunk_expirations,
        ]
        return {
            "schema_version": "market.storage_lifecycle_plan.v1",
            "policy": policy.to_dict(),
            "observed_at": observed_at.isoformat(),
            "execution_enabled": policy.execution_enabled,
            "canonical_retention": canonical,
            "summary": {
                "action_count": len(actions),
                "eligible_count": sum(bool(item["eligible"]) for item in actions),
                "blocked_count": sum(not bool(item["eligible"]) for item in actions),
                "archive_compaction_count": len(compactions),
                "archive_expiration_count": len(archive_expirations),
                "canonical_candidate_count": len(canonical["actions"]),
                "canonical_execution_available": canonical["execution_available"],
                "chunk_compression_count": len(chunk_compressions),
                "chunk_expiration_count": len(chunk_expirations),
                "estimated_reclaim_bytes": sum(
                    int(item.get("estimated_reclaim_bytes") or 0)
                    for item in [*archive_expirations, *chunk_expirations]
                    if item["eligible"]
                ),
            },
            "archive_compactions": compactions,
            "archive_expirations": archive_expirations,
            "chunk_compressions": chunk_compressions,
            "chunk_expirations": chunk_expirations,
        }

    def run(
        self,
        *,
        policy: MarketStorageLifecyclePolicy,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        execute: bool = False,
        owner_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> dict[str, Any]:
        requested_execute = bool(execute)
        if requested_execute and not policy.execution_enabled:
            raise ValueError(
                "market_storage_lifecycle_execution_not_enabled: "
                "enable the policy only after reviewing a dry-run plan"
            )
        if not requested_execute:
            plan = self.plan(policy=policy, now=now, storage_root=storage_root)
            return {
                "schema_version": "market.storage_lifecycle_run.v1",
                "status": "dry_run",
                "plan": plan,
                "outcomes": [],
                "failure_count": 0,
            }

        owner = str(
            owner_id
            or f"market-lifecycle:{socket.gethostname()}:{threading.get_native_id()}"
        )
        outcomes: list[dict[str, Any]] = []
        # Capacity/family scans are observations, not destructive authority.
        # Keep them outside the raw-expiry exclusive fence. Raw action planning
        # retains its previous lock scope; final canonical execution will own
        # its separate shared-verification/exclusive-handoff phases.
        canonical = self.canonical_repository.plan(
            policy=policy.canonical_retention, storage_root=storage_root,
        )
        with self.lifecycle_repository.lifecycle_lock(owner_id=owner):
            plan = self._assemble_plan(policy=policy, observed_at=_utc(now or datetime.now(UTC)), canonical=canonical)
            store = FilesystemRawArchiveObjectStore(
                Path(storage_root).expanduser().resolve() / "objects"
            )
            for item in plan["archive_compactions"]:
                if item["eligible"]:
                    outcomes.append(
                        self._execute_compaction(item=item, store=store)
                    )
            for item in plan["archive_expirations"]:
                if item["eligible"]:
                    outcomes.append(
                        self._execute_archive_expiration(item=item, store=store)
                    )
            for item in plan["chunk_compressions"]:
                if item["eligible"]:
                    outcomes.append(self._execute_chunk_compression(item=item))
            for item in plan["chunk_expirations"]:
                if item["eligible"]:
                    outcomes.append(self._execute_chunk_expiration(item=item))
        failures = [item for item in outcomes if item.get("status") == "failed"]
        status = "degraded" if failures else "completed"
        logger.log(
            logging.ERROR if failures else logging.INFO,
            "market_storage_lifecycle_run_finished | owner_id=%s status=%s "
            "outcomes=%s failures=%s",
            owner,
            status,
            len(outcomes),
            len(failures),
        )
        return {
            "schema_version": "market.storage_lifecycle_run.v1",
            "status": status,
            "plan": plan,
            "outcomes": outcomes,
            "failure_count": len(failures),
        }

    def _plan_compactions(
        self, *, policy: MarketStorageLifecyclePolicy, now: datetime
    ) -> list[dict[str, Any]]:
        rows = self.lifecycle_repository.list_compaction_manifests(
            older_than=now - timedelta(minutes=policy.compaction_min_age_minutes)
        )
        grouped: dict[tuple[str, str, int, datetime], list[dict[str, Any]]] = (
            defaultdict(list)
        )
        for row in rows:
            received = _utc(row["first_received_at"])
            partition_hour = received.replace(minute=0, second=0, microsecond=0)
            grouped[
                (
                    str(row["definition_id"]),
                    str(row["session_id"]),
                    int(row["connection_epoch"]),
                    partition_hour,
                )
            ].append(row)

        planned: list[dict[str, Any]] = []
        for key in sorted(grouped):
            candidates = sorted(
                grouped[key],
                key=lambda item: (int(item["first_receive_ordinal"]), str(item["id"])),
            )
            run: list[dict[str, Any]] = []
            run_bytes = 0
            for candidate in candidates:
                contiguous = not run or (
                    int(run[-1]["last_receive_ordinal"]) + 1
                    == int(candidate["first_receive_ordinal"])
                )
                fits = run_bytes + int(candidate["byte_count"]) <= policy.compaction_target_bytes
                if run and (not contiguous or not fits):
                    self._append_compaction_plan(
                        planned=planned,
                        rows=run,
                        policy=policy,
                        partition_hour=key[3],
                    )
                    run = []
                    run_bytes = 0
                run.append(candidate)
                run_bytes += int(candidate["byte_count"])
            self._append_compaction_plan(
                planned=planned,
                rows=run,
                policy=policy,
                partition_hour=key[3],
            )
            if len(planned) >= policy.max_compaction_groups_per_run:
                break
        return planned[: policy.max_compaction_groups_per_run]

    def _append_compaction_plan(
        self,
        *,
        planned: list[dict[str, Any]],
        rows: Sequence[Mapping[str, Any]],
        policy: MarketStorageLifecyclePolicy,
        partition_hour: datetime,
    ) -> None:
        if len(rows) < policy.compaction_min_objects:
            return
        source_ids = [str(row["id"]) for row in rows]
        target_id = "rams_" + hashlib.sha256(
            "|".join(source_ids).encode("utf-8")
        ).hexdigest()
        operation_id = lifecycle_operation_id(
            action="archive_compact",
            target_kind="raw_manifest_set",
            target_id=target_id,
            policy_version=MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
        )
        if self.lifecycle_repository.operation_completed(operation_id=operation_id):
            return
        planned.append(
            {
                "action": "archive_compact",
                "operation_id": operation_id,
                "target_kind": "raw_manifest_set",
                "target_id": target_id,
                "eligible": True,
                "blockers": [],
                "definition_id": str(rows[0]["definition_id"]),
                "session_id": str(rows[0]["session_id"]),
                "connection_epoch": int(rows[0]["connection_epoch"]),
                "partition_hour": partition_hour.isoformat(),
                "source_manifest_ids": source_ids,
                "source_object_keys": [str(row["object_key"]) for row in rows],
                "source_object_sha256": [str(row["object_sha256"]) for row in rows],
                "source_bytes": sum(int(row["byte_count"]) for row in rows),
                "source_record_count": sum(int(row["record_count"]) for row in rows),
            }
        )

    def _plan_archive_expirations(
        self, *, policy: MarketStorageLifecyclePolicy, now: datetime
    ) -> list[dict[str, Any]]:
        cutoffs = {
            "raw_trade": now - timedelta(days=policy.raw_trade_archive_days),
            "raw_l2": now - timedelta(days=policy.raw_l2_archive_days),
            "book_checkpoint": now
            - timedelta(days=policy.book_checkpoint_archive_days),
            "compacted_source": now
            - timedelta(hours=policy.compacted_source_grace_hours),
        }
        rows = self.lifecycle_repository.list_archive_expiration_candidates(
            raw_trade_cutoff=cutoffs["raw_trade"],
            raw_l2_cutoff=cutoffs["raw_l2"],
            checkpoint_cutoff=cutoffs["book_checkpoint"],
            compacted_source_cutoff=cutoffs["compacted_source"],
            limit=policy.max_archive_expirations_per_run,
        )
        planned: list[dict[str, Any]] = []
        for row in rows:
            channels = tuple(row.get("channels") or ())
            if row.get("replacement_manifest_id") and row.get("compacted_at"):
                reason = "compacted_source_replaced"
                cutoff = cutoffs["compacted_source"]
            elif str(row["target_kind"]) == "book_checkpoint":
                reason = "book_checkpoint_retention_elapsed"
                cutoff = cutoffs["book_checkpoint"]
            elif "level2" in channels:
                reason = "raw_l2_retention_elapsed"
                cutoff = cutoffs["raw_l2"]
            else:
                reason = "raw_trade_retention_elapsed"
                cutoff = cutoffs["raw_trade"]
            blockers = []
            if int(row.get("explicit_pin_count") or 0):
                blockers.append("explicit_retention_pin")
            if int(row.get("dataset_pin_count") or 0):
                blockers.append("frozen_dataset_pin")
            if int(row.get("canonical_dependency_count") or 0):
                blockers.append("canonical_archive_dependency")
            target_id = str(row["target_id"])
            operation_id = lifecycle_operation_id(
                action="archive_expire",
                target_kind=str(row["target_kind"]),
                target_id=target_id,
                policy_version=MARKET_STORAGE_LIFECYCLE_POLICY_VERSION,
            )
            planned.append(
                {
                    "action": "archive_expire",
                    "operation_id": operation_id,
                    "target_kind": str(row["target_kind"]),
                    "target_id": target_id,
                    "eligible": not blockers,
                    "blockers": blockers,
                    "reason": reason,
                    "cutoff_at": cutoff.isoformat(),
                    "effective_at": _utc(row["effective_at"]).isoformat(),
                    "object_key": str(row["object_key"]),
                    "object_sha256": str(row["object_sha256"]),
                    "estimated_reclaim_bytes": int(row["byte_count"]),
                    "replacement_manifest_id": (
                        str(row["replacement_manifest_id"])
                        if row.get("replacement_manifest_id")
                        else None
                    ),
                }
            )
        return planned

    def _execute_compaction(
        self,
        *,
        item: Mapping[str, Any],
        store: FilesystemRawArchiveObjectStore,
    ) -> dict[str, Any]:
        evidence = {
            "definition_id": item["definition_id"],
            "session_id": item["session_id"],
            "connection_epoch": item["connection_epoch"],
            "source_manifest_ids": list(item["source_manifest_ids"]),
        }
        operation_id = str(item["operation_id"])
        self.lifecycle_repository.append_event(
            operation_id=operation_id,
            action="archive_compact",
            event_type="planned",
            target_kind="raw_manifest_set",
            target_id=str(item["target_id"]),
            evidence=evidence,
        )
        try:
            active = self.market_repository.list_session_manifests(
                definition_id=str(item["definition_id"]),
                session_id=str(item["session_id"]),
            )
            active_by_id = {str(row["id"]): row for row in active}
            sources = [
                active_by_id[str(manifest_id)]
                for manifest_id in item["source_manifest_ids"]
            ]
            paths: list[Path] = []
            for source in sources:
                path = store.local_path(str(source["object_key"]))
                if not path.exists():
                    raise RuntimeError(
                        "market_archive_compaction_source_missing: "
                        f"manifest_id={source['id']}"
                    )
                observed = _file_sha256(path)
                if observed != str(source["object_sha256"]):
                    raise RuntimeError(
                        "market_archive_compaction_source_corrupt: "
                        f"manifest_id={source['id']}"
                    )
                paths.append(path)
            encoded, acknowledgement, records = publish_compacted_raw_archives(
                paths,
                object_store=store,
                temporary_directory=store.root.parent / "tmp",
            )
            commit = self.market_repository.commit_compacted_archive(
                definition_id=str(item["definition_id"]),
                encoded=encoded,
                acknowledgement=acknowledgement,
                records=records,
                source_manifest_ids=list(item["source_manifest_ids"]),
            )
            completed = {
                **evidence,
                "replacement_manifest_id": commit.manifest_id,
                "replacement_object_key": acknowledgement.object_key,
                "replacement_object_sha256": acknowledgement.sha256,
                "record_count": len(records),
                "content_fingerprint": encoded.content_fingerprint,
            }
            self.lifecycle_repository.append_event(
                operation_id=operation_id,
                action="archive_compact",
                event_type="completed",
                target_kind="raw_manifest_set",
                target_id=str(item["target_id"]),
                evidence=completed,
            )
            return {
                "action": "archive_compact",
                "operation_id": operation_id,
                "status": "completed",
                **completed,
            }
        except Exception as exc:  # noqa: BLE001 - persisted lifecycle evidence
            self._record_failure(item=item, error=exc, evidence=evidence)
            return self._failure_outcome(item=item, error=exc)

    def _execute_archive_expiration(
        self,
        *,
        item: Mapping[str, Any],
        store: FilesystemRawArchiveObjectStore,
    ) -> dict[str, Any]:
        operation_id = str(item["operation_id"])
        evidence = {
            "reason": item["reason"],
            "object_key": item["object_key"],
            "object_sha256": item["object_sha256"],
            "replacement_manifest_id": item.get("replacement_manifest_id"),
        }
        try:
            status = self.lifecycle_repository.archive_target_status(
                target_kind=str(item["target_kind"]),
                target_id=str(item["target_id"]),
            )
            if status["expired"]:
                return {
                    "action": "archive_expire",
                    "operation_id": operation_id,
                    "status": "already_completed",
                }
            if status["pinned"]:
                self.lifecycle_repository.append_event(
                    operation_id=operation_id,
                    action="archive_expire",
                    event_type="skipped",
                    target_kind=str(item["target_kind"]),
                    target_id=str(item["target_id"]),
                    cutoff_at=datetime.fromisoformat(str(item["cutoff_at"])),
                    reason="retention pin appeared after planning",
                    evidence={**evidence, "status": status},
                )
                return {
                    "action": "archive_expire",
                    "operation_id": operation_id,
                    "status": "skipped",
                    "reason": "pinned",
                }
            replacement_id = item.get("replacement_manifest_id")
            if replacement_id:
                replacement = self.lifecycle_repository.archive_target_status(
                    target_kind="raw_manifest",
                    target_id=str(replacement_id),
                )
                replacement_path = store.local_path(str(replacement["object_key"]))
                if replacement["expired"] or not replacement_path.exists():
                    raise RuntimeError(
                        "market_archive_expiration_replacement_unavailable: "
                        f"source={item['target_id']} replacement={replacement_id}"
                    )
                if _file_sha256(replacement_path) != str(
                    replacement["object_sha256"]
                ):
                    raise RuntimeError(
                        "market_archive_expiration_replacement_corrupt: "
                        f"replacement={replacement_id}"
                    )
            recovering = self.lifecycle_repository.operation_started(
                operation_id=operation_id
            )
            self.lifecycle_repository.append_event(
                operation_id=operation_id,
                action="archive_expire",
                event_type="planned",
                target_kind=str(item["target_kind"]),
                target_id=str(item["target_id"]),
                cutoff_at=datetime.fromisoformat(str(item["cutoff_at"])),
                reason=str(item["reason"]),
                evidence=evidence,
            )
            acknowledgement = store.delete_verified(
                object_key=str(item["object_key"]),
                expected_sha256=str(item["object_sha256"]),
                allow_missing=recovering,
            )
            completed = {
                **evidence,
                "deleted_bytes": acknowledgement.byte_count,
                "deleted_at": acknowledgement.deleted_at.isoformat(),
                "recovered_after_prior_plan": acknowledgement.already_absent,
            }
            self.lifecycle_repository.append_event(
                operation_id=operation_id,
                action="archive_expire",
                event_type="completed",
                target_kind=str(item["target_kind"]),
                target_id=str(item["target_id"]),
                cutoff_at=datetime.fromisoformat(str(item["cutoff_at"])),
                reason=str(item["reason"]),
                evidence=completed,
            )
            return {
                "action": "archive_expire",
                "operation_id": operation_id,
                "status": "completed",
                "target_id": item["target_id"],
                **completed,
            }
        except Exception as exc:  # noqa: BLE001 - persisted lifecycle evidence
            self._record_failure(item=item, error=exc, evidence=evidence)
            return self._failure_outcome(item=item, error=exc)

    def _execute_chunk_compression(
        self, *, item: Mapping[str, Any]
    ) -> dict[str, Any]:
        evidence = {
            "table_names": list(item["table_names"]),
            "range_start": item["range_start"],
            "range_end": item["range_end"],
        }
        self.lifecycle_repository.append_event(
            operation_id=str(item["operation_id"]),
            action="chunk_compress",
            event_type="planned",
            target_kind="hypertable_chunk",
            target_id=str(item["target_id"]),
            evidence=evidence,
        )
        try:
            result = self.lifecycle_repository.compress_chunk_group(
                table_names=list(item["table_names"]),
                range_start=datetime.fromisoformat(str(item["range_start"])),
                range_end=datetime.fromisoformat(str(item["range_end"])),
                operation_id=str(item["operation_id"]),
                target_id=str(item["target_id"]),
                evidence=evidence,
            )
            return {
                "action": "chunk_compress",
                "operation_id": item["operation_id"],
                **result,
            }
        except Exception as exc:  # noqa: BLE001 - persisted lifecycle evidence
            self._record_failure(item=item, error=exc, evidence=evidence)
            return self._failure_outcome(item=item, error=exc)

    def _execute_chunk_expiration(
        self, *, item: Mapping[str, Any]
    ) -> dict[str, Any]:
        evidence = {
            "table_names": list(item["table_names"]),
            "range_start": item["range_start"],
            "range_end": item["range_end"],
        }
        self.lifecycle_repository.append_event(
            operation_id=str(item["operation_id"]),
            action="chunk_expire",
            event_type="planned",
            target_kind="hypertable_chunk",
            target_id=str(item["target_id"]),
            evidence=evidence,
        )
        try:
            result = self.lifecycle_repository.expire_chunk_group(
                table_names=list(item["table_names"]),
                pin_table_name=str(item["pin_table_name"]),
                range_start=datetime.fromisoformat(str(item["range_start"])),
                range_end=datetime.fromisoformat(str(item["range_end"])),
                operation_id=str(item["operation_id"]),
                target_id=str(item["target_id"]),
                evidence=evidence,
            )
            return {
                "action": "chunk_expire",
                "operation_id": item["operation_id"],
                **result,
            }
        except Exception as exc:  # noqa: BLE001 - persisted lifecycle evidence
            self._record_failure(item=item, error=exc, evidence=evidence)
            return self._failure_outcome(item=item, error=exc)

    def _record_failure(
        self,
        *,
        item: Mapping[str, Any],
        error: Exception,
        evidence: Mapping[str, Any],
    ) -> None:
        logger.exception(
            "market_storage_lifecycle_action_failed | action=%s operation_id=%s "
            "target_kind=%s target_id=%s error=%s",
            item["action"],
            item["operation_id"],
            item["target_kind"],
            item["target_id"],
            error,
        )
        self.lifecycle_repository.append_event(
            operation_id=str(item["operation_id"]),
            action=str(item["action"]),
            event_type="failed",
            target_kind=str(item["target_kind"]),
            target_id=str(item["target_id"]),
            reason=f"{type(error).__name__}: {error}",
            evidence={**dict(evidence), "error_type": type(error).__name__},
        )

    @staticmethod
    def _failure_outcome(
        *, item: Mapping[str, Any], error: Exception
    ) -> dict[str, Any]:
        return {
            "action": item["action"],
            "operation_id": item["operation_id"],
            "target_id": item["target_id"],
            "status": "failed",
            "error": f"{type(error).__name__}: {error}",
        }


class MarketStorageLifecycleSupervisor:
    """Own the recurring lifecycle schedule without blocking acquisition."""

    def __init__(
        self,
        *,
        policy: MarketStorageLifecyclePolicy,
        service: Optional[MarketStorageLifecycleService] = None,
        storage_root: Path = DEFAULT_STORAGE_ROOT,
        owner_id: Optional[str] = None,
    ) -> None:
        self.policy = policy
        self.service = service or MarketStorageLifecycleService()
        self.storage_root = Path(storage_root)
        self.owner_id = str(owner_id or f"market-lifecycle:{socket.gethostname()}")
        self._stop = threading.Event()
        self._snapshot_lock = threading.Lock()
        self._snapshot: dict[str, Any] = {
            "state": "disabled" if not policy.enabled else "starting",
            "policy": policy.to_dict(),
            "last_run": None,
            "last_error": None,
        }
        self._thread = threading.Thread(
            target=self._run,
            name="market-storage-lifecycle",
            daemon=True,
        )

    def start(self) -> None:
        if self.policy.enabled:
            self._thread.start()

    def stop(self, *, timeout_seconds: float = 30.0) -> None:
        if not self.policy.enabled:
            return
        self._stop.set()
        self._thread.join(timeout=max(1.0, timeout_seconds))
        if self._thread.is_alive():
            raise RuntimeError(
                f"market_storage_lifecycle_stop_timeout: owner_id={self.owner_id}"
            )

    def snapshot(self) -> dict[str, Any]:
        with self._snapshot_lock:
            return dict(self._snapshot)

    def run_once(self) -> dict[str, Any]:
        result = self.service.run(
            policy=self.policy,
            storage_root=self.storage_root,
            execute=self.policy.execution_enabled,
            owner_id=self.owner_id,
        )
        with self._snapshot_lock:
            self._snapshot = {
                "state": "running",
                "policy": self.policy.to_dict(),
                "last_run": {
                    "status": result["status"],
                    "summary": result["plan"]["summary"],
                    "failure_count": result["failure_count"],
                    "finished_at": datetime.now(UTC).isoformat(),
                },
                "last_error": None,
            }
        return result

    def _run(self) -> None:
        with self._snapshot_lock:
            self._snapshot["state"] = "running"
        while not self._stop.is_set():
            try:
                self.run_once()
            except MarketStorageLifecycleBusyError as exc:
                logger.warning(
                    "market_storage_lifecycle_schedule_busy | owner_id=%s error=%s",
                    self.owner_id,
                    exc,
                )
            except Exception as exc:  # noqa: BLE001 - durable recurring worker
                logger.exception(
                    "market_storage_lifecycle_schedule_failed | owner_id=%s",
                    self.owner_id,
                )
                with self._snapshot_lock:
                    self._snapshot["state"] = "degraded"
                    self._snapshot["last_error"] = f"{type(exc).__name__}: {exc}"
            if self._stop.wait(self.policy.interval_seconds):
                break
        with self._snapshot_lock:
            self._snapshot["state"] = "stopped"


market_storage_lifecycle_service = MarketStorageLifecycleService()


__all__ = [
    "MarketStorageLifecycleService",
    "MarketStorageLifecycleSupervisor",
    "market_storage_lifecycle_service",
]

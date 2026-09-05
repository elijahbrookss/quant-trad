"""Read-only canonical retention inventory and pressure planning.

Plans are observations, never reusable deletion authority. This boundary reads
catalogs and filesystem capacity only: no mkdir, archive decode, seal, or DDL.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from time import monotonic

from sqlalchemy import text

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from core.storage_mounts import (
    StorageMountError, inspect_filesystem, require_configured_archive_mount,
)
from portal.backend.db.fact_storage_schema import fact_partition_name
from ._shared import db
from .fact_archival import FACT_ARCHIVE_VERIFIER_VERSION, _partition_manifest_set_hash
from .fact_reclamation import unproven_reclamation_fact_types


def archive_admission_blockers(*, policy, filesystem, publication=True):
    if filesystem["status"] != "available":
        return ["archive_mount_unavailable"]
    blockers = ["archive_mount_read_only"] if filesystem["read_only"] else []
    if not publication:
        return blockers
    reserve = 4 * policy.max_page_logical_bytes
    if filesystem["available_bytes"] < policy.archive_min_free_bytes:
        blockers.append("archive_free_reserve_reached")
    elif filesystem["available_bytes"] < policy.archive_min_free_bytes + reserve:
        blockers.append("archive_publication_headroom_insufficient")
    budget = policy.archive_filesystem_budget_bytes
    if budget is not None:
        if filesystem["used_bytes"] >= budget:
            blockers.append("archive_filesystem_budget_reached")
        elif filesystem["used_bytes"] + reserve > budget:
            blockers.append("archive_publication_budget_insufficient")
    return blockers


class PostgresCanonicalFactRetentionRepository:
    def __init__(self, *, database=db):
        self.database = database

    def _inventory(self, policy: CanonicalFactRetentionPolicy, *, after_storage_day: date | None):
        deadline = monotonic() + policy.max_plan_seconds
        with self.database.session() as session:
            # A metadata snapshot does not need the mutation/pin fence. Concurrent
            # physical changes can invalidate sizes and require a fresh plan;
            # execution must always repeat its own fenced admission.
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            def query(statement, params=None):
                remaining_ms = int((deadline - monotonic()) * 1000)
                if remaining_ms <= 0:
                    raise RuntimeError("canonical_retention_plan_budget_exceeded: retry with a smaller candidate page or explicitly reviewed limits")
                session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                                {"timeout": str(min(policy.plan_statement_timeout_ms, remaining_ms))})
                return session.execute(statement, params or {})

            sizes = dict(query(text("""
                SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date AS database_day,
                       pg_database_size(current_database()) AS database_bytes,
                       pg_total_relation_size('market.fact_versions') AS canonical_header_bytes,
                       pg_total_relation_size('market.raw_archive_record_mappings') AS raw_mapping_bytes
            """)).mappings().one())
            partitions = [dict(row) for row in query(text("""
                SELECT storage_day,state,expected_rows,manifest_set_hash FROM market.fact_retention_partitions
                WHERE state <> 'reclaimed' ORDER BY storage_day LIMIT :limit
            """), {"limit": policy.max_inventory_partitions + 1}).mappings()]
            physical = list(query(text("""
                SELECT child.relname,pg_total_relation_size(child.oid) AS bytes
                FROM pg_inherits JOIN pg_class AS child ON child.oid=inhrelid
                WHERE inhparent='market.fact_hot_payloads'::regclass
                ORDER BY child.relname LIMIT :limit
            """), {"limit": policy.max_inventory_partitions + 1}).mappings())
            if max(len(partitions), len(physical)) > policy.max_inventory_partitions:
                raise RuntimeError("canonical_retention_inventory_budget_exceeded: no partial storage total is reported")
            by_name = {row["relname"]: int(row["bytes"]) for row in physical}
            if set(by_name) != {fact_partition_name(row["storage_day"]) for row in partitions}:
                raise RuntimeError("canonical_retention_inventory_changed: progress and physical partitions disagree; retry and inspect cutover if persistent")
            sizes["hot_payload_bytes"] = sum(by_name.values())
            sizes["hot_partition_count"] = len(partitions)
            for row in partitions:
                row["hot_payload_bytes"] = by_name[fact_partition_name(row["storage_day"])]
            candidates = [row for row in partitions
                          if after_storage_day is None or row["storage_day"] > after_storage_day]
            deferred = len(candidates) > policy.max_candidate_partitions
            candidates = candidates[:policy.max_candidate_partitions]
            for row in candidates:
                row["fact_types"] = list(query(text("""
                    SELECT DISTINCT fact_type FROM market.fact_versions
                    WHERE storage_day=:day ORDER BY fact_type LIMIT 257
                """), {"day": row["storage_day"]}).scalars())
                if len(row["fact_types"]) > 256:
                    raise RuntimeError(f"canonical_retention_family_budget_exceeded: storage_day={row['storage_day']}")
                row.update(query(text("""
                    SELECT count(*) AS page_count,coalesce(sum(pages.row_count),0) AS archived_rows,
                           count(receipts.manifest_id) AS verified_page_count
                    FROM market.fact_archive_manifests AS pages
                    LEFT JOIN market.fact_archive_verifications AS receipts
                      ON receipts.manifest_id=pages.id AND receipts.verifier_version=:version
                    WHERE pages.storage_day=:day
                """), {"day": row["storage_day"], "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one())
            return {**sizes, "partitions": candidates,
                    "next_after_storage_day": candidates[-1]["storage_day"] if deferred else None}

    @staticmethod
    def _filesystem(storage_root: Path):
        root = Path(storage_root).expanduser().resolve()
        try:
            # In dedicated-HDD mode validate both the configured UUID and the
            # requested path. Development mode still reports capacity without
            # creating a previously absent directory.
            admitted = require_configured_archive_mount(root, require_writable=False)
            evidence = admitted or inspect_filesystem(root, require_writable=False)
            return {"status": "available", "requested_path": str(root), **asdict(evidence)}
        except StorageMountError as exc:
            return {"status": "unavailable", "path": str(root), "error": str(exc)}

    def plan(self, *, policy: CanonicalFactRetentionPolicy, storage_root: Path,
             after_storage_day: date | None = None):
        if after_storage_day is not None and type(after_storage_day) is not date:
            raise ValueError("canonical_retention_cursor_invalid: expected a storage date")
        inventory = self._inventory(policy, after_storage_day=after_storage_day)
        filesystem = self._filesystem(storage_root)
        return self._build_plan(policy=policy, inventory=inventory, filesystem=filesystem)

    @staticmethod
    def _build_plan(*, policy, inventory, filesystem):
        today = inventory["database_day"]
        hot_budget = policy.hot_payload_budget_bytes
        hot_excess = max(0, inventory["hot_payload_bytes"] - hot_budget) if hot_budget is not None else None
        hot_pressure = hot_budget is not None and inventory["hot_payload_bytes"] >= hot_budget
        archive_budget = policy.archive_filesystem_budget_bytes
        archive_excess = None
        archive_blockers = archive_admission_blockers(policy=policy, filesystem=filesystem)
        if filesystem["status"] == "available" and archive_budget is not None:
            archive_excess = max(0, filesystem["used_bytes"] - archive_budget)
        actions = []
        for row in inventory["partitions"]:
            day = row["storage_day"]
            window = policy.hot_window_days(row["fact_types"])
            cutoff = today - timedelta(days=window)
            blockers = []
            if day >= today:
                blockers.append("active_or_future_storage_day")
            elif day >= cutoff:
                blockers.append("hot_window_not_elapsed")
            unsupported = unproven_reclamation_fact_types(row["fact_types"])
            if unsupported:
                blockers.append("canonical_dependency_proof_required")
            if row["state"] == "open":
                action = "seal_partition"
            elif row["state"] == "sealed":
                if row["archived_rows"] > row["expected_rows"]:
                    raise RuntimeError(f"canonical_retention_progress_invalid: storage_day={day} archived rows exceed sealed rows")
                action = ("stage_page" if row["archived_rows"] < row["expected_rows"]
                          else "verify_page" if row["verified_page_count"] < row["page_count"]
                          else "verify_partition")
            elif row["state"] == "verified":
                if row["archived_rows"] != row["expected_rows"]:
                    raise RuntimeError(f"canonical_retention_progress_invalid: storage_day={day} verified source coverage disagrees")
                stale = (row["verified_page_count"] != row["page_count"]
                         or (row["page_count"] == 0 and row["manifest_set_hash"] != _partition_manifest_set_hash(day, 0, [])))
                action = "restart_verification" if stale else "reclaim_partition"
            else:
                raise RuntimeError(f"canonical_retention_progress_invalid: storage_day={day} state={row['state']}")
            # Lack of HDD headroom blocks publication, not byte-preserving
            # reclamation of an already verified copy. Every action still
            # requires an available writable archive mount before execution.
            blockers.extend(item for item in archive_blockers if action in {"seal_partition", "stage_page"}
                            or item in {"archive_mount_unavailable", "archive_mount_read_only"})
            actions.append({
                "action": action, "storage_day": day.isoformat(), "state": row["state"],
                "fact_types": row["fact_types"], "unproven_fact_types": unsupported,
                "hot_window_days": window, "eligible_before": cutoff.isoformat(),
                "window_elapsed_on": (day + timedelta(days=window + 1)).isoformat(),
                "eligible": not blockers, "blockers": blockers,
                "requires_execution_recheck": True,
                "hot_payload_bytes": row["hot_payload_bytes"],
                "expected_rows": row["expected_rows"], "archived_rows": int(row["archived_rows"]),
                "page_count": int(row["page_count"]), "verified_page_count": int(row["verified_page_count"]),
            })
        if hot_pressure:
            actions.sort(key=lambda row: (row["action"] != "reclaim_partition", not row["eligible"], row["storage_day"]))
        pressure_actions = []
        if hot_pressure:
            pressure_actions.append("prioritize_verified_window_eligible_hot_reclamation")
        if archive_blockers:
            pressure_actions.append("restore_archive_mount_or_add_archive_headroom_before_publication")
        if hot_pressure or archive_blockers:
            pressure_actions.append("operator_review_required_if_safe_work_cannot_restore_headroom")
        cursor = inventory["next_after_storage_day"]
        return {
            "schema_version": "market.canonical_retention_plan.v1",
            "database_day": today.isoformat(), "policy": policy.to_dict(),
            "execution_available": True,
            "execution_enabled": policy.execution_enabled,
            "execution_blockers": [] if policy.execution_enabled else ["canonical_retention_execution_disabled"],
            "inventory": {name: inventory[name] for name in (
                "database_bytes", "canonical_header_bytes", "raw_mapping_bytes", "hot_payload_bytes", "hot_partition_count")},
            "archive_filesystem": filesystem,
            "pressure": {"hot_payload_excess_bytes": hot_excess,
                         "hot_payload_budget_reached": hot_pressure,
                         "archive_filesystem_excess_bytes": archive_excess,
                         "archive_write_blockers": archive_blockers, "actions": pressure_actions,
                         "may_override_hot_windows_or_evidence_checks": False,
                         "may_change_ingestion_policy": False},
            "actions": actions, "candidate_count": len(actions),
            "eligible_count": sum(row["eligible"] for row in actions),
            "next_after_storage_day": cursor.isoformat() if cursor else None,
            "candidate_scan_complete": cursor is None,
            "metadata_eligible_reclaim_bytes": sum(row["hot_payload_bytes"] for row in actions
                if row["eligible"] and row["action"] == "reclaim_partition"),
        }


canonical_fact_retention_repository = PostgresCanonicalFactRetentionRepository()


def require_hot_window_elapsed(session, partition, *, policy: CanonicalFactRetentionPolicy):
    """Repeat family/window admission under the partition owner's row lock."""
    day = partition["storage_day"]
    today = session.execute(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")).scalar_one()
    families = session.execute(text(
        "SELECT DISTINCT fact_type FROM market.fact_versions WHERE storage_day=:day ORDER BY fact_type LIMIT 257"
    ), {"day": day}).scalars().all()
    if len(families) > 256:
        raise RuntimeError(f"canonical_retention_family_budget_exceeded: storage_day={day}")
    unsupported = unproven_reclamation_fact_types(families)
    if unsupported:
        raise RuntimeError(f"canonical_retention_dependency_proof_required: storage_day={day} fact_types={unsupported}")
    cutoff = today - timedelta(days=policy.hot_window_days(families))
    if day >= cutoff:
        raise RuntimeError(f"canonical_retention_hot_window_not_elapsed: storage_day={day} eligible_before={cutoff}")
    return cutoff

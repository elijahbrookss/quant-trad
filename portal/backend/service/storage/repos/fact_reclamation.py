"""Default-disabled, one-partition physical reclamation after cold admission.

This primitive is intentionally not wired to the scheduler or CLI yet. It never
deletes canonical headers, archive bytes, dataset bindings, or dependency pins.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from time import monotonic

from sqlalchemy import text

from core.storage_mounts import require_configured_archive_mount
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from portal.backend.db.fact_storage_schema import fact_partition_name
from .market_lifecycle import _LIFECYCLE_LOCK_NAME, MarketStorageLifecycleBusyError

logger = logging.getLogger(__name__)

# Only standalone source facts have complete dependency admission today. Book
# state, trade-flow, derivative composites, and normalized windows still need
# transitive proofs. A mixed physical day must pass for EVERY family in it.
_ADMITTED_FACT_TYPES = frozenset({
    "candle.ohlcv", "derivatives.funding_rate", "derivatives.open_interest",
    "market.reference_price", "market.reserve_balance", "market.trade",
})


@dataclass(frozen=True)
class FactReclamationLimits:
    statement_timeout_ms: int = 1000
    max_handoff_seconds: int = 10

    def __post_init__(self):
        for field in ("statement_timeout_ms", "max_handoff_seconds"):
            if type(getattr(self, field)) is not int or getattr(self, field) <= 0:
                raise ValueError(f"canonical_reclaim_limit_invalid: field={field}")


class PostgresCanonicalFactReclamationRepository:
    def __init__(self, *, archive_repository, enabled: bool = False,
                 limits: FactReclamationLimits = FactReclamationLimits()):
        if type(enabled) is not bool:
            raise ValueError("canonical_reclaim_enabled_must_be_boolean")
        self.archive = archive_repository
        self.enabled = enabled
        self.limits = limits

    def _timeouts(self, session):
        session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                        {"timeout": str(self.limits.statement_timeout_ms)})

    @staticmethod
    def _assert_archive_admission(objects=None):
        # Ordinary research reads can tolerate a read-only archive mount;
        # destructive hot-tier reclamation cannot. Recheck after a remount,
        # not just when the object store was originally constructed.
        require_configured_archive_mount(require_writable=True)
        if objects is not None:
            objects.assert_unchanged()

    @staticmethod
    def _eligibility(session, partition, *, eligible_before):
        day = partition["storage_day"]
        today = session.execute(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")).scalar_one()
        if day >= eligible_before or eligible_before > today:
            raise RuntimeError(f"canonical_reclaim_outside_cutoff: storage_day={day} eligible_before={eligible_before} database_day={today}")
        if partition["state"] not in {"verified", "reclaimed"}:
            raise RuntimeError(f"canonical_reclaim_not_verified: storage_day={day} state={partition['state']}")
        families = session.execute(text(
            "SELECT DISTINCT fact_type FROM market.fact_versions WHERE storage_day=:day LIMIT 65"
        ), {"day": day}).scalars().all()
        unsupported = set(families) - _ADMITTED_FACT_TYPES
        if unsupported:
            raise RuntimeError(f"canonical_reclaim_dependency_proof_required: storage_day={day} fact_types={sorted(unsupported)}")

    @staticmethod
    def _relation(session, day, *, reclaimed=False):
        name = fact_partition_name(day)
        row = session.execute(text("""
            SELECT relation.oid, relation.relkind, relation.relispartition,
                   pg_get_expr(relation.relpartbound, relation.oid) AS bounds,
                   ARRAY(SELECT inhparent::regclass::text FROM pg_inherits WHERE inhrelid=relation.oid) AS parents
            FROM pg_class AS relation JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
            WHERE namespace.nspname='market' AND relation.relname=:name
        """), {"name": name}).mappings().one_or_none()
        if reclaimed:
            if row is not None:
                raise RuntimeError(f"canonical_reclaim_relation_reappeared: storage_day={day}")
            return None
        expected_bounds = f"FOR VALUES FROM ('{day.isoformat()}') TO ('{(day + timedelta(days=1)).isoformat()}')"
        if (row is None or row["relkind"] != "r" or not row["relispartition"]
                or row["parents"] != ["market.fact_hot_payloads"] or row["bounds"] != expected_bounds):
            raise RuntimeError(f"canonical_reclaim_relation_scope_invalid: storage_day={day}")
        return row["oid"]

    @staticmethod
    def _pinned_ranges(session, day):
        # A pin prevents evidence expiry, not verified tier movement. All
        # headers and their complete cold payloads survive, so no pinned range
        # is subtracted from the source. Count overlapping bindings for audit.
        return session.execute(text("""
            SELECT count(*) FROM market.dataset_series AS pins WHERE EXISTS (
                SELECT 1 FROM market.fact_versions AS versions WHERE versions.storage_day=:day
                  AND versions.series_id=pins.series_id AND versions.observation_time>=pins.range_start
                  AND versions.observation_time<pins.range_end AND versions.market_commit_seq<=pins.max_commit_seq)
        """), {"day": day}).scalar_one()

    def reclaim_partition(self, day: date, *, eligible_before: date, execute: bool = False,
                          verification_limits: ArchiveVerificationLimits = ArchiveVerificationLimits()):
        """Recheck cold evidence; optionally DROP exactly one daily hot relation.

        Both instance enablement and explicit execution are required. Dry-run
        does the same admission reads but never seals, publishes, or updates.
        Fresh-byte work uses a shared fence. A separate transaction then tries
        the exclusive fence, rechecks catalogs/files, and atomically commits
        DROP plus progress. There is no in-place advisory lock upgrade.
        """
        if type(day) is not date or type(eligible_before) is not date or type(execute) is not bool:
            raise ValueError("canonical_reclaim_request_invalid")
        if execute and not self.enabled:
            raise RuntimeError("canonical_reclaim_disabled: explicitly enable only after validated rollout")
        try:
            return self._reclaim_partition(day, eligible_before=eligible_before, execute=execute,
                                           verification_limits=verification_limits)
        except Exception:
            logger.exception("canonical_hot_partition_reclaim_failed | storage_day=%s execute=%s", day, execute)
            raise

    def _reclaim_partition(self, day, *, eligible_before, execute, verification_limits):
        self._assert_archive_admission()
        with self.archive.database.session() as session:
            self._timeouts(session)
            partition = self.archive._lock(session, day)
            self._eligibility(session, partition, eligible_before=eligible_before)
            already_reclaimed = partition["state"] == "reclaimed"
            relation_oid = self._relation(session, day, reclaimed=already_reclaimed)
            objects = ArchiveVerificationBatch(self.archive.object_store, limits=verification_limits)
            evidence = self.archive._partition_evidence(session, partition, limits=verification_limits, objects=objects)
            if evidence["manifest_set_hash"] != partition["manifest_set_hash"]:
                raise RuntimeError(f"canonical_reclaim_evidence_changed: storage_day={day}")
            self._assert_archive_admission(objects)
            evidence["protected_dataset_ranges"] = self._pinned_ranges(session, day)
            if already_reclaimed:
                return {**evidence, "status": "already_reclaimed", "reclaimed_bytes": partition["reclaimed_bytes"]}
            if not execute:
                size = session.execute(text("SELECT pg_total_relation_size(:oid)"), {"oid": relation_oid}).scalar_one()
                return {**evidence, "status": "dry_run", "enabled": self.enabled, "reclaimable_bytes": size}

        started = monotonic()
        def check_budget():
            if monotonic() - started >= self.limits.max_handoff_seconds:
                raise RuntimeError(f"canonical_reclaim_handoff_budget_exceeded: storage_day={day}")

        with self.archive.database.session() as session:
            self._timeouts(session)
            acquired = session.execute(text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"
            ), {"name": _LIFECYCLE_LOCK_NAME}).scalar_one()
            if not acquired:
                raise MarketStorageLifecycleBusyError(f"canonical_reclaim_lifecycle_busy: storage_day={day}; retry later")
            # _lock's shared acquisition is reentrant on THIS connection's
            # already-owned exclusive fence, never a second-connection wait.
            partition = self.archive._lock(session, day)
            self._eligibility(session, partition, eligible_before=eligible_before)
            if partition["state"] == "reclaimed":
                self._relation(session, day, reclaimed=True)
                raise RuntimeError(f"canonical_reclaim_progress_changed: storage_day={day}; retry to inspect committed progress")
            if self._relation(session, day) != relation_oid:
                raise RuntimeError(f"canonical_reclaim_relation_changed: storage_day={day}")
            current = self.archive._partition_evidence(session, partition, limits=verification_limits, check_budget=check_budget)
            if current["manifest_set_hash"] != evidence["manifest_set_hash"] or current["manifest_set_hash"] != partition["manifest_set_hash"]:
                raise RuntimeError(f"canonical_reclaim_evidence_changed: storage_day={day}")
            current["protected_dataset_ranges"] = self._pinned_ranges(session, day)
            self._assert_archive_admission(objects)
            check_budget()
            # Never wait behind a query/collector holding the parent. This
            # intentionally small final window can be retried without damage.
            session.execute(text("LOCK TABLE ONLY market.fact_hot_payloads IN ACCESS EXCLUSIVE MODE NOWAIT"))
            name = fact_partition_name(day)
            session.execute(text(f'LOCK TABLE market."{name}" IN ACCESS EXCLUSIVE MODE NOWAIT'))
            if self._relation(session, day) != relation_oid:
                raise RuntimeError(f"canonical_reclaim_relation_changed: storage_day={day}")
            size = session.execute(text("SELECT pg_total_relation_size(:oid)"), {"oid": relation_oid}).scalar_one()
            check_budget()
            # Exact generated daily relation only; no CASCADE or row DELETE.
            session.execute(text(f'DROP TABLE market."{name}"'))
            self._assert_archive_admission(objects)
            check_budget()
            updated = session.execute(text("""
                UPDATE market.fact_retention_partitions
                SET state='reclaimed', reclaimed_at=clock_timestamp(), reclaimed_bytes=:bytes
                WHERE storage_day=:day AND state='verified'
            """), {"day": day, "bytes": size})
            if updated.rowcount != 1:
                raise RuntimeError(f"canonical_reclaim_progress_update_failed: storage_day={day}")
            check_budget()
        logger.info("canonical_hot_partition_reclaimed | storage_day=%s rows=%s reclaimed_bytes=%s protected_dataset_ranges=%s manifest_set_hash=%s",
                    day, current["row_count"], size, current["protected_dataset_ranges"], current["manifest_set_hash"])
        return {**current, "status": "partition_reclaimed", "reclaimed_bytes": size}

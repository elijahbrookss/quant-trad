"""Default-disabled, one-partition physical reclamation after cold admission.

The lifecycle executor supplies explicit policy/step bounds. This owner never
deletes canonical headers, archive bytes, dataset bindings, or dependency pins.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import isfinite
import logging
from time import monotonic, perf_counter

from sqlalchemy import text

from core.storage_mounts import require_configured_archive_mount
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from portal.backend.db.fact_storage_schema import fact_partition_name
from .fact_dependencies import SELF_CONTAINED_FACT_TYPES
from .fact_handoff import FactHandoffProof
from .market_lifecycle import _LIFECYCLE_LOCK_NAME, MarketStorageLifecycleBusyError

logger = logging.getLogger(__name__)

# Standalone facts, book states and response/flow/basis/derivative composites
# have lossless dependency admission. Spec-bound normalized windows use the
# recursive v12 proof. A mixed physical day must pass for EVERY family in it.
_ADMITTED_FACT_TYPES = SELF_CONTAINED_FACT_TYPES | frozenset({
    "market.trade", "market.trade_flow", "market.trade_flow_feature",
    "market.l2_book", "market.bbo", "market.depth_observation",
    "market.futures_spot_relationship",
    "market.derivative_state",
    "market.market_response",
})


def unproven_reclamation_fact_types(fact_types):
    """Shared admission status for planning and the final destructive gate."""
    return sorted(family for family in set(fact_types) - _ADMITTED_FACT_TYPES
                  if not family.startswith("market.normalized."))


_PINNED_RANGES_QUERY = text("""
    /* canonical_reclaim_pinned_ranges */
    WITH candidates AS MATERIALIZED (
        SELECT pins.dataset_id, pins.series_id, pins.range_start, pins.range_end,
               pins.max_commit_seq,
               GREATEST(pins.range_start, min(bounds.first_observation_at)) AS first_observation_at,
               max(bounds.last_observation_at) AS last_observation_at
        FROM market.dataset_series AS pins
        JOIN market.fact_archive_series AS bounds ON bounds.series_id=pins.series_id
        JOIN market.fact_archive_manifests AS pages ON pages.id=bounds.manifest_id
        WHERE pages.storage_day=:day AND bounds.first_observation_at<pins.range_end
          AND bounds.last_observation_at>=pins.range_start
        GROUP BY pins.dataset_id, pins.series_id, pins.range_start, pins.range_end, pins.max_commit_seq
    ), search_ranges AS MATERIALIZED (
        SELECT series_id, first_observation_at, last_observation_at,
               range_end, max_commit_seq
        FROM candidates
        GROUP BY series_id, first_observation_at, last_observation_at,
                 range_end, max_commit_seq
    ), matched_ranges AS MATERIALIZED (
        SELECT ranges.series_id, ranges.first_observation_at,
               ranges.last_observation_at, ranges.range_end,
               ranges.max_commit_seq
        FROM search_ranges AS ranges
        CROSS JOIN LATERAL (
            SELECT 1
            FROM market.fact_versions AS versions
            WHERE versions.storage_day=:day
              AND versions.series_id=ranges.series_id
              AND versions.observation_time>=ranges.first_observation_at
              AND versions.observation_time<=ranges.last_observation_at
              AND versions.observation_time<ranges.range_end
              AND versions.market_commit_seq<=ranges.max_commit_seq
            LIMIT 1
        ) AS matched
    )
    SELECT count(*)
    FROM candidates AS pins
    JOIN matched_ranges AS matched
      ON matched.series_id=pins.series_id
     AND matched.first_observation_at=pins.first_observation_at
     AND matched.last_observation_at=pins.last_observation_at
     AND matched.range_end=pins.range_end
     AND matched.max_commit_seq=pins.max_commit_seq
""")

_RECLAIM_ELIGIBILITY_FAMILIES_QUERY = text(
    "SELECT DISTINCT fact_type FROM market.fact_versions WHERE storage_day=:day LIMIT 65"
)
_RECLAIM_RELATION_QUERY = text("""
    SELECT relation.oid, relation.relkind, relation.relispartition,
           pg_get_expr(relation.relpartbound, relation.oid) AS bounds,
           ARRAY(SELECT inhparent::regclass::text FROM pg_inherits WHERE inhrelid=relation.oid) AS parents
    FROM pg_class AS relation JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
    WHERE namespace.nspname='market' AND relation.relname=:name
""")


@dataclass(frozen=True)
class FactReclamationLimits:
    statement_timeout_ms: int = 1000
    pinned_ranges_statement_timeout_ms: int = 1000
    max_handoff_seconds: int = 10

    def __post_init__(self):
        for field in ("statement_timeout_ms", "pinned_ranges_statement_timeout_ms",
                      "max_handoff_seconds"):
            if type(getattr(self, field)) is not int or getattr(self, field) <= 0:
                raise ValueError(f"canonical_reclaim_limit_invalid: field={field}")
        for field in ("statement_timeout_ms", "pinned_ranges_statement_timeout_ms"):
            if getattr(self, field) > 60000:
                raise ValueError(f"canonical_reclaim_limit_invalid: field={field}")
        if self.max_handoff_seconds > 60:
            raise ValueError("canonical_reclaim_limit_invalid: field=max_handoff_seconds")


class PostgresCanonicalFactReclamationRepository:
    def __init__(self, *, archive_repository, enabled: bool = False,
                 limits: FactReclamationLimits = FactReclamationLimits(),
                 remaining_run_seconds=None):
        if type(enabled) is not bool:
            raise ValueError("canonical_reclaim_enabled_must_be_boolean")
        if remaining_run_seconds is not None and not callable(remaining_run_seconds):
            raise ValueError("canonical_reclaim_remaining_run_seconds_invalid")
        self.archive = archive_repository
        self.enabled = enabled
        self.limits = limits
        self.remaining_run_seconds = remaining_run_seconds

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
        families = session.execute(
            _RECLAIM_ELIGIBILITY_FAMILIES_QUERY, {"day": day}
        ).scalars().all()
        unsupported = unproven_reclamation_fact_types(families)
        if unsupported:
            raise RuntimeError(f"canonical_reclaim_dependency_proof_required: storage_day={day} fact_types={sorted(unsupported)}")

    @staticmethod
    def _relation(session, day, *, reclaimed=False):
        name = fact_partition_name(day)
        row = session.execute(
            _RECLAIM_RELATION_QUERY, {"name": name}
        ).mappings().one_or_none()
        if reclaimed:
            if row is not None:
                raise RuntimeError(f"canonical_reclaim_relation_reappeared: storage_day={day}")
            return None
        expected_bounds = f"FOR VALUES FROM ('{day.isoformat()}') TO ('{(day + timedelta(days=1)).isoformat()}')"
        if (row is None or row["relkind"] != "r" or not row["relispartition"]
                or row["parents"] != ["market.fact_hot_payloads"] or row["bounds"] != expected_bounds):
            raise RuntimeError(f"canonical_reclaim_relation_scope_invalid: storage_day={day}")
        return row["oid"]

    def _pinned_ranges_timeout_ms(self, *, remaining_seconds=None):
        self.archive._check_budget()
        # The pin audit has its own policy timeout. The executor already
        # caps it by the general execution policy; the ordinary reclaimer's
        # one-second query bound must not silently override that setting.
        candidates = [self.limits.pinned_ranges_statement_timeout_ms]
        for clock in (self.remaining_run_seconds, remaining_seconds):
            if clock is None:
                continue
            seconds = clock()
            if type(seconds) not in {int, float} or not isfinite(seconds) or seconds <= 0:
                raise RuntimeError("canonical_reclaim_pinned_ranges_budget_exhausted")
            milliseconds = int(seconds * 1000)
            if milliseconds <= 0:
                raise RuntimeError("canonical_reclaim_pinned_ranges_budget_insufficient")
            candidates.append(milliseconds)
        return min(candidates)

    def _pinned_ranges(self, session, day, *, remaining_seconds=None):
        # A pin prevents evidence expiry, not verified tier movement. All
        # headers and their complete cold payloads survive, so no pinned range
        # is subtracted from the source. Count overlapping bindings for audit.
        # Both callers have just checked complete partition/catalog evidence.
        # Materialize exact binding envelopes, deduplicate identical search
        # envelopes, then stop each indexed series/time seek at its first exact
        # eligible header. The final join counts every distinct Dataset binding
        # while avoiding a full materialized scan of each candidate series/day.
        # Bounds can contain gaps; the header, placement day, exclusive range
        # end and frozen commit checks remain exact. The page's last clock is
        # inclusive and is not a substitute for the binding's exclusive end.
        timeout = self._pinned_ranges_timeout_ms(remaining_seconds=remaining_seconds)
        logger.info(
            "canonical_reclaim_pinned_ranges_started | storage_day=%s statement_timeout_ms=%s",
            day, timeout,
        )
        session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                        {"timeout": str(timeout)})
        count = session.execute(_PINNED_RANGES_QUERY, {"day": day}).scalar_one()
        # Restore only the setting widened for this audit. _timeouts() marks
        # transaction-phase admission in the handoff tests and also resets the
        # unchanged lock timeout; invoking it here would create a false phase.
        session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                        {"timeout": str(self.limits.statement_timeout_ms)})
        logger.info(
            "canonical_reclaim_pinned_ranges_finished | storage_day=%s statement_timeout_ms=%s protected_dataset_ranges=%s",
            day, timeout, count,
        )
        return count, timeout

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
            partition = self.archive._lock(session, day, statement_timeout_ms=self.limits.statement_timeout_ms)
            self._eligibility(session, partition, eligible_before=eligible_before)
            unfinished = session.execute(text(
                "SELECT id FROM market.fact_archive_preparations "
                "WHERE storage_day=:day AND state IN ('building','ready') ORDER BY page_ordinal,generation LIMIT 1"
            ), {"day": day}).scalar_one_or_none()
            if unfinished is not None:
                raise RuntimeError(
                    f"canonical_reclaim_archive_preparation_incomplete: storage_day={day} preparation_id={unfinished}"
                )
            already_reclaimed = partition["state"] == "reclaimed"
            relation_oid = self._relation(session, day, reclaimed=already_reclaimed)
            objects = ArchiveVerificationBatch(self.archive.object_store, limits=verification_limits,
                                               check_budget=self.archive.check_budget)
            handoff = FactHandoffProof(check_budget=self.archive._check_budget) if execute and not already_reclaimed else None
            with objects.distinct_references():
                evidence = self.archive._partition_evidence(session, partition, limits=verification_limits,
                    objects=objects, handoff=handoff)
            if evidence["manifest_set_hash"] != partition["manifest_set_hash"]:
                raise RuntimeError(f"canonical_reclaim_evidence_changed: storage_day={day}")
            self._assert_archive_admission(objects)
            if handoff is not None:
                handoff.bind(day, relation_oid, evidence, verification_limits)
                # Keep the current source working set near the handoff. This
                # extra shared-phase check is inside the original run budget;
                # the exclusive phase MUST still read/compare every row again.
                prepared = perf_counter()
                handoff.prepare_headers(session, self.archive._check_budget)
                self._assert_archive_admission(objects)
                logger.info("canonical_reclaim_handoff_prepared | storage_day=%s source_rows=%s seconds=%s witness_bytes=%s",
                            day, sum(item.count for item in handoff.source_headers),
                            perf_counter() - prepared, handoff.retained_bytes)
            evidence["protected_dataset_ranges"], evidence["pinned_ranges_statement_timeout_ms"] = (
                self._pinned_ranges(session, day))
            if already_reclaimed:
                return {**evidence, "status": "already_reclaimed", "reclaimed_bytes": partition["reclaimed_bytes"]}
            if not execute:
                size = session.execute(text("SELECT pg_total_relation_size(:oid)"), {"oid": relation_oid}).scalar_one()
                return {**evidence, "status": "dry_run", "enabled": self.enabled, "reclaimable_bytes": size}

        started = monotonic()
        logger.info("canonical_reclaim_handoff_started | storage_day=%s pages=%s max_seconds=%s",
                    day, evidence["page_count"], self.limits.max_handoff_seconds)
        def check_budget():
            self.archive._check_budget()
            if monotonic() - started >= self.limits.max_handoff_seconds:
                raise RuntimeError(f"canonical_reclaim_handoff_budget_exceeded: storage_day={day}")

        def remaining_handoff_seconds():
            check_budget()
            return self.limits.max_handoff_seconds - (monotonic() - started)

        with self.archive.database.session() as session:
            self._timeouts(session)
            acquired = session.execute(text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"
            ), {"name": _LIFECYCLE_LOCK_NAME}).scalar_one()
            if not acquired:
                raise MarketStorageLifecycleBusyError(f"canonical_reclaim_lifecycle_busy: storage_day={day}; retry later")
            # _lock's shared acquisition is reentrant on THIS connection's
            # already-owned exclusive fence, never a second-connection wait.
            partition = self.archive._lock(session, day, statement_timeout_ms=self.limits.statement_timeout_ms)
            self._eligibility(session, partition, eligible_before=eligible_before)
            if partition["state"] == "reclaimed":
                self._relation(session, day, reclaimed=True)
                raise RuntimeError(f"canonical_reclaim_progress_changed: storage_day={day}; retry to inspect committed progress")
            if self._relation(session, day) != relation_oid:
                raise RuntimeError(f"canonical_reclaim_relation_changed: storage_day={day}")
            current = self.archive._partition_evidence(session, partition, limits=verification_limits,
                check_budget=check_budget, handoff=handoff)
            if current["manifest_set_hash"] != evidence["manifest_set_hash"] or current["manifest_set_hash"] != partition["manifest_set_hash"]:
                raise RuntimeError(f"canonical_reclaim_evidence_changed: storage_day={day}")
            if current["source_placement_hash"] != evidence["source_placement_hash"]:
                raise RuntimeError(f"canonical_reclaim_source_placement_changed: storage_day={day}; retry to verify current files")
            current["protected_dataset_ranges"], current["pinned_ranges_statement_timeout_ms"] = (
                self._pinned_ranges(session, day,
                                    remaining_seconds=remaining_handoff_seconds))
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

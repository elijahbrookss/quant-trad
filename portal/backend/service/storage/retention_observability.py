"""Bounded, metadata-only projection of canonical retention progress.

This module is deliberately an observer. It never calls the retention planner,
opens archive objects, reads fact payloads, or acquires lifecycle ownership.
"""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import text

from core.settings import get_settings
from core.storage_targets import StoragePolicy
from portal.backend.service.provenance import source_revision


PARTITION_SCAN_LIMIT = 2048
MANIFEST_SCAN_LIMIT = 10000
STATEMENT_TIMEOUT_MS = 5000
LOCK_TIMEOUT_MS = 250


_PROJECTION_SQL = text("""
WITH clock AS (
    SELECT CAST(:sampled_at AS timestamptz) AS sampled_at,
           (CAST(:sampled_at AS timestamptz) AT TIME ZONE 'UTC')::date AS database_day
),
partition_page AS (
    SELECT storage_day,state,created_at,expected_rows,source_bytes,sealed_at,
           verified_at,reclaimed_at,reclaimed_bytes
    FROM market.fact_retention_partitions
    ORDER BY storage_day
    LIMIT :partition_page_limit
),
partitions AS (
    SELECT * FROM partition_page ORDER BY storage_day LIMIT :partition_limit
),
manifest_page AS (
    SELECT m.id,m.storage_day,m.row_count,m.byte_count,m.acknowledged_at
    FROM market.fact_archive_manifests AS m
    ORDER BY m.acknowledged_at DESC,m.storage_day DESC,m.id DESC
    LIMIT :manifest_page_limit
),
manifest_projection AS (
    SELECT m.storage_day,m.row_count,m.byte_count,m.acknowledged_at,
           verification.verified_at
    FROM manifest_page AS m
    LEFT JOIN LATERAL (
        SELECT max(v.verified_at) AS verified_at
        FROM market.fact_archive_verifications AS v
        WHERE v.manifest_id=m.id
    ) AS verification ON true
),
manifests AS (
    SELECT * FROM manifest_projection
    ORDER BY acknowledged_at DESC,storage_day DESC
    LIMIT :manifest_limit
)
SELECT
    clock.sampled_at,clock.database_day,
    (SELECT count(*) FROM partition_page) > :partition_limit AS partitions_truncated,
    (SELECT count(*) FROM manifest_page) > :manifest_limit AS manifests_truncated,
    count(partitions.storage_day) AS partition_count,
    count(*) FILTER (WHERE state='open') AS open_partitions,
    count(*) FILTER (WHERE state='sealed') AS sealed_partitions,
    count(*) FILTER (WHERE state='verified') AS verified_partitions,
    count(*) FILTER (WHERE state='reclaimed') AS reclaimed_partitions,
    coalesce(sum(expected_rows) FILTER (WHERE state<>'reclaimed'),0)::bigint AS estimated_hot_rows,
    coalesce(sum(source_bytes) FILTER (WHERE state IN ('sealed','verified')),0)::bigint AS physical_hot_source_bytes,
    count(*) FILTER (WHERE state='open' AND storage_day < clock.database_day-:recent_days) AS eligible_open_partitions,
    count(*) FILTER (WHERE state='sealed' AND storage_day < clock.database_day-:recent_days) AS eligible_sealed_partitions,
    count(*) FILTER (WHERE state='verified' AND storage_day < clock.database_day-:recent_days) AS eligible_verified_partitions,
    coalesce(sum(source_bytes) FILTER (
        WHERE state<>'reclaimed' AND storage_day < clock.database_day-:recent_days
    ),0)::bigint AS eligible_source_bytes,
    coalesce(sum(source_bytes) FILTER (
        WHERE state='verified' AND storage_day < clock.database_day-:recent_days
    ),0)::bigint AS verified_guarded_source_bytes,
    min(storage_day) FILTER (
        WHERE state<>'reclaimed' AND storage_day < clock.database_day-:recent_days
    ) AS oldest_eligible_day,
    (SELECT sealed_at FROM partitions WHERE sealed_at IS NOT NULL
        ORDER BY sealed_at DESC,storage_day DESC LIMIT 1) AS last_sealed_at,
    (SELECT storage_day FROM partitions WHERE sealed_at IS NOT NULL
        ORDER BY sealed_at DESC,storage_day DESC LIMIT 1) AS last_sealed_day,
    (SELECT verified_at FROM partitions WHERE verified_at IS NOT NULL
        ORDER BY verified_at DESC,storage_day DESC LIMIT 1) AS last_partition_verified_at,
    (SELECT storage_day FROM partitions WHERE verified_at IS NOT NULL
        ORDER BY verified_at DESC,storage_day DESC LIMIT 1) AS last_partition_verified_day,
    (SELECT reclaimed_at FROM partitions WHERE reclaimed_at IS NOT NULL
        ORDER BY reclaimed_at DESC,storage_day DESC LIMIT 1) AS last_reclaimed_at,
    (SELECT storage_day FROM partitions WHERE reclaimed_at IS NOT NULL
        ORDER BY reclaimed_at DESC,storage_day DESC LIMIT 1) AS last_reclaimed_day,
    coalesce(sum(reclaimed_bytes),0)::bigint AS cumulative_reclaimed_bytes,
    (SELECT coalesce(sum(byte_count),0)::bigint FROM manifests) AS compressed_archive_bytes,
    (SELECT coalesce(sum(row_count),0)::bigint FROM manifests) AS archived_rows,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE verified_at IS NOT NULL),0)::bigint FROM manifests) AS verified_archive_bytes,
    (SELECT max(acknowledged_at) FROM manifests) AS last_archived_at,
    (SELECT storage_day FROM manifests ORDER BY acknowledged_at DESC,storage_day DESC LIMIT 1) AS last_archived_day,
    (SELECT max(verified_at) FROM manifests) AS last_page_verified_at,
    (SELECT storage_day FROM manifests WHERE verified_at IS NOT NULL ORDER BY verified_at DESC,storage_day DESC LIMIT 1) AS last_page_verified_day,
    coalesce(sum(source_bytes) FILTER (WHERE sealed_at >= clock.sampled_at-interval '6 hours'),0)::bigint AS intake_6h,
    coalesce(sum(source_bytes) FILTER (WHERE sealed_at >= clock.sampled_at-interval '24 hours'),0)::bigint AS intake_24h,
    coalesce(sum(source_bytes) FILTER (WHERE sealed_at >= clock.sampled_at-interval '7 days'),0)::bigint AS intake_7d,
    coalesce(sum(reclaimed_bytes) FILTER (WHERE reclaimed_at >= clock.sampled_at-interval '6 hours'),0)::bigint AS reclaim_6h,
    coalesce(sum(reclaimed_bytes) FILTER (WHERE reclaimed_at >= clock.sampled_at-interval '24 hours'),0)::bigint AS reclaim_24h,
    coalesce(sum(reclaimed_bytes) FILTER (WHERE reclaimed_at >= clock.sampled_at-interval '7 days'),0)::bigint AS reclaim_7d,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE acknowledged_at >= clock.sampled_at-interval '6 hours'),0)::bigint FROM manifests) AS archive_6h,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE acknowledged_at >= clock.sampled_at-interval '24 hours'),0)::bigint FROM manifests) AS archive_24h,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE acknowledged_at >= clock.sampled_at-interval '7 days'),0)::bigint FROM manifests) AS archive_7d,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE verified_at >= clock.sampled_at-interval '6 hours'),0)::bigint FROM manifests) AS verify_6h,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE verified_at >= clock.sampled_at-interval '24 hours'),0)::bigint FROM manifests) AS verify_24h,
    (SELECT coalesce(sum(byte_count) FILTER (WHERE verified_at >= clock.sampled_at-interval '7 days'),0)::bigint FROM manifests) AS verify_7d
FROM clock LEFT JOIN partitions ON true
GROUP BY clock.sampled_at,clock.database_day
""")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _aware_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None and value.utcoffset() is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _latest_progress(row: dict[str, Any]) -> dict[str, Any] | None:
    candidates = []
    for stage, time_key, day_key in (
        ("sealed", "last_sealed_at", "last_sealed_day"),
        ("archive_page_committed", "last_archived_at", "last_archived_day"),
        ("archive_page_verified", "last_page_verified_at", "last_page_verified_day"),
        ("partition_verified", "last_partition_verified_at", "last_partition_verified_day"),
        ("reclaimed", "last_reclaimed_at", "last_reclaimed_day"),
    ):
        if row.get(time_key) is not None:
            candidates.append((row[time_key], stage, row.get(day_key)))
    if not candidates:
        return None
    happened_at, stage, storage_day = max(candidates, key=lambda item: item[0])
    return {"stage": stage, "storage_day": _iso(storage_day), "happened_at": _iso(happened_at),
            "authority": "durable_catalog"}


def _worker_phase(workers: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    if len(workers) > 100:
        return {"state": "unknown", "reason": "maintenance_worker_inventory_limit"}
    candidates = []
    for worker in workers:
        context = worker.get("context")
        lifecycle = context.get("storage_lifecycle") if isinstance(context, dict) else None
        if (worker.get("worker_role") in (None, "market_storage_maintenance")
                and isinstance(lifecycle, dict)):
            candidates.append((worker, lifecycle))
    live = []
    invalid_clock = False
    for worker, lifecycle in candidates:
        heartbeat = _aware_time(worker.get("heartbeat_at"))
        expires = _aware_time(worker.get("expires_at"))
        if heartbeat is None or expires is None or heartbeat > now:
            invalid_clock = True
            continue
        if expires > now and worker.get("state") not in {"stopping", "stopped"}:
            live.append((worker, lifecycle, heartbeat))
    if not live:
        if invalid_clock:
            return {"state": "unknown", "reason": "maintenance_clock_invalid"}
        return {"state": "stale" if candidates else "unknown",
                "reason": "maintenance_worker_expired" if candidates
                          else "maintenance_worker_missing"}
    if len(live) != 1:
        return {"state": "unknown", "reason": "multiple_maintenance_workers"}
    worker, lifecycle, heartbeat = live[0]
    if lifecycle.get("state") not in {"running", "degraded"}:
        return {"state": "unknown", "reason": "maintenance_loop_not_running",
                "worker_id": worker.get("worker_id"), "heartbeat_at": heartbeat.isoformat()}
    phase = lifecycle.get("current_phase")
    if isinstance(phase, dict):
        started = _aware_time(phase.get("started_at"))
        observed = _aware_time(phase.get("observed_at")) if phase.get("observed_at") is not None else None
        if started is None or started > now or (phase.get("observed_at") is not None
                                                and (observed is None or observed > now)):
            return {"state": "unknown", "reason": "maintenance_clock_invalid",
                    "worker_id": worker.get("worker_id"),
                    "heartbeat_at": heartbeat.isoformat()}
        return {"state": "running", "worker_id": worker.get("worker_id"),
                "heartbeat_at": heartbeat.isoformat(), **phase}
    return {"state": "idle", "worker_id": worker.get("worker_id"),
            "heartbeat_at": heartbeat.isoformat(), "last_run": lifecycle.get("last_run")}


def _recovery_floor(*, policy: StoragePolicy, targets: list[dict[str, Any]],
                    sampled_at: datetime) -> dict[str, Any]:
    settings = get_settings()
    path = settings.storage.maintenance_limits_path
    if not path:
        return {"state": "unknown", "reason": "maintenance_limits_unconfigured",
                "sampled_at": sampled_at.isoformat()}
    try:
        from portal.backend.service.storage.maintenance_runtime import read_storage_maintenance_limits
        _, recovery, _ = read_storage_maintenance_limits(path)
    except (OSError, ValueError, TypeError) as exc:
        return {"state": "unknown", "reason": "maintenance_limits_unavailable",
                "detail": type(exc).__name__, "sampled_at": sampled_at.isoformat()}
    target_id = policy.recent[0] if len(policy.recent) == 1 else None
    target = next((item for item in targets if item.get("target_id") == target_id), None)
    capacity = target.get("capacity") if isinstance(target, dict) else None
    headroom = recovery.get("headroom_bytes", {}).get(target_id)
    if not isinstance(capacity, dict) or type(headroom) is not int:
        return {"state": "unknown", "reason": "recent_capacity_or_headroom_unavailable",
                "sampled_at": sampled_at.isoformat()}
    total = capacity.get("total_bytes")
    available = capacity.get("available_bytes")
    reserved = target.get("reserved_bytes", 0)
    if any(type(value) is not int for value in (total, available, reserved)):
        return {"state": "unknown", "reason": "recent_capacity_invalid",
                "sampled_at": sampled_at.isoformat()}
    reserve = (total * policy.reserve_percent + 99) // 100
    floor = reserve + reserved + headroom
    return {"state": "above_floor" if available >= floor else "below_floor",
            "sampled_at": sampled_at.isoformat(), "target_id": target_id,
            "available_bytes": available, "floor_bytes": floor,
            "margin_bytes": available - floor, "policy_reserve_bytes": reserve,
            "registered_reserved_bytes": reserved, "maintenance_headroom_bytes": headroom,
            "basis": "current_filesystem_sample_plus_saved_policy_and_declared_recovery_headroom"}


class RetentionObservability:
    """Build one bounded projection from durable catalog metadata."""

    def snapshot(self, session, *, policy_payload: dict[str, Any], revision: int,
                 targets: list[dict[str, Any]], workers: list[dict[str, Any]],
                 sampled_at: datetime | None = None) -> dict[str, Any]:
        policy = StoragePolicy.from_dict(policy_payload)
        settings = get_settings()
        canonical = settings.market_data_lifecycle.canonical_retention
        session.execute(text(
            f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_MS}ms'"
        ))
        session.execute(text(
            f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"
        ))
        sampled_at = sampled_at or session.scalar(text("SELECT clock_timestamp()"))
        if sampled_at.tzinfo is None:
            sampled_at = sampled_at.replace(tzinfo=UTC)
        values = {"sampled_at": sampled_at, "partition_limit": PARTITION_SCAN_LIMIT,
                  "partition_page_limit": PARTITION_SCAN_LIMIT + 1,
                  "manifest_limit": MANIFEST_SCAN_LIMIT,
                  "manifest_page_limit": MANIFEST_SCAN_LIMIT + 1,
                  "recent_days": policy.recent_days}
        row = dict(session.execute(_PROJECTION_SQL, values).mappings().one())
        sampled_at = row["sampled_at"]
        if sampled_at.tzinfo is None:
            sampled_at = sampled_at.replace(tzinfo=UTC)
        eligible = sum(int(row[f"eligible_{state}_partitions"] or 0)
                       for state in ("open", "sealed", "verified"))
        truncated = bool(row["partitions_truncated"] or row["manifests_truncated"])
        configured_overrides = dict(canonical.hot_days_by_fact_type)
        # The installed maintenance service uses saved_canonical_policy(), which
        # replaces the base hot window and clears family overrides. Mirror that
        # effective policy here; inactive base settings do not make the
        # saved-policy backlog incomplete.
        incomplete = truncated or int(row["eligible_open_partitions"] or 0) > 0
        blockers = []
        for stage, key in (("awaiting_seal", "eligible_open_partitions"),
                           ("awaiting_archive_or_verification", "eligible_sealed_partitions"),
                           ("verified_waiting_for_fresh_reclaim_guards", "eligible_verified_partitions")):
            if row[key]:
                blockers.append({"stage": stage, "partition_count": int(row[key])})
        windows = []
        for name, seconds, suffix in (("6h", 21600, "6h"), ("24h", 86400, "24h"),
                                      ("7d", 604800, "7d")):
            windows.append({"name": name, "window_seconds": seconds,
                "intake": {"estimated_source_bytes": int(row[f"intake_{suffix}"] or 0),
                           "basis": "partition_source_bytes_at_seal_time_proxy"},
                "archive": {"compressed_bytes": int(row[f"archive_{suffix}"] or 0)},
                "verify": {"compressed_bytes": int(row[f"verify_{suffix}"] or 0)},
                "reclaim": {"physical_hot_bytes": int(row[f"reclaim_{suffix}"] or 0)}})
        oldest = row.get("oldest_eligible_day")
        return {
            "schema_version": "qt.storage_retention_observability.v1",
            "state": "partial" if incomplete else "ready",
            "sampled_at": sampled_at.isoformat(), "source_revision": source_revision(),
            "bounds": {"statement_timeout_ms": STATEMENT_TIMEOUT_MS,
                       "lock_timeout_ms": LOCK_TIMEOUT_MS,
                       "partition_rows": PARTITION_SCAN_LIMIT,
                       "partition_scan_rows": PARTITION_SCAN_LIMIT + 1,
                       "manifest_rows": MANIFEST_SCAN_LIMIT,
                       "manifest_scan_rows": MANIFEST_SCAN_LIMIT + 1,
                       "verification_rows_per_manifest": None,
                       "verification_scan_backstop": "primary_key_scope_plus_statement_timeout",
                       "metadata_only": True},
            "completeness": {"complete": not incomplete, "truncated": truncated,
                "partition_scan_truncated": bool(row["partitions_truncated"]),
                "manifest_scan_truncated": bool(row["manifests_truncated"]),
                "unknowns": (["open_partition_source_bytes_unmeasured"]
                               if row["eligible_open_partitions"] else [])},
            "policy": {"revision": revision, "fingerprint": policy.fingerprint,
                "saved_recent_days": policy.recent_days,
                "effective_default_hot_days": policy.recent_days,
                "effective_fact_type_hot_days": {},
                "configured_base_hot_days": canonical.hot_days,
                "configured_base_fact_type_hot_days": configured_overrides,
                "saved_policy_execution_enabled": policy.movement_enabled,
                "eligibility_basis": "saved_history_policy_default_window_with_family_overrides_cleared"},
            "lifecycle": {"current": _worker_phase(workers, now=sampled_at),
                          "last_durable_advancement": _latest_progress(row)},
            "backlog": {"eligible_partitions": eligible,
                "eligible_source_bytes": int(row["eligible_source_bytes"] or 0),
                "verified_guarded_partitions": int(row["eligible_verified_partitions"] or 0),
                "verified_guarded_source_bytes": int(row["verified_guarded_source_bytes"] or 0),
                "oldest_eligible_day": _iso(oldest),
                "oldest_eligible_age_days": ((row["database_day"] - oldest).days if oldest else None),
                "stages": blockers},
            "storage": {
                "physical_hot": {"source_bytes": int(row["physical_hot_source_bytes"] or 0),
                    "estimated_rows": int(row["estimated_hot_rows"] or 0),
                    "basis": "non_reclaimed_sealed_partition_catalog"},
                "compressed_archive": {"bytes": int(row["compressed_archive_bytes"] or 0),
                    "rows": int(row["archived_rows"] or 0),
                    "verified_bytes": int(row["verified_archive_bytes"] or 0)},
                "cumulative_reclaimed_bytes": int(row["cumulative_reclaimed_bytes"] or 0),
                "filesystem_delta": {"state": "unavailable", "reason": "one_sample_has_no_aligned_delta"},
                "permanent_metadata_growth": {"state": "unavailable",
                    "reason": "no_durable_aligned_metadata_growth_series"}},
            "rates": {"windows": windows,
                "eta": {"state": "unavailable",
                        "reason": "aligned_physical_intake_and_reclaim_series_required"}},
            "recovery_floor": _recovery_floor(policy=policy, targets=targets, sampled_at=sampled_at),
        }


retention_observability = RetentionObservability()

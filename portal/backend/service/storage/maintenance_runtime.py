"""Connect the existing lifecycle loop to explicit deployment operating limits.

Saved Storage policy remains the only placement/schedule authority. This file
does not create targets, migrate data, activate policy or add a scheduler.
"""
import json
from functools import partial
from pathlib import Path

from .header_resource_claims import _limits
from .history_maintenance import run_history_maintenance
from .recovery_maintenance import run_due_local_recovery, validate_recovery_maintenance_limits

_PG15 = Path("/usr/lib/postgresql/15/bin")
_MAX_CONFIG_BYTES = 128 * 1024


def _unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("storage_maintenance_duplicate_configuration_field")
        result[key] = value
    return result


def read_storage_maintenance_limits(limits_path):
    """One strict parser shared by maintenance and deployment admission."""
    path = Path(limits_path)
    if not path.is_absolute():
        raise ValueError("storage_maintenance_limits_absolute_path_required")
    with path.open("rb") as handle:
        raw = handle.read(_MAX_CONFIG_BYTES + 1)
    if len(raw) > _MAX_CONFIG_BYTES:
        raise ValueError("storage_maintenance_limits_file_too_large")
    payload = json.loads(raw, object_pairs_hook=_unique_fields)
    if (not isinstance(payload, dict) or set(payload) != {"schema_version", "history", "recovery"}
            or payload["schema_version"] not in ("qt.storage_maintenance_limits.v1",
                                               "qt.storage_maintenance_limits.v2")):
        raise ValueError("storage_maintenance_limits_schema_invalid")
    history = _limits(payload["history"])
    recovery = payload["recovery"]
    encrypted = payload["schema_version"] == "qt.storage_maintenance_limits.v2"
    fields = {"max_bytes", "timeout_seconds", "headroom_bytes", "max_objects"}
    if encrypted:
        fields.add("incremental")
    if not isinstance(recovery, dict) or set(recovery) != fields:
        raise ValueError("storage_maintenance_recovery_fields_invalid")
    recovery = dict(recovery)
    incremental = None
    if encrypted:
        from .incremental_recovery import IncrementalRecoveryConfig
        incremental = IncrementalRecoveryConfig.from_dict(recovery.pop("incremental"))
    validate_recovery_maintenance_limits(**recovery)
    return history, recovery, incremental


def storage_maintenance_runners(database, *, storage_root, limits_path=None):
    """Return the existing two runners only when operating limits are explicit."""
    if limits_path is None:
        return {}
    history, recovery, incremental = read_storage_maintenance_limits(limits_path)
    # The existing lifecycle service owns payload archival; bind it to the
    # same saved policy as header movement without adding a second scheduler.
    from ..market.market_storage_lifecycle import MarketStorageLifecycleService
    from .repos.fact_retention import PostgresCanonicalFactRetentionRepository
    return {
        "service": MarketStorageLifecycleService(
            canonical_repository=PostgresCanonicalFactRetentionRepository(database=database),
            use_saved_history_policy=True),
        "history_runner": partial(run_history_maintenance, database,
            pg_controldata=_PG15/"pg_controldata", resource_limits=history),
        "recovery_runner": partial(run_due_local_recovery, database,
            storage_root=storage_root, pg_dump=_PG15/"pg_dump",
            pg_controldata=_PG15/"pg_controldata", incremental=incremental, **recovery),
    }

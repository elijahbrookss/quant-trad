"""Apply the declared auxiliary storage allowances to cold retirement staging."""

from sqlalchemy import text

from core.storage_mounts import StorageMountError
from .header_resources import observe_header_resources
from .history_policy import _read, _target, registered_header_targets
from .repos.market_lifecycle import MarketStorageLifecycleBusyError


class ColdStagePressure(MarketStorageLifecycleBusyError):
    """The selected stage can be retried after temporary storage pressure clears."""


def admit_cold_stage_pressure(session, *, pg_controldata, temporary_bytes, wal_bytes):
    """Bound this staging transaction using the existing maintenance envelope.

    Estimates include all WAL generated on this PostgreSQL cluster during the
    check window. Checks run between bounded archive pages. PostgreSQL makes
    the temporary-file limit per backend, so parallel query workers are disabled.
    """
    saved, _ = _read(session)
    if saved is None or not saved.movement_enabled or len(saved.recent) != 1:
        raise RuntimeError("cold_retirement_stage_policy_unavailable")
    target_id, = saved.recent
    records = {record.id: record for record in registered_header_targets(session)}
    if (target_id not in records or set(records) != set(temporary_bytes)
            or records[target_id].medium != "ssd" or records[target_id].state != "active"):
        raise RuntimeError("cold_retirement_stage_resource_binding_unavailable")
    record = records[target_id]
    target = _target(record)
    targets = tuple(_target(row) for row in records.values())
    try:
        resources = observe_header_resources(session.connection(), targets, pg_controldata=pg_controldata)
    except StorageMountError as exc:
        raise ColdStagePressure("cold_retirement_stage_resources_unavailable") from exc
    roles = {"database", "database_default", "wal", "temporary_files"}
    bindings = [item for item in resources.bindings if item["role"] in roles]
    if ({item["role"] for item in bindings} != roles
            or {item["target_id"] for item in bindings} != {target_id}):
        raise RuntimeError("cold_retirement_stage_wal_or_temporary_storage_not_on_recent_ssd")
    cap_kib = min(temporary_bytes[target_id] // 1024, 2**31 - 1)
    temp_kib = session.scalar(text("""
        SELECT set_config('temp_file_limit',
            least(CASE WHEN setting::bigint<0 THEN :cap ELSE setting::bigint END, :cap)::text, true)
        FROM pg_catalog.pg_settings WHERE name='temp_file_limit'
    """), {"cap": cap_kib})
    if temp_kib is None:
        raise RuntimeError("cold_retirement_stage_temp_limit_unavailable")
    effective_temp = session.scalar(text("""
        SELECT setting::bigint * 1024 FROM pg_catalog.pg_settings WHERE name='temp_file_limit'
    """))
    session.execute(text("SET LOCAL max_parallel_workers_per_gather=0"))
    start = session.scalar(text("SELECT pg_catalog.pg_current_wal_insert_lsn()::text"))

    def check():
        try:
            current = target.inspect(require_writable=False)
        except StorageMountError as exc:
            raise ColdStagePressure("cold_retirement_stage_recent_mount_unavailable") from exc
        if current.device_id != resources.capacity[target_id].device_id:
            raise ColdStagePressure("cold_retirement_stage_recent_device_changed")
        free = current.available_bytes
        wal = session.scalar(text("""
            SELECT pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_insert_lsn(), CAST(:start AS pg_lsn))
        """), {"start": start})
        if wal < 0 or wal >= wal_bytes:
            raise ColdStagePressure("cold_retirement_stage_wal_budget_reached")
        need = int(wal_bytes - wal) + effective_temp + record.reserved_bytes + record.auxiliary_reserved_bytes
        if free <= need:
            raise ColdStagePressure(f"cold_retirement_stage_ssd_runway_unavailable: free_bytes={free} needed_bytes={need}")

    check()
    return check

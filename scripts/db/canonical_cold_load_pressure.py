"""Admission and physical headroom for the inactive cold catalog loader."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from core.storage_mounts import StorageMountError
from portal.backend.service.storage.header_admission import registered_header_targets
from portal.backend.service.storage.header_filesystem import _device_id, _process_readlink, _same_process_file
from portal.backend.service.storage.header_inspection import _bound_resource_targets
from portal.backend.service.storage.header_movement import _MoveWatch
from portal.backend.service.storage.header_resources import observe_header_resources
from portal.backend.service.storage_management import _target
from scripts.db.canonical_cold_custody_placement import _target as registered_tablespace


class LoadPressure(RuntimeError):
    """Leave the source active and resume from the last committed page."""


@dataclass(frozen=True)
class CatalogLoadLimits:
    recent_target_id: str
    history_target_id: str
    min_recent_free_bytes: int
    min_history_free_bytes: int
    max_page_wal_bytes: int
    max_page_history_bytes: int
    max_page_recent_catalog_bytes: int
    max_resident_wal_bytes: int
    max_ready_wal_segments: int
    temp_file_limit_kib: int

    def __post_init__(self):
        if (not all(isinstance(key, str) and key for key in (self.recent_target_id, self.history_target_id))
                or self.recent_target_id == self.history_target_id):
            raise ValueError("canonical_cold_load_pressure_invalid_target_ids")
        for field, value in vars(self).items():
            if field.endswith("_id"):
                continue
            floor = 0 if field == "max_ready_wal_segments" else 1
            ceiling = 2**31 - 1 if field == "temp_file_limit_kib" else 2**63 - 1
            if type(value) is not int or not floor <= value <= ceiling:
                raise ValueError(f"canonical_cold_load_pressure_invalid_limit: {field}")


_WAL = """
    SELECT pg_current_wal_insert_lsn()::text AS lsn,
           (SELECT coalesce(sum(size),0) FROM pg_catalog.pg_ls_waldir()) AS resident,
           (SELECT count(*) FROM pg_catalog.pg_ls_archive_statusdir()
            WHERE name ~ '^[0-9A-F]{24}[.]ready$') AS ready
"""


def _measure_wal(conn, *, limits, previous_lsn=None):
    observed = conn.execute(text(_WAL)).one()
    if observed.resident > limits.max_resident_wal_bytes:
        raise LoadPressure("canonical_cold_load_resident_wal_pressure")
    if observed.ready > limits.max_ready_wal_segments:
        raise LoadPressure("canonical_cold_load_archive_backlog_pressure")
    if previous_lsn is not None:
        generated = conn.scalar(text("SELECT pg_wal_lsn_diff(CAST(:end AS pg_lsn),CAST(:begin AS pg_lsn))"),
                                {"begin": previous_lsn, "end": observed.lsn})
        if not 0 <= generated <= limits.max_page_wal_bytes:
            raise LoadPressure("canonical_cold_load_page_wal_budget_exceeded")
    return observed.lsn


def _require_history_path(conn, resources, history):
    loc = registered_tablespace(conn, resources.database_identity, history.target_id)
    path = Path(loc["location"])
    root = Path(resources.capacity[history.target_id].path)
    if not path.is_absolute() or not path.is_relative_to(root) or path.resolve(strict=True) != path:
        raise LoadPressure("canonical_cold_load_pressure_history_path_changed")
    info = path.stat()
    if _device_id(info.st_dev) != resources.capacity[history.target_id].device_id:
        raise LoadPressure("canonical_cold_load_pressure_history_device_changed")
    database = next(row for row in resources.bindings if row["role"] == "database")
    link = Path(database["directory"]) / "pg_tblspc" / str(loc["tablespace_oid"])
    if (not link.is_symlink() or link.readlink() != path
            or _process_readlink(resources.backend_pid, link) != str(path)):
        raise LoadPressure("canonical_cold_load_pressure_history_link_changed")
    _same_process_file(resources.backend_pid, path, info, require_writable=True)


def _begin_page(session, *, temp_file_limit_kib):
    if not session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.cold.catalog.load.v1',0))")):
        raise LoadPressure("canonical_cold_load_other_loader_active")
    conn = session.connection()
    conn.exec_driver_sql("SET LOCAL max_parallel_workers_per_gather=0")
    session.execute(text("""
        SELECT set_config('temp_file_limit',
            LEAST(CASE WHEN setting::bigint<0 THEN :limit ELSE setting::bigint END,:limit)::text,true)
        FROM pg_settings WHERE name='temp_file_limit'
    """), {"limit": temp_file_limit_kib})
    return conn


@contextmanager
def supervise_page(session, *, limits, pg_controldata: Path, deadline):
    """Check one transaction's native paths and supervise its allocation."""
    conn = _begin_page(session, temp_file_limit_kib=limits.temp_file_limit_kib)
    records = {record.id: record for record in registered_header_targets(session)}
    ids = (limits.recent_target_id, limits.history_target_id)
    if not set(ids) <= records.keys():
        raise LoadPressure("canonical_cold_load_pressure_target_missing")
    recent, history = targets = tuple(_target(records[key]) for key in ids)
    if (recent.medium != "ssd" or history.medium != "hdd" or "recent" not in recent.roles
            or "history" not in history.roles or any(target.state != "active" for target in targets)):
        raise LoadPressure("canonical_cold_load_pressure_target_roles_changed")
    remaining = int(deadline - monotonic())
    if remaining < 1:
        raise LoadPressure("canonical_cold_load_page_deadline")
    try:
        resources = observe_header_resources(conn, targets, pg_controldata=pg_controldata, timeout_seconds=min(remaining, 30))
    except StorageMountError as exc:
        raise LoadPressure("canonical_cold_load_pressure_native_resources_unavailable") from exc
    wal_target, temporary_targets = _bound_resource_targets(resources, targets, resources.capacity)
    if wal_target != recent.target_id or temporary_targets != (recent.target_id,):
        raise LoadPressure("canonical_cold_load_pressure_unreviewed_wal_or_temp_target")
    try:
        _require_history_path(conn, resources, history)
    except (OSError, StorageMountError) as exc:
        raise LoadPressure("canonical_cold_load_pressure_history_path_unavailable") from exc
    minimums = {recent.target_id: limits.min_recent_free_bytes, history.target_id: limits.min_history_free_bytes}
    allowances = {recent.target_id: limits.max_page_wal_bytes + limits.temp_file_limit_kib * 1024
                                    + limits.max_page_recent_catalog_bytes,
                  history.target_id: limits.max_page_history_bytes}
    floors = {}
    for key in ids:
        available = resources.capacity[key].available_bytes
        minimum = minimums[key] + records[key].reserved_bytes + records[key].auxiliary_reserved_bytes
        if available - minimum < allowances[key]:
            raise LoadPressure(f"canonical_cold_load_page_capacity: target={key}")
        floors[key] = available - allowances[key]
    with _watch_page(conn, limits=limits, resources=resources, targets=targets, floors=floors, deadline=deadline):
        yield


@contextmanager
def _watch_page(conn, *, limits, resources, targets, floors, deadline):
    beginning_lsn = _measure_wal(conn, limits=limits)
    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets, capacity=resources.capacity,
                      floors=floors, deadline=deadline, cancelled=None, grace=5)
    try:
        watch.start()
        yield
        watch.check()
        _measure_wal(conn, limits=limits, previous_lsn=beginning_lsn)
    except Exception as exc:
        if watch.failure is not None:
            raise LoadPressure(f"canonical_cold_load_pressure_supervision: {watch.failure}") from exc
        if (isinstance(exc, DBAPIError) and getattr(exc.orig, "pgcode", None) == "53400"
                and (getattr(getattr(exc.orig, "diag", None), "message_primary", "") or "").startswith(
                    "temporary file size exceeds temp_file_limit")):
            raise LoadPressure("canonical_cold_load_page_temp_budget_exceeded") from exc
        raise
    finally:
        watch.stop(conn)

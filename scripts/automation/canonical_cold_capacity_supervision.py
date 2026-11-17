"""Watch the approved physical devices throughout the cutover transaction."""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic

from sqlalchemy import text

from core.storage_mounts import inspect_filesystem
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.header_movement import _MoveWatch


@dataclass(frozen=True)
class _CapacityTarget:
    target_id: str
    path: str
    filesystem_uuid: str

    def inspect(self, *, require_writable=True):
        # PostgreSQL owns write access. The launcher needs a physical
        # observation of that device, even when it cannot write at its root.
        evidence = inspect_filesystem(Path(self.path), expected_uuid=self.filesystem_uuid, require_writable=False)
        if require_writable and evidence.read_only:
            raise RuntimeError("canonical_cold_capacity_supervision_read_only")
        return evidence


def _wal_sample(conn, *, start_lsn=None):
    return dict(conn.execute(text("""
        SELECT pg_current_wal_insert_lsn()::text AS insert_lsn,
               pg_wal_lsn_diff(pg_current_wal_insert_lsn(), CAST(:start_lsn AS pg_lsn))::bigint AS generated_bytes,
               (SELECT coalesce(sum(size),0)::bigint FROM pg_catalog.pg_ls_waldir()) AS resident_wal_bytes,
               (SELECT count(*) FROM pg_catalog.pg_ls_archive_statusdir()
                WHERE name ~ '^[0-9A-F]{24}[.]ready$') AS pending_segments
    """), {"start_lsn": start_lsn}).mappings().one())


@contextmanager
def supervise_approved_capacity(conn, receipt, *, timeout_seconds):
    """Cancel only the active transaction if sampled space crosses its allowance."""
    binding = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    if (not conn.in_transaction() or receipt.get("receipt_hash") != archive_evidence_hash(binding)
            or set(receipt.get("devices", {})) != {"ssd", "hdd"} or type(timeout_seconds) is not int
            or timeout_seconds <= 0):
        raise ValueError("canonical_cold_capacity_supervision_unbound")
    began = monotonic()
    targets, initial, floors = [], {}, {}
    for tier, approved in receipt["devices"].items():
        target = _CapacityTarget(tier, approved["path"], approved["filesystem_uuid"])
        reserve, temporary = approved["reserve_bytes"], approved["temporary_bytes"]
        current = target.inspect()
        if current.path != target.path or current.device_id != approved["device_id"]:
            raise RuntimeError(f"canonical_cold_capacity_supervision_device_changed: tier={tier}")
        if (type(reserve) is not int or type(temporary) is not int or reserve <= 0 or temporary <= 0
                or current.available_bytes < reserve + temporary):
            raise RuntimeError(f"canonical_cold_capacity_supervision_insufficient: tier={tier}")
        targets.append(target)
        initial[tier] = current
        floors[tier] = current.available_bytes - temporary
    seen = {}
    start_wal = _wal_sample(conn)
    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets, capacity=initial,
                      floors=floors, deadline=began + timeout_seconds, cancelled=None, grace=5)
    try:
        watch.start()
        yield seen
        watch.check()
        end_wal = _wal_sample(conn, start_lsn=start_wal["insert_lsn"])
        end = {target.target_id: target.inspect() for target in targets}
        watch.check()
        if type(end_wal["generated_bytes"]) is not int or end_wal["generated_bytes"] < 0:
            raise RuntimeError("canonical_cold_capacity_supervision_wal_changed")
    except Exception as error:
        if watch.failure is not None:
            raise RuntimeError(f"canonical_cold_capacity_supervision_failed: {watch.failure}") from error
        raise
    finally:
        watch.stop(conn)
    if watch.failure is not None:
        raise RuntimeError(f"canonical_cold_capacity_supervision_failed: {watch.failure}")
    seen.update(capacity_receipt_hash=receipt["receipt_hash"], elapsed_seconds=monotonic() - began,
                devices={tier: {"before_available_bytes": initial[tier].available_bytes,
                                "after_available_bytes": end[tier].available_bytes, "floor_bytes": floors[tier]}
                         for tier in initial}, before_wal=start_wal, after_wal=end_wal)
    seen["receipt_hash"] = archive_evidence_hash(seen)

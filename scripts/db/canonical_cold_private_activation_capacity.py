"""Bound one private PostgreSQL promotion transaction on the shared rehearsal HDD."""

from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
from time import monotonic

from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.header_movement import _MoveWatch
from scripts.automation.canonical_cold_capacity_supervision import _wal_sample
from scripts.db.canonical_cold_load_pressure import _bound_temporary_files
from scripts.db.canonical_cold_private_load import observe_private_hdd, shared_hdd_floor


@dataclass(frozen=True)
class SharedHddActivationLimits:
    min_free_bytes: int
    max_wal_bytes: int
    max_resident_wal_bytes: int
    max_ready_wal_segments: int
    temp_file_limit_kib: int
    other_transaction_bytes: int

    def __post_init__(self):
        for field, value in vars(self).items():
            floor = 0 if field == "max_ready_wal_segments" else 1
            maximum = 2**31 - 1 if field == "temp_file_limit_kib" else 2**63 - 1
            if type(value) is not int or not floor <= value <= maximum:
                raise ValueError(f"private_cold_activation_invalid_capacity_limit: {field}")


def load_activation_limits(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= 8192:
        raise ValueError("private_cold_activation_invalid_capacity_file_size")
    return SharedHddActivationLimits(**json.loads(raw))


def _check_wal(sample, limits):
    if (sample["resident_wal_bytes"] > limits.max_resident_wal_bytes
            or sample["pending_segments"] > limits.max_ready_wal_segments):
        raise RuntimeError("private_cold_activation_wal_pressure")
    generated = sample["generated_bytes"]
    if generated is not None and not 0 <= generated <= limits.max_wal_bytes:
        raise RuntimeError("private_cold_activation_wal_budget_exceeded")


@contextmanager
def supervise_private_activation(conn, remaining_seconds, *, clone, placement, execution, limits, pg_controldata):
    """Supervise native moves and acceptance inside the held writer transaction."""
    if (not isinstance(limits, SharedHddActivationLimits) or not conn.in_transaction()
            or not isinstance(pg_controldata, Path) or not pg_controldata.is_absolute() or not pg_controldata.is_file()):
        raise ValueError("private_cold_activation_capacity_unbound")
    began = monotonic()
    deadline = began + remaining_seconds()
    _bound_temporary_files(conn, temp_file_limit_kib=limits.temp_file_limit_kib)
    conn.exec_driver_sql("SET LOCAL max_parallel_maintenance_workers=0")
    record, target, resources = observe_private_hdd(
        conn, clone=clone, placement=placement, pg_controldata=pg_controldata, deadline=deadline,
    )
    scope = execution["raw_mapping_index"]
    custody = execution.get("custody_move")
    if scope["target_id"] != target.target_id or execution["custody_target_id"] != target.target_id:
        raise ValueError("private_cold_activation_capacity_target_changed")
    copies = scope["max_copy_bytes"] + (0 if custody is None else custody["max_copy_bytes"])
    allowance = copies + limits.max_wal_bytes + 1024 * limits.temp_file_limit_kib + limits.other_transaction_bytes
    minimum = limits.min_free_bytes + record.reserved_bytes + record.auxiliary_reserved_bytes
    before = resources.capacity[target.target_id]
    floor = shared_hdd_floor(before.available_bytes, minimum=minimum, allowance=allowance)
    start_wal = _wal_sample(conn)
    _check_wal(start_wal, limits)
    seen = {}
    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=(target,), capacity=resources.capacity,
                      floors={target.target_id: floor}, deadline=deadline, cancelled=None, grace=5)
    try:
        watch.start()
        yield seen
        watch.check()
        end_wal = _wal_sample(conn, start_lsn=start_wal["insert_lsn"])
        _check_wal(end_wal, limits)
        after = target.inspect(require_writable=True)
        watch.check()
        if ((after.path, after.device_id, after.filesystem_uuid) != (before.path, before.device_id, before.filesystem_uuid)
                or after.available_bytes < floor):
            raise RuntimeError("private_cold_activation_capacity_final_sample_changed")
    except Exception as error:
        if watch.failure is not None:
            raise RuntimeError(f"private_cold_activation_capacity_failed: {watch.failure}") from error
        raise
    finally:
        watch.stop(conn)
    if watch.failure is not None:
        raise RuntimeError(f"private_cold_activation_capacity_failed: {watch.failure}")
    seen.update(schema="qt.private_canonical_cold_activation_capacity.v1", target_id=target.target_id,
                device_id=before.device_id, filesystem_uuid=before.filesystem_uuid,
                elapsed_seconds=monotonic() - began, before_available_bytes=before.available_bytes,
                after_available_bytes=after.available_bytes, floor_bytes=floor, min_free_bytes=minimum,
                shared_allowance_bytes=allowance, limits=vars(limits), before_wal=start_wal, after_wal=end_wal)
    seen["receipt_hash"] = archive_evidence_hash(seen)

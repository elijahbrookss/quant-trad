"""Load a verified cold checkpoint on the promoted same-HDD rehearsal clone.

WAL, temporary files, recent catalog rows and history catalog rows all use
one free-space budget. The production loader retains its two-device preflight.
Run this loader in the verified private PostgreSQL process and mount namespace,
with the staged candidate archive read-only.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from functools import partial
import json
import os
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_mounts import StorageMountError, configured_archive_root
from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db.storage_target_models import StorageTargetRecord
from portal.backend.service.storage.header_inspection import _bound_resource_targets
from portal.backend.service.storage.header_resources import observe_header_resources
from portal.backend.service.storage_management import _target
from scripts.db.canonical_cold_load import TargetDatabase, _load_checkpoint
from scripts.db.canonical_cold_load_pressure import LoadPressure, _begin_page, _require_history_path, _watch_page
from scripts.db.canonical_cold_migration import MigrationCheckpoint
from scripts.db.canonical_cold_private_seal import load_private_clone


@dataclass(frozen=True)
class SharedHddLoadLimits:
    min_free_bytes: int
    max_page_wal_bytes: int
    max_page_history_bytes: int
    max_page_recent_catalog_bytes: int
    max_resident_wal_bytes: int
    max_ready_wal_segments: int
    temp_file_limit_kib: int

    def __post_init__(self):
        for field, value in vars(self).items():
            floor = 0 if field == "max_ready_wal_segments" else 1
            ceiling = 2**31 - 1 if field == "temp_file_limit_kib" else 2**63 - 1
            if type(value) is not int or not floor <= value <= ceiling:
                raise ValueError(f"private_cold_load_invalid_limit: {field}")

    @property
    def page_allocation_bytes(self):
        return (self.max_page_wal_bytes + self.max_page_history_bytes
                + self.max_page_recent_catalog_bytes + self.temp_file_limit_kib * 1024)


def shared_hdd_floor(available, *, minimum, allowance):
    if available - minimum < allowance:
        raise LoadPressure("private_cold_shared_hdd_capacity")
    return available - allowance


def observe_private_hdd(conn, *, clone, placement, pg_controldata, deadline):
    from scripts.db.canonical_cold_private_placement import assert_private_placement

    assert_private_placement(conn, clone=clone, placement=placement, require_writable=True)
    target_id = placement["history_target_id"]
    targets_table = StorageTargetRecord.__table__
    record = conn.execute(select(targets_table).where(targets_table.c.id == target_id)).one_or_none()
    if record is None:
        raise LoadPressure("private_cold_load_history_target_missing")
    history = _target(record)
    targets = (history,)
    remaining = int(deadline - monotonic())
    if remaining < 1:
        raise LoadPressure("private_cold_load_page_deadline")
    try:
        resources = observe_header_resources(conn, targets, pg_controldata=pg_controldata,
                                             timeout_seconds=min(remaining, 30))
        if resources.database_identity != clone.proof["database_identity"]:
            raise LoadPressure("private_cold_load_native_database_changed")
        if _bound_resource_targets(resources, targets, resources.capacity) != (target_id, (target_id,)):
            raise LoadPressure("private_cold_load_unbound_wal_or_temp_target")
        if any(binding["target_id"] != target_id for binding in resources.bindings):
            raise LoadPressure("private_cold_load_resource_outside_shared_hdd")
        _require_history_path(conn, resources, history)
    except (OSError, StorageMountError) as exc:
        raise LoadPressure("private_cold_load_native_resources_unavailable") from exc

    observed = resources.capacity[target_id]
    if observed.device_id != clone.proof["device_id"]:
        raise LoadPressure("private_cold_load_native_device_changed")
    return record, history, resources


@contextmanager
def supervise_private_page(session, *, clone, placement, limits, pg_controldata, deadline):
    conn = _begin_page(session, temp_file_limit_kib=limits.temp_file_limit_kib)
    record, history, resources = observe_private_hdd(
        conn, clone=clone, placement=placement, pg_controldata=pg_controldata, deadline=deadline,
    )
    target_id = history.target_id
    try:
        floor = shared_hdd_floor(resources.capacity[target_id].available_bytes,
                                 minimum=limits.min_free_bytes + record.reserved_bytes + record.auxiliary_reserved_bytes,
                                 allowance=limits.page_allocation_bytes)
    except LoadPressure as exc:
        raise LoadPressure("private_cold_load_shared_hdd_page_capacity") from exc
    with _watch_page(conn, limits=limits, resources=resources, targets=(history,),
                     floors={target_id: floor}, deadline=deadline):
        yield


def _limits(path):
    with path.open("rb") as stream:
        raw = stream.read(8193)
    if not 0 < len(raw) <= 8192:
        raise ValueError("private_cold_load_invalid_limits_file_size")
    return SharedHddLoadLimits(**json.loads(raw))


def run(args):
    from scripts.db.canonical_cold_private_placement import assert_private_placement, load_private_placement

    clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
    placement = load_private_placement(args.placement, args.placement_sha256, clone,
                                       clone_proof_sha256=args.proof_sha256)
    system_identifier, oid = clone.proof["database_identity"].split("/")
    target = TargetDatabase(system_identifier, clone.url.database, int(oid))
    limits = _limits(args.limits)
    if (not args.pg_controldata.is_absolute() or not args.pg_controldata.is_file()
            or not 1 <= args.max_seconds <= MAX_MIGRATION_SECONDS):
        raise ValueError("private_cold_load_invalid_binary_or_time_budget")
    archive = configured_archive_root()
    if archive != Path(placement["archive_root"]) or not os.statvfs(archive).f_flag & os.ST_RDONLY:
        raise ValueError("private_cold_load_requires_bound_readonly_candidate_archive")
    store = FilesystemRawArchiveObjectStore(archive / "objects", writable=False)
    engine = create_engine(clone.url, poolclass=NullPool, hide_parameters=True, isolation_level="READ COMMITTED",
                           connect_args={"application_name": "qt_private_cold_load", "connect_timeout": 5,
                                         "passfile": "/dev/null", "options": "-c statement_timeout=30000"})
    try:
        with engine.begin() as conn:
            assert_private_placement(conn, clone=clone, placement=placement, require_writable=True)
        supervise = partial(supervise_private_page, clone=clone, placement=placement,
                            limits=limits, pg_controldata=args.pg_controldata)
        result = _load_checkpoint(
            engine, MigrationCheckpoint(args.checkpoint), target=target, object_store=store, day=args.day,
            after_ordinal=args.after_ordinal, max_pages=args.max_pages, max_seconds=args.max_seconds,
            statement_timeout_ms=30_000, history_target_id=placement["history_target_id"], supervise=supervise,
        )
    finally:
        engine.dispose()
    return {"schema": "qt.private_same_hdd_catalog_load.v1", "clone_proof_sha256": args.proof_sha256,
            "placement_sha256": args.placement_sha256, "device_id": clone.proof["device_id"],
            "shared_page_allocation_bytes": limits.page_allocation_bytes, **result}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("proof", "placement", "checkpoint", "limits", "pg-controldata"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("proof-sha256", "placement-sha256"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--after-ordinal", type=int, default=-1)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--max-seconds", type=int, default=120)
    try:
        result = run(parser.parse_args(argv))
    except SQLAlchemyError as error:
        parser.exit(1, f"private_cold_load_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        parser.exit(1, f"private_cold_load_refused: {error}\n")
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 75 if "deferred_reason" in result else 0


if __name__ == "__main__":
    raise SystemExit(main())

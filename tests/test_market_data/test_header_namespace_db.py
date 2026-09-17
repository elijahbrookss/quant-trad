"""Real PostgreSQL/file verification in a private Unix-socket-only cluster.

The fixture runs as the database OS user in the same filesystem/PID namespaces.
Its two udev UUID entries are synthetic; process identity, pg_controldata,
paths, distinct devices and statvfs/inode probes are real. History uses an
already-mounted private /dev/shm directory. No device is mounted or formatted.
This is correctness qualification, not HDD performance qualification.
"""
from __future__ import annotations

import json
import os
import pwd
import shutil
import subprocess
import sys
import tempfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session
from sqlalchemy.engine import URL

from core.storage_mounts import StorageMountError
from core.storage_targets import StoragePolicy, StorageTarget
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from portal.backend.db.storage_target_models import (
    StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord, StorageHeaderTablespaceRecord,
    StorageHeaderBatchRecord, StorageHeaderMoveRecord,
)
from portal.backend.service.storage.header_destinations import register_header_tablespaces, review_header_moves
from portal.backend.service.storage.header_journal import reserve_header_batch, cancel_unstarted_header_batch
from portal.backend.service.storage.header_inspection import inspect_reserved_header_move
from portal.backend.service.storage_management import StorageConflict
from portal.backend.service.storage.header_catalog import read_header_catalog, read_locked_header_group
from portal.backend.service.storage.header_filesystem import verify_header_filesystem

pytestmark = pytest.mark.db
BIN = Path("/usr/lib/postgresql/15/bin")


def _worker(root, history):
    root, history = root.resolve(strict=True), history.resolve(strict=True)
    if (root.parent != Path("/tmp") or not root.name.startswith("qt-header-ns-")
            or history.parent != Path("/dev/shm") or not history.name.startswith("qt-header-history-")
            or os.geteuid() == 0
            or any(path.stat().st_uid != os.geteuid() or path.stat().st_mode & 0o077
                   for path in (root, history))):
        raise RuntimeError("namespace_fixture_requires_private_roots_and_nonroot_worker")
    if root.stat().st_dev == history.stat().st_dev:
        raise RuntimeError("namespace_fixture_requires_distinct_filesystems")
    if shutil.disk_usage(history).free < 4 * 1024**2:
        raise RuntimeError("namespace_fixture_requires_four_megabytes_of_disposable_history_space")
    data, socket = root / "pgdata", root / "socket"
    socket.mkdir()
    initializing = monotonic()
    subprocess.run([str(BIN / "initdb"), "-D", str(data), "--no-locale", "--encoding=UTF8",
                    "--auth-local=trust", "--auth-host=reject"], check=True, capture_output=True, text=True, timeout=120)
    initialization_seconds = monotonic() - initializing
    options = f"-c listen_addresses= -c unix_socket_directories={socket} -c shared_buffers=16MB -c max_connections=10"
    def control(action):
        args = [str(BIN / "pg_ctl"), "-D", str(data), "-w", "-t", "45"]
        args += ["-l", str(root / "postgres.log"), "-o", options, "start"] if action == "start" else ["-m", "fast", "stop"]
        return subprocess.run(args, check=True, capture_output=True, text=True, timeout=60)
    engine = None
    try:
        control("start")
        engine = create_engine(URL.create("postgresql+psycopg2", username=pwd.getpwuid(os.geteuid()).pw_name,
                                          database="postgres", query={"host": str(socket)}))
        from psycopg2 import sql
        with engine.begin() as conn:
            storage_day = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")) - timedelta(days=45)
            partition_name = fact_header_partition_name(storage_day)
            with conn.connection.driver_connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    CREATE SCHEMA market;
                    CREATE TABLE market.fact_header_partitions(storage_day date PRIMARY KEY);
                    INSERT INTO market.fact_header_partitions VALUES ({day});
                    CREATE TABLE market.fact_versions (id text,storage_day date NOT NULL,payload text)
                        PARTITION BY RANGE(storage_day);
                    CREATE TABLE market.{partition} PARTITION OF market.fact_versions
                        FOR VALUES FROM ({day}) TO ({next_day});
                    CREATE INDEX header_id ON market.fact_versions(id);
                    INSERT INTO market.fact_versions
                        SELECT 'one',{day},string_agg(md5(n::text),'')
                        FROM generate_series(1,2000) n;
                """).format(partition=sql.Identifier(partition_name), day=sql.Literal(storage_day),
                            next_day=sql.Literal(storage_day + timedelta(days=1))))
        before = None
        with engine.connect() as conn:
            before = conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions")).all()
        udev = root / "udev"
        udev.mkdir()
        device = root.stat().st_dev
        metadata = udev / f"b{os.major(device)}:{os.minor(device)}"
        metadata.write_text("E:ID_FS_UUID=uuid-disposable-namespace\n")
        history_device = history.stat().st_dev
        history_metadata = udev / f"b{os.major(history_device)}:{os.minor(history_device)}"
        history_metadata.write_text("E:ID_FS_UUID=uuid-disposable-history\n")
        os.environ["QT_STORAGE_UDEV_ROOT"] = str(udev)
        target = StorageTarget("fixture", "Disposable source", "uuid-disposable-namespace", str(root), "ssd")
        history_target = StorageTarget("history", "Disposable history", "uuid-disposable-history", str(history), "hdd")
        targets = (target, history_target)
        def verify(inventory, assignments=None):
            return verify_header_filesystem(inventory, targets, pg_controldata=BIN / "pg_controldata",
                                            destination_assignments=assignments)
        inventory = read_header_catalog(engine)
        baseline = verify(inventory)
        report = {
            "initialization_seconds": round(initialization_seconds, 3),
            "baseline_bound": all(item.target_id == "fixture" for item in baseline.snapshot.partitions[0].relations),
            "distinct_filesystems": baseline.capacity["fixture"].device_id != baseline.capacity["history"].device_id,
            "real_control_binary": str(BIN / "pg_controldata"),
            "real_process_and_files": True,
            "synthetic_uuid": True,
        }
        # Observe the ordinary index left in pg_default when only the table moves.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            with conn.connection.driver_connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE TABLESPACE qt_fixture_history LOCATION {}").format(sql.Literal(str(history))))
        with engine.connect() as conn:
            destination_oid = conn.scalar(text(
                "SELECT oid::bigint FROM pg_tablespace WHERE spcname='qt_fixture_history'"))
        destination_inventory = read_header_catalog(engine, destination_tablespace_oids=(destination_oid,))
        destination, = verify(destination_inventory, {"history": destination_oid}).destinations
        database_oid = destination_inventory.snapshot.database_identity.split("/")[1]
        report["empty_destination_verified"] = (
            destination.tablespace_name == "qt_fixture_history"
            and Path(destination.directory).is_relative_to(history)
            and not (Path(destination.directory) / database_oid).exists()
        )
        default_inventory = read_header_catalog(engine, destination_tablespace_oids=(1663,))
        default, = verify(default_inventory, {"fixture": 1663}).destinations
        report["default_destination_verified"] = Path(default.directory) == data / "base" / database_oid
        wrong_observation = replace(destination_inventory.destination_tablespaces[0],
                                    location=str(root / "wrong-destination"))
        try:
            verify(replace(destination_inventory, destination_tablespaces=(wrong_observation,)),
                   {"history": destination_oid})
            report["changed_destination_refused"] = False
        except StorageMountError:
            report["changed_destination_refused"] = True

        from tests.test_market_data.header_resource_fixture import prove_resource_paths
        report.update(prove_resource_paths(engine, targets, destination_oid, BIN / "pg_controldata"))
        report.update(_prove_registered_pipeline(engine, targets, destination_oid))

        def move_indexes(conn):
            names = conn.execute(text("""
                SELECT n.nspname,c.relname FROM pg_index i
                JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE i.indrelid=CAST(:header AS regclass)
            """), {"header": "market." + partition_name}).all()
            with conn.connection.driver_connection.cursor() as cursor:
                for schema, name in names:
                    cursor.execute(sql.SQL("ALTER INDEX {}.{} SET TABLESPACE qt_fixture_history").format(
                        sql.Identifier(schema), sql.Identifier(name)))

        class InjectedRollback(Exception):
            pass

        original_files = {item["relation_oid"]: item["relative_path"] for item in inventory.physical_locations}
        original_nodes = {item.oid: item.relfilenode for item in inventory.snapshot.partitions[0].relations}
        for phase in ("table", "whole_group"):
            try:
                with engine.begin() as conn:
                    locked_before = read_locked_header_group(conn, storage_day=storage_day,
                        heap_oid=inventory.snapshot.partitions[0].heap.oid,
                        destination_tablespace_oids=(destination_oid,))
                    bound_before = verify(locked_before, {"history": destination_oid})
                    conn.execute(text(f'ALTER TABLE market."{partition_name}" SET TABLESPACE qt_fixture_history'))
                    if phase == "whole_group":
                        move_indexes(conn)
                        locked_after = read_locked_header_group(conn, storage_day=storage_day,
                            heap_oid=inventory.snapshot.partitions[0].heap.oid,
                            destination_tablespace_oids=(destination_oid,))
                        bound_after = verify(locked_after, {"history": destination_oid})
                        report["same_transaction_group_verified"] = (
                            all(item.target_id == "fixture" for item in bound_before.snapshot.partitions[0].relations)
                            and all(item.target_id == "history" for item in bound_after.snapshot.partitions[0].relations)
                            and not bound_before.snapshot.inventory_complete
                            and not bound_after.snapshot.inventory_complete
                        )
                    raise InjectedRollback()
            except InjectedRollback:
                pass
            restored = read_header_catalog(engine)
            verify(restored)
            with engine.connect() as conn:
                same_rows = conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions")).all() == before
            report["rollback_" + phase] = (
                {item["relation_oid"]: item["relative_path"] for item in restored.physical_locations} == original_files
                and {item.oid: item.relfilenode for item in restored.snapshot.partitions[0].relations} == original_nodes
                and same_rows
            )

        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE market."{partition_name}" SET TABLESPACE qt_fixture_history'))
        split = read_header_catalog(engine)
        verify(split)
        report["table_only_move_has_split_tablespaces"] = len({item["tablespace_oid"] for item in split.physical_locations}) == 2
        with engine.begin() as conn:
            move_indexes(conn)
        moved = read_header_catalog(engine)
        verified = verify(moved)
        report["complete_group_tablespace"] = len({item["tablespace_oid"] for item in moved.physical_locations}) == 1
        report["tablespace_paths_verified"] = all(Path(item["path"]).is_relative_to(history) for item in verified.bindings)
        report["complete_group_target"] = all(item.target_id == "history" for item in verified.snapshot.partitions[0].relations)
        report["toast_colocated"] = moved.snapshot.partitions[0].toast_colocated
        with engine.connect() as conn:
            report["rows_preserved"] = conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions")).all() == before

        def refused(candidate):
            try:
                verify(candidate)
            except StorageMountError:
                return True
            return False
        report["stale_start_refused"] = refused(replace(moved, postmaster_started_at=datetime.fromtimestamp(int(moved.server_postmaster_identity[2]), UTC) - timedelta(seconds=1)))
        report["stale_start_refused"] = report["stale_start_refused"] and refused(replace(moved, server_postmaster_identity=()))
        metadata.write_text("E:ID_FS_UUID=uuid-wrong-device\n")
        report["wrong_uuid_refused"] = refused(moved)
        metadata.write_text("E:ID_FS_UUID=uuid-disposable-namespace\n")
        clone = root / "copied-cluster"
        (clone / "global").mkdir(parents=True)
        for relative in ("PG_VERSION", "postmaster.pid", "global/pg_control"):
            shutil.copyfile(data / relative, clone / relative)
        report["copied_identity_refused"] = refused(replace(moved, server_data_directory=str(clone)))
        from tests.test_market_data.header_atomic_fixture import prove_atomic_moves
        report.update(prove_atomic_moves(engine, root, targets, destination_oid,
                                         storage_day, partition_name, BIN / "pg_controldata"))
        # Relocate WAL only inside this owned disposable root, with its server stopped.
        engine.dispose()
        control("stop")
        relocated = root / "relocated-wal"
        (data / "pg_wal").rename(relocated)
        (data / "pg_wal").symlink_to(Path("../relocated-wal"), target_is_directory=True)
        control("start")
        from portal.backend.service.storage.header_resources import observe_header_resources
        with engine.begin() as conn:
            resources = observe_header_resources(conn, targets, pg_controldata=BIN / "pg_controldata")
        wal = next(item for item in resources.bindings if item["role"] == "wal")
        report["resources_relocated_wal"] = wal["directory"] == str(relocated) and wal["target_id"] == "fixture"
        return report
    finally:
        if engine is not None:
            engine.dispose()
        if (data / "postmaster.pid").exists():
            control("stop")


def _prove_registered_pipeline(engine, targets, destination_oid):
    """Exercise real adapters and transactions before any physical movement."""
    for model in (StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord,
                  StorageHeaderTablespaceRecord, StorageHeaderBatchRecord, StorageHeaderMoveRecord):
        model.__table__.create(engine)
    policy = StoragePolicy(recent=("fixture",), history=("history",),
        archives=("history",), backups=("history",), movement_enabled=True)
    with Session(engine) as session, session.begin():
        for target in targets:
            session.add(StorageTargetRecord(id=target.target_id, label=target.label,
                filesystem_uuid=target.filesystem_uuid, root=target.root, medium=target.medium,
                roles=list(target.roles), reserved_bytes=0))
        session.add(StoragePolicyRecord(id=1, revision=0, policy=policy.to_dict()))
        session.add(StoragePlanRecord(id="namespace-pipeline", request_id="namespace-pipeline",
            base_revision=0, policy=policy.to_dict(), policy_hash=policy.fingerprint,
            state="queued", impact={}, progress={}))
    observed = read_header_catalog(engine, destination_tablespace_oids=(destination_oid,))
    verified = verify_header_filesystem(observed, targets, pg_controldata=BIN / "pg_controldata",
                                        destination_assignments={"history": destination_oid})
    expected_bytes = sum(item.byte_count for item in verified.snapshot.partitions[0].relations)
    if not 0 < expected_bytes <= 4 * 1024**2:
        raise RuntimeError("namespace_pipeline_copy_budget_exceeded")
    with Session(engine) as session, session.begin():
        registration = register_header_tablespaces(session, verified=verified)
    review = review_header_moves(verified=verified, policy=policy, targets=targets,
                                 reserved_bytes={"fixture": 0, "history": 0})
    if not review["planning_complete"] or len(review["moves"]) != 1:
        raise RuntimeError("namespace_pipeline_requires_one_real_cross_target_move")
    with Session(engine) as session, session.begin():
        first = reserve_header_batch(session, plan_id="namespace-pipeline",
                                     review_hash=review["plan_hash"], verified=verified)
    with Session(engine) as session, session.begin():
        retry = reserve_header_batch(session, plan_id="namespace-pipeline",
                                     review_hash=review["plan_hash"], verified=verified)
        row = session.scalar(select(StorageHeaderMoveRecord))
        reserved = session.get(StorageTargetRecord, "history").reserved_bytes
        destination = row.destination_binding
        exact_binding = (
            destination["tablespace_oid"] == destination_oid
            and destination["target_root"] == targets[1].root
            and destination["filesystem_uuid"] == targets[1].filesystem_uuid
            and destination["directory_inode"] == verified.destinations[0].directory_inode
            and row.heap_oid == verified.snapshot.partitions[0].heap.oid
        )
    with Session(engine) as session, session.begin():
        inspection = inspect_reserved_header_move(session, move_id=first["moves"][0]["id"],
            review_hash=review["plan_hash"], pg_controldata=BIN / "pg_controldata")
        try:
            with Session(engine) as competing, competing.begin():
                cancel_unstarted_header_batch(competing, plan_id="namespace-pipeline",
                                               review_hash=review["plan_hash"])
            cancellation_busy = False
        except StorageConflict as exc:
            cancellation_busy = str(exc) == "storage_journal_busy"
    with Session(engine) as session, session.begin():
        cancelled = cancel_unstarted_header_batch(session, plan_id="namespace-pipeline",
                                                  review_hash=review["plan_hash"])
    after = read_header_catalog(engine)
    with Session(engine) as session:
        released = session.get(StorageTargetRecord, "history").reserved_bytes == 0
    return {
        "real_pipeline_registered": registration == [
            {"target_id": "history", "tablespace_oid": destination_oid, "reused": False}],
        "real_pipeline_reserved_bytes": reserved,
        "real_pipeline_expected_bytes": expected_bytes,
        "real_pipeline_exact_binding": exact_binding,
        "real_pipeline_idempotent": retry["reused"] and retry["moves"] == first["moves"],
        "real_pipeline_cancelled": cancelled["cancelled"] and released,
        "real_pipeline_source_unmoved": observed.physical_locations == after.physical_locations,
        "real_inspection_copy_bytes": inspection.copy_bytes,
        "real_inspection_blocks_cancellation": cancellation_busy,
        "real_inspection_execution_disabled": not inspection.execution_available,
    }


@pytest.fixture(scope="module")
def namespace_report():
    if os.environ.get("QT_DB_TEST_ISOLATED") != "1":
        raise RuntimeError("namespace_fixture_requires_supported_isolated_db_runner")
    if not (BIN / "initdb").is_file():
        raise RuntimeError("namespace_fixture_requires_postgresql15_server_tools")
    command_prefix = []
    account = None
    if os.geteuid() == 0:
        account = pwd.getpwnam("postgres")
        runuser = shutil.which("runuser")
        if not runuser:
            raise RuntimeError("namespace_fixture_requires_runuser")
        command_prefix = [runuser, "-u", "postgres", "--"]
    root = Path(tempfile.mkdtemp(prefix="qt-header-ns-", dir="/tmp"))
    history = None
    # Always retain ownership of both allocated paths, including setup failures.
    try:
        history = Path(tempfile.mkdtemp(prefix="qt-header-history-", dir="/dev/shm"))
        if account is not None:
            for directory in (root, history):
                os.chown(directory, account.pw_uid, account.pw_gid)
    except BaseException:
        for directory in (history, root):
            if directory is not None:
                directory.rmdir()  # freshly allocated and still empty
        raise
    command = [*command_prefix, sys.executable, "-m", "tests.test_market_data.test_header_namespace_db",
               str(root), str(history)]
    # No inherited DSN, passwords or dotenv: only the private socket is used.
    source_root = Path(__file__).resolve().parents[2]
    environment = {"PATH": os.environ["PATH"], "PYTHONPATH": f"{source_root / 'src'}:{source_root}",
                   "QT_DISABLE_DOTENV": "1", "PYTHONUTF8": "1"}
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True,
                                timeout=300, cwd=source_root, env=environment)
        report = json.loads(result.stdout.strip().splitlines()[-1])
    except subprocess.CalledProcessError as exc:
        log = root / "postgres.log"
        server_tail = log.read_text(errors="replace")[-4000:] if log.exists() else ""
        raise RuntimeError("namespace_worker_failed:\n" + (exc.stdout or "")[-4000:]
                           + (exc.stderr or "")[-8000:] + server_tail) from exc
    finally:
        # This directory was created above; never clean an externally supplied PGDATA.
        resolved = root.resolve(strict=True)
        if resolved != root or resolved.parent != Path("/tmp") or not resolved.name.startswith("qt-header-ns-"):
            raise RuntimeError("namespace_fixture_cleanup_path_mismatch")
        if (resolved / "pgdata" / "postmaster.pid").exists():
            subprocess.run([*command_prefix, str(BIN / "pg_ctl"), "-D", str(resolved / "pgdata"),
                            "-w", "-t", "45", "-m", "fast", "stop"],
                           check=True, capture_output=True, text=True, timeout=60, env=environment)
        resolved_history = history.resolve(strict=True)
        if (resolved_history != history or resolved_history.parent != Path("/dev/shm")
                or not resolved_history.name.startswith("qt-header-history-")):
            raise RuntimeError("namespace_history_cleanup_path_mismatch")
        shutil.rmtree(resolved_history)
        shutil.rmtree(resolved)
    report["disposable_roots_removed"] = not root.exists() and not history.exists()
    print("namespace_fixture_report=" + json.dumps(report, sort_keys=True))
    return report


def test_real_cluster_identity_and_files_bind_without_inherited_credentials(namespace_report):
    assert namespace_report["baseline_bound"]
    assert namespace_report["real_process_and_files"]
    assert namespace_report["synthetic_uuid"]


def test_real_table_and_index_tablespace_changes_are_observed_and_preserve_rows(namespace_report):
    for key in ("table_only_move_has_split_tablespaces", "complete_group_tablespace",
                "tablespace_paths_verified", "toast_colocated", "rows_preserved"):
        assert namespace_report[key], key


def test_real_table_and_index_movement_rolls_back_as_a_group(namespace_report):
    assert namespace_report["rollback_table"]
    assert namespace_report["rollback_whole_group"]


def test_real_cluster_copies_stale_process_evidence_and_wrong_uuid_are_refused(namespace_report):
    for key in ("stale_start_refused", "wrong_uuid_refused", "copied_identity_refused"):
        assert namespace_report[key], key


def test_real_empty_and_default_tablespace_destinations_are_verified_before_copy(namespace_report):
    assert namespace_report["empty_destination_verified"]
    assert namespace_report["default_destination_verified"]


def test_real_tablespace_destination_cannot_be_repointed_by_observation(namespace_report):
    assert namespace_report["changed_destination_refused"]


def test_real_history_group_crosses_distinct_filesystems(namespace_report):
    assert namespace_report["distinct_filesystems"]
    assert namespace_report["complete_group_target"]


def test_real_catalog_and_filesystem_proof_feed_registration_and_reservation(namespace_report):
    assert namespace_report["real_pipeline_registered"]
    assert namespace_report["real_pipeline_exact_binding"]
    assert 0 < namespace_report["real_pipeline_reserved_bytes"] == namespace_report["real_pipeline_expected_bytes"]
    assert namespace_report["real_pipeline_idempotent"]


def test_real_unstarted_cancellation_preserves_source_and_releases_its_reservation(namespace_report):
    assert namespace_report["real_pipeline_cancelled"]
    assert namespace_report["real_pipeline_source_unmoved"]


def test_real_reserved_move_inspection_holds_ownership_without_executing(namespace_report):
    assert namespace_report["real_inspection_copy_bytes"] == namespace_report["real_pipeline_expected_bytes"]
    assert namespace_report["real_inspection_blocks_cancellation"]
    assert namespace_report["real_inspection_execution_disabled"]
    assert namespace_report["real_pipeline_source_unmoved"]


def test_locked_group_files_verify_before_and_after_uncommitted_copy_on_same_connection(namespace_report):
    assert namespace_report["same_transaction_group_verified"]


def test_both_private_filesystem_roots_are_cleaned_after_postmaster_stops(namespace_report):
    assert namespace_report["disposable_roots_removed"]


@pytest.mark.parametrize("phase", ["table", "index"])
def test_atomic_move_python_failure_rolls_back_its_savepoint_without_losing_caller_work(namespace_report, phase):
    assert namespace_report["atomic_savepoint_" + phase]


def test_atomic_move_completion_and_capacity_release_roll_back_with_outer_transaction(namespace_report):
    assert namespace_report["atomic_completion_is_provisional"]
    assert namespace_report["atomic_outer_rollback"]


@pytest.mark.parametrize("phase", ["table", "index", "before_commit"])
def test_atomic_move_backend_termination_preserves_source_intent_and_reservation(namespace_report, phase):
    assert namespace_report["atomic_backend_loss_" + phase]


def test_real_lost_commit_response_reconciles_without_copying_or_releasing_twice(namespace_report):
    assert namespace_report["atomic_lost_commit_is_committed"]
    assert namespace_report["atomic_retry_no_copy_or_release"]
    assert namespace_report["atomic_rows_preserved"]


def test_atomic_index_only_move_preserves_destination_heap_and_refuses_stale_completion(namespace_report):
    assert namespace_report["atomic_retains_destination_heap"]
    assert namespace_report["atomic_changed_completion_refused"]


def test_blocked_completion_bookkeeping_times_out_and_rolls_back_copy(namespace_report):
    assert namespace_report["atomic_bookkeeping_timeout"]


@pytest.mark.parametrize("case", [
    "default_bound", "preserve_transaction", "custom_and_fallback",
    "real_temp_allocation", "existing_temp_directory", "stale_temp_name_refused",
    "quoted_temp_name", "relocated_wal",
])
def test_real_resource_roots_and_caller_settings_are_verified(namespace_report, case):
    assert namespace_report["resources_" + case]


if __name__ == "__main__":
    try:
        print(json.dumps(_worker(Path(sys.argv[1]), Path(sys.argv[2])), sort_keys=True))
    except subprocess.TimeoutExpired as exc:
        for label, value in (("stdout", exc.stdout), ("stderr", exc.stderr)):
            if value:
                value = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
                print("namespace_timeout_" + label + "=" + value[-4000:], file=sys.stderr)
        raise

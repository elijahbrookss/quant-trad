"""Real PostgreSQL/file verification in a private Unix-socket-only cluster.

The fixture runs as the database OS user in the same filesystem/PID namespaces.
Its udev UUID entry is synthetic; process identity, pg_controldata, paths and
statvfs/device/inode probes are real. This is not HDD performance qualification.
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
from datetime import timedelta
from pathlib import Path
from time import monotonic

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from core.storage_mounts import StorageMountError
from core.storage_targets import StorageTarget
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_filesystem import verify_header_filesystem

pytestmark = pytest.mark.db
BIN = Path("/usr/lib/postgresql/15/bin")


def _worker(root):
    root = root.resolve(strict=True)
    if root.parent != Path("/tmp") or not root.name.startswith("qt-header-ns-") or os.geteuid() == 0:
        raise RuntimeError("namespace_fixture_requires_private_root_and_nonroot_worker")
    data, socket, history = root / "pgdata", root / "socket", root / "history"
    socket.mkdir()
    history.mkdir()
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
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE SCHEMA market;
                CREATE TABLE market.fact_header_partitions(storage_day date PRIMARY KEY);
                INSERT INTO market.fact_header_partitions VALUES ('2026-09-01');
                CREATE TABLE market.fact_versions (id text,storage_day date NOT NULL,payload text)
                    PARTITION BY RANGE(storage_day);
                CREATE TABLE market.fact_versions_20260901 PARTITION OF market.fact_versions
                    FOR VALUES FROM ('2026-09-01') TO ('2026-09-02');
                CREATE INDEX header_id ON market.fact_versions(id);
                INSERT INTO market.fact_versions
                    SELECT 'one','2026-09-01'::date,string_agg(md5(n::text),'')
                    FROM generate_series(1,2000) n;
            """))
        before = None
        with engine.connect() as conn:
            before = conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions")).all()
        udev = root / "udev"
        udev.mkdir()
        device = root.stat().st_dev
        metadata = udev / f"b{os.major(device)}:{os.minor(device)}"
        metadata.write_text("E:ID_FS_UUID=uuid-disposable-namespace\n")
        os.environ["QT_STORAGE_UDEV_ROOT"] = str(udev)
        target = StorageTarget("fixture", "Disposable filesystem", "uuid-disposable-namespace", str(root), "ssd")
        def verify(inventory, assignments=None):
            return verify_header_filesystem(inventory, [target], pg_controldata=BIN / "pg_controldata",
                                            destination_assignments=assignments)
        inventory = read_header_catalog(engine)
        baseline = verify(inventory)
        report = {
            "initialization_seconds": round(initialization_seconds, 3),
            "baseline_bound": all(item.target_id == "fixture" for item in baseline.snapshot.partitions[0].relations),
            "real_control_binary": str(BIN / "pg_controldata"),
            "real_process_and_files": True,
            "synthetic_uuid": True,
        }
        # Observe the ordinary index left in pg_default when only the table moves.
        from psycopg2 import sql
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            with conn.connection.driver_connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE TABLESPACE qt_fixture_history LOCATION {}").format(sql.Literal(str(history))))
        with engine.connect() as conn:
            destination_oid = conn.scalar(text(
                "SELECT oid::bigint FROM pg_tablespace WHERE spcname='qt_fixture_history'"))
        destination_inventory = read_header_catalog(engine, destination_tablespace_oids=(destination_oid,))
        destination, = verify(destination_inventory, {"fixture": destination_oid}).destinations
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
                   {"fixture": destination_oid})
            report["changed_destination_refused"] = False
        except StorageMountError:
            report["changed_destination_refused"] = True

        def move_indexes(conn):
            names = conn.execute(text("""
                SELECT n.nspname,c.relname FROM pg_index i
                JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE i.indrelid='market.fact_versions_20260901'::regclass
            """)).all()
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
                    conn.execute(text("ALTER TABLE market.fact_versions_20260901 SET TABLESPACE qt_fixture_history"))
                    if phase == "whole_group":
                        move_indexes(conn)
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
            conn.execute(text("ALTER TABLE market.fact_versions_20260901 SET TABLESPACE qt_fixture_history"))
        split = read_header_catalog(engine)
        verify(split)
        report["table_only_move_has_split_tablespaces"] = len({item["tablespace_oid"] for item in split.physical_locations}) == 2
        with engine.begin() as conn:
            move_indexes(conn)
        moved = read_header_catalog(engine)
        verified = verify(moved)
        report["complete_group_tablespace"] = len({item["tablespace_oid"] for item in moved.physical_locations}) == 1
        report["tablespace_paths_verified"] = all(Path(item["path"]).is_relative_to(history) for item in verified.bindings)
        report["toast_colocated"] = moved.snapshot.partitions[0].toast_colocated
        with engine.connect() as conn:
            report["rows_preserved"] = conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions")).all() == before

        def refused(candidate):
            try:
                verify(candidate)
            except StorageMountError:
                return True
            return False
        report["stale_start_refused"] = refused(replace(moved, postmaster_started_at=moved.postmaster_started_at - timedelta(seconds=1)))
        metadata.write_text("E:ID_FS_UUID=uuid-wrong-device\n")
        report["wrong_uuid_refused"] = refused(moved)
        metadata.write_text("E:ID_FS_UUID=uuid-disposable-namespace\n")
        clone = root / "copied-cluster"
        (clone / "global").mkdir(parents=True)
        for relative in ("PG_VERSION", "postmaster.pid", "global/pg_control"):
            shutil.copyfile(data / relative, clone / relative)
        report["copied_identity_refused"] = refused(replace(moved, server_data_directory=str(clone)))
        return report
    finally:
        if engine is not None:
            engine.dispose()
        if (data / "postmaster.pid").exists():
            control("stop")


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
    if account is not None:
        os.chown(root, account.pw_uid, account.pw_gid)
    command = [*command_prefix, sys.executable, "-m", "tests.test_market_data.test_header_namespace_db", str(root)]
    # No inherited DSN, passwords or dotenv: only the private socket is used.
    source_root = Path(__file__).resolve().parents[2]
    environment = {"PATH": os.environ["PATH"], "PYTHONPATH": f"{source_root / 'src'}:{source_root}",
                   "QT_DISABLE_DOTENV": "1", "PYTHONUTF8": "1"}
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True,
                                timeout=300, cwd=source_root, env=environment)
        report = json.loads(result.stdout.strip().splitlines()[-1])
        print("namespace_fixture_report=" + json.dumps(report, sort_keys=True))
        return report
    except subprocess.CalledProcessError as exc:
        log = root / "postgres.log"
        server_tail = log.read_text(errors="replace")[-4000:] if log.exists() else ""
        raise RuntimeError("namespace_worker_failed:\n" + (exc.stdout or "")[-4000:]
                           + (exc.stderr or "")[-8000:] + server_tail) from exc
    finally:
        # This directory was created above; never clean an externally supplied PGDATA.
        resolved = root.resolve(strict=True)
        if resolved.parent != Path("/tmp") or not resolved.name.startswith("qt-header-ns-"):
            raise RuntimeError("namespace_fixture_cleanup_path_mismatch")
        if (resolved / "pgdata" / "postmaster.pid").exists():
            subprocess.run([*command_prefix, str(BIN / "pg_ctl"), "-D", str(resolved / "pgdata"),
                            "-w", "-t", "45", "-m", "fast", "stop"],
                           check=True, capture_output=True, text=True, timeout=60, env=environment)
        shutil.rmtree(resolved)


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


if __name__ == "__main__":
    try:
        print(json.dumps(_worker(Path(sys.argv[1])), sort_keys=True))
    except subprocess.TimeoutExpired as exc:
        for label, value in (("stdout", exc.stdout), ("stderr", exc.stderr)):
            if value:
                value = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
                print("namespace_timeout_" + label + "=" + value[-4000:], file=sys.stderr)
        raise

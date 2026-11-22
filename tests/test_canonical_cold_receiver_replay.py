from contextlib import contextmanager
import hashlib
import os
import socket
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import URL

from scripts.automation import canonical_cold_receiver_replay as replay
from scripts.automation.stage_encrypted_recovery_handoff import _identity


@pytest.fixture
def private_replay(tmp_path, monkeypatch):
    workspace = tmp_path / "work"
    pgdata = workspace / "pgdata"
    socket_root = workspace / "socket"
    endpoint = socket_root / ".s.PGSQL.55431"
    for path in (workspace, pgdata, socket_root, pgdata / "global", pgdata / "pg_wal", pgdata / "pg_tblspc"):
        path.mkdir(mode=0o700)
    (pgdata / "recovery.signal").touch(mode=0o600)
    control = pgdata / "global" / "pg_control"
    control.write_bytes(b"a" * 8192)
    control.chmod(0o600)
    scope = {"database_identity": "100/20", "timeline": 1, "target_lsn": "0/3000050", "replay_lsn": "0/3000090",
             "accepted_commit_seq": 2, "storage_layout": {"layout_version": "fixture", "certificate_sha256": "b" * 64}}
    archive = {"schema": "qt.paused_archive_suffix.v1", **scope}
    live = {"database_identity": "100/20", "role": "qt", "role_oid": 10, "role_unchanged": True,
            "timeline": 1, "target_timeline": "1", "paused": True, "archive_only": True, "isolated_socket": True,
            "exact_target": True, "restore_command": "/qt-local-wal %f %p", "pgdata": str(pgdata), "socket": str(socket_root),
            "target_lsn": scope["target_lsn"], "replay_lsn": scope["replay_lsn"], "read_only": "on",
            "isolation": "repeatable read", "version": 150006, "postmaster_started": "123.5"}
    stopped = """pg_control version number:          1300
Database cluster state:               shut down in recovery
Database system identifier:           100
Latest checkpoint location:          0/3000030
Latest checkpoint's REDO location:   0/3000020
Latest checkpoint's REDO WAL file:   000000010000000000000003
Latest checkpoint's TimeLineID:       1
Minimum recovery ending location:     0/3000090
Min recovery ending loc's timeline:   1
Bytes per WAL segment:                16777216
"""
    events = []

    class Connection:
        def __init__(self, database):
            self.database = database
            self.transaction = False

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execution_options(self, *, isolation_level):
            assert isolation_level == ("AUTOCOMMIT" if self.database == "postgres" else "REPEATABLE READ")
            return self

        @contextmanager
        def begin(self):
            self.transaction = True
            yield
            self.transaction = False

        def in_transaction(self):
            return self.transaction

        def exec_driver_sql(self, command):
            if command == "CHECKPOINT":
                assert self.database == "postgres" and not self.transaction
            else:
                assert command == "SET TRANSACTION READ ONLY" and self.transaction
            events.append(command)
            return SimpleNamespace(close=lambda: None)

        def execute(self, query, parameters=None):
            if query is replay._NATIVE_PAUSE:
                assert parameters == {"target": "qt"}
                return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: {"database": self.database, **live}))
            if "set_config" in str(query):
                return SimpleNamespace(close=lambda: None)
            assert "pg_tablespace_location" in str(query)
            return []

    def engine(database):
        return SimpleNamespace(
            url=URL.create("postgresql", username="qt", database=database, port=55431, query={"host": str(socket_root)}),
            connect=lambda: Connection(database),
        )

    @contextmanager
    def boot(path, *, socket, port, timeline, target_lsn, check):
        assert (path, socket, port, timeline, target_lsn) == (pgdata, socket_root, 55431, 1, scope["target_lsn"])
        check()
        events.append("boot")
        with socket_module.socket(socket_module.AF_UNIX) as server:
            server.bind(str(endpoint))
            endpoint.chmod(0o700)
            try:
                yield engine("postgres"), engine("qt")
            finally:
                endpoint.unlink()
                control.write_bytes(b"b" * 8192)
                events.append("fast stop")

    def verify_archive(conn, check):
        assert conn.transaction and check() > 0
        events.append("archive")
        return archive

    def read_control(path, check):
        assert path == pgdata and check() > 0 and events[-1] == "fast stop" and not endpoint.exists()
        events.append("pg_controldata")
        return stopped

    socket_module = socket
    monkeypatch.setattr(replay, "_snapshot_layout", lambda _: scope["storage_layout"])
    monkeypatch.setattr(replay.PostgresMarketDataRepository, "_current_commit_seq_with_session", lambda _: 2)
    options = {"pgdata": pgdata, "workspace": workspace, "socket": socket_root, "port": 55431,
               "expected_roots": [(path, _identity(path.lstat())) for path in (workspace, pgdata)],
               "expected_control_sha256": hashlib.sha256(control.read_bytes()).hexdigest(),
               "database_identity": scope["database_identity"], "database": "qt", "role": "qt", "role_oid": 10,
               "timeline": 1, "target_lsn": scope["target_lsn"], "replay_lsn": scope["replay_lsn"],
               "restore_command": live["restore_command"], "boot_paused": boot,
               "verify_archive": verify_archive, "read_stopped_control": read_control, "timeout_seconds": 10}
    return options, live, events, scope, archive, stopped


def test_private_receiver_checks_pause_and_fast_stop_with_optional_restartpoint(private_replay):
    options, _, events, scope, archive, stopped = private_replay
    assert replay.replay_private_receiver(**options, request_restartpoint=True) == {
        "paused_receipt": scope, "archive_receipt": archive, "stopped_control": stopped,
        "stopped_control_sha256": hashlib.sha256(b"b" * 8192).hexdigest(),
    }
    assert events == ["boot", "SET TRANSACTION READ ONLY", "archive", "CHECKPOINT", "fast stop", "pg_controldata"]


def test_private_receiver_rejects_wrong_pause_without_restartpoint(private_replay):
    options, live, events, *_ = private_replay
    live["paused"] = False
    with pytest.raises(RuntimeError, match="native_pause_changed"):
        replay.replay_private_receiver(**options, request_restartpoint=True)
    assert events == ["boot", "SET TRANSACTION READ ONLY", "fast stop"]


def test_private_receiver_rejects_unlisted_tablespace_before_start(private_replay):
    options, _, events, *_ = private_replay
    target = options["workspace"] / "unapproved-tablespace"
    target.mkdir(mode=0o700)
    os.symlink(target, options["pgdata"] / "pg_tblspc" / "999")
    with pytest.raises(ValueError, match="native_tablespace_changed"):
        replay.replay_private_receiver(**options)
    assert events == []


def test_private_receiver_rejects_changed_binary_control_after_fast_stop(private_replay):
    options, _, _, _, _, stopped = private_replay

    def swap_control(path, _check):
        (path / "global" / "pg_control").write_bytes(b"c" * 8192)
        return stopped

    with pytest.raises(RuntimeError, match="stopped_control_changed"):
        replay.replay_private_receiver(**(options | {"read_stopped_control": swap_control}))

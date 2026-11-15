"""Verify a private PostgreSQL replay through pause and clean fast shutdown."""

import hashlib
import os
from pathlib import Path
import re
import stat
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic

from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes
from portal.backend.service.storage.recovery_copies import _snapshot_layout
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root
from scripts.db.canonical_cold_replayed_checkpoint import _control_fields, _endpoint_scope, _lsn, _lsn_text


_NATIVE_PAUSE = text("""
    SELECT control.system_identifier::text || '/' || target.oid::text AS database_identity,
           current_database() AS database, session_user AS role, roles.oid::bigint AS role_oid,
           current_user = session_user AS role_unchanged, checkpoint.timeline_id::bigint AS timeline,
           extract(epoch FROM pg_postmaster_start_time())::text AS postmaster_started,
           current_setting('server_version_num')::int AS version,
           pg_is_in_recovery() AND pg_get_wal_replay_pause_state()='paused' AS paused,
           pg_last_wal_replay_lsn()::text AS replay_lsn,
           current_setting('recovery_target_lsn') AS target_lsn,
           current_setting('recovery_target_timeline') AS target_timeline,
           current_setting('recovery_target_action')='pause' AND current_setting('recovery_target_inclusive')='on'
             AND current_setting('recovery_target_name')='' AND current_setting('recovery_target')='' AS exact_target,
           current_setting('archive_mode')='off' AND current_setting('primary_conninfo')=''
             AND current_setting('primary_slot_name')='' AND current_setting('archive_cleanup_command')=''
             AND current_setting('recovery_end_command')='' AND NOT EXISTS(SELECT FROM pg_stat_wal_receiver) AS archive_only,
           current_setting('restore_command') AS restore_command,
           inet_server_addr() IS NULL AND inet_client_addr() IS NULL AND current_setting('listen_addresses')=''
             AND current_setting('fsync')='on' AS isolated_socket,
           current_setting('data_directory') AS pgdata, current_setting('unix_socket_directories') AS socket,
           current_setting('transaction_read_only') AS read_only, current_setting('transaction_isolation') AS isolation
    FROM pg_control_system() control CROSS JOIN pg_control_checkpoint() checkpoint
    CROSS JOIN pg_database target CROSS JOIN pg_roles roles
    WHERE target.datname=:target AND roles.rolname=session_user AND roles.rolcanlogin
""")


def _require_stopped(pgdata, endpoint):
    recovery = pgdata / "recovery.signal"
    if (not recovery.is_file() or recovery.is_symlink()
            or any(os.path.lexists(path) for path in (
                pgdata / "postmaster.pid", pgdata / "standby.signal", endpoint, endpoint.with_name(endpoint.name + ".lock"),
            ))):
        raise RuntimeError("canonical_receiver_replay_requires_stopped_recovery")


def _socket_identity(endpoint, device):
    state = endpoint.lstat()
    if (not stat.S_ISSOCK(state.st_mode) or state.st_uid != os.getuid() or state.st_dev != device
            or state.st_mode & 0o077):
        raise RuntimeError("canonical_receiver_replay_private_socket_changed")
    return _identity(state)


def _assert_data_paths(pgdata, device, spaces, roots):
    if any(_private_root(pgdata / name)[1][0] != device for name in ("global", "pg_tblspc", "pg_wal")):
        raise ValueError("canonical_receiver_replay_native_data_mount_changed")
    with os.scandir(pgdata) as entries:
        for index, entry in enumerate(entries):
            if index >= 128 or entry.is_symlink():
                raise ValueError("canonical_receiver_replay_native_data_path_changed")
    expected_links = {}
    for path, oid in spaces.values():
        target = Path(path)
        if type(oid) is not int or not 1 <= oid < 2**32 or target not in roots or str(oid) in expected_links:
            raise ValueError("canonical_receiver_replay_native_tablespace_changed")
        expected_links[str(oid)] = (path, roots[target])
    found = set()
    with os.scandir(pgdata / "pg_tblspc") as entries:
        for entry in entries:
            link = Path(entry.path)
            pinned = expected_links.get(entry.name)
            if (pinned is None or not link.is_symlink() or os.readlink(link) != pinned[0]
                    or _identity(link.stat()) != pinned[1]):
                raise ValueError("canonical_receiver_replay_native_tablespace_changed")
            found.add(entry.name)
    if found != expected_links.keys():
        raise ValueError("canonical_receiver_replay_native_tablespace_changed")


def _private_url(engine, *, database, role, socket, port):
    url = engine.url
    if (url.get_backend_name(), url.username, url.password, url.host, url.port, url.database, dict(url.query)) != (
        "postgresql", role, None, None, port, database, {"host": str(socket)},
    ):
        raise ValueError("canonical_receiver_replay_invalid_private_endpoint")


def _statement_timeout(conn, seconds, *, local):
    milliseconds = int(seconds * 1000)
    if milliseconds < 1:
        raise TimeoutError("canonical_receiver_replay_deadline")
    conn.execute(text("SELECT set_config('statement_timeout', :milliseconds, :local)"),
                 {"milliseconds": str(milliseconds), "local": local}).close()


def _native_pause(conn, *, database, expected, target_lsn, replay_lsn, check, repeatable):
    _statement_timeout(conn, min(5, check()), local=repeatable)
    state = conn.execute(_NATIVE_PAUSE, {"target": expected["database"]}).mappings().one()
    if (any(state[field] != value for field, value in expected.items() if field != "database")
            or state["database"] != database or not 150000 <= state["version"] < 160000
            or _lsn(state["target_lsn"]) != target_lsn or _lsn(state["replay_lsn"]) != replay_lsn
            or state["read_only"] != "on" or (repeatable and state["isolation"] != "repeatable read")):
        raise RuntimeError("canonical_receiver_replay_native_pause_changed")
    return state["postmaster_started"]


def _read_native_control(pgdata, workspace, binary, check):
    binary = _binary(binary)
    if binary.name != "pg_controldata":
        raise ValueError("canonical_receiver_replay_native_tool_invalid")
    with TemporaryDirectory(prefix=".qt-native-control-", dir=workspace) as temporary:
        env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": temporary, "TMPDIR": temporary}

        def run(argument):
            native = subprocess.run([str(binary), argument], stdin=subprocess.DEVNULL, capture_output=True,
                                    env=env, cwd=temporary, timeout=check())
            if native.returncode or native.stderr or len(native.stdout) > 65_536:
                raise RuntimeError("canonical_receiver_replay_native_control_failed")
            return native.stdout.decode("ascii")

        if not re.fullmatch(r"pg_controldata \(PostgreSQL\) 15(?:\.[0-9]+)+\s*", run("--version")):
            raise ValueError("canonical_receiver_replay_native_tool_version_changed")
        return run(str(pgdata))


def replay_private_receiver(*, pgdata, workspace, socket, port, expected_roots, expected_control_sha256,
                            database_identity, database, role, role_oid, timeline, target_lsn, replay_lsn,
                            restore_command, boot_paused, verify_archive, timeout_seconds,
                            pg_controldata=None, read_stopped_control=None, tablespaces=None, request_restartpoint=False):
    """Return paused SQL/archive receipts and the stopped copy's native control evidence.

    The caller holds the approved private mounts and retained WAL in an isolated
    runtime. The boot callback owns its native process and fast-stops it cleanly
    on context exit. An optional sidecar control reader must run trusted PG15
    pg_controldata once stopped, with C locale, checking exit status and stderr.
    Tablespace mappings use names with exact (private path, native OID) values.
    """
    if (not all(callable(callback) for callback in (boot_paused, verify_archive))
            or (pg_controldata is None) == (read_stopped_control is None)
            or (read_stopped_control is not None and not callable(read_stopped_control))
            or type(request_restartpoint) is not bool
            or not isinstance(database_identity, str) or re.fullmatch(r"[1-9][0-9]{0,19}/[1-9][0-9]{0,9}", database_identity) is None
            or any(not isinstance(value, str) or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value) is None
                   for value in (database, role))
            or not isinstance(restore_command, str) or not restore_command
            or any(char in restore_command for char in ("\x00", "\n", "\r"))
            or any(type(value) is not int or not 1 <= value <= limit for value, limit in (
                (role_oid, 2**32 - 1), (timeline, 2**32 - 1), (port, 65_535), (timeout_seconds, MAX_MIGRATION_SECONDS),
            ))):
        raise ValueError("canonical_receiver_replay_invalid_request")
    target_value, replay_value = _lsn(target_lsn), _lsn(replay_lsn)
    if not 0 < target_value < replay_value:
        raise ValueError("canonical_receiver_replay_invalid_request")
    pgdata, data_id = _private_root(pgdata)
    workspace, workspace_id = _private_root(workspace)
    socket, socket_id = _private_root(socket)
    roots = {Path(path): identity for path, identity in expected_roots}
    endpoint = socket / f".s.PGSQL.{port}"
    if (roots.get(pgdata) != data_id or roots.get(workspace) != workspace_id
            or workspace.is_relative_to(pgdata) or not socket.is_relative_to(workspace) or socket == workspace
            or (socket in roots and roots[socket] != socket_id)
            or any(socket.is_relative_to(path) or path.is_relative_to(socket) for path in roots if path not in (workspace, socket))
            or len(os.fsencode(endpoint)) >= 108):
        raise ValueError("canonical_receiver_replay_private_paths_changed")
    roots[socket] = socket_id
    spaces = {name: (str(path), oid) for name, (path, oid) in (tablespaces or {}).items()}
    control_file = pgdata / "global" / "pg_control"
    deadline = monotonic() + timeout_seconds

    def check():
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("canonical_receiver_replay_deadline")
        if any(_identity(path.lstat()) != identity for path, identity in roots.items()):
            raise RuntimeError("canonical_receiver_replay_private_mount_changed")
        return remaining

    def control_hash():
        raw = _private_bytes(control_file, limit=8_192)
        if len(raw) != 8_192:
            raise ValueError("canonical_receiver_replay_native_control_size_changed")
        return hashlib.sha256(raw).hexdigest()

    check()
    _require_stopped(pgdata, endpoint)
    _assert_data_paths(pgdata, data_id[0], spaces, roots)
    if control_hash() != expected_control_sha256:
        raise RuntimeError("canonical_receiver_replay_original_control_changed")
    expected = {"database_identity": database_identity, "database": database, "role": role, "role_oid": role_oid,
                "role_unchanged": True, "timeline": timeline, "target_timeline": str(timeline), "paused": True,
                "exact_target": True, "archive_only": True, "isolated_socket": True,
                "restore_command": restore_command, "pgdata": str(pgdata), "socket": str(socket)}
    with boot_paused(pgdata, socket=socket, port=port, timeline=timeline, target_lsn=_lsn_text(target_value),
                     check=check) as (control_engine, target_engine):
        _private_url(control_engine, database="postgres", role=role, socket=socket, port=port)
        _private_url(target_engine, database=database, role=role, socket=socket, port=port)
        pinned_socket = _socket_identity(endpoint, socket_id[0])

        def probe(conn, name, *, repeatable):
            if _socket_identity(endpoint, socket_id[0]) != pinned_socket:
                raise RuntimeError("canonical_receiver_replay_private_socket_changed")
            return _native_pause(conn, database=name, expected=expected, target_lsn=target_value,
                                 replay_lsn=replay_value, check=check, repeatable=repeatable)

        with target_engine.connect().execution_options(isolation_level="REPEATABLE READ") as target, target.begin():
            target.exec_driver_sql("SET TRANSACTION READ ONLY")
            started = probe(target, database, repeatable=True)
            observed_spaces = {name: (location, oid) for name, location, oid in target.execute(text("""
                SELECT spcname, pg_tablespace_location(oid), oid::bigint FROM pg_tablespace
                WHERE spcname NOT IN ('pg_default','pg_global')
            """))}
            if observed_spaces != spaces:
                raise RuntimeError("canonical_receiver_replay_native_tablespace_changed")
            paused = {"database_identity": database_identity, "timeline": timeline, "target_lsn": _lsn_text(target_value),
                      "replay_lsn": _lsn_text(replay_value),
                      "accepted_commit_seq": PostgresMarketDataRepository._current_commit_seq_with_session(target),
                      "storage_layout": _snapshot_layout(target)}
            archive = verify_archive(target, check)
            if (not isinstance(archive, dict) or archive.get("schema") != "qt.paused_archive_suffix.v1"
                    or _endpoint_scope(archive) != _endpoint_scope(paused) or not target.in_transaction()
                    or probe(target, database, repeatable=True) != started):
                raise RuntimeError("canonical_receiver_replay_archive_endpoint_changed")
        if request_restartpoint:
            with control_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as control:
                if probe(control, "postgres", repeatable=False) != started:
                    raise RuntimeError("canonical_receiver_replay_postmaster_changed")
                _statement_timeout(control, check(), local=False)
                control.exec_driver_sql("CHECKPOINT").close()
                if probe(control, "postgres", repeatable=False) != started:
                    raise RuntimeError("canonical_receiver_replay_postmaster_changed")
    check()
    _require_stopped(pgdata, endpoint)
    stopped_hash = control_hash()
    raw = (read_stopped_control(pgdata, check) if read_stopped_control is not None
           else _read_native_control(pgdata, workspace, pg_controldata, check))
    native = _control_fields(raw)
    check()
    _require_stopped(pgdata, endpoint)
    _assert_data_paths(pgdata, data_id[0], spaces, roots)
    if (stopped_hash != control_hash() or native["pg_control version number"] != "1300"
            or native["Database cluster state"] != "shut down in recovery"
            or native["Database system identifier"] != database_identity.split("/")[0]
            or native["Latest checkpoint's TimeLineID"] != str(timeline)
            or native["Min recovery ending loc's timeline"] != str(timeline)
            or not 0 < _lsn(native["Minimum recovery ending location"]) <= replay_value):
        raise RuntimeError("canonical_receiver_replay_stopped_control_changed")
    return {"paused_receipt": paused, "archive_receipt": archive, "stopped_control": raw,
            "stopped_control_sha256": stopped_hash}

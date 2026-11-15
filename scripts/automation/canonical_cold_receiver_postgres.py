"""Run the exact private PostgreSQL receiver through its two native boot phases."""

from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import re
import signal
import socket as network
import stat
import subprocess
from tempfile import TemporaryDirectory
from threading import Lock
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes
from scripts.automation.canonical_cold_receiver_replay import _require_stopped, _socket_identity
from scripts.automation.stage_encrypted_recovery_handoff import _identity, _private_root, _stamp
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.db.canonical_cold_replayed_checkpoint import _lsn, _lsn_text


_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}")
_COMMAND_PATH = re.compile(r"/[a-zA-Z0-9_./-]+")
_TARGET = re.compile(rb"(?im)^[ \t]*(recovery_target(?:_[a-z_]+)?)[ \t]*=([^\r\n]*)$")
_VERSION = b"postgres (PostgreSQL) 15.6\n"


def _network_identity():
    if os.geteuid() == 0 or {name for _, name in network.if_nameindex()} != {"lo"}:
        raise ValueError("canonical_receiver_postgres_requires_isolated_nonroot_network")
    return _identity(Path("/proc/self/ns/net").stat())


def _regular(path):
    info = path.lstat()
    if (path.resolve(strict=True) != path or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid not in (0, os.getuid()) or info.st_mode & 0o022):
        raise ValueError("canonical_receiver_postgres_unsafe_configuration")
    return info


@contextmanager
def _native_executable(path, expected_sha256, env, workspace, check):
    path = _binary(path)
    before = _regular(path)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as binary:
        fd = binary.fileno()
        digest = hashlib.sha256()
        while data := binary.read(1024**2):
            check()
            digest.update(data)
        if (digest.hexdigest() != expected_sha256 or _stamp(os.fstat(fd)) != _stamp(before)
                or _stamp(path.lstat()) != _stamp(before)):
            raise RuntimeError("canonical_receiver_postgres_executable_changed")
        executable = f"/proc/self/fd/{fd}"
        version = subprocess.run([str(path), "--version"], executable=executable, pass_fds=(fd,),
                                 stdin=subprocess.DEVNULL, capture_output=True, cwd=workspace, env=env,
                                 timeout=min(5, check()))
        if version.returncode or version.stderr or version.stdout != _VERSION:
            raise RuntimeError("canonical_receiver_postgres_native_version_changed")
        yield str(path), executable, fd


def _target_configuration(pgdata, *, paused, timeline):
    raw = _private_bytes(pgdata / "postgresql.auto.conf", limit=65536)
    settings = _TARGET.findall(raw)
    values = {key: value.strip() for key, value in settings}
    name = values.get(b"recovery_target_name", b"")
    native_timeline = values.get(b"recovery_target_timeline", b"")
    action = values.get(b"recovery_target_action")
    if (len(settings) != 3 or set(values) != {b"recovery_target_name", b"recovery_target_timeline", b"recovery_target_action"}
            or re.fullmatch(rb"'[1-9][0-9]{0,9}'", native_timeline) is None
            or int(native_timeline[1:-1]) > 2**32 - 1
            or (paused and (re.fullmatch(rb"'qt_prepared_[0-9a-f]{32}'", name) is None or action != b"'shutdown'"
                            or int(native_timeline[1:-1]) != timeline))
            or (not paused and (re.fullmatch(rb"'qt_original_[0-9a-f]{32}'", name) is None or action != b"'promote'"))):
        raise RuntimeError("canonical_receiver_postgres_recovery_configuration_changed")
    return name[1:-1].decode("ascii"), native_timeline[1:-1].decode("ascii")


def _stop(process, seconds):
    if process.poll() is not None:
        raise RuntimeError(f"canonical_receiver_postgres_process_exited: code={process.returncode}")
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=seconds)
    except subprocess.TimeoutExpired:
        process.send_signal(signal.SIGQUIT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        raise TimeoutError("canonical_receiver_postgres_fast_stop_deadline") from None
    if process.returncode:
        raise RuntimeError(f"canonical_receiver_postgres_process_failed: exit={process.returncode}")


class ReceiverPostgres:
    """Owned PostgreSQL 15.6 processes in a network-disabled receiver namespace."""

    def __init__(self, *, postgres, postgres_sha256, pgdata, workspace, socket, port, database, role,
                 pgbackrest, settings, expected_roots, shutdown_seconds=30):
        if (not _sha256(postgres_sha256) or type(port) is not int or not 1 <= port <= 65535
                or type(shutdown_seconds) is not int or not 1 <= shutdown_seconds <= 60
                or any(not isinstance(value, str) or not _IDENTIFIER.fullmatch(value) for value in (database, role))
                or database == "postgres"):
            raise ValueError("canonical_receiver_postgres_invalid_scope")
        self._network = _network_identity()
        self.postgres = _binary(postgres)
        self.postgres_sha256 = postgres_sha256
        self.pgdata, data_id = _private_root(pgdata)
        self.workspace, workspace_id = _private_root(workspace)
        self.socket, socket_id = _private_root(socket)
        self._roots = {Path(path): identity for path, identity in dict(expected_roots).items()}
        settings = Path(settings)
        pgbackrest = _binary(pgbackrest)
        if (self.postgres.name != "postgres" or pgbackrest.name != "pgbackrest"
                or self._roots.get(self.pgdata) != data_id or self._roots.get(self.workspace) != workspace_id
                or self._roots.get(self.socket, socket_id) != socket_id
                or not self.pgdata.is_relative_to(self.workspace) or self.pgdata == self.workspace
                or not self.socket.is_relative_to(self.workspace) or self.socket == self.workspace
                or any(self.socket.is_relative_to(path) or path.is_relative_to(self.socket)
                       for path in self._roots if path not in (self.workspace, self.socket))
                or settings.parent != self.workspace or settings.name != "pgbackrest.conf"
                or not all(_COMMAND_PATH.fullmatch(str(value)) for value in (pgbackrest, settings))
                or len(os.fsencode(self.socket / f".s.PGSQL.{port}")) >= 108):
            raise ValueError("canonical_receiver_postgres_private_paths_changed")
        self._roots[self.socket] = socket_id
        self.settings = settings
        self.restore_command = f'{pgbackrest} --config={settings} --stanza=qt archive-get %f "%p"'
        self.port = port
        self.database = database
        self.role = role
        self._shutdown_seconds = shutdown_seconds
        self._process_owner = Lock()

    def _check(self, check):
        remaining = check()
        if remaining <= 0:
            raise TimeoutError("canonical_receiver_postgres_deadline")
        if (_network_identity() != self._network or any(
                path.resolve(strict=True) != path or _identity(path.lstat()) != identity
                for path, identity in self._roots.items())):
            raise RuntimeError("canonical_receiver_postgres_private_boundary_changed")
        return remaining

    def _wait(self, process, engine, endpoint, *, paused, name, timeline, target_lsn, check):
        while True:
            check()
            if process.poll() is not None:
                raise RuntimeError(f"canonical_receiver_postgres_startup_exit: code={process.returncode}")
            try:
                socket_identity = _socket_identity(endpoint, self._roots[self.socket][0])
                pidfile = _private_bytes(self.pgdata / "postmaster.pid", limit=4096)
            except FileNotFoundError:
                sleep(0.1)
                continue
            lines = pidfile.splitlines()
            if len(lines) < 5:
                sleep(0.1)
                continue
            if (lines[0] != str(process.pid).encode() or lines[1] != os.fsencode(self.pgdata)
                    or lines[3] != str(self.port).encode() or lines[4] != os.fsencode(self.socket)):
                raise RuntimeError("canonical_receiver_postgres_postmaster_changed")
            try:
                with engine.begin() as conn:
                    state = conn.exec_driver_sql("""
                        SELECT pg_is_in_recovery(), CASE WHEN pg_is_in_recovery() THEN pg_get_wal_replay_pause_state() END,
                               current_setting('recovery_target_name'), current_setting('recovery_target_lsn'),
                               current_setting('recovery_target_timeline'), current_setting('recovery_target_action'),
                               current_setting('restore_command'), current_setting('data_directory'),
                               inet_server_addr() IS NULL AND inet_client_addr() IS NULL
                                 AND current_setting('listen_addresses')='' AND current_setting('archive_mode')='off'
                                 AND current_setting('unix_socket_directories') IS NOT NULL,
                               current_setting('unix_socket_directories'), current_setting('server_version_num')
                    """).one()
            except OperationalError as failure:
                code = getattr(failure.orig, "sqlstate", None) or getattr(failure.orig, "pgcode", None)
                if code not in (None, "57P03", "08006"):
                    raise RuntimeError(f"canonical_receiver_postgres_private_connection_failed: code={code}") from None
                sleep(0.1)
                continue
            target = ("", target_lsn, timeline, "pause") if paused else (name, "", timeline, "promote")
            if (tuple(state[2:6]) != target or tuple(state[6:]) != (
                    self.restore_command, str(self.pgdata), True, str(self.socket), "150006")
                    or _socket_identity(endpoint, self._roots[self.socket][0]) != socket_identity):
                raise RuntimeError("canonical_receiver_postgres_native_scope_changed")
            if paused and not state[0]:
                raise RuntimeError("canonical_receiver_postgres_unexpected_promotion")
            if (paused and state[1] == "paused") or (not paused and not state[0]):
                return
            sleep(0.1)

    def boot_paused(self, pgdata, *, socket, port, timeline, target_lsn, check):
        target = _lsn(target_lsn)
        if (Path(socket) != self.socket or port != self.port or type(timeline) is not int
                or not 1 <= timeline < 2**32 or target <= 0):
            raise ValueError("canonical_receiver_postgres_invalid_pause_scope")
        return self._boot(pgdata, check, timeline=timeline, target_lsn=_lsn_text(target))

    def boot_final(self, pgdata, check):
        return self._boot(pgdata, check)

    @contextmanager
    def _boot(self, pgdata, check, *, timeline=None, target_lsn=None):
        paused = target_lsn is not None
        if Path(pgdata) != self.pgdata or not self._process_owner.acquire(blocking=False):
            raise RuntimeError("canonical_receiver_postgres_private_instance_changed")

        def checked():
            return self._check(check)

        try:
            checked()
            endpoint = self.socket / f".s.PGSQL.{self.port}"
            _require_stopped(self.pgdata, endpoint)
            recovery_signal_id = _identity(_regular(self.pgdata / "recovery.signal"))
            _regular(self.settings)
            _regular(self.pgdata / "postgresql.conf")
            name, native_timeline = _target_configuration(self.pgdata, paused=paused, timeline=timeline)
            with TemporaryDirectory(prefix=".qt-receiver-postgres-", dir=self.workspace) as temporary:
                work = Path(temporary)
                hba = work / "pg_hba.conf"
                hba.write_text(f'local "postgres" "{self.role}" trust\nlocal "{self.database}" "{self.role}" trust\n',
                               encoding="ascii")
                ident = work / "pg_ident.conf"
                ident.touch()
                env = {"PATH": os.defpath, "LC_ALL": "C", "HOME": str(work), "TMPDIR": str(work)}
                fixed = {"data_directory": self.pgdata, "config_file": self.pgdata / "postgresql.conf", "hba_file": hba,
                         "ident_file": ident, "external_pid_file": "", "listen_addresses": "",
                         "unix_socket_directories": self.socket, "unix_socket_permissions": "0700", "unix_socket_group": "",
                         "port": self.port,
                         "fsync": "on", "ssl": "off", "hot_standby": "on", "archive_mode": "off", "archive_command": "",
                         "archive_library": "", "primary_conninfo": "", "primary_slot_name": "", "archive_cleanup_command": "",
                         "recovery_end_command": "", "restore_command": self.restore_command, "log_destination": "stderr",
                         "logging_collector": "off", "log_statement": "none", "log_min_messages": "warning",
                         "dynamic_library_path": "$libdir", "shared_preload_libraries": "timescaledb,pg_stat_statements",
                         "session_preload_libraries": "", "local_preload_libraries": "", "timescaledb.telemetry_level": "off",
                         "default_transaction_read_only": "on" if paused else "off"}
                if paused:
                    fixed.update(recovery_target="", recovery_target_name="", recovery_target_time="", recovery_target_xid="",
                                 recovery_target_lsn=target_lsn, recovery_target_timeline=timeline,
                                 recovery_target_action="pause", recovery_target_inclusive="on")
                arguments = [item for key, value in fixed.items() for item in ("-c", f"{key}={value}")]
                url = URL.create("postgresql+psycopg2", username=self.role, port=self.port, database="postgres",
                                 query={"host": str(self.socket)})
                connection = {"connect_timeout": 2, "hostaddr": "", "sslmode": "disable", "gssencmode": "disable"}
                probe = create_engine(url, poolclass=NullPool,
                                      connect_args=connection | {"options": "-c statement_timeout=3000"})
                control = create_engine(url, poolclass=NullPool, connect_args=connection)
                target = create_engine(url.set(database=self.database), poolclass=NullPool, connect_args=connection)
                try:
                    with _native_executable(self.postgres, self.postgres_sha256, env, work, checked) as (binary, executable, fd):
                        with (work / "postgres.log").open("wb") as log:
                            process = subprocess.Popen([binary, "-D", str(self.pgdata), *arguments], executable=executable,
                                pass_fds=(fd,), stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                cwd=work, env=env, start_new_session=True)
                            failure = None
                            try:
                                self._wait(process, probe, endpoint, paused=paused, name=name, timeline=native_timeline,
                                           target_lsn=target_lsn, check=checked)
                                yield control, target
                            except BaseException as error:
                                failure = error
                                raise
                            finally:
                                try:
                                    _stop(process, self._shutdown_seconds)
                                except (OSError, RuntimeError, subprocess.TimeoutExpired) as stop_error:
                                    if failure is None:
                                        raise
                                    failure.add_note(str(stop_error))
                finally:
                    for engine in (probe, control, target):
                        engine.dispose()
            checked()
            stopped = (self.pgdata / "postmaster.pid", self.pgdata / "standby.signal", endpoint, endpoint.with_name(endpoint.name + ".lock"))
            recovery = self.pgdata / "recovery.signal"
            if (any(os.path.lexists(path) for path in stopped)
                    or (paused and _identity(_regular(recovery)) != recovery_signal_id)
                    or (not paused and os.path.lexists(recovery))):
                raise RuntimeError("canonical_receiver_postgres_stop_state_changed")
        finally:
            self._process_owner.release()

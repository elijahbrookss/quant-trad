"""Shared fixed-host boundary for storage migration operations.

Owns bounded Docker I/O, host identity observations, the deployment flock and
private durable receipt I/O. Phase modules own receipt schemas, transitions and
caller deadlines. Observations and persisted bytes alone grant no switch,
resumption or recovery authority. There is no command-line entrypoint here.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import select
import stat
import subprocess
import tempfile
import time
import threading

HOLD = "storage-handoff.json"


STOP = ("backend", "initialize", "market-data-collector", "frontend", "frontend-v2", "grafana", "pgadmin")


PASSIVE = ("tsdb", "loki", "alloy", "docker-events", "docker-stats")


FIELDS = {
    "id": ".Id", "image": ".Image",
    "project": 'index .Config.Labels "com.docker.compose.project"',
    "service": 'index .Config.Labels "com.docker.compose.service"',
    "oneoff": 'index .Config.Labels "com.docker.compose.oneoff"',
    "restart": ".HostConfig.RestartPolicy.Name",
    "running": ".State.Running", "restarting": ".State.Restarting",
    "paused": ".State.Paused", "pid": ".State.Pid", "status": ".State.Status",
    "exit_code": ".State.ExitCode", "oom": ".State.OOMKilled",
}


INSPECT = "{" + ",".join(json.dumps(k) + ":{{json (" + v + ")}}" for k, v in FIELDS.items()) + "}"


_DOCKER_DEADLINE = ContextVar("storage_pause_docker_deadline", default=None)


@contextmanager
def docker_deadline(deadline):
    """Shorten every nested Docker call under one caller-owned host deadline."""
    previous = _DOCKER_DEADLINE.get()
    token = _DOCKER_DEADLINE.set(min(deadline, previous) if previous is not None else deadline)
    try:
        yield
    finally:
        _DOCKER_DEADLINE.reset(token)


def docker(*args: str, timeout: int = 30, env=None, input=None) -> str:
    deadline = _DOCKER_DEADLINE.get()
    if deadline is not None:
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise RuntimeError("storage_pause_host_deadline_expired")
        timeout = min(timeout, remaining)
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, env=env, input=input)
    if deadline is not None and time.monotonic() >= deadline:
        raise RuntimeError("storage_pause_host_deadline_expired")
    # Do not expose Docker diagnostics or inspected configuration: these may
    # include credentials. Private database settings are hashed by the caller.
    if result.returncode:
        raise RuntimeError(f"storage_pause_docker_failed: operation={args[0]} exit={result.returncode}")
    if len(result.stdout) > 524288:
        raise RuntimeError("storage_pause_inventory_too_large")
    return result.stdout


def inventory(project: str, *, database_preparing: bool = False, operator_id: str | None = None,
              activating: bool = False, removing_database_id: str | None = None,
              runtime_maintenance: bool = False, removing_client_id: str | None = None) -> dict[str, dict]:
    if removing_database_id is not None and (not database_preparing or activating
            or not re.fullmatch(r"[0-9a-f]{64}", removing_database_id)):
        raise ValueError("storage_pause_invalid_removing_database")
    if (runtime_maintenance and not activating or removing_client_id is not None and
            (not activating or not runtime_maintenance or not re.fullmatch(r"[0-9a-f]{64}",removing_client_id))):
        raise ValueError("storage_runtime_invalid_client_transition")
    allowed_services=STOP+PASSIVE+(("storage-maintenance",) if runtime_maintenance else ())
    ids = set()
    for selector in (f"label=com.docker.compose.project={project}", f"network={project}_quanttrad"):
        ids.update(docker("ps", "--all", "--quiet", "--no-trunc", "--filter", selector).split())
    if not ids or len(ids) > 64 or any(not re.fullmatch(r"[0-9a-f]{64}", x) for x in ids):
        raise RuntimeError("storage_pause_invalid_container_inventory")
    if operator_id is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", operator_id):
            raise ValueError("storage_pause_invalid_operator_identity")
        ids.discard(operator_id)
    rows = [json.loads(line) for line in docker("inspect", "--format", INSPECT, *sorted(ids)).splitlines()]
    if {x["id"] for x in rows} != ids or len(rows) != len(ids):
        raise RuntimeError("storage_pause_incomplete_inspection")
    result = {}
    for row in rows:
        service = row["service"]
        if (row["project"] != project or service not in allowed_services
                or row["oneoff"] != "False" or service in result):
            raise RuntimeError("storage_pause_unexpected_client: resolve active bots, one-off or unrecognized containers first")
        if row["restart"] not in ("no", "unless-stopped"):
            raise RuntimeError(f"storage_pause_unsafe_restart_policy: service={service}")
        if (activating and row["id"]==removing_client_id and service in
                ("backend","initialize","market-data-collector") and row["status"]=="removing"
                and row["running"] is False and row["pid"]==0 and row["exit_code"] in (0,143)
                and not any(row[k] for k in ("paused","restarting","oom"))):
            result[service]=row
            continue
        if activating and service != "tsdb":
            if row["paused"] or row["status"] not in ("created", "running", "restarting", "exited"):
                raise RuntimeError(f"storage_runtime_unexpected_container_state: service={service}")
            result[service] = row
            continue
        if row["paused"] or row["restarting"] or row["oom"]:
            raise RuntimeError(f"storage_pause_unstable_container: service={service}")
        if (service == "tsdb" and row["id"] == removing_database_id
                and row["status"] == "removing" and row["running"] is False
                and row["pid"] == 0 and row["exit_code"] == 0):
            # Only the exact cleanly stopped database whose removal the caller
            # has already journaled. Other services and uncertain states refuse.
            result[service] = row
            continue
        if row["running"]:
            if row["status"] != "running" or row["pid"] <= 0:
                raise RuntimeError(f"storage_pause_unstable_container: service={service}")
        elif row["status"] not in ("exited", "created") or row["pid"] != 0 or row["exit_code"] not in (0, 143):
            raise RuntimeError(f"storage_pause_unclean_stop: service={service}")
        result[service] = row
    required = ("tsdb",) if activating else (STOP if database_preparing else STOP + ("tsdb",))
    if not set(required) <= result.keys() or (not database_preparing and not result["tsdb"]["running"]):
        raise RuntimeError("storage_pause_required_service_missing_or_database_stopped")
    return result


def source_clients_serving(rows):
    """The initializer may have completed; never rerun it to resume collection."""
    return all(row["running"] or (name == "initialize" and row["status"] == "exited"
                                  and row["exit_code"] == 0)
               for name in STOP for row in (rows[name],))


def identities(rows: dict[str, dict]) -> dict[str, dict]:
    return {service: {key: row[key] for key in ("id", "image", "restart")}
            for service, row in rows.items()}


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def save_receipt(path: Path, receipt: dict, *, initial: bool) -> None:
    data = (json.dumps(receipt, sort_keys=True) + "\n").encode()
    if initial:
        # An incomplete write is intentionally still a blocking hold.
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    else:
        descriptor, name = tempfile.mkstemp(prefix=".storage-handoff-", dir=path.parent)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, path)
        finally:
            if os.path.exists(name):
                os.unlink(name)
    sync_directory(path.parent)


def load_receipt(path: Path, *, max_bytes: int = 65536) -> dict:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > max_bytes:
            raise RuntimeError("storage_pause_invalid_hold_file")
        data = stream.read(max_bytes+1)
        if len(data)>max_bytes:
            raise RuntimeError("storage_pause_invalid_hold_file")
    def fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError("storage_pause_duplicate_hold_field")
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=fields)


TCP_INSPECT = ['CMD-SHELL', 'pg_isready -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"']


SOCKET_INSPECT = ['CMD-SHELL', 'pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"']


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def database_details(container: str) -> dict:
    # Resolved environment is inspected privately and hashed, never persisted
    # into the hold or exposed in diagnostics.
    expression = '{"id":{{json .Id}},"config":{{json .Config}},"host":{{json .HostConfig}},"mounts":{{json .Mounts}},"networks":{{json .NetworkSettings.Networks}},"image":{{json .Image}}}'
    return json.loads(docker("inspect", "--format", expression, container))


def preserved_client_observations(containers, network_names):
    """One fresh, bounded observation for clients sharing a restoration check.

    Private config and state stay in memory. Callers must discard this result at
    the end of the check; it is never a receipt or authority for a later action.
    """
    containers = tuple(containers)
    network_names = tuple(sorted(set(network_names)))
    if not 1 <= len(containers) <= 16 or len(set(containers)) != len(containers) or not 1 <= len(network_names) <= 32:
        raise ValueError("storage_preserved_observation_bound_invalid")
    expression = '{"id":{{json .Id}},"config":{{json .Config}},"host":{{json .HostConfig}},"mounts":{{json .Mounts}},"networks":{{json .NetworkSettings.Networks}},"image":{{json .Image}},"state":{{json .State}}}'
    values = [json.loads(line) for line in docker("inspect", "--format", expression, *containers).splitlines()]
    details = {value["id"]:value for value in values}
    if len(values) != len(containers) or set(details) != set(containers):
        raise RuntimeError("storage_preserved_observation_container_changed")
    networks = [json.loads(line) for line in docker("network", "inspect", "--format",
        '{"name":{{json .Name}},"id":{{json .Id}}}', *network_names).splitlines()]
    identities = {value["name"]:value["id"] for value in networks}
    if len(networks) != len(network_names) or set(identities) != set(network_names):
        raise RuntimeError("storage_preserved_observation_network_changed")
    return details, identities


def database_contract(details: dict, *, tcp_upgrade: bool = False) -> str:
    config = json.loads(json.dumps(details["config"]))
    config.pop("Image", None)  # resolved image ID is verified separately
    config.pop("Volumes", None)  # exact mounted devices are verified separately
    config["Env"] = sorted(config.get("Env") or [])
    labels = config.get("Labels") or {}
    # Compose records how it was invoked in these labels. Recreating an
    # already-resolved recipe can omit environment_file without changing Env;
    # effective environment values remain fully bound below and in preflight.
    for name in ("config-hash", "project.config_files", "project.working_dir",
                 "project.environment_file", "image", "version", "replace"):
        labels.pop("com.docker.compose."+name, None)
    config["Labels"] = labels
    if tcp_upgrade:
        probe = config.get("Healthcheck", {}).get("Test")
        if probe not in (TCP_INSPECT, SOCKET_INSPECT):
            raise RuntimeError("storage_database_unsupported_readiness_probe")
        config["Healthcheck"]["Test"] = TCP_INSPECT
    host = {key: value for key, value in details["host"].items() if key not in ("Binds", "Mounts")}
    # Docker changes this default between null and false across startup. A real
    # true value remains distinct and must not be silently altered.
    if host.get("OomKillDisable") is None:
        host["OomKillDisable"] = False
    return digest({"config": config, "host": host})


def database_networks(details: dict) -> dict:
    return {name: {"network_id": value["NetworkID"],
                   "aliases": sorted(alias for alias in value.get("Aliases") or []
                                     if alias not in (details["id"], details["id"][:12])),
                   "ipam": value.get("IPAMConfig")}
            for name, value in details["networks"].items()}


def same_database_networks(details: dict, expected: dict, *, network_ids=None) -> bool:
    actual = database_networks(details)
    if set(actual) != set(expected):
        return False
    for name, original in expected.items():
        current = actual[name]
        # A created/stopped endpoint can lack its runtime network ID. The
        # existing external network itself must still be exactly the saved one.
        if (current["network_id"] not in ("", original["network_id"])
                or current["aliases"] != original["aliases"] or current["ipam"] != original["ipam"]
                or (network_ids.get(name) if network_ids is not None else
                    docker("network", "inspect", "--format", "{{.Id}}", name).strip()) != original["network_id"]):
            return False
    return True


def database_query(container: str, sql: str, *, read_only_seconds: int | None = None) -> str:
    if read_only_seconds is not None and (type(read_only_seconds) is not int or not 1 <= read_only_seconds <= 30):
        raise ValueError("storage_database_read_bound_invalid")
    options = ([] if read_only_seconds is None else ["--env",
        "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout="+str(read_only_seconds*1000)])
    return docker("exec", *options, container, "sh", "-ec",
        'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" '
        '-v ON_ERROR_STOP=1 -Atc "$1"', "storage-preparation", sql).strip()


def maintenance_query(container: str, sql: str) -> str:
    """Fixed cluster-local control path, using the database's existing identity.

    The caller owns SQL meaning and an absolute Docker deadline. Target is passed
    privately by psql variable from the existing recipe, never a second DSN.
    """
    if current_docker_deadline() is None:
        raise RuntimeError("storage_maintenance_deadline_required")
    return docker("exec", "-i", container, "sh", "-ec",
        'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d postgres '
        '-v ON_ERROR_STOP=1 -v target="$POSTGRES_DB" -qAtf -',
        input="SET statement_timeout='5s';\n"+sql).strip()


def cluster_identifier(container: str, *, maintenance: bool = False) -> str:
    value = (maintenance_query if maintenance else database_query)(container, "SELECT system_identifier FROM pg_control_system()")
    if not re.fullmatch(r"[0-9]{1,20}", value):
        raise ValueError("storage_database_cluster_identity_unavailable")
    return value


def source_storage(container: str) -> None:
    source = json.loads(database_query(container,
        "SELECT json_build_object('directory',current_setting('data_directory'),"
        "'tablespaces',(SELECT count(*) FROM pg_tablespace WHERE pg_tablespace_location(oid)<>''))"))
    if source["tablespaces"] != 0:
        raise RuntimeError("storage_database_source_tablespaces_require_review")
    actual = docker("exec", container, "readlink", "-f", source["directory"]).strip()
    if not Path(actual).is_absolute() or not Path(actual).is_relative_to(Path("/var/lib/postgresql/data")):
        raise RuntimeError("storage_database_data_directory_outside_retained_volume")


@contextmanager
def deployment_lock(state_root: Path):
    if not state_root.is_absolute() or state_root == Path("/"):
        raise ValueError("storage_pause_invalid_state_root")
    info = state_root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise RuntimeError("storage_pause_unsafe_state_directory")
    descriptor = os.open(state_root / "deployment.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("storage_pause_deployment_lock_busy") from exc
        yield


def current_docker_deadline():
    """Observe the enclosing command ceiling without changing it."""
    return _DOCKER_DEADLINE.get()


def supervised_source_action(arguments, *, deadline, check, input=None):
    """Bound one journaled Docker request under live source-fence supervision.

    Caller owns the exact arguments, intent and original deadline. Local CLI
    kill/reap never cancels a daemon action; any failed check leaves its outcome
    unresolved. No stdout/stderr or environment contents are returned.
    """
    if input is not None and (not isinstance(input, str) or len(input.encode()) > 4096):
        raise ValueError("storage_online_source_action_input_bound_exceeded")
    check()
    if time.monotonic() >= deadline:
        raise RuntimeError("storage_online_resume_deadline_expired")
    process = subprocess.Popen(["docker", *arguments],
        stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if input is not None:
            process.stdin.write(input.encode())
            process.stdin.close()
        while True:
            check()
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise RuntimeError("storage_online_resume_deadline_expired")
            result = process.poll()
            if result is not None:
                if result != 0:
                    raise RuntimeError("storage_online_resume_start_failed")
                return
            time.sleep(min(.1, remaining))
    finally:
        # Reap only our local CLI, including cancellation. This grants no more
        # host-operation time and makes no claim about daemon request outcome.
        if process.poll() is None:
            process.kill()
        process.wait(timeout=1)  # Cleanup only, never another daemon action.


class OnlineWorkerChannel:
    """One bounded, non-replayable host conversation with the retained worker.

    Phase owners supply deadlines and durable intent before calling exchange.
    Any framing, timeout, identity or sequence failure permanently poisons this
    channel. It never reconnects, replays, reconciles outcomes or grants action
    authority. The existing launcher still owns process retirement.
    """
    def __init__(self, process, *, deadline, command_seconds=40):
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or deadline <= time.monotonic()
                or type(command_seconds) not in (int, float)
                or not math.isfinite(command_seconds) or not 0 < command_seconds <= 3600):
            raise ValueError("storage_online_channel_budget_invalid")
        self._input = process.stdin.fileno()
        self._output = process.stdout.fileno()
        self._deadline = deadline
        self._command_seconds = command_seconds
        self._sequence = 0
        self._controller_id = None
        self._final_deadline = None
        self._failed = False
        self._lock = threading.Lock()
        self.last_reply = None
        try:
            greeting = self._read(min(deadline, time.monotonic()+command_seconds))
            self._validate(greeting, operation=None)
            if greeting.get("state") != "background" or greeting.get("bound_final_deadline") is not None:
                raise RuntimeError("storage_online_channel_greeting_invalid")
            self._controller_id = greeting["controller_id"]
            self.greeting = greeting
            self.last_reply = greeting
        except BaseException:
            self._failed = True
            raise

    @property
    def sequence(self):
        return self._sequence

    @staticmethod
    def _unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("storage_online_channel_duplicate_field")
            result[key] = value
        return result

    @staticmethod
    def _finite(value):
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("storage_online_channel_nonfinite_number")
        return value

    @staticmethod
    def _wait(fd, event, deadline):
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError("storage_online_channel_deadline_expired")
        poller = select.poll()  # Descriptor counts may exceed FD_SETSIZE.
        poller.register(fd, event)
        if not poller.poll(math.ceil(remaining*1000)) or time.monotonic() >= deadline:
            raise TimeoutError("storage_online_channel_deadline_expired")

    def _read(self, deadline):
        data = bytearray()
        previous = os.get_blocking(self._output)
        os.set_blocking(self._output, False)
        try:
            while True:
                self._wait(self._output, select.POLLIN, deadline)
                try:
                    part = os.read(self._output, 16385-len(data))
                except BlockingIOError:
                    continue
                if not part:
                    raise RuntimeError("storage_online_channel_truncated_reply" if data else "storage_online_channel_eof")
                data.extend(part)
                if len(data) > 16384:
                    raise RuntimeError("storage_online_channel_reply_bound_exceeded")
                if b"\n" in data:
                    if data.count(b"\n") != 1 or not data.endswith(b"\n"):
                        raise RuntimeError("storage_online_channel_pipelined_reply")
                    try:
                        return json.loads(data, object_pairs_hook=self._unique,
                            parse_float=self._finite, parse_constant=self._finite)
                    except (ValueError, UnicodeError, RecursionError):
                        raise RuntimeError("storage_online_channel_reply_invalid") from None
        finally:
            os.set_blocking(self._output, previous)

    def _validate(self, reply, *, operation):
        if (not isinstance(reply, dict)
                or reply.get("schema_version") != "qt.storage_online_controller.v1"
                or not isinstance(reply.get("controller_id"), str)
                or not re.fullmatch(r"[0-9a-f]{32}", reply["controller_id"])
                or self._controller_id is not None and reply["controller_id"] != self._controller_id
                or type(reply.get("last_sequence")) is not int
                or reply["last_sequence"] != self._sequence
                or reply.get("operation") != operation
                or any(reply.get(key) is not False for key in (
                    "migration_ready", "final_switch_authorized", "collection_resume_authorized"))):
            raise RuntimeError("storage_online_channel_reply_binding_changed")
        bound = reply.get("bound_final_deadline")
        if bound is not None:
            if (type(bound) not in (int, float) or not math.isfinite(bound)
                    or bound > self._deadline
                    or self._final_deadline is not None and bound != self._final_deadline):
                raise RuntimeError("storage_online_channel_final_deadline_changed")
            self._final_deadline = bound
        elif self._final_deadline is not None:
            raise RuntimeError("storage_online_channel_final_deadline_changed")

    def exchange(self, operation, *, response_deadline=None, **parameters):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("storage_online_channel_command_inflight")
        try:
            if self._failed:
                raise RuntimeError("storage_online_channel_unresolved")
            try:
                if (not isinstance(operation, str) or not re.fullmatch(r"[a-z_]{1,64}", operation)
                        or set(parameters) & {"controller_id", "sequence", "operation"}):
                    raise ValueError("storage_online_channel_command_invalid")
                deadline = min(self._deadline, time.monotonic()+self._command_seconds)
                for value in (response_deadline, parameters.get("deadline"), self._final_deadline):
                    if value is not None:
                        if type(value) not in (int, float) or not math.isfinite(value):
                            raise ValueError("storage_online_channel_budget_invalid")
                        deadline = min(deadline, value)
                self._sequence += 1  # Never reuse a possibly dispatched sequence.
                if self._sequence > 2**53:
                    raise RuntimeError("storage_online_channel_sequence_exhausted")
                data = json.dumps(dict(controller_id=self._controller_id,
                    sequence=self._sequence, operation=operation, **parameters),
                    separators=(",", ":"), allow_nan=False).encode()+b"\n"
                if len(data) > 4096:
                    raise ValueError("storage_online_channel_command_bound_exceeded")
                previous = os.get_blocking(self._input)
                os.set_blocking(self._input, False)
                try:
                    while data:
                        self._wait(self._input, select.POLLOUT, deadline)
                        try:
                            count = os.write(self._input, data)
                        except BlockingIOError:
                            continue
                        if count <= 0:
                            raise RuntimeError("storage_online_channel_write_failed")
                        data = data[count:]
                finally:
                    os.set_blocking(self._input, previous)
                reply = self._read(deadline)
                self._validate(reply, operation=operation)
                self.last_reply = reply
                return reply
            except BaseException as exc:
                self._failed = True
                logging.getLogger(__name__).error(
                    "storage_online_channel_failed | operation=%s sequence=%s error_type=%s",
                    operation, self._sequence, type(exc).__name__)
                raise
        finally:
            self._lock.release()

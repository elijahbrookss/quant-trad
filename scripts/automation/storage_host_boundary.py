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
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time

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


def database_contract(details: dict, *, tcp_upgrade: bool = False) -> str:
    config = json.loads(json.dumps(details["config"]))
    config.pop("Image", None)  # resolved image ID is verified separately
    config.pop("Volumes", None)  # exact mounted devices are verified separately
    config["Env"] = sorted(config.get("Env") or [])
    labels = config.get("Labels") or {}
    for name in ("config-hash", "project.config_files", "project.working_dir", "image", "version", "replace"):
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


def same_database_networks(details: dict, expected: dict) -> bool:
    actual = database_networks(details)
    if set(actual) != set(expected):
        return False
    for name, original in expected.items():
        current = actual[name]
        # A created/stopped endpoint can lack its runtime network ID. The
        # existing external network itself must still be exactly the saved one.
        if (current["network_id"] not in ("", original["network_id"])
                or current["aliases"] != original["aliases"] or current["ipam"] != original["ipam"]
                or docker("network", "inspect", "--format", "{{.Id}}", name).strip() != original["network_id"]):
            return False
    return True


def database_query(container: str, sql: str) -> str:
    return docker("exec", container, "sh", "-ec",
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

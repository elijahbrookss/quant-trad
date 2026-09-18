"""Internal fixed-server pause boundary; not a standalone cutover command.

The deployment lock spans the caller's cutover. The durable hold always remains
on exit, including success: only a future verified activation/recovery procedure
may retire it. A stopped container is not proof of spool or database durability.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

HOLD = "storage-handoff.json"
STOP = ("backend", "initialize", "market-data-collector", "frontend", "frontend-v2", "grafana", "pgadmin")
PASSIVE = ("tsdb", "loki", "alloy", "docker-events", "docker-stats")
SCHEMA = "qt.storage_handoff_pause.v1"
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


def _docker(*args: str, timeout: int = 30) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    # Do not expose Docker diagnostics or full inspection/configuration: these
    # may include resolved credentials. Only selected nonsecret fields are read.
    if result.returncode:
        raise RuntimeError(f"storage_pause_docker_failed: operation={args[0]} exit={result.returncode}")
    if len(result.stdout) > 524288:
        raise RuntimeError("storage_pause_inventory_too_large")
    return result.stdout


def _inventory(project: str) -> dict[str, dict]:
    ids = set()
    for selector in (f"label=com.docker.compose.project={project}", f"network={project}_quanttrad"):
        ids.update(_docker("ps", "--all", "--quiet", "--no-trunc", "--filter", selector).split())
    if not ids or len(ids) > 64 or any(not re.fullmatch(r"[0-9a-f]{64}", x) for x in ids):
        raise RuntimeError("storage_pause_invalid_container_inventory")
    rows = [json.loads(line) for line in _docker("inspect", "--format", INSPECT, *sorted(ids)).splitlines()]
    if {x["id"] for x in rows} != ids or len(rows) != len(ids):
        raise RuntimeError("storage_pause_incomplete_inspection")
    result = {}
    for row in rows:
        service = row["service"]
        if (row["project"] != project or service not in STOP + PASSIVE
                or row["oneoff"] != "False" or service in result):
            raise RuntimeError("storage_pause_unexpected_client: resolve active bots, one-off or unrecognized containers first")
        if row["restart"] not in ("no", "unless-stopped"):
            raise RuntimeError(f"storage_pause_unsafe_restart_policy: service={service}")
        if row["paused"] or row["restarting"] or row["oom"]:
            raise RuntimeError(f"storage_pause_unstable_container: service={service}")
        if row["running"]:
            if row["status"] != "running" or row["pid"] <= 0:
                raise RuntimeError(f"storage_pause_unstable_container: service={service}")
        elif row["status"] not in ("exited", "created") or row["pid"] != 0 or row["exit_code"] not in (0, 143):
            raise RuntimeError(f"storage_pause_unclean_stop: service={service}")
        result[service] = row
    if not set(STOP + ("tsdb",)) <= result.keys() or not result["tsdb"]["running"]:
        raise RuntimeError("storage_pause_required_service_missing_or_database_stopped")
    return result


def _identities(rows: dict[str, dict]) -> dict[str, dict]:
    return {service: {key: row[key] for key in ("id", "image", "restart")}
            for service, row in rows.items()}


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _save(path: Path, receipt: dict, *, initial: bool) -> None:
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
    _sync_directory(path.parent)


def _load(path: Path) -> dict:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 65536:
            raise RuntimeError("storage_pause_invalid_hold_file")
        data = stream.read(65537)
    def fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError("storage_pause_duplicate_hold_field")
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=fields)


@contextmanager
def paused_storage_clients(state_root: Path, *, project: str, source_revision: str):
    """Pause fixed clients under the existing host lock; retain the hold on exit.

    Re-entry requires the same recorded release and exact container identities.
    Unexpected peers (including bot runtimes) are refused, never stopped. This
    protects the current controller only; direct Docker/SQL or older controllers
    remain privileged bypasses and must be excluded by the cutover procedure.
    """
    state_root = Path(state_root)
    if (not state_root.is_absolute() or state_root == Path("/")
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project)
            or not re.fullmatch(r"[0-9a-f]{40}", source_revision)):
        raise ValueError("storage_pause_invalid_binding")
    # Existing operator state only. No implicit installation or environment setup.
    info = state_root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise RuntimeError("storage_pause_unsafe_state_directory")
    descriptor = os.open(state_root / "deployment.lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("storage_pause_deployment_lock_busy") from exc
        for name in ("promotion.env", "alert-preview.env"):
            if os.path.lexists(state_root / name):
                raise RuntimeError("storage_pause_unfinished_server_operation")
        release = (state_root / "release.env").read_text()
        if re.findall(r"^current_revision=(.*)$", release, flags=re.MULTILINE) != [source_revision]:
            raise RuntimeError("storage_pause_recorded_release_mismatch")
        rows = _inventory(project)
        identities = _identities(rows)
        path = state_root / HOLD
        if os.path.lexists(path):
            receipt = _load(path)
            if (set(receipt) != {"schema_version", "project", "source_revision", "containers", "phase"}
                    or receipt["schema_version"] != SCHEMA or receipt["project"] != project
                    or receipt["source_revision"] != source_revision or receipt["containers"] != identities
                    or receipt["phase"] not in ("pausing", "paused")):
                raise RuntimeError("storage_pause_hold_binding_mismatch")
        else:
            receipt = dict(schema_version=SCHEMA, project=project, source_revision=source_revision,
                           containers=identities, phase="pausing")
            _save(path, receipt, initial=True)
        print("event=storage_handoff_pause_started", file=sys.stderr, flush=True)
        for service in STOP:
            rows = _inventory(project)
            if _identities(rows) != identities:
                raise RuntimeError("storage_pause_container_changed")
            if rows[service]["running"]:
                grace = 300 if service == "market-data-collector" else 60
                _docker("stop", "--time", str(grace), rows[service]["id"], timeout=grace + 30)
        rows = _inventory(project)
        if _identities(rows) != identities or any(rows[x]["running"] for x in STOP):
            raise RuntimeError("storage_pause_clients_not_stopped")
        receipt = {**receipt, "phase": "paused"}
        _save(path, receipt, initial=False)
        print("event=storage_handoff_clients_stopped resume_authorized=false", file=sys.stderr, flush=True)
        yield receipt
        # No finally-start, old-image recovery, hold removal or success activation.

"""Internal fixed-server pause boundary; not a standalone cutover command.

The deployment lock spans the caller's cutover. The durable hold always remains
on exit, including success: only a future verified activation/recovery procedure
may retire it. A stopped container is not proof of spool or database durability.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time

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


def _docker(*args: str, timeout: int = 30, env=None, input=None) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, env=env, input=input)
    # Do not expose Docker diagnostics or inspected configuration: these may
    # include credentials. Private database settings are hashed by the caller.
    if result.returncode:
        raise RuntimeError(f"storage_pause_docker_failed: operation={args[0]} exit={result.returncode}")
    if len(result.stdout) > 524288:
        raise RuntimeError("storage_pause_inventory_too_large")
    return result.stdout


def _inventory(project: str, *, database_preparing: bool = False, operator_id: str | None = None, activating: bool = False) -> dict[str, dict]:
    ids = set()
    for selector in (f"label=com.docker.compose.project={project}", f"network={project}_quanttrad"):
        ids.update(_docker("ps", "--all", "--quiet", "--no-trunc", "--filter", selector).split())
    if not ids or len(ids) > 64 or any(not re.fullmatch(r"[0-9a-f]{64}", x) for x in ids):
        raise RuntimeError("storage_pause_invalid_container_inventory")
    if operator_id is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", operator_id):
            raise ValueError("storage_pause_invalid_operator_identity")
        ids.discard(operator_id)
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
        if activating and service != "tsdb":
            if row["paused"] or row["status"] not in ("created", "running", "restarting", "exited"):
                raise RuntimeError(f"storage_runtime_unexpected_container_state: service={service}")
            result[service] = row
            continue
        if row["paused"] or row["restarting"] or row["oom"]:
            raise RuntimeError(f"storage_pause_unstable_container: service={service}")
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


def _load(path: Path, *, max_bytes: int = 65536) -> dict:
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


DATABASE_RECIPE = "storage-database.compose.json"
_DATABASE_FIELDS = {"recipe_sha256", "history_root", "history_uuid", "original_id", "image",
                    "source_contract", "target_contract", "source_mount", "networks",
                    "cluster_identifier", "source_stopped", "replacement_id", "deadline"}
_TCP_PROBE = ['CMD-SHELL', 'pg_isready -h 127.0.0.1 -U "$${POSTGRES_USER}" -d "$${POSTGRES_DB}"']
# Docker inspection sees the container shell variables after Compose escaping.
_TCP_INSPECT = ['CMD-SHELL', 'pg_isready -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"']
_SOCKET_INSPECT = ['CMD-SHELL', 'pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"']


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _database_details(container: str) -> dict:
    # Resolved environment is inspected privately and hashed, never persisted
    # into the hold or exposed in diagnostics.
    expression = '{"id":{{json .Id}},"config":{{json .Config}},"host":{{json .HostConfig}},"mounts":{{json .Mounts}},"networks":{{json .NetworkSettings.Networks}},"image":{{json .Image}}}'
    return json.loads(_docker("inspect", "--format", expression, container))


def _database_contract(details: dict, *, tcp_upgrade: bool = False) -> str:
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
        if probe not in (_TCP_INSPECT, _SOCKET_INSPECT):
            raise RuntimeError("storage_database_unsupported_readiness_probe")
        config["Healthcheck"]["Test"] = _TCP_INSPECT
    host = {key: value for key, value in details["host"].items() if key not in ("Binds", "Mounts")}
    # Docker changes this default between null and false across startup. A real
    # true value remains distinct and must not be silently altered.
    if host.get("OomKillDisable") is None:
        host["OomKillDisable"] = False
    return _digest({"config": config, "host": host})


def _database_networks(details: dict) -> dict:
    return {name: {"network_id": value["NetworkID"],
                   "aliases": sorted(alias for alias in value.get("Aliases") or []
                                     if alias not in (details["id"], details["id"][:12])),
                   "ipam": value.get("IPAMConfig")}
            for name, value in details["networks"].items()}


def _same_database_networks(details: dict, expected: dict) -> bool:
    actual = _database_networks(details)
    if set(actual) != set(expected):
        return False
    for name, original in expected.items():
        current = actual[name]
        # A created/stopped endpoint can lack its runtime network ID. The
        # existing external network itself must still be exactly the saved one.
        if (current["network_id"] not in ("", original["network_id"])
                or current["aliases"] != original["aliases"] or current["ipam"] != original["ipam"]
                or _docker("network", "inspect", "--format", "{{.Id}}", name).strip() != original["network_id"]):
            return False
    return True


def _database_query(container: str, sql: str) -> str:
    return _docker("exec", container, "sh", "-ec",
        'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" '
        '-v ON_ERROR_STOP=1 -Atc "$1"', "storage-preparation", sql).strip()


def _cluster_identifier(container: str) -> str:
    value = _database_query(container, "SELECT system_identifier FROM pg_control_system()")
    if not re.fullmatch(r"[0-9]{1,20}", value):
        raise ValueError("storage_database_cluster_identity_unavailable")
    return value


def _source_storage(container: str) -> None:
    source = json.loads(_database_query(container,
        "SELECT json_build_object('directory',current_setting('data_directory'),"
        "'tablespaces',(SELECT count(*) FROM pg_tablespace WHERE pg_tablespace_location(oid)<>''))"))
    if source["tablespaces"] != 0:
        raise RuntimeError("storage_database_source_tablespaces_require_review")
    actual = _docker("exec", container, "readlink", "-f", source["directory"]).strip()
    if not Path(actual).is_absolute() or not Path(actual).is_relative_to(Path("/var/lib/postgresql/data")):
        raise RuntimeError("storage_database_data_directory_outside_retained_volume")


def _history_filesystem(root: str, uuid: str) -> None:
    from src.core.storage_mounts import inspect_filesystem
    if not re.fullmatch(r"[A-Za-z0-9-]{4,128}", uuid) or Path(root) == Path("/"):
        raise ValueError("storage_database_invalid_history_binding")
    evidence = inspect_filesystem(Path(root), expected_uuid=uuid,
        udev_root=Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data")))
    if evidence.path != root:
        raise RuntimeError("storage_database_history_path_must_be_canonical")


def _existing_network_recipe(networks: dict, project: str) -> bool:
    if not isinstance(networks,dict) or set(networks)!={"quanttrad"} or not isinstance(networks["quanttrad"],dict):
        return False
    definition=dict(networks["quanttrad"])
    # Compose emits this empty default when rendering an external network.
    if definition.get("ipam")=={}:
        definition.pop("ipam")
    return definition=={"name":project+"_quanttrad","external":True}


def _database_recovery_mounts(service, volumes, history_root):
    """Admit only the fixed private key bind and shared PostgreSQL socket."""
    entries = service.get("volumes", [])
    mounts = {entry["target"]:entry for entry in entries}
    keys = mounts.get("/run/quanttrad/recovery")
    socket = mounts.get("/var/run/postgresql")
    if keys is None and socket is None:
        return {}
    if keys is None or socket is None:
        raise RuntimeError("storage_database_recovery_mount_pair_required")
    root = Path(keys.get("source", ""))
    if (keys != dict(type="bind",source=str(root),target="/run/quanttrad/recovery",
                     read_only=True,bind=dict(create_host_path=False))
            or not root.is_absolute() or root.resolve(strict=True) != root
            or root.is_relative_to(Path(history_root).resolve(strict=True))):
        raise RuntimeError("storage_database_recovery_keys_mount_invalid")
    info = root.stat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077
            or info.st_uid not in (0,70,os.getuid())):
        raise RuntimeError("storage_database_recovery_keys_not_private")
    if (socket.get("type") != "volume" or socket.get("source") != "storage-recovery-socket"
            or socket.get("read_only",False) or socket.get("volume")
            or set(socket)-{"type","source","target","read_only","volume"}):
        raise RuntimeError("storage_database_recovery_socket_mount_invalid")
    volume = volumes.get("storage-recovery-socket",{})
    if (set(volume)!={"name","external"} or volume["external"] is not True
            or not isinstance(volume["name"],str)
            or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}",volume["name"])):
        raise RuntimeError("storage_database_recovery_socket_volume_invalid")
    return {"/run/quanttrad/recovery":keys, "/var/run/postgresql":socket}


def _database_recipe(state_root: Path, project: str) -> tuple[dict, str]:
    model = _load(state_root / DATABASE_RECIPE)
    if (set(model) != {"name", "services", "networks", "volumes"} or model["name"] != project
            or set(model["services"]) != {"tsdb"}
            or not _existing_network_recipe(model["networks"],project)):
        raise RuntimeError("storage_database_invalid_fixed_recipe")
    service = model["services"]["tsdb"]
    allowed = {"image", "command", "hostname", "init", "restart", "shm_size", "healthcheck",
               "environment", "labels", "volumes", "networks", "logging", "pull_policy", "user"}
    if (set(service)-allowed or service.get("pull_policy") != "never"
            or service.get("healthcheck", {}).get("test") != _TCP_PROBE
            or set(service.get("networks", {})) != {"quanttrad"}
            or set(service["networks"]["quanttrad"])-{"aliases"}):
        raise RuntimeError("storage_database_unsafe_fixed_recipe")
    mounts = service.get("volumes", [])
    by_target = {value["target"]: value for value in mounts}
    recovery = _database_recovery_mounts(service,model["volumes"],by_target.get("/qt-history",{}).get("source","/"))
    if len(mounts) != len(by_target) or set(by_target) != {"/var/lib/postgresql/data", "/qt-history"}|set(recovery):
        raise RuntimeError("storage_database_invalid_mount_recipe")
    history = by_target["/qt-history"]
    if history != {"type": "bind", "source": history.get("source"), "target": "/qt-history",
                   "bind": {"create_host_path": False}}:
        raise RuntimeError("storage_database_history_requires_existing_writable_bind")
    pgdata = by_target["/var/lib/postgresql/data"]
    if (pgdata.get("type") != "volume" or pgdata.get("source") != "postgres-data"
            or pgdata.get("read_only", False) or set(pgdata)-{"type", "source", "target", "volume", "read_only"}
            or pgdata.get("volume", {})):
        raise RuntimeError("storage_database_pgdata_recipe_changed")
    return model, history["source"]


def _begin_database_preparation(state_root: Path, receipt: dict, rows: dict, history_uuid: str) -> dict:
    model, history_root = _database_recipe(state_root, receipt["project"])
    _history_filesystem(history_root, history_uuid)
    row = rows["tsdb"]
    details = _database_details(row["id"])
    if len(details["mounts"]) != 1:
        raise RuntimeError("storage_database_source_mounts_unexpected")
    source_mount = details["mounts"][0]
    if (source_mount.get("Type") != "volume" or source_mount.get("Destination") != "/var/lib/postgresql/data"
            or not source_mount.get("RW") or set(details["networks"]) != {receipt["project"]+"_quanttrad"}
            or model["volumes"].get("postgres-data") != {"name": source_mount["Name"], "external": True}
            or set(model["volumes"])-{"postgres-data","storage-recovery-socket"}):
        raise RuntimeError("storage_database_source_binding_mismatch")
    service = model["services"]["tsdb"]
    recovery = _database_recovery_mounts(service,model["volumes"],history_root)
    if recovery:
        # The socket volume must be explicitly provisioned before pausing.
        _docker("volume","inspect",model["volumes"]["storage-recovery-socket"]["name"])
    elif set(model["volumes"])!={"postgres-data"}:
        raise RuntimeError("storage_database_unexpected_volume")
    image = json.loads(_docker("image", "inspect", "--format", '{{json .}}', service["image"]))
    image_config = image["Config"]
    proposed_env = dict(value.split("=", 1) for value in image_config.get("Env") or [])
    proposed_env.update(service.get("environment", {}))
    if (image["Id"] != row["image"] or details["image"] != row["image"]
            or proposed_env != dict(value.split("=", 1) for value in details["config"].get("Env") or [])
            or service.get("command", image_config["Cmd"]) != details["config"]["Cmd"]
            or image_config.get("Entrypoint") != details["config"].get("Entrypoint")
            or service.get("hostname") != details["config"]["Hostname"]
            or service.get("restart") != row["restart"]):
        raise RuntimeError("storage_database_image_or_settings_changed")
    _source_storage(row["id"])
    preparation = dict(recipe_sha256=_digest(model), history_root=history_root, history_uuid=history_uuid,
        original_id=row["id"], image=row["image"], source_contract=_database_contract(details),
        target_contract=_database_contract(details, tcp_upgrade=True), source_mount=source_mount,
        networks=_database_networks(details), cluster_identifier=_cluster_identifier(row["id"]),
        source_stopped=False, replacement_id=None, deadline=time.time()+600)
    return {**receipt, "phase": "preparing_database", "database_preparation": preparation}


def _prepare_database(state_root: Path, receipt: dict, *, operator_id=None) -> dict:
    preparation = receipt["database_preparation"]
    project = receipt["project"]
    path = state_root / HOLD
    def check():
        model, root = _database_recipe(state_root, project)
        if _digest(model) != preparation["recipe_sha256"] or root != preparation["history_root"]:
            raise RuntimeError("storage_database_preparation_recipe_changed")
        _history_filesystem(root, preparation["history_uuid"])
        rows = _inventory(project, database_preparing=True, operator_id=operator_id)
        if ({key: value for key, value in _identities(rows).items() if key != "tsdb"}
                != {key: value for key, value in receipt["containers"].items() if key != "tsdb"}
                or any(rows[name]["running"] for name in STOP)):
            raise RuntimeError("storage_database_preparation_clients_changed")
        return rows
    def mutate(*args, timeout):
        check()
        remaining = int(preparation["deadline"]-time.time())
        if remaining < 1:
            raise RuntimeError("storage_database_preparation_deadline_expired")
        return _docker(*args, timeout=min(timeout, remaining))
    rows = check()
    row = rows.get("tsdb")
    if row and row["id"] == preparation["original_id"]:
        details = _database_details(row["id"])
        if (preparation["replacement_id"] or details["image"] != preparation["image"]
                or _database_contract(details) != preparation["source_contract"]
                or details["mounts"] != [preparation["source_mount"]]
                or not _same_database_networks(details, preparation["networks"])):
            raise RuntimeError("storage_database_original_changed")
        if row["running"]:
            mutate("stop", "--time", "120", row["id"], timeout=150)
        rows = check()
        row = rows["tsdb"]
        if row["id"] != preparation["original_id"] or row["running"] or row["exit_code"] != 0:
            raise RuntimeError("storage_database_source_not_cleanly_stopped")
        preparation["source_stopped"] = True
        _save(path, receipt, initial=False)
        # Remove only the verified stopped container, never its named volume.
        # Explicit sequencing avoids a half-finished Compose replacement leaving
        # both old and new tsdb containers behind after process death.
        mutate("rm", row["id"], timeout=60)
        mutate("compose", "--project-name", project, "--file", str(state_root / DATABASE_RECIPE),
               "create", "--no-build", "--pull", "never", "tsdb", timeout=120)
    elif row is None:
        if not preparation["source_stopped"] or preparation["replacement_id"]:
            raise RuntimeError("storage_database_unexpected_missing_container")
        mutate("compose", "--project-name", project, "--file", str(state_root / DATABASE_RECIPE),
               "create", "--no-build", "--pull", "never", "tsdb", timeout=120)
    rows = check()
    row = rows["tsdb"]
    details = _database_details(row["id"])
    mounts = {value["Destination"]: value for value in details["mounts"]}
    history = mounts.get("/qt-history", {})
    model,_ = _database_recipe(state_root,project)
    recovery = _database_recovery_mounts(model["services"]["tsdb"],model["volumes"],preparation["history_root"])
    extra_ok = True
    if recovery:
        keys = mounts.get("/run/quanttrad/recovery",{})
        socket = mounts.get("/var/run/postgresql",{})
        extra_ok = (keys.get("Type")=="bind" and keys.get("Source")==recovery["/run/quanttrad/recovery"]["source"]
            and keys.get("RW") is False and keys.get("Propagation")=="rprivate"
            and socket.get("Type")=="volume" and socket.get("RW") is True
            and socket.get("Name")==model["volumes"]["storage-recovery-socket"]["name"])
    admitted = {
        "recovery_mounts": extra_ok,
        "source_stop": preparation["source_stopped"],
        "replacement_id": row["id"] != preparation["original_id"] and preparation["replacement_id"] in (None, row["id"]),
        "image": details["image"] == preparation["image"],
        "settings": _database_contract(details) == preparation["target_contract"],
        "network": _same_database_networks(details, preparation["networks"]),
        "mount_count": len(details["mounts"]) == 2+len(recovery),
        "pgdata": mounts.get("/var/lib/postgresql/data") == preparation["source_mount"],
        "history": history.get("Type") == "bind" and history.get("Source") == preparation["history_root"]
                   and history.get("RW") and history.get("Propagation") == "rprivate",
    }
    failed = [name for name, valid in admitted.items() if not valid]
    if failed:
        raise RuntimeError("storage_database_replacement_not_admitted: checks="+",".join(failed))
    if preparation["replacement_id"] is None:
        preparation["replacement_id"] = row["id"]
        _save(path, receipt, initial=False)
    if not row["running"]:
        if row["status"] not in ("created", "exited") or row["exit_code"] != 0:
            raise RuntimeError("storage_database_replacement_unclean")
        mutate("start", row["id"], timeout=60)
    # TCP SQL proves the final database, not the socket-only bootstrap server.
    # Wait boundedly for a started container; a failed inspection leaves the hold.
    waiting_reported = False
    while True:
        check()
        try:
            identifier = _cluster_identifier(row["id"])
            break
        except RuntimeError:
            if not waiting_reported:
                print("event=storage_database_readiness_waiting hold_retained=true", file=sys.stderr, flush=True)
                waiting_reported = True
            remaining = preparation["deadline"]-time.time()
            if remaining <= 0:
                raise RuntimeError("storage_database_preparation_readiness_expired") from None
            time.sleep(min(1, remaining))
    if identifier != preparation["cluster_identifier"]:
        raise RuntimeError("storage_database_cluster_identity_changed")
    rows = check()
    if rows.get("tsdb", {}).get("id") != row["id"] or not rows["tsdb"]["running"]:
        raise RuntimeError("storage_database_replacement_changed_after_start")
    receipt = {**receipt, "phase": "database_prepared", "containers": _identities(rows)}
    _save(path, receipt, initial=False)
    print("event=storage_handoff_database_prepared resume_authorized=false", file=sys.stderr, flush=True)
    return receipt



@contextmanager
def _deployment_lock(state_root: Path):
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


@contextmanager
def paused_storage_clients(state_root: Path, *, project: str, source_revision: str,
                           prepare_database: bool = False, history_uuid: str = "", _operator_id=None):
    """Pause fixed clients and optionally prepare only the PostgreSQL HDD mount.

    The fixed private recipe must exist in the state directory. Initial and
    interrupted preparation retain the same lock/hold; no application resumes.
    """
    state_root = Path(state_root)
    if (not state_root.is_absolute() or state_root == Path("/")
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project)
            or not re.fullmatch(r"[0-9a-f]{40}", source_revision)
            or not isinstance(prepare_database, bool) or not isinstance(history_uuid, str)
            or bool(history_uuid) != prepare_database
            or (prepare_database and not re.fullmatch(r"[A-Za-z0-9-]{4,128}", history_uuid))):
        raise ValueError("storage_pause_invalid_binding")
    with _deployment_lock(state_root):
        for name in ("promotion.env", "alert-preview.env"):
            if os.path.lexists(state_root / name):
                raise RuntimeError("storage_pause_unfinished_server_operation")
        release = (state_root / "release.env").read_text()
        if re.findall(r"^current_revision=(.*)$", release, flags=re.MULTILINE) != [source_revision]:
            raise RuntimeError("storage_pause_recorded_release_mismatch")
        path = state_root / HOLD
        receipt = _load(path) if os.path.lexists(path) else None
        preparing = bool(receipt and receipt.get("phase") == "preparing_database")
        rows = _inventory(project, database_preparing=preparing, operator_id=_operator_id)
        identities = _identities(rows)
        if receipt:
            has_database = "database_preparation" in receipt
            expected_fields = {"schema_version", "project", "source_revision", "containers", "phase"}
            if has_database:
                expected_fields.add("database_preparation")
            if (set(receipt) != expected_fields or receipt["schema_version"] != SCHEMA
                    or receipt["project"] != project or receipt["source_revision"] != source_revision
                    or receipt["phase"] not in ("pausing", "paused", "preparing_database", "database_prepared")
                    or has_database != (receipt["phase"] in ("preparing_database", "database_prepared"))
                    or (not preparing and receipt["containers"] != identities)):
                raise RuntimeError("storage_pause_hold_binding_mismatch")
            if has_database:
                preparation = receipt["database_preparation"]
                if (not prepare_database or not isinstance(preparation, dict) or set(preparation) != _DATABASE_FIELDS
                        or preparation["history_uuid"] != history_uuid
                        or type(preparation["source_stopped"]) is not bool
                        or type(preparation["deadline"]) not in (int, float)
                        or not 0 < preparation["deadline"] < 1e12
                        or any(not isinstance(preparation[key], str) or not re.fullmatch(r"[0-9a-f]{64}", preparation[key])
                               for key in ("original_id", "recipe_sha256", "source_contract", "target_contract"))
                        or (preparation["replacement_id"] is not None
                            and (not isinstance(preparation["replacement_id"], str)
                                 or not re.fullmatch(r"[0-9a-f]{64}", preparation["replacement_id"])))):
                    raise RuntimeError("storage_pause_database_binding_mismatch")
                if preparing:
                    receipt = _prepare_database(state_root, receipt, operator_id=_operator_id)
                else:
                    # Re-enter through all physical/configuration/cluster checks.
                    receipt = _prepare_database(state_root, {**receipt, "phase": "preparing_database"}, operator_id=_operator_id)
                yield receipt
                return
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
        if prepare_database:
            receipt = _begin_database_preparation(state_root, receipt, rows, history_uuid)
            _save(path, receipt, initial=False)  # intent precedes every DB mutation
            receipt = _prepare_database(state_root, receipt, operator_id=_operator_id)
        print("event=storage_handoff_clients_stopped resume_authorized=false", file=sys.stderr, flush=True)
        yield receipt


_OPERATOR_STATE = "storage-operator-state.json"
_OPERATOR_REQUEST = "storage-operator-request.json"
_OPERATOR_CAPS = ["CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_SETGID", "CAP_SETUID"]
_OPERATOR_COMMAND = ["-c", "from scripts.automation.storage_host_prepare import legacy_working_operator_main; "
    "raise SystemExit(legacy_working_operator_main())"]


def _operator_contract(details, binding):
    # Docker inherits the peer hostname only when the shared network starts.
    # Admission separately permits only the worker's default or that exact peer.
    normalized = {**details, "config": {**details["config"], "Hostname": binding["database_hostname"]}}
    return _database_contract(normalized)


def _operator_admit(identity, saved):
    details = _database_details(identity)
    config, host = details["config"], details["host"]
    expected = saved["binding"]
    mounted = [m for m in details["mounts"] if m["Type"] != "tmpfs"]
    mounts = {m["Destination"]: (m["Type"], m["Source"], m["RW"]) for m in mounted}
    admitted = (
        details["image"] == expected["image"] and config["User"] == "0:0"
        and config["Entrypoint"] == ["python"] and config["Cmd"] == _OPERATOR_COMMAND
        and config["Hostname"] in (identity[:12], expected["database_hostname"])
        and config["Labels"].get("qt.storage.handoff") == expected["request_sha256"]
        and _digest(sorted(config["Env"])) == expected["environment_sha256"]
        and host["NetworkMode"] == "container:"+expected["database_id"]
        and host["PidMode"] == "container:"+expected["database_id"]
        and host["ReadonlyRootfs"] and not host["Privileged"]
        and host["RestartPolicy"]["Name"] == "no" and host["Init"] is True
        and host["CapDrop"] == ["ALL"] and sorted(host.get("CapAdd") or []) == _OPERATOR_CAPS
        and not host.get("Devices") and not host.get("DeviceRequests")
        and not host.get("GroupAdd") and not host.get("Sysctls")
        and host["SecurityOpt"] == ["no-new-privileges"]
        and host["Memory"] == 2*1024**3 and host["NanoCpus"] == 2*10**9
        and host["PidsLimit"] == 128
        and host["Tmpfs"] == {"/tmp":"rw,nosuid,nodev,size=67108864,uid=70,gid=70,mode=1770",
                              "/app/logs":"rw,nosuid,nodev,size=16777216,uid=70,gid=70,mode=0750"}
        and len(mounts) == len(mounted) == len(expected["mounts"])
        # Docker reports configured tmpfs in HostConfig even when Mounts omits
        # them on a created container. Reject any reported foreign tmpfs target.
        and {m["Destination"] for m in details["mounts"] if m["Type"] == "tmpfs"} <= set(host["Tmpfs"])
        and mounts == {key: tuple(value) for key,value in expected["mounts"].items()}
    )
    if not admitted or (saved["contract"] is not None
                        and _operator_contract(details, expected) != saved["contract"]):
        raise RuntimeError("storage_database_operator_container_changed")
    return details


@contextmanager
def _held_database_handoff(state_root: Path, *, project: str, source_revision: str,
                              history_uuid: str, image: str, request: dict,
                              inventory_path: Path):
    """Invoke the packaged database sequence under the existing deployment hold.

    The operator uses the prepared database's PID/network/storage namespaces and
    the existing SSD working bind. It has no Docker socket or provider settings.
    The bounded legacy ownership phase runs with only its required capabilities,
    then permanently drops to UID/GID 70 before opening a database connection.
    An interrupted controller reuses its exact container; clients remain held on
    every outcome. Runtime activation/hold retirement are intentionally separate.
    """
    from urllib.parse import quote, urlsplit, unquote
    state_root, inventory_path = Path(state_root), Path(inventory_path)
    if (not re.fullmatch(r"sha256:[0-9a-f]{64}", image)
            or not inventory_path.is_absolute() or inventory_path.resolve(strict=True) != inventory_path
            or len(json.dumps(request)) > 65536):
        raise ValueError("storage_database_operator_invalid_inputs")
    request_hash = _digest(request)
    state_path = state_root/_OPERATOR_STATE
    saved = _load(state_path) if os.path.lexists(state_path) else None
    name = project+"-storage-handoff"
    found = _docker("ps", "-aq", "--no-trunc", "--filter", "name=^/"+name+"$").split()
    if len(found) > 1 or (found and (saved is None or saved.get("container_id") not in (None,found[0]))):
        raise RuntimeError("storage_database_operator_unexpected_container")
    if saved is not None:
        if (set(saved) != {"binding", "container_id", "contract", "deadline"}
                or saved["binding"].get("request_sha256") != request_hash
                or saved["binding"].get("image") != image
                or saved["binding"].get("project") != project
                or saved["binding"].get("source_revision") != source_revision
                or (saved["container_id"] is not None and not found)):
            raise RuntimeError("storage_database_operator_saved_binding_changed")
        if found:
            _operator_admit(found[0],saved)
    with paused_storage_clients(state_root,project=project,source_revision=source_revision,
            prepare_database=True,history_uuid=history_uuid,
            _operator_id=found[0] if found else None) as receipt:
        database_id = receipt["containers"]["tsdb"]["id"]
        database = _database_details(database_id)
        collector = _database_details(receipt["containers"]["market-data-collector"]["id"])
        mounts = [m for m in collector["mounts"] if m["Destination"] == "/app/logs/market-structure"]
        if len(mounts) != 1 or mounts[0]["Type"] != "bind" or not mounts[0]["RW"]:
            raise RuntimeError("storage_database_operator_existing_working_bind_required")
        working = mounts[0]["Source"]
        if Path(working).resolve(strict=True) != Path(working):
            raise RuntimeError("storage_database_operator_working_path_changed")
        udev = Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
        if not udev.is_absolute() or udev.resolve(strict=True) != udev:
            raise RuntimeError("storage_database_operator_udev_path_invalid")
        image_details = json.loads(_docker("image", "inspect", "--format", '{{json .}}', image))
        image_env = dict(v.split("=",1) for v in image_details["Config"].get("Env") or [])
        if (image_details["Id"] != image
                or image_env.get("QT_IMAGE_SOURCE_REVISION") != request.get("source_revision")
                or image_env.get("QT_IMAGE_SOURCE_TREE_HASH") != request.get("source_tree_hash")):
            raise RuntimeError("storage_database_operator_image_source_changed")
        if (request.get("source_root") != "/app/logs/market-structure/objects"
                or request.get("destination_root") != "/qt-history/archives/objects"
                or request.get("inventory_path") != "/run/quanttrad/storage-inventory.json"):
            raise RuntimeError("storage_database_operator_fixed_paths_required")
        identity = _database_query(database_id,
            "SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c "
            "CROSS JOIN pg_database d WHERE d.datname=current_database()")
        if identity != request.get("database_identity"):
            raise RuntimeError("storage_database_operator_database_changed")
        old_env = dict(v.split("=",1) for v in collector["config"].get("Env") or [])
        db_env = dict(v.split("=",1) for v in database["config"]["Env"])
        url = urlsplit(old_env.get("PG_DSN", ""))
        if (url.scheme != "postgresql+psycopg2" or url.hostname not in ("tsdb","tsdb.quanttrad")
                or url.port not in (None,5432) or url.query or url.fragment
                or unquote(url.username or "") != db_env["POSTGRES_USER"]
                or unquote(url.password or "") != db_env["POSTGRES_PASSWORD"]
                or unquote(url.path.removeprefix("/")) != db_env["POSTGRES_DB"]):
            raise RuntimeError("storage_database_operator_connection_binding_changed")
        dsn = "postgresql+psycopg2://"+quote(db_env["POSTGRES_USER"],safe="")+":"+quote(db_env["POSTGRES_PASSWORD"],safe="")+"@127.0.0.1:5432/"+quote(db_env["POSTGRES_DB"],safe="")
        inventory = json.loads(inventory_path.read_text())
        targets = inventory.get("targets",[])
        ssd = [t for t in targets if t.get("medium")=="ssd"]
        hdd = [t for t in targets if t.get("medium")=="hdd"]
        if (len(targets)!=2 or len(ssd)!=1 or len(hdd)!=1
                or ssd[0].get("root")!="/var/lib/postgresql/data"
                or hdd[0].get("root")!="/qt-history" or hdd[0].get("filesystem_uuid")!=history_uuid):
            raise RuntimeError("storage_database_operator_inventory_changed")
        overrides = {"PG_DSN":dsn,"QT_DISABLE_DOTENV":"1",
            "MARKET_STRUCTURE_STORAGE_ROOT":"/qt-history/archives",
            "MARKET_STRUCTURE_WORKING_ROOT":"/app/logs/market-structure",
            "QT_STORAGE_UDEV_ROOT":"/run/qt-handoff/udev",
            "QT_MARKET_DATA_EXPECTED_UUID":history_uuid,
            "QT_MARKET_DATA_WORKING_EXPECTED_UUID":ssd[0]["filesystem_uuid"],
            "QT_HANDOFF_WORKING_DEVICE":str(Path(working).stat().st_dev),
            "QT_HANDOFF_WORKING_INODE":str(Path(working).stat().st_ino),
            "QT_HANDOFF_WORKING_SECONDS":str(request["max_duration_seconds"])}
        request_path = state_root/_OPERATOR_REQUEST
        if not saved and not os.path.lexists(request_path):
            descriptor = os.open(request_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o444)
            with os.fdopen(descriptor,"w") as handle:
                handle.write(json.dumps(request,sort_keys=True));handle.flush();os.fsync(handle.fileno())
            _sync_directory(state_root)
        if (request_path.is_symlink() or request_path.stat().st_uid != os.getuid()
                or stat.S_IMODE(request_path.stat().st_mode)!=0o444
                or _digest(json.loads(request_path.read_text()))!=request_hash):
            raise RuntimeError("storage_database_operator_request_file_changed")
        expected_mounts = {m["Destination"]:[m["Type"],m["Source"],m["RW"]] for m in database["mounts"]}
        binds = {"/app/logs/market-structure":working,
                 "/run/quanttrad/storage-inventory.json":str(inventory_path),
                 "/run/qt-handoff/request.json":str(request_path),"/run/qt-handoff/udev":str(udev)}
        expected_mounts.update({target:["bind",source,target=="/app/logs/market-structure"]
                                for target,source in binds.items()})
        expected_env = {**image_env,**overrides}
        binding = dict(project=project,source_revision=source_revision,image=image,
            request_sha256=request_hash,database_id=database_id,database_hostname=database["config"]["Hostname"],
            mounts=expected_mounts,
            inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
            environment_sha256=_digest(sorted(k+"="+v for k,v in expected_env.items())))
        if saved is None:
            saved = dict(binding=binding,container_id=None,contract=None,
                         deadline=time.time()+request["max_duration_seconds"]+60)
            _save(state_path,saved,initial=True)
        elif saved["binding"] != binding:
            raise RuntimeError("storage_database_operator_saved_binding_changed")
        remaining = int(saved["deadline"]-time.time())
        if remaining < 1:
            if found:
                _docker("stop","--time","30",found[0],timeout=45)
            raise RuntimeError("storage_database_operator_deadline_expired")
        if not found:
            args = ["create","--name",name,"--pull","never","--user","0:0","--init",
                "--restart","no","--read-only","--cap-drop","ALL","--security-opt","no-new-privileges",
                "--memory","2g","--cpus","2","--pids-limit","128",
                "--network","container:"+database_id,"--pid","container:"+database_id,
                "--volumes-from",database_id,"--entrypoint","python",
                "--label","qt.storage.handoff="+request_hash,
                "--tmpfs","/tmp:rw,nosuid,nodev,size=67108864,uid=70,gid=70,mode=1770",
                "--tmpfs","/app/logs:rw,nosuid,nodev,size=16777216,uid=70,gid=70,mode=0750"]
            for capability in _OPERATOR_CAPS:
                args += ["--cap-add",capability]
            for target,source in binds.items():
                if "," in source:
                    raise ValueError("storage_database_operator_mount_path_invalid")
                args += ["--mount","type=bind,source="+source+",target="+target
                         + ("" if target=="/app/logs/market-structure" else ",readonly")]
            for key,value in overrides.items():
                args += ["--env",key if key=="PG_DSN" else key+"="+value]
            found = [_docker(*args,image,*_OPERATOR_COMMAND,env={**os.environ,"PG_DSN":dsn}).strip()]
        details = _operator_admit(found[0],saved)
        saved.update(container_id=found[0],contract=_operator_contract(details, binding))
        _save(state_path,saved,initial=False)
        state = json.loads(_docker("inspect","--format","{{json .State}}",found[0]))
        if state["OOMKilled"] or state["Paused"] or state["Restarting"]:
            raise RuntimeError("storage_database_operator_unstable")
        if not state["Running"]:
            _docker("start",found[0])
        print("event=storage_database_operator_waiting clients_held=true",file=sys.stderr,flush=True)
        try:
            status = _docker("wait",found[0],timeout=remaining).strip()
        except subprocess.TimeoutExpired:
            _docker("stop","--time","30",found[0],timeout=45)
            raise RuntimeError("storage_database_operator_deadline_expired") from None
        _operator_admit(found[0],saved)
        if status != "0":
            raise RuntimeError("storage_database_operator_failed_hold_retained")
        # Docker applies --tail across stdout and stderr together. A final
        # stderr shutdown message can otherwise hide the stdout receipt.
        lines = _docker("logs", "--tail", "100", found[0]).strip().splitlines()
        try:
            result = json.loads(lines[-1]) if lines else None
        except (json.JSONDecodeError, TypeError):
            raise RuntimeError("storage_database_operator_outcome_invalid") from None
        if not isinstance(result, dict):
            raise RuntimeError("storage_database_operator_outcome_invalid")
        if (result.get("schema_version")!="qt.storage_database_operator_result.v1"
                or result.get("request_sha256")!=request_hash or result.get("database_identity")!=identity
                or any(result.get(key) is not True for key in ("database_sequence_complete","source_preserved","policy_current","runtime_activation_required"))
                or result.get("collection_resume_authorized") is not False):
            raise RuntimeError("storage_database_operator_outcome_invalid")
        rows = _inventory(project,operator_id=found[0])
        if _identities(rows)!=receipt["containers"] or any(rows[s]["running"] for s in STOP):
            raise RuntimeError("storage_database_operator_clients_changed")
        print("event=storage_database_operator_completed clients_held=true",file=sys.stderr,flush=True)
        yield result,receipt,binding


def run_held_database_handoff(state_root: Path, **options):
    """Database-only boundary; retain the host hold after its lock is released."""
    with _held_database_handoff(state_root,**options) as (result,_,__):
        return result


RUNTIME_RECIPE = "storage-runtime.compose.json"
_RUNTIME_WRITERS = {
    "backend": "portal.backend.run_backend",
    "initialize": "portal.backend.workers.single_node_initializer",
    "market-data-collector": "portal.backend.workers.market_data_collector",
}


def _runtime_configuration_bytes(path: Path, maximum: int = 128*1024) -> bytes:
    if not path.is_absolute() or path.resolve(strict=True)!=path:
        raise RuntimeError("storage_runtime_configuration_path_changed")
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,"rb") as handle:
        before=os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_size>maximum
                or before.st_uid not in (0,os.getuid()) or before.st_mode & 0o022):
            raise RuntimeError("storage_runtime_configuration_file_invalid")
        raw=handle.read(maximum+1)
        after=os.fstat(handle.fileno())
        if len(raw)>maximum or (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns):
            raise RuntimeError("storage_runtime_configuration_changed_during_read")
        return raw


_RUNTIME_MAINTENANCE_PROBE = """
import json, sys
from portal.backend.service.storage.maintenance_runtime import read_storage_maintenance_limits
history, recovery, _ = read_storage_maintenance_limits(sys.argv[1])
ids = set(json.loads(sys.argv[2]))
if any(set(history[key]) != ids for key in
       ("temporary_bytes", "growth_bytes_per_second", "maintenance_bytes")):
    raise ValueError("storage_runtime_history_targets_changed")
if set(recovery["headroom_bytes"]) != ids:
    raise ValueError("storage_runtime_recovery_targets_changed")
print(json.dumps({"validated": True}))
"""


def _validate_runtime_maintenance(image: str, path: Path, target_ids: list[str]):
    """Use the pinned worker's routine parser without database, keys or network.

    Migration budgets belong to the original capture attempt. The independently
    prepared routine file keeps its one-hour limit and is bound by its hash in
    the activation receipt; equality with multi-day migration limits is invalid.
    """
    if "," in str(path):
        raise RuntimeError("storage_runtime_maintenance_path_invalid")
    output = _docker("run", "--rm", "--pull", "never", "--network", "none",
        "--read-only", "--user", "70:70", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--memory", "512m", "--cpus", "1",
        "--pids-limit", "64", "--env", "QT_DISABLE_DOTENV=1",
        "--mount", "type=bind,source="+str(path)+",target=/run/qt-maintenance.json,readonly",
        "--entrypoint", "python", image, "-c", _RUNTIME_MAINTENANCE_PROBE,
        "/run/qt-maintenance.json", json.dumps(sorted(target_ids)))
    if json.loads(output) != {"validated": True}:
        raise RuntimeError("storage_runtime_maintenance_validation_failed")


def _runtime_recipe(state_root: Path, receipt: dict, binding: dict, request: dict):
    """Admit the fixed candidate before any application container is changed.

    The private recipe is a rendered, image-pinned Compose snapshot using only
    existing volumes/network. It is not a public placement or deployment API.
    Compose preserves dollar escaping in rendered JSON; decode it only when
    comparing with the actual environment of the stopped collector.
    """
    path = state_root/RUNTIME_RECIPE
    model = _load(path,max_bytes=524288)
    recipe_hash = _digest(model)
    project = receipt["project"]
    if (set(model)!={"name","services","networks","volumes"} or model["name"]!=project
            or set(model["services"])!=set(receipt["containers"])
            or not _existing_network_recipe(model["networks"],project)):
        raise RuntimeError("storage_runtime_fixed_topology_required")
    # The prepared database definition and its existing volume are retained.
    database_model,_ = _database_recipe(state_root,project)
    candidate_database=json.loads(json.dumps(model["services"]["tsdb"]))
    if candidate_database.get("entrypoint") is None:
        candidate_database.pop("entrypoint",None)
    shared_memory=candidate_database.get("shm_size")
    if (isinstance(shared_memory,str) and re.fullmatch(r"[0-9]{1,20}",shared_memory)
            and type(database_model["services"]["tsdb"].get("shm_size")) is int):
        candidate_database["shm_size"]=int(shared_memory)
    for mount in candidate_database.get("volumes",[]):
        if mount.get("volume")=={}:
            mount.pop("volume")
    if (_digest(database_model)!=receipt["database_preparation"]["recipe_sha256"]
            or candidate_database!=database_model["services"]["tsdb"]
            or model["volumes"].get("postgres-data")!=database_model["volumes"]["postgres-data"]):
        raise RuntimeError("storage_runtime_database_recipe_changed")
    old = {name:_database_details(row["id"]) for name,row in receipt["containers"].items()}
    volume_names = {m["Name"] for details in old.values() for m in details["mounts"] if m["Type"]=="volume"}
    if any(set(value)!={"name","external"} or value["external"] is not True
           or value["name"] not in volume_names for value in model["volumes"].values()):
        raise RuntimeError("storage_runtime_existing_volumes_required")
    mounts = binding["mounts"]
    history = mounts["/qt-history"][1]
    working = mounts["/app/logs/market-structure"][1]
    inventory = Path(mounts["/run/quanttrad/storage-inventory.json"][1])
    inventory_bytes=_runtime_configuration_bytes(inventory)
    if hashlib.sha256(inventory_bytes).hexdigest()!=binding["inventory_sha256"]:
        raise RuntimeError("storage_runtime_inventory_changed")
    targets = json.loads(inventory_bytes)["targets"]
    ssd = next(t for t in targets if t["medium"]=="ssd")
    hdd = next(t for t in targets if t["medium"]=="hdd")
    original_env = dict(value.split("=",1) for value in old["market-data-collector"]["config"]["Env"])
    literal = lambda value: value.replace("$$","$") if isinstance(value,str) else value
    file_bindings = {str(inventory):binding["inventory_sha256"]}
    pinned = {}
    for name,service in model["services"].items():
        image = service.get("image", "")
        if (not re.fullmatch(r"sha256:[0-9a-f]{64}",image) or service.get("pull_policy")!="never"
                or any(key in service for key in ("build","env_file","extends","profiles"))
                or set(service.get("networks",{}))!={"quanttrad"}):
            raise RuntimeError("storage_runtime_pinned_image_and_network_required")
        pinned[name]=image
        if name in _RUNTIME_WRITERS:
            if (image!=binding["image"] or service.get("user")!="70:70"
                    or service.get("command")!=["python","-m",_RUNTIME_WRITERS[name]]
                    or service.get("entrypoint") is not None
                    or service.get("privileged",False) or service.get("devices") or service.get("cap_add")
                    or (name!="backend" and service.get("group_add"))):
                raise RuntimeError("storage_runtime_writer_identity_or_command_changed")
        if name in (*_RUNTIME_WRITERS,"docker-stats","frontend","frontend-v2"):
            details=json.loads(_docker("image","inspect","--format",'{{json .}}',image))
            environment=dict(value.split("=",1) for value in details["Config"].get("Env") or [])
            if (details["Id"]!=image
                    or environment.get("QT_IMAGE_SOURCE_REVISION")!=request["source_revision"]
                    or environment.get("QT_IMAGE_SOURCE_TREE_HASH")!=request["source_tree_hash"]):
                raise RuntimeError("storage_runtime_candidate_source_changed")
        elif image!=receipt["containers"][name]["image"]:
            raise RuntimeError("storage_runtime_unrelated_image_changed")
        if name not in _RUNTIME_WRITERS:
            continue
        environment={key:literal(value) for key,value in service.get("environment",{}).items()}
        required={"QT_DISABLE_DOTENV":"1","PG_DSN":original_env["PG_DSN"],
            "MARKET_STRUCTURE_STORAGE_ROOT":"/qt-history/archives",
            "MARKET_STRUCTURE_WORKING_ROOT":"/app/logs/market-structure",
            "QT_MARKET_DATA_EXPECTED_UUID":hdd["filesystem_uuid"],
            "QT_MARKET_DATA_WORKING_EXPECTED_UUID":ssd["filesystem_uuid"],
            "QT_STORAGE_INVENTORY_PATH":"/run/quanttrad/storage-inventory.json",
            "QT_STORAGE_UDEV_ROOT":"/run/qt-host-udev/data"}
        if any(environment.get(key)!=value for key,value in required.items()):
            raise RuntimeError("storage_runtime_writer_configuration_changed")
        if name=="backend" and environment.get("QT_MARKET_DATA_ROOT")!=history+"/archives":
            raise RuntimeError("storage_runtime_bot_archive_root_changed")
        if name=="market-data-collector":
            if (service.get("pid")!="service:tsdb"
                    or any(environment.get(key)!="true" for key in ("QT_MARKET_DATA_LIFECYCLE_ENABLED",
                        "QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED","QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED"))
                    or environment.get("QT_STORAGE_MAINTENANCE_LIMITS_PATH")!="/run/quanttrad/storage-maintenance.json"):
                raise RuntimeError("storage_runtime_automatic_policy_configuration_changed")
        entries=service.get("volumes",[])
        by_target={value["target"]:value for value in entries}
        if len(entries)!=len(by_target):
            raise RuntimeError("storage_runtime_duplicate_mount")
        expected={"/qt-history":(history,False),"/app/logs/market-structure":(working,False),
            "/run/quanttrad/storage-inventory.json":(str(inventory),True),
            "/run/qt-host-udev":(str(Path(mounts["/run/qt-handoff/udev"][1]).parent),True)}
        for target,(source,readonly) in expected.items():
            value=by_target.get(target,{})
            if (value.get("type")!="bind" or literal(value.get("source"))!=source
                    or value.get("read_only",False)!=readonly
                    or value.get("bind")!={"create_host_path":False}):
                raise RuntimeError("storage_runtime_writer_mount_changed")
        allowed=set(expected)|{"/var/lib/postgresql/data","/app/secrets.env"}
        if name=="backend":
            allowed.add("/var/run/docker.sock")
        if name=="market-data-collector":
            allowed.add("/run/quanttrad/storage-maintenance.json")
            recovery_mounts = _database_recovery_mounts(
                database_model["services"]["tsdb"],model["volumes"],history)
            for target, expected_mount in recovery_mounts.items():
                actual_mount = dict(by_target.get(target,{}))
                if actual_mount.get("volume")=={}:
                    actual_mount.pop("volume")
                if actual_mount != expected_mount:
                    raise RuntimeError("storage_runtime_recovery_mount_changed")
            allowed.update(recovery_mounts)
        if set(by_target)!=allowed:
            raise RuntimeError("storage_runtime_unexpected_writer_mount")
        secret=by_target["/app/secrets.env"]
        original_secrets=[value for value in old[name]["mounts"] if value["Destination"]=="/app/secrets.env"]
        if (len(original_secrets)!=1 or secret.get("type")!="bind" or secret.get("read_only") is not True
                or literal(secret.get("source"))!=original_secrets[0]["Source"]):
            raise RuntimeError("storage_runtime_secrets_mount_changed")
        if name=="backend":
            socket=by_target["/var/run/docker.sock"]
            if socket.get("type")!="bind" or literal(socket.get("source"))!="/var/run/docker.sock":
                raise RuntimeError("storage_runtime_backend_socket_changed")
        pgdata=by_target.get("/var/lib/postgresql/data",{})
        if (pgdata.get("type")!="volume" or pgdata.get("source")!="postgres-data"
                or pgdata.get("read_only",False) or pgdata.get("volume")):
            raise RuntimeError("storage_runtime_writer_database_mount_changed")
        if name=="market-data-collector":
            value=by_target.get("/run/quanttrad/storage-maintenance.json",{})
            limits_path=Path(literal(value.get("source","")))
            if (value.get("type")!="bind" or value.get("read_only") is not True
                    or value.get("bind")!={"create_host_path":False}
                    or not limits_path.is_absolute() or limits_path.resolve(strict=True)!=limits_path):
                raise RuntimeError("storage_runtime_maintenance_mount_changed")
            raw=_runtime_configuration_bytes(limits_path)
            _validate_runtime_maintenance(binding["image"], limits_path,
                [target["target_id"] for target in targets])
            if _runtime_configuration_bytes(limits_path) != raw:
                raise RuntimeError("storage_runtime_maintenance_changed_during_validation")
            file_bindings[str(limits_path)]=hashlib.sha256(raw).hexdigest()
    if _digest(_load(path,max_bytes=524288))!=recipe_hash:
        raise RuntimeError("storage_runtime_recipe_changed_during_admission")
    return dict(recipe_sha256=recipe_hash,images=pinned,files=file_bindings)


_RUNTIME_PROBE = """
import json, os, sys
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from scripts.db.fact_header_v2_handoff import inspect_runtime_handoff
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.workers.market_data_collector_health import live_worker_for_host
raw=sys.stdin.buffer.read(65537)
if len(raw)>65536: raise ValueError('storage_runtime_request_too_large')
request=json.loads(raw,object_pairs_hook=_unique_fields)
worker=live_worker_for_host()
engine=create_engine(os.environ['PG_DSN'],poolclass=NullPool,hide_parameters=True,
                     connect_args={'connect_timeout':10})
try:
    result=inspect_runtime_handoff(request,engine=engine,worker=worker)
    print(json.dumps(result,sort_keys=True),flush=True)
finally:
    engine.dispose()
"""


def _runtime_observation(container: str, request: dict, database_result: dict):
    value=json.loads(_docker("exec","-i",container,"python","-c",_RUNTIME_PROBE,
        input=json.dumps(request,sort_keys=True),timeout=45).strip().splitlines()[-1])
    if value.get("ready") is False and set(value)=={"ready","reason"} and value["reason"] in (
            "maintenance_starting","maintenance_degraded","current_layout_recovery_pending",
            "storage_operation_running"):
        return value
    if (value.get("ready") is not True or value.get("database_identity")!=request["database_identity"]
            or value.get("plan_id")!=database_result["plan_id"]
            or not re.fullmatch(r"copy_[0-9a-f]{32}",value.get("recovery_generation",""))
            or not isinstance(value.get("storage_layout"),dict)
            or value["storage_layout"].get("layout_version")!="market.fact_storage_tiers.v2"
            or not re.fullmatch(r"[0-9a-f]{64}",value["storage_layout"].get("certificate_sha256",""))):
        raise RuntimeError("storage_runtime_observation_invalid")
    return value


_RUNTIME_STATE = "storage-runtime-state.json"


def _runtime_candidate_details(name, row, model, saved):
    """Inspect created or running candidate containers against the fixed recipe."""
    details = _database_details(row["id"])
    service = model["services"][name]
    image = json.loads(_docker("image", "inspect", "--format", '{{json .}}', service["image"]))
    config, host = details["config"], details["host"]
    literal = lambda value: value.replace("$$", "$") if isinstance(value, str) else value
    expected_env = dict(value.split("=", 1) for value in image["Config"].get("Env") or [])
    for key, value in service.get("environment", {}).items():
        if value is None:
            expected_env.pop(key, None)
        else:
            expected_env[key] = literal(str(value))
    expected_command = service.get("command")
    if expected_command is None:
        expected_command = image["Config"].get("Cmd")
    expected_entrypoint = service.get("entrypoint")
    if expected_entrypoint is None:
        expected_entrypoint = image["Config"].get("Entrypoint")
    if (row["image"] != saved["admission"]["images"][name] or details["image"] != row["image"]
            or image["Id"] != row["image"]
            or config.get("Labels", {}).get("com.docker.compose.config-hash") != saved["compose_hashes"][name]
            or dict(value.split("=", 1) for value in config.get("Env") or []) != expected_env
            or config.get("Cmd") != expected_command or config.get("Entrypoint") != expected_entrypoint
            or config.get("User", "") != service.get("user", image["Config"].get("User", ""))
            or host.get("Privileged") or host.get("Devices") or host.get("DeviceRequests")
            or host.get("CapAdd") or host.get("PidMode", "") != (
                "container:"+saved["binding"]["database_id"] if name == "market-data-collector" else "")):
        raise RuntimeError("storage_runtime_candidate_configuration_changed: service="+name)
    network = saved["receipt"]["project"]+"_quanttrad"
    network_id = saved["receipt"]["database_preparation"]["networks"][network]["network_id"]
    if (set(details["networks"]) != {network}
            or details["networks"][network]["NetworkID"] not in ("", network_id)):
        raise RuntimeError("storage_runtime_candidate_network_changed: service="+name)
    expected_mounts = {}
    for mount in service.get("volumes", []):
        kind = mount["type"]
        source = literal(mount["source"])
        if kind == "volume":
            source = model["volumes"][source]["name"]
        elif kind != "bind":
            raise RuntimeError("storage_runtime_unsupported_mount: service="+name)
        expected_mounts[mount["target"]] = (kind, source, not mount.get("read_only", False))
    actual_mounts = {mount["Destination"]: (mount["Type"],
        mount["Name"] if mount["Type"] == "volume" else mount["Source"], mount["RW"])
        for mount in details["mounts"]}
    if actual_mounts != expected_mounts or len(actual_mounts) != len(details["mounts"]):
        raise RuntimeError("storage_runtime_candidate_mount_changed: service="+name)
    expected_probe = service.get("healthcheck", {}).get("test", image["Config"].get("Healthcheck", {}).get("Test"))
    if expected_probe is not None and config.get("Healthcheck", {}).get("Test") != [literal(part) for part in expected_probe]:
        raise RuntimeError("storage_runtime_candidate_healthcheck_changed: service="+name)
    return details


def _runtime_bound_model(state_root, saved, request):
    if (saved.get("schema_version") != "qt.storage_runtime_activation.v1"
            or saved.get("request_sha256") != _digest(request)
            or saved.get("probe_sha256") != hashlib.sha256(_RUNTIME_PROBE.encode()).hexdigest()
            or saved.get("phase") not in ("starting", "verified", "complete")):
        raise RuntimeError("storage_runtime_saved_binding_changed")
    if any(os.path.lexists(state_root/name) for name in ("promotion.env", "alert-preview.env")):
        raise RuntimeError("storage_runtime_unfinished_server_operation")
    model = _load(state_root/RUNTIME_RECIPE, max_bytes=524288)
    if _digest(model) != saved["admission"]["recipe_sha256"]:
        raise RuntimeError("storage_runtime_recipe_changed")
    for name, expected in saved["admission"]["files"].items():
        if hashlib.sha256(_runtime_configuration_bytes(Path(name))).hexdigest() != expected:
            raise RuntimeError("storage_runtime_configuration_file_changed")
    preparation = saved["receipt"]["database_preparation"]
    _history_filesystem(preparation["history_root"], preparation["history_uuid"])
    database = _database_details(saved["binding"]["database_id"])
    actual_mounts = {m["Destination"]: (m["Type"], m["Source"], m["RW"]) for m in database["mounts"]}
    recovery = _database_recovery_mounts(model["services"]["tsdb"], model["volumes"], preparation["history_root"])
    expected_mounts = {target: tuple(saved["binding"]["mounts"][target])
                       for target in {"/var/lib/postgresql/data", "/qt-history"} | set(recovery)}
    recovery_unchanged = True
    if recovery:
        by_target = {m["Destination"]: m for m in database["mounts"]}
        keys = by_target.get("/run/quanttrad/recovery", {})
        socket = by_target.get("/var/run/postgresql", {})
        recovery_unchanged = (
            keys.get("Type") == "bind" and keys.get("RW") is False
            and keys.get("Source") == recovery["/run/quanttrad/recovery"]["source"]
            and keys.get("Propagation") == "rprivate"
            and socket.get("Type") == "volume" and socket.get("RW") is True
            and socket.get("Name") == model["volumes"]["storage-recovery-socket"]["name"])
    if (database["image"] != preparation["image"]
            or _database_contract(database) != preparation["target_contract"]
            or actual_mounts != expected_mounts or len(database["mounts"]) != len(expected_mounts)
            or not recovery_unchanged
            or not _same_database_networks(database, preparation["networks"])
            or _cluster_identifier(database["id"]) != preparation["cluster_identifier"]):
        raise RuntimeError("storage_runtime_prepared_database_changed")
    return model


def _runtime_rows(saved):
    rows = _inventory(saved["receipt"]["project"], operator_id=saved["operator_id"], activating=True)
    if not set(rows) <= set(saved["receipt"]["containers"]) or rows["tsdb"]["id"] != saved["binding"]["database_id"]:
        raise RuntimeError("storage_runtime_service_inventory_changed")
    return rows


def _runtime_healthy(rows, model, saved):
    if set(rows) != set(saved["receipt"]["containers"]):
        return False
    for name, row in rows.items():
        if name == "tsdb":
            continue
        _runtime_candidate_details(name, row, model, saved)
        state = json.loads(_docker("inspect", "--format", "{{json .State}}", row["id"]))
        if state["OOMKilled"] or state["Paused"] or state["Restarting"]:
            return False
        if name == "initialize":
            if state["Running"] or state["Status"] != "exited" or state["ExitCode"] != 0:
                return False
        elif (not state["Running"]
                or state.get("Health", {}).get("Status", "healthy") != "healthy"
                or (name in ("backend", "market-data-collector", "frontend", "frontend-v2")
                    and "Health" not in state)):
            return False
    return True


def _record_storage_release(state_root, saved, request):
    from datetime import datetime, timezone
    # Persist the exact intended release bytes before replacing release.env.
    # A crash after replacement can then reconcile without selecting an old image.
    if "release_text" not in saved:
        # The pre-migration image is not a compatible automatic rollback for
        # new writes. Its revision remains in the private migration receipt.
        saved["release_text"] = (
            "current_revision="+request["source_revision"]+"\n"
            "current_source_tree_hash="+request["source_tree_hash"]+"\n"
            "previous_revision=\n"
            "deployed_at="+datetime.now(timezone.utc).isoformat()+"\n"
            "storage_layout=ssd-hdd-v1\n")
        _save(state_root/_RUNTIME_STATE, saved, initial=False)
    current = (state_root/"release.env").read_text()
    if current not in (saved["prior_release"], saved["release_text"]):
        raise RuntimeError("storage_runtime_recorded_release_changed")
    if current != saved["release_text"]:
        descriptor, name = tempfile.mkstemp(prefix=".storage-release-", dir=state_root)
        try:
            with os.fdopen(descriptor, "w") as handle:
                handle.write(saved["release_text"]); handle.flush(); os.fsync(handle.fileno())
            os.replace(name, state_root/"release.env")
            _sync_directory(state_root)
        finally:
            if os.path.exists(name):
                os.unlink(name)


def _finish_runtime_activation(state_root, saved, request):
    path = state_root/_RUNTIME_STATE
    hold = state_root/HOLD
    # The host hold is retained through every app mutation and verification.
    # Completion reentry only observes the already recorded exact release.
    if saved["phase"] == "complete":
        if os.path.lexists(hold) or (state_root/"release.env").read_text() != saved["release_text"]:
            raise RuntimeError("storage_runtime_completed_release_changed")
        return saved["outcome"]
    if os.path.lexists(hold):
        if _load(hold) != saved["receipt"]:
            raise RuntimeError("storage_runtime_hold_changed")
    elif saved["phase"] != "verified" or (state_root/"release.env").read_text() != saved.get("release_text"):
        raise RuntimeError("storage_runtime_hold_missing_before_completion")
    model = _runtime_bound_model(state_root, saved, request)
    current_release = (state_root/"release.env").read_text()
    if current_release not in (saved["prior_release"], saved.get("release_text")):
        raise RuntimeError("storage_runtime_recorded_release_changed")
    rows = _runtime_rows(saved)
    if saved["phase"] == "starting":
        for name, row in rows.items():
            if name != "tsdb" and row["id"] != saved["receipt"]["containers"][name]["id"]:
                _runtime_candidate_details(name, row, model, saved)
        candidates = all(name == "tsdb" or row["id"] != saved["receipt"]["containers"][name]["id"]
                         for name, row in rows.items())
        already_running = candidates and _runtime_healthy(rows, model, saved)
        if not already_running:
            remaining = int(saved["deadline"]-time.time())
            if remaining < 1:
                raise RuntimeError("storage_runtime_activation_deadline_expired")
            print("event=storage_runtime_candidate_starting hold_retained=true", file=sys.stderr, flush=True)
            _docker("compose", "--project-name", saved["receipt"]["project"], "--file", str(state_root/RUNTIME_RECIPE),
                "up", "--detach", "--no-deps", "--no-build", "--pull", "never", "--wait",
                "--wait-timeout", str(min(360, remaining)), *sorted(set(model["services"])-{"tsdb"}), timeout=min(360, remaining)+30)
    last_reason = None
    while True:
        model = _runtime_bound_model(state_root, saved, request)
        rows = _runtime_rows(saved)
        if not _runtime_healthy(rows, model, saved):
            reason = "candidate_services_not_healthy"
        else:
            outcome = _runtime_observation(rows["market-data-collector"]["id"], request, saved["database_result"])
            if outcome["ready"]:
                break
            reason = outcome["reason"]
        remaining = saved["deadline"]-time.time()
        if remaining <= 0:
            raise RuntimeError("storage_runtime_activation_deadline_expired")
        if reason != last_reason:
            print("event=storage_runtime_activation_waiting reason="+reason+" hold_retained=true", file=sys.stderr, flush=True)
            last_reason = reason
        time.sleep(min(2, remaining))
    saved.update(phase="verified", outcome=outcome)
    _save(path, saved, initial=False)
    _record_storage_release(state_root, saved, request)
    if os.path.lexists(hold):
        hold.unlink()
        _sync_directory(state_root)
    saved["phase"] = "complete"
    _save(path, saved, initial=False)
    print("event=storage_runtime_activation_completed storage_layout=ssd-hdd-v1", file=sys.stderr, flush=True)
    return outcome


def run_held_runtime_handoff(state_root: Path, *, activation_timeout_seconds: int, **options):
    """Complete only the fixed initial cutover; never fall back to old images."""
    state_root = Path(state_root)
    if type(activation_timeout_seconds) is not int or not 1 <= activation_timeout_seconds <= 86400:
        raise ValueError("storage_runtime_activation_budget_invalid")
    path = state_root/_RUNTIME_STATE
    request = options["request"]
    if os.path.lexists(path):
        with _deployment_lock(state_root):
            saved = _load(path, max_bytes=131072)
            if (saved.get("request_sha256") != _digest(request)
                    or saved.get("binding", {}).get("image") != options["image"]
                    or saved.get("receipt", {}).get("project") != options["project"]
                    or saved.get("receipt", {}).get("source_revision") != options["source_revision"]
                    or saved["receipt"]["database_preparation"]["history_uuid"] != options["history_uuid"]
                    or saved["binding"]["mounts"]["/run/quanttrad/storage-inventory.json"][1] != str(Path(options["inventory_path"]))):
                raise RuntimeError("storage_runtime_saved_binding_changed")
            return _finish_runtime_activation(state_root, saved, request)
    with _held_database_handoff(state_root, **options) as (database_result, receipt, binding):
        admission = _runtime_recipe(state_root, receipt, binding, request)
        model = _load(state_root/RUNTIME_RECIPE, max_bytes=524288)
        # Compose hashes the collector after resolving its shared PID namespace.
        # Bind that resolution to the already prepared database; keep the admitted
        # on-disk recipe unchanged for startup and retry comparisons.
        hash_model = json.loads(json.dumps(model))
        hash_model["services"]["market-data-collector"]["pid"] = "container:" + binding["database_id"]
        hashes = _docker("compose", "--project-name", receipt["project"], "--file", "-",
            "config", "--hash", "*", input=json.dumps(hash_model))
        compose_hashes = dict(line.split() for line in hashes.splitlines())
        if set(compose_hashes) != set(model["services"]) or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in compose_hashes.values()):
            raise RuntimeError("storage_runtime_compose_hashes_invalid")
        operator = _load(state_root/_OPERATOR_STATE)
        saved = dict(schema_version="qt.storage_runtime_activation.v1", phase="starting",
            request_sha256=_digest(request), probe_sha256=hashlib.sha256(_RUNTIME_PROBE.encode()).hexdigest(),
            database_result=database_result, receipt=receipt, binding=binding,
            admission=admission, compose_hashes=compose_hashes, operator_id=operator["container_id"],
            prior_release=(state_root/"release.env").read_text(),
            deadline=min(operator["deadline"], time.time()+activation_timeout_seconds))
        _save(path, saved, initial=True)
        return _finish_runtime_activation(state_root, saved, request)

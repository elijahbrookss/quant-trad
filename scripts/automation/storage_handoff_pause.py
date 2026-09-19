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


def _docker(*args: str, timeout: int = 30) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    # Do not expose Docker diagnostics or inspected configuration: these may
    # include credentials. Private database settings are hashed by the caller.
    if result.returncode:
        raise RuntimeError(f"storage_pause_docker_failed: operation={args[0]} exit={result.returncode}")
    if len(result.stdout) > 524288:
        raise RuntimeError("storage_pause_inventory_too_large")
    return result.stdout


def _inventory(project: str, *, database_preparing: bool = False) -> dict[str, dict]:
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
    required = STOP if database_preparing else STOP + ("tsdb",)
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
    expression = '{"config":{{json .Config}},"host":{{json .HostConfig}},"mounts":{{json .Mounts}},"networks":{{json .NetworkSettings.Networks}},"image":{{json .Image}}}'
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
                                     if not re.fullmatch(r"[0-9a-f]{64}", alias)),
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


def _database_recipe(state_root: Path, project: str) -> tuple[dict, str]:
    model = _load(state_root / DATABASE_RECIPE)
    if (set(model) != {"name", "services", "networks", "volumes"} or model["name"] != project
            or set(model["services"]) != {"tsdb"}
            or model["networks"] != {"quanttrad": {"name": project+"_quanttrad", "external": True}}):
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
    if len(mounts) != 2 or set(by_target) != {"/var/lib/postgresql/data", "/qt-history"}:
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
            or model["volumes"] != {"postgres-data": {"name": source_mount["Name"], "external": True}}):
        raise RuntimeError("storage_database_source_binding_mismatch")
    service = model["services"]["tsdb"]
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


def _prepare_database(state_root: Path, receipt: dict) -> dict:
    preparation = receipt["database_preparation"]
    project = receipt["project"]
    path = state_root / HOLD
    def check():
        model, root = _database_recipe(state_root, project)
        if _digest(model) != preparation["recipe_sha256"] or root != preparation["history_root"]:
            raise RuntimeError("storage_database_preparation_recipe_changed")
        _history_filesystem(root, preparation["history_uuid"])
        rows = _inventory(project, database_preparing=True)
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
    admitted = {
        "source_stop": preparation["source_stopped"],
        "replacement_id": row["id"] != preparation["original_id"] and preparation["replacement_id"] in (None, row["id"]),
        "image": details["image"] == preparation["image"],
        "settings": _database_contract(details) == preparation["target_contract"],
        "network": _same_database_networks(details, preparation["networks"]),
        "mount_count": len(details["mounts"]) == 2,
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
def paused_storage_clients(state_root: Path, *, project: str, source_revision: str,
                           prepare_database: bool = False, history_uuid: str = ""):
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
        path = state_root / HOLD
        receipt = _load(path) if os.path.lexists(path) else None
        preparing = bool(receipt and receipt.get("phase") == "preparing_database")
        rows = _inventory(project, database_preparing=preparing)
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
                    receipt = _prepare_database(state_root, receipt)
                else:
                    # Re-enter through all physical/configuration/cluster checks.
                    receipt = _prepare_database(state_root, {**receipt, "phase": "preparing_database"})
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
            receipt = _prepare_database(state_root, receipt)
        print("event=storage_handoff_clients_stopped resume_authorized=false", file=sys.stderr, flush=True)
        yield receipt

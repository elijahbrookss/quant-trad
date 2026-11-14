"""Read-only fixed-host inventory for a cold cutover release preflight."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_final as prior
from scripts.automation import storage_online_release as release


SCHEMA = "qt.canonical_cold_host_inventory.v1"
PROJECT = "quant-trad-single-node"
SERVICE_ROSTER = tuple(sorted((*host.STOP, *host.PASSIVE, "storage-maintenance")))
STOPPABLE_SERVICES = frozenset((*host.STOP, "storage-maintenance")) - {"initialize"}
_OPTIONAL_TERMINAL_SERVICES = {"loki", "alloy", "docker-events", "docker-stats", "frontend", "frontend-v2", "grafana", "pgadmin"}
_APP_COMMANDS = {
    "backend": "portal.backend.run_backend",
    "initialize": "portal.backend.workers.single_node_initializer",
    "market-data-collector": "portal.backend.workers.market_data_collector",
    "storage-maintenance": "portal.backend.workers.storage_maintenance",
}
_RELEASE_SERVICES = {*_APP_COMMANDS, "frontend", "frontend-v2", "docker-stats"}
_HEALTHCHECK_SERVICES = {"tsdb", "backend", "market-data-collector", "storage-maintenance", "frontend",
                         "frontend-v2", "grafana"}
_STATE_KEYS = ("running", "status", "pid", "exit_code", "health", "started_at")
_ID = re.compile(r"[0-9a-f]{64}")
_IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
_REVISION = re.compile(r"[0-9a-f]{40}")
_DETAILS = '{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"config":{{json .Config}},' \
           '"host":{{json .HostConfig}},"mounts":{{json .Mounts}},"networks":{{json .NetworkSettings.Networks}},' \
           '"state":{{json .State}}}'


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _terminal_chain(root, project, expected_revision):
    saved = prior._load(root / prior.STATE)
    binding = saved.get("binding", {})
    if (saved.get("phase") != "recovery_runtime_ready" or binding.get("project") != project
            or saved.get("release", {}).get("status") != "deployed"):
        raise RuntimeError("canonical_host_previous_release_not_terminal")
    release._admit_initial_hold(root, saved)
    worker = host.load_receipt(root / "storage-online-worker.json")
    if (host.digest(worker) != binding.get("worker_sha256")
            or worker.get("container_id") != binding.get("worker_id")):
        raise RuntimeError("canonical_host_previous_worker_binding_changed")
    raw = release.read_private_environment(root / "release.env")
    fields = release._release_values(raw)
    if (set(fields) != {"current_revision", "current_source_tree_hash", "previous_revision", "deployed_at", "storage_layout"}
            or fields.get("storage_layout") != "ssd-hdd-v1" or fields.get("current_revision") != expected_revision
            or not _matches(_ID, fields.get("current_source_tree_hash"))):
        raise RuntimeError("canonical_host_serving_release_changed")
    hold = host.load_receipt(root / host.HOLD)
    preparation = host.load_receipt(root / "storage-online-preparation.json")
    if host.digest(preparation) != binding["preparation_sha256"] or host.digest(hold) != preparation["hold_sha256"]:
        raise RuntimeError("canonical_host_previous_hold_changed")
    return {"final_sha256": host.digest(saved), "preparation_sha256": binding["preparation_sha256"],
            "worker_sha256": binding["worker_sha256"], "release_sha256": hashlib.sha256(raw).hexdigest(),
            "initial_hold_sha256": host.digest(hold), "source_tree_hash": fields["current_source_tree_hash"]}, binding["worker_id"]


def _state(row):
    state = row["state"]
    if (any(state.get(field) is not False for field in ("Paused", "Restarting", "OOMKilled", "Dead"))
            or type(state.get("Running")) is not bool or type(state.get("Pid")) is not int
            or type(state.get("ExitCode")) is not int or not isinstance(state.get("StartedAt"), str)):
        raise RuntimeError("canonical_host_unstable_container")
    return {"running": state["Running"], "status": state.get("Status"), "pid": state["Pid"],
            "exit_code": state["ExitCode"], "health": (state.get("Health") or {}).get("Status"),
            "started_at": state["StartedAt"]}


def _clean_exit(state, allowed=(0, 143)):
    return (not state["running"] and state["status"] == "exited" and state["pid"] == 0
            and state["exit_code"] in allowed)


def _command(config):
    return (config.get("Entrypoint") or []) + (config.get("Cmd") or [])


def _bot_terminal(row, labels, state):
    bot, run = labels.get("quanttrad.bot_id"), labels.get("quanttrad.run_id")
    token = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
    restart = row["host"].get("RestartPolicy", {}).get("Name")
    return (labels.get("quanttrad.runtime") == "bot" and labels.get("loki.service") == "bot-runtime"
            and labels.get("com.docker.compose.oneoff") in (None, "", "False")
            and _matches(token, bot) and _matches(token, run)
            and row["name"] == f"/quant-trad-bots-{bot}-{run}"
            and _command(row["config"]) == ["python", "-m", "portal.backend.service.bots.container_runtime"]
            and restart in ("", "no") and _clean_exit(state, range(256)))


def _service_contract(row, service, revision, source_tree_hash, state, stopped):
    config, mounts = row["config"], row["mounts"]
    restart = row["host"].get("RestartPolicy", {}).get("Name")
    if restart not in ("no", "unless-stopped"):
        raise RuntimeError(f"canonical_host_unexpected_restart_policy: service={service}")
    if service == "initialize" or service in stopped or (service in _OPTIONAL_TERMINAL_SERVICES and not state["running"]):
        if not _clean_exit(state, (0,) if service == "initialize" else (0, 143)):
            raise RuntimeError(f"canonical_host_service_not_cleanly_stopped: service={service}")
    elif (not state["running"] or state["status"] != "running" or state["pid"] <= 0
            or state["exit_code"] != 0 or state["health"] not in (None, "healthy")
            or service in _HEALTHCHECK_SERVICES and state["health"] != "healthy"):
        raise RuntimeError(f"canonical_host_required_service_not_healthy: service={service}")
    if len(mounts) > 64 or any(not isinstance(mount, dict) for mount in mounts):
        raise RuntimeError("canonical_host_invalid_mount_inventory")
    if service in _RELEASE_SERVICES:
        entries = config.get("Env") or []
        try:
            environment = dict(entry.split("=", 1) for entry in entries)
        except (ValueError, AttributeError):
            raise RuntimeError("canonical_host_invalid_environment") from None
        if (len(environment) != len(entries) or environment.get("QT_IMAGE_SOURCE_REVISION") != revision
                or environment.get("QT_IMAGE_SOURCE_TREE_HASH") != source_tree_hash):
            raise RuntimeError(f"canonical_host_application_revision_changed: service={service}")
    if service in _APP_COMMANDS:
        if _command(config) != ["python", "-m", _APP_COMMANDS[service]]:
            raise RuntimeError(f"canonical_host_application_command_changed: service={service}")
        archive_root = "/qt-history/archives"
        working_root = "/app/logs/market-structure"
        expected_working = archive_root if service == "storage-maintenance" else working_root
        by_destination = {item.get("Destination"): item for item in mounts}
        required = {"/qt-history"} | ({working_root} if service != "storage-maintenance" else set())
        if (len(by_destination) != len(mounts) or not required <= by_destination.keys()
                or any(by_destination[key].get("Type") != "bind" or by_destination[key].get("RW") is not True
                       for key in required)
                or environment.get("MARKET_STRUCTURE_STORAGE_ROOT") != archive_root
                or environment.get("MARKET_STRUCTURE_WORKING_ROOT") != expected_working
                or len({by_destination[key].get("Source") for key in required}) != len(required)
                or any(not isinstance(key, str) or key.startswith(("/app/src", "/app/portal", "/app/scripts"))
                       for key in by_destination)):
            raise RuntimeError(f"canonical_host_application_mounts_changed: service={service}")
    return {"id": row["id"], "image": row["image"], "restart": restart, **state,
            "configuration_sha256": host.database_contract(row),
            "mounts_sha256": host.digest(sorted(mounts, key=lambda value: json.dumps(value, sort_keys=True))),
            "networks_sha256": host.digest(_networks(row))}


def _networks(row):
    # Stopped containers can omit their endpoint ID, while the Compose network
    # still exists. The actual network ID is independently bound below.
    return {name: {key: value for key, value in details.items() if key != "network_id"}
            for name, details in host.database_networks(row).items()}


def _snapshot(project, revision, source_tree_hash, stopped, worker_id):
    ids = host._inventory_ids(project, None)
    rows = host._inspection_rows(host.docker("inspect", "--format", _DETAILS, *sorted(ids)), ids)
    services, bots, wired = {}, {}, {}
    for row in rows:
        if not _matches(_IMAGE, row.get("image")):
            raise RuntimeError("canonical_host_container_image_invalid")
        labels = row["config"].get("Labels") or {}
        service = labels.get("com.docker.compose.service")
        state = _state(row)
        if row["id"] == worker_id:
            if not _clean_exit(state) or row["host"].get("RestartPolicy", {}).get("Name") not in ("", "no"):
                raise RuntimeError("canonical_host_previous_worker_not_terminal")
            continue
        if (labels.get("com.docker.compose.project") != project or service not in SERVICE_ROSTER
                or labels.get("com.docker.compose.oneoff") != "False" or service in services):
            if not _bot_terminal(row, labels, state):
                raise RuntimeError("canonical_host_unknown_container_or_writer")
            bots[row["id"]] = host.digest({"id": row["id"], "image": row["image"], "state": state})
            continue
        services[service] = _service_contract(row, service, revision, source_tree_hash, state, stopped)
        wired[service] = host.database_networks(row)
    if set(services) != set(SERVICE_ROSTER):
        raise RuntimeError("canonical_host_required_service_missing")
    network = f"{project}_quanttrad"
    network_id = host.docker("network", "inspect", "--format", "{{.Id}}", network).strip()
    if not _matches(_ID, network_id):
        raise RuntimeError("canonical_host_serving_network_changed")
    for name, endpoints in wired.items():
        attached = endpoints.get(network, {}).get("network_id")
        if (set(endpoints) != {network} or attached not in (network_id, "")
                or services[name]["running"] and attached != network_id):
            raise RuntimeError("canonical_host_serving_network_changed")
    return services, bots, network_id


def _inspect(state_root, *, project, expected_source_revision, stopped, timeout_seconds):
    if project != PROJECT or not _matches(_REVISION, expected_source_revision):
        raise ValueError("canonical_host_project_or_release_invalid")
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("canonical_host_timeout_invalid")
    root = Path(state_root)
    if not root.is_absolute() or root == Path(root.anchor) or root.resolve(strict=True) != root:
        raise ValueError("canonical_host_state_root_invalid")
    ownership = root.stat()
    if not stat.S_ISDIR(ownership.st_mode) or ownership.st_uid != os.getuid() or ownership.st_mode & 0o022:
        raise RuntimeError("canonical_host_state_directory_not_owned")
    chain, worker_id = _terminal_chain(root, project, expected_source_revision)
    with host.docker_deadline(time.monotonic() + timeout_seconds):
        first = _snapshot(project, expected_source_revision, chain["source_tree_hash"], stopped, worker_id)
        # Check peers outside the Compose project and network for writable
        # storage overlap or shared collector/database namespaces as well.
        prior._admit_mount_writers(first[0], operator_id=first[0]["backend"]["id"])
        if _snapshot(project, expected_source_revision, chain["source_tree_hash"], stopped, worker_id) != first:
            raise RuntimeError("canonical_host_container_inventory_changed")
    if _terminal_chain(root, project, expected_source_revision) != (chain, worker_id):
        raise RuntimeError("canonical_host_previous_release_changed")
    services, bots, network_id = first
    return {"schema": SCHEMA, "project": project, "source_revision": expected_source_revision,
            "network_id": network_id, "terminal_chain": chain, "services": services,
            "exited_bot_count": len(bots), "exited_bots_sha256": host.digest(bots)}


def inspect_host_inventory(state_root, *, project, expected_source_revision, expected_container_ids=None,
                           expected_image_ids=None, timeout_seconds=30):
    """Observe serving state without stopping, starting or rewriting anything."""
    observed = _inspect(state_root, project=project, expected_source_revision=expected_source_revision,
                        stopped=frozenset(), timeout_seconds=timeout_seconds)
    for field, expected in (("id", expected_container_ids), ("image", expected_image_ids)):
        if expected is not None and {name: row[field] for name, row in observed["services"].items()} != expected:
            raise RuntimeError("canonical_host_approved_container_identity_changed")
    return observed


def assert_host_inventory(state_root, *, expected, stopped_services=(), timeout_seconds=30):
    """Allow only the caller's exact requested clean stop subset on the same fleet."""
    stopped = frozenset(stopped_services)
    if expected.get("schema") != SCHEMA or not stopped <= STOPPABLE_SERVICES:
        raise ValueError("canonical_host_invalid_expected_inventory")
    actual = _inspect(state_root, project=expected["project"], expected_source_revision=expected["source_revision"],
                      stopped=stopped, timeout_seconds=timeout_seconds)

    def stable(value):
        return {**value, "services": {name: {key: item for key, item in row.items() if key not in _STATE_KEYS}
                                       for name, row in value["services"].items()}}

    if stable(actual) != stable(expected):
        raise RuntimeError("canonical_host_approved_inventory_changed")
    if any(not expected["services"][name]["running"] for name in stopped):
        raise ValueError("canonical_host_unexpected_stop_subset")
    if any(actual["services"][name]["running"] is not (row["running"] and name not in stopped)
           for name, row in expected["services"].items()):
        raise RuntimeError("canonical_host_approved_process_state_changed")
    return actual

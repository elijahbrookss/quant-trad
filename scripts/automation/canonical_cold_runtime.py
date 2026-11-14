"""Recreate only the approved runtime services after the canonical SQL switch."""

from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path, PurePosixPath
import re
from subprocess import TimeoutExpired
import stat
from time import monotonic, sleep

from scripts.automation import canonical_cold_host_inventory as inventory
from scripts.automation import storage_host_boundary as host
from scripts.automation.storage_online_release import _publish_file, _release_values, read_private_environment
from scripts.automation.storage_handoff_pause import _runtime_candidate_details
from scripts.automation.storage_online_runtime import admit_candidate_privileges


REPLACE = ("backend", "initialize", "market-data-collector", "storage-maintenance",
           "frontend", "frontend-v2", "docker-stats")
_CODE_ENV = {"SOURCE_REVISION", "SOURCE_TREE_HASH", "QT_BOT_RUNTIME_IMAGE"}
_IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
_REVISION = re.compile(r"[0-9a-f]{40}")
_HASH = re.compile(r"[0-9a-f]{64}")


def _environment(items):
    fields = dict(item.split("=", 1) for item in items)
    if len(fields) != len(items):
        raise RuntimeError("canonical_cold_runtime_duplicate_environment")
    return fields


def _runtime_binds(model):
    bindings = {}
    for service in model["services"].values():
        for mount in service.get("volumes", []):
            if mount["type"] != "bind":
                continue
            path = Path(mount["source"])
            bindings[str(path)] = _bind_identity(path)
    return bindings


def _bind_identity(path):
    resolved = Path(path).resolve(strict=True)
    info = resolved.stat()
    identity = [str(resolved), info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_mode]
    if stat.S_ISREG(info.st_mode):
        identity.extend((info.st_size, info.st_mtime_ns, info.st_ctime_ns))
    return identity


def _compose_command(model):
    # An empty implicit env file avoids accidental interpolation of a checkout's
    # secrets. The reviewed resolved model is supplied on stdin.
    return ["compose", "--env-file", "/dev/null", "--project-name", model["name"], "--file", "-"]


def _escaped_model(model):
    if isinstance(model, str):
        return model.replace("$", "$$")
    if isinstance(model, list):
        return [_escaped_model(item) for item in model]
    if isinstance(model, dict):
        return {key: _escaped_model(item) for key, item in model.items()}
    return model


def _compose_hashes(model):
    result = {}
    for line in host.docker(*_compose_command(model), "config", "--hash", "*",
                            input=json.dumps(model)).splitlines():
        name, digest = line.split()
        if name in result or not _HASH.fullmatch(digest):
            raise RuntimeError("canonical_cold_runtime_invalid_compose_hash")
        result[name] = digest
    if result.keys() != model["services"].keys():
        raise RuntimeError("canonical_cold_runtime_compose_hash_scope")
    return result


def _require_allowed_recipe(service):
    if (any(key in service for key in (
            "build", "env_file", "extends", "profiles", "secrets", "configs", "volumes_from", "network_mode",
            "devices", "device_cgroup_rules", "cap_add", "sysctls", "uts", "userns_mode", "deploy",
        )) or service.get("privileged") or set(service.get("networks", {})) != {"quanttrad"}):
        raise RuntimeError("canonical_cold_runtime_unsupported_recipe")


def prepare_runtime(model, *, source):
    """Read-only admission of the exact resolved replacement configuration."""
    if (not isinstance(model, dict) or set(model) != {"name", "services", "networks", "volumes"}
            or model["name"] != source["project"] or set(model["services"]) != set(REPLACE)
            or model["networks"] != {"quanttrad": {"name": model["name"] + "_quanttrad", "external": True}}):
        raise ValueError("canonical_cold_runtime_recipe_scope")
    model = deepcopy(model)
    all_images, code = {}, {}
    source_mounts = set()
    for name in REPLACE:
        service = model["services"][name]
        _require_allowed_recipe(service)
        image_id = service.get("image", "")
        old = host.database_details(source["services"][name]["id"])
        if (not _IMAGE.fullmatch(image_id) or service.get("pull_policy") != "never"
                or old["image"] != source["services"][name]["image"]
                or host.database_contract(old) != source["services"][name]["configuration_sha256"]):
            raise RuntimeError(f"canonical_cold_runtime_recipe_unqualified: {name}")
        candidate = json.loads(host.docker("image", "inspect", "--format", "{{json .}}", image_id))
        if candidate["Id"] != image_id:
            raise RuntimeError("canonical_cold_runtime_candidate_image_changed")
        candidate_env = _environment(candidate["Config"].get("Env") or [])
        revision = candidate_env.get("QT_IMAGE_SOURCE_REVISION", "")
        tree = candidate_env.get("QT_IMAGE_SOURCE_TREE_HASH", "")
        if not _REVISION.fullmatch(revision) or not _HASH.fullmatch(tree) or revision == source["source_revision"]:
            raise RuntimeError("canonical_cold_runtime_image_attestation")
        code[name] = (revision, tree)
        all_images[name] = image_id

        expected_env = dict(candidate_env)
        for key, value in service.get("environment", {}).items():
            if value is None:
                raise RuntimeError(f"canonical_cold_runtime_environment_not_resolved: {name}")
            expected_env[key] = str(value)
        old_env = _environment(old["config"].get("Env") or [])
        if (expected_env.get("QT_IMAGE_SOURCE_REVISION"), expected_env.get("QT_IMAGE_SOURCE_TREE_HASH")) != code[name]:
            raise RuntimeError("canonical_cold_runtime_environment_masks_attestation")
        if name in inventory._APP_COMMANDS:
            controlled = {"QT_IMAGE_SOURCE_REVISION", "QT_IMAGE_SOURCE_TREE_HASH", *_CODE_ENV}
            if ({key: value for key, value in expected_env.items() if key not in controlled}
                    != {key: value for key, value in old_env.items() if key not in controlled}):
                raise RuntimeError(f"canonical_cold_runtime_application_environment_changed: {name}")
            for key, value in {
                "SOURCE_REVISION": revision, "SOURCE_TREE_HASH": tree, "QT_BOT_RUNTIME_IMAGE": image_id,
            }.items():
                references = (value, "quanttrad-backend:" + revision) if key == "QT_BOT_RUNTIME_IMAGE" else (value,)
                if key in expected_env and expected_env[key] not in references:
                    raise RuntimeError(f"canonical_cold_runtime_source_reference_changed: {name} {key}")
            if (service.get("command") != ["python", "-m", inventory._APP_COMMANDS[name]]
                    or service.get("entrypoint") is not None
                    or service.get("restart") != ("no" if name == "initialize" else old["host"]["RestartPolicy"]["Name"])):
                raise RuntimeError(f"canonical_cold_runtime_application_command_changed: {name}")

        mounted = {}
        for mount in service.get("volumes", []):
            kind, target = mount["type"], mount["target"]
            if target in mounted or kind not in ("bind", "volume"):
                raise RuntimeError("canonical_cold_runtime_mount_scope")
            location = mount["source"]
            if kind == "volume":
                definition = model["volumes"][location]
                if definition.get("external") is not True:
                    raise RuntimeError("canonical_cold_runtime_requires_existing_volume")
                location = definition["name"]
            else:
                if not isinstance(location, str) or not Path(location).is_absolute() or not Path(location).exists():
                    raise RuntimeError("canonical_cold_runtime_requires_existing_mount")
                mount.setdefault("bind", {})["create_host_path"] = False
            mounted[target] = (kind, location, not mount.get("read_only", False))
        old_mounts = {
            mount["Destination"]: (mount["Type"], mount.get("Name") if mount["Type"] == "volume" else mount["Source"], mount["RW"])
            for mount in old["mounts"]
        }
        if mounted != old_mounts:
            raise RuntimeError(f"canonical_cold_runtime_mounts_changed: {name}")
        if name == "market-data-collector":
            source_mounts.update(mounted[target][1] for target in ("/qt-history", "/app/logs/market-structure"))
        if name == "storage-maintenance":
            database_id = source["services"]["tsdb"]["id"]
            if service.get("pid") not in ("service:tsdb", "container:" + database_id):
                raise RuntimeError("canonical_cold_runtime_database_namespace_changed")
            service["pid"] = "container:" + database_id
        elif service.get("pid"):
            raise RuntimeError("canonical_cold_runtime_unexpected_pid_namespace")
    if len(set(code.values())) != 1 or not source_mounts or any(not PurePosixPath(p).is_absolute() for p in source_mounts):
        raise RuntimeError("canonical_cold_runtime_inconsistent_release")
    revision, tree = next(iter(code.values()))
    wire_model = _escaped_model(model)
    return wire_model, {
        "images": all_images, "revision": revision, "source_tree_hash": tree,
        "compose_hashes": _compose_hashes(wire_model), "protected_mounts": sorted(source_mounts),
        "wire_model_hash": host.digest(wire_model),
        "bind_identity": _runtime_binds(model),
    }


def _is_started(row, name):
    if name == "initialize":
        return inventory._clean_exit(row, (0,))
    required_health = ("healthy",) if name in inventory._HEALTHCHECK_SERVICES else ("healthy", None)
    return row["running"] and row["health"] in required_health


def _candidate_observation(model, admission, source, name, row):
    observation = {
        "admission": admission, "compose_hashes": admission["compose_hashes"],
        "binding": {"database_id": source["services"]["tsdb"]["id"]},
        "receipt": {"project": source["project"], "database_preparation": {
            "networks": {source["project"] + "_quanttrad": {"network_id": source["network_id"]}},
        }},
    }
    details = _runtime_candidate_details(name, row, model, observation)
    if name in inventory._APP_COMMANDS:
        admit_candidate_privileges(details, model["services"][name])


def restart_runtime(state_root, *, source, model, admission, stopped_services, remaining, record, verify_ready):
    """Run the exact prepared candidate recipe while the host owner remains held.

    The caller verifies persisted SQL and current recovery/acceptance first.
    Every ambiguity retains the action journal for read-only reconciliation.
    """
    stopped = set(stopped_services)
    candidate_ids = {}
    removed = set()
    actions = {}
    completed = []
    stable_source = inventory.stable_host_inventory(source)
    replacing = model is not None
    if replacing != (admission is not None) or not stopped <= inventory.STOPPABLE_SERVICES:
        raise ValueError("canonical_cold_runtime_invalid_restart_scope")
    if replacing and host.digest(model) != admission["wire_model_hash"]:
        raise RuntimeError("canonical_cold_runtime_prepared_recipe_changed")
    revision = (admission["revision"], admission["source_tree_hash"]) if replacing else None

    def observe():
        if replacing:
            for supplied, expected in admission["bind_identity"].items():
                if _bind_identity(supplied) != expected:
                    raise RuntimeError("canonical_cold_runtime_bind_identity_changed")
        actual = inventory._inspect(
            state_root, project=source["project"], expected_source_revision=source["source_revision"],
            stopped=frozenset(stopped), timeout_seconds=max(1, min(30, int(remaining()))),
            missing=frozenset(removed), actions=actions,
            ready_to_start=frozenset(candidate_ids.keys() - actions.keys() - started),
            retained_protection=admission["protected_mounts"] if replacing else None,
            replacements={name: revision for name in removed},
        )
        stable = inventory.stable_host_inventory(actual)
        if any(stable[key] != value for key, value in stable_source.items() if key != "services"):
            raise RuntimeError("canonical_cold_runtime_source_binding_changed")
        for name, row in actual["services"].items():
            if name not in removed:
                if stable["services"][name] != stable_source["services"][name]:
                    raise RuntimeError(f"canonical_cold_runtime_preserved_service_changed: {name}")
                if name not in actions and row["running"] is not (source["services"][name]["running"] and name not in stopped):
                    raise RuntimeError(f"canonical_cold_runtime_preserved_service_state_changed: {name}")
            elif name not in actions and name not in candidate_ids:
                raise RuntimeError(f"canonical_cold_runtime_unowned_candidate: {name}")
            if name in removed:
                if name in candidate_ids and row["id"] != candidate_ids[name]:
                    raise RuntimeError(f"canonical_cold_runtime_candidate_id_changed: {name}")
                _candidate_observation(model, admission, source, name, row)
                if name in started and not _is_started(row, name):
                    raise RuntimeError(f"canonical_cold_runtime_candidate_lost_health: {name}")
        if not candidate_ids.keys() <= actual["services"].keys():
            raise RuntimeError("canonical_cold_runtime_candidate_missing")
        return actual["services"]

    def dispatch(action, arguments, *, recipe=None):
        record("runtime_dispatching", {"action": action, "completed": list(completed),
                                       "candidate_ids": dict(candidate_ids)})
        try:
            host.docker(*arguments, input=recipe, timeout=min(60, remaining()))
        except (TimeoutError, TimeoutExpired, OSError, RuntimeError) as error:
            # Only an independently confirmed exact result can reconcile a
            # missing Docker reply. Never repeat the potentially applied request.
            kind, service = action.split(":")
            if kind != "start":
                raise
            try:
                if _is_started(observe()[service], service):
                    return
            except (TimeoutError, TimeoutExpired, OSError, RuntimeError) as observed_error:
                error.add_note(f"Start outcome needs reconciliation ({type(observed_error).__name__}).")
            raise

    def finish(action):
        completed.append(action)
        record("runtime_action_confirmed", {"action": action, "completed": list(completed),
                                           "candidate_ids": dict(candidate_ids)})

    started = set()
    compose = _compose_command(model) if replacing else ()
    for name in REPLACE if replacing else ():
        row = observe()[name]
        if row["running"] or (source["services"][name]["running"] and name not in stopped):
            raise RuntimeError(f"canonical_cold_runtime_replacement_still_running: {name}")
        action = "remove:" + name
        dispatch(action, ["rm", row["id"]])
        removed.add(name)
        if name in observe():
            raise RuntimeError(f"canonical_cold_runtime_source_not_removed: {name}")
        finish(action)
        action = "create:" + name
        actions[name] = "create"
        dispatch(action, [*compose, "up", "--no-start", "--no-deps", "--no-recreate", "--no-build", "--pull", "never", name],
                 recipe=json.dumps(model))
        created = observe().get(name)
        if created is None:
            raise RuntimeError(f"canonical_cold_runtime_creation_unconfirmed: {name}")
        candidate_ids[name] = created["id"]
        actions.pop(name)
        finish(action)
        stopped.discard(name)
        # Hold all remaining candidates down until the ordered start phase.

    for name in (*REPLACE, "grafana", "pgadmin"):
        if name != "initialize" and not source["services"][name]["running"]:
            continue
        if not replacing and name not in stopped:
            continue
        before = observe()[name]
        action = "start:" + name
        actions[name] = "start"
        dispatch(action, ["start", before["id"]])
        while True:
            current = observe()[name]
            if _is_started(current, name):
                break
            if current["status"] == "exited":
                raise RuntimeError(f"canonical_cold_runtime_service_exited: {name}")
            sleep(min(.2, remaining()))
        actions.pop(name)
        started.add(name)
        stopped.discard(name)
        finish(action)
    final = observe()
    evidence = verify_ready(final)
    if not isinstance(evidence, dict) or evidence.get("ready") is not True:
        raise RuntimeError("canonical_cold_runtime_acceptance_not_verified")
    record("runtime_verified", {"candidate_ids": dict(candidate_ids), "acceptance": evidence})
    observe()
    return evidence


def observe_interrupted_start(state_root, *, source, model, admission, hold, timeout_seconds=30):
    """Check a journaled start after process loss using its pinned container ID.

    This observation resolves only the requested container's action. Overall
    fleet and database acceptance still belong to a live maintenance owner.
    """
    if (hold.get("phase") != "runtime_dispatching" or hold.get("inventory_hash") != host.digest(source)
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60):
        raise ValueError("canonical_cold_runtime_start_observation_scope")
    pending = hold.get("runtime_evidence", {})
    action = pending.get("action", "").split(":")
    if len(action) != 2 or action[0] != "start" or action[1] not in (*REPLACE, "grafana", "pgadmin"):
        raise ValueError("canonical_cold_runtime_start_not_journaled")
    name = action[1]
    candidate = pending.get("candidate_ids", {}).get(name)
    identity = candidate or source["services"][name]["id"]
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ValueError("canonical_cold_runtime_invalid_start_identity")
    directory = Path(state_root)
    marker = directory / "canonical-cold-hold.json"
    if host.load_receipt(marker) != hold:
        raise RuntimeError("canonical_cold_runtime_start_receipt_changed")
    network = source["project"] + "_quanttrad"
    with host.docker_deadline(monotonic() + timeout_seconds):
        details, networks = host.preserved_client_observations([identity], [network])
        if networks != {network: source["network_id"]}:
            raise RuntimeError("canonical_cold_runtime_start_observed_network_changed")
        current = details[identity]
        state = inventory._state(current)
        if candidate is None:
            old = source["services"][name]
            mounts = sorted(current["mounts"], key=lambda value: json.dumps(value, sort_keys=True))
            if (current["image"] != old["image"] or host.database_contract(current) != old["configuration_sha256"]
                    or host.digest(mounts) != old["mounts_sha256"]
                    or host.digest(inventory._networks(current)) != old["networks_sha256"]):
                raise RuntimeError("canonical_cold_runtime_start_observed_binding_changed")
        else:
            if model is None or admission is None or admission["wire_model_hash"] != host.digest(model):
                raise RuntimeError("canonical_cold_runtime_start_observed_recipe_changed")
            _candidate_observation(model, admission, source, name, {"id": identity, "image": current["image"]})
    if host.load_receipt(marker) != hold:
        raise RuntimeError("canonical_cold_runtime_start_receipt_changed")
    status = "started" if _is_started(state, name) else "starting" if state["running"] else "unresolved"
    return {"service": name, "container_id": identity, "status": status, "container": state}


def _record_serving_release(state_root, *, source, admission, observed_at, record):
    """Only publish a cold-capable release after its full runtime is verified."""
    path = Path(state_root) / "release.env"
    old = read_private_environment(path)
    fields = _release_values(old)
    if (sha256(old).hexdigest() != source["terminal_chain"]["release_sha256"]
            or fields.get("current_revision") != source["source_revision"]
            or fields.get("storage_layout") != "ssd-hdd-v1"):
        raise RuntimeError("canonical_cold_runtime_release_record_changed")
    new_fields = {
        "current_revision": admission["revision"], "current_source_tree_hash": admission["source_tree_hash"],
        "previous_revision": "", "deployed_at": observed_at,
        "storage_layout": fields["storage_layout"],
    }
    new = "".join(f"{key}={value}\n" for key, value in new_fields.items()).encode()
    hashes = {"old_sha256": sha256(old).hexdigest(), "new_sha256": sha256(new).hexdigest()}
    record("runtime_recording_release", hashes | {"release": new_fields})
    if read_private_environment(path) != old:
        raise RuntimeError("canonical_cold_runtime_release_record_changed")
    _publish_file(path, before=old, after=new)
    if read_private_environment(path) != new:
        raise RuntimeError("canonical_cold_runtime_release_record_unconfirmed")
    record("runtime_release_recorded", hashes)

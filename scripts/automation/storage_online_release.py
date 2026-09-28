"""Private deployment configuration for the completed online migration.

The existing operation owns admission and the existing deployer owns release
publication. This module prepares their exact environment handoff; it never
changes the active environment, release metadata, receipts or running services.
"""
from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
import re
import stat

from dotenv.parser import parse_stream

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_handoff_pause as preserving
from scripts.automation import storage_online_runtime as runtime

SOURCE_ENVIRONMENT = "storage-online-source.env"
PREPARED_ENVIRONMENT = "storage-online-deployment.env"


def read_private_environment(path):
    raw = preserving._runtime_configuration_bytes(path)
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise RuntimeError("storage_online_deployment_environment_not_private")
    return raw


def _environment_bytes(raw, bindings):
    """Preserve unrelated definitions/comments verbatim, including credential bytes.

    Use the existing dotenv parser rather than guessing at multiline quoting.
    Duplicate or malformed input refuses: Compose and application precedence must
    not disagree. New host bindings use the deployer's existing unquoted literal format.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeError:
        raise RuntimeError("storage_online_deployment_environment_encoding") from None
    if "\x00" in text:
        raise RuntimeError("storage_online_deployment_environment_invalid")
    seen = set()
    retained = []
    for item in parse_stream(io.StringIO(text)):
        if item.error or item.key is not None and item.key in seen:
            raise RuntimeError("storage_online_deployment_environment_ambiguous")
        if item.key is not None:
            seen.add(item.key)
        if item.key not in bindings and item.key != "QT_STORAGE_SOURCE_FENCE_ROOT":
            retained.append(item.original.string)
    prefix = "".join(retained)
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    for key, value in sorted(bindings.items()):
        if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_./:@+-]+", value)
                or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key)):
            raise RuntimeError("storage_online_deployment_binding_invalid")
        prefix += key + "=" + value + "\n"
    result = prefix.encode()
    if len(result) > 128*1024:
        raise RuntimeError("storage_online_deployment_environment_too_large")
    return result


def _preserve_file(path, raw):
    """Create a private artifact once; interruption or changed bytes never overwrite it."""
    if os.path.lexists(path):
        if read_private_environment(path) != raw:
            raise RuntimeError("storage_online_deployment_artifact_changed")
        return
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    host.sync_directory(path.parent)


def prepare_deployment_environment(state_root, *, environment_path, saved, execute=False):
    """Stage exact resource bindings after fresh completed-runtime/pair admission.

    The caller holds the deployment lock and has freshly verified completion.
    Files are proposals only: normal deployment remains blocked by the retained
    final receipt until canonical configuration and release publication qualify.
    """
    if type(execute) is not bool or saved.get("phase") != "recovery_runtime_ready":
        raise RuntimeError("storage_online_deployment_completion_required")
    state_root = Path(state_root)
    source = Path(environment_path)
    if source in {state_root/SOURCE_ENVIRONMENT, state_root/PREPARED_ENVIRONMENT}:
        raise RuntimeError("storage_online_deployment_environment_alias")
    before = read_private_environment(source)
    model = host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288)
    if host.digest(model) != saved["runtime"]["admission"]["recipe_sha256"]:
        raise RuntimeError("storage_online_deployment_recipe_changed")
    services = model["services"]
    db = services["tsdb"]
    backend = services["backend"]
    maintenance = services["storage-maintenance"]
    literal = lambda value: value.replace("$$", "$")
    def mount(service, target):
        matches = [m["source"] for m in service["volumes"] if m["target"] == target]
        if len(matches) != 1:
            raise RuntimeError("storage_online_deployment_mount_missing")
        return literal(matches[0])
    history = mount(db, "/qt-history")
    environment = backend["environment"]
    group = str(environment["QT_ARCHIVE_SHARED_GROUP_ID"])
    sockets = [str(g) for g in backend["group_add"] if str(g) != group]
    if (len(sockets) != 1 or not sockets[0].isdecimal()
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", db["image"])):
        raise RuntimeError("storage_online_deployment_identity_binding_invalid")
    bindings = {
        "QT_COMPOSE_PROJECT_NAME": saved["binding"]["project"],
        "QT_SINGLE_NODE_STATE_ROOT": str(state_root),
        "QT_STORAGE_HDD_ROOT": history,
        "QT_MARKET_DATA_ROOT": history + "/archives",
        "QT_MARKET_DATA_WORKING_ROOT": saved["runtime_spool"]["destination"],
        "QT_MARKET_DATA_EXPECTED_UUID": environment["QT_MARKET_DATA_EXPECTED_UUID"],
        "QT_MARKET_DATA_WORKING_EXPECTED_UUID": environment["QT_MARKET_DATA_WORKING_EXPECTED_UUID"],
        "QT_STORAGE_INVENTORY_HOST_PATH": mount(backend, "/run/quanttrad/storage-inventory.json"),
        "QT_STORAGE_RECOVERY_SECRETS_ROOT": mount(db, "/run/quanttrad/recovery"),
        "QT_STORAGE_MAINTENANCE_LIMITS_HOST_PATH": mount(maintenance, "/run/quanttrad/storage-maintenance.json"),
        "QT_ARCHIVE_SHARED_GROUP_ID": group,
        "QT_DOCKER_SOCKET_GID": sockets[0],
        "QT_STORAGE_DATABASE_IMAGE": db["image"],
        "QT_STORAGE_POSTGRES_VOLUME": model["volumes"]["postgres-data"]["name"],
        "QT_STORAGE_RECOVERY_SOCKET_VOLUME": model["volumes"]["storage-recovery-socket"]["name"],
        "QT_STORAGE_NETWORK": model["networks"]["quanttrad"]["name"],
    }
    prepared = _environment_bytes(before, bindings)
    def check():
        if (read_private_environment(source) != before
                or host.load_receipt(state_root/"storage-online-final.json") != saved
                or host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288) != model):
            raise RuntimeError("storage_online_deployment_input_changed")
    check()
    if execute:
        _preserve_file(state_root/SOURCE_ENVIRONMENT, before)
        _preserve_file(state_root/PREPARED_ENVIRONMENT, prepared)
        check()
    return dict(source_environment_sha256=hashlib.sha256(before).hexdigest(),
        prepared_environment_sha256=hashlib.sha256(prepared).hexdigest(),
        recipe_sha256=host.digest(model), environment_prepared=execute,
        active_environment_changed=False, ordinary_relaunch_authorized=False)

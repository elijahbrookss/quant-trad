"""Internal key-free initial preparation and exact source-client resumption.

No capture, copying, ownership change, recovery activation or final switch.
The durable hold and preparation receipt remain after successful resumption.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import time

from scripts.automation import storage_host_boundary as host_boundary
from scripts.automation import storage_handoff_pause as held

STATE = "storage-online-preparation.json"
SCHEMA = "qt.storage_online_preparation.v1"
_FIELDS = {"schema", "project", "source_revision", "history_uuid", "phase",
           "started_at", "deadline", "clients", "source_roots", "recipe_sha256",
           "cluster", "hold_sha256", "database", "completed_at"}


def _mount_digest(mounts):
    if len({mount["Destination"] for mount in mounts}) != len(mounts):
        raise RuntimeError("storage_online_preparation_duplicate_mount")
    return host_boundary.digest(sorted(mounts, key=lambda mount: mount["Destination"]))


def _client_contract(details):
    return host_boundary.digest({"contract": host_boundary.database_contract(details),
                         "mounts": _mount_digest(details["mounts"])})


def _source_roots(details):
    mounts = [m for m in details["mounts"]
              if m.get("Destination") == "/app/logs/market-structure"]
    if len(mounts) != 1 or mounts[0].get("Type") != "bind" or not mounts[0].get("RW"):
        raise RuntimeError("storage_online_preparation_source_mount_required")
    root = Path(mounts[0]["Source"])
    result = {}
    for path in (root, root/"objects"):
        if not path.is_absolute() or path == Path("/") or path.resolve(strict=True) != path:
            raise RuntimeError("storage_online_preparation_source_path_changed")
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode):
            raise RuntimeError("storage_online_preparation_source_directory_required")
        result[str(path)] = [info.st_dev, info.st_ino, info.st_uid, info.st_gid,
                             stat.S_IMODE(info.st_mode)]
    return result


# The previous payload cutover deliberately retains its fenced rollback table.
# It is historical source data, not this online migration's capture namespace.
# Admit that one fixed namespace only with its logged table and attached indexes;
# an extra relation/function or any other cutover namespace still refuses.
_UNCAPTURED_QUERY = """
SELECT json_build_object(
    'archive_mode', current_setting('archive_mode'),
    'archive_command_disabled', current_setting('archive_command') IN ('','(disabled)'),
    'archive_configuration', (SELECT count(*) FROM pg_file_settings
        WHERE name IN ('archive_command','archive_library') AND setting<>''),
    'captures', (SELECT count(*) FROM pg_namespace n
        WHERE n.nspname LIKE 'qt%cutover%' AND (
            n.nspname <> 'qt_fact_storage_cutover_v1'
            OR NOT EXISTS (SELECT 1 FROM pg_class r
                WHERE r.oid=to_regclass('qt_fact_storage_cutover_v1.fact_versions')
                  AND r.relnamespace=n.oid AND r.relkind='r' AND r.relpersistence='p')
            OR EXISTS (SELECT 1 FROM pg_class c WHERE c.relnamespace=n.oid
                AND c.oid<>to_regclass('qt_fact_storage_cutover_v1.fact_versions')
                AND NOT (c.relkind='i' AND EXISTS (SELECT 1 FROM pg_index i
                    WHERE i.indexrelid=c.oid
                      AND i.indrelid=to_regclass('qt_fact_storage_cutover_v1.fact_versions'))))
            OR EXISTS (SELECT 1 FROM pg_proc p WHERE p.pronamespace=n.oid)
        )))
"""


def _before_capture(database_id):
    # Only catalog/settings lookups: no source history or archive content scan.
    host_boundary.source_storage(database_id)
    value = json.loads(host_boundary.database_query(database_id, _UNCAPTURED_QUERY))
    if value != {"archive_mode": "off", "archive_command_disabled": True,
                 "archive_configuration": 0, "captures": 0}:
        raise RuntimeError("storage_online_preparation_requires_uncaptured_key_free_source")


def _recipe(state_root, project):
    model, _ = held._database_recipe(state_root, project)
    if (set(model["volumes"]) != {"postgres-data"}
            or len(model["services"]["tsdb"]["volumes"]) != 2):
        raise RuntimeError("storage_online_preparation_key_free_recipe_required")
    return host_boundary.digest(model)


def _load(state_root):
    saved = host_boundary.load_receipt(state_root/STATE)
    if (set(saved) != _FIELDS or saved["schema"] != SCHEMA
            or saved["phase"] not in ("preparing", "resuming", "serving")
            or type(saved["started_at"]) not in (int, float)
            or type(saved["deadline"]) not in (int, float)
            or not 0 < saved["started_at"] < saved["deadline"] < 1e12
            or saved["deadline"]-saved["started_at"] != 600):
        raise RuntimeError("storage_online_preparation_receipt_invalid")
    if saved["phase"] == "serving":
        if (type(saved["completed_at"]) not in (int, float)
                or not saved["started_at"] <= saved["completed_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_preparation_completion_invalid")
    elif saved["completed_at"] is not None:
        raise RuntimeError("storage_online_preparation_completion_invalid")
    return saved


def _remaining(saved):
    remaining = int(saved["deadline"]-time.time())
    if remaining < 1:
        raise RuntimeError("storage_online_preparation_deadline_expired")
    return remaining


def _clients(rows):
    result = {}
    for name in rows:
        if name == "tsdb":
            continue
        details = host_boundary.database_details(rows[name]["id"])
        result[name] = dict(identity=host_boundary.identities(rows)[name], was_running=rows[name]["running"],
                            contract=_client_contract(details),
                            networks=host_boundary.database_networks(details))
    return result


def _admit_clients(rows, expected):
    if set(rows)-{"tsdb"} != set(expected):
        raise RuntimeError("storage_online_preparation_clients_changed")
    for name, saved in expected.items():
        admit_preserved_client(name, rows[name], saved)


def admit_preserved_client(name, row, saved, *, details=None, network_ids=None):
    """One exact original-client identity/configuration check across both resumes."""
    if (name == "initialize" and not saved["was_running"]
            and (row["running"] or row["status"] != "exited" or row["exit_code"] != 0)):
        raise RuntimeError("storage_online_preparation_completed_initializer_changed")
    if details is None:
        details = host_boundary.database_details(row["id"])
    if (details["id"] != row["id"] or host_boundary.identities({name:row})[name] != saved["identity"]
            or _client_contract(details) != saved["contract"]
            or not (host_boundary.same_database_networks(details, saved["networks"]) if network_ids is None else
                    host_boundary.same_database_networks(details, saved["networks"], network_ids=network_ids))):
        raise RuntimeError("storage_online_preparation_clients_changed")
    return details


def _admit_source(state_root, saved, *, require_running, operator_id=None, maintenance=False):
    if _recipe(state_root, saved["project"]) != saved["recipe_sha256"]:
        raise RuntimeError("storage_online_preparation_recipe_changed")
    release = (state_root/"release.env").read_text()
    if re.findall(r"^current_revision=(.*)$", release, flags=re.MULTILINE) != [saved["source_revision"]]:
        raise RuntimeError("storage_online_preparation_release_changed")
    receipt = host_boundary.load_receipt(state_root/host_boundary.HOLD)
    if host_boundary.digest(receipt) != saved["hold_sha256"]:
        raise RuntimeError("storage_online_preparation_hold_changed")
    preparation = receipt["database_preparation"]
    held._history_filesystem(preparation["history_root"], saved["history_uuid"])
    rows = host_boundary.inventory(saved["project"], operator_id=operator_id)
    if host_boundary.identities(rows) != receipt["containers"]:
        raise RuntimeError("storage_online_preparation_clients_changed")
    _admit_clients(rows, saved["clients"])
    database = host_boundary.database_details(rows["tsdb"]["id"])
    checks = dict(mounts=_mount_digest(database["mounts"]) == saved["database"]["mounts"],
                  contract=host_boundary.database_contract(database) == saved["database"]["contract"],
                  networks=host_boundary.same_database_networks(database, saved["database"]["networks"]),
                  cluster=host_boundary.cluster_identifier(rows["tsdb"]["id"], **({"maintenance": True} if maintenance else {})) == saved["cluster"])
    if not all(checks.values()):
        raise RuntimeError("storage_online_preparation_database_changed: checks="+
                           ",".join(name for name, valid in checks.items() if not valid))
    if _source_roots(host_boundary.database_details(rows["market-data-collector"]["id"])) != saved["source_roots"]:
        raise RuntimeError("storage_online_preparation_source_metadata_changed")
    if (require_running and (not host_boundary.source_clients_serving(rows)
            or any(rows[name]["running"] != saved["clients"][name]["was_running"] for name in host_boundary.STOP))):
        raise RuntimeError("storage_online_preparation_source_not_running")
    return rows



def _source_healthy(rows):
    for name in host_boundary.STOP + ("tsdb",):
        if name == "initialize" and not rows[name]["running"]:
            if rows[name]["status"] != "exited" or rows[name]["exit_code"] != 0:
                return False
            continue
        state = json.loads(host_boundary.docker("inspect", "--format", "{{json .State}}", rows[name]["id"]))
        if (not state.get("Running") or state.get("OOMKilled") or state.get("Paused")
                or state.get("Restarting")
                or state.get("Health", {}).get("Status", "healthy") != "healthy"):
            return False
    return True


def admit_serving_source(state_root, *, project, source_revision, operator_id=None):
    """Read-only admission under the caller's deployment lock, never a restart."""
    state_root = Path(state_root)
    saved = _load(state_root)
    if (saved["phase"] != "serving" or saved["project"] != project
            or saved["source_revision"] != source_revision):
        raise RuntimeError("storage_online_preparation_not_complete")
    rows = _admit_source(state_root, saved, require_running=True, operator_id=operator_id)
    if not _source_healthy(rows):
        raise RuntimeError("storage_online_preparation_source_not_healthy")
    return saved


def inspect_unprepared_source(state_root, *, project, source_revision, history_uuid):
    """Read-only admission before the initial pause; no journal or new clock."""
    release = (state_root/"release.env").read_text()
    if (re.findall(r"^current_revision=(.*)$", release, flags=re.MULTILINE) != [source_revision]
            or re.search(r"^storage_layout=.", release, flags=re.MULTILINE)):
        raise RuntimeError("storage_online_preparation_source_release_required")
    rows = host_boundary.inventory(project)
    if not host_boundary.source_clients_serving(rows):
        raise RuntimeError("storage_online_preparation_source_not_running")
    _before_capture(rows["tsdb"]["id"])
    recipe = _recipe(state_root, project)
    database_plan=held.inspect_database_preparation(state_root,project,rows,history_uuid)
    if database_plan["recipe_sha256"]!=recipe:
        raise RuntimeError("storage_online_preparation_recipe_changed")
    return rows,database_plan


def prepare_online_source_locked(state_root, *, project, source_revision, history_uuid):
    """Internal bounded initial transition; caller must qualify host admission.

    Reuses the original database preparation and resumes only its exact stopped
    source containers. No production CLI or inference of final switch readiness.
    """
    state_root = Path(state_root)
    if (not state_root.is_absolute() or state_root == Path("/")
            or state_root.resolve(strict=True) != state_root
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project)
            or not re.fullmatch(r"[0-9a-f]{40}", source_revision)
            or not re.fullmatch(r"[A-Za-z0-9-]{4,128}", history_uuid)):
        raise ValueError("storage_online_preparation_invalid_binding")
    for name in ("promotion.env", "alert-preview.env",
                 "storage-online-worker.json", "storage-online-request.json", "storage-online-final.json"):
        if os.path.lexists(state_root/name):
            raise RuntimeError("storage_online_preparation_conflicting_operation")
    saved = _load(state_root) if os.path.lexists(state_root/STATE) else None
    if saved is None:
        if os.path.lexists(state_root/host_boundary.HOLD):
            raise RuntimeError("storage_online_preparation_unowned_hold")
        rows,database_plan=inspect_unprepared_source(state_root,project=project,
            source_revision=source_revision,history_uuid=history_uuid)
        recipe=database_plan["recipe_sha256"]
        started = time.time()
        saved = dict(schema=SCHEMA, project=project, source_revision=source_revision,
            history_uuid=history_uuid, phase="preparing", started_at=started,
            deadline=started+600, clients=_clients(rows),
            source_roots=_source_roots(host_boundary.database_details(rows["market-data-collector"]["id"])),
            recipe_sha256=recipe, cluster=host_boundary.cluster_identifier(rows["tsdb"]["id"]),
            hold_sha256=None, database=None, completed_at=None)
        # Intent is durable BEFORE the first source stop/database mutation.
        host_boundary.save_receipt(state_root/STATE, saved, initial=True)
    if (saved["project"], saved["source_revision"], saved["history_uuid"]) != (project, source_revision, history_uuid):
        raise RuntimeError("storage_online_preparation_binding_changed")
    if saved["phase"] == "serving":
        return admit_serving_source(state_root, project=project, source_revision=source_revision)
    _remaining(saved)
    if _recipe(state_root, project) != saved["recipe_sha256"]:
        raise RuntimeError("storage_online_preparation_recipe_changed")
    if saved["phase"] == "preparing":
        rows = host_boundary.inventory(project, database_preparing=True)
        _admit_clients(rows, saved["clients"])
        with held._paused_storage_clients_locked(state_root, project=project,
                source_revision=source_revision, prepare_database=True,
                history_uuid=history_uuid, _preparation_deadline=saved["deadline"]) as receipt:
            _remaining(saved)
            _before_capture(receipt["containers"]["tsdb"]["id"])
            database = host_boundary.database_details(receipt["containers"]["tsdb"]["id"])
            saved.update(phase="resuming", hold_sha256=host_boundary.digest(receipt),
                database=dict(contract=host_boundary.database_contract(database),
                    mounts=_mount_digest(database["mounts"]), networks=host_boundary.database_networks(database)))
            host_boundary.save_receipt(state_root/STATE, saved, initial=False)
    for name in host_boundary.STOP:
        _remaining(saved)
        rows = _admit_source(state_root, saved, require_running=False)
        _before_capture(rows["tsdb"]["id"])
        if not saved["clients"][name]["was_running"]:
            if rows[name]["running"] or rows[name]["exit_code"] != 0:
                raise RuntimeError("storage_online_preparation_completed_initializer_changed")
        elif not rows[name]["running"]:
            host_boundary.docker("start", rows[name]["id"], timeout=min(60, _remaining(saved)))
    while True:
        _remaining(saved)
        rows = _admit_source(state_root, saved, require_running=True)
        if _source_healthy(rows):
            break
        time.sleep(min(1, _remaining(saved)))
    _remaining(saved)
    saved.update(phase="serving", completed_at=time.time())
    host_boundary.save_receipt(state_root/STATE, saved, initial=False)
    return saved
def prepare_online_source(state_root, *, project, source_revision, history_uuid):
    """Standalone internal preparation retains the same deployment lock."""
    with host_boundary.deployment_lock(Path(state_root)):
        return prepare_online_source_locked(state_root, project=project,
            source_revision=source_revision, history_uuid=history_uuid)

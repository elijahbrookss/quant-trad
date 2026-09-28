"""Fixed preserving recovery mounts after the online switch and reader retirement.

The existing final-state owner supplies a live source hold and committed result.
This module owns only database recreation, using the same private recipe rules
and host I/O. It has no CLI, source resumption, key generation or runtime start.
"""
from __future__ import annotations

from copy import deepcopy
import json
import math
from pathlib import Path
import re
import sys
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_handoff_pause as preserving
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial

RECIPE = "storage-online-recovery-database.compose.json"
_ACTIONS = ("stop", "remove", "create", "start")


def validate_recovery_journal(saved):
    value = saved["recovery"]
    if (not isinstance(value, dict)
            or set(value) != {"recipe_sha256", "original_id", "replacement_id", "started_at",
                             "deadline_monotonic", "completed", "inflight", "finished_at"}
            or any(not isinstance(value[k], str) or not re.fullmatch(r"[0-9a-f]{64}", value[k])
                   for k in ("recipe_sha256", "original_id"))
            or value["replacement_id"] is not None and (not isinstance(value["replacement_id"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", value["replacement_id"])
                or value["replacement_id"] == value["original_id"])
            or type(value["started_at"]) not in (int, float)
            or not saved["commit"]["confirmed_at"] <= value["started_at"] <= saved["deadline"]
            or type(value["deadline_monotonic"]) not in (int, float)
            or not math.isfinite(value["deadline_monotonic"])
            or not 0 < value["deadline_monotonic"] <= saved["switch"]["deadline_monotonic"]
            or not isinstance(value["completed"], list)
            or value["completed"] not in [list(_ACTIONS[:i]) for i in range(5)]
            or value["inflight"] not in (None, *_ACTIONS)
            or value["inflight"] is not None and (len(value["completed"]) == 4
                or value["inflight"] != _ACTIONS[len(value["completed"])])
            or (len(value["completed"]) >= 3) != (value["replacement_id"] is not None)):
        raise RuntimeError("storage_online_recovery_journal_invalid")
    if saved["phase"] == "recovery_database_ready":
        if (value["completed"] != list(_ACTIONS) or value["inflight"] is not None
                or type(value["finished_at"]) not in (int, float)
                or not value["started_at"] <= value["finished_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_recovery_completion_invalid")
    elif value["finished_at"] is not None:
        raise RuntimeError("storage_online_recovery_completion_invalid")


def prepare_database(state_root, *, saved, worker_process, keys_root, socket_volume,
                     max_duration_seconds, source_check):
    """Single live invocation; every failed dispatch remains unresolved, never retried."""
    if type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 600:
        raise ValueError("storage_online_recovery_duration_invalid")
    binding = saved["binding"]
    worker_id = binding["worker_id"]
    worker = host.load_receipt(state_root/launch._STATE)
    preparation = initial._load(state_root)
    if (host.digest(worker) != binding["worker_sha256"]
            or host.digest(preparation) != binding["preparation_sha256"]
            or worker["container_id"] != worker_id):
        raise RuntimeError("storage_online_recovery_binding_changed")
    deadline = min(time.monotonic()+max_duration_seconds, saved["switch"]["deadline_monotonic"])

    def retired():
        source_check()
        if time.monotonic() >= deadline or time.time() >= worker["deadline"]:
            raise RuntimeError("storage_online_recovery_deadline_expired")
        if (worker_process.args != ["docker", "start", "--attach", "--interactive", worker_id]
                or worker_process.poll() is None):
            raise RuntimeError("storage_online_recovery_attach_not_reaped")
        launch._admit(worker_id, worker["binding"], worker["contract"])
        status = json.loads(host.docker("inspect", "--format", "{{json .State}}", worker_id))
        if (any(status.get(k) is not False for k in ("Running", "Paused", "Restarting", "Dead"))
                or type(status.get("Pid")) is not int or status["Pid"] != 0
                or status.get("Status") != "exited"
                or status.get("StartedAt") != binding["worker_started_at"]):
            raise RuntimeError("storage_online_recovery_reader_not_retired")

    with host.docker_deadline(deadline):
        retired()
        rows = initial._admit_source(state_root, preparation, require_running=False,
                                     operator_id=worker_id, maintenance=True)
        if any(rows[name]["running"] for name in host.STOP):
            raise RuntimeError("storage_online_recovery_source_not_stopped")
        original = rows["tsdb"]
        details = host.database_details(original["id"])
        mounts = {m["Destination"]: m for m in details["mounts"]}
        if len(mounts) != 2 or set(mounts) != {"/var/lib/postgresql/data", "/qt-history"}:
            raise RuntimeError("storage_online_recovery_requires_key_free_hdd_database")
        base, history = preserving._database_recipe(state_root, binding["project"])
        if host.digest(base) != preparation["recipe_sha256"]:
            raise RuntimeError("storage_online_recovery_original_recipe_changed")
        model = deepcopy(base)
        service = model["services"]["tsdb"]
        service["volumes"] += [dict(type="bind", source=str(keys_root),
            target="/run/quanttrad/recovery", read_only=True, bind=dict(create_host_path=False)),
            dict(type="volume", source="storage-recovery-socket", target="/var/run/postgresql")]
        model["volumes"]["storage-recovery-socket"] = dict(name=socket_volume, external=True)
        preserving._database_recovery_mounts(service, model["volumes"], history)
        socket_info = json.loads(host.docker("volume", "inspect", "--format", "{{json .}}", socket_volume))
        if (socket_volume == mounts["/var/lib/postgresql/data"].get("Name")
                or socket_info.get("Name") != socket_volume or socket_info.get("Driver") != "local"
                or socket_info.get("Options") not in (None, {}) or socket_info.get("Scope") != "local"):
            raise RuntimeError("storage_online_recovery_socket_not_independent")
        key_stat = Path(keys_root).stat()
        key_binding = (key_stat.st_dev, key_stat.st_ino, key_stat.st_uid, key_stat.st_gid, key_stat.st_mode)
        recipe_path = state_root/RECIPE
        if recipe_path.exists() or recipe_path.is_symlink():
            raise RuntimeError("storage_online_recovery_recipe_already_exists")
        expected_gate = {**saved["login_gate"]["database"], "allow_connections": False}
        from scripts.automation.storage_online_final import _GATE_OBSERVE, _admit_mount_writers

        def gate(identity):
            observed = json.loads(host.maintenance_query(identity, _GATE_OBSERVE))
            if observed != expected_gate:
                raise RuntimeError("storage_online_recovery_gate_changed")
            other = json.loads(host.maintenance_query(identity,
                "SELECT json_build_object('backends',(SELECT count(*) FROM pg_stat_activity "
                "WHERE datname=:'target'),'prepared',(SELECT count(*) FROM pg_prepared_xacts "
                "WHERE database=:'target'))::text"))
            if other != {"backends": 0, "prepared": 0}:
                raise RuntimeError("storage_online_recovery_database_not_quiescent")

        gate(original["id"])
        _admit_mount_writers(rows, operator_id=worker_id)
        retired()
        # A different fixed file preserves the initial recipe and its original
        # 600s journal. No secret bytes are included in the final-state receipt.
        host.save_receipt(recipe_path, model, initial=True)
        saved.update(phase="recovery_preparing", recovery=dict(recipe_sha256=host.digest(model),
            original_id=original["id"], replacement_id=None, started_at=time.time(),
            deadline_monotonic=deadline, completed=[], inflight=None, finished_at=None))
        journal = saved["recovery"]
        path = state_root/"storage-online-final.json"
        host.save_receipt(path, saved, initial=False)

        def check():
            retired()
            if (host.load_receipt(path) != saved
                    or host.digest(host.load_receipt(recipe_path)) != journal["recipe_sha256"]
                    or initial._recipe(state_root, binding["project"]) != preparation["recipe_sha256"]):
                raise RuntimeError("storage_online_recovery_saved_binding_changed")
            preserving._database_recovery_mounts(service, model["volumes"], history)
            info = Path(keys_root).stat()
            if (info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_mode) != key_binding:
                raise RuntimeError("storage_online_recovery_key_directory_changed")
            current_socket = json.loads(host.docker("volume", "inspect", "--format", "{{json .}}", socket_volume))
            if current_socket != socket_info:
                raise RuntimeError("storage_online_recovery_socket_changed")
            preserving._history_filesystem(history, preparation["history_uuid"])
            current = host.inventory(binding["project"], database_preparing=True, operator_id=worker_id,
                **({"removing_database_id": original["id"]} if journal["inflight"] == "remove" else {}))
            initial._admit_clients(current, preparation["clients"])
            if any(current[n]["running"] for n in host.STOP):
                raise RuntimeError("storage_online_recovery_source_restarted")
            return current

        def dispatch(action, arguments):
            check()
            journal["inflight"] = action
            host.save_receipt(path, saved, initial=False)
            print(f"event=storage_online_recovery_dispatch action={action} intent_retained=true",
                  file=sys.stderr, flush=True)
            host.supervised_source_action(arguments, deadline=deadline, check=check)

        def completed(action):
            journal["completed"].append(action)
            journal["inflight"] = None
            host.save_receipt(path, saved, initial=False)
            check()

        dispatch("stop", ["stop", "--signal", "SIGTERM", "--timeout", "-1", original["id"]])
        stopped = check()["tsdb"]
        if (stopped["id"] != original["id"] or stopped["running"]
                or stopped["pid"] != 0 or stopped["exit_code"] != 0):
            raise RuntimeError("storage_online_recovery_database_unclean_stop")
        completed("stop")
        dispatch("remove", ["rm", original["id"]])
        if "tsdb" in check():
            raise RuntimeError("storage_online_recovery_original_still_present")
        completed("remove")
        dispatch("create", ["compose", "--project-name", binding["project"], "--file",
                            str(recipe_path), "create", "--no-build", "--pull", "never", "tsdb"])
        replacement = check()["tsdb"]
        candidate = host.database_details(replacement["id"])
        copied = {m["Destination"]: m for m in candidate["mounts"]}
        keys, socket = copied.get("/run/quanttrad/recovery", {}), copied.get("/var/run/postgresql", {})
        if (replacement["id"] == original["id"] or replacement["running"]
                or replacement["status"] != "created" or replacement["pid"] != 0
                or candidate["image"] != details["image"]
                or host.database_contract(candidate) != host.database_contract(details)
                or not host.same_database_networks(candidate, host.database_networks(details))
                or len(candidate["mounts"]) != 4
                or any(copied.get(k) != value for k, value in mounts.items())
                or keys.get("Type") != "bind" or keys.get("Source") != str(keys_root)
                or keys.get("RW") is not False or keys.get("Propagation") != "rprivate"
                or socket.get("Type") != "volume" or socket.get("Name") != socket_volume
                or socket.get("RW") is not True):
            raise RuntimeError("storage_online_recovery_replacement_changed")
        journal["replacement_id"] = replacement["id"]
        completed("create")
        dispatch("start", ["start", replacement["id"]])
        completed("start")
        waiting = False
        while True:
            current = check()["tsdb"]
            if current["id"] != replacement["id"] or not current["running"]:
                raise RuntimeError("storage_online_recovery_replacement_not_running")
            try:
                identity = host.cluster_identifier(replacement["id"], maintenance=True)
            except RuntimeError:
                if not waiting:
                    print("event=storage_online_recovery_readiness_waiting intent_retained=true",
                          file=sys.stderr, flush=True)
                    waiting = True
                time.sleep(min(.2, max(0, deadline-time.monotonic())))
                continue
            if identity != preparation["cluster"]:
                raise RuntimeError("storage_online_recovery_cluster_changed")
            gate(replacement["id"])
            break
        check()
        saved["phase"] = "recovery_database_ready"
        journal["finished_at"] = time.time()
        host.save_receipt(path, saved, initial=False)
        check()
        return {"database_id": replacement["id"], "database_recovery_mounts_ready": True,
                "collection_resume_authorized": False, "runtime_activation_authorized": False}

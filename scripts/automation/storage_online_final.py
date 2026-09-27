"""Internal final source-stop boundary under the existing launcher's host lock.

No switch, resumption, recovery activation or production CLI. A completed stop is
not proof of database/file drain. Intent always survives failure and completion.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import time

from scripts.automation import storage_handoff_pause as held
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial

STATE = "storage-online-final.json"
SCHEMA = "qt.storage_online_final.v1"
_FIELDS = {"schema", "phase", "binding", "started_at", "deadline", "boot_id",
           "started_boot", "deadline_boot", "duration_seconds", "paused_at"}


def _boot_id():
    value = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    if not re.fullmatch(r"[0-9a-f-]{36}", value):
        raise RuntimeError("storage_online_final_boot_identity_unavailable")
    return value


def _boot_seconds():
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def _remaining(saved):
    if saved["boot_id"] != _boot_id():
        raise RuntimeError("storage_online_final_boot_changed")
    wall, boot = time.time(), _boot_seconds()
    if wall < saved["started_at"] or boot < saved["started_boot"]:
        raise RuntimeError("storage_online_final_clock_moved_backwards")
    remaining = min(saved["deadline"]-wall, saved["deadline_boot"]-boot)
    if remaining <= 0:
        raise RuntimeError("storage_online_final_deadline_expired")
    return remaining


def _load(path):
    saved = held._load(path)
    if (set(saved) != _FIELDS or saved["schema"] != SCHEMA
            or saved["phase"] not in ("stopping", "paused")
            or type(saved["duration_seconds"]) is not int
            or not 1 <= saved["duration_seconds"] <= 96*3600
            or any(type(saved[k]) not in (int, float) or not math.isfinite(saved[k])
                   for k in ("started_at", "deadline", "started_boot", "deadline_boot"))
            or not 0 < saved["started_at"] < saved["deadline"]
            or not 0 < saved["started_boot"] < saved["deadline_boot"]
            or abs(saved["deadline"]-saved["started_at"]-saved["duration_seconds"]) > .000001
            or abs(saved["deadline_boot"]-saved["started_boot"]-saved["duration_seconds"]) > .000001):
        raise RuntimeError("storage_online_final_receipt_invalid")
    if saved["phase"] == "paused":
        if (type(saved["paused_at"]) not in (int, float)
                or not saved["started_at"] <= saved["paused_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_final_receipt_invalid")
    elif saved["paused_at"] is not None:
        raise RuntimeError("storage_online_final_receipt_invalid")
    return saved


def _observe(state_root, *, project, source_revision, controller_id, worker_id):
    preparation = initial._load(state_root)
    if (preparation["phase"] != "serving" or preparation["project"] != project
            or preparation["source_revision"] != source_revision):
        raise RuntimeError("storage_online_final_completed_preparation_required")
    worker = held._load(state_root/launch._STATE)
    if (worker["container_id"] != worker_id
            or worker["binding"]["project"] != project
            or worker["binding"]["source_revision"] != source_revision):
        raise RuntimeError("storage_online_final_worker_binding_changed")
    launch._admit(worker_id, worker["binding"], worker["contract"])
    inventory = launch._canonical(worker["binding"]["mounts"]["/run/qt-online/inventory.json"]["source"])
    if (inventory.stat().st_size > 65536
            or hashlib.sha256(inventory.read_bytes()).hexdigest() != worker["binding"]["inventory_sha256"]):
        raise RuntimeError("storage_online_final_inventory_changed")
    runtime = json.loads(held._docker("inspect", "--format", "{{json .State}}", worker_id))
    if (not runtime["Running"] or runtime["Paused"] or runtime["Restarting"]
            or runtime["OOMKilled"] or runtime["Pid"] <= 0):
        raise RuntimeError("storage_online_final_live_worker_required")
    request_path = state_root/"storage-online-request.json"
    request = held._load(request_path)
    digest = hashlib.sha256(request_path.read_bytes()).hexdigest()
    if digest != worker["binding"]["request_sha256"]:
        raise RuntimeError("storage_online_final_request_changed")
    rows = initial._admit_source(state_root, preparation, require_running=False, operator_id=worker_id)
    capture = json.loads(held._database_query(rows["tsdb"]["id"],
        "SELECT to_jsonb(c)::text FROM qt_fact_header_cutover_v2.capture c WHERE id=1"))
    seconds = capture.get("attempt_seconds", 86400)
    started = datetime.fromisoformat(capture["prepared_at"])
    if (started.tzinfo is None or type(seconds) is not int or not 1 <= seconds <= 96*3600
            or started != datetime.fromisoformat(request["expected_started_at"])
            or started.timestamp()+seconds != worker["deadline"]):
        raise RuntimeError("storage_online_final_original_capture_changed")
    allowance = request["resource_limits"]["movement_timeout_seconds"]
    if type(allowance) is not int or not 1 <= allowance <= 96*3600:
        raise RuntimeError("storage_online_final_resource_bound_invalid")
    binding = dict(project=project, source_revision=source_revision, controller_id=controller_id,
        worker_id=worker_id, worker_started_at=runtime["StartedAt"], worker_pid=runtime["Pid"],
        preparation_sha256=held._digest(preparation), worker_sha256=held._digest(worker),
        request_sha256=digest, capture=capture)
    return binding, preparation, rows, {"seconds": allowance, "capture_deadline": worker["deadline"]}


def stop_online_source_locked(state_root, *, project, source_revision, controller_id,
                              worker_id, max_duration_seconds):
    """Stop exact resumed clients under the caller-owned deployment lock.

    The caller binds controller_id from its live private pipe greeting, retains
    that same worker/lock and admits the COMPLETE final window before entry.
    No default or production pause budget is inferred here. Reentry only finishes
    the same stop under its original wall/boot ceilings; worker restart or host
    reboot refuses. The prior initial600s preparation receipt is never edited.
    """
    state_root = Path(state_root)
    if (not state_root.is_absolute() or state_root == Path("/")
            or state_root.resolve(strict=True) != state_root
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project)
            or not re.fullmatch(r"[0-9a-f]{40}", source_revision)
            or not re.fullmatch(r"[0-9a-f]{32}", controller_id)
            or not re.fullmatch(r"[0-9a-f]{64}", worker_id)
            or type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 96*3600):
        raise ValueError("storage_online_final_inputs_invalid")
    for name in ("promotion.env", "alert-preview.env"):
        if os.path.lexists(state_root/name):
            raise RuntimeError("storage_online_final_conflicting_operation")
    path = state_root/STATE
    saved = _load(path) if os.path.lexists(path) else None
    if saved is None:
        wall, boot = time.time(), _boot_seconds()
        saved = dict(schema=SCHEMA, phase="stopping", binding=None, started_at=wall,
            deadline=wall+max_duration_seconds, boot_id=_boot_id(),
            started_boot=boot, deadline_boot=boot+max_duration_seconds,
            duration_seconds=max_duration_seconds, paused_at=None)
    elif saved["duration_seconds"] != max_duration_seconds:
        raise RuntimeError("storage_online_final_duration_changed")
    with held._docker_deadline(time.monotonic()+_remaining(saved)):
        def observe():
            _remaining(saved)
            result = _observe(state_root, project=project, source_revision=source_revision,
                              controller_id=controller_id, worker_id=worker_id)
            _remaining(saved)
            if saved["binding"] is not None and result[0] != saved["binding"]:
                raise RuntimeError("storage_online_final_binding_changed")
            if (max_duration_seconds > result[3]["seconds"]
                    or saved["deadline"] > result[3]["capture_deadline"]):
                raise RuntimeError("storage_online_final_window_not_admitted")
            return result
        binding, preparation, rows, _ = observe()
        if saved["binding"] is None:
            if (not held._source_clients_serving(rows)
                    or any(rows[name]["running"] != preparation["clients"][name]["was_running"]
                           for name in held.STOP) or not initial._source_healthy(rows)):
                raise RuntimeError("storage_online_final_serving_source_required")
            saved["binding"] = binding
            held._save(path, saved, initial=True)  # BEFORE first source mutation.
            print("event=storage_online_final_stop_started resume_authorized=false",
                  file=sys.stderr, flush=True)
        if saved["phase"] == "paused":
            if any(rows[name]["running"] for name in held.STOP):
                raise RuntimeError("storage_online_final_stopped_client_restarted")
            return saved
        for name in held.STOP:
            _, _, rows, _ = observe()
            if rows[name]["running"]:
                # Infinite daemon grace avoids forced SIGKILL. The host call is
                # still capped by its original deadline. Lost reply/timeout may
                # leave a stop in flight: retain intent, never claim drained.
                held._docker("stop", "--signal", "SIGTERM", "--timeout", "-1", rows[name]["id"],
                             timeout=_remaining(saved))
        _, _, rows, _ = observe()
        if any(rows[name]["running"] for name in held.STOP):
            raise RuntimeError("storage_online_final_clients_not_stopped")
        saved.update(phase="paused", paused_at=time.time())
        _remaining(saved)
        held._save(path, saved, initial=False)
        _remaining(saved)
        print("event=storage_online_final_clients_stopped database_switch_authorized=false",
              file=sys.stderr, flush=True)
        return saved

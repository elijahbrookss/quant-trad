"""Internal final source-stop boundary under the existing launcher's host lock.

Held database switch, preserving recovery and matching application startup; no CLI.
Internal source abort requires the live SQL fence; intent survives every outcome.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import sys
import subprocess
import time

from scripts.automation import storage_host_boundary as host_boundary
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial

STATE = "storage-online-final.json"
SCHEMA = "qt.storage_online_final.v1"
_RUNTIME_PHASES = {"recovery_runtime_starting", "recovery_runtime_ready"}
_SPOOL_PHASES = {"recovery_spool_preparing", "recovery_spool_ready"} | _RUNTIME_PHASES
_REPOSITORY_PHASES = {"recovery_repository_preparing", "recovery_wal_ready"} | _SPOOL_PHASES
_RECOVERY_PHASES = {"recovery_preparing", "recovery_database_ready"} | _REPOSITORY_PHASES
_COMMIT_PHASES = {"commit_dispatching", "committed"} | _RECOVERY_PHASES
_SOURCE_HOLD = ContextVar("storage_online_source_hold", default=None)
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
    saved = host_boundary.load_receipt(path)
    recovering = saved.get("phase") in _RECOVERY_PHASES
    committing = saved.get("phase") in _COMMIT_PHASES
    gated = committing or saved.get("phase") in {"login_closing", "login_closed"} or (
        saved.get("phase") in {"source_resuming", "source_resumed"} and "login_gate" in saved)
    resuming = saved.get("phase") in {"source_resuming", "source_resumed"}
    entered = saved.get("phase") == "switch_entered" or resuming or gated
    fields = _FIELDS | ({"switch"} if entered else set()) | ({"resume"} if resuming else set()) | ({"login_gate"} if gated else set()) | ({"commit"} if committing else set()) | ({"recovery"} if recovering else set()) | ({"repositories"} if saved.get("phase") in _REPOSITORY_PHASES else set()) | ({"runtime_spool"} if saved.get("phase") in _SPOOL_PHASES else set()) | ({"runtime"} if saved.get("phase") in _RUNTIME_PHASES else set())
    if "release" in saved:
        fields = fields | {"release"}
    if (set(saved) != fields or saved["schema"] != SCHEMA
            or saved["phase"] not in {"stopping", "paused", "switch_entered", "source_resuming", "source_resumed", "login_closing", "login_closed"} | _COMMIT_PHASES
            or type(saved["duration_seconds"]) is not int
            or not 1 <= saved["duration_seconds"] <= 96*3600
            or any(type(saved[k]) not in (int, float) or not math.isfinite(saved[k])
                   for k in ("started_at", "deadline", "started_boot", "deadline_boot"))
            or not 0 < saved["started_at"] < saved["deadline"]
            or not 0 < saved["started_boot"] < saved["deadline_boot"]
            or abs(saved["deadline"]-saved["started_at"]-saved["duration_seconds"]) > .000001
            or abs(saved["deadline_boot"]-saved["started_boot"]-saved["duration_seconds"]) > .000001):
        raise RuntimeError("storage_online_final_receipt_invalid")
    if saved["phase"] in {"paused", "switch_entered", "source_resuming", "source_resumed", "login_closing", "login_closed"} | _COMMIT_PHASES:
        if (type(saved["paused_at"]) not in (int, float)
                or not saved["started_at"] <= saved["paused_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_final_receipt_invalid")
    elif saved["paused_at"] is not None:
        raise RuntimeError("storage_online_final_receipt_invalid")
    if entered:
        switch = saved["switch"]
        if (not isinstance(switch, dict)
                or set(switch) != {"entered_at", "deadline_monotonic", "worker_sequence"}
                or type(switch["entered_at"]) not in (int, float)
                or not saved["paused_at"] <= switch["entered_at"] <= saved["deadline"]
                or type(switch["deadline_monotonic"]) not in (int, float)
                or not math.isfinite(switch["deadline_monotonic"])
                or switch["deadline_monotonic"] <= 0
                or type(switch["worker_sequence"]) is not int or switch["worker_sequence"] < 1):
            raise RuntimeError("storage_online_final_switch_receipt_invalid")
    if resuming:
        resume = saved["resume"]
        if (not isinstance(resume, dict)
                or set(resume) != {"started_at", "completed", "inflight", "finished_at"} | ({"gate_restore"} if gated else set())
                or type(resume["started_at"]) not in (int, float)
                or not saved["switch"]["entered_at"] <= resume["started_at"] <= saved["deadline"]
                or not isinstance(resume["completed"], list)
                or any(not isinstance(n, str) or n not in host_boundary.STOP for n in resume["completed"])
                or len(set(resume["completed"])) != len(resume["completed"])
                or resume["completed"] != [n for n in host_boundary.STOP if n in resume["completed"]]):
            raise RuntimeError("storage_online_resume_receipt_invalid")
        if gated:
            restore = resume["gate_restore"]
            if (not isinstance(restore, dict) or set(restore) != {"inflight", "completed"}
                    or restore["inflight"] not in (None, "logins", "jobs")
                    or restore["completed"] not in ([], ["logins"], ["logins", "jobs"])
                    or (restore["inflight"] == "logins" and restore["completed"] != [])
                    or (restore["inflight"] == "jobs" and restore["completed"] != ["logins"])
                    or (resume["completed"] or resume["inflight"] or saved["phase"] == "source_resumed")
                        and restore != {"inflight": None, "completed": ["logins", "jobs"]}):
                raise RuntimeError("storage_online_gate_restore_receipt_invalid")
        action = resume["inflight"]
        if action is not None and (not isinstance(action, dict)
                or set(action) != {"service", "container_id", "requested_at"}
                or not isinstance(action["service"], str) or action["service"] not in host_boundary.STOP
                or action["service"] in resume["completed"]
                or not isinstance(action["container_id"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", action["container_id"])
                or type(action["requested_at"]) not in (int, float)
                or not resume["started_at"] <= action["requested_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_resume_receipt_invalid")
        if saved["phase"] == "source_resumed":
            if (action is not None or type(resume["finished_at"]) not in (int, float)
                    or not resume["started_at"] <= resume["finished_at"] <= saved["deadline"]):
                raise RuntimeError("storage_online_resume_receipt_invalid")
        elif resume["finished_at"] is not None:
            raise RuntimeError("storage_online_resume_receipt_invalid")
    if gated:
        gate = saved["login_gate"]
        if (not isinstance(gate, dict) or set(gate) != {"database", "requested_at", "closed_at", "database_jobs_stopped"}
                or not isinstance(gate["database"], dict)
                or not _valid_database_gate(gate["database"])
                or gate["database"]["allow_connections"] is not True
                or type(gate["requested_at"]) not in (int, float)
                or not saved["switch"]["entered_at"] <= gate["requested_at"] <= saved["deadline"]
                or gate["database_jobs_stopped"] is not (saved["phase"] != "login_closing")
                or (saved["phase"] == "login_closing" and gate["closed_at"] is not None)
                or (saved["phase"] != "login_closing" and (type(gate["closed_at"]) not in (int, float)
                    or not gate["requested_at"] <= gate["closed_at"] <= saved["deadline"]))):
            raise RuntimeError("storage_online_login_receipt_invalid")
    if committing:
        commit = saved["commit"]
        if (not isinstance(commit, dict) or set(commit)-{"confirmed_plan_id"} != {"requested_at", "worker_sequence", "source_image", "confirmed_at", "initial_policy_activated"}
                or ("confirmed_plan_id" in commit and commit["confirmed_plan_id"] is not None
                    and (not isinstance(commit["confirmed_plan_id"], str)
                         or not re.fullmatch(r"handoff-[0-9a-f]{32}", commit["confirmed_plan_id"])))
                or commit.get("initial_policy_activated") is not (saved["phase"] != "commit_dispatching")
                or type(commit["requested_at"]) not in (int, float)
                or not saved["login_gate"]["closed_at"] <= commit["requested_at"] <= saved["deadline"]
                or type(commit["worker_sequence"]) is not int
                or commit["worker_sequence"] <= saved["switch"]["worker_sequence"]
                or not isinstance(commit["source_image"], str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", commit["source_image"])
                or (saved["phase"] == "commit_dispatching" and commit["confirmed_at"] is not None)
                or (saved["phase"] != "commit_dispatching" and (type(commit["confirmed_at"]) not in (int, float)
                    or not commit["requested_at"] <= commit["confirmed_at"] <= saved["deadline"]))):
            raise RuntimeError("storage_online_commit_receipt_invalid")
    if recovering:
        from scripts.automation.storage_online_recovery import validate_recovery_journal
        validate_recovery_journal(saved)
    if saved["phase"] in _REPOSITORY_PHASES:
        from scripts.automation.storage_online_repositories import validate_journal
        validate_journal(saved)
    if saved["phase"] in _SPOOL_PHASES:
        from scripts.automation.storage_online_runtime import validate_spool_journal
        validate_spool_journal(saved)
    if saved["phase"] in _RUNTIME_PHASES:
        from scripts.automation.storage_online_runtime import validate_runtime_journal
        validate_runtime_journal(saved)
    if "release" in saved:
        from scripts.automation.storage_online_release import validate_release_journal
        validate_release_journal(saved)
    return saved


def _valid_database_gate(value):
    return (set(value) == {"cluster", "oid", "name", "allow_connections"}
        and isinstance(value["cluster"], str) and bool(re.fullmatch(r"[0-9]{1,20}", value["cluster"]))
        and type(value["oid"]) is int and value["oid"] > 0
        and isinstance(value["name"], str) and 1 <= len(value["name"].encode()) <= 63
        and "\x00" not in value["name"] and value["name"] not in {"postgres", "template0", "template1"}
        and type(value["allow_connections"]) is bool)



def _admit_mount_writers(rows, *, operator_id):
    """Reject unadmitted live Docker peers sharing source mounts or namespaces.

    This bounded observation supplements project/network admission. It does not
    exclude host processes, future daemon starts, SQL publishers or path aliases,
    and never grants switch authority. The caller retains its original deadline.
    No environment, mount path or namespace name is logged or persisted.
    """
    if host_boundary.current_docker_deadline() is None:
        raise RuntimeError("storage_online_writer_deadline_required")
    allowed = {row["id"] for row in rows.values()} | {operator_id}
    if any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
           for value in allowed):
        raise RuntimeError("storage_online_writer_binding_invalid")
    expression = ('{"id":{{json .Id}},"running":{{json .State.Running}},'
        '"paused":{{json .State.Paused}},"restarting":{{json .State.Restarting}},'
        '"pid":{{json .State.Pid}},"started":{{json .State.StartedAt}},'
        '"mounts":{{json .Mounts}},"name":{{json .Name}},'
        '"network_mode":{{json .HostConfig.NetworkMode}},"pid_mode":{{json .HostConfig.PidMode}}}')

    def snapshot():
        ids = host_boundary.docker("ps", "--all", "--quiet", "--no-trunc").split()
        if (not ids or len(ids) > 256 or len(ids) != len(set(ids))
                or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in ids)
                or not allowed <= set(ids)):
            raise RuntimeError("storage_online_writer_inventory_invalid")
        result = [json.loads(line) for line in host_boundary.docker(
            "inspect", "--format", expression, *sorted(ids)).splitlines()]
        if (len(result) != len(ids) or {row.get("id") for row in result} != set(ids)):
            raise RuntimeError("storage_online_writer_inventory_invalid")
        for row in result:
            if (set(row) != {"id", "running", "paused", "restarting", "pid", "started", "mounts",
                                     "name", "network_mode", "pid_mode"}
                    or any(type(row[k]) is not bool for k in ("running", "paused", "restarting"))
                    or type(row["pid"]) is not int or row["pid"] < 0
                    or not isinstance(row["started"], str) or not isinstance(row["mounts"], list)
                    or len(row["mounts"]) > 64
                    or not isinstance(row["name"], str)
                    or not re.fullmatch(r"/[a-zA-Z0-9][a-zA-Z0-9_.-]*", row["name"])
                    or any(not isinstance(row[k], str) or len(row[k]) > 256
                           for k in ("network_mode", "pid_mode"))):
                raise RuntimeError("storage_online_writer_inventory_invalid")
            # Docker's Mounts array is unordered across inspect calls. Preserve
            # every descriptor but compare a canonical order, not daemon order.
            row["mounts"] = sorted(row["mounts"], key=lambda value: json.dumps(value, sort_keys=True))
        return {row["id"]: row for row in result}

    def sources(row):
        result = []
        for mount in row["mounts"]:
            if (not isinstance(mount, dict) or type(mount.get("RW")) is not bool
                    or mount.get("Type") not in {"bind", "volume", "tmpfs"}):
                raise RuntimeError("storage_online_writer_mount_invalid")
            if mount["Type"] == "tmpfs":
                continue
            source = mount.get("Source")
            if (not isinstance(source, str) or not source.startswith("/")
                    or str(PurePosixPath(source)) != source or ".." in PurePosixPath(source).parts):
                raise RuntimeError("storage_online_writer_mount_invalid")
            result.append((PurePosixPath(source), mount["RW"]))
        return result

    first = snapshot()
    names = {row["name"].removeprefix("/"): identity for identity, row in first.items()}
    if len(names) != len(first):
        raise RuntimeError("storage_online_namespace_inventory_invalid")

    def namespace(identity, field):
        # Docker supports container IDs and names. Resolve chains against this
        # SAME bounded snapshot; a missing, ambiguous or cyclic alias refuses.
        seen = set()
        while identity not in seen:
            seen.add(identity)
            mode = first[identity][field]
            if not mode.startswith("container:"):
                return (field, "host" if mode == "host" else identity)
            alias = mode.removeprefix("container:")
            candidates = {key for key in first if alias and key.startswith(alias)}
            if alias.removeprefix("/") in names:
                candidates.add(names[alias.removeprefix("/")])
            if len(candidates) != 1:
                raise RuntimeError("storage_online_namespace_alias_invalid")
            identity = candidates.pop()
        raise RuntimeError("storage_online_namespace_alias_invalid")

    protected_namespaces = {namespace(rows[name]["id"], field)
                            for name in ("tsdb", "market-data-collector")
                            for field in ("network_mode", "pid_mode")}
    protected = [path for name in ("tsdb", "market-data-collector")
                 for path, _ in sources(first[rows[name]["id"]])]
    if not protected:
        raise RuntimeError("storage_online_writer_source_mounts_required")
    for identity, row in first.items():
        mounts = sources(row)
        if identity in allowed or not (row["running"] or row["paused"] or row["restarting"] or row["pid"]):
            continue
        if any(writable and (path == root or path.is_relative_to(root) or root.is_relative_to(path))
               for path, writable in mounts for root in protected):
            raise RuntimeError("storage_online_unadmitted_mount_writer")
        if any(namespace(identity, field) in protected_namespaces
               for field in ("network_mode", "pid_mode")):
            raise RuntimeError("storage_online_unadmitted_namespace_peer")
    def comparable(snapshot_rows):
        # Exact source clients may be in a caller-journaled stop/start while
        # this check supervises the CLI. Their lifecycle remains owned by the
        # existing source admission; their immutable mount descriptors must not
        # change. Worker and unadmitted peer runtime changes still refuse.
        return {identity: ({key: row[key] for key in ("id", "mounts", "name", "network_mode", "pid_mode")}
                           if identity in allowed and identity != operator_id else row)
                for identity, row in snapshot_rows.items()}
    if comparable(snapshot()) != comparable(first):
        raise RuntimeError("storage_online_writer_inventory_changed")


def _observe(state_root, *, project, source_revision, controller_id, worker_id, session=None):
    preparation = initial._load(state_root)
    if (preparation["phase"] != "serving" or preparation["project"] != project
            or preparation["source_revision"] != source_revision):
        raise RuntimeError("storage_online_final_completed_preparation_required")
    worker = host_boundary.load_receipt(state_root/launch._STATE)
    if (worker["container_id"] != worker_id
            or worker["binding"]["project"] != project
            or worker["binding"]["source_revision"] != source_revision):
        raise RuntimeError("storage_online_final_worker_binding_changed")
    launch._admit(worker_id, worker["binding"], worker["contract"])
    inventory = launch._canonical(worker["binding"]["mounts"]["/run/qt-online/inventory.json"]["source"])
    if (inventory.stat().st_size > 65536
            or hashlib.sha256(inventory.read_bytes()).hexdigest() != worker["binding"]["inventory_sha256"]):
        raise RuntimeError("storage_online_final_inventory_changed")
    runtime = json.loads(host_boundary.docker("inspect", "--format", "{{json .State}}", worker_id))
    if (not runtime["Running"] or runtime["Paused"] or runtime["Restarting"]
            or runtime["OOMKilled"] or runtime["Pid"] <= 0):
        raise RuntimeError("storage_online_final_live_worker_required")
    request_path = state_root/"storage-online-request.json"
    request = host_boundary.load_receipt(request_path)
    digest = hashlib.sha256(request_path.read_bytes()).hexdigest()
    if digest != worker["binding"]["request_sha256"]:
        raise RuntimeError("storage_online_final_request_changed")
    rows = initial._admit_source(state_root, preparation, require_running=False, operator_id=worker_id,
        **({"maintenance": True} if session is not None else {}))
    _admit_mount_writers(rows, operator_id=worker_id)
    forward_owner = None
    from scripts.automation.storage_online_forward_worker import request_binding, capture_binding
    if request_binding(request) is not None:
        from scripts.automation import storage_online_forward as forward
        # Publication and durable launch precede any authority carried by the
        # owning SQL session. Closed logins must never force a reconnect.
        forward.inspect_published_operation(state_root, request=request)
        if session is None:
            initialization, capture = forward.observe_adoption(rows["tsdb"]["id"], request=request)
        else:
            pinned = forward.load_launch(state_root, request=request)
            initialization, capture = pinned["initialization"], pinned["capture"]
            if session["capture"] != capture_binding(capture):
                raise RuntimeError("storage_forward_retained_session_proof_changed")
        forward_owner, deadline = forward.admit_launched_adoption(state_root, request, worker,
            initialization=initialization, capture=capture)
        capture = capture_binding(capture)
    else:
        capture = (session["capture"] if session is not None else json.loads(host_boundary.database_query(rows["tsdb"]["id"],
            "SELECT to_jsonb(c)::text FROM qt_fact_header_cutover_v2.capture c WHERE id=1")))
        observed = {"started_at": capture["prepared_at"], "seconds": capture.get("attempt_seconds", 86400)}
        deadline = launch.admit_capture(request, observed)
        if (deadline != worker["deadline"]
                or (launch.capture_preparation(request) is not None and worker.get("capture") != observed)):
            raise RuntimeError("storage_online_final_original_capture_changed")
    allowance = request["resource_limits"]["movement_timeout_seconds"]
    if type(allowance) is not int or not 1 <= allowance <= 96*3600:
        raise RuntimeError("storage_online_final_resource_bound_invalid")
    binding = dict(project=project, source_revision=source_revision, controller_id=controller_id,
        worker_id=worker_id, worker_started_at=runtime["StartedAt"], worker_pid=runtime["Pid"],
        preparation_sha256=host_boundary.digest(preparation), worker_sha256=host_boundary.digest(worker),
        request_sha256=digest, capture=capture)
    if forward_owner is not None:
        binding["forward"] = forward_owner
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
    if saved is not None and saved["phase"] not in {"stopping", "paused"}:
        raise RuntimeError("storage_online_final_switch_reconciliation_required")
    if saved is None:
        wall, boot = time.time(), _boot_seconds()
        saved = dict(schema=SCHEMA, phase="stopping", binding=None, started_at=wall,
            deadline=wall+max_duration_seconds, boot_id=_boot_id(),
            started_boot=boot, deadline_boot=boot+max_duration_seconds,
            duration_seconds=max_duration_seconds, paused_at=None)
    elif saved["duration_seconds"] != max_duration_seconds:
        raise RuntimeError("storage_online_final_duration_changed")
    with host_boundary.docker_deadline(time.monotonic()+_remaining(saved)):
        def observe():
            _remaining(saved)
            result = _observe(state_root, project=project, source_revision=source_revision,
                              controller_id=controller_id, worker_id=worker_id)
            _remaining(saved)
            if saved["binding"] is not None and result[0] != saved["binding"]:
                raise RuntimeError("storage_online_final_binding_changed")
            if (max_duration_seconds > result[3]["seconds"]
                    or ("forward" in result[0] and max_duration_seconds > 600)
                    or saved["deadline"] > result[3]["capture_deadline"]):
                raise RuntimeError("storage_online_final_window_not_admitted")
            return result
        binding, preparation, rows, _ = observe()
        if saved["binding"] is None:
            if (not host_boundary.source_clients_serving(rows)
                    or any(rows[name]["running"] != preparation["clients"][name]["was_running"]
                           for name in host_boundary.STOP) or not initial._source_healthy(rows)):
                raise RuntimeError("storage_online_final_serving_source_required")
            saved["binding"] = binding
            host_boundary.save_receipt(path, saved, initial=True)  # BEFORE first source mutation.
            print("event=storage_online_final_stop_started resume_authorized=false",
                  file=sys.stderr, flush=True)
        if saved["phase"] == "paused":
            if any(rows[name]["running"] for name in host_boundary.STOP):
                raise RuntimeError("storage_online_final_stopped_client_restarted")
            return saved
        for name in host_boundary.STOP:
            _, _, rows, _ = observe()
            if rows[name]["running"]:
                # Infinite daemon grace avoids forced SIGKILL. The host call is
                # still capped by its original deadline. Lost reply/timeout may
                # leave a stop in flight: retain intent, never claim drained.
                host_boundary.docker("stop", "--signal", "SIGTERM", "--timeout", "-1", rows[name]["id"],
                             timeout=_remaining(saved))
        _, _, rows, _ = observe()
        if any(rows[name]["running"] for name in host_boundary.STOP):
            raise RuntimeError("storage_online_final_clients_not_stopped")
        saved.update(phase="paused", paused_at=time.time())
        _remaining(saved)
        host_boundary.save_receipt(path, saved, initial=False)
        _remaining(saved)
        print("event=storage_online_final_clients_stopped database_switch_authorized=false",
              file=sys.stderr, flush=True)
        return saved


def observe_source_drain_locked(state_root, *, exchange, max_entries):
    """Observe spool through the SAME live worker while exact clients stay held.

    exchange must send this request over the caller-owned existing pipe and
    bound both writing and reading to the supplied absolute monotonic deadline.
    A fresh response is checked, never persisted as switch/restart authority.
    """
    if not callable(exchange) or type(max_entries) is not int or not 1 <= max_entries <= 1_000_000:
        raise ValueError("storage_online_spool_host_inputs_invalid")
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] != "paused":
        raise RuntimeError("storage_online_spool_paused_source_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    def admit():
        _remaining(saved)
        observed, _, rows, _ = _observe(state_root, **args)
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_spool_source_changed")
        _remaining(saved)
    with host_boundary.docker_deadline(time.monotonic()+_remaining(saved)):
        admit()
        request = host_boundary.load_receipt(state_root/"storage-online-request.json")
        seconds = min(request["command_seconds"], _remaining(saved))
        deadline = time.monotonic()+seconds
        reply = exchange("source_drain", deadline=deadline, max_entries=max_entries)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_spool_reply_deadline_expired")
        admit()
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != "source_drain" or reply.get("state") != "background"
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False):
            raise RuntimeError("storage_online_spool_reply_invalid")
        result = reply.get("result")
        if (not isinstance(result, dict)
                or result.get("publisher_drain_authorized") is not False
                or result.get("final_switch_authorized") is not False
                or result.get("collection_resume_authorized") is not False
                or type(result.get("spool_empty_at_observation")) is not bool):
            raise RuntimeError("storage_online_spool_reply_invalid")
        return result


def copy_final_delta_locked(state_root, *, exchange, deadline, max_rounds):
    """Bounded tails through the SAME paused worker; never switch/resume authority.

    Caller owns the launcher lock/private pipe and supplies one already admitted
    absolute monotonic deadline within the persisted original wall/boot window.
    Keep that exact value for further rounds. A new allowance after interruption
    is not supported. Confirmed login_closed also admits residual pages on the
    retained session after job retirement, with fresh source checks each round.
    Pending spool and in-flight publishers still require their
    independent admission before any future database switch.
    """
    if (not callable(exchange) or type(deadline) not in (int, float)
            or not math.isfinite(deadline) or type(max_rounds) is not int
            or not 1 <= max_rounds <= 64):
        raise ValueError("storage_online_final_delta_host_inputs_invalid")
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] not in {"paused", "login_closed"}:
        raise RuntimeError("storage_online_final_delta_paused_source_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}

    gated = saved["phase"] == "login_closed"
    if gated and deadline != saved["switch"]["deadline_monotonic"]:
        raise RuntimeError("storage_online_final_delta_host_deadline_invalid")
    sequence = saved["switch"]["worker_sequence"] if gated else 0
    session_pids = None

    def admit():
        nonlocal sequence, session_pids
        remaining = deadline-time.monotonic()
        if not 0 < remaining <= _remaining(saved):
            raise RuntimeError("storage_online_final_delta_host_deadline_invalid")
        session = None
        if gated:
            reply = exchange("final_session_check", deadline=deadline)
            session, sequence = _final_session_reply(reply, operation="final_session_check",
                binding=binding, deadline=deadline, sequence=sequence)
            if session["database"] != {**saved["login_gate"]["database"], "allow_connections": False}:
                raise RuntimeError("storage_online_final_delta_gate_changed")
            pids = (session["backend_pid"], session["owner_pid"])
            if session_pids is not None and pids != session_pids:
                raise RuntimeError("storage_online_final_delta_session_changed")
            session_pids = pids
        observed, _, rows, _ = _observe(state_root, **args,
            **({"session": session} if session is not None else {}))
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_final_delta_source_changed")
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_final_delta_host_deadline_expired")
        _remaining(saved)

    with host_boundary.docker_deadline(deadline):
        last = None
        for index in range(max_rounds):
            admit()
            reply = exchange("final_delta", deadline=deadline)
            if gated:
                if (not isinstance(reply, dict) or type(reply.get("last_sequence")) is not int
                        or reply["last_sequence"] <= sequence
                        or reply.get("bound_final_deadline") != deadline):
                    raise RuntimeError("storage_online_final_delta_reply_invalid")
                sequence = reply["last_sequence"]
            admit()
            if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                    or reply.get("operation") != "final_delta" or reply.get("state") != "background"
                    or reply.get("final_switch_authorized") is not False
                    or reply.get("collection_resume_authorized") is not False):
                raise RuntimeError("storage_online_final_delta_reply_invalid")
            last = reply.get("result")
            if (not isinstance(last, dict) or last.get("migration_ready") is not False
                    or last.get("final_switch_authorized") is not False
                    or last.get("collection_resume_authorized") is not False
                    or not isinstance(last.get("sql"), dict)
                    or not isinstance(last.get("archives"), list)
                    or len(last["archives"]) != 3
                    or any(not isinstance(row, dict) for row in last["archives"])
                    or {row.get("family") for row in last["archives"]} !=
                        {"fact_archive_manifests", "raw_archive_manifests", "book_checkpoint_manifests"}):
                raise RuntimeError("storage_online_final_delta_reply_invalid")
            if (last["sql"].get("outcome") == "both_tails_observed_empty"
                    and all(isinstance(row, dict) and row.get("captured_tail_empty_at_observation") is True
                            for row in last["archives"])):
                break
        return {"rounds": index+1, "last_observation": last, "migration_ready": False,
                "publisher_drain_authorized": False, "final_switch_authorized": False,
                "collection_resume_authorized": False}


def record_switch_entry_locked(state_root, *, observe_worker, deadline):
    """Persist possible dispatch BEFORE a future switch; never dispatch or authorize.

    Caller owns the same launcher lock and private pipe. observe_worker must
    freshly read that worker's status within the supplied absolute deadline.
    This conservative checkpoint does not establish publisher exclusion, perform
    COMMIT or make any saved observation restart authority. Once recorded, even
    interruption before dispatch requires explicit outcome reconciliation; this
    function cannot replay or clear it.
    """
    if (not callable(observe_worker) or type(deadline) not in (int, float)
            or not math.isfinite(deadline)):
        raise ValueError("storage_online_switch_entry_inputs_invalid")
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] == "switch_entered":
        raise RuntimeError("storage_online_final_switch_reconciliation_required")
    if saved["phase"] != "paused":
        raise RuntimeError("storage_online_switch_entry_paused_source_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}

    def admit():
        if not 0 < deadline-time.monotonic() <= _remaining(saved):
            raise RuntimeError("storage_online_switch_entry_deadline_invalid")
        observed, _, rows, _ = _observe(state_root, **args)
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_switch_entry_source_changed")
        _remaining(saved)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_switch_entry_deadline_expired")

    with host_boundary.docker_deadline(deadline):
        admit()
        reply = observe_worker(deadline=deadline)
        admit()
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != "status" or reply.get("state") != "background"
                or reply.get("bound_final_deadline") != deadline
                or type(reply.get("last_sequence")) is not int or reply["last_sequence"] < 1
                or reply.get("migration_ready") is not False
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False):
            raise RuntimeError("storage_online_switch_entry_live_worker_required")
        saved.update(phase="switch_entered", switch={"entered_at": time.time(),
            "deadline_monotonic": deadline, "worker_sequence": reply["last_sequence"]})
        # Saving is the only mutation. Any failure after it remains uncertain;
        # no dispatch, ordinary pause reentry or marker removal follows here.
        host_boundary.save_receipt(state_root/STATE, saved, initial=False)
        admit()
        print("event=storage_online_switch_entry_recorded database_switch_authorized=false",
              file=sys.stderr, flush=True)
        return {"switch_intent_recorded": True, "database_switch_authorized": False,
                "collection_resume_authorized": False}


def inspect_switch_outcome_locked(state_root, *, exchange):
    """Fresh read-only same-worker outcome; leave durable intent and source held."""
    if not callable(exchange):
        raise ValueError("storage_online_outcome_exchange_invalid")
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] != "switch_entered":
        raise RuntimeError("storage_online_outcome_switch_intent_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    request = host_boundary.load_receipt(state_root/"storage-online-request.json")
    seconds = request["command_seconds"]
    if type(seconds) is not int or not 1 <= seconds <= 60:
        raise RuntimeError("storage_online_outcome_command_budget_invalid")
    deadline = min(saved["switch"]["deadline_monotonic"], time.monotonic()+min(5, seconds, _remaining(saved)))
    def admit():
        _remaining(saved)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_outcome_host_deadline_expired")
        observed, _, rows, _ = _observe(state_root, **args)
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_outcome_source_changed")
        _remaining(saved)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_outcome_host_deadline_expired")
    with host_boundary.docker_deadline(deadline):
        admit()
        reply = exchange("inspect_outcome", deadline=deadline)
        admit()
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != "inspect_outcome"
                or reply.get("state") not in {"background", "commit_unknown", "committed", "rolled_back"}
                or reply.get("bound_final_deadline") != saved["switch"]["deadline_monotonic"]
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False):
            raise RuntimeError("storage_online_outcome_reply_invalid")
        result = reply.get("result")
        expected = {"pending": None, "uncommitted": False, "committed": True}
        if (not isinstance(result, dict) or not isinstance(result.get("outcome"), str)
                or result["outcome"] not in expected
                or result.get("database_handoff_committed") is not expected[result["outcome"]]
                or result.get("collection_resume_authorized") is not False
                or result.get("runtime_activation_authorized") is not False):
            raise RuntimeError("storage_online_outcome_reply_invalid")
        # In particular, an uncommitted observation is not a live rollback fence.
        # Keep switch_entered unchanged, even when no COMMIT was dispatched.
        return result



def _supervised_source_start(container_id, *, deadline, check):
    if not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise ValueError("storage_online_resume_container_invalid")
    host_boundary.supervised_source_action(["start", container_id], deadline=deadline, check=check)



def resume_online_source_locked(state_root, *, exchange):
    """Internal exact-source abort under the SAME launcher lock and private pipe.

    No reentry or saved-negative shortcut: begin a live authoritative SQL fence,
    retain it across every supervised start, and terminalize that controller only
    after exact original clients are healthy. Failure can leave clients partly
    running or a daemon start in flight. Preserve the journal and refuse every
    ordinary relaunch/switch/recovery path; explicit reconciliation is required.
    This helper does not clear the final marker or activate a candidate runtime.
    """
    if not callable(exchange):
        raise ValueError("storage_online_resume_exchange_invalid")
    state_root = launch._canonical(state_root)
    path = state_root/STATE
    saved = _load(path)
    if saved["phase"] not in {"switch_entered", "login_closed"}:
        raise RuntimeError("storage_online_resume_switch_intent_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    deadline = saved["switch"]["deadline_monotonic"]
    sequence = saved["switch"]["worker_sequence"]
    gated = saved["phase"] == "login_closed"
    session = None
    fenced = False

    def budget():
        if not 0 < deadline-time.monotonic() <= _remaining(saved):
            raise RuntimeError("storage_online_resume_deadline_expired")

    def admit():
        nonlocal sequence, session
        budget()
        if gated and not fenced:
            reply = exchange("final_session_check", deadline=deadline)
            session, sequence = _final_session_reply(reply, operation="final_session_check",
                binding=binding, deadline=deadline, sequence=sequence)
            if session["database"] != {**saved["login_gate"]["database"], "allow_connections": False}:
                raise RuntimeError("storage_online_resume_gate_changed")
        observed, preparation, rows, limits = _observe(state_root, **args,
            **({"session": session} if gated else {}))
        if (observed != binding or saved["duration_seconds"] > limits["seconds"]
                or saved["deadline"] > limits["capture_deadline"]):
            raise RuntimeError("storage_online_resume_source_changed")
        allowed = set(saved.get("resume", {}).get("completed", []))
        action = saved.get("resume", {}).get("inflight")
        if action is not None:
            if rows[action["service"]]["id"] != action["container_id"]:
                raise RuntimeError("storage_online_resume_source_changed")
            allowed.add(action["service"])
        for name in host_boundary.STOP:
            if (rows[name]["running"] and (name not in allowed
                    or not preparation["clients"][name]["was_running"])):
                raise RuntimeError("storage_online_resume_unexpected_running_client")
            if name in saved.get("resume", {}).get("completed", []) and not rows[name]["running"]:
                raise RuntimeError("storage_online_resume_started_client_stopped")
        budget()
        return preparation, rows

    def fence(operation):
        nonlocal sequence, session, fenced
        budget()
        reply = exchange(operation, deadline=deadline)
        budget()
        ending = operation == "rollback_fence_end"
        result = reply.get("result") if isinstance(reply, dict) else None
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != operation
                or reply.get("state") != ("aborted" if ending else "resume_fenced")
                or reply.get("bound_final_deadline") != deadline
                or type(reply.get("last_sequence")) is not int or reply["last_sequence"] <= sequence
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False
                or not isinstance(result, dict)
                or result.get("database_handoff_committed") is not False
                or result.get("database_resume_fence_held") is not (not ending)
                or result.get("collection_resume_authorized") is not False
                or result.get("runtime_activation_authorized") is not False):
            raise RuntimeError("storage_online_resume_fence_reply_invalid")
        if gated:
            current, _ = _final_session_reply(reply, operation=operation, binding=binding,
                deadline=deadline, sequence=sequence, state="aborted" if ending else "resume_fenced")
            if (current["database"] != {**saved["login_gate"]["database"],
                                      "allow_connections": current["database"]["allow_connections"]}
                    or current["backend_pid"] != session["backend_pid"]
                    or current["owner_pid"] != session["owner_pid"]):
                raise RuntimeError("storage_online_resume_gate_session_changed")
            restore = saved.get("resume", {}).get("gate_restore")
            if restore is None or restore["inflight"] != "logins":
                expected_open = restore is not None and "logins" in restore["completed"]
                if current["database"]["allow_connections"] is not expected_open:
                    raise RuntimeError("storage_online_resume_gate_changed")
            session = current
        sequence = reply["last_sequence"]
        fenced = True

    def check():
        if not gated:admit()
        fence("rollback_fence_check")
        admit()

    with host_boundary.docker_deadline(deadline):
        preparation, rows = admit()
        fence("rollback_fence_begin")
        admit()
        saved.update(phase="source_resuming", resume={"started_at": time.time(),
            "completed": [], "inflight": None, "finished_at": None})
        if gated:
            saved["resume"]["gate_restore"] = {"inflight": None, "completed": []}
        host_boundary.save_receipt(path, saved, initial=False)
        if gated:
            # Gate and job requests are journaled before dispatch, under the SAME
            # negative-outcome fence. Local cancellation never undoes a SQL action.
            for action in ("logins", "jobs"):
                check()
                _, current_rows = admit()
                saved["resume"]["gate_restore"]["inflight"] = action
                host_boundary.save_receipt(path, saved, initial=False)
                database = saved["login_gate"]["database"]
                if action == "logins":
                    sql = ("SELECT format('ALTER DATABASE %I ALLOW_CONNECTIONS true',datname) "
                        "FROM pg_database WHERE datname=:'target' AND oid="+str(database["oid"])+
                        " AND NOT datallowconn AND (SELECT system_identifier FROM pg_control_system())="+
                        database["cluster"]+"\n\\gexec\n")
                    target = 'postgres'
                else:
                    sql = ("DO $qt$ BEGIN IF NOT _timescaledb_functions.start_background_workers() "
                           "THEN RAISE EXCEPTION 'storage_online_jobs_restart_not_accepted'; END IF; END $qt$;")
                    target = '"$POSTGRES_DB"'
                arguments = ["exec", "-i", current_rows["tsdb"]["id"], "sh", "-ec",
                    'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d '+target+
                    ' -v ON_ERROR_STOP=1 -v target="$POSTGRES_DB" -qAtf -']
                print("event=storage_online_gate_restore_started action="+action+
                      " controller_id="+binding["controller_id"], file=sys.stderr, flush=True)
                try:
                    host_boundary.supervised_source_action(arguments, deadline=deadline, check=check,
                        input="SET statement_timeout='5s';\n"+sql)
                    check()
                    if session["database"]["allow_connections"] is not True:
                        raise RuntimeError("storage_online_resume_gate_not_open")
                except BaseException:
                    print("event=storage_online_gate_restore_failed action="+action+
                          " outcome=unresolved", file=sys.stderr, flush=True)
                    raise
                saved["resume"]["gate_restore"]["completed"].append(action)
                saved["resume"]["gate_restore"]["inflight"] = None
                host_boundary.save_receipt(path, saved, initial=False)
        print("event=storage_online_source_resume_started runtime_activation_authorized=false",
              file=sys.stderr, flush=True)
        for name in host_boundary.STOP:
            if not preparation["clients"][name]["was_running"]:
                continue
            check()
            _, rows = admit()
            saved["resume"]["inflight"] = {"service": name, "container_id": rows[name]["id"],
                                             "requested_at": time.time()}
            host_boundary.save_receipt(path, saved, initial=False)  # BEFORE Docker dispatch.
            _supervised_source_start(rows[name]["id"], deadline=deadline, check=check)
            check()
            _, rows = admit()
            if not rows[name]["running"]:
                raise RuntimeError("storage_online_resume_client_not_running")
            saved["resume"]["completed"].append(name)
            saved["resume"]["inflight"] = None
            host_boundary.save_receipt(path, saved, initial=False)
        while True:
            check()
            _, rows = admit()
            if initial._source_healthy(rows):
                break
            time.sleep(min(.1, deadline-time.monotonic()))
        fence("rollback_fence_end")
        # End terminalizes the worker; requiring it to stay running here would
        # race normal process exit. Exact source health was admitted under the
        # live fence immediately before end. No more host starts follow.
        budget()
        saved["phase"] = "source_resumed"
        saved["resume"]["finished_at"] = time.time()
        host_boundary.save_receipt(path, saved, initial=False)
        budget()
        print("event=storage_online_original_source_resumed final_marker_retained=true",
              file=sys.stderr, flush=True)
        return {"original_source_resumed": True, "final_marker_retained": True,
                "database_switch_authorized": False, "runtime_activation_authorized": False,
                **({"original_login_gate_restored": True, "database_jobs_restart_requested": True} if gated else {})}


def reconcile_source_resumed_locked(state_root, *, exchange):
    """Record completed starts after a lost end reply on the SAME live pipe.

    No Docker starts, fence recreation or saved-negative shortcut. Every start
    must already have a completed durable journal entry with no in-flight action.
    A fresh aborted-controller outcome and exact healthy original source are
    required within the original final window. Worker loss, partial/unread pipe
    framing, partial starts and expiry remain unresolved. Retain the marker.
    """
    if not callable(exchange):
        raise ValueError("storage_online_resume_exchange_invalid")
    state_root = launch._canonical(state_root)
    path = state_root/STATE
    saved = _load(path)
    if (saved["phase"] != "source_resuming" or saved["resume"]["inflight"] is not None):
        raise RuntimeError("storage_online_resume_completed_journal_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    request = host_boundary.load_receipt(state_root/"storage-online-request.json")
    seconds = request["command_seconds"]
    if type(seconds) is not int or not 1 <= seconds <= 60:
        raise RuntimeError("storage_online_resume_command_budget_invalid")
    deadline = min(saved["switch"]["deadline_monotonic"],
                   time.monotonic()+min(5, seconds, _remaining(saved)))

    def admit():
        _remaining(saved)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_resume_deadline_expired")
        observed, preparation, rows, limits = _observe(state_root, **args)
        if (observed != binding or saved["duration_seconds"] > limits["seconds"]
                or saved["deadline"] > limits["capture_deadline"]):
            raise RuntimeError("storage_online_resume_source_changed")
        expected = [n for n in host_boundary.STOP if preparation["clients"][n]["was_running"]]
        if saved["resume"]["completed"] != expected:
            raise RuntimeError("storage_online_resume_completed_journal_required")
        if (any(rows[n]["running"] != (n in expected) for n in host_boundary.STOP)
                or not initial._source_healthy(rows)):
            raise RuntimeError("storage_online_resume_healthy_source_required")
        _remaining(saved)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_resume_deadline_expired")

    with host_boundary.docker_deadline(deadline):
        admit()
        reply = exchange("inspect_outcome", deadline=deadline)
        admit()
        result = reply.get("result") if isinstance(reply, dict) else None
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != "inspect_outcome" or reply.get("state") != "aborted"
                or reply.get("bound_final_deadline") != saved["switch"]["deadline_monotonic"]
                or type(reply.get("last_sequence")) is not int
                or reply["last_sequence"] <= saved["switch"]["worker_sequence"]
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False
                or not isinstance(result, dict) or result.get("outcome") != "uncommitted"
                or result.get("database_handoff_committed") is not False
                or result.get("collection_resume_authorized") is not False
                or result.get("runtime_activation_authorized") is not False):
            raise RuntimeError("storage_online_resume_terminal_reply_invalid")
        saved["phase"] = "source_resumed"
        saved["resume"]["finished_at"] = time.time()
        host_boundary.save_receipt(path, saved, initial=False)
        _remaining(saved)
        if time.monotonic() >= deadline:
            raise RuntimeError("storage_online_resume_deadline_expired")
        print("event=storage_online_source_resume_reconciled final_marker_retained=true",
              file=sys.stderr, flush=True)
        return {"original_source_resumed": True, "final_marker_retained": True,
                "database_switch_authorized": False, "runtime_activation_authorized": False}


_GATE_OBSERVE = """SELECT json_build_object(
 'cluster',(SELECT system_identifier::text FROM pg_control_system()),
 'oid',oid::bigint,'name',datname,'allow_connections',datallowconn)
 FROM pg_database WHERE datname=:'target';
"""


def _valid_forward_session(value):
    """Validate only wire identity; never infer forward authority from a reply.

    A host owner must have admitted and persisted this exact identity before
    login closure. Legacy bindings retain their distinct-session requirement.
    Fresh request/publication and actual SQL admission remain separate owners.
    """
    if (not isinstance(value, dict) or set(value) != {
            "schema_version", "operation_sha256", "cancellation_intent_sha256",
            "started_at", "expires_at", "attempt_seconds", "end_day"}
            or value["schema_version"] != "qt.storage_online_forward_session.v1"
            or any(not isinstance(value[k], str) or not re.fullmatch(r"[0-9a-f]{64}", value[k])
                   for k in ("operation_sha256", "cancellation_intent_sha256"))
            or value["operation_sha256"] == value["cancellation_intent_sha256"]
            or type(value["attempt_seconds"]) is not int
            or not 30 <= value["attempt_seconds"] <= 96*3600
            or any(not isinstance(value[k], str) for k in ("started_at", "expires_at", "end_day"))):
        return False
    try:
        start, end = (datetime.fromisoformat(value[k]) for k in ("started_at", "expires_at"))
        return (start.tzinfo is not None and end.tzinfo is not None
                and end-start == timedelta(seconds=value["attempt_seconds"])
                and date.fromisoformat(value["end_day"]).isoformat() == value["end_day"])
    except ValueError:
        return False


def _session_owner_matches(result, binding):
    """Same PID is allowed only by the exact pre-admitted forward binding."""
    if any(type(result.get(k)) is not int or result[k] <= 0 for k in ("backend_pid", "owner_pid")):
        return False
    if "forward" not in binding:
        return "forward" not in result and result["backend_pid"] != result["owner_pid"]
    return (_valid_forward_session(binding["forward"])
            and result.get("forward") == binding["forward"]
            and result["backend_pid"] == result["owner_pid"])


def _final_session_reply(reply, *, operation, binding, deadline, sequence, state="background"):
    """Validate a fresh same-worker session observation, without granting authority."""
    result = reply.get("result") if isinstance(reply, dict) else None
    if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
            or reply.get("operation") != operation or reply.get("state") != state
            or reply.get("bound_final_deadline") != deadline
            or type(reply.get("last_sequence")) is not int or reply["last_sequence"] <= sequence
            or reply.get("final_switch_authorized") is not False
            or reply.get("collection_resume_authorized") is not False
            or not isinstance(result, dict) or not isinstance(result.get("database"), dict)
            or not _valid_database_gate(result["database"])
            or result.get("builtin_jobs_admitted") is not True
            or result.get("capture") != binding["capture"]
            or not _session_owner_matches(result, binding)
            or any(result.get(k) is not False for k in ("database_switch_authorized",
                "collection_resume_authorized", "runtime_activation_authorized"))):
        raise RuntimeError("storage_online_login_worker_reply_invalid")
    return result, reply["last_sequence"]


def close_database_logins_locked(state_root, *, exchange):
    """Persist and close only the exact target's new logins. Never reopen here.

    Caller retains the launcher flock and SAME live worker. This is a necessary
    exclusion boundary, not COMMIT authority. After closure, the same worker
    stops pinned Timescale jobs and requires other target sessions to leave.
    Archive/spool publishers and host source admission still require integration.
    Lost replies leave login_closing and may already have closed access. No retry,
    source resumption or gate restoration follows an uncertain result.
    """
    if not callable(exchange):
        raise ValueError("storage_online_login_exchange_invalid")
    state_root = launch._canonical(state_root)
    path = state_root/STATE
    saved = _load(path)
    if saved["phase"] != "switch_entered":
        raise RuntimeError("storage_online_login_switch_intent_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    deadline = saved["switch"]["deadline_monotonic"]
    sequence = saved["switch"]["worker_sequence"]

    def budget():
        if not 0 < deadline-time.monotonic() <= _remaining(saved):
            raise RuntimeError("storage_online_login_deadline_expired")

    def admit(session=None):
        budget()
        observed, preparation, rows, _ = _observe(state_root, **args,
            **({"session": session} if session is not None else {}))
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_login_source_changed")
        budget()
        return preparation, rows

    def observe_worker(operation):
        nonlocal sequence
        budget()
        reply = exchange(operation, deadline=deadline)
        budget()
        result, sequence = _final_session_reply(reply, operation=operation,
            binding=binding, deadline=deadline, sequence=sequence)
        return result

    with host_boundary.docker_deadline(deadline):
        preparation, rows = admit()
        session = observe_worker("final_session_begin")
        admit()
        database = session["database"]
        control = json.loads(host_boundary.maintenance_query(rows["tsdb"]["id"], _GATE_OBSERVE))
        if database != control or database["cluster"] != preparation["cluster"] or not database["allow_connections"]:
            raise RuntimeError("storage_online_login_database_changed")
        budget()
        saved.update(phase="login_closing", login_gate={"database": database,
            "requested_at": time.time(), "closed_at": None, "database_jobs_stopped": False})
        host_boundary.save_receipt(path, saved, initial=False)  # BEFORE ALTER DATABASE.
        budget()
        # Identifiers come from the current catalog; numeric identities are
        # validated above. A changed target produces no ALTER and refuses below.
        sql = ("SELECT format('ALTER DATABASE %I ALLOW_CONNECTIONS false',datname) "
            "FROM pg_database WHERE datname=:'target' AND oid="+str(database["oid"])+
            " AND datallowconn AND (SELECT system_identifier FROM pg_control_system())="+
            database["cluster"]+"\n\\gexec\n"+_GATE_OBSERVE)
        observed = json.loads(host_boundary.maintenance_query(rows["tsdb"]["id"], sql))
        budget()
        expected = {**database, "allow_connections": False}
        if observed != expected:
            raise RuntimeError("storage_online_login_not_closed")
        current = observe_worker("final_session_check")
        if (current["database"] != expected or current["backend_pid"] != session["backend_pid"]
                or current["owner_pid"] != session["owner_pid"]):
            raise RuntimeError("storage_online_login_session_changed")
        admit(current)
        quiesced = observe_worker("final_session_quiesce")
        if (quiesced["database"] != expected or quiesced["backend_pid"] != session["backend_pid"]
                or quiesced["owner_pid"] != session["owner_pid"]
                or quiesced.get("database_jobs_stopped") is not True
                or quiesced.get("job_definitions_preserved") is not True):
            raise RuntimeError("storage_online_login_jobs_unconfirmed")
        admit(quiesced)
        saved["phase"] = "login_closed"
        saved["login_gate"]["closed_at"] = time.time()
        saved["login_gate"]["database_jobs_stopped"] = True
        host_boundary.save_receipt(path, saved, initial=False)
        budget()
        print("event=storage_online_database_logins_closed database_switch_authorized=false",
              file=sys.stderr, flush=True)
        return {"new_logins_closed": True, "database_switch_authorized": False,
                "collection_resume_authorized": False, "runtime_activation_authorized": False}


_SOURCE_WRITERS = {
    "backend": "portal.backend.run_backend",
    "initialize": "portal.backend.workers.single_node_initializer",
    "market-data-collector": "portal.backend.workers.market_data_collector",
}


def _source_writer_contract(details, *, service, source_image, source_revision, root):
    """Admit fixed guarded source commands, not arbitrary image/entrypoint flags.

    source_image is the separately qualified immutable preparatory release.
    The original preparation contract binds the remaining configuration. This
    check does not certify an unqualified image merely because its env matches.
    """
    config = details["config"]
    entries = config.get("Env") or []
    environment = dict(item.split("=", 1) for item in entries)
    command = (config.get("Entrypoint") or []) + (config.get("Cmd") or [])
    target = "/app/logs/market-structure"
    mounts = [m for m in details["mounts"] if m.get("Destination") == target]
    if (len(environment) != len(entries) or details["image"] != source_image
            or command != ["python", "-m", _SOURCE_WRITERS[service]]
            or environment.get("QT_IMAGE_SOURCE_REVISION") != source_revision
            or environment.get("QT_STORAGE_SOURCE_FENCE_ROOT") != target
            or environment.get("MARKET_STRUCTURE_STORAGE_ROOT") != target
            or environment.get("MARKET_STRUCTURE_WORKING_ROOT", target) != target
            or len(mounts) != 1 or mounts[0].get("Type") != "bind"
            or mounts[0].get("Source") != str(root) or mounts[0].get("RW") is not True
            or any(m.get("Destination", "").startswith(("/app/src", "/app/portal", "/app/scripts"))
                   for m in details["mounts"])):
        raise RuntimeError("storage_online_guarded_source_contract_required")


@contextmanager
def held_source_writers_locked(state_root, *, source_image):
    """Hold the exact prepared source inode across final work and retirement.

    Enter only after source stop, under the existing launcher/deployment lock.
    The caller supplies the independently qualified source image and retains this
    context through its transition. It reuses the existing kernel namespace
    owner; no persisted receipt or returned observation is lock authority.
    SQL gates, unknown publishers, live proof and outcome remain separate checks.
    """
    from market_data.archive_namespace import archive_namespace

    if not isinstance(source_image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", source_image):
        raise ValueError("storage_online_source_image_required")
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] != "paused":
        raise RuntimeError("storage_online_source_hold_paused_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    deadline = time.monotonic()+_remaining(saved)
    with host_boundary.docker_deadline(deadline):
        observed, preparation, rows, _ = _observe(state_root, **args)
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_source_hold_changed")
        roots = preparation["source_roots"]
        candidates = [Path(p) for p in roots if str(Path(p)/"objects") in roots]
        if len(roots) != 2 or len(candidates) != 1:
            raise RuntimeError("storage_online_source_hold_roots_invalid")
        root = candidates[0]
        for service in _SOURCE_WRITERS:
            details = host_boundary.database_details(rows[service]["id"])
            _source_writer_contract(details, service=service, source_image=source_image,
                                    source_revision=binding["source_revision"], root=root)
        with archive_namespace(root, exclusive=True) as namespace_check:
            active = True
            def check():
                if not active:
                    raise RuntimeError("storage_online_source_hold_closed")
                current = _load(state_root/STATE)
                _remaining(current)
                if current["binding"] != binding or time.monotonic() >= deadline:
                    raise RuntimeError("storage_online_source_hold_binding_changed")
                namespace_check()
                for name, expected in roots.items():
                    info = Path(name).stat()
                    actual = [info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_mode & 0o7777]
                    if actual != expected or Path(name).resolve(strict=True) != Path(name):
                        raise RuntimeError("storage_online_source_hold_metadata_changed")
            token = _SOURCE_HOLD.set((state_root, binding, source_image, check))
            try:
                check()
                yield check
                check()
            finally:
                active = False
                _SOURCE_HOLD.reset(token)


def commit_online_handoff_locked(state_root, *, exchange):
    """Dispatch once under the live source hold; freshly reconcile on SAME pipe.

    A send/receive failure retains commit_dispatching and propagates. Only a
    fully received reply permits the next fresh observation here. No replay,
    source restart, read-worker retirement or recovery activation is performed.
    """
    state_root = launch._canonical(state_root)
    held = _SOURCE_HOLD.get()
    if not callable(exchange) or held is None or held[0] != state_root:
        raise RuntimeError("storage_online_live_source_hold_required")
    _, binding, source_image, source_check = held
    path = state_root/STATE
    saved = _load(path)
    if saved["phase"] != "login_closed" or saved["binding"] != binding:
        raise RuntimeError("storage_online_commit_closed_gate_required")
    deadline = saved["switch"]["deadline_monotonic"]
    sequence = saved["switch"]["worker_sequence"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    expected_gate = {**saved["login_gate"]["database"], "allow_connections": False}

    def budget():
        source_check()
        if not 0 < deadline-time.monotonic() <= _remaining(saved):
            raise RuntimeError("storage_online_commit_deadline_expired")

    def admit(session):
        budget()
        observed, _, rows, _ = _observe(state_root, **args, session=session)
        if observed != binding or any(rows[n]["running"] for n in host_boundary.STOP):
            raise RuntimeError("storage_online_commit_source_changed")
        current_gate = json.loads(host_boundary.maintenance_query(rows["tsdb"]["id"], _GATE_OBSERVE))
        if current_gate != expected_gate:
            raise RuntimeError("storage_online_commit_gate_changed")
        budget()

    with host_boundary.docker_deadline(deadline):
        budget()
        reply = exchange("final_session_check", deadline=deadline)
        session, sequence = _final_session_reply(reply, operation="final_session_check",
            binding=binding, deadline=deadline, sequence=sequence)
        if session["database"] != expected_gate:
            raise RuntimeError("storage_online_commit_gate_changed")
        admit(session)
        saved.update(phase="commit_dispatching", commit={"requested_at": time.time(),
            "worker_sequence": sequence+1, "source_image": source_image, "confirmed_at": None,
            "initial_policy_activated": False, "confirmed_plan_id": None})
        host_boundary.save_receipt(path, saved, initial=False)  # BEFORE possible COMMIT.
        budget()
        reply = exchange("commit_database", deadline=deadline)
        budget()
        result = reply.get("result") if isinstance(reply, dict) else None
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != "commit_database"
                or reply.get("state") not in {"commit_unknown", "committed"}
                or reply.get("bound_final_deadline") != deadline
                or reply.get("last_sequence") != sequence+1
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False
                or not isinstance(result, dict)
                or result.get("database_handoff_committed") is not (True if reply["state"] == "committed" else None)
                or result.get("initial_policy_activated") is not (True if reply["state"] == "committed" else None)
                or result.get("collection_resume_authorized") is not False
                or result.get("runtime_activation_authorized") is not False):
            raise RuntimeError("storage_online_commit_reply_invalid")
        admit(session)
        # A reply, including a successful reply, is not the final outcome proof.
        reply = exchange("inspect_outcome", deadline=min(deadline, time.monotonic()+5))
        budget()
        result = reply.get("result") if isinstance(reply, dict) else None
        if (not isinstance(reply, dict) or reply.get("controller_id") != binding["controller_id"]
                or reply.get("operation") != "inspect_outcome"
                or reply.get("state") not in {"commit_unknown", "committed"}
                or reply.get("bound_final_deadline") != deadline
                or reply.get("last_sequence") != sequence+2
                or reply.get("final_switch_authorized") is not False
                or reply.get("collection_resume_authorized") is not False
                or not isinstance(result, dict) or result.get("outcome") not in {"committed", "uncommitted", "pending"}
                or result.get("database_handoff_committed") is not {"committed": True, "uncommitted": False, "pending": None}[result["outcome"]]
                or result.get("collection_resume_authorized") is not False
                or result.get("runtime_activation_authorized") is not False):
            raise RuntimeError("storage_online_commit_outcome_invalid")
        admit(session)
        if result["outcome"] == "committed":
            if result.get("initial_policy_activated") is not True:
                raise RuntimeError("storage_online_committed_policy_unconfirmed")
            if (not isinstance(result.get("confirmed_plan_id"), str)
                    or not re.fullmatch(r"handoff-[0-9a-f]{32}", result["confirmed_plan_id"])):
                raise RuntimeError("storage_online_committed_plan_unconfirmed")
            saved["commit"]["confirmed_plan_id"] = result["confirmed_plan_id"]
            saved["commit"]["initial_policy_activated"] = True
            saved["phase"] = "committed"
            saved["commit"]["confirmed_at"] = time.time()
            host_boundary.save_receipt(path, saved, initial=False)
            budget()
        return result


def prepare_recovery_database_locked(state_root, *, worker_process, keys_root,
                                     socket_volume, max_duration_seconds):
    """Preserve the committed HDD database while adding its recovery mounts.

    Only this still-live source hold may enter after confirmed COMMIT and actual
    worker retirement. The separate phase bound shortens the original final
    window; it never reuses or renews initial preparation. No runtime is started.
    """
    from scripts.automation.storage_online_recovery import prepare_database

    state_root = launch._canonical(state_root)
    held = _SOURCE_HOLD.get()
    if held is None or held[0] != state_root:
        raise RuntimeError("storage_online_live_source_hold_required")
    _, binding, source_image, source_check = held
    saved = _load(state_root/STATE)
    if (saved["phase"] != "committed" or saved["binding"] != binding
            or saved["commit"]["source_image"] != source_image):
        raise RuntimeError("storage_online_recovery_committed_live_hold_required")
    source_check()
    return prepare_database(state_root, saved=saved, worker_process=worker_process,
        keys_root=keys_root, socket_volume=socket_volume,
        max_duration_seconds=max_duration_seconds, source_check=source_check)


def prepare_online_repositories_locked(state_root, *, worker_process, max_bytes,
        reserve_bytes, recent_free_bytes, max_duration_seconds):
    """Continue the same held committed operation through repository/WAL readiness."""
    from scripts.automation.storage_online_repositories import prepare_repositories
    state_root = launch._canonical(state_root)
    held = _SOURCE_HOLD.get()
    if held is None or held[0] != state_root:
        raise RuntimeError("storage_online_live_source_hold_required")
    _, binding, image, check = held
    saved = _load(state_root/STATE)
    if (saved["phase"] != "recovery_database_ready" or saved["binding"] != binding
            or saved["commit"]["source_image"] != image):
        raise RuntimeError("storage_online_repository_live_transition_required")
    check()
    return prepare_repositories(state_root, saved=saved, worker_process=worker_process,
        source_check=check, max_bytes=max_bytes, reserve_bytes=reserve_bytes,
        recent_free_bytes=recent_free_bytes, max_duration_seconds=max_duration_seconds)


def prepare_online_runtime_spool_locked(state_root, *, worker_process, destination,
        max_bytes, max_entries, reserve_bytes, max_duration_seconds):
    """Prepare preserved pending WAL for the ordinary candidate application."""
    from scripts.automation.storage_online_runtime import prepare_spool
    state_root=launch._canonical(state_root)
    held=_SOURCE_HOLD.get()
    if held is None or held[0]!=state_root:
        raise RuntimeError("storage_online_live_source_hold_required")
    _,binding,image,check=held
    saved=_load(state_root/STATE)
    if (saved["phase"]!="recovery_wal_ready" or saved["binding"]!=binding
            or saved["commit"]["source_image"]!=image):
        raise RuntimeError("storage_online_runtime_spool_live_transition_required")
    check()
    return prepare_spool(state_root,saved=saved,worker_process=worker_process,source_check=check,
        destination=destination,max_bytes=max_bytes,max_entries=max_entries,
        reserve_bytes=reserve_bytes,max_duration_seconds=max_duration_seconds)


def activate_online_runtime_locked(state_root, *, worker_process, max_duration_seconds):
    """Start matching applications only through the same committed held operation."""
    from scripts.automation.storage_online_runtime import activate_runtime
    state_root=launch._canonical(state_root)
    held=_SOURCE_HOLD.get()
    if held is None or held[0]!=state_root:
        raise RuntimeError("storage_online_live_source_hold_required")
    _,binding,image,check=held
    saved=_load(state_root/STATE)
    if (saved["phase"]!="recovery_spool_ready" or saved["binding"]!=binding
            or saved["commit"]["source_image"]!=image):
        raise RuntimeError("storage_online_runtime_live_transition_required")
    check()
    return activate_runtime(state_root,saved=saved,worker_process=worker_process,
        source_check=check,max_duration_seconds=max_duration_seconds)


def inspect_runtime_completion_locked(state_root, *, timeout_seconds=60):
    """Observe a fully journaled runtime; never resolve or replay uncertain actions.

    Original clocks remain unchanged. Expiry is allowed only for this bounded
    inspection after all starts durably finished inside the original window.
    Reboot and backward clocks still refuse; no saved observation grants release.
    """
    from scripts.automation.storage_online_runtime import inspect_completed_runtime
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] != "recovery_runtime_ready":
        raise RuntimeError("storage_online_completion_runtime_not_confirmed")
    if saved["boot_id"] != _boot_id():
        raise RuntimeError("storage_online_final_boot_changed")
    if time.time() < saved["runtime"]["finished_at"] or _boot_seconds() < saved["started_boot"]:
        raise RuntimeError("storage_online_final_clock_moved_backwards")
    return inspect_completed_runtime(state_root, saved=saved, timeout_seconds=timeout_seconds)


def publish_deployment_configuration_locked(state_root, *, repository, environment_path):
    """Terminal metadata transition; all migration actions must already be complete."""
    from scripts.automation import storage_online_release as release
    state_root=launch._canonical(state_root)
    observed=inspect_runtime_completion_locked(state_root)
    if observed.get("ready") is not True:
        raise RuntimeError("storage_online_release_complete_recovery_required")
    saved=_load(state_root/STATE)
    if "release" in saved:
        if saved["release"]["status"]!="publishing":
            raise RuntimeError("storage_online_release_already_published")
        if (saved["release"]["repository"]!=str(repository)
                or saved["release"]["environment_path"]!=str(environment_path)):
            raise RuntimeError("storage_online_release_binding_changed")
        release.inspect_deployment_configuration(state_root, repository=repository,
            environment_path=environment_path, saved=saved)
        return release.reconcile_configuration_files(state_root,saved=saved)
    configuration=release.inspect_deployment_configuration(state_root,
        repository=repository,environment_path=environment_path,saved=saved)
    return release.publish_configuration(state_root,repository=repository,
        environment_path=environment_path,saved=saved,configuration=configuration)

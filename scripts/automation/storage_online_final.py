"""Internal final source-stop boundary under the existing launcher's host lock.

No database switch, recovery activation or production CLI. Internal source abort
resumption requires the live SQL fence; intent survives failure and completion.
"""
from __future__ import annotations

from datetime import datetime
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
    resuming = saved.get("phase") in {"source_resuming", "source_resumed"}
    entered = saved.get("phase") == "switch_entered" or resuming
    fields = _FIELDS | ({"switch"} if entered else set()) | ({"resume"} if resuming else set())
    if (set(saved) != fields or saved["schema"] != SCHEMA
            or saved["phase"] not in ("stopping", "paused", "switch_entered", "source_resuming", "source_resumed")
            or type(saved["duration_seconds"]) is not int
            or not 1 <= saved["duration_seconds"] <= 96*3600
            or any(type(saved[k]) not in (int, float) or not math.isfinite(saved[k])
                   for k in ("started_at", "deadline", "started_boot", "deadline_boot"))
            or not 0 < saved["started_at"] < saved["deadline"]
            or not 0 < saved["started_boot"] < saved["deadline_boot"]
            or abs(saved["deadline"]-saved["started_at"]-saved["duration_seconds"]) > .000001
            or abs(saved["deadline_boot"]-saved["started_boot"]-saved["duration_seconds"]) > .000001):
        raise RuntimeError("storage_online_final_receipt_invalid")
    if saved["phase"] in ("paused", "switch_entered", "source_resuming", "source_resumed"):
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
                or set(resume) != {"started_at", "completed", "inflight", "finished_at"}
                or type(resume["started_at"]) not in (int, float)
                or not saved["switch"]["entered_at"] <= resume["started_at"] <= saved["deadline"]
                or not isinstance(resume["completed"], list)
                or any(not isinstance(n, str) or n not in host_boundary.STOP for n in resume["completed"])
                or len(set(resume["completed"])) != len(resume["completed"])
                or resume["completed"] != [n for n in host_boundary.STOP if n in resume["completed"]]):
            raise RuntimeError("storage_online_resume_receipt_invalid")
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
    return saved



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


def _observe(state_root, *, project, source_revision, controller_id, worker_id):
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
    rows = initial._admit_source(state_root, preparation, require_running=False, operator_id=worker_id)
    _admit_mount_writers(rows, operator_id=worker_id)
    capture = json.loads(host_boundary.database_query(rows["tsdb"]["id"],
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
        preparation_sha256=host_boundary.digest(preparation), worker_sha256=host_boundary.digest(worker),
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
    is not supported. Pending spool and in-flight publishers still require their
    independent admission before any future database switch.
    """
    if (not callable(exchange) or type(deadline) not in (int, float)
            or not math.isfinite(deadline) or type(max_rounds) is not int
            or not 1 <= max_rounds <= 64):
        raise ValueError("storage_online_final_delta_host_inputs_invalid")
    state_root = launch._canonical(state_root)
    saved = _load(state_root/STATE)
    if saved["phase"] != "paused":
        raise RuntimeError("storage_online_final_delta_paused_source_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}

    def admit():
        remaining = deadline-time.monotonic()
        if not 0 < remaining <= _remaining(saved):
            raise RuntimeError("storage_online_final_delta_host_deadline_invalid")
        observed, _, rows, _ = _observe(state_root, **args)
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
    """Supervise one exact daemon request; killing its CLI is NOT cancellation.

    The caller has already persisted the in-flight action. On any loss, leave
    that action unresolved and issue no further starts. The daemon may complete
    it later. The launcher lock and durable marker continue to exclude managed
    switching/deployment. This is not a bound on database/network check latency.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", container_id):
        raise ValueError("storage_online_resume_container_invalid")
    check()
    if time.monotonic() >= deadline:
        raise RuntimeError("storage_online_resume_deadline_expired")
    process = subprocess.Popen(["docker", "start", container_id],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        while True:
            check()
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise RuntimeError("storage_online_resume_deadline_expired")
            result = process.poll()
            if result is not None:
                if result != 0:
                    raise RuntimeError("storage_online_resume_start_failed")
                return
            time.sleep(min(.1, remaining))
    finally:
        # Reap only our local CLI, including cancellation. This grants no more
        # host-operation time and makes no claim about daemon request outcome.
        if process.poll() is None:
            process.kill()
        process.wait(timeout=1)  # Cleanup only, never another daemon action.


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
    if saved["phase"] != "switch_entered":
        raise RuntimeError("storage_online_resume_switch_intent_required")
    binding = saved["binding"]
    args = {key: binding[key] for key in ("project", "source_revision", "controller_id", "worker_id")}
    deadline = saved["switch"]["deadline_monotonic"]
    sequence = saved["switch"]["worker_sequence"]

    def budget():
        if not 0 < deadline-time.monotonic() <= _remaining(saved):
            raise RuntimeError("storage_online_resume_deadline_expired")

    def admit():
        budget()
        observed, preparation, rows, limits = _observe(state_root, **args)
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
        nonlocal sequence
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
        sequence = reply["last_sequence"]

    def check():
        admit()
        fence("rollback_fence_check")
        admit()

    with host_boundary.docker_deadline(deadline):
        preparation, rows = admit()
        fence("rollback_fence_begin")
        admit()
        saved.update(phase="source_resuming", resume={"started_at": time.time(),
            "completed": [], "inflight": None, "finished_at": None})
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
                "database_switch_authorized": False, "runtime_activation_authorized": False}


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

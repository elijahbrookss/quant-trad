"""Explicit, stopped-worker deadline amendment for the existing migrate operator.

The host deployment lock excludes dispatch while the database audit and immutable
worker request are reconciled. This owner never stops collection, sends commands
into a live worker, extends a phase deadline, or replays an uncertain SQL change.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
import logging
import os
import re
from pathlib import Path
import tempfile
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_launch as launch
from scripts.db import fact_header_v2_deadline as database

STATE = "storage-online-deadline-amendment.json"
REQUEST = "storage-online-request.json"
logger = logging.getLogger(__name__)


def request_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def require_settled(state_root):
    if os.path.lexists(state_root/STATE) and host.load_receipt(state_root/STATE).get("phase") != "complete":
        raise RuntimeError("storage_online_deadline_amendment_requires_reconciliation")


def _check_clock(wall_deadline, monotonic_deadline):
    if time.time() >= wall_deadline or time.monotonic() >= monotonic_deadline:
        raise RuntimeError("storage_online_deadline_amendment_expired_requires_reconciliation")


def _replace(path, before, after):
    # A torn multi-file publication can contain only these exact old/new bytes.
    # An unknown file is never overwritten to force reentry.
    host.load_receipt(path)
    current = path.read_bytes()
    if current == after:
        return
    if current != before:
        raise RuntimeError("storage_online_deadline_file_changed")
    descriptor, name = tempfile.mkstemp(prefix=".storage-deadline-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(after); stream.flush(); os.fsync(stream.fileno())
        os.replace(name, path)
        host.sync_directory(path.parent)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _retired(saved):
    identity = saved["container_id"]
    if not identity or not saved["contract"]:
        raise RuntimeError("storage_online_deadline_owned_worker_required")
    launch._admit(identity, saved["binding"], saved["contract"])
    value = json.loads(host.docker("inspect", "--format", "{{json .State}}", identity))
    if (value.get("Running") is not False or type(value.get("Pid")) is not int or value["Pid"] != 0
            or value.get("Status") not in {"created", "exited"}
            or any(value.get(k) is not False for k in ("Paused", "Restarting", "Dead", "OOMKilled"))):
        raise RuntimeError("storage_online_deadline_worker_must_be_retired")
    return host.database_details(identity)


_PROBE = """
import contextlib,json,os,sys
from sqlalchemy import create_engine,text
args=json.loads(sys.stdin.read(65537))
with contextlib.redirect_stdout(sys.stderr):
    engine=create_engine(os.environ['PG_DSN'],future=True,connect_args={'connect_timeout':5})
    try:
        with engine.begin() as conn:
            if args['action']=='inspect':
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
            conn.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
            conn.exec_driver_sql("SET LOCAL idle_in_transaction_session_timeout='5s'")
            if args['action']=='apply':
                result=amend_capture_deadline(conn,**args['arguments'])
            elif args['action']=='inspect':
                capture.inspect_capture(conn)
                result=dict(capture=conn.scalar(text(f'SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1')),
                            audit=inspect_amendment(conn))
            else:
                raise ValueError('storage_online_deadline_probe_action_invalid')
    finally:
        engine.dispose()
print(json.dumps(result),flush=True)
"""


def _probe(plan, saved, *, action, arguments=None):
    binding = saved["binding"]
    db = host.database_details(binding["database_id"])
    collector = host.database_details(binding["clients"]["market-data-collector"]["id"])
    if (host.database_contract(db) != binding["database_contract"]
            or host.database_contract(collector) != binding["collector_contract"]):
        raise RuntimeError("storage_online_deadline_peer_changed")
    # Only SQL/network capability, no PID namespace, source data or key mounts.
    code = Path(database.__file__).read_text()+"\n"+_PROBE
    return json.loads(host.docker("run", "--rm", "--pull", "never", "--interactive",
        "--network", "container:"+binding["database_id"], "--read-only", "--user", "1000:1000",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--memory", "512m",
        "--cpus", "1", "--pids-limit", "64", "--env", "QT_DISABLE_DOTENV=1", "--env", "PG_DSN",
        "--entrypoint", "python", plan["image"], "-c", code,
        env={**os.environ, "PG_DSN": launch._dsn(db, collector)},
        input=json.dumps(dict(action=action, arguments=arguments)), timeout=30))


def _capacity(value, *, plan_bytes, seconds, deadline, paths, reserve_percent, reserve_floors):
    """Fresh measured forecast plus current filesystem space; not a limit-only pass.

    The private operator input contains named remaining peak allocations, growth,
    reserves and evidence digests. It must cover the amended absolute horizon.
    Normal live phase resource guards still run on reentry and before the switch.
    """
    now = time.time()
    if (set(value) != {"schema_version", "plan_sha256", "attempt_seconds", "through_epoch", "observed_at", "filesystems"}
            or value["schema_version"] != "qt.storage_deadline_capacity.v1"
            or value["plan_sha256"] != _sha(plan_bytes) or value["attempt_seconds"] != seconds
            or value["through_epoch"] != deadline or type(value["observed_at"]) not in (int, float)
            or not 0 <= now-value["observed_at"] <= 600
            or not isinstance(value["filesystems"], list) or len(value["filesystems"]) != 2):
        raise RuntimeError("storage_online_deadline_capacity_stale_or_unbound")
    if {x["path"] for x in value["filesystems"]} != set(paths):
        raise RuntimeError("storage_online_deadline_capacity_paths_changed")
    for item in value["filesystems"]:
        if (set(item) != {"path", "device", "reserve_bytes", "remaining_peak_bytes", "evidence_sha256"}
                or type(item["device"]) is not int or type(item["reserve_bytes"]) is not int
                or item["reserve_bytes"] <= 0 or not isinstance(item["remaining_peak_bytes"], dict)
                or set(item["remaining_peak_bytes"]) != {"targets", "growth", "queue", "wal", "temporary", "maintenance", "recovery"}
                or any(type(v) is not int or v < 0 for v in item["remaining_peak_bytes"].values())
                or not isinstance(item["evidence_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["evidence_sha256"])):
            raise ValueError("storage_online_deadline_capacity_invalid")
        path = launch._canonical(item["path"])
        info = path.stat(); space = os.statvfs(path)
        minimum_reserve = max(reserve_floors[item["path"]], (space.f_blocks*space.f_frsize*reserve_percent+99)//100)
        if (info.st_dev != item["device"] or item["reserve_bytes"] < minimum_reserve
                or space.f_bavail*space.f_frsize < item["reserve_bytes"]+sum(item["remaining_peak_bytes"].values())):
            raise RuntimeError("storage_online_deadline_capacity_insufficient")


def amend_operation(path, *, attempt_seconds, capacity_file, execute):
    from scripts.automation import storage_online_operation as operation

    path = launch._canonical(path)
    plan = operation.load_operation_plan(path)
    root = launch._canonical(plan["state_root"])
    if type(execute) is not bool or type(attempt_seconds) is not int or not 1 <= attempt_seconds <= 96*3600:
        raise ValueError("storage_online_deadline_arguments_invalid")
    operator_sha256 = _sha(Path(__file__).read_bytes()+Path(database.__file__).read_bytes())
    with host.deployment_lock(root):
        for name in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
            if os.path.lexists(root/name):
                raise RuntimeError("storage_online_deadline_pre_final_serving_source_required")
        journal = host.load_receipt(root/STATE) if os.path.lexists(root/STATE) else None
        if journal and (journal["operation_path"] != str(path) or journal["attempt_seconds"] != attempt_seconds
                or journal["operator_sha256"] != operator_sha256):
            raise RuntimeError("storage_online_deadline_amendment_binding_changed")
        if journal and journal["phase"] == "complete":
            return dict(phase="deadline_amendment_recorded", deadline=journal["new_worker"]["deadline"],
                        current_capture_verified=False, migration_started=False)
        if journal:
            check = {k:v for k,v in journal.items() if k != "intent_sha256"}
            check["phase"] = "prepared"
            if (host.digest(check) != journal["intent_sha256"]
                    or journal["phase"] not in {"prepared", "database_dispatched", "database_reconciled"}):
                raise RuntimeError("storage_online_deadline_intent_changed")
            before_plan = journal["old_plan"]
            if plan not in (before_plan, journal["new_plan"]):
                raise RuntimeError("storage_online_deadline_plan_changed")
            saved = journal["old_worker"]
            remaining = min(journal["wall_deadline"]-time.time(), journal["monotonic_deadline"]-time.monotonic())
            if Path("/proc/sys/kernel/random/boot_id").read_text().strip() != journal["boot_id"] or remaining <= 0:
                raise RuntimeError("storage_online_deadline_amendment_expired_requires_reconciliation")
        else:
            before_plan = plan
            saved, _, found = launch.observe_owned_worker(root, plan["project"])
            if not saved or found != [saved["container_id"]] or attempt_seconds <= plan["attempt_seconds"]:
                raise RuntimeError("storage_online_deadline_original_attempt_required")
            remaining = 300
        wall_deadline = journal["wall_deadline"] if journal else time.time()+remaining
        monotonic_deadline = journal["monotonic_deadline"] if journal else time.monotonic()+remaining
        with host.docker_deadline(monotonic_deadline):
            details = _retired(saved)
            arguments = {k: before_plan[k] for k in ("project", "source_revision", "source_image", "image",
                "request", "inventory_path", "keys_root", "socket_volume", "spool_destination")}
            operation.inspect_prepared_operation(root, **arguments,
                deadline=monotonic_deadline, operator_id=saved["container_id"])
            if journal is None:
                observed = _probe(plan, saved, action="inspect")
                original = observed["capture"]
                if observed["audit"] is not None or original["attempt_seconds"] != plan["attempt_seconds"]:
                    raise RuntimeError("storage_online_deadline_capture_requires_reconciliation")
                old_request = host.load_receipt(root/REQUEST)
                if (launch.admit_capture(old_request, saved["capture"]) != saved["deadline"]
                        or datetime.fromisoformat(original["prepared_at"]) != datetime.fromisoformat(saved["capture"]["started_at"])
                        or original["attempt_seconds"] != saved["capture"]["seconds"]
                        or _sha((root/REQUEST).read_bytes()) != saved["binding"]["request_sha256"]):
                    raise RuntimeError("storage_online_deadline_capture_binding_changed")
                if {k:v for k,v in old_request.items() if k != "capture_preparation"} != plan["request"]:
                    raise RuntimeError("storage_online_deadline_request_plan_changed")
                new_request = deepcopy(old_request)
                new_request["capture_preparation"]["attempt_seconds"] = attempt_seconds
                new_worker = deepcopy(saved)
                new_worker.update(container_id=None, contract=None,
                    capture={**saved["capture"], "seconds": attempt_seconds})
                new_worker["deadline"] = launch.admit_capture(new_request, new_worker["capture"])
                digest = _sha(request_bytes(new_request))
                new_worker["binding"]["request_sha256"] = digest
                env = dict(v.split("=", 1) for v in details["config"]["Env"])
                env["QT_ONLINE_REQUEST_SHA256"] = digest
                new_worker["binding"]["environment_sha256"] = host.digest(sorted(k+"="+v for k,v in env.items()))
                capacity = host.load_receipt(launch._canonical(capacity_file))
                paths = [saved["binding"]["mounts"][p]["host_source"] for p in ("/app/logs/market-structure", "/qt-history")]
                _capacity(capacity, plan_bytes=path.read_bytes(), seconds=attempt_seconds, deadline=new_worker["deadline"], paths=paths,
                    reserve_percent=plan["request"]["policy"]["reserve_percent"], reserve_floors={
                        paths[0]: max(plan["limits"]["spool_reserve_bytes"], plan["limits"]["recent_free_bytes"]),
                        paths[1]: plan["limits"]["repository_reserve_bytes"]})
                if not execute:
                    return dict(phase="deadline_amendment_inspected", old_deadline=saved["deadline"],
                                proposed_deadline=new_worker["deadline"], storage_mutations_performed=False)
                new_plan = {**plan, "attempt_seconds": attempt_seconds}
                journal = dict(operation_path=str(path), operator_sha256=operator_sha256, attempt_seconds=attempt_seconds, old_plan=plan, new_plan=new_plan,
                    old_plan_bytes=path.read_text(), old_request_bytes=(root/REQUEST).read_text(),
                    old_worker_bytes=(root/launch._STATE).read_text(), old_worker=saved, new_worker=new_worker,
                    new_request=new_request, expected_capture=original, capacity=capacity,
                    phase="prepared", wall_deadline=wall_deadline, monotonic_deadline=monotonic_deadline,
                    boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
                journal["intent_sha256"] = host.digest(journal)
                if len(json.dumps(journal).encode()) > 65535:
                    raise ValueError("storage_online_deadline_intent_budget_exceeded")
                _check_clock(wall_deadline, monotonic_deadline)
                host.save_receipt(root/STATE, journal, initial=True)
            if not execute:
                return dict(phase="deadline_amendment_reconciliation_required", storage_mutations_performed=False)
            if journal["phase"] == "prepared":
                paths = [saved["binding"]["mounts"][p]["host_source"] for p in ("/app/logs/market-structure", "/qt-history")]
                _capacity(journal["capacity"], plan_bytes=journal["old_plan_bytes"].encode(), seconds=attempt_seconds,
                    deadline=journal["new_worker"]["deadline"], paths=paths,
                    reserve_percent=before_plan["request"]["policy"]["reserve_percent"], reserve_floors={
                        paths[0]: max(before_plan["limits"]["spool_reserve_bytes"], before_plan["limits"]["recent_free_bytes"]),
                        paths[1]: before_plan["limits"]["repository_reserve_bytes"]})
                _check_clock(wall_deadline, monotonic_deadline)
                journal["phase"] = "database_dispatched"
                host.save_receipt(root/STATE, journal, initial=False)
                logger.info("storage_online_deadline_dispatch | intent=%s old_seconds=%s new_seconds=%s",
                    journal["intent_sha256"], journal["expected_capture"]["attempt_seconds"], attempt_seconds)
                _check_clock(wall_deadline, monotonic_deadline)
                _probe(before_plan, saved, action="apply", arguments=dict(expected_capture=journal["expected_capture"],
                    attempt_seconds=attempt_seconds, intent_sha256=journal["intent_sha256"]))
            actual = _probe(before_plan, saved, action="inspect")
            expected_after = {**journal["expected_capture"], "attempt_seconds": attempt_seconds}
            expected_audit = dict(schema_version="qt.fact_header_deadline_amendment.v1",
                intent_sha256=journal["intent_sha256"], before=journal["expected_capture"], after=expected_after)
            if actual != dict(capture=expected_after, audit=expected_audit):
                raise RuntimeError("storage_online_deadline_database_outcome_unresolved_no_replay")
            _check_clock(wall_deadline, monotonic_deadline)
            journal["phase"] = "database_reconciled"
            host.save_receipt(root/STATE, journal, initial=False)
            _retired(saved)
            # Preserve the stopped container and original private bytes. Reentry
            # creates a worker with the same image and newly bound request.
            archived = before_plan["project"]+"-storage-online-before-"+journal["intent_sha256"][:16]
            name = host.docker("inspect", "--format", "{{.Name}}", saved["container_id"]).strip()
            if name == "/"+before_plan["project"]+"-storage-online":
                _check_clock(wall_deadline, monotonic_deadline)
                host.docker("rename", saved["container_id"], archived)
            elif name != "/"+archived:
                raise RuntimeError("storage_online_deadline_worker_name_changed")
            _check_clock(wall_deadline, monotonic_deadline)
            _replace(root/REQUEST, journal["old_request_bytes"].encode(), request_bytes(journal["new_request"]))
            _check_clock(wall_deadline, monotonic_deadline)
            _replace(root/launch._STATE, journal["old_worker_bytes"].encode(),
                (json.dumps(journal["new_worker"], sort_keys=True)+"\n").encode())
            _check_clock(wall_deadline, monotonic_deadline)
            _replace(path, journal["old_plan_bytes"].encode(),
                (json.dumps(journal["new_plan"], sort_keys=True)+"\n").encode())
            _check_clock(wall_deadline, monotonic_deadline)
            journal["phase"] = "complete"
            host.save_receipt(root/STATE, journal, initial=False)
            logger.info("storage_online_deadline_amended | intent=%s worker_preserved=true migration_started=false", journal["intent_sha256"])
            return dict(phase="deadline_amended", deadline=journal["new_worker"]["deadline"],
                original_started_at=journal["expected_capture"]["prepared_at"], migration_started=False)

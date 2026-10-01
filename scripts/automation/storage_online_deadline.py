"""Explicit stopped-worker amendments for the existing migrate operator.

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
from scripts.automation import storage_online_runtime as runtime
from scripts.db import fact_header_v2_deadline as database

STATE = "storage-online-deadline-amendment.json"
PACKAGE_STATE = "storage-online-package-amendment.json"
PACKAGE_JOURNAL_BYTES = 2*1024**2
REQUEST = "storage-online-request.json"
logger = logging.getLogger(__name__)


def request_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode()


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def require_settled(state_root):
    for name, kind in ((STATE, "deadline"), (PACKAGE_STATE, "package")):
        if os.path.lexists(state_root/name) and host.load_receipt(state_root/name, max_bytes=PACKAGE_JOURNAL_BYTES if name == PACKAGE_STATE else 65536).get("phase") != "complete":
            raise RuntimeError("storage_online_"+kind+"_amendment_requires_reconciliation")


def _check_clock(wall_deadline, monotonic_deadline):
    if time.time() >= wall_deadline or time.monotonic() >= monotonic_deadline:
        raise RuntimeError("storage_online_deadline_amendment_expired_requires_reconciliation")


def _replace(path, before, after):
    # A torn multi-file publication can contain only these exact old/new bytes.
    # An unknown file is never overwritten to force reentry.
    host.load_receipt(path, max_bytes=max(65536,len(before),len(after)))
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
            if args['action'] in {'inspect','package_inspect'}:
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
            elif args['action']=='package_inspect':
                from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
                from scripts.db import fact_header_v2_online_proof as protection
                from scripts.db.fact_header_v2_admission import assert_v1_source_admission
                with capture.migration_step(conn,5):
                    if not conn.scalar(text('SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))'),
                                       {'name':CONTROLLER_LOCK}):
                        raise RuntimeError('storage_online_package_controller_active')
                    assert_v1_source_admission(conn,identity_capture=False)
                    result=dict(capture=conn.scalar(text(f'SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1')),
                        header=conn.scalar(text(f'SELECT to_jsonb(c) FROM {headers.STATE} c WHERE id=1')),
                        raw=conn.scalar(text(f'SELECT to_jsonb(c) FROM {raw.STATE} c WHERE id=1')),
                        proof=protection.inspect_protection(conn))
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
        if os.path.lexists(root/PACKAGE_STATE) and host.load_receipt(root/PACKAGE_STATE, max_bytes=PACKAGE_JOURNAL_BYTES).get("phase") != "complete":
            raise RuntimeError("storage_online_package_amendment_requires_reconciliation")
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


def _unstarted_header_capture(observed, saved, request):
    header, raw = observed["header"], observed["raw"]
    capture = observed["capture"]
    if (not header.get("identity_baseline_complete") or not header["identity_history_ready"]
            or header["identity_capture"] or header["baseline_complete"] or header["verified_rows"] != 0
            or header.get("header_id_order", False)
            or any(header["after_"+k] is not None for k in ("day", "seq", "id"))
            or not raw["baseline_complete"] or not raw["history_ready"]
            or datetime.fromisoformat(capture["prepared_at"]) != datetime.fromisoformat(saved["capture"]["started_at"])
            or capture["attempt_seconds"] != saved["capture"]["seconds"]
            or launch.admit_capture(request, saved["capture"]) != saved["deadline"]):
        raise RuntimeError("storage_online_package_unstarted_header_capture_required")


def amend_worker_package(path, *, package_file, execute):
    """Replace one retired pre-header worker package without any database writes.

    This shares the existing stopped-worker/file-publication boundary with the
    deadline amendment. The manifest changes only candidate image/provenance;
    original capture, placement, budgets, source fleet and private paths remain.
    Partial publication is reconciled from exact old/new bytes, never guessed.
    """
    from scripts.automation import storage_online_operation as operation

    path = launch._canonical(path)
    plan = operation.load_operation_plan(path)
    root = launch._canonical(plan["state_root"])
    package = host.load_receipt(launch._canonical(package_file))
    if (type(execute) is not bool or set(package) != {"schema_version", "plan_sha256", "image", "source_revision", "source_tree_hash"}
            or package["schema_version"] != "qt.storage_online_package.v1"
            or any(not isinstance(package[k], str) or not re.fullmatch(pattern, package[k])
                   for k,pattern in (("plan_sha256",r"[0-9a-f]{64}"),("image",r"sha256:[0-9a-f]{64}"),
                       ("source_revision",r"[0-9a-f]{40}"),("source_tree_hash",r"[0-9a-f]{64}")))):
        raise ValueError("storage_online_package_manifest_invalid")
    operator_sha256 = _sha(Path(__file__).read_bytes()+Path(database.__file__).read_bytes())
    with host.deployment_lock(root):
        if os.path.lexists(root/STATE) and host.load_receipt(root/STATE).get("phase") != "complete":
            raise RuntimeError("storage_online_deadline_amendment_requires_reconciliation")
        for name in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
            if os.path.lexists(root/name):
                raise RuntimeError("storage_online_package_pre_final_serving_source_required")
        journal = host.load_receipt(root/PACKAGE_STATE, max_bytes=PACKAGE_JOURNAL_BYTES) if os.path.lexists(root/PACKAGE_STATE) else None
        if journal:
            if (journal["operation_path"] != str(path) or journal["package"] != package
                    or journal["operator_sha256"] != operator_sha256):
                raise RuntimeError("storage_online_package_amendment_binding_changed")
            if journal["phase"] == "complete":
                return dict(phase="package_amendment_recorded", migration_started=False, current_capture_verified=False)
            check = {k:v for k,v in journal.items() if k != "intent_sha256"}; check["phase"] = "prepared"
            if (host.digest(check) != journal["intent_sha256"] or journal["phase"] not in {"prepared", "publishing"}
                    or plan not in (journal["old_plan"], journal["new_plan"])):
                raise RuntimeError("storage_online_package_intent_changed")
            if Path("/proc/sys/kernel/random/boot_id").read_text().strip() != journal["boot_id"]:
                raise RuntimeError("storage_online_package_reboot_requires_reconciliation")
            before_plan, saved = journal["old_plan"], journal["old_worker"]
            wall_deadline, monotonic_deadline = journal["wall_deadline"], journal["monotonic_deadline"]
        else:
            if _sha(path.read_bytes()) != package["plan_sha256"] or package["image"] == plan["image"]:
                raise RuntimeError("storage_online_package_original_plan_required")
            saved, _, found = launch.observe_owned_worker(root, plan["project"])
            if not saved or found != [saved["container_id"]]:
                raise RuntimeError("storage_online_package_original_worker_required")
            before_plan = plan
            wall_deadline = min(time.time()+300, saved["deadline"])
            monotonic_deadline = time.monotonic()+max(0, wall_deadline-time.time())
        _check_clock(wall_deadline, monotonic_deadline)
        with host.docker_deadline(monotonic_deadline):
            details = _retired(saved)
            old_request = (json.loads(journal["old_request_bytes"]) if journal else host.load_receipt(root/REQUEST))
            if ({k:v for k,v in old_request.items() if k != "capture_preparation"} != before_plan["request"]
                    or _sha(request_bytes(old_request)) != saved["binding"]["request_sha256"]):
                raise RuntimeError("storage_online_package_original_request_changed")
            new_request = {**old_request, **{k:package[k] for k in ("source_revision", "source_tree_hash")}}
            new_plan = {**before_plan, "image":package["image"],
                        "request":{k:v for k,v in new_request.items() if k != "capture_preparation"}}
            runtime_path = root/runtime.RUNTIME_RECIPE
            current_runtime = host.load_receipt(runtime_path, max_bytes=524288)
            old_runtime_bytes = journal["old_runtime_bytes"] if journal else runtime_path.read_text()
            old_runtime = json.loads(old_runtime_bytes) if journal else current_runtime
            new_runtime = deepcopy(old_runtime)
            for name in runtime._APPLICATIONS:
                if old_runtime["services"][name].get("image") != before_plan["image"]:
                    raise RuntimeError("storage_online_package_original_runtime_image_changed")
                new_runtime["services"][name]["image"] = new_plan["image"]
            new_runtime_bytes = (json.dumps(new_runtime,sort_keys=True)+"\n").encode()
            if len(new_runtime_bytes) > 524288 or runtime_path.read_bytes() not in (old_runtime_bytes.encode(),new_runtime_bytes):
                raise RuntimeError("storage_online_package_runtime_recipe_changed")
            for candidate, proposed in ((before_plan, old_runtime), (new_plan, new_runtime)):
                arguments = {k:candidate[k] for k in ("project", "source_revision", "source_image", "image",
                    "request", "inventory_path", "keys_root", "socket_volume", "spool_destination")}
                operation.inspect_prepared_operation(root, **arguments,
                    deadline=monotonic_deadline, operator_id=saved["container_id"], proposed_runtime_recipe=proposed)
            old_observed = _probe(before_plan, saved, action="package_inspect")
            new_observed = _probe(new_plan, saved, action="package_inspect")
            _unstarted_header_capture(old_observed, saved, old_request)
            if new_observed != old_observed:
                raise RuntimeError("storage_online_package_existing_proof_changed")
            observed_sha256 = host.digest(old_observed)
            if journal and (journal["observed_sha256"] != observed_sha256 or journal["new_plan"] != new_plan):
                raise RuntimeError("storage_online_package_progress_changed")
            if not execute:
                return dict(phase="package_amendment_reconciliation_required" if journal else "package_amendment_inspected",
                            storage_mutations_performed=False, original_deadline=saved["deadline"], migration_started=False)
            if journal is None:
                if (operation.load_operation_plan(path) != before_plan
                        or _sha(path.read_bytes()) != package["plan_sha256"]
                        or _sha((root/REQUEST).read_bytes()) != saved["binding"]["request_sha256"]
                        or host.load_receipt(root/launch._STATE) != saved
                        or runtime_path.read_bytes() != old_runtime_bytes.encode()):
                    raise RuntimeError("storage_online_package_original_files_changed")
                image_env = launch.inspect_candidate_image(new_plan["image"], new_request)
                old_env = dict(v.split("=",1) for v in details["config"]["Env"])
                overrides = {k:old_env[k] for k in ("PG_DSN", "QT_DISABLE_DOTENV", "QT_ARCHIVE_SHARED_GROUP_ID",
                    "QT_LOGGING_LOKI_URL", "QT_STORAGE_UDEV_ROOT")}
                digest = _sha(request_bytes(new_request)); overrides["QT_ONLINE_REQUEST_SHA256"] = digest
                new_worker = deepcopy(saved); new_worker.update(container_id=None, contract=None)
                new_worker["binding"].update(image=new_plan["image"], request_sha256=digest,
                    environment_sha256=host.digest(sorted(k+"="+v for k,v in {**image_env,**overrides}.items())))
                journal = dict(operation_path=str(path), operator_sha256=operator_sha256, package=package,
                    old_plan=before_plan, new_plan=new_plan, old_plan_bytes=path.read_text(),
                    old_request_bytes=(root/REQUEST).read_text(), old_worker_bytes=(root/launch._STATE).read_text(),
                    old_worker=saved, new_worker=new_worker, new_request=new_request, observed_sha256=observed_sha256,
                    old_runtime_bytes=old_runtime_bytes, new_runtime=new_runtime,
                    phase="prepared", wall_deadline=wall_deadline, monotonic_deadline=monotonic_deadline,
                    boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
                journal["intent_sha256"] = host.digest(journal)
                if len(json.dumps(journal).encode()) > PACKAGE_JOURNAL_BYTES-1:
                    raise ValueError("storage_online_package_intent_budget_exceeded")
                _check_clock(wall_deadline, monotonic_deadline)
                host.save_receipt(root/PACKAGE_STATE, journal, initial=True)
            journal["phase"] = "publishing"
            host.save_receipt(root/PACKAGE_STATE, journal, initial=False)
            _retired(saved)
            archived = before_plan["project"]+"-storage-online-package-before-"+journal["intent_sha256"][:16]
            name = host.docker("inspect", "--format", "{{.Name}}", saved["container_id"]).strip()
            _check_clock(wall_deadline, monotonic_deadline)
            if name == "/"+before_plan["project"]+"-storage-online":
                host.docker("rename", saved["container_id"], archived)
            elif name != "/"+archived:
                raise RuntimeError("storage_online_package_worker_name_changed")
            for target,before,after in (
                (runtime_path,journal["old_runtime_bytes"].encode(),new_runtime_bytes),
                (root/REQUEST,journal["old_request_bytes"].encode(),request_bytes(journal["new_request"])),
                (root/launch._STATE,journal["old_worker_bytes"].encode(),(json.dumps(journal["new_worker"],sort_keys=True)+"\n").encode()),
                (path,journal["old_plan_bytes"].encode(),(json.dumps(journal["new_plan"],sort_keys=True)+"\n").encode())):
                _check_clock(wall_deadline, monotonic_deadline)
                _replace(target,before,after)
            _check_clock(wall_deadline, monotonic_deadline)
            journal["phase"] = "complete"
            host.save_receipt(root/PACKAGE_STATE, journal, initial=False)
            logger.info("storage_online_package_amended | intent=%s worker_preserved=true migration_started=false",journal["intent_sha256"])
            return dict(phase="package_amended", original_deadline=saved["deadline"], migration_started=False)

"""Explicit stopped-worker amendments of the current forward operation.

V1 keeps the adoption deadline. One V2 follow-up may extend its cumulative
bound with fresh capacity evidence. Explicit guarded continuation can retain
completed proof after expiry. Neither copies data, stops
source clients, restarts workers or resets proof, start times or phase clocks.
"""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
import logging
import os
from pathlib import Path
import re
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_deadline as publication
from scripts.automation import storage_online_forward as forward
from scripts.automation import storage_online_forward_worker as worker
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_runtime as runtime

MAX_BYTES = 2*1024**2
LOG = logging.getLogger(__name__)


def state_file(operation_sha256, *, version=1):
    if not isinstance(operation_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", operation_sha256):
        raise ValueError("storage_forward_reschedule_operation_invalid")
    if type(version) is not int or version not in (1, 2):
        raise ValueError("storage_forward_reschedule_version_invalid")
    suffix = "" if version == 1 else "-v2"
    return "storage-online-forward-reschedule-"+operation_sha256+suffix+".json"


def _bytes(value):
    return (json.dumps(value, sort_keys=True)+"\n").encode()


def _proposal(plan, request, package):
    fields = {"schema_version", "plan_sha256", "operation_sha256", "image", "source_revision",
        "source_tree_hash", "end_day", "max_objects", "descriptor_limit", "forward_plan_path", "capacity_file"}
    extension = isinstance(package, dict) and package.get("schema_version") == "qt.storage_online_forward_reschedule_package.v2"
    if extension:
        fields |= {"previous_amendment_sha256", "attempt_seconds"}
        if "continue_guarded_proof" in package:
            fields.add("continue_guarded_proof")
            if package["continue_guarded_proof"] is not True or package.get("raw_mapping_mode") != "retain_source":
                raise ValueError("storage_forward_guarded_continuation_invalid")
        if "raw_mapping_mode" in package:
            fields.add("raw_mapping_mode")
            if package["raw_mapping_mode"] != "retain_source":
                raise ValueError("storage_forward_reschedule_raw_mapping_mode_invalid")
    if (not isinstance(package, dict) or set(package) != fields
            or package["schema_version"] not in {"qt.storage_online_forward_reschedule_package.v1", "qt.storage_online_forward_reschedule_package.v2"}
            or any(not isinstance(package[k], str) or not re.fullmatch(pattern, package[k]) for k,pattern in (
                ("plan_sha256", r"[0-9a-f]{64}"), ("operation_sha256", r"[0-9a-f]{64}"),
                ("image", r"sha256:[0-9a-f]{64}"), ("source_revision", r"[0-9a-f]{40}"),
                ("source_tree_hash", r"[0-9a-f]{64}")))
            or (not extension and "forward_reschedule" in request)
            or worker.request_binding(request) is None
            or request["forward"]["schema_version"] != "qt.storage_online_forward_intent.v2"
            or request["forward"]["operation_sha256"] != package["operation_sha256"]):
        raise ValueError("storage_forward_reschedule_package_invalid")
    previous = request.get("forward_reschedule", {})
    if extension and (previous.get("schema_version") != "qt.storage_online_forward_reschedule.v1"
            or not isinstance(package["previous_amendment_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", package["previous_amendment_sha256"])
            or package["end_day"] <= previous["end_day"]
            or type(package["max_objects"]) is not int or package["max_objects"] < request["max_objects"]):
        raise ValueError("storage_forward_reschedule_predecessor_invalid")
    original = worker.original_request(request)
    revised = {**deepcopy(request), **{k:package[k] for k in ("source_revision", "source_tree_hash", "max_objects")},
        "forward_reschedule":dict(schema_version="qt.storage_online_forward_reschedule.v1",
            original_request_sha256=host.digest(original), original_max_objects=original["max_objects"],
            end_day=package["end_day"])}
    if extension:
        revised["forward_reschedule"].update(schema_version="qt.storage_online_forward_reschedule.v2",
            previous_request_sha256=host.digest(request), attempt_seconds=package["attempt_seconds"])
    if extension and "continue_guarded_proof" in package:
        revised["forward_reschedule"]["continue_guarded_proof"] = True
    if extension and "raw_mapping_mode" in package:
        revised["forward_reschedule"]["raw_mapping_mode"] = package["raw_mapping_mode"]
    worker.request_binding(revised)
    # Increasing the descriptor ceiling pays only for the explicitly increased
    # immutable-object inventory; it cannot raise memory, bytes or other limits.
    if (type(package["descriptor_limit"]) is not int
            or package["descriptor_limit"]-revised["max_objects"] != plan["descriptor_limit"]-request["max_objects"]):
        raise ValueError("storage_forward_reschedule_descriptor_scope_changed")
    result = {**deepcopy(plan), "image":package["image"], "request":revised,
        "descriptor_limit":package["descriptor_limit"]}
    if extension:
        result["attempt_seconds"] = worker.adoption_seconds(revised)
    launch.validate_launch_inputs(**{k:result[k] for k in
        ("project", "source_revision", "image", "request", "descriptor_limit", "memory_bytes")})
    return result, revised


def _new_worker(before, *, package, request, environment_sha256):
    result = deepcopy(before)
    result.update(container_id=None, contract=None)
    result["binding"].update(image=package["image"], request_sha256=publication._sha(publication.request_bytes(request)),
        descriptor_limit=package["descriptor_limit"], environment_sha256=environment_sha256)
    if package["schema_version"] == "qt.storage_online_forward_reschedule_package.v2":
        result["deadline"] += worker.adoption_seconds(request)-request["forward"]["original_capture"]["attempt_seconds"]
    return result


def _new_launch(journal):
    result = deepcopy(journal["old_launch"])
    result["binding"] = dict(publication_sha256=journal["intent_sha256"],
        request_sha256=journal["new_worker"]["binding"]["request_sha256"],
        worker_binding_sha256=host.digest(journal["new_worker"]["binding"]),
        operation_sha256=journal["package"]["operation_sha256"])
    result.update(worker=deepcopy(journal["new_worker"]), pending_worker=None)
    sql = _after_sql(journal)
    result["initialization"] = {k:v for k,v in sql["initialization"].items() if k != "id"}
    result["capture"] = sql["capture"]
    result["forward"], result["deadline"] = forward.admit_adoption_observation(journal["new_request"],
        initialization=result["initialization"], capture=result["capture"], now=journal["started_at"])
    return result


def _after_sql(journal):
    result = deepcopy(journal["sql_before"])
    result["initialization"]["binding"] = worker.initialization_binding(journal["new_request"])
    if journal["package"]["schema_version"] == "qt.storage_online_forward_reschedule_package.v2":
        seconds = worker.adoption_seconds(journal["new_request"])
        expiry = datetime.fromisoformat(result["capture"]["started_at"])+timedelta(seconds=seconds)
        result["capture"].update(attempt_seconds=seconds, expires_at=expiry.isoformat())
    if journal["new_request"].get("forward_reschedule", {}).get("raw_mapping_mode") == "retain_source":
        result["raw_mapping_mode"] = "retain_source"
    return result


def _window(journal):
    boundary = datetime.fromisoformat(journal["package"]["end_day"]).replace(tzinfo=timezone.utc)
    old_expiry = datetime.fromisoformat(journal["sql_before"]["capture"]["expires_at"])
    expiry = datetime.fromisoformat(_after_sql(journal)["capture"]["expires_at"])
    if (not max(time.time(), journal["wall_deadline"]) < boundary.timestamp()
            or boundary+timedelta(seconds=journal["new_plan"]["limits"]["final_seconds"]) > expiry):
        raise RuntimeError("storage_forward_reschedule_outside_original_deadline")
    forward._launch_clock(journal["old_launch"])
    deadline = expiry if journal["package"].get("continue_guarded_proof") is True else old_expiry
    forward._launch_remaining(journal["old_launch"], deadline.timestamp())


def _amendment_base(root, base, package):
    """V2 has exactly one named completed V1 predecessor, never a latest chain."""
    if package.get("schema_version") != "qt.storage_online_forward_reschedule_package.v2":
        return base
    path = root/state_file(base["forward"]["operation_sha256"])
    previous = host.load_receipt(path, max_bytes=MAX_BYTES)
    if previous.get("schema_version") != "qt.storage_online_forward_reschedule_publication.v1":
        raise RuntimeError("storage_forward_reschedule_predecessor_changed")
    previous = _verify_journal(root, base, previous)
    if (previous["phase"] != "complete"
            or previous["schema_version"] != "qt.storage_online_forward_reschedule_publication.v1"
            or publication._sha(path.read_bytes()) != package["previous_amendment_sha256"]):
        raise RuntimeError("storage_forward_reschedule_predecessor_changed")
    return {**base, **{k:previous[k] for k in
        ("package", "new_plan", "new_request", "new_worker", "new_runtime", "old_worker", "intent_sha256")}}


def _verify_journal(root, base, journal):
    fields = {"schema_version", "package", "operator_sha256", "original_publication_sha256", "original_plan_bytes",
        "old_request_bytes", "old_worker_bytes", "old_worker", "old_runtime_bytes", "old_launch_bytes", "old_launch",
        "new_plan", "new_request", "new_worker", "new_runtime", "sql_before", "capacity",
        "started_at", "started_monotonic", "wall_deadline", "monotonic_deadline", "boot_id", "phase", "intent_sha256"}
    extension = journal.get("schema_version") == "qt.storage_online_forward_reschedule_publication.v2"
    if (set(journal) != fields or journal["schema_version"] not in {
            "qt.storage_online_forward_reschedule_publication.v1", "qt.storage_online_forward_reschedule_publication.v2"}
            or journal["phase"] not in {"prepared", "database_dispatched", "database_reconciled", "publishing", "complete"}
            or host.digest({k:v for k,v in journal.items() if k not in {"intent_sha256", "phase"}}) != journal["intent_sha256"]
            or extension != (journal["package"].get("schema_version") == "qt.storage_online_forward_reschedule_package.v2")):
        raise RuntimeError("storage_forward_reschedule_journal_changed")
    base = _amendment_base(root, base, journal["package"])
    if journal["original_publication_sha256"] != base["intent_sha256"]:
        raise RuntimeError("storage_forward_reschedule_journal_changed")
    original_path = Path(base["package"]["forward_plan_path"])
    package = journal["package"]
    plan = json.loads(journal["original_plan_bytes"])
    proposed, request = _proposal(plan, base["new_request"], package)
    old_worker = journal["old_worker"]
    old_launch = journal["old_launch"]
    forward._launch_shape(old_launch, base["forward"])
    sql = journal["sql_before"]
    old_initial = {k:v for k,v in sql["initialization"].items() if k != "id"}
    owner, expiry = forward.admit_adoption_observation(base["new_request"], initialization=old_initial,
        capture=sql["capture"], now=journal["started_at"],
        allow_expired=package.get("continue_guarded_proof") is True)
    expected_binding = dict(publication_sha256=base["intent_sha256"],
        request_sha256=old_worker["binding"]["request_sha256"],
        worker_binding_sha256=host.digest(old_worker["binding"]), operation_sha256=package["operation_sha256"])
    env = journal["new_worker"]["binding"].get("environment_sha256")
    if (not isinstance(env,str) or not re.fullmatch(r"[0-9a-f]{64}",env)
            or original_path.read_text() != journal["original_plan_bytes"]
            or publication._sha(original_path.read_bytes()) != package["plan_sha256"]
            or plan != base["new_plan"] or journal["new_plan"] != proposed or journal["new_request"] != request
            or journal["old_request_bytes"].encode() != publication.request_bytes(base["new_request"])
            or json.loads(journal["old_worker_bytes"]) != old_worker
            or json.loads(journal["old_launch_bytes"]) != old_launch
            or json.loads(journal["old_runtime_bytes"]) != base["new_runtime"]
            or old_worker["binding"] != base["new_worker"]["binding"]
            or old_worker["deadline"] != expiry
            or old_launch["worker"] != old_worker or old_launch["pending_worker"] is not None
            or old_launch["binding"] != expected_binding
            or old_launch["initialization"] != old_initial or old_launch["capture"] != sql["capture"]
            or old_launch["forward"] != owner or old_launch["deadline"] != expiry
            or journal["new_worker"] != _new_worker(old_worker, package=package, request=request, environment_sha256=env)
            or journal["new_runtime"] != forward._published_runtime(base["new_runtime"], plan, package)):
        raise RuntimeError("storage_forward_reschedule_binding_changed")
    forward._new_plan_path(package["forward_plan_path"], original_path)
    return journal


def inspect_published(root, base, *, request=None, operation_path=None):
    """Verify both original evidence and the exact completed publication."""
    forward._inspect_publication(root, base, active=False)
    path = root/state_file(base["forward"]["operation_sha256"], version=2)
    if not os.path.lexists(path):
        path = root/state_file(base["forward"]["operation_sha256"])
    journal = _verify_journal(root, base, host.load_receipt(path, max_bytes=MAX_BYTES))
    if journal["phase"] != "complete":
        raise RuntimeError("storage_forward_reschedule_reconciliation_required")
    new_path = Path(journal["package"]["forward_plan_path"])
    if ((request is not None and request != journal["new_request"])
            or (operation_path is not None and launch._canonical(operation_path) != new_path)):
        raise RuntimeError("storage_forward_reschedule_request_changed")
    for target, expected in (
        (new_path,_bytes(journal["new_plan"])),
        (root/publication.REQUEST,publication.request_bytes(journal["new_request"])),
        (root/runtime.RUNTIME_RECIPE,_bytes(journal["new_runtime"]))):
        host.load_receipt(target,max_bytes=MAX_BYTES)
        if target.read_bytes() != expected:
            raise RuntimeError("storage_forward_reschedule_published_file_changed")
    current_launch = forward.load_launch(root, request=journal["new_request"])
    expected_launch = _new_launch(journal)
    # Only container publication progresses after this completed initializer.
    # Every clock and SQL observation stays anchored to the retained preimage.
    if ({k:v for k,v in current_launch.items() if k not in {"worker", "pending_worker"}}
            != {k:v for k,v in expected_launch.items() if k not in {"worker", "pending_worker"}}):
        raise RuntimeError("storage_forward_reschedule_launch_changed")
    # The launch owner checks its mutable worker progression, with original
    # clocks. Returning this view never manufactures a new launch receipt.
    return {**base, **{k:journal[k] for k in ("new_plan", "new_request", "new_worker", "new_runtime", "old_worker", "intent_sha256")}}


_PROBE = """
import contextlib,json,os,sys
from sqlalchemy import create_engine,text
from scripts.automation.storage_online_forward_worker import inspect_reschedule,reschedule_initialization
payload=sys.stdin.buffer.read(1048577)
if len(payload)>1048576:raise ValueError('storage_forward_reschedule_probe_budget')
args=json.loads(payload)
with contextlib.redirect_stdout(sys.stderr):
 engine=create_engine(os.environ['PG_DSN'],future=True,connect_args={'connect_timeout':5})
 try:
  with engine.begin() as conn:
   if args['action']=='inspect':conn.exec_driver_sql('SET TRANSACTION READ ONLY')
   conn.exec_driver_sql("SET LOCAL statement_timeout='10s'")
   conn.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
   conn.exec_driver_sql("SET LOCAL idle_in_transaction_session_timeout='10s'")
   actual=conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
   if actual!=args['request']['database_identity']:raise RuntimeError('storage_forward_reschedule_database_changed')
   if args['action']=='inspect':result=inspect_reschedule(conn,request=args['request'])
   elif args['action']=='apply':result=reschedule_initialization(conn,request=args['request'],expected=args['expected'],final_seconds=args['final_seconds'])
   else:raise ValueError('storage_forward_reschedule_probe_action_invalid')
 finally:engine.dispose()
output=json.dumps(result)
if len(output.encode())>1048576:raise ValueError('storage_forward_reschedule_probe_result_budget')
print(output,flush=True)
"""


def _probe(plan, saved, *, request, action, expected=None):
    binding = saved["binding"]
    db = host.database_details(binding["database_id"])
    collector = host.database_details(binding["clients"]["market-data-collector"]["id"])
    if (host.database_contract(db) != binding["database_contract"]
            or host.database_contract(collector) != binding["collector_contract"]):
        raise RuntimeError("storage_forward_reschedule_peer_changed")
    payload = json.dumps(dict(action=action, request=request, expected=expected,
        final_seconds=plan["limits"]["final_seconds"]))
    if len(payload.encode()) > 1048576:
        raise ValueError("storage_forward_reschedule_probe_budget")
    # SQL only: no data/key mounts, database PID namespace or inherited source
    # capabilities. The SQL owner additionally rejects an active controller.
    return json.loads(host.docker("run", "--rm", "--pull", "never", "--interactive",
        "--network", "container:"+binding["database_id"], "--read-only", "--user", "1000:1000",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--memory", "512m",
        "--memory-swap", "512m", "--cpus", "1", "--pids-limit", "64",
        "--env", "QT_DISABLE_DOTENV=1", "--env", "QT_LOGGING_LOKI_URL=", "--env", "PG_DSN",
        "--entrypoint", "python", plan["image"], "-c", _PROBE,
        env={**os.environ, "PG_DSN":launch._dsn(db,collector)}, input=payload, timeout=30))


def _capacity(journal):
    plan = json.loads(journal["original_plan_bytes"])
    saved = journal["old_worker"]
    paths = [saved["binding"]["mounts"][key]["host_source"] for key in ("/app/logs/market-structure", "/qt-history")]
    capture = _after_sql(journal)["capture"]
    publication._capacity(journal["capacity"], plan_bytes=journal["original_plan_bytes"].encode(),
        seconds=capture["attempt_seconds"], deadline=datetime.fromisoformat(capture["expires_at"]).timestamp(),
        paths=paths, reserve_percent=plan["request"]["policy"]["reserve_percent"], reserve_floors={
            paths[0]:max(plan["limits"]["spool_reserve_bytes"],plan["limits"]["recent_free_bytes"]),
            paths[1]:plan["limits"]["repository_reserve_bytes"]})


def _preimages(root, journal):
    operation = journal["package"]["operation_sha256"]
    for path, before, after in (
        (root/publication.REQUEST,journal["old_request_bytes"].encode(),publication.request_bytes(journal["new_request"])),
        (root/runtime.RUNTIME_RECIPE,journal["old_runtime_bytes"].encode(),_bytes(journal["new_runtime"])),
        (root/launch._STATE,journal["old_worker_bytes"].encode(),_bytes(journal["new_worker"])),
        (root/forward.operation_file(forward.LAUNCH_STATE,operation_sha256=operation),
            journal["old_launch_bytes"].encode(),_bytes(_new_launch(journal)))):
        host.load_receipt(path,max_bytes=MAX_BYTES)
        if path.read_bytes() not in (before,after):
            raise RuntimeError("storage_forward_reschedule_preimage_changed")
    target = Path(journal["package"]["forward_plan_path"])
    if os.path.lexists(target):
        host.load_receipt(target,max_bytes=MAX_BYTES)
        if target.read_bytes() != _bytes(journal["new_plan"]):
            raise RuntimeError("storage_forward_reschedule_plan_changed")


def publish(path, *, package_file, execute=False):
    """Inspect or perform one journaled request change after worker retirement."""
    from scripts.automation import storage_online_operation as operation

    if type(execute) is not bool:
        raise ValueError("storage_forward_reschedule_execute_invalid")
    path = launch._canonical(path)
    plan = operation.load_operation_plan(path)
    root = launch._canonical(plan["state_root"])
    package = host.load_receipt(launch._canonical(package_file))
    # Read a specifically named original publication, never the current/latest
    # request when recovering a torn multi-file change.
    extension = package.get("schema_version") == "qt.storage_online_forward_reschedule_package.v2"
    name = state_file(package.get("operation_sha256"), version=2 if extension else 1)
    base_path = root/forward.operation_file(forward.STATE,operation_sha256=package["operation_sha256"])
    original_base = host.load_receipt(base_path,max_bytes=MAX_BYTES)
    operator = publication._sha(b"".join(Path(module.__file__).read_bytes() for module in
        (publication, forward, worker, launch, operation, host, runtime, forward.terminal)) + Path(__file__).read_bytes())
    with host.deployment_lock(root):
        forward._inspect_publication(root,original_base,active=False)
        base = _amendment_base(root,original_base,package)
        if Path(base["package"]["forward_plan_path"]) != path or plan != base["new_plan"]:
            raise RuntimeError("storage_forward_reschedule_original_plan_required")
        proposed, request = _proposal(plan,base["new_request"],package)
        new_path = forward._new_plan_path(package["forward_plan_path"],path)
        if publication._sha(path.read_bytes()) != package["plan_sha256"]:
            raise RuntimeError("storage_forward_reschedule_original_plan_changed")
        for marker in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
            if os.path.lexists(root/marker):
                raise RuntimeError("storage_forward_reschedule_pre_final_required")
        journal = host.load_receipt(root/name,max_bytes=MAX_BYTES) if os.path.lexists(root/name) else None
        if journal is not None:
            _verify_journal(root,original_base,journal)
            if journal["package"] != package or journal["operator_sha256"] != operator:
                raise RuntimeError("storage_forward_reschedule_reconciliation_binding_changed")
            if journal["phase"] == "complete":
                inspect_published(root,original_base)
                return dict(phase="forward_reschedule_published",operation_sha256=package["operation_sha256"],
                    migration_started=False,deadline_renewed=extension)
        saved = journal["old_worker"] if journal else host.load_receipt(root/launch._STATE)
        wall,mono = time.time(),time.monotonic()
        clock = journal or dict(started_at=wall,started_monotonic=mono,wall_deadline=wall+300,
            monotonic_deadline=mono+300,boot_id=forward.terminal._boot())
        forward._clock(clock)
        with host.docker_deadline(clock["monotonic_deadline"]):
            details = publication._retired(saved)
            rows = host.inventory(plan["project"],operator_id=saved["container_id"])
            if host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows):
                raise RuntimeError("storage_forward_reschedule_source_changed")
            image_env = launch.inspect_candidate_image(package["image"],request)
            old_runtime = json.loads(journal["old_runtime_bytes"]) if journal else host.load_receipt(root/runtime.RUNTIME_RECIPE,max_bytes=MAX_BYTES)
            new_runtime = forward._published_runtime(old_runtime,plan,package)
            for candidate,recipe in ((plan,old_runtime),(proposed,new_runtime)):
                args = {k:candidate[k] for k in ("project","source_revision","source_image","image","request",
                    "inventory_path","keys_root","socket_volume","spool_destination")}
                operation.inspect_prepared_operation(root,**args,deadline=clock["monotonic_deadline"],
                    operator_id=saved["container_id"],proposed_runtime_recipe=recipe)
            if journal is None:
                before = _probe(proposed,saved,request=request,action="inspect")
                launch_path = root/forward.operation_file(forward.LAUNCH_STATE,operation_sha256=package["operation_sha256"])
                old_env = dict(value.split("=",1) for value in details["config"]["Env"])
                overrides = {key:old_env[key] for key in ("PG_DSN","QT_DISABLE_DOTENV","QT_ARCHIVE_SHARED_GROUP_ID",
                    "QT_LOGGING_LOKI_URL","QT_STORAGE_UDEV_ROOT")}
                overrides["QT_ONLINE_REQUEST_SHA256"] = publication._sha(publication.request_bytes(request))
                new_worker = _new_worker(saved,package=package,request=request,
                    environment_sha256=host.digest(sorted(k+"="+v for k,v in {**image_env,**overrides}.items())))
                journal = dict(schema_version="qt.storage_online_forward_reschedule_publication.v2" if extension
                    else "qt.storage_online_forward_reschedule_publication.v1",package=package,
                    operator_sha256=operator,original_publication_sha256=base["intent_sha256"],original_plan_bytes=path.read_text(),
                    old_request_bytes=(root/publication.REQUEST).read_text(),old_worker_bytes=(root/launch._STATE).read_text(),
                    old_worker=saved,old_runtime_bytes=(root/runtime.RUNTIME_RECIPE).read_text(),
                    old_launch_bytes=launch_path.read_text(),old_launch=host.load_receipt(launch_path,max_bytes=MAX_BYTES),
                    new_plan=proposed,new_request=request,new_worker=new_worker,new_runtime=new_runtime,sql_before=before,
                    capacity=host.load_receipt(launch._canonical(package["capacity_file"])),**clock)
                journal["intent_sha256"] = host.digest(journal)
                journal["phase"] = "prepared"
                _verify_journal(root,original_base,journal)
                _window(journal); _capacity(journal); _preimages(root,journal)
                if not execute:
                    return dict(phase="forward_reschedule_inspected",operation_sha256=package["operation_sha256"],
                        end_day=package["end_day"],max_objects=package["max_objects"],
                        expires_at=_after_sql(journal)["capture"]["expires_at"],storage_mutations_performed=False,migration_started=False)
                if len(_bytes(journal)) > MAX_BYTES:
                    raise ValueError("storage_forward_reschedule_journal_budget")
                forward._clock(journal)
                host.save_receipt(root/name,journal,initial=True)
            if not execute:
                return dict(phase="forward_reschedule_reconciliation_required",storage_mutations_performed=False)
            _window(journal); _capacity(journal); _preimages(root,journal)
            if journal["phase"] == "prepared":
                forward._clock(journal)
                journal["phase"] = "database_dispatched"
                host.save_receipt(root/name,journal,initial=False)
                _probe(proposed,saved,request=request,action="apply",expected=journal["sql_before"])
            # Every uncertain or repeated entry is observation-only for SQL.
            actual = _probe(proposed,saved,request=request,action="inspect")
            if actual != _after_sql(journal):
                raise RuntimeError("storage_forward_reschedule_outcome_unresolved_no_replay")
            forward._clock(journal)
            journal["phase"] = "database_reconciled"
            host.save_receipt(root/name,journal,initial=False)
            publication._retired(saved)
            _preimages(root,journal)
            rows = host.inventory(plan["project"],operator_id=saved["container_id"])
            if host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows):
                raise RuntimeError("storage_forward_reschedule_source_changed")
            archived = plan["project"]+"-storage-online-reschedule-before-"+journal["intent_sha256"][:16]
            container_name = host.docker("inspect","--format","{{.Name}}",saved["container_id"]).strip()
            if container_name == "/"+plan["project"]+"-storage-online":
                forward._clock(journal)
                host.docker("rename",saved["container_id"],archived)
            elif container_name != "/"+archived:
                raise RuntimeError("storage_forward_reschedule_retired_worker_name_changed")
            journal["phase"] = "publishing"
            host.save_receipt(root/name,journal,initial=False)
            for target,before,after in (
                (root/runtime.RUNTIME_RECIPE,journal["old_runtime_bytes"].encode(),_bytes(journal["new_runtime"])),
                (root/publication.REQUEST,journal["old_request_bytes"].encode(),publication.request_bytes(journal["new_request"])),
                (root/launch._STATE,journal["old_worker_bytes"].encode(),_bytes(journal["new_worker"])),
                (root/forward.operation_file(forward.LAUNCH_STATE,operation_sha256=package["operation_sha256"]),
                    journal["old_launch_bytes"].encode(),_bytes(_new_launch(journal)))):
                forward._clock(journal)
                publication._replace(target,before,after)
            forward._clock(journal)
            forward._publish_plan(new_path,journal["new_plan"])
            _preimages(root,journal)
            journal["phase"] = "complete"
            host.save_receipt(root/name,journal,initial=False)
            inspect_published(root,original_base,request=request,operation_path=new_path)
            LOG.info("storage_forward_reschedule_published operation_sha256=%s intent_sha256=%s end_day=%s deadline_extended=%s start_unchanged=true",
                package["operation_sha256"],journal["intent_sha256"],package["end_day"],extension)
            return dict(phase="forward_reschedule_published",operation_sha256=package["operation_sha256"],
                end_day=package["end_day"],expires_at=_after_sql(journal)["capture"]["expires_at"],
                migration_started=False,deadline_renewed=extension)

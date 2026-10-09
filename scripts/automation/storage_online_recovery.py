"""Fixed preserving recovery mounts after the online switch and reader retirement.

The existing final-state owner supplies a live source hold and committed result.
This module owns database recreation and the explicit, narrowly admitted recovery
of a missing-config repository failure after commit. Recovery reuses the existing
repository, spool and runtime owners; it never replays SQL or generates keys.
"""
from __future__ import annotations

from copy import deepcopy
from contextvars import ContextVar
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_handoff_pause as preserving
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial

RECIPE = "storage-online-recovery-database.compose.json"
_ACTIONS = ("stop", "remove", "create", "start")
_CONTINUATION = ContextVar("storage_committed_recovery_continuation", default=None)
CONTINUATION_STATE = "storage-online-repository-continuation.json"


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
    if saved["phase"] != "recovery_preparing":
        if (value["completed"] != list(_ACTIONS) or value["inflight"] is not None
                or type(value["finished_at"]) not in (int, float)
                or not value["started_at"] <= value["finished_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_recovery_completion_invalid")
    elif value["finished_at"] is not None:
        raise RuntimeError("storage_online_recovery_completion_invalid")


def database_recipe(base, *, keys_root, socket_volume, history):
    """Render the fixed preserving mounts without starting or changing a database."""
    model = deepcopy(base)
    service = model["services"]["tsdb"]
    service["volumes"] += [dict(type="bind", source=str(keys_root),
        target="/run/quanttrad/recovery", read_only=True, bind=dict(create_host_path=False)),
        dict(type="volume", source="storage-recovery-socket", target="/var/run/postgresql")]
    model["volumes"]["storage-recovery-socket"] = dict(name=socket_volume, external=True)
    preserving._database_recovery_mounts(service, model["volumes"], history)
    return model


def inspect_socket_volume(socket_volume, pgdata_volume):
    """Read-only admission of the fixed independent local PostgreSQL socket."""
    socket_info = json.loads(host.docker("volume", "inspect", "--format", "{{json .}}", socket_volume))
    if (socket_volume == pgdata_volume
            or socket_info.get("Name") != socket_volume or socket_info.get("Driver") != "local"
            or socket_info.get("Options") not in (None, {}) or socket_info.get("Scope") != "local"):
        raise RuntimeError("storage_online_recovery_socket_not_independent")
    return socket_info


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
        admit_retired_reader(worker_process, worker, saved, source_check, deadline)

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
        model = database_recipe(base, keys_root=keys_root, socket_volume=socket_volume, history=history)
        service = model["services"]["tsdb"]
        socket_info = inspect_socket_volume(socket_volume, mounts["/var/lib/postgresql/data"].get("Name"))
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


def admit_retired_reader(worker_process, worker, saved, source_check, deadline):
    """Recheck the exact retired reader before exposing recovery keys."""
    worker_id = worker["container_id"]
    source_check()
    continuation = _CONTINUATION.get()
    continued = continuation is not None and continuation is source_check
    if time.monotonic() >= deadline or (not continued and time.time() >= worker["deadline"]):
        raise RuntimeError("storage_online_recovery_deadline_expired")
    if (worker_process is None and not continued or worker_process is not None and
            (worker_process.args != ["docker", "start", "--attach", "--interactive", worker_id]
             or worker_process.poll() is None)):
        raise RuntimeError("storage_online_recovery_attach_not_reaped")
    launch._admit(worker_id, worker["binding"], worker["contract"])
    status = json.loads(host.docker("inspect", "--format", "{{json .State}}", worker_id))
    if (any(status.get(k) is not False for k in ("Running", "Paused", "Restarting", "Dead"))
            or type(status.get("Pid")) is not int or status["Pid"] != 0
            or status.get("Status") != "exited"
            or status.get("StartedAt") != saved["binding"]["worker_started_at"]):
        raise RuntimeError("storage_online_recovery_reader_not_retired")


def _continuation_request(path):
    value = host.load_receipt(launch._canonical(path))
    if (set(value) != {"schema_version", "operation_sha256", "final_sha256", "duration_seconds"}
            or value["schema_version"] != "qt.storage_repository_continuation.v1"
            or any(not isinstance(value[k], str) or not re.fullmatch(r"[0-9a-f]{64}", value[k])
                   for k in ("operation_sha256", "final_sha256"))
            or type(value["duration_seconds"]) is not int or not 1 <= value["duration_seconds"] <= 600):
        raise ValueError("storage_repository_continuation_request_invalid")
    return value


def _failed_preparer(state_root, saved, database):
    """Recognize one observed pre-preparation failure; never infer a lost outcome."""
    from scripts.automation import storage_online_repositories as repositories
    journal = saved["repositories"]
    if (saved["phase"] != "recovery_repository_preparing"
            or journal["completed"] != ["logins", "create"] or journal["inflight"] != "prepare"
            or any(journal[k] is not None for k in ("report", "helper_retirement", "finished_at"))):
        raise RuntimeError("storage_repository_continuation_failure_not_supported")
    recipe = host.load_receipt(state_root/repositories.RECIPE)
    if host.digest(recipe) != journal["recipe_sha256"]:
        raise RuntimeError("storage_repository_continuation_recipe_changed")
    helper = host.database_details(journal["helper_id"])
    if repositories._preparer_contract(helper, database) != journal["helper_contract"]:
        raise RuntimeError("storage_repository_continuation_helper_changed")
    command = recipe["services"]["prepare"]["command"]
    if helper["config"].get("Cmd") != command:
        raise RuntimeError("storage_repository_continuation_command_changed")
    arguments = json.loads(command[-1])
    if arguments.get("incremental_config") != "/run/quanttrad/recovery/incremental-config.json":
        raise RuntimeError("storage_repository_continuation_failure_not_supported")
    status = json.loads(host.docker("inspect", "--format", "{{json .State}}", journal["helper_id"]))
    if (status.get("Status") != "exited" or status.get("ExitCode") != 1 or status.get("Pid") != 0
            or any(status.get(k) is not False for k in ("Running", "Paused", "Restarting", "Dead", "OOMKilled"))):
        raise RuntimeError("storage_repository_continuation_helper_outcome_uncertain")
    remaining = host.current_docker_deadline()-time.monotonic()
    if remaining <= 0:
        raise RuntimeError("storage_repository_continuation_inspection_expired")
    logs = subprocess.run(["docker","logs","--tail","60",journal["helper_id"]],
        stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,check=True,
        timeout=min(10,remaining)).stdout
    if len(logs.encode()) > 16384:
        raise RuntimeError("storage_repository_continuation_helper_logs_oversized")
    # This exact missing-input
    # exception precedes prepare()'s ownership lock and every repository mutation.
    expected = "FileNotFoundError: [Errno 2] No such file or directory: '/run/quanttrad/recovery/incremental-config.json'"
    if not logs.rstrip().endswith(expected):
        raise RuntimeError("storage_repository_continuation_failure_not_supported")
    return dict(helper_id=journal["helper_id"], helper_state_sha256=host.digest(status),
                recipe_sha256=host.digest(recipe), helper_logs_sha256=hashlib.sha256(logs.encode()).hexdigest())


def _committed_policy(database_id, saved, request):
    """Inspect only the small canonical certificate/policy records, not history."""
    from core.storage_targets import StoragePolicy
    from scripts.db.fact_header_v2_handoff import _policy_plan_id, POLICY_OPERATION, RECEIPT_VERSION
    plan_id = saved["commit"]["confirmed_plan_id"]
    if not re.fullmatch(r"handoff-[0-9a-f]{32}", plan_id):
        raise RuntimeError("storage_repository_continuation_commit_unconfirmed")
    sql = """SELECT json_build_object(
      'layout',(SELECT row_to_json(x) FROM (SELECT state,evidence FROM market.fact_storage_state
        WHERE layout_version='market.fact_storage_tiers.v2') x),
      'plan',(SELECT row_to_json(x) FROM (SELECT state,policy_hash,policy,base_revision,progress
        FROM public.portal_storage_plans WHERE id='"""+plan_id+"""') x),
      'policy',(SELECT row_to_json(x) FROM (SELECT revision,policy,applied_plan_id
        FROM public.portal_storage_policy WHERE id=1) x),
      'clients',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()
        AND backend_type='client backend' AND pid<>pg_backend_pid()),
      'prepared',(SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()),
      'archive_mode',current_setting('archive_mode'),
      'archive_disabled',current_setting('archive_command') IN ('','(disabled)'))::text"""
    observed = json.loads(host.database_query(database_id, sql, read_only_seconds=5))
    policy = StoragePolicy.from_dict(request["policy"])
    row, plan, current = (observed[k] for k in ("layout", "plan", "policy"))
    evidence = row.get("evidence") if isinstance(row, dict) else None
    receipt = evidence.get("handoff") if isinstance(evidence, dict) else None
    if (not isinstance(receipt, dict) or row["state"] != "ready"
            or evidence.get("source_retained") is not True
            or receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("initial_policy_required") is not True
            or receipt.get("policy_fingerprint") != policy.fingerprint
            or _policy_plan_id(receipt) != plan_id or not isinstance(plan, dict)
            or plan["state"] != "completed" or plan["policy_hash"] != policy.fingerprint
            or plan["policy"] != policy.to_dict()
            or plan["progress"] != dict(operation=POLICY_OPERATION,policy_revision=plan["base_revision"]+1,
                database_handoff_plan=plan_id,runtime_activation_required=True)
            or current != dict(revision=plan["base_revision"]+1,policy=policy.to_dict(),applied_plan_id=plan_id)
            or observed["clients"] != 0 or observed["prepared"] != 0
            or observed["archive_mode"] != "off" or observed["archive_disabled"] is not True):
        raise RuntimeError("storage_repository_continuation_database_changed")
    return plan_id


def _inspect_continuation(state_root, plan, package):
    from scripts.automation import storage_online_final as final, storage_online_runtime as runtime
    from scripts.automation import storage_online_repositories as repositories
    saved = final._load(state_root/final.STATE)
    original = preserving._runtime_configuration_bytes(state_root/final.STATE)
    if hashlib.sha256(original).hexdigest() != package["final_sha256"]:
        raise RuntimeError("storage_repository_continuation_final_changed")
    if saved["boot_id"] != final._boot_id() or time.time() < saved["started_at"] or final._boot_seconds() < saved["started_boot"]:
        raise RuntimeError("storage_repository_continuation_boot_or_clock_changed")
    worker = host.load_receipt(state_root/launch._STATE)
    preparation = initial._load(state_root)
    request_raw = preserving._runtime_configuration_bytes(state_root/"storage-online-request.json")
    request = json.loads(request_raw)
    if (host.digest(worker) != saved["binding"]["worker_sha256"]
            or host.digest(preparation) != saved["binding"]["preparation_sha256"]
            or hashlib.sha256(request_raw).hexdigest() != worker["binding"]["request_sha256"]
            or worker["container_id"] != saved["binding"]["worker_id"]
            or saved["binding"]["project"] != plan["project"]
            or saved["commit"]["source_image"] != plan["source_image"]
            or worker["binding"]["image"] != plan["image"]):
        raise RuntimeError("storage_repository_continuation_binding_changed")
    launch._admit(worker["container_id"], worker["binding"], worker["contract"])
    status = json.loads(host.docker("inspect", "--format", "{{json .State}}", worker["container_id"]))
    if (status.get("Status") != "exited" or status.get("Pid") != 0 or status.get("ExitCode") != 0
            or status.get("StartedAt") != saved["binding"]["worker_started_at"]
            or any(status.get(k) is not False for k in ("Running", "Paused", "Restarting", "Dead", "OOMKilled"))):
        raise RuntimeError("storage_repository_continuation_reader_not_retired")
    rows = host.inventory(plan["project"], operator_id=worker["container_id"])
    initial._admit_clients(rows, preparation["clients"])
    final._admit_mount_writers(rows,operator_id=worker["container_id"])
    if any(rows[name]["running"] for name in host.STOP) or rows["tsdb"]["id"] != saved["recovery"]["replacement_id"]:
        raise RuntimeError("storage_repository_continuation_source_not_stopped")
    database_model = host.load_receipt(state_root/RECIPE)
    if host.digest(database_model) != saved["recovery"]["recipe_sha256"]:
        raise RuntimeError("storage_repository_continuation_database_recipe_changed")
    digest_rows = host.docker("compose", "--project-name",plan["project"],"--file",str(state_root/RECIPE),"config","--hash","tsdb").split()
    if len(digest_rows) != 2 or digest_rows[0] != "tsdb" or not re.fullmatch(r"[0-9a-f]{64}",digest_rows[1]):
        raise RuntimeError("storage_repository_continuation_database_render_invalid")
    database = host.database_details(rows["tsdb"]["id"])
    observation = dict(admission=dict(images=dict(tsdb=database_model["services"]["tsdb"]["image"])),
        compose_hashes=dict(tsdb=digest_rows[1]),binding=dict(database_id=database["id"]),
        receipt=dict(project=plan["project"],database_preparation=dict(networks=host.database_networks(database))))
    preserving._runtime_candidate_details("tsdb",rows["tsdb"],database_model,observation)
    if host.cluster_identifier(database["id"],maintenance=True) != preparation["cluster"]:
        raise RuntimeError("storage_repository_continuation_cluster_changed")
    helper = _failed_preparer(state_root,saved,database)
    _committed_policy(database["id"],saved,request)
    roots = preparation["source_roots"]
    candidates = [Path(p) for p in roots if str(Path(p)/"objects") in roots]
    if len(roots) != 2 or len(candidates) != 1:
        raise RuntimeError("storage_repository_continuation_source_roots_invalid")
    for name in final._SOURCE_WRITERS:
        final._source_writer_contract(host.database_details(rows[name]["id"]),service=name,
            source_image=plan["source_image"],source_revision=saved["binding"]["source_revision"],root=candidates[0])
    runtime.inspect_spool_destination(candidates[0], plan["spool_destination"])
    binding = worker["binding"]
    _, admission = runtime.inspect_runtime_configuration(state_root,database_model=database_model,
        image_id=binding["image"],request=request,
        inventory=Path(binding["mounts"]["/run/qt-online/inventory.json"]["source"]),
        udev_root=binding["mounts"]["/run/qt-online/udev"]["source"],
        destination=plan["spool_destination"],rows=rows,database_id=database["id"])
    incremental = repositories.incremental_configuration(state_root)
    return saved, worker, preparation, candidates[0], dict(**helper, runtime=admission,
        incremental_sha256=host.digest(incremental),database_id=database["id"],
        worker_state_sha256=host.digest(status),plan_id=saved["commit"]["confirmed_plan_id"])


@contextmanager
def _held_continuation(state_root, *, saved, worker, preparation, source, source_image, audit):
    """Reacquire real source exclusion after the former controller exited."""
    from scripts.automation import storage_online_final as final
    from market_data.archive_namespace import archive_namespace
    binding = saved["binding"]
    with archive_namespace(source,exclusive=True) as namespace_check:
        active = True
        def check():
            if not active or host.load_receipt(state_root/CONTINUATION_STATE) != audit:
                raise RuntimeError("storage_repository_continuation_owner_changed")
            current = final._load(state_root/final.STATE)
            final._remaining(current)
            if (current["binding"] != binding or current["deadline"] != audit["deadline"]
                    or current["deadline_boot"] != audit["deadline_boot"]
                    or current["switch"]["deadline_monotonic"] != audit["deadline_monotonic"]):
                raise RuntimeError("storage_repository_continuation_binding_changed")
            namespace_check()
            for name, expected in preparation["source_roots"].items():
                info = Path(name).stat()
                if [info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode&0o7777] != expected or Path(name).resolve(strict=True) != Path(name):
                    raise RuntimeError("storage_repository_continuation_source_changed")
            for identity,digest in ((worker["container_id"],audit["observation"]["worker_state_sha256"]),
                    (audit["observation"]["helper_id"],audit["observation"]["helper_state_sha256"])):
                status=json.loads(host.docker("inspect","--format","{{json .State}}",identity))
                if host.digest(status) != digest:
                    raise RuntimeError("storage_repository_continuation_retired_process_changed")
        token = final._SOURCE_HOLD.set((state_root,binding,source_image,check))
        continued = _CONTINUATION.set(check)
        try:
            check()
            yield check
            check()
        finally:
            active = False
            _CONTINUATION.reset(continued)
            final._SOURCE_HOLD.reset(token)


def continue_repositories(operation_path, *, request_file, execute=False):
    """Explicit recovery of one confirmed missing-config failure; never rerun SQL."""
    from scripts.automation import storage_online_operation as operation, storage_online_final as final
    from scripts.automation import storage_online_repositories as repositories
    from scripts.automation.storage_online_release import _preserve_file
    if type(execute) is not bool:
        raise ValueError("storage_repository_continuation_execute_invalid")
    operation_path=launch._canonical(operation_path)
    package=_continuation_request(request_file)
    raw=preserving._runtime_configuration_bytes(operation_path)
    if hashlib.sha256(raw).hexdigest()!=package["operation_sha256"]:
        raise RuntimeError("storage_repository_continuation_operation_changed")
    plan=operation.load_operation_plan(operation_path)
    root=launch._canonical(plan["state_root"])
    with host.deployment_lock(root),host.docker_deadline(time.monotonic()+60):
        if os.path.lexists(root/CONTINUATION_STATE):
            raise RuntimeError("storage_repository_continuation_intent_exists_reconcile_required")
        saved,worker,preparation,source,observed=_inspect_continuation(root,plan,package)
        proposal=dict(phase="committed_repository_recovery_inspected",observation_sha256=host.digest(observed),
            plan_id=observed["plan_id"],duration_seconds=package["duration_seconds"],migration_replay_authorized=False,
            storage_mutations_performed=False,original_final_deadline=saved["deadline"])
        if not execute:return proposal
        # This is an explicitly admitted recovery extension, never a silent reset.
        # Preserve the original bytes and original start anchors/downtime evidence.
        # The capture plan and completed SQL work remain unchanged. The retained
        # predecessor records the original, failed final window in full.
        elapsed=max(time.time()-saved["started_at"],final._boot_seconds()-saved["started_boot"])
        duration=math.floor(elapsed)+package["duration_seconds"]
        if duration<=saved["duration_seconds"] or duration>96*3600:
            raise RuntimeError("storage_repository_continuation_window_invalid")
        extension=duration-saved["duration_seconds"]
        digest=host.digest(package)
        original=preserving._runtime_configuration_bytes(root/final.STATE)
        if hashlib.sha256(original).hexdigest()!=package["final_sha256"]:
            raise RuntimeError("storage_repository_continuation_final_changed")
        retained_final=root/("storage-online-final.failed-"+digest+".json")
        retained_recipe=root/("storage-online-repository.failed-"+digest+".json")
        if os.path.lexists(retained_final) or os.path.lexists(retained_recipe):
            raise RuntimeError("storage_repository_continuation_preservation_exists")
        amended=deepcopy(saved)
        amended.update(phase="recovery_database_ready",duration_seconds=duration,
            deadline=saved["deadline"]+extension,deadline_boot=saved["deadline_boot"]+extension)
        amended["switch"]["deadline_monotonic"]+=extension
        del amended["repositories"]
        audit=dict(schema_version="qt.storage_repository_continuation.v1",request=package,
            started_at=time.time(),deadline=amended["deadline"],deadline_boot=amended["deadline_boot"],
            deadline_monotonic=amended["switch"]["deadline_monotonic"],observation=observed,
            original_final=str(retained_final),original_recipe=str(retained_recipe),
            original_final_deadline=saved["deadline"],original_duration_seconds=saved["duration_seconds"],
            phase="preparing",finished_at=None)
        host.save_receipt(root/CONTINUATION_STATE,audit,initial=True)
        _preserve_file(retained_final,original)
        # All further uncertainty keeps both attempt records and refuses reentry.
        with host.docker_deadline(amended["switch"]["deadline_monotonic"]):
            if _inspect_continuation(root,plan,package)[4]!=observed:
                raise RuntimeError("storage_repository_continuation_preflight_changed")
            root.joinpath(repositories.RECIPE).rename(retained_recipe)
            host.docker("rename",observed["helper_id"],plan["project"]+"-failed-repository-"+digest[:12])
            host.save_receipt(root/final.STATE,amended,initial=False)
            with _held_continuation(root,saved=amended,worker=worker,preparation=preparation,
                    source=source,source_image=plan["source_image"],audit=audit) as check:
                limits=operation.OperationLimits(**plan["limits"])
                repositories.prepare_repositories(root,saved=amended,worker_process=None,source_check=check,
                    max_bytes=limits.repository_max_bytes,reserve_bytes=limits.repository_reserve_bytes,
                    recent_free_bytes=limits.recent_free_bytes,max_duration_seconds=package["duration_seconds"],
                    logins_already_open=True)
                final.prepare_online_runtime_spool_locked(root,worker_process=None,destination=plan["spool_destination"],
                    max_bytes=limits.spool_max_bytes,max_entries=limits.spool_max_entries,
                    reserve_bytes=limits.spool_reserve_bytes,max_duration_seconds=package["duration_seconds"])
                runtime=final.activate_online_runtime_locked(root,worker_process=None,
                    max_duration_seconds=package["duration_seconds"])
            audit.update(phase="runtime_ready",finished_at=time.time())
            host.save_receipt(root/CONTINUATION_STATE,audit,initial=False)
            return dict(phase="runtime_ready",runtime=runtime,migration_replay_authorized=False,
                original_final_preserved=str(retained_final),complete_backup_confirmed=False,
                ordinary_relaunch_authorized=False)

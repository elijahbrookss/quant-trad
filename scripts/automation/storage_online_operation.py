"""Fixed online migration sequence over the existing host and database owners.

This internal driver begins with admitted source-serving preparation and an
immutable worker request. It creates no state machine, receipt, retry policy or
release authority. Every mutation belongs to its existing phase owner.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import logging
import json
from copy import deepcopy
from datetime import date
import os
from pathlib import Path
import re
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_final as final
from scripts.automation import storage_online_runtime as runtime_owner
from scripts.automation import storage_online_prepare as initial
from scripts.automation import storage_online_recovery as recovery

logger = logging.getLogger(__name__)
_FAMILIES = {"raw_archive_manifests", "fact_archive_manifests", "book_checkpoint_manifests"}
_CATALOGS = ("market.fact_archive_material_aliases", "market.fact_archive_canonical_dependencies")


@dataclass(frozen=True)
class OperationLimits:
    """Explicit admitted limits, never fixture defaults or a production estimate."""
    preparation_seconds: int
    final_seconds: int
    recovery_seconds: int
    runtime_seconds: int
    spool_max_bytes: int
    spool_max_entries: int
    spool_reserve_bytes: int
    repository_max_bytes: int
    repository_reserve_bytes: int
    recent_free_bytes: int

    def validate(self, request):
        values = vars(self)
        if any(type(value) is not int for value in values.values()):
            raise ValueError("storage_online_operation_limits_invalid")
        if (not 1 <= self.preparation_seconds <= 3600
                or not 1 <= self.final_seconds <= 96*3600
                or not 1 <= self.recovery_seconds <= 600
                or not 1 <= self.runtime_seconds <= 600
                or not 0 < self.spool_max_bytes <= 64*1024**3
                or not 1 <= self.spool_max_entries <= 1_000_000
                or min(self.spool_reserve_bytes, self.repository_reserve_bytes, self.recent_free_bytes) < 0
                or self.repository_max_bytes <= 0
                or max(self.preparation_seconds, self.final_seconds) > request["resource_limits"]["movement_timeout_seconds"]):
            raise ValueError("storage_online_operation_limits_invalid")


def prepare_background(exchange, *, preparation_seconds):
    """Follow finite baseline phases, then fairly catch SQL/archive live tails.

    The channel and worker retain the original capture deadline on every call.
    Empty observations only end background preparation; final owners independently
    exclude publishers, drain, verify and decide whether a switch is admissible.
    """
    if type(preparation_seconds) is not int or not 1 <= preparation_seconds <= 3600:
        raise ValueError("storage_online_operation_preparation_limit_invalid")

    def phase(step, relation=None):
        result = exchange("prepare_step", step=step, relation=relation,
            max_duration_seconds=preparation_seconds)["result"]
        if result.get("committed") is not True:
            raise RuntimeError("storage_online_operation_preparation_unconfirmed")

    phase("catalog_history", "qt_fact_storage_cutover_v1.fact_versions")
    while True:
        result = exchange("sql_copy")["result"]
        if result["outcome"] == "identity_relocation_required":
            phase("identity_history")
        elif result["outcome"] == "raw_relocation_required":
            phase("raw_history")
        elif result["phase"] == "catch_up":
            break  # Do not chase a growing tail before finishing finite preparation.
        elif result["outcome"] not in {"page_budget_reached", "pass_time_budget_reached"}:
            raise RuntimeError("storage_online_operation_copy_progress_invalid")
    phase("identity_capture")
    after = None
    seen = set()
    while True:
        page = exchange("inspect_references", after=after)["result"]
        names = [row["relation"] for row in page["references"]]
        if (tuple(page["catalogs"]) != _CATALOGS or len(names) > 32
                or names != sorted(set(names)) or seen.intersection(names)
                or any(after is not None and name <= after for name in names)
                or len(seen)+len(names) > 8192
                or page["next_after"] is not None and (not names or page["next_after"] != names[-1])):
            raise RuntimeError("storage_online_operation_reference_page_invalid")
        for relation in names:
            phase("reference_prepare", relation)
            phase("reference_validate", relation)
        seen.update(names)
        after = page["next_after"]
        if after is None:
            break
    phase("reference_adopt")
    for relation in _CATALOGS:
        phase("catalog_history", relation)
    baselines = set()
    rounds = 0
    while True:
        archive = exchange("archive_copy")["result"]
        if archive["family"] not in _FAMILIES:
            raise RuntimeError("storage_online_operation_archive_family_invalid")
        if archive.get("baseline_complete") is True:
            baselines.add(archive["family"])
        proof = exchange("reprove")
        sql = exchange("sql_copy")
        rounds += 1
        if (baselines == _FAMILIES
                and set(proof["reproved_families_at_observation"]) == _FAMILIES
                and sql["result"]["outcome"] == "both_tails_observed_empty"):
            break
    logger.info("storage_online_background_prepared | references=%s rounds=%s switch_authorized=false", len(seen), rounds)
    return dict(reference_count=len(seen), tail_rounds=rounds, final_switch_authorized=False)


def inspect_prepared_operation(state_root, *, project, source_revision, source_image,
        image, request, inventory_path, keys_root, socket_volume, spool_destination,
        deadline, operator_id=None):
    """Read-only prerequisite observation; never pause, capture or switch authority.

    Reuse actual source and runtime owners before worker launch and before final
    pause. A saved or equal observation cannot replace their later live checks.
    The caller supplies the existing preparation/capture bound for all host I/O.
    """
    with host.docker_deadline(deadline):
        if operator_id is None:
            _, _, found = launch.observe_owned_worker(state_root, project)
            operator_id = found[0] if found else None
        preparation = initial.admit_serving_source(state_root, project=project,
            source_revision=source_revision, operator_id=operator_id)
        rows = host.inventory(project, operator_id=operator_id)
        observed=inspect_operation_configuration(state_root,project=project,
            source_revision=source_revision,source_image=source_image,image=image,request=request,
            inventory_path=inventory_path,keys_root=keys_root,socket_volume=socket_volume,
            spool_destination=spool_destination,roots=preparation["source_roots"],rows=rows,
            recipe_sha256=preparation["recipe_sha256"],deadline=deadline)
        return dict(preparation_sha256=host.digest(preparation),**observed)


def inspect_operation_configuration(state_root, *, project, source_revision, source_image,
        image, request, inventory_path, keys_root, socket_volume, spool_destination,
        roots, rows, recipe_sha256, deadline):
    """Shared planned runtime checks for both initial and final preparation."""
    candidates = [Path(p) for p in roots if str(Path(p)/"objects") in roots]
    if len(roots) != 2 or len(candidates) != 1:
        raise RuntimeError("storage_online_operation_source_roots_invalid")
    source = candidates[0]
    launch.inspect_candidate_image(image, request)
    for name in final._SOURCE_WRITERS:
        final._source_writer_contract(host.database_details(rows[name]["id"]),
            service=name, source_image=source_image, source_revision=source_revision, root=source)
    destination,_,info = runtime_owner.inspect_spool_destination(source, spool_destination)
    base, history = recovery.preserving._database_recipe(state_root, project)
    if host.digest(base) != recipe_sha256:
        raise RuntimeError("storage_online_recovery_original_recipe_changed")
    model = recovery.database_recipe(base, keys_root=keys_root,
        socket_volume=socket_volume, history=history)
    socket = recovery.inspect_socket_volume(socket_volume, base["volumes"]["postgres-data"]["name"])
    udev = launch._canonical(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
    runtime, admission = runtime_owner.inspect_runtime_configuration(state_root,
        database_model=model, image_id=image, request=request,
        inventory=launch._canonical(inventory_path), udev_root=udev,
        destination=str(destination), rows=rows, database_id=rows["tsdb"]["id"])
    # The replacement does not exist yet: pin its service, never the source PID.
    if runtime["services"]["storage-maintenance"].get("pid") != "service:tsdb":
        raise RuntimeError("storage_online_operation_future_database_service_required")
    key = Path(keys_root).stat()
    observed = dict(runtime=admission,
        socket_sha256=host.digest(socket), source_image=source_image,
        destination=[str(destination),info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode],
        keys=[str(keys_root),key.st_dev,key.st_ino,key.st_uid,key.st_gid,key.st_mode])
    # Inspectors can take time; do not proceed after their shared bound expires.
    if time.monotonic() >= deadline:
        raise RuntimeError("storage_online_operation_preflight_expired")
    return observed


def validate_operation_arguments(state_root, *, limits, request, source_image):
    if not isinstance(limits, OperationLimits):
        raise ValueError("storage_online_operation_limits_invalid")
    limits.validate(request)
    if not isinstance(source_image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", source_image):
        raise ValueError("storage_online_operation_source_image_invalid")
    state_root = launch._canonical(state_root)
    return state_root


def run_prepared_operation_locked(state_root, *, project, source_revision, source_image,
        image, request, inventory_path, descriptor_limit, memory_bytes,
        limits, keys_root, socket_volume, spool_destination):
    """One continuous live operation from prepared source through runtime readiness.

    Caller supplies reviewed private recipes, prepared roots and measured limits.
    Uncertainty propagates immediately, retaining phase intent. No automatic abort,
    reopen, reconnect, replay, marker deletion or repeated COMMIT occurs here.
    Runtime readiness is not encrypted-pair completion or ordinary relaunch authority.
    """
    state_root=validate_operation_arguments(state_root,limits=limits,request=request,source_image=source_image)
    started = time.monotonic()
    try:
        with ExitStack() as holds:
            plan = dict(project=project, source_revision=source_revision, source_image=source_image,
                image=image, request=request, inventory_path=inventory_path, keys_root=keys_root,
                socket_volume=socket_volume, spool_destination=spool_destination)
            before = inspect_prepared_operation(state_root, **plan,
                deadline=time.monotonic()+limits.preparation_seconds)
            with launch.launched_online_worker_locked(state_root, project=project,
                    source_revision=source_revision, image=image, request=request,
                    inventory_path=inventory_path, descriptor_limit=descriptor_limit,
                    memory_bytes=memory_bytes) as (worker, receipt):
                channel = host.OnlineWorkerChannel(worker,
                    deadline=time.monotonic()+receipt["deadline"]-time.time(),
                    command_seconds=max(40, limits.preparation_seconds))
                exchange = channel.exchange
                prepared = prepare_background(exchange, preparation_seconds=limits.preparation_seconds)
                current = inspect_prepared_operation(state_root, **plan,
                    deadline=min(time.monotonic()+limits.preparation_seconds,
                        time.monotonic()+receipt["deadline"]-time.time()),
                    operator_id=receipt["container_id"])
                if current != before:
                    raise RuntimeError("storage_online_operation_preflight_changed")
                paused = final.stop_online_source_locked(state_root, project=project,
                    source_revision=source_revision, worker_id=receipt["container_id"],
                    controller_id=channel.greeting["controller_id"], max_duration_seconds=limits.final_seconds)
                holds.enter_context(final.held_source_writers_locked(state_root, source_image=source_image))
                final.observe_source_drain_locked(state_root, exchange=exchange,
                    max_entries=limits.spool_max_entries)
                deadline = time.monotonic()+final._remaining(paused)-0.1
                final.copy_final_delta_locked(state_root, exchange=exchange, deadline=deadline, max_rounds=64)
                final.record_switch_entry_locked(state_root, deadline=deadline,
                    observe_worker=lambda **args: exchange("status", response_deadline=args["deadline"]))
                final.close_database_logins_locked(state_root, exchange=exchange)
                final.copy_final_delta_locked(state_root, exchange=exchange, deadline=deadline, max_rounds=64)
                outcome = final.commit_online_handoff_locked(state_root, exchange=exchange)
                if outcome.get("outcome") != "committed" or outcome.get("initial_policy_activated") is not True:
                    raise RuntimeError("storage_online_operation_commit_unconfirmed")
                exchange("close")
            # Launcher has independently reaped/verified the reader. Existing
            # recovery owners check that fact again before exposing any keys.
            final.prepare_recovery_database_locked(state_root, worker_process=worker,
                keys_root=keys_root, socket_volume=socket_volume,
                max_duration_seconds=limits.recovery_seconds)
            final.prepare_online_repositories_locked(state_root, worker_process=worker,
                max_bytes=limits.repository_max_bytes, reserve_bytes=limits.repository_reserve_bytes,
                recent_free_bytes=limits.recent_free_bytes, max_duration_seconds=limits.recovery_seconds)
            final.prepare_online_runtime_spool_locked(state_root, worker_process=worker,
                destination=spool_destination, max_bytes=limits.spool_max_bytes,
                max_entries=limits.spool_max_entries, reserve_bytes=limits.spool_reserve_bytes,
                max_duration_seconds=limits.recovery_seconds)
            runtime = final.activate_online_runtime_locked(state_root, worker_process=worker,
                max_duration_seconds=limits.runtime_seconds)
            return dict(background=prepared, runtime=runtime, elapsed_seconds=time.monotonic()-started,
                complete_backup_confirmed=False, ordinary_relaunch_authorized=False)
    except BaseException as exc:
        logger.error("storage_online_operation_stopped | project=%s error_type=%s intent_retained=true replay_authorized=false",
                     project, type(exc).__name__)
        raise


def run_prepared_operation(state_root, **arguments):
    """Internal prepared-source adapter; full operation retains this lock earlier."""
    state_root=validate_operation_arguments(state_root,**{k:arguments[k] for k in ("limits","request","source_image")})
    with host.deployment_lock(state_root):
        return run_prepared_operation_locked(state_root, **arguments)


_PLAN_FIELDS={"schema_version","state_root","project","source_revision","source_image","image",
    "history_uuid","history_before","attempt_seconds","request","inventory_path",
    "descriptor_limit","memory_bytes","limits","keys_root","socket_volume","spool_destination"}


def load_operation_plan(path):
    """One private declarative plan, never a receipt or persisted execution authority."""
    from scripts.automation.storage_online_worker import validate_request_shape

    plan=host.load_receipt(launch._canonical(path))
    if (not isinstance(plan,dict) or set(plan)-{"deployment_environment","deployment_repository"}!=_PLAN_FIELDS
            or plan["schema_version"]!="qt.storage_online_operation.v1"
            or not isinstance(plan["limits"],dict)
            or set(plan["limits"])!=set(OperationLimits.__dataclass_fields__)
            or not isinstance(plan["history_uuid"],str)
            or not re.fullmatch(r"[A-Za-z0-9-]{4,128}",plan["history_uuid"])
            or type(plan["attempt_seconds"]) is not int or not 1<=plan["attempt_seconds"]<=96*3600
            or not isinstance(plan["history_before"],str)
            or date.fromisoformat(plan["history_before"]).isoformat()!=plan["history_before"]):
        raise ValueError("storage_online_operation_plan_invalid")
    request=plan["request"]
    validate_request_shape(request)
    if request["expected_started_at"] is not None or "capture_preparation" in request:
        raise ValueError("storage_online_operation_plan_must_not_invent_capture_clock")
    launch.validate_launch_inputs(**{k:plan[k] for k in
        ("project","source_revision","image","request","descriptor_limit","memory_bytes")})
    validate_operation_arguments(plan["state_root"],limits=OperationLimits(**plan["limits"]),
        request=request,source_image=plan["source_image"])
    for field in ("state_root","inventory_path","keys_root","spool_destination"):
        if not isinstance(plan[field],str) or str(launch._canonical(plan[field]))!=plan[field]:
            raise ValueError("storage_online_operation_plan_path_invalid")
    if "deployment_environment" in plan:
        value=plan["deployment_environment"]
        if not isinstance(value,str) or str(launch._canonical(value))!=value:
            raise ValueError("storage_online_operation_plan_path_invalid")
        from scripts.automation.storage_online_release import read_private_environment
        read_private_environment(Path(value))
    if "deployment_repository" in plan:
        value=plan["deployment_repository"]
        if ("deployment_environment" not in plan or not isinstance(value,str)
                or str(launch._canonical(value))!=value or not Path(value).is_dir()):
            raise ValueError("storage_online_operation_deployment_repository_invalid")
    return plan


_REQUEST_PROBE="""
import contextlib,json,sys
from pathlib import Path
plan=json.loads(sys.stdin.read(65537))
with contextlib.redirect_stdout(sys.stderr):
    from scripts.automation.storage_online_worker import inspect_request_configuration
    result=inspect_request_configuration(plan['request'],Path('/run/qt-online/inventory.json'),history_uuid=plan['history_uuid'])
print(json.dumps(result),flush=True)
"""


def inspect_initial_operation(state_root, *, plan, deadline):
    """Validate the actual initial source and complete proposed configuration before pause."""
    for name in (initial.STATE,host.HOLD,launch._STATE,"storage-online-request.json","storage-online-final.json",
                 "promotion.env","alert-preview.env"):
        if os.path.lexists(state_root/name):
            raise RuntimeError("storage_online_initial_operation_requires_reconciliation")
    with host.docker_deadline(deadline):
        rows,database_plan=initial.inspect_unprepared_source(state_root,project=plan["project"],
            source_revision=plan["source_revision"],history_uuid=plan["history_uuid"])
        if not initial._source_healthy(rows):
            raise RuntimeError("storage_online_preparation_source_not_healthy")
        collector=host.database_details(rows["market-data-collector"]["id"])
        roots=initial._source_roots(collector)
        source=next(Path(p)/"objects" for p in roots if str(Path(p)/"objects") in roots)
        info=source.stat();request=plan["request"]
        if (info.st_dev,info.st_ino)!=(request["source_device"],request["source_inode"]):
            raise RuntimeError("storage_online_source_root_changed")
        arguments={k:plan[k] for k in ("project","source_revision","source_image","image","request",
            "inventory_path","keys_root","socket_volume","spool_destination")}
        observed=inspect_operation_configuration(state_root,**arguments,roots=roots,rows=rows,
            recipe_sha256=database_plan["recipe_sha256"],deadline=deadline)
        # The pinned candidate runs its SAME policy/limit/job checks with read-only
        # SQL. No source/archive/PGDATA/key mount, PID sharing or capabilities.
        database=host.database_details(rows["tsdb"]["id"])
        raw=json.dumps(dict(request=request,history_uuid=plan["history_uuid"]),sort_keys=True,separators=(",",":"),allow_nan=False)
        if len(raw.encode())>65536:
            raise ValueError("storage_online_request_budget_exceeded")
        output=host.docker("run","--rm","--pull","never","--interactive",
            "--network","container:"+rows["tsdb"]["id"],"--read-only","--user","1000:1000",
            "--cap-drop","ALL","--security-opt","no-new-privileges","--memory","512m",
            "--cpus","1","--pids-limit","64","--env","QT_DISABLE_DOTENV=1","--env","PG_DSN",
            "--mount","type=bind,source="+plan["inventory_path"]+",target=/run/qt-online/inventory.json,readonly",
            "--entrypoint","python",plan["image"],"-c",_REQUEST_PROBE,
            env={**os.environ,"PG_DSN":launch._dsn(database,collector)},input=raw)
        if json.loads(output)!={"validated":True} or time.monotonic()>=deadline:
            raise RuntimeError("storage_online_operation_initial_probe_unconfirmed")
        return observed


def run_operation_plan(path, *, execute=False):
    """Single local operator: inspect by default, execute the existing fixed owners.

    The plan supplies measured limits and prepared paths. Initial preparation's
    original 600-second receipt supplies capture-preparation timing; user input
    cannot manufacture or renew it. Partial initial/final intents require explicit
    reconciliation. Successful return means runtime readiness, not release closure.
    """
    if type(execute) is not bool:
        raise ValueError("storage_online_operation_execute_invalid")
    plan=load_operation_plan(path)
    state_root=launch._canonical(plan["state_root"])
    limits=OperationLimits(**plan["limits"])
    with host.deployment_lock(state_root):
        if os.path.lexists(state_root/"storage-online-final.json"):
            saved = final._load(state_root/final.STATE)
            worker = host.load_receipt(state_root/launch._STATE)
            request = host.load_receipt(state_root/"storage-online-request.json")
            request.pop("capture_preparation", None)
            if (saved["binding"]["project"] != plan["project"]
                    or saved["binding"]["source_revision"] != plan["source_revision"]
                    or saved.get("commit",{}).get("source_image") != plan["source_image"]
                    or worker["binding"]["image"] != plan["image"] or request != plan["request"]
                    or saved.get("runtime_spool",{}).get("destination") != plan["spool_destination"]):
                raise RuntimeError("storage_online_completion_plan_changed")
            if "release" in saved:
                from scripts.automation import storage_online_release as release
                value = saved["release"]
                if (plan.get("deployment_repository") != value["repository"]
                        or plan.get("deployment_environment") != value["environment_path"]):
                    raise RuntimeError("storage_online_release_binding_changed")
                if load_operation_plan(path) != plan:
                    raise RuntimeError("storage_online_operation_plan_changed")
                if value["status"] == "publishing":
                    if execute:
                        return final.publish_deployment_configuration_locked(state_root,
                            repository=plan["deployment_repository"], environment_path=plan["deployment_environment"])
                    return dict(phase="deployment_configuration_unresolved", ordinary_relaunch_authorized=False,
                        migration_replay_authorized=False)
                release.admit_deployment(state_root, environment_path=plan["deployment_environment"],
                    repository=plan["deployment_repository"], action="deploy", revision=value["candidate_revision"])
                # Ordinary deployment owns current fleet verification after this
                # handoff. Never reuse the retired migration's container identities.
                return dict(phase="deployment_recorded" if value["status"] == "deployed" else "deployment_configuration_published",
                    deployment_revision=value["candidate_revision"], migration_replay_authorized=False,
                    ordinary_relaunch_authorized=False, current_fleet_verified=False)
            result = final.inspect_runtime_completion_locked(state_root)
            if load_operation_plan(path) != plan:
                raise RuntimeError("storage_online_operation_plan_changed")
            if result["ready"] and "deployment_environment" in plan:
                from scripts.automation.storage_online_release import prepare_deployment_environment
                result["deployment"] = prepare_deployment_environment(state_root,
                    environment_path=plan["deployment_environment"], saved=saved, execute=execute)
                if "deployment_repository" in plan:
                    from scripts.automation.storage_online_release import inspect_deployment_configuration
                    if not execute and not any(os.path.lexists(state_root/name) for name in
                            ("storage-online-source.env", "storage-online-deployment.env")):
                        result["deployment"]["configuration"] = dict(preparation_required=True,
                            ordinary_relaunch_authorized=False)
                    else:
                        result["deployment"]["configuration"] = inspect_deployment_configuration(state_root,
                            repository=plan["deployment_repository"], environment_path=plan["deployment_environment"], saved=saved)
                if load_operation_plan(path) != plan:
                    raise RuntimeError("storage_online_operation_plan_changed")
                if execute and "deployment_repository" in plan:
                    return final.publish_deployment_configuration_locked(state_root,
                        repository=plan["deployment_repository"], environment_path=plan["deployment_environment"])
            return dict(phase="recovery_verified" if result["ready"] else "runtime_ready", **result)
        prepared=initial._load(state_root) if os.path.lexists(state_root/initial.STATE) else None
        if prepared is None:
            observation=inspect_initial_operation(state_root,plan=plan,
                deadline=time.monotonic()+limits.preparation_seconds)
        else:
            if prepared["phase"]!="serving":
                raise RuntimeError("storage_online_initial_operation_requires_reconciliation")
            arguments={k:plan[k] for k in ("project","source_revision","source_image","image","request",
                "inventory_path","keys_root","socket_volume","spool_destination")}
            observation=inspect_prepared_operation(state_root,**arguments,
                deadline=time.monotonic()+limits.preparation_seconds)
        if load_operation_plan(path)!=plan:
            raise RuntimeError("storage_online_operation_plan_changed")
        if not execute:
            return dict(phase="inspected",configuration_sha256=host.digest(observation),
                storage_mutations_performed=False,final_switch_authorized=False)
        if prepared is None:
            prepared=initial.prepare_online_source_locked(state_root,project=plan["project"],
                source_revision=plan["source_revision"],history_uuid=plan["history_uuid"])
        if prepared["history_uuid"]!=plan["history_uuid"]:
            raise RuntimeError("storage_online_preparation_binding_changed")
        request=deepcopy(plan["request"])
        request["capture_preparation"]=dict(requested_at=prepared["completed_at"],
            deadline=prepared["deadline"],history_before=plan["history_before"],attempt_seconds=plan["attempt_seconds"])
        # On reentry the launcher requires byte-identical request and actual capture.
        arguments={k:plan[k] for k in ("project","source_revision","source_image","image",
            "inventory_path","descriptor_limit","memory_bytes","keys_root","socket_volume","spool_destination")}
        result=run_prepared_operation_locked(state_root,**arguments,request=request,limits=limits)
        return dict(phase="runtime_ready",initial_preparation_seconds=prepared["completed_at"]-prepared["started_at"],**result)

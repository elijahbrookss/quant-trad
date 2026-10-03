"""Preserving terminal operation for one owned, stopped online attempt.

No normal-worker restart, package amendment, capture renewal or replacement
attempt. The original plan/request/worker files are never rewritten.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
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
from scripts.automation import storage_online_launch as launch

STATE = "storage-online-terminal.json"
PROBE = "storage-online-terminal-worker.json"
COMMAND = ["-m", "scripts.automation.storage_online_terminal", "--worker"]


def _boot():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def _check_clock(journal):
    if (_boot() != journal["boot_id"] or time.time() >= journal["wall_deadline"]
            or time.monotonic() >= journal["monotonic_deadline"]):
        raise RuntimeError("storage_online_terminal_expired_or_rebooted_no_dispatch")


def _retire_previous_probe(root, original, package):
    """Recover create/start/stop uncertainty for this exact confined worker only."""
    path = root/PROBE
    if not os.path.lexists(path):
        return
    saved = host.load_receipt(path)
    if (saved.get("owner") != dict(root=str(root), worker=original["container_id"], package=package)
            or saved.get("name") != original["binding"]["project"]+"-storage-terminal"):
        raise RuntimeError("storage_online_terminal_probe_owner_changed")
    with host.docker_deadline(time.monotonic()+35):
        found = host.docker("ps", "-aq", "--no-trunc", "--filter", "name=^/"+saved["name"]+"$").split()
        if not found:
            if saved["container_id"] is not None and not saved.get("retired"):
                raise RuntimeError("storage_online_terminal_worker_missing_unretired")
            return
        if len(found) != 1 or saved["container_id"] not in (None, found[0]):
            raise RuntimeError("storage_online_terminal_worker_changed")
        identity = found[0]
        contract = launch._admit(identity, saved["binding"], saved["contract"], command=COMMAND)
        saved.update(container_id=identity, contract=contract)
        host.save_receipt(path, saved, initial=False)
        status = json.loads(host.docker("inspect", "--format", "{{json .State}}", identity))
        if status["Running"]:
            host.docker("stop", "--time", "5", identity, timeout=15)
        launch._admit(identity, saved["binding"], contract, command=COMMAND)
        status = json.loads(host.docker("inspect", "--format", "{{json .State}}", identity))
        if (status["Running"] or status["Pid"] != 0 or status["Status"] not in {"created", "exited"}
                or any(status[k] for k in ("Paused", "Restarting", "Dead"))):
            raise RuntimeError("storage_online_terminal_worker_retirement_unproven")
        saved["retired"] = True
        host.save_receipt(path, saved, initial=False)
        # Only this disposable terminal container, never the original worker or
        # any volumes. Retirement is durable before remove for crash recovery.
        host.docker("rm", identity)


def _probe(root, plan, original, package, *, action, wall_deadline, expected_capture=None, intent_sha256=None, original_request=None):
    """One fixed command, explicit read-only mounts, exact post-exit retirement."""
    from scripts.automation import storage_online_deadline as amendment
    request = (host.load_receipt(root/amendment.REQUEST) if original_request is None else original_request)
    if original_request is not None and (action != "reconcile"
            or amendment._sha(amendment.request_bytes(request)) != original["binding"]["request_sha256"]):
        raise RuntimeError("storage_online_terminal_original_request_changed")
    _retire_previous_probe(root, original, package)
    payload = dict(action=action, request=request, package=package, wall_deadline=wall_deadline,
                   expected_capture=expected_capture, intent_sha256=intent_sha256)
    data = amendment.request_bytes(payload)
    if len(data) > 65536:
        raise ValueError("storage_online_terminal_input_budget_exceeded")
    digest = amendment._sha(data)
    binding = deepcopy(original["binding"])
    db = host.database_details(binding["database_id"])
    collector = host.database_details(binding["clients"]["market-data-collector"]["id"])
    if (host.database_contract(db) != binding["database_contract"]
            or host.database_contract(collector) != binding["collector_contract"]):
        raise RuntimeError("storage_online_terminal_peer_changed")
    image_env = launch.inspect_candidate_image(package["image"], package)
    from scripts.automation.storage_online_worker import archive_group_override
    overrides = dict(PG_DSN=launch._dsn(db, collector), QT_DISABLE_DOTENV="1",
        QT_ARCHIVE_SHARED_GROUP_ID=archive_group_override(request), QT_LOGGING_LOKI_URL="",
        QT_ONLINE_REQUEST_SHA256=digest, QT_STORAGE_UDEV_ROOT="/run/qt-online/udev")
    binding.update(image=package["image"], request_sha256=digest,
        environment_sha256=host.digest(sorted(k+"="+v for k,v in {**image_env,**overrides}.items())))
    for mount in binding["mounts"].values():
        mount["readonly"] = True
    name = plan["project"]+"-storage-terminal"
    saved = dict(name=name, binding=binding, container_id=None, contract=None, retired=False,
                 owner=dict(root=str(root),worker=original["container_id"],package=package))
    host.save_receipt(root/PROBE, saved, initial=not os.path.lexists(root/PROBE))
    identity = host.docker(*launch._arguments(name, package["image"], binding["database_id"],
        binding["mounts"], overrides, binding["descriptor_limit"], binding["memory_bytes"], digest,
        command=COMMAND), env={**os.environ, "PG_DSN":overrides["PG_DSN"]}).strip()
    contract = launch._admit(identity, binding, command=COMMAND)
    saved.update(container_id=identity, contract=contract)
    host.save_receipt(root/PROBE, saved, initial=False)
    monotonic_deadline = host._DOCKER_DEADLINE.get()
    remaining = min(60, wall_deadline-time.time()-70,
                    monotonic_deadline-time.monotonic()-70 if monotonic_deadline is not None else 60)
    if remaining <= 0:
        _retire_previous_probe(root, original, package)
        raise RuntimeError("storage_online_terminal_worker_start_window_expired")
    process = subprocess.Popen(["docker", "start", "--attach", "--interactive", identity],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None)
    try:
        output, _ = process.communicate(data, timeout=remaining)
        if process.returncode != 0 or len(output) > 524288:
            raise RuntimeError("storage_online_terminal_worker_failed_or_output_exceeded")
        return json.loads(output)
    finally:
        # SQL reply/CLI exit alone cannot grant retirement or recovery access.
        launch._retire_worker(process, identity, binding, contract, command=COMMAND)
        _retire_previous_probe(root, original, package)


def _admit_original(root, path, plan, package, saved, *, deadline):
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_deadline as amendment
    if amendment._sha(path.read_bytes()) != package["plan_sha256"]:
        raise RuntimeError("storage_online_terminal_plan_changed")
    actual, _, found = launch.observe_owned_worker(root, plan["project"])
    if actual != saved or not saved or found != [saved["container_id"]]:
        raise RuntimeError("storage_online_terminal_original_worker_required")
    amendment._retired(saved)
    request = host.load_receipt(root/amendment.REQUEST)
    if amendment._sha(launch._canonical(plan["inventory_path"]).read_bytes()) != saved["binding"]["inventory_sha256"]:
        raise RuntimeError("storage_online_terminal_inventory_changed")
    if (amendment._sha((root/amendment.REQUEST).read_bytes()) != saved["binding"]["request_sha256"]
            or {k:v for k,v in request.items() if k != "capture_preparation"} != plan["request"]
            or saved["binding"]["image"] != plan["image"]
            or saved["binding"]["source_revision"] != plan["source_revision"]):
        raise RuntimeError("storage_online_terminal_original_request_changed")
    observed = saved.get("capture")
    if (not observed or observed["seconds"] != plan["attempt_seconds"]
            or datetime.fromisoformat(observed["started_at"]).timestamp()+observed["seconds"] != saved["deadline"]):
        raise RuntimeError("storage_online_terminal_original_capture_changed")
    arguments = {k:plan[k] for k in ("project", "source_revision", "source_image", "image", "request",
        "inventory_path", "keys_root", "socket_volume", "spool_destination")}
    observation = operation.inspect_prepared_operation(root, **arguments, deadline=deadline,
                                                       operator_id=saved["container_id"])
    rows = host.inventory(plan["project"], operator_id=saved["container_id"])
    if host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows):
        raise RuntimeError("storage_online_terminal_source_fleet_changed")
    if operation.load_operation_plan(path) != plan:
        raise RuntimeError("storage_online_terminal_plan_changed")
    launch.inspect_candidate_image(package["image"], package)
    return observation


def cancel_operation(path, *, package_file, execute=False):
    """Inspect by default; dispatch at most once under a durable 300s intent."""
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_deadline as amendment
    path = launch._canonical(path)
    plan = operation.load_operation_plan(path)
    root = launch._canonical(plan["state_root"])
    package = host.load_receipt(launch._canonical(package_file))
    if (type(execute) is not bool or set(package) != {"schema_version", "plan_sha256", "image", "source_revision", "source_tree_hash"}
            or package["schema_version"] != "qt.storage_online_terminal.v1"
            or any(not isinstance(package[k], str) or re.fullmatch(pattern, package[k]) is None
                   for k,pattern in (("plan_sha256",r"[0-9a-f]{64}"),("image",r"sha256:[0-9a-f]{64}"),
                       ("source_revision",r"[0-9a-f]{40}"),("source_tree_hash",r"[0-9a-f]{64}")))):
        raise ValueError("storage_online_terminal_package_invalid")
    operator_sha256 = amendment._sha(Path(__file__).read_bytes())
    with host.deployment_lock(root):
        for name in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
            if os.path.lexists(root/name):
                raise RuntimeError("storage_online_terminal_pre_final_source_required")
        for name in (amendment.STATE, amendment.PACKAGE_STATE):
            if os.path.lexists(root/name) and host.load_receipt(root/name, max_bytes=amendment.PACKAGE_JOURNAL_BYTES).get("phase") != "complete":
                raise RuntimeError("storage_online_terminal_amendment_unresolved")
        journal = host.load_receipt(root/STATE, max_bytes=524288) if os.path.lexists(root/STATE) else None
        if journal:
            check = {k:v for k,v in journal.items() if k not in {"intent_sha256", "phase", "receipt"}}
            if (host.digest(check) != journal["intent_sha256"] or journal["operation_path"] != str(path)
                    or journal["package"] != package or journal["operator_sha256"] != operator_sha256
                    or journal["phase"] not in {"prepared", "dispatched", "complete"}):
                raise RuntimeError("storage_online_terminal_intent_changed")
            saved = journal["worker"]
            if amendment._sha((root/launch._STATE).read_bytes()) != journal["worker_sha256"]:
                raise RuntimeError("storage_online_terminal_original_worker_file_changed")
        else:
            saved, _, found = launch.observe_owned_worker(root, plan["project"])
            if not saved or found != [saved["container_id"]]:
                raise RuntimeError("storage_online_terminal_original_worker_required")
        # A reconciliation observation has a new read-only bound, never new
        # dispatch authority. Prepared reentry retains the original intent clock.
        deadline = time.monotonic()+300
        wall = time.time()+300
        if journal and journal["phase"] == "prepared":
            _check_clock(journal)
            deadline, wall = journal["monotonic_deadline"], journal["wall_deadline"]
        with host.docker_deadline(deadline):
            _retire_previous_probe(root, saved, package)
            observation = _admit_original(root, path, plan, package, saved, deadline=deadline)
            if journal is None:
                original = _probe(root, plan, saved, package, action="inspect", wall_deadline=wall)
                if (original["attempt_seconds"] != saved["capture"]["seconds"]
                        or datetime.fromisoformat(original["prepared_at"]) != datetime.fromisoformat(saved["capture"]["started_at"])):
                    raise RuntimeError("storage_online_terminal_capture_changed")
                if not execute:
                    return dict(phase="terminal_inspected", storage_mutations_performed=False,
                                final_switch_authorized=False, capture_expired=saved["deadline"] <= time.time())
                journal = dict(operation_path=str(path), package=package, operator_sha256=operator_sha256,
                    worker=saved, worker_sha256=amendment._sha((root/launch._STATE).read_bytes()),
                    original_capture=original, observation=observation,
                    wall_deadline=wall, monotonic_deadline=deadline, boot_id=_boot())
                journal["intent_sha256"] = host.digest(journal)
                journal["phase"] = "prepared"
                _check_clock(journal)
                host.save_receipt(root/STATE, journal, initial=True)
            if journal["phase"] == "prepared":
                if not execute:
                    return dict(phase="terminal_prepared", storage_mutations_performed=False)
                _check_clock(journal)
                _admit_original(root, path, plan, package, saved, deadline=deadline)
                journal["phase"] = "dispatched"
                host.save_receipt(root/STATE, journal, initial=False)
                _check_clock(journal)
                _probe(root, plan, saved, package, action="apply", wall_deadline=wall,
                       expected_capture=journal["original_capture"], intent_sha256=journal["intent_sha256"])
            receipt = _probe(root, plan, saved, package, action="reconcile", wall_deadline=wall,
                       expected_capture=journal["original_capture"], intent_sha256=journal["intent_sha256"])
            if (not isinstance(receipt, dict) or receipt.get("schema_version") != "qt.fact_header_cancel.v2"
                    or receipt.get("intent_sha256") != journal["intent_sha256"]
                    or receipt.get("capture") != journal["original_capture"]
                    or receipt.get("source_retained") is not True or receipt.get("partial_copies_retained") is not True
                    or receipt.get("migration_ready") is not False or receipt.get("final_switch_authorized") is not False):
                raise RuntimeError("storage_online_terminal_outcome_unresolved_no_replay")
            _admit_original(root, path, plan, package, saved, deadline=deadline)
            journal.update(phase="complete", receipt=receipt)
            host.save_receipt(root/STATE, journal, initial=False)
            return dict(phase="attempt_cancelled", source_retained=True, partial_copies_retained=True,
                        migration_started=False, final_switch_authorized=False)


def worker_main():
    """Internal fixed-image command; caller must enforce host mount confinement."""
    import contextlib
    data = sys.stdin.buffer.read(65537)
    if len(data) > 65536 or hashlib.sha256(data).hexdigest() != os.environ.get("QT_ONLINE_REQUEST_SHA256"):
        raise RuntimeError("storage_online_terminal_input_changed")
    payload = json.loads(data)
    if (set(payload) != {"action", "request", "package", "wall_deadline", "expected_capture", "intent_sha256"}
            or payload["action"] not in {"inspect", "apply", "reconcile"}
            or type(payload["wall_deadline"]) not in (int, float)
            or not math.isfinite(payload["wall_deadline"]) or not 0 < payload["wall_deadline"]-time.time() <= 300):
        raise ValueError("storage_online_terminal_input_invalid")
    package, request = payload["package"], payload["request"]
    for name, variable in (("source_revision", "QT_IMAGE_SOURCE_REVISION"), ("source_tree_hash", "QT_IMAGE_SOURCE_TREE_HASH")):
        if package[name] != os.environ.get(variable):
            raise RuntimeError("storage_online_terminal_image_changed")
    from scripts.automation.storage_online_worker import enter_source_read_identity, validate_request_shape
    validate_request_shape(request)
    source = Path("/app/logs/market-structure/objects")
    enter_source_read_identity(source, expected_device=request["source_device"], expected_inode=request["source_inode"])
    with contextlib.redirect_stdout(sys.stderr):
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import NullPool
        from scripts.db import fact_header_v2_cancel as cancellation
        from scripts.db import fact_header_v2_capture as capture
        from scripts.automation.storage_online_controller import OnlineController
        engine = create_engine(os.environ["PG_DSN"], poolclass=NullPool, connect_args={"connect_timeout":5})
        try:
            with engine.begin() as conn:
                if payload["action"] != "apply":
                    conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                remaining = min(30, int(payload["wall_deadline"]-time.time()))
                if remaining < 1:
                    raise RuntimeError("storage_online_terminal_worker_expired")
                with capture._bounded_step(conn, remaining):
                    identity = conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
                    if identity != request["database_identity"]:
                        raise RuntimeError("storage_online_terminal_database_changed")
                    OnlineController._require_job_environment(conn, allow_connections=True)
                    OnlineController._supported_builtin_catalog(conn)
                    if payload["action"] == "inspect":
                        cancellation._require_retired_controller(conn)
                        capture.require_not_cancelled(conn)
                        capture.inspect_capture(conn)
                        result = cancellation._capture_binding(conn)
                    elif payload["action"] == "apply":
                        expected = payload["expected_capture"]
                        result = cancellation.cancel_attempt(conn,
                            expected_started_at=datetime.fromisoformat(expected["prepared_at"]).isoformat(),
                            source_root=source, destination_root=Path("/qt-history/archives/objects"),
                            expected_capture=expected, intent_sha256=payload["intent_sha256"], timeout_seconds=remaining, read_only_namespace=True)
                    else:
                        result = cancellation.inspect_cancellation(conn, expected_capture=payload["expected_capture"],
                            intent_sha256=payload["intent_sha256"], timeout_seconds=remaining)
        finally:
            engine.dispose()
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["--worker"]:
            raise ValueError("storage_online_terminal_private_entrypoint")
        worker_main()
    except Exception as exc:
        print("event=storage_online_terminal_failed error_type="+type(exc).__name__, file=sys.stderr, flush=True)
        raise SystemExit(1) from None

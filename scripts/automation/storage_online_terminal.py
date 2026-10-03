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
FORWARD_STATE = "storage-online-forward-terminal.json"
FORWARD_PROBE = "storage-online-forward-terminal-worker.json"
KEY_PROBE = "storage-online-key-preparation-worker.json"
COMMAND = ["-m", "scripts.automation.storage_online_terminal", "--worker"]


def _boot():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def _check_clock(journal):
    if (_boot() != journal["boot_id"] or time.time() >= journal["wall_deadline"]
            or time.monotonic() >= journal["monotonic_deadline"]):
        raise RuntimeError("storage_online_terminal_expired_or_rebooted_no_dispatch")


def _retire_previous_probe(root, original, package, *, forward=False, keys=False):
    """Recover create/start/stop uncertainty for this exact confined worker only."""
    path = root/(KEY_PROBE if keys else FORWARD_PROBE if forward else PROBE)
    if not os.path.lexists(path):
        return
    saved = host.load_receipt(path)
    if (saved.get("owner") != dict(root=str(root), worker=original["container_id"], package=package)
            or saved.get("name") != original["binding"]["project"]+("-storage-key-preparation" if keys else "-storage-forward-terminal" if forward else "-storage-terminal")):
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
    """One fixed command, exact declared mounts and post-exit retirement."""
    from scripts.automation import storage_online_deadline as amendment
    request = (host.load_receipt(root/amendment.REQUEST) if original_request is None else original_request)
    if original_request is not None and (action != "reconcile"
            or amendment._sha(amendment.request_bytes(request)) != original["binding"]["request_sha256"]):
        raise RuntimeError("storage_online_terminal_original_request_changed")
    from scripts.automation.storage_online_forward_worker import request_binding
    is_forward = request_binding(request) is not None
    is_keys = action in {"inspect_keys", "prepare_keys"}
    if is_keys and is_forward:
        raise RuntimeError("storage_key_preparation_before_publication_required")
    probe_options = {"keys":True} if is_keys else {"forward":True} if is_forward else {}
    probe_path = root/(KEY_PROBE if is_keys else FORWARD_PROBE if is_forward else PROBE)
    _retire_previous_probe(root, original, package, **probe_options)
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
    if not is_keys:
        for mount in binding["mounts"].values():
            mount["readonly"] = True
    name = plan["project"]+("-storage-key-preparation" if is_keys else "-storage-forward-terminal" if is_forward else "-storage-terminal")
    saved = dict(name=name, binding=binding, container_id=None, contract=None, retired=False,
                 owner=dict(root=str(root),worker=original["container_id"],package=package))
    host.save_receipt(probe_path, saved, initial=not os.path.lexists(probe_path))
    identity = host.docker(*launch._arguments(name, package["image"], binding["database_id"],
        binding["mounts"], overrides, binding["descriptor_limit"], binding["memory_bytes"], digest,
        command=COMMAND), env={**os.environ, "PG_DSN":overrides["PG_DSN"]}).strip()
    contract = launch._admit(identity, binding, command=COMMAND)
    saved.update(container_id=identity, contract=contract)
    host.save_receipt(probe_path, saved, initial=False)
    monotonic_deadline = host._DOCKER_DEADLINE.get()
    remaining = min(3600 if action == "prepare_keys" else 60, wall_deadline-time.time()-70,
                    monotonic_deadline-time.monotonic()-70 if monotonic_deadline is not None else 60)
    if remaining <= 0:
        _retire_previous_probe(root, original, package, **probe_options)
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
        _retire_previous_probe(root, original, package, **probe_options)


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
        from scripts.automation import storage_online_forward as forward
        if os.path.lexists(root/forward.STATE):
            return _cancel_forward_locked(root, path, plan, package, execute=execute)
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


def _admit_forward(root, path, plan, package, saved, *, deadline):
    """Verify the stopped forward worker and serving source without work admission."""
    from scripts.automation import storage_online_forward as forward
    from scripts.automation import storage_online_deadline as amendment
    from scripts.automation import storage_online_operation as operation
    published = forward.inspect_published_operation(root, operation_path=path)
    current = forward.load_launch(root)
    expected = dict(publication_sha256=published["intent_sha256"],
        request_sha256=saved["binding"]["request_sha256"],
        worker_binding_sha256=host.digest(saved["binding"]),
        operation_sha256=published["forward"]["operation_sha256"])
    if (amendment._sha(path.read_bytes()) != package["plan_sha256"]
            or operation.load_operation_plan(path) != plan or published["new_plan"] != plan
            or current.get("schema_version") != "qt.storage_online_forward_launch.v1"
            or current["binding"] != expected or current["worker"] != saved
            or current["pending_worker"] is not None
            or saved["binding"] != published["new_worker"]["binding"]):
        raise RuntimeError("storage_forward_terminal_launch_changed")
    # Retirement does not readmit work or renew launch clocks. Their exact
    # bytes are pinned below; the separate terminal intent owns dispatch time.
    # Read-only uncertain-COMMIT reconciliation remains possible after reboot.
    actual, _, found = launch.observe_owned_worker(root, plan["project"])
    if actual != saved or found != [saved["container_id"]]:
        raise RuntimeError("storage_forward_terminal_worker_changed")
    amendment._retired(saved)
    amendment._retired(published["old_worker"])
    arguments = {k:plan[k] for k in ("project", "source_revision", "source_image", "image",
        "inventory_path", "keys_root", "socket_volume", "spool_destination")}
    observation = operation.inspect_prepared_operation(root, **arguments,
        request=published["new_request"], deadline=deadline, operator_id=saved["container_id"])
    rows = host.inventory(plan["project"], operator_id=saved["container_id"])
    if host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows):
        raise RuntimeError("storage_forward_terminal_source_fleet_changed")
    launch.inspect_candidate_image(package["image"], package)
    return dict(publication_sha256=published["intent_sha256"], launch_sha256=host.digest(current),
                request_sha256=host.digest(published["new_request"]), observation=observation)


def _forward_result(value, *, request_sha256, capture=None):
    if (not isinstance(value, dict)
            or set(value) != {"schema_version", "capture", "request_sha256", "retired", "terminal_sha256",
                "source_retained", "partial_copies_retained", "migration_ready", "final_switch_authorized"}
            or value["schema_version"] != "qt.storage_forward_terminal.v1"
            or value["request_sha256"] != request_sha256
            or (capture is not None and value["capture"] != capture)
            or type(value["retired"]) is not bool
            or (value["terminal_sha256"] is not None if not value["retired"] else
                not isinstance(value["terminal_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", value["terminal_sha256"]))
            or any(value[k] is not True for k in ("source_retained", "partial_copies_retained"))
            or any(value[k] is not False for k in ("migration_ready", "final_switch_authorized"))):
        raise RuntimeError("storage_forward_terminal_outcome_unresolved_no_replay")
    return value


def _cancel_forward_locked(root, path, plan, package, *, execute):
    """One separate durable retirement intent; old cancellation stays immutable."""
    from scripts.automation import storage_online_forward as forward
    from scripts.automation.storage_online_forward_worker import capture_binding
    for name in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
        if os.path.lexists(root/name):
            raise RuntimeError("storage_forward_terminal_pre_final_source_required")
    published = forward.inspect_published_operation(root, operation_path=path)
    request = published["new_request"]
    operator_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    journal_path = root/FORWARD_STATE
    journal = host.load_receipt(journal_path, max_bytes=524288) if os.path.lexists(journal_path) else None
    saved = host.load_receipt(root/launch._STATE)
    if journal is not None:
        immutable = {k:v for k,v in journal.items() if k not in {"intent_sha256", "phase", "receipt"}}
        if (host.digest(immutable) != journal["intent_sha256"] or journal["operation_path"] != str(path)
                or journal["package"] != package or journal["operator_sha256"] != operator_sha256
                or journal["worker"] != saved or journal["phase"] not in {"prepared", "dispatched", "complete"}):
            raise RuntimeError("storage_forward_terminal_intent_changed")
    deadline, wall = time.monotonic()+300, time.time()+300
    if journal is not None and journal["phase"] == "prepared":
        _check_clock(journal)
        deadline, wall = journal["monotonic_deadline"], journal["wall_deadline"]
    with host.docker_deadline(deadline):
        _retire_previous_probe(root, saved, package, forward=True)
        observation = _admit_forward(root, path, plan, package, saved, deadline=deadline)
        if journal is not None and observation != journal["observation"]:
            raise RuntimeError("storage_forward_terminal_preimage_changed")
        if journal is None:
            inspected = _forward_result(_probe(root, plan, saved, package, action="inspect", wall_deadline=wall),
                request_sha256=host.digest(request))
            launch_saved = forward.load_launch(root)
            if (inspected["capture"].get("operation_sha256") != published["forward"]["operation_sha256"]
                    or (launch_saved["capture"] is not None
                        and capture_binding(launch_saved["capture"]) != inspected["capture"])):
                raise RuntimeError("storage_forward_terminal_adoption_changed")
            if not execute:
                return dict(phase="forward_retirement_inspected", already_retired=inspected["retired"],
                    storage_mutations_performed=False, final_switch_authorized=False)
            if inspected["retired"]:
                raise RuntimeError("storage_forward_terminal_unowned_retired_result")
            journal = dict(operation_path=str(path), package=package, operator_sha256=operator_sha256,
                worker=saved, observation=observation, original_capture=inspected["capture"],
                wall_deadline=wall, monotonic_deadline=deadline, boot_id=_boot())
            journal["intent_sha256"] = host.digest(journal)
            journal["phase"] = "prepared"
            _check_clock(journal)
            host.save_receipt(journal_path, journal, initial=True)
        if journal["phase"] == "prepared":
            if not execute:
                return dict(phase="forward_retirement_prepared", storage_mutations_performed=False)
            _check_clock(journal)
            if _admit_forward(root, path, plan, package, saved, deadline=deadline) != observation:
                raise RuntimeError("storage_forward_terminal_preimage_changed")
            journal["phase"] = "dispatched"
            host.save_receipt(journal_path, journal, initial=False)
            _check_clock(journal)
            _probe(root, plan, saved, package, action="apply", wall_deadline=wall,
                expected_capture=journal["original_capture"], intent_sha256=journal["intent_sha256"])
        receipt = _forward_result(_probe(root, plan, saved, package, action="reconcile", wall_deadline=wall,
            expected_capture=journal["original_capture"], intent_sha256=journal["intent_sha256"]),
            request_sha256=host.digest(request), capture=journal["original_capture"])
        if not receipt["retired"] or (journal["phase"] == "complete" and receipt != journal["receipt"]):
            raise RuntimeError("storage_forward_terminal_outcome_unresolved_no_replay")
        if _admit_forward(root, path, plan, package, saved, deadline=deadline) != observation:
            raise RuntimeError("storage_forward_terminal_preimage_changed")
        journal.update(phase="complete", receipt=receipt)
        host.save_receipt(journal_path, journal, initial=False)
        return dict(phase="forward_retired", source_retained=True, partial_copies_retained=True,
            migration_ready=False, final_switch_authorized=False)


def _forward_sql(conn, payload, *, timeout_seconds):
    """Same confined command and database guards, exact forward SQL owner."""
    from datetime import timedelta
    from scripts.db import fact_header_forward_adoption as adoption
    from scripts.automation.storage_online_forward_worker import (
        request_binding, capture_observation, capture_binding, _initial)
    request = payload["request"]
    intent = request_binding(request)
    with adoption._step(conn, timeout_seconds):
        state = adoption._state(conn)
        initial = _initial(conn)
        expected = dict(request_sha256=host.digest(request), operation_sha256=intent["operation_sha256"],
            cancellation_intent_sha256=intent["cancellation_intent_sha256"], key_seconds=3600,
            initial_seconds=600, attempt_seconds=intent["original_capture"]["attempt_seconds"])
        if (state is None or initial is None or initial["complete"] is not True
                or initial["binding"] != expected or initial["duration_seconds"] != 600
                or state["operation_sha256"] != intent["operation_sha256"]
                or state["attempt_seconds"] != expected["attempt_seconds"]
                or not initial["started_at"] <= state["started_at"] <= initial["expires_at"]
                or state["expires_at"] != state["started_at"]+timedelta(seconds=state["attempt_seconds"])):
            raise RuntimeError("storage_forward_terminal_initialization_changed")
        observation = capture_observation(state)
        canceled = observation["cancellation"]["receipt"]
        if (canceled.get("capture") != intent["original_capture"]
                or canceled.get("intent_sha256") != intent["cancellation_intent_sha256"]
                or canceled.get("source_retained") is not True or canceled.get("partial_copies_retained") is not True):
            raise RuntimeError("storage_forward_terminal_cancellation_changed")
        identity = capture_binding(observation)
        if payload["action"] != "inspect" and payload["expected_capture"] != identity:
            raise RuntimeError("storage_forward_terminal_adoption_changed")
        receipt = adoption.inspect_retirement(conn, operation_sha256=intent["operation_sha256"],
                                              timeout_seconds=timeout_seconds)
        if payload["action"] == "apply":
            if receipt is not None:
                raise RuntimeError("storage_forward_terminal_already_retired_no_dispatch")
            adoption.retire_adoption(conn, operation_sha256=intent["operation_sha256"],
                                     timeout_seconds=timeout_seconds, read_only_namespace=True)
            receipt = adoption.inspect_retirement(conn, operation_sha256=intent["operation_sha256"],
                                                  timeout_seconds=timeout_seconds)
        result = dict(schema_version="qt.storage_forward_terminal.v1", capture=identity,
            request_sha256=host.digest(request), retired=receipt is not None,
            terminal_sha256=host.digest(receipt) if receipt is not None else None,
            source_retained=True, partial_copies_retained=True,
            migration_ready=False, final_switch_authorized=False)
        if payload["action"] == "reconcile" and receipt is None:
            return None
        return result


def worker_main():
    """Internal fixed-image command; caller must enforce host mount confinement."""
    import contextlib
    data = sys.stdin.buffer.read(65537)
    if len(data) > 65536 or hashlib.sha256(data).hexdigest() != os.environ.get("QT_ONLINE_REQUEST_SHA256"):
        raise RuntimeError("storage_online_terminal_input_changed")
    payload = json.loads(data)
    if (set(payload) != {"action", "request", "package", "wall_deadline", "expected_capture", "intent_sha256"}
            or payload["action"] not in {"inspect", "apply", "reconcile", "inspect_keys", "prepare_keys"}
            or type(payload["wall_deadline"]) not in (int, float)
            or not math.isfinite(payload["wall_deadline"]) or not 0 < payload["wall_deadline"]-time.time() <= (3600 if payload["action"] == "prepare_keys" else 300)):
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
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        engine = create_engine(os.environ["PG_DSN"], poolclass=NullPool, connect_args={"connect_timeout":5})
        try:
            if payload["action"] in {"inspect_keys", "prepare_keys"}:
                from scripts.automation.storage_online_keys import worker_keys
                result = worker_keys(engine, payload)
            else:
                result = _terminal_sql(engine, payload, request, source)
        finally:
            engine.dispose()
    print(json.dumps(result), flush=True)


def _terminal_sql(engine, payload, request, source):
    from sqlalchemy import text
    from scripts.db import fact_header_v2_cancel as cancellation
    from scripts.db import fact_header_v2_capture as capture
    from scripts.automation.storage_online_controller import OnlineController
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
            from scripts.automation.storage_online_forward_worker import request_binding
            if request_binding(request) is not None:
                result = _forward_sql(conn, payload, timeout_seconds=remaining)
            elif payload["action"] == "inspect":
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
    return result


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["--worker"]:
            raise ValueError("storage_online_terminal_private_entrypoint")
        worker_main()
    except Exception as exc:
        print("event=storage_online_terminal_failed error_type="+type(exc).__name__, file=sys.stderr, flush=True)
        raise SystemExit(1) from None

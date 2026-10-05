"""Preserving package publication for a separately owned forward operation.

Package publication preserves the canceled plan/capture, terminal receipt and
retired worker as evidence. Separate launch intent owns worker progression and
actual preparation clocks; publication alone grants no launch or source stop.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
import logging
import os
from pathlib import Path
import re
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_deadline as publication
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_runtime as runtime
from scripts.automation import storage_online_terminal as terminal

LOG = logging.getLogger(__name__)
STATE = "storage-online-forward.json"
_MAX_BYTES = publication.PACKAGE_JOURNAL_BYTES


def operation_file(name, *, request=None, operation_sha256=None):
    """Select one explicit successor journal; legacy names remain immutable."""
    from scripts.automation.storage_online_forward_worker import request_binding
    if request is not None:
        intent = request_binding(request)
        selected = intent["operation_sha256"] if intent and intent["schema_version"] == "qt.storage_online_forward_intent.v2" else None
        if operation_sha256 is not None and operation_sha256 != selected:
            raise ValueError("storage_forward_journal_operation_changed")
        operation_sha256 = selected
    if operation_sha256 is None:
        return name
    if not isinstance(operation_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", operation_sha256):
        raise ValueError("storage_forward_journal_operation_invalid")
    return name.removesuffix(".json")+"-"+operation_sha256+".json"


def _terminal(root, original_path):
    value = host.load_receipt(root/terminal.STATE, max_bytes=524288)
    check = {k:v for k,v in value.items() if k not in {"intent_sha256", "phase", "receipt"}}
    receipt = value.get("receipt", {})
    if (value.get("phase") != "complete" or value.get("operation_path") != str(original_path)
            or host.digest(check) != value.get("intent_sha256")
            or receipt.get("schema_version") != "qt.fact_header_cancel.v2"
            or receipt.get("intent_sha256") != value["intent_sha256"]
            or receipt.get("capture") != value.get("original_capture")
            or any(receipt.get(k) is not True for k in ("source_retained", "partial_copies_retained"))
            or any(receipt.get(k) is not False for k in ("migration_ready", "final_switch_authorized"))):
        raise RuntimeError("storage_forward_committed_cancellation_required")
    return value


def _clock(journal):
    if terminal._boot() != journal["boot_id"]:
        raise RuntimeError("storage_forward_publication_rebooted")
    if time.time() < journal["started_at"] or time.monotonic() < journal["started_monotonic"]:
        raise RuntimeError("storage_forward_publication_clock_reversed")
    publication._check_clock(journal["wall_deadline"], journal["monotonic_deadline"])


def _new_plan_path(value, original):
    if not isinstance(value, str):
        raise ValueError("storage_forward_plan_path_invalid")
    path = Path(value)
    if (not path.is_absolute() or path.parent != original.parent or path == original
            or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*\.json", path.name)
            or path.is_symlink() or path.parent.resolve(strict=True) != path.parent):
        raise ValueError("storage_forward_plan_path_invalid")
    return path


def _publish_plan(path, expected):
    if os.path.lexists(path):
        if host.load_receipt(path) != expected or path.read_bytes() != (json.dumps(expected, sort_keys=True)+"\n").encode():
            raise RuntimeError("storage_forward_plan_file_changed")
    else:
        host.save_receipt(path, expected, initial=True)


def _admit_preimages(root, original_path, new_path, journal):
    """Refuse foreign edits across the whole publication before any mutation."""
    if (original_path.read_text() != journal["original_plan_bytes"]
            or publication._sha((root/journal.get("terminal_file", terminal.STATE)).read_bytes()) != journal["terminal_sha256"]):
        raise RuntimeError("storage_forward_original_evidence_changed")
    for name, digest in journal.get("preserved_files", {}).items():
        if publication._sha((root/name).read_bytes()) != digest:
            raise RuntimeError("storage_forward_predecessor_evidence_changed")
    pairs = ((root/publication.REQUEST, journal["old_request_bytes"].encode(), publication.request_bytes(journal["new_request"])),
        (root/launch._STATE, journal["old_worker_bytes"].encode(), (json.dumps(journal["new_worker"], sort_keys=True)+"\n").encode()),
        (root/runtime.RUNTIME_RECIPE, journal["old_runtime_bytes"].encode(), (json.dumps(journal["new_runtime"], sort_keys=True)+"\n").encode()))
    for target, before, after in pairs:
        host.load_receipt(target, max_bytes=max(65536, len(before), len(after)))
        if target.read_bytes() not in (before, after):
            raise RuntimeError("storage_forward_preimages_changed")
    if os.path.lexists(new_path):
        expected = (json.dumps(journal["new_plan"], sort_keys=True)+"\n").encode()
        host.load_receipt(new_path, max_bytes=max(65536, len(expected)))
        if new_path.read_bytes() != expected:
            raise RuntimeError("storage_forward_plan_file_changed")


def _successor_predecessor(root, package):
    """Admit the explicitly retired first forward attempt, never discover latest."""
    prior = host.load_receipt(root/STATE, max_bytes=_MAX_BYTES)
    if prior.get("forward", {}).get("schema_version") != "qt.storage_online_forward_intent.v1":
        raise RuntimeError("storage_forward_first_predecessor_required")
    _inspect_publication(root, prior, active=False)
    old_path = launch._canonical(prior["package"]["forward_plan_path"])
    name = package["predecessor_terminal_file"]
    if name not in (terminal.FORWARD_STATE, terminal.FORWARD_RECOVERY_STATE):
        raise ValueError("storage_forward_predecessor_terminal_file_invalid")
    target = root/name
    retired = host.load_receipt(target, max_bytes=524288)
    immutable = {k:v for k,v in retired.items() if k not in {"intent_sha256", "phase", "receipt"}}
    receipt = terminal._forward_result(retired.get("receipt"), request_sha256=host.digest(prior["new_request"]))
    old_launch = load_launch(root)
    if (retired.get("phase") != "complete" or host.digest(immutable) != retired.get("intent_sha256")
            or publication._sha(target.read_bytes()) != package["predecessor_terminal_file_sha256"]
            or retired.get("operation_path") != str(old_path)
            or receipt["retired"] is not True
            or receipt["terminal_sha256"] != package["predecessor_terminal_sha256"]
            or receipt["capture"].get("operation_sha256") != package["predecessor_operation_sha256"]
            or prior["forward"]["operation_sha256"] != package["predecessor_operation_sha256"]
            or retired.get("original_capture") != receipt["capture"]
            or old_launch["worker"] != retired["worker"] or old_launch["pending_worker"] is not None
            or retired["observation"].get("launch_sha256") != host.digest(old_launch)
            or retired["observation"].get("publication_sha256") != prior["intent_sha256"]
            or retired["worker"]["binding"] != prior["new_worker"]["binding"]):
        raise RuntimeError("storage_forward_retired_predecessor_changed")
    if name == terminal.FORWARD_RECOVERY_STATE:
        terminal._recovery_predecessor(root, old_path, retired["package"], retired["worker"])
    files = {name: publication._sha((root/name).read_bytes()) for name in
        (STATE, LAUNCH_STATE, terminal.STATE, package["predecessor_terminal_file"])}
    if name == terminal.FORWARD_RECOVERY_STATE:
        files[terminal.FORWARD_STATE] = publication._sha((root/terminal.FORWARD_STATE).read_bytes())
    return prior, retired, old_path, files


def observe_lookup_placement(database_id, package):
    """Bounded catalog-only proof before publishing the explicit successor."""
    present = host.database_query(database_id,
        "SELECT (to_regclass('qt_fact_header_forward_v2.lookup_placement') IS NOT NULL)::text", read_only_seconds=5)
    if present.strip() != "true":
        raise RuntimeError("storage_forward_lookup_completion_required")
    rows = json.loads(host.database_query(database_id,
        "SELECT json_agg(r)::text FROM (SELECT operation_sha256,binding->>'predecessor_operation_sha256' AS predecessor_operation_sha256,"
        "binding->>'predecessor_terminal_sha256' AS predecessor_terminal_sha256,completion IS NOT NULL AS complete "
        "FROM qt_fact_header_forward_v2.lookup_placement LIMIT 2) r", read_only_seconds=5))
    expected = dict(operation_sha256=package["lookup_operation_sha256"],
        predecessor_operation_sha256=package["predecessor_operation_sha256"],
        predecessor_terminal_sha256=package["predecessor_terminal_sha256"], complete=True)
    if rows != [expected]:
        raise RuntimeError("storage_forward_lookup_completion_changed")
    return expected


def publish_package(path, *, package_file, execute=False):
    """Inspect or publish one exact forward package under one original 300s intent.

    The existing migrate CLI owns this explicit phase. Both complete prepared
    runtime preflights and a live read-only cancellation reconciliation precede
    publication. Interrupted writes accept only recorded old/new bytes. The
    original operation file and original terminal marker are never overwritten.
    """
    from scripts.automation import storage_online_operation as operation

    if type(execute) is not bool:
        raise ValueError("storage_forward_execute_invalid")
    path = launch._canonical(path)
    plan = operation.load_operation_plan(path)
    root = launch._canonical(plan["state_root"])
    package = host.load_receipt(launch._canonical(package_file))
    successor = isinstance(package, dict) and package.get("schema_version") == "qt.storage_online_forward_package.v2"
    fields = {"schema_version", "plan_sha256", "image", "source_revision", "source_tree_hash", "forward_plan_path", "end_day"}
    lineage = {"predecessor_operation_sha256", "predecessor_terminal_sha256", "lookup_operation_sha256"}
    if successor:
        fields |= lineage | {"predecessor_terminal_file", "predecessor_terminal_file_sha256"}
    if (not isinstance(package, dict) or set(package) != fields
            or package["schema_version"] not in ("qt.storage_online_forward_package.v1", "qt.storage_online_forward_package.v2")
            or successor and any(not isinstance(package[k], str) or not re.fullmatch(r"[0-9a-f]{64}", package[k])
                for k in lineage | {"predecessor_terminal_file_sha256"})
            or any(not isinstance(package[k], str) or not re.fullmatch(pattern, package[k])
                for k,pattern in (("plan_sha256", r"[0-9a-f]{64}"), ("image", r"sha256:[0-9a-f]{64}"),
                    ("source_revision", r"[0-9a-f]{40}"), ("source_tree_hash", r"[0-9a-f]{64}")))
            or not isinstance(package["end_day"], str)
            or date.fromisoformat(package["end_day"]).isoformat() != package["end_day"]):
        raise ValueError("storage_forward_package_invalid")
    new_path = _new_plan_path(package["forward_plan_path"], path)
    operator = publication._sha(b"".join(Path(module.__file__).read_bytes() for module in
        (publication, terminal, launch, operation, host, runtime)) + Path(__file__).read_bytes())
    with host.deployment_lock(root):
        if execute:
            from scripts.automation.storage_online_keys import require_finished
            require_finished(root)
        for name in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
            if os.path.lexists(root/name):
                raise RuntimeError("storage_forward_pre_final_source_required")
        for name in (publication.STATE, publication.PACKAGE_STATE):
            if os.path.lexists(root/name) and host.load_receipt(root/name, max_bytes=_MAX_BYTES).get("phase") != "complete":
                raise RuntimeError("storage_forward_prior_publication_unresolved")
        prior = None
        if successor:
            prior, canceled, predecessor_path, preserved_files = _successor_predecessor(root, package)
            if predecessor_path != path:
                raise RuntimeError("storage_forward_predecessor_plan_changed")
            from scripts.automation.storage_online_keys import require_lookups_finished
            require_lookups_finished(root, package)
            terminal_file = package["predecessor_terminal_file"]
            old_intent = prior["forward"]
        else:
            canceled = _terminal(root, path)
            terminal_file = terminal.STATE
            old_intent = canceled
        forward = dict(schema_version="qt.storage_online_forward_intent.v2" if successor else "qt.storage_online_forward_intent.v1",
            original_plan_sha256=package["plan_sha256"], cancellation_intent_sha256=old_intent["cancellation_intent_sha256"] if successor else canceled["intent_sha256"],
            original_capture=old_intent["original_capture"], end_day=package["end_day"],
            candidate_revision=package["source_revision"], candidate_source_hash=package["source_tree_hash"])
        if successor:
            forward.update({k:package[k] for k in lineage})
        forward["operation_sha256"] = host.digest(forward)
        from scripts.automation.storage_online_forward_worker import request_binding
        request_binding({**plan["request"], **{k:package[k] for k in ("source_revision", "source_tree_hash")}, "forward":forward})
        state_file = operation_file(STATE, operation_sha256=forward["operation_sha256"] if successor else None)
        terminal_sha256 = publication._sha((root/terminal_file).read_bytes())
        journal = host.load_receipt(root/state_file, max_bytes=_MAX_BYTES) if os.path.lexists(root/state_file) else None
        if journal:
            immutable = {k:v for k,v in journal.items() if k not in {"intent_sha256", "phase"}}
            if (host.digest(immutable) != journal.get("intent_sha256")
                    or journal.get("phase") not in {"prepared", "publishing", "complete"}
                    or journal.get("package") != package or journal.get("operator_sha256") != operator
                    or journal.get("original_plan_bytes") != path.read_text()
                    or journal.get("terminal_sha256") != publication._sha((root/terminal_file).read_bytes())):
                raise RuntimeError("storage_forward_publication_intent_changed")
            if journal["phase"] == "complete":
                _verify_published(root, new_path, journal)
                return dict(phase="forward_package_recorded", migration_started=False,
                    current_source_verified=False, forward_worker_authorized=False)
            _clock(journal)
            _admit_preimages(root, path, new_path, journal)
        else:
            if (publication._sha(path.read_bytes()) != package["plan_sha256"]
                    or os.path.lexists(new_path)):
                raise RuntimeError("storage_forward_original_plan_required")
        if journal is None:
            for name in (publication.REQUEST, launch._STATE, runtime.RUNTIME_RECIPE):
                host.load_receipt(root/name, max_bytes=524288 if name == runtime.RUNTIME_RECIPE else 65536)
        saved = canceled["worker"]
        if journal is None and publication._sha((root/launch._STATE).read_bytes()) != (publication._sha((json.dumps(canceled["worker"], sort_keys=True)+"\n").encode()) if successor else canceled["worker_sha256"]):
            raise RuntimeError("storage_forward_original_worker_file_changed")
        wall, mono = time.time(), time.monotonic()
        clock = journal or dict(started_at=wall, started_monotonic=mono,
            wall_deadline=wall+300, monotonic_deadline=mono+300, boot_id=terminal._boot())
        _clock(clock)
        with host.docker_deadline(clock["monotonic_deadline"]):
            details = publication._retired(saved)
            old_request_bytes = journal["old_request_bytes"] if journal else (root/publication.REQUEST).read_text()
            old_request = json.loads(old_request_bytes)
            if (publication._sha(old_request_bytes.encode()) != saved["binding"]["request_sha256"]
                    or {k:v for k,v in old_request.items() if k not in ({"capture_preparation", "forward"} if successor else {"capture_preparation"})} != plan["request"]
                    or successor and old_request != prior["new_request"]
                    or publication._sha(launch._canonical(plan["inventory_path"]).read_bytes()) != saved["binding"]["inventory_sha256"]):
                raise RuntimeError("storage_forward_original_request_changed")
            rows = host.inventory(plan["project"], operator_id=saved["container_id"])
            if host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows):
                raise RuntimeError("storage_forward_source_fleet_changed")
            new_plan = deepcopy(plan)
            new_plan["image"] = package["image"]
            new_plan["request"].update({k:package[k] for k in ("source_revision", "source_tree_hash")})
            runtime_path = root/runtime.RUNTIME_RECIPE
            old_runtime_bytes = journal["old_runtime_bytes"] if journal else runtime_path.read_text()
            old_runtime = json.loads(old_runtime_bytes)
            new_runtime = deepcopy(old_runtime)
            for name in runtime._APPLICATIONS:
                if old_runtime["services"][name].get("image") != plan["image"]:
                    raise RuntimeError("storage_forward_original_runtime_changed")
                new_runtime["services"][name]["image"] = package["image"]
            new_runtime_bytes = (json.dumps(new_runtime, sort_keys=True)+"\n").encode()
            if runtime_path.read_bytes() not in (old_runtime_bytes.encode(), new_runtime_bytes):
                raise RuntimeError("storage_forward_runtime_file_changed")
            for candidate, recipe in ((plan, old_runtime), (new_plan, new_runtime)):
                args = {k:candidate[k] for k in ("project", "source_revision", "source_image", "image", "request",
                    "inventory_path", "keys_root", "socket_volume", "spool_destination")}
                operation.inspect_prepared_operation(root, **args, deadline=clock["monotonic_deadline"],
                    operator_id=saved["container_id"], proposed_runtime_recipe=recipe)
            probe_package = canceled["package"]
            probe_options = {}
            if successor:
                # Reconcile the old receipt using code that understands the
                # explicit lookup transition; retain the old probe journals too.
                probe_package = {**probe_package, **{k:package[k] for k in ("image", "source_revision", "source_tree_hash")}}
                probe_options["probe_operation_sha256"] = host.digest(dict(successor=forward["operation_sha256"], purpose="publication_reconciliation"))
            receipt = terminal._probe(root, plan, saved, probe_package, action="reconcile",
                wall_deadline=clock["wall_deadline"], expected_capture=canceled["original_capture"],
                intent_sha256=canceled["intent_sha256"], original_request=old_request, **probe_options)
            if receipt != canceled["receipt"]:
                raise RuntimeError("storage_forward_cancellation_proof_changed")
            if successor:
                observe_lookup_placement(saved["binding"]["database_id"], package)
            _clock(clock)
            if not execute:
                return dict(phase="forward_package_reconciliation_required" if journal else "forward_package_inspected",
                    storage_mutations_performed=False, migration_started=False, forward_worker_authorized=False)
            if journal is None:
                # Separate forward identity; the expired capture's start/expiry
                # and the original initial-preparation receipt are never renewed.
                new_request = {**deepcopy(new_plan["request"]), "forward": forward}
                image_env = launch.inspect_candidate_image(package["image"], new_plan["request"])
                old_env = dict(value.split("=", 1) for value in details["config"]["Env"])
                overrides = {k:old_env[k] for k in ("PG_DSN", "QT_DISABLE_DOTENV", "QT_ARCHIVE_SHARED_GROUP_ID",
                    "QT_LOGGING_LOKI_URL", "QT_STORAGE_UDEV_ROOT")}
                digest = publication._sha(publication.request_bytes(new_request))
                overrides["QT_ONLINE_REQUEST_SHA256"] = digest
                new_worker = deepcopy(saved)
                new_worker.pop("capture", None)
                new_worker.update(container_id=None, contract=None, deadline=None)
                new_worker["binding"].update(image=package["image"], request_sha256=digest,
                    environment_sha256=host.digest(sorted(k+"="+v for k,v in {**image_env,**overrides}.items())))
                journal = dict(package=package, operator_sha256=operator, original_plan_bytes=path.read_text(),
                    terminal_sha256=terminal_sha256, old_worker=saved,
                    old_worker_bytes=(root/launch._STATE).read_text(), old_request_bytes=old_request_bytes,
                    old_runtime_bytes=old_runtime_bytes, new_runtime=new_runtime, new_request=new_request,
                    new_worker=new_worker, new_plan=new_plan, forward=forward, **clock)
                if successor:
                    journal.update(terminal_file=terminal_file, preserved_files=preserved_files)
                journal["intent_sha256"] = host.digest(journal)
                journal["phase"] = "prepared"
                if len(json.dumps(journal).encode()) > _MAX_BYTES-1:
                    raise ValueError("storage_forward_intent_budget_exceeded")
                if (publication._sha(path.read_bytes()) != package["plan_sha256"]
                        or publication._sha((root/launch._STATE).read_bytes()) != (publication._sha((json.dumps(canceled["worker"], sort_keys=True)+"\n").encode()) if successor else canceled["worker_sha256"])
                        or (root/publication.REQUEST).read_text() != old_request_bytes
                        or runtime_path.read_text() != old_runtime_bytes):
                    raise RuntimeError("storage_forward_preimages_changed")
                _admit_preimages(root, path, new_path, journal)
                host.save_receipt(root/state_file, journal, initial=True)
            _clock(journal)
            _admit_preimages(root, path, new_path, journal)
            rows = host.inventory(plan["project"], operator_id=saved["container_id"])
            if (publication._sha(launch._canonical(plan["inventory_path"]).read_bytes()) != saved["binding"]["inventory_sha256"]
                    or host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows)):
                raise RuntimeError("storage_forward_source_fleet_changed")
            LOG.info("storage_forward_publication_start project=%s intent_sha256=%s", plan["project"], journal["intent_sha256"])
            journal["phase"] = "publishing"
            host.save_receipt(root/state_file, journal, initial=False)
            publication._retired(saved)
            archived = plan["project"]+"-storage-online-forward-before-"+journal["intent_sha256"][:16]
            name = host.docker("inspect", "--format", "{{.Name}}", saved["container_id"]).strip()
            if name == "/"+plan["project"]+"-storage-online":
                host.docker("rename", saved["container_id"], archived)
            elif name != "/"+archived:
                raise RuntimeError("storage_forward_retired_worker_name_changed")
            for target, before, after in (
                (runtime_path, journal["old_runtime_bytes"].encode(), new_runtime_bytes),
                (root/publication.REQUEST, journal["old_request_bytes"].encode(), publication.request_bytes(journal["new_request"])),
                (root/launch._STATE, journal["old_worker_bytes"].encode(), (json.dumps(journal["new_worker"], sort_keys=True)+"\n").encode())):
                _clock(journal)
                publication._replace(target, before, after)
            _clock(journal)
            _publish_plan(new_path, journal["new_plan"])
            _clock(journal)
            _verify_published(root, new_path, journal)
            journal["phase"] = "complete"
            host.save_receipt(root/state_file, journal, initial=False)
            LOG.info("storage_forward_publication_complete project=%s intent_sha256=%s", plan["project"], journal["intent_sha256"])
            return dict(phase="forward_package_published", migration_started=False, forward_worker_authorized=False)


def _verify_published(root, new_path, journal):
    for path, expected in ((root/publication.REQUEST, publication.request_bytes(journal["new_request"])),
            (root/launch._STATE, (json.dumps(journal["new_worker"], sort_keys=True)+"\n").encode()),
            (root/runtime.RUNTIME_RECIPE, (json.dumps(journal["new_runtime"], sort_keys=True)+"\n").encode()),
            (new_path, (json.dumps(journal["new_plan"], sort_keys=True)+"\n").encode())):
        host.load_receipt(path, max_bytes=max(65536, len(expected)))
        if path.read_bytes() != expected:
            raise RuntimeError("storage_forward_published_file_changed")



def inspect_published_operation(root, *, request=None, operation_path=None):
    """Admit immutable publication evidence; no launch, SQL or clock renewal.

    A completed publication outlives its publication window. Execution must own
    separate original phase receipts and fresh source/runtime/resource admission.
    The canonical worker's mutable lifecycle is deliberately checked by its
    launcher, against the immutable new binding returned here.
    """
    from scripts.automation.storage_online_forward_worker import request_binding

    root = launch._canonical(root)
    selected_request = request if request is not None else host.load_receipt(root/publication.REQUEST)
    journal = host.load_receipt(root/operation_file(STATE, request=selected_request), max_bytes=_MAX_BYTES)
    return _inspect_publication(root, journal, request=request, operation_path=operation_path)


def _inspect_publication(root, journal, *, request=None, operation_path=None, active=True):
    from scripts.automation.storage_online_forward_worker import request_binding
    immutable = {k:v for k,v in journal.items() if k not in {"intent_sha256", "phase"}}
    if journal.get("phase") != "complete" or host.digest(immutable) != journal.get("intent_sha256"):
        raise RuntimeError("storage_forward_completed_publication_required")
    package = journal["package"]
    successor = package["schema_version"] == "qt.storage_online_forward_package.v2"
    if successor:
        prior, canceled, original_path, preserved = _successor_predecessor(root, package)
        if journal.get("terminal_file") != package["predecessor_terminal_file"] or journal.get("preserved_files") != preserved:
            raise RuntimeError("storage_forward_predecessor_evidence_changed")
        old_intent = prior["forward"]
    else:
        canceled = host.load_receipt(root/terminal.STATE, max_bytes=524288)
        original_path = launch._canonical(canceled["operation_path"])
        canceled = _terminal(root, original_path)
        old_intent = dict(original_capture=canceled["original_capture"], cancellation_intent_sha256=canceled["intent_sha256"])
    new_path = _new_plan_path(package["forward_plan_path"], original_path)
    if operation_path is not None and launch._canonical(operation_path) != new_path:
        raise RuntimeError("storage_forward_operation_path_changed")
    expected = journal["new_request"]
    intent = request_binding(expected)
    original = json.loads(journal["original_plan_bytes"])
    candidate = deepcopy(original)
    candidate["image"] = package["image"]
    candidate["request"].update({k:package[k] for k in ("source_revision", "source_tree_hash")})
    old_request = json.loads(journal["old_request_bytes"])
    expected_runtime = json.loads(journal["old_runtime_bytes"])
    for name in runtime._APPLICATIONS:
        if expected_runtime["services"][name].get("image") != original["image"]:
            raise RuntimeError("storage_forward_original_runtime_changed")
        expected_runtime["services"][name]["image"] = package["image"]
    expected_worker = deepcopy(journal["old_worker"])
    expected_worker.pop("capture", None)
    expected_worker.update(container_id=None, contract=None, deadline=None)
    environment = journal["new_worker"]["binding"].get("environment_sha256")
    if not isinstance(environment, str) or not re.fullmatch(r"[0-9a-f]{64}", environment):
        raise RuntimeError("storage_forward_published_environment_invalid")
    expected_worker["binding"].update(image=package["image"],
        request_sha256=publication._sha(publication.request_bytes(expected)), environment_sha256=environment)
    if (intent is None or intent != journal["forward"]
            or journal["original_plan_bytes"] != original_path.read_text()
            or publication._sha(original_path.read_bytes()) != package["plan_sha256"]
            or publication._sha((root/journal.get("terminal_file", terminal.STATE)).read_bytes()) != journal["terminal_sha256"]
            or journal["old_worker"] != canceled["worker"]
            or publication._sha(journal["old_worker_bytes"].encode()) != (publication._sha((json.dumps(canceled["worker"], sort_keys=True)+"\n").encode()) if successor else canceled["worker_sha256"])
            or json.loads(journal["old_worker_bytes"]) != canceled["worker"]
            or publication._sha(journal["old_request_bytes"].encode()) != canceled["worker"]["binding"]["request_sha256"]
            or intent["original_plan_sha256"] != package["plan_sha256"]
            or intent["original_capture"] != old_intent["original_capture"]
            or intent["cancellation_intent_sha256"] != old_intent["cancellation_intent_sha256"]
            or intent["end_day"] != package["end_day"]
            or expected != {**candidate["request"], "forward":intent}
            or journal["new_plan"] != candidate
            or {k:v for k,v in old_request.items() if k not in ({"capture_preparation", "forward"} if successor else {"capture_preparation"})} != original["request"]
            or successor and (old_request != prior["new_request"] or any(intent[k] != package[k] for k in
                ("predecessor_operation_sha256", "predecessor_terminal_sha256", "lookup_operation_sha256")))
            or journal["new_runtime"] != expected_runtime
            or journal["new_worker"] != expected_worker
            or (request is not None and request != expected)):
        raise RuntimeError("storage_forward_publication_binding_changed")
    checks = [(new_path, (json.dumps(candidate, sort_keys=True)+"\n").encode())]
    if active:
        checks += [(root/publication.REQUEST, publication.request_bytes(expected)),
            (root/runtime.RUNTIME_RECIPE, (json.dumps(journal["new_runtime"], sort_keys=True)+"\n").encode())]
    for path, data in checks:
        host.load_receipt(path, max_bytes=max(65536, len(data)))
        if path.read_bytes() != data:
            raise RuntimeError("storage_forward_published_file_changed")
    if (publication._sha(launch._canonical(candidate["inventory_path"]).read_bytes())
            != journal["old_worker"]["binding"]["inventory_sha256"]):
        raise RuntimeError("storage_forward_inventory_changed")
    for name in (publication.STATE, publication.PACKAGE_STATE):
        if os.path.lexists(root/name) and host.load_receipt(root/name, max_bytes=_MAX_BYTES).get("phase") != "complete":
            raise RuntimeError("storage_forward_prior_publication_unresolved")
    return journal


def admit_adoption_observation(request, *, initialization, capture, now):
    """Bind actual SQL clocks to the published request, never the old expiry.

    This read-only validator is shared by host launch and final observations.
    It confers no publication, launch, source-stop, COMMIT or replay authority.
    """
    from datetime import datetime, timedelta
    import math
    from scripts.automation.storage_online_forward_worker import request_binding

    intent = request_binding(request)
    if intent is None or type(now) not in (int, float) or not math.isfinite(now):
        raise ValueError("storage_forward_observation_inputs_invalid")
    def instant(value):
        if not isinstance(value, str):
            raise RuntimeError("storage_forward_observation_clock_invalid")
        result = datetime.fromisoformat(value)
        if result.utcoffset() != timedelta(0):
            raise RuntimeError("storage_forward_observation_clock_invalid")
        return result
    if (not isinstance(initialization, dict)
            or set(initialization) != {"binding", "started_at", "expires_at", "duration_seconds", "complete"}
            or initialization["complete"] is not True
            or type(initialization["duration_seconds"]) is not int
            or initialization["duration_seconds"] != 600):
        raise RuntimeError("storage_forward_initialization_observation_invalid")
    seconds = intent["original_capture"].get("attempt_seconds")
    expected = dict(request_sha256=host.digest(request), operation_sha256=intent["operation_sha256"],
        cancellation_intent_sha256=intent["cancellation_intent_sha256"], key_seconds=3600,
        initial_seconds=600, attempt_seconds=seconds)
    start, end = instant(initialization["started_at"]), instant(initialization["expires_at"])
    if (initialization["binding"] != expected
            or any(type(initialization["binding"][k]) is not int for k in ("key_seconds", "initial_seconds", "attempt_seconds"))
            or end != start+timedelta(seconds=600)):
        raise RuntimeError("storage_forward_initialization_binding_changed")
    if (not isinstance(capture, dict)
            or set(capture) != {"operation_sha256", "started_at", "expires_at", "attempt_seconds", "cancellation", "placement"}
            or capture["operation_sha256"] != intent["operation_sha256"]
            or type(seconds) is not int or not 30 <= seconds <= 345600
            or type(capture["attempt_seconds"]) is not int or capture["attempt_seconds"] != seconds
            or not isinstance(capture["cancellation"], dict)
            or not isinstance(capture["placement"], dict) or not capture["placement"]):
        raise RuntimeError("storage_forward_adoption_observation_invalid")
    canceled = capture["cancellation"]
    receipt = canceled.get("receipt", {})
    if (receipt.get("schema_version") != "qt.fact_header_cancel.v2"
            or receipt.get("capture") != intent["original_capture"]
            or receipt.get("intent_sha256") != intent["cancellation_intent_sha256"]
            or any(receipt.get(k) is not True for k in ("source_retained", "partial_copies_retained"))
            or any(receipt.get(k) is not False for k in ("migration_ready", "final_switch_authorized"))):
        raise RuntimeError("storage_forward_cancellation_observation_changed")
    adopted, expiry = instant(capture["started_at"]), instant(capture["expires_at"])
    if (not start <= adopted <= end or expiry != adopted+timedelta(seconds=seconds)
            or not adopted.timestamp() <= now < expiry.timestamp()):
        raise RuntimeError("storage_forward_adoption_original_clock_invalid")
    owner = dict(schema_version="qt.storage_online_forward_session.v1",
        operation_sha256=intent["operation_sha256"], cancellation_intent_sha256=intent["cancellation_intent_sha256"],
        started_at=adopted.isoformat(), expires_at=expiry.isoformat(), attempt_seconds=seconds, end_day=intent["end_day"])
    return owner, expiry.timestamp()


def _observation_names(request):
    from scripts.automation.storage_online_forward_worker import request_binding, initialization_relation
    intent = request_binding(request) if request is not None else None
    if request is not None and intent is None:
        raise ValueError("storage_forward_observation_intent_required")
    successor = intent is not None and intent["schema_version"] == "qt.storage_online_forward_intent.v2"
    # This is the fixed database namespace protocol, covered by native reader
    # tests. Never discover/select the latest adoption from catalog inventory.
    adoption = "qt_fwd_"+intent["operation_sha256"][:56]+".adoption" if successor else "qt_fact_header_forward_v2.adoption"
    return intent, (initialization_relation(intent), adoption)


def observe_adoption(database_id, *, request=None):
    """Read both atomic initialization/adoption receipts in one SQL snapshot.

    A missing or retired adoption fails closed. The caller must separately admit
    the published request, owned worker and source before using this observation.
    This query neither runs the expensive proof scan nor changes database state.
    """
    intent, names = _observation_names(request)
    successor = intent is not None and intent["schema_version"] == "qt.storage_online_forward_intent.v2"
    placement = "binding->'lookup_placement'->'binding'->'placement'" if successor else "binding->'old_headers'->'placement'"
    lineage = (",binding->'predecessor'->>'operation_sha256' AS predecessor_operation_sha256,"
        "binding->'predecessor'->>'terminal_sha256' AS predecessor_terminal_sha256,"
        "binding->'lookup_placement'->>'operation_sha256' AS lookup_operation_sha256") if successor else ""
    present = json.loads(host.database_query(database_id,
        "SELECT json_build_array(to_regclass('"+names[0]+"') IS NOT NULL,"
        "to_regclass('"+names[1]+"') IS NOT NULL)::text", read_only_seconds=5))
    if present != [True, True]:
        raise RuntimeError("storage_forward_active_adoption_required")
    rows = json.loads(host.database_query(database_id,
        "SELECT json_build_object('initialization',(SELECT json_agg(i) FROM ("
        "SELECT id,binding,started_at,expires_at,duration_seconds,complete FROM "+names[0]+" LIMIT 2) i),"
        "'adoption',(SELECT json_agg(a) FROM (SELECT id,operation_sha256,started_at,expires_at,"
        "attempt_seconds,terminal,binding->'terminal' AS cancellation,"
        +placement+" AS placement"+lineage+" FROM "+names[1]+" LIMIT 2) a))::text", read_only_seconds=5))
    if (not isinstance(rows, dict) or set(rows) != {"initialization", "adoption"}
            or any(not isinstance(rows[k], list) or len(rows[k]) != 1
                or type(rows[k][0].get("id")) is not int or rows[k][0]["id"] != 1 for k in rows)
            or rows["adoption"][0].get("terminal") is not None):
        raise RuntimeError("storage_forward_active_adoption_required")
    if successor:
        for name in ("predecessor_operation_sha256", "predecessor_terminal_sha256", "lookup_operation_sha256"):
            if rows["adoption"][0].pop(name, None) != intent[name]:
                raise RuntimeError("storage_forward_successor_observation_changed")
    initialization = {k:v for k,v in rows["initialization"][0].items() if k != "id"}
    capture = {k:v for k,v in rows["adoption"][0].items() if k not in {"id", "terminal"}}
    return _clock_row(initialization), _clock_row(capture)


LAUNCH_STATE = "storage-online-forward-launch.json"


def load_launch(root, *, request=None, operation_sha256=None):
    return host.load_receipt(root/operation_file(LAUNCH_STATE, request=request, operation_sha256=operation_sha256), max_bytes=_MAX_BYTES)


def save_launch(root, saved, *, initial=False):
    # Full proof metadata is intentionally larger than the ordinary 64KiB hold.
    # Refuse BEFORE publication if the existing 2MiB metadata budget is exceeded.
    if len(json.dumps(saved, sort_keys=True, allow_nan=False).encode())+1 > _MAX_BYTES:
        raise RuntimeError("storage_forward_launch_receipt_budget_exceeded")
    host.save_receipt(root/operation_file(LAUNCH_STATE, operation_sha256=saved.get("successor_operation_sha256")), saved, initial=initial)


def _launch_clock(saved):
    import math
    wall, mono, boot = time.time(), time.monotonic(), time.clock_gettime(time.CLOCK_BOOTTIME)
    fields = ("started_at", "started_monotonic", "started_boot", "key_deadline", "key_deadline_monotonic", "key_deadline_boot")
    if (saved.get("boot_id") != terminal._boot()
            or any(type(saved.get(k)) not in (int, float) or not math.isfinite(saved[k]) for k in fields)
            or saved["key_deadline"] != saved["started_at"]+3600
            or saved["key_deadline_monotonic"] != saved["started_monotonic"]+3600
            or saved["key_deadline_boot"] != saved["started_boot"]+3600
            or wall < saved["started_at"] or mono < saved["started_monotonic"] or boot < saved["started_boot"]):
        raise RuntimeError("storage_forward_launch_clock_changed")
    return wall, mono, boot


def _launch_remaining(saved, deadline):
    wall, mono, boot = _launch_clock(saved)
    elapsed_limit = deadline-saved["started_at"]
    remaining = min(deadline-wall, saved["started_monotonic"]+elapsed_limit-mono,
        saved["started_boot"]+elapsed_limit-boot)
    if remaining <= 0:
        raise RuntimeError("storage_forward_launch_original_phase_expired")
    return remaining


def _worker_transition(before, after, saved):
    fields = {"binding", "container_id", "contract", "deadline"}
    if (set(before) != fields or set(after) != fields or before["binding"] != after["binding"]
            or any(before[k] is not None and before[k] != after[k] for k in ("container_id", "contract", "deadline"))
            or (after["container_id"] is None) != (after["contract"] is None)
            or after["deadline"] != (saved["deadline"] if saved["capture"] is not None else None)):
        raise RuntimeError("storage_forward_worker_progress_changed")


def launch_intent(root, published, worker_binding):
    """Own the exact published worker preimage before any container creation.

    Caller retains the deployment lock and fresh source/runtime admission. Lost
    worker-file publication accepts only durable old/new preimages; no restart,
    source mutation or renewal is inferred from this receipt.
    """
    root = launch._canonical(root)
    if inspect_published_operation(root, request=published["new_request"]) != published or worker_binding != published["new_worker"]["binding"]:
        raise RuntimeError("storage_forward_launch_publication_changed")
    publication._retired(published["old_worker"])
    path = root/operation_file(LAUNCH_STATE, request=published["new_request"])
    current = host.load_receipt(root/launch._STATE)
    owner = dict(publication_sha256=published["intent_sha256"],
        request_sha256=worker_binding["request_sha256"], worker_binding_sha256=host.digest(worker_binding),
        operation_sha256=published["forward"]["operation_sha256"])
    if not os.path.lexists(path):
        if current != published["new_worker"]:
            raise RuntimeError("storage_forward_launch_preimage_changed")
        wall, mono, boot = time.time(), time.monotonic(), time.clock_gettime(time.CLOCK_BOOTTIME)
        saved = dict(schema_version="qt.storage_online_forward_launch.v1", binding=owner,
            boot_id=terminal._boot(), started_at=wall, started_monotonic=mono, started_boot=boot,
            key_deadline=wall+3600, key_deadline_monotonic=mono+3600, key_deadline_boot=boot+3600,
            keys=None, initialization=None, capture=None, forward=None, deadline=None,
            worker=current, pending_worker=None)
        if published["forward"]["schema_version"] == "qt.storage_online_forward_intent.v2":
            saved["successor_operation_sha256"] = published["forward"]["operation_sha256"]
        save_launch(root, saved, initial=True)
    saved = load_launch(root, request=published["new_request"])
    fields = {"schema_version", "binding", "boot_id", "started_at", "started_monotonic", "started_boot",
        "key_deadline", "key_deadline_monotonic", "key_deadline_boot", "keys", "initialization",
        "capture", "forward", "deadline", "worker", "pending_worker"}
    if published["forward"]["schema_version"] == "qt.storage_online_forward_intent.v2":
        fields.add("successor_operation_sha256")
        if saved.get("successor_operation_sha256") != published["forward"]["operation_sha256"]:
            raise RuntimeError("storage_forward_launch_intent_changed")
    if (set(saved) != fields or saved["schema_version"] != "qt.storage_online_forward_launch.v1"
            or saved["binding"] != owner or saved["worker"]["binding"] != worker_binding
            or current not in (saved["worker"], saved["pending_worker"])):
        raise RuntimeError("storage_forward_launch_intent_changed")
    _launch_clock(saved)
    if saved["pending_worker"] is not None:
        _worker_transition(saved["worker"], saved["pending_worker"], saved)
        host.save_receipt(root/launch._STATE, saved["pending_worker"], initial=False)
        saved["worker"], saved["pending_worker"] = saved["pending_worker"], None
        save_launch(root, saved)
    return saved


def save_launched_worker(root, saved, worker):
    """Journal the exact next worker file before publishing that progression."""
    if (load_launch(root, operation_sha256=saved.get("successor_operation_sha256")) != saved or saved["pending_worker"] is not None
            or host.load_receipt(root/launch._STATE) != saved["worker"]):
        raise RuntimeError("storage_forward_worker_preimage_changed")
    _worker_transition(saved["worker"], worker, saved)
    saved["pending_worker"] = deepcopy(worker)
    save_launch(root, saved)
    host.save_receipt(root/launch._STATE, worker, initial=False)
    saved["worker"], saved["pending_worker"] = deepcopy(worker), None
    save_launch(root, saved)


def observe_preparation(database_id, *, request=None):
    """Read fixed preparation rows; absence is only a startup observation."""
    _, names = _observation_names(request)
    names = ("qt_fact_header_forward_v2.key_preparation", names[0])
    present = json.loads(host.database_query(database_id,
        "SELECT json_build_array(to_regclass('"+names[0]+"') IS NOT NULL,"
        "to_regclass('"+names[1]+"') IS NOT NULL)::text", read_only_seconds=5))
    if not isinstance(present, list) or len(present) != 2 or any(type(v) is not bool for v in present):
        raise RuntimeError("storage_forward_preparation_observation_invalid")
    expressions = ["(SELECT json_agg(r) FROM (SELECT * FROM "+name+" LIMIT 2) r)" if exists else "NULL"
        for name, exists in zip(names, present)]
    values = json.loads(host.database_query(database_id,
        "SELECT json_build_array("+",".join(expressions)+")::text", read_only_seconds=5))
    if not isinstance(values, list) or len(values) != 2:
        raise RuntimeError("storage_forward_preparation_observation_invalid")
    result = []
    for exists, rows in zip(present, values):
        if not exists and rows is None:
            result.append(None)
        elif (isinstance(rows, list) and len(rows) == 1 and type(rows[0].get("id")) is int and rows[0]["id"] == 1):
            result.append(_clock_row({k:v for k,v in rows[0].items() if k != "id"}))
        else:
            raise RuntimeError("storage_forward_preparation_observation_invalid")
    return tuple(result)


def admit_startup(root, saved, request, *, keys, initialization, capture=None):
    """Pin actual SQL phase receipts under the original host launch clock.

    A completed adoption can outlive initialization. An unfinished initializer
    cannot, and neither worker retry nor a delayed host observation creates time.
    """
    from datetime import datetime, timedelta
    from scripts.automation.storage_online_forward_worker import request_binding
    intent = request_binding(request)
    if (intent is None or saved["binding"]["operation_sha256"] != intent["operation_sha256"]
            or saved["binding"]["request_sha256"] != publication._sha(publication.request_bytes(request))
            or load_launch(root, request=request) != saved):
        raise RuntimeError("storage_forward_startup_request_changed")
    before = deepcopy(saved)
    wall, _, _ = _launch_clock(saved)
    def epoch(value):
        stamp = datetime.fromisoformat(value)
        if stamp.utcoffset() != timedelta(0):
            raise RuntimeError("storage_forward_startup_clock_invalid")
        return stamp.timestamp()
    deadline = saved["key_deadline"]
    if keys is not None:
        if (set(keys) != {"binding", "started_at", "expires_at", "duration_seconds", "index_oids", "complete"}
                or type(keys["duration_seconds"]) is not int or keys["duration_seconds"] != 3600
                or type(keys["complete"]) is not bool
                or keys["binding"].get("capture") != intent["original_capture"]
                or keys["binding"].get("cancellation_intent_sha256") != intent["cancellation_intent_sha256"]
                or epoch(keys["expires_at"]) != epoch(keys["started_at"])+3600
                or epoch(keys["started_at"]) > wall):
            raise RuntimeError("storage_forward_key_observation_changed")
        if saved["keys"] is not None and keys != saved["keys"]:
            raise RuntimeError("storage_forward_completed_keys_changed")
        # A completed key build is reusable evidence. Its SQL deadline only
        # bounds unfinished index work; the unchanged host launch deadline
        # still bounds starting initialization when no initializer exists yet.
        if not keys["complete"]:
            deadline = min(deadline, epoch(keys["expires_at"]))
        if keys["complete"]:
            if (not isinstance(keys["index_oids"], dict)
                    or set(keys["index_oids"]) != {"qt_header_forward_day_pk", "qt_header_forward_revision_day"}
                    or any(type(oid) is not int or oid <= 0 for oid in keys["index_oids"].values())):
                raise RuntimeError("storage_forward_completed_keys_changed")
            saved["keys"] = deepcopy(keys)
    elif saved["keys"] is not None or initialization is not None:
        raise RuntimeError("storage_forward_completed_keys_missing")
    if initialization is not None:
        expected = dict(request_sha256=host.digest(request), operation_sha256=intent["operation_sha256"],
            cancellation_intent_sha256=intent["cancellation_intent_sha256"], key_seconds=3600,
            initial_seconds=600, attempt_seconds=intent["original_capture"]["attempt_seconds"])
        if (saved["keys"] is None or set(initialization) != {"binding", "started_at", "expires_at", "duration_seconds", "complete"}
                or initialization["binding"] != expected
                or any(type(initialization["binding"][k]) is not int for k in ("key_seconds", "initial_seconds", "attempt_seconds"))
                or type(initialization["complete"]) is not bool
                or type(initialization["duration_seconds"]) is not int or initialization["duration_seconds"] != 600
                or epoch(initialization["expires_at"]) != epoch(initialization["started_at"])+600
                or not epoch(keys["started_at"]) <= epoch(initialization["started_at"]) <= saved["key_deadline"]
                or epoch(initialization["started_at"]) > wall):
            raise RuntimeError("storage_forward_initialization_clock_changed")
        old = saved["initialization"]
        if old is not None and (any(initialization[k] != old[k] for k in old if k != "complete")
                or old["complete"] and not initialization["complete"]):
            raise RuntimeError("storage_forward_initialization_clock_changed")
        saved["initialization"] = deepcopy(initialization)
        deadline = epoch(initialization["expires_at"])
        if initialization["complete"]:
            owner, deadline = admit_adoption_observation(request, initialization=initialization, capture=capture, now=wall)
            if saved["capture"] is not None and (saved["capture"] != capture or saved["forward"] != owner or saved["deadline"] != deadline):
                raise RuntimeError("storage_forward_original_adoption_changed")
            saved.update(capture=deepcopy(capture), forward=owner, deadline=deadline)
        elif capture is not None:
            raise RuntimeError("storage_forward_uncommitted_adoption")
    elif saved["initialization"] is not None or capture is not None:
        raise RuntimeError("storage_forward_initialization_missing")
    remaining = _launch_remaining(saved, deadline)
    if saved != before:
        save_launch(root, saved)
    return remaining


def _clock_row(row):
    from datetime import datetime
    return {**row, **{key:datetime.fromisoformat(row[key]).isoformat() for key in ("started_at", "expires_at")}}


def admit_launched_adoption(root, request, worker, *, initialization, capture):
    """Read-only final admission of a ready, durably published worker/adoption."""
    published = inspect_published_operation(root, request=request)
    saved = load_launch(root, request=request)
    expected = dict(publication_sha256=published["intent_sha256"],
        request_sha256=worker["binding"]["request_sha256"],
        worker_binding_sha256=host.digest(worker["binding"]), operation_sha256=published["forward"]["operation_sha256"])
    if (saved.get("schema_version") != "qt.storage_online_forward_launch.v1"
            or saved["binding"] != expected or saved["worker"] != worker or saved["pending_worker"] is not None
            or worker["binding"] != published["new_worker"]["binding"]
            or worker["container_id"] is None or worker["contract"] is None
            or saved["initialization"] != initialization or saved["capture"] != capture):
        raise RuntimeError("storage_forward_launched_adoption_changed")
    owner, deadline = admit_adoption_observation(request, initialization=initialization, capture=capture, now=time.time())
    if saved["forward"] != owner or saved["deadline"] != deadline or worker["deadline"] != deadline:
        raise RuntimeError("storage_forward_launched_clock_changed")
    # Reuse startup admission read-only: every phase is already pinned, so
    # any new or missing proof is refused before it could publish progression.
    if saved["keys"] is None or saved["initialization"]["complete"] is not True:
        raise RuntimeError("storage_forward_launched_preparation_missing")
    admit_startup(root, deepcopy(saved), request, keys=saved["keys"],
        initialization=initialization, capture=capture)
    return owner, deadline

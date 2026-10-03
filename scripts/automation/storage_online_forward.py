"""Preserving package publication for a separately owned forward operation.

This canonical host phase publishes metadata only. The canceled plan/capture,
terminal receipt and retired worker remain evidence. It grants no worker start,
new capture, source stop, database switch, recovery or deployment authority.
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
            or publication._sha((root/terminal.STATE).read_bytes()) != journal["terminal_sha256"]):
        raise RuntimeError("storage_forward_original_evidence_changed")
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
    if (not isinstance(package, dict) or set(package) != {
            "schema_version", "plan_sha256", "image", "source_revision", "source_tree_hash", "forward_plan_path", "end_day"}
            or package["schema_version"] != "qt.storage_online_forward_package.v1"
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
        for name in ("storage-online-final.json", "promotion.env", "alert-preview.env"):
            if os.path.lexists(root/name):
                raise RuntimeError("storage_forward_pre_final_source_required")
        for name in (publication.STATE, publication.PACKAGE_STATE):
            if os.path.lexists(root/name) and host.load_receipt(root/name, max_bytes=_MAX_BYTES).get("phase") != "complete":
                raise RuntimeError("storage_forward_prior_publication_unresolved")
        canceled = _terminal(root, path)
        terminal_sha256 = publication._sha((root/terminal.STATE).read_bytes())
        journal = host.load_receipt(root/STATE, max_bytes=_MAX_BYTES) if os.path.lexists(root/STATE) else None
        if journal:
            immutable = {k:v for k,v in journal.items() if k not in {"intent_sha256", "phase"}}
            if (host.digest(immutable) != journal.get("intent_sha256")
                    or journal.get("phase") not in {"prepared", "publishing", "complete"}
                    or journal.get("package") != package or journal.get("operator_sha256") != operator
                    or journal.get("original_plan_bytes") != path.read_text()
                    or journal.get("terminal_sha256") != publication._sha((root/terminal.STATE).read_bytes())):
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
        if journal is None and publication._sha((root/launch._STATE).read_bytes()) != canceled["worker_sha256"]:
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
                    or {k:v for k,v in old_request.items() if k != "capture_preparation"} != plan["request"]
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
            receipt = terminal._probe(root, plan, saved, canceled["package"], action="reconcile",
                wall_deadline=clock["wall_deadline"], expected_capture=canceled["original_capture"],
                intent_sha256=canceled["intent_sha256"], original_request=old_request)
            if receipt != canceled["receipt"]:
                raise RuntimeError("storage_forward_cancellation_proof_changed")
            _clock(clock)
            if not execute:
                return dict(phase="forward_package_reconciliation_required" if journal else "forward_package_inspected",
                    storage_mutations_performed=False, migration_started=False, forward_worker_authorized=False)
            if journal is None:
                # Separate forward identity; the expired capture's start/expiry
                # and the original initial-preparation receipt are never renewed.
                forward = dict(schema_version="qt.storage_online_forward_intent.v1",
                    original_plan_sha256=package["plan_sha256"], cancellation_intent_sha256=canceled["intent_sha256"],
                    original_capture=canceled["original_capture"], end_day=package["end_day"],
                    candidate_revision=package["source_revision"], candidate_source_hash=package["source_tree_hash"])
                forward["operation_sha256"] = host.digest(forward)
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
                journal["intent_sha256"] = host.digest(journal)
                journal["phase"] = "prepared"
                if len(json.dumps(journal).encode()) > _MAX_BYTES-1:
                    raise ValueError("storage_forward_intent_budget_exceeded")
                if (publication._sha(path.read_bytes()) != package["plan_sha256"]
                        or publication._sha((root/launch._STATE).read_bytes()) != canceled["worker_sha256"]
                        or (root/publication.REQUEST).read_text() != old_request_bytes
                        or runtime_path.read_text() != old_runtime_bytes):
                    raise RuntimeError("storage_forward_preimages_changed")
                _admit_preimages(root, path, new_path, journal)
                host.save_receipt(root/STATE, journal, initial=True)
            _clock(journal)
            _admit_preimages(root, path, new_path, journal)
            rows = host.inventory(plan["project"], operator_id=saved["container_id"])
            if (publication._sha(launch._canonical(plan["inventory_path"]).read_bytes()) != saved["binding"]["inventory_sha256"]
                    or host.identities(rows) != saved["binding"]["clients"] or not host.source_clients_serving(rows)):
                raise RuntimeError("storage_forward_source_fleet_changed")
            LOG.info("storage_forward_publication_start project=%s intent_sha256=%s", plan["project"], journal["intent_sha256"])
            journal["phase"] = "publishing"
            host.save_receipt(root/STATE, journal, initial=False)
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
            host.save_receipt(root/STATE, journal, initial=False)
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

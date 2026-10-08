"""Explicit raw-placement adapter inside the existing maintenance container.

The host operator still owns deployment exclusion, exact container admission,
measured collection/spool budgets and paired recovery. This command neither
launches containers nor borrows the closed migration's authority or clock.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import signal
import sys
import threading

from core.storage_targets import StoragePolicy
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from scripts.db import raw_mapping_v2_placement as placement


def read_request(path):
    if path == "-":
        raw = sys.stdin.buffer.read(65537)
    else:
        with Path(path).open("rb") as stream:
            raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError("raw_history_operator_request_too_large")
    request = json.loads(raw, object_pairs_hook=_unique_fields)
    if (not isinstance(request, dict) or set(request) != {
            "schema_version", "source_revision", "request_id", "handoff_sha256", "policy", "resource_limits"}
            or request["schema_version"] != placement.OPERATION
            or not isinstance(request["source_revision"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", request["source_revision"])):
        raise ValueError("raw_history_operator_request_invalid")
    placement._plan_id(request["request_id"])
    if (not isinstance(request["handoff_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", request["handoff_sha256"])):
        raise ValueError("raw_history_handoff_hash_invalid")
    placement._limits(request["resource_limits"])
    StoragePolicy.from_dict(request["policy"])
    return request


def _runtime(request):
    from core.settings import get_settings
    from core.storage_mounts import require_configured_archive_mount, require_configured_working_mount
    from portal.backend.service.provenance import evidence_source_revision
    from portal.backend.workers.market_data_collector_health import live_worker_for_host

    settings = get_settings()
    if (getattr(os, "geteuid", lambda: -1)() != 70
            or settings.storage.maintenance_owner != "dedicated"
            or settings.storage.archive_shared_group_id is None):
        raise RuntimeError("raw_history_existing_maintenance_runtime_required")
    if evidence_source_revision() != request["source_revision"]:
        raise RuntimeError("raw_history_operator_revision_changed")
    require_configured_archive_mount()
    require_configured_working_mount()
    return live_worker_for_host(storage_maintenance=True)


@contextmanager
def _cancellation():
    stop = threading.Event()
    previous = {}
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, lambda *_: stop.set())
        yield stop.is_set
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def run_local_placement(path, *, execute=False, cancel=False):
    """Inspect by default; mutations require an explicit, separate action.

    No schema bootstrap, automatic retry, recovery pruning or service change.
    A disconnected caller must inspect this same request before doing more work.
    The database owner retains its deadline even if the Docker client disappears.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    if type(execute) is not bool or type(cancel) is not bool or execute and cancel:
        raise ValueError("raw_history_operator_action_invalid")
    request = read_request(path)
    engine = None
    try:
        worker = _runtime(request)
        dsn = os.environ.get("PG_DSN", "").strip()
        if not dsn:
            raise ValueError("raw_history_operator_connection_missing")
        engine = create_engine(dsn, poolclass=NullPool, hide_parameters=True,
            connect_args={"connect_timeout": 10, "application_name": "qt_retained_raw_placement"})
        options = {key: request[key] for key in ("request_id", "handoff_sha256", "resource_limits")}
        options["policy"] = StoragePolicy.from_dict(request["policy"])
        with _cancellation() as cancelled:
            if cancel:
                result = placement.cancel_retained_raw_history(engine,
                    request_id=request["request_id"], handoff_sha256=request["handoff_sha256"])
            elif execute:
                result = placement.move_retained_raw_history(engine, cancelled=cancelled, **options)
            else:
                result = placement.inspect_retained_raw_history(engine, **options)
        return {**result, "source_revision": request["source_revision"],
            "maintenance_worker_id": worker["worker_id"], "recovery_verified": False,
            "service_changes_performed": False}
    except Exception as exc:
        # SQLAlchemy errors can contain DSNs, SQL parameters or provider data.
        # Preserve the failure, exposing only explicit guard codes or its type.
        message = str(exc)
        code = message if re.fullmatch(r"[a-z][a-z0-9_]{1,160}", message) else type(exc).__name__
        raise RuntimeError("raw_history_operator_failed: " + code + "; inspect_same_request_before_retry") from None
    finally:
        if engine is not None:
            engine.dispose()

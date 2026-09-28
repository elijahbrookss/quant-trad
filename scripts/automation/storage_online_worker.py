"""Confined source-read identity for the persistent online storage worker.

The legacy held worker changes ownership and cannot run beside old collectors.
This Linux-only boundary retains DAC_READ_SEARCH after dropping to UID70; the
source must be a separately bound read-only filesystem. It grants no write
bypass and changes no file ownership/mode. The explicit admitted request may
prepare capture and bounded background work; it never starts source services.
Host admission must exclude writable aliases of the source and bind the image,
mounts and resource limits before invoking this internal helper.
"""
from __future__ import annotations

import ctypes
from datetime import date, datetime
import math
import time
import os
from pathlib import Path
import stat

_DAC_READ_SEARCH = 1 << 2
_SETGID = 1 << 6
_SETUID = 1 << 7
_INITIAL = _DAC_READ_SEARCH | _SETGID | _SETUID
_PR_SET_KEEPCAPS = 8
_PR_SET_NO_NEW_PRIVS = 38
_CAPABILITY_VERSION_3 = 0x20080522


class _Header(ctypes.Structure):
    _fields_ = [("version", ctypes.c_uint32), ("pid", ctypes.c_int)]


class _Data(ctypes.Structure):
    _fields_ = [("effective", ctypes.c_uint32), ("permitted", ctypes.c_uint32),
                ("inheritable", ctypes.c_uint32)]


def _status():
    values = {}
    for line in Path("/proc/self/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in {"CapEff", "CapPrm", "CapInh", "CapAmb", "NoNewPrivs", "Threads"}:
            values[key] = int(value.strip(), 16 if key.startswith("Cap") else 10)
    return values


def _source(root, expected_device, expected_inode):
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise RuntimeError("storage_online_source_canonical_path_required")
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode)
            or (info.st_dev, info.st_ino) != (expected_device, expected_inode)):
        raise RuntimeError("storage_online_source_identity_changed")
    if not os.statvfs(root).f_flag & os.ST_RDONLY:
        raise RuntimeError("storage_online_source_readonly_mount_required")
    return info


def enter_source_read_identity(source_root, *, expected_device, expected_inode):
    """Irreversibly drop root and all effective capabilities except read bypass.

    Call once, on the single-threaded worker before opening PG_DSN or archives.
    A failed transition is fatal: callers must exit, never fall back to root or
    to changing permissions. No subprocess inherits this effective capability.
    """
    if (os.getresuid() != (0, 0, 0) or os.getresgid() != (0, 0, 0)
            or type(expected_device) is not int or expected_device < 0
            or type(expected_inode) is not int or expected_inode <= 0):
        raise RuntimeError("storage_online_identity_entry_invalid")
    before = _status()
    if (before.get("Threads") != 1 or before.get("CapEff") != _INITIAL
            or before.get("CapPrm") != _INITIAL
            or before.get("CapInh") != 0 or before.get("CapAmb") != 0):
        raise RuntimeError("storage_online_initial_capabilities_invalid")
    root = Path(source_root)
    original = _source(root, expected_device, expected_inode)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                           ctypes.c_ulong, ctypes.c_ulong]
    libc.prctl.restype = ctypes.c_int
    libc.capset.argtypes = [ctypes.POINTER(_Header), ctypes.POINTER(_Data)]
    libc.capset.restype = ctypes.c_int

    def prctl(option, value):
        if libc.prctl(option, value, 0, 0, 0) != 0:
            raise RuntimeError("storage_online_identity_prctl_failed")

    prctl(_PR_SET_NO_NEW_PRIVS, 1)
    prctl(_PR_SET_KEEPCAPS, 1)
    os.setgroups([])
    os.setresgid(70, 70, 70)
    os.setresuid(70, 70, 70)
    data = (_Data * 2)()
    data[0].effective = data[0].permitted = _DAC_READ_SEARCH
    if libc.capset(ctypes.byref(_Header(_CAPABILITY_VERSION_3, 0)), data) != 0:
        raise RuntimeError("storage_online_identity_capset_failed")
    prctl(_PR_SET_KEEPCAPS, 0)
    after = _status()
    if (os.getresuid() != (70, 70, 70) or os.getresgid() != (70, 70, 70)
            or os.getgroups() or after.get("CapEff") != _DAC_READ_SEARCH
            or after.get("CapPrm") != _DAC_READ_SEARCH
            or after.get("CapInh") != 0 or after.get("CapAmb") != 0
            or after.get("NoNewPrivs") != 1 or after.get("Threads") != 1):
        raise RuntimeError("storage_online_identity_transition_failed")
    observed = _source(root, expected_device, expected_inode)
    if (observed.st_uid, observed.st_gid, observed.st_mode) != (
            original.st_uid, original.st_gid, original.st_mode):
        raise RuntimeError("storage_online_source_permissions_changed")
    return {"schema_version": "qt.storage_online_source_identity.v1",
            "uid": 70, "gid": 70, "source_readonly": True,
            "source_device": expected_device, "source_inode": expected_inode,
            "effective_capabilities": ["DAC_READ_SEARCH"],
            "source_ownership_changed": False, "migration_ready": False}


def archive_group_override(request):
    """Bind the optional archive group to this worker's existing fixed GID.

    No supplementary groups or new capabilities are admitted. Omitted requests
    retain private publication and explicitly override image/YAML defaults.
    """
    if "archive_shared_group_id" not in request:
        return "null"  # Central YAML environment parser: explicit private default.
    value = request["archive_shared_group_id"]
    if type(value) is not int or value != 70:
        raise ValueError("storage_online_archive_group_requires_worker_gid")
    return str(value)


def admit_archive_configuration(request, destination):
    """Check explicit settings and the already prepared root before opening SQL.

    The existing archive capture and live file proof bind the destination inode.
    The object store owns group membership, path and permission checks; it never
    repairs an existing private root. Source archive access remains read-only.
    """
    from core.settings import get_settings
    from market_data.archive import FilesystemRawArchiveObjectStore

    configured = get_settings().storage.archive_shared_group_id
    expected = int(archive_group_override(request)) if "archive_shared_group_id" in request else None
    if configured != expected:
        raise RuntimeError("storage_online_archive_configuration_changed")
    if expected is not None:
        FilesystemRawArchiveObjectStore(destination)


def capture_preparation(request):
    """Optional initial capture binding, inside the original host preparation.

    This is input to the existing worker, not a second migration entrypoint.
    The immutable request and host receipt precede any tablespace/capture work.
    """
    value = request.get("capture_preparation")
    if value is None:
        return None
    if (not isinstance(value, dict) or set(value) != {
            "requested_at", "deadline", "history_before", "attempt_seconds"}
            or request.get("expected_started_at") is not None
            or any(type(value[k]) not in (int, float) or not math.isfinite(value[k])
                   for k in ("requested_at", "deadline"))
            or not 0 < value["requested_at"] < value["deadline"] < 1e12
            or value["deadline"]-value["requested_at"] > 600
            or type(value["attempt_seconds"]) is not int
            or not 1 <= value["attempt_seconds"] <= 96*3600
            or not isinstance(value["history_before"], str)):
        raise ValueError("storage_online_capture_preparation_invalid")
    if date.fromisoformat(value["history_before"]).isoformat() != value["history_before"]:
        raise ValueError("storage_online_capture_preparation_invalid")
    return value


def admit_capture(request, observed):
    """Match an actual persisted capture; never manufacture its start or clock."""
    started = datetime.fromisoformat(observed["started_at"])
    seconds = observed["seconds"]
    if started.tzinfo is None or type(seconds) is not int or not 1 <= seconds <= 96*3600:
        raise RuntimeError("storage_online_original_attempt_changed")
    preparation = capture_preparation(request)
    if preparation is None:
        valid = started == datetime.fromisoformat(request["expected_started_at"])
    else:
        valid = (preparation["requested_at"] <= started.timestamp() <= preparation["deadline"]
                 and seconds == preparation["attempt_seconds"])
    if not valid:
        raise RuntimeError("storage_online_original_attempt_changed")
    deadline = started.timestamp()+seconds
    if deadline <= time.time():
        raise RuntimeError("storage_online_original_attempt_expired")
    return deadline


def prepare_capture(engine, request, *, targets, policy, limits, source):
    """Create only placement and the bounded atomic capture while source serves.

    Existing captures are inspected, never replaced. A restart uses the original
    capture even after the initial preparation window; an absent capture may be
    created only before that original window ends. The controller subsequently
    verifies every existing capture/protection/archive binding before copying.
    """
    from sqlalchemy import text
    from scripts.db import fact_header_v2_capture as capture
    from scripts.db import fact_header_v2_copy as headers
    from scripts.db import fact_header_v2_online as online
    from scripts.db import fact_header_v2_placement as physical
    from scripts.automation.storage_online_controller import OnlineController

    preparation = capture_preparation(request)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout='10s'")
        exists = conn.scalar(text("SELECT to_regclass('qt_fact_header_cutover_v2.capture')"))
        if exists is not None:
            with capture.migration_step(conn, 10):
                attempt = capture.inspect_capture(conn)
                seconds = conn.scalar(text("SELECT attempt_seconds FROM qt_fact_header_cutover_v2.capture WHERE id=1"))
                admit_capture(request, {"started_at": attempt["started_at"], "seconds": seconds})
                saved = headers._inspect_progress(conn)["placement"]
                if saved is None:
                    raise RuntimeError("storage_online_prepared_placement_required")
                placement = physical._restore(saved["plan"])
                if preparation is not None and placement.history_before.isoformat() != preparation["history_before"]:
                    raise RuntimeError("storage_online_capture_cutoff_changed")
                return placement, attempt["started_at"]
        if preparation is None:
            raise RuntimeError("storage_online_prepared_placement_required")
        # Admit the actual supported environment BEFORE persistent preparation.
        OnlineController._require_job_environment(conn, allow_connections=True)
        OnlineController._supported_builtin_catalog(conn)
    monotonic_deadline = time.monotonic()+preparation["deadline"]-time.time()
    def remaining():
        seconds = int(min(preparation["deadline"]-time.time(), monotonic_deadline-time.monotonic(), 60))
        if seconds < 1:
            raise RuntimeError("storage_online_capture_preparation_expired")
        return seconds
    by_id = {target.target_id: target for target in targets}
    placement = physical.prepare_history_tablespace(engine, recent=by_id[policy.recent[0]],
        history=by_id[policy.history[0]], history_before=date.fromisoformat(preparation["history_before"]),
        pg_controldata=Path("/usr/lib/postgresql/15/bin/pg_controldata"), timeout_seconds=remaining())
    result = online.prepare_attempt(engine, placement=placement, policy=policy,
        resource_limits=limits, source_root=source,
        destination_root=Path("/qt-history/archives/objects"),
        attempt_seconds=preparation["attempt_seconds"], max_duration_seconds=remaining(),
        cancelled=lambda: time.monotonic() >= monotonic_deadline or time.time() >= preparation["deadline"])
    admit_capture(request, {"started_at": result["started_at"], "seconds": preparation["attempt_seconds"]})
    return placement, result["started_at"]


def validate_request_shape(request):
    if (not isinstance(request, dict) or set(request)-{"archive_shared_group_id", "capture_preparation"} != {
            "schema_version", "source_revision", "source_tree_hash", "database_identity",
            "source_device", "source_inode", "expected_started_at", "policy",
            "resource_limits", "max_page_bytes", "max_objects", "max_bytes",
            "page_rows", "command_seconds"}
            or request["schema_version"] != "qt.storage_online_worker.v1"):
        raise ValueError("storage_online_request_invalid")
    archive_group_override(request)
    capture_preparation(request)


def request_configuration(request, inventory_path):
    """Shared read-only policy, inventory and budget validation; no SQL or files opened for writing."""
    from core.storage_inventory import read_storage_inventory
    from core.storage_targets import StoragePolicy
    from scripts.db import archive_reference_v2_placement as references
    from scripts.db.archive_file_v2_proof import validate_inventory_budget
    from scripts.automation.storage_online_controller import validate_copy_budget
    from portal.backend.service.storage.header_resource_claims import _limits

    validate_request_shape(request)
    validate_copy_budget(**{k:request[k] for k in ("command_seconds","page_rows","max_page_bytes")})
    validate_inventory_budget(max_files=request["max_objects"],max_bytes=request["max_bytes"])
    policy = StoragePolicy.from_dict(request["policy"])
    limits = _limits(request["resource_limits"], migration=True)
    targets = read_storage_inventory(inventory_path)
    if len(targets) != 2:
        raise RuntimeError("storage_online_two_targets_required")
    references._fixed_inputs(policy, limits, targets)
    if (policy.archives != policy.history or policy.backups != policy.history
            or not policy.movement_enabled or not policy.backup_enabled):
        raise RuntimeError("storage_online_fixed_policy_required")
    if {t.root for t in targets} != {"/var/lib/postgresql/data", "/qt-history"}:
        raise RuntimeError("storage_online_fixed_roots_required")
    return policy,limits,targets


def inspect_request_configuration(request, inventory_path, *, history_uuid):
    """Candidate-image probe: read-only SQL environment and original cluster only."""
    from sqlalchemy import create_engine,text
    from sqlalchemy.pool import NullPool
    from scripts.automation.storage_online_controller import OnlineController

    policy, _, targets = request_configuration(request, inventory_path)
    history = next(target for target in targets if target.target_id == policy.history[0])
    if history.filesystem_uuid != history_uuid:
        raise RuntimeError("storage_online_history_uuid_changed")
    engine=create_engine(os.environ["PG_DSN"],poolclass=NullPool,connect_args={"connect_timeout":5})
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='10s'")
            identity=conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text "
                "FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
            if identity!=request["database_identity"]:
                raise RuntimeError("storage_online_database_binding_changed")
            OnlineController._require_job_environment(conn,allow_connections=True)
            OnlineController._supported_builtin_catalog(conn)
    finally:
        engine.dispose()
    return {"validated":True}


def prepared_controller_main():
    """Private explicit capture/controller entrypoint; never pause or activate."""
    import contextlib
    import hashlib
    import json
    import sys

    def unique(pairs):
        values = {}
        for key, value in pairs:
            if key in values:
                raise ValueError("storage_online_request_duplicate_field")
            values[key] = value
        return values

    path = Path("/run/qt-online/request.json")
    # The host pins the immutable request bytes and explicit mounts before start.
    # Read at most one bounded request, not arbitrary paths from stdin.
    with path.open("rb") as handle:
        data = handle.read(65537)
    if (len(data) > 65536
            or hashlib.sha256(data).hexdigest() != os.environ.get("QT_ONLINE_REQUEST_SHA256")):
        raise RuntimeError("storage_online_request_binding_changed")
    request = json.loads(data, object_pairs_hook=unique)
    validate_request_shape(request)
    for key, variable in (("source_revision", "QT_IMAGE_SOURCE_REVISION"),
                          ("source_tree_hash", "QT_IMAGE_SOURCE_TREE_HASH")):
        value = request[key]
        length = 40 if key == "source_revision" else 64
        if (not isinstance(value, str) or len(value) != length
                or any(c not in "0123456789abcdef" for c in value)
                or value != os.environ.get(variable)):
            raise RuntimeError("storage_online_image_binding_changed")
    source = Path("/app/logs/market-structure/objects")
    # The transition precedes every application/SQLAlchemy/controller import.
    enter_source_read_identity(source, expected_device=request["source_device"],
                                expected_inode=request["source_inode"])
    protocol_fd = os.dup(sys.stdout.fileno())
    try:
        # Application lifecycle diagnostics stay on stderr. The original stdout
        # descriptor is exclusively the bounded host protocol.
        with contextlib.redirect_stdout(sys.stderr):
            from sqlalchemy import create_engine, text
            from sqlalchemy.pool import NullPool
            from scripts.automation.storage_online_controller import OnlineController, serve

            admit_archive_configuration(request, Path("/qt-history/archives/objects"))
            policy,limits,targets = request_configuration(request, Path("/run/qt-online/inventory.json"))
            dsn = os.environ.get("PG_DSN")
            if not dsn:
                raise RuntimeError("storage_online_pg_dsn_required")
            engine = create_engine(dsn, poolclass=NullPool,
                                   connect_args={"connect_timeout": 5})
            try:
                with engine.begin() as conn:
                    conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                    conn.exec_driver_sql("SET LOCAL statement_timeout='10s'")
                    identity = conn.scalar(text(
                        "SELECT c.system_identifier::text||'/'||d.oid::text "
                        "FROM pg_control_system() c CROSS JOIN pg_database d "
                        "WHERE d.datname=current_database()"))
                    if identity != request["database_identity"]:
                        raise RuntimeError("storage_online_database_binding_changed")
                placement, started = prepare_capture(engine, request, targets=targets,
                    policy=policy, limits=limits, source=source)
                if {t.target_id: t for t in targets} != {
                        t.target_id: t for t in (placement.recent, placement.history)}:
                    raise RuntimeError("storage_online_inventory_binding_changed")
                with OnlineController(engine, placement=placement, policy=policy,
                        resource_limits=limits, source_root=source,
                        destination_root=Path("/qt-history/archives/objects"), expected_started_at=started,
                        **{key: request[key] for key in (
                            "max_page_bytes", "max_objects",
                            "max_bytes", "page_rows", "command_seconds")}) as controller:
                    controller.admit_builtin_database_jobs()
                    serve(controller, input_fd=sys.stdin.fileno(), output_fd=protocol_fd)
            finally:
                engine.dispose()
    finally:
        os.close(protocol_fd)


if __name__ == "__main__":
    import sys
    try:
        prepared_controller_main()
    except Exception as exc:
        # Raw SQL/driver errors may contain private connection material.
        print("event=storage_online_worker_failed error_type="+type(exc).__name__,
              file=sys.stderr, flush=True)
        raise SystemExit(1) from None

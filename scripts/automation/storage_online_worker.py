"""Confined source-read identity for the persistent online storage worker.

The legacy held worker changes ownership and cannot run beside old collectors.
This Linux-only boundary retains DAC_READ_SEARCH after dropping to UID70; the
source must be a separately bound read-only filesystem. It grants no write
bypass, changes no file ownership/mode and never starts migration or services.
Host admission must exclude writable aliases of the source and bind the image,
mounts and resource limits before invoking this internal helper.
"""
from __future__ import annotations

import ctypes
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


def prepared_controller_main():
    """Private prepared-attempt entrypoint; never bootstrap, pause or activate."""
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
    if (not isinstance(request, dict) or set(request) != {
            "schema_version", "source_revision", "source_tree_hash", "database_identity",
            "source_device", "source_inode", "expected_started_at", "policy",
            "resource_limits", "max_page_bytes", "max_objects", "max_bytes",
            "page_rows", "command_seconds"}
            or request["schema_version"] != "qt.storage_online_worker.v1"):
        raise ValueError("storage_online_request_invalid")
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
            from core.storage_inventory import read_storage_inventory
            from core.storage_targets import StoragePolicy
            from scripts.db import fact_header_v2_copy as headers
            from scripts.db import fact_header_v2_capture as capture
            from scripts.db import fact_header_v2_placement as physical
            from scripts.db import archive_reference_v2_placement as references
            from scripts.automation.storage_online_controller import OnlineController, serve
            from portal.backend.service.storage.header_resource_claims import _limits

            policy = StoragePolicy.from_dict(request["policy"])
            limits = _limits(request["resource_limits"], migration=True)
            targets = read_storage_inventory(Path("/run/qt-online/inventory.json"))
            if len(targets) != 2:
                raise RuntimeError("storage_online_two_targets_required")
            references._fixed_inputs(policy, limits, targets)
            if (policy.archives != policy.history or policy.backups != policy.history
                    or not policy.movement_enabled or not policy.backup_enabled):
                raise RuntimeError("storage_online_fixed_policy_required")
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
                    with capture.migration_step(conn, 10):
                        saved = headers._inspect_progress(conn)["placement"]
                        if saved is None:
                            raise RuntimeError("storage_online_prepared_placement_required")
                        placement = physical._restore(saved["plan"])
                        if {t.target_id: t for t in targets} != {
                                t.target_id: t for t in (placement.recent, placement.history)}:
                            raise RuntimeError("storage_online_inventory_binding_changed")
                        if (placement.recent.root != "/var/lib/postgresql/data"
                                or placement.history.root != "/qt-history"):
                            raise RuntimeError("storage_online_fixed_roots_required")
                with OnlineController(engine, placement=placement, policy=policy,
                        resource_limits=limits, source_root=source,
                        destination_root=Path("/qt-history/archives/objects"),
                        **{key: request[key] for key in (
                            "expected_started_at", "max_page_bytes", "max_objects",
                            "max_bytes", "page_rows", "command_seconds")}) as controller:
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

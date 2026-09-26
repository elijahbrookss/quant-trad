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

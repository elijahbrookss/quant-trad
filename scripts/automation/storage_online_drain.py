"""Bounded read-only spool observation; never publisher/switch authority."""
from __future__ import annotations

import math
import os
from pathlib import Path
import stat
from time import monotonic


def inspect_spool(working_root, *, deadline, max_entries, check):
    """Observe pending WAL without opening, repairing or removing its contents.

    The caller must separately exclude publishers. A clean observation can go
    stale immediately and never authorizes a root switch. Acknowledgement files
    are disposable projections, not evidence that adjacent WAL may be discarded.
    """
    if (type(deadline) not in (int, float) or not math.isfinite(deadline)
            or type(max_entries) is not int or not 1 <= max_entries <= 1_000_000
            or not callable(check)):
        raise ValueError("storage_online_spool_budget_invalid")
    root = Path(working_root)
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise ValueError("storage_online_spool_root_invalid")
    result = dict(entries=0, directories=0, pending_files=0, pending_bytes=0,
                  acknowledgement_files=0, spool_present=False,
                  spool_empty_at_observation=False, publisher_drain_authorized=False,
                  final_switch_authorized=False, collection_resume_authorized=False)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    stable = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_mtime_ns, s.st_ctime_ns)

    def budget():
        if monotonic() >= deadline:
            raise RuntimeError("storage_online_spool_deadline_expired")
        check()
        if monotonic() >= deadline:
            raise RuntimeError("storage_online_spool_deadline_expired")

    def walk(fd, depth, device):
        budget()
        if depth > 8:
            raise RuntimeError("storage_online_spool_depth_exceeded")
        before = os.fstat(fd)
        if before.st_dev != device:
            raise RuntimeError("storage_online_spool_filesystem_changed")
        result["directories"] += 1
        with os.scandir(fd) as entries:
            for entry in entries:
                budget()
                result["entries"] += 1
                if result["entries"] > max_entries:
                    raise RuntimeError("storage_online_spool_entry_budget_exceeded")
                info = entry.stat(follow_symlinks=False)
                if info.st_dev != device:
                    raise RuntimeError("storage_online_spool_filesystem_changed")
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(entry.name, flags, dir_fd=fd)
                    try:
                        if stable(os.fstat(child)) != stable(info):
                            raise RuntimeError("storage_online_spool_changed")
                        walk(child, depth+1, device)
                        if stable(os.stat(entry.name, dir_fd=fd, follow_symlinks=False)) != stable(info):
                            raise RuntimeError("storage_online_spool_changed")
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    if entry.name.endswith(".ack.json"):
                        result["acknowledgement_files"] += 1
                    else:
                        # Includes open/sealed WAL, partial acknowledgements and
                        # unknown files. Even header-only WAL needs normal source
                        # recovery; this observer never repairs or deletes it.
                        result["pending_files"] += 1
                        result["pending_bytes"] += info.st_size
                    if stable(os.stat(entry.name, dir_fd=fd, follow_symlinks=False)) != stable(info):
                        raise RuntimeError("storage_online_spool_changed")
                else:
                    raise RuntimeError("storage_online_spool_nonregular_entry")
        budget()
        if stable(os.fstat(fd)) != stable(before):
            raise RuntimeError("storage_online_spool_changed")

    budget()
    working = os.open(root, flags)
    try:
        before = os.fstat(working)
        try:
            spool = os.open("spool", flags, dir_fd=working)
        except FileNotFoundError:
            spool = None
        if spool is not None:
            try:
                identity = os.fstat(spool)
                result["spool_present"] = True
                walk(spool, 0, before.st_dev)
                if stable(os.stat("spool", dir_fd=working, follow_symlinks=False)) != stable(identity):
                    raise RuntimeError("storage_online_spool_changed")
            finally:
                os.close(spool)
        budget()
        if stable(os.fstat(working)) != stable(before) or stable(root.stat()) != stable(before):
            raise RuntimeError("storage_online_spool_changed")
        result["spool_empty_at_observation"] = result["pending_files"] == 0
        return result
    finally:
        os.close(working)

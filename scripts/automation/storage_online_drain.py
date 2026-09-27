"""Bounded spool observation and preserving recovery copies; no switch authority."""
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
    return _inspect_spool(working_root, deadline=deadline, max_entries=max_entries,
                          check=check)


def _inspect_spool(working_root, *, deadline, max_entries, check, visit=None):
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

    def walk(fd, depth, device, parts):
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
                        walk(child, depth+1, device, (*parts, entry.name))
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
                    if visit is not None:
                        visit(fd, entry.name, info, parts)
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
                walk(spool, 0, before.st_dev, ("spool",))
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


def prepare_recovery_spool(working_root, destination, *, deadline, max_entries,
                           max_bytes, check):
    """Copy pending WAL from a held read-only source into a NEW private SSD root.

    This internal preparation runs in a key-free filesystem helper, never an
    ordinary application process. The caller owns continuous writer exclusion,
    the original final deadline and activation. Copies alone grant no authority.
    Original WAL and acknowledgement projections remain untouched. Normal QT
    recovery must acknowledge the copied WAL against PostgreSQL before retiring
    it. Failure retains an unactivated partial destination and forbids reuse.
    """
    import hashlib

    if (type(deadline) not in (int, float) or not math.isfinite(deadline)
            or type(max_entries) is not int or not 1 <= max_entries <= 1_000_000
            or not callable(check)):
        raise ValueError("storage_online_spool_budget_invalid")
    if type(max_bytes) is not int or not 0 < max_bytes <= 64 * 1024**3:
        raise ValueError("storage_online_spool_copy_byte_budget_invalid")
    source, target = Path(working_root), Path(destination)
    if (not target.is_absolute() or target.resolve(strict=True) != target
            or source == target or source in target.parents or target in source.parents):
        raise ValueError("storage_online_spool_copy_roots_invalid")
    if not os.statvfs(source).f_flag & os.ST_RDONLY:
        raise RuntimeError("storage_online_spool_copy_readonly_source_required")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    stable = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid,
                        s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    copied, created_dirs = [], set()
    total = 0

    def budget():
        if monotonic() >= deadline:
            raise RuntimeError("storage_online_spool_deadline_expired")
        check()
        if monotonic() >= deadline:
            raise RuntimeError("storage_online_spool_deadline_expired")

    def target_parent(parts, target_fd):
        fd = os.dup(target_fd)
        try:
            for index, name in enumerate(parts):
                budget()
                relative = tuple(parts[:index+1])
                if relative not in created_dirs:
                    os.mkdir(name, 0o700, dir_fd=fd)
                    created_dirs.add(relative)
                    os.fsync(fd)
                child = os.open(name, flags, dir_fd=fd)
                os.close(fd)
                fd = child
            return fd
        except BaseException:
            os.close(fd)
            raise

    def copy_file(source_fd, name, info, parts):
        nonlocal total
        if name.endswith(".ack.json"):
            # Disposable projections stay in the retained original source.
            return
        if not name.endswith((".open", ".sealed")):
            raise RuntimeError("storage_online_spool_copy_unknown_pending_file")
        if len(copied) >= 4096 or total + info.st_size > max_bytes:
            raise RuntimeError("storage_online_spool_copy_budget_exceeded")
        parent = target_parent(parts, target_fd)
        try:
            src = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=source_fd)
            try:
                if stable(os.fstat(src)) != stable(info):
                    raise RuntimeError("storage_online_spool_changed")
                dst = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL |
                              os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
                try:
                    digest = hashlib.sha256()
                    size = 0
                    while True:
                        budget()
                        chunk = os.read(src, min(1024**2, info.st_size-size+1))
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > info.st_size:
                            raise RuntimeError("storage_online_spool_changed")
                        digest.update(chunk)
                        view = memoryview(chunk)
                        while view:
                            budget()
                            written = os.write(dst, view)
                            if written <= 0:
                                raise RuntimeError("storage_online_spool_copy_write_failed")
                            view = view[written:]
                    if size != info.st_size or stable(os.fstat(src)) != stable(info):
                        raise RuntimeError("storage_online_spool_changed")
                    os.fsync(dst)
                    os.lseek(dst, 0, os.SEEK_SET)
                    verified = hashlib.sha256()
                    while True:
                        budget()
                        chunk = os.read(dst, 1024**2)
                        if not chunk:
                            break
                        verified.update(chunk)
                    if verified.digest() != digest.digest():
                        raise RuntimeError("storage_online_spool_copy_checksum_mismatch")
                    os.fchmod(dst, 0o600)
                    os.fchown(dst, 1000, 1000)
                    os.fsync(dst)
                finally:
                    os.close(dst)
                if stable(os.stat(name, dir_fd=source_fd, follow_symlinks=False)) != stable(info):
                    raise RuntimeError("storage_online_spool_changed")
                copied.append({"path": str(Path(*parts, name)),
                               "bytes": size, "sha256": digest.hexdigest(),
                               "source_stat": stable(info)})
                total += size
                os.fsync(parent)
            finally:
                os.close(src)
        finally:
            os.close(parent)
    budget()
    target_fd = os.open(target, flags)
    try:
        target_info = os.fstat(target_fd)
        if (target_info.st_dev != source.stat().st_dev
                or os.geteuid() not in (0, 1000)
                or target_info.st_uid not in (os.geteuid(), 1000)
                or stat.S_IMODE(target_info.st_mode) != 0o700
                or os.listdir(target_fd)):
            raise RuntimeError("storage_online_spool_copy_new_private_ssd_root_required")
        working_fd = os.open(source, flags)
        try:
            source_info = os.fstat(working_fd)
            observed = _inspect_spool(source, deadline=deadline, max_entries=max_entries,
                                      check=check, visit=copy_file)
            after = inspect_spool(source, deadline=deadline, max_entries=max_entries, check=check)
            if after != observed or len(copied) != observed["pending_files"] or total != observed["pending_bytes"]:
                raise RuntimeError("storage_online_spool_changed")
            for item in copied:
                budget()
                if stable((source/item["path"]).lstat()) != item.pop("source_stat"):
                    raise RuntimeError("storage_online_spool_changed")
            if stable(source.lstat()) != stable(source_info):
                raise RuntimeError("storage_online_spool_changed")
        finally:
            os.close(working_fd)
        # Only newly created destination directories change ownership, bottom-up.
        # The original source is mounted read-only and never repaired or renamed.
        for parts in sorted(created_dirs, key=len, reverse=True):
            budget()
            fd = os.open(target.joinpath(*parts), flags)
            try:
                os.fchown(fd, 1000, 1000)
                os.fsync(fd)
            finally:
                os.close(fd)
        budget()
        if (target.lstat().st_dev, target.lstat().st_ino) != (target_info.st_dev, target_info.st_ino):
            raise RuntimeError("storage_online_spool_copy_destination_changed")
        os.fchown(target_fd, 1000, 1000)
        os.fsync(target_fd)
        budget()
        return {"copied_files": copied, "copied_bytes": total,
                "source_preserved": True, "original_observation": observed,
                "runtime_activation_authorized": False, "final_switch_authorized": False}
    finally:
        os.close(target_fd)

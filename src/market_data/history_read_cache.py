"""Disposable, bounded SSD copies of immutable canonical archive objects.

The manifest and HDD object stay authoritative. A directory flock serializes
fills/accounting; shared file locks protect readers from eviction. There is no
persistent cache catalog, dirty data, write-back or background thread.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import logging
import os
from pathlib import Path
import re
import stat
from time import monotonic, time

from core.execution_control import execution_checkpoint, consume_execution_resource, measure_execution_stage, current_execution_control

logger = logging.getLogger(__name__)
_NAME = re.compile(r"[0-9a-f]{64}\.(parquet|partial)\Z")
_CHUNK = 1024 * 1024


class CacheBypass(RuntimeError):
    """An optional copy cannot be admitted; use the authoritative reader."""


@dataclass(frozen=True)
class HistoryCacheLimits:
    max_bytes: int
    max_objects: int = 8192
    idle_seconds: int = 14 * 86400
    fill_seconds: int = 30

    def __post_init__(self):
        for name in ("max_bytes", "max_objects", "idle_seconds", "fill_seconds"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"history_cache_limit_invalid: field={name}")
        if self.max_objects > 100_000 or self.fill_seconds > 60:
            raise ValueError("history_cache_limit_invalid: bounded inventory/fill required")


class CanonicalArchiveReadCache:
    def __init__(self, root: Path, *, limits: HistoryCacheLimits, capacity_scope, check_mount):
        self.root = Path(root)
        self.limits = limits
        self.capacity_scope = capacity_scope
        self.check_mount = check_mount

    @contextmanager
    def _namespace(self):
        import fcntl
        self.check_mount(self.root)
        if (not self.root.is_absolute() or self.root.resolve(strict=True) != self.root
                or self.root.name != "history-read-cache"):
            raise CacheBypass("root_invalid")
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
                raise CacheBypass("root_requires_private_owner")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CacheBypass("namespace_busy") from None
            current = self.root.lstat()
            if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                raise CacheBypass("root_changed")
            yield fd
        finally:
            os.close(fd)

    def _check_namespace(self, directory):
        self.check_mount(self.root)
        bound = os.fstat(directory)
        current = self.root.lstat()
        if ((current.st_dev, current.st_ino) != (bound.st_dev, bound.st_ino)
                or not stat.S_ISDIR(current.st_mode)
                or current.st_uid != os.geteuid()
                or stat.S_IMODE(current.st_mode) != 0o700):
            raise CacheBypass("root_changed")

    @staticmethod
    def _open(directory, name, *, create=False):
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
        if create:
            flags |= os.O_CREAT | os.O_EXCL
        fd = os.open(name, flags, 0o600, dir_fd=directory)
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_dev != os.fstat(directory).st_dev or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600):
            os.close(fd)
            raise CacheBypass("entry_invalid")
        return fd

    def _remove(self, directory, name):
        import fcntl
        fd = self._open(directory, name)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            os.unlink(name, dir_fd=directory)
            consume_execution_resource("archive_cache_evictions", 1)
            return True
        finally:
            os.close(fd)

    def _inventory(self, directory, check):
        entries = []
        # Inspect a bounded flat namespace, never archive/spool directories.
        with os.scandir(directory) as scan:
            for item in scan:
                check()
                if len(entries) >= self.limits.max_objects + 1:
                    raise CacheBypass("inventory_limit")
                if not _NAME.fullmatch(item.name):
                    raise CacheBypass("unexpected_entry")
                info = item.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise CacheBypass("entry_invalid")
                entries.append((info.st_mtime, item.name, info.st_size, info.st_blocks * 512))
        return sorted(entries)

    def _space(self, directory, required, check):
        entries = self._inventory(directory, check)
        total = sum(max(size, allocated) for _, _, size, allocated in entries)
        count = len(entries)
        now = time()
        for used_at, name, size, allocated in entries:
            free = check()
            expired = name.endswith(".partial") or now - used_at >= self.limits.idle_seconds
            if (expired or total + required > self.limits.max_bytes
                    or count >= self.limits.max_objects or free < required):
                if self._remove(directory, name):
                    total -= max(size, allocated)
                    count -= 1
        if total + required > self.limits.max_bytes or count >= self.limits.max_objects:
            raise CacheBypass("quota_or_active_readers")

    def _fill(self, directory, source, digest, size):
        import fcntl
        started = monotonic()
        with self.capacity_scope() as capacity:
            def check(additional=0):
                execution_checkpoint()
                if monotonic() - started > self.limits.fill_seconds:
                    raise CacheBypass("fill_deadline")
                self._check_namespace(directory)
                return capacity(additional)
            # Include allocation rounding, not just logical Parquet size.
            block = os.fstatvfs(directory).f_frsize
            required = ((size + block - 1) // block) * block
            self._space(directory, required, check)
            check(required)
            partial = digest + ".partial"
            fd = self._open(directory, partial, create=True)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                hashed = hashlib.sha256()
                copied = 0
                source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
                with os.fdopen(source_fd, "rb") as handle:
                    before = os.fstat(handle.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_size != size:
                        raise RuntimeError("canonical_archive_source_size_mismatch")
                    while copied < size:
                        check(min(_CHUNK, size - copied))
                        chunk = handle.read(min(_CHUNK, size - copied))
                        if not chunk:
                            raise RuntimeError("canonical_archive_source_truncated")
                        consume_execution_resource("archive_read_bytes", len(chunk))
                        consume_execution_resource("archive_source_bytes", len(chunk))
                        hashed.update(chunk)
                        remaining = memoryview(chunk)
                        while remaining:
                            execution_checkpoint()
                            wrote = os.write(fd, remaining)
                            if wrote <= 0:
                                raise OSError("history_cache_write_stalled")
                            consume_execution_resource("archive_cache_write_bytes", wrote)
                            remaining = remaining[wrote:]
                        copied += len(chunk)
                    after = os.fstat(handle.fileno())
                    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                            after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        raise RuntimeError("canonical_archive_source_changed")
                if hashed.hexdigest() != digest:
                    raise RuntimeError("canonical_archive_checksum_mismatch: source during cache fill")
                check()
                os.fsync(fd)
                check()
                os.rename(partial, digest + ".parquet", src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
                consume_execution_resource("archive_cache_fills", 1)
            finally:
                os.close(fd)
                # Only our fixed-name temporary copy; a published copy is retained.
                try:
                    os.unlink(partial, dir_fd=directory)
                except FileNotFoundError:
                    pass

    @contextmanager
    def open_copy(self, source: Path, *, digest: str, size: int):
        """Yield a protected file or None for a deliberate HDD bypass."""
        import fcntl
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or type(size) is not int or size <= 0:
            raise ValueError("history_cache_object_identity_invalid")
        handle = None
        try:
            with self._namespace() as directory:
                execution_checkpoint()
                if size > self.limits.max_bytes:
                    raise CacheBypass("object_exceeds_quota")
                name = digest + ".parquet"
                try:
                    fd = self._open(directory, name)
                except FileNotFoundError:
                    control = current_execution_control()
                    if control is not None:
                        metrics = control.snapshot()
                        allowed = min(self.limits.max_bytes, metrics["limits"].get(
                            "archive_cache_write_bytes", self.limits.max_bytes))
                        if metrics["consumed"].get("archive_cache_write_bytes", 0) + size > allowed:
                            raise CacheBypass("operation_fill_budget")
                    consume_execution_resource("archive_cache_misses", 1)
                    with measure_execution_stage("archive_cache_fill"):
                        self._fill(directory, source, digest, size)
                    fd = self._open(directory, name)
                else:
                    consume_execution_resource("archive_cache_hits", 1)
                try:
                    if os.fstat(fd).st_size != size:
                        os.close(fd)
                        fd = None
                        self._remove(directory, name)
                        raise CacheBypass("cached_size_mismatch")
                    self._check_namespace(directory)
                    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    os.utime(fd, None)
                    handle = os.fdopen(fd, "rb")
                except BaseException:
                    if fd is not None:
                        os.close(fd)
                    raise
        except (CacheBypass, OSError) as error:
            consume_execution_resource("archive_cache_bypasses", 1)
            logger.warning("history_cache_bypass | reason=%s object_sha256=%s", error, digest)
        try:
            yield handle
        finally:
            if handle is not None:
                handle.close()

    def invalidate(self, digest):
        """Discard corrupt disposable bytes only if no other reader holds them."""
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("history_cache_object_identity_invalid")
        try:
            with self._namespace() as directory:
                self._remove(directory, digest + ".parquet")
        except (CacheBypass, OSError) as error:
            logger.warning("history_cache_invalidation_deferred | reason=%s object_sha256=%s", error, digest)

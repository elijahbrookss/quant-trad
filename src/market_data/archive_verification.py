"""Bounded current-byte verification for immutable local archive admission.

File stamps only detect changes during a short admission handoff. They never
replace a fresh checksum in a later retention run or prove market semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
import stat

from .archive import RawArchiveObjectStore


@dataclass(frozen=True)
class ArchiveVerificationLimits:
    max_objects: int = 100_000
    max_bytes: int = 64 * 1024**3
    max_pages: int = 10_000

    def __post_init__(self):
        for name in ("max_objects", "max_bytes", "max_pages"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"archive_verification_limit_invalid: field={name}")


def _stamp(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


@dataclass(frozen=True)
class VerifiedArchiveObject:
    object_key: str
    object_sha256: str
    byte_count: int
    stamp: tuple[int, ...]


class ArchiveVerificationBatch:
    """Verify each distinct object once within explicit count and byte budgets."""

    def __init__(self, object_store: RawArchiveObjectStore, *, limits: ArchiveVerificationLimits, check_budget=None):
        self.object_store = object_store
        self.limits = limits
        self.objects: dict[str, VerifiedArchiveObject] = {}
        self.byte_count = 0
        self.check_budget = check_budget

    def _check_budget(self):
        if self.check_budget is not None:
            self.check_budget()

    def verify(self, object_key: str, object_sha256: str, *, expected_bytes: int | None = None):
        self._check_budget()
        if (not isinstance(object_sha256, str) or len(object_sha256) != 64
                or any(char not in "0123456789abcdef" for char in object_sha256)):
            raise ValueError(f"archive_verification_hash_invalid: object_key={object_key}")
        if expected_bytes is not None and (type(expected_bytes) is not int or expected_bytes < 0):
            raise ValueError(f"archive_verification_size_invalid: object_key={object_key}")
        previous = self.objects.get(object_key)
        if previous is not None:
            if (previous.object_sha256 != object_sha256
                    or (expected_bytes is not None and previous.byte_count != expected_bytes)):
                raise RuntimeError(f"archive_verification_object_conflict: object_key={object_key}")
            self._assert_unchanged(previous)
            return previous
        if len(self.objects) >= self.limits.max_objects:
            raise RuntimeError("archive_verification_object_budget_exceeded")
        path = self.object_store.local_path(object_key)
        before = path.stat()
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError(f"archive_verification_not_regular: object_key={object_key}")
        if expected_bytes is not None and before.st_size != expected_bytes:
            raise RuntimeError(f"archive_verification_size_mismatch: object_key={object_key}")
        if self.byte_count + before.st_size > self.limits.max_bytes:
            raise RuntimeError("archive_verification_byte_budget_exceeded")
        # NONBLOCK also prevents a replacement FIFO from hanging between stat
        # and open. The descriptor must still be the same regular file.
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as handle:
            if _stamp(os.fstat(handle.fileno())) != _stamp(before):
                raise RuntimeError(f"archive_verification_object_changed: object_key={object_key}")
            digest = hashlib.sha256()
            read_bytes = 0
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                self._check_budget()
                self.byte_count += len(block)
                read_bytes += len(block)
                if self.byte_count > self.limits.max_bytes:
                    raise RuntimeError("archive_verification_byte_budget_exceeded")
                digest.update(block)
            if (_stamp(os.fstat(handle.fileno())) != _stamp(before)
                    or _stamp(path.stat()) != _stamp(before) or read_bytes != before.st_size):
                raise RuntimeError(f"archive_verification_object_changed: object_key={object_key}")
        if digest.hexdigest() != object_sha256:
            raise RuntimeError(f"archive_verification_checksum_mismatch: object_key={object_key}")
        result = VerifiedArchiveObject(object_key, object_sha256, read_bytes, _stamp(before))
        self.objects[object_key] = result
        return result

    def _assert_unchanged(self, verified: VerifiedArchiveObject):
        self._check_budget()
        path = self.object_store.local_path(verified.object_key)
        if _stamp(path.stat()) != verified.stamp:
            raise RuntimeError(f"archive_verification_object_changed: object_key={verified.object_key}")

    def assert_unchanged(self):
        """Recheck the short handoff, including mount/root admission for every path."""
        for verified in self.objects.values():
            self._assert_unchanged(verified)

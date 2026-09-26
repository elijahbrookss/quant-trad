"""Live Linux read-lease proof for background-verified destination files.

Internal migration primitive, never a persisted readiness certificate. Every
required path is checked again at the final publisher-drained catalog fence.
A dead controller loses this proof and must rehash in the background. No disk
format, filesystem feature, file contents, permissions or limits are changed.
"""
from __future__ import annotations

import fcntl
import hashlib
import math
import os
from pathlib import Path
import re
import resource
import signal
import stat
import struct
import threading
from time import monotonic

_ACTIVE = None


class ArchiveFileProof:
    """One main-thread, bounded, live proof context in a dedicated operator.

    Read leases exclude preexisting writers/mappings and signal later write or
    truncate attempts. SIGIO is blocked only in the owning thread, explicitly
    targeted with F_SETOWN_EX/F_OWNER_TID, and synchronously consumed. A break
    permanently invalidates the context, even if the attempted write failed.
    Leases do not protect names: the final verifier still checks every required
    root/path/inode against the retained descriptor while publishers are drained.
    """

    def __init__(self, root, *, max_files, max_bytes, deadline):
        if type(max_files) is not int or not 1 <= max_files <= 1_000_000:
            raise ValueError("archive_file_proof_file_budget_invalid")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("archive_file_proof_byte_budget_invalid")
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not 0 < deadline-monotonic() <= 96*3600):
            raise ValueError("archive_file_proof_deadline_invalid")
        self.root = Path(root)
        if not self.root.is_absolute() or self.root.resolve(strict=True) != self.root:
            raise ValueError("archive_file_proof_unsymlinked_root_required")
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("archive_file_proof_directory_required")
        self.identity = (info.st_dev, info.st_ino)
        self.max_files, self.max_bytes, self.deadline = max_files, max_bytes, deadline
        self._files = {}
        self._bytes = 0
        self._owner = None
        self._mask = None
        self._closed = False
        self._failure = None
        self.hashed_bytes = 0

    def __enter__(self):
        global _ACTIVE
        if (self._closed or self._owner is not None or _ACTIVE is not None
                or threading.current_thread() is not threading.main_thread()):
            raise RuntimeError("archive_file_proof_exclusive_main_thread_required")
        if signal.getsignal(signal.SIGIO) != signal.SIG_DFL:
            raise RuntimeError("archive_file_proof_sigio_already_owned")
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        occupied = len(os.listdir("/proc/self/fd"))
        if soft != resource.RLIM_INFINITY and self.max_files+occupied+128 > soft:
            raise RuntimeError("archive_file_proof_descriptor_budget_not_admitted")
        self._mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGIO})
        if signal.SIGIO in self._mask or signal.SIGIO in signal.sigpending():
            signal.pthread_sigmask(signal.SIG_SETMASK, self._mask)
            self._mask = None
            raise RuntimeError("archive_file_proof_sigio_already_owned")
        self._owner = (os.getpid(), threading.get_native_id())
        _ACTIVE = self
        return self

    def _fail(self, reason):
        self._failure = reason
        raise RuntimeError(reason)

    def check(self, check_budget=None):
        if (self._closed or self._owner != (os.getpid(), threading.get_native_id())
                or _ACTIVE is not self):
            raise RuntimeError("archive_file_proof_live_owner_required")
        if self._failure:
            raise RuntimeError(self._failure)
        if signal.sigtimedwait({signal.SIGIO}, 0) is not None:
            self._fail("archive_file_proof_lease_broken")
        if monotonic() >= self.deadline:
            self._fail("archive_file_proof_deadline_exceeded")
        if check_budget is not None:
            check_budget()
        info = self.root.lstat()
        if (self.root.resolve(strict=True) != self.root or not stat.S_ISDIR(info.st_mode)
                or (info.st_dev, info.st_ino) != self.identity):
            self._fail("archive_file_proof_root_changed")

    def _path(self, key):
        from scripts.db.archive_root_v2_copy import _path
        return _path(self.root, key, self.identity[0])

    def observe(self, *, key, sha256, byte_count, check_budget=None):
        """Acquire exclusion first, then hash exact bytes; retain the open lease."""
        self.check(check_budget)
        if (not isinstance(sha256, str) or not re.fullmatch("[0-9a-f]{64}", sha256)
                or type(byte_count) is not int or byte_count <= 0):
            raise ValueError("archive_file_proof_descriptor_invalid")
        if key in self._files:
            return self.verify(key=key, sha256=sha256, byte_count=byte_count,
                               check_budget=check_budget)
        if len(self._files) >= self.max_files or self._bytes+byte_count > self.max_bytes:
            raise RuntimeError("archive_file_proof_budget_exceeded")
        fd = os.open(self._path(key), os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            # Linux F_SETOWN_EX=15, F_OWNER_TID=0. Direct the blocked signal to
            # this thread even when the resource watchdog has another thread.
            fcntl.fcntl(fd, 15, struct.pack("ii", 0, self._owner[1]))
            fcntl.fcntl(fd, fcntl.F_SETLEASE, fcntl.F_RDLCK)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size != byte_count:
                self._fail("archive_file_proof_size_mismatch")
            digest = hashlib.sha256()
            while True:
                self.check(check_budget)
                data = os.read(fd, 1024*1024)
                if not data:
                    break
                digest.update(data)
                self.hashed_bytes += len(data)
            if digest.hexdigest() != sha256:
                self._fail("archive_file_proof_checksum_mismatch")
            self._files[key] = (fd, sha256, byte_count, info.st_dev, info.st_ino)
            fd = None
            self._bytes += byte_count
            self.verify(key=key, sha256=sha256, byte_count=byte_count, check_budget=check_budget)
        except OSError as exc:
            self._fail("archive_file_proof_lease_or_io_refused: errno="+str(exc.errno))
        finally:
            if fd is not None:
                os.close(fd)

    def verify(self, *, key, sha256, byte_count, check_budget=None):
        """Check retained lease and exact path binding, without rereading content."""
        self.check(check_budget)
        saved = self._files.get(key)
        if saved is None:
            raise RuntimeError("archive_file_proof_object_not_verified: key="+key)
        fd, expected, size, device, inode = saved
        if (expected, size) != (sha256, byte_count):
            self._fail("archive_file_proof_descriptor_changed")
        if fcntl.fcntl(fd, fcntl.F_GETLEASE) != fcntl.F_RDLCK:
            self._fail("archive_file_proof_lease_lost")
        info = self._path(key).lstat()
        if (info.st_dev, info.st_ino, info.st_size) != (device, inode, size):
            self._fail("archive_file_proof_path_changed: key="+key)
        self.check(check_budget)

    def verify_all(self, *, check_budget=None):
        """Recheck namespace/lease bindings after caller work, before commit."""
        self.check(check_budget)
        for key, (_, digest, size, _, _) in self._files.items():
            self.verify(key=key, sha256=digest, byte_count=size, check_budget=check_budget)

    def __exit__(self, *_):
        global _ACTIVE
        if self._owner != (os.getpid(), threading.get_native_id()):
            raise RuntimeError("archive_file_proof_close_owner_required")
        try:
            for fd, *_ in self._files.values():
                os.close(fd)
            self._files.clear()
            # Consume lease-break notifications before restoring the default
            # signal mask. No lease can generate a later break after its close.
            while signal.sigtimedwait({signal.SIGIO}, 0) is not None:
                pass
        finally:
            self._closed = True
            _ACTIVE = None
            signal.pthread_sigmask(signal.SIG_SETMASK, self._mask)

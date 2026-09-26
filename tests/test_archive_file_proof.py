"""Native Linux leases, not simulated mtime or saved-hash readiness."""
import errno
import hashlib
import mmap
import os
from pathlib import Path
import signal
import subprocess
import sys
from time import monotonic

import pytest

from scripts.db.archive_file_v2_proof import ArchiveFileProof


def _descriptor(root, name="object"):
    data = b"immutable QT test bytes"*128
    (root/name).write_bytes(data)
    return dict(key=name, sha256=hashlib.sha256(data).hexdigest(), byte_count=len(data))


def _proof(root, **kwargs):
    return ArchiveFileProof(root, **(dict(max_files=10, max_bytes=1024**2,
                                         deadline=monotonic()+30) | kwargs))


def test_native_proof_reuses_hash_and_cannot_outlive_context(tmp_path):
    row = _descriptor(tmp_path)
    old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    with _proof(tmp_path) as proof:
        proof.observe(**row)
        count = proof.hashed_bytes
        proof.observe(**row)
        proof.verify_all()
        assert proof.hashed_bytes == count == row["byte_count"]
    assert signal.pthread_sigmask(signal.SIG_BLOCK, set()) == old_mask
    with pytest.raises(RuntimeError, match="live_owner"):
        proof.verify(**row)
    # Closed proof is not a serialized certificate; a fresh process/context
    # starts empty and must hash again while the source is still serving.
    with _proof(tmp_path) as resumed:
        with pytest.raises(RuntimeError, match="not_verified"):
            resumed.verify(**row)
        resumed.observe(**row)
        assert resumed.hashed_bytes == row["byte_count"]


@pytest.mark.parametrize("kind", ["descriptor", "mapping"])
def test_writer_and_writable_mapping_refuse_background_proof(tmp_path, kind):
    row = _descriptor(tmp_path)
    writer = (tmp_path/row["key"]).open("r+b")
    mapping = mmap.mmap(writer.fileno(), 0) if kind == "mapping" else None
    if mapping is not None:
        writer.close()
    try:
        with _proof(tmp_path) as proof:
            with pytest.raises(RuntimeError, match="lease_or_io_refused"):
                proof.observe(**row)
    finally:
        if mapping is not None:
            mapping.close()
        writer.close()


def test_later_write_break_invalidates_even_nonblocking_failed_open(tmp_path):
    row = _descriptor(tmp_path)
    with _proof(tmp_path) as proof:
        proof.observe(**row)
        code = """import os,sys,errno
try:
 fd=os.open(sys.argv[1],os.O_WRONLY|os.O_NONBLOCK)
except OSError as e:
 assert e.errno in (errno.EAGAIN,errno.EWOULDBLOCK),e
else:
 os.close(fd);raise AssertionError('leased destination opened for write')
"""
        subprocess.run([sys.executable, "-c", code, str(tmp_path/row["key"])],
                       check=True, timeout=5)
        with pytest.raises(RuntimeError, match="lease_broken|lease_lost"):
            proof.verify(**row)
        with pytest.raises(RuntimeError, match="lease_broken|lease_lost"):
            proof.observe(**row)
    assert (tmp_path/row["key"]).read_bytes()


@pytest.mark.parametrize("change", ["replace", "symlink", "root"])
def test_namespace_changes_do_not_reuse_leased_inode_proof(tmp_path, change):
    root = tmp_path/"root";root.mkdir()
    row = _descriptor(root)
    with _proof(root) as proof:
        proof.observe(**row)
        path = root/row["key"]
        if change == "root":
            root.rename(tmp_path/"old");root.mkdir()
        else:
            path.rename(root/"old")
            if change == "replace":
                path.write_bytes((root/"old").read_bytes())
            else:
                path.symlink_to(root/"old")
        with pytest.raises(RuntimeError, match="path_changed|not_regular|root_changed"):
            proof.verify(**row)


def test_budgets_checksum_and_expiry_refuse_without_changing_limits(tmp_path, monkeypatch):
    row = _descriptor(tmp_path)
    with _proof(tmp_path, max_files=1) as proof:
        with pytest.raises(RuntimeError, match="checksum_mismatch"):
            proof.observe(**(row | {"sha256": "0"*64}))
    with _proof(tmp_path, max_files=1) as proof:
        proof.observe(**row)
        other = _descriptor(tmp_path, "second")
        with pytest.raises(RuntimeError, match="budget_exceeded"):
            proof.observe(**other)
        with pytest.raises(RuntimeError, match="deadline_exceeded"):
            monkeypatch.setattr("scripts.db.archive_file_v2_proof.monotonic",
                                lambda: proof.deadline+1)
            proof.verify(**row)


def test_killed_controller_drops_lease_and_reentry_rehashes(tmp_path):
    row = _descriptor(tmp_path)
    code = """import os,signal,sys
from time import monotonic
from scripts.db.archive_file_v2_proof import ArchiveFileProof
with ArchiveFileProof(sys.argv[1],max_files=2,max_bytes=1048576,deadline=monotonic()+30) as proof:
 proof.observe(key='object',sha256=sys.argv[2],byte_count=int(sys.argv[3]))
 os.kill(os.getpid(),signal.SIGKILL)
"""
    process = subprocess.run([sys.executable, "-c", code, str(tmp_path),
                              row["sha256"], str(row["byte_count"])], timeout=10)
    assert process.returncode == -signal.SIGKILL
    path = tmp_path/"object"
    fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
    try:
        os.write(fd, b"X")
    finally:
        os.close(fd)
    with _proof(tmp_path) as resumed:
        with pytest.raises(RuntimeError, match="not_verified"):
            resumed.verify(**row)
        with pytest.raises(RuntimeError, match="checksum_mismatch"):
            resumed.observe(**row)


def test_descriptor_admission_and_lost_lease_are_not_silent_fallbacks(tmp_path, monkeypatch):
    import fcntl
    import resource
    row = _descriptor(tmp_path)
    with monkeypatch.context() as limits:
        limits.setattr(resource, "getrlimit", lambda _: (128, 128))
        with pytest.raises(RuntimeError, match="descriptor_budget_not_admitted"):
            with _proof(tmp_path):
                pytest.fail("insufficient FD budget admitted")
    with _proof(tmp_path) as proof:
        proof.observe(**row)
        fcntl.fcntl(proof._files[row["key"]][0], fcntl.F_SETLEASE, fcntl.F_UNLCK)
        with pytest.raises(RuntimeError, match="lease_lost"):
            proof.verify(**row)

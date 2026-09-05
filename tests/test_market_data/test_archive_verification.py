import hashlib
import os

import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits


def _fixture(tmp_path, **limits):
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    path = store.root / "test.parquet"
    path.write_bytes(b"immutable test bytes")
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    return ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits(**limits)), path, checksum


def test_deduplicated_objects_use_one_byte_and_object_budget(tmp_path):
    batch, path, checksum = _fixture(tmp_path, max_objects=1, max_bytes=20)
    first = batch.verify(path.name, checksum, expected_bytes=20)
    assert batch.verify(path.name, checksum) is first
    batch.assert_unchanged()
    assert len(batch.objects) == 1 and batch.byte_count == 20
    with pytest.raises(RuntimeError, match="object_budget_exceeded"):
        batch.verify("another.parquet", checksum)
    with pytest.raises(RuntimeError, match="object_conflict"):
        batch.verify(path.name, "a" * 64)
    with pytest.raises(RuntimeError, match="object_conflict"):
        batch.verify(path.name, checksum, expected_bytes=21)


@pytest.mark.parametrize("mode", ["bytes", "size", "corrupt", "missing", "directory"])
def test_unreadable_unbounded_or_changed_bytes_are_not_verified(tmp_path, mode):
    batch, path, checksum = _fixture(tmp_path, max_bytes=1 if mode == "bytes" else 1000)
    if mode == "corrupt":
        original = path.stat()
        path.write_bytes(b"x" * 20)
        os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    elif mode in {"missing", "directory"}:
        path.unlink()
        if mode == "directory":
            path.mkdir()
    with pytest.raises(FileNotFoundError if mode == "missing" else RuntimeError):
        batch.verify(path.name, checksum, expected_bytes=21 if mode == "size" else None)
    assert batch.objects == {}
    if mode == "bytes":
        assert batch.byte_count == 0  # Oversized files rejected before reading.


def test_short_handoff_detects_replacement_and_new_batch_rehashes(tmp_path):
    batch, path, checksum = _fixture(tmp_path)
    batch.verify(path.name, checksum)
    original = path.stat()
    replacement = path.with_suffix(".replacement")
    replacement.write_bytes(b"x" * 20)
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    replacement.replace(path)
    with pytest.raises(RuntimeError, match="object_changed"):
        batch.assert_unchanged()
    with pytest.raises(RuntimeError, match="object_changed"):
        batch.verify(path.name, checksum)
    restarted = ArchiveVerificationBatch(batch.object_store, limits=batch.limits)
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        restarted.verify(path.name, checksum)


def test_mutation_during_checksum_cannot_get_a_verified_stamp(tmp_path, monkeypatch):
    from market_data import archive_verification
    batch, path, checksum = _fixture(tmp_path)
    real_digest = hashlib.sha256
    class Mutation:
        def __init__(self):
            self.digest = real_digest()
        def update(self, block):
            self.digest.update(block)
            path.write_bytes(b"x" * 20)
        def hexdigest(self):
            return self.digest.hexdigest()
    monkeypatch.setattr(archive_verification.hashlib, "sha256", Mutation)
    with pytest.raises(RuntimeError, match="object_changed"):
        batch.verify(path.name, checksum)
    assert batch.objects == {}


def test_cooperative_run_budget_interrupts_hashing_before_receipting(tmp_path):
    batch, path, checksum = _fixture(tmp_path)
    checks = 0

    def budget():
        nonlocal checks
        checks += 1
        if checks == 2:
            raise RuntimeError("test run budget exhausted")

    batch.check_budget = budget
    with pytest.raises(RuntimeError, match="run budget exhausted"):
        batch.verify(path.name, checksum)
    assert checks == 2 and batch.objects == {}


@pytest.mark.parametrize("name", ["max_pages", "max_objects", "max_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, "1000"])
def test_verification_limits_are_positive_explicit_integers(name, value):
    with pytest.raises(ValueError, match="archive_verification_limit_invalid"):
        ArchiveVerificationLimits(**{name: value})

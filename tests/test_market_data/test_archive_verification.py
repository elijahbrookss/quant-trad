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


def test_distinct_reference_scope_checks_one_path_then_fences_every_object(tmp_path, monkeypatch):
    batch, path, checksum = _fixture(tmp_path)
    calls = []
    local_path = batch.object_store.local_path

    def counted(key):
        calls.append(key)
        return local_path(key)

    monkeypatch.setattr(batch.object_store, "local_path", counted)
    with batch.distinct_references():
        first = batch.verify(path.name, checksum, expected_bytes=20)
        for _ in range(1000):
            assert batch.verify(path.name, checksum) is first
        assert calls == [path.name]
        assert batch.byte_count == 20
    assert calls == [path.name, path.name]
    # Outside this explicit proof scope the original immediate check remains.
    batch.verify(path.name, checksum)
    assert calls == [path.name] * 3
    # A later proof must hash current bytes again, never reuse a stamp.
    path.write_bytes(b"x" * 20)
    restarted = ArchiveVerificationBatch(batch.object_store, limits=batch.limits)
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        with restarted.distinct_references():
            restarted.verify(path.name, checksum)


@pytest.mark.parametrize("claim", ["checksum", "size", "invalid_hash", "invalid_size"])
def test_distinct_references_still_validate_every_claim(tmp_path, claim):
    batch, path, checksum = _fixture(tmp_path)
    with pytest.raises((RuntimeError, ValueError), match="object_conflict|hash_invalid|size_invalid"):
        with batch.distinct_references():
            batch.verify(path.name, checksum)
            batch.verify(path.name, "a" * 64 if claim == "checksum" else
                         "invalid" if claim == "invalid_hash" else checksum,
                         expected_bytes=21 if claim == "size" else
                         True if claim == "invalid_size" else None)
    assert not batch._distinct_references


@pytest.mark.parametrize("change", ["replacement", "same_size", "missing", "root_alias"])
def test_distinct_scope_cannot_finish_after_file_or_path_change(tmp_path, change):
    batch, path, checksum = _fixture(tmp_path)
    with pytest.raises((RuntimeError, FileNotFoundError, ValueError),
                       match="object_changed|No such file|escapes root"):
        with batch.distinct_references():
            batch.verify(path.name, checksum)
            original = path.stat()
            if change == "replacement":
                replacement = path.with_suffix(".replacement")
                replacement.write_bytes(b"immutable test bytes")
                replacement.replace(path)
            elif change == "same_size":
                path.write_bytes(b"x" * 20)
                os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
            elif change == "missing":
                path.unlink()
            else:
                moved = tmp_path / "moved"
                batch.object_store.root.rename(moved)
                batch.object_store.root.symlink_to(moved, target_is_directory=True)
            # Repeated claims can be grouped; scope completion must still fail.
            batch.verify(path.name, checksum)
    assert not batch._distinct_references


def test_distinct_scope_rechecks_mount_uuid_on_exit(tmp_path, monkeypatch):
    batch, path, checksum = _fixture(tmp_path)
    udev = tmp_path / "udev"
    udev.mkdir()
    device = path.stat().st_dev
    record = udev / f"b{os.major(device)}:{os.minor(device)}"
    record.write_text("E:ID_FS_UUID=fixture-good-uuid\n")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(batch.object_store.root))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "fixture-good-uuid")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    with pytest.raises(RuntimeError, match="storage_mount_identity_mismatch"):
        with batch.distinct_references():
            batch.verify(path.name, checksum)
            record.write_text("E:ID_FS_UUID=fixture-wrong-uuid\n")
            batch.verify(path.name, checksum)
    assert not batch._distinct_references


def test_distinct_scope_keeps_per_reference_cancellation_and_resets_on_error(tmp_path):
    batch, path, checksum = _fixture(tmp_path)
    batch.verify(path.name, checksum)
    def cancelled():
        raise RuntimeError("cancelled repeated reference")
    batch.check_budget = cancelled
    with pytest.raises(RuntimeError, match="cancelled repeated reference"):
        with batch.distinct_references():
            batch.verify(path.name, checksum)
    assert not batch._distinct_references


def test_distinct_scope_rejects_nesting_without_disabling_outer_fence(tmp_path):
    batch, path, checksum = _fixture(tmp_path)
    with pytest.raises(RuntimeError, match="object_changed"):
        with batch.distinct_references():
            batch.verify(path.name, checksum)
            with pytest.raises(RuntimeError, match="distinct_scope_nested"):
                with batch.distinct_references():
                    pass
            path.write_bytes(b"x" * 20)
    assert not batch._distinct_references


def test_distinct_scope_fences_objects_added_before_and_during_scope(tmp_path):
    batch, path, checksum = _fixture(tmp_path)
    batch.verify(path.name, checksum)
    other = path.with_name("second.parquet")
    other.write_bytes(b"another current object")
    with pytest.raises(RuntimeError, match="object_changed"):
        with batch.distinct_references():
            batch.verify(other.name, hashlib.sha256(other.read_bytes()).hexdigest())
            # No repeated reference to this changed object is needed: exit
            # fences all verified objects, including those predating the scope.
            path.write_bytes(b"x" * 20)

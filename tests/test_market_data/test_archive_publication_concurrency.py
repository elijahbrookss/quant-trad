from concurrent.futures import ThreadPoolExecutor
import hashlib
import threading

import pytest

import market_data.archive as archive


def test_reuse_syncs_a_competing_publication_before_acknowledgement(tmp_path, monkeypatch):
    store = archive.FilesystemRawArchiveObjectStore(tmp_path / "objects")
    destination = store.local_path("fact.parquet")
    destination.write_bytes(b"already linked immutable bytes")
    synced = []
    monkeypatch.setattr(archive, "_fsync_directory", synced.append)
    result = store.put_verified(object_key="fact.parquet", source_path=destination,
                                expected_sha256=hashlib.sha256(destination.read_bytes()).hexdigest())
    assert result.reused_existing is True
    assert synced == [destination.parent]


@pytest.mark.parametrize("identical", [False, True])
def test_publication_never_replaces_a_winner_during_copy(tmp_path, monkeypatch, identical):
    store = archive.FilesystemRawArchiveObjectStore(tmp_path / "objects")
    source = tmp_path / "source"
    source.write_bytes(b"candidate archive")
    winner = source.read_bytes() if identical else b"other immutable archive"
    destination = store.local_path("fact.parquet")
    copy = archive.shutil.copyfileobj

    def competing_publish(source_handle, target, **kwargs):
        copy(source_handle, target, **kwargs)
        destination.write_bytes(winner)

    monkeypatch.setattr(archive.shutil, "copyfileobj", competing_publish)
    kwargs = dict(object_key="fact.parquet", source_path=source,
                  expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    if identical:
        assert store.put_verified(**kwargs).reused_existing is True
    else:
        with pytest.raises(RuntimeError, match="market_archive_object_conflict"):
            store.put_verified(**kwargs)
    assert destination.read_bytes() == winner
    assert list(store.root.iterdir()) == [destination]


@pytest.mark.parametrize("identical", [False, True])
def test_threads_use_distinct_staging_files_and_one_immutable_winner(tmp_path, monkeypatch, identical):
    store = archive.FilesystemRawArchiveObjectStore(tmp_path / "objects")
    sources = [tmp_path / "first", tmp_path / "second"]
    sources[0].write_bytes(b"first archive")
    sources[1].write_bytes(sources[0].read_bytes() if identical else b"second archive")
    copied = threading.Barrier(2)
    copy = archive.shutil.copyfileobj

    def synchronized_copy(source_handle, target, **kwargs):
        copy(source_handle, target, **kwargs)
        copied.wait(timeout=5)

    monkeypatch.setattr(archive.shutil, "copyfileobj", synchronized_copy)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(store.put_verified, object_key="fact.parquet", source_path=source,
                               expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
                   for source in sources]
        successes, failures = [], []
        for future in futures:
            try:
                successes.append(future.result(timeout=10))
            except RuntimeError as exc:
                failures.append(exc)
    destination = store.local_path("fact.parquet")
    if identical:
        assert len(successes) == 2
        assert sorted(result.reused_existing for result in successes) == [False, True]
        assert not failures
    else:
        assert len(successes) == 1
        assert len(failures) == 1
        assert "market_archive_object_conflict" in str(failures[0])
    assert hashlib.sha256(destination.read_bytes()).hexdigest() == successes[0].sha256
    assert list(store.root.iterdir()) == [destination]


def test_bounded_publication_interrupts_inflight_copy_and_retry_reuses_bytes(tmp_path):
    store = archive.FilesystemRawArchiveObjectStore(tmp_path / "objects")
    source = tmp_path / "source"
    original = b"bounded archive" * (256 * 1024)
    source.write_bytes(original)
    expected = hashlib.sha256(original).hexdigest()
    kwargs = dict(object_key="nested/history.parquet", source_path=source,
                  expected_sha256=expected)
    interrupted = []

    def stop_during_copy():
        partials = list(store.root.rglob("*.partial"))
        if partials and partials[0].stat().st_size >= 1024 * 1024:
            interrupted.append(partials[0].stat().st_size)
            raise RuntimeError("disposable_copy_cancelled")

    with pytest.raises(RuntimeError, match="disposable_copy_cancelled"):
        store.put_verified(**kwargs, check_budget=stop_during_copy)
    assert interrupted and interrupted[0] < len(original)
    assert source.read_bytes() == original
    assert not store.local_path(kwargs["object_key"]).exists()
    assert not list(store.root.rglob("*.partial"))
    first = store.put_verified(**kwargs, check_budget=lambda: None)
    before = store.local_path(kwargs["object_key"]).stat()
    retry = store.put_verified(**kwargs, check_budget=lambda: None)
    assert not first.reused_existing and retry.reused_existing
    assert store.local_path(kwargs["object_key"]).stat().st_ino == before.st_ino
    assert source.read_bytes() == store.local_path(kwargs["object_key"]).read_bytes() == original


def test_bounded_reuse_checks_budget_while_hashing_existing_bytes(tmp_path):
    store = archive.FilesystemRawArchiveObjectStore(tmp_path / "objects")
    source = tmp_path / "source"
    source.write_bytes(b"x" * (3 * 1024 * 1024))
    kwargs = dict(object_key="history.parquet", source_path=source,
                  expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    store.put_verified(**kwargs)
    checks = []

    def exhausted():
        checks.append(True)
        # Initial admission + source hash (four checks), then existing-object
        # hash must also remain cancellable between chunks.
        if len(checks) == 7:
            raise RuntimeError("disposable_hash_budget_exhausted")

    with pytest.raises(RuntimeError, match="disposable_hash_budget_exhausted"):
        store.put_verified(**kwargs, check_budget=exhausted)
    assert len(checks) == 7
    assert store.local_path("history.parquet").read_bytes() == source.read_bytes()

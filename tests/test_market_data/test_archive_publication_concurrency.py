from concurrent.futures import ThreadPoolExecutor
import hashlib
import threading

import pytest

import market_data.archive as archive


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

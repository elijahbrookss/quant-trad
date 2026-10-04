from contextlib import contextmanager
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import threading
from time import time

import pytest

from core.execution_control import ExecutionControl, ExecutionCancelledError, ExecutionBudgetExceededError, controlled_execution
from market_data.fact_archive import encode_canonical_fact_archive, read_canonical_fact_archive
from market_data.history_read_cache import CanonicalArchiveReadCache, HistoryCacheLimits, CacheBypass
from tests.test_market_data.test_canonical_fact_archive import _row
from tests.test_market_data.test_fact_storage_reads import Session, _catalog, _cold
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import publish_canonical_fact_archive


@pytest.fixture
def cache(tmp_path):
    root = tmp_path / "history-read-cache"
    root.mkdir(mode=0o700)
    @contextmanager
    def capacity():
        yield lambda additional: 1024**3
    return CanonicalArchiveReadCache(root, limits=HistoryCacheLimits(1024**2),
                                     capacity_scope=capacity, check_mount=lambda root: None)


def _source(tmp_path, name, content):
    path = tmp_path / name
    path.write_bytes(content)
    return path, hashlib.sha256(content).hexdigest(), len(content)


def _read(cache, source):
    path, digest, size = source
    with cache.open_copy(path, digest=digest, size=size) as handle:
        return None if handle is None else handle.read()


def test_repeated_copy_uses_ssd_and_preserves_source(cache, tmp_path):
    source = _source(tmp_path, "source", b"original historical bytes")
    original = source[0].stat()
    control = ExecutionControl()
    with controlled_execution(control):
        assert _read(cache, source) == source[0].read_bytes()
        assert _read(cache, source) == source[0].read_bytes()
    metrics = control.snapshot()["consumed"]
    assert metrics["archive_cache_fills"] == 1
    assert metrics["archive_cache_hits"] == 1
    assert metrics["archive_source_bytes"] == source[2]
    assert source[0].stat().st_mtime_ns == original.st_mtime_ns
    assert source[0].read_bytes() == b"original historical bytes"


def test_lru_eviction_uses_access_time_not_market_age(cache, tmp_path):
    cache.limits = replace(cache.limits, max_objects=2)
    a = _source(tmp_path, "a", b"2022 data")
    b = _source(tmp_path, "b", b"2026 data")
    c = _source(tmp_path, "c", b"new request")
    _read(cache, a)
    _read(cache, b)
    for source in (a, b):
        os.utime(cache.root / (source[1] + ".parquet"), (time()-60, time()-60))
    _read(cache, a)
    _read(cache, c)
    assert (cache.root / (a[1]+".parquet")).exists()
    assert not (cache.root / (b[1]+".parquet")).exists()
    assert all(item[0].exists() for item in (a,b,c))


def test_active_reader_is_not_evicted(cache, tmp_path):
    cache.limits = replace(cache.limits, max_objects=1)
    a = _source(tmp_path, "a", b"a")
    b = _source(tmp_path, "b", b"b")
    with cache.open_copy(a[0], digest=a[1], size=a[2]) as reader:
        assert _read(cache, b) is None
        assert reader.read() == b"a"
        assert (cache.root / (a[1]+".parquet")).exists()
    assert _read(cache, b) == b"b"
    assert not (cache.root / (a[1]+".parquet")).exists()


def test_concurrent_duplicate_fill_never_publishes_twice(cache, tmp_path):
    source = _source(tmp_path, "source", b"same object")
    entered, release = threading.Event(), threading.Event()
    @contextmanager
    def capacity():
        entered.set()
        assert release.wait(5)
        yield lambda additional: 1024**3
    cache.capacity_scope = capacity
    values, errors = [], []
    def first():
        try:
            values.append(_read(cache, source))
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=first)
    thread.start()
    try:
        assert entered.wait(5)
        assert _read(cache, source) is None  # HDD bypass; no second fill/wait.
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and not errors
    assert values == [b"same object"]
    assert [p.name for p in cache.root.iterdir()] == [source[1]+".parquet"]


def test_cancelled_fill_cleans_partial_and_releases_locks(cache, tmp_path):
    source = _source(tmp_path, "source", b"a" * (2*1024**2))
    cache.limits = replace(cache.limits, max_bytes=4*1024**2)
    control = ExecutionControl()
    @contextmanager
    def capacity():
        def check(additional):
            if list(cache.root.glob("*.partial")):
                control.stop(ExecutionCancelledError("cancel test"))
            control.check()
            return 1024**3
        yield check
    cache.capacity_scope = capacity
    with pytest.raises(ExecutionCancelledError, match="cancel test"), controlled_execution(control):
        _read(cache, source)
    assert not list(cache.root.iterdir())
    assert source[0].stat().st_size == source[2]


def test_abandoned_partial_recovery_is_confined(cache, tmp_path):
    source = _source(tmp_path, "source", b"durable")
    partial = cache.root / (source[1]+".partial")
    partial.write_bytes(b"interrupted")
    partial.chmod(0o600)
    untouched = tmp_path / "spool.partial"
    untouched.write_bytes(b"keep")
    assert _read(cache, source) == b"durable"
    assert not partial.exists() and untouched.read_bytes() == b"keep"


@pytest.mark.parametrize("fault", ["missing_root", "symlink", "hardlink", "unexpected_file", "wrong_mount", "unprivate_root"])
def test_invalid_cache_bypasses_without_changing_source(cache, tmp_path, fault):
    source = _source(tmp_path, "source", b"source")
    target = cache.root / (source[1]+".parquet")
    if fault == "missing_root":
        cache.root.rmdir()
    elif fault == "symlink":
        target.symlink_to(source[0])
    elif fault == "hardlink":
        os.link(source[0], target)
    elif fault == "unexpected_file":
        (cache.root / "unowned.txt").write_text("keep")
    elif fault == "wrong_mount":
        def wrong(path):
            raise CacheBypass("wrong_mount")
        cache.check_mount = wrong
    else:
        cache.root.chmod(0o755)
    assert _read(cache, source) is None
    assert source[0].read_bytes() == b"source"


def test_oversized_object_bypasses(cache, tmp_path):
    cache.limits = replace(cache.limits, max_bytes=1)
    assert _read(cache, _source(tmp_path,"source",b"large")) is None
    assert not list(cache.root.iterdir())


def test_pressure_evicts_unused_copy_then_checks_fresh_capacity(cache, tmp_path):
    a = _source(tmp_path,"a",b"old")
    b = _source(tmp_path,"b",b"new")
    _read(cache,a)
    @contextmanager
    def capacity():
        def available(additional):
            space = 0 if (cache.root/(a[1]+".parquet")).exists() else 1024**2
            if additional > space:
                raise CacheBypass("headroom")
            return space
        yield available
    cache.capacity_scope=capacity
    assert _read(cache,b) == b"new"
    assert not (cache.root/(a[1]+".parquet")).exists()


def test_insufficient_headroom_never_publishes(cache,tmp_path):
    @contextmanager
    def capacity():
        def available(additional):
            if additional: raise CacheBypass("headroom")
            return 0
        yield available
    cache.capacity_scope=capacity
    assert _read(cache,_source(tmp_path,"source",b"a")) is None
    assert not list(cache.root.iterdir())


def test_expired_inactive_copy_is_reclaimed_on_fill(cache,tmp_path):
    a=_source(tmp_path,"a",b"a")
    b=_source(tmp_path,"b",b"b")
    _read(cache,a)
    old=time()-cache.limits.idle_seconds-1
    os.utime(cache.root/(a[1]+".parquet"),(old,old))
    assert _read(cache,b)==b"b"
    assert not (cache.root/(a[1]+".parquet")).exists()


def test_cache_full_codec_equivalence_and_corrupt_copy_fallback(cache,tmp_path):
    rows=[_row(1),_row(2)]
    encoded=encode_canonical_fact_archive(rows,temporary_directory=tmp_path/"encoding")
    control=ExecutionControl()
    with controlled_execution(control):
        first=read_canonical_fact_archive(encoded.path,expected=encoded.manifest,cache=cache)
        second=read_canonical_fact_archive(encoded.path,expected=encoded.manifest,cache=cache)
    assert first==second==tuple(rows)
    stats=control.snapshot()["consumed"]
    assert stats["archive_source_bytes"]==encoded.manifest.byte_count
    assert stats["archive_cache_bytes"]>encoded.manifest.byte_count
    cached=cache.root/(encoded.manifest.object_sha256+".parquet")
    content=cached.read_bytes()
    cached.write_bytes(b"x"+content[1:])
    assert read_canonical_fact_archive(encoded.path,expected=encoded.manifest,cache=cache)==tuple(rows)
    assert not cached.exists()
    assert encoded.path.read_bytes()==content


def test_corrupt_authoritative_source_is_not_hidden_by_fill(cache,tmp_path):
    encoded=encode_canonical_fact_archive([_row()],temporary_directory=tmp_path/"encoding")
    encoded.path.write_bytes(b"x"*encoded.manifest.byte_count)
    with pytest.raises(RuntimeError,match="checksum_mismatch"):
        read_canonical_fact_archive(encoded.path,expected=encoded.manifest,cache=cache)
    assert not list(cache.root.iterdir())


def test_archive_io_budget_applies_on_source_and_cached_reads(cache,tmp_path):
    encoded=encode_canonical_fact_archive([_row()],temporary_directory=tmp_path/"encoding")
    read_canonical_fact_archive(encoded.path,expected=encoded.manifest,cache=cache)
    for selected_cache in (None,cache):
        control=ExecutionControl()
        control.limit(seconds=10,archive_read_bytes=10)
        with pytest.raises(ExecutionBudgetExceededError,match="archive_read_bytes"), controlled_execution(control):
            read_canonical_fact_archive(encoded.path,expected=encoded.manifest,cache=selected_cache)


def test_repository_cache_does_not_select_or_modify_frozen_envelopes(cache,tmp_path):
    rows=[_row(1),_row(2)]
    store=FilesystemRawArchiveObjectStore(tmp_path/"objects")
    manifest=publish_canonical_fact_archive(rows,object_store=store,temporary_directory=tmp_path/"staging")
    repo=PostgresCanonicalFactStorageRepository(object_store_factory=lambda:store,read_cache_factory=lambda:cache)
    session=Session([_catalog(manifest,row["id"]) for row in rows])
    expected=[{**row,"storage_day":_cold(row)["storage_day"]} for row in rows]
    for _ in range(2):
        assert repo.hydrate_rows(session,[_cold(row) for row in rows])==expected
    assert store.local_path(manifest.object_key).exists()


def _child_cache(root, max_objects=8192):
    @contextmanager
    def capacity():
        yield lambda additional: 1024**3
    return CanonicalArchiveReadCache(Path(root), limits=HistoryCacheLimits(1024**2, max_objects=max_objects),
                                    capacity_scope=capacity, check_mount=lambda root: None)


def _crash_cache_fill(root, source, published):
    cache = _child_cache(root)
    rename = os.rename
    def interrupt(*args, **kwargs):
        if published:
            rename(*args, **kwargs)
        os._exit(17)
    os.rename = interrupt
    _read(cache, source)


def _hold_cache_reader(root, source, entered, release):
    cache = _child_cache(root, max_objects=1)
    with cache.open_copy(source[0], digest=source[1], size=source[2]) as handle:
        entered.set()
        assert release.wait(15)
        assert handle.read() == b"first"


@pytest.mark.parametrize("published", [False, True])
def test_process_crash_at_publication_recovers_only_disposable_copy(cache, tmp_path, published):
    import multiprocessing
    source = _source(tmp_path, "source", b"durable history")
    context = multiprocessing.get_context("spawn")
    child = context.Process(target=_crash_cache_fill, args=(cache.root, source, published))
    child.start()
    child.join(15)
    if child.is_alive():
        child.terminate()
        child.join(5)
        pytest.fail("cache crash fixture did not finish")
    assert child.exitcode == 17
    expected = ".parquet" if published else ".partial"
    assert [item.name for item in cache.root.iterdir()] == [source[1] + expected]
    assert _read(cache, source) == b"durable history"
    assert not list(cache.root.glob("*.partial"))
    assert source[0].read_bytes() == b"durable history"


def test_other_process_active_reader_prevents_eviction(cache, tmp_path):
    import multiprocessing
    cache.limits = replace(cache.limits, max_objects=1)
    source = _source(tmp_path, "first", b"first")
    other = _source(tmp_path, "other", b"other")
    context = multiprocessing.get_context("spawn")
    entered, release = context.Event(), context.Event()
    child = context.Process(target=_hold_cache_reader, args=(cache.root, source, entered, release))
    child.start()
    try:
        assert entered.wait(15)
        assert _read(cache, other) is None
    finally:
        release.set()
        child.join(5)
        if child.is_alive():
            child.terminate()
            child.join(5)
    assert child.exitcode == 0
    assert _read(cache, other) == b"other"


@pytest.mark.parametrize("fault", ["deadline", "write_error", "root_replaced"])
def test_fill_fault_bypasses_without_publishing(cache, tmp_path, monkeypatch, fault):
    import market_data.history_read_cache as module
    source = _source(tmp_path, "source", b"durable")
    if fault == "write_error":
        def fail(*args):
            raise OSError("injected full disk")
        monkeypatch.setattr(module.os, "write", fail)
    else:
        @contextmanager
        def capacity():
            def check(additional):
                if list(cache.root.glob("*.partial")):
                    if fault == "deadline":
                        monkeypatch.setattr(module, "monotonic", lambda: float("inf"))
                    else:
                        cache.root.rename(tmp_path / "old-cache")
                        cache.root.mkdir(mode=0o700)
                return 1024**3
            yield check
        cache.capacity_scope = capacity
    assert _read(cache, source) is None
    assert not list(cache.root.iterdir())
    assert not list((tmp_path / "old-cache").glob("*"))
    assert source[0].read_bytes() == b"durable"


def test_manifest_over_codec_limit_does_not_populate_cache(cache, tmp_path):
    from market_data.fact_archive import FactArchiveLimits
    encoded = encode_canonical_fact_archive([_row()], temporary_directory=tmp_path/"encoding")
    with pytest.raises(ValueError, match="manifest bytes"):
        read_canonical_fact_archive(encoded.path, expected=encoded.manifest, cache=cache,
                                    limits=replace(FactArchiveLimits(), max_file_bytes=1))
    assert not list(cache.root.iterdir())


def test_enabled_configuration_composes_cache_without_opening_database(tmp_path, monkeypatch):
    import core.settings as settings_module
    from portal.backend.service.storage.history_policy import configured_history_read_cache
    settings = settings_module.get_settings()
    monkeypatch.setattr(settings_module, "get_settings", lambda: replace(settings,
        storage=replace(settings.storage, history_cache_bytes=1024**2, history_cache_min_free_bytes=100)))
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(tmp_path))
    cache = configured_history_read_cache()
    assert isinstance(cache, CanonicalArchiveReadCache)
    assert cache.root == tmp_path / "history-read-cache"
    assert cache.limits.max_bytes == 1024**2
    assert not cache.root.exists()

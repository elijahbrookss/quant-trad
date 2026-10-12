from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from market_data import fact_archive
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from tests.test_market_data.test_canonical_fact_archive import BASE, _row
from tests.test_market_data.test_fact_storage_reads import _catalog, _cold


class ColdSession:
    def __init__(self, rows, manifests, *, batches=()):
        self.rows = {row["id"]: row for row in rows}
        self.manifests = manifests
        self.selected = MagicMock()
        self.selected.scalars.return_value.partitions.return_value = iter(batches)
        self.calls = []

    def execute(self, query, params, **kwargs):
        if kwargs:
            return self.selected
        self.calls.append((str(query), params))
        ids = params["fact_ids"]
        if "FROM market.fact_identities" in str(query):
            rows = [{"id": identity, "storage_day": BASE.date()} for identity in ids]
        elif "SELECT requested.id" in str(query):
            rows = [_catalog(self.manifests[identity], identity) for identity in ids]
        else:
            assert "WHERE versions.id = ANY(:fact_ids)" in str(query)
            rows = [_cold(self.rows[identity]) for identity in ids]
        result = MagicMock()
        result.mappings.return_value.all.return_value = rows
        return result


def archived_reader(tmp_path, rows):
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    manifest = fact_archive.publish_canonical_fact_archive(
        rows, object_store=store, temporary_directory=tmp_path / "staging")
    reader = PostgresCanonicalFactStorageRepository(
        object_store_factory=lambda: FilesystemRawArchiveObjectStore(store.root, writable=False),
        read_cache_factory=lambda: None)
    return reader, manifest, store


def count_decodes(monkeypatch):
    original = fact_archive.pq.ParquetFile
    calls = []
    def decode(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(fact_archive.pq, "ParquetFile", decode)
    return calls


def test_one_stream_decodes_repeated_page_once_and_next_stream_revalidates(tmp_path, monkeypatch):
    rows = [_row(1), _row(2)]
    reader, manifest, _ = archived_reader(tmp_path, rows)
    calls = count_decodes(monkeypatch)
    ids = [row["id"] for row in rows]
    for request in range(2):
        session = ColdSession(rows, dict.fromkeys(ids, manifest), batches=[[identity] for identity in ids])
        with reader.stream_rows_by_ids(session, object(), batch_size=1) as stream:
            assert list(stream) == [{**row, "storage_day": BASE.date()} for row in rows]
        session.selected.close.assert_called_once()
        assert len(calls) == request + 1


def test_one_identity_read_reuses_page_across_sql_batches(tmp_path, monkeypatch):
    rows = [_row(i) for i in range(1, 1002)]
    reader, manifest, _ = archived_reader(tmp_path, rows)
    calls = count_decodes(monkeypatch)
    ids = [row["id"] for row in rows]
    session = ColdSession(rows, dict.fromkeys(ids, manifest))
    assert reader.read_rows_by_ids(session, ids) == {
        row["id"]: {**row, "storage_day": BASE.date()} for row in rows}
    assert len(calls) == 1
    assert max(len(params["fact_ids"]) for _, params in session.calls) <= 1000


def test_reused_page_still_rejects_changed_file_and_closes_stream(tmp_path, monkeypatch):
    rows = [_row(1), _row(2)]
    reader, manifest, store = archived_reader(tmp_path, rows)
    ids = [row["id"] for row in rows]
    session = ColdSession(rows, dict.fromkeys(ids, manifest), batches=[[identity] for identity in ids])
    with pytest.raises(fact_archive.ArchiveChecksumMismatch):
        with reader.stream_rows_by_ids(session, object(), batch_size=1) as stream:
            assert next(stream)["id"] == ids[0]
            store.local_path(manifest.object_key).write_bytes(b"corrupted after first batch")
            next(stream)
    session.selected.close.assert_called_once()


def test_reused_page_still_checks_each_selected_envelope(tmp_path):
    rows = [_row(1), _row(2)]
    reader, manifest, _ = archived_reader(tmp_path, rows)
    ids = [row["id"] for row in rows]
    session = ColdSession(rows, dict.fromkeys(ids, manifest), batches=[[identity] for identity in ids])
    with pytest.raises(RuntimeError, match="canonical_archive_envelope_mismatch"):
        with reader.stream_rows_by_ids(session, object(), batch_size=1) as stream:
            next(stream)
            session.rows[ids[1]] = {**rows[1], "row_hash": "changed"}
            next(stream)
    session.selected.close.assert_called_once()


def test_interleaved_streams_own_independent_pages_and_mutable_outputs(tmp_path, monkeypatch):
    rows = [_row(1), _row(2)]
    reader, manifest, _ = archived_reader(tmp_path, rows)
    ids = [row["id"] for row in rows]
    sessions = [ColdSession(rows, dict.fromkeys(ids, manifest), batches=[[ids[0]], [ids[0]]]) for _ in range(2)]
    calls = count_decodes(monkeypatch)
    with reader.stream_rows_by_ids(sessions[0], object(), batch_size=1) as first:
        with reader.stream_rows_by_ids(sessions[1], object(), batch_size=1) as second:
            changed = next(first)
            changed["payload"].clear()
            changed["provenance"].clear()
            assert next(second) == {**rows[0], "storage_day": BASE.date()}
            assert next(first) == {**rows[0], "storage_day": BASE.date()}
            assert next(second) == {**rows[0], "storage_day": BASE.date()}
    assert len(calls) == 2


@pytest.mark.parametrize("cancel", [False, True])
def test_stream_releases_retained_page_on_early_exit_or_cancellation(tmp_path, monkeypatch, cancel):
    from portal.backend.service.storage.repos import fact_storage
    rows = [_row(1), _row(2)]
    reader, manifest, _ = archived_reader(tmp_path, rows)
    ids = [row["id"] for row in rows]
    session = ColdSession(rows, dict.fromkeys(ids, manifest), batches=[[identity] for identity in ids])
    original = fact_archive.CanonicalArchiveReadPage
    scopes = []
    def scope():
        page = original()
        scopes.append(page)
        return page
    monkeypatch.setattr(fact_storage, "CanonicalArchiveReadPage", scope)
    with reader.stream_rows_by_ids(session, object(), batch_size=1) as stream:
        next(stream)
        assert scopes[0]._entry is not None
        if cancel:
            def cancelled():
                raise RuntimeError("cancelled fixture request")
            monkeypatch.setattr(fact_storage, "execution_checkpoint", cancelled)
            with pytest.raises(RuntimeError, match="cancelled fixture request"):
                next(stream)
    assert len(scopes) == 1 and scopes[0]._entry is None
    session.selected.close.assert_called_once()


def test_decoded_page_reuses_only_one_manifest_and_exact_limits(tmp_path, monkeypatch):
    from dataclasses import replace
    rows = [_row(1), _row(2)]
    _, first, store = archived_reader(tmp_path, rows)
    second = fact_archive.publish_canonical_fact_archive(
        [_row(3)], object_store=store, temporary_directory=tmp_path / "staging")
    page = fact_archive.CanonicalArchiveReadPage()
    calls = count_decodes(monkeypatch)
    limits = fact_archive.FactArchiveLimits()
    for manifest in [first, second, first]:
        fact_archive.read_canonical_fact_archive(
            store.local_path(manifest.object_key), expected=manifest, limits=limits, decoded_page=page)
        assert page._entry[0] == manifest
    assert len(calls) == 3
    with pytest.raises(ValueError, match="footer row count"):
        fact_archive.read_canonical_fact_archive(store.local_path(first.object_key), expected=first,
            limits=replace(limits, max_rows=1, row_group_size=1), decoded_page=page)
    assert page._entry is None


def test_decoded_page_never_trusts_a_changed_manifest(tmp_path):
    from dataclasses import replace
    rows = [_row(1), _row(2)]
    _, manifest, store = archived_reader(tmp_path, rows)
    page = fact_archive.CanonicalArchiveReadPage()
    path = store.local_path(manifest.object_key)
    actual = fact_archive.read_canonical_fact_archive(path, expected=manifest, decoded_page=page)
    actual[0]["payload"].clear()
    assert fact_archive.read_canonical_fact_archive(path, expected=manifest, decoded_page=page)[0] == rows[0]
    with pytest.raises(RuntimeError, match="canonical_archive_manifest_mismatch"):
        fact_archive.read_canonical_fact_archive(path,
            expected=replace(manifest, content_fingerprint="0" * 64), decoded_page=page)
    assert page._entry is None

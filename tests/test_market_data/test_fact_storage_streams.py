from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository


def _stream_fixture(monkeypatch, batches):
    session = MagicMock()
    selected = session.execute.return_value
    selected.scalars.return_value.partitions.return_value = iter(batches)
    storage = PostgresCanonicalFactStorageRepository()
    hydrate = MagicMock(side_effect=lambda _session, ids: {identity: {"id": identity} for identity in reversed(ids)})
    monkeypatch.setattr(storage, "read_rows_by_ids", hydrate)
    return storage, session, selected, hydrate


def test_stream_hydrates_lazily_in_selected_order_and_closes_early(monkeypatch):
    storage, session, selected, hydrate = _stream_fixture(monkeypatch, [["b", "a"], ["c"]])
    statement = object()
    with storage.stream_rows_by_ids(session, statement, {"scope": 1}, batch_size=2) as rows:
        hydrate.assert_not_called()
        assert next(rows) == {"id": "b"}
        assert next(rows) == {"id": "a"}
        hydrate.assert_called_once_with(session, ["b", "a"])
    selected.close.assert_called_once()
    assert list(rows) == []
    session.execute.assert_called_once_with(statement, {"scope": 1}, execution_options={"stream_results": True, "yield_per": 2})


def test_stream_reads_multiple_batches_and_propagates_hydration_failure(monkeypatch):
    storage, session, selected, hydrate = _stream_fixture(monkeypatch, [["b"], ["a"]])
    with storage.stream_rows_by_ids(session, object(), batch_size=1) as rows:
        assert [row["id"] for row in rows] == ["b", "a"]
    assert hydrate.call_count == 2
    selected.close.assert_called_once()
    storage, session, selected, hydrate = _stream_fixture(monkeypatch, [["broken"]])
    hydrate.side_effect = RuntimeError("canonical_archive_checksum_mismatch")
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        with storage.stream_rows_by_ids(session, object()) as rows:
            next(rows)
    selected.close.assert_called_once()


@pytest.mark.parametrize("batch_size", [0, 1001, -1, True, 1.5, "128"])
def test_stream_rejects_invalid_limits_before_sql(batch_size):
    session = MagicMock()
    with pytest.raises(ValueError, match="canonical_stream_batch_invalid"):
        with PostgresCanonicalFactStorageRepository().stream_rows_by_ids(session, object(), batch_size=batch_size):
            pytest.fail("invalid stream was admitted")
    session.execute.assert_not_called()


def test_duplicate_identity_batch_fails_and_closes(monkeypatch):
    storage, session, selected, hydrate = _stream_fixture(monkeypatch, [["a", "a"]])
    with pytest.raises(RuntimeError, match="identity_duplicate"):
        with storage.stream_rows_by_ids(session, object()) as rows:
            next(rows)
    hydrate.assert_not_called()
    selected.close.assert_called_once()


def _witness_fixture(monkeypatch, *, candidate="a", provenance=None, series_id=1):
    storage = PostgresCanonicalFactStorageRepository()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = candidate
    row = dict(id="a", series_id=series_id, material_hash="canonical", provenance=provenance or {})
    read = MagicMock(return_value={"a": row})
    monkeypatch.setattr(storage, "read_rows_by_ids", read)
    return storage, session, row, read


def test_material_candidate_requires_payload_proof_and_matching_scope(monkeypatch):
    storage, session, row, read = _witness_fixture(monkeypatch)
    assert storage.material_witness_exists(session, series_ids=[1], material_hash="canonical")
    read.assert_called_once_with(session, ["a"])
    with pytest.raises(RuntimeError, match="alias_mismatch"):
        storage.material_witness_exists(session, series_ids=[1], material_hash="invented")
    row["series_id"] = 2
    with pytest.raises(RuntimeError, match="scope_mismatch"):
        storage.material_witness_exists(session, series_ids=[1], material_hash="canonical")


def test_typed_witness_requires_exact_legacy_key_not_canonical_hash(monkeypatch):
    storage, session, row, _ = _witness_fixture(monkeypatch, provenance={"_qt_bbo_evidence": {"legacy_material_hash": "legacy"}})
    assert storage.material_witness_exists(session, series_ids=[1], material_hash="legacy", evidence_key="_qt_bbo_evidence", include_canonical=False)
    for material, key in [("canonical", "_qt_bbo_evidence"), ("legacy", "_qt_other_evidence")]:
        with pytest.raises(RuntimeError, match="alias_mismatch"):
            storage.material_witness_exists(session, series_ids=[1], material_hash=material, evidence_key=key, include_canonical=False)


def test_unindexed_legacy_witness_streams_cold_rows_and_closes_on_match(monkeypatch, caplog):
    storage, session, row, read = _witness_fixture(monkeypatch, candidate=None, provenance={"custom_old_key": {"legacy_material_hash": "legacy"}})
    closed = []
    @contextmanager
    def stream(*args, **kwargs):
        try:
            yield iter([row])
        finally:
            closed.append(True)
    monkeypatch.setattr(storage, "stream_rows_by_ids", stream)
    assert storage.material_witness_exists(session, series_ids=[1], material_hash="legacy")
    assert closed == [True]
    assert "canonical_material_unindexed_cold_search" in caplog.text
    assert not storage.material_witness_exists(session, series_ids=[1], material_hash="absent")
    read.assert_not_called()


def test_empty_witness_scope_does_not_query():
    session = MagicMock()
    assert not PostgresCanonicalFactStorageRepository().material_witness_exists(session, series_ids=[], material_hash="anything")
    session.execute.assert_not_called()

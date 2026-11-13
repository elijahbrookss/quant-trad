"""Behavioral ordering at canonical publication boundaries; no database required."""
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from portal.backend.service.storage.repos import (
    market_data, market_lifecycle, market_structure, normalization,
)


class BoundaryReached(Exception):
    pass


class Result:
    def __init__(self, row=None):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row

    def one(self):
        return self.row


class Session:
    def __init__(self):
        self.admitted = False
        self.calls = []
        self.rows = []

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "pg_advisory_xact_lock_shared" in sql:
            assert parameters == {"name": "quant-trad:market-storage-lifecycle:v1"}
            assert "hashtextextended" in sql
            self.admitted = True
            return Result()
        return Result(self.rows.pop(0) if self.rows else None)


@pytest.fixture
def session(monkeypatch):
    session = Session()

    @contextmanager
    def transaction():
        yield session

    database = SimpleNamespace(session=transaction)
    for module in (market_structure, normalization):
        monkeypatch.setattr(module, "db", database)
    return session


def _stop_after_admission(session, *args, **kwargs):
    assert session.admitted, "source/lease access occurred before lifecycle admission"
    raise BoundaryReached


def test_common_canonical_writer_admits_before_collection_lease_or_source_reads(session, monkeypatch):
    repo = market_data.PostgresMarketDataRepository()
    monkeypatch.setattr(repo, "_assert_collection_fence", _stop_after_admission)
    with pytest.raises(BoundaryReached):
        repo._ingest_canonical_rows_with_session(
            session, run_id="run", series_id=1, rows=(), allow_corrections=True,
            collection_fence={"fence_kind": "stream"},
        )
    assert "pg_advisory_xact_lock_shared" in session.calls[0][0]
    assert "pg_advisory_xact_lock(:series_id)" in session.calls[1][0]


@pytest.mark.parametrize("lane", ["trade", "book"])
def test_stream_publication_admits_before_raw_holder_and_lease_fence(session, monkeypatch, lane):
    repo = market_structure.PostgresMarketStructureRepository()
    monkeypatch.setattr(repo, "_require_fence", _stop_after_admission)
    claim = SimpleNamespace(source_id=1, definition_id="definition", session_id="session")
    monkeypatch.setattr(market_structure.market_data_repo, "get_source_identity", lambda *_: object())
    with pytest.raises(BoundaryReached):
        if lane == "trade":
            from tests.test_market_data.test_market_structure_phase1 import _fact
            fact, _raw = _fact()
            repo.ingest_trades(claim, facts=[fact])
        else:
            repo.ingest_book_facts(
                claim, snapshots=(), batches=(), validity_versions=(), lifecycle=None,
                final_validity_interval_id=None, checkpoint_id=None, final_state_hash=None,
                final_connection_epoch=1, final_receive_ordinal=1,
                final_event_ordinal=1, final_sequence_num=None,
            )
    assert len(session.calls) == 1


def test_derived_feature_admits_before_canonical_source_witness(session, monkeypatch):
    monkeypatch.setattr(market_structure.market_data_repo, "register_source", lambda *a, **k: 1)
    monkeypatch.setattr(market_structure, "canonicalize_bbo_feature", lambda *a, **k: object())
    monkeypatch.setattr(market_structure, "_require_book_state_source", _stop_after_admission)
    fact = SimpleNamespace(
        material_hash="material", source_l2_series_id=1,
        source_position=SimpleNamespace(material=lambda: {}),
        validity_interval_id="interval", source_state_hash="state",
    )
    with pytest.raises(BoundaryReached):
        market_structure.PostgresMarketStructureRepository().ingest_market_state_features(bbo_facts=[fact])
    assert len(session.calls) == 1


def test_normalization_admits_before_source_witness_or_hot_payload_read(session, monkeypatch):
    monkeypatch.setattr(normalization.market_data_repo, "register_source", lambda *a, **k: 1)
    spec = SimpleNamespace(output_fact_type="normalized", spec_hash="spec-hash")
    monkeypatch.setattr(normalization, "_spec_from_row", lambda _: spec)
    monkeypatch.setattr(normalization, "_ensure_payload_schema", lambda *_: SimpleNamespace(schema_id="schema"))
    monkeypatch.setattr(normalization.canonical_fact_storage_repository, "material_witness_exists", _stop_after_admission)
    session.rows = [{"id": "spec"}, {"fact_type": "normalized", "contract_version": "schema"}]
    now = datetime(2026, 1, 1, tzinfo=UTC)
    fact = SimpleNamespace(
        effective_at=now, known_at=now, material_hash="material", series_id=1,
        spec_id="spec", spec_hash="spec-hash", source_material_hashes=("source",), source_series_ids=(2,),
    )
    with pytest.raises(BoundaryReached):
        normalization.PostgresNormalizationRepository().ingest([fact])
    assert "pg_advisory_xact_lock_shared" in session.calls[0][0]


def test_dataset_pin_and_publication_use_the_same_transaction_fence(session):
    market_lifecycle.PostgresMarketStorageLifecycleRepository.acquire_dataset_pin_lock(session)
    market_lifecycle.PostgresMarketStorageLifecycleRepository.acquire_publication_lock(session)
    assert session.calls[0] == session.calls[1]


def test_heartbeat_does_not_wait_on_publication_admission(session, monkeypatch):
    repo = market_structure.PostgresMarketStructureRepository()
    monkeypatch.setattr(repo, "_require_fence", lambda *a, **k: None)
    expires = datetime(2026, 1, 1, tzinfo=UTC)
    session.rows = [(expires,)]
    assert repo.heartbeat(SimpleNamespace(definition_id="definition"), lease_seconds=60) == expires
    assert len(session.calls) == 1
    assert "UPDATE market.stream_lease_state" in session.calls[0][0]
    assert not session.admitted


def test_stale_stream_lease_still_rejects_after_admission_without_mutation(session, monkeypatch):
    from portal.backend.service.storage.repos import fact_references
    monkeypatch.setattr(fact_references, "lock_stream_raw_mapping_access", lambda s: _assert_admitted(s))
    market_lifecycle.PostgresMarketStorageLifecycleRepository.acquire_publication_lock(session)
    with pytest.raises(market_structure.MarketStructureOwnershipError, match="stale worker mutation rejected"):
        market_structure.PostgresMarketStructureRepository._require_fence(
            session, SimpleNamespace(definition_id="stale"), raw_mapping_access=True,
        )
    assert "FOR UPDATE OF leases" in session.calls[-1][0]
    assert not any("INSERT " in sql or "UPDATE market." in sql for sql, _ in session.calls)


def _assert_admitted(session):
    assert session.admitted

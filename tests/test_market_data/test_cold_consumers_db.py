from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text

from market_data.normalization import NormalizationFormula, NormalizationInput, NormalizationSpec, evaluate_normalization
from portal.backend.db import MarketNormalizationSpecRecord
from portal.backend.service.storage.repos import collector_operations, market_data, market_structure, normalization
from tests.test_market_data.test_fact_storage_tiers_db import (
    BASE, _ingest, _placement, _read, _verified_cold_fixture, storage,
)
from tests.test_market_data.test_fact_reclamation_db import _prepare

pytestmark = pytest.mark.db


def test_collector_recent_facts_survive_physical_reclamation(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(collector_operations, "db", storage.database)
    day, archive, reclaimer = _prepare(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(collector_operations, "canonical_fact_storage_repository", market_data.canonical_fact_storage_repository)
    operations = collector_operations.PostgresCollectorOperationsRepository()
    before = operations.recent_facts(series_ids=[storage.series_id], limit=1)
    assert len(before) == 1
    reclaimer.reclaim_partition(day, eligible_before=storage.today, execute=True)
    assert operations.recent_facts(series_ids=[storage.series_id], limit=1) == before


def _normalization_spec():
    return NormalizationSpec(
        feature_name="cold_funding", semantic_version="1.0.0",
        input_fact_type="derivatives.funding_rate", output_fact_type="market.normalized.cold_funding",
        formula=NormalizationFormula.BASIS_POINTS, units="bps",
        window_seconds=None, minimum_observations=0, warmup_observations=0,
    )


def test_normalization_source_noop_and_causal_guards_survive_cooling(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(normalization, "db", storage.database)
    repo = normalization.PostgresNormalizationRepository()
    spec = repo.register_spec(_normalization_spec())
    output_series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type=spec.output_fact_type,
        timeframe_seconds=None, contract_version=f"market.normalized_feature.v1/{spec.spec_id}",
    )
    _ingest(storage)
    _ingest(storage, replace(storage.fact, payload={**storage.fact.payload, "rate": "0.2", "raw_rate": "0.2"},
                            accepted_at=BASE + timedelta(seconds=2), known_at=BASE + timedelta(seconds=2)))
    source = _read(storage)[0]
    evidence = NormalizationInput(
        source_series_id=storage.series_id, effective_at=BASE, known_at=source.fact.known_at,
        market_commit_seq=source.market_commit_seq, material_hash=source.fact.material_hash,
        value=Decimal(source.fact.payload["rate"]),
    )
    fact = evaluate_normalization(spec, [evidence], output_series_id=output_series)[0]
    assert repo.ingest([fact]).inserted_count == 1
    before = repo.read_records(series_id=output_series, start=BASE, end=BASE + timedelta(days=1))

    def check_guards():
        assert repo.list_specs() == (spec,)
        assert repo.ingest([fact]).noop_count == 1
        assert repo.read_records(series_id=output_series, start=BASE, end=BASE + timedelta(days=1)) == before
        for stale in (replace(fact, input_watermark=fact.input_watermark - 1),
                      replace(fact, known_at=fact.known_at - timedelta(seconds=1))):
            with pytest.raises(RuntimeError, match="market_normalized_ingest_stale"):
                repo.ingest([stale])
        with pytest.raises(RuntimeError, match="one evidence watermark"):
            repo.ingest([replace(fact, input_fingerprint="b" * 64)])
        with pytest.raises(RuntimeError, match="identical inputs"):
            repo.ingest([replace(fact, value=Decimal("999"))])
        with pytest.raises(ValueError, match="canonical source witness is missing"):
            repo.ingest([replace(fact, source_material_hashes=("c" * 64,))])

    check_guards()
    archive_path = _verified_cold_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(normalization, "canonical_fact_storage_repository", market_data.canonical_fact_storage_repository)
    check_guards()
    # A server-side ID cursor must permit nested hydration queries on this
    # same connection and preserve its selected order over multiple batches.
    with storage.database.session() as session:
        expected = session.execute(text("SELECT id FROM market.fact_versions ORDER BY market_commit_seq DESC")).scalars().all()
        with market_data.canonical_fact_storage_repository.stream_rows_by_ids(
            session, text("SELECT id FROM market.fact_versions ORDER BY market_commit_seq DESC"), batch_size=1,
        ) as rows:
            assert [row["id"] for row in rows] == expected
    archive_path.write_bytes(b"corrupt test archive")
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        repo.ingest([fact])


def test_legacy_witness_and_referenced_spec_guard_survive_cooling(storage, tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(normalization, "db", storage.database)
    repo = normalization.PostgresNormalizationRepository()
    spec = _normalization_spec()
    legacy_id = "nsp_" + spec.spec_hash[:40]
    material = {key: value for key, value in spec.material().items() if key != "schema_version"}
    with storage.database.session() as session:
        session.add(MarketNormalizationSpecRecord(id=legacy_id, spec_hash=spec.spec_hash, **material))
    assert repo.list_specs() == ()
    # An external event name alone is not the old provenance-reference test.
    unreferenced = replace(storage.fact, external_event_group_key=legacy_id,
                           provenance={"_qt_normalization_evidence": {"spec_id": ["opaque_not_a_spec"]}})
    _ingest(storage, unreferenced)
    assert repo.list_specs() == ()
    _ingest(storage, replace(unreferenced, provenance={
        "custom_old_key": {"legacy_material_hash": "b" * 64},
        "numeric_old_key": {"legacy_material_hash": int("7" * 64)},
        "_qt_normalization_evidence": {"spec_id": legacy_id},
    }))

    def check_guards():
        with pytest.raises(RuntimeError, match="legacy_identity_referenced"):
            repo.list_specs()
        with storage.database.session() as session:
            reader = market_data.canonical_fact_storage_repository
            assert reader.material_witness_exists(session, series_ids=[storage.series_id], material_hash="b" * 64)
            assert reader.material_witness_exists(session, series_ids=[storage.series_id], material_hash="7" * 64)
            assert not reader.material_witness_exists(session, series_ids=[storage.series_id], material_hash="c" * 64)
            assert not reader.material_witness_exists(session, series_ids=[storage.series_id], material_hash="b" * 64,
                                                      evidence_key="_qt_bbo_evidence", include_canonical=False)

    check_guards()
    _verified_cold_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(normalization, "canonical_fact_storage_repository", market_data.canonical_fact_storage_repository)
    check_guards()
    assert "canonical_material_unindexed_cold_search" in caplog.text
    assert "market_normalization_legacy_reference_scan" in caplog.text


def _persist_reader_fixture(storage, series_id, facts):
    # This fixture exercises canonical read semantics, not fenced stream/raw
    # archive admission. Production L2 ingestion still requires its owner.
    run_id = str(uuid4())
    storage.repo._start_ingestion_run(
        run_id=run_id, source_id=storage.source_id, request={"fixture": "cold_reader"},
        source_revision=None, requested_start=BASE, requested_end=BASE + timedelta(days=1),
        requested_count=len(facts),
    )
    return storage.repo._ingest_canonical_rows(
        run_id=run_id, series_id=series_id, rows=facts, allow_corrections=True,
    )


def test_book_sources_replay_and_trade_flow_status_survive_cooling(storage, tmp_path, monkeypatch):
    from market_data.canonical_adapters import canonicalize_l2_snapshot, canonicalize_trade_flow
    from market_data.market_state import DEPTH_FACT_TYPE, DEPTH_FACT_VERSION
    from market_data.order_book import Level2BookReconstructor
    from market_data.structure import MarketSide
    from tests.test_market_data.test_market_state_phase3 import _aggregate, _trade
    from tests.test_market_data.test_order_book_phase2 import _contract, _snapshot
    from tests.test_market_data.test_structured_dataset_revision_identity_db import _book_feature_revisions

    monkeypatch.setattr(market_structure, "db", storage.database)
    book_series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type="market.l2_book", timeframe_seconds=None,
        contract_version="market.l2_book.v1",
    )
    flow_series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type="market.trade_flow", timeframe_seconds=1,
        contract_version="market.trade_flow.v1",
    )
    bbo_series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type="market.bbo", timeframe_seconds=1,
        contract_version="market.bbo.v1",
    )
    depth_series = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type=DEPTH_FACT_TYPE, timeframe_seconds=1,
        contract_version=DEPTH_FACT_VERSION,
    )
    snapshot = Level2BookReconstructor(series_id=book_series, contract=_contract()).process(_snapshot()).snapshot
    book = canonicalize_l2_snapshot(snapshot, source=storage.fact.source)
    _persist_reader_fixture(storage, book_series, [book])
    aggregate = _aggregate((_trade("one", offset="0.1", side=MarketSide.BUY, price="100", receive_ordinal=1),))
    flow = canonicalize_trade_flow(aggregate, source=storage.fact.source)
    _persist_reader_fixture(storage, flow_series, [flow])
    incomplete = replace(aggregate, aggregate_complete=False, known_at=aggregate.known_at + timedelta(seconds=1))
    _persist_reader_fixture(storage, flow_series, [canonicalize_trade_flow(incomplete, source=storage.fact.source)])
    position = book.provenance["_qt_l2_evidence"]
    bbo, _ = _book_feature_revisions(
        source=storage.fact.source, l2_series_id=book_series, bbo_series_id=bbo_series,
        depth_series_id=depth_series, definition_id=position["definition_id"],
        session_id=position["session_id"], receive_ordinal=1,
    )
    _persist_reader_fixture(storage, bbo_series, [bbo])
    revised_bbo, _ = _book_feature_revisions(
        source=storage.fact.source, l2_series_id=book_series, bbo_series_id=bbo_series,
        depth_series_id=depth_series, definition_id=position["definition_id"],
        session_id=position["session_id"], receive_ordinal=2,
    )
    _persist_reader_fixture(storage, bbo_series, [revised_bbo])
    legacy_bbo_hash = bbo.provenance["_qt_bbo_evidence"]["legacy_material_hash"]
    assert legacy_bbo_hash != bbo.material_hash
    repo = market_structure.PostgresMarketStructureRepository()

    def check_sources():
        with storage.database.session() as session:
            market_structure._require_book_state_source(
                session, series_id=book_series, position=position,
                validity_interval_id=snapshot.validity_interval_id, state_hash=snapshot.state_hash,
            )
            for invalid in ({**position, "receive_ordinal": 999}, {**position, "session_id": "other"}):
                with pytest.raises(ValueError, match="acknowledged book state is missing"):
                    market_structure._require_book_state_source(
                        session, series_id=book_series, position=invalid,
                        validity_interval_id=snapshot.validity_interval_id, state_hash=snapshot.state_hash,
                    )
            with pytest.raises(ValueError, match="acknowledged book state is missing"):
                market_structure._require_book_state_source(
                    session, series_id=book_series, position=position,
                    validity_interval_id=snapshot.validity_interval_id, state_hash="0" * 64,
                )
            # Typed source admission can still witness an older revision.
            market_structure._require_canonical_typed_material_source(
                session, series_id=bbo_series, material_hash=legacy_bbo_hash,
                evidence_key="_qt_bbo_evidence",
            )
            with pytest.raises(ValueError, match="canonical typed source material is missing"):
                market_structure._require_canonical_typed_material_source(
                    session, series_id=bbo_series, material_hash=bbo.material_hash,
                    evidence_key="_qt_bbo_evidence",
                )
            return market_structure._trade_flow_status_rows(session, [flow_series])

    before = check_sources()
    assert before[0]["bucket_count"] == 1
    assert before[0]["complete_bucket_count"] == 0
    assert before[0]["incomplete_bucket_count"] == 1
    replay_args = dict(definition_id=position["definition_id"], session_id=position["session_id"],
                       snapshot_ids=[snapshot.snapshot_id], batch_ids=[], final_state_hash=None)
    replay_before = repo.reconcile_book_replay(**replay_args)
    _verified_cold_fixture(storage, tmp_path, monkeypatch)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", market_data.canonical_fact_storage_repository)
    assert check_sources() == before
    assert repo.reconcile_book_replay(**replay_args) == replay_before
    # Recovery fold reads the archived suffix; a repeated fold adds nothing.
    with storage.database.session() as session:
        market_structure._advance_book_fact_rollup(session, series_id=book_series, expected_new_fact_count=1)
        market_structure._advance_book_fact_rollup(session, series_id=book_series, expected_new_fact_count=0)
        assert session.execute(text("SELECT snapshot_count FROM market.book_operational_rollups WHERE series_id=:id"),
                               {"id": book_series}).scalar_one() == 1
    _placement(monkeypatch, storage.today + timedelta(days=1))
    newest = replace(aggregate, known_at=aggregate.known_at + timedelta(seconds=2))
    _persist_reader_fixture(storage, flow_series, [canonicalize_trade_flow(newest, source=storage.fact.source)])
    after = check_sources()[0]
    assert (after["bucket_count"], after["complete_bucket_count"], after["incomplete_bucket_count"]) == (1, 1, 0)

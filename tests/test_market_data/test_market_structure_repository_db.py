from __future__ import annotations

import gzip
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    publish_compacted_raw_archives,
    publish_spool_archive,
)
from market_data.book_archive import publish_book_checkpoint
from market_data.contracts import DatasetSeriesRequest, SourceIdentity
from market_data.market_state import (
    BBO_FACT_TYPE,
    BBO_FACT_VERSION,
    DEPTH_FACT_TYPE,
    DEPTH_FACT_VERSION,
    TRADE_FLOW_FEATURE_FACT_TYPE,
    TRADE_FLOW_FEATURE_FACT_VERSION,
    MarketStateValuationContract,
    derive_book_features,
    derive_trade_flow_feature,
)
from market_data.normalization import (
    NormalizationFormula,
    NormalizationSpec,
)
from market_data.order_book import (
    BookLifecycle,
    L2ProductContract,
    Level2BookReconstructor,
    translate_coinbase_l2_event,
)
from market_data.structure import (
    ArchiveStatus,
    CoverageStatus,
    OrderingAssurance,
    PHASE1_COINBASE_TRADE_CONTRACTS,
    ProductTradeContract,
    RawStreamRecord,
    TradeCoverageIntervalVersion,
    aggregate_trade_bucket,
    bucket_start_for,
    translate_coinbase_market_trade,
)
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market.normalization_service import market_normalization_service
from portal.backend.service.storage.repos.market_data import market_data_repo
from portal.backend.service.storage.repos.normalization import (
    normalization_repository,
)
from portal.backend.service.storage.repos.market_structure import (
    MarketStructureOwnershipError,
    market_structure_repository,
)


pytestmark = pytest.mark.db

FIXTURE_PATH = (
    Path(__file__).parents[1]
    / "fixtures/providers/coinbase/market_structure_phase0/raw_frames.json.gz"
)


def _btc_update_frame() -> str:
    with gzip.open(FIXTURE_PATH, "rt", encoding="utf-8") as handle:
        frames = json.load(handle)["frames"]
    for row in frames:
        payload = json.loads(row["raw_frame"])
        if payload.get("channel") != "market_trades":
            continue
        events = payload.get("events") or []
        trades = events[0].get("trades") if events else []
        if (
            events
            and events[0].get("type") == "update"
            and trades
            and trades[0].get("product_id") == "BTC-USD"
        ):
            return row["raw_frame"]
    raise AssertionError("BTC update fixture missing")


def _btc_l2_frames() -> list[str]:
    with gzip.open(FIXTURE_PATH, "rt", encoding="utf-8") as handle:
        frames = json.load(handle)["frames"]
    selected: list[str] = []
    observed_types: set[str] = set()
    for row in frames:
        payload = json.loads(row["raw_frame"])
        if payload.get("channel") != "l2_data":
            continue
        for event in payload.get("events") or []:
            if event.get("product_id") != "BTC-USD":
                continue
            event_type = str(event.get("type") or "")
            if event_type in {"snapshot", "update"} and event_type not in observed_types:
                selected.append(row["raw_frame"])
                observed_types.add(event_type)
                break
        if observed_types == {"snapshot", "update"}:
            return selected
    raise AssertionError("BTC Level 2 snapshot/update fixtures missing")


def test_continuous_validation_evidence_reports_active_elapsed_time() -> None:
    token = uuid.uuid4().hex
    instrument_id = f"ms-evidence-{token[:20]}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol=f"EVIDENCE-{token[:8].upper()}",
                instrument_type="spot",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={},
            )
        )
    source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="stream",
            adapter_version=f"continuous-evidence-db-test.{token}",
        )
    )
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.trade",
        timeframe_seconds=None,
        contract_version="market.trade.v1",
    )
    definition_id = f"msevidence_{token}"
    market_structure_repository.upsert_stream_definition(
        definition_id=definition_id,
        source_id=source_id,
        series_id=series_id,
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        provider_product_id="BTC-USD",
        channels=("market_trades", "heartbeats"),
        auth_mode="public",
        contract_version="market.trade.v1",
        max_spool_bytes=1024**3,
        max_segment_bytes=128 * 1024**2,
        config={},
    )
    claim = market_structure_repository.claim_stream(
        definition_id=definition_id,
        owner_id="continuous-evidence-db-test",
        lease_seconds=120,
        bounded=True,
    )
    try:
        market_structure_repository.append_session_event(
            claim,
            event_ordinal=0,
            connection_epoch=0,
            event_type="connected",
            occurred_at=datetime.now(UTC) - timedelta(seconds=65),
        )
        evidence = market_structure_repository.continuous_validation_evidence(
            definition_id=definition_id,
            session_id=claim.session_id,
        )
        assert evidence["session_active"] is True
        assert 60 <= evidence["duration_seconds"] < 120
        assert "validation_session_still_active" in evidence["blockers"]
        assert (
            "graceful_terminal_event_missing_or_duplicated"
            not in evidence["blockers"]
        )
        assert "coverage_interval_still_open" not in evidence["blockers"]
    finally:
        market_structure_repository.release(claim)


def test_phase1_archive_trade_coverage_and_aggregate_are_fenced_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    token = uuid.uuid4().hex
    instrument_id = f"ms-db-{token[:24]}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol=f"BTC-{token[:8].upper()}",
                instrument_type="spot",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={},
            )
        )
    source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="stream",
            adapter_version=f"market-structure-db-test.{token}",
        )
    )
    trade_series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.trade",
        timeframe_seconds=None,
        contract_version="market.trade.v1",
    )
    aggregate_series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.trade_flow",
        timeframe_seconds=1,
        contract_version="market.trade_flow.v1",
    )
    flow_feature_series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type=TRADE_FLOW_FEATURE_FACT_TYPE,
        timeframe_seconds=1,
        contract_version=TRADE_FLOW_FEATURE_FACT_VERSION,
    )
    base_contract = PHASE1_COINBASE_TRADE_CONTRACTS["BTC-USD"]
    product_definition_id = f"coinbase.BTC-USD.db-test.{token}"
    contract = ProductTradeContract(
        provider_product_id=base_contract.provider_product_id,
        provider_size_unit=base_contract.provider_size_unit,
        base_currency=base_contract.base_currency,
        quote_currency=base_contract.quote_currency,
        product_definition_version_id=product_definition_id,
    )
    market_structure_repository.register_product_definition(
        definition_version_id=product_definition_id,
        source_id=source_id,
        instrument_id=instrument_id,
        provider_product_id="BTC-USD",
        product_type="spot",
        venue="COINBASE_DIRECT",
        status="test",
        base_currency="BTC",
        quote_currency="USD",
        provider_size_unit="base",
        contract_size=None,
        price_increment=None,
        base_increment=None,
        effective_at=datetime(2026, 8, 2, tzinfo=UTC),
        received_at=datetime.now(UTC),
        provenance={"fixture": "market_structure_repository_db"},
    )
    definition_id = f"msdb_{token}"
    market_structure_repository.upsert_stream_definition(
        definition_id=definition_id,
        source_id=source_id,
        series_id=trade_series_id,
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        provider_product_id="BTC-USD",
        channels=("market_trades", "heartbeats"),
        auth_mode="public",
        contract_version="market.trade.v1",
        max_spool_bytes=1024**3,
        max_segment_bytes=128 * 1024**2,
        config={"aggregate_series_ids": {"1": aggregate_series_id}},
    )
    claim = market_structure_repository.claim_stream(
        definition_id=definition_id,
        owner_id="market-structure-db-test",
        lease_seconds=120,
        bounded=True,
    )
    opening_event_id = market_structure_repository.append_session_event(
        claim,
        event_ordinal=0,
        connection_epoch=0,
        event_type="connected",
        occurred_at=datetime.now(UTC),
    )

    raw_frame = _btc_update_frame()
    received_at = datetime.now(UTC)
    provider_message = ProviderRawMessage.build(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        stream_session_id=claim.session_id,
        connection_epoch=0,
        receive_ordinal=1,
        received_at=received_at.isoformat(),
        raw_frame=raw_frame,
    )
    spool = DurableRawSpoolSegment(
        root=tmp_path / "spool",
        definition_id=definition_id,
        session_id=claim.session_id,
        connection_epoch=0,
    )
    raw_record = RawStreamRecord.from_provider_message(
        provider_message,
        definition_id=definition_id,
        spool_segment_id=spool.spool_segment_id,
        provider_product_id="BTC-USD",
        requested_channel="market_trades",
        observed_channel="market_trades",
    )
    spool.append(raw_record)
    spool.seal()
    object_store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    encoded, acknowledgement, archived_records = publish_spool_archive(
        spool,
        object_store=object_store,
        temporary_directory=tmp_path / "tmp",
    )
    archive = market_structure_repository.commit_archive(
        claim,
        encoded=encoded,
        acknowledgement=acknowledgement,
        records=archived_records,
    )
    assert archive.mapped_record_count == 1

    parsed = CoinbaseMessageParser().parse_raw(
        raw_frame,
        received_at=received_at.isoformat(),
    )
    event = next(value for value in parsed if value.event_kind == "market_trade")
    coverage_id = f"coverage-{token}"
    fact = translate_coinbase_market_trade(
        event,
        contract=contract,
        raw_record_id=raw_record.raw_record_id,
        connection_epoch=0,
        receive_ordinal=1,
        accepted_at=received_at + timedelta(milliseconds=1),
        coverage_interval_id=coverage_id,
    )
    first = market_structure_repository.ingest_trades(claim, facts=[fact])
    repeated = market_structure_repository.ingest_trades(claim, facts=[fact])
    assert first.inserted_count == 1
    assert repeated.noop_count == 1
    assert market_structure_repository.read_trades(
        series_id=trade_series_id,
        start=fact.provider_event_time - timedelta(seconds=1),
        end=fact.provider_event_time + timedelta(seconds=1),
    )[0].fact.material_hash == fact.material_hash

    bucket = bucket_start_for(fact.provider_event_time, interval_seconds=1)
    coverage = TradeCoverageIntervalVersion(
        interval_id=coverage_id,
        revision=1,
        definition_id=definition_id,
        session_id=claim.session_id,
        connection_epoch=0,
        provider_product_id="BTC-USD",
        channel="market_trades",
        status=CoverageStatus.CLOSED_VALID,
        ordering_assurance=OrderingAssurance.PROVIDER_SEQUENCE_CONTIGUOUS,
        archive_status=ArchiveStatus.COMPLETE,
        opening_raw_record_id=raw_record.raw_record_id,
        opening_receive_ordinal=1,
        opening_effective_at=bucket - timedelta(seconds=1),
        last_raw_record_id=raw_record.raw_record_id,
        last_receive_ordinal=1,
        last_effective_at=bucket + timedelta(seconds=2),
        closing_raw_record_id=raw_record.raw_record_id,
        closing_receive_ordinal=1,
        closing_effective_at=bucket + timedelta(seconds=2),
        canonicalization_watermark_ordinal=1,
        archive_complete_through_ordinal=1,
        known_at=received_at + timedelta(seconds=1),
        first_provider_sequence_num=fact.provider_sequence_num,
        last_provider_sequence_num=fact.provider_sequence_num,
    )
    closing_event_id = market_structure_repository.append_session_event(
        claim,
        event_ordinal=1,
        connection_epoch=0,
        event_type="bounded_capture_stopped",
        occurred_at=datetime.now(UTC),
    )
    market_structure_repository.append_coverage_version(
        claim,
        coverage=coverage,
        opening_session_event_id=opening_event_id,
        closing_session_event_id=closing_event_id,
    )
    frozen_coverage = market_structure_repository.get_coverage_version(
        interval_id=coverage.interval_id,
        revision=coverage.revision,
    )
    assert frozen_coverage == coverage
    assert frozen_coverage.material_hash == coverage.material_hash
    with pytest.raises(ValueError, match="market_stream_coverage_unknown"):
        market_structure_repository.get_coverage_version(
            interval_id=coverage.interval_id,
            revision=coverage.revision + 1,
        )
    aggregate = aggregate_trade_bucket(
        [fact],
        interval_seconds=1,
        bucket_start=bucket,
        coverage=coverage,
        computed_at=max(datetime.now(UTC), bucket + timedelta(seconds=1)),
    )
    aggregate_outcome = market_structure_repository.ingest_aggregates(
        series_id=aggregate_series_id,
        facts=[aggregate],
    )
    assert aggregate_outcome.inserted_count == 1
    stored_aggregate = market_structure_repository.read_aggregates(
        series_id=aggregate_series_id,
        interval_seconds=1,
        start=bucket,
        end=bucket + timedelta(seconds=1),
    )
    assert stored_aggregate[0].fact.material_hash == aggregate.material_hash

    flow_feature = derive_trade_flow_feature(
        series_id=flow_feature_series_id,
        source_trade_flow_series_id=aggregate_series_id,
        aggregate=aggregate,
        trades=[fact],
        computed_at=max(datetime.now(UTC), aggregate.bucket_end),
    )
    assert flow_feature is not None
    first_feature = market_structure_repository.ingest_market_state_features(
        flow_facts=[flow_feature]
    )
    repeated_feature = market_structure_repository.ingest_market_state_features(
        flow_facts=[flow_feature]
    )
    assert first_feature.inserted_count == 1
    assert repeated_feature.noop_count == 1
    stored_flow = market_structure_repository.read_trade_flow_features(
        series_id=flow_feature_series_id,
        start=bucket,
        end=bucket + timedelta(seconds=1),
        known_at=flow_feature.known_at + timedelta(microseconds=1),
    )
    assert stored_flow[0].material_hash == flow_feature.material_hash

    aggressive_spec = NormalizationSpec(
        feature_name=f"aggressive_buy_share_db_{token[:8]}",
        semantic_version="1.0.0",
        input_fact_type=TRADE_FLOW_FEATURE_FACT_TYPE,
        output_fact_type=f"market.normalized.aggressive_buy_share_db_{token[:8]}",
        formula=NormalizationFormula.RATIO,
        units="unit_interval",
        window_seconds=None,
        minimum_observations=0,
        warmup_observations=0,
        parameters={
            "value_field": "aggressor_buy_notional",
            "denominator_field": "quote_notional",
            "input_step_seconds": 1,
        },
    )
    aggressive_spec = normalization_repository.register_spec(
        aggressive_spec,
        created_by="pytest",
        approved_by="pytest",
    )
    normalization = market_normalization_service.compare_persisted(
        spec_id=aggressive_spec.spec_id,
        source_series_id=flow_feature_series_id,
        start=bucket,
        end=bucket + timedelta(seconds=1),
        known_at=max(flow_feature.known_at, bucket + timedelta(seconds=1)),
    )
    assert normalization["persisted_equal"] is True
    assert normalization["provider_call_performed"] is False
    assert normalization["statuses"] == {"valid": 1}
    normalized_series_id = int(normalization["output_series_id"])
    normalized_before = normalization_repository.read_records(
        series_id=normalized_series_id,
        start=bucket,
        end=bucket + timedelta(seconds=1),
    )
    assert len(normalized_before) == 1
    assert normalized_before[0].fact.source_series_ids == (flow_feature_series_id,)

    requests = [
        DatasetSeriesRequest(
            series_id=trade_series_id,
            start=fact.provider_event_time - timedelta(microseconds=1),
            end=fact.provider_event_time + timedelta(microseconds=1),
        ),
        DatasetSeriesRequest(
            series_id=aggregate_series_id,
            start=bucket,
            end=bucket + timedelta(seconds=1),
        ),
        DatasetSeriesRequest(
            series_id=flow_feature_series_id,
            start=bucket,
            end=bucket + timedelta(seconds=1),
        ),
        DatasetSeriesRequest(
            series_id=normalized_series_id,
            start=bucket,
            end=bucket + timedelta(seconds=1),
        ),
    ]
    frozen = market_data_repo.freeze_dataset(
        requests,
        name="phase1-provider-free-db-test",
        purpose="test",
        created_by="pytest",
    )
    repeated_frozen = market_data_repo.freeze_dataset(
        requests,
        name="ignored-on-content-reuse",
        purpose="test",
        created_by="pytest",
    )
    assert repeated_frozen.dataset_id == frozen.dataset_id
    assert repeated_frozen.dataset_hash == frozen.dataset_hash
    assert repeated_frozen.reused_existing is True
    frozen_trades = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=trade_series_id,
    )
    frozen_aggregates = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=aggregate_series_id,
    )
    assert frozen_trades[0].fact.row_hash == fact.row_hash
    assert frozen_aggregates[0].fact.material_hash == aggregate.material_hash
    frozen_normalized = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=normalized_series_id,
    )
    assert [row.fact.material_hash for row in frozen_normalized] == [
        row.fact.material_hash for row in normalized_before
    ]
    normalization_refs = frozen.metadata["normalization_refs"]
    assert len(normalization_refs) == 1
    normalization_ref = normalization_refs[0]
    assert normalization_ref["source_series_ids"] == [flow_feature_series_id]
    source_manifest = next(
        row for row in frozen.series if int(row["series_id"]) == flow_feature_series_id
    )
    assert normalization_ref["source_dataset_fingerprints"] == {

        str(flow_feature_series_id): source_manifest["material_hash"]
    }
    assert normalization_ref["input_fingerprint"]

    second_message = ProviderRawMessage.build(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        stream_session_id=claim.session_id,
        connection_epoch=0,
        receive_ordinal=2,
        received_at=(received_at + timedelta(microseconds=1)).isoformat(),
        raw_frame=raw_frame,
    )
    second_spool = DurableRawSpoolSegment(
        root=tmp_path / "spool",
        definition_id=definition_id,
        session_id=claim.session_id,
        connection_epoch=0,
        segment_ordinal=1,
    )
    second_record = RawStreamRecord.from_provider_message(
        second_message,
        definition_id=definition_id,
        spool_segment_id=second_spool.spool_segment_id,
        provider_product_id="BTC-USD",
        requested_channel="market_trades",
        observed_channel="market_trades",
    )
    second_spool.append(second_record)
    second_spool.seal()
    second_encoded, second_ack, second_records = publish_spool_archive(
        second_spool,
        object_store=object_store,
        temporary_directory=tmp_path / "tmp",
    )
    second_archive = market_structure_repository.commit_archive(
        claim,
        encoded=second_encoded,
        acknowledgement=second_ack,
        records=second_records,
    )

    compacted_encoded, compacted_ack, compacted_records = publish_compacted_raw_archives(
        [
            object_store.local_path(acknowledgement.object_key),
            object_store.local_path(second_ack.object_key),
        ],
        object_store=object_store,
        temporary_directory=tmp_path / "tmp",
    )
    compacted = market_structure_repository.commit_archive(
        claim,
        encoded=compacted_encoded,
        acknowledgement=compacted_ack,
        records=compacted_records,
        compaction_source_manifest_ids=[
            archive.manifest_id,
            second_archive.manifest_id,
        ],
    )
    assert compacted.manifest_id != archive.manifest_id
    after_compaction = market_data_repo.get_dataset(frozen.dataset_id)
    replayed_after_compaction = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=normalized_series_id,
    )
    assert after_compaction.dataset_hash == frozen.dataset_hash
    assert [row.fact.material_hash for row in replayed_after_compaction] == [
        row.fact.material_hash for row in frozen_normalized
    ]

    with db.session() as session:
        archive_ref_count = session.execute(
            text(
                "SELECT count(*) FROM market.dataset_archive_refs WHERE dataset_id = :dataset_id"
            ),
            {"dataset_id": frozen.dataset_id},
        ).scalar_one()
    assert archive_ref_count == 1
    retention = market_structure_repository.archive_retention_status(
        target_kind="raw_manifest",
        target_id=archive.manifest_id,
    )
    assert retention["dataset_pin_count"] == 1
    assert retention["pinned"] is True
    assert retention["ordinary_retention_eligible"] is False

    market_structure_repository.release(claim)
    with pytest.raises(MarketStructureOwnershipError, match="ownership_lost"):
        market_structure_repository.heartbeat(claim, lease_seconds=120)


def test_phase2_book_archive_validity_checkpoint_and_replay_are_atomic(
    tmp_path: Path,
) -> None:
    token = uuid.uuid4().hex
    instrument_id = f"ms-l2-db-{token[:21]}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol=f"BTC-L2-{token[:8].upper()}",
                instrument_type="spot",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={},
            )
        )
    source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="stream",
            adapter_version=f"market-structure-l2-db-test.{token}",
        )
    )
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.l2_book",
        timeframe_seconds=None,
        contract_version="market.l2_book.v1",
    )
    bbo_series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type=BBO_FACT_TYPE,
        timeframe_seconds=1,
        contract_version=BBO_FACT_VERSION,
    )
    depth_series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type=DEPTH_FACT_TYPE,
        timeframe_seconds=1,
        contract_version=DEPTH_FACT_VERSION,
    )
    product_definition_id = f"coinbase.BTC-USD.l2-db-test.{token}"
    market_structure_repository.register_product_definition(
        definition_version_id=product_definition_id,
        source_id=source_id,
        instrument_id=instrument_id,
        provider_product_id="BTC-USD",
        product_type="spot",
        venue="COINBASE_DIRECT",
        status="test",
        base_currency="BTC",
        quote_currency="USD",
        provider_size_unit="base",
        contract_size=None,
        price_increment=Decimal("0.01"),
        base_increment=Decimal("0.00000001"),
        effective_at=datetime(2026, 8, 2, tzinfo=UTC),
        received_at=datetime.now(UTC),
        provenance={"fixture": "market_structure_repository_db_l2"},
    )
    definition_id = f"msl2db_{token}"
    market_structure_repository.upsert_stream_definition(
        definition_id=definition_id,
        source_id=source_id,
        series_id=series_id,
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        provider_product_id="BTC-USD",
        channels=("level2", "heartbeats"),
        auth_mode="public",
        contract_version="market.l2_book.v1",
        max_spool_bytes=1024**3,
        max_segment_bytes=128 * 1024**2,
        config={"product_definition_version_id": product_definition_id},
    )
    claim = market_structure_repository.claim_stream(
        definition_id=definition_id,
        owner_id="market-structure-l2-db-test",
        lease_seconds=600,
        bounded=True,
    )

    parser = CoinbaseMessageParser()
    contract = L2ProductContract(
        provider_product_id="BTC-USD",
        product_definition_version_id=product_definition_id,
        provider_size_unit="base",
        price_increment=Decimal("0.01"),
        quantity_increment=Decimal("0.00000001"),
    )
    reducer = Level2BookReconstructor(series_id=series_id, contract=contract)
    raw_records = []
    snapshots = []
    batches = []
    validity_versions = []
    checkpoints = []
    states = []
    spools = []
    last_fact = None
    for ordinal, raw_frame in enumerate(_btc_l2_frames(), start=1):
        spool = DurableRawSpoolSegment(
            root=tmp_path / "spool",
            definition_id=definition_id,
            session_id=claim.session_id,
            connection_epoch=0,
            segment_ordinal=ordinal - 1,
        )
        received_at = datetime.now(UTC) + timedelta(milliseconds=ordinal)
        message = ProviderRawMessage.build(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            stream_session_id=claim.session_id,
            connection_epoch=0,
            receive_ordinal=ordinal,
            received_at=received_at.isoformat(),
            raw_frame=raw_frame,
        )
        raw = RawStreamRecord.from_provider_message(
            message,
            definition_id=definition_id,
            spool_segment_id=spool.spool_segment_id,
            provider_product_id="BTC-USD",
            requested_channel="level2",
            observed_channel="level2",
        )
        spool.append(raw)
        spool.seal()
        spools.append(spool)
        raw_records.append(raw)
        events = parser.parse_raw(raw_frame, received_at=received_at.isoformat())
        for event in events:
            if event.event_kind not in {"market_l2_snapshot", "market_l2_update"}:
                continue
            fact = translate_coinbase_l2_event(
                event,
                raw_record=raw,
                contract=contract,
                accepted_at=received_at + timedelta(milliseconds=1),
            )
            result = reducer.process(fact)
            if result.snapshot is not None:
                snapshots.append(result.snapshot)
            if result.batch is not None:
                batches.append(result.batch)
            if result.state is not None:
                states.append(result.state)
            validity_versions.extend(result.validity_versions)
            checkpoints.extend(result.checkpoints)
            last_fact = fact
    assert len(snapshots) == len(batches) == len(checkpoints) == 1
    assert last_fact is not None
    final_state_hash = reducer.current_state_hash
    final_interval_id = reducer.current_interval.interval_id
    validity_versions.extend(reducer.close_bounded(at_event=last_fact))
    object_store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    published_sources = [
        publish_spool_archive(
            spool,
            object_store=object_store,
            temporary_directory=tmp_path / "tmp",
        )
        for spool in spools
    ]

    with pytest.raises(ValueError, match="market_l2_archive_incomplete"):
        market_structure_repository.ingest_book_facts(
            claim,
            snapshots=snapshots,
            batches=batches,
            validity_versions=validity_versions,
            lifecycle=BookLifecycle.AWAITING_SNAPSHOT,
            final_validity_interval_id=final_interval_id,
            checkpoint_id=checkpoints[0].checkpoint_id,
            final_state_hash=final_state_hash,
            final_connection_epoch=last_fact.position.connection_epoch,
            final_receive_ordinal=last_fact.position.receive_ordinal,
            final_event_ordinal=last_fact.position.event_ordinal,
            final_sequence_num=last_fact.position.provider_sequence_num,
        )

    source_archives = [
        market_structure_repository.commit_archive(
            claim,
            encoded=encoded,
            acknowledgement=acknowledgement,
            records=archived_records,
        )
        for encoded, acknowledgement, archived_records in published_sources
    ]
    compacted_encoded, compacted_ack, compacted_records = (
        publish_compacted_raw_archives(
            [
                object_store.local_path(acknowledgement.object_key)
                for _encoded, acknowledgement, _records in published_sources
            ],
            object_store=object_store,
            temporary_directory=tmp_path / "tmp",
        )
    )
    compacted_archive = market_structure_repository.commit_archive(
        claim,
        encoded=compacted_encoded,
        acknowledgement=compacted_ack,
        records=compacted_records,
        compaction_source_manifest_ids=[
            archive.manifest_id for archive in source_archives
        ],
    )
    active_manifests = market_structure_repository.list_session_manifests(
        definition_id=definition_id,
        session_id=claim.session_id,
    )
    assert [row["id"] for row in active_manifests] == [
        compacted_archive.manifest_id
    ]
    source_retention = market_structure_repository.archive_retention_status(
        target_kind="raw_manifest",
        target_id=source_archives[0].manifest_id,
    )
    assert source_retention["replacement_manifest_ids"] == [
        compacted_archive.manifest_id
    ]
    assert source_retention["ordinary_retention_eligible"] is True
    active_pin = market_structure_repository.append_archive_retention_pin_version(
        target_kind="raw_manifest",
        target_id=source_archives[0].manifest_id,
        owner_kind="test",
        owner_id=token,
        active=True,
        reason="phase2 compaction safety proof",
    )
    assert market_structure_repository.archive_retention_status(
        target_kind="raw_manifest",
        target_id=source_archives[0].manifest_id,
    )["pinned"] is True
    released_pin = market_structure_repository.append_archive_retention_pin_version(
        target_kind="raw_manifest",
        target_id=source_archives[0].manifest_id,
        owner_kind="test",
        owner_id=token,
        active=False,
        reason="phase2 compaction safety proof",
    )
    assert released_pin != active_pin
    assert market_structure_repository.archive_retention_status(
        target_kind="raw_manifest",
        target_id=source_archives[0].manifest_id,
    )["ordinary_retention_eligible"] is True
    checkpoint_encoded, checkpoint_ack = publish_book_checkpoint(
        checkpoints[0],
        object_store=object_store,
        temporary_directory=tmp_path / "tmp",
    )
    assert market_structure_repository.commit_book_checkpoint(
        claim,
        checkpoint=checkpoints[0],
        encoded=checkpoint_encoded,
        acknowledgement=checkpoint_ack,
        source_manifest_ids=[compacted_archive.manifest_id],
    ) is True
    assert market_structure_repository.commit_book_checkpoint(
        claim,
        checkpoint=checkpoints[0],
        encoded=checkpoint_encoded,
        acknowledgement=checkpoint_ack,
        source_manifest_ids=[compacted_archive.manifest_id],
    ) is False

    ingest_kwargs = {
        "snapshots": snapshots,
        "batches": batches,
        "validity_versions": validity_versions,
        "lifecycle": BookLifecycle.AWAITING_SNAPSHOT,
        "final_validity_interval_id": final_interval_id,
        "checkpoint_id": checkpoints[0].checkpoint_id,
        "final_state_hash": final_state_hash,
        "final_connection_epoch": last_fact.position.connection_epoch,
        "final_receive_ordinal": last_fact.position.receive_ordinal,
        "final_event_ordinal": last_fact.position.event_ordinal,
        "final_sequence_num": last_fact.position.provider_sequence_num,
    }
    first = market_structure_repository.ingest_book_facts(claim, **ingest_kwargs)
    repeated = market_structure_repository.ingest_book_facts(claim, **ingest_kwargs)
    assert first.inserted_snapshot_count == first.inserted_batch_count == 1
    assert first.inserted_validity_count == 2
    assert repeated.noop_snapshot_count == repeated.noop_batch_count == 1
    assert repeated.inserted_validity_count == 0
    valuation = MarketStateValuationContract(
        product_definition_version_id=product_definition_id,
        provider_size_unit="base",
        base_currency="BTC",
        quote_currency="USD",
        contract_size=None,
    )
    computed_at = max(
        datetime.now(UTC),
        max(state.effective_at for state in states) + timedelta(seconds=1),
    )
    bbo_facts, depth_facts = derive_book_features(
        states,
        contract=valuation,
        bbo_series_id=bbo_series_id,
        depth_series_id=depth_series_id,
        computed_at=computed_at,
    )
    assert bbo_facts
    assert len(depth_facts) == len(bbo_facts) * 3
    first_features = market_structure_repository.ingest_market_state_features(
        bbo_facts=bbo_facts,
        depth_facts=depth_facts,
    )
    repeated_features = market_structure_repository.ingest_market_state_features(
        bbo_facts=bbo_facts,
        depth_facts=depth_facts,
    )
    assert first_features.inserted_count == len(bbo_facts) + len(depth_facts)
    assert repeated_features.noop_count == len(bbo_facts) + len(depth_facts)
    feature_start = bbo_facts[0].bucket_start
    feature_end = bbo_facts[-1].bucket_end
    stored_bbo = market_structure_repository.read_bbo_features(
        series_id=bbo_series_id,
        start=feature_start,
        end=feature_end,
        known_at=datetime.now(UTC),
    )
    stored_depth = market_structure_repository.read_depth_features(
        series_id=depth_series_id,
        start=feature_start,
        end=feature_end,
        known_at=datetime.now(UTC),
    )
    assert [row.material_hash for row in stored_bbo] == [
        row.material_hash for row in bbo_facts
    ]
    assert sorted(row.material_hash for row in stored_depth) == sorted(
        row.material_hash for row in depth_facts
    )
    input_watermark = market_structure_repository.cross_stream_input_commit_seq(
        futures_bbo_series_id=bbo_series_id,
        spot_bbo_series_id=bbo_series_id,
        oi_series_id=None,
        funding_series_id=None,
        start=feature_start,
        end=feature_end,
        known_at=datetime.now(UTC),
    )
    assert 0 < input_watermark <= first_features.max_commit_seq
    assert market_data_repo.current_commit_seq() >= first_features.max_commit_seq

    replay = market_structure_repository.reconcile_book_replay(
        definition_id=definition_id,
        session_id=claim.session_id,
        snapshot_ids=[snapshots[0].snapshot_id],
        batch_ids=[batches[0].batch_id],
        final_state_hash=final_state_hash,
    )
    assert replay["equal"] is True
    assert market_structure_repository.list_book_checkpoints(
        definition_id=definition_id,
        session_id=claim.session_id,
    )[0]["state_hash"] == checkpoints[0].state_hash
    with db.session() as session:
        snapshot_level_count = session.execute(
            text(
                "SELECT count(*) FROM market.l2_snapshot_levels "
                "WHERE snapshot_version_id = :snapshot_id"
            ),
            {"snapshot_id": snapshots[0].snapshot_id},
        ).scalar_one()
        mutation_count = session.execute(
            text(
                "SELECT count(*) FROM market.l2_mutations WHERE batch_id = :batch_id"
            ),
            {"batch_id": batches[0].batch_id},
        ).scalar_one()
        reconstruction_epoch = session.execute(
            text(
                "SELECT connection_epoch FROM market.book_reconstruction_state "
                "WHERE series_id = :series_id"
            ),
            {"series_id": claim.series_id},
        ).scalar_one()
    assert snapshot_level_count == len(snapshots[0].bids) + len(snapshots[0].asks)
    assert mutation_count == len(batches[0].event.mutations)
    assert reconstruction_epoch == last_fact.position.connection_epoch
    quality_event_id = market_structure_repository.record_quality_event(
        claim,
        connection_epoch=0,
        receive_ordinal=last_fact.position.receive_ordinal,
        channel="level2",
        classification="unknown_zero_delete",
        reason="fixture proves typed non-invalidating quality evidence",
        detected_at=last_fact.known_at,
        raw_record_id=last_fact.raw_record_id,
        sequence_after=last_fact.position.provider_sequence_num,
        evidence={"unknown_level_count": 1},
    )
    market_structure_repository.link_book_quality_event(
        claim,
        quality_event_id=quality_event_id,
        validity_interval_id=final_interval_id,
        link_role="observed_within",
        known_at=last_fact.known_at,
    )
    status = market_structure_repository.archive_status(definition_id=definition_id)
    assert status["book_reconstruction"]["snapshot_count"] == 1
    assert status["book_reconstruction"]["batch_count"] == 1
    assert status["book_reconstruction"]["checkpoint_count"] == 1
    assert status["quality_counts"]["unknown_zero_delete"] == 1

    market_structure_repository.release(claim)
    with pytest.raises(MarketStructureOwnershipError, match="ownership_lost"):
        market_structure_repository.ingest_book_facts(claim, **ingest_kwargs)

from __future__ import annotations

import gzip
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text

from data_providers.streams.coinbase import CoinbaseMessageParser
from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    publish_spool_archive,
)
from market_data.contracts import DatasetSeriesRequest, SourceIdentity
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
from portal.backend.service.storage.repos.market_data import market_data_repo
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


def test_phase1_archive_trade_coverage_and_aggregate_are_fenced_and_idempotent(
    tmp_path: Path,
) -> None:
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
    encoded, acknowledgement, archived_records = publish_spool_archive(
        spool,
        object_store=FilesystemRawArchiveObjectStore(tmp_path / "objects"),
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
    with db.session() as session:
        archive_ref_count = session.execute(
            text(
                "SELECT count(*) FROM market.dataset_archive_refs WHERE dataset_id = :dataset_id"
            ),
            {"dataset_id": frozen.dataset_id},
        ).scalar_one()
    assert archive_ref_count == 1

    market_structure_repository.release(claim)
    with pytest.raises(MarketStructureOwnershipError, match="ownership_lost"):
        market_structure_repository.heartbeat(claim, lease_seconds=120)

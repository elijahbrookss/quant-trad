from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import runpy

from market_data.contracts import (
    CandleFact,
    FundingRateFact,
    NumericFact,
    OpenInterestFact,
    SourceIdentity,
)
from market_data.structure import MarketTradeFact, TradeFlowAggregateFact


_MIGRATION = runpy.run_path(
    str(
        Path(__file__).resolve().parents[2]
        / "scripts/db/migrate_canonical_fact_data_v1.py"
    )
)
_UTC = timezone.utc


def _source_columns() -> dict[str, object]:
    source = SourceIdentity(
        provider="TEST",
        venue="ISOLATED",
        source_kind="fixture",
        adapter_version="fixture.v1",
    )
    return {
        "source_id": 11,
        "source_identity_key": source.identity_key,
        "source_provider": source.provider,
        "source_venue": source.venue,
        "source_kind": source.source_kind,
        "source_adapter_version": source.adapter_version,
    }


def _envelope() -> dict[str, object]:
    return {
        "series_id": 21,
        "revision": 2,
        "market_commit_seq": 31,
        "ingestion_run_id": "migration-fixture",
        "source_published_at": None,
        "received_at": datetime(2026, 8, 9, 12, 0, 1, tzinfo=_UTC),
        "accepted_at": datetime(2026, 8, 9, 12, 0, 2, tzinfo=_UTC),
        "known_at": datetime(2026, 8, 9, 12, 0, 2, tzinfo=_UTC),
        "known_at_method": "platform_acceptance",
        "provenance": {"external_object": "fixture-1"},
        **_source_columns(),
    }


def _assert_preserved(result, legacy_hash: str, schema_id: str) -> None:
    values = result.values
    assert result.source_row_hash == legacy_hash
    assert values["row_hash"] == legacy_hash
    assert values["payload_schema_id"] == schema_id
    assert values["series_id"] == 21
    assert values["revision"] == 2
    assert values["market_commit_seq"] == 31
    assert values["source_id"] == 11
    assert len(str(values["id"])) == 44
    provenance = json.loads(str(values["provenance"]))
    assert provenance["external_object"] == "fixture-1"
    assert provenance["_qt_migration"]["source_table"].startswith("market.")


def test_candle_migration_preserves_v1_evidence_identity() -> None:
    row = _envelope()
    open_time = datetime(2026, 8, 9, 11, 59, tzinfo=_UTC)
    fact = CandleFact(
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=100.0,
        high=103.0,
        low=99.0,
        close=102.0,
        volume=4.5,
        trade_count=7,
        source_published_at=row["source_published_at"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
    )
    row.update(
        {
            "candle_open_time": fact.open_time,
            "candle_close_time": fact.close_time,
            "open": fact.open,
            "high": fact.high,
            "low": fact.low,
            "close": fact.close,
            "volume": fact.volume,
            "trade_count": fact.trade_count,
            "row_hash": fact.row_hash,
        }
    )

    result = _MIGRATION["_candle"](row)

    _assert_preserved(result, fact.row_hash, "candle.ohlcv.v1")
    payload = json.loads(str(result.values["payload"]))
    assert payload["open"] == "100.0"
    assert payload["close_time"] == "2026-08-09T12:00:00.000000Z"


def test_open_interest_and_funding_migrations_preserve_v1_hashes() -> None:
    oi_row = _envelope()
    oi_fact = OpenInterestFact(
        sample_time=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        value=1234.5,
        unit="contracts",
        sample_time_method="collector_schedule",
        source_published_at=oi_row["source_published_at"],
        received_at=oi_row["received_at"],
        accepted_at=oi_row["accepted_at"],
        known_at=oi_row["known_at"],
        known_at_method=oi_row["known_at_method"],
    )
    oi_row.update(
        {
            "sample_time": oi_fact.sample_time,
            "open_interest": oi_fact.value,
            "unit": oi_fact.unit,
            "sample_time_method": oi_fact.sample_time_method,
            "row_hash": oi_fact.row_hash,
        }
    )
    oi_result = _MIGRATION["_open_interest"](oi_row)
    _assert_preserved(
        oi_result, oi_fact.row_hash, "derivatives.open_interest.v1"
    )

    funding_row = _envelope()
    funding_fact = FundingRateFact(
        sample_time=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        rate=0.0001,
        funding_time=datetime(2026, 8, 9, 11, 0, tzinfo=_UTC),
        interval_seconds=3600,
        unit="fraction",
        sample_time_method="collector_schedule",
        source_published_at=funding_row["source_published_at"],
        received_at=funding_row["received_at"],
        accepted_at=funding_row["accepted_at"],
        known_at=funding_row["known_at"],
        known_at_method=funding_row["known_at_method"],
    )
    funding_row.update(
        {
            "sample_time": funding_fact.sample_time,
            "funding_rate": funding_fact.rate,
            "funding_time": funding_fact.funding_time,
            "funding_interval_seconds": funding_fact.interval_seconds,
            "unit": funding_fact.unit,
            "sample_time_method": funding_fact.sample_time_method,
            "row_hash": funding_fact.row_hash,
        }
    )
    funding_result = _MIGRATION["_funding"](funding_row)
    _assert_preserved(
        funding_result, funding_fact.row_hash, "derivatives.funding_rate.v1"
    )
    funding_payload = json.loads(str(funding_result.values["payload"]))
    assert funding_payload["funding_time"] == "2026-08-09T11:00:00.000000Z"


def test_numeric_migration_preserves_exact_value_and_source_material() -> None:
    row = _envelope()
    source_material_hash = "a" * 64
    fact = NumericFact(
        fact_type="market.reference_price",
        contract_version="market.reference_price.v1",
        value=Decimal("3210.1250"),
        raw_value="321012500000",
        unit="USD",
        dimensions={"quote_currency": "USD"},
        effective_at=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        effective_at_method="chainlink_round_updated_at",
        source_published_at=row["source_published_at"],
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=row["known_at_method"],
        source_event_key="round:42",
        source_event_group_key="block:9",
        source_event_component_key="answer",
        source_event_material_hash=source_material_hash,
    )
    row.update(
        {
            "fact_type": fact.fact_type,
            "contract_version": fact.contract_version,
            "numeric_value": fact.value,
            "raw_value": fact.raw_value,
            "unit": fact.unit,
            "dimensions": dict(fact.dimensions),
            "effective_at": fact.effective_at,
            "effective_at_method": fact.effective_at_method,
            "source_event_key": fact.source_event_key,
            "source_event_group_key": fact.source_event_group_key,
            "source_event_component_key": fact.source_event_component_key,
            "source_event_material_hash": fact.source_event_material_hash,
            "state": fact.state.value,
            "row_hash": fact.row_hash,
        }
    )

    result = _MIGRATION["_numeric"](row)

    _assert_preserved(result, fact.row_hash, "market.reference_price.v1")
    payload = json.loads(str(result.values["payload"]))
    assert payload == {
        "raw_value": "321012500000",
        "unit": "USD",
        "value": "3210.125",
    }
    provenance = json.loads(str(result.values["provenance"]))
    migration = provenance["_qt_migration"]
    assert migration["source_event_material_hash"] == source_material_hash
    assert migration["series_dimensions"] == {"quote_currency": "USD"}


def test_trade_migration_preserves_atomic_payload_and_v1_hashes() -> None:
    row = _envelope()
    row["id"] = "mtv_fixture"
    row["ingestion_run_id"] = None
    row["provenance_hash"] = "b" * 64
    row["quality"] = {"gap": False}
    fact = MarketTradeFact(
        provider_product_id="BTC-PERP-INTX",
        provider_trade_id="trade-42",
        delivery_kind="update",
        price=Decimal("118000"),
        provider_size=Decimal("3"),
        provider_size_unit="contracts",
        maker_side="SELL",
        aggressor_side="BUY",
        aggressor_transform_version="coinbase_maker_to_aggressor.v1",
        contract_quantity=Decimal("3"),
        base_quantity=Decimal("0.03"),
        quote_notional=Decimal("3540"),
        base_currency="BTC",
        quote_currency="USD",
        product_definition_version_id="pdv_fixture",
        provider_event_time=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        provider_message_time=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        received_at=row["received_at"],
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        provider_sequence_num=101,
        connection_epoch=2,
        receive_ordinal=5,
        event_ordinal=0,
        trade_ordinal=0,
        raw_record_id="raw_fixture",
        coverage_interval_id="coverage_fixture",
    )
    row.update(fact.__dict__)
    row["material_hash"] = fact.material_hash
    row["row_hash"] = fact.row_hash

    result = _MIGRATION["_trade"](row)

    _assert_preserved(result, fact.row_hash, "market.trade.v1")
    assert result.values["material_hash"] == fact.material_hash
    payload = json.loads(str(result.values["payload"]))
    assert payload["base_quantity"] == "0.03"
    assert payload["aggressor_side"] == "BUY"
    assert "provider_product_id" not in payload
    provenance = json.loads(str(result.values["provenance"]))
    assert provenance["_qt_trade_evidence"]["provider_trade_id"] == "trade-42"
    assert provenance["_qt_migration"]["legacy_version_id"] == "mtv_fixture"


def test_trade_flow_migration_preserves_material_and_quality_semantics() -> None:
    row = _envelope()
    row.update(
        {
            "id": "tfav_fixture",
            "source_match_count": 1,
            "aggregation_version": "market.trade_flow.v1",
            "provenance_hash": "c" * 64,
            "quality": {"source_gap": False},
            "ingestion_run_id": None,
        }
    )
    fact = TradeFlowAggregateFact(
        interval_seconds=1,
        bucket_start=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        bucket_end=datetime(2026, 8, 9, 12, 0, 1, tzinfo=_UTC),
        trade_count=2,
        maker_buy_count=1,
        maker_sell_count=1,
        aggressor_buy_count=1,
        aggressor_sell_count=1,
        contract_volume=Decimal("2"),
        base_volume=Decimal("0.02"),
        quote_notional=Decimal("2361"),
        maker_buy_base_volume=Decimal("0.01"),
        maker_sell_base_volume=Decimal("0.01"),
        aggressor_buy_base_volume=Decimal("0.01"),
        aggressor_sell_base_volume=Decimal("0.01"),
        cvd_delta=Decimal("0"),
        cvd_unit="base",
        open_price=Decimal("118000"),
        high_price=Decimal("118100"),
        low_price=Decimal("118000"),
        close_price=Decimal("118100"),
        first_trade_id="trade-41",
        last_trade_id="trade-42",
        first_receive_ordinal=4,
        last_receive_ordinal=5,
        coverage_interval_id="coverage_fixture",
        coverage_revision=1,
        aggregate_complete=True,
        archive_complete=True,
        canonicalization_complete=True,
        late_trade_count=0,
        known_at=datetime(2026, 8, 9, 12, 0, 2, tzinfo=_UTC),
        input_fingerprint="d" * 64,
    )
    row.update(fact.__dict__)
    row["material_hash"] = fact.material_hash

    result = _MIGRATION["_trade_flow"](row)

    assert result.source_row_hash is None
    assert result.values["payload_schema_id"] == "market.trade_flow.v1"
    assert result.values["material_hash"] == fact.material_hash
    assert result.values["ingestion_run_id"] is None
    payload = json.loads(str(result.values["payload"]))
    assert payload["trade_count"] == 2
    assert payload["base_volume"] == "0.02"
    quality = json.loads(str(result.values["quality"]))
    assert quality["_qt_trade_flow_quality"] == {
        "aggregate_complete": True,
        "archive_complete": True,
        "canonicalization_complete": True,
        "late_trade_count": 0,
    }

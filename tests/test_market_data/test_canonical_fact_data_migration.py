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
from market_data.order_book import BookSourcePosition, L2EventFact, L2Mutation
from market_data.market_state import BboFeatureFact, DepthFeatureFact
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


def _book_position() -> BookSourcePosition:
    return BookSourcePosition(
        definition_id="fixture-l2",
        session_id="fixture-session",
        connection_epoch=0,
        provider_product_id="BTC-USD",
        provider_sequence_num=5,
        receive_ordinal=7,
        event_ordinal=0,
    )


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


def _l2_row(*, event_type: str) -> tuple[dict[str, object], L2EventFact]:
    position = BookSourcePosition(
        definition_id="stream-fixture",
        session_id="session-fixture",
        connection_epoch=1,
        provider_product_id="BTC-USD",
        provider_sequence_num=101,
        receive_ordinal=7,
        event_ordinal=0,
    )
    quantities = (Decimal("1.25"), Decimal("0"))
    sides = ("bid", "ask")
    mutations = tuple(
        L2Mutation(
            mutation_ordinal=ordinal,
            side=side,
            price=Decimal("118000") + ordinal,
            new_quantity=quantities[ordinal],
            provider_event_time=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
            provider_size_unit="base",
        )
        for ordinal, side in enumerate(sides)
    )
    event = L2EventFact(
        event_type=event_type,
        position=position,
        product_definition_version_id="pdv_fixture",
        mutations=mutations,
        provider_message_time=datetime(2026, 8, 9, 12, 0, tzinfo=_UTC),
        received_at=datetime(2026, 8, 9, 12, 0, 1, tzinfo=_UTC),
        accepted_at=datetime(2026, 8, 9, 12, 0, 2, tzinfo=_UTC),
        known_at=datetime(2026, 8, 9, 12, 0, 2, tzinfo=_UTC),
        raw_record_id="raw-l2-fixture",
    )
    row: dict[str, object] = {
        "id": f"l2-{event_type}-fixture",
        "series_id": 21,
        "revision": 1,
        "market_commit_seq": 32,
        "ingestion_run_id": None,
        "definition_id": position.definition_id,
        "session_id": position.session_id,
        "connection_epoch": position.connection_epoch,
        "provider_product_id": position.provider_product_id,
        "product_definition_version_id": event.product_definition_version_id,
        "provider_sequence_num": position.provider_sequence_num,
        "receive_ordinal": position.receive_ordinal,
        "event_ordinal": position.event_ordinal,
        "effective_at": event.effective_at,
        "provider_message_time": event.provider_message_time,
        "received_at": event.received_at,
        "accepted_at": event.accepted_at,
        "known_at": event.known_at,
        "event_material_hash": event.material_hash,
        "raw_record_id": event.raw_record_id,
        "validity_interval_id": "validity-fixture",
        "provenance_hash": "e" * 64,
        "quality": {"gap": False},
        "entries": [
            {
                "ordinal": mutation.mutation_ordinal,
                "side": mutation.side.value,
                "price": str(mutation.price),
                "quantity": str(mutation.new_quantity),
                "provider_size_unit": mutation.provider_size_unit.value,
                "provider_event_time": mutation.provider_event_time,
            }
            for mutation in mutations
        ],
        **_source_columns(),
    }
    return row, event


def test_l2_migration_preserves_atomic_snapshot_and_mutation_entries() -> None:
    snapshot_row, snapshot_event = _l2_row(event_type="snapshot")
    snapshot_row["entries"][1]["quantity"] = "2.5"  # type: ignore[index]
    snapshot_event = L2EventFact(
        event_type="snapshot",
        position=snapshot_event.position,
        product_definition_version_id=snapshot_event.product_definition_version_id,
        mutations=(
            snapshot_event.mutations[0],
            L2Mutation(
                mutation_ordinal=1,
                side="ask",
                price=Decimal("118001"),
                new_quantity=Decimal("2.5"),
                provider_event_time=snapshot_event.effective_at,
                provider_size_unit="base",
            ),
        ),
        provider_message_time=snapshot_event.provider_message_time,
        received_at=snapshot_event.received_at,
        accepted_at=snapshot_event.accepted_at,
        known_at=snapshot_event.known_at,
        raw_record_id=snapshot_event.raw_record_id,
    )
    snapshot_row["event_material_hash"] = snapshot_event.material_hash
    snapshot_row["level_count"] = 2
    snapshot_row["state_hash"] = "f" * 64

    snapshot = _MIGRATION["_l2_snapshot"](snapshot_row)

    snapshot_payload = json.loads(str(snapshot.values["payload"]))
    assert snapshot.values["payload_schema_id"] == "market.l2_book.v1"
    assert snapshot_payload["event_type"] == "snapshot"
    assert snapshot_payload["entry_count"] == 2
    assert snapshot_payload["entries"][1]["quantity"] == "2.5"
    assert snapshot.values["external_event_component_key"] == snapshot_row["id"]

    mutation_row, mutation_event = _l2_row(event_type="update")
    mutation_row.update(
        {
            "mutation_count": 2,
            "before_state_hash": "1" * 64,
            "after_state_hash": "2" * 64,
            "unknown_zero_delete_count": 1,
        }
    )
    assert mutation_row["event_material_hash"] == mutation_event.material_hash

    mutation = _MIGRATION["_l2_mutation"](mutation_row)

    mutation_payload = json.loads(str(mutation.values["payload"]))
    assert mutation_payload["event_type"] == "update"
    assert mutation_payload["before_state_hash"] == "1" * 64
    assert mutation_payload["entries"][1]["quantity"] == "0"
    assert mutation_payload["unknown_zero_delete_count"] == 1


def test_book_feature_migration_preserves_typed_material_evidence() -> None:
    bucket_start = datetime(2026, 8, 9, 12, 0, tzinfo=_UTC)
    bucket_end = bucket_start + timedelta(seconds=1)
    source_effective_at = bucket_start + timedelta(milliseconds=500)
    position = _book_position()
    bbo = BboFeatureFact(
        series_id=21,
        source_l2_series_id=20,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        source_effective_at=source_effective_at,
        known_at=bucket_end,
        source_position=position,
        validity_interval_id="validity-1",
        product_definition_version_id="coinbase.BTC-USD.v1",
        provider_size_unit="base",
        source_state_hash="a" * 64,
        bid_price=Decimal("99"),
        bid_quantity=Decimal("2"),
        bid_base_quantity=Decimal("2"),
        ask_price=Decimal("101"),
        ask_quantity=Decimal("3"),
        ask_base_quantity=Decimal("3"),
        mid_price=Decimal("100"),
        spread=Decimal("2"),
        spread_bps=Decimal("200"),
        input_fingerprint="b" * 64,
    )
    bbo_row = {
        **_envelope(),
        "id": "legacy-bbo-1",
        "source_l2_series_id": bbo.source_l2_series_id,
        "bucket_start": bbo.bucket_start,
        "bucket_end": bbo.bucket_end,
        "known_at": bbo.known_at,
        "source_effective_at": bbo.source_effective_at,
        "source_position": bbo.source_position.material(),
        "validity_interval_id": bbo.validity_interval_id,
        "product_definition_version_id": bbo.product_definition_version_id,
        "provider_size_unit": bbo.provider_size_unit.value,
        "source_state_hash": bbo.source_state_hash,
        "bid_price": bbo.bid_price,
        "bid_quantity": bbo.bid_quantity,
        "bid_base_quantity": bbo.bid_base_quantity,
        "ask_price": bbo.ask_price,
        "ask_quantity": bbo.ask_quantity,
        "ask_base_quantity": bbo.ask_base_quantity,
        "mid_price": bbo.mid_price,
        "spread": bbo.spread,
        "spread_bps": bbo.spread_bps,
        "input_fingerprint": bbo.input_fingerprint,
        "material_hash": bbo.material_hash,
        "provenance_hash": "c" * 64,
        "quality": {},
    }
    bbo_result = _MIGRATION["_bbo_feature"](bbo_row)
    assert bbo_result.values["payload_schema_id"] == "market.bbo.v1"
    bbo_provenance = json.loads(str(bbo_result.values["provenance"]))
    assert bbo_provenance["_qt_bbo_evidence"]["legacy_material_hash"] == (
        bbo.material_hash
    )

    depth = DepthFeatureFact(
        series_id=21,
        source_l2_series_id=20,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        source_effective_at=source_effective_at,
        known_at=bucket_end,
        source_position=position,
        validity_interval_id="validity-1",
        source_state_hash="a" * 64,
        bbo_input_fingerprint="b" * 64,
        provider_size_unit="base",
        band_bps=5,
        mid_price=Decimal("100"),
        bid_quantity=Decimal("2"),
        ask_quantity=Decimal("3"),
        bid_base_quantity=Decimal("2"),
        ask_base_quantity=Decimal("3"),
        bid_notional=Decimal("198"),
        ask_notional=Decimal("303"),
        imbalance=Decimal("-0.2"),
        input_fingerprint="d" * 64,
    )
    depth_row = {
        **_envelope(),
        "id": "legacy-depth-1",
        "source_l2_series_id": depth.source_l2_series_id,
        "bucket_start": depth.bucket_start,
        "bucket_end": depth.bucket_end,
        "known_at": depth.known_at,
        "source_effective_at": depth.source_effective_at,
        "source_position": depth.source_position.material(),
        "validity_interval_id": depth.validity_interval_id,
        "source_state_hash": depth.source_state_hash,
        "bbo_input_fingerprint": depth.bbo_input_fingerprint,
        "provider_size_unit": depth.provider_size_unit.value,
        "band_bps": depth.band_bps,
        "mid_price": depth.mid_price,
        "bid_quantity": depth.bid_quantity,
        "ask_quantity": depth.ask_quantity,
        "bid_base_quantity": depth.bid_base_quantity,
        "ask_base_quantity": depth.ask_base_quantity,
        "bid_notional": depth.bid_notional,
        "ask_notional": depth.ask_notional,
        "imbalance": depth.imbalance,
        "input_fingerprint": depth.input_fingerprint,
        "material_hash": depth.material_hash,
        "provenance_hash": "e" * 64,
        "quality": {},
    }
    depth_result = _MIGRATION["_depth_feature"](depth_row)
    assert depth_result.values["payload_schema_id"] == "market.depth_band.v1"
    depth_payload = json.loads(str(depth_result.values["payload"]))
    assert depth_payload["band_bps"] == 5
    assert depth_payload["imbalance"] == "-0.2"

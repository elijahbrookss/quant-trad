from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.canonical_adapters import (
    canonicalize_bbo_feature,
    canonicalize_depth_feature,
    decode_bbo_feature_record,
    decode_depth_feature_record,
)
from market_data.contracts import SourceIdentity
from market_data.fact_registry import (
    get_fact_payload_schema,
    supported_fact_payload_schemas,
)
from market_data.market_state import BboFeatureFact, DepthFeatureFact
from market_data.order_book import BookSourcePosition


_BASE = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
_COINBASE = SourceIdentity(
    provider="COINBASE",
    venue="COINBASE_DIRECT",
    source_kind="poll_api",
    adapter_version="coinbase_advanced_trade.funding_rate.public_poll.v2",
)


def _funding_fact(**overrides: object) -> CanonicalFact:
    values: dict[str, object] = {
        "fact_type": "derivatives.funding_rate",
        "payload_schema_id": "derivatives.funding_rate.v2",
        "observation_key": "schedule:2026-08-09T12:00:00Z",
        "observation_time": _BASE,
        "observation_time_method": "collector_schedule",
        "received_at": _BASE + timedelta(seconds=1),
        "accepted_at": _BASE + timedelta(seconds=2),
        "known_at": _BASE + timedelta(seconds=2),
        "known_at_method": "platform_acceptance",
        "source": _COINBASE,
        "transformation_id": "coinbase_funding_to_canonical.v2",
        "payload": {
            "rate": Decimal("-0.000025000"),
            "raw_rate": "-0.000025000",
            "funding_time": _BASE - timedelta(hours=1),
            "interval_seconds": 3600,
            "unit": "fraction",
        },
        "provenance": {
            "provider_product_id": "BTC-PERP-INTX",
            "response_hash": "a" * 64,
        },
    }
    values.update(overrides)
    return CanonicalFact(**values)  # type: ignore[arg-type]


def test_payload_schema_is_strict_versioned_and_hash_addressed() -> None:
    schema = get_fact_payload_schema("derivatives.funding_rate.v2")

    assert schema.fact_type == "derivatives.funding_rate"
    assert schema.contract["additional_properties"] is False
    assert schema.contract_hash == get_fact_payload_schema(
        "derivatives.funding_rate.v2"
    ).contract_hash
    assert len(schema.contract_hash) == 64
    assert len({row.schema_id for row in supported_fact_payload_schemas()}) == len(
        supported_fact_payload_schemas()
    )

    with pytest.raises(ValueError, match="unexpected=provider"):
        schema.normalize_payload(
            {
                "rate": "0.1",
                "raw_rate": "0.1",
                "funding_time": _BASE,
                "interval_seconds": 3600,
                "unit": "fraction",
                "provider": "coinbase",
            }
        )
    with pytest.raises(ValueError, match="missing=raw_rate"):
        schema.normalize_payload(
            {
                "rate": "0.1",
                "funding_time": _BASE,
                "interval_seconds": 3600,
                "unit": "fraction",
            }
        )


def test_trade_and_trade_flow_payloads_are_provider_neutral_and_strict() -> None:
    trade = get_fact_payload_schema("market.trade.v1")
    normalized_trade = trade.normalize_payload(
        {
            "price": Decimal("118000.00"),
            "reported_quantity": Decimal("3"),
            "reported_quantity_unit": "contracts",
            "contract_quantity": Decimal("3"),
            "base_quantity": Decimal("0.03"),
            "quote_notional": Decimal("3540"),
            "base_currency": "BTC",
            "quote_currency": "USD",
            "maker_side": "SELL",
            "aggressor_side": "BUY",
        }
    )
    assert normalized_trade["base_quantity"] == "0.03"
    assert "provider" not in normalized_trade
    assert "provider_trade_id" not in normalized_trade

    flow = get_fact_payload_schema("market.trade_flow.v1")
    normalized_flow = flow.normalize_payload(
        {
            "bucket_end": _BASE + timedelta(seconds=1),
            "trade_count": 2,
            "maker_buy_count": 1,
            "maker_sell_count": 1,
            "aggressor_buy_count": 1,
            "aggressor_sell_count": 1,
            "contract_volume": None,
            "base_volume": Decimal("0.02"),
            "quote_notional": Decimal("2360"),
            "maker_buy_base_volume": Decimal("0.01"),
            "maker_sell_base_volume": Decimal("0.01"),
            "aggressor_buy_base_volume": Decimal("0.01"),
            "aggressor_sell_base_volume": Decimal("0.01"),
            "cvd_delta": Decimal("0"),
            "cvd_unit": "base",
            "open_price": Decimal("118000"),
            "high_price": Decimal("118100"),
            "low_price": Decimal("118000"),
            "close_price": Decimal("118100"),
        }
    )
    assert normalized_flow["bucket_end"] == "2026-08-09T12:00:01.000000Z"
    assert normalized_flow["cvd_delta"] == "0"

    with pytest.raises(ValueError, match="unexpected=provider_product_id"):
        trade.normalize_payload({**normalized_trade, "provider_product_id": "BTC-PERP"})


def test_derived_market_state_payload_contracts_are_registered_and_strict() -> None:
    expected = {
        "market.bbo.v1": "market.bbo",
        "market.depth_band.v1": "market.depth_observation",
        "market.trade_flow_feature.v1": "market.trade_flow_feature",
        "market.futures_spot_basis.v1": "market.futures_spot_relationship",
        "market.derivative_state.v1": "market.derivative_state",
        "market.market_response.v1": "market.market_response",
    }
    assert {
        schema_id: get_fact_payload_schema(schema_id).fact_type
        for schema_id in expected
    } == expected

    bbo = get_fact_payload_schema("market.bbo.v1")
    normalized = bbo.normalize_payload(
        {
            "bucket_end": _BASE + timedelta(seconds=1),
            "product_definition_version_id": "coinbase.BTC-USD.v1",
            "validity_interval_id": "book-validity-1",
            "provider_size_unit": "base",
            "source_state_hash": "a" * 64,
            "bid_price": Decimal("117999.99"),
            "bid_quantity": Decimal("0.5"),
            "bid_base_quantity": Decimal("0.5"),
            "ask_price": Decimal("118000.01"),
            "ask_quantity": Decimal("0.4"),
            "ask_base_quantity": Decimal("0.4"),
            "mid_price": Decimal("118000"),
            "spread": Decimal("0.02"),
            "spread_bps": Decimal("0.001694915254237288135593220339"),
            "input_fingerprint": "b" * 64,
        }
    )
    assert normalized["mid_price"] == "118000"
    with pytest.raises(ValueError, match="unexpected=provider"):
        bbo.normalize_payload({**normalized, "provider": "coinbase"})


def test_book_features_round_trip_without_source_shape_in_payload() -> None:
    position = BookSourcePosition(
        definition_id="btc-l2",
        session_id="session-1",
        connection_epoch=0,
        provider_product_id="BTC-USD",
        provider_sequence_num=101,
        receive_ordinal=7,
        event_ordinal=0,
    )
    source_effective_at = _BASE + timedelta(milliseconds=500)
    bucket_end = _BASE + timedelta(seconds=1)
    bbo = BboFeatureFact(
        series_id=51,
        source_l2_series_id=41,
        bucket_start=_BASE,
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
    depth = DepthFeatureFact(
        series_id=52,
        source_l2_series_id=41,
        bucket_start=_BASE,
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
        input_fingerprint="c" * 64,
    )
    canonical_bbo = canonicalize_bbo_feature(bbo, source=_COINBASE)
    canonical_depth = canonicalize_depth_feature(depth, source=_COINBASE)
    assert "source_l2_series_id" not in canonical_bbo.payload
    assert "source_l2_series_id" not in canonical_depth.payload
    assert decode_bbo_feature_record(
        CanonicalFactRecord(
            series_id=51,
            source_id=1,
            revision=1,
            market_commit_seq=1,
            fact=canonical_bbo,
        )
    ).fact == bbo
    assert decode_depth_feature_record(
        CanonicalFactRecord(
            series_id=52,
            source_id=1,
            revision=1,
            market_commit_seq=2,
            fact=canonical_depth,
        )
    ).fact == depth


def test_l2_payload_validates_every_atomic_entry() -> None:
    schema = get_fact_payload_schema("market.l2_book.v1")
    payload = {
        "event_type": "snapshot",
        "product_definition_version_id": "coinbase.BTC-USD.v1",
        "validity_interval_id": "validity-1",
        "reconstruction_version": "l2-absolute.v1",
        "before_state_hash": None,
        "after_state_hash": "a" * 64,
        "event_material_hash": "b" * 64,
        "entry_count": 1,
        "unknown_zero_delete_count": 0,
        "entries": [
            {
                "ordinal": 0,
                "side": "bid",
                "price": Decimal("118000.00"),
                "quantity": Decimal("1.2500"),
                "provider_size_unit": "base",
                "provider_event_time": _BASE,
            }
        ],
    }

    normalized = schema.normalize_payload(payload)

    assert normalized["entries"] == [
        {
            "ordinal": 0,
            "side": "bid",
            "price": "118000",
            "quantity": "1.25",
            "provider_size_unit": "base",
            "provider_event_time": "2026-08-09T12:00:00.000000Z",
        }
    ]
    assert schema.contract["fields"][-1]["items"]["additional_properties"] is False

    invalid = {**payload, "entries": [{**payload["entries"][0], "venue": "COINBASE"}]}
    with pytest.raises(ValueError, match="unexpected=venue"):
        schema.normalize_payload(invalid)
    invalid = {**payload, "entries": [{**payload["entries"][0], "price": 1.0}]}
    with pytest.raises(ValueError, match="forbids binary floating point"):
        schema.normalize_payload(invalid)


def test_exact_decimal_payload_rejects_binary_float_and_is_canonical() -> None:
    fact = _funding_fact()

    assert fact.payload == {
        "rate": "-0.000025",
        "raw_rate": "-0.000025000",
        "funding_time": "2026-08-09T11:00:00.000000Z",
        "interval_seconds": 3600,
        "unit": "fraction",
    }
    assert len(fact.payload_contract_hash) == 64
    assert len(fact.payload_hash) == 64
    assert len(fact.material_hash) == 64
    assert len(fact.provenance_hash) == 64
    assert len(fact.row_hash) == 64

    with pytest.raises(ValueError, match="forbids binary floating point"):
        _funding_fact(
            payload={
                "rate": -0.000025,
                "raw_rate": "-0.000025",
                "funding_time": _BASE,
                "interval_seconds": 3600,
                "unit": "fraction",
            }
        )


def test_structured_payload_remains_one_atomic_fact() -> None:
    fact = _funding_fact()
    record = CanonicalFactRecord(
        series_id=41,
        source_id=7,
        revision=1,
        market_commit_seq=99,
        ingestion_run_id="mfi_funding_1",
        fact=fact,
    )

    assert record.fact.payload["rate"] == "-0.000025"
    assert record.fact.payload["funding_time"] == "2026-08-09T11:00:00.000000Z"
    assert record.fact.payload["interval_seconds"] == 3600
    assert record.fact_version_id.startswith("mfv_")


def test_provider_identity_changes_provenance_not_canonical_payload_meaning() -> None:
    first = _funding_fact()
    alternate = replace(
        first,
        source=SourceIdentity(
            provider="TEST_SOURCE",
            venue="ISOLATED",
            source_kind="fixture",
            adapter_version="fixture.funding.v2",
        ),
        transformation_id="fixture_funding_to_canonical.v2",
    )

    assert alternate.payload == first.payload
    assert alternate.payload_hash == first.payload_hash
    assert alternate.material_hash == first.material_hash
    assert alternate.provenance_hash != first.provenance_hash
    assert alternate.row_hash != first.row_hash


def test_canonical_envelope_enforces_schema_and_causal_clocks() -> None:
    with pytest.raises(ValueError, match="fact type and payload schema disagree"):
        _funding_fact(fact_type="market.reference_price")
    clock_skewed = _funding_fact(
        observation_time=_BASE + timedelta(seconds=3),
        source_published_at=_BASE + timedelta(seconds=3),
    )
    assert clock_skewed.known_at < clock_skewed.observation_time
    assert clock_skewed.known_at < clock_skewed.source_published_at
    with pytest.raises(ValueError, match="accepted_at precedes received_at"):
        _funding_fact(
            received_at=_BASE + timedelta(seconds=2),
            accepted_at=_BASE + timedelta(seconds=1),
            known_at=_BASE + timedelta(seconds=2),
        )
    with pytest.raises(ValueError, match="receipt-based known_at precedes acceptance"):
        _funding_fact(
            accepted_at=_BASE + timedelta(seconds=3),
            known_at=_BASE + timedelta(seconds=2),
        )


def test_record_can_preserve_a_schema_owned_historical_row_hash() -> None:
    fact = _funding_fact()
    historical_hash = "a" * 64

    record = CanonicalFactRecord(
        series_id=1,
        source_id=2,
        revision=3,
        market_commit_seq=4,
        fact=fact,
        row_hash=historical_hash,
    )

    assert record.row_hash == historical_hash
    assert record.fact_version_id is not None
    assert len(record.fact_version_id) == 44

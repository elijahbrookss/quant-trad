from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.contracts import SourceIdentity
from market_data.fact_registry import (
    get_fact_payload_schema,
    supported_fact_payload_schemas,
)


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
    with pytest.raises(ValueError, match="known_at precedes observation_time"):
        _funding_fact(known_at=_BASE - timedelta(seconds=1))
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

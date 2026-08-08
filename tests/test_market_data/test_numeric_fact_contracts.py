from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from data_providers.numeric_facts import (
    ProviderNumericBatch,
    ProviderNumericObservation,
)
from market_data.contracts import (
    NumericFact,
    NumericFactRecord,
    NumericFactState,
    SourceIdentity,
    build_numeric_fact_material_hash,
    build_provenance_hash,
)


_BASE = datetime(2026, 8, 7, 22, 3, tzinfo=UTC)


def _price_fact(**overrides: object) -> NumericFact:
    values: dict[str, object] = {
        "fact_type": "market.reference_price",
        "contract_version": "market.reference_price.v1",
        "value": Decimal("1914.28523541"),
        "raw_value": "191428523541",
        "unit": "usd",
        "dimensions": {"quote_currency": "usd"},
        "effective_at": _BASE,
        "effective_at_method": "chainlink_round_updated_at",
        "source_published_at": _BASE,
        "received_at": _BASE + timedelta(seconds=2),
        "accepted_at": _BASE + timedelta(seconds=3),
        "known_at": _BASE + timedelta(seconds=3),
        "known_at_method": "platform_acceptance",
        "source_event_key": "evm:1:proxy:round:answer",
        "source_event_group_key": "evm:1:proxy:round",
        "source_event_component_key": "answer",
    }
    values.update(overrides)
    return NumericFact(**values)  # type: ignore[arg-type]


def _reserve_fact(**overrides: object) -> NumericFact:
    values: dict[str, object] = {
        "fact_type": "market.reserve_balance",
        "contract_version": "market.reserve_balance.v1",
        "value": Decimal("501928900.880000000000000000"),
        "raw_value": "501928900880000000000000000",
        "unit": "usd",
        "dimensions": {"reserve_unit": "usd"},
        "effective_at": _BASE,
        "effective_at_method": "chainlink_round_updated_at",
        "source_published_at": _BASE,
        "received_at": _BASE + timedelta(seconds=2),
        "accepted_at": _BASE + timedelta(seconds=3),
        "known_at": _BASE + timedelta(seconds=3),
        "known_at_method": "platform_acceptance",
        "source_event_key": "evm:1:tusd-proxy:round:answer",
        "source_event_group_key": "evm:1:tusd-proxy:round",
        "source_event_component_key": "answer",
    }
    values.update(overrides)
    return NumericFact(**values)  # type: ignore[arg-type]


def _record(fact: NumericFact, *, revision: int, commit: int) -> NumericFactRecord:
    return NumericFactRecord(
        series_id=41,
        revision=revision,
        market_commit_seq=commit,
        ingestion_run_id=f"numeric-run-{revision}",
        source_identity_key="chainlink-source",
        source=SourceIdentity(
            provider="CHAINLINK",
            venue="ETHEREUM_MAINNET",
            source_kind="public_evm_contract",
            adapter_version="chainlink_aggregator_v3.v1",
        ),
        provenance={"block_hash": "0xabc"},
        fact=fact,
    )


def test_numeric_fact_preserves_large_raw_value_and_exact_decimal() -> None:
    fact = _reserve_fact()

    assert fact.value == Decimal("501928900.880000000000000000")
    assert fact.raw_value == "501928900880000000000000000"
    assert fact.unit == "USD"
    assert fact.dimensions == {"reserve_unit": "USD"}
    assert fact.to_dict()["value"] == "501928900.88"
    assert fact.to_dict()["raw_value"] == "501928900880000000000000000"

    with pytest.raises(ValueError, match="must be Decimal, integer, or decimal string"):
        _reserve_fact(value=501928900.88)


def test_contract_governs_units_dimensions_and_value_domain() -> None:
    assert _price_fact().dimensions == {"quote_currency": "USD"}
    assert _reserve_fact(value="0").value == Decimal(0)

    with pytest.raises(ValueError, match="expected_from_dimension=USD"):
        _price_fact(unit="EUR")
    with pytest.raises(ValueError, match="missing=quote_currency"):
        _price_fact(dimensions={})
    with pytest.raises(ValueError, match="unexpected=window_seconds"):
        _price_fact(
            dimensions={"quote_currency": "USD", "window_seconds": 3600}
        )
    with pytest.raises(ValueError, match="value must be > 0"):
        _price_fact(value="0")


def test_provider_observation_forbids_binary_float_before_canonicalization() -> None:
    with pytest.raises(ValueError, match="forbids binary floating point"):
        ProviderNumericObservation(
            value=1914.28523541,
            raw_value="191428523541",
            effective_at=_BASE,
            effective_at_method="chainlink_round_updated_at",
            source_published_at=_BASE,
            known_at=_BASE + timedelta(seconds=12),
            known_at_method="evm_confirmation_block",
            source_event_key="evm:1:proxy:round:answer",
            source_event_group_key="evm:1:proxy:round",
            source_event_component_key="answer",
            provenance={},
        )


@pytest.mark.parametrize("invalid_usage", [True, "1", 1.5])
def test_provider_batch_requires_exact_integer_usage_counters(
    invalid_usage: object,
) -> None:
    with pytest.raises(ValueError, match="must be nonnegative integer"):
        ProviderNumericBatch(
            observations=(),
            gaps=(),
            range_start=_BASE,
            range_end=_BASE + timedelta(seconds=1),
            source_position_start="100",
            source_position_end="100",
            source_position_head="100",
            status="complete",
            capabilities={},
            request={},
            budget_requests_used=invalid_usage,  # type: ignore[arg-type]
        )


def test_numeric_hashes_are_exact_stable_and_revision_independent() -> None:
    fact = _price_fact()
    same_value_different_scale = replace(fact, value=Decimal("1914.28523541000"))
    changed_raw = replace(fact, raw_value="0191428523541")
    invalidated = replace(fact, state=NumericFactState.INVALIDATED)

    assert same_value_different_scale.row_hash == fact.row_hash
    assert changed_raw.row_hash != fact.row_hash
    assert invalidated.row_hash != fact.row_hash

    series_identity = {
        "identity_key": "series-price-usd",
        "instrument_id": "ETH",
        "fact_type": fact.fact_type,
        "timeframe_seconds": None,
        "contract_version": fact.contract_version,
        "dimensions": {"quote_currency": "USD"},
    }
    first = build_numeric_fact_material_hash(
        series_identity=series_identity,
        records=[_record(fact, revision=1, commit=10)],
    )
    replayed = build_numeric_fact_material_hash(
        series_identity=series_identity,
        records=[_record(fact, revision=9, commit=99)],
    )
    corrected = build_numeric_fact_material_hash(
        series_identity=series_identity,
        records=[_record(changed_raw, revision=2, commit=11)],
    )
    revision_history = build_numeric_fact_material_hash(
        series_identity=series_identity,
        records=[
            _record(fact, revision=1, commit=10),
            _record(changed_raw, revision=2, commit=11),
        ],
    )
    reordered_history = build_numeric_fact_material_hash(
        series_identity=series_identity,
        records=[
            _record(changed_raw, revision=2, commit=11),
            _record(fact, revision=1, commit=10),
        ],
    )
    invalidation_history = build_numeric_fact_material_hash(
        series_identity=series_identity,
        records=[
            _record(fact, revision=1, commit=10),
            _record(invalidated, revision=2, commit=11),
        ],
    )

    assert first == replayed
    assert first != corrected
    assert revision_history == reordered_history
    assert revision_history != invalidation_history
    assert revision_history not in {first, corrected}

    provenance_history = build_provenance_hash(
        [
            _record(fact, revision=1, commit=10),
            _record(changed_raw, revision=2, commit=11),
        ]
    )
    reordered_provenance_history = build_provenance_hash(
        [
            _record(changed_raw, revision=2, commit=11),
            _record(fact, revision=1, commit=10),
        ]
    )
    assert provenance_history == reordered_provenance_history

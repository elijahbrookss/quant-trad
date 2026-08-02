from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_data.contracts import MarketDataRequirement
from market_data.normalization import (
    NormalizationFormula,
    NormalizationInput,
    NormalizationSpec,
    NormalizedStatus,
    evaluate_normalization,
)
from market_data.fact_registry import get_fact_contract

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _input(offset: timedelta, value: str | None, *, ordinal: int,
           denominator: str | None = None, partition: str = "series",
           valid: bool = True) -> NormalizationInput:
    effective = BASE + offset
    return NormalizationInput(
        source_series_id=11,
        effective_at=effective,
        known_at=effective + timedelta(seconds=1),
        market_commit_seq=ordinal,
        material_hash=_digest(f"source-{ordinal}"),
        value=Decimal(value) if value is not None else None,
        denominator=Decimal(denominator) if denominator is not None else None,
        partition_key=partition,
        valid=valid,
        invalid_reason=None if valid else "book_invalid",
    )


def _spec(formula: NormalizationFormula, *, window_seconds: int | None = None,
          minimum: int = 0, warmup: int = 0, parameters: dict | None = None,
          feature: str = "test_feature") -> NormalizationSpec:
    return NormalizationSpec(
        feature_name=feature,
        semantic_version="1.0.0",
        input_fact_type="market.bbo",
        output_fact_type=f"market.normalized.{feature}",
        formula=formula,
        units="ratio",
        window_seconds=window_seconds,
        minimum_observations=minimum,
        warmup_observations=warmup,
        parameters=parameters or {},
    )


def test_direct_transform_references_current_once() -> None:
    source = _input(timedelta(), "0.001", ordinal=1)
    result = evaluate_normalization(
        _spec(NormalizationFormula.BASIS_POINTS), [source], output_series_id=22
    )[0]
    assert result.status is NormalizedStatus.VALID
    assert result.value == Decimal("10")
    assert result.input_count == 1
    assert result.source_material_hashes == (source.material_hash,)


def test_ratio_zero_denominator_is_explicit_null() -> None:
    source = _input(timedelta(), "2", ordinal=1, denominator="0")
    result = evaluate_normalization(
        _spec(NormalizationFormula.RATIO), [source], output_series_id=22
    )[0]
    assert result.status is NormalizedStatus.ZERO_DENOMINATOR
    assert result.value is None
    assert result.reason == "denominator_is_zero_or_missing"


def test_truncation_invariance_and_no_future_input() -> None:
    spec = _spec(
        NormalizationFormula.CAUSAL_PERCENTILE,
        window_seconds=180,
        minimum=2,
        warmup=2,
        parameters={"require_full_window": False},
    )
    inputs = [
        _input(timedelta(minutes=index), str(value), ordinal=index + 1)
        for index, value in enumerate((2, 1, 3, 5, 4))
    ]
    prefix = evaluate_normalization(spec, inputs[:4], output_series_id=22)
    complete = evaluate_normalization(spec, inputs, output_series_id=22)
    assert tuple(row.material_hash for row in prefix) == tuple(
        row.material_hash for row in complete[:4]
    )
    assert prefix[-1].value == Decimal("1")
    assert inputs[-1].material_hash not in prefix[-1].source_material_hashes


def test_thirty_day_window_does_not_silently_shorten() -> None:
    spec = _spec(
        NormalizationFormula.CAUSAL_PERCENTILE,
        window_seconds=30 * 24 * 60 * 60,
        minimum=2,
        warmup=2,
        parameters={"require_full_window": True},
        feature="thirty_day",
    )
    inputs = [
        _input(timedelta(days=0), "1", ordinal=1),
        _input(timedelta(days=29), "2", ordinal=2),
        _input(timedelta(days=30), "3", ordinal=3),
    ]
    results = evaluate_normalization(spec, inputs, output_series_id=22)
    assert results[1].status is NormalizedStatus.INSUFFICIENT_HISTORY
    assert results[2].status is NormalizedStatus.VALID
    assert results[2].input_start == BASE


def test_time_of_day_baseline_uses_matching_prior_partition_only() -> None:
    spec = _spec(
        NormalizationFormula.TIME_OF_DAY_MEDIAN_RATIO,
        window_seconds=10 * 24 * 60 * 60,
        minimum=1,
        warmup=1,
        parameters={"require_full_window": False},
    )
    inputs = [
        _input(timedelta(days=0), "10", ordinal=1, partition="09:30"),
        _input(timedelta(days=1), "1000", ordinal=2, partition="10:30"),
        _input(timedelta(days=2), "20", ordinal=3, partition="09:30"),
    ]
    result = evaluate_normalization(spec, inputs, output_series_id=22)[-1]
    assert result.value == Decimal("2")
    assert result.source_material_hashes == (
        inputs[0].material_hash,
        inputs[2].material_hash,
    )


def test_zscore_uses_prior_values() -> None:
    spec = _spec(
        NormalizationFormula.CAUSAL_ZSCORE,
        window_seconds=600,
        minimum=2,
        warmup=2,
        parameters={"require_full_window": False},
    )
    inputs = [
        _input(timedelta(minutes=0), "1", ordinal=1),
        _input(timedelta(minutes=1), "3", ordinal=2),
        _input(timedelta(minutes=2), "5", ordinal=3),
    ]
    result = evaluate_normalization(spec, inputs, output_series_id=22)[-1]
    assert result.value == Decimal("3")


def test_volatility_adjustment_is_prior_only() -> None:
    spec = _spec(
        NormalizationFormula.VOLATILITY_ADJUSTED_RETURN,
        window_seconds=600,
        minimum=2,
        warmup=2,
        parameters={"require_full_window": False},
    )
    inputs = [
        _input(timedelta(minutes=index), value, ordinal=index + 1)
        for index, value in enumerate(("100", "101", "99", "102", "104"))
    ]
    prefix = evaluate_normalization(spec, inputs[:4], output_series_id=22)
    complete = evaluate_normalization(spec, inputs, output_series_id=22)
    assert prefix[-1].status is NormalizedStatus.VALID
    assert prefix[-1].material_hash == complete[3].material_hash
    assert inputs[4].material_hash not in prefix[-1].source_material_hashes


def test_invalid_source_is_visible_null() -> None:
    invalid = _input(timedelta(minutes=1), None, ordinal=2, valid=False)
    result = evaluate_normalization(
        _spec(NormalizationFormula.BASIS_POINTS), [invalid], output_series_id=22
    )[0]
    assert result.status is NormalizedStatus.INVALID_INPUT
    assert result.value is None
    assert result.reason == "book_invalid"


def test_spec_and_output_fingerprints_are_stable() -> None:
    first = _spec(NormalizationFormula.RATIO)
    second = _spec(NormalizationFormula.RATIO)
    source = _input(timedelta(), "2", ordinal=1, denominator="4")
    left = evaluate_normalization(first, [source], output_series_id=22)[0]
    right = evaluate_normalization(second, [source], output_series_id=22)[0]
    assert first.spec_id == second.spec_id
    assert left.input_fingerprint == right.input_fingerprint
    assert left.material_hash == right.material_hash


def test_registry_accepts_funding_and_spec_bound_normalized_series() -> None:
    funding = MarketDataRequirement(
        fact_type="derivatives.funding_rate", key="funding",
        max_staleness_seconds=120,
    )
    normalized = MarketDataRequirement(
        fact_type="market.normalized.aggressive_buy_share",
        contract_version=f"market.normalized_feature.v1/nsp_{'1' * 31}",
        timeframe_seconds=60,
        key="aggressive_buy_share",
        alignment="latest_known",
        max_staleness_seconds=120,
    )
    assert funding.contract_version == "derivatives.funding_rate.v1"
    assert normalized.fact_type == "market.normalized.aggressive_buy_share"


def test_rolling_spec_requires_window() -> None:
    with pytest.raises(ValueError, match="rolling formula requires a window"):
        _spec(NormalizationFormula.CAUSAL_PERCENTILE)


def test_rolling_lineage_has_full_fingerprint_but_bounded_witnesses() -> None:
    spec = _spec(
        NormalizationFormula.CAUSAL_PERCENTILE,
        window_seconds=3_600,
        minimum=2,
        warmup=2,
        parameters={"require_full_window": False},
    )
    inputs = [
        _input(timedelta(minutes=index), str(index), ordinal=index + 1)
        for index in range(10)
    ]
    original = evaluate_normalization(spec, inputs, output_series_id=22)[-1]
    changed_inputs = [
        *inputs[:4],
        replace(
            inputs[4],
            value=Decimal("400"),
            material_hash=_digest("changed-middle-source"),
        ),
        *inputs[5:],
    ]
    changed = evaluate_normalization(spec, changed_inputs, output_series_id=22)[-1]

    assert original.input_count == 10
    assert len(original.source_material_hashes) <= 3
    assert changed.source_material_hashes == original.source_material_hashes
    assert changed.input_fingerprint != original.input_fingerprint
    assert changed.material_hash != original.material_hash


def test_normalization_input_rejects_non_hex_material_hash() -> None:
    with pytest.raises(ValueError, match="SHA-256 hex digest"):
        replace(
            _input(timedelta(), "1", ordinal=1),
            material_hash="z" * 64,
        )


def test_raw_l2_is_registered_but_not_dataset_eligible() -> None:
    contract = get_fact_contract("market.l2_book")
    contract.validate(
        contract_version="market.l2_book.v1",
        timeframe_seconds=None,
    )
    assert contract.dataset_eligible is False


def test_normalized_contract_requires_exact_spec_identity() -> None:
    with pytest.raises(ValueError, match="market_fact_contract_mismatch"):
        MarketDataRequirement(
            fact_type="market.normalized.aggressive_buy_share",
            contract_version="market.normalized_feature.v1/nsp_1234",
            key="aggressive_buy_share",
        )

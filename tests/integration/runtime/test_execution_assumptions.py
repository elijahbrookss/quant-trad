from __future__ import annotations

import pytest

from engines.bot_runtime.core.execution import SpotExecutionConstraints, SpotExecutionModel
from engines.bot_runtime.core.execution_assumptions import (
    CONSERVATIVE_BAR_MODEL_VERSION,
    resolve_execution_assumptions,
)
from engines.bot_runtime.core.execution_intent import ExecutionIntent, LimitParams
from engines.bot_runtime.core.execution_order import OrderType, build_fill_order
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from engines.bot_runtime.core.execution_runtime import DeterministicExecutionModel
from engines.bot_runtime.core.fees import FeeResolver, FeeSchedule
from portal.backend.service.bots.runtime_control_service import (
    _apply_start_overrides,
    _start_config_hash,
)


def _conservative_payload() -> dict:
    return {
        "schema_version": "execution_assumptions.v1",
        "model_version": CONSERVATIVE_BAR_MODEL_VERSION,
        "market_slippage_bps": 5.0,
        "stop_slippage_bps": 12.0,
        "passive_fill_policy": "strict_penetration",
        "fee_policy": "instrument_resolved",
        "full_fill_assumption": True,
        "cost_stress_scenarios": [
            {"id": "moderate", "additional_slippage_bps": 5.0, "fee_multiplier": 1.25},
            {"id": "severe", "additional_slippage_bps": 15.0, "fee_multiplier": 1.5},
        ],
    }


def _limit_intent(side: str) -> ExecutionIntent:
    return ExecutionIntent(
        order_id=f"limit-{side}",
        side=side,
        qty=1.0,
        symbol="BTC-USD",
        order_type="limit_maker",
        requested_price=100.0,
        limit_params=LimitParams(
            anchor_price="signal_price",
            offset_type="price",
            offset_value=0.0,
            validity_window=1,
            fallback="cancel",
            limit_price=100.0,
        ),
        metadata={"pending_evaluation": True},
    )


def test_economic_manifest_is_deterministic_and_x2() -> None:
    left = resolve_execution_assumptions("economic", _conservative_payload())
    right = resolve_execution_assumptions("economic", _conservative_payload())

    assert left.execution_quality_ceiling == "X2"
    assert left.manifest_hash == right.manifest_hash
    assert left.to_dict()["economic_claim_intent"] == "economic"


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({}, "model_version is required"),
        ({**_conservative_payload(), "market_slippage_bps": 0.0}, "non-zero market_slippage_bps"),
        ({**_conservative_payload(), "fee_policy": "explicit_zero"}, "instrument_resolved"),
    ],
)
def test_economic_manifest_fails_closed(payload: dict, expected: str) -> None:
    with pytest.raises(ValueError, match=expected):
        resolve_execution_assumptions("economic", payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("full_fill_assumption", "true"),
        ("explicit_zero_cost_override", 0),
    ],
)
def test_economic_manifest_rejects_truthy_non_boolean_flags(
    field: str,
    value: object,
) -> None:
    payload = _conservative_payload()
    payload[field] = value

    with pytest.raises(ValueError, match="must be a boolean"):
        resolve_execution_assumptions("economic", payload)


def test_cost_stress_cannot_reduce_fee_costs() -> None:
    payload = _conservative_payload()
    payload["cost_stress_scenarios"] = [
        {
            "id": "not-adverse",
            "additional_slippage_bps": 1.0,
            "fee_multiplier": 0.5,
        }
    ]

    with pytest.raises(ValueError, match="fee_multiplier must be at least 1.0"):
        resolve_execution_assumptions("economic", payload)


def test_run_start_requires_and_pins_claim_intent() -> None:
    with pytest.raises(ValueError, match="economic_claim_intent is required"):
        _apply_start_overrides({"id": "bot-1"}, {})

    exploration = _apply_start_overrides(
        {"id": "bot-1"},
        {"economic_claim_intent": "exploration"},
    )
    economic = _apply_start_overrides(
        {"id": "bot-1"},
        {
            "economic_claim_intent": "economic",
            "execution_assumptions": _conservative_payload(),
        },
    )
    assert exploration["execution_assumptions"]["model_version"] == "legacy_bar_touch.v1"
    assert economic["execution_assumptions"]["execution_quality_ceiling"] == "X2"
    assert _start_config_hash(exploration) != _start_config_hash(economic)


def test_run_start_preserves_and_validates_pre_resolved_manifest() -> None:
    manifest = resolve_execution_assumptions(
        "selection",
        _conservative_payload(),
        source="experiment_plan",
    ).to_dict()

    resolved = _apply_start_overrides(
        {"id": "bot-1"},
        {
            "economic_claim_intent": "selection",
            "execution_assumptions": manifest,
        },
    )

    assert resolved["execution_assumptions"] == manifest
    tampered = dict(manifest)
    tampered["manifest_hash"] = "0" * 64
    with pytest.raises(ValueError, match="execution_assumption_manifest_hash_mismatch"):
        _apply_start_overrides(
            {"id": "bot-1"},
            {
                "economic_claim_intent": "selection",
                "execution_assumptions": tampered,
            },
        )


def test_fee_contract_distinguishes_valid_configuration_from_invalid_defaults() -> None:
    instrument = {
        "id": "btc-usd",
        "symbol": "BTC-USD",
        "instrument_type": "spot",
        "tick_size": 0.01,
        "contract_size": 1.0,
        "tick_value": 0.01,
        "base_currency": "BTC",
        "quote_currency": "USD",
        "maker_fee_rate": 0.001,
        "taker_fee_rate": 0.002,
        "fee_source": "instrument_contract",
        "fee_schedule_version": "fee-v1",
    }

    configured = compile_series_execution_profile(instrument)
    assert configured.fees.configured is True
    assert configured.fees.source == "instrument_contract"
    assert configured.fees.version == "fee-v1"

    invalid = compile_series_execution_profile({**instrument, "maker_fee_rate": "not-a-rate"})
    assert invalid.fees.configured is False
    assert invalid.fees.source == "default_zero"


@pytest.mark.parametrize(
    "side, exact_high, exact_low, penetrated_high, penetrated_low",
    [
        ("buy", 101.0, 100.0, 101.0, 99.99),
        ("sell", 100.0, 99.0, 100.01, 99.0),
    ],
)
def test_x2_passive_fill_requires_strict_penetration(
    side: str,
    exact_high: float,
    exact_low: float,
    penetrated_high: float,
    penetrated_low: float,
) -> None:
    assumptions = resolve_execution_assumptions("economic", _conservative_payload())
    model = DeterministicExecutionModel(
        FeeResolver(FeeSchedule(maker_rate=0.001, taker_rate=0.002, source="test", version="fees.v1")),
        assumptions=assumptions,
    )
    intent = _limit_intent(side)

    touch, rejection = model.evaluate(
        intent,
        candle_high=exact_high,
        candle_low=exact_low,
        candle_close=100.0,
        candle_open=100.0,
    )
    assert rejection is None
    assert touch.status == "open"

    penetration, rejection = model.evaluate(
        intent,
        candle_high=penetrated_high,
        candle_low=penetrated_low,
        candle_close=100.0,
        candle_open=100.0,
    )
    assert rejection is None
    assert penetration.status == "filled"
    assert penetration.metadata["passive_fill_policy"] == "strict_penetration"


def test_entry_market_and_exit_stop_use_pinned_distinct_slippage() -> None:
    assumptions = resolve_execution_assumptions("economic", _conservative_payload())
    entry_model = DeterministicExecutionModel(
        FeeResolver(FeeSchedule(maker_rate=0.001, taker_rate=0.002, source="test", version="fees.v1")),
        assumptions=assumptions,
    )
    outcome, rejection = entry_model.evaluate(
        ExecutionIntent(
            order_id="market-buy",
            side="buy",
            qty=1.0,
            symbol="BTC-USD",
            order_type="market",
            requested_price=100.0,
        ),
        candle_high=101.0,
        candle_low=99.0,
        candle_close=100.0,
        candle_open=100.0,
    )
    assert rejection is None
    assert outcome.avg_fill_price == pytest.approx(100.05)
    assert outcome.metadata["slippage_bps"] == 5.0

    exit_model = SpotExecutionModel(
        SpotExecutionConstraints(
            tick_size=0.01,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=1.0,
        ),
        assumptions=assumptions,
    )
    stop_order = build_fill_order(
        side="sell",
        requested_qty=1.0,
        price=100.0,
        order_type=OrderType.STOP_MARKET.value,
        liquidity_role="taker",
        price_source="stop",
        maker_fee_rate=0.001,
        taker_fee_rate=0.002,
    )
    stop_fill, rejection = exit_model.execute_order(stop_order)
    assert rejection is None
    assert stop_fill is not None
    assert stop_fill.fill_price == pytest.approx(99.88)
    assert stop_fill.metadata["slippage_bps"] == 12.0

    maker_order = build_fill_order(
        side="sell",
        requested_qty=1.0,
        price=100.0,
        order_type=OrderType.LIMIT_RESTING.value,
        liquidity_role="maker",
        price_source="target",
        maker_fee_rate=0.001,
        taker_fee_rate=0.002,
    )
    maker_fill, rejection = exit_model.execute_order(maker_order)
    assert rejection is None
    assert maker_fill is not None
    assert maker_fill.fill_price == 100.0
    assert maker_fill.metadata["slippage_bps"] == 0.0

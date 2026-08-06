from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from engines.bot_runtime.core.execution_assumptions import (
    CONSERVATIVE_BAR_MODEL_VERSION,
    resolve_execution_assumptions,
)
from engines.bot_runtime.core.execution import SpotExecutionConstraints, SpotExecutionModel
from engines.bot_runtime.core.execution_context import (
    ResolvedExecutionContext,
    ResolvedExecutionContextBundle,
    build_execution_context_bundle,
    resolve_execution_context,
)
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from engines.bot_runtime.core.execution_intent import ExecutionIntent, LimitParams
from engines.bot_runtime.core.execution_order import build_fill_order, execute_fill_order
from engines.bot_runtime.core.execution_runtime import DeterministicExecutionModel
from engines.bot_runtime.core.fees import FeeResolver, FeeSchedule


def _assumptions():
    return resolve_execution_assumptions(
        "economic",
        {
            "model_version": CONSERVATIVE_BAR_MODEL_VERSION,
            "market_slippage_bps": 5.0,
            "stop_slippage_bps": 12.0,
            "passive_fill_policy": "strict_penetration",
            "fee_policy": "instrument_resolved",
            "full_fill_assumption": True,
            "cost_stress_scenarios": [
                {"id": "adverse", "additional_slippage_bps": 5.0, "fee_multiplier": 1.25}
            ],
        },
    )


def _venue_profile(profile_id: str, *, order_types: list[str], tif: list[str]) -> dict:
    roles = {
        order_type: "maker" if order_type in {"limit_maker", "limit_resting"} else "taker"
        for order_type in order_types
    }
    return {
        "profile_id": profile_id,
        "version": f"{profile_id}.v1",
        "venue_id": f"{profile_id}-venue",
        "supported_order_types": order_types,
        "supported_time_in_force": tif,
        "post_only_supported": True,
        "post_only_behavior": "reject_would_cross",
        "liquidity_role_by_order_type": roles,
        "price_increment_policy": "reject",
        "quantity_increment_policy": "reject",
        "max_market_order_notional": 1_000.0,
        "market_price_collar_bps": None,
        "book_data_capability": "bars",
        "lifecycle_event_mapping": {
            "submitted": "accepted",
            "open": "working",
            "filled": "done",
            "rejected": "rejected",
        },
        "external_order_submission_enabled": False,
        "source": "synthetic_conformance_fixture",
    }


def _instrument(
    *,
    instrument_id: str = "btc-usd",
    symbol: str = "BTC-USD",
    venue_profile: dict | None = None,
    fee_schedule: dict | None = None,
) -> dict:
    profile = venue_profile or _venue_profile(
        "synthetic-alpha",
        order_types=["market", "limit_maker"],
        tif=["gtc"],
    )
    schedule = fee_schedule or {
        "schedule_id": "synthetic-alpha:retail",
        "venue_profile_id": profile["profile_id"],
        "version": "fees.v1",
        "maker_rate": 0.001,
        "taker_rate": 0.002,
        "source": "synthetic_conformance_fixture",
        "fee_currency": "USD",
        "calculation_basis": "quote_notional",
        "rounding_mode": "half_up",
        "precision": 2,
        "tier": "retail",
        "configured": True,
        "verified_zero": False,
    }
    return {
        "id": instrument_id,
        "symbol": symbol,
        "instrument_type": "spot",
        "datasource": "fixture-provider",
        "exchange": profile["venue_id"],
        "tick_size": 0.01,
        "contract_size": 1.0,
        "tick_value": 0.01,
        "base_currency": symbol.split("-")[0],
        "quote_currency": "USD",
        "min_order_size": 0.001,
        "qty_step": 0.001,
        "max_qty": 10.0,
        "min_notional": 1.0,
        "maker_fee_rate": schedule["maker_rate"],
        "taker_fee_rate": schedule["taker_rate"],
        "fee_source": schedule["source"],
        "fee_schedule_version": schedule["version"],
        "venue_execution_profile": profile,
        "fee_schedule": schedule,
    }


def _context(instrument: dict):
    profile = compile_series_execution_profile(instrument)
    return resolve_execution_context(
        profile,
        _assumptions(),
        instrument_payload=instrument,
        source="test_startup_resolution",
    )


def test_resolved_context_hashes_every_component_and_detects_tampering() -> None:
    left = _context(_instrument())
    right = _context(_instrument())

    assert left.context_hash == right.context_hash
    assert left.instrument.contract_hash
    assert left.venue.profile_hash
    assert left.fee_schedule.schedule_hash
    assert left.model.artifact_hash
    assert left.model.assumption_manifest_hash == _assumptions().manifest_hash

    restored = ResolvedExecutionContext.from_dict(left.to_dict())
    assert restored == left

    tampered = deepcopy(left.to_dict())
    tampered["venue"]["supported_time_in_force"] = (
        *tampered["venue"]["supported_time_in_force"],
        "ioc",
    )
    with pytest.raises(ValueError, match="venue_execution_profile_hash_mismatch"):
        ResolvedExecutionContext.from_dict(tampered)


def test_two_different_profiles_use_the_same_context_resolver() -> None:
    alpha = _context(_instrument())
    beta_profile = _venue_profile(
        "synthetic-beta",
        order_types=["market", "limit_resting", "stop_market"],
        tif=["gtc", "ioc"],
    )
    beta_profile.update(
        {
            "post_only_supported": False,
            "post_only_behavior": "cancel_would_cross",
            "price_increment_policy": "round_down",
            "quantity_increment_policy": "round_down",
            "max_market_order_notional": 250.0,
            "market_price_collar_bps": 25.0,
            "book_data_capability": "l1",
            "lifecycle_event_mapping": {
                "submitted": "queued",
                "open": "resting",
                "filled": "complete",
                "rejected": "declined",
            },
        }
    )
    beta_schedule = {
        "schedule_id": "synthetic-beta:pro",
        "venue_profile_id": "synthetic-beta",
        "version": "fees.v7",
        "maker_rate": 0.0001,
        "taker_rate": 0.0008,
        "source": "synthetic_conformance_fixture",
        "fee_currency": "USD",
        "calculation_basis": "quote_notional",
        "rounding_mode": "down",
        "precision": 4,
        "tier": "pro",
        "configured": True,
        "verified_zero": False,
    }
    beta = _context(
        _instrument(
            instrument_id="eth-usd",
            symbol="ETH-USD",
            venue_profile=beta_profile,
            fee_schedule=beta_schedule,
        )
    )

    assert alpha.venue.supported_order_types == ("limit_maker", "market")
    assert beta.venue.supported_order_types == ("limit_resting", "market", "stop_market")
    assert alpha.fee_schedule.rounding_mode == "half_up"
    assert beta.fee_schedule.rounding_mode == "down"
    assert alpha.venue.post_only_supported is True
    assert beta.venue.post_only_supported is False
    assert alpha.venue.price_increment_policy == "reject"
    assert beta.venue.price_increment_policy == "round_down"
    assert alpha.venue.book_data_capability == "bars"
    assert beta.venue.book_data_capability == "l1"
    assert dict(alpha.venue.lifecycle_event_mapping)["filled"] == "done"
    assert dict(beta.venue.lifecycle_event_mapping)["filled"] == "complete"

    bundle = build_execution_context_bundle([beta, alpha])
    restored = ResolvedExecutionContextBundle.from_dict(bundle.to_dict())
    assert restored.bundle_hash == bundle.bundle_hash
    assert restored.context_for(
        instrument_id="btc-usd", symbol="BTC-USD", execution_semantics="spot"
    ).context_hash == alpha.context_hash


def test_profile_conformance_rejects_unsupported_tif_increments_and_protections() -> None:
    context = _context(_instrument())

    accepted = context.validate_order(
        order_type="limit_maker",
        time_in_force="gtc",
        post_only=True,
        side="buy",
        quantity=0.010,
        price=100.00,
        liquidity_role="maker",
    )
    assert accepted.accepted is True
    assert accepted.liquidity_role == "maker"

    unsupported_order = context.validate_order(
        order_type="stop_market",
        time_in_force="gtc",
        post_only=False,
        side="sell",
        quantity=0.010,
        price=100.00,
        liquidity_role="taker",
    )
    assert unsupported_order.reason == "UNSUPPORTED_ORDER_TYPE"

    unsupported_tif = context.validate_order(
        order_type="market",
        time_in_force="ioc",
        post_only=False,
        side="buy",
        quantity=0.010,
        price=100.00,
        liquidity_role="taker",
    )
    assert unsupported_tif.reason == "UNSUPPORTED_TIME_IN_FORCE"

    off_step = context.validate_order(
        order_type="market",
        time_in_force="gtc",
        post_only=False,
        side="buy",
        quantity=0.0105,
        price=100.00,
        liquidity_role="taker",
    )
    assert off_step.reason == "QTY_INCREMENT_MISMATCH"

    off_tick = context.validate_order(
        order_type="limit_maker",
        time_in_force="gtc",
        post_only=True,
        side="buy",
        quantity=0.010,
        price=100.005,
        liquidity_role="maker",
    )
    assert off_tick.reason == "PRICE_INCREMENT_MISMATCH"

    protected = context.validate_order(
        order_type="market",
        time_in_force="gtc",
        post_only=False,
        side="buy",
        quantity=10.0,
        price=101.0,
        liquidity_role="taker",
    )
    assert protected.reason == "MARKET_PROTECTION_NOTIONAL_EXCEEDED"


def test_round_down_increment_policy_normalizes_before_fill() -> None:
    venue_profile = _venue_profile(
        "synthetic-round-down",
        order_types=["market", "limit_maker"],
        tif=["gtc"],
    )
    venue_profile["price_increment_policy"] = "round_down"
    venue_profile["quantity_increment_policy"] = "round_down"
    context = _context(_instrument(venue_profile=venue_profile))

    conformance = context.validate_order(
        order_type="limit_maker",
        time_in_force="gtc",
        post_only=True,
        side="buy",
        quantity=0.0105,
        price=100.005,
        liquidity_role="maker",
    )
    assert conformance.accepted is True
    assert conformance.normalized_qty == pytest.approx(0.010)
    assert conformance.normalized_price == pytest.approx(100.00)

    model = SpotExecutionModel(
        SpotExecutionConstraints(
            tick_size=0.01,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=1.0,
        ),
        assumptions=_assumptions(),
        execution_context=context,
    )
    order = build_fill_order(
        side="buy",
        requested_qty=0.0105,
        price=100.005,
        order_type="limit_maker",
        liquidity_role="maker",
        price_source="test",
        time_in_force="gtc",
        post_only=True,
        execution_context=context,
    )
    fill, rejection = execute_fill_order(model, order)

    assert rejection is None
    assert fill is not None
    assert fill.filled_qty == pytest.approx(0.010)
    assert fill.fill_price == pytest.approx(100.00)
    assert fill.metadata["requested_price"] == pytest.approx(100.005)
    assert fill.metadata["normalized_price"] == pytest.approx(100.00)


def test_market_price_collar_rejects_adverse_entry_and_fill_order() -> None:
    venue_profile = _venue_profile(
        "synthetic-tight-collar",
        order_types=["market"],
        tif=["gtc"],
    )
    venue_profile["market_price_collar_bps"] = 0.0
    context = _context(_instrument(venue_profile=venue_profile))
    assumptions = _assumptions()
    entry_model = DeterministicExecutionModel(
        FeeResolver(context.fee_schedule),
        assumptions=assumptions,
        execution_context=context,
    )

    outcome, rejection = entry_model.evaluate(
        ExecutionIntent(
            order_id="collar-entry",
            side="buy",
            qty=1.0,
            symbol="BTC-USD",
            order_type="market",
            requested_price=100.0,
            time_in_force="gtc",
            post_only=False,
        ),
        candle_high=101.0,
        candle_low=99.0,
        candle_close=100.0,
        candle_open=100.0,
    )
    assert outcome.status == "rejected"
    assert rejection is not None
    assert rejection.reason == "MARKET_PROTECTION_PRICE_COLLAR_EXCEEDED"

    exit_model = SpotExecutionModel(
        SpotExecutionConstraints(
            tick_size=0.01,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=1.0,
        ),
        assumptions=assumptions,
        execution_context=context,
    )
    order = build_fill_order(
        side="buy",
        requested_qty=1.0,
        price=100.0,
        order_type="market",
        liquidity_role="taker",
        price_source="test",
        execution_context=context,
    )
    fill, rejection = execute_fill_order(exit_model, order)
    assert fill is None
    assert rejection is not None
    assert rejection.reason == "MARKET_PROTECTION_PRICE_COLLAR_EXCEEDED"


def test_profile_post_only_cross_behavior_maps_to_canceled_outcome() -> None:
    venue_profile = _venue_profile(
        "synthetic-cancel-cross",
        order_types=["limit_maker"],
        tif=["gtc"],
    )
    venue_profile["post_only_behavior"] = "cancel_would_cross"
    context = _context(_instrument(venue_profile=venue_profile))
    model = DeterministicExecutionModel(
        FeeResolver(context.fee_schedule),
        assumptions=_assumptions(),
        execution_context=context,
    )

    outcome, rejection = model.evaluate(
        ExecutionIntent(
            order_id="post-only-cross",
            side="buy",
            qty=1.0,
            symbol="BTC-USD",
            order_type="limit_maker",
            requested_price=100.0,
            time_in_force="gtc",
            post_only=True,
            limit_params=LimitParams(
                anchor_price="signal_price",
                offset_type="ticks",
                offset_value=0.0,
                validity_window=1,
                fallback="cancel",
                limit_price=100.0,
            ),
        ),
        candle_high=101.0,
        candle_low=99.0,
        candle_close=100.0,
        candle_open=100.0,
    )

    assert rejection is not None
    assert rejection.reason == "POST_ONLY_WOULD_CROSS"
    assert outcome.status == "canceled"

def test_fee_schedule_owns_currency_rounding_tier_and_hash() -> None:
    context = _context(_instrument())
    detail = FeeResolver(context.fee_schedule).resolve(
        role="maker",
        price=123.45,
        quantity=1.0,
        contract_size=1.0,
    )

    assert detail.fee_paid == 0.12
    assert detail.currency == "USD"
    assert detail.rounding_mode == "half_up"
    assert detail.precision == 2
    assert detail.tier == "retail"
    assert detail.schedule_hash == context.fee_schedule.schedule_hash


@pytest.mark.parametrize(
    ("fee_updates", "reason"),
    (
        (
            {"fee_currency": "BTC", "calculation_basis": "base_quantity"},
            "phase_2a_non_quote_fee_currency_unsupported",
        ),
        (
            {"fee_currency": "USD", "calculation_basis": "base_quantity"},
            "phase_2a_fee_calculation_basis_unsupported",
        ),
        (
            {"maker_rate": -0.0001},
            "phase_2a_fee_rebate_unsupported",
        ),
    ),
)
def test_context_rejects_fee_schedules_canonical_accounting_cannot_settle(
    fee_updates: dict[str, object],
    reason: str,
) -> None:
    instrument = _instrument()
    fee_schedule = dict(instrument["fee_schedule"])
    fee_schedule.update(fee_updates)
    instrument["fee_schedule"] = fee_schedule

    with pytest.raises(ValueError, match=reason):
        _context(instrument)


def test_phase_2a_profiles_cannot_enable_external_order_submission() -> None:
    profile = _venue_profile(
        "synthetic-live",
        order_types=["market"],
        tif=["gtc"],
    )
    profile["external_order_submission_enabled"] = True

    with pytest.raises(ValueError, match="cannot enable external order submission"):
        _context(_instrument(venue_profile=profile))


def test_resolved_context_drives_entry_exit_fees_and_fill_provenance() -> None:
    assumptions = _assumptions()
    context = _context(_instrument())
    entry_model = DeterministicExecutionModel(
        FeeResolver(context.fee_schedule),
        assumptions=assumptions,
        execution_context=context,
    )
    outcome, rejection = entry_model.evaluate(
        ExecutionIntent(
            order_id="entry-1",
            side="buy",
            qty=1.0,
            symbol="BTC-USD",
            order_type="market",
            requested_price=100.0,
            time_in_force="gtc",
            post_only=False,
        ),
        candle_high=101.0,
        candle_low=99.0,
        candle_close=100.0,
        candle_open=100.0,
    )

    assert rejection is None
    assert outcome.status == "filled"
    assert outcome.fee_paid == 0.2
    assert outcome.metadata["resolved_execution_context_hash"] == context.context_hash
    assert outcome.metadata["fee_schedule_hash"] == context.fee_schedule.schedule_hash
    assert outcome.metadata["execution_model_artifact_hash"] == context.model.artifact_hash

    exit_model = SpotExecutionModel(
        SpotExecutionConstraints(
            tick_size=0.01,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=1.0,
        ),
        assumptions=assumptions,
        execution_context=context,
    )
    order = build_fill_order(
        side="sell",
        requested_qty=1.0,
        price=100.0,
        order_type="limit_maker",
        liquidity_role="maker",
        price_source="target",
        time_in_force="gtc",
        post_only=True,
        execution_context=context,
    )
    fill, rejection = execute_fill_order(exit_model, order)

    assert rejection is None
    assert fill is not None
    assert fill.fee == 0.1
    assert fill.metadata["resolved_execution_context_hash"] == context.context_hash
    assert fill.metadata["fee_currency"] == "USD"
    assert fill.metadata["fee_rounding_mode"] == "half_up"


def test_explicit_fee_schedule_inherits_economic_status_and_requires_identity() -> None:
    assumptions = _assumptions()
    instrument_payload = {
        **_instrument(),
        "venue_execution_profile": _venue_profile(
            "synthetic-rounded",
            order_types=["market"],
            tif=["gtc"],
        ),
        "fee_schedule": {
            "schedule_id": "synthetic-rounded:retail",
            "version": "synthetic-rounded.fees.v1",
            "maker_rate": 0.001,
            "taker_rate": 0.002,
            "source": "synthetic_fixture",
            "fee_currency": "USD",
            "calculation_basis": "quote_notional",
            "rounding_mode": "half_even",
            "precision": 2,
            "tier": "retail",
        },
    }
    profile = compile_series_execution_profile(instrument_payload)

    context = resolve_execution_context(
        profile,
        assumptions,
        instrument_payload=instrument_payload,
        source="test",
    )

    assert context.fee_schedule.configured is True
    assert context.fee_schedule.verified_zero is False
    malformed = context.fee_schedule.to_dict()
    malformed["schedule_id"] = ""
    malformed["schedule_hash"] = ""
    with pytest.raises(ValueError, match="schedule_id is required"):
        FeeSchedule.from_dict(malformed)


def test_generic_execution_modules_contain_no_named_venue_branches() -> None:
    root = Path(__file__).resolve().parents[3]
    generic_modules = (
        root / "src/engines/bot_runtime/core/execution_context.py",
        root / "src/engines/bot_runtime/core/execution.py",
        root / "src/engines/bot_runtime/core/execution_order.py",
        root / "src/engines/bot_runtime/core/execution_runtime.py",
    )
    for module in generic_modules:
        source = module.read_text(encoding="utf-8").lower()
        assert "coinbase" not in source
        assert "kraken" not in source

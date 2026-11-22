"""Tick protection is opt-in; it does not imply causal market-entry clocks."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from types import SimpleNamespace

import pytest

from engines.bot_runtime.core.domain import Candle, LadderRiskEngine
from engines.bot_runtime.core.domain.position import LadderPosition
from engines.bot_runtime.core.execution_assumptions import (
    TICK_ALIGNED_CONSERVATIVE_BAR_MODEL_VERSION as V2,
    resolve_execution_assumptions,
)
from engines.bot_runtime.core.execution_context import resolve_execution_context
from engines.bot_runtime.core.execution_plan import RuntimeStopAdjustment
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile
from tests.integration.runtime.test_execution_assumptions import _conservative_payload
from tests.integration.runtime.test_execution_contexts import _instrument


def manifest(model=V2, **overrides):
    return resolve_execution_assumptions("exploration", {
        **_conservative_payload(), "model_version": model, **overrides,
    })


def grid(price, tick, up):
    # Independent Decimal oracle, preserving the actual float input contract.
    return float((Decimal(str(price)) / Decimal(str(tick))).to_integral_value(
        rounding=ROUND_CEILING if up else ROUND_FLOOR) * Decimal(str(tick)))


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("tick,entry", [(.01, 100.060005), (.05, 101.123),
    (.25, 100.375), (.0001, 98765.43215)])
def test_initial_stop_target_and_effective_price_risk(direction, tick, entry):
    risk = LadderRiskEngine(
        config={"initial_stop": {"atr_multiplier": 2}, "execution_mode": "market",
            "take_profit_orders": [{"id": "tp", "ticks": 10, "size_fraction": 1}]},
        instrument={**_instrument(), "tick_size": tick, "tick_value": tick},
        risk_config={"base_risk_per_trade": 5}, execution_assumptions=manifest())
    nominal_distance = 7.5 * tick
    sign = 1 if direction == "long" else -1
    stop = risk._calculate_stop_price(entry, direction, 7.5)
    assert stop == grid(entry - sign * nominal_distance, tick, direction == "short")
    effective_distance = abs(entry - stop)
    assert effective_distance >= nominal_distance - 1e-10
    assert effective_distance < nominal_distance + tick + 1e-10
    candle = Candle(time=datetime(2024, 2, 1, tzinfo=timezone.utc),
        open=entry, high=entry+1, low=entry-1, close=entry, atr=1)
    legs = risk._build_legs(candle, direction, 7.5, 1, entry_price=entry)
    assert legs[0].target_price == grid(entry + sign * 10 * tick, tick, direction == "long")


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("tick,entry", [(.01, 100.060005), (.05, 101.123),
    (.25, 100.375), (.0001, 98765.43215)])
@pytest.mark.parametrize("mechanism", ["breakeven", "adjustment", "trailing"])
def test_dynamic_stop_aligns_before_monotonic_clamp(direction, tick, entry, mechanism):
    sign = 1 if direction == "long" else -1
    position = LadderPosition(entry_time=datetime(2024, 2, 1, tzinfo=timezone.utc),
        entry_price=entry, direction=direction, tick_size=tick,
        stop_price=grid(entry-sign*20*tick, tick, direction == "short"),
        execution_assumptions=manifest())
    position.mfe_ticks = 100
    position.r_ticks = 10
    if mechanism == "breakeven":
        candidate = entry
        position.breakeven_trigger_ticks = 1
        apply = position._maybe_move_breakeven
    elif mechanism == "adjustment":
        candidate = entry + sign * 3.3 * tick
        position.stop_adjustments = [RuntimeStopAdjustment("r_multiple", None, 1,
            "move_to_r", .33)]
        apply = position._maybe_apply_stop_adjustments
    else:
        position.best_price = entry + sign * 10 * tick
        position.trailing_distance_ticks = 2.3
        position.trailing_activation_ticks = 1
        candidate = position.best_price - sign * 2.3 * tick
        apply = position._maybe_trail_stop
    apply()
    expected = grid(candidate, tick, direction == "short")
    assert position.stop_price == expected
    # An already tighter executable stop must never loosen after rounding.
    tighter = grid(expected + sign * 2*tick, tick, direction == "short")
    position.stop_price = tighter
    position.moved_to_breakeven = False
    for rule in position.stop_adjustments:
        rule.fired = False
    apply()
    assert position.stop_price == tighter


@pytest.mark.parametrize("tick,price", [(.01, 100.06), (.05, 101.15),
    (.25, 100.5), (.0001, 98765.4321)])
@pytest.mark.parametrize("order_type,role", [("stop_market", "taker"), ("limit_resting", "maker")])
def test_large_tick_count_grid_acceptance_does_not_accept_genuine_off_grid(tick, price, order_type, role):
    instrument = _instrument()
    instrument.update(tick_size=tick, tick_value=tick)
    instrument["venue_execution_profile"]["supported_order_types"] += ["stop_market", "limit_resting"]
    instrument["venue_execution_profile"]["liquidity_role_by_order_type"].update(
        stop_market="taker", limit_resting="maker")
    context = resolve_execution_context(compile_series_execution_profile(instrument),
        manifest(), instrument_payload=instrument, source="qualification")
    def check(value):
        return context.validate_order(order_type=order_type, time_in_force="gtc",
            post_only=False, side="sell", quantity=.01, price=value, liquidity_role=role)
    assert check(price).accepted
    rejected = check(price + tick / 3)
    assert not rejected.accepted
    assert rejected.reason == "PRICE_INCREMENT_MISMATCH"


@pytest.mark.parametrize("mode,execution_mode,claim,full_fill", [
    ("paper", "fast", "exploration", True), ("live", "fast", "exploration", True),
    ("backtest", "full", "exploration", True), ("backtest", "fast", "economic", True),
    ("backtest", "fast", "exploration", False)])
def test_worker_rejects_prohibited_v2_modes_before_data_access(mode, execution_mode, claim, full_fill):
    from engines.bot_runtime.strategy.series_builder import SeriesBuilder
    from tests.test_portal.test_series_builder_incremental import _builder_deps
    def no_read(*args, **kwargs):
        raise AssertionError("Rejected execution model must not read instruments or prices")
    resolved = resolve_execution_assumptions(claim, {
        **_conservative_payload(), "model_version": V2, "full_fill_assumption": full_fill})
    builder = SeriesBuilder(bot_id="fixture", run_type=mode,
        config={"economic_claim_intent": claim, "execution_assumptions": resolved.to_dict(),
            "execution_mode": execution_mode},
        deps=_builder_deps(fetch_ohlcv=no_read, get_instrument_record=no_read, resolve_instrument=no_read))
    with pytest.raises(ValueError, match="exploratory fast full-fill backtest"):
        builder._build_single_series(SimpleNamespace(id="strategy"), SimpleNamespace(instrument_id="fixture"))


def test_v2_artifact_explicitly_preserves_legacy_clock_and_delayed_target_finalization():
    from engines.bot_runtime.core.execution_context import execution_model_artifact_from_assumptions
    from engines.bot_runtime.core.bar_execution_clock import execution_clock
    resolved = manifest()
    artifact = execution_model_artifact_from_assumptions(resolved)
    assert artifact.parameters["decision_fill_clock"] == "legacy_same_bar_close"
    assert "target_finalization" not in artifact.parameters
    assert not resolved.uses_causal_bar_clock
    assert not resolved.finalizes_last_target_at_fill
    candle = Candle(time=datetime(2024, 2, 1, tzinfo=timezone.utc),
        open=100, high=101, low=99, close=100)
    assert execution_clock(candle, resolved) == candle.time


def test_full_market_runtime_preserves_fill_fees_wallet_and_legacy_clock(monkeypatch, tmp_path):
    """Real runtime with fixture ports, not market evidence or a DB readback."""
    from tests.helpers.builders import persisted_runtime_harness as h
    from engines.bot_runtime.core.fees import FeeSchedule
    real_runtime, real_instrument = h.BotRuntime, h._instrument
    schedule = FeeSchedule(maker_rate=.005, taker_rate=.009,
        source="simulation:conservative-v2-qualification", version="fixture.v1",
        schedule_id="fixture", venue_profile_id="canonical_bar_simulation",
        fee_currency="USD", rounding_mode="half_up", precision=2).to_dict()
    def instrument():
        return {**real_instrument(), "instrument_type": "spot", "can_short": False,
            "tick_size": .01, "tick_value": .01, "qty_step": .00000001,
            "min_order_size": .00000001,
            "metadata": {"info": {"base_increment": ".00000001"}}}
    def runtime(bot_id, config, **kwargs):
        return real_runtime(bot_id, {**config, "execution_semantics": "spot",
            "execution_behavior": "simulated", "economic_claim_intent": "exploration",
            "execution_assumptions": manifest().to_dict(),
            "simulation_fee_schedules": {h.INSTRUMENT_ID: schedule}}, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(h, "_instrument", instrument)
        patch.setattr(h, "BotRuntime", runtime)
        result = h.run_persisted_reference(monkeypatch=patch, root=tmp_path,
            include_future_suffix=True)
    stream = result.artifact["runtime_event_stream"]
    entries = [event for event in stream if event["event_name"] == "ENTRY_FILLED"]
    exits = [event for event in stream if event["event_name"] == "EXIT_FILLED"]
    assert len(entries) == len(exits) == len(result.trades) == 1
    entry, exit_fill, trade = entries[0]["context"], exits[0]["context"], result.trades[0]
    assert entry["fee_source"] == exit_fill["fee_source"] == schedule["source"]
    assert entry["fee_schedule_hash"] == exit_fill["fee_schedule_hash"]
    assert entry["resolved_execution_context_hash"] == exit_fill["resolved_execution_context_hash"]
    assert entry["qty"] == exit_fill["qty"]
    assert entry["notional"] == pytest.approx(entry["qty"] * trade["entry_price"])
    fees = entry["fee_paid"] + exit_fill["fee_paid"]
    assert trade["fees_paid"] == pytest.approx(fees)
    assert trade["net_pnl"] == pytest.approx(trade["gross_pnl"] - fees)
    equity = h.STARTING_CASH + trade["net_pnl"]
    assert result.summary["summary"]["equity_end"] == pytest.approx(equity)
    assert result.artifact["wallet_state"]["balances"]["USD"] == pytest.approx(equity)
    assert result.artifact["wallet_state"]["locked_margin"] == {}
    # This model deliberately retains recognizing-bar labels; not causal v2.
    decision = next(e for e in stream if e["event_name"] == "DECISION_ACCEPTED")
    assert entries[0]["event_ts"] == decision["event_ts"]
    assert result.manifest["status"] == "completed"

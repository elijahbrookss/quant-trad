from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pandas as pd

from engines.bot_runtime.core.domain import Candle, LadderPosition, Leg, SameBarResolutionPolicy
from engines.bot_runtime.core.runtime_events import RuntimeEventName
from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.runtime.components.run_context import RunContext
from engines.bot_runtime.runtime.core import SeriesExecutionState
from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.bot_runtime.runtime.components.runtime_policy import ExecutionMode
from tests.helpers.builders.runtime_scenario_builder import RuntimeScenarioBuilder


def _ts(minutes: int = 0) -> datetime:
    return datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=minutes)


def _candle(
    minute: int,
    *,
    open: float = 100.0,
    high: float = 100.0,
    low: float = 100.0,
    close: float = 100.0,
    duration_minutes: int = 1,
) -> Candle:
    start = _ts(minute)
    return Candle(
        time=start,
        open=open,
        high=high,
        low=low,
        close=close,
        end=start + timedelta(minutes=duration_minutes),
    )


def _position(direction: str) -> LadderPosition:
    if direction == "long":
        stop_price = 96.0
        target_price = 110.0
    else:
        stop_price = 104.0
        target_price = 90.0
    return LadderPosition(
        entry_time=_ts(),
        entry_price=100.0,
        direction=direction,
        stop_price=stop_price,
        tick_size=1.0,
        legs=[Leg(name="TP1", ticks=10, target_price=target_price, contracts=1.0, leg_id="tp-1")],
    )


def _engine_with_position(direction: str = "long"):
    engine = RuntimeScenarioBuilder.spot_engine()
    position = _position(direction)
    engine.active_trade = position
    engine.trades.append(position)
    return engine


def _runtime_deps(fetch_ohlcv) -> BotRuntimeDeps:
    return BotRuntimeDeps(
        fetch_strategy=lambda _strategy_id: None,
        fetch_ohlcv=fetch_ohlcv,
        resolve_instrument=lambda _datasource, _exchange, _symbol: None,
        strategy_evaluate=lambda *args, **kwargs: {},
        strategy_run_preview=lambda *args, **kwargs: {},
        indicator_get_instance_meta=lambda *args, **kwargs: {},
        indicator_build_runtime_graph=lambda *args, **kwargs: ({}, []),
        indicator_build_runtime_instance=lambda *args, **kwargs: None,
        indicator_runtime_input_plan_for_instance=lambda *args, **kwargs: {},
        build_indicator_context=lambda bot_id, _overlay_cache: SimpleNamespace(
            cache_owner="test",
            cache_scope_id=bot_id,
        ),
        record_bot_runtime_event=lambda _payload: None,
        record_bot_runtime_events_batch=lambda _payloads: 0,
        record_bot_trade=lambda _payload: None,
        record_bot_trade_event=lambda _payload: None,
        record_bot_run_steps_batch=lambda _payloads: 0,
        update_bot_run_artifact=lambda _run_id, _payload: None,
        build_run_artifact_bundle=lambda _bot_id, _run_id, _config, _series: None,
    )


def _intrabar_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    index = [_ts(idx) for idx in range(len(rows))]
    return pd.DataFrame(rows, index=pd.DatetimeIndex(index))


def _runtime_for_intrabar(fetch_ohlcv, *, execution_mode: str = "full") -> BotRuntime:
    return BotRuntime(
        "bot-1",
        {
            "mode": "instant",
            "execution_mode": execution_mode,
            "wallet_config": {"balances": {"USD": 1_000_000.0}},
        },
        deps=_runtime_deps(fetch_ohlcv),
    )


def _series_state(engine) -> tuple[SimpleNamespace, SeriesExecutionState, Candle]:
    parent = _candle(0, high=111.0, low=95.0, close=100.0, duration_minutes=5)
    series = SimpleNamespace(
        strategy_id="strategy-1",
        symbol="BTC-USD",
        timeframe="5m",
        datasource="coinbase",
        exchange="coinbase",
        instrument={"id": "instrument-btc", "symbol": "BTC-USD"},
        risk_engine=engine,
        execution_profile=None,
    )
    return series, SeriesExecutionState(series=series, total_bars=1), parent


def test_fast_long_tp_and_stop_same_bar_resolves_stop() -> None:
    position = _position("long")
    bar = _candle(0, high=111.0, low=95.0)

    events = position.apply_bar(bar, same_bar_policy=SameBarResolutionPolicy.PESSIMISTIC_STOP)

    assert [event["type"] for event in events if event["type"] != "close"] == ["stop"]


def test_fast_short_tp_and_stop_same_bar_resolves_stop() -> None:
    position = _position("short")
    bar = _candle(0, high=105.0, low=89.0)

    events = position.apply_bar(bar, same_bar_policy=SameBarResolutionPolicy.PESSIMISTIC_STOP)

    assert [event["type"] for event in events if event["type"] != "close"] == ["stop"]


def test_full_intrabar_tp_then_stop_resolves_take_profit() -> None:
    engine = _engine_with_position("long")
    parent = _candle(0, high=111.0, low=95.0, duration_minutes=5)
    intrabar = [
        _candle(0, high=111.0, low=100.0),
        _candle(1, high=100.0, low=95.0),
    ]

    result = engine.step_intrabar_sequence(parent_candle=parent, intrabar_candles=intrabar)

    assert result.fallback_reason is None
    assert [event["type"] for event in result.events if event["type"] != "close"] == ["target"]


def test_full_intrabar_stop_then_tp_resolves_stop() -> None:
    engine = _engine_with_position("long")
    parent = _candle(0, high=111.0, low=95.0, duration_minutes=5)
    intrabar = [
        _candle(0, high=100.0, low=95.0),
        _candle(1, high=111.0, low=100.0),
    ]

    result = engine.step_intrabar_sequence(parent_candle=parent, intrabar_candles=intrabar)

    assert result.fallback_reason is None
    assert [event["type"] for event in result.events if event["type"] != "close"] == ["stop"]


def test_full_missing_intrabar_falls_back_to_pessimistic_strategy_bar() -> None:
    runtime = _runtime_for_intrabar(lambda *args, **kwargs: None)
    runtime._run_context = RunContext(bot_id="bot-1", run_id="run-1")
    engine = _engine_with_position("long")
    series, state, parent = _series_state(engine)
    state.series = series

    events = runtime._prime_intrabar_or_step_bar(state, parent)

    assert [event["type"] for event in events if event["type"] != "close"] == ["stop"]
    assert runtime.state["warnings"][0]["warning_type"] == "execution_intrabar_fallback_pessimistic"
    assert runtime.state["warnings"][0]["context"]["reason"] == "missing_1m_data"
    fallback_events = [
        event
        for event in runtime._run_context.runtime_event_stream
        if event["event_name"] == RuntimeEventName.EXECUTION_INTRABAR_FALLBACK_PESSIMISTIC.value
    ]
    assert len(fallback_events) == 1
    assert fallback_events[0]["context"]["reason"] == "missing_1m_data"


def test_full_single_intrabar_candle_tp_and_stop_falls_back_to_pessimistic() -> None:
    rows = [
        {"open": 100.0, "high": 111.0, "low": 95.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
    ]
    runtime = _runtime_for_intrabar(lambda *args, **kwargs: _intrabar_frame(rows))
    runtime._run_context = RunContext(bot_id="bot-1", run_id="run-1")
    engine = _engine_with_position("long")
    series, state, parent = _series_state(engine)
    state.series = series

    events = runtime._prime_intrabar_or_step_bar(state, parent)

    assert [event["type"] for event in events if event["type"] != "close"] == ["stop"]
    assert runtime.state["warnings"][0]["context"]["reason"] == "ambiguous_1m_candle"


def test_full_complete_intrabar_sequence_does_not_emit_fallback_warning() -> None:
    rows = [
        {"open": 100.0, "high": 111.0, "low": 100.0, "close": 110.0},
        {"open": 110.0, "high": 110.0, "low": 100.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
        {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
    ]
    runtime = _runtime_for_intrabar(lambda *args, **kwargs: _intrabar_frame(rows))
    runtime._run_context = RunContext(bot_id="bot-1", run_id="run-1")
    engine = _engine_with_position("long")
    series, state, parent = _series_state(engine)
    state.series = series

    events = runtime._prime_intrabar_or_step_bar(state, parent)

    assert [event["type"] for event in events if event["type"] != "close"] == ["target"]
    assert runtime.state.get("warnings") in (None, [])
    assert [
        event
        for event in runtime._run_context.runtime_event_stream
        if event["event_name"] == RuntimeEventName.EXECUTION_INTRABAR_FALLBACK_PESSIMISTIC.value
    ] == []


def test_execution_mode_is_separate_from_playback_mode() -> None:
    runtime = _runtime_for_intrabar(lambda *args, **kwargs: None, execution_mode="fast")
    runtime.apply_config({"mode": "walk-forward"})

    assert runtime.playback_mode == "walk-forward"
    assert runtime.execution_mode == ExecutionMode.FAST


def test_full_intrabar_resolution_is_repeatable_for_same_inputs() -> None:
    def run_once() -> tuple[str | None, list[tuple[Any, ...]]]:
        engine = _engine_with_position("long")
        parent = _candle(0, high=111.0, low=95.0, duration_minutes=5)
        intrabar = [
            _candle(0, high=100.0, low=95.0),
            _candle(1, high=111.0, low=100.0),
        ]
        result = engine.step_intrabar_sequence(parent_candle=parent, intrabar_candles=intrabar)
        signature = [
            (
                event.get("type"),
                event.get("leg"),
                event.get("price"),
                event.get("gross_pnl"),
                event.get("net_pnl"),
            )
            for event in result.events
        ]
        return result.fallback_reason, signature

    assert run_once() == run_once()

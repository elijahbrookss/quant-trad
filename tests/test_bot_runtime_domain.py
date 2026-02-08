from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from engines.bot_runtime.core.domain import (
    Candle,
    LadderPosition,
    Leg,
    timeframe_to_seconds,
)


def test_timeframe_to_seconds_parses_minutes():
    assert timeframe_to_seconds("15m") == 900


def test_ladder_position_targets_and_stops():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    position = LadderPosition(
        entry_time=start,
        entry_price=100.0,
        direction="long",
        stop_price=95.0,
        tick_size=1.0,
        legs=[
            Leg(name="TP1", ticks=10, target_price=110.0, contracts=1),
            Leg(name="TP2", ticks=30, target_price=130.0, contracts=1),
        ],
        breakeven_trigger_ticks=5,
        tick_value=1.0,
        contract_size=1.0,
        taker_fee_rate=0.0,
    )
    position.register_entry_fee()

    first_candle = Candle(time=start, open=100, high=112, low=99, close=110)
    events = position.apply_bar(first_candle)
    target_events = [event for event in events if event["type"] == "target"]
    assert target_events, "Target leg should fill when price trades through target"
    assert position.moved_to_breakeven, "Stop should move to breakeven after target fill"
    assert any(event["type"] == "stop" for event in events), "Remaining legs should stop at breakeven in same candle"
    assert not position.is_active()


def test_ladder_position_emits_settlement_events():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    exit_settlement = MagicMock()
    position = LadderPosition(
        entry_time=start,
        entry_price=100.0,
        direction="long",
        stop_price=95.0,
        tick_size=1.0,
        legs=[Leg(name="TP1", ticks=10, target_price=110.0, contracts=1, leg_id="leg-1")],
        breakeven_trigger_ticks=5,
        tick_value=1.0,
        contract_size=1.0,
        taker_fee_rate=0.0,
        exit_settlement=exit_settlement,
    )
    first_candle = Candle(time=start, open=100, high=112, low=99, close=110)
    events = position.apply_bar(first_candle)
    target_events = [event for event in events if event["type"] == "target"]
    assert target_events
    assert "settlement" in target_events[0]
    assert target_events[0]["settlement"]["trade_id"] == position.trade_id
    exit_settlement.apply_exit_fill.assert_not_called()

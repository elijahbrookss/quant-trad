from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from engines.bot_runtime.core.domain import (
    Candle,
    LadderRiskEngine,
    LadderPosition,
    Leg,
    normalize_epoch,
    timeframe_to_seconds,
)


def test_timeframe_to_seconds_parses_minutes():
    assert timeframe_to_seconds("15m") == 900


def test_normalize_epoch_converts_millisecond_values_to_seconds():
    assert normalize_epoch(1755777600000) == 1755777600
    assert normalize_epoch("1755777600000") == 1755777600
    assert normalize_epoch("1755777600000.0") == 1755777600


def test_risk_engine_serialise_trade_window_does_not_serialize_dropped_history():
    class _Trade:
        def __init__(self, trade_id: str, *, active: bool) -> None:
            self.trade_id = trade_id
            self._active = active
            self.serialized = 0

        def is_active(self) -> bool:
            return self._active

        def serialize(self):
            self.serialized += 1
            return {"trade_id": self.trade_id, "status": "open" if self._active else "closed"}

    old_closed = _Trade("closed-old", active=False)
    new_closed = _Trade("closed-new", active=False)
    open_trade = _Trade("open-1", active=True)
    engine = LadderRiskEngine.__new__(LadderRiskEngine)
    engine.trades = [old_closed, new_closed, open_trade]

    payload = engine.serialise_trade_window(max_closed=1)

    assert [entry["trade_id"] for entry in payload] == ["closed-new", "open-1"]
    assert old_closed.serialized == 0
    assert new_closed.serialized == 1
    assert open_trade.serialized == 1


def test_risk_engine_serialise_trade_changes_since_uses_revision_cursor():
    class _Trade:
        def __init__(self, trade_id: str, *, active: bool) -> None:
            self.trade_id = trade_id
            self._active = active
            self.serialized = 0

        def is_active(self) -> bool:
            return self._active

        def serialize(self):
            self.serialized += 1
            return {"trade_id": self.trade_id, "status": "open" if self._active else "closed"}

    old_closed = _Trade("closed-old", active=False)
    new_closed = _Trade("closed-new", active=False)
    open_trade = _Trade("open-1", active=True)
    engine = LadderRiskEngine.__new__(LadderRiskEngine)
    engine.trades = [old_closed, new_closed, open_trade]
    engine.trade_revision = 3
    engine._trade_change_log = [
        (1, ("closed-old",)),
        (2, ("closed-new",)),
        (3, ("open-1",)),
    ]

    payload = engine.serialise_trade_changes_since(1)

    assert payload["from_revision"] == 1
    assert payload["to_revision"] == 3
    assert payload["total_trades"] == 3
    assert payload["cursor_expired"] is False
    assert [entry["trade_id"] for entry in payload["trades"]] == ["closed-new", "open-1"]
    assert old_closed.serialized == 0
    assert new_closed.serialized == 1
    assert open_trade.serialized == 1


def test_risk_engine_advances_position_commit_seq_for_changed_trade_only():
    class _Trade:
        def __init__(self, trade_id: str, *, entry_price: float) -> None:
            self.trade_id = trade_id
            self.direction = "long"
            self.entry_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
            self.entry_price = entry_price
            self.stop_price = 95.0
            self.moved_to_breakeven = False
            self.trailing_active = False
            self.closed_at = None
            self.gross_pnl = 0.0
            self.fees_paid = 0.0
            self.net_pnl = 0.0
            self.legs = []
            self.position_commit_seq = 0

        def is_active(self) -> bool:
            return True

    changed_trade = _Trade("trade-a", entry_price=100.0)
    unchanged_trade = _Trade("trade-b", entry_price=200.0)
    engine = LadderRiskEngine.__new__(LadderRiskEngine)
    engine.trades = [changed_trade, unchanged_trade]
    engine.active_trade = changed_trade
    engine.trade_revision = 0
    engine._trade_change_log = []
    previous_signature = engine._trade_material_signature_by_id()

    changed_trade.entry_price = 101.0

    assert engine._bump_trade_revision_if_material_changed(previous_signature) is True
    assert engine.trade_revision == 1
    assert changed_trade.position_commit_seq == 1
    assert unchanged_trade.position_commit_seq == 0
    assert engine._trade_change_log == [(1, ("trade-a",))]


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

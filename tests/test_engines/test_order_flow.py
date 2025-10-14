from datetime import datetime, timedelta, timezone

import pytest

pd = pytest.importorskip("pandas")

from engines import (
    ExecutionRequest,
    OrderEngine,
    SimBroker,
    StrategyBox,
    StrategyContext,
)
from signals.base import BaseSignal


class StubPriceFeed:
    """In-memory price feed returning deterministic next-bar opens."""

    def __init__(self) -> None:
        self._prices = {}

    def set_next_open(self, symbol: str, current_ts: datetime, next_ts: datetime, price: float) -> None:
        self._prices[(symbol, current_ts)] = (next_ts, price)

    def lookup(self, symbol: str, current_ts: datetime):
        return self._prices.get((symbol, current_ts))


def build_frame(start: datetime, freq: str, prices: list[float]) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=len(prices), freq=freq, tz=timezone.utc)
    return pd.DataFrame({"close": prices}, index=index)


def simple_signal_generator(df: pd.DataFrame):
    last_idx = df.index[-1]
    yield BaseSignal(
        type="momentum",
        symbol="CL",
        time=last_idx.to_pydatetime(),
        confidence=0.6,
        metadata={"close": float(df.iloc[-1]["close"])}
    )


def simple_rule(signals, latest_row):
    if not signals:
        return None
    if float(latest_row["close"]) <= 0:
        return None
    return {"side": "buy", "reason": "positive_close"}


def simple_sizer(decision, signals, latest_row):
    return 1.0


def test_multi_strategy_flow_updates_positions():
    feed = StubPriceFeed()
    broker = SimBroker(feed.lookup, slippage_bps=5)
    engine = OrderEngine(
        broker,
        per_strategy_position_limit={"alpha": 2.0, "beta": 2.0},
        symbol_exposure_limit={"CL": 3.0},
    )

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    df_fast = build_frame(start, "15min", [80.0, 81.0, 82.0])
    df_slow = build_frame(start, "1h", [79.0, 80.5])

    feed.set_next_open("CL", df_fast.index[-1].to_pydatetime(), df_fast.index[-1].to_pydatetime() + timedelta(minutes=15), 83.0)
    feed.set_next_open("CL", df_slow.index[-1].to_pydatetime(), df_slow.index[-1].to_pydatetime() + timedelta(hours=1), 81.0)

    box_fast = StrategyBox(
        StrategyContext("alpha", "CL", "15m"),
        simple_signal_generator,
        simple_rule,
        simple_sizer,
        engine,
    )
    box_slow = StrategyBox(
        StrategyContext("beta", "CL", "1h"),
        simple_signal_generator,
        simple_rule,
        simple_sizer,
        engine,
    )

    status_fast = box_fast.on_bar_close(df_fast)
    status_slow = box_slow.on_bar_close(df_slow)

    assert status_fast == "accepted"
    assert status_slow == "accepted"

    positions = engine.get_positions()
    assert positions[("alpha", "CL")] == 1.0
    assert positions[("beta", "CL")] == 1.0

    symbol_exposure = engine.get_symbol_exposure()
    assert symbol_exposure["CL"] == 2.0

    executions = engine.get_executions()
    assert len(executions) == 2
    assert executions[0].fill_price != executions[1].fill_price


def test_duplicate_request_rejected_after_first_acceptance():
    feed = StubPriceFeed()
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    feed.set_next_open("CL", now, now + timedelta(minutes=15), 100.0)
    broker = SimBroker(feed.lookup)
    engine = OrderEngine(broker)

    request = ExecutionRequest(
        correlation_id="dup-1",
        strategy_id="alpha",
        symbol="CL",
        side="buy",
        qty=1.0,
        timestamp=now,
        metadata={},
    )

    first = engine.submit(request)
    second = engine.submit(request)

    assert first.status == "accepted"
    assert second.status == "rejected"
    assert second.reason == "duplicate"
    assert second.order_id == first.order_id


def test_limits_block_invalid_orders():
    feed = StubPriceFeed()
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    feed.set_next_open("CL", now, now + timedelta(minutes=15), 100.0)
    broker = SimBroker(feed.lookup)
    engine = OrderEngine(
        broker,
        per_strategy_position_limit={"alpha": 1.0},
        symbol_exposure_limit={"CL": 1.0},
    )

    ok_request = ExecutionRequest(
        correlation_id="limit-1",
        strategy_id="alpha",
        symbol="CL",
        side="buy",
        qty=1.0,
        timestamp=now,
        metadata={},
    )

    rejected_request = ExecutionRequest(
        correlation_id="limit-2",
        strategy_id="alpha",
        symbol="CL",
        side="buy",
        qty=1.0,
        timestamp=now + timedelta(minutes=1),
        metadata={},
    )

    first = engine.submit(ok_request)
    assert first.status == "accepted"

    feed.set_next_open("CL", rejected_request.timestamp, rejected_request.timestamp + timedelta(minutes=15), 101.0)
    second = engine.submit(rejected_request)

    assert second.status == "rejected"
    assert second.reason in {"strategy position limit", "symbol exposure limit"}

    bad_qty = ExecutionRequest(
        correlation_id="limit-3",
        strategy_id="alpha",
        symbol="CL",
        side="buy",
        qty=0,
        timestamp=now + timedelta(minutes=2),
        metadata={},
    )

    third = engine.submit(bad_qty)
    assert third.status == "rejected"
    assert third.reason == "quantity must be positive"


def test_recent_orders_and_positions_snapshot():
    feed = StubPriceFeed()
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    feed.set_next_open("CL", now, now + timedelta(minutes=15), 100.0)
    broker = SimBroker(feed.lookup)
    engine = OrderEngine(broker)

    request = ExecutionRequest(
        correlation_id="snap-1",
        strategy_id="alpha",
        symbol="CL",
        side="buy",
        qty=1.0,
        timestamp=now,
        metadata={"reason": "test"},
    )

    response = engine.submit(request)
    assert response.status == "accepted"

    orders = engine.get_recent_orders()
    assert len(orders) == 1
    assert orders[0].order_id == response.order_id

    positions = engine.get_positions()
    assert positions[("alpha", "CL")] == 1.0

    executions = engine.get_executions()
    assert len(executions) == 1
    assert executions[0].order_id == response.order_id

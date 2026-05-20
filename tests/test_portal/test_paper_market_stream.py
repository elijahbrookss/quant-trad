from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from data_providers.streams import CanonicalMarketEvent
from engines.bot_runtime.live_market import LiveCandleStore
from portal.backend.service.bots.paper_market_stream import PaperMarketStreamRunner


class _ScriptedStream:
    def __init__(self, events):
        self.events_script = list(events)
        self.closed = False

    async def connect(self) -> None:
        return None

    async def subscribe(self, subscriptions) -> None:
        self.subscriptions = list(subscriptions)

    async def events(self):
        for item in self.events_script:
            if isinstance(item, Exception):
                raise item
            yield item
        while True:
            await asyncio.sleep(60)

    async def close(self) -> None:
        self.closed = True


def _ticker_event(*, price: str = "101.25") -> CanonicalMarketEvent:
    return CanonicalMarketEvent.build(
        event_kind="market_ticker",
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        symbol="BTC-PERP",
        product_id="BIP-20DEC30-CDE",
        provider_event_time="2026-01-01T00:00:05Z",
        received_at="2026-01-01T00:00:06Z",
        payload={"price": price},
    )


def _series() -> list[SimpleNamespace]:
    return [
        SimpleNamespace(
            strategy_id="strategy-1",
            datasource="COINBASE",
            exchange="COINBASE_DIRECT",
            symbol="BTC-PERP",
            timeframe="1h",
            instrument={
                "id": "instrument-1",
                "metadata": {
                    "product": {
                        "product_id": "BIP-20DEC30-CDE",
                    }
                },
            },
        )
    ]


def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def test_paper_market_stream_builds_coinbase_subscriptions_from_runtime_series() -> None:
    runner = PaperMarketStreamRunner(
        bot_id="bot-1",
        run_id="run-1",
        store=LiveCandleStore(),
        series=[
            SimpleNamespace(
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol="BTC-PERP",
                timeframe="1h",
                instrument={
                    "metadata": {
                        "product": {
                            "product_id": "BIP-20DEC30-CDE",
                        }
                    }
                },
            )
        ],
    )

    subscriptions = runner._subscriptions()

    assert len(subscriptions) == 1
    assert subscriptions[0].to_dict() == {
        "provider": "COINBASE",
        "venue": "COINBASE_DIRECT",
        "symbol": "BTC-PERP",
        "product_id": "BIP-20DEC30-CDE",
        "channels": ["heartbeats", "ticker", "candles"],
        "timeframe": "1h",
        "auth_mode": "public",
    }


def test_paper_market_stream_emits_throttled_provisional_candle_from_ticker() -> None:
    emitted = []
    runner = PaperMarketStreamRunner(
        bot_id="bot-1",
        run_id="run-1",
        store=LiveCandleStore(),
        series=[
            SimpleNamespace(
                strategy_id="strategy-1",
                datasource="COINBASE",
                exchange="COINBASE_DIRECT",
                symbol="BTC-PERP",
                timeframe="1h",
                instrument={
                    "id": "instrument-1",
                    "metadata": {
                        "product": {
                            "product_id": "BIP-20DEC30-CDE",
                        }
                    },
                },
            )
        ],
        provisional_candle_sink=lambda payload: not emitted.append(dict(payload)),
        provisional_emit_interval_ms=1000,
    )
    runner._subscriptions()

    event = {
        "event_kind": "market_ticker",
        "provider": "COINBASE",
        "venue": "COINBASE_DIRECT",
        "symbol": "BTC-PERP",
        "product_id": "BIP-20DEC30-CDE",
        "provider_event_time": "2026-01-01T00:00:05Z",
        "received_at": "2026-01-01T00:00:06Z",
        "payload": {"price": "101.25"},
    }
    runner._handle_event(event)
    runner._handle_event({**event, "received_at": "2026-01-01T00:00:07Z", "payload": {"price": "101.50"}})

    assert len(emitted) == 1
    payload = emitted[0]
    assert payload["series_key"] == "instrument-1|1h"
    assert payload["strategy_id"] == "strategy-1"
    assert payload["provisional_candle"]["time"] == "2026-01-01T00:00:00Z"
    assert payload["provisional_candle"]["close"] == 101.25
    assert payload["provisional_candle"]["is_closed"] is False
    assert payload["provisional_candle"]["execution_eligible"] is False
    assert runner.snapshot()["event_counts"]["provisional_candle_throttled"] == 1


def test_paper_market_stream_reconnects_transient_websocket_drop_without_failing_store() -> None:
    streams = [
        _ScriptedStream([_ticker_event(), RuntimeError("no close frame received or sent")]),
        _ScriptedStream([_ticker_event(price="102.00")]),
    ]
    runner = PaperMarketStreamRunner(
        bot_id="bot-1",
        run_id="run-1",
        store=LiveCandleStore(),
        series=_series(),
        market_data_stream_policy={
            "reconnect_enabled": True,
            "initial_backoff_seconds": 0.001,
            "max_backoff_seconds": 0.001,
            "continuous_disconnect_budget_seconds": 1.0,
            "heartbeat_stale_seconds": 10.0,
        },
        stream_factory=lambda _subscriptions: streams.pop(0),
    )

    try:
        runner.start()
        _wait_until(lambda: runner.snapshot()["stream_diagnostics"]["reconnect_success_count"] == 1)
        snapshot = runner.snapshot()
    finally:
        runner.stop()

    assert snapshot["store"]["failure_message"] is None
    assert snapshot["stream_diagnostics"]["disconnect_count"] == 1
    assert snapshot["stream_diagnostics"]["reconnect_attempt_count"] >= 1
    assert snapshot["stream_diagnostics"]["reconnect_success_count"] == 1
    assert snapshot["event_counts"]["stream_reconnect_succeeded"] == 1


def test_paper_market_stream_fails_after_continuous_disconnect_budget_exhausted() -> None:
    runner = PaperMarketStreamRunner(
        bot_id="bot-1",
        run_id="run-1",
        store=LiveCandleStore(),
        series=_series(),
        market_data_stream_policy={
            "reconnect_enabled": True,
            "initial_backoff_seconds": 0.001,
            "max_backoff_seconds": 0.001,
            "continuous_disconnect_budget_seconds": 0.03,
            "heartbeat_stale_seconds": 0.01,
        },
        stream_factory=lambda _subscriptions: _ScriptedStream(
            [RuntimeError("no close frame received or sent")]
        ),
    )

    try:
        runner.start()
        _wait_until(
            lambda: bool(runner.snapshot()["store"]["failure_message"]),
            timeout=1.0,
        )
        snapshot = runner.snapshot()
    finally:
        runner.stop()

    assert "disconnected longer than budget" in snapshot["store"]["failure_message"]
    assert snapshot["stream_diagnostics"]["disconnect_count"] == 1
    assert snapshot["stream_diagnostics"]["reconnect_attempt_count"] >= 1
    assert snapshot["event_counts"]["disconnect_budget_exhausted"] == 1

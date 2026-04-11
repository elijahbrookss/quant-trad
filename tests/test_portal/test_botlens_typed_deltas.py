from __future__ import annotations

import asyncio
import json

from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
from portal.backend.service.bots.botlens_typed_deltas import (
    SymbolTypedDeltaBuilder,
    TypedDeltaInstrumentation,
)


class FakeWebSocket:
    def __init__(self) -> None:
        self.accepted = False
        self.messages: list[dict] = []
        self.closed = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        self.messages.append(json.loads(payload))

    async def close(self, code: int = 1000) -> None:
        self.closed = True


def test_symbol_typed_delta_builder_and_summary_cover_all_supported_delta_types() -> None:
    prepared = SymbolTypedDeltaBuilder.build(
        run_id="run-1",
        symbol_key="instrument-btc|1m",
        seq=42,
        event_time="2026-01-01T00:01:00Z",
        delta={
            "candle": {"time": 1, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0},
            "overlay_delta": {"ops": [{"op": "upsert", "key": "overlay:regime", "overlay": {"type": "regime_overlay"}}]},
            "trade_upserts": [{"trade_id": "trade-1", "symbol_key": "instrument-btc|1m"}],
            "trade_removals": [],
            "log_append": [{"id": "log-1", "message": "runtime log"}],
            "decision_append": [{"event_id": "decision-1", "event": "decision"}],
            "runtime": {"status": "running"},
        },
    )

    assert [entry.event.delta_type for entry in prepared] == [
        "symbol_candle_delta",
        "symbol_overlay_delta",
        "symbol_trade_delta",
        "symbol_log_delta",
        "symbol_decision_delta",
        "symbol_runtime_delta",
    ]
    assert all(entry.event.symbol_key == "instrument-btc|1m" for entry in prepared)
    assert all(entry.payload_bytes > 0 for entry in prepared)
    summary = TypedDeltaInstrumentation.emission_summary(prepared)
    assert summary["event_count"] == 6
    assert summary["counts_by_type"]["symbol_runtime_delta"] == 1
    assert summary["total_payload_bytes"] >= sum(entry.payload_bytes for entry in prepared)


def test_run_stream_broadcast_typed_delta_filters_to_selected_symbol_only() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        ws = FakeWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )

        eth_delta = SymbolTypedDeltaBuilder.build(
            run_id="run-1",
            symbol_key="instrument-eth|5m",
            seq=2,
            event_time="2026-01-01T00:00:02Z",
            delta={"candle": {"time": 2, "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0}},
        )[0]
        btc_delta = SymbolTypedDeltaBuilder.build(
            run_id="run-1",
            symbol_key="instrument-btc|1m",
            seq=3,
            event_time="2026-01-01T00:00:03Z",
            delta={"candle": {"time": 3, "open": 3.0, "high": 3.0, "low": 3.0, "close": 3.0}},
        )[0]

        eth_delivery = await stream.broadcast_typed_delta(eth_delta)
        btc_delivery = await stream.broadcast_typed_delta(btc_delta)

        await stream.deliver_symbol_snapshot(
            run_id="run-1",
            ws=ws,
            snapshot={
                "run_id": "run-1",
                "symbol_key": "instrument-btc|1m",
                "seq": 2,
                "detail": {
                    "symbol_key": "instrument-btc|1m",
                    "candles": [{"time": 2, "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0}],
                },
            },
        )

        assert [message["type"] for message in ws.messages] == [
            "botlens_run_connected",
            "botlens_symbol_snapshot",
            "symbol_candle_delta",
        ]
        assert ws.messages[-1]["symbol_key"] == "instrument-btc|1m"
        assert eth_delivery.viewer_count == 0
        assert eth_delivery.filtered_viewer_count == 1
        assert btc_delivery.viewer_count == 0
        assert btc_delivery.filtered_viewer_count == 0

        await stream.update_viewer_subscription(
            run_id="run-1",
            ws=ws,
            payload={"type": "set_selected_symbol", "symbol_key": "instrument-eth|5m"},
        )
        eth_delta_reselected = SymbolTypedDeltaBuilder.build(
            run_id="run-1",
            symbol_key="instrument-eth|5m",
            seq=4,
            event_time="2026-01-01T00:00:04Z",
            delta={"runtime": {"status": "running"}},
        )[0]
        reselected_delivery = await stream.broadcast_typed_delta(eth_delta_reselected)

        await stream.deliver_symbol_snapshot(
            run_id="run-1",
            ws=ws,
            snapshot={
                "run_id": "run-1",
                "symbol_key": "instrument-eth|5m",
                "seq": 3,
                "detail": {
                    "symbol_key": "instrument-eth|5m",
                    "runtime": {"status": "warming"},
                },
            },
        )

        assert ws.messages[-1]["type"] == "symbol_runtime_delta"
        assert ws.messages[-1]["symbol_key"] == "instrument-eth|5m"
        assert reselected_delivery.viewer_count == 0
        assert reselected_delivery.filtered_viewer_count == 0

    asyncio.run(scenario())

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

from core.settings import get_settings
from portal.backend.service.observability import get_observability_sink, reset_observability_sink
from portal.backend.service.bots.botlens_run_stream import BotLensRunStream
import portal.backend.service.bots.botlens_run_stream as run_stream_mod
from portal.backend.service.bots.botlens_state import (
    CandleDelta,
    DecisionDelta,
    DiagnosticDelta,
    RunFaultDelta,
    RunHealthDelta,
    RunLifecycleDelta,
    RunOpenTradesDelta,
    RunSymbolCatalogDelta,
    SeriesStatsDelta,
    SignalDelta,
    TradeDelta,
    empty_run_projection_snapshot,
)
from portal.backend.service.bots.botlens_transport import (
    BotLensTransport,
    LiveDeltaInstrumentation,
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


def test_run_stream_uses_configured_default_ring_size() -> None:
    assert BotLensRunStream()._ring_size == int(get_settings().bot_runtime.botlens.ring_size)


def test_symbol_transport_maps_internal_concern_deltas_to_transport_owned_live_contracts() -> None:
    prepared = BotLensTransport().build_symbol_prepared_deltas(
        run_id="run-1",
        deltas=(
            CandleDelta(
                symbol_key="instrument-btc|1m",
                seq=42,
                event_time="2026-01-01T00:01:00Z",
                candle={"time": 1, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0},
            ),
            SignalDelta(
                symbol_key="instrument-btc|1m",
                seq=42,
                event_time="2026-01-01T00:01:00Z",
                appended_signals=({"event_id": "signal-1", "signal_id": "signal-1"},),
            ),
            DecisionDelta(
                symbol_key="instrument-btc|1m",
                seq=42,
                event_time="2026-01-01T00:01:00Z",
                appended_decisions=({"event_id": "decision-1", "decision_id": "decision-1"},),
            ),
            DiagnosticDelta(
                symbol_key="instrument-btc|1m",
                seq=42,
                event_time="2026-01-01T00:01:00Z",
                appended_diagnostics=({"event_id": "diag-1", "id": "diag-1", "message": "runtime log"},),
            ),
            TradeDelta(
                symbol_key="instrument-btc|1m",
                seq=42,
                event_time="2026-01-01T00:01:00Z",
                trade_upserts=({"trade_id": "trade-1", "symbol_key": "instrument-btc|1m"},),
                trade_removals=(),
            ),
            SeriesStatsDelta(
                symbol_key="instrument-btc|1m",
                seq=42,
                event_time="2026-01-01T00:01:00Z",
                stats={"total_trades": 2},
            ),
        ),
    )

    assert [entry.event.message_type for entry in prepared] == [
        "botlens_symbol_candle_delta",
        "botlens_symbol_signal_delta",
        "botlens_symbol_decision_delta",
        "botlens_symbol_diagnostic_delta",
        "botlens_symbol_trade_delta",
        "botlens_symbol_stats_delta",
    ]
    assert all(entry.event.scope == "symbol" for entry in prepared)
    assert all(entry.event.concern for entry in prepared)
    summary = LiveDeltaInstrumentation.emission_summary(prepared)
    assert summary["event_count"] == 6
    assert summary["counts_by_type"]["botlens_symbol_signal_delta"] == 1


def test_run_transport_splits_run_concerns_into_separate_live_contracts() -> None:
    state = replace(
        empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1"),
        seq=12,
    )
    prepared = BotLensTransport().build_run_prepared_deltas(
        state=state,
        deltas=(
            RunLifecycleDelta(seq=12, event_time="2026-01-01T00:00:12Z", lifecycle={"status": "running"}),
            RunHealthDelta(seq=12, event_time="2026-01-01T00:00:12Z", health={"status": "running"}),
            RunFaultDelta(seq=12, event_time="2026-01-01T00:00:12Z", appended_faults=({"event_id": "fault-1"},)),
            RunSymbolCatalogDelta(
                seq=12,
                event_time="2026-01-01T00:00:12Z",
                symbol_upserts=(
                    {
                        "symbol_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTCUSD",
                        "timeframe": "1m",
                    },
                ),
                symbol_removals=(),
            ),
            RunOpenTradesDelta(
                seq=12,
                event_time="2026-01-01T00:00:12Z",
                upserts=({"trade_id": "trade-1", "symbol_key": "instrument-btc|1m"},),
                removals=(),
            ),
        ),
    )

    assert [entry.event.message_type for entry in prepared] == [
        "botlens_run_lifecycle_delta",
        "botlens_run_health_delta",
        "botlens_run_fault_delta",
        "botlens_run_symbol_catalog_delta",
        "botlens_run_open_trades_delta",
    ]
    assert all(entry.event.scope == "run" for entry in prepared)
    assert all(entry.event.symbol_key is None for entry in prepared)


def test_run_stream_filters_symbol_deltas_and_assigns_monotonic_stream_seq() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        transport = BotLensTransport()
        ws = FakeWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )

        run_state = empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1")
        run_delta = transport.build_run_prepared_deltas(
            state=run_state,
            deltas=(RunHealthDelta(seq=2, event_time="2026-01-01T00:00:02Z", health={"status": "running"}),),
        )[0]
        eth_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-eth|5m",
                    seq=3,
                    event_time="2026-01-01T00:00:03Z",
                    candle={"time": 3, "open": 3.0, "high": 3.0, "low": 3.0, "close": 3.0},
                ),
            ),
        )[0]
        btc_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-btc|1m",
                    seq=4,
                    event_time="2026-01-01T00:00:04Z",
                    candle={"time": 4, "open": 4.0, "high": 4.0, "low": 4.0, "close": 4.0},
                ),
            ),
        )[0]

        run_delivery = await stream.broadcast_live_delta(run_delta)
        eth_delivery = await stream.broadcast_live_delta(eth_delta)
        btc_delivery = await stream.broadcast_live_delta(btc_delta)

        assert [message["type"] for message in ws.messages] == [
            "botlens_live_connected",
            "botlens_run_health_delta",
            "botlens_symbol_candle_delta",
        ]
        assert ws.messages[1]["stream_seq"] == 1
        assert ws.messages[2]["stream_seq"] == 3
        assert ws.messages[1]["scope_seq"] == 2
        assert ws.messages[2]["scope_seq"] == 4
        assert run_delivery.viewer_count == 1
        assert eth_delivery.viewer_count == 0
        assert eth_delivery.filtered_viewer_count == 1
        assert btc_delivery.viewer_count == 1

    asyncio.run(scenario())


def test_run_stream_tracks_cursor_lineage_for_selected_symbol_bootstrap() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream()
        transport = BotLensTransport()
        ws = FakeWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )

        run_state = empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1")
        run_delta = transport.build_run_prepared_deltas(
            state=run_state,
            deltas=(RunHealthDelta(seq=2, event_time="2026-01-01T00:00:02Z", health={"status": "running"}),),
        )[0]
        eth_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-eth|5m",
                    seq=3,
                    event_time="2026-01-01T00:00:03Z",
                    candle={"time": 3, "open": 3.0, "high": 3.0, "low": 3.0, "close": 3.0},
                ),
            ),
        )[0]
        btc_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-btc|1m",
                    seq=4,
                    event_time="2026-01-01T00:00:04Z",
                    candle={"time": 4, "open": 4.0, "high": 4.0, "low": 4.0, "close": 4.0},
                ),
            ),
        )[0]

        await stream.broadcast_live_delta(run_delta)
        await stream.broadcast_live_delta(eth_delta)
        await stream.broadcast_live_delta(btc_delta)

        cursor = await stream.current_symbol_cursor(
            run_id="run-1",
            symbol_key="instrument-btc|1m",
        )

        assert cursor["base_seq"] == 3
        assert cursor["run_scope_seq"] == 2
        assert cursor["symbol_scope_seq"] == 4
        assert isinstance(cursor["stream_session_id"], str)

    asyncio.run(scenario())


def test_run_stream_replays_missed_deltas_from_resume_cursor() -> None:
    async def scenario() -> None:
        reset_observability_sink()
        run_stream_mod._OBSERVER._gauge_emit_at.clear()
        stream = BotLensRunStream(ring_size=8)
        transport = BotLensTransport()
        first_ws = FakeWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=first_ws,
            selected_symbol_key="instrument-btc|1m",
        )
        connected = first_ws.messages[0]

        run_state = empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1")
        run_delta = transport.build_run_prepared_deltas(
            state=run_state,
            deltas=(RunHealthDelta(seq=2, event_time="2026-01-01T00:00:02Z", health={"status": "running"}),),
        )[0]
        btc_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-btc|1m",
                    seq=4,
                    event_time="2026-01-01T00:00:04Z",
                    candle={"time": 4, "open": 4.0, "high": 4.0, "low": 4.0, "close": 4.0},
                ),
            ),
        )[0]

        await stream.broadcast_live_delta(run_delta)
        await stream.broadcast_live_delta(btc_delta)

        replay_ws = FakeWebSocket()
        await stream.add_run_viewer(
            run_id="run-1",
            ws=replay_ws,
            selected_symbol_key="instrument-btc|1m",
            stream_session_id=str(connected["stream_session_id"]),
            resume_from_seq=1,
        )

        assert [message["type"] for message in replay_ws.messages] == [
            "botlens_live_connected",
            "botlens_symbol_candle_delta",
        ]
        assert replay_ws.messages[0]["replayed_count"] == 1
        assert replay_ws.messages[1]["stream_seq"] == 2
        assert replay_ws.messages[1]["scope_seq"] == 4
        metrics = get_observability_sink().snapshot()["metrics"]
        metric_names = [metric["metric_name"] for metric in metrics]
        assert "replay_hit_total" in metric_names
        assert "replay_message_count" in metric_names
        assert "replay_requested_gap" in metric_names
        assert "replay_requested_gap_max" in metric_names
        assert "replay_ring_occupancy" in metric_names
        assert "replay_ring_high_water_mark" in metric_names

    asyncio.run(scenario())


def test_selected_symbol_subscription_replays_deltas_after_snapshot_base_seq() -> None:
    async def scenario() -> None:
        stream = BotLensRunStream(ring_size=8)
        transport = BotLensTransport()
        ws = FakeWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )
        connected = ws.messages[0]

        btc_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-btc|1m",
                    seq=1,
                    event_time="2026-01-01T00:00:01Z",
                    candle={"time": 1, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0},
                ),
            ),
        )[0]
        eth_delta = transport.build_symbol_prepared_deltas(
            run_id="run-1",
            deltas=(
                CandleDelta(
                    symbol_key="instrument-eth|5m",
                    seq=2,
                    event_time="2026-01-01T00:00:02Z",
                    candle={"time": 2, "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0},
                ),
            ),
        )[0]

        await stream.broadcast_live_delta(btc_delta)
        await stream.broadcast_live_delta(eth_delta)
        assert [message["type"] for message in ws.messages] == [
            "botlens_live_connected",
            "botlens_symbol_candle_delta",
        ]

        await stream.update_viewer_subscription(
            run_id="run-1",
            ws=ws,
            payload={
                "type": "set_selected_symbol",
                "symbol_key": "instrument-eth|5m",
                "resume_from_seq": 1,
                "stream_session_id": connected["stream_session_id"],
            },
        )

        assert [message["type"] for message in ws.messages] == [
            "botlens_live_connected",
            "botlens_symbol_candle_delta",
            "botlens_symbol_candle_delta",
        ]
        assert ws.messages[-1]["symbol_key"] == "instrument-eth|5m"
        assert ws.messages[-1]["stream_seq"] == 2

    asyncio.run(scenario())


def test_run_stream_requests_fresh_bootstrap_when_replay_window_expired() -> None:
    async def scenario() -> None:
        reset_observability_sink()
        run_stream_mod._OBSERVER._gauge_emit_at.clear()
        stream = BotLensRunStream(ring_size=1)
        transport = BotLensTransport()
        ws = FakeWebSocket()

        await stream.add_run_viewer(
            run_id="run-1",
            ws=ws,
            selected_symbol_key="instrument-btc|1m",
        )
        connected = ws.messages[0]

        run_state = empty_run_projection_snapshot(bot_id="bot-1", run_id="run-1")
        first = transport.build_run_prepared_deltas(
            state=run_state,
            deltas=(RunHealthDelta(seq=1, event_time="2026-01-01T00:00:01Z", health={"status": "running"}),),
        )[0]
        second = transport.build_run_prepared_deltas(
            state=run_state,
            deltas=(RunLifecycleDelta(seq=2, event_time="2026-01-01T00:00:02Z", lifecycle={"status": "running"}),),
        )[0]
        await stream.broadcast_live_delta(first)
        await stream.broadcast_live_delta(second)

        replay_ws = FakeWebSocket()
        await stream.add_run_viewer(
            run_id="run-1",
            ws=replay_ws,
            selected_symbol_key="instrument-btc|1m",
            stream_session_id=str(connected["stream_session_id"]),
            resume_from_seq=0,
        )

        assert replay_ws.messages[0]["type"] == "botlens_live_reset_required"
        assert replay_ws.messages[0]["reason"] == "resume_window_expired"
        assert replay_ws.closed is True
        metrics = get_observability_sink().snapshot()["metrics"]
        metric_names = [metric["metric_name"] for metric in metrics]
        assert "replay_miss_total" in metric_names
        assert "reset_required_total" in metric_names
        assert "replay_requested_gap_max" in metric_names

    asyncio.run(scenario())

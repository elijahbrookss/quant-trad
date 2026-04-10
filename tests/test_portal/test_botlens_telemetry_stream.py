from __future__ import annotations

import asyncio
import json
import time

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import telemetry_stream as stream
from portal.backend.service.bots.botlens_contract import (
    BRIDGE_BOOTSTRAP_KIND,
    BRIDGE_FACTS_KIND,
    RUN_SCOPE_KEY,
)

BotTelemetryHub = stream.BotTelemetryHub


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


def _facts_batch(
    *,
    candle_time: int,
    symbol_key: str = "instrument-btc|1m",
    symbol: str = "BTC",
    warnings: list[dict] | None = None,
) -> list[dict]:
    instrument_id, timeframe = str(symbol_key).split("|", 1)
    return [
        {
            "fact_type": "runtime_state_observed",
            "runtime": {
                "status": "running",
                "worker_count": 2,
                "active_workers": 1,
                "warnings": list(warnings or []),
            },
        },
        {
            "fact_type": "series_state_observed",
            "series_key": symbol_key,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "timeframe": timeframe,
        },
        {
            "fact_type": "candle_upserted",
            "series_key": symbol_key,
            "candle": {"time": candle_time, "open": float(candle_time), "high": float(candle_time), "low": float(candle_time), "close": float(candle_time)},
        },
        {
            "fact_type": "trade_upserted",
            "series_key": symbol_key,
            "trade": {"trade_id": "trade-1", "symbol": symbol},
        },
    ]


def test_process_bootstrap_persists_run_summary_and_symbol_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        persisted_rows: list[dict] = []
        persisted_events: list[dict] = []

        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: persisted_rows.append(dict(row)) or dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: persisted_events.append(dict(row)) or dict(row))
        monkeypatch.setattr(stream, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1", "strategy_name": "Momentum"})

        async def fake_publish_runtime_update(**kwargs) -> None:
            return None

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]

        await hub._process_ingest(
          {
              "payload": {
                  "kind": BRIDGE_BOOTSTRAP_KIND,
                  "bot_id": "bot-1",
                  "run_id": "run-1",
                  "series_key": "instrument-btc|1m",
                  "run_seq": 1,
                  "bridge_session_id": "bridge-1",
                  "bridge_seq": 1,
                  "facts": _facts_batch(candle_time=1),
                  "event_time": "2026-01-01T00:00:00Z",
                  "known_at": "2026-01-01T00:00:00Z",
              }
          }
        )

        summary_row = next(row for row in persisted_rows if row["series_key"] == RUN_SCOPE_KEY)
        detail_row = next(row for row in persisted_rows if row["series_key"] == "instrument-btc|1m")
        assert summary_row["seq"] == 1
        assert detail_row["payload"]["detail"]["symbol_key"] == "instrument-btc|1m"
        assert summary_row["payload"]["summary"]["symbol_index"]["instrument-btc|1m"]["symbol"] == "BTC"
        assert summary_row["payload"]["summary"]["open_trades_index"]["trade-1"]["symbol_key"] == "instrument-btc|1m"
        assert summary_row["payload"]["summary"]["health"]["warnings"] == []
        assert persisted_events[0]["event_type"] == "botlens.runtime_bootstrap_facts"

    asyncio.run(scenario())


def test_process_facts_broadcasts_run_scoped_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: dict(row))
        monkeypatch.setattr(stream, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1", "strategy_name": "Momentum"})

        async def fake_publish_runtime_update(**kwargs) -> None:
            return None

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]
        ws = FakeWebSocket()

        await hub.add_run_viewer(run_id="run-1", ws=ws, cursor_seq=0, selected_symbol_key="instrument-btc|1m")

        await hub._process_ingest(
            {
                "payload": {
                    "kind": BRIDGE_BOOTSTRAP_KIND,
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 1,
                    "bridge_session_id": "bridge-1",
                    "bridge_seq": 1,
                    "facts": _facts_batch(candle_time=1),
                    "event_time": "2026-01-01T00:00:00Z",
                    "known_at": "2026-01-01T00:00:00Z",
                }
            }
        )
        await hub._process_ingest(
            {
                "payload": {
                    "kind": BRIDGE_FACTS_KIND,
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 2,
                    "bridge_session_id": "bridge-1",
                    "bridge_seq": 2,
                    "facts": _facts_batch(candle_time=2),
                    "event_time": "2026-01-01T00:01:00Z",
                    "known_at": "2026-01-01T00:01:00Z",
                }
            }
        )

        message_types = [message["type"] for message in ws.messages]
        assert "botlens_run_connected" in message_types
        assert "botlens_run_summary_delta" in message_types
        assert "botlens_open_trades_delta" in message_types
        assert "botlens_symbol_detail_delta" in message_types
        detail_messages = [message for message in ws.messages if message["type"] == "botlens_symbol_detail_delta"]
        assert detail_messages[-1]["payload"]["candle"]["time"] == 2
        summary_messages = [message for message in ws.messages if message["type"] == "botlens_run_summary_delta"]
        assert summary_messages[-1]["payload"]["health"]["warnings"] == []

    asyncio.run(scenario())


def test_process_facts_carries_grouped_runtime_warnings_into_summary_health(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        persisted_rows: list[dict] = []

        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: persisted_rows.append(dict(row)) or dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: dict(row))
        monkeypatch.setattr(stream, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1", "strategy_name": "Momentum"})

        async def fake_publish_runtime_update(**kwargs) -> None:
            return None

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]

        warning = {
            "warning_id": "indicator_overlay_payload_exceeded::typed_regime::instrument-btc|1m::indicator_guard",
            "warning_type": "indicator_overlay_payload_exceeded",
            "indicator_id": "typed_regime",
            "title": "Overlay payload budget exceeded",
            "message": "typed_regime exceeded the overlay payload budget.",
            "count": 4,
            "first_seen_at": "2026-01-01T00:00:00Z",
            "last_seen_at": "2026-01-01T00:04:00Z",
        }

        await hub._process_ingest(
            {
                "payload": {
                    "kind": BRIDGE_BOOTSTRAP_KIND,
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 1,
                    "bridge_session_id": "bridge-1",
                    "bridge_seq": 1,
                    "facts": _facts_batch(candle_time=1, warnings=[warning]),
                    "event_time": "2026-01-01T00:00:00Z",
                    "known_at": "2026-01-01T00:00:00Z",
                }
            }
        )

        summary_row = next(row for row in persisted_rows if row["series_key"] == RUN_SCOPE_KEY)
        health = summary_row["payload"]["summary"]["health"]
        assert health["warning_count"] == 1
        assert health["warnings"][0]["warning_id"] == warning["warning_id"]
        assert health["warnings"][0]["count"] == 4

    asyncio.run(scenario())


def test_add_run_viewer_replays_buffered_messages_after_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        ws = FakeWebSocket()
        async with hub._run_stream._lock:  # type: ignore[attr-defined]
            hub._run_stream._run_stream_session_id["run-1"] = "session-1"  # type: ignore[attr-defined]
            hub._run_stream._run_tail_ring["run-1"].append(  # type: ignore[attr-defined]
                {
                    "type": "botlens_run_summary_delta",
                    "run_id": "run-1",
                    "seq": 2,
                    "stream_session_id": "session-1",
                    "payload": {"health": {"status": "running"}, "lifecycle": {}, "symbol_upserts": [], "symbol_removals": []},
                }
            )

        await hub.add_run_viewer(run_id="run-1", ws=ws, cursor_seq=1, selected_symbol_key="instrument-btc|1m")

        assert [message["type"] for message in ws.messages] == ["botlens_run_connected", "botlens_run_summary_delta"]

    asyncio.run(scenario())


def test_run_meta_loaded_once_per_run_when_summary_state_is_hot(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        run_meta_calls: list[str] = []

        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: dict(row))
        monkeypatch.setattr(
            stream,
            "get_bot_run",
            lambda run_id: run_meta_calls.append(str(run_id)) or {"run_id": run_id, "bot_id": "bot-1", "strategy_name": "Momentum"},
        )

        async def fake_publish_runtime_update(**kwargs) -> None:
            return None

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]

        for payload in (
            {
                "kind": BRIDGE_BOOTSTRAP_KIND,
                "bot_id": "bot-1",
                "run_id": "run-1",
                "series_key": "instrument-btc|1m",
                "run_seq": 1,
                "bridge_session_id": "bridge-1",
                "bridge_seq": 1,
                "facts": _facts_batch(candle_time=1),
                "event_time": "2026-01-01T00:00:00Z",
                "known_at": "2026-01-01T00:00:00Z",
            },
            {
                "kind": BRIDGE_FACTS_KIND,
                "bot_id": "bot-1",
                "run_id": "run-1",
                "series_key": "instrument-btc|1m",
                "run_seq": 2,
                "bridge_session_id": "bridge-1",
                "bridge_seq": 2,
                "facts": _facts_batch(candle_time=2),
                "event_time": "2026-01-01T00:01:00Z",
                "known_at": "2026-01-01T00:01:00Z",
            },
        ):
            await hub._process_ingest({"payload": payload})

        assert run_meta_calls == ["run-1"]

    asyncio.run(scenario())


def test_prune_pass_cleans_all_run_scoped_state(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        evicted_runs: list[str] = []

        async def fake_evict_run(*, run_id: str) -> None:
            evicted_runs.append(run_id)

        hub._run_stream.evict_run = fake_evict_run  # type: ignore[method-assign]

        async with hub._lock:
            hub._latest_summary_state[("bot-1", "run-1")] = {"seq": 1}
            hub._latest_detail_state[("bot-1", "run-1", "instrument-btc|1m")] = {"seq": 1}
            hub._latest_run_by_bot["bot-1"] = "run-1"
            hub._latest_run_lifecycle["run-1"] = {"phase": "completed"}
            hub._latest_lifecycle_seq["run-1"] = 1_000_000_001
            hub._run_last_activity["run-1"] = time.monotonic() - (stream._ACTIVE_RUN_TTL_S + 10.0)
            hub._run_terminal_at["run-1"] = time.monotonic() - (stream._TERMINAL_RUN_TTL_S + 10.0)

        await hub._run_prune_pass(reason="test")

        async with hub._lock:
            assert ("bot-1", "run-1") not in hub._latest_summary_state
            assert ("bot-1", "run-1", "instrument-btc|1m") not in hub._latest_detail_state
            assert "run-1" not in hub._latest_run_lifecycle
            assert "run-1" not in hub._latest_lifecycle_seq
            assert "run-1" not in hub._run_last_activity
            assert "run-1" not in hub._run_terminal_at
            assert "bot-1" not in hub._latest_run_by_bot
        assert evicted_runs == ["run-1"]

    asyncio.run(scenario())


def test_schedule_prune_throttles_repeated_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        calls: list[str] = []

        async def fake_run_prune_pass(*, reason: str) -> None:
            calls.append(reason)

        hub._run_prune_pass = fake_run_prune_pass  # type: ignore[method-assign]

        await hub._schedule_prune(reason="first")
        if hub._prune_task is not None:
            await hub._prune_task

        await hub._schedule_prune(reason="second")
        await asyncio.sleep(0)
        if hub._prune_task is not None:
            hub._prune_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await hub._prune_task

        hub._last_prune_started_monotonic = time.monotonic() - (stream._PRUNE_INTERVAL_S + 1.0)
        await hub._schedule_prune(reason="third")
        if hub._prune_task is not None:
            await hub._prune_task

        assert calls == ["first", "third"]

    asyncio.run(scenario())

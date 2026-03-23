from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import telemetry_stream as stream
from engines.bot_runtime.runtime.event_types import BOTLENS_SERIES_BOOTSTRAP, BOTLENS_SERIES_DELTA

BotTelemetryHub = stream.BotTelemetryHub


class FakeWebSocket:
    def __init__(self, *, send_delay_s: float = 0.0) -> None:
        self.accepted = False
        self.closed = False
        self.closed_code = None
        self.send_delay_s = float(send_delay_s)
        self.messages: list[dict] = []

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, payload: str) -> None:
        if self.send_delay_s > 0:
            await asyncio.sleep(self.send_delay_s)
        self.messages.append(json.loads(payload))

    async def close(self, code: int = 1000) -> None:
        self.closed = True
        self.closed_code = int(code)


async def _noop() -> None:
    return


def _projection(*, candle_time: int, series_key: str = "instrument-btc|1m", symbol: str = "BTC") -> dict:
    instrument_id, timeframe = str(series_key).split("|", 1)
    return {
        "series": [
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "candles": [
                    {
                        "time": candle_time,
                        "open": float(candle_time),
                        "high": float(candle_time),
                        "low": float(candle_time),
                        "close": float(candle_time),
                    }
                ],
                "overlays": [],
                "stats": {"total_trades": 0},
            }
        ],
        "trades": [],
        "logs": [],
        "decisions": [],
        "warnings": [],
        "runtime": {"status": "running"},
    }


def _runtime_delta(*, candle_time: int, series_key: str = "instrument-btc|1m", symbol: str = "BTC") -> dict:
    instrument_id, timeframe = str(series_key).split("|", 1)
    return {
        "event": "bar_closed",
        "runtime": {"status": "running", "warnings": ["runtime warning"]},
        "stats": {"latency_ms": 12},
        "logs": [{"message": "delta log"}],
        "decisions": [{"event": "decision"}],
        "series": [
            {
                "series_key": series_key,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "candle": {
                    "time": candle_time,
                    "open": float(candle_time),
                    "high": float(candle_time),
                    "low": float(candle_time),
                    "close": float(candle_time),
                },
                "overlay_delta": {
                    "ops": [
                        {
                            "op": "upsert",
                            "key": "overlay:regime",
                            "overlay": {
                                "type": "regime_overlay",
                                "payload": {"state": "risk_on"},
                            },
                        }
                    ]
                },
                "stats": {"total_trades": 1},
                "trades": [{"trade_id": "trade-1", "symbol": symbol}],
            }
        ],
    }


def _window(*, seq: int, series_key: str = "instrument-btc|1m") -> dict:
    projection = _projection(candle_time=seq, series_key=series_key)
    return {
        "run_id": "run-1",
        "series_key": series_key,
        "schema_version": 1,
        "seq": seq,
        "event_time": "2026-01-01T00:00:00Z",
        "window": {
            "projection": projection,
            "selected_series": projection["series"][0],
            "candles": projection["series"][0]["candles"],
            "trades": [],
            "logs": [],
            "decisions": [],
            "warnings": [],
            "runtime": {"status": "running"},
            "markers": [],
            "status": "running",
        },
    }


def test_process_ingest_bootstrap_persists_latest_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        persisted_rows: list[dict] = []
        persisted_events: list[dict] = []
        published_updates: list[dict] = []

        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: persisted_rows.append(dict(row)) or dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: persisted_events.append(dict(row)) or dict(row))

        async def fake_publish_runtime_update(**kwargs) -> None:
            published_updates.append(dict(kwargs))

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]

        await hub._process_ingest(
            {
                "payload": {
                    "kind": "botlens_series_bootstrap",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 1,
                    "series_seq": 1,
                    "projection": _projection(candle_time=1),
                    "event_time": "2026-01-01T00:00:00Z",
                    "known_at": "2026-01-01T00:00:00Z",
                }
            }
        )

        latest = hub._latest_view_state[("bot-1", "run-1", "instrument-btc|1m")]
        assert latest["seq"] == 1
        assert latest["payload"]["series"][0]["series_key"] == "instrument-btc|1m"
        assert persisted_rows[0]["series_key"] == "instrument-btc|1m"
        assert persisted_events[0]["event_type"] == BOTLENS_SERIES_BOOTSTRAP
        assert persisted_events[0]["payload"]["projection"]["series"][0]["series_key"] == "instrument-btc|1m"
        assert published_updates[0]["seq"] == 1

    asyncio.run(scenario())


def test_process_ingest_delta_materializes_projection_and_broadcasts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        persisted_rows: list[dict] = []
        persisted_events: list[dict] = []

        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: persisted_rows.append(dict(row)) or dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: persisted_events.append(dict(row)) or dict(row))

        async def fake_publish_runtime_update(**kwargs) -> None:
            return None

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]
        ws = FakeWebSocket()

        async with hub._lock:
            hub._run_stream_session_id["run-1"] = "session-1"
            hub._series_viewers[("run-1", "instrument-btc|1m")][ws] = {"last_seq": 1, "replaying": False}

        await hub._process_ingest(
            {
                "payload": {
                    "kind": "botlens_series_bootstrap",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 1,
                    "series_seq": 1,
                    "projection": _projection(candle_time=1),
                    "event_time": "2026-01-01T00:00:00Z",
                    "known_at": "2026-01-01T00:00:00Z",
                }
            }
        )
        await hub._process_ingest(
            {
                "payload": {
                    "kind": "botlens_series_delta",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 2,
                    "series_seq": 2,
                    "runtime_delta": _runtime_delta(candle_time=2),
                    "event_time": "2026-01-01T00:01:00Z",
                    "known_at": "2026-01-01T00:01:00Z",
                }
            }
        )

        latest = hub._latest_view_state[("bot-1", "run-1", "instrument-btc|1m")]
        assert latest["seq"] == 2
        assert [row["time"] for row in latest["payload"]["series"][0]["candles"]] == [1, 2]
        assert latest["payload"]["series"][0]["overlays"][0]["overlay_id"] == "overlay:regime"
        assert latest["payload"]["series"][0]["stats"]["total_trades"] == 1
        assert latest["payload"]["trades"][0]["trade_id"] == "trade-1"
        assert latest["payload"]["warnings"] == ["runtime warning"]
        assert ws.messages[-1]["type"] == "botlens_live_tail"
        assert ws.messages[-1]["message_type"] == "series_delta"
        assert ws.messages[-1]["stream_session_id"] == "session-1"
        assert persisted_events[-1]["event_type"] == BOTLENS_SERIES_DELTA
        assert persisted_rows[-1]["seq"] == 2

    asyncio.run(scenario())


def test_process_ingest_delta_invalidates_run_on_series_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        invalidations: list[dict] = []

        monkeypatch.setattr(stream, "upsert_bot_run_view_state", lambda row: dict(row))
        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: dict(row))

        async def fake_publish_runtime_update(**kwargs) -> None:
            return None

        async def fake_invalidate_run_live_continuity(**kwargs) -> None:
            invalidations.append(dict(kwargs))

        hub._publish_runtime_update = fake_publish_runtime_update  # type: ignore[method-assign]
        hub._invalidate_run_live_continuity = fake_invalidate_run_live_continuity  # type: ignore[method-assign]
        hub._latest_view_state[("bot-1", "run-1", "instrument-btc|1m")] = {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "series_key": "instrument-btc|1m",
            "seq": 2,
            "schema_version": 1,
            "payload": _projection(candle_time=2),
            "event_time": "2026-01-01T00:01:00Z",
            "known_at": "2026-01-01T00:01:00Z",
        }

        await hub._process_ingest(
            {
                "payload": {
                    "kind": "botlens_series_delta",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 4,
                    "series_seq": 4,
                    "runtime_delta": _runtime_delta(candle_time=4),
                    "event_time": "2026-01-01T00:03:00Z",
                    "known_at": "2026-01-01T00:03:00Z",
                }
            }
        )

        assert invalidations == [
            {
                "run_id": "run-1",
                "reason": "seq_gap",
                "details": {
                    "series_key": "instrument-btc|1m",
                    "previous_seq": 2,
                    "incoming_seq": 4,
                    "seq_gap": 1,
                },
            }
        ]

    asyncio.run(scenario())


def test_add_series_viewer_sends_atomic_bootstrap_then_replays_buffered_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        hub._ensure_workers = _noop  # type: ignore[method-assign]
        monkeypatch.setattr(stream, "get_series_window", lambda **kwargs: _window(seq=10))
        hub._series_live_tail_ring[("run-1", "instrument-btc|1m")].append(
            {
                "type": "botlens_live_tail",
                "run_id": "run-1",
                "series_key": "instrument-btc|1m",
                "schema_version": 1,
                "seq": 11,
                "known_at": "2026-01-01T00:01:00Z",
                "message_type": "series_delta",
                "stream_session_id": "session-a",
                "payload": {
                    "event": "bar_closed",
                    "runtime": {"status": "running"},
                    "stats": {},
                    "logs": [],
                    "decisions": [],
                    "series_delta": {
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "candle": {"time": 11, "open": 11, "high": 11, "low": 11, "close": 11},
                    },
                },
            }
        )
        ws = FakeWebSocket()

        await hub.add_series_viewer(run_id="run-1", series_key="instrument-btc|1m", ws=ws)

        assert ws.accepted is True
        assert [message["type"] for message in ws.messages] == ["botlens_live_bootstrap", "botlens_live_tail"]
        assert [message["seq"] for message in ws.messages] == [10, 11]

    asyncio.run(scenario())


def test_add_series_viewer_emits_resync_when_replay_buffer_has_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        hub._ensure_workers = _noop  # type: ignore[method-assign]
        monkeypatch.setattr(stream, "get_series_window", lambda **kwargs: _window(seq=4))
        hub._series_live_tail_ring[("run-1", "instrument-btc|1m")].append(
            {
                "type": "botlens_live_tail",
                "run_id": "run-1",
                "series_key": "instrument-btc|1m",
                "schema_version": 1,
                "seq": 7,
                "known_at": "2026-01-01T00:01:00Z",
                "message_type": "series_delta",
                "stream_session_id": "session-a",
                "payload": {
                    "event": "bar_closed",
                    "runtime": {"status": "running"},
                    "stats": {},
                    "logs": [],
                    "decisions": [],
                    "series_delta": {
                        "series_key": "instrument-btc|1m",
                        "instrument_id": "instrument-btc",
                        "symbol": "BTC",
                        "timeframe": "1m",
                        "candle": {"time": 7, "open": 7, "high": 7, "low": 7, "close": 7},
                    },
                },
            }
        )
        ws = FakeWebSocket()

        await hub.add_series_viewer(run_id="run-1", series_key="instrument-btc|1m", ws=ws)

        assert [message["type"] for message in ws.messages] == [
            "botlens_live_bootstrap",
            "botlens_live_resync_required",
        ]
        assert ws.closed is True
        assert ws.closed_code == 1013
        assert ("run-1", "instrument-btc|1m") not in hub._series_viewers

    asyncio.run(scenario())

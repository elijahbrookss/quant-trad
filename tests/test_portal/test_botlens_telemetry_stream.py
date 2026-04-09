from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import telemetry_stream as stream
from portal.backend.service.bots.container_runtime import ContainerStartupContext, _handle_worker_error
from portal.backend.service.bots.botlens_contract import (
    BRIDGE_BOOTSTRAP_KIND,
    BRIDGE_FACTS_KIND,
    CONTINUITY_RESYNC_REQUIRED,
    EVENT_TYPE_LIFECYCLE,
    EVENT_TYPE_RUNTIME_FACTS,
    EVENT_TYPE_RUNTIME_BOOTSTRAP,
    LIFECYCLE_KIND,
)

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


def _facts_batch(*, candle_time: int, series_key: str = "instrument-btc|1m", symbol: str = "BTC") -> list[dict]:
    instrument_id, timeframe = str(series_key).split("|", 1)
    return [
        {"fact_type": "runtime_state_observed", "runtime": {"status": "running", "warnings": ["runtime warning"]}},
        {
            "fact_type": "series_state_observed",
            "series_key": series_key,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "timeframe": timeframe,
        },
        {
            "fact_type": "candle_upserted",
            "series_key": series_key,
            "candle": {
                "time": candle_time,
                "open": float(candle_time),
                "high": float(candle_time),
                "low": float(candle_time),
                "close": float(candle_time),
            },
            "replace_last": False,
        },
        {
            "fact_type": "overlay_ops_emitted",
            "series_key": series_key,
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
        },
        {
            "fact_type": "series_stats_updated",
            "series_key": series_key,
            "stats": {"total_trades": 1},
        },
        {
            "fact_type": "trade_upserted",
            "series_key": series_key,
            "trade": {"trade_id": "trade-1", "symbol": symbol},
        },
        {"fact_type": "log_emitted", "log": {"id": "log-1", "message": "delta log"}},
        {"fact_type": "decision_emitted", "decision": {"event_id": "decision-1", "event": "decision"}},
    ]


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

        latest = hub._latest_view_state[("bot-1", "run-1", "instrument-btc|1m")]
        assert latest["seq"] == 1
        assert latest["payload"]["projection"]["series"][0]["series_key"] == "instrument-btc|1m"
        assert persisted_rows[0]["series_key"] == "instrument-btc|1m"
        assert persisted_events[0]["event_type"] == EVENT_TYPE_RUNTIME_BOOTSTRAP
        assert persisted_events[0]["payload"]["facts"][0]["fact_type"] == "runtime_state_observed"
        assert published_updates[0]["seq"] == 1

    asyncio.run(scenario())


def test_process_ingest_facts_materializes_projection_and_broadcasts(monkeypatch: pytest.MonkeyPatch) -> None:
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

        latest = hub._latest_view_state[("bot-1", "run-1", "instrument-btc|1m")]
        assert latest["seq"] == 2
        assert [row["time"] for row in latest["payload"]["projection"]["series"][0]["candles"]] == [1, 2]
        assert latest["payload"]["projection"]["series"][0]["overlays"][0]["overlay_id"] == "overlay:regime"
        assert latest["payload"]["projection"]["series"][0]["stats"]["total_trades"] == 1
        assert latest["payload"]["projection"]["trades"][0]["trade_id"] == "trade-1"
        assert latest["payload"]["projection"]["warnings"] == ["runtime warning"]
        assert ws.messages[-1]["type"] == "botlens_live_tail"
        assert ws.messages[-1]["message_type"] == "projection_update"
        assert ws.messages[-1]["stream_session_id"] == "session-1"
        assert ws.messages[-1]["payload"]["window"]["projection"]["series"][0]["candles"][-1]["time"] == 2
        assert persisted_events[-1]["event_type"] == EVENT_TYPE_RUNTIME_FACTS
        assert persisted_rows[-1]["seq"] == 2

    asyncio.run(scenario())


def test_process_ingest_facts_invalidates_run_on_series_gap(monkeypatch: pytest.MonkeyPatch) -> None:
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
            "payload": {
                "projection": _projection(candle_time=2),
                "continuity": {"status": "ready", "bridge_session_id": "bridge-1", "last_bridge_seq": 2, "details": {}},
            },
            "event_time": "2026-01-01T00:01:00Z",
            "known_at": "2026-01-01T00:01:00Z",
        }

        await hub._process_ingest(
            {
                "payload": {
                    "kind": BRIDGE_FACTS_KIND,
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "series_key": "instrument-btc|1m",
                    "run_seq": 4,
                    "bridge_session_id": "bridge-1",
                    "bridge_seq": 4,
                    "facts": _facts_batch(candle_time=4),
                    "event_time": "2026-01-01T00:03:00Z",
                    "known_at": "2026-01-01T00:03:00Z",
                }
            }
        )

        assert invalidations == [
            {
                "run_id": "run-1",
                "reason": "bridge_seq_gap",
                "details": {
                    "series_key": "instrument-btc|1m",
                    "previous_bridge_seq": 2,
                    "incoming_bridge_seq": 4,
                    "bridge_gap": 1,
                },
            }
        ]
        latest = hub._latest_view_state[("bot-1", "run-1", "instrument-btc|1m")]
        assert latest["payload"]["continuity"]["status"] == CONTINUITY_RESYNC_REQUIRED
        assert [row["time"] for row in latest["payload"]["projection"]["series"][0]["candles"]] == [2]

    asyncio.run(scenario())


def test_process_ingest_lifecycle_event_persists_and_broadcasts(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        persisted_events: list[dict] = []
        refreshed: list[str] = []
        lifecycle_updates: list[dict] = []

        monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: persisted_events.append(dict(row)) or dict(row))

        async def fake_publish_projected_bot(*, bot_id: str) -> None:
            refreshed.append(bot_id)

        async def fake_broadcast_run_lifecycle(*, run_id: str, lifecycle: dict) -> None:
            lifecycle_updates.append({"run_id": run_id, "lifecycle": dict(lifecycle)})

        hub._publish_projected_bot = fake_publish_projected_bot  # type: ignore[method-assign]
        hub._live_series.broadcast_run_lifecycle = fake_broadcast_run_lifecycle  # type: ignore[method-assign]

        await hub._process_ingest(
            {
                "payload": {
                    "kind": LIFECYCLE_KIND,
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "seq": 7,
                    "phase": "live",
                    "status": "running",
                    "owner": "supervisor",
                    "message": "runtime entered live mode",
                    "checkpoint_at": "2026-01-01T00:02:00Z",
                    "metadata": {"source": "startup"},
                }
            }
        )

        assert persisted_events[0]["event_type"] == EVENT_TYPE_LIFECYCLE
        assert persisted_events[0]["payload"]["phase"] == "live"
        assert refreshed == ["bot-1"]
        assert lifecycle_updates == [
            {
                "run_id": "run-1",
                "lifecycle": {
                    "run_id": "run-1",
                    "phase": "live",
                    "status": "running",
                    "owner": "supervisor",
                    "message": "runtime entered live mode",
                    "checkpoint_at": "2026-01-01T00:02:00Z",
                    "updated_at": "2026-01-01T00:02:00Z",
                    "metadata": {"source": "startup"},
                    "live": True,
                },
            }
        ]

    asyncio.run(scenario())


def test_process_ingest_projection_refresh_publishes_projected_bot_without_container_inspect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        published: list[tuple[str, bool]] = []

        monkeypatch.setattr(
            stream,
            "publish_projected_bot",
            lambda bot_id, *, inspect_container=True: published.append((str(bot_id), bool(inspect_container))),
        )

        await hub._process_ingest(
            {
                "payload": {
                    "kind": "bot_projection_refresh",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "phase": "startup_failed",
                    "status": "startup_failed",
                }
            }
        )

        assert published == [("bot-1", False)]

    asyncio.run(scenario())


def test_process_ingest_projection_refresh_broadcasts_projected_bot(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        hub = BotTelemetryHub()
        refreshed: list[dict] = []

        def fake_publish_projected_bot(bot_id: str, inspect_container: bool = True) -> None:
            refreshed.append({"bot_id": bot_id, "inspect_container": inspect_container})

        monkeypatch.setattr(stream, "publish_projected_bot", fake_publish_projected_bot)

        await hub._process_ingest(
            {
                "payload": {
                    "kind": "bot_projection_refresh",
                    "bot_id": "bot-1",
                    "run_id": "run-1",
                    "phase": "startup_failed",
                    "status": "startup_failed",
                    "known_at": "2026-04-09T04:21:43Z",
                }
            }
        )

        assert refreshed == [{"bot_id": "bot-1", "inspect_container": False}]

    asyncio.run(scenario())


def test_lifecycle_event_seq_stays_monotonic_and_within_int32(monkeypatch: pytest.MonkeyPatch) -> None:
    hub = BotTelemetryHub()
    persisted_events: list[dict] = []

    monkeypatch.setattr(
        stream,
        "get_latest_bot_runtime_event",
        lambda **_kwargs: {
            "seq": 1_000_000_123,
            "event_type": EVENT_TYPE_LIFECYCLE,
        },
    )
    monkeypatch.setattr(stream, "record_bot_runtime_event", lambda row: persisted_events.append(dict(row)) or dict(row))

    asyncio.run(
        hub._persist_lifecycle_event(
            bot_id="bot-1",
            run_id="run-1",
            lifecycle={
                "phase": "runtime_subscribing",
                "status": "starting",
                "checkpoint_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        )
    )
    asyncio.run(
        hub._persist_lifecycle_event(
            bot_id="bot-1",
            run_id="run-1",
            lifecycle={
                "phase": "degraded",
                "status": "degraded",
                "checkpoint_at": "2026-01-01T00:01:00Z",
                "updated_at": "2026-01-01T00:01:00Z",
            },
        )
    )

    assert persisted_events[0]["seq"] == 1_000_000_124
    assert persisted_events[1]["seq"] == 1_000_000_125
    assert persisted_events[1]["seq"] < 2_147_483_647


def test_series_event_id_is_deterministic_and_fits_storage_limit() -> None:
    event_id = stream._series_event_id(
        bot_id="83bd32b2-79e7-4c05-ab3d-d7f3fbb7ca4d",
        run_id="b88fbd56-edac-4a4d-b951-e3b0aa64edf8",
        event_type=EVENT_TYPE_RUNTIME_FACTS,
        series_key="c209795e-1c91-4562-9b1d-ac6ffcbaf63c|1h",
        bridge_session_id="f3c9e829a2fe840a0ee56da2c18ad6a8a55a89b68c67-extra-padding-to-force-overflow",
        bridge_seq=1775728240754846,
        projection_seq=400,
    )

    assert len(event_id) <= 128
    assert event_id == stream._series_event_id(
        bot_id="83bd32b2-79e7-4c05-ab3d-d7f3fbb7ca4d",
        run_id="b88fbd56-edac-4a4d-b951-e3b0aa64edf8",
        event_type=EVENT_TYPE_RUNTIME_FACTS,
        series_key="c209795e-1c91-4562-9b1d-ac6ffcbaf63c|1h",
        bridge_session_id="f3c9e829a2fe840a0ee56da2c18ad6a8a55a89b68c67-extra-padding-to-force-overflow",
        bridge_seq=1775728240754846,
        projection_seq=400,
    )


def test_worker_failure_with_surviving_runtime_marks_lifecycle_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    persisted: list[dict] = []

    monkeypatch.setattr(
        "portal.backend.service.bots.container_runtime._persist_lifecycle_phase",
        lambda **kwargs: persisted.append(dict(kwargs)) or dict(kwargs),
    )

    ctx = ContainerStartupContext(
        bot_id="bot-1",
        run_id="run-1",
        bot={},
        runtime_bot_config={},
        strategy_id="strategy-1",
        symbols=["BTCUSDT", "ETHUSDT"],
        symbol_shards=[["BTCUSDT"], ["ETHUSDT"]],
        wallet_config={},
        manager=type("Manager", (), {"shutdown": lambda self: None})(),
        shared_wallet_proxy={},
        worker_symbols={"worker-1": ["BTCUSDT"], "worker-2": ["ETHUSDT"]},
        children={
            "worker-1": type("Proc", (), {"exitcode": 1})(),
            "worker-2": type("Proc", (), {"exitcode": None})(),
        },
    )

    _handle_worker_error(ctx, "worker-1", error="worker-1 exited with code 1", exit_code=1)

    assert ctx.series_states["BTCUSDT"]["status"] == "failed"
    assert persisted[-1]["phase"] == "degraded"
    assert persisted[-1]["status"] == "degraded"


def test_add_series_viewer_sends_atomic_bootstrap_then_replays_buffered_facts(monkeypatch: pytest.MonkeyPatch) -> None:
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
                "message_type": "projection_update",
                "stream_session_id": "session-a",
                "payload": {
                    "cursor": {"projection_seq": 11},
                    "continuity": {"status": "ready", "bridge_session_id": "bridge-1", "last_bridge_seq": 11, "details": {}},
                    "lifecycle": {"phase": "live", "status": "running", "live": True},
                    "window": {
                        "projection": _projection(candle_time=11),
                        "selected_series": _projection(candle_time=11)["series"][0],
                        "candles": _projection(candle_time=11)["series"][0]["candles"],
                        "trades": [],
                        "logs": [],
                        "decisions": [],
                        "warnings": [],
                        "runtime": {"status": "running"},
                        "markers": [],
                        "status": "running",
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
                "message_type": "projection_update",
                "stream_session_id": "session-a",
                "payload": {
                    "cursor": {"projection_seq": 7},
                    "continuity": {"status": "ready", "bridge_session_id": "bridge-1", "last_bridge_seq": 7, "details": {}},
                    "lifecycle": {"phase": "live", "status": "running", "live": True},
                    "window": {
                        "projection": _projection(candle_time=7),
                        "selected_series": _projection(candle_time=7)["series"][0],
                        "candles": _projection(candle_time=7)["series"][0]["candles"],
                        "trades": [],
                        "logs": [],
                        "decisions": [],
                        "warnings": [],
                        "runtime": {"status": "running"},
                        "markers": [],
                        "status": "running",
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

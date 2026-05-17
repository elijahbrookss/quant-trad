from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import botlens_bootstrap_service as svc
from portal.backend.service.bots.botlens_state import (
    SymbolCandlesState,
    SymbolReadinessState,
    empty_symbol_projection_snapshot,
)
from portal.backend.service.observability import get_observability_sink, reset_observability_sink


def _summary_payload() -> dict:
    return {
        "summary": {
            "seq": 9,
            "health": {
                "status": "running",
                "phase": "live",
                "warning_count": 1,
                "warnings": [
                    {
                        "warning_id": "indicator_overlay_payload_exceeded::typed_regime::instrument-eth|5m::indicator_guard",
                        "warning_type": "indicator_overlay_payload_exceeded",
                        "indicator_id": "typed_regime",
                        "title": "Overlay payload budget exceeded",
                        "message": "typed_regime exceeded the overlay payload budget.",
                        "count": 3,
                        "first_seen_at": "2026-04-09T10:00:00Z",
                        "last_seen_at": "2026-04-09T10:02:00Z",
                    }
                ],
            },
            "symbol_index": {
                "instrument-btc|1m": {
                    "symbol_key": "instrument-btc|1m",
                    "symbol": "BTC-USD",
                    "timeframe": "1m",
                    "instrument_id": "instrument-btc",
                    "last_activity_at": "2026-04-09T10:01:00Z",
                },
                "instrument-eth|5m": {
                    "symbol_key": "instrument-eth|5m",
                    "symbol": "ETH-USD",
                    "timeframe": "5m",
                    "instrument_id": "instrument-eth",
                    "last_activity_at": "2026-04-09T10:02:00Z",
                },
            },
            "open_trades_index": {
                "trade-1": {
                    "trade_id": "trade-1",
                    "symbol": "ETH-USD",
                    "symbol_key": "instrument-eth|5m",
                }
            },
        }
    }


def _run_state() -> SimpleNamespace:
    entries = _summary_payload()["summary"]["symbol_index"]
    return SimpleNamespace(
        seq=9,
        lifecycle=SimpleNamespace(
            phase="live",
            status="running",
            to_dict=lambda: {"phase": "live", "status": "running", "live": True},
        ),
        health=SimpleNamespace(
            to_dict=lambda: {
                "status": "running",
                "phase": "live",
                "warning_count": 1,
                "warnings": [
                    {
                        "warning_id": "indicator_overlay_payload_exceeded::typed_regime::instrument-eth|5m::indicator_guard",
                        "warning_type": "indicator_overlay_payload_exceeded",
                        "indicator_id": "typed_regime",
                        "title": "Overlay payload budget exceeded",
                        "message": "typed_regime exceeded the overlay payload budget.",
                        "count": 3,
                        "first_seen_at": "2026-04-09T10:00:00Z",
                        "last_seen_at": "2026-04-09T10:02:00Z",
                    }
                ],
            }
        ),
        symbol_catalog=SimpleNamespace(entries=entries),
        open_trades=SimpleNamespace(
            entries={
                "trade-1": {
                    "trade_id": "trade-1",
                    "symbol": "ETH-USD",
                    "symbol_key": "instrument-eth|5m",
                }
            }
        ),
        readiness=SimpleNamespace(catalog_discovered=True, run_live=True),
    )


class _FakeTelemetryHub:
    def __init__(self):
        self.run_state = _run_state()
        self.symbol_state = replace(
            empty_symbol_projection_snapshot("instrument-eth|5m"),
            seq=12,
            readiness=SymbolReadinessState(snapshot_ready=True, symbol_live=True),
            candles=SymbolCandlesState(
                candles=(
                    {
                        "time": "2026-04-09T10:00:00Z",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                    },
                )
            ),
        )

    async def ensure_run_snapshot(self, **kwargs):
        return self.run_state

    async def ensure_symbol_snapshot(self, **kwargs):
        return self.symbol_state

    async def current_cursor(self, **kwargs):
        return {"base_seq": 14, "stream_session_id": "stream-1"}


def test_get_active_botlens_run_bootstrap_returns_inactive_without_active_run(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "idle",
            "active_run_id": None,
            "lifecycle": {"phase": "idle", "status": "idle"},
        },
    )

    result = asyncio.run(svc.get_active_botlens_run_bootstrap(bot_id="bot-1"))

    assert result["contract"] == "botlens_run_bootstrap"
    assert result["state"] == "inactive"
    assert result["contract_state"] == "inactive"
    assert result["live_transport"]["eligible"] is False
    assert result["readiness"] == {
        "catalog_discovered": False,
        "snapshot_ready": False,
        "symbol_live": False,
        "run_live": False,
    }
    assert result["run"]["meta"] is None
    assert result["navigation"]["selected_symbol_key"] is None


def test_get_active_botlens_run_bootstrap_is_run_scoped_and_embeds_selected_symbol_state(monkeypatch) -> None:
    reset_observability_sink()
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "running",
            "active_run_id": "run-1",
            "lifecycle": {"phase": "live", "status": "running"},
        },
    )
    monkeypatch.setattr(
        svc,
        "get_bot_run",
        lambda run_id: {
            "run_id": run_id,
            "bot_id": "bot-1",
            "strategy_name": "Momentum Variant A",
            "status": "running",
            "started_at": "2026-04-09T10:00:00Z",
        },
    )
    result = asyncio.run(svc.get_active_botlens_run_bootstrap(bot_id="bot-1"))

    assert result["state"] == "ready"
    assert result["contract_state"] == "bootstrap_ready"
    assert result["scope"]["run_id"] == "run-1"
    assert result["readiness"] == {
        "catalog_discovered": True,
        "snapshot_ready": True,
        "symbol_live": True,
        "run_live": True,
    }
    assert result["run"]["meta"]["run_id"] == "run-1"
    assert result["navigation"]["selected_symbol_key"] == "instrument-eth|5m"
    assert result["run"]["health"]["warnings"][0]["indicator_id"] == "typed_regime"
    assert [entry["identity"]["display_label"] for entry in result["navigation"]["symbols"]] == ["BTC-USD · 1m", "ETH-USD · 5m"]
    assert result["selected_symbol"]["metadata"]["symbol_key"] == "instrument-eth|5m"
    assert result["selected_symbol"]["metadata"]["readiness"]["snapshot_ready"] is True
    assert result["selected_symbol"]["current"]["continuity"]["candle_count"] == 1
    assert result["bootstrap"]["base_seq"] == 14
    assert "detail" not in result
    metrics = get_observability_sink().snapshot()["metrics"]
    metric_names = {metric["metric_name"] for metric in metrics}
    assert "botlens_run_bootstrap_request_ms" in metric_names
    assert "botlens_run_bootstrap_response_payload_bytes" in metric_names
    assert "botlens_projection_read_ms" in metric_names
    projection_reads = [metric for metric in metrics if metric["metric_name"] == "botlens_projection_read_ms"]
    assert {metric["tags"]["source_reason"] for metric in projection_reads} == {
        "ensure_run_snapshot",
        "ensure_symbol_snapshot",
    }
    assert all(metric["tags"]["pipeline_stage"] == "botlens_run_bootstrap" for metric in projection_reads)


def test_get_active_botlens_run_bootstrap_does_not_persist_observer_continuity_by_default(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(svc, "should_persist_observer_continuity", lambda **kwargs: False)
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "running",
            "active_run_id": "run-1",
            "lifecycle": {"phase": "live", "status": "running"},
        },
    )
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "emit_candle_continuity_summary",
        lambda *args, **kwargs: pytest.fail("ordinary BotLens bootstrap reads must not durably emit observer continuity"),
    )

    result = asyncio.run(svc.get_active_botlens_run_bootstrap(bot_id="bot-1"))

    assert result["selected_symbol"]["current"]["continuity"]["candle_count"] == 1


def test_get_active_botlens_run_bootstrap_can_persist_diagnostic_continuity_in_debug_mode(monkeypatch) -> None:
    captured: list[dict] = []
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(svc, "should_persist_observer_continuity", lambda **kwargs: True)
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "running",
            "active_run_id": "run-1",
            "lifecycle": {"phase": "live", "status": "running"},
        },
    )
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})

    def _emit(*args, **kwargs):
        _ = args
        captured.append(dict(kwargs))
        return {}

    monkeypatch.setattr(svc, "emit_candle_continuity_summary", _emit)

    asyncio.run(svc.get_active_botlens_run_bootstrap(bot_id="bot-1"))

    assert captured
    assert captured[0]["boundary_name"] == "run_bootstrap_selected_symbol"
    assert captured[0]["message_kind"] == "ephemeral"
    assert captured[0]["extra"]["materiality"] == "diagnostic"
    assert captured[0]["extra"]["diagnostic_scope"] == "botlens_observer"


def test_get_active_botlens_run_bootstrap_passes_internal_state_only_to_run_bootstrap_contract(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: _FakeTelemetryHub())
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "running",
            "active_run_id": "run-1",
            "lifecycle": {"phase": "live", "status": "running"},
        },
    )
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(svc, "run_bootstrap_contract", lambda **kwargs: kwargs)

    result = asyncio.run(svc.get_active_botlens_run_bootstrap(bot_id="bot-1"))

    assert result["selected_symbol_key"] == "instrument-eth|5m"
    assert result["selected_symbol_state"].symbol_key == "instrument-eth|5m"
    assert result["symbol_catalog"]["instrument-btc|1m"]["symbol"] == "BTC-USD"
    assert result["run_live"] is True
    assert result["transport_eligible"] is True
    assert result["base_seq"] == 14
    assert result["stream_session_id"] == "stream-1"


def test_get_active_botlens_run_bootstrap_surfaces_granular_startup_wait_state(monkeypatch) -> None:
    telemetry_hub = _FakeTelemetryHub()
    telemetry_hub.run_state = SimpleNamespace(
        seq=4,
        lifecycle=SimpleNamespace(
            phase="awaiting_first_snapshot",
            status="starting",
            to_dict=lambda: {
                "phase": "awaiting_first_snapshot",
                "status": "starting",
                "message": "Series bootstrap completed; waiting for first live runtime facts.",
                "metadata": {
                    "series_progress": {
                        "total_series": 3,
                        "bootstrapped_series": ["BTC", "ETH", "SOL"],
                        "live_series": [],
                    }
                },
            },
        ),
        health=SimpleNamespace(to_dict=lambda: {"status": "starting", "phase": "awaiting_first_snapshot"}),
        symbol_catalog=SimpleNamespace(entries={}),
        open_trades=SimpleNamespace(entries={}),
        readiness=SimpleNamespace(catalog_discovered=False, run_live=False),
    )
    monkeypatch.setattr(svc, "_telemetry_hub", lambda: telemetry_hub)
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "starting",
            "active_run_id": "run-1",
            "lifecycle": {"phase": "awaiting_first_snapshot", "status": "starting"},
        },
    )
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})

    result = asyncio.run(svc.get_active_botlens_run_bootstrap(bot_id="bot-1"))

    assert result["state"] == "awaiting_first_snapshot"
    assert result["contract_state"] == "awaiting_first_snapshot"
    assert result["message"] == "Bootstrap completed; waiting for first live runtime facts (0/3 series live)."


def test_resolve_active_botlens_stream_returns_run_scope_only(monkeypatch) -> None:
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": "bot-1",
            "active_run_id": "run-1",
        },
    )

    result = svc.resolve_active_botlens_stream(bot_id="bot-1")

    assert result == {
        "run_id": "run-1",
        "run_bootstrap": {
            "scope": {"bot_id": "bot-1", "run_id": "run-1"},
        },
    }

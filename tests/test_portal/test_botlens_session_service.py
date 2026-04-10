from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import botlens_session_service as svc
from portal.backend.service.bots.botlens_contract import RUN_SCOPE_KEY


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
                    "display_label": "BTC-USD · 1m",
                    "last_activity_at": "2026-04-09T10:01:00Z",
                },
                "instrument-eth|5m": {
                    "symbol_key": "instrument-eth|5m",
                    "symbol": "ETH-USD",
                    "timeframe": "5m",
                    "display_label": "ETH-USD · 5m",
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


def test_get_active_botlens_session_returns_inactive_without_active_run(monkeypatch) -> None:
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

    result = svc.get_active_botlens_session(bot_id="bot-1")

    assert result["state"] == "inactive"
    assert result["live"] is False
    assert result["run_meta"] is None
    assert result["selected_symbol_key"] is None


def test_get_active_botlens_session_selects_open_trade_symbol(monkeypatch) -> None:
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
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {"payload": _summary_payload()} if kwargs.get("series_key") == RUN_SCOPE_KEY else None,
    )
    monkeypatch.setattr(
        svc,
        "get_symbol_detail",
        lambda **kwargs: {
            "run_id": kwargs["run_id"],
            "symbol_key": kwargs["symbol_key"],
            "seq": 9,
            "detail": {
                "symbol_key": kwargs["symbol_key"],
                "symbol": "ETH-USD",
                "timeframe": "5m",
                "candles": [],
                "overlays": [],
            },
        },
    )

    result = svc.get_active_botlens_session(bot_id="bot-1")

    assert result["state"] == "ready"
    assert result["run_meta"]["run_id"] == "run-1"
    assert result["selected_symbol_key"] == "instrument-eth|5m"
    assert result["health"]["warnings"][0]["indicator_id"] == "typed_regime"
    assert [entry["display_label"] for entry in result["symbol_summaries"]] == ["BTC-USD · 1m", "ETH-USD · 5m"]
    assert result["detail"]["symbol_key"] == "instrument-eth|5m"


def test_get_active_botlens_session_rejects_missing_requested_symbol(monkeypatch) -> None:
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
        "get_latest_bot_run_view_state",
        lambda **kwargs: {"payload": _summary_payload()} if kwargs.get("series_key") == RUN_SCOPE_KEY else None,
    )

    result = svc.get_active_botlens_session(bot_id="bot-1", symbol_key="instrument-sol|1m")

    assert result["state"] == "symbol_unavailable"
    assert result["detail"] is None
    assert result["selected_symbol_key"] is None
    assert result["symbol_summaries"][0]["display_label"] == "BTC-USD · 1m"


def test_resolve_active_botlens_stream_returns_active_run_and_selected_symbol(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
        "get_active_botlens_session",
        lambda **kwargs: {
            "state": "ready",
            "run_meta": {"run_id": "run-1"},
            "selected_symbol_key": "instrument-btc|1m",
        },
    )

    result = svc.resolve_active_botlens_stream(bot_id="bot-1", symbol_key="instrument-btc|1m")

    assert result == {
        "run_id": "run-1",
        "selected_symbol_key": "instrument-btc|1m",
        "session": {
            "state": "ready",
            "run_meta": {"run_id": "run-1"},
            "selected_symbol_key": "instrument-btc|1m",
        },
    }

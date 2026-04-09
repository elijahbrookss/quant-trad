from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import botlens_session_service as svc


def _projection(*, series_key: str, symbol: str, timeframe: str) -> dict:
    instrument_id, _ = str(series_key).split("|", 1)
    return {
        "series": [
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "candles": [{"time": 1, "open": 1, "high": 1, "low": 1, "close": 1}],
                "overlays": [],
                "stats": {"total_trades": 1},
            }
        ],
        "trades": [],
        "logs": [],
        "decisions": [],
        "warnings": [],
        "runtime": {"status": "running"},
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
    assert result["run"] is None
    assert result["series_catalog"] == []
    assert result["selected_series_key"] is None


def test_get_active_botlens_session_uses_latest_view_row_series_as_default(monkeypatch) -> None:
    monkeypatch.setattr(
        svc.bot_service,
        "get_bot",
        lambda bot_id: {
            "id": bot_id,
            "status": "running",
            "active_run_id": "run-1",
            "lifecycle": {"phase": "live", "status": "running", "message": "Runtime is live."},
        },
    )
    monkeypatch.setattr(
        svc,
        "get_bot_run",
        lambda run_id: {
            "run_id": run_id,
            "bot_id": "bot-1",
            "strategy_name": "Momentum Variant A",
            "datasource": "COINBASE",
            "exchange": "coinbase_direct",
            "symbols": ["BTC-USD", "ETH-USD"],
            "status": "running",
            "started_at": "2026-04-09T10:00:00Z",
        },
    )
    rows = [
        {
            "series_key": "instrument-btc|1m",
            "seq": 8,
            "event_time": "2026-04-09T10:01:00Z",
            "known_at": "2026-04-09T10:01:00Z",
            "payload": {"projection": _projection(series_key="instrument-btc|1m", symbol="BTC-USD", timeframe="1m")},
        },
        {
            "series_key": "instrument-eth|5m",
            "seq": 9,
            "event_time": "2026-04-09T10:02:00Z",
            "known_at": "2026-04-09T10:02:00Z",
            "payload": {"projection": _projection(series_key="instrument-eth|5m", symbol="ETH-USD", timeframe="5m")},
        },
    ]
    monkeypatch.setattr(svc, "list_bot_run_view_states", lambda **kwargs: rows)
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {"series_key": "instrument-eth|5m", "seq": 9},
    )
    monkeypatch.setattr(
        svc,
        "get_series_window",
        lambda **kwargs: {
            "run_id": kwargs["run_id"],
            "series_key": kwargs["series_key"],
            "seq": 9,
            "continuity": {"status": "ready"},
            "lifecycle": {"phase": "live", "status": "running"},
            "window": {
                "projection": _projection(series_key=kwargs["series_key"], symbol="ETH-USD", timeframe="5m"),
                "runtime": {"status": "running"},
            },
        },
    )

    result = svc.get_active_botlens_session(bot_id="bot-1")

    assert result["state"] == "ready"
    assert result["run"]["run_id"] == "run-1"
    assert result["selected_series_key"] == "instrument-eth|5m"
    assert [entry["display_label"] for entry in result["series_catalog"]] == ["BTC-USD · 1m", "ETH-USD · 5m"]
    assert result["snapshot"]["series_key"] == "instrument-eth|5m"


def test_get_active_botlens_session_rejects_missing_requested_series(monkeypatch) -> None:
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
        },
    )
    monkeypatch.setattr(
        svc,
        "list_bot_run_view_states",
        lambda **kwargs: [
            {
                "series_key": "instrument-btc|1m",
                "seq": 8,
                "payload": {"projection": _projection(series_key="instrument-btc|1m", symbol="BTC-USD", timeframe="1m")},
            }
        ],
    )
    monkeypatch.setattr(svc, "get_latest_bot_run_view_state", lambda **kwargs: {"series_key": "instrument-btc|1m", "seq": 8})

    result = svc.get_active_botlens_session(bot_id="bot-1", series_key="instrument-eth|5m")

    assert result["state"] == "series_unavailable"
    assert result["snapshot"] is None
    assert result["selected_series_key"] is None
    assert result["series_catalog"][0]["display_label"] == "BTC-USD · 1m"


def test_resolve_active_botlens_stream_returns_active_run_and_series(monkeypatch) -> None:
    monkeypatch.setattr(
        svc,
        "get_active_botlens_session",
        lambda **kwargs: {
            "state": "ready",
            "run": {"run_id": "run-1"},
            "selected_series_key": "instrument-btc|1m",
        },
    )

    result = svc.resolve_active_botlens_stream(bot_id="bot-1", series_key="instrument-btc|1m")

    assert result == {
        "run_id": "run-1",
        "series_key": "instrument-btc|1m",
        "session": {
            "state": "ready",
            "run": {"run_id": "run-1"},
            "selected_series_key": "instrument-btc|1m",
        },
    }

from __future__ import annotations
import pytest
pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import botlens_series_service as svc


def test_get_series_window_returns_bounded_window(monkeypatch):
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "list_bot_runtime_events",
        lambda **kwargs: [
            {
                "seq": 5,
                "event_time": "2026-01-01T00:00:00Z",
                "payload": {
                    "snapshot": {
                        "series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": i} for i in range(10)]}],
                        "trades": [],
                        "runtime": {"status": "running"},
                    }
                },
            }
        ],
    )

    result = svc.get_series_window(run_id="run-1", series_key="BTC|1m", to="now", limit=3)
    assert result["seq"] == 5
    assert [row["time"] for row in result["window"]["candles"]] == [7, 8, 9]
    assert result["window"]["projection"]["series"][0]["series_key"] == "BTC|1m"


def test_get_series_window_prefers_authoritative_runtime_event_projection(monkeypatch):
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "list_bot_runtime_events",
        lambda **kwargs: [
            {
                "seq": 7,
                "event_time": "2026-01-01T00:00:00Z",
                "payload": {
                    "projection": {
                        "series": [
                            {
                                "symbol": "BTC",
                                "timeframe": "1m",
                                "candles": [{"time": 1, "open": 1, "high": 1, "low": 1, "close": 1}],
                                "overlays": [{"type": "regime_overlay", "payload": {"state": "risk_on"}}],
                                "stats": {"total_trades": 2},
                            }
                        ],
                        "trades": [{"trade_id": "t-1", "symbol": "BTC"}],
                        "logs": ["runtime log"],
                        "warnings": ["warning"],
                        "decisions": ["decision"],
                        "runtime": {"status": "running"},
                    }
                },
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "get_latest_bot_run_view_state",
        lambda **kwargs: {
            "seq": 99,
            "payload": {
                "series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": 999}]}],
                "runtime": {"status": "stale"},
            },
        },
    )

    result = svc.get_series_window(run_id="run-1", series_key="BTC|1m", to="now", limit=5)

    assert result["seq"] == 7
    assert [row["time"] for row in result["window"]["candles"]] == [1]
    assert result["window"]["projection"]["warnings"] == ["warning"]
    assert result["window"]["projection"]["trades"][0]["trade_id"] == "t-1"


def test_get_series_history_returns_deduped_older_page(monkeypatch):
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "list_bot_runtime_events",
        lambda **kwargs: [
            {"payload": {"snapshot": {"series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": 1}, {"time": 2}]}]}}},
            {"payload": {"snapshot": {"series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": 2}, {"time": 3}]}]}}},
            {"payload": {"snapshot": {"series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": 4}]}]}}},
        ],
    )

    result = svc.get_series_history(run_id="run-1", series_key="BTC|1m", before_ts="1970-01-01T00:00:04Z", limit=3)
    assert [row["time"] for row in result["history"]["candles"]] == [1, 2, 3]


def test_get_series_history_normalizes_string_time_identity(monkeypatch):
    monkeypatch.setattr(svc, "get_bot_run", lambda run_id: {"run_id": run_id, "bot_id": "bot-1"})
    monkeypatch.setattr(
        svc,
        "list_bot_runtime_events",
        lambda **kwargs: [
            {
                "payload": {
                    "projection": {
                        "series": [
                            {
                                "symbol": "BTC",
                                "timeframe": "1m",
                                "candles": [
                                    {"time": "2026-01-01T00:00:00Z", "open": 1, "high": 1, "low": 1, "close": 1},
                                    {"time": "2026-01-01T00:01:00Z", "open": 2, "high": 2, "low": 2, "close": 2},
                                ],
                            }
                        ]
                    }
                }
            },
            {
                "payload": {
                    "projection": {
                        "series": [
                            {
                                "symbol": "BTC",
                                "timeframe": "1m",
                                "candles": [
                                    {"time": 1767225600, "open": 3, "high": 3, "low": 3, "close": 3},
                                    {"time": 1767225660, "open": 4, "high": 4, "low": 4, "close": 4},
                                ],
                            }
                        ]
                    }
                }
            },
        ],
    )

    result = svc.get_series_history(run_id="run-1", series_key="BTC|1m", before_ts="2026-01-01T00:02:00Z", limit=5)

    assert [row["time"] for row in result["history"]["candles"]] == [1767225600, 1767225660]


def test_build_live_tail_messages_is_incremental():
    prev = {"series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": 10, "close": 1.0}]}], "runtime": {"status": "running"}}
    curr = {"series": [{"symbol": "BTC", "timeframe": "1m", "candles": [{"time": 10, "close": 1.5}]}], "runtime": {"status": "running"}}

    events = svc.build_live_tail_messages(
        run_id="run-1",
        series_key="BTC|1m",
        seq=11,
        known_at="2026-01-01T00:00:00Z",
        previous_snapshot=prev,
        current_snapshot=curr,
    )
    assert events[0]["message_type"] == "bar_update"
    assert "snapshot" not in events[0].get("payload", {})


def test_build_live_tail_messages_normalizes_time_identity():
    prev = {
        "series": [
            {
                "symbol": "BTC",
                "timeframe": "1m",
                "candles": [{"time": "2026-01-01T00:00:00Z", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}],
            }
        ],
        "runtime": {"status": "running"},
    }
    curr = {
        "series": [
            {
                "symbol": "BTC",
                "timeframe": "1m",
                "candles": [{"time": 1767225600, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.5}],
            }
        ],
        "runtime": {"status": "running"},
    }

    events = svc.build_live_tail_messages(
        run_id="run-1",
        series_key="BTC|1m",
        seq=11,
        known_at="2026-01-01T00:00:00Z",
        previous_snapshot=prev,
        current_snapshot=curr,
    )

    assert events[0]["message_type"] == "bar_update"

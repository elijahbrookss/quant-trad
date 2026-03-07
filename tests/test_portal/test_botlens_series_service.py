from __future__ import annotations

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

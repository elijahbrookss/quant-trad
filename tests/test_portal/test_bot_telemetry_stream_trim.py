from __future__ import annotations

import sys
import types

sys.modules.setdefault("fastapi", types.SimpleNamespace(WebSocket=object))

# Telemetry trim tests are pure function tests; stub storage imports to avoid
# pulling full DB dependencies in this test environment.
sys.modules.pop("portal.backend.service.storage", None)
sys.modules.pop("portal.backend.service.storage.storage", None)
storage_pkg = types.ModuleType("portal.backend.service.storage")
storage_pkg.__path__ = []  # mark as package
storage_mod = types.ModuleType("portal.backend.service.storage.storage")
storage_mod.get_latest_bot_run_view_state = lambda *args, **kwargs: None
storage_mod.get_latest_bot_runtime_run_id = lambda *args, **kwargs: None
storage_mod.upsert_bot_run_view_state = lambda *args, **kwargs: {}
sys.modules["portal.backend.service.storage"] = storage_pkg
sys.modules["portal.backend.service.storage.storage"] = storage_mod

from portal.backend.service.bots import telemetry_stream as stream


def test_trim_chart_snapshot_preserves_series_stats_logs_and_decisions(monkeypatch):
    monkeypatch.setattr(stream, "_MAX_SERIES", 4)
    monkeypatch.setattr(stream, "_MAX_CANDLES", 0)
    monkeypatch.setattr(stream, "_MAX_OVERLAYS", 0)
    monkeypatch.setattr(stream, "_MAX_TRADES", 10)
    monkeypatch.setattr(stream, "_MAX_LOGS", 2)
    monkeypatch.setattr(stream, "_MAX_DECISIONS", 3)

    raw_chart = {
        "series": [
            {
                "strategy_id": "strategy-1",
                "symbol": "BTC-USD",
                "timeframe": "1h",
                "candles": [{"time": "a"}, {"time": "b"}],
                "overlays": [{"type": "x"}, {"type": "y"}],
                "stats": {"total_trades": 12, "net_pnl": 45.5, "win_rate": 0.42, "max_drawdown": 11.0},
            }
        ],
        "trades": [{"trade_id": "t1"}, {"trade_id": "t2"}],
        "logs": [
            {"id": "l1"},
            {"id": "l2"},
            {"id": "l3"},
        ],
        "decisions": [
            {"event_id": "d1"},
            {"event_id": "d2"},
            {"event_id": "d3"},
            {"event_id": "d4"},
        ],
        "runtime": {"status": "running"},
        "warnings": [{"id": "w1"}],
    }

    trimmed = stream._trim_chart_snapshot(raw_chart)

    assert len(trimmed["series"]) == 1
    assert trimmed["series"][0]["symbol"] == "BTC-USD"
    assert trimmed["series"][0]["stats"]["total_trades"] == 12
    assert trimmed["series"][0]["stats"]["net_pnl"] == 45.5
    assert trimmed["series"][0]["stats"]["win_rate"] == 0.42
    assert trimmed["series"][0]["stats"]["max_drawdown"] == 11.0

    assert [entry["id"] for entry in trimmed["logs"]] == ["l2", "l3"]
    assert [entry["event_id"] for entry in trimmed["decisions"]] == ["d2", "d3", "d4"]
    assert len(trimmed["trades"]) == 2
    assert trimmed["runtime"]["status"] == "running"


def test_trim_chart_snapshot_bounds_overlay_geometry_points(monkeypatch):
    monkeypatch.setattr(stream, "_MAX_SERIES", 2)
    monkeypatch.setattr(stream, "_MAX_CANDLES", 10)
    monkeypatch.setattr(stream, "_MAX_OVERLAYS", 10)
    monkeypatch.setattr(stream, "_MAX_OVERLAY_POINTS", 3)

    raw_chart = {
        "series": [
            {
                "strategy_id": "strategy-1",
                "symbol": "ETH-USD",
                "timeframe": "15m",
                "candles": [{"time": "a"}],
                "overlays": [
                    {
                        "id": "ov-1",
                        "type": "polyline",
                        "points": [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}, {"x": 5}],
                    }
                ],
                "stats": {},
            }
        ],
        "trades": [],
        "logs": [],
        "decisions": [],
        "runtime": {"status": "running"},
    }

    trimmed = stream._trim_chart_snapshot(raw_chart)
    overlays = trimmed["series"][0]["overlays"]
    assert len(overlays) == 1
    points = overlays[0]["points"]
    assert [point["x"] for point in points] == [3, 4, 5]


def test_build_overlay_delta_snapshot_emits_changed_and_removed_overlays():
    previous = {
        "series": [
            {
                "strategy_id": "strategy-1",
                "symbol": "BTC-USD",
                "timeframe": "1h",
                "overlays": [
                    {"id": "a", "points": [{"x": 1}]},
                    {"id": "b", "points": [{"x": 2}]},
                ],
            }
        ],
        "trades": [],
        "runtime": {},
        "logs": [],
        "decisions": [],
        "warnings": [],
    }
    current = {
        "series": [
            {
                "strategy_id": "strategy-1",
                "symbol": "BTC-USD",
                "timeframe": "1h",
                "overlays": [
                    {"id": "a", "points": [{"x": 10}]},
                    {"id": "c", "points": [{"x": 3}]},
                ],
            }
        ],
        "trades": [],
        "runtime": {},
        "logs": [],
        "decisions": [],
        "warnings": [],
    }

    delta_snapshot = stream._build_overlay_delta_snapshot(previous=previous, current=current)
    series = delta_snapshot["series"][0]
    assert series["overlay_delta"]["mode"] == "delta"
    assert "id:b" in series["overlay_delta"]["removed"]
    changed_ids = {item.get("id") for item in series["overlays"]}
    assert changed_ids == {"a", "c"}

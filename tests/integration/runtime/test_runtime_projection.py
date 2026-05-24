from __future__ import annotations

from types import SimpleNamespace

from engines.bot_runtime.runtime.mixins.runtime_projection import RuntimeProjectionMixin


class _ChartStateBuilder:
    @staticmethod
    def chart_state(candles, trades, stats, overlays, logs, decision_events=None):
        return {
            "candles": candles,
            "trades": trades,
            "stats": stats,
            "overlays": overlays,
            "logs": logs,
            "decisions": decision_events or [],
        }

    @staticmethod
    def visible_candles(series, status, bar_index, intrabar_manager):
        _ = status, intrabar_manager
        return list(series.candles[: bar_index + 1])


class _ProjectionRuntime(RuntimeProjectionMixin):
    def __init__(self, series):
        self._series = list(series)
        self._chart_overlays = []
        self._chart_state_builder = _ChartStateBuilder()
        self._intrabar_manager = None
        self._last_stats = {"total_trades": 3}
        self._botlens_chart_closed_trade_limit = 1
        self.state = {"status": "running", "stats": {"total_trades": 3}}

    def _visible_candles(self):
        return []

    def _visible_overlays(self):
        return []

    def _series_visible_overlays(self, series, *, status):
        _ = series, status
        return []

    def _series_state_for(self, series):
        _ = series
        return SimpleNamespace(bar_index=1)

    def _aggregate_stats(self):
        raise AssertionError("chart projection should use cached runtime stats")

    def logs(self, limit=200):
        _ = limit
        return []

    def decision_events(self, limit=200):
        _ = limit
        return []

    def _runtime_log_context(self, **kwargs):
        return dict(kwargs)


class _WindowEngine:
    def __init__(self, trades):
        self._trades = list(trades)
        self.trades = []
        self.window_calls = []

    def serialise_trade_window(self, *, max_closed):
        self.window_calls.append(max_closed)
        return [dict(entry) for entry in self._trades]

    def serialise_trades(self):
        raise AssertionError("full trade list was serialized")

    def stats(self):
        return {"fees_paid": 0.0}


def _series(series_id: str, trades):
    return SimpleNamespace(
        instrument={"id": f"instrument-{series_id}"},
        timeframe="1h",
        strategy_id=f"strategy-{series_id}",
        symbol=f"SYM-{series_id}",
        datasource="COINBASE",
        exchange="coinbase_direct",
        candles=[{"time": 1}, {"time": 2}],
        risk_engine=_WindowEngine(trades),
    )


def test_chart_state_uses_bounded_trade_window_for_top_level_and_series_payloads():
    first = _series(
        "a",
        [
            {"trade_id": "closed-a", "status": "closed", "closed_at": "2026-01-01T00:00:00Z"},
            {"trade_id": "open-a", "status": "open"},
        ],
    )
    second = _series("b", [{"trade_id": "open-b", "status": "open"}])
    runtime = _ProjectionRuntime([first, second])

    payload = runtime._chart_state()

    assert [trade["trade_id"] for trade in payload["trades"]] == ["closed-a", "open-a", "open-b"]
    assert [trade["trade_id"] for trade in payload["series"][0]["trades"]] == ["closed-a", "open-a"]
    assert [trade["trade_id"] for trade in payload["series"][1]["trades"]] == ["open-b"]
    assert first.risk_engine.window_calls == [1]
    assert second.risk_engine.window_calls == [1]

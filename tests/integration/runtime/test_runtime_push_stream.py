from __future__ import annotations

from queue import Empty
from types import SimpleNamespace

from engines.bot_runtime.runtime.mixins.runtime_push_stream import RuntimePushStreamMixin


class _FakeRuntime(RuntimePushStreamMixin):
    def __init__(self) -> None:
        self._lock = SimpleNamespace()
        self._subscribers = {}

    def _runtime_log_context(self, **kwargs):
        return dict(kwargs)


class _SimpleLock:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


def _runtime() -> _FakeRuntime:
    runtime = _FakeRuntime()
    runtime._lock = _SimpleLock()
    return runtime


def test_subscribe_drop_and_signal_replaces_backpressure_with_gap_event() -> None:
    runtime = _runtime()
    token, queue_ref = runtime.subscribe(overflow_policy="drop_and_signal")

    for index in range(queue_ref.maxsize):
        queue_ref.put_nowait({"type": f"seed-{index}"})

    subscribers, dropped = runtime._broadcast("facts", {"payload": "next"})

    assert subscribers == 1
    assert dropped == 0

    gap = queue_ref.get_nowait()
    assert gap == {
        "type": "gap",
        "reason": "subscriber_backpressure",
        "event": "facts",
    }

    with runtime._lock:
        assert runtime._subscribers[token]["overflowed"] is True

    runtime.unsubscribe(token)
    with runtime._lock:
        assert token not in runtime._subscribers
    try:
        queue_ref.get_nowait()
        raise AssertionError("queue should be drained after unsubscribe")
    except Empty:
        pass


def test_botlens_bootstrap_payload_emits_fact_batch_for_selected_series() -> None:
    runtime = _runtime()
    series = SimpleNamespace(
        instrument={"id": "instrument-bip"},
        timeframe="1h",
        strategy_id="strategy-1",
        symbol="BIP-20DEC30-CDE",
        datasource="COINBASE",
        exchange="coinbase_direct",
    )
    runtime._series = [series]
    runtime.snapshot = lambda: {
        "status": "running",
        "known_at": "2026-04-09T14:00:00Z",
        "last_snapshot_at": "2026-04-09T14:00:00Z",
        "stats": {"bars_processed": 12},
    }
    runtime.chart_payload = lambda: {
        "series": [
            {
                "instrument_id": "instrument-bip",
                "symbol": "BIP-20DEC30-CDE",
                "timeframe": "1h",
                "bar_index": 1,
                "candles": [
                    {"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                    {"time": 2, "open": 1.5, "high": 2.5, "low": 1.0, "close": 2.0},
                ],
                "overlays": [{"type": "line", "value": 1.5}],
                "stats": {"open_trades": 0},
            }
        ],
        "trades": [{"trade_id": "trade-1", "status": "open"}],
        "logs": [{"id": "log-1", "message": "bootstrap"}],
        "decisions": [{"event_id": "decision-1", "action": "hold"}],
    }

    payload = runtime.botlens_bootstrap_payload()

    assert payload["type"] == "facts"
    assert payload["event"] == "bootstrap"
    assert payload["series_key"] == "instrument-bip|1h"
    assert "projection" not in payload
    assert "runtime_delta" not in payload

    fact_types = [fact["fact_type"] for fact in payload["facts"]]
    assert "runtime_state_observed" in fact_types
    assert "series_state_observed" in fact_types
    assert fact_types.count("candle_upserted") == 2
    assert "overlay_ops_emitted" in fact_types
    assert "series_stats_updated" in fact_types
    assert "trade_upserted" in fact_types
    assert "log_emitted" in fact_types
    assert "decision_emitted" in fact_types

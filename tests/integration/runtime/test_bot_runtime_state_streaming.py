from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.runtime.mixins.state_streaming import RuntimeStateStreamingMixin
from utils.log_context import build_log_context


class _RecordingSeriesStateBuffer:
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def record(self, payload: dict) -> float:
        self.payloads.append(dict(payload))
        return 1.25


class _FailingStepTraceBuffer:
    def record(self, payload: dict) -> float:
        raise RuntimeError("step-trace write failed")

    def metrics_snapshot(self) -> dict:
        return {
            "queue_depth": 0.0,
            "dropped_count": 0.0,
            "persist_lag_ms": 0.0,
            "persist_batch_ms": 0.0,
            "persist_error_count": 0.0,
        }


class _DummyRuntime(RuntimeStateStreamingMixin):
    def __init__(self) -> None:
        self.bot_id = "bot-1"
        self.run_type = "backtest"
        self._run_context = SimpleNamespace(run_id="run-1")
        self._series_state_buffer = _RecordingSeriesStateBuffer()
        self._step_trace_buffer = _FailingStepTraceBuffer()
        self._seq = 0

    def _runtime_log_context(self, **fields: object) -> dict[str, object]:
        return build_log_context(
            bot_id=self.bot_id,
            bot_mode=self.run_type,
            run_id=self._run_context.run_id,
            **fields,
        )

    def _allocate_runtime_event_seq(self) -> int:
        self._seq += 1
        return int(self._seq)


def _series_with_runtime_state() -> tuple[SimpleNamespace, Candle]:
    candle_time = datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc)
    candle = Candle(
        time=candle_time,
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        end=candle_time,
    )
    key_time = candle_time.replace(tzinfo=None)
    runtime_derived_state = SimpleNamespace(
        instrument_id="instr-1",
        timeframe_seconds=3600,
        stats_version="stats-v1",
        regime_version="regime-v1",
        candle_stats_by_time={key_time: {"atr_ratio": 1.2}},
        regime_rows={key_time: {"volatility": {"state": "low"}, "confidence": 0.9}},
    )
    series = SimpleNamespace(
        strategy_id="strategy-1",
        name="Strategy (BTCUSD)",
        symbol="BTCUSD",
        timeframe="1h",
        datasource="local",
        exchange="test",
        candles=[candle],
        instrument={"id": "instr-1"},
        runtime_derived_state=runtime_derived_state,
    )
    return series, candle


def test_persist_series_state_snapshot_does_not_duplicate_run_id_in_log_context() -> None:
    runtime = _DummyRuntime()
    series, candle = _series_with_runtime_state()

    enqueue_ms = runtime._persist_series_state_snapshot(series=series, candle=candle, bar_index=12)

    assert enqueue_ms == 1.25
    assert len(runtime._series_state_buffer.payloads) == 1
    payload = runtime._series_state_buffer.payloads[0]
    assert payload["run_id"] == "run-1"
    assert payload["event_type"] == "series_state.snapshot"
    assert payload["payload"]["series_key"] == "BTCUSD|1h"
    assert payload["payload"]["bar_index"] == 12


def test_record_step_trace_failure_does_not_duplicate_run_id_in_log_context() -> None:
    runtime = _DummyRuntime()

    enqueue_ms = runtime._record_step_trace(
        "step_update_state",
        started_at=datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 3, 15, 1, 0, 1, tzinfo=timezone.utc),
        ok=False,
        error="boom",
    )

    assert enqueue_ms is None

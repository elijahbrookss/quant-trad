from __future__ import annotations

import json

import pytest

from datetime import datetime, timezone
from types import SimpleNamespace

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.runtime.mixins.runtime_persistence import RuntimePersistenceMixin
from engines.bot_runtime.runtime.mixins.runtime_projection import RuntimeProjectionMixin
from engines.bot_runtime.runtime.mixins.runtime_push_stream import RuntimePushStreamMixin
from overlays.schema import build_overlay
from utils.log_context import build_log_context


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


class _DummyRuntime(
    RuntimePersistenceMixin,
    RuntimeProjectionMixin,
    RuntimePushStreamMixin,
):
    def __init__(self) -> None:
        self.bot_id = "bot-1"
        self.run_type = "backtest"
        self._run_context = SimpleNamespace(run_id="run-1")
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
    series = SimpleNamespace(
        strategy_id="strategy-1",
        name="Strategy (BTCUSD)",
        symbol="BTCUSD",
        timeframe="1h",
        datasource="local",
        exchange="test",
        candles=[candle],
        instrument={"id": "instr-1"},
    )
    return series, candle


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


def test_overlay_delta_uses_overlay_id_and_ignores_reorder_without_content_change() -> None:
    runtime = _DummyRuntime()
    cache: dict[str, object] = {}
    overlay_a = {
        "overlay_id": "market_profile.value_area",
        **build_overlay("strategy_signal", {"markers": [{"time": 1, "price": 100.0, "shape": "circle", "color": "#10b981"}]}),
    }
    overlay_b = {
        "overlay_id": "market_profile.breakout_markers",
        **build_overlay("strategy_signal", {"markers": [{"time": 2, "price": 101.0, "shape": "circle", "color": "#f87171"}]}),
    }
    overlay_a["indicator_commit_seq"] = 1
    overlay_a["indicator_commit_seq_status"] = "indicator_scoped"
    overlay_b["indicator_commit_seq"] = 1
    overlay_b["indicator_commit_seq_status"] = "indicator_scoped"

    first = runtime._build_overlay_delta(cache, [overlay_a, overlay_b])
    overlay_a_next = {**overlay_a, "indicator_commit_seq": 2}
    overlay_b_next = {**overlay_b, "indicator_commit_seq": 2}
    second = runtime._build_overlay_delta(cache, [overlay_b_next, overlay_a_next])

    assert isinstance(first, dict)
    assert first["overlay_commit_seq"] == 1
    assert first["base_overlay_commit_seq"] == 0
    assert first["overlay_commit_seq_status"] == "overlay_scoped"
    assert [op["key"] for op in first["ops"]] == [
        "market_profile.value_area",
        "market_profile.breakout_markers",
    ]
    assert second is None


def test_overlay_delta_patches_stable_overlay_payload_changes() -> None:
    from portal.backend.service.bots.botlens_state import apply_overlay_delta

    runtime = _DummyRuntime()
    cache: dict[str, object] = {}
    first_overlay = {
        "overlay_id": "market_profile.value_area",
        **build_overlay(
            "strategy_signal",
            {"markers": [{"time": 1, "price": 100.0, "shape": "circle", "color": "#10b981"}]},
        ),
    }
    second_overlay = {
        **first_overlay,
        "payload": {
            "markers": [
                {"time": 1, "price": 100.0, "shape": "circle", "color": "#10b981"},
                {"time": 2, "price": 101.0, "shape": "circle", "color": "#f87171"},
            ]
        },
    }

    first = runtime._build_overlay_delta(cache, [first_overlay])
    second = runtime._build_overlay_delta(cache, [second_overlay])

    assert isinstance(first, dict)
    assert isinstance(second, dict)
    assert first["ops"][0]["op"] == "upsert"
    assert second["ops"][0]["op"] == "patch"
    projected = apply_overlay_delta([first["ops"][0]["overlay"]], second)
    assert projected[0]["payload"]["markers"] == second_overlay["payload"]["markers"]


def test_overlay_delta_encodes_large_rolling_polylines_as_small_exact_tail_patch() -> None:
    from portal.backend.service.bots.botlens_state import apply_overlay_delta

    runtime = _DummyRuntime()
    cache: dict[str, object] = {}

    def overlay(start: int) -> dict:
        return {
            "overlay_id": "indicator.lines",
            "type": "strategy_signal",
            "payload": {
                "polylines": [
                    {
                        "role": f"line-{line_index}",
                        "color": "#38bdf8",
                        "points": [
                            {"time": point, "price": float(point + line_index)}
                            for point in range(start, start + 640)
                        ],
                    }
                    for line_index in range(3)
                ]
            },
        }

    first = runtime._build_overlay_delta(cache, [overlay(0)])
    second = runtime._build_overlay_delta(cache, [overlay(25)])

    assert first is not None
    assert second is not None
    patch = second["ops"][0]["payload_patch"]
    assert "polylines" not in patch.get("replace", {})
    assert [entry["drop_prefix"] for entry in patch["polyline_tail"]["entries"]] == [25, 25, 25]
    assert [len(entry["append"]) for entry in patch["polyline_tail"]["entries"]] == [25, 25, 25]
    assert len(json.dumps(second, separators=(",", ":")).encode("utf-8")) < 10_000

    projected = apply_overlay_delta([first["ops"][0]["overlay"]], second)
    assert projected[0]["payload"]["polylines"] == overlay(25)["payload"]["polylines"]

    corrupted = [dict(first["ops"][0]["overlay"])]
    corrupted[0]["payload"] = {
        **dict(corrupted[0]["payload"]),
        "polylines": [{**line, "points": list(line["points"])[1:]} for line in corrupted[0]["payload"]["polylines"]],
    }
    with pytest.raises(ValueError, match="base fingerprint mismatch"):
        apply_overlay_delta(corrupted, second)

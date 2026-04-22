from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import engines.indicator_engine.runtime_engine as runtime_engine_module
from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    DetailDefinition,
    Indicator,
    IndicatorRuntimeSpec,
    OverlayDefinition,
    OutputDefinition,
    OutputRef,
    RuntimeDetail,
    RuntimeOverlay,
    RuntimeOutput,
)
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine, IndicatorGuardConfig
from indicators.candle_stats.runtime import TypedCandleStatsIndicator
from indicators.market_profile.runtime.typed_indicator import TypedMarketProfileIndicator
from indicators.regime.runtime import TypedRegimeIndicator
from overlays.builders import build_line_overlay
from overlays.registry import register_overlay_type
from overlays.schema import build_overlay
from strategies.compiler import compile_strategy
from strategies.evaluator import DecisionEvaluationState, evaluate_strategy_bar


register_overlay_type(
    "test_indicator_overlay",
    label="Test Overlay",
    pane_views=("marker",),
    renderers={"lightweight": "marker"},
    payload_keys=("markers",),
)


class _SourceIndicator(Indicator):
    def __init__(self) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="source",
            manifest_type="source",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="signal", type="signal"),),
            overlays=(OverlayDefinition(name="markers", overlay_type="test_indicator_overlay"),),
        )
        self._bar_time = datetime.min.replace(tzinfo=timezone.utc)

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs
        self._bar_time = BAR_TIME

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "signal": RuntimeOutput(
                bar_time=self._bar_time,
                ready=True,
                value={"events": [{"key": "go"}]},
            )
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return {
            "markers": RuntimeOverlay(
                bar_time=self._bar_time,
                ready=True,
                value=build_overlay(
                    "test_indicator_overlay",
                    {"markers": [{"time": int(self._bar_time.timestamp()), "price": 100.0, "shape": "circle", "color": "#38bdf8"}]},
                ),
            )
        }


class _NotReadyIndicator(Indicator):
    def __init__(self) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="upstream",
            manifest_type="upstream",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="metric", type="metric"),),
            overlays=(OverlayDefinition(name="markers", overlay_type="test_indicator_overlay"),),
        )
        self._bar_time = datetime.min.replace(tzinfo=timezone.utc)

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs
        self._bar_time = BAR_TIME

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "metric": RuntimeOutput(
                bar_time=self._bar_time,
                ready=False,
                value={},
            )
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return {
            "markers": RuntimeOverlay(
                bar_time=self._bar_time,
                ready=False,
                value={},
            )
        }


class _DependentIndicator(Indicator):
    def __init__(self) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="dependent",
            manifest_type="dependent",
            version="v1",
            dependencies=(OutputRef(indicator_id="upstream", output_name="metric"),),
            outputs=(OutputDefinition(name="context", type="context"),),
            overlays=(OverlayDefinition(name="markers", overlay_type="test_indicator_overlay"),),
        )
        self._bar_time = datetime.min.replace(tzinfo=timezone.utc)

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs
        self._bar_time = BAR_TIME

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "context": RuntimeOutput(
                bar_time=self._bar_time,
                ready=False,
                value={},
            )
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return {
            "markers": RuntimeOverlay(
                bar_time=self._bar_time,
                ready=False,
                value={},
            )
        }


class _MissingOverlayIndicator(Indicator):
    def __init__(self) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="broken",
            manifest_type="broken",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="metric", type="metric"),),
            overlays=(OverlayDefinition(name="markers", overlay_type="test_indicator_overlay"),),
        )

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "metric": RuntimeOutput(
                bar_time=BAR_TIME,
                ready=True,
                value={"value": 1.0},
            )
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return {}


class _OverlayCountIndicator(Indicator):
    def __init__(self) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="counted",
            manifest_type="counted",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="metric", type="metric"),),
            overlays=(OverlayDefinition(name="markers", overlay_type="test_indicator_overlay"),),
        )
        self.overlay_snapshot_calls = 0

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "metric": RuntimeOutput(
                bar_time=BAR_TIME,
                ready=True,
                value={"value": 1.0},
            )
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        self.overlay_snapshot_calls += 1
        return {
            "markers": RuntimeOverlay(
                bar_time=BAR_TIME,
                ready=True,
                value=build_overlay(
                    "test_indicator_overlay",
                    {"markers": [{"time": int(BAR_TIME.timestamp()), "price": 100.0, "shape": "circle", "color": "#38bdf8"}]},
                ),
            )
        }


class _GuardedOverlayIndicator(Indicator):
    def __init__(self, *, marker_count: int, note_size: int = 0) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="guarded",
            manifest_type="guarded",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="metric", type="metric"),),
            overlays=(OverlayDefinition(name="markers", overlay_type="test_indicator_overlay"),),
        )
        self._marker_count = int(marker_count)
        self._note_size = int(note_size)

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "metric": RuntimeOutput(
                bar_time=BAR_TIME,
                ready=True,
                value={"value": 1.0},
            )
        }

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        markers = []
        for index in range(self._marker_count):
            markers.append(
                {
                    "time": int(BAR_TIME.timestamp()) + index,
                    "price": 100.0 + float(index),
                    "shape": "circle",
                    "color": "#38bdf8",
                    "note": "x" * self._note_size,
                }
            )
        return {
            "markers": RuntimeOverlay(
                bar_time=BAR_TIME,
                ready=True,
                value=build_overlay("test_indicator_overlay", {"markers": markers}),
            )
        }


class _DetailIndicator(Indicator):
    def __init__(self) -> None:
        self.detail_snapshot_calls = 0
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="detailed",
            manifest_type="detailed",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="metric", type="metric"),),
            details=(DetailDefinition(name="readout"),),
        )

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        _ = bar, inputs

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {
            "metric": RuntimeOutput(
                bar_time=BAR_TIME,
                ready=True,
                value={"value": 1.0},
            )
        }

    def detail_snapshot(self) -> Mapping[str, RuntimeDetail]:
        self.detail_snapshot_calls += 1
        return {
            "readout": RuntimeDetail(
                bar_time=BAR_TIME,
                ready=True,
                value={"blocks": [{"x1": 1, "x2": 2}]},
            )
        }


BAR_TIME = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)


def test_indicator_engine_returns_outputs_and_overlays() -> None:
    engine = IndicatorExecutionEngine([_SourceIndicator()])

    frame = engine.step(bar=object(), bar_time=BAR_TIME)

    assert frame.outputs["source.signal"].ready is True
    assert frame.outputs["source.signal"].value == {"events": [{"key": "go"}]}
    assert frame.overlays["source.markers"].ready is True
    assert frame.overlays["source.markers"].value["type"] == "test_indicator_overlay"


def test_indicator_engine_returns_declared_details() -> None:
    engine = IndicatorExecutionEngine([_DetailIndicator()])

    frame = engine.step(bar=object(), bar_time=BAR_TIME)

    assert frame.details["detailed.readout"].ready is True
    assert frame.details["detailed.readout"].value["blocks"][0]["x1"] == 1


def test_indicator_engine_can_skip_detail_snapshot_for_trading_steps() -> None:
    indicator = _DetailIndicator()
    engine = IndicatorExecutionEngine([indicator])

    frame = engine.step(bar=object(), bar_time=BAR_TIME, include_details=False)

    assert frame.details == {}
    assert indicator.detail_snapshot_calls == 0


def test_build_line_overlay_emits_single_polyline_payload() -> None:
    register_overlay_type(
        "test_line_overlay",
        label="Test Line",
        pane_key="volatility",
        pane_views=("polyline",),
        renderers={"lightweight": "polyline"},
        payload_keys=("polylines",),
    )

    overlay = build_line_overlay(
        "test_line_overlay",
        points=[
            {"time": 1, "price": 100.0},
            {"time": 2, "price": 101.0},
        ],
        color="#38bdf8",
        role="main",
        line_width=2.0,
    )

    assert overlay["type"] == "test_line_overlay"
    assert overlay["pane_key"] == "volatility"
    polylines = overlay["payload"]["polylines"]
    assert len(polylines) == 1
    assert polylines[0]["points"][0]["time"] == 1
    assert polylines[0]["color"] == "#38bdf8"
    assert polylines[0]["role"] == "main"


def test_dependency_not_ready_propagates_to_overlays_and_outputs() -> None:
    engine = IndicatorExecutionEngine([_NotReadyIndicator(), _DependentIndicator()])

    frame = engine.step(bar=object(), bar_time=BAR_TIME)

    assert frame.outputs["upstream.metric"].ready is False
    assert frame.overlays["upstream.markers"].ready is False
    assert frame.outputs["dependent.context"].ready is False
    assert frame.overlays["dependent.markers"].ready is False


def test_overlay_snapshot_must_return_exact_declared_names() -> None:
    engine = IndicatorExecutionEngine([_MissingOverlayIndicator()])

    try:
        engine.step(bar=object(), bar_time=BAR_TIME)
    except RuntimeError as exc:
        assert "overlay presence mismatch" in str(exc)
    else:
        raise AssertionError("expected overlay presence mismatch")


def test_indicator_engine_can_skip_overlay_snapshot_on_intermediate_steps() -> None:
    indicator = _OverlayCountIndicator()
    engine = IndicatorExecutionEngine([indicator])

    frame = engine.step(bar=object(), bar_time=BAR_TIME, include_overlays=False)

    assert frame.overlays == {}
    assert indicator.overlay_snapshot_calls == 0

    frame = engine.step(bar=object(), bar_time=BAR_TIME, include_overlays=True)

    assert "counted.markers" in frame.overlays
    assert indicator.overlay_snapshot_calls == 1


def test_indicator_guard_emits_time_budget_warning_after_repeated_breaches() -> None:
    indicator = _OverlayCountIndicator()
    engine = IndicatorExecutionEngine(
        [indicator],
        guard_config=IndicatorGuardConfig(
            enabled=True,
            time_soft_limit_ms=10.0,
            time_consecutive_bars=2,
            time_window_bars=4,
            time_window_breach_count=3,
            overlay_points_soft_limit=1000,
            overlay_points_hard_limit=0,
            overlay_payload_soft_limit_bytes=100000,
            overlay_payload_hard_limit_bytes=0,
        ),
    )

    original_perf_counter = runtime_engine_module.time.perf_counter
    perf_samples = iter([0.0, 0.020, 0.020, 0.020, 0.030, 0.050, 0.050, 0.050])
    runtime_engine_module.time.perf_counter = lambda: next(perf_samples)
    try:
        first = engine.step(bar=object(), bar_time=BAR_TIME, include_overlays=False)
        second = engine.step(bar=object(), bar_time=BAR_TIME, include_overlays=False)
    finally:
        runtime_engine_module.time.perf_counter = original_perf_counter

    assert first.guard_warnings == ()
    assert second.guard_warnings[-1].warning_type == "indicator_time_budget_exceeded"
    assert second.guard_warnings[-1].context["execution_time_ms"] == 20.0


def test_indicator_guard_emits_overlay_point_warning_without_suppression() -> None:
    engine = IndicatorExecutionEngine(
        [_GuardedOverlayIndicator(marker_count=7)],
        guard_config=IndicatorGuardConfig(
            enabled=True,
            time_soft_limit_ms=1000.0,
            time_consecutive_bars=3,
            time_window_bars=20,
            time_window_breach_count=5,
            overlay_points_soft_limit=5,
            overlay_points_hard_limit=20,
            overlay_payload_soft_limit_bytes=100000,
            overlay_payload_hard_limit_bytes=0,
        ),
    )

    frame = engine.step(bar=object(), bar_time=BAR_TIME)

    assert frame.overlays["guarded.markers"].ready is True
    assert any(warning.warning_type == "indicator_overlay_points_exceeded" for warning in frame.guard_warnings)


def test_indicator_guard_emits_payload_warning_and_can_suppress_overlay_emission() -> None:
    soft_engine = IndicatorExecutionEngine(
        [_GuardedOverlayIndicator(marker_count=2, note_size=120)],
        guard_config=IndicatorGuardConfig(
            enabled=True,
            time_soft_limit_ms=1000.0,
            time_consecutive_bars=3,
            time_window_bars=20,
            time_window_breach_count=5,
            overlay_points_soft_limit=1000,
            overlay_points_hard_limit=0,
            overlay_payload_soft_limit_bytes=150,
            overlay_payload_hard_limit_bytes=5000,
        ),
    )

    soft_frame = soft_engine.step(bar=object(), bar_time=BAR_TIME)
    assert soft_frame.overlays["guarded.markers"].ready is True
    assert any(warning.warning_type == "indicator_overlay_payload_exceeded" for warning in soft_frame.guard_warnings)

    hard_engine = IndicatorExecutionEngine(
        [_GuardedOverlayIndicator(marker_count=12)],
        guard_config=IndicatorGuardConfig(
            enabled=True,
            time_soft_limit_ms=1000.0,
            time_consecutive_bars=3,
            time_window_bars=20,
            time_window_breach_count=5,
            overlay_points_soft_limit=5,
            overlay_points_hard_limit=10,
            overlay_payload_soft_limit_bytes=100000,
            overlay_payload_hard_limit_bytes=0,
        ),
    )

    hard_frame = hard_engine.step(bar=object(), bar_time=BAR_TIME)

    assert hard_frame.overlays["guarded.markers"].ready is False
    assert any(warning.warning_type == "indicator_overlay_suppressed" for warning in hard_frame.guard_warnings)


def test_candle_stats_atr_overlays_are_emitted_in_their_declared_panes() -> None:
    indicator = TypedCandleStatsIndicator(
        indicator_id="candle-stats",
        version="v1",
        params={
            "atr_short_window": 14,
            "atr_long_window": 50,
            "atr_z_window": 100,
            "directional_efficiency_window": 20,
            "slope_window": 20,
            "range_window": 20,
            "expansion_window": 20,
            "volume_window": 50,
            "overlap_window": 8,
            "slope_stability_lookback": 150,
            "warmup_bars": 200,
        },
    )
    engine = IndicatorExecutionEngine([indicator])

    frame = None
    for index in range(220):
        base = 100.0 + float(index) * 0.25
        candle = Candle(
            time=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
            open=base,
            high=base + 2.0,
            low=base - 1.0,
            close=base + 1.0,
            volume=1000.0 + float(index),
        )
        frame = engine.step(bar=candle, bar_time=candle.time)

    assert frame is not None
    for overlay_key, overlay_type, pane_key in (
        ("candle-stats.atr_short", "candle_stats_atr_short", "volatility"),
        ("candle-stats.atr_long", "candle_stats_atr_long", "volatility"),
        ("candle-stats.atr_zscore", "candle_stats_atr_zscore", "oscillator"),
    ):
        overlay = frame.overlays[overlay_key]
        assert overlay.ready is True
        assert overlay.value["type"] == overlay_type
        assert overlay.value["pane_key"] == pane_key
        polylines = overlay.value["payload"]["polylines"]
        assert len(polylines) == 1
        assert len(polylines[0]["points"]) >= 200
    assert frame.overlays["candle-stats.atr_short"].value["ui"]["color"] == "#ef4444"
    assert frame.overlays["candle-stats.atr_short"].value["payload"]["polylines"][0]["color"] == "#ef4444"
    assert frame.overlays["candle-stats.atr_long"].value["ui"]["color"] == "#22c55e"
    assert frame.overlays["candle-stats.atr_long"].value["payload"]["polylines"][0]["color"] == "#22c55e"
    assert frame.overlays["candle-stats.atr_zscore"].value["ui"]["color"] == "#38bdf8"


def test_candle_stats_atr_overlays_do_not_wait_for_full_metric_readiness() -> None:
    indicator = TypedCandleStatsIndicator(
        indicator_id="candle-stats",
        version="v1",
        params={
            "atr_short_window": 14,
            "atr_long_window": 50,
            "atr_z_window": 100,
            "directional_efficiency_window": 20,
            "slope_window": 20,
            "range_window": 20,
            "expansion_window": 20,
            "volume_window": 50,
            "overlap_window": 8,
            "slope_stability_lookback": 150,
            "warmup_bars": 200,
        },
    )
    engine = IndicatorExecutionEngine([indicator])

    frame = None
    for index in range(120):
        base = 100.0 + float(index) * 0.25
        candle = Candle(
            time=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
            open=base,
            high=base + 2.0,
            low=base - 1.0,
            close=base + 1.0,
            volume=1000.0 + float(index),
        )
        frame = engine.step(bar=candle, bar_time=candle.time)

    assert frame is not None
    assert frame.outputs["candle-stats.candle_stats"].ready is False
    assert frame.overlays["candle-stats.atr_short"].ready is True
    assert frame.overlays["candle-stats.atr_long"].ready is True
    assert frame.overlays["candle-stats.atr_zscore"].ready is True


def test_candle_stats_overlay_history_limit_can_match_replay_window() -> None:
    indicator = TypedCandleStatsIndicator(
        indicator_id="candle-stats",
        version="v1",
        params={
            "atr_short_window": 14,
            "atr_long_window": 50,
            "atr_z_window": 100,
            "directional_efficiency_window": 20,
            "slope_window": 20,
            "range_window": 20,
            "expansion_window": 20,
            "volume_window": 50,
            "overlap_window": 8,
            "slope_stability_lookback": 150,
            "warmup_bars": 200,
        },
    )
    indicator.configure_replay_window(history_bars=650)
    engine = IndicatorExecutionEngine([indicator])

    frame = None
    for index in range(650):
        base = 100.0 + float(index) * 0.25
        candle = Candle(
            time=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=index),
            open=base,
            high=base + 2.0,
            low=base - 1.0,
            close=base + 1.0,
            volume=1000.0 + float(index),
        )
        frame = engine.step(
            bar=candle,
            bar_time=candle.time,
            include_overlays=index == 649,
        )

    assert frame is not None
    overlay = frame.overlays["candle-stats.atr_short"]
    assert overlay.ready is True
    points = overlay.value["payload"]["polylines"][0]["points"]
    assert len(points) == 650


def _compact_outputs(frame) -> dict[str, tuple[bool, dict[str, Any]]]:
    return {
        key: (output.ready, output.value)
        for key, output in sorted(frame.outputs.items())
    }


def _sample_candles(count: int = 18) -> list[Candle]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    candles: list[Candle] = []
    for index in range(count):
        base = 100.0 + float(index) * 0.75
        candles.append(
            Candle(
                time=start + timedelta(minutes=index),
                open=base,
                high=base + 2.0 + (0.1 * (index % 3)),
                low=base - 1.0,
                close=base + 0.5,
                volume=1000.0 + float(index * 10),
            )
        )
    return candles


def _candle_stats(indicator_id: str = "stats-1") -> TypedCandleStatsIndicator:
    return TypedCandleStatsIndicator(
        indicator_id=indicator_id,
        version="v1",
        params={
            "atr_short_window": 3,
            "atr_long_window": 5,
            "atr_z_window": 5,
            "directional_efficiency_window": 3,
            "slope_window": 3,
            "range_window": 3,
            "expansion_window": 3,
            "volume_window": 3,
            "overlap_window": 2,
            "slope_stability_lookback": 3,
            "warmup_bars": 6,
        },
    )


def _market_profile(indicator_id: str = "profile-1") -> TypedMarketProfileIndicator:
    return TypedMarketProfileIndicator(
        indicator_id=indicator_id,
        version="v1",
        params={
            "bin_size": 1.0,
            "price_precision": 2,
            "use_merged_value_areas": False,
            "merge_threshold": 0.5,
            "min_merge_sessions": 2,
            "extend_value_area_to_chart_end": True,
        },
        source_facts={
            "symbol": "TEST",
            "profile_params": {
                "use_merged_value_areas": False,
                "extend_value_area_to_chart_end": True,
            },
            "profiles": [
                {
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-01T00:05:00Z",
                    "known_at": "2026-01-01T00:00:00Z",
                    "VAH": 105.0,
                    "VAL": 95.0,
                    "POC": 100.0,
                    "session_count": 1,
                    "precision": 2,
                }
            ],
        },
    )


def _assert_output_equivalence(indicators_with_overlays, indicators_without_overlays) -> None:
    enabled = IndicatorExecutionEngine(indicators_with_overlays)
    disabled = IndicatorExecutionEngine(indicators_without_overlays)
    for candle in _sample_candles():
        enabled_frame = enabled.step(bar=candle, bar_time=candle.time, include_overlays=True)
        disabled_frame = disabled.step(bar=candle, bar_time=candle.time, include_overlays=False)
        assert _compact_outputs(disabled_frame) == _compact_outputs(enabled_frame)


def test_candle_stats_outputs_match_when_overlays_are_disabled() -> None:
    _assert_output_equivalence([_candle_stats()], [_candle_stats()])


def test_regime_outputs_match_when_overlays_are_disabled() -> None:
    def indicators():
        return [
            _candle_stats("stats-1"),
            TypedRegimeIndicator(
                indicator_id="regime-1",
                version="v1",
                params={
                    "min_confidence": 0.5,
                    "structure_min_confidence": 0.4,
                    "structure_confirm_bars": 1,
                    "volatility_confirm_bars": 1,
                    "liquidity_confirm_bars": 1,
                    "expansion_confirm_bars": 1,
                    "smoothing_alpha": 1.0,
                },
                candle_stats_indicator_id="stats-1",
            ),
        ]

    _assert_output_equivalence(indicators(), indicators())


def test_market_profile_outputs_match_when_overlays_are_disabled() -> None:
    _assert_output_equivalence([_market_profile()], [_market_profile()])


def test_strategy_decisions_match_when_indicator_overlays_are_disabled() -> None:
    compiled = compile_strategy(
        strategy_id="strategy-1",
        timeframe="1m",
        rules=[
            {
                "id": "rule-1",
                "name": "Go",
                "intent": "enter_long",
                "priority": 10,
                "trigger": {
                    "type": "signal_match",
                    "indicator_id": "source",
                    "output_name": "signal",
                    "event_key": "go",
                },
                "guards": [],
            }
        ],
        attached_indicator_ids=["source"],
        indicator_meta_getter=lambda _indicator_id: {
            "typed_outputs": [{"name": "signal", "type": "signal", "event_keys": ["go"]}]
        },
    )
    enabled_engine = IndicatorExecutionEngine([_SourceIndicator()])
    disabled_engine = IndicatorExecutionEngine([_SourceIndicator()])
    enabled_frame = enabled_engine.step(bar=object(), bar_time=BAR_TIME, include_overlays=True)
    disabled_frame = disabled_engine.step(bar=object(), bar_time=BAR_TIME, include_overlays=False)

    enabled_result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs=enabled_frame.outputs,
        output_types=enabled_engine.output_types,
        instrument_id="instrument-1",
        symbol="TEST",
        timeframe="1m",
        bar_time=BAR_TIME,
    )
    disabled_result = evaluate_strategy_bar(
        compiled_strategy=compiled,
        state=DecisionEvaluationState(),
        outputs=disabled_frame.outputs,
        output_types=disabled_engine.output_types,
        instrument_id="instrument-1",
        symbol="TEST",
        timeframe="1m",
        bar_time=BAR_TIME,
    )

    assert disabled_result.artifacts == enabled_result.artifacts
    assert disabled_result.selected_artifact == enabled_result.selected_artifact

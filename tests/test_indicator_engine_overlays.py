from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    Indicator,
    IndicatorRuntimeSpec,
    OverlayDefinition,
    OutputDefinition,
    OutputRef,
    RuntimeOverlay,
    RuntimeOutput,
)
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.candle_stats.runtime import TypedCandleStatsIndicator
from overlays.builders import build_line_overlay
from overlays.registry import register_overlay_type
from overlays.schema import build_overlay


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


BAR_TIME = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)


def test_indicator_engine_returns_outputs_and_overlays() -> None:
    engine = IndicatorExecutionEngine([_SourceIndicator()])

    frame = engine.step(bar=object(), bar_time=BAR_TIME)

    assert frame.outputs["source.signal"].ready is True
    assert frame.outputs["source.signal"].value == {"events": [{"key": "go"}]}
    assert frame.overlays["source.markers"].ready is True
    assert frame.overlays["source.markers"].value["type"] == "test_indicator_overlay"


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

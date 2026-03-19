from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from engines.indicator_engine.contracts import (
    Indicator,
    IndicatorManifest,
    OverlayDefinition,
    OutputDefinition,
    OutputRef,
    RuntimeOverlay,
    RuntimeOutput,
)
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from signals.overlays.registry import register_overlay_type
from signals.overlays.schema import build_overlay


register_overlay_type(
    "test_indicator_overlay",
    label="Test Overlay",
    pane_views=("marker",),
    renderers={"lightweight": "marker"},
    payload_keys=("markers",),
)


class _SourceIndicator(Indicator):
    def __init__(self) -> None:
        self.manifest = IndicatorManifest(
            id="source",
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
        self.manifest = IndicatorManifest(
            id="upstream",
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
        self.manifest = IndicatorManifest(
            id="dependent",
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
        self.manifest = IndicatorManifest(
            id="broken",
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


BAR_TIME = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)


def test_indicator_engine_returns_outputs_and_overlays() -> None:
    engine = IndicatorExecutionEngine([_SourceIndicator()])

    frame = engine.step(bar=object(), bar_time=BAR_TIME)

    assert frame.outputs["source.signal"].ready is True
    assert frame.outputs["source.signal"].value == {"events": [{"key": "go"}]}
    assert frame.overlays["source.markers"].ready is True
    assert frame.overlays["source.markers"].value["type"] == "test_indicator_overlay"


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

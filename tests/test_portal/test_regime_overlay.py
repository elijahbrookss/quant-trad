import importlib.util
from pathlib import Path

import pytest


def _load_regime_overlay_module():
    root = Path(__file__).resolve().parents[2]
    module_path = root / "portal" / "backend" / "service" / "bots" / "bot_runtime" / "strategy" / "regime_overlay.py"
    spec = importlib.util.spec_from_file_location("regime_overlay", module_path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise RuntimeError("Unable to load regime_overlay module")
    spec.loader.exec_module(module)
    return module


regime_overlay = _load_regime_overlay_module()


def test_state_color_defaults():
    assert regime_overlay.state_color("trend") == "#16a34a"
    assert regime_overlay.state_color(" Range ") == "#64748b"
    assert regime_overlay.state_color("transition") == "#f59e0b"
    assert regime_overlay.state_color("chop") == "#ef4444"
    assert regime_overlay.state_color("unknown") == "#94a3b8"
    assert regime_overlay.state_color(None) == "#94a3b8"


def test_confidence_to_opacity_clamps():
    assert regime_overlay.confidence_to_opacity(None) == pytest.approx(0.06)
    assert regime_overlay.confidence_to_opacity(0) == pytest.approx(0.06)
    assert regime_overlay.confidence_to_opacity(0.5) == pytest.approx(0.14)
    assert regime_overlay.confidence_to_opacity(1) == pytest.approx(0.22)
    assert regime_overlay.confidence_to_opacity(2) == pytest.approx(0.22)
    assert regime_overlay.confidence_to_opacity(-1) == pytest.approx(0.06)


def test_detect_regime_changes_tracks_state_transitions():
    points = [
        {"time": 100, "structure_state": "trend"},
        {"time": 200, "structure_state": "trend"},
        {"time": 300, "structure_state": "range"},
        {"time": 400, "structure_state": "range"},
        {"time": 500, "state": "transition"},
        {"time": "bad", "structure_state": "chop"},
        {"time": 600, "structure_state": ""},
        {"time": 700, "structure_state": "chop"},
    ]

    assert regime_overlay.detect_regime_changes(points) == [300, 500, 700]


def test_build_regime_markers_emits_changes():
    class DummyCandle:
        def __init__(self, time, close):
            self.time = time
            self.close = close

    candles = [
        DummyCandle(time=100, close=10.0),
        DummyCandle(time=200, close=11.0),
        DummyCandle(time=300, close=12.0),
    ]
    points = [
        {"time": 100, "structure_state": "trend", "confidence": 0.8},
        {"time": 200, "structure_state": "trend", "confidence": 0.7},
        {"time": 300, "structure_state": "range", "confidence": 0.5},
    ]

    markers = regime_overlay.build_regime_markers(points, candles)

    assert len(markers) == 1
    marker = markers[0]
    assert marker["time"] == 300
    assert marker["price"] == 12.0
    assert marker["color"] == "#64748b"

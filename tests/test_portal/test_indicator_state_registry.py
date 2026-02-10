from __future__ import annotations

from datetime import datetime

import pytest

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state import (
    IndicatorPluginManifest,
    IndicatorPluginRegistry,
    ensure_builtin_indicator_plugins_registered,
    plugin_registry,
)


def _candle() -> Candle:
    return Candle(time=datetime(2024, 1, 1), open=1.0, high=2.0, low=0.5, close=1.5)


def test_registry_rejects_plugin_without_engine() -> None:
    registry = IndicatorPluginRegistry()
    with pytest.raises(RuntimeError, match="engine is required"):
        registry.register(
            IndicatorPluginManifest(
                indicator_type="bad",
                engine_factory=None,  # type: ignore[arg-type]
                evaluation_mode="rolling",
            )
        )


def test_all_shipped_indicators_are_registered() -> None:
    ensure_builtin_indicator_plugins_registered()
    registry = plugin_registry()
    expected = {"market_profile", "pivot_level", "trendline", "vwap"}
    assert expected.issubset(set(registry.list_types()))


def test_session_based_modes_are_explicit() -> None:
    ensure_builtin_indicator_plugins_registered()
    registry = plugin_registry()
    assert registry.resolve("market_profile").evaluation_mode == "session"
    assert registry.resolve("vwap").evaluation_mode == "session"
    assert registry.resolve("pivot_level").evaluation_mode == "rolling"


def test_each_indicator_engine_produces_state_delta_smoke() -> None:
    ensure_builtin_indicator_plugins_registered()
    registry = plugin_registry()
    for indicator_type in ("market_profile", "pivot_level", "trendline", "vwap"):
        manifest = registry.resolve(indicator_type)
        engine = manifest.engine_factory({"id": f"{indicator_type}-id"})
        state = engine.initialize({"symbol": "BTC/USD"})
        delta = engine.apply_bar(state, _candle())
        assert delta is not None
        snapshot = engine.snapshot(state)
        assert snapshot.revision >= 0

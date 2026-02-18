from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

from engines.bot_runtime.core.indicator_state.plugins import plugin_registry
from signals.engine import signal_generator


@pytest.fixture(autouse=True)
def restore_registry():
    original_decorated = dict(signal_generator._DECORATED)
    registry = plugin_registry()
    original_plugins = dict(registry._plugins)
    original_pending_rules = dict(registry._pending_signal_rules)
    original_pending_overlays = dict(registry._pending_signal_overlay_adapters)
    try:
        signal_generator._DECORATED.clear()
        registry._plugins.clear()
        registry._pending_signal_rules.clear()
        registry._pending_signal_overlay_adapters.clear()
        yield
    finally:
        signal_generator._DECORATED.clear()
        signal_generator._DECORATED.update(original_decorated)
        registry._plugins.clear()
        registry._plugins.update(original_plugins)
        registry._pending_signal_rules.clear()
        registry._pending_signal_rules.update(original_pending_rules)
        registry._pending_signal_overlay_adapters.clear()
        registry._pending_signal_overlay_adapters.update(original_pending_overlays)


def test_market_profile_breakout_v3_rule_is_catalog_discoverable():
    if "pandas" not in sys.modules:
        class _DummyTimestamp:
            def __init__(self, value=None):
                self._value = value
                self.tzinfo = getattr(value, "tzinfo", None)
                try:
                    self.value = int(value.timestamp() * 10**9) if value is not None else 0
                except Exception:
                    self.value = 0

            def tz_convert(self, *_args, **_kwargs):
                return self

            def tz_localize(self, *_args, **_kwargs):
                return self

        sys.modules["pandas"] = types.SimpleNamespace(
            __spec__=None,
            DataFrame=object,
            Timestamp=_DummyTimestamp,
            Timedelta=lambda *_, **__: None,
            isna=lambda value: value is None,
        )

    module_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "signals"
        / "rules"
        / "market_profile"
        / "breakout_v3_confirmed.py"
    )
    spec = importlib.util.spec_from_file_location("test_breakout_v3_confirmed_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    descriptions = signal_generator.describe_indicator_rules("market_profile")
    ids = {str(item.get("id") or "") for item in descriptions}

    assert "market_profile_breakout_v3_confirmed" in ids

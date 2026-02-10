from typing import Any, Mapping, Sequence

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


def test_decorated_rule_and_overlay_registration():
    @signal_generator.signal_rule(
        "ExampleIndicator",
        rule_id="example_rule",
        label="Example",
        description="Example rule description",
    )
    def example_rule(
        context: Mapping[str, Any],
        payload: Any,
    ) -> Sequence[Mapping[str, Any]]:
        return [
            {
                "type": "example",
                "symbol": context.get("symbol", "TEST"),
                "time": 1,
                "confidence": 0.5,
            }
        ]

    @signal_generator.overlay_adapter("ExampleIndicator")
    def example_overlay(signals, df, **kwargs):
        return [
            {
                "type": "ExampleIndicator",
                "payload": len(signals),
                "columns": list(getattr(df, "columns", [])),
                "kwargs": sorted(kwargs.keys()),
            }
        ]

    registry = plugin_registry()
    assert tuple(registry.get_signal_rules("exampleindicator")) == (example_rule,)
    assert registry.get_signal_overlay_adapter("exampleindicator") is example_overlay

    descriptions = signal_generator.describe_indicator_rules("ExampleIndicator")
    assert descriptions == [
        {
            "id": "example_rule",
            "label": "Example",
            "description": "Example rule description",
        }
    ]


def test_overlay_adapter_updates_existing_registration():
    def legacy_rule(context: Mapping[str, Any], payload: Any):
        return []

    signal_generator.register_indicator_rules("Legacy", [legacy_rule])

    @signal_generator.overlay_adapter("Legacy")
    def legacy_overlay(signals, df, **kwargs):
        return []

    registry = plugin_registry()
    assert registry.get_signal_overlay_adapter("legacy") is legacy_overlay

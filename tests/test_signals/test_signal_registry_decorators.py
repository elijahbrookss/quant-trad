from typing import Any, Mapping, Sequence

import pytest

from signals.engine import signal_generator


@pytest.fixture(autouse=True)
def restore_registry():
    original_registry = dict(signal_generator._REGISTRY)
    original_decorated = dict(signal_generator._DECORATED)
    try:
        signal_generator._REGISTRY.clear()
        signal_generator._DECORATED.clear()
        yield
    finally:
        signal_generator._REGISTRY.clear()
        signal_generator._REGISTRY.update(original_registry)
        signal_generator._DECORATED.clear()
        signal_generator._DECORATED.update(original_decorated)


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

    registration = signal_generator._REGISTRY.get("ExampleIndicator")
    assert registration is not None
    assert tuple(registration.rules) == (example_rule,)
    assert registration.overlay_adapter is example_overlay

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

    registration = signal_generator._REGISTRY.get("Legacy")
    assert registration is not None
    assert registration.overlay_adapter is legacy_overlay

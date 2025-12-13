import pytest
from datetime import datetime, timezone

from signals.base import BaseSignal
from signals.engine import signal_generator
from signals.engine.signal_generator import (
    build_signal_overlays,
    register_indicator_rules,
    run_indicator_rules,
)


@pytest.fixture(autouse=True)
def reset_registry():
    original = dict(signal_generator._REGISTRY)
    signal_generator._REGISTRY.clear()
    try:
        yield
    finally:
        signal_generator._REGISTRY.clear()
        signal_generator._REGISTRY.update(original)


class DummyIndicator:
    NAME = "dummy"


def test_register_indicator_rules_rejects_duplicates():
    register_indicator_rules("dup", [lambda *_: []])

    with pytest.raises(ValueError):
        register_indicator_rules("dup", [lambda *_: []])


def test_run_indicator_rules_emits_base_signals():
    ts = datetime(2023, 4, 1, tzinfo=timezone.utc)
    payload = {"ts": ts, "value": 42}

    def dummy_rule(context, item):
        return [{
            "type": "dummy_breakout",
            "time": item["ts"],
            "symbol": context["symbol"],
            "extra": item["value"],
        }]

    register_indicator_rules(DummyIndicator.NAME, [dummy_rule])

    class DummyFrame:
        index = []
        shape = (2, 1)

    df = DummyFrame()

    signals = run_indicator_rules(
        DummyIndicator(),
        df,
        rule_payloads=[payload],
        symbol="ES",
    )

    assert len(signals) == 1
    signal = signals[0]
    assert isinstance(signal, BaseSignal)
    assert signal.type == "dummy_breakout"
    assert signal.symbol == "ES"
    assert signal.time == ts
    assert signal.metadata["extra"] == 42


def test_build_signal_overlays_uses_registered_adapter():
    called = {}

    def adapter(signals, plot_df, label_prefix="test"):
        called["signals"] = list(signals)
        called["plot_df_shape"] = plot_df.shape
        return [{"kind": "custom", "label": label_prefix}]

    register_indicator_rules("overlay", [lambda *_: []], overlay_adapter=adapter)

    dummy_signal = BaseSignal(
        type="x",
        symbol="ES",
        time=datetime(2023, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
        metadata={},
    )

    class DummyPlotFrame:
        shape = (1, 1)

    df = DummyPlotFrame()

    overlays = build_signal_overlays("overlay", [dummy_signal], df, label_prefix="ok")

    assert overlays == [
        {
            "type": "overlay",
            "payload": {
                "kind": "custom",
                "label": "ok",
                "bubbles": [],
                "markers": [],
                "price_lines": [],
                "polylines": [],
            },
        }
    ]
    assert called["signals"] == [dummy_signal]
    assert called["plot_df_shape"] == df.shape


def test_run_indicator_rules_injects_symbol_into_context():
    captured = {}

    def rule(context, payload):
        captured.update(context)
        return []

    indicator_type = "dummy_context"
    register_indicator_rules(indicator_type, [rule])

    class DummyFrame:
        index = []
        shape = (0, 0)

    df = DummyFrame()

    run_indicator_rules(indicator_type, df, symbol="NQ")

    assert captured.get("symbol") == "NQ"

from datetime import datetime, timezone
import logging

import pytest


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


def test_market_profile_indicator_injected_before_rules(monkeypatch, caplog):
    pd = pytest.importorskip("pandas")
    from indicators.market_profile import MarketProfileIndicator
    from signals.rules.market_profile import market_profile_breakout_rule
    import signals.rules.market_profile._bootstrap as bootstrap

    original_breakout = market_profile_breakout_rule
    captured_contexts = []

    def capturing_breakout(context, payload):
        captured_contexts.append(dict(context))
        return original_breakout(context, payload)

    monkeypatch.setattr(bootstrap, "market_profile_breakout_rule", capturing_breakout)

    register_indicator_rules("market_profile", [capturing_breakout])

    index = pd.date_range("2025-01-01 09:30", periods=3, freq="15min", tz="UTC")
    df = pd.DataFrame(
        {
            "open": [100.0, 100.5, 100.2],
            "high": [100.6, 101.0, 100.8],
            "low": [99.8, 100.2, 100.0],
            "close": [100.4, 100.7, 100.1],
            "volume": [1000, 1100, 1050],
        },
        index=index,
    )

    indicator = MarketProfileIndicator(df)

    caplog.set_level(logging.DEBUG, logger="MarketProfileBreakout")

    run_indicator_rules(indicator, df, rule_payloads=[{}])

    assert captured_contexts
    assert captured_contexts[0].get("market_profile") is indicator
    assert "No market profile data in context" not in caplog.text


def test_market_profile_breakout_uses_list_cache(monkeypatch):
    import sys
    import types

    if "pandas" not in sys.modules:
        sys.modules["pandas"] = types.SimpleNamespace(
            DataFrame=object,
            Timestamp=object,
            Timedelta=lambda *_, **__: None,
        )

    # Provide lightweight stubs for indicator package imports to avoid optional dependencies.
    if "indicators.market_profile" not in sys.modules:
        indicators_mod = types.ModuleType("indicators")
        indicators_mod.__path__ = []
        market_profile_mod = types.ModuleType("indicators.market_profile")

        pivot_mod = types.ModuleType("indicators.pivot_level")

        class _DummyLevel:
            ...

        class _DummyPivotIndicator:
            ...

        pivot_mod.Level = _DummyLevel
        pivot_mod.PivotLevelIndicator = _DummyPivotIndicator

        class _DummyIndicator:
            NAME = "market_profile"

            def __init__(self, *_args, **_kwargs):
                self.daily_profiles = []

        market_profile_mod.MarketProfileIndicator = _DummyIndicator
        indicators_mod.market_profile = market_profile_mod
        sys.modules.setdefault("indicators.pivot_level", pivot_mod)
        sys.modules.setdefault("indicators", indicators_mod)
        sys.modules.setdefault("indicators.market_profile", market_profile_mod)

    from signals.rules.market_profile import breakout as breakout_module

    calls = {}
    appended = []

    def fake_ensure_cache(context, key, default_factory, *, ready_flag=None, initialised_flag=None):
        calls["default_factory_type"] = type(default_factory())
        calls["ready_flag"] = ready_flag
        calls["initialised_flag"] = initialised_flag
        context.setdefault(key, default_factory())
        return context

    def fake_append_to_cache(context, key, items):
        snapshot = list(items)
        appended.append(snapshot)
        cache = context.get(key)
        if isinstance(cache, list):
            cache.extend(snapshot)
        else:
            context[key] = snapshot
        return context

    monkeypatch.setattr(breakout_module, "ensure_cache", fake_ensure_cache)
    monkeypatch.setattr(breakout_module, "append_to_cache", fake_append_to_cache)
    monkeypatch.setattr(breakout_module, "_resolve_breakout_bar_index", lambda *_, **__: 0)
    monkeypatch.setattr(
        breakout_module,
        "resolve_breakout_config",
        lambda ctx: type("Cfg", (), {"confirmation_bars": 1})(),
    )
    monkeypatch.setattr(
        breakout_module,
        "evaluate_signal_patterns",
        lambda **kwargs: [
            {"direction": "up", "level_type": "VAH", "level_price": 101.0}
        ],
    )

    class DummyIndicator:
        daily_profiles = [object()]

        def __bool__(self):
            return True

    class DummyFrame:
        empty = False

    context = {"df": DummyFrame(), "market_profile": DummyIndicator()}

    results = breakout_module.market_profile_breakout_rule(context, payload={})

    assert calls["default_factory_type"] is list
    assert calls["ready_flag"] is None
    assert calls["initialised_flag"] is None
    assert breakout_module._BREAKOUT_CACHE_KEY in context
    assert appended and appended[0][0]["level_type"] == "VAH"
    assert results and results[0]["direction"] == "up"
    assert context.get(breakout_module._BREAKOUT_READY_FLAG) is True

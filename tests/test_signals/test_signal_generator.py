from datetime import datetime, timezone
import logging
import sys
import types

import pytest

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


def test_build_signal_overlays_fallback_bubbles():
    class NoOverlayIndicator:
        NAME = "FallbackBubbleIndicator"

    signal = BaseSignal(
        type="breakout",
        symbol="ES",
        time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=0.5,
        metadata={"level_price": 101.0, "direction": "above"},
    )

    def noop_rule(context, payload):
        return []

    register_indicator_rules(NoOverlayIndicator.NAME, [noop_rule])

    overlays = signal_generator.build_signal_overlays(
        NoOverlayIndicator.NAME, [signal], [signal.time]
    )

    assert overlays, "Expected fallback overlays when adapter returns none"
    bubble_payload = overlays[0]["payload"]["bubbles"][0]
    assert bubble_payload["subtype"] == "bubble"
    assert bubble_payload["price"] == 101.0
    assert bubble_payload["time"] == int(signal.time.timestamp())


def test_market_profile_breakout_cache_carries_value_area(monkeypatch):
    import pkgutil
    import sys
    import types

    sys.modules.setdefault("requests", types.SimpleNamespace())
    sys.modules.setdefault("numpy", types.SimpleNamespace())
    sys.modules.setdefault("matplotlib", types.SimpleNamespace(patches=types.SimpleNamespace()))
    sys.modules.setdefault(
        "mplfinance",
        types.SimpleNamespace(plotting=types.SimpleNamespace(make_addplot=lambda *_, **__: None)),
    )
    sys.modules.setdefault("mplfinance.plotting", types.SimpleNamespace(make_addplot=lambda *_, **__: None))
    sys.modules.setdefault("indicators", types.SimpleNamespace(__path__=[], __spec__=None))
    sys.modules.setdefault(
        "indicators.market_profile",
        types.SimpleNamespace(MarketProfileIndicator=type("MarketProfileIndicator", (), {})),
    )
    sys.modules.setdefault(
        "indicators.trendline",
        types.SimpleNamespace(
            TrendlineIndicator=type("TrendlineIndicator", (), {}),
            trendline_overlay_adapter=lambda *_, **__: None,
        ),
    )
    sys.modules.setdefault(
        "indicators.pivot_level",
        types.SimpleNamespace(
            PivotLevelIndicator=type("PivotLevelIndicator", (), {}),
            Level=type("Level", (), {}),
            pivot_level_overlay_adapter=lambda *_, **__: None,
        ),
    )
    monkeypatch.setattr(pkgutil, "walk_packages", lambda *_, **__: [])

    import signals.rules.market_profile.breakout as breakout_rule

    match = {
        "type": "breakout",
        "pattern_id": "value_area_breakout",
        "time": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "breakout_direction": "above",
        "level_type": "VAH",
        "level_price": 101.0,
        "value_area_id": "session-123",
        "value_area_start": datetime(2024, 12, 31, tzinfo=timezone.utc),
        "value_area_end": datetime(2025, 1, 2, tzinfo=timezone.utc),
        "value_area_start_index": 10,
        "value_area_end_index": 20,
        "VAH": 101.5,
        "VAL": 99.5,
        "POC": 100.5,
    }

    monkeypatch.setattr(breakout_rule, "evaluate_signal_patterns", lambda *_, **__: [match])
    monkeypatch.setattr(breakout_rule, "resolve_breakout_config", lambda *_: None)
    monkeypatch.setattr(breakout_rule, "_resolve_breakout_bar_index", lambda *_: 12)

    class DummyIndicator:
        daily_profiles = [object()]

    class DummyFrame:
        empty = False
        index = list(range(30))

    context = {
        "df": DummyFrame(),
        "market_profile": DummyIndicator(),
    }

    signals = breakout_rule.market_profile_breakout_rule(context, {})

    assert signals, "Expected breakout signals"
    cached = context.get("market_profile_breakouts")
    assert cached and cached[0]["value_area_id"] == "session-123"
    assert cached[0]["value_area_start_index"] == 10
    assert cached[0]["value_area_end_index"] == 20


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
    eval_calls = {}

    def fake_evaluate_signal_patterns(ctx, payload, patterns, *, default_confidence=1.0):
        eval_calls["context"] = ctx
        eval_calls["payload"] = payload
        eval_calls["patterns"] = patterns
        eval_calls["default_confidence"] = default_confidence
        return [
            {
                "direction": "up",
                "level_type": "VAH",
                "level_price": 101.0,
                "time": 123,
                "type": "breakout",
            }
        ]

    monkeypatch.setattr(
        breakout_module, "evaluate_signal_patterns", fake_evaluate_signal_patterns
    )

    class DummyIndicator:
        daily_profiles = [object()]

        def __bool__(self):
            return True

    class DummyFrame:
        empty = False

    context = {"df": DummyFrame(), "market_profile": DummyIndicator()}

    payload = {"VAH": 101, "VAL": 99}
    results = breakout_module.market_profile_breakout_rule(context, payload)

    assert eval_calls["context"] is context
    assert eval_calls["payload"] == payload
    assert eval_calls["patterns"] == [breakout_module.BREAKOUT_PATTERN]
    assert calls["default_factory_type"] is list
    assert calls["ready_flag"] is None
    assert calls["initialised_flag"] is None
    assert breakout_module._BREAKOUT_CACHE_KEY in context
    assert appended and appended[0][0]["level_type"] == "VAH"
    assert results and results[0]["direction"] == "up"
    assert results[0]["time"] == 123
    assert results[0]["type"] == "breakout"
    assert context.get(breakout_module._BREAKOUT_READY_FLAG) is True


def test_market_profile_signal_logging_summary(monkeypatch, caplog):
    import sys
    import types

    if "pandas" not in sys.modules:
        sys.modules["pandas"] = types.SimpleNamespace(
            DataFrame=object,
            Timestamp=object,
            Timedelta=lambda *_, **__: None,
        )
    sys.modules.setdefault("requests", types.SimpleNamespace())
    sys.modules.setdefault("numpy", types.SimpleNamespace())

    signals_rules_module = types.ModuleType("signals.rules")
    signals_rules_module.__path__ = []
    market_profile_module = types.ModuleType("signals.rules.market_profile")
    market_profile_module.__path__ = []
    bootstrap_module = types.ModuleType("signals.rules.market_profile._bootstrap")
    bootstrap_module.ensure_breakouts_ready = lambda *_, **__: None
    market_profile_module._bootstrap = bootstrap_module
    signals_rules_module.market_profile = market_profile_module

    sys.modules.setdefault("signals.rules", signals_rules_module)
    sys.modules.setdefault("signals.rules.market_profile", market_profile_module)
    sys.modules.setdefault("signals.rules.market_profile._bootstrap", bootstrap_module)

    caplog.set_level(logging.INFO, logger="signals.engine.signal_generator")

    breakout_called = {}

    def fake_breakout(context, payload):
        breakout_called["payload"] = payload
        return [
            {"type": "breakout", "time": 1, "symbol": context["symbol"]},
            {"type": "breakout", "time": 2, "symbol": context["symbol"]},
        ]
    fake_breakout.signal_id = "market_profile_breakout"

    def fake_retest(context, payload):
        return [
            {"type": "retest", "time": 3, "symbol": context["symbol"]},
        ]
    fake_retest.signal_id = "market_profile_retest"

    register_indicator_rules(
        "market_profile", [fake_breakout, fake_retest]
    )

    class DummyFrame:
        empty = False
        index = []
        shape = (0, 0)

    signals = run_indicator_rules(
        "market_profile",
        DummyFrame(),
        rule_payloads=[{"foo": "bar"}],
        symbol="ABC",
        market_profile=object(),
    )

    assert breakout_called["payload"] == {"foo": "bar"}
    assert len(signals) == 3
    assert "Signal run triggered | indicator=market_profile | payloads=1" in caplog.text
    assert "Market profile signal summary | total=3 | breakouts=2 | retests=1" in caplog.text
    assert "Signal run complete | indicator=market_profile | total_signals=3" in caplog.text

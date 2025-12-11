from __future__ import annotations

import sys
import types
from typing import Any, Dict

import pytest

dummy_storage = types.ModuleType("portal.backend.service.storage")
dummy_storage.get_indicator = lambda *_, **__: None
dummy_storage.load_indicators = lambda *_, **__: []
dummy_storage.upsert_indicator = lambda *_, **__: None
dummy_storage.delete_indicator = lambda *_, **__: None
dummy_storage.strategies_for_indicator = lambda *_, **__: []
dummy_storage.upsert_strategy_indicator = lambda *_, **__: None
sys.modules.setdefault("portal.backend.service.storage", dummy_storage)


class _DataSource:
    ALPACA = type("E", (), {"value": "ALPACA"})
    CCXT = type("E", (), {"value": "CCXT"})

    def __init__(self, value):
        if value not in ("ALPACA", "CCXT"):
            raise ValueError
        self.value = value


class _DataContext:
    def __init__(self, **_):
        pass

    def validate(self):
        return None


class _DummyIndicator:
    NAME = "dummy"

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def from_context(cls, provider=None, ctx=None, **_):
        return cls()


sys.modules.setdefault(
    "data_providers.alpaca_provider", types.SimpleNamespace(AlpacaProvider=object)
)
sys.modules.setdefault(
    "data_providers.base_provider", types.SimpleNamespace(DataSource=_DataSource)
)
sys.modules.setdefault(
    "data_providers.factory", types.SimpleNamespace(get_provider=lambda *_, **__: object())
)
sys.modules.setdefault("indicators.config", types.SimpleNamespace(DataContext=_DataContext))
sys.modules.setdefault("indicators.market_profile", types.SimpleNamespace(MarketProfileIndicator=_DummyIndicator))
sys.modules.setdefault("indicators.pivot_level", types.SimpleNamespace(PivotLevelIndicator=_DummyIndicator))
sys.modules.setdefault("indicators.trendline", types.SimpleNamespace(TrendlineIndicator=_DummyIndicator))
sys.modules.setdefault("indicators.vwap", types.SimpleNamespace(VWAPIndicator=_DummyIndicator))
sys.modules.setdefault(
    "pandas",
    types.SimpleNamespace(DataFrame=type("_DF", (), {"copy": lambda self: self})),
)
sys.modules.setdefault(
    "signals.rules.pivot",
    types.SimpleNamespace(
        PivotBreakoutConfig=lambda **kwargs: types.SimpleNamespace(
            confirmation_bars=3,
            early_confirmation_window=2,
            early_confirmation_distance_pct=0.5,
            **kwargs,
        ),
        _PIVOT_BREAKOUT_READY_FLAG="ready",
    ),
)
sys.modules.setdefault(
    "signals.rules.market_profile",
    types.SimpleNamespace(
        MarketProfileBreakoutConfig=lambda **kwargs: types.SimpleNamespace(
            confirmation_bars=5,
            early_confirmation_window=4,
            early_confirmation_distance_pct=0.25,
            **kwargs,
        ),
        _BREAKOUT_CACHE_INITIALISED="init",
        _BREAKOUT_CACHE_KEY="cache_key",
        _BREAKOUT_READY_FLAG="ready_flag",
    ),
)
sys.modules.setdefault(
    "signals.base",
    types.SimpleNamespace(
        BaseSignal=type(
            "_BaseSignal",
            (),
            {
                "__init__": lambda self, **kwargs: setattr(self, "metadata", kwargs),
                "type": "breakout",
                "symbol": "BTC",
                "time": "t",
                "confidence": 1.0,
            },
        )
    ),
)

from portal.backend.service.data_provider_resolver import DataProviderResolver
from portal.backend.service.indicator_breakout_cache import IndicatorBreakoutCache
from portal.backend.service.indicator_cache import IndicatorCacheManager
from portal.backend.service.indicator_factory import IndicatorFactory
from portal.backend.service.indicator_repository import IndicatorRepository
from portal.backend.service.indicator_signal_runner import IndicatorSignalRunner


class _StubRepo(IndicatorRepository):
    def __init__(self, record: Dict[str, Any]):
        self.record = record
        self.upserts = []

    def get(self, inst_id: str):
        return dict(self.record) if self.record and self.record.get("id") == inst_id else None

    def upsert(self, payload):
        self.upserts.append(dict(payload))
        self.record = dict(payload)


class _StubFactory(IndicatorFactory):
    def __init__(self):
        super().__init__()
        self.calls = []

    def build_meta_from_record(self, record):
        self.calls.append(("meta", dict(record)))
        return dict(record)

    def build_indicator_instance(self, meta):
        self.calls.append(("instance", dict(meta)))
        return {"built_from": meta}


def test_indicator_repository_delegates(monkeypatch):
    calls = {}

    def fake_get(inst_id):
        calls.setdefault("get", inst_id)
        return {"id": inst_id}

    def fake_load():
        calls.setdefault("load", True)
        return [{"id": "abc"}]

    def fake_upsert(payload):
        calls.setdefault("upsert", payload)

    monkeypatch.setattr(
        "portal.backend.service.indicator_repository.storage.get_indicator", fake_get
    )
    monkeypatch.setattr(
        "portal.backend.service.indicator_repository.storage.load_indicators", fake_load
    )
    monkeypatch.setattr(
        "portal.backend.service.indicator_repository.storage.upsert_indicator", fake_upsert
    )

    repo = IndicatorRepository()
    assert repo.get("abc") == {"id": "abc"}
    assert repo.load()[0]["id"] == "abc"
    repo.upsert({"id": "abc"})

    assert calls == {"get": "abc", "load": True, "upsert": {"id": "abc"}}


def test_cache_manager_reuses_instances_and_backfills(monkeypatch):
    base_record = {
        "id": "inst-1",
        "params": {"symbol": "ETH", "start": "s", "end": "e", "interval": "1h"},
    }
    repo = _StubRepo(base_record)
    factory = _StubFactory()
    manager = IndicatorCacheManager(repository=repo, factory=factory)

    first = manager.get_entry("inst-1")
    second = manager.get_entry("inst-1")

    assert first is second
    assert factory.calls[0][0] == "meta"

    backfill_repo = _StubRepo({"id": "inst-2", "params": {}})
    backfill_manager = IndicatorCacheManager(repository=backfill_repo, factory=factory)
    backfill_entry = backfill_manager.get_entry(
        "inst-2",
        fallback_context={"symbol": "BTC", "start": "s", "end": "e", "interval": "15m"},
        persist_backfill=True,
    )

    assert backfill_repo.upserts, "expected backfill to persist updated params"
    assert backfill_repo.upserts[0]["params"]["symbol"] == "BTC"
    assert backfill_entry.meta["params"]["symbol"] == "BTC"


def test_indicator_factory_color_defaults():
    factory = IndicatorFactory()
    record = {"id": "abc", "type": "pivot_level", "params": {}}
    meta = factory.build_meta_from_record(record)

    assert meta["color"] == "#4f46e5"
    assert meta["name"] == "pivot_level"


def test_signal_runner_delegates(monkeypatch):
    runner = IndicatorSignalRunner()
    calls = {}

    def fake_describe(indicator_type):
        calls.setdefault("describe", indicator_type)
        return ["rule"]

    def fake_run(*_, **kwargs):
        calls.setdefault("run", kwargs)
        return ["signals"]

    def fake_overlays(*_, **kwargs):
        calls.setdefault("overlays", kwargs)
        return {"over": True}

    monkeypatch.setattr(
        "portal.backend.service.indicator_signal_runner.describe_indicator_rules",
        fake_describe,
    )
    monkeypatch.setattr(
        "portal.backend.service.indicator_signal_runner.run_indicator_rules", fake_run
    )
    monkeypatch.setattr(
        "portal.backend.service.indicator_signal_runner.build_signal_overlays", fake_overlays
    )

    assert runner.describe_rules("demo") == ["rule"]
    runner.run_rules("indicator", "df", mode="live")
    runner.build_overlays("indicator", ["sig"], "df", mode="live")

    assert calls["describe"] == "demo"
    assert calls["run"] == {"mode": "live"}
    assert calls["overlays"]["mode"] == "live"


def test_signal_runner_catalog_enriches(monkeypatch):
    runner = IndicatorSignalRunner()

    monkeypatch.setattr(
        "portal.backend.service.indicator_signal_runner.describe_indicator_rules",
        lambda indicator_type: [{"id": f"{indicator_type}_breakout", "label": "Breakout"}],
    )

    catalog = runner.build_signal_catalog("market_profile")

    assert catalog[0]["signal_type"] == "breakout"
    assert any(direction["id"] == "long" for direction in catalog[0]["directions"])


def test_data_provider_resolver_defaults_and_normalizes(monkeypatch):
    resolver = DataProviderResolver()
    calls = []

    monkeypatch.setattr(
        "portal.backend.service.data_provider_resolver.AlpacaProvider", lambda: "alpaca-provider"
    )

    def fake_provider(datasource, exchange=None):
        calls.append((datasource, exchange))
        return {"ds": datasource, "ex": exchange}

    monkeypatch.setattr(
        "portal.backend.service.data_provider_resolver.get_provider", fake_provider
    )

    assert resolver.normalize_datasource("alpaca") == "ALPACA"
    assert resolver.normalize_exchange("BINANCE") == "binance"
    assert resolver.resolve(None) == "alpaca-provider"

    provider = resolver.resolve(None, exchange="binance")
    assert provider == {"ds": "CCXT", "ex": "binance"}
    assert calls[-1] == ("CCXT", "binance")


def test_breakout_cache_round_trip():
    cache = IndicatorBreakoutCache()
    cache_key = cache.build_cache_key("id", "pivot_level", "BTC", "1h", "s", "e", ("sig",))

    assert cache.get_cached_breakouts(cache_key) is None

    cache.store_breakout_cache(cache_key, [{"value": 1}])
    cached = cache.get_cached_breakouts(cache_key)

    assert cached == [{"value": 1}]
    cached.append({"value": 2})
    assert cache.get_cached_breakouts(cache_key) == [{"value": 1}]

    cache.purge_indicator("id")
    assert cache.get_cached_breakouts(cache_key) is None


def test_breakout_cache_overlay_clone_updates_symbol():
    cache = IndicatorBreakoutCache()
    indicator = _DummyIndicator(symbol="ETH", bin_size=10, mode="tpo", interval="30m")

    class _DummyDF:
        def copy(self):
            return self

    df = _DummyDF()

    clone = cache.build_market_profile_overlay_indicator(
        indicator, df, interval="15m", symbol="BTC"
    )

    assert getattr(clone, "symbol", None) == "BTC"
    assert getattr(clone, "interval", None) == "15m"

from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.indicators.indicator_factory import IndicatorFactory
from indicators.market_profile import MarketProfileIndicator


class _FakeResolver:
    def normalize_datasource(self, value):  # noqa: ANN001
        return value

    def normalize_exchange(self, value):  # noqa: ANN001
        return value

    def resolve(self, datasource, exchange=None):  # noqa: ANN001
        _ = datasource, exchange
        return object()


def test_market_profile_build_uses_incremental_cache(monkeypatch):
    called = {"incremental": 0, "regular": 0}

    def _from_context_with_incremental_cache(cls, **kwargs):  # noqa: ANN001
        _ = cls, kwargs
        called["incremental"] += 1
        return SimpleNamespace()

    def _from_context(cls, **kwargs):  # noqa: ANN001
        _ = cls, kwargs
        called["regular"] += 1
        return SimpleNamespace()

    monkeypatch.setattr(
        MarketProfileIndicator,
        "from_context_with_incremental_cache",
        classmethod(_from_context_with_incremental_cache),
    )
    monkeypatch.setattr(
        MarketProfileIndicator,
        "from_context",
        classmethod(_from_context),
    )

    from portal.backend.service.market import instrument_service

    monkeypatch.setattr(instrument_service, "require_instrument_id", lambda *a, **k: "inst-1")

    factory = IndicatorFactory(
        resolver=_FakeResolver(),
        ctx=SimpleNamespace(incremental_cache=object()),
    )
    meta = {
        "id": "ind-1",
        "type": "market_profile",
        "params": {
            "symbol": "BTC/USDT",
            "start": "2026-01-01T00:00:00+00:00",
            "end": "2026-01-02T00:00:00+00:00",
            "interval": "1m",
            "days_back": 10,
            "use_merged_value_areas": True,
            "merge_threshold": 0.6,
            "min_merge_sessions": 3,
        },
        "datasource": "demo",
        "exchange": "demo",
    }

    factory.build_indicator_instance(meta)

    assert called["incremental"] == 1
    assert called["regular"] == 0

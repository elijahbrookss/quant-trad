import types
from datetime import datetime, timedelta, timezone

import pytest

pd = pytest.importorskip("pandas")

from data_providers.providers import DataSource
from data_providers.providers import factory as provider_factory
from data_providers.providers import interactive_brokers as ib_module


class DummyIB:
    """Test double that mimics the subset of the IB API we rely on."""

    def __init__(self):
        self.connected = False
        self.contracts = []

    # Connection management -------------------------------------------------
    def isConnected(self):  # pragma: no cover - signature defined by ib_insync
        return self.connected

    def connect(self, host, port, clientId, readonly=True):  # pragma: no cover
        self.connected = True

    # Contract helpers ------------------------------------------------------
    def qualifyContracts(self, contract):  # pragma: no cover
        self.contracts.append(contract)
        return [contract]

    # Historical data -------------------------------------------------------
    def reqHistoricalData(
        self,
        contract,
        endDateTime=None,
        durationStr=None,
        barSizeSetting=None,
        whatToShow=None,
        useRTH=False,
        formatDate=1,
        keepUpToDate=False,
    ):
        self.contracts.append(contract)
        return [
            types.SimpleNamespace(
                date=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc).isoformat(),
                open=100.0,
                high=101.5,
                low=99.8,
                close=101.0,
                volume=1500,
            )
        ]


def _patch_ib_dependencies(monkeypatch):
    """Patch ib_insync dependencies so the provider runs offline."""

    monkeypatch.setattr(ib_module, "IB", DummyIB)

    def _to_frame(bars):
        rows = [
            {
                "date": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            for bar in bars
        ]
        return pd.DataFrame(rows)

    monkeypatch.setattr(ib_module, "util", types.SimpleNamespace(df=_to_frame))
    monkeypatch.setattr(ib_module.InteractiveBrokersProvider, "_load_symbol_overrides", lambda self: {})


def test_factory_returns_ib_provider(monkeypatch):
    """The provider factory should instantiate and cache the IB implementation."""

    _patch_ib_dependencies(monkeypatch)
    provider_factory.reset_provider_cache()

    provider = provider_factory.get_provider(DataSource.IBKR.value, exchange="SMART")
    assert isinstance(provider, ib_module.InteractiveBrokersProvider)
    assert provider.get_datasource() == DataSource.IBKR.value

    again = provider_factory.get_provider(DataSource.IBKR.value, exchange="smart")
    assert again is provider


def test_ib_provider_fetches_history(monkeypatch):
    """Fetching data from the IB provider yields a normalised dataframe."""

    _patch_ib_dependencies(monkeypatch)
    provider = ib_module.InteractiveBrokersProvider()

    end = datetime.now(tz=timezone.utc).replace(microsecond=0)
    start = end - timedelta(minutes=30)

    frame = provider.fetch_from_api("AAPL", start.isoformat(), end.isoformat(), "1m")

    assert list(frame.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert not frame.empty
    assert frame["timestamp"].dt.tz is not None


def test_factory_legacy_cache_alias_points_to_registry_cache():
    """Legacy cache alias should remain a live view of the registry cache."""

    provider_factory.reset_provider_cache()
    assert provider_factory._PROVIDER_CACHE is provider_factory._REGISTRY.cache

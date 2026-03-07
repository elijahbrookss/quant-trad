from __future__ import annotations

import types

import pytest

pytest.importorskip("pandas")

from data_providers.registry import ProviderConfig
from data_providers.providers import factory


class _DummyProvider:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def test_registry_instantiates_provider_from_provider_config(monkeypatch):
    registry = factory.ProviderRegistry(runtime_config={"mode": "test"})

    monkeypatch.setattr(factory, "_resolve_ids", lambda provider_id, venue_id: ("DUMMY", None))
    monkeypatch.setattr(
        factory,
        "get_provider_config",
        lambda provider: ProviderConfig(
            id="DUMMY",
            label="Dummy",
            supported_venues=[],
            implementation_module="dummy.module",
            implementation_class="DummyProvider",
        ),
    )

    module = types.SimpleNamespace(DummyProvider=_DummyProvider)
    monkeypatch.setattr(factory, "import_module", lambda _name: module)

    instance = registry.get_provider("DUMMY")

    assert isinstance(instance, _DummyProvider)
    assert instance.kwargs["settings"] == {"mode": "test"}


def test_registry_fails_loud_when_provider_implementation_missing(monkeypatch):
    registry = factory.ProviderRegistry(runtime_config={"mode": "test"})

    monkeypatch.setattr(factory, "_resolve_ids", lambda provider_id, venue_id: ("BROKEN", None))
    monkeypatch.setattr(
        factory,
        "get_provider_config",
        lambda provider: ProviderConfig(
            id="BROKEN",
            label="Broken",
            supported_venues=[],
        ),
    )

    with pytest.raises(RuntimeError, match="provider_factory_implementation_missing"):
        registry.get_provider("BROKEN")


def test_registry_passes_exchange_slug_when_provider_requires_exchange_id(monkeypatch):
    registry = factory.ProviderRegistry(runtime_config={"mode": "test"})

    monkeypatch.setattr(factory, "_resolve_ids", lambda provider_id, venue_id: ("CCXT", "BINANCE_US"))
    monkeypatch.setattr(
        factory,
        "get_provider_config",
        lambda provider: ProviderConfig(
            id="CCXT",
            label="ccxt",
            supported_venues=["BINANCE_US"],
            implementation_module="dummy.ccxt",
            implementation_class="DummyCcxtProvider",
        ),
    )

    class _DummyCcxtProvider:
        def __init__(self, exchange_id: str, persistence=None, settings=None):
            self.exchange_id = exchange_id
            self.persistence = persistence
            self.settings = settings

    module = types.SimpleNamespace(DummyCcxtProvider=_DummyCcxtProvider)
    monkeypatch.setattr(factory, "import_module", lambda _name: module)

    instance = registry.get_provider("CCXT", venue="BINANCE_US")

    assert instance.exchange_id == "binanceus"

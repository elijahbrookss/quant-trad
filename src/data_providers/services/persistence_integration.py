"""Integration hook to wire provider persistence without coupling layers."""

from __future__ import annotations

from typing import Callable

from ..providers import factory as provider_factory


def configure_provider_persistence(factory: Callable[[], object]) -> None:
    """Register the persistence factory for data providers.

    This keeps the provider layer decoupled from service implementations.
    """

    provider_factory.configure_persistence_factory(factory)


__all__ = ["configure_provider_persistence"]

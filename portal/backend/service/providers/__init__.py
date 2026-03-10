"""Provider services and resolvers.

Module imports are intentionally side-effect free so test collection and light
consumers do not trigger persistence/bootstrap wiring.
"""

from .data_provider_resolver import DataProviderResolver
from .provider_service import translate_market


def ensure_provider_persistence_bootstrap() -> None:
    """Explicitly trigger provider persistence bootstrap when required."""

    from . import persistence_bootstrap  # noqa: F401


__all__ = [
    "DataProviderResolver",
    "translate_market",
    "ensure_provider_persistence_bootstrap",
]

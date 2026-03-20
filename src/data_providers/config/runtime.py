from dataclasses import dataclass
from typing import Optional

from core.settings import get_settings

_SETTINGS = get_settings()

@dataclass(frozen=True)
class PersistenceConfig:
    """Database configuration for persisting OHLCV candles and closure windows."""

    dsn: Optional[str]
    candles_raw_table: str
    derivatives_state_table: str
    closures_table: str


@dataclass(frozen=True)
class ProviderRuntimeConfig:
    """Runtime knobs shared across data providers."""

    history_segment_points: int
    persistence: PersistenceConfig


def _parse_int(value: Optional[str], default: int) -> int:
    try:
        return max(1, int(value)) if value is not None else default
    except (TypeError, ValueError):
        return default


def runtime_config_from_env() -> ProviderRuntimeConfig:
    """Build a runtime configuration from environment variables."""

    runtime_settings = _SETTINGS.providers.runtime
    return ProviderRuntimeConfig(
        history_segment_points=_parse_int(runtime_settings.history_segment_points, 1000),
        persistence=PersistenceConfig(
            dsn=_SETTINGS.database.dsn,
            candles_raw_table=runtime_settings.persistence.candles_raw_table,
            derivatives_state_table=runtime_settings.persistence.derivatives_state_table,
            closures_table=runtime_settings.persistence.closures_table,
        ),
    )


__all__ = [
    "PersistenceConfig",
    "ProviderRuntimeConfig",
    "runtime_config_from_env",
]

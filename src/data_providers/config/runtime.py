from dataclasses import dataclass
from typing import Optional
import os


@dataclass(frozen=True)
class PersistenceConfig:
    """Database configuration for persisting OHLCV candles and closure windows."""

    dsn: Optional[str]
    ohlc_table: str
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

    return ProviderRuntimeConfig(
        history_segment_points=_parse_int(os.getenv("HISTORY_SEGMENT_POINTS"), 1000),
        persistence=PersistenceConfig(
            dsn=os.getenv("PG_DSN"),
            ohlc_table=os.getenv("OHLC_TABLE", "ohlc_raw"),
            closures_table=os.getenv("OHLC_CLOSURES_TABLE", "portal_ohlc_closures"),
        ),
    )


__all__ = [
    "PersistenceConfig",
    "ProviderRuntimeConfig",
    "runtime_config_from_env",
]

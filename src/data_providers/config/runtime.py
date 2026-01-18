from dataclasses import dataclass
from typing import Optional
import os


@dataclass(frozen=True)
class PersistenceConfig:
    """Database configuration for persisting OHLCV candles and closure windows."""

    dsn: Optional[str]
    candles_raw_table: str
    candle_stats_table: str
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

    return ProviderRuntimeConfig(
        history_segment_points=_parse_int(os.getenv("HISTORY_SEGMENT_POINTS"), 1000),
        persistence=PersistenceConfig(
            dsn=os.getenv("PG_DSN"),
            candles_raw_table=os.getenv("CANDLES_RAW_TABLE", "market_candles_raw"),
            candle_stats_table=os.getenv("CANDLE_STATS_TABLE", "candle_stats"),
            derivatives_state_table=os.getenv("DERIVATIVES_STATE_TABLE", "derivatives_market_state"),
            closures_table=os.getenv("CANDLE_CLOSURES_TABLE", "portal_candle_closures"),
        ),
    )


__all__ = [
    "PersistenceConfig",
    "ProviderRuntimeConfig",
    "runtime_config_from_env",
]

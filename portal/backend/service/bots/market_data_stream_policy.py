"""Bot-owned market data stream reconnect policy."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict

from core.settings import get_settings
from data_providers.streams.runtime import StreamReconnectPolicy


def default_market_data_stream_policy() -> Dict[str, Any]:
    settings = get_settings().bot_runtime.market_data_stream_policy
    return {
        "reconnect_enabled": bool(settings.reconnect_enabled),
        "initial_backoff_seconds": float(settings.initial_backoff_seconds),
        "max_backoff_seconds": float(settings.max_backoff_seconds),
        "continuous_disconnect_budget_seconds": float(settings.continuous_disconnect_budget_seconds),
        "heartbeat_stale_seconds": float(settings.heartbeat_stale_seconds),
    }


def normalize_market_data_stream_policy(value: Mapping[str, Any] | None) -> Dict[str, Any]:
    if value is not None and not isinstance(value, Mapping):
        raise ValueError("market_data_stream_policy must be an object")
    defaults = default_market_data_stream_policy()
    return StreamReconnectPolicy.from_mapping(
        value,
        defaults=StreamReconnectPolicy.from_mapping(defaults),
    ).to_dict()


__all__ = [
    "default_market_data_stream_policy",
    "normalize_market_data_stream_policy",
]

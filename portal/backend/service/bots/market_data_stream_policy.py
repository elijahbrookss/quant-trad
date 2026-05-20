"""Bot-owned market data stream reconnect policy."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict

from core.settings import get_settings


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
    policy = default_market_data_stream_policy()
    if value is None:
        return policy
    if not isinstance(value, Mapping):
        raise ValueError("market_data_stream_policy must be an object")
    for key in policy:
        if key in value and value[key] is not None:
            policy[key] = value[key]

    policy["reconnect_enabled"] = _coerce_bool(policy.get("reconnect_enabled"), "reconnect_enabled")
    policy["initial_backoff_seconds"] = _coerce_float(
        policy.get("initial_backoff_seconds"),
        "initial_backoff_seconds",
        minimum=0.0,
    )
    policy["max_backoff_seconds"] = _coerce_float(
        policy.get("max_backoff_seconds"),
        "max_backoff_seconds",
        minimum=0.001,
    )
    policy["continuous_disconnect_budget_seconds"] = _coerce_float(
        policy.get("continuous_disconnect_budget_seconds"),
        "continuous_disconnect_budget_seconds",
        minimum=0.001,
    )
    policy["heartbeat_stale_seconds"] = _coerce_float(
        policy.get("heartbeat_stale_seconds"),
        "heartbeat_stale_seconds",
        minimum=0.001,
    )
    if policy["max_backoff_seconds"] < policy["initial_backoff_seconds"]:
        raise ValueError("market_data_stream_policy.max_backoff_seconds must be >= initial_backoff_seconds")
    return policy


def _coerce_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    raise ValueError(f"market_data_stream_policy.{field} must be boolean")


def _coerce_float(value: Any, field: str, *, minimum: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"market_data_stream_policy.{field} must be numeric") from None
    if number < minimum:
        raise ValueError(f"market_data_stream_policy.{field} must be >= {minimum}")
    return number


__all__ = [
    "default_market_data_stream_policy",
    "normalize_market_data_stream_policy",
]

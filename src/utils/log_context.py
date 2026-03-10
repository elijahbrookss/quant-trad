"""Logging context helpers for bot runtime."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

LOG_CONTEXT_ORDER = (
    "request_id",
    "session_id",
    "bot_id",
    "bot_mode",
    "run_id",
    "strategy_id",
    "strategy_name",
    "symbol",
    "timeframe",
    "provider",
    "datasource",
    "exchange",
    "instrument_id",
    "indicator_id",
    "indicator_type",
    "indicator_version",
    "trade_id",
    "playback_time",
    "bar_time",
)


def build_log_context(**fields: Optional[object]) -> Dict[str, object]:
    """Return a filtered log context dict without empty values."""
    context: Dict[str, object] = {}
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        context[key] = value
    return context


def merge_log_context(*contexts: Mapping[str, object], **fields: Optional[object]) -> Dict[str, object]:
    """Merge multiple context mappings and extra fields into a new dict."""
    merged: Dict[str, object] = {}
    for ctx in contexts:
        for key, value in (ctx or {}).items():
            merged[key] = value
    merged.update(build_log_context(**fields))
    return merged


def format_log_context(context: Mapping[str, object]) -> str:
    """Render log context as a stable key=value string."""
    if not context:
        return ""
    parts = []
    ordered = set(LOG_CONTEXT_ORDER)
    for key in LOG_CONTEXT_ORDER:
        if key not in context:
            continue
        parts.append(_format_kv(key, context[key]))
    extras = sorted(k for k in context.keys() if k not in ordered)
    for key in extras:
        parts.append(_format_kv(key, context[key]))
    return " | ".join(parts)


def with_log_context(message: str, context: Mapping[str, object]) -> str:
    """Append formatted context to a message if present."""
    rendered = format_log_context(context)
    if not rendered:
        return message
    return f"{message} | {rendered}"


def series_log_context(series: Any, **fields: Optional[object]) -> Dict[str, object]:
    """Build log context from a StrategySeries-like object."""
    return build_log_context(
        strategy_id=getattr(series, "strategy_id", None),
        strategy_name=getattr(series, "name", None),
        symbol=getattr(series, "symbol", None),
        timeframe=getattr(series, "timeframe", None),
        datasource=getattr(series, "datasource", None),
        exchange=getattr(series, "exchange", None),
        **fields,
    )


def strategy_log_context(strategy: Any, **fields: Optional[object]) -> Dict[str, object]:
    """Build log context from a Strategy-like object."""
    return build_log_context(
        strategy_id=getattr(strategy, "id", None),
        strategy_name=getattr(strategy, "name", None),
        timeframe=getattr(strategy, "timeframe", None),
        datasource=getattr(strategy, "datasource", None),
        exchange=getattr(strategy, "exchange", None),
        **fields,
    )


def _format_kv(key: str, value: object) -> str:
    if isinstance(value, float):
        return f"{key}={value:.8f}"
    return f"{key}={value}"


__all__ = [
    "LOG_CONTEXT_ORDER",
    "build_log_context",
    "merge_log_context",
    "format_log_context",
    "with_log_context",
    "series_log_context",
    "strategy_log_context",
]

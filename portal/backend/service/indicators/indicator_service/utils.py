from __future__ import annotations

import logging
import math
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Tuple

from .context import IndicatorServiceContext, _context

logger = logging.getLogger(__name__)


def build_meta_from_record(
    record: Mapping[str, Any], *, ctx: IndicatorServiceContext = _context
) -> Dict[str, Any]:
    return ctx.factory.build_meta_from_record(record)


def load_indicator_record(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    record = ctx.repository.get(inst_id)
    if not record:
        raise KeyError("Indicator not found")
    return record


# Runtime params that should NOT be stored in indicator config
# - datasource, exchange: stored in separate fields at top level
# - symbol/start/end/interval and execution ids are runtime context, not config
_RUNTIME_PARAM_KEYS = {
    "datasource",
    "exchange",
    "symbol",
    "start",
    "end",
    "interval",
    "provider_id",
    "venue_id",
    "instrument_id",
    "bot_id",
    "strategy_id",
    "bot_mode",
    "run_id",
}


def purge_overlay_cache(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    ctx.overlay_cache.purge_indicator(inst_id)


def scrub_runtime_params(params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(params, Mapping):
        return {}
    cleaned: Dict[str, Any] = {}
    for key, value in params.items():
        if key in _RUNTIME_PARAM_KEYS:
            continue
        cleaned[key] = value
    return cleaned


def coerce_int(value: Any, default: int, *, minimum: Optional[int] = None) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and result < minimum:
        return default
    return result


def coerce_float(value: Any, default: float, *, minimum: Optional[float] = None) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result) or math.isinf(result):
        return default
    if minimum is not None and result < minimum:
        return default
    return result


def normalize_color(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def ensure_color(meta: Dict[str, Any], *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    if "color" not in meta:
        meta["color"] = None
    meta["params"] = scrub_runtime_params(meta.get("params") or {})
    return meta


def normalize_datasource(
    value: Optional[str], *, ctx: IndicatorServiceContext = _context
) -> Optional[str]:
    return ctx.resolver.normalize_datasource(value)


def normalize_exchange(
    value: Optional[str], *, ctx: IndicatorServiceContext = _context
) -> Optional[str]:
    return ctx.resolver.normalize_exchange(value)


def resolve_data_provider(
    datasource: Optional[str], *, exchange: Optional[str] = None, ctx: IndicatorServiceContext = _context
):
    """Resolve data provider from datasource/exchange.

    No defaults: datasource must be explicitly provided.
    Fails loudly if datasource is missing.
    """
    ds = normalize_datasource(datasource, ctx=ctx)
    if not ds:
        raise ValueError("datasource is required to resolve data provider")
    ex = normalize_exchange(exchange, ctx=ctx)
    return ctx.resolver.resolve(ds, exchange=ex)


def pull_datasource_exchange(
    params: Dict[str, Any],
    *,
    fallback_meta: Optional[Mapping[str, Any]] = None,
    ctx: IndicatorServiceContext = _context,
) -> Tuple[Optional[str], Optional[str]]:
    """Extract datasource/exchange from params with fallback to meta.

    Allows fallback to stored meta values, but NO hardcoded defaults.
    If datasource is missing from both params and meta, this will return None
    and the caller should handle the error.
    """
    defaults = fallback_meta or {}
    datasource = normalize_datasource(params.pop("datasource", defaults.get("datasource")), ctx=ctx)
    exchange = normalize_exchange(params.pop("exchange", defaults.get("exchange")), ctx=ctx)
    # No implicit defaults: caller must provide datasource explicitly
    return datasource, exchange


def sanitize_json(obj):
    import numpy as np
    import pandas as pd

    if isinstance(obj, (int,)) or isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        v = float(obj)
        return v if math.isfinite(v) else None
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if isinstance(obj, dict):
        cleaned = {k: sanitize_json(v) for k, v in obj.items()}
        if ("price" in cleaned and cleaned["price"] is None) or ("value" in cleaned and cleaned["value"] is None):
            return None
        return {k: v for k, v in cleaned.items() if v is not None}
    if isinstance(obj, (list, tuple)):
        return [v for v in (sanitize_json(v) for v in obj) if v is not None]
    return obj


__all__ = [
    "_context",
    "IndicatorServiceContext",
    "build_meta_from_record",
    "coerce_float",
    "coerce_int",
    "ensure_color",
    "load_indicator_record",
    "normalize_color",
    "normalize_datasource",
    "normalize_exchange",
    "pull_datasource_exchange",
    "purge_overlay_cache",
    "resolve_data_provider",
    "sanitize_json",
    "scrub_runtime_params",
]

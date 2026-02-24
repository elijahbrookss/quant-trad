from __future__ import annotations

import inspect
import logging
import math
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from data_providers import DataSource
from signals.engine.signal_generator import describe_indicator_rules

from .context import IndicatorServiceContext, _context

logger = logging.getLogger(__name__)


_RULE_HINTS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "market_profile": {
        "market_profile_breakout": {
            "signal_type": "breakout",
            "directions": [
                {
                    "id": "long",
                    "label": "Long breakout",
                    "description": "Breakout above the active value area high (VAH) that confirms continuation.",
                },
                {
                    "id": "short",
                    "label": "Short breakdown",
                    "description": "Breakdown below the active value area low (VAL) signalling downside momentum.",
                },
            ],
        },
        "market_profile_retest": {
            "signal_type": "retest",
            "directions": [
                {
                    "id": "long",
                    "label": "Long retest",
                    "description": (
                        "Breakout above VAH with a successful retest hold or a reclaim of VAL after a breakout,"
                        " favouring continuation to the upside."
                    ),
                },
                {
                    "id": "short",
                    "label": "Short retest",
                    "description": (
                        "Breakdown below VAH with a rejection retest or a breakdown of VAL that holds, signalling continuation lower."
                    ),
                },
            ],
        },
    },
    "pivot_level": {
        "pivot_breakout": {
            "signal_type": "breakout",
        },
        "pivot_retest": {
            "signal_type": "retest",
        },
    },
}

_RULE_SUNSET: Dict[str, set[str]] = {
    "market_profile": {
        "market_profile_breakout_v2",
        "market_profile_retest_v2",
        "market_profile_breakout_v3_confirmed",
    }
}


def build_meta_from_record(
    record: Mapping[str, Any], *, ctx: IndicatorServiceContext = _context
) -> Dict[str, Any]:
    return ctx.factory.build_meta_from_record(record)


def build_indicator_instance(meta: Mapping[str, Any], *, ctx: IndicatorServiceContext = _context):
    return ctx.factory.build_indicator_instance(meta)


def load_indicator_record(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    record = ctx.repository.get(inst_id)
    if not record:
        raise KeyError("Indicator not found")
    return record


def get_indicator_entry(
    inst_id: str,
    *,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    build_instance: bool = True,
    ctx: IndicatorServiceContext = _context,
):
    """Build indicator entry directly from DB without caching.

    This ensures we always use fresh parameters from the database,
    avoiding stale cached instances with outdated configuration.
    """
    from dataclasses import dataclass
    from typing import Any

    @dataclass
    class IndicatorEntry:
        """Temporary container for indicator metadata and instance."""
        meta: Dict[str, Any]
        instance: Optional[Any]
        updated_at: Optional[str] = None

    # Load fresh record from DB
    record = ctx.repository.get(inst_id)
    if not record:
        raise KeyError("Indicator not found")

    # Build meta and instance fresh from DB record
    meta = ctx.factory.build_meta_from_record(record)

    instance = None
    if build_instance:
        instance = ctx.factory.build_indicator_instance(
            meta,
            datasource=datasource,
            exchange=exchange,
        )

    return IndicatorEntry(
        meta=meta,
        instance=instance,
        updated_at=str(record.get("updated_at") or "")
    )


# REMOVED: refresh_strategy_links function
# Strategies now load indicators fresh from DB, no snapshot refresh needed


# Runtime params that should NOT be stored in indicator config
# - datasource, exchange: stored in separate fields at top level
# - symbol, start, end, interval: DataContext fields (runtime context, not config)
_RUNTIME_PARAM_KEYS = {"datasource", "exchange", "symbol", "start", "end", "interval"}


def purge_breakout_cache(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    ctx.breakout_cache.purge_indicator(inst_id)


def purge_overlay_cache(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    ctx.overlay_cache.purge_indicator(inst_id)


def purge_incremental_cache(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    ctx.incremental_cache.purge_indicator(inst_id)


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


def attach_signal_catalog(
    meta: Dict[str, Any], *, ctx: IndicatorServiceContext = _context
) -> Dict[str, Any]:
    indicator_type = meta.get("type") or meta.get("name")

    if not indicator_type:
        logger.warning("attach_signal_catalog_missing_type | meta_keys=%s", list(meta.keys()))
        return meta

    catalog = build_signal_catalog(str(indicator_type))

    if catalog:
        meta["signal_rules"] = catalog
    return meta


def _guess_signal_type(indicator_type: str, rule_id: str) -> str:
    hints = _RULE_HINTS.get(indicator_type.lower(), {}).get(rule_id.lower(), {})
    hinted = hints.get("signal_type")
    if hinted:
        return str(hinted)

    rule_key = rule_id.lower()
    if "retest" in rule_key:
        return "retest"
    if "breakout" in rule_key or "break" in rule_key:
        return "breakout"
    if "touch" in rule_key:
        return "touch"
    if "trend" in rule_key:
        return "trend"
    return rule_key or "signal"


def _default_direction_hints(signal_type: str) -> List[Dict[str, str]]:
    normalized = (signal_type or "").lower()
    if normalized in {"breakout", "retest", "touch", "trend"}:
        return [
            {
                "id": "long",
                "label": "Long",
                "description": "Setup that supports a long bias.",
            },
            {
                "id": "short",
                "label": "Short",
                "description": "Setup that supports a short bias.",
            },
        ]
    return []


def build_signal_catalog(indicator_type: str) -> List[Dict[str, Any]]:
    rule_meta = describe_indicator_rules(indicator_type) or []

    if not rule_meta:
        logger.warning(
            "signal_catalog_empty | indicator_type=%s",
            indicator_type,
        )
        return []

    catalog: List[Dict[str, Any]] = []
    indicator_key = str(indicator_type or "").lower()
    hints_for_indicator = _RULE_HINTS.get(indicator_key, {})

    for entry in rule_meta:
        rule_id = str(entry.get("id", "")).strip()
        if not rule_id:
            continue
        if rule_id.lower() in _RULE_SUNSET.get(indicator_key, set()):
            continue
        hint = hints_for_indicator.get(rule_id.lower(), {})
        signal_type = hint.get("signal_type") or _guess_signal_type(indicator_key, rule_id)
        directions = hint.get("directions") or _default_direction_hints(signal_type)
        enriched = dict(entry)
        enriched["signal_type"] = signal_type
        if directions:
            enriched["directions"] = directions
        catalog.append(enriched)
    return catalog


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
    return attach_signal_catalog(meta, ctx=ctx)


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


def extract_ctor_params(inst) -> Dict[str, Any]:
    """Extract constructor params from indicator instance.

    IMPORTANT: Filters out DataContext fields (symbol, start, end, interval) which are
    runtime context, not indicator configuration. These should not be persisted.
    """
    # DataContext fields that should NOT be stored in indicator params
    CONTEXT_FIELDS = {"symbol", "start", "end", "interval"}

    sig = inspect.signature(inst.__class__.__init__)
    out: Dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
            continue
        # Skip DataContext fields - they're runtime context, not config
        if name in CONTEXT_FIELDS:
            continue
        if not hasattr(inst, name):
            continue
        if (
            name == "bin_size"
            and hasattr(inst, "_bin_size_locked")
            and not getattr(inst, "_bin_size_locked")
        ):
            continue
        out[name] = getattr(inst, name)
    return out


def sanitize_json(obj):
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
    "attach_signal_catalog",
    "build_indicator_instance",
    "build_signal_catalog",
    "build_meta_from_record",
    "coerce_float",
    "coerce_int",
    "ensure_color",
    "extract_ctor_params",
    "get_indicator_entry",
    "load_indicator_record",
    "normalize_color",
    "normalize_datasource",
    "normalize_exchange",
    "pull_datasource_exchange",
    "purge_breakout_cache",
    "purge_overlay_cache",
    # REMOVED: "refresh_strategy_links" - no longer needed
    "resolve_data_provider",
    "sanitize_json",
    "scrub_runtime_params",
]

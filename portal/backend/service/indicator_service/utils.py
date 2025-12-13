from __future__ import annotations

import inspect
import logging
import math
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from data_providers import DataSource

from .context import IndicatorServiceContext, _context

logger = logging.getLogger(__name__)


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
    fallback_context: Optional[Mapping[str, Any]] = None,
    persist_backfill: bool = False,
    ctx: IndicatorServiceContext = _context,
):
    return ctx.cache_manager.get_entry(
        inst_id,
        fallback_context=fallback_context,
        persist_backfill=persist_backfill,
    )


def refresh_strategy_links(
    inst_id: str, meta: Mapping[str, Any], *, ctx: IndicatorServiceContext = _context
) -> None:
    strategies = ctx.repository.strategies_for_indicator(inst_id)
    if not strategies:
        return
    snapshot = deepcopy(meta)
    for strategy in strategies:
        strategy_id = strategy.get("id")
        if not strategy_id:
            continue
        ctx.repository.upsert_strategy_indicator(
            strategy_id=strategy_id,
            indicator_id=inst_id,
            snapshot=snapshot,
        )


_RUNTIME_PARAM_KEYS = {"datasource", "exchange"}


def purge_breakout_cache(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    ctx.breakout_cache.purge_indicator(inst_id)


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

    logger.debug(
        "attach_signal_catalog | meta.type=%s | meta.name=%s | resolved_type='%s'",
        meta.get("type"),
        meta.get("name"),
        indicator_type
    )

    if not indicator_type:
        logger.warning("⚠ attach_signal_catalog: No indicator type in meta | meta_keys=%s", list(meta.keys()))
        return meta

    catalog = ctx.signal_runner.build_signal_catalog(str(indicator_type))

    logger.info(
        "attach_signal_catalog | indicator_type='%s' | catalog_size=%d | signal_ids=%s",
        indicator_type,
        len(catalog) if catalog else 0,
        [s.get('id') for s in catalog] if catalog else []
    )

    if catalog:
        meta["signal_rules"] = catalog
    return meta


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
    ds = normalize_datasource(datasource, ctx=ctx) or DataSource.ALPACA.value
    ex = normalize_exchange(exchange, ctx=ctx)
    return ctx.resolver.resolve(ds, exchange=ex)


def pull_datasource_exchange(
    params: Dict[str, Any],
    *,
    fallback_meta: Optional[Mapping[str, Any]] = None,
    ctx: IndicatorServiceContext = _context,
) -> Tuple[Optional[str], Optional[str]]:
    defaults = fallback_meta or {}
    datasource = normalize_datasource(params.pop("datasource", defaults.get("datasource")), ctx=ctx)
    exchange = normalize_exchange(params.pop("exchange", defaults.get("exchange")), ctx=ctx)
    if exchange and not datasource:
        datasource = DataSource.CCXT.value
    return datasource, exchange


def extract_ctor_params(inst) -> Dict[str, Any]:
    sig = inspect.signature(inst.__class__.__init__)
    out: Dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
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
    "refresh_strategy_links",
    "resolve_data_provider",
    "sanitize_json",
    "scrub_runtime_params",
]

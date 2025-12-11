# service/indicator_service.py
from __future__ import annotations

import inspect
import logging
import math
import uuid
from collections.abc import Mapping, MutableMapping, Sequence
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from data_providers.base_provider import DataSource
from indicators.config import DataContext
from indicators.vwap import VWAPIndicator
from indicators.pivot_level import PivotLevelIndicator
from indicators.trendline import TrendlineIndicator
from indicators.market_profile import MarketProfileIndicator
from signals.base import BaseSignal
from signals.engine import market_profile_generator  # noqa: F401
from signals.engine import pivot_level_generator  # noqa: F401
from signals.engine.market_profile_generator import build_value_area_payloads
from signals.rules.market_profile import MarketProfileBreakoutConfig
from signals.rules.pivot import PivotBreakoutConfig
from .data_provider_resolver import DataProviderResolver, default_resolver
from .indicator_breakout_cache import IndicatorBreakoutCache, default_breakout_cache
from .indicator_cache import IndicatorCacheEntry, IndicatorCacheManager, default_cache_manager
from .indicator_factory import INDICATOR_MAP as _INDICATOR_MAP
from .indicator_factory import IndicatorFactory
from .indicator_repository import IndicatorRepository, default_repository
from .indicator_signal_runner import IndicatorSignalRunner, default_signal_runner

pivot_level_generator.ensure_registration()

logger = logging.getLogger(__name__)

_repository: IndicatorRepository = default_repository()
_resolver: DataProviderResolver = default_resolver()
_factory: IndicatorFactory = IndicatorFactory(resolver=_resolver)
_cache_manager: IndicatorCacheManager = default_cache_manager(
    _repository, factory=_factory
)
_signal_runner: IndicatorSignalRunner = default_signal_runner()
_breakout_cache: IndicatorBreakoutCache = default_breakout_cache()


def _build_meta_from_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    return _factory.build_meta_from_record(record)


def _build_indicator_instance(meta: Mapping[str, Any]):
    return _factory.build_indicator_instance(meta)


def _load_indicator_record(inst_id: str) -> Dict[str, Any]:
    record = _repository.get(inst_id)
    if not record:
        raise KeyError("Indicator not found")
    return record


def _get_indicator_entry(
    inst_id: str,
    *,
    fallback_context: Optional[Mapping[str, Any]] = None,
    persist_backfill: bool = False,
) -> IndicatorCacheEntry:
    return _cache_manager.get_entry(
        inst_id,
        fallback_context=fallback_context,
        persist_backfill=persist_backfill,
    )


def _refresh_strategy_links(inst_id: str, meta: Mapping[str, Any]) -> None:
    """Update stored strategy indicator snapshots after metadata changes."""

    strategies = _repository.strategies_for_indicator(inst_id)
    if not strategies:
        return
    snapshot = deepcopy(meta)
    for strategy in strategies:
        strategy_id = strategy.get("id")
        if not strategy_id:
            continue
        _repository.upsert_strategy_indicator(
            strategy_id=strategy_id,
            indicator_id=inst_id,
            snapshot=snapshot,
        )


_RUNTIME_PARAM_KEYS = {"datasource", "exchange"}

_DEFAULT_PIVOT_BREAKOUT_CONFIG = PivotBreakoutConfig()
_DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG = MarketProfileBreakoutConfig()


def _purge_breakout_cache(inst_id: str) -> None:
    _breakout_cache.purge_indicator(inst_id)


def _scrub_runtime_params(params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(params, Mapping):
        return {}
    cleaned: Dict[str, Any] = {}
    for key, value in params.items():
        if key in _RUNTIME_PARAM_KEYS:
            continue
        cleaned[key] = value
    return cleaned


def _coerce_int(value: Any, default: int, *, minimum: Optional[int] = None) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and result < minimum:
        return default
    return result


def _coerce_float(value: Any, default: float, *, minimum: Optional[float] = None) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result) or math.isinf(result):
        return default
    if minimum is not None and result < minimum:
        return default
    return result


    if not inst_id:
        return


def _attach_signal_catalog(meta: Dict[str, Any]) -> Dict[str, Any]:
    indicator_type = meta.get("type") or meta.get("name")
    if not indicator_type:
        return meta
    catalog = _signal_runner.build_signal_catalog(str(indicator_type))
    if catalog:
        meta["signal_rules"] = catalog
    return meta


def _normalize_color(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _ensure_color(meta: Dict[str, Any]) -> Dict[str, Any]:
    if "color" not in meta:
        meta["color"] = None
    meta["params"] = _scrub_runtime_params(meta.get("params") or {})
    return _attach_signal_catalog(meta)


def _normalize_datasource(value: Optional[str]) -> Optional[str]:
    return _resolver.normalize_datasource(value)


def _normalize_exchange(value: Optional[str]) -> Optional[str]:
    return _resolver.normalize_exchange(value)


def _resolve_data_provider(
    datasource: Optional[str], *, exchange: Optional[str] = None
):
    """Return a data provider instance honouring local monkeypatches."""

    ds = _normalize_datasource(datasource) or DataSource.ALPACA.value
    ex = _normalize_exchange(exchange)
    return _resolver.resolve(ds, exchange=ex)

def _extract_ctor_params(inst) -> Dict[str, Any]:
    """Reflectively capture constructor params currently set on the instance."""
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
            # Auto-selected bin sizes should not be persisted so they can be
            # recalculated when context (e.g., symbol) changes.
            continue
        out[name] = getattr(inst, name)
    return out

def _sanitize_json(obj):
    """Recursively drop/neutralize NaN/Inf and make timestamps JSON friendly."""
    # numbers
    if isinstance(obj, (int,)) or isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        v = float(obj)
        return v if math.isfinite(v) else None
    # pandas/np timestamps
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    # containers
    if isinstance(obj, dict):
        cleaned = {k: _sanitize_json(v) for k, v in obj.items()}
        # if an overlay item has a price/value that ended up None, drop the item
        if ("price" in cleaned and cleaned["price"] is None) or ("value" in cleaned and cleaned["value"] is None):
            return None
        return {k: v for k, v in cleaned.items() if v is not None}
    if isinstance(obj, (list, tuple)):
        return [v for v in (_sanitize_json(v) for v in obj) if v is not None]
    return obj

def list_types() -> List[str]:
    return list(_INDICATOR_MAP.keys())

def get_type_details(type_id: str) -> Dict[str, Any]:
    Cls = _INDICATOR_MAP.get(type_id)
    if not Cls:
        raise KeyError(f"Unknown indicator type: {type_id}")

    sig = inspect.signature(Cls.__init__)
    required, defaults, field_types = [], {}, {}
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
            continue
        anno = param.annotation
        if anno is inspect._empty:
            tname = "string"
        elif hasattr(anno, "__name__"):
            tname = anno.__name__
        else:
            tname = str(anno)
        field_types[name] = tname
        if param.default is inspect._empty:
            required.append(name)
        else:
            defaults[name] = param.default
    indicator_name = getattr(Cls, "NAME", type_id)

    details = {
        "id": type_id,
        "name": indicator_name,
        "required_params": required,
        "default_params": defaults,
        "field_types": field_types,
    }

    rule_meta = _signal_runner.build_signal_catalog(indicator_name)
    if rule_meta:
        details["signal_rules"] = rule_meta

    return details


def list_instances_meta() -> List[Dict[str, Any]]:
    records = _repository.load()
    if not records:
        return []
    return [_build_meta_from_record(record) for record in records]


def get_instance_meta(inst_id: str) -> Dict[str, Any]:
    record = _load_indicator_record(inst_id)
    return _build_meta_from_record(record)


def list_indicator_strategies(inst_id: str) -> List[Dict[str, Any]]:
    """Return persisted strategies referencing the indicator."""

    return _repository.strategies_for_indicator(inst_id)

def delete_instance(inst_id: str) -> None:
    _load_indicator_record(inst_id)  # ensure it exists
    _cache_manager.evict(inst_id)
    _purge_breakout_cache(inst_id)
    _repository.delete(inst_id)


def duplicate_instance(inst_id: str, name: Optional[str] = None) -> Dict[str, Any]:
    """Clone a stored indicator instance."""

    base_record = _load_indicator_record(inst_id)
    clone_id = str(uuid.uuid4())
    clone_record = deepcopy(base_record)
    clone_record["id"] = clone_id
    clone_record["name"] = name or f"{base_record.get('name') or base_record.get('type')} Copy"
    _repository.upsert(clone_record)
    refreshed = _repository.get(clone_id)
    persisted = _build_meta_from_record(refreshed) if refreshed else _build_meta_from_record(clone_record)
    inst = _build_indicator_instance(persisted)
    _cache_manager.cache_indicator(
        clone_id, persisted, inst, (refreshed or {}).get("updated_at")
    )
    return persisted


def set_instance_enabled(inst_id: str, enabled: bool) -> Dict[str, Any]:
    """Toggle the enabled flag for a stored indicator."""

    record = _load_indicator_record(inst_id)
    updated = deepcopy(record)
    updated["enabled"] = bool(enabled)
    _repository.upsert(updated)
    refreshed = _repository.get(inst_id)
    persisted = _build_meta_from_record(refreshed) if refreshed else _build_meta_from_record(updated)
    _cache_manager.evict(inst_id)
    return persisted


def bulk_set_enabled(inst_ids: Sequence[str], enabled: bool) -> List[Dict[str, Any]]:
    """Set the enabled flag for a collection of indicators."""

    results: List[Dict[str, Any]] = []
    for inst_id in inst_ids:
        try:
            results.append(set_instance_enabled(inst_id, enabled))
        except KeyError:
            continue
    return results


def bulk_delete_instances(inst_ids: Sequence[str]) -> int:
    """Delete multiple indicator instances."""

    removed = 0
    for inst_id in inst_ids:
        try:
            delete_instance(inst_id)
            removed += 1
        except KeyError:
            continue
    return removed

def create_instance(
    type_str: str,
    name: Optional[str],
    params: Dict[str, Any],
    color: Optional[str] = None,
) -> Dict[str, Any]:
    Cls = _INDICATOR_MAP.get(type_str)
    if not Cls:
        raise ValueError(f"Unknown indicator type: {type_str}")

    params = dict(params)
    # Extract context → DataContext
    ctx_keys = ("symbol", "start", "end", "interval")
    try:
        ctx_kwargs = {k: params.pop(k) for k in ctx_keys}
    except KeyError as e:
        raise ValueError(f"Missing required context param: {e.args[0]}")
    ctx = DataContext(**ctx_kwargs)
    ctx.validate()

    datasource = _normalize_datasource(params.pop("datasource", None))
    exchange = _normalize_exchange(params.pop("exchange", None))
    if exchange and not datasource:
        datasource = DataSource.CCXT.value

    provider = _resolve_data_provider(datasource, exchange=exchange)

    try:
        logger.info("event=indicator_create type=%s params=%s", type_str, params)
        inst = Cls.from_context(provider=provider, ctx=ctx, **params)
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate indicator: {e}")

    if isinstance(inst, MarketProfileIndicator):
        setattr(inst, "symbol", ctx.symbol)

    captured = _extract_ctor_params(inst)
    runtime_params = dict(captured)
    if datasource:
        runtime_params["datasource"] = datasource
    if exchange:
        runtime_params["exchange"] = exchange
    inst_id = str(uuid.uuid4())
    meta = {
        "id": inst_id,
        "type": type_str,
        "params": _scrub_runtime_params(runtime_params),
        "enabled": True,
        "name": name or type_str.replace("_", " ").title(),
    }
    meta["datasource"] = datasource or DataSource.ALPACA.value
    if exchange:
        meta["exchange"] = exchange
    meta["color"] = _normalize_color(color)
    _repository.upsert(meta)
    persisted = _repository.get(inst_id)
    persisted_meta = _build_meta_from_record(persisted) if persisted else _factory.ensure_color(meta)
    _cache_manager.cache_indicator(
        inst_id, persisted_meta, inst, (persisted or {}).get("updated_at")
    )
    _refresh_strategy_links(inst_id, persisted_meta)
    return persisted_meta

def update_instance(
    inst_id: str,
    type_str: str,
    params: Dict[str, Any],
    name: Optional[str],
    *,
    color: Optional[str] = None,
    color_provided: bool = False,
) -> Dict[str, Any]:
    record = _load_indicator_record(inst_id)
    meta = _build_meta_from_record(record)
    if type_str != meta["type"]:
        raise ValueError("Cannot change indicator type; create a new instance instead")

    params = dict(params)
    try:
        cached_entry = _cache_manager.get_entry(inst_id)
        cached_inst = cached_entry.instance
    except KeyError:
        cached_entry = None
        cached_inst = None
    if (
        type_str == MarketProfileIndicator.NAME
        and isinstance(cached_inst, MarketProfileIndicator)
        and "bin_size" in params
        and not getattr(cached_inst, "_bin_size_locked", False)
    ):
        params.pop("bin_size", None)

    # Validate params against ctor
    Cls = _INDICATOR_MAP.get(type_str)
    sig = inspect.signature(Cls.__init__)
    for pname, p in sig.parameters.items():
        if pname in ("self", "df"):
            continue
        if pname not in params:
            if p.default is inspect._empty:
                raise ValueError(f"Missing required parameter: {pname}")
            params[pname] = p.default

    # Rebuild instance with new context/params
    try:
        ctx_kwargs = {k: params.pop(k) for k in ("symbol", "start", "end", "interval")}
    except KeyError as e:
        raise ValueError(f"Missing required context param: {e.args[0]}")
    ctx = DataContext(**ctx_kwargs)
    ctx.validate()

    datasource = _normalize_datasource(params.pop("datasource", meta.get("datasource")))
    exchange = _normalize_exchange(params.pop("exchange", meta.get("exchange")))
    if exchange and not datasource:
        datasource = DataSource.CCXT.value

    provider = _resolve_data_provider(datasource, exchange=exchange)
    try:
        new_inst = Cls.from_context(provider=provider, ctx=ctx, **params)
    except Exception as e:
        raise RuntimeError(f"Failed to re-instantiate indicator: {e}")

    if isinstance(new_inst, MarketProfileIndicator):
        setattr(new_inst, "symbol", ctx.symbol)

    captured = _extract_ctor_params(new_inst)
    runtime_params = dict(captured)
    if datasource:
        runtime_params["datasource"] = datasource
    if exchange:
        runtime_params["exchange"] = exchange
    _purge_breakout_cache(inst_id)
    meta_payload = dict(meta)
    meta_payload["params"] = _scrub_runtime_params(runtime_params)
    if name:
        meta_payload["name"] = name
    if color_provided:
        meta_payload["color"] = _normalize_color(color)
    meta_payload["datasource"] = datasource or DataSource.ALPACA.value
    if exchange:
        meta_payload["exchange"] = exchange
    elif "exchange" in meta_payload:
        meta_payload.pop("exchange", None)
    meta_payload = _ensure_color(meta_payload)
    _repository.upsert(meta_payload)
    refreshed = _repository.get(inst_id)
    persisted_meta = _build_meta_from_record(refreshed) if refreshed else meta_payload
    _cache_manager.cache_indicator(
        inst_id, persisted_meta, new_inst, (refreshed or {}).get("updated_at")
    )
    _refresh_strategy_links(inst_id, persisted_meta)
    return persisted_meta

def overlays_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    *,
    overlay_options: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compute Lightweight-Charts-ready overlays for an existing indicator UUID,
    using the requested chart window (start/end/interval). Does not require
    indicator params (they come from the stored instance).
    """
    entry = _get_indicator_entry(
        inst_id,
        fallback_context={
            "symbol": symbol,
            "start": start,
            "end": end,
            "interval": interval,
        },
        persist_backfill=True,
    )
    inst = entry.instance
    base_params = entry.meta.get("params", {})
    sym = symbol or base_params.get("symbol")
    if not sym:
        raise ValueError("Stored indicator has no symbol and none was provided")

    meta = entry.meta
    stored_params = meta.get("params", {})
    stored_datasource = _normalize_datasource(meta.get("datasource") or stored_params.get("datasource"))
    stored_exchange = _normalize_exchange(meta.get("exchange") or stored_params.get("exchange"))

    req_datasource = _normalize_datasource(datasource)
    req_exchange = _normalize_exchange(exchange)

    effective_datasource = req_datasource or stored_datasource
    effective_exchange = req_exchange or stored_exchange

    if effective_exchange and not effective_datasource:
        effective_datasource = DataSource.CCXT.value

    provider = _resolve_data_provider(
        effective_datasource,
        exchange=effective_exchange,
    )
    logger.info(
        "event=indicator_overlay_prepare indicator=%s symbol=%s interval=%s start=%s end=%s",
        inst_id,
        sym,
        interval,
        start,
        end,
    )
    ctx = DataContext(symbol=sym, start=start, end=end, interval=interval)
    df = provider.get_ohlcv(ctx)
    if df is None or df.empty:
        raise LookupError("No candles available for given window")

    overlay_indicator = inst
    options = dict(overlay_options or {})
    if isinstance(inst, MarketProfileIndicator) and hasattr(inst, "to_lightweight"):
        overlay_indicator = _breakout_cache.build_market_profile_overlay_indicator(
            inst,
            df,
            interval=interval,
            symbol=sym,
        )
        logger.debug(
            "event=indicator_overlay_runtime_clone indicator=%s symbol=%s interval=%s",
            inst_id,
            sym,
            interval,
        )

        if "extend_value_area_to_chart_end" in options:
            setattr(
                overlay_indicator,
                "extend_value_area_to_chart_end",
                bool(options["extend_value_area_to_chart_end"]),
            )

    # Expect indicator to expose one of: to_lightweight(df) | to_overlays(df)
    if hasattr(overlay_indicator, "to_lightweight"):
        payload = overlay_indicator.to_lightweight(df)
    elif hasattr(overlay_indicator, "to_overlays"):
        payload = overlay_indicator.to_overlays(df)
    else:
        raise RuntimeError("Indicator does not implement overlay serialization")

    raw_payload = payload
    payload = _sanitize_json(payload)
    if not payload:
        raise LookupError("No overlays computed for given window")

    LAYERS = ("price_lines", "markers", "boxes", "segments", "polylines")
    has_visuals = any(
        isinstance(payload.get(k), (list, tuple)) and len(payload.get(k)) > 0
        for k in LAYERS
    )
    if not has_visuals:
        raise LookupError("No overlays computed for given window")

    if isinstance(payload, dict):
        counts = {k: len(payload.get(k) or []) for k in LAYERS if isinstance(payload.get(k), (list, tuple))}
    else:
        counts = {}

    logger.info(
        "event=indicator_overlay_result indicator=%s price_lines=%s markers=%s boxes=%s segments=%s polylines=%s",
        inst_id,
        counts.get("price_lines", 0),
        counts.get("markers", 0),
        counts.get("boxes", 0),
        counts.get("segments", 0),
        counts.get("polylines", 0),
    )

    boxes = []
    if isinstance(raw_payload, dict):
        boxes = raw_payload.get("boxes") or []
    if isinstance(boxes, list):
        for idx, box in enumerate(boxes):
            if not isinstance(box, dict):
                continue
            logger.debug(
                "event=indicator_overlay_box indicator=%s index=%d x1=%s x2=%s y1=%s y2=%s",
                inst_id,
                idx,
                box.get("x1"),
                box.get("x2"),
                box.get("y1"),
                box.get("y2"),
            )

    return payload


def generate_signals_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute registered signal rules for an indicator instance."""

    entry = _get_indicator_entry(
        inst_id,
        fallback_context={
            "symbol": symbol,
            "start": start,
            "end": end,
            "interval": interval,
        },
        persist_backfill=True,
    )
    inst = entry.instance
    base_params = entry.meta.get("params", {})
    sym = symbol or base_params.get("symbol")
    if not sym:
        raise ValueError("Stored indicator has no symbol and none was provided")

    meta = entry.meta
    stored_datasource = _normalize_datasource(meta.get("datasource") or base_params.get("datasource"))
    stored_exchange = _normalize_exchange(meta.get("exchange") or base_params.get("exchange"))

    req_datasource = _normalize_datasource(datasource)
    req_exchange = _normalize_exchange(exchange)
    effective_datasource = req_datasource or stored_datasource
    effective_exchange = req_exchange or stored_exchange
    if effective_exchange and not effective_datasource:
        effective_datasource = DataSource.CCXT.value

    provider = _resolve_data_provider(
        effective_datasource,
        exchange=effective_exchange,
    )
    logger.info(
        "event=indicator_signal_prepare indicator=%s symbol=%s interval=%s start=%s end=%s",
        inst_id,
        sym,
        interval,
        start,
        end,
    )
    ctx = DataContext(symbol=sym, start=start, end=end, interval=interval)
    df = provider.get_ohlcv(ctx)
    if df is None or df.empty:
        raise LookupError("No candles available for given window")

    rule_config: Dict[str, Any] = dict(config or {})
    stored_params = meta.get("params", {}) if isinstance(meta, Mapping) else {}

    if "pivot_breakout_confirmation_bars" not in rule_config:
        pivot_bars = stored_params.get("pivot_breakout_confirmation_bars")
        if pivot_bars is None and hasattr(inst, "pivot_breakout_confirmation_bars"):
            pivot_bars = getattr(inst, "pivot_breakout_confirmation_bars")
        rule_config["pivot_breakout_confirmation_bars"] = _coerce_int(
            pivot_bars,
            _DEFAULT_PIVOT_BREAKOUT_CONFIG.confirmation_bars,
            minimum=1,
        )

    if "market_profile_breakout_confirmation_bars" not in rule_config:
        mp_bars = stored_params.get("market_profile_breakout_confirmation_bars")
        if mp_bars is None and hasattr(inst, "market_profile_breakout_confirmation_bars"):
            mp_bars = getattr(inst, "market_profile_breakout_confirmation_bars")
        rule_config["market_profile_breakout_confirmation_bars"] = _coerce_int(
            mp_bars,
            _DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG.confirmation_bars,
            minimum=1,
        )

    rule_config.setdefault("symbol", sym)

    if isinstance(inst, MarketProfileIndicator) and "rule_payloads" not in rule_config:
        rule_config.setdefault(
            "market_profile_use_merged_value_areas",
            getattr(inst, "use_merged_value_areas", True),
        )
        rule_config.setdefault(
            "market_profile_merge_threshold",
            getattr(inst, "merge_threshold", 0.6),
        )
        payloads = build_value_area_payloads(
            inst,
            df,
            interval=interval,
            use_merged=rule_config.get("market_profile_use_merged_value_areas"),
            merge_threshold=rule_config.get("market_profile_merge_threshold"),
            min_merge_sessions=rule_config.get("market_profile_merge_min_sessions"),
        )
        rule_config["rule_payloads"] = payloads

    indicator_name = getattr(inst, "NAME", inst.__class__.__name__)
    cache_spec = _breakout_cache.spec_for(indicator_name)

    requested_rule_ids: Optional[Set[str]] = None
    cache_key: Optional[Tuple[Any, ...]] = None
    using_cached_breakouts = False
    drop_breakout_from_response = False

    enabled_rules_config = rule_config.get("enabled_rules")
    if enabled_rules_config is not None:
        normalised_rules: List[str] = []
        seen: Set[str] = set()
        for rule_id in enabled_rules_config:
            if rule_id is None:
                continue
            rule_str = str(rule_id).strip()
            if not rule_str:
                continue
            norm = rule_str.lower()
            if norm not in seen:
                normalised_rules.append(norm)
                seen.add(norm)
        if normalised_rules:
            rule_config["enabled_rules"] = normalised_rules
            requested_rule_ids = set(normalised_rules)
        else:
            rule_config.pop("enabled_rules")

    if cache_spec is not None:
        signature = cache_spec.config_signature_builder(rule_config)
        cache_key = _breakout_cache.build_cache_key(
            inst_id,
            indicator_name,
            sym,
            interval,
            start,
            end,
            signature,
        )
        if (
            requested_rule_ids
            and cache_spec.retest_rule_id in requested_rule_ids
            and cache_spec.breakout_rule_id not in requested_rule_ids
        ):
            cached_breakouts = _breakout_cache.get_cached_breakouts(cache_key)
            if cached_breakouts:
                using_cached_breakouts = True
                rule_config[cache_spec.cache_context_key] = cached_breakouts
                rule_config[cache_spec.ready_flag_key] = True
                if cache_spec.initialised_flag_key:
                    rule_config[cache_spec.initialised_flag_key] = True
                for extra_key, extra_value in cache_spec.context_defaults.items():
                    rule_config.setdefault(extra_key, extra_value)
                logger.debug(
                    "event=indicator_breakout_cache_hit indicator=%s rule=%s entries=%d",
                    inst_id,
                    cache_spec.breakout_rule_id,
                    len(cached_breakouts),
                )
            else:
                drop_breakout_from_response = True
                current_rules = list(rule_config.get("enabled_rules", []))
                if cache_spec.breakout_rule_id not in current_rules:
                    current_rules.append(cache_spec.breakout_rule_id)
                if current_rules:
                    rule_config["enabled_rules"] = current_rules
                logger.debug(
                    "event=indicator_breakout_cache_miss indicator=%s rule=%s",
                    inst_id,
                    cache_spec.breakout_rule_id,
                )

    noisy_keys = {"rule_payloads"}
    if cache_spec is not None:
        noisy_keys.add(cache_spec.cache_context_key)

    log_config: Dict[str, Any] = {}
    for key, value in rule_config.items():
        if key in noisy_keys:
            try:
                length = len(value)  # type: ignore[arg-type]
            except Exception:
                length = "?"
            log_config[key] = f"<{key}:len={length}>"
        else:
            log_config[key] = value

    logger.info(
        "event=indicator_signal_execute indicator=%s name=%s symbol=%s interval=%s start=%s end=%s config=%s",
        inst_id,
        indicator_name,
        sym,
        interval,
        start,
        end,
        log_config,
    )

    signals_all = _signal_runner.run_rules(inst, df, **rule_config)

    if cache_spec is not None and cache_key is not None and not using_cached_breakouts:
        enabled_for_run = rule_config.get("enabled_rules")
        ran_breakout = enabled_for_run is None or cache_spec.breakout_rule_id in enabled_for_run
        if ran_breakout:
            breakout_payloads = [
                _breakout_cache.flatten_breakout_signal(sig)
                for sig in signals_all
                if sig.type == "breakout"
            ]
            _breakout_cache.store_breakout_cache(cache_key, breakout_payloads)
            logger.debug(
                "event=indicator_breakout_cache_store indicator=%s rule=%s entries=%d",
                inst_id,
                cache_spec.breakout_rule_id,
                len(breakout_payloads),
            )

    filtered_signals: List[BaseSignal] = list(signals_all)
    if cache_spec is not None:
        if requested_rule_ids:
            allowed_types: Set[str] = set()
            for rule_id in requested_rule_ids:
                allowed_types.update(cache_spec.rule_signal_types.get(rule_id, set()))
            if allowed_types:
                filtered_signals = [
                    sig for sig in filtered_signals if sig.type in allowed_types
                ]
        if drop_breakout_from_response:
            filtered_signals = [
                sig for sig in filtered_signals if sig.type != "breakout"
            ]

    if len(filtered_signals) != len(signals_all):
        logger.debug(
            "event=indicator_signal_filtered indicator=%s total=%d returned=%d",
            inst_id,
            len(signals_all),
            len(filtered_signals),
        )

    overlays = _signal_runner.build_overlays(inst, filtered_signals, df, **rule_config)

    logger.info(
        "event=indicator_signal_complete indicator=%s signals=%d overlays=%d",
        inst_id,
        len(filtered_signals),
        len(overlays),
    )

    sanitized_overlays = _sanitize_json(overlays) or []
    return {
        "signals": [sig.to_dict() for sig in filtered_signals],
        "overlays": sanitized_overlays,
    }

# service/indicator_service.py
from __future__ import annotations

import inspect
import logging
import math
import uuid
from collections.abc import Mapping, MutableMapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from data_providers.alpaca_provider import AlpacaProvider
from indicators.config import DataContext
from indicators.vwap import VWAPIndicator
from indicators.pivot_level import PivotLevelIndicator
from indicators.trendline import TrendlineIndicator
from indicators.market_profile import MarketProfileIndicator
from signals.engine.signal_generator import (
    build_signal_overlays,
    describe_indicator_rules,
    run_indicator_rules,
)
from signals.base import BaseSignal
from signals.engine import market_profile_generator  # noqa: F401
from signals.engine import pivot_level_generator  # noqa: F401
from signals.engine.market_profile_generator import build_value_area_payloads
from signals.rules.market_profile import (
    MarketProfileBreakoutConfig,
    _BREAKOUT_CACHE_INITIALISED,
    _BREAKOUT_CACHE_KEY,
    _BREAKOUT_READY_FLAG,
)
from signals.rules.pivot import PivotBreakoutConfig, _PIVOT_BREAKOUT_READY_FLAG

pivot_level_generator.ensure_registration()

logger = logging.getLogger(__name__)

# Registered indicator types
_INDICATOR_MAP = {
    "vwap":           VWAPIndicator,
    "pivot_level":    PivotLevelIndicator,
    "trendline":      TrendlineIndicator,
    "market_profile": MarketProfileIndicator,
}

# Ensure default signal rules are registered for built-in indicators
# In-memory registry: id -> {"meta": <pydantic-like dict>, "instance": <object>}
_REGISTRY: Dict[str, Dict[str, Any]] = {}


@dataclass(frozen=True)
class BreakoutCacheSpec:
    breakout_rule_id: str
    retest_rule_id: str
    cache_context_key: str
    ready_flag_key: str
    initialised_flag_key: Optional[str]
    config_signature_builder: Callable[[Mapping[str, Any]], Tuple[Any, ...]]
    rule_signal_types: Dict[str, Set[str]] = field(default_factory=dict)
    context_defaults: Mapping[str, Any] = field(default_factory=dict)


_PIVOT_BREAKOUT_CACHE_KEY = "pivot_breakouts"


_DEFAULT_PIVOT_BREAKOUT_CONFIG = PivotBreakoutConfig()
_DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG = MarketProfileBreakoutConfig()


_BREAKOUT_CACHE_SPECS: Dict[str, BreakoutCacheSpec] = {}


_BREAKOUT_SIGNAL_CACHE: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}


def _purge_breakout_cache(inst_id: str) -> None:
    if not inst_id:
        return
    stale_keys = [key for key in _BREAKOUT_SIGNAL_CACHE if key and key[0] == inst_id]
    for cache_key in stale_keys:
        _BREAKOUT_SIGNAL_CACHE.pop(cache_key, None)


def _hashable_signature(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, Mapping):
        return tuple(sorted((k, _hashable_signature(v)) for k, v in value.items()))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_hashable_signature(v) for v in value)
    return str(value)


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


def _pivot_breakout_signature(config: Mapping[str, Any]) -> Tuple[Any, ...]:
    cfg = config.get("pivot_breakout_config")
    if isinstance(cfg, PivotBreakoutConfig):
        confirmation = cfg.confirmation_bars
        early_window = cfg.early_confirmation_window
        early_pct = cfg.early_confirmation_distance_pct
    else:
        confirmation = _coerce_int(
            config.get("pivot_breakout_confirmation_bars"),
            _DEFAULT_PIVOT_BREAKOUT_CONFIG.confirmation_bars,
            minimum=1,
        )
        early_window = _coerce_int(
            config.get("pivot_breakout_early_window"),
            _DEFAULT_PIVOT_BREAKOUT_CONFIG.early_confirmation_window,
            minimum=1,
        )
        early_pct = _coerce_float(
            config.get("pivot_breakout_early_distance_pct"),
            _DEFAULT_PIVOT_BREAKOUT_CONFIG.early_confirmation_distance_pct,
            minimum=0.0,
        )
    mode = str(config.get("mode", "backtest")).lower()
    return (mode, confirmation, early_window, float(early_pct))


def _market_profile_breakout_signature(config: Mapping[str, Any]) -> Tuple[Any, ...]:
    cfg = config.get("market_profile_breakout_config")
    if isinstance(cfg, MarketProfileBreakoutConfig):
        confirmation = cfg.confirmation_bars
        early_window = cfg.early_confirmation_window
        early_pct = cfg.early_confirmation_distance_pct
    else:
        confirmation = _coerce_int(
            config.get("market_profile_breakout_confirmation_bars"),
            _DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG.confirmation_bars,
            minimum=1,
        )
        early_window = _coerce_int(
            config.get("market_profile_breakout_early_window"),
            _DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG.early_confirmation_window,
            minimum=1,
        )
        early_pct = _coerce_float(
            config.get("market_profile_breakout_early_distance_pct"),
            _DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG.early_confirmation_distance_pct,
            minimum=0.0,
        )
    mode = str(config.get("mode", "backtest")).lower()
    payload_sig = _hashable_signature(config.get("rule_payloads"))
    return (mode, confirmation, early_window, float(early_pct), payload_sig)


def _clone_breakouts(breakouts: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return deepcopy(list(breakouts)) if breakouts else []


def _get_cached_breakouts(cache_key: Tuple[Any, ...]) -> Optional[List[Dict[str, Any]]]:
    cached = _BREAKOUT_SIGNAL_CACHE.get(cache_key)
    if cached is None:
        return None
    return deepcopy(cached)


def _store_breakout_cache(
    cache_key: Tuple[Any, ...], breakouts: Sequence[Mapping[str, Any]]
) -> None:
    _BREAKOUT_SIGNAL_CACHE[cache_key] = _clone_breakouts(breakouts)


def _flatten_breakout_signal(signal: BaseSignal) -> Dict[str, Any]:
    metadata = dict(signal.metadata or {})
    metadata.setdefault("type", signal.type)
    metadata.setdefault("symbol", signal.symbol)
    metadata.setdefault("time", signal.time)
    metadata.setdefault("confidence", signal.confidence)
    return metadata


def _build_market_profile_overlay_indicator(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    interval: Optional[str] = None,
    symbol: Optional[str] = None,
) -> MarketProfileIndicator:
    """Create a fresh MarketProfileIndicator aligned with the overlay request window."""

    runtime = MarketProfileIndicator(
        df=df.copy(),
        bin_size=getattr(indicator, "bin_size", 0.1),
        mode=getattr(indicator, "mode", "tpo"),
        interval=interval or getattr(indicator, "interval", "30m"),
        extend_value_area_to_chart_end=getattr(
            indicator,
            "extend_value_area_to_chart_end",
            True,
        ),
        use_merged_value_areas=getattr(indicator, "use_merged_value_areas", True),
        merge_threshold=getattr(indicator, "merge_threshold", 0.6),
        min_merge_sessions=getattr(
            indicator,
            "min_merge_sessions",
            getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
        ),
    )

    if symbol is None:
        symbol = getattr(indicator, "symbol", None)
    if symbol is not None:
        setattr(runtime, "symbol", symbol)

    return runtime


_BREAKOUT_CACHE_SPECS.update(
    {
        PivotLevelIndicator.NAME: BreakoutCacheSpec(
            breakout_rule_id="pivot_breakout",
            retest_rule_id="pivot_retest",
            cache_context_key=_PIVOT_BREAKOUT_CACHE_KEY,
            ready_flag_key=_PIVOT_BREAKOUT_READY_FLAG,
            initialised_flag_key=None,
            config_signature_builder=_pivot_breakout_signature,
            rule_signal_types={
                "pivot_breakout": {"breakout"},
                "pivot_retest": {"retest"},
            },
        ),
        MarketProfileIndicator.NAME: BreakoutCacheSpec(
            breakout_rule_id="market_profile_breakout",
            retest_rule_id="market_profile_retest",
            cache_context_key=_BREAKOUT_CACHE_KEY,
            ready_flag_key=_BREAKOUT_READY_FLAG,
            initialised_flag_key=_BREAKOUT_CACHE_INITIALISED,
            config_signature_builder=_market_profile_breakout_signature,
            rule_signal_types={
                "market_profile_breakout": {"breakout"},
                "market_profile_retest": {"retest"},
            },
            context_defaults={_BREAKOUT_CACHE_INITIALISED: True},
        ),
    }
)


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
    return meta

def _extract_ctor_params(inst) -> Dict[str, Any]:
    """Reflectively capture constructor params currently set on the instance."""
    sig = inspect.signature(inst.__class__.__init__)
    out: Dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
            continue
        if hasattr(inst, name):
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
    details = {
        "id": type_id,
        "name": getattr(Cls, "NAME", type_id),
        "required_params": required,
        "default_params": defaults,
        "field_types": field_types,
    }

    rule_meta = describe_indicator_rules(getattr(Cls, "NAME", type_id))
    if rule_meta:
        details["signal_rules"] = rule_meta

    return details


def list_instances_meta() -> List[Dict[str, Any]]:
    return [_ensure_color(entry["meta"]) for entry in _REGISTRY.values()]


def get_instance_meta(inst_id: str) -> Dict[str, Any]:
    entry = _REGISTRY.get(inst_id)
    if not entry:
        raise KeyError("Indicator not found")
    return _ensure_color(entry["meta"])

def delete_instance(inst_id: str) -> None:
    if inst_id not in _REGISTRY:
        raise KeyError("Indicator not found")
    del _REGISTRY[inst_id]
    _purge_breakout_cache(inst_id)

def create_instance(
    type_str: str,
    name: Optional[str],
    params: Dict[str, Any],
    color: Optional[str] = None,
) -> Dict[str, Any]:
    Cls = _INDICATOR_MAP.get(type_str)
    if not Cls:
        raise ValueError(f"Unknown indicator type: {type_str}")

    # Extract context → DataContext
    ctx_keys = ("symbol", "start", "end", "interval")
    try:
        ctx_kwargs = {k: params.pop(k) for k in ctx_keys}
    except KeyError as e:
        raise ValueError(f"Missing required context param: {e.args[0]}")
    ctx = DataContext(**ctx_kwargs)
    ctx.validate()

    provider = AlpacaProvider()

    try:
        logger.info("event=indicator_create type=%s params=%s", type_str, params)
        inst = Cls.from_context(provider=provider, ctx=ctx, **params)
    except Exception as e:
        raise RuntimeError(f"Failed to instantiate indicator: {e}")

    captured = _extract_ctor_params(inst)
    inst_id = str(uuid.uuid4())
    meta = {
        "id": inst_id,
        "type": type_str,
        "params": captured,
        "enabled": True,
        "name": name or type_str.replace("_", " ").title(),
    }
    meta["color"] = _normalize_color(color)
    _ensure_color(meta)
    _REGISTRY[inst_id] = {"meta": meta, "instance": inst}
    return meta

def update_instance(
    inst_id: str,
    type_str: str,
    params: Dict[str, Any],
    name: Optional[str],
    *,
    color: Optional[str] = None,
    color_provided: bool = False,
) -> Dict[str, Any]:
    entry = _REGISTRY.get(inst_id)
    if not entry:
        raise KeyError("Indicator not found")
    if type_str != entry["meta"]["type"]:
        raise ValueError("Cannot change indicator type; create a new instance instead")

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

    provider = AlpacaProvider()
    try:
        new_inst = Cls.from_context(provider=provider, ctx=ctx, **params)
    except Exception as e:
        raise RuntimeError(f"Failed to re-instantiate indicator: {e}")

    captured = _extract_ctor_params(new_inst)
    entry["instance"] = new_inst
    _purge_breakout_cache(inst_id)
    meta = _ensure_color(entry["meta"])
    meta["params"] = captured
    if name:
        meta["name"] = name
    if color_provided:
        meta["color"] = _normalize_color(color)
    return meta

def overlays_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compute Lightweight-Charts-ready overlays for an existing indicator UUID,
    using the requested chart window (start/end/interval). Does not require
    indicator params (they come from the stored instance).
    """
    entry = _REGISTRY.get(inst_id)
    if not entry:
        raise KeyError("Indicator not found")

    inst = entry["instance"]
    # prefer the stored symbol unless explicitly overridden
    base_params = entry["meta"]["params"]
    sym = symbol or base_params.get("symbol")
    if not sym:
        raise ValueError("Stored indicator has no symbol and none was provided")

    # fetch windowed OHLCV for overlay computation
    provider = AlpacaProvider()
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
    if isinstance(inst, MarketProfileIndicator) and hasattr(inst, "to_lightweight"):
        overlay_indicator = _build_market_profile_overlay_indicator(
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

    # Expect indicator to expose one of: to_lightweight(df) | to_overlays(df)
    if hasattr(overlay_indicator, "to_lightweight"):
        payload = overlay_indicator.to_lightweight(
            df,
            use_merged=getattr(inst, "use_merged_value_areas", True),
            merge_threshold=getattr(inst, "merge_threshold", 0.6),
            min_merge=getattr(
                inst,
                "min_merge_sessions",
                getattr(MarketProfileIndicator, "DEFAULT_MIN_MERGE_SESSIONS", 3),
            ),
            extend_boxes_to_chart_end=getattr(
                inst,
                "extend_value_area_to_chart_end",
                True,
            ),
        )
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
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute registered signal rules for an indicator instance."""

    entry = _REGISTRY.get(inst_id)
    if not entry:
        raise KeyError("Indicator not found")

    inst = entry["instance"]
    base_params = entry["meta"].get("params", {})
    sym = symbol or base_params.get("symbol")
    if not sym:
        raise ValueError("Stored indicator has no symbol and none was provided")

    provider = AlpacaProvider()
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
    rule_config.setdefault("pivot_breakout_confirmation_bars", 3)
    rule_config.setdefault("market_profile_breakout_confirmation_bars", 3)
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
    cache_spec = _BREAKOUT_CACHE_SPECS.get(indicator_name)

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
        cache_key = (
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
            cached_breakouts = _get_cached_breakouts(cache_key)
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

    signals_all = run_indicator_rules(inst, df, **rule_config)

    if cache_spec is not None and cache_key is not None and not using_cached_breakouts:
        enabled_for_run = rule_config.get("enabled_rules")
        ran_breakout = enabled_for_run is None or cache_spec.breakout_rule_id in enabled_for_run
        if ran_breakout:
            breakout_payloads = [
                _flatten_breakout_signal(sig)
                for sig in signals_all
                if sig.type == "breakout"
            ]
            _store_breakout_cache(cache_key, breakout_payloads)
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

    overlays = build_signal_overlays(inst, filtered_signals, df, **rule_config)

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

# service/indicator_service.py
from __future__ import annotations

import uuid
import inspect
import logging
from typing import Any, Dict, List, Optional, Tuple
import math
import numpy as np
import pandas as pd

from data_providers.alpaca_provider import AlpacaProvider
from indicators.config import DataContext
from indicators.vwap import VWAPIndicator
from indicators.pivot_level import PivotLevelIndicator
from indicators.trendline import TrendlineIndicator
from indicators.market_profile import MarketProfileIndicator

logger = logging.getLogger(__name__)

# Registered indicator types
_INDICATOR_MAP = {
    "vwap":           VWAPIndicator,
    "pivot_level":    PivotLevelIndicator,
    "trendline":      TrendlineIndicator,
    "market_profile": MarketProfileIndicator,
}

# In-memory registry: id -> {"meta": <pydantic-like dict>, "instance": <object>}
_REGISTRY: Dict[str, Dict[str, Any]] = {}

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
    return {
        "id": type_id,
        "name": getattr(Cls, "NAME", type_id),
        "required_params": required,
        "default_params": defaults,
        "field_types": field_types,
    }

def list_instances_meta() -> List[Dict[str, Any]]:
    return [entry["meta"] for entry in _REGISTRY.values()]

def get_instance_meta(inst_id: str) -> Dict[str, Any]:
    entry = _REGISTRY.get(inst_id)
    if not entry:
        raise KeyError("Indicator not found")
    return entry["meta"]

def delete_instance(inst_id: str) -> None:
    if inst_id not in _REGISTRY:
        raise KeyError("Indicator not found")
    del _REGISTRY[inst_id]

def create_instance(type_str: str, name: Optional[str], params: Dict[str, Any]) -> Dict[str, Any]:
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
        logger.info("Instantiating %s with params=%s", type_str, params)
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
    _REGISTRY[inst_id] = {"meta": meta, "instance": inst}
    return meta

def update_instance(inst_id: str, type_str: str, params: Dict[str, Any], name: Optional[str]) -> Dict[str, Any]:
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
    entry["meta"]["params"] = captured
    if name:
        entry["meta"]["name"] = name
    return entry["meta"]

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
    ctx = DataContext(symbol=sym, start=start, end=end, interval=interval)
    df = provider.get_ohlcv(ctx)
    if df is None or df.empty:
        raise LookupError("No candles available for given window")

    # Expect indicator to expose one of: to_lightweight(df) | to_overlays(df)
    if hasattr(inst, "to_lightweight"):
        payload = inst.to_lightweight(df)
    elif hasattr(inst, "to_overlays"):
        payload = inst.to_overlays(df)
    else:
        raise RuntimeError("Indicator does not implement overlay serialization")

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

    return payload

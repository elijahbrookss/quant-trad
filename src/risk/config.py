"""Canonical risk/sizing configuration helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Mapping, Optional


DEFAULT_RISK_CONFIG: Dict[str, Any] = {
    "base_risk_per_trade": None,
    "global_risk_multiplier": 1.0,
    "instrument_multipliers": {},
}


def _coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalise_risk_config(
    config: Optional[Mapping[str, Any]],
    *,
    base: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a fully populated risk config."""

    result = deepcopy(base or DEFAULT_RISK_CONFIG)
    payload = dict(config or {})

    result["base_risk_per_trade"] = _coerce_float(
        payload.get("base_risk_per_trade"),
        result.get("base_risk_per_trade"),
    )
    result["global_risk_multiplier"] = (
        _coerce_float(payload.get("global_risk_multiplier"), 1.0) or 1.0
    )

    raw_overrides = payload.get("instrument_multipliers")
    overrides: Dict[str, float] = {}
    if isinstance(raw_overrides, Mapping):
        for raw_symbol, raw_value in raw_overrides.items():
            symbol = str(raw_symbol or "").strip().upper()
            numeric = _coerce_float(raw_value)
            if symbol and numeric is not None:
                overrides[symbol] = float(numeric)
    result["instrument_multipliers"] = overrides

    instrument_multiplier = _coerce_float(payload.get("instrument_risk_multiplier"))
    if instrument_multiplier is not None:
        result["instrument_risk_multiplier"] = float(instrument_multiplier)
    else:
        result.pop("instrument_risk_multiplier", None)

    return result


__all__ = ["DEFAULT_RISK_CONFIG", "normalise_risk_config"]

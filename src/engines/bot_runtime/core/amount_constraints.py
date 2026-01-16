"""Resolve and enforce instrument amount/quantity constraints."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

_STEP_TOLERANCE = 1e-9


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric


def _step_from_precision(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and float(value).is_integer():
        integer = int(float(value))
        if integer >= 0:
            return float(10 ** (-integer))
    numeric = _coerce_float(value)
    if numeric in (None, 0):
        return None
    return float(numeric) if 0 < float(numeric) < 1 else None


def _precision_from_step(step: Optional[float]) -> Optional[int]:
    if step in (None, 0):
        return None
    normalized = f"{step:.12f}".rstrip("0").rstrip(".")
    if "." not in normalized:
        return 0
    decimals = len(normalized.split(".")[1])
    return decimals if decimals >= 0 else None


def _steps_compatible(lhs: float, rhs: float) -> bool:
    if lhs == rhs:
        return True
    return abs(lhs - rhs) <= _STEP_TOLERANCE


def _floor_to_step(qty: float, step: Optional[float]) -> float:
    if step in (None, 0):
        return qty
    return math.floor((qty + 1e-12) / step) * step


def _ceil_to_step(qty: float, step: Optional[float]) -> float:
    if step in (None, 0):
        return qty
    return math.ceil((qty - 1e-12) / step) * step


@dataclass(frozen=True)
class AmountConstraints:
    """Resolved quantity constraints and their origins."""

    min_qty: Optional[float]
    max_qty: Optional[float]
    qty_step: Optional[float]
    min_notional: Optional[float]
    precision: Optional[int]
    step_source: Optional[str]
    min_qty_source: Optional[str]
    max_qty_source: Optional[str]
    precision_source: Optional[str]


@dataclass(frozen=True)
class QtyNormalization:
    """Normalization outcome for a requested qty."""

    qty_raw: float
    qty_clamped: float
    qty_rounded: float
    qty_final: Optional[float]
    qty_step: Optional[float]
    min_qty: Optional[float]
    max_qty: Optional[float]
    min_qty_aligned: Optional[float]
    precision: Optional[int]
    max_clamped: bool
    rejected_reason: Optional[str]

    @property
    def ok(self) -> bool:
        return self.qty_final is not None and self.rejected_reason is None

    def to_log_dict(self) -> Dict[str, Any]:
        return {
            "qty_raw": self.qty_raw,
            "qty_clamped": self.qty_clamped,
            "qty_rounded": self.qty_rounded,
            "qty_final": self.qty_final,
            "qty_step": self.qty_step,
            "min_qty": self.min_qty,
            "max_qty": self.max_qty,
            "min_qty_aligned": self.min_qty_aligned,
            "precision": self.precision,
            "max_clamped": self.max_clamped,
            "rejected_reason": self.rejected_reason,
        }


def resolve_amount_constraints(instrument: Mapping[str, Any]) -> AmountConstraints:
    """Resolve amount/quantity constraints with explicit source precedence.

    Priority order for step size:
    1) metadata.info.base_increment
    2) instrument.qty_step / instrument.step_size
    3) metadata.qty_step (CCXT)
    4) metadata.info.qty_step (raw exchange)
    5) metadata.precision.amount (derived)

    If multiple step sources are present and disagree, this raises ValueError.
    """
    min_qty = _coerce_float(
        instrument.get("min_qty")
        or instrument.get("min_order_size")
        or instrument.get("min_quantity")
    )
    max_qty = _coerce_float(
        instrument.get("max_qty")
        or instrument.get("max_order_size")
        or instrument.get("max_quantity")
    )
    min_notional = _coerce_float(instrument.get("min_notional") or instrument.get("min_cost"))

    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    info = metadata.get("info") if isinstance(metadata.get("info"), Mapping) else {}
    limits = metadata.get("limits") if isinstance(metadata.get("limits"), Mapping) else {}
    amount_limits = limits.get("amount") if isinstance(limits.get("amount"), Mapping) else {}
    cost_limits = limits.get("cost") if isinstance(limits.get("cost"), Mapping) else {}
    precision = metadata.get("precision") if isinstance(metadata.get("precision"), Mapping) else {}

    if min_qty in (None, 0):
        min_qty = _coerce_float(amount_limits.get("min"))
    if max_qty in (None, 0):
        max_qty = _coerce_float(amount_limits.get("max"))
    if min_notional in (None, 0):
        min_notional = _coerce_float(cost_limits.get("min"))

    base_increment = _coerce_float(info.get("base_increment"))
    instrument_step = _coerce_float(instrument.get("qty_step") or instrument.get("step_size"))
    metadata_step = _coerce_float(metadata.get("qty_step"))
    info_step = _coerce_float(info.get("qty_step"))
    precision_step = _step_from_precision(precision.get("amount"))

    step_sources = {
        "base_increment": base_increment,
        "instrument": instrument_step,
        "metadata": metadata_step,
        "metadata_info": info_step,
        "precision": precision_step,
    }

    provided_steps = {key: value for key, value in step_sources.items() if value not in (None, 0)}
    if len(provided_steps) > 1:
        values = list(provided_steps.values())
        first = values[0]
        for value in values[1:]:
            if not _steps_compatible(first, value):
                raise ValueError(
                    "Conflicting qty_step sources detected: "
                    + ", ".join(f"{key}={value}" for key, value in provided_steps.items())
                )

    step_priority = [
        ("base_increment", base_increment),
        ("instrument", instrument_step),
        ("metadata", metadata_step),
        ("metadata_info", info_step),
        ("precision", precision_step),
    ]
    qty_step = None
    step_source = None
    for key, value in step_priority:
        if value not in (None, 0):
            qty_step = float(value)
            step_source = key
            break

    if qty_step is not None and qty_step <= 0:
        qty_step = None
        step_source = None

    precision_value = precision.get("amount")
    precision_source = "metadata_precision" if precision_value is not None else None
    resolved_precision = None
    if isinstance(precision_value, (int, float)) and float(precision_value).is_integer():
        resolved_precision = int(float(precision_value))
    if resolved_precision is None:
        resolved_precision = _precision_from_step(qty_step)
        precision_source = "qty_step" if resolved_precision is not None else precision_source

    min_qty_source = "instrument" if _coerce_float(
        instrument.get("min_qty")
        or instrument.get("min_order_size")
        or instrument.get("min_quantity")
    ) not in (None, 0) else ("metadata_limits" if _coerce_float(amount_limits.get("min")) not in (None, 0) else None)

    max_qty_source = "instrument" if _coerce_float(
        instrument.get("max_qty")
        or instrument.get("max_order_size")
        or instrument.get("max_quantity")
    ) not in (None, 0) else ("metadata_limits" if _coerce_float(amount_limits.get("max")) not in (None, 0) else None)

    return AmountConstraints(
        min_qty=min_qty,
        max_qty=max_qty,
        qty_step=qty_step,
        min_notional=min_notional,
        precision=resolved_precision,
        step_source=step_source,
        min_qty_source=min_qty_source,
        max_qty_source=max_qty_source,
        precision_source=precision_source,
    )


def normalize_qty(
    instrument: Mapping[str, Any],
    qty_raw: float,
) -> QtyNormalization:
    constraints = resolve_amount_constraints(instrument)
    return normalize_qty_with_constraints(constraints, qty_raw)


def normalize_qty_with_constraints(
    constraints: AmountConstraints,
    qty_raw: float,
) -> QtyNormalization:
    qty_clamped = float(qty_raw)
    max_clamped = False
    if constraints.max_qty not in (None, 0) and qty_clamped > float(constraints.max_qty):
        qty_clamped = float(constraints.max_qty)
        max_clamped = True

    qty_rounded = _floor_to_step(qty_clamped, constraints.qty_step)

    min_qty_aligned = constraints.min_qty
    if constraints.min_qty not in (None, 0) and constraints.qty_step not in (None, 0):
        min_qty_aligned = _ceil_to_step(float(constraints.min_qty), constraints.qty_step)

    rejected_reason = None
    qty_final: Optional[float] = qty_rounded

    if qty_final <= 0:
        rejected_reason = "QTY_ROUNDS_TO_ZERO"
        qty_final = None
    elif min_qty_aligned not in (None, 0) and qty_final + 1e-12 < float(min_qty_aligned):
        rejected_reason = "MIN_QTY_NOT_MET"
        qty_final = None

    return QtyNormalization(
        qty_raw=float(qty_raw),
        qty_clamped=qty_clamped,
        qty_rounded=qty_rounded,
        qty_final=qty_final,
        qty_step=constraints.qty_step,
        min_qty=constraints.min_qty,
        max_qty=constraints.max_qty,
        min_qty_aligned=min_qty_aligned,
        precision=constraints.precision,
        max_clamped=max_clamped,
        rejected_reason=rejected_reason,
    )


__all__ = [
    "AmountConstraints",
    "QtyNormalization",
    "resolve_amount_constraints",
    "normalize_qty",
    "normalize_qty_with_constraints",
]

"""Canonical runtime execution plan compiled from normalized ATM templates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence


SUPPORTED_ENTRY_ANCHORS = {"signal_price"}
SUPPORTED_STOP_ACTIONS = {"move_to_breakeven", "move_to_r"}
SUPPORTED_STOP_TRIGGERS = {"r_multiple", "target_hit"}


@dataclass(frozen=True)
class LimitMakerPlan:
    """Post-only entry configuration for immediate signal-bar maker orders."""

    anchor_price: str
    offset_type: str
    offset_value: float
    validity_window: int
    fallback: str


@dataclass(frozen=True)
class EntryExecutionPlan:
    """Canonical entry order semantics."""

    order_type: str
    limit_maker: LimitMakerPlan


@dataclass(frozen=True)
class FixedHorizonExitPlan:
    """Close remaining legs after a fixed number of completed position bars."""

    enabled: bool
    bars: Optional[int]
    price: str = "close"
    order_type: str = "market"


@dataclass(frozen=True)
class BreakevenPlan:
    """Direct stop-to-breakeven policy for simple strategies."""

    enabled: bool
    ticks: float = 0.0
    r_multiple: Optional[float] = None
    target_index: Optional[int] = None
    target_id: Optional[str] = None


@dataclass(frozen=True)
class TrailingStopPlan:
    """Trailing stop activation and distance policy."""

    enabled: bool
    activation_type: str = "r_multiple"
    ticks: float = 0.0
    atr_multiplier: Optional[float] = None
    r_multiple: Optional[float] = None
    target_index: Optional[int] = None
    target_id: Optional[str] = None


@dataclass(frozen=True)
class StopAdjustmentPlanRule:
    """Canonical one-time stop movement rule from ATM config."""

    rule_id: Optional[str]
    trigger_type: str
    trigger_value: object
    trigger_ticks: Optional[float]
    action_type: str
    action_value: Optional[float]


@dataclass
class RuntimeStopAdjustment:
    """Resolved mutable stop adjustment rule attached to an open position."""

    trigger_type: str
    trigger_target_id: Optional[str]
    trigger_ticks: Optional[float]
    action_type: str
    action_r: Optional[float]
    fired: bool = False


@dataclass(frozen=True)
class RuntimeExecutionPlan:
    """Canonical execution policy consumed by the risk engine."""

    entry: EntryExecutionPlan
    fixed_horizon: FixedHorizonExitPlan
    breakeven: BreakevenPlan
    trailing: TrailingStopPlan
    stop_adjustments: tuple[StopAdjustmentPlanRule, ...]


def compile_runtime_execution_plan(template: Mapping[str, Any]) -> RuntimeExecutionPlan:
    """Compile a normalized ATM template into runtime-only execution semantics."""

    return RuntimeExecutionPlan(
        entry=_compile_entry_plan(template),
        fixed_horizon=_compile_fixed_horizon_plan(template),
        breakeven=_compile_breakeven_plan(template),
        trailing=_compile_trailing_plan(template),
        stop_adjustments=tuple(_compile_stop_adjustment_rules(template.get("stop_adjustments") or [])),
    )


def _compile_entry_plan(template: Mapping[str, Any]) -> EntryExecutionPlan:
    order_type = str(template.get("execution_mode") or "market").strip().lower()
    if order_type != "limit_maker":
        order_type = "market"
    source = template.get("limit_maker")
    limit_maker = dict(source or {}) if isinstance(source, Mapping) else {}
    anchor = str(limit_maker.get("anchor_price") or "signal_price").strip().lower()
    if anchor not in SUPPORTED_ENTRY_ANCHORS:
        raise ValueError(
            "unsupported entry anchor "
            f"anchor_price={anchor!r}; supported={sorted(SUPPORTED_ENTRY_ANCHORS)}. "
            "Next-bar entry requires an explicit pending signal-entry lifecycle."
        )
    fallback = str(limit_maker.get("fallback") or "cancel").strip().lower()
    if fallback not in {"cancel", "convert_to_market"}:
        raise ValueError(
            "unsupported limit-maker fallback "
            f"fallback={fallback!r}; supported=['cancel', 'convert_to_market']"
        )
    return EntryExecutionPlan(
        order_type=order_type,
        limit_maker=LimitMakerPlan(
            anchor_price=anchor,
            offset_type=str(limit_maker.get("offset_type") or "ticks").strip().lower(),
            offset_value=float(_coerce_float(limit_maker.get("offset_value"), 0.0) or 0.0),
            validity_window=max(int(_coerce_int(limit_maker.get("validity_window"), 1) or 1), 1),
            fallback=fallback,
        ),
    )


def _compile_fixed_horizon_plan(template: Mapping[str, Any]) -> FixedHorizonExitPlan:
    exit_plan = template.get("exit_plan") if isinstance(template.get("exit_plan"), Mapping) else {}
    fixed = exit_plan.get("fixed_horizon") if isinstance(exit_plan, Mapping) else {}
    if not isinstance(fixed, Mapping):
        fixed = {}
    bars = _coerce_int(fixed.get("bars"))
    enabled = bool(fixed.get("enabled")) and bars is not None and bars > 0
    return FixedHorizonExitPlan(
        enabled=enabled,
        bars=int(bars) if enabled else None,
        price="close",
        order_type="market",
    )


def _compile_breakeven_plan(template: Mapping[str, Any]) -> BreakevenPlan:
    source = template.get("breakeven")
    config = dict(source or {}) if isinstance(source, Mapping) else {}
    return BreakevenPlan(
        enabled=bool(config.get("enabled")),
        ticks=max(float(_coerce_float(config.get("ticks"), 0.0) or 0.0), 0.0),
        r_multiple=_positive_float(config.get("r_multiple")),
        target_index=_non_negative_int(config.get("target_index")),
        target_id=_optional_text(config.get("target_id")),
    )


def _compile_trailing_plan(template: Mapping[str, Any]) -> TrailingStopPlan:
    source = template.get("trailing")
    config = dict(source or {}) if isinstance(source, Mapping) else {}
    activation_type = str(config.get("activation_type") or "r_multiple").strip().lower()
    if activation_type not in {"r_multiple", "target_hit"}:
        raise ValueError(
            "unsupported trailing activation_type "
            f"activation_type={activation_type!r}; supported=['r_multiple', 'target_hit']"
        )
    return TrailingStopPlan(
        enabled=bool(config.get("enabled")),
        activation_type=activation_type,
        ticks=max(float(_coerce_float(config.get("ticks"), 0.0) or 0.0), 0.0),
        atr_multiplier=_positive_float(config.get("atr_multiplier")),
        r_multiple=_positive_float(config.get("r_multiple")),
        target_index=_non_negative_int(config.get("target_index")),
        target_id=_optional_text(config.get("target_id")),
    )


def _compile_stop_adjustment_rules(
    source: Sequence[object],
) -> list[StopAdjustmentPlanRule]:
    rules: list[StopAdjustmentPlanRule] = []
    for entry in source:
        if not isinstance(entry, Mapping):
            continue
        trigger_type = str(entry.get("trigger_type") or "").strip().lower().replace("_reached", "")
        if trigger_type not in SUPPORTED_STOP_TRIGGERS:
            raise ValueError(
                "unsupported stop adjustment trigger_type "
                f"trigger_type={trigger_type!r}; supported={sorted(SUPPORTED_STOP_TRIGGERS)}"
            )
        action_type = str(entry.get("action_type") or "").strip().lower()
        if action_type not in SUPPORTED_STOP_ACTIONS:
            raise ValueError(
                "unsupported stop adjustment action_type "
                f"action_type={action_type!r}; supported={sorted(SUPPORTED_STOP_ACTIONS)}. "
                "Use top-level trailing config for trailing stops."
            )
        trigger_value = entry.get("trigger_value")
        if trigger_value is None:
            trigger_value = entry.get("trigger_target_id")
        trigger_ticks = _positive_float(entry.get("trigger_ticks"))
        if trigger_type == "r_multiple":
            trigger_value = _positive_float(trigger_value)
            if trigger_value is None and trigger_ticks is None:
                continue
        elif trigger_value in (None, ""):
            continue

        action_value = _positive_float(entry.get("action_value"))
        if action_type == "move_to_r" and action_value is None:
            continue
        rules.append(
            StopAdjustmentPlanRule(
                rule_id=_optional_text(entry.get("id")),
                trigger_type=trigger_type,
                trigger_value=trigger_value,
                trigger_ticks=trigger_ticks,
                action_type=action_type,
                action_value=action_value,
            )
        )
    return rules


def _coerce_int(value: object, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_float(value: object, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _positive_float(value: object) -> Optional[float]:
    numeric = _coerce_float(value)
    if numeric is None or numeric <= 0:
        return None
    return float(numeric)


def _non_negative_int(value: object) -> Optional[int]:
    numeric = _coerce_int(value)
    if numeric is None or numeric < 0:
        return None
    return int(numeric)


def _optional_text(value: object) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "BreakevenPlan",
    "EntryExecutionPlan",
    "FixedHorizonExitPlan",
    "LimitMakerPlan",
    "RuntimeExecutionPlan",
    "RuntimeStopAdjustment",
    "StopAdjustmentPlanRule",
    "TrailingStopPlan",
    "compile_runtime_execution_plan",
]

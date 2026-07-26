"""Canonical runtime execution plan compiled from normalized ATM templates."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from atm.schema import (
    ATM_BREAKEVEN_FIELDS,
    ATM_EXIT_PLAN_FIELDS,
    ATM_FIXED_HORIZON_FIELDS,
    ATM_INITIAL_STOP_FIELDS,
    ATM_LIMIT_MAKER_FIELDS,
    ATM_SCHEMA_VERSION,
    ATM_STOP_ADJUSTMENT_FIELDS,
    ATM_TAKE_PROFIT_FIELDS,
    ATM_TEMPLATE_FIELDS,
    ATM_TRAILING_FIELDS,
)


SUPPORTED_ENTRY_ANCHORS = {"signal_price"}
SUPPORTED_ENTRY_ORDER_TYPES = {"limit_maker", "market"}
SUPPORTED_LIMIT_OFFSET_TYPES = {"atr_pct", "r_fraction", "ticks"}
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
class InitialStopPlan:
    """Canonical ATR-based initial stop semantics."""

    mode: str
    atr_period: int
    atr_multiplier: float


@dataclass(frozen=True)
class TakeProfitPlan:
    """Canonical take-profit leg intent."""

    target_id: str
    label: str
    ticks: Optional[int]
    r_multiple: Optional[float]
    price: Optional[float]
    size_fraction: float


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

    rule_id: str
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
    initial_stop: InitialStopPlan
    take_profits: tuple[TakeProfitPlan, ...]
    fixed_horizon: FixedHorizonExitPlan
    breakeven: BreakevenPlan
    trailing: TrailingStopPlan
    stop_adjustments: tuple[StopAdjustmentPlanRule, ...]


def compile_runtime_execution_plan(template: Mapping[str, Any]) -> RuntimeExecutionPlan:
    """Compile normalized ATM configuration and reject ambiguous semantics."""

    if not isinstance(template, Mapping):
        raise ValueError("ATM template must be a mapping")

    _reject_unknown_fields(template, ATM_TEMPLATE_FIELDS, path="ATM template")
    schema_version = _required_int(template.get("schema_version"), path="schema_version")
    if schema_version != ATM_SCHEMA_VERSION:
        raise ValueError(
            f"schema_version={schema_version} is unsupported; expected {ATM_SCHEMA_VERSION}"
        )
    take_profits = tuple(_compile_take_profit_plans(template.get("take_profit_orders")))
    target_ids = {target.target_id for target in take_profits}
    return RuntimeExecutionPlan(
        entry=_compile_entry_plan(template),
        initial_stop=_compile_initial_stop_plan(template),
        take_profits=take_profits,
        fixed_horizon=_compile_fixed_horizon_plan(template),
        breakeven=_compile_breakeven_plan(template),
        trailing=_compile_trailing_plan(
            template,
            target_ids=target_ids,
            target_count=len(take_profits),
        ),
        stop_adjustments=tuple(
            _compile_stop_adjustment_rules(
                template.get("stop_adjustments"),
                target_ids=target_ids,
            )
        ),
    )


def _compile_entry_plan(template: Mapping[str, Any]) -> EntryExecutionPlan:
    order_type = _required_choice(
        template.get("execution_mode"),
        path="execution_mode",
        supported=SUPPORTED_ENTRY_ORDER_TYPES,
    )
    limit_maker = _required_mapping(template.get("limit_maker"), path="limit_maker")
    _reject_unknown_fields(limit_maker, ATM_LIMIT_MAKER_FIELDS, path="limit_maker")
    anchor = str(limit_maker.get("anchor_price") or "").strip().lower()
    if anchor not in SUPPORTED_ENTRY_ANCHORS:
        raise ValueError(
            "unsupported entry anchor "
            f"anchor_price={anchor!r}; supported={sorted(SUPPORTED_ENTRY_ANCHORS)}. "
            "Next-bar entry requires an explicit pending signal-entry lifecycle."
        )
    fallback = _required_choice(
        limit_maker.get("fallback"),
        path="limit_maker.fallback",
        supported={"cancel", "convert_to_market"},
    )
    offset_type = _required_choice(
        limit_maker.get("offset_type"),
        path="limit_maker.offset_type",
        supported=SUPPORTED_LIMIT_OFFSET_TYPES,
    )
    offset_value = _required_float(limit_maker.get("offset_value"), path="limit_maker.offset_value")
    if offset_value < 0:
        raise ValueError("limit_maker.offset_value must be >= 0")
    validity_window = _required_int(
        limit_maker.get("validity_window"),
        path="limit_maker.validity_window",
    )
    if validity_window <= 0:
        raise ValueError("limit_maker.validity_window must be > 0")
    return EntryExecutionPlan(
        order_type=order_type,
        limit_maker=LimitMakerPlan(
            anchor_price=anchor,
            offset_type=offset_type,
            offset_value=offset_value,
            validity_window=validity_window,
            fallback=fallback,
        ),
    )


def _compile_initial_stop_plan(template: Mapping[str, Any]) -> InitialStopPlan:
    source = _required_mapping(template.get("initial_stop"), path="initial_stop")
    _reject_unknown_fields(source, ATM_INITIAL_STOP_FIELDS, path="initial_stop")
    mode = _required_choice(source.get("mode"), path="initial_stop.mode", supported={"atr"})
    atr_period = _required_int(source.get("atr_period"), path="initial_stop.atr_period")
    if atr_period <= 0:
        raise ValueError("initial_stop.atr_period must be > 0")
    atr_multiplier = _required_positive_float(
        source.get("atr_multiplier"),
        path="initial_stop.atr_multiplier",
    )
    return InitialStopPlan(
        mode=mode,
        atr_period=atr_period,
        atr_multiplier=atr_multiplier,
    )


def _compile_take_profit_plans(source: object) -> list[TakeProfitPlan]:
    entries = _required_sequence(source, path="take_profit_orders")
    if not entries:
        raise ValueError("take_profit_orders must contain at least one target")

    targets: list[TakeProfitPlan] = []
    seen_ids: set[str] = set()
    fraction_total = 0.0
    for index, raw_entry in enumerate(entries):
        path = f"take_profit_orders[{index}]"
        entry = _required_mapping(raw_entry, path=path)
        _reject_unknown_fields(entry, ATM_TAKE_PROFIT_FIELDS, path=path)
        target_id = _required_text(entry.get("id"), path=f"{path}.id")
        if target_id in seen_ids:
            raise ValueError(f"{path}.id duplicates target id {target_id!r}")
        seen_ids.add(target_id)

        ticks = _optional_positive_int(entry.get("ticks"), path=f"{path}.ticks")
        r_multiple = _optional_positive_float(entry.get("r_multiple"), path=f"{path}.r_multiple")
        price = _optional_positive_float(entry.get("price"), path=f"{path}.price")
        configured_prices = sum(value is not None for value in (ticks, r_multiple, price))
        if configured_prices != 1:
            raise ValueError(
                f"{path} must define exactly one positive target: ticks, r_multiple, or price"
            )

        size_fraction = _required_positive_float(
            entry.get("size_fraction"),
            path=f"{path}.size_fraction",
        )
        if size_fraction > 1:
            raise ValueError(f"{path}.size_fraction must be <= 1")
        fraction_total += size_fraction
        targets.append(
            TakeProfitPlan(
                target_id=target_id,
                label=_required_text(
                    entry.get("label") or f"Target {index + 1}",
                    path=f"{path}.label",
                ),
                ticks=ticks,
                r_multiple=r_multiple,
                price=price,
                size_fraction=size_fraction,
            )
        )

    if not math.isclose(fraction_total, 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(
            "take_profit_orders size_fraction values must sum to 1.0; "
            f"got {fraction_total!r}"
        )
    return targets


def _compile_fixed_horizon_plan(template: Mapping[str, Any]) -> FixedHorizonExitPlan:
    exit_plan = _required_mapping(template.get("exit_plan"), path="exit_plan")
    _reject_unknown_fields(exit_plan, ATM_EXIT_PLAN_FIELDS, path="exit_plan")
    fixed = _required_mapping(exit_plan.get("fixed_horizon"), path="exit_plan.fixed_horizon")
    _reject_unknown_fields(
        fixed, ATM_FIXED_HORIZON_FIELDS, path="exit_plan.fixed_horizon"
    )
    enabled = _required_bool(fixed.get("enabled"), path="exit_plan.fixed_horizon.enabled")
    bars = _optional_positive_int(fixed.get("bars"), path="exit_plan.fixed_horizon.bars")
    price = _required_choice(
        fixed.get("price"),
        path="exit_plan.fixed_horizon.price",
        supported={"close"},
    )
    order_type = _required_choice(
        fixed.get("order_type"),
        path="exit_plan.fixed_horizon.order_type",
        supported={"market"},
    )
    if enabled and bars is None:
        raise ValueError("exit_plan.fixed_horizon.bars is required when enabled")
    if not enabled and bars is not None:
        raise ValueError("exit_plan.fixed_horizon.bars must be null when disabled")
    return FixedHorizonExitPlan(
        enabled=enabled,
        bars=bars,
        price=price,
        order_type=order_type,
    )


def _compile_breakeven_plan(template: Mapping[str, Any]) -> BreakevenPlan:
    config = _required_mapping(template.get("breakeven"), path="breakeven")
    _reject_unknown_fields(config, ATM_BREAKEVEN_FIELDS, path="breakeven")
    enabled = _required_bool(config.get("enabled"), path="breakeven.enabled")
    activation_type = _required_choice(
        config.get("activation_type"),
        path="breakeven.activation_type",
        supported={"r_multiple"},
    )
    if activation_type != "r_multiple":
        raise AssertionError("validated breakeven activation unexpectedly changed")
    ticks = _required_non_negative_float(config.get("ticks"), path="breakeven.ticks")
    r_multiple = _optional_positive_float(config.get("r_multiple"), path="breakeven.r_multiple")
    if enabled and (ticks > 0) == (r_multiple is not None):
        raise ValueError(
            "enabled breakeven must define exactly one trigger: positive ticks or r_multiple"
        )
    return BreakevenPlan(
        enabled=enabled,
        ticks=ticks,
        r_multiple=r_multiple,
    )


def _compile_trailing_plan(
    template: Mapping[str, Any],
    *,
    target_ids: set[str],
    target_count: int,
) -> TrailingStopPlan:
    config = _required_mapping(template.get("trailing"), path="trailing")
    _reject_unknown_fields(config, ATM_TRAILING_FIELDS, path="trailing")
    enabled = _required_bool(config.get("enabled"), path="trailing.enabled")
    activation_type = _required_choice(
        config.get("activation_type"),
        path="trailing.activation_type",
        supported={"r_multiple", "target_hit"},
    )
    ticks = _required_non_negative_float(config.get("ticks"), path="trailing.ticks")
    atr_multiplier = _optional_positive_float(
        config.get("atr_multiplier"),
        path="trailing.atr_multiplier",
    )
    r_multiple = _optional_positive_float(config.get("r_multiple"), path="trailing.r_multiple")
    target_index = _optional_non_negative_int(config.get("target_index"), path="trailing.target_index")
    target_id = _optional_text(config.get("target_id"))

    configured_distances = int(atr_multiplier is not None) + int(ticks > 0)
    if configured_distances > 1:
        raise ValueError(
            "trailing must define at most one distance: atr_multiplier or positive ticks"
        )
    if activation_type == "r_multiple":
        if target_index is not None or target_id is not None:
            raise ValueError(
                "trailing target_index/target_id require activation_type='target_hit'"
            )
        if r_multiple is None:
            raise ValueError("r_multiple trailing requires trailing.r_multiple")
    else:
        if r_multiple is not None:
            raise ValueError(
                "trailing.r_multiple is invalid when activation_type='target_hit'"
            )
        if (target_index is not None) == (target_id is not None):
            raise ValueError(
                "target_hit trailing must define exactly one of target_index or target_id"
            )
        if target_index is not None and target_index >= target_count:
            raise ValueError(
                f"trailing.target_index={target_index} does not reference a configured target"
            )
        if target_id is not None and target_id not in target_ids:
            raise ValueError(
                f"trailing.target_id={target_id!r} does not reference a configured target"
            )
    if enabled and configured_distances != 1:
        raise ValueError(
            "enabled trailing must define exactly one distance: "
            "atr_multiplier or positive ticks"
        )

    return TrailingStopPlan(
        enabled=enabled,
        activation_type=activation_type,
        ticks=ticks,
        atr_multiplier=atr_multiplier,
        r_multiple=r_multiple,
        target_index=target_index,
        target_id=target_id,
    )


def _compile_stop_adjustment_rules(
    source: object,
    *,
    target_ids: set[str],
) -> list[StopAdjustmentPlanRule]:
    entries = _required_sequence(source, path="stop_adjustments")
    rules: list[StopAdjustmentPlanRule] = []
    seen_rule_ids: set[str] = set()
    for index, raw_entry in enumerate(entries):
        path = f"stop_adjustments[{index}]"
        entry = _required_mapping(raw_entry, path=path)
        _reject_unknown_fields(entry, ATM_STOP_ADJUSTMENT_FIELDS, path=path)
        rule_id = _required_text(entry.get("id"), path=f"{path}.id")
        if rule_id in seen_rule_ids:
            raise ValueError(f"{path}.id duplicates stop-adjustment id {rule_id!r}")
        seen_rule_ids.add(rule_id)
        trigger_type = _required_choice(
            entry.get("trigger_type"),
            path=f"{path}.trigger_type",
            supported=SUPPORTED_STOP_TRIGGERS,
        )
        action_type = _required_choice(
            entry.get("action_type"),
            path=f"{path}.action_type",
            supported=SUPPORTED_STOP_ACTIONS,
        )
        trigger_value: object = entry.get("trigger_value")
        trigger_ticks = _optional_positive_float(
            entry.get("trigger_ticks"),
            path=f"{path}.trigger_ticks",
        )
        if trigger_type == "r_multiple":
            numeric_trigger = _optional_positive_float(
                trigger_value,
                path=f"{path}.trigger_value",
            )
            if (numeric_trigger is not None) == (trigger_ticks is not None):
                raise ValueError(
                    f"{path} must define exactly one r_multiple trigger: "
                    "trigger_value or trigger_ticks"
                )
            trigger_value = numeric_trigger
        else:
            target_id = _required_text(trigger_value, path=f"{path}.trigger_value")
            if trigger_ticks is not None:
                raise ValueError(f"{path}.trigger_ticks is invalid for a target_hit trigger")
            if target_id not in target_ids:
                raise ValueError(
                    f"{path}.trigger_value={target_id!r} does not reference a configured target"
                )
            trigger_value = target_id

        action_value = _optional_positive_float(
            entry.get("action_value"),
            path=f"{path}.action_value",
        )
        if action_type == "move_to_r" and action_value is None:
            raise ValueError(f"{path}.action_value is required for move_to_r")
        if action_type == "move_to_breakeven" and action_value is not None:
            raise ValueError(f"{path}.action_value is invalid for move_to_breakeven")
        rules.append(
            StopAdjustmentPlanRule(
                rule_id=rule_id,
                trigger_type=trigger_type,
                trigger_value=trigger_value,
                trigger_ticks=trigger_ticks,
                action_type=action_type,
                action_value=action_value,
            )
        )
    return rules


def _required_mapping(value: object, *, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} must be a mapping")
    return value


def _reject_unknown_fields(
    payload: Mapping[str, Any], allowed: frozenset[str], *, path: str
) -> None:
    unknown = sorted(set(payload) - set(allowed))
    if unknown:
        raise ValueError(f"{path} contains unsupported fields: {unknown!r}")


def _required_sequence(value: object, *, path: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{path} must be a sequence")
    return value


def _required_choice(value: object, *, path: str, supported: set[str]) -> str:
    text = str(value or "").strip().lower()
    if text not in supported:
        raise ValueError(f"{path}={text!r} is unsupported; supported={sorted(supported)}")
    return text


def _required_bool(value: object, *, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be a boolean")
    return value


def _required_float(value: object, *, path: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{path} must be a finite number")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path} must be a finite number") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"{path} must be a finite number")
    return numeric


def _required_non_negative_float(value: object, *, path: str) -> float:
    numeric = _required_float(value, path=path)
    if numeric < 0:
        raise ValueError(f"{path} must be >= 0")
    return numeric


def _required_positive_float(value: object, *, path: str) -> float:
    numeric = _required_float(value, path=path)
    if numeric <= 0:
        raise ValueError(f"{path} must be > 0")
    return numeric


def _optional_positive_float(value: object, *, path: str) -> Optional[float]:
    if value is None:
        return None
    return _required_positive_float(value, path=path)


def _required_int(value: object, *, path: str) -> int:
    numeric = _required_float(value, path=path)
    if not numeric.is_integer():
        raise ValueError(f"{path} must be an integer")
    return int(numeric)


def _optional_positive_int(value: object, *, path: str) -> Optional[int]:
    if value is None:
        return None
    numeric = _required_int(value, path=path)
    if numeric <= 0:
        raise ValueError(f"{path} must be > 0")
    return numeric


def _optional_non_negative_int(value: object, *, path: str) -> Optional[int]:
    if value is None:
        return None
    numeric = _required_int(value, path=path)
    if numeric < 0:
        raise ValueError(f"{path} must be >= 0")
    return numeric


def _required_text(value: object, *, path: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{path} must be non-empty text")
    return text


def _optional_text(value: object) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "BreakevenPlan",
    "EntryExecutionPlan",
    "FixedHorizonExitPlan",
    "InitialStopPlan",
    "LimitMakerPlan",
    "RuntimeExecutionPlan",
    "RuntimeStopAdjustment",
    "StopAdjustmentPlanRule",
    "TakeProfitPlan",
    "TrailingStopPlan",
    "compile_runtime_execution_plan",
]

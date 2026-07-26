"""ATM template normalization and processing utilities."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional, Sequence

from .schema import (
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
    DEFAULT_ATM_TEMPLATE,
)


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


def _required_int(value: object, *, path: str) -> int:
    numeric = _required_float(value, path=path)
    if not numeric.is_integer():
        raise ValueError(f"{path} must be an integer")
    return int(numeric)


def _required_bool(value: object, *, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be a boolean")
    return value


def _reject_unknown_fields(
    payload: Mapping[str, Any],
    allowed: frozenset[str],
    *,
    path: str,
) -> None:
    unknown = sorted(set(payload) - set(allowed))
    if unknown:
        raise ValueError(f"{path} contains unsupported fields: {unknown!r}")


def _positive_float(value: object, *, path: str) -> float:
    numeric = _required_float(value, path=path)
    if numeric <= 0:
        raise ValueError(f"{path} must be > 0")
    return numeric


def _normalise_take_profits(
    entries: Sequence[Mapping[str, Any]],
) -> Sequence[Dict[str, Any]]:
    if not entries:
        raise ValueError("take_profit_orders must contain at least one target")

    cleaned: list[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    fraction_total = 0.0
    for idx, entry in enumerate(entries):
        path = f"take_profit_orders[{idx}]"
        if not isinstance(entry, Mapping):
            raise ValueError(f"{path} must be a mapping")
        _reject_unknown_fields(entry, ATM_TAKE_PROFIT_FIELDS, path=path)

        order_id = str(entry.get("id") or "").strip()
        if not order_id:
            raise ValueError(f"{path}.id must be non-empty text")
        if order_id in seen_ids:
            raise ValueError(f"{path}.id duplicates target id {order_id!r}")
        seen_ids.add(order_id)

        raw_ticks = entry.get("ticks")
        ticks = _required_int(raw_ticks, path=f"{path}.ticks") if raw_ticks is not None else None
        if ticks is not None and ticks <= 0:
            raise ValueError(f"{path}.ticks must be > 0")
        r_multiple = (
            _positive_float(entry.get("r_multiple"), path=f"{path}.r_multiple")
            if entry.get("r_multiple") is not None
            else None
        )
        price = (
            _positive_float(entry.get("price"), path=f"{path}.price")
            if entry.get("price") is not None
            else None
        )
        if sum(value is not None for value in (ticks, r_multiple, price)) != 1:
            raise ValueError(
                f"{path} must define exactly one positive target: ticks, r_multiple, or price"
            )
        if entry.get("size_fraction") is None:
            raise ValueError(f"{path}.size_fraction is required")
        size_fraction = _positive_float(
            entry.get("size_fraction"),
            path=f"{path}.size_fraction",
        )
        if size_fraction > 1:
            raise ValueError(f"{path}.size_fraction must be <= 1")
        fraction_total += size_fraction

        label = str(entry.get("label") or f"Target {idx + 1}").strip()
        cleaned.append(
            {
                "id": order_id,
                "label": label or f"Target {idx + 1}",
                "ticks": ticks,
                "r_multiple": r_multiple,
                "price": price,
                "size_fraction": size_fraction,
            }
        )

    if not math.isclose(fraction_total, 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(
            "take_profit_orders size_fraction values must sum to 1.0; "
            f"got {fraction_total!r}"
        )
    return cleaned


def _extract_take_profits(payload: Mapping[str, Any]) -> Optional[Sequence[Mapping[str, Any]]]:
    if "take_profit_orders" not in payload:
        return None
    value = payload.get("take_profit_orders")
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError("take_profit_orders must be a sequence")
    if not value:
        raise ValueError("take_profit_orders must contain at least one target")
    return value  # type: ignore[return-value]


def _normalise_stop_adjustments(payload: Mapping[str, Any]) -> Sequence[Dict[str, Any]]:
    if "stop_adjustments" not in payload:
        return []
    source = payload.get("stop_adjustments")
    if not isinstance(source, Sequence) or isinstance(source, (str, bytes)):
        raise ValueError("stop_adjustments must be a sequence")

    rules: list[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for idx, entry in enumerate(source):
        path = f"stop_adjustments[{idx}]"
        if not isinstance(entry, Mapping):
            raise ValueError(f"{path} must be a mapping")
        _reject_unknown_fields(entry, ATM_STOP_ADJUSTMENT_FIELDS, path=path)

        rule_id = str(entry.get("id") or "").strip()
        if not rule_id:
            raise ValueError(f"{path}.id must be non-empty text")
        if rule_id in seen_ids:
            raise ValueError(f"{path}.id duplicates stop-adjustment id {rule_id!r}")
        seen_ids.add(rule_id)
        trigger_type = str(entry.get("trigger_type") or "").strip().lower()
        if trigger_type not in {"r_multiple", "target_hit"}:
            raise ValueError(
                f"{path}.trigger_type={trigger_type!r} is unsupported; "
                "supported=['r_multiple', 'target_hit']"
            )
        trigger_value = entry.get("trigger_value")
        trigger_ticks = (
            _positive_float(entry.get("trigger_ticks"), path=f"{path}.trigger_ticks")
            if entry.get("trigger_ticks") is not None
            else None
        )
        action_type = str(entry.get("action_type") or "").strip().lower()
        if action_type not in {"move_to_breakeven", "move_to_r"}:
            raise ValueError(
                f"{path}.action_type={action_type!r} is unsupported; "
                "supported=['move_to_breakeven', 'move_to_r']"
            )

        if trigger_type == "r_multiple":
            numeric_trigger = (
                _positive_float(trigger_value, path=f"{path}.trigger_value")
                if trigger_value is not None
                else None
            )
            if (numeric_trigger is not None) == (trigger_ticks is not None):
                raise ValueError(
                    f"{path} must define exactly one r_multiple trigger: "
                    "trigger_value or trigger_ticks"
                )
            trigger_value = numeric_trigger
        else:
            trigger_value = str(trigger_value or "").strip()
            if not trigger_value:
                raise ValueError(f"{path}.trigger_value must reference a target id")
            if trigger_ticks is not None:
                raise ValueError(f"{path}.trigger_ticks is invalid for target_hit")

        action_value = None
        if action_type == "move_to_r":
            action_value = _positive_float(
                entry.get("action_value"),
                path=f"{path}.action_value",
            )
        elif entry.get("action_value") is not None:
            raise ValueError(f"{path}.action_value is invalid for move_to_breakeven")

        rules.append({
            "id": rule_id,
            "trigger_type": trigger_type,
            "trigger_value": trigger_value,
            "trigger_ticks": trigger_ticks,
            "action_type": action_type,
            "action_value": action_value,
        })

    return rules


def _normalise_breakeven(
    payload: Mapping[str, Any],
    base: Mapping[str, Any],
) -> Dict[str, Any]:
    config = dict(base)
    source = payload.get("breakeven")
    if isinstance(source, Mapping):
        _reject_unknown_fields(source, ATM_BREAKEVEN_FIELDS, path="breakeven")
        if "enabled" in source:
            config["enabled"] = _required_bool(source.get("enabled"), path="breakeven.enabled")
        if source.get("activation_type") is not None:
            activation_type = str(source.get("activation_type") or "").strip().lower()
            if activation_type != "r_multiple":
                raise ValueError("breakeven.activation_type must be 'r_multiple'")
            config["activation_type"] = activation_type
        if "ticks" in source:
            ticks = _required_int(source.get("ticks"), path="breakeven.ticks")
            if ticks < 0:
                raise ValueError("breakeven.ticks must be >= 0")
            if ticks > 0 and source.get("r_multiple") is not None:
                raise ValueError("breakeven must define ticks or r_multiple, not both")
            config["ticks"] = ticks
            if ticks > 0:
                config["r_multiple"] = None
        if "r_multiple" in source:
            raw_r_multiple = source.get("r_multiple")
            if raw_r_multiple is None:
                config["r_multiple"] = None
            else:
                config["r_multiple"] = _positive_float(
                    raw_r_multiple,
                    path="breakeven.r_multiple",
                )
                config["ticks"] = 0
    elif source is not None:
        raise ValueError("breakeven must be a mapping")
    return config


def _normalise_trailing(
    payload: Mapping[str, Any],
    base: Mapping[str, Any],
) -> Dict[str, Any]:
    config = dict(base)
    source = payload.get("trailing")
    if isinstance(source, Mapping):
        _reject_unknown_fields(source, ATM_TRAILING_FIELDS, path="trailing")
        if "enabled" in source:
            config["enabled"] = _required_bool(source.get("enabled"), path="trailing.enabled")
        activation_type = str(
            source.get("activation_type", config.get("activation_type", "r_multiple"))
        ).strip().lower()
        if activation_type not in {"r_multiple", "target_hit"}:
            raise ValueError(
                "trailing.activation_type must be 'r_multiple' or 'target_hit'"
            )
        config["activation_type"] = activation_type
        if "target_index" in source:
            raw_target_index = source.get("target_index")
            if raw_target_index is None:
                config["target_index"] = None
            else:
                target_index = _required_int(raw_target_index, path="trailing.target_index")
                if target_index < 0:
                    raise ValueError("trailing.target_index must be >= 0")
                config["target_index"] = target_index
        if "target_id" in source:
            config["target_id"] = str(source.get("target_id") or "").strip() or None
        if "ticks" in source:
            ticks = _required_int(source.get("ticks"), path="trailing.ticks")
            if ticks < 0:
                raise ValueError("trailing.ticks must be >= 0")
            config["ticks"] = ticks
        if "atr_multiplier" in source:
            raw_atr_multiplier = source.get("atr_multiplier")
            config["atr_multiplier"] = (
                _positive_float(
                    raw_atr_multiplier,
                    path="trailing.atr_multiplier",
                )
                if raw_atr_multiplier is not None
                else None
            )
        if "r_multiple" in source:
            config["r_multiple"] = (
                _positive_float(source.get("r_multiple"), path="trailing.r_multiple")
                if source.get("r_multiple") is not None
                else None
            )
        if activation_type == "target_hit":
            if source.get("r_multiple") is not None:
                raise ValueError("trailing.r_multiple is invalid for target_hit activation")
            config["r_multiple"] = None
    elif source is not None:
        raise ValueError("trailing must be a mapping")
    return config


def _normalise_exit_plan(
    payload: Mapping[str, Any],
    base: Mapping[str, Any],
) -> Dict[str, Any]:
    config = deepcopy(dict(base))
    source = payload.get("exit_plan")
    if source is None:
        source = {}
    elif not isinstance(source, Mapping):
        raise ValueError("exit_plan must be a mapping")
    _reject_unknown_fields(source, ATM_EXIT_PLAN_FIELDS, path="exit_plan")

    fixed_base = config.get("fixed_horizon")
    fixed_config = dict(fixed_base) if isinstance(fixed_base, Mapping) else {}
    fixed_source = source.get("fixed_horizon")
    if isinstance(fixed_source, Mapping):
        _reject_unknown_fields(
            fixed_source,
            ATM_FIXED_HORIZON_FIELDS,
            path="exit_plan.fixed_horizon",
        )
        if "enabled" in fixed_source:
            fixed_config["enabled"] = _required_bool(
                fixed_source.get("enabled"),
                path="exit_plan.fixed_horizon.enabled",
            )
        if "bars" in fixed_source:
            raw_bars = fixed_source.get("bars")
            bars = (
                _required_int(raw_bars, path="exit_plan.fixed_horizon.bars")
                if raw_bars is not None
                else None
            )
            if bars is not None and bars <= 0:
                raise ValueError("exit_plan.fixed_horizon.bars must be > 0")
            fixed_config["bars"] = bars
        if fixed_source.get("price") is not None:
            price = str(fixed_source.get("price") or "").strip().lower()
            if price != "close":
                raise ValueError("exit_plan.fixed_horizon.price must be 'close'")
            fixed_config["price"] = price
        if fixed_source.get("order_type") is not None:
            order_type = str(fixed_source.get("order_type") or "").strip().lower()
            if order_type != "market":
                raise ValueError("exit_plan.fixed_horizon.order_type must be 'market'")
            fixed_config["order_type"] = order_type
    elif fixed_source is not None:
        raise ValueError("exit_plan.fixed_horizon must be a mapping")

    if fixed_config.get("enabled") and not fixed_config.get("bars"):
        raise ValueError("exit_plan.fixed_horizon.bars is required when enabled")
    if not fixed_config.get("enabled") and fixed_config.get("bars") is not None:
        raise ValueError("exit_plan.fixed_horizon.bars must be null when disabled")
    config["fixed_horizon"] = fixed_config
    return config


def normalise_template(
    template: Optional[Mapping[str, Any]],
    *,
    require_template: bool = False,
) -> Dict[str, Any]:
    """Return a fully populated canonical ATM execution-policy template."""

    if require_template and not template:
        raise ValueError("ATM template must be provided.")

    if template is not None and not isinstance(template, Mapping):
        raise ValueError("ATM template must be a mapping.")
    result = deepcopy(DEFAULT_ATM_TEMPLATE)
    if isinstance(result.get("stop_adjustments"), list):
        result["stop_adjustments"] = list(_normalise_stop_adjustments(result))
    if isinstance(result.get("take_profit_orders"), Sequence) and not isinstance(
        result.get("take_profit_orders"),
        (str, bytes),
    ):
        result["take_profit_orders"] = list(
            _normalise_take_profits(result["take_profit_orders"])
        )

    payload: Mapping[str, Any] = template or {}
    _reject_unknown_fields(payload, ATM_TEMPLATE_FIELDS, path="ATM template")

    template_provided = template is not None

    if payload.get("name") is not None:
        candidate_name = str(payload.get("name") or "").strip()
        result["name"] = candidate_name or result.get("name") or DEFAULT_ATM_TEMPLATE["name"]

    resolved_name = str(result.get("name") or "").strip()
    if template_provided and not resolved_name:
        raise ValueError("ATM template name is required.")
    result["name"] = resolved_name or DEFAULT_ATM_TEMPLATE["name"]

    schema_version = _required_int(
        payload.get("schema_version", result.get("schema_version", DEFAULT_ATM_TEMPLATE["schema_version"])),
        path="schema_version",
    )
    if schema_version != ATM_SCHEMA_VERSION:
        raise ValueError(
            f"schema_version={schema_version} is unsupported; expected {ATM_SCHEMA_VERSION}"
        )
    result["schema_version"] = schema_version

    execution_mode = payload.get("execution_mode")
    if execution_mode is not None:
        normalized_mode = str(execution_mode or "").strip().lower()
        if normalized_mode not in {"market", "limit_maker"}:
            raise ValueError(
                "execution_mode must be 'market' or 'limit_maker'"
            )
        result["execution_mode"] = normalized_mode

    limit_maker_payload = payload.get("limit_maker")
    if limit_maker_payload is not None:
        if not isinstance(limit_maker_payload, Mapping):
            raise ValueError("limit_maker must be a mapping")
        _reject_unknown_fields(
            limit_maker_payload, ATM_LIMIT_MAKER_FIELDS, path="limit_maker"
        )
        if "limit_maker" not in result or not isinstance(result["limit_maker"], Mapping):
            result["limit_maker"] = {}
        anchor_price = limit_maker_payload.get("anchor_price")
        if anchor_price is not None:
            normalized_anchor = str(anchor_price or "").strip().lower()
            if normalized_anchor != "signal_price":
                raise ValueError(
                    "unsupported entry anchor "
                    f"anchor_price={normalized_anchor!r}. "
                    "Next-bar entry requires an explicit pending signal-entry lifecycle."
                )
            result["limit_maker"]["anchor_price"] = normalized_anchor
        offset_type = limit_maker_payload.get("offset_type")
        offset_value = limit_maker_payload.get("offset_value")
        if offset_type is not None:
            normalized_offset_type = str(offset_type or "").strip().lower()
            if normalized_offset_type not in {"ticks", "atr_pct", "r_fraction"}:
                raise ValueError(
                    "limit_maker.offset_type must be 'ticks', 'atr_pct', or 'r_fraction'"
                )
            result["limit_maker"]["offset_type"] = normalized_offset_type
        if offset_value is not None:
            normalized_offset = _required_float(offset_value, path="limit_maker.offset_value")
            if normalized_offset < 0:
                raise ValueError("limit_maker.offset_value must be >= 0")
            result["limit_maker"]["offset_value"] = normalized_offset
        validity_window = limit_maker_payload.get("validity_window")
        if validity_window is not None:
            normalized_validity = _required_int(
                validity_window,
                path="limit_maker.validity_window",
            )
            if normalized_validity <= 0:
                raise ValueError("limit_maker.validity_window must be > 0")
            result["limit_maker"]["validity_window"] = normalized_validity
        fallback = limit_maker_payload.get("fallback")
        if fallback is not None:
            normalized_fallback = str(fallback or "").strip().lower()
            if normalized_fallback not in {"cancel", "convert_to_market"}:
                raise ValueError(
                    "limit_maker.fallback must be 'cancel' or 'convert_to_market'"
                )
            result["limit_maker"]["fallback"] = normalized_fallback

    initial_stop_config = payload.get("initial_stop")
    if isinstance(initial_stop_config, Mapping):
        _reject_unknown_fields(
            initial_stop_config, ATM_INITIAL_STOP_FIELDS, path="initial_stop"
        )
        if "initial_stop" not in result or not isinstance(result["initial_stop"], dict):
            result["initial_stop"] = {}
        if initial_stop_config.get("mode") is not None:
            mode = str(initial_stop_config.get("mode") or "").strip().lower()
            if mode != "atr":
                raise ValueError("initial_stop.mode must be 'atr'")
            result["initial_stop"]["mode"] = mode
        if initial_stop_config.get("atr_period") is not None:
            atr_period = _required_int(
                initial_stop_config.get("atr_period"),
                path="initial_stop.atr_period",
            )
            if atr_period <= 0:
                raise ValueError("initial_stop.atr_period must be > 0")
            result["initial_stop"]["atr_period"] = atr_period
        if initial_stop_config.get("atr_multiplier") is not None:
            result["initial_stop"]["atr_multiplier"] = _positive_float(
                initial_stop_config.get("atr_multiplier"),
                path="initial_stop.atr_multiplier",
            )
    elif initial_stop_config is not None:
        raise ValueError("initial_stop must be a mapping")

    result["exit_plan"] = _normalise_exit_plan(
        payload,
        result.get("exit_plan") if isinstance(result.get("exit_plan"), Mapping) else {},
    )
    result["breakeven"] = _normalise_breakeven(
        payload,
        result.get("breakeven") if isinstance(result.get("breakeven"), Mapping) else {},
    )
    result["trailing"] = _normalise_trailing(
        payload,
        result.get("trailing") if isinstance(result.get("trailing"), Mapping) else {},
    )

    entries = _extract_take_profits(payload)
    if entries is not None:
        result["take_profit_orders"] = list(_normalise_take_profits(entries))

    stop_adjustments = list(_normalise_stop_adjustments(payload))
    if "stop_adjustments" in payload:
        result["stop_adjustments"] = stop_adjustments
    elif not isinstance(result.get("stop_adjustments"), list):
        result["stop_adjustments"] = []

    return result


__all__ = [
    "normalise_template",
]

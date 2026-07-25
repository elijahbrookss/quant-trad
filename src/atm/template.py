"""ATM template normalization and processing utilities."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from .schema import DEFAULT_ATM_TEMPLATE




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


def _first_present(payload: Mapping[str, Any], *keys: str) -> object:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _positive_float(value: object, *, path: str) -> float:
    numeric = _required_float(value, path=path)
    if numeric <= 0:
        raise ValueError(f"{path} must be > 0")
    return numeric


def _normalise_take_profits(
    entries: Sequence[Mapping[str, Any]],
    fallback_contracts: Optional[int],
) -> Tuple[Sequence[Dict[str, Any]], int]:
    if not entries:
        return [], 0

    del fallback_contracts
    cleaned: list[Dict[str, Any]] = []
    fractions: list[Optional[float]] = []
    for idx, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise ValueError(f"take_profit_orders[{idx}] must be a mapping")
        if entry.get("size_fraction") is None:
            fractions.append(None)
        else:
            fraction = _positive_float(
                entry.get("size_fraction"),
                path=f"take_profit_orders[{idx}].size_fraction",
            )
            if fraction > 1:
                raise ValueError(f"take_profit_orders[{idx}].size_fraction must be <= 1")
            fractions.append(fraction)

    provided_fraction_count = sum(value is not None for value in fractions)
    if provided_fraction_count not in {0, len(entries)}:
        raise ValueError(
            "take_profit_orders must provide size_fraction for every target or for none"
        )
    if provided_fraction_count:
        normalized_fractions = [float(value) for value in fractions if value is not None]
        fraction_total = sum(normalized_fractions)
        if not math.isclose(fraction_total, 1.0, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError(
                "take_profit_orders size_fraction values must sum to 1.0; "
                f"got {fraction_total!r}"
            )
    else:
        normalized_fractions = [1.0 / len(entries) for _ in entries]

    seen_ids: set[str] = set()
    for idx, entry in enumerate(entries):
        path = f"take_profit_orders[{idx}]"
        tick_keys = [
            key
            for key in ("ticks", "target_ticks", "offset_ticks")
            if entry.get(key) is not None
        ]
        if len(tick_keys) > 1:
            raise ValueError(f"{path} defines conflicting tick fields: {tick_keys!r}")
        raw_ticks = entry.get(tick_keys[0]) if tick_keys else None
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
        label = str(entry.get("label") or entry.get("name") or f"Target {idx + 1}").strip()
        order_id = str(entry.get("id") or f"tp-{idx + 1}").strip()
        if not order_id:
            raise ValueError(f"{path}.id must be non-empty text")
        if order_id in seen_ids:
            raise ValueError(f"{path}.id duplicates target id {order_id!r}")
        seen_ids.add(order_id)
        size_fraction = normalized_fractions[idx] if idx < len(normalized_fractions) else (1.0 / len(entries))
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

    return cleaned, 0  # No longer calculating contracts


def _extract_take_profits(payload: Mapping[str, Any]) -> Optional[Sequence[Mapping[str, Any]]]:
    configured_keys = [
        key
        for key in ("take_profit_orders", "take_profit_targets", "take_profits", "targets")
        if key in payload
    ]
    if len(configured_keys) > 1:
        raise ValueError(
            "ATM template must define only one take-profit field; "
            f"got {configured_keys!r}"
        )
    for key in (
        "take_profit_orders",
        "take_profit_targets",
        "take_profits",
        "targets",
    ):
        if key not in payload:
            continue
        value = payload.get(key)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise ValueError(f"{key} must be a sequence")
        if not value:
            raise ValueError(f"{key} must contain at least one target")
        if key == "targets" and all(
            isinstance(item, (int, float)) and not isinstance(item, bool)
            for item in value
        ):
            targets = []
            for idx, item in enumerate(value):
                ticks = _required_int(item, path=f"targets[{idx}]")
                if ticks <= 0:
                    raise ValueError(f"targets[{idx}] must be > 0")
                targets.append(
                    {"id": f"tp-{idx + 1}", "label": f"TP +{ticks}", "ticks": ticks}
                )
            return targets
        return value  # type: ignore[return-value]
    return None


def _normalise_stop_adjustments(payload: Mapping[str, Any]) -> Sequence[Dict[str, Any]]:
    if "stop_adjustments" not in payload:
        return []
    source = payload.get("stop_adjustments")
    if not isinstance(source, Sequence) or isinstance(source, (str, bytes)):
        raise ValueError("stop_adjustments must be a sequence")

    rules: list[Dict[str, Any]] = []
    for idx, entry in enumerate(source):
        path = f"stop_adjustments[{idx}]"
        if not isinstance(entry, Mapping):
            raise ValueError(f"{path} must be a mapping")

        raw_trigger = entry.get("trigger")
        if raw_trigger is not None and not isinstance(raw_trigger, Mapping):
            raise ValueError(f"{path}.trigger must be a mapping")
        raw_action = entry.get("action")
        if raw_action is not None and not isinstance(raw_action, Mapping):
            raise ValueError(f"{path}.action must be a mapping")
        trigger = raw_trigger if isinstance(raw_trigger, Mapping) else {}
        action = raw_action if isinstance(raw_action, Mapping) else {}

        trigger_type = str(
            trigger.get("type")
            or entry.get("trigger_type")
            or ""
        ).strip().replace("_reached", "").lower()
        if trigger_type not in {"r_multiple", "target_hit"}:
            raise ValueError(
                f"{path}.trigger_type={trigger_type!r} is unsupported; "
                "supported=['r_multiple', 'target_hit']"
            )
        trigger_value = trigger.get("value")
        if trigger_value is None:
            trigger_value = _first_present(
                entry,
                "trigger_value",
                "trigger_target_id",
                "target_id",
            )
        trigger_ticks = (
            _positive_float(entry.get("trigger_ticks"), path=f"{path}.trigger_ticks")
            if entry.get("trigger_ticks") is not None
            else None
        )
        action_type = str(action.get("type") or entry.get("action_type") or "").strip().lower()
        if action_type not in {"move_to_breakeven", "move_to_r"}:
            suffix = " Use top-level trailing config for trailing stops." if action_type == "trail_atr" else ""
            raise ValueError(
                f"{path}.action_type={action_type!r} is unsupported; "
                f"supported=['move_to_breakeven', 'move_to_r'].{suffix}"
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
            raw_action_value = (
                action.get("value") if action.get("value") is not None
                else _first_present(entry, "action_value", "action_r")
            )
            action_value = _positive_float(
                raw_action_value,
                path=f"{path}.action_value",
            )
        elif action.get("value") is not None or entry.get("action_value") is not None:
            raise ValueError(f"{path}.action_value is invalid for move_to_breakeven")

        rules.append({
            "id": entry.get("id"),
            "trigger_type": trigger_type,
            "trigger_value": trigger_value,
            "trigger_ticks": trigger_ticks if trigger_ticks and trigger_ticks > 0 else None,
            "action_type": action_type,
            "action_value": action_value,
            "atr_period": None,
            "atr_multiplier": None,
        })

    return rules


def _normalise_breakeven(
    payload: Mapping[str, Any],
    base: Mapping[str, Any],
) -> Dict[str, Any]:
    config = dict(base)
    source = payload.get("breakeven")
    if isinstance(source, Mapping):
        if "enabled" in source:
            config["enabled"] = _required_bool(source.get("enabled"), path="breakeven.enabled")
        if source.get("activation_type") is not None:
            activation_type = str(source.get("activation_type") or "").strip().lower()
            if activation_type != "r_multiple":
                raise ValueError("breakeven.activation_type must be 'r_multiple'")
            config["activation_type"] = activation_type
        if "target_index" in source:
            raw_target_index = source.get("target_index")
            if raw_target_index is None:
                config["target_index"] = None
            else:
                target_index = _required_int(raw_target_index, path="breakeven.target_index")
                if target_index < 0:
                    raise ValueError("breakeven.target_index must be >= 0")
                config["target_index"] = target_index
        if "target_id" in source:
            config["target_id"] = str(source.get("target_id") or "").strip() or None
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
        ticks = _required_int(source, path="breakeven")
        if ticks <= 0:
            raise ValueError("breakeven must be > 0")
        config["ticks"] = ticks
        config["r_multiple"] = None

    if payload.get("breakeven_trigger_ticks") is not None:
        legacy_ticks = _required_int(
            payload.get("breakeven_trigger_ticks"),
            path="breakeven_trigger_ticks",
        )
        if legacy_ticks <= 0:
            raise ValueError("breakeven_trigger_ticks must be > 0")
        config["enabled"] = legacy_ticks > 0
        config["ticks"] = legacy_ticks
        config["r_multiple"] = None
    if payload.get("breakeven_target_index") is not None:
        raise ValueError(
            "breakeven_target_index is unsupported; use a target_hit stop_adjustment"
        )
    return config


def _normalise_trailing(
    payload: Mapping[str, Any],
    base: Mapping[str, Any],
) -> Dict[str, Any]:
    config = dict(base)
    source = payload.get("trailing")
    if source is None:
        source = payload.get("trailing_stop")
    if isinstance(source, Mapping):
        if "enabled" in source:
            config["enabled"] = _required_bool(source.get("enabled"), path="trailing.enabled")
        activation_type = str(
            source.get("activation_type")
            or (
                "target_hit"
                if source.get("target_index") is not None or source.get("target_id") is not None
                else "r_multiple"
            )
            or "r_multiple"
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
        if source.get("atr_period") is not None:
            atr_period = _required_int(source.get("atr_period"), path="trailing.atr_period")
            if atr_period <= 0:
                raise ValueError("trailing.atr_period must be > 0")
            config["atr_period"] = atr_period
        if source.get("r_multiple") is not None:
            config["r_multiple"] = _positive_float(
                source.get("r_multiple"),
                path="trailing.r_multiple",
            )
        elif "r_multiple" in source:
            config["r_multiple"] = None
        if activation_type == "target_hit":
            if source.get("r_multiple") is not None:
                raise ValueError("trailing.r_multiple is invalid for target_hit activation")
            config["r_multiple"] = None
    elif isinstance(source, bool):
        config["enabled"] = source
    elif source is not None:
        raise ValueError("trailing must be a mapping or boolean")

    if payload.get("trail_after_target_index") is not None:
        legacy_target = _required_int(
            payload.get("trail_after_target_index"),
            path="trail_after_target_index",
        )
        if legacy_target < 0:
            raise ValueError("trail_after_target_index must be >= 0")
        config["target_index"] = legacy_target
        config["activation_type"] = "target_hit"
    if payload.get("trail_after_ticks") is not None:
        legacy_ticks = _required_int(payload.get("trail_after_ticks"), path="trail_after_ticks")
        if legacy_ticks <= 0:
            raise ValueError("trail_after_ticks must be > 0")
        config["enabled"] = True
        config["ticks"] = legacy_ticks
        config["r_multiple"] = None
    if payload.get("trail_atr_multiplier") is not None:
        config["enabled"] = True
        config["atr_multiplier"] = _positive_float(
            payload.get("trail_atr_multiplier"),
            path="trail_atr_multiplier",
        )
    if payload.get("trail_atr_period") is not None:
        config["atr_period"] = _required_int(
            payload.get("trail_atr_period"),
            path="trail_atr_period",
        )
        if config["atr_period"] <= 0:
            raise ValueError("trail_atr_period must be > 0")
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

    fixed_base = config.get("fixed_horizon")
    fixed_config = dict(fixed_base) if isinstance(fixed_base, Mapping) else {}
    fixed_source = _first_present(source, "fixed_horizon", "fixedHorizon")
    if fixed_source is None:
        fixed_source = payload.get("fixed_horizon")
    if isinstance(fixed_source, Mapping):
        if "enabled" in fixed_source:
            fixed_config["enabled"] = _required_bool(
                fixed_source.get("enabled"),
                path="exit_plan.fixed_horizon.enabled",
            )
        raw_bars = _first_present(fixed_source, "bars", "hold_bars")
        if raw_bars is not None:
            bars = _required_int(raw_bars, path="exit_plan.fixed_horizon.bars")
            if bars <= 0:
                raise ValueError("exit_plan.fixed_horizon.bars must be > 0")
            fixed_config["bars"] = bars
            if "enabled" not in fixed_source:
                fixed_config["enabled"] = True
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
    else:
        legacy_raw_bars = _first_present(payload, "fixed_horizon_bars", "hold_bars")
        if legacy_raw_bars is not None:
            legacy_bars = _required_int(legacy_raw_bars, path="fixed_horizon_bars")
            if legacy_bars <= 0:
                raise ValueError("fixed_horizon_bars must be > 0")
            fixed_config["bars"] = legacy_bars
            fixed_config["enabled"] = True

    if fixed_config.get("enabled") and not fixed_config.get("bars"):
        raise ValueError("exit_plan.fixed_horizon.bars is required when enabled")
    if not fixed_config.get("enabled") and fixed_config.get("bars") is not None:
        raise ValueError("exit_plan.fixed_horizon.bars must be null when disabled")
    config["fixed_horizon"] = fixed_config
    return config


def normalise_template(
    template: Optional[Mapping[str, Any]],
    *,
    base: Optional[Mapping[str, Any]] = None,
    require_template: bool = False,
) -> Dict[str, Any]:
    """Return a fully-populated ATM template merged with defaults."""

    if require_template and not template:
        raise ValueError("ATM template must be provided.")

    if template is not None and not isinstance(template, Mapping):
        raise ValueError("ATM template must be a mapping.")
    result = deepcopy(base or DEFAULT_ATM_TEMPLATE)
    if isinstance(result.get("stop_adjustments"), list):
        result["stop_adjustments"] = list(_normalise_stop_adjustments(result))
    if isinstance(result.get("take_profit_orders"), Sequence) and not isinstance(
        result.get("take_profit_orders"),
        (str, bytes),
    ):
        base_orders, _ = _normalise_take_profits(
            result["take_profit_orders"],
            result.get("contracts"),
        )
        result["take_profit_orders"] = list(base_orders)

    payload: Mapping[str, Any]
    if not template:
        payload = {}
    elif "atm_template" in template and isinstance(template["atm_template"], Mapping):
        payload = template["atm_template"]  # type: ignore[assignment]
    else:
        payload = template

    payload_meta = payload.get("_meta") if isinstance(payload.get("_meta"), Mapping) else {}
    meta: Dict[str, Any] = dict(result.get("_meta") or {})
    template_provided = template is not None

    if payload.get("name") is not None:
        candidate_name = str(payload.get("name") or "").strip()
        result["name"] = candidate_name or result.get("name") or DEFAULT_ATM_TEMPLATE["name"]

    resolved_name = str(result.get("name") or "").strip()
    if template_provided and not resolved_name:
        raise ValueError("ATM template name is required.")
    result["name"] = resolved_name or DEFAULT_ATM_TEMPLATE["name"]

    # Handle schema_version
    result["schema_version"] = _required_int(
        payload.get("schema_version", result.get("schema_version", DEFAULT_ATM_TEMPLATE["schema_version"])),
        path="schema_version",
    )

    execution_mode = _first_present(payload, "execution_mode", "executionMode")
    if execution_mode is not None:
        normalized_mode = str(execution_mode or "").strip().lower()
        if normalized_mode not in {"market", "limit_maker"}:
            raise ValueError(
                "execution_mode must be 'market' or 'limit_maker'"
            )
        result["execution_mode"] = normalized_mode

    limit_maker_payload = _first_present(payload, "limit_maker", "limitMaker")
    if limit_maker_payload is not None:
        if not isinstance(limit_maker_payload, Mapping):
            raise ValueError("limit_maker must be a mapping")
        if "limit_maker" not in result or not isinstance(result["limit_maker"], Mapping):
            result["limit_maker"] = {}
        anchor_price = _first_present(limit_maker_payload, "anchor_price", "anchorPrice")
        if anchor_price is not None:
            normalized_anchor = str(anchor_price or "").strip().lower()
            if normalized_anchor != "signal_price":
                raise ValueError(
                    "unsupported entry anchor "
                    f"anchor_price={normalized_anchor!r}. "
                    "Next-bar entry requires an explicit pending signal-entry lifecycle."
                )
            result["limit_maker"]["anchor_price"] = normalized_anchor
        offset = limit_maker_payload.get("offset")
        if isinstance(offset, Mapping):
            offset_type = _first_present(offset, "type", "offset_type", "offsetType")
            offset_value = _first_present(offset, "value", "offset_value", "offsetValue")
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
        elif offset is not None:
            raise ValueError("limit_maker.offset must be a mapping")
        else:
            offset_type = _first_present(limit_maker_payload, "offset_type", "offsetType")
            offset_value = _first_present(limit_maker_payload, "offset_value", "offsetValue")
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
        validity_window = _first_present(limit_maker_payload, "validity_window", "validityWindow")
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

    # Handle nested initial_stop object (schema v2)
    initial_stop_config = payload.get("initial_stop")
    # Schema v2: nested initial_stop object
    if isinstance(initial_stop_config, Mapping):
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
        orders, total_contracts = _normalise_take_profits(entries, result.get("contracts"))
        result["take_profit_orders"] = list(orders)
        if total_contracts:
            result["contracts"] = total_contracts

    legacy_stop_fields = [
        field
        for field in (
            "stop_ticks",
            "stop_loss_ticks",
            "stop",
            "stop_r",
            "stop_r_multiple",
            "stop_price",
            "ticks_stop",
        )
        if field in payload
    ]
    if legacy_stop_fields:
        raise ValueError(
            f"unsupported ATM stop fields {legacy_stop_fields!r}; use initial_stop"
        )

    # Handle stop adjustments
    stop_adjustments = list(_normalise_stop_adjustments(payload))
    if "stop_adjustments" in payload:
        result["stop_adjustments"] = stop_adjustments
    elif stop_adjustments:
        result["stop_adjustments"] = stop_adjustments
    elif not isinstance(result.get("stop_adjustments"), list):
        result["stop_adjustments"] = []

    def _should_override(field: str, provided: Any) -> bool:
        flag = payload_meta.get(f"{field}_override")
        if flag is not None:
            return bool(flag)
        if provided is None:
            return False
        current = result.get(field)
        return provided != current

    for key in (
        "tick_size",
        "tick_value",
        "contract_size",
        "maker_fee_rate",
        "taker_fee_rate",
        "quote_currency",
    ):
        provided = payload.get(key) if isinstance(payload, Mapping) else None
        if _should_override(key, provided):
            result[key] = provided
            meta[f"{key}_override"] = True
        else:
            meta[f"{key}_override"] = False

    if meta:
        result["_meta"] = meta
    elif "_meta" in result:
        result.pop("_meta", None)

    return result


def merge_templates(*templates: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Merge multiple template sources from left to right."""

    merged = deepcopy(DEFAULT_ATM_TEMPLATE)
    for template in templates:
        if template:
            merged = normalise_template(template, base=merged)
    return merged


__all__ = [
    "merge_templates",
    "normalise_template",
]

"""ATM template normalization and processing utilities."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from .schema import DEFAULT_ATM_TEMPLATE


def _coerce_int(value: Optional[object], default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        numeric = int(float(value))
    except (TypeError, ValueError):
        return default
    return numeric


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric


def _normalise_take_profits(
    entries: Sequence[Mapping[str, Any]],
    fallback_contracts: Optional[int],
) -> Tuple[Sequence[Dict[str, Any]], int]:
    if not entries:
        return [], 0

    cleaned: list[Dict[str, Any]] = []
    fallback = max(int(fallback_contracts or len(entries)), len(entries) or 1)

    fractions: list[Optional[float]] = []
    for entry in entries:
        # v2 schema: size_fraction (0-1 range)
        raw_fraction = entry.get("size_fraction")
        value = _coerce_float(raw_fraction)
        # Keep as fraction (0-1 range) for consistency
        if value is not None and 0 <= value <= 1:
            fractions.append(value)
        elif value is not None and value > 1:
            # If value > 1, assume it's a percentage and convert to fraction
            fractions.append(value / 100)
        else:
            fractions.append(None)

    has_fraction = any(value is not None for value in fractions)

    # Normalize fractions to sum to 1.0
    if has_fraction:
        weights = [max(value or 0, 0.0) for value in fractions]
        weight_total = sum(weights)
        if weight_total > 0:
            # Normalize so fractions sum to 1.0
            normalized_fractions = [weight / weight_total for weight in weights]
        else:
            # Equal distribution if no weights
            normalized_fractions = [1.0 / len(entries) for _ in entries]
    else:
        # Equal distribution if no fractions specified
        normalized_fractions = [1.0 / len(entries) for _ in entries]

    for idx, entry in enumerate(entries):
        ticks = _coerce_int(
            entry.get("ticks")
            or entry.get("target_ticks")
            or entry.get("offset_ticks"),
            0,
        ) or 0
        label = (entry.get("label") or entry.get("name") or f"Target {idx + 1}").strip()
        order_id = entry.get("id") or f"tp-{idx + 1}"
        r_multiple = _coerce_float(entry.get("r_multiple"))
        price = _coerce_float(entry.get("price"))
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


def _extract_take_profits(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    for key in (
        "take_profit_orders",
        "take_profit_targets",
        "take_profits",
        "targets",
    ):
        value = payload.get(key)
        if isinstance(value, Sequence) and value:
            if key == "targets" and all(isinstance(item, (int, float)) for item in value):
                return [
                    {"id": f"tp-{idx + 1}", "label": f"TP +{int(item)}", "ticks": int(item)}
                    for idx, item in enumerate(value)
                ]
            return value  # type: ignore[return-value]
    return []


def _normalise_stop_adjustments(payload: Mapping[str, Any]) -> Sequence[Dict[str, Any]]:
    source = payload.get("stop_adjustments")
    if not isinstance(source, Sequence) or isinstance(source, (str, bytes)):
        return []

    rules: list[Dict[str, Any]] = []
    for entry in source:
        if not isinstance(entry, Mapping):
            continue

        # Schema v2: nested trigger/action format
        trigger = entry.get("trigger")
        action = entry.get("action")

        if not isinstance(trigger, Mapping) or not isinstance(action, Mapping):
            continue

        trigger_type = str(trigger.get("type") or "").replace("_reached", "").lower()
        trigger_value = trigger.get("value")
        action_type = str(action.get("type") or "").lower()

        if trigger_type not in {"r_multiple", "target_hit"}:
            continue

        # Validate trigger value
        if trigger_type == "r_multiple":
            trigger_value = _coerce_float(trigger_value, 0.0)
            if trigger_value is None or trigger_value <= 0:
                continue
        if trigger_type == "target_hit" and trigger_value is None:
            continue

        # Parse action
        action_value = None
        atr_period = None
        atr_multiplier = None

        if action_type == "move_to_r":
            action_value = _coerce_float(action.get("value"), 0.0)
            if action_value is None or action_value <= 0:
                continue
        elif action_type == "trail_atr":
            atr_period = _coerce_int(action.get("atr_period"), 14)
            atr_multiplier = _coerce_float(action.get("atr_multiplier"), 1.0)
        elif action_type != "move_to_breakeven":
            continue

        rules.append({
            "id": entry.get("id"),
            "trigger_type": trigger_type,
            "trigger_value": trigger_value,
            "action_type": action_type,
            "action_value": action_value,
            "atr_period": atr_period,
            "atr_multiplier": atr_multiplier,
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
            config["enabled"] = bool(source.get("enabled"))
        if source.get("target_index") is not None:
            config["target_index"] = max(_coerce_int(source.get("target_index"), 0) or 0, 0)
        if source.get("ticks") is not None:
            config["ticks"] = max(_coerce_int(source.get("ticks"), 0) or 0, 0)
        if source.get("r_multiple") is not None:
            config["r_multiple"] = float(source.get("r_multiple") or 0.0)
    elif source is not None:
        config["ticks"] = max(_coerce_int(source, 0) or 0, 0)

    legacy_ticks = _coerce_int(payload.get("breakeven_trigger_ticks"))
    if legacy_ticks is not None:
        config["ticks"] = max(legacy_ticks, 0)
    legacy_target = _coerce_int(payload.get("breakeven_target_index"))
    if legacy_target is not None:
        config["target_index"] = max(legacy_target, 0)
    return config


def _normalise_trailing(
    payload: Mapping[str, Any],
    base: Mapping[str, Any],
) -> Dict[str, Any]:
    config = dict(base)
    source = payload.get("trailing")
    if isinstance(source, Mapping):
        if "enabled" in source:
            config["enabled"] = bool(source.get("enabled"))
        activation_type = str(
            source.get("activation_type")
            or ("target_hit" if source.get("target_index") is not None else "r_multiple")
            or "r_multiple"
        ).lower()
        if activation_type not in {"r_multiple", "target_hit"}:
            activation_type = "r_multiple"
        config["activation_type"] = activation_type
        if source.get("target_index") is not None:
            config["target_index"] = max(_coerce_int(source.get("target_index"), 0) or 0, 0)
        if source.get("target_id") is not None:
            config["target_id"] = source.get("target_id")
        if source.get("ticks") is not None:
            config["ticks"] = max(_coerce_int(source.get("ticks"), 0) or 0, 0)
        if source.get("atr_multiplier") is not None:
            config["atr_multiplier"] = float(source.get("atr_multiplier") or 1.0)
        if source.get("atr_period") is not None:
            config["atr_period"] = max(_coerce_int(source.get("atr_period"), 14) or 14, 1)
        if source.get("r_multiple") is not None:
            config["r_multiple"] = float(source.get("r_multiple") or 0.0)
    elif isinstance(source, bool):
        config["enabled"] = source

    legacy_target = _coerce_int(payload.get("trail_after_target_index"))
    if legacy_target is not None:
        config["target_index"] = max(legacy_target, 0)
        config["activation_type"] = "target_hit"
    legacy_ticks = _coerce_int(payload.get("trail_after_ticks"))
    if legacy_ticks is not None:
        config["ticks"] = max(legacy_ticks, 0)
    legacy_multiplier = _coerce_float(payload.get("trail_atr_multiplier"))
    if legacy_multiplier is not None:
        config["atr_multiplier"] = float(legacy_multiplier)
    legacy_period = _coerce_int(payload.get("trail_atr_period"))
    if legacy_period is not None:
        config["atr_period"] = max(legacy_period, 1)
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

    result = deepcopy(base or DEFAULT_ATM_TEMPLATE)
    if not template:
        return result

    payload: Mapping[str, Any]
    if "atm_template" in template and isinstance(template["atm_template"], Mapping):
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
    schema_version = payload.get("schema_version", 1)
    result["schema_version"] = schema_version

    # Handle nested initial_stop object (schema v2)
    initial_stop_config = payload.get("initial_stop")
    # Schema v2: nested initial_stop object
    if isinstance(initial_stop_config, Mapping):
        if "initial_stop" not in result or not isinstance(result["initial_stop"], dict):
            result["initial_stop"] = {}

        if initial_stop_config.get("mode") is not None:
            result["initial_stop"]["mode"] = str(initial_stop_config.get("mode") or "atr")
        if initial_stop_config.get("atr_period") is not None:
            result["initial_stop"]["atr_period"] = max(_coerce_int(initial_stop_config.get("atr_period"), 14) or 14, 1)
        if initial_stop_config.get("atr_multiplier") is not None:
            result["initial_stop"]["atr_multiplier"] = float(initial_stop_config.get("atr_multiplier") or 1.0)
    # Schema v2: nested risk object
    risk_config = payload.get("risk")
    if isinstance(risk_config, Mapping):
        if "risk" not in result or not isinstance(result["risk"], dict):
            result["risk"] = {}

        if risk_config.get("global_risk_multiplier") is not None:
            result["risk"]["global_risk_multiplier"] = _coerce_float(risk_config.get("global_risk_multiplier"), 1.0) or 1.0
        if risk_config.get("base_risk_per_trade") is not None:
            result["risk"]["base_risk_per_trade"] = _coerce_float(risk_config.get("base_risk_per_trade"))

    entries = _extract_take_profits(payload)
    if entries:
        orders, total_contracts = _normalise_take_profits(entries, result.get("contracts"))
        if orders:
            result["take_profit_orders"] = list(orders)
            if total_contracts:
                result["contracts"] = total_contracts

    stop_ticks = _coerce_int(
        payload.get("stop_ticks")
        or payload.get("stop_loss_ticks")
        or payload.get("stop"),
        result.get("stop_ticks"),
    )
    if stop_ticks is not None:
        result["stop_ticks"] = max(stop_ticks, 1)

    stop_r_multiple = _coerce_float(payload.get("stop_r") or payload.get("stop_r_multiple"))
    if stop_r_multiple is not None:
        value = float(stop_r_multiple)
        if value == 0:
            result["stop_r_multiple"] = result.get("stop_r_multiple") or 1.0
        else:
            result["stop_r_multiple"] = abs(value)

    stop_price = _coerce_float(payload.get("stop_price"))
    if stop_price is not None:
        result["stop_price"] = float(stop_price)

    # Handle stop adjustments
    stop_adjustments = list(_normalise_stop_adjustments(payload))
    if stop_adjustments:
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


def template_metrics(template: Optional[Mapping[str, Any]]) -> Dict[str, Optional[float]]:
    """Compute aggregate metrics such as average reward and R:R."""

    config = normalise_template(template)
    take_profits: Sequence[Mapping[str, Any]] = config.get("take_profit_orders") or []
    total_contracts = max(int(config.get("contracts") or 0), 0)
    weighted_reward = 0.0
    max_reward = 0.0
    actual_contracts = 0
    for order in take_profits:
        ticks = abs(float(order.get("ticks") or 0.0))
        contracts = max(int(order.get("contracts") or 0), 0)
        weighted_reward += ticks * contracts
        max_reward = max(max_reward, ticks)
        actual_contracts += contracts
    if total_contracts == 0 and actual_contracts > 0:
        total_contracts = actual_contracts
    avg_reward = weighted_reward / total_contracts if total_contracts else 0.0
    stop_ticks = abs(float(config.get("stop_ticks") or 0.0))
    reward_to_risk = (avg_reward / stop_ticks) if stop_ticks else None
    return {
        "average_reward_ticks": round(avg_reward, 4),
        "max_reward_ticks": round(max_reward, 4),
        "stop_ticks": round(stop_ticks, 4),
        "reward_to_risk": round(reward_to_risk, 4) if reward_to_risk is not None else None,
        "contracts": total_contracts,
    }


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
    "template_metrics",
]

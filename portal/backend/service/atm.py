"""ATM template helpers for per-strategy risk configuration."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

DEFAULT_ATM_TEMPLATE: Dict[str, Any] = {
    "contracts": 3,
    "tick_size": 0.01,
    "risk_unit_mode": "atr",
    "ticks_stop": None,
    "global_risk_multiplier": 1.0,
    "base_risk_per_trade": None,
    "atr_r_multiple": 1.0,
    "stop_ticks": 35,
    "stop_price": None,
    "take_profit_orders": [
        {"id": "tp-1", "label": "TP +20", "ticks": 20, "contracts": 1},
        {"id": "tp-2", "label": "TP +40", "ticks": 40, "contracts": 1},
        {"id": "tp-3", "label": "TP +60", "ticks": 60, "contracts": 1},
    ],
    "stop_adjustments": [],
    "breakeven": {"enabled": True, "target_index": 0, "ticks": 20},
    "trailing": {
        "enabled": True,
        "target_index": 1,
        "ticks": None,
        "atr_multiplier": 1.0,
        "atr_period": 14,
    },
}


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

    percents: list[Optional[float]] = []
    for entry in entries:
        raw_percent = entry.get("size_percent") or entry.get("size_pct") or entry.get("size")
        value = _coerce_float(raw_percent)
        if value is not None and 0 <= value <= 1:
            value *= 100
        percents.append(value if value is None or value >= 0 else None)

    has_percent = any(value is not None for value in percents)

    specified_contracts = [max(_coerce_int(entry.get("contracts"), 0) or 0, 0) for entry in entries]
    contract_counts: list[int] = []
    if has_percent:
        weights = [max(value or 0, 0.0) for value in percents]
        weight_total = sum(weights)
        if weight_total > 0:
            scaled = [(weight / weight_total) * fallback for weight in weights]
            floors = [int(math.floor(value)) for value in scaled]
            remainder = max(fallback - sum(floors), 0)
            order = sorted(range(len(scaled)), key=lambda idx: scaled[idx] - floors[idx], reverse=True)
            for idx in range(remainder):
                floors[order[idx % len(order)]] += 1
            contract_counts = floors
        else:
            base = max(fallback // len(entries), 1)
            remainder = max(fallback - base * len(entries), 0)
            for idx in range(len(entries)):
                contract_counts.append(base + (1 if idx < remainder else 0))
    else:
        use_distribution = sum(specified_contracts) == 0
        if use_distribution:
            base = max(fallback // len(entries), 1)
            remainder = max(fallback - base * len(entries), 0)
            for idx in range(len(entries)):
                contract_counts.append(base + (1 if idx < remainder else 0))
        else:
            contract_counts = [count if count > 0 else 1 for count in specified_contracts]

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
        contracts = contract_counts[idx] if idx < len(contract_counts) else 1
        size_percent = percents[idx] if idx < len(percents) else None
        computed_percent = (contracts / fallback * 100) if fallback else None
        cleaned.append(
            {
                "id": order_id,
                "label": label or f"Target {idx + 1}",
                "ticks": ticks,
                "r_multiple": r_multiple,
                "price": price,
                "contracts": contracts,
                "size_percent": size_percent if size_percent is not None else computed_percent,
            }
        )

    total_contracts = sum(order["contracts"] for order in cleaned)
    return cleaned, total_contracts


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

        trigger_type = str(entry.get("trigger_type") or "").lower()
        action_type = str(entry.get("action_type") or "").lower()
        if trigger_type not in {"r_multiple", "target_hit"}:
            continue
        if action_type not in {"move_to_breakeven", "move_to_r"}:
            continue

        trigger_value = entry.get("trigger_value")
        if trigger_type == "r_multiple":
            trigger_value = _coerce_float(trigger_value, 0.0)
            if trigger_value is None or trigger_value <= 0:
                continue
        if trigger_type == "target_hit" and trigger_value is None:
            continue

        action_value = None
        if action_type == "move_to_r":
            action_value = _coerce_float(entry.get("action_value"), 0.0)
            if action_value is None or action_value <= 0:
                continue

        rules.append(
            {
                "id": entry.get("id"),
                "trigger_type": trigger_type,
                "trigger_value": trigger_value,
                "action_type": action_type,
                "action_value": action_value if action_type == "move_to_r" else None,
            }
        )

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
        if source.get("target_index") is not None:
            config["target_index"] = max(_coerce_int(source.get("target_index"), 0) or 0, 0)
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
) -> Dict[str, Any]:
    """Return a fully-populated ATM template merged with defaults."""

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

    if payload.get("atr_r_multiple") is not None:
        result["atr_r_multiple"] = float(payload.get("atr_r_multiple") or result.get("atr_r_multiple") or 1.0)

    risk_mode = str(payload.get("risk_unit_mode") or payload.get("rMode") or result.get("risk_unit_mode") or "atr").lower()
    if risk_mode not in {"atr", "ticks"}:
        risk_mode = "atr"
    result["risk_unit_mode"] = risk_mode

    ticks_stop = _coerce_int(
        payload.get("ticks_stop")
        or payload.get("rRiskTicks")
        or payload.get("risk_ticks"),
        result.get("ticks_stop"),
    )
    if ticks_stop is not None:
        result["ticks_stop"] = max(ticks_stop, 1)

    if payload.get("global_risk_multiplier") is not None:
        result["global_risk_multiplier"] = _coerce_float(
            payload.get("global_risk_multiplier"), result.get("global_risk_multiplier")
        )

    if payload.get("contracts") is not None:
        result["contracts"] = max(_coerce_int(payload.get("contracts"), result["contracts"]) or 1, 1)

    if payload.get("base_risk_per_trade") is not None:
        result["base_risk_per_trade"] = _coerce_float(
            payload.get("base_risk_per_trade"), result.get("base_risk_per_trade")
        )

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
        result["stop_r_multiple"] = float(stop_r_multiple)

    stop_price = _coerce_float(payload.get("stop_price"))
    if stop_price is not None:
        result["stop_price"] = float(stop_price)

    breakeven_config = _normalise_breakeven(payload, result.get("breakeven", {}))
    stop_adjustments = list(_normalise_stop_adjustments(payload))
    if stop_adjustments:
        result["stop_adjustments"] = stop_adjustments
    elif breakeven_config.get("enabled"):
        trigger_type = "target_hit" if breakeven_config.get("target_index") is not None else "r_multiple"
        trigger_value = (
            breakeven_config.get("target_index")
            if trigger_type == "target_hit"
            else breakeven_config.get("r_multiple")
        )
        result["stop_adjustments"] = [
            {
                "id": "sa-1",
                "trigger_type": trigger_type,
                "trigger_value": trigger_value if trigger_value is not None else 0.0,
                "action_type": "move_to_breakeven",
                "action_value": None,
            }
        ]
    elif not isinstance(result.get("stop_adjustments"), list):
        result["stop_adjustments"] = []

    result["breakeven"] = breakeven_config
    result["trailing"] = _normalise_trailing(payload, result.get("trailing", {}))

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
    "DEFAULT_ATM_TEMPLATE",
    "normalise_template",
    "merge_templates",
    "template_metrics",
]

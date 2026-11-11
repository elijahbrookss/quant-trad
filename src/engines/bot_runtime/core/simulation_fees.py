"""Explicit run-scoped fees for simulated historical backtests only."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import fields
from math import isfinite
from typing import Any, Mapping

from .fees import FeeSchedule


def normalize_simulation_fee_schedules(raw: Any, *, run_type: str,
        execution_behavior: str, economic_claim_intent: str) -> dict[str, dict[str, Any]]:
    if raw is None:
        return {}
    if str(run_type).lower() != "backtest" or str(execution_behavior).lower() != "simulated":
        raise ValueError("simulation_fee_schedules require a simulated backtest")
    if economic_claim_intent not in {"exploration", "economic"}:
        raise ValueError("simulation_fee_schedules cannot support selection or promotion")
    if not isinstance(raw, Mapping) or not raw or len(raw) > 128:
        raise ValueError("simulation_fee_schedules require 1..128 instrument schedules")
    allowed = {field.name for field in fields(FeeSchedule)}
    result = {}
    for identifier, value in sorted(raw.items(), key=lambda item: str(item[0])):
        if not isinstance(identifier, str) or not identifier.strip() or len(identifier) > 128:
            raise ValueError("simulation_fee_schedules require explicit instrument IDs")
        if identifier != identifier.strip() or not isinstance(value, Mapping) or set(value) - allowed:
            raise ValueError("simulation_fee_schedule invalid instrument or schedule fields")
        if not str(value.get("source") or "").startswith("simulation:"):
            raise ValueError("simulation_fee_schedule source must start with simulation:")
        if not isinstance(value.get("version"), str) or not value["version"].strip():
            raise ValueError("simulation_fee_schedule requires an explicit version")
        if value.get("configured") is not True or value.get("verified_zero") is not False:
            raise ValueError("simulation_fee_schedule must be configured and not verified zero")
        for key in ("maker_rate", "taker_rate"):
            rate = value.get(key)
            if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not 0 <= rate < 1 or not isfinite(rate):
                raise ValueError("simulation_fee_schedule requires finite numeric fee rates")
        schedule = FeeSchedule.from_dict(value)
        if not 0 <= schedule.maker_rate < 1 or not 0 < schedule.taker_rate < 1:
            raise ValueError("simulation_fee_schedule requires decimal maker in [0,1) and taker in (0,1)")
        if schedule.calculation_basis != "quote_notional":
            raise ValueError("simulation_fee_schedule requires quote_notional")
        result[identifier] = schedule.to_dict()
    return result


def apply_simulation_fee_schedule(instrument: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    raw = config.get("simulation_fee_schedules")
    if raw is None:
        return dict(instrument)
    schedules = normalize_simulation_fee_schedules(raw,
        run_type=str(config.get("run_type") or "backtest"),
        execution_behavior=str(config.get("execution_behavior") or "simulated"),
        economic_claim_intent=str(config.get("economic_claim_intent") or ""))
    identifier = str(instrument.get("id") or "")
    if identifier not in schedules:
        raise ValueError("simulation_fee_schedule missing for instrument " + identifier)
    schedule = schedules[identifier]
    # Copy only this run's execution material. Never update the instrument repository.
    result = deepcopy(dict(instrument))
    result.update(fee_schedule=schedule, maker_fee_rate=schedule["maker_rate"],
        taker_fee_rate=schedule["taker_rate"], fee_source=schedule["source"],
        fee_schedule_version=schedule["version"], fee_zero_verified=False)
    return result

"""Storage wrappers for strategy service persistence concerns."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

from .. import storage


def load_strategies() -> Iterable[Dict[str, Any]]:
    return storage.load_strategies()


def upsert_strategy(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return storage.upsert_strategy(payload)


def delete_strategy(strategy_id: str) -> None:
    storage.delete_strategy(strategy_id)


def upsert_strategy_indicator(strategy_id: str, indicator_id: str, snapshot: Mapping[str, Any]) -> None:
    storage.upsert_strategy_indicator(
        strategy_id=strategy_id,
        indicator_id=indicator_id,
        snapshot=snapshot,
    )


def delete_strategy_indicator(strategy_id: str, indicator_id: str) -> None:
    storage.delete_strategy_indicator(strategy_id, indicator_id)


def upsert_strategy_instrument(strategy_id: str, instrument_id: str, snapshot: Mapping[str, Any]) -> None:
    storage.upsert_strategy_instrument(
        strategy_id=strategy_id,
        instrument_id=instrument_id,
        snapshot=snapshot,
    )


def delete_strategy_instrument(strategy_id: str, instrument_id: str) -> None:
    storage.delete_strategy_instrument(strategy_id, instrument_id)


def list_strategy_instrument_symbols(strategy_id: str) -> list[str]:
    return storage.list_strategy_instrument_symbols(strategy_id)


def upsert_strategy_rule(payload: Mapping[str, Any]) -> None:
    storage.upsert_strategy_rule(payload)


def delete_strategy_rule(rule_id: str) -> None:
    storage.delete_strategy_rule(rule_id)


def get_atm_template(template_id: str) -> Optional[Dict[str, Any]]:
    return storage.get_atm_template(template_id)


def list_atm_templates() -> list[Dict[str, Any]]:
    return storage.load_atm_templates()


def upsert_atm_template(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return storage.upsert_atm_template(payload)


def list_symbol_presets() -> list[Dict[str, Any]]:
    return storage.list_symbol_presets()


def upsert_symbol_preset(payload: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    return storage.upsert_symbol_preset(payload)


def delete_symbol_preset(preset_id: str) -> None:
    storage.delete_symbol_preset(preset_id)


__all__ = [
    "delete_strategy",
    "delete_strategy_indicator",
    "delete_strategy_rule",
    "delete_symbol_preset",
    "get_atm_template",
    "list_atm_templates",
    "list_strategies",
    "list_symbol_presets",
    "upsert_atm_template",
    "upsert_strategy",
    "upsert_strategy_indicator",
    "upsert_strategy_rule",
    "upsert_symbol_preset",
    "upsert_strategy_instrument",
    "delete_strategy_instrument",
    "list_strategy_instrument_symbols",
]

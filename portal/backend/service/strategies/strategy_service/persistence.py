"""Storage wrappers for strategy service persistence concerns."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

from ...storage.repos import atm as atm_repo
from ...storage.repos import instruments as instrument_repo
from ...storage.repos import presets as preset_repo
from ...storage.repos import strategies as strategy_repo


def load_strategies() -> Iterable[Dict[str, Any]]:
    return strategy_repo.load_strategies()


def upsert_strategy(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return strategy_repo.upsert_strategy(payload)


def delete_strategy(strategy_id: str) -> None:
    strategy_repo.delete_strategy(strategy_id)


def upsert_strategy_indicator(strategy_id: str, indicator_id: str) -> None:
    strategy_repo.upsert_strategy_indicator(
        strategy_id=strategy_id,
        indicator_id=indicator_id,
    )


def delete_strategy_indicator(strategy_id: str, indicator_id: str) -> None:
    strategy_repo.delete_strategy_indicator(strategy_id, indicator_id)


def upsert_strategy_instrument(strategy_id: str, instrument_id: str, snapshot: Mapping[str, Any]) -> None:
    strategy_repo.upsert_strategy_instrument(
        strategy_id=strategy_id,
        instrument_id=instrument_id,
        snapshot=snapshot,
    )


def delete_strategy_instrument(strategy_id: str, instrument_id: str) -> None:
    strategy_repo.delete_strategy_instrument(strategy_id, instrument_id)


def list_strategy_instrument_symbols(strategy_id: str) -> list[str]:
    return instrument_repo.list_strategy_instrument_symbols(strategy_id)


def list_strategy_instrument_links(strategy_id: str) -> list[Dict[str, Any]]:
    return instrument_repo.list_strategy_instrument_links(strategy_id)


def delete_orphan_strategy_instrument_links(strategy_id: str) -> int:
    return strategy_repo.delete_orphan_strategy_instrument_links(strategy_id)


def upsert_strategy_rule(payload: Mapping[str, Any]) -> None:
    strategy_repo.upsert_strategy_rule(payload)


def delete_strategy_rule(rule_id: str) -> None:
    strategy_repo.delete_strategy_rule(rule_id)


def list_strategy_variants(strategy_id: str) -> list[Dict[str, Any]]:
    return strategy_repo.list_strategy_variants(strategy_id)


def get_strategy_variant(variant_id: str) -> Optional[Dict[str, Any]]:
    return strategy_repo.get_strategy_variant(variant_id)


def ensure_default_strategy_variant(strategy_id: str) -> Dict[str, Any]:
    return strategy_repo.ensure_default_strategy_variant(strategy_id)


def upsert_strategy_variant(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return strategy_repo.upsert_strategy_variant(dict(payload))


def delete_strategy_variant(variant_id: str) -> None:
    strategy_repo.delete_strategy_variant(variant_id)


def get_atm_template(template_id: str) -> Optional[Dict[str, Any]]:
    return atm_repo.get_atm_template(template_id)


def list_atm_templates() -> list[Dict[str, Any]]:
    return atm_repo.load_atm_templates()


def upsert_atm_template(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return atm_repo.upsert_atm_template(payload)


def list_symbol_presets() -> list[Dict[str, Any]]:
    return preset_repo.list_symbol_presets()


def upsert_symbol_preset(payload: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    return preset_repo.upsert_symbol_preset(payload)


def delete_symbol_preset(preset_id: str) -> None:
    preset_repo.delete_symbol_preset(preset_id)


__all__ = [
    "delete_strategy",
    "delete_strategy_indicator",
    "delete_strategy_rule",
    "delete_strategy_variant",
    "delete_symbol_preset",
    "ensure_default_strategy_variant",
    "get_atm_template",
    "get_strategy_variant",
    "list_atm_templates",
    "list_strategies",
    "list_strategy_variants",
    "list_symbol_presets",
    "upsert_atm_template",
    "upsert_strategy",
    "upsert_strategy_indicator",
    "upsert_strategy_rule",
    "upsert_strategy_variant",
    "upsert_symbol_preset",
    "upsert_strategy_instrument",
    "delete_strategy_instrument",
    "list_strategy_instrument_symbols",
    "list_strategy_instrument_links",
    "delete_orphan_strategy_instrument_links",
]

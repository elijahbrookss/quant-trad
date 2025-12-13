from __future__ import annotations

"""Metadata helpers for market profile signal rules."""

from typing import Any, Iterable, Mapping, MutableMapping, Sequence, Set


def _expand_rule_identifier(identifier: str) -> Set[str]:
    variants = {identifier}
    if identifier.endswith("_rule"):
        variants.add(identifier[: -len("_rule")])
    else:
        variants.add(f"{identifier}_rule")
    return variants


def _gather_aliases(target: MutableMapping[str, Any], key: str, aliases: Set[str]) -> None:
    value = target.get(key)
    if isinstance(value, str):
        value = value.strip()
        if value:
            aliases.add(value)
    elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        for item in value:
            if item is None:
                continue
            item_str = str(item).strip()
            if item_str:
                aliases.add(item_str)


def ensure_market_profile_rule_metadata(
    meta: MutableMapping[str, Any],
    *,
    rule_id: str,
    pattern_id: str,
    aliases: Sequence[str] | None = None,
) -> MutableMapping[str, Any]:
    """Ensure rule identifiers and aliases are present on rule metadata."""

    meta.setdefault("rule_id", rule_id)
    meta.setdefault("pattern_id", pattern_id)

    alias_set: Set[str] = set()
    for key in ("aliases", "rule_aliases", "pattern_aliases", "signal_aliases"):
        _gather_aliases(meta, key, alias_set)

    alias_set.update(_expand_rule_identifier(rule_id))
    alias_set.update(_expand_rule_identifier(pattern_id))
    if aliases:
        for alias in aliases:
            alias_str = str(alias).strip()
            if alias_str:
                alias_set.update(_expand_rule_identifier(alias_str))

    meta["aliases"] = sorted(alias_set)

    nested = meta.get("metadata")
    if isinstance(nested, MutableMapping):
        nested.setdefault("rule_id", meta["rule_id"])
        nested.setdefault("pattern_id", meta["pattern_id"])
        nested_aliases: Set[str] = set(alias_set)
        for key in ("aliases", "rule_aliases", "pattern_aliases", "signal_aliases"):
            _gather_aliases(nested, key, nested_aliases)
        nested["aliases"] = sorted(nested_aliases)

    return meta


__all__ = ["ensure_market_profile_rule_metadata"]

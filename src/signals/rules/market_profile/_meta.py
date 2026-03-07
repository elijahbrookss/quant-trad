"""Market-profile signal metadata helpers.

These helpers normalize rule identifiers onto both the top-level signal payload and
its nested ``metadata`` map so downstream filter paths can resolve rule aliases
consistently.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def _normalize_aliases(aliases: Iterable[Any] | None) -> list[str]:
    if aliases is None:
        return []

    normalized: list[str] = []
    seen: set[str] = set()

    for alias in aliases:
        text = str(alias or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)

    return normalized


def ensure_market_profile_rule_metadata(
    signal: Mapping[str, Any],
    *,
    rule_id: str,
    pattern_id: str,
    aliases: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Return a signal payload with normalized market-profile rule identifiers.

    Args:
        signal: Base signal payload (must include ``type``, ``symbol``, ``time``).
        rule_id: Canonical rule identifier for rule filtering.
        pattern_id: Canonical pattern identifier.
        aliases: Optional aliases accepted by strategy filters.
    """

    normalized_rule_id = str(rule_id or "").strip().lower()
    normalized_pattern_id = str(pattern_id or "").strip().lower()
    if not normalized_rule_id:
        raise ValueError("rule_id is required")
    if not normalized_pattern_id:
        raise ValueError("pattern_id is required")

    payload = dict(signal)
    metadata = dict(payload.get("metadata") or {})

    merged_aliases = _normalize_aliases(
        [
            *(_normalize_aliases(aliases)),
            *(_normalize_aliases(metadata.get("aliases") if isinstance(metadata.get("aliases"), Iterable) and not isinstance(metadata.get("aliases"), (str, bytes, Mapping)) else None)),
            *(_normalize_aliases(payload.get("aliases") if isinstance(payload.get("aliases"), Iterable) and not isinstance(payload.get("aliases"), (str, bytes, Mapping)) else None)),
        ]
    )

    payload["rule_id"] = normalized_rule_id
    payload["pattern_id"] = normalized_pattern_id
    if merged_aliases:
        payload["aliases"] = merged_aliases

    metadata["rule_id"] = normalized_rule_id
    metadata["pattern_id"] = normalized_pattern_id
    metadata["rule_aliases"] = merged_aliases
    if merged_aliases:
        metadata["aliases"] = merged_aliases

    payload["metadata"] = metadata
    return payload


__all__ = ["ensure_market_profile_rule_metadata"]

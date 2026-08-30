"""Strategy write-boundary guards for canonical market identity."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


FORBIDDEN_STRATEGY_MARKET_IDENTITY_KEYS = frozenset({"provider_id", "venue_id"})


def reject_forbidden_strategy_market_identity(
    value: Any,
    *,
    path: str = "strategy_write",
) -> None:
    """Reject provider/venue identifiers anywhere in a Strategy write payload.

    The guard is intentionally pure and accepts the JSON-shaped mappings and
    sequences used at the Strategy application boundary. Legacy read and
    bootstrap normalization remains separate from this write-only check.
    """

    forbidden_paths: list[str] = []

    def visit(current: Any, current_path: str) -> None:
        if isinstance(current, Mapping):
            for raw_key, nested in current.items():
                key = str(raw_key)
                nested_path = f"{current_path}.{key}"
                if key.casefold() in FORBIDDEN_STRATEGY_MARKET_IDENTITY_KEYS:
                    forbidden_paths.append(nested_path)
                visit(nested, nested_path)
            return
        if isinstance(current, (list, tuple)):
            for index, nested in enumerate(current):
                visit(nested, f"{current_path}[{index}]")

    visit(value, path)
    if forbidden_paths:
        fields = ", ".join(sorted(forbidden_paths))
        raise ValueError(
            "Strategy writes reject provider_id and venue_id; "
            f"forbidden fields: {fields}"
        )

"""Shared startup validation helpers for backend orchestration and container bootstrap."""

from __future__ import annotations

from typing import Any, Dict, Mapping


MIN_STARTING_WALLET = 10.0


def validate_wallet_config(wallet_config: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Validate and normalize wallet config for startup-time use."""

    if not isinstance(wallet_config, Mapping):
        raise ValueError("wallet_config is required and must be an object")
    balances = wallet_config.get("balances")
    if not isinstance(balances, Mapping) or not balances:
        raise ValueError("wallet_config.balances is required and cannot be empty")

    normalized: Dict[str, float] = {}
    total = 0.0
    for currency, amount in balances.items():
        code = str(currency).strip().upper()
        if not code:
            raise ValueError("wallet_config.balances contains an empty currency key")
        try:
            numeric = float(amount)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"wallet_config.balances[{code}] must be numeric") from exc
        if numeric < 0:
            raise ValueError(f"wallet_config.balances[{code}] must be non-negative")
        normalized[code] = numeric
        total += numeric

    if total < MIN_STARTING_WALLET:
        raise ValueError(f"wallet_config balances must sum to at least {MIN_STARTING_WALLET}")

    return {"balances": normalized}


__all__ = ["MIN_STARTING_WALLET", "validate_wallet_config"]

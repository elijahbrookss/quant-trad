"""In-memory wallet ledger and projection utilities for spot backtests."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


@dataclass(frozen=True)
class WalletEvent:
    """Append-only wallet event for ledger auditing."""

    event_id: str
    event_type: str
    timestamp: str
    payload: Dict[str, Any]


@dataclass
class WalletState:
    """Projected wallet state derived from ledger events."""

    balances: Dict[str, float] = field(default_factory=dict)


class WalletLedger:
    """Append-only wallet ledger with deterministic projections."""

    def __init__(self) -> None:
        self._events: List[WalletEvent] = []

    def events(self) -> List[WalletEvent]:
        return list(self._events)

    def append(self, event_type: str, payload: Mapping[str, Any]) -> WalletEvent:
        event = WalletEvent(
            event_id=str(uuid.uuid4()),
            event_type=str(event_type),
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            payload=dict(payload or {}),
        )
        self._events.append(event)
        return event

    def deposit(self, balances: Mapping[str, float]) -> WalletEvent:
        normalized = {str(k).upper(): float(v) for k, v in (balances or {}).items()}
        return self.append("DEPOSIT", {"balances": normalized})

    def trade_fill(
        self,
        *,
        side: str,
        base_currency: str,
        quote_currency: str,
        qty: float,
        price: float,
        fee: float,
        notional: float,
        symbol: Optional[str] = None,
        trade_id: Optional[str] = None,
        leg_id: Optional[str] = None,
    ) -> WalletEvent:
        return self.append(
            "TRADE_FILL",
            {
                "side": str(side),
                "base_currency": str(base_currency).upper(),
                "quote_currency": str(quote_currency).upper(),
                "qty": float(qty),
                "price": float(price),
                "fee": float(fee),
                "notional": float(notional),
                "symbol": symbol,
                "trade_id": trade_id,
                "leg_id": leg_id,
            },
        )

    def rejected(self, reason: str, payload: Mapping[str, Any]) -> WalletEvent:
        return self.append("REJECTED", {"reason": reason, **dict(payload or {})})

    def project(self) -> WalletState:
        return project_wallet(self._events)


def project_wallet(events: Iterable[WalletEvent]) -> WalletState:
    balances: Dict[str, float] = {}
    for event in events:
        event_type = event.event_type
        payload = event.payload or {}
        if event_type == "DEPOSIT":
            for currency, amount in (payload.get("balances") or {}).items():
                code = str(currency).upper()
                balances[code] = balances.get(code, 0.0) + float(amount)
        elif event_type == "TRADE_FILL":
            side = str(payload.get("side") or "").lower()
            base = str(payload.get("base_currency") or "").upper()
            quote = str(payload.get("quote_currency") or "").upper()
            qty = float(payload.get("qty") or 0.0)
            notional = float(payload.get("notional") or 0.0)
            fee = float(payload.get("fee") or 0.0)
            if side in {"buy", "long"}:
                balances[base] = balances.get(base, 0.0) + qty
                balances[quote] = balances.get(quote, 0.0) - notional - fee
            elif side in {"sell", "short"}:
                balances[base] = balances.get(base, 0.0) - qty
                balances[quote] = balances.get(quote, 0.0) + notional - fee
        elif event_type == "REJECTED":
            continue
    return WalletState(balances=balances)


def wallet_can_apply(
    *,
    state: WalletState,
    side: str,
    base_currency: str,
    quote_currency: str,
    qty: float,
    notional: float,
    fee: float,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    base = str(base_currency).upper()
    quote = str(quote_currency).upper()
    balances = state.balances
    available_base = balances.get(base, 0.0)
    available_quote = balances.get(quote, 0.0)
    if side in {"buy", "long"}:
        required = float(notional) + float(fee)
        if available_quote + 1e-12 < required:
            return (
                False,
                "WALLET_INSUFFICIENT_CASH",
                {
                    "available": available_quote,
                    "required": required,
                    "currency": quote,
                    "notional": notional,
                    "fee": fee,
                    "qty": qty,
                },
            )
        return True, None, {}
    required_qty = float(qty)
    if available_base + 1e-12 < required_qty:
        return (
            False,
            "WALLET_INSUFFICIENT_QTY",
            {
                "available": available_base,
                "required": required_qty,
                "currency": base,
                "qty": qty,
            },
        )
    return True, None, {}


__all__ = [
    "WalletEvent",
    "WalletLedger",
    "WalletState",
    "project_wallet",
    "wallet_can_apply",
]

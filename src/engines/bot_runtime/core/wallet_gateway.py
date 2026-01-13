"""Wallet gateway interfaces for bot runtime."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Protocol, Tuple

from .wallet import WalletLedger, WalletState, wallet_can_apply


class WalletGateway(Protocol):
    """Abstract wallet layer used by the risk engine."""

    def can_apply(
        self,
        *,
        side: str,
        base_currency: str,
        quote_currency: str,
        qty: float,
        notional: float,
        fee: float,
        short_requires_borrow: bool,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        ...

    def apply_fill(
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
    ) -> None:
        ...

    def reject(self, reason: str, payload: Mapping[str, Any]) -> None:
        ...

    def project(self) -> WalletState:
        ...


class LedgerWalletGateway:
    """Wallet gateway backed by the in-memory wallet ledger."""

    def __init__(self, ledger: WalletLedger) -> None:
        self._ledger = ledger

    @property
    def ledger(self) -> WalletLedger:
        return self._ledger

    def can_apply(
        self,
        *,
        side: str,
        base_currency: str,
        quote_currency: str,
        qty: float,
        notional: float,
        fee: float,
        short_requires_borrow: bool,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        state = self._ledger.project()
        return wallet_can_apply(
            state=state,
            side=side,
            base_currency=base_currency,
            quote_currency=quote_currency,
            qty=qty,
            notional=notional,
            fee=fee,
            short_requires_borrow=short_requires_borrow,
        )

    def apply_fill(
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
    ) -> None:
        self._ledger.trade_fill(
            side=side,
            base_currency=base_currency,
            quote_currency=quote_currency,
            qty=qty,
            price=price,
            fee=fee,
            notional=notional,
            symbol=symbol,
            trade_id=trade_id,
            leg_id=leg_id,
        )

    def reject(self, reason: str, payload: Mapping[str, Any]) -> None:
        self._ledger.rejected(reason, payload)

    def project(self) -> WalletState:
        return self._ledger.project()


__all__ = ["WalletGateway", "LedgerWalletGateway"]

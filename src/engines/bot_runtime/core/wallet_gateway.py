"""Wallet gateway interfaces for bot runtime."""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Protocol, Tuple

from .margin import MarginSessionType
from .wallet import WalletLedger, WalletState, wallet_can_apply

logger = logging.getLogger(__name__)


class WalletGateway(Protocol):
    """Abstract wallet layer used by the risk engine."""

    def can_apply(
        self,
        *,
        side: str,
        base_currency: str,
        quote_currency: str,
        qty: float,
        qty_raw: Optional[float] = None,
        qty_final: Optional[float] = None,
        notional: float,
        fee: float,
        short_requires_borrow: bool,
        instrument: Optional[Mapping[str, Any]] = None,
        margin_session: Optional[MarginSessionType] = None,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        ...

    def apply_fill(
        self,
        *,
        event_type: str = "TRADE_FILL",
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
        position_direction: Optional[str] = None,
        accounting_mode: Optional[str] = None,
        realized_pnl: Optional[float] = None,
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
        qty_raw: Optional[float] = None,
        qty_final: Optional[float] = None,
        notional: float,
        fee: float,
        short_requires_borrow: bool,
        instrument: Optional[Mapping[str, Any]] = None,
        margin_session: Optional[MarginSessionType] = None,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        state = self._ledger.project()
        return wallet_can_apply(
            state=state,
            side=side,
            base_currency=base_currency,
            quote_currency=quote_currency,
            qty=qty,
            qty_raw=qty_raw,
            qty_final=qty_final,
            notional=notional,
            fee=fee,
            short_requires_borrow=short_requires_borrow,
            instrument=instrument,
            margin_session=margin_session,
        )

    def apply_fill(
        self,
        *,
        event_type: str = "TRADE_FILL",
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
        position_direction: Optional[str] = None,
        accounting_mode: Optional[str] = None,
        realized_pnl: Optional[float] = None,
    ) -> None:
        if event_type in {"ENTRY_FILL", "EXIT_FILL"} and accounting_mode is None:
            logger.warning(
                "wallet_fill_missing_accounting_mode | event_type=%s | symbol=%s | trade_id=%s | leg_id=%s | side=%s | position_direction=%s",
                event_type,
                symbol,
                trade_id,
                leg_id,
                side,
                position_direction,
            )
        self._ledger.trade_fill(
            event_type=event_type,
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
            position_direction=position_direction,
            accounting_mode=accounting_mode,
            realized_pnl=realized_pnl,
        )

    def reject(
        self, reason: str, payload: Mapping[str, Any], trade_id: Optional[str] = None, leg_id: Optional[str] = None
    ) -> None:
        self._ledger.rejected(reason, payload, trade_id=trade_id, leg_id=leg_id)

    def project(self) -> WalletState:
        return self._ledger.project()


__all__ = ["WalletGateway", "LedgerWalletGateway"]

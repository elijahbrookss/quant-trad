"""Event schemas for bot runtime position lifecycle."""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional, TypedDict


class ExitSettlementPayload(TypedDict, total=False):
    event_type: str
    exit_kind: str
    side: str
    base_currency: str
    quote_currency: str
    qty: float
    price: float
    fee: float
    notional: float
    fee_rate: float
    fee_type: str
    fee_source: str
    fee_version: Optional[str]
    trade_id: str
    leg_id: str
    position_direction: str
    accounting_mode: Optional[str]
    realized_pnl: float
    allow_short_borrow: bool
    instrument: Dict[str, Any]


class TargetFillEvent(TypedDict, total=False):
    type: Literal["target"]
    leg: str
    leg_id: str
    trade_id: str
    price: float
    time: str
    pnl: float
    currency: str
    contracts: float
    ticks: float
    direction: str
    notional: float
    fee_paid: float
    fee_rate: float
    fee_type: str
    fee_source: str
    fee_version: Optional[str]
    settlement: ExitSettlementPayload


class StopFillEvent(TypedDict, total=False):
    type: Literal["stop"]
    trade_id: str
    price: float
    time: str
    currency: str
    leg: str
    leg_id: str
    contracts: float
    pnl: float
    ticks: float
    direction: str
    notional: float
    fee_paid: float
    fee_rate: float
    fee_type: str
    fee_source: str
    fee_version: Optional[str]
    settlement: ExitSettlementPayload


class TerminalFillEvent(TypedDict, total=False):
    type: Literal["backtest_end", "terminal_liquidation"]
    trade_id: str
    price: float
    time: str
    currency: str
    leg: str
    leg_id: str
    contracts: float
    pnl: float
    ticks: float
    direction: str
    notional: float
    fee_paid: float
    fee_rate: float
    fee_type: str
    fee_source: str
    fee_version: Optional[str]
    reason_code: str
    close_reason: str
    settlement: ExitSettlementPayload


class CloseEvent(TypedDict, total=False):
    type: Literal["close"]
    trade_id: str
    time: str
    gross_pnl: float
    fees_paid: float
    net_pnl: float
    currency: str
    contracts: float
    direction: str
    metrics: Dict[str, object]
    reason_code: str
    close_reason: str
    exit_price: float


ExitEvent = TargetFillEvent | StopFillEvent | TerminalFillEvent | CloseEvent


__all__ = [
    "CloseEvent",
    "ExitEvent",
    "ExitSettlementPayload",
    "StopFillEvent",
    "TerminalFillEvent",
    "TargetFillEvent",
]

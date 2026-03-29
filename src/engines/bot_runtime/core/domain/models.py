"""Domain datamodels for bot runtime core."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..execution_intent import ExecutionIntent, LimitParams
from .time_utils import isoformat

if TYPE_CHECKING:
    from ..entry_execution import PendingEntry
    from .position import LadderPosition


@dataclass
class Candle:
    """Single OHLC datapoint used by the simulated bot."""

    time: datetime
    open: float
    high: float
    low: float
    close: float
    end: Optional[datetime] = None
    atr: Optional[float] = None
    volume: Optional[float] = None
    range: Optional[float] = None
    lookback_15: Optional[Dict[str, Optional[float]]] = None

    def serialize(self) -> Dict[str, Optional[float]]:
        payload = {
            "time": isoformat(self.time),
            "open": round(self.open, 4),
            "high": round(self.high, 4),
            "low": round(self.low, 4),
            "close": round(self.close, 4),
            "end": isoformat(self.end),
        }
        if self.range is not None:
            payload["range"] = round(self.range, 6)
        if self.atr is not None:
            payload["atr"] = round(self.atr, 6)
        if self.volume is not None:
            payload["volume"] = round(self.volume, 6)
        return payload

    def to_dict(self) -> Dict[str, Optional[float]]:
        return self.serialize()

    @property
    def start_time(self) -> datetime:
        return self.time

    @property
    def end_time(self) -> datetime:
        return self.end or self.time


@dataclass
class StrategySignal:
    """Queued strategy action derived from rule markers."""

    epoch: int
    direction: str
    decision_id: Optional[str] = None
    rule_id: Optional[str] = None
    intent: Optional[str] = None
    event_key: Optional[str] = None


@dataclass
class EntryValidation:
    """Validation result for entry sizing and intent construction."""

    ok: bool
    rejection_reason: Optional[str] = None
    rejection_detail: Optional[Dict[str, Any]] = None


@dataclass
class EntryRequest:
    """Prepared entry sizing and intent information for execution."""

    trade_id: Optional[str]
    order_intent_id: Optional[str]
    direction: str
    requested_qty: float
    qty_raw: float
    r_ticks: float
    r_value: Optional[float]
    atr_at_entry: Optional[float]
    r_multiple_at_entry: Optional[float]
    order_type: str
    limit_params: Optional[LimitParams]
    side: str
    requested_price: float
    intent: Optional[ExecutionIntent]
    validation: EntryValidation
    margin_info: Optional[Dict[str, Any]]
    was_margin_capped: bool


@dataclass
class CandleSnapshot:
    """Minimal candle context for execution fills."""

    time: datetime
    open: float
    high: float
    low: float
    close: float
    atr: Optional[float] = None
    lookback_15: Optional[Dict[str, Optional[float]]] = None

    def is_complete(self) -> bool:
        return all(
            value is not None
            for value in (
                self.time,
                self.open,
                self.high,
                self.low,
                self.close,
            )
        )


@dataclass
class EntryFill:
    """Normalized entry fill event for execution adapters."""

    order_intent_id: str
    trade_id: str
    candle: Optional[CandleSnapshot]
    filled_qty: float
    fill_price: float
    fee_paid: float
    liquidity_role: Optional[str]
    fill_time: Optional[str]
    raw: Optional[Dict[str, Any]] = None


@dataclass
class EntryFillResult:
    """Result of applying an entry fill against domain state."""

    status: str
    pending: Optional[PendingEntry]
    position: Optional[LadderPosition]
    events: List[Dict[str, Any]]
    settlement_payloads: List[Dict[str, Any]]
    rejection_reason: Optional[str] = None
    rejection_detail: Optional[Dict[str, Any]] = None


@dataclass
class DecisionLedgerEvent:
    """Represents a causal ledger event for BotLens explainability."""

    event_id: str
    event_ts: str
    event_type: str
    reason_code: str
    event_subtype: Optional[str] = None
    parent_event_id: Optional[str] = None
    trade_id: Optional[str] = None
    position_id: Optional[str] = None
    strategy_id: Optional[str] = None
    strategy_name: Optional[str] = None
    symbol: Optional[str] = None
    instrument_id: Optional[str] = None
    timeframe: Optional[str] = None
    side: Optional[str] = None
    qty: Optional[float] = None
    price: Optional[float] = None
    event_impact_pnl: Optional[float] = None
    trade_net_pnl: Optional[float] = None
    reason_detail: Optional[str] = None
    evidence_refs: Optional[List[Dict[str, Any]]] = None
    context: Optional[Dict[str, Any]] = None
    alternatives_rejected: Optional[List[Dict[str, Any]]] = None
    created_at: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.reason_code:
            raise ValueError("reason_code is required for DecisionLedgerEvent")

    def serialize(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the ledger event."""
        payload: Dict[str, Any] = {
            "event_id": self.event_id,
            "event_ts": self.event_ts,
            "event_type": self.event_type,
            "reason_code": self.reason_code,
        }
        if self.event_subtype is not None:
            payload["event_subtype"] = self.event_subtype
        if self.parent_event_id is not None:
            payload["parent_event_id"] = self.parent_event_id
        if self.trade_id is not None:
            payload["trade_id"] = self.trade_id
        if self.position_id is not None:
            payload["position_id"] = self.position_id
        if self.strategy_id is not None:
            payload["strategy_id"] = self.strategy_id
        if self.strategy_name is not None:
            payload["strategy_name"] = self.strategy_name
        if self.symbol is not None:
            payload["symbol"] = self.symbol
        if self.instrument_id is not None:
            payload["instrument_id"] = self.instrument_id
        if self.timeframe is not None:
            payload["timeframe"] = self.timeframe
        if self.side is not None:
            payload["side"] = self.side
        if self.qty is not None:
            payload["qty"] = round(float(self.qty), 6)
        if self.price is not None:
            payload["price"] = round(float(self.price), 4)
        if self.event_impact_pnl is not None:
            payload["event_impact_pnl"] = round(float(self.event_impact_pnl), 4)
        if self.trade_net_pnl is not None:
            payload["trade_net_pnl"] = round(float(self.trade_net_pnl), 4)
        if self.reason_detail is not None:
            payload["reason_detail"] = self.reason_detail
        if self.evidence_refs is not None:
            payload["evidence_refs"] = self.evidence_refs
        if self.context is not None:
            payload["context"] = self.context
        if self.alternatives_rejected is not None:
            payload["alternatives_rejected"] = self.alternatives_rejected
        if self.created_at is not None:
            payload["created_at"] = self.created_at
        return payload


@dataclass
class Leg:
    """Take-profit leg metadata."""

    name: str
    ticks: int
    target_price: float
    status: str = "open"
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    exit_created_at: Optional[str] = None
    contracts: float = 1.0
    pnl: float = 0.0
    leg_id: Optional[str] = None

    def serialize(self) -> Dict[str, Optional[float]]:
        return {
            "name": self.name,
            "ticks": self.ticks,
            "target_price": round(self.target_price, 4),
            "status": self.status,
            "exit_price": None if self.exit_price is None else round(self.exit_price, 4),
            "exit_time": self.exit_time,
            "exit_created_at": self.exit_created_at,
            "contracts": self.contracts,
            "pnl": round(self.pnl, 4),
            "id": self.leg_id,
        }


__all__ = [
    "Candle",
    "CandleSnapshot",
    "DecisionLedgerEvent",
    "EntryFill",
    "EntryFillResult",
    "EntryRequest",
    "EntryValidation",
    "Leg",
    "StrategySignal",
]

"""Domain models and ladder risk math for bot runtime."""

from __future__ import annotations

import logging
import math
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

from .. import risk_math
from ..atm import merge_templates
from .execution import FillRejection, FillResult, SpotExecutionConstraints, SpotExecutionModel
from .wallet import WalletLedger, wallet_can_apply

logger = logging.getLogger(__name__)

_TIMEFRAME_MULTIPLIERS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "w": 60 * 60 * 24 * 7,
}


def isoformat(value: Optional[datetime]) -> Optional[str]:
    """Return a UTC ISO8601 string with Z suffix for *value*."""

    if value is None:
        return None
    target = value
    if target.tzinfo is None:
        return target.replace(tzinfo=None).isoformat() + "Z"
    return target.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    """Attempt to cast *value* to float and fall back to *default* on failure."""

    try:
        if value is None:
            return default
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric


def coalesce_numeric(*values: Optional[float], default: float = 0.0, allow_zero: bool = False) -> float:
    """Return the first non-None, non-zero value, or default.

    Args:
        *values: Values to check in order of precedence
        default: Value to return if all inputs are None or zero
        allow_zero: If True, treat 0 as a valid value (don't skip it)

    Returns:
        First valid value or default
    """
    for value in values:
        if value is None:
            continue
        if not allow_zero and value == 0:
            continue
        return float(value)
    return default


def timeframe_to_seconds(label: Optional[str]) -> Optional[int]:
    """Convert timeframe strings like '15m' or '4h' into seconds."""

    if not label:
        return None
    value = str(label).strip().lower()
    if not value:
        return None
    match = re.fullmatch(r"(\d+)([a-z]+)", value)
    if not match:
        return None
    amount = int(match.group(1))
    suffix = match.group(2)
    key = suffix[0]
    multiplier = _TIMEFRAME_MULTIPLIERS.get(key)
    if not multiplier:
        return None
    return amount * multiplier


def timeframe_duration(label: Optional[str]) -> Optional[timedelta]:
    seconds = timeframe_to_seconds(label)
    if not seconds:
        return None
    return timedelta(seconds=seconds)


def normalize_epoch(value: Any) -> Optional[int]:
    """Convert various timestamp formats to Unix epoch (seconds since 1970-01-01 UTC).

    Handles:
    - None or empty string -> None
    - int/float -> int (already epoch)
    - numeric string -> int
    - ISO 8601 string -> epoch via parsing

    Args:
        value: Timestamp in various formats

    Returns:
        Unix epoch timestamp in seconds, or None if invalid
    """
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    try:
        return int(float(text))
    except (TypeError, ValueError):
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1]
        parsed = datetime.fromisoformat(text)
        return int(parsed.timestamp())
    except ValueError:
        return None


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


@dataclass
class DecisionEvent:
    """Represents a strategy-level decision point for observability."""

    event_id: str
    event: str  # "signal_received", "signal_accepted", "signal_rejected", "trade_opened", etc.
    timestamp: str  # ISO8601 when event occurred
    bar_time: str  # ISO8601 of the bar that triggered this event
    strategy_id: str
    strategy_name: str
    symbol: str
    signal_type: str  # e.g., "breakout", "retest"
    signal_direction: Optional[str] = None  # "long" | "short"
    signal_price: Optional[float] = None
    rule_id: Optional[str] = None
    decision: Optional[str] = None  # "accepted" | "rejected"
    reason: Optional[str] = None  # Why rejected (if applicable)
    trade_id: Optional[str] = None  # Links to actual trade if opened
    conditions: Optional[List[Dict[str, Any]]] = None  # Rule condition results
    metadata: Optional[Dict[str, Any]] = None

    def serialize(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the decision event."""
        payload: Dict[str, Any] = {
            "id": self.event_id,
            "event": self.event,
            "timestamp": self.timestamp,
            "bar_time": self.bar_time,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "signal_type": self.signal_type,
        }
        if self.signal_direction is not None:
            payload["direction"] = self.signal_direction
        if self.signal_price is not None:
            payload["price"] = round(self.signal_price, 4)
        if self.rule_id is not None:
            payload["rule_id"] = self.rule_id
        if self.decision is not None:
            payload["decision"] = self.decision
        if self.reason is not None:
            payload["reason"] = self.reason
        if self.trade_id is not None:
            payload["trade_id"] = self.trade_id
        if self.conditions is not None:
            payload["conditions"] = self.conditions
        if self.metadata is not None:
            payload["metadata"] = self.metadata
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
            "contracts": self.contracts,
            "pnl": round(self.pnl, 4),
            "id": self.leg_id,
        }


@dataclass
class LadderPosition:
    """Track laddered take-profit and stop-loss behaviour for a trade."""

    entry_time: datetime
    entry_price: float
    direction: str
    stop_price: float
    tick_size: float
    instrument_type: Optional[str] = None
    execution_model: Optional[SpotExecutionModel] = None
    wallet_ledger: Optional[WalletLedger] = None
    base_currency: Optional[str] = None
    quote_currency_code: Optional[str] = None
    legs: List[Leg] = field(default_factory=list)
    breakeven_trigger_ticks: float = 20.0
    tick_value: float = 1.0
    contract_size: float = 1.0
    maker_fee_rate: float = 0.0
    taker_fee_rate: float = 0.0
    quote_currency: str = "USD"
    moved_to_breakeven: bool = False
    closed_at: Optional[datetime] = None
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    gross_pnl: float = 0.0
    fees_paid: float = 0.0
    net_pnl: float = 0.0
    atr_at_entry: Optional[float] = None
    r_multiple_at_entry: Optional[float] = None
    r_value: Optional[float] = None
    r_ticks: Optional[float] = None
    mae_ticks: float = 0.0
    mfe_ticks: float = 0.0
    bars_held: int = 0
    best_price: float = 0.0
    worst_price: float = 0.0
    trailing_activation_ticks: Optional[float] = None
    trailing_distance_ticks: Optional[float] = None
    trailing_active: bool = False
    trailing_atr_multiple: float = 0.0
    pre_entry_context: Optional[Dict[str, Optional[float]]] = None
    stop_adjustments: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.best_price = self.entry_price
        self.worst_price = self.entry_price

    def register_entry_fee(self) -> None:
        total_contracts = sum(max(leg.contracts, 0) for leg in self.legs) or 1
        self._apply_fee(self.entry_price, total_contracts)

    def apply_entry_fee(self, fee: float) -> None:
        if fee:
            self.fees_paid += fee
            self._update_net()

    def _apply_fee_amount(self, fee: float) -> None:
        if fee:
            self.fees_paid += fee
            self._update_net()

    def _execute_spot_fill(
        self, price: float, contracts: float, side: str
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        if not self.execution_model:
            return None, None
        return self.execution_model.fill_market(
            side=side,
            requested_qty=contracts,
            price=price,
            fee_rate=self.taker_fee_rate or 0.0,
            enforce_price_tick=False,
        )

    def _wallet_can_apply_fill(self, fill: FillResult, side: str) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        if not self.wallet_ledger:
            return True, None, {}
        state = self.wallet_ledger.project()
        return wallet_can_apply(
            state=state,
            side=side,
            base_currency=self.base_currency or "",
            quote_currency=self.quote_currency_code or "",
            qty=fill.filled_qty,
            notional=fill.notional,
            fee=fill.fee,
        )

    def _update_excursions(self, candle: Candle) -> None:
        favorable_price = candle.high if self.direction == "long" else candle.low
        adverse_price = candle.low if self.direction == "long" else candle.high
        self.best_price = max(self.best_price, favorable_price) if self.direction == "long" else min(self.best_price, favorable_price)
        self.worst_price = min(self.worst_price, adverse_price) if self.direction == "long" else max(self.worst_price, adverse_price)
        favorable_ticks = self._ticks_from_entry(favorable_price)
        adverse_ticks = self._ticks_from_entry(adverse_price)
        self.mfe_ticks = max(self.mfe_ticks, favorable_ticks)
        self.mae_ticks = min(self.mae_ticks, adverse_ticks)
        self.bars_held += 1

    def _apply_leg_fills(self, candle: Candle) -> List[Dict[str, str]]:
        """Check if candle price hits any target levels and process fills."""
        events: List[Dict[str, str]] = []
        ordered = sorted(self.legs, key=lambda leg: leg.ticks)

        for leg in ordered:
            if leg.status != "open":
                continue

            # Check if target was hit based on direction
            is_filled = (
                candle.high >= leg.target_price if self.direction == "long"
                else candle.low <= leg.target_price
            )

            if not is_filled:
                continue

            fill_result = None
            if str(self.instrument_type or "").lower() == "spot":
                side = "sell" if self.direction == "long" else "buy"
                fill_result, rejection = self._execute_spot_fill(
                    leg.target_price, leg.contracts, side=side
                )
                if rejection:
                    logger.warning(
                        "spot_exit_rejected | trade=%s | leg=%s | reason=%s",
                        self.trade_id,
                        leg.leg_id or leg.name,
                        rejection.reason,
                    )
                    events.append(
                        {
                            "type": "execution_rejected",
                            "leg": leg.name,
                            "leg_id": leg.leg_id,
                            "trade_id": self.trade_id,
                            "price": round(leg.target_price, 4),
                            "time": isoformat(candle.time),
                            "reason": rejection.reason,
                            "currency": self.quote_currency,
                            "contracts": leg.contracts,
                            "ticks": leg.ticks,
                            "direction": self.direction,
                        }
                    )
                    continue
                allowed, reason, payload = self._wallet_can_apply_fill(fill_result, side=side)
                if not allowed:
                    if self.wallet_ledger:
                        self.wallet_ledger.rejected(reason, payload)
                    logger.warning(
                        "wallet_exit_rejected | trade=%s | leg=%s | reason=%s",
                        self.trade_id,
                        leg.leg_id or leg.name,
                        reason,
                    )
                    events.append(
                        {
                            "type": "execution_rejected",
                            "leg": leg.name,
                            "leg_id": leg.leg_id,
                            "trade_id": self.trade_id,
                            "price": round(leg.target_price, 4),
                            "time": isoformat(candle.time),
                            "reason": reason,
                            "currency": self.quote_currency,
                            "contracts": leg.contracts,
                            "ticks": leg.ticks,
                            "direction": self.direction,
                        }
                    )
                    continue

            exit_price = fill_result.fill_price if fill_result else leg.target_price
            exit_qty = fill_result.filled_qty if fill_result else leg.contracts
            pnl = self._pnl_for_exit(exit_price, exit_qty)
            leg.status = "target"
            leg.exit_price = exit_price
            leg.exit_time = isoformat(candle.time)
            leg.contracts = exit_qty
            leg.pnl = pnl
            self._record_pnl(pnl)
            if fill_result:
                self._apply_fee_amount(fill_result.fee)
                if self.wallet_ledger:
                    self.wallet_ledger.trade_fill(
                        side=side,
                        base_currency=self.base_currency or "",
                        quote_currency=self.quote_currency_code or "",
                        qty=fill_result.filled_qty,
                        price=fill_result.fill_price,
                        fee=fill_result.fee,
                        notional=fill_result.notional,
                        symbol=None,
                        trade_id=self.trade_id,
                        leg_id=leg.leg_id,
                    )
            else:
                self._apply_fee(exit_price, exit_qty)

            events.append(
                {
                    "type": "target",
                    "leg": leg.name,
                    "leg_id": leg.leg_id,
                    "trade_id": self.trade_id,
                    "price": round(exit_price, 4),
                    "time": leg.exit_time,
                    "pnl": round(pnl, 4),
                    "currency": self.quote_currency,
                    "contracts": exit_qty,
                    "ticks": leg.ticks,
                    "direction": self.direction,
                }
            )

            # Move to breakeven if threshold reached
            if not self.moved_to_breakeven and leg.ticks >= self.breakeven_trigger_ticks:
                self.stop_price = self.entry_price
                self.moved_to_breakeven = True

        return events

    def _maybe_move_breakeven(self) -> None:
        if self.moved_to_breakeven or self.trailing_active:
            return
        if self.breakeven_trigger_ticks and self.mfe_ticks >= self.breakeven_trigger_ticks:
            self.stop_price = risk_math.clamp_stop(self.stop_price, self.entry_price, self.direction)
            self.moved_to_breakeven = True

    def _maybe_apply_stop_adjustments(self) -> None:
        """Apply one-time stop adjustments while trailing is inactive."""

        if self.trailing_active or not self.stop_adjustments:
            return

        triggered_targets = {leg.leg_id for leg in self.legs if leg.status == "target" and leg.leg_id}

        for rule in self.stop_adjustments:
            if rule.get("fired"):
                continue

            if rule.get("trigger_type") == "r_multiple":
                trigger_ticks = rule.get("trigger_ticks")
                if trigger_ticks in (None, 0) or self.mfe_ticks < float(trigger_ticks):
                    continue
            else:
                target_id = rule.get("trigger_target_id")
                if not target_id or target_id not in triggered_targets:
                    continue

            candidate = None
            if rule.get("action_type") == "move_to_breakeven":
                candidate = self.entry_price
            elif rule.get("action_type") == "move_to_r":
                action_r = rule.get("action_r")
                if action_r not in (None, 0) and self.r_value not in (None, 0):
                    candidate = risk_math.price_from_r(self.entry_price, self.direction, float(self.r_value), float(action_r))

            if candidate is None:
                continue

            self.stop_price = risk_math.clamp_stop(self.stop_price, candidate, self.direction)
            rule["fired"] = True
            if candidate == self.entry_price:
                self.moved_to_breakeven = True

    def _maybe_trail_stop(self) -> None:
        if self.trailing_distance_ticks in (None, 0):
            return
        if self.trailing_activation_ticks is not None and self.mfe_ticks < self.trailing_activation_ticks:
            return
        distance_price = self.trailing_distance_ticks * self.tick_size
        if distance_price <= 0:
            return
        self.trailing_active = True
        candidate = self.best_price - distance_price if self.direction == "long" else self.best_price + distance_price
        self.stop_price = risk_math.clamp_stop(self.stop_price, candidate, self.direction)

    def _apply_stop(self, candle: Candle) -> List[Dict[str, str]]:
        events: List[Dict[str, str]] = []
        triggered = False
        if self.direction == "long" and candle.low <= self.stop_price:
            triggered = True
        elif self.direction == "short" and candle.high >= self.stop_price:
            triggered = True
        if triggered:
            tick_distance = round(self._ticks_from_entry(self.stop_price), 4)
            for leg in self.legs:
                if leg.status != "open":
                    continue
                fill_result = None
                if str(self.instrument_type or "").lower() == "spot":
                    side = "sell" if self.direction == "long" else "buy"
                    fill_result, rejection = self._execute_spot_fill(
                        self.stop_price, leg.contracts, side=side
                    )
                    if rejection:
                        logger.warning(
                            "spot_stop_rejected | trade=%s | leg=%s | reason=%s",
                            self.trade_id,
                            leg.leg_id or leg.name,
                            rejection.reason,
                        )
                        events.append(
                            {
                                "type": "execution_rejected",
                                "trade_id": self.trade_id,
                                "price": round(self.stop_price, 4),
                                "time": isoformat(candle.time),
                                "currency": self.quote_currency,
                                "leg": leg.name,
                                "leg_id": leg.leg_id,
                                "contracts": leg.contracts,
                                "ticks": tick_distance,
                                "direction": self.direction,
                                "reason": rejection.reason,
                            }
                        )
                        continue
                    allowed, reason, payload = self._wallet_can_apply_fill(fill_result, side=side)
                    if not allowed:
                        if self.wallet_ledger:
                            self.wallet_ledger.rejected(reason, payload)
                        logger.warning(
                            "wallet_stop_rejected | trade=%s | leg=%s | reason=%s",
                            self.trade_id,
                            leg.leg_id or leg.name,
                            reason,
                        )
                        events.append(
                            {
                                "type": "execution_rejected",
                                "trade_id": self.trade_id,
                                "price": round(self.stop_price, 4),
                                "time": isoformat(candle.time),
                                "currency": self.quote_currency,
                                "leg": leg.name,
                                "leg_id": leg.leg_id,
                                "contracts": leg.contracts,
                                "ticks": tick_distance,
                                "direction": self.direction,
                                "reason": reason,
                            }
                        )
                        continue

                exit_price = fill_result.fill_price if fill_result else self.stop_price
                exit_qty = fill_result.filled_qty if fill_result else leg.contracts
                pnl = self._pnl_for_exit(exit_price, exit_qty)
                leg.status = "stop"
                leg.exit_price = exit_price
                leg.exit_time = isoformat(candle.time)
                leg.contracts = exit_qty
                leg.pnl = pnl
                self._record_pnl(pnl)
                if fill_result:
                    self._apply_fee_amount(fill_result.fee)
                    if self.wallet_ledger:
                        self.wallet_ledger.trade_fill(
                            side=side,
                            base_currency=self.base_currency or "",
                            quote_currency=self.quote_currency_code or "",
                            qty=fill_result.filled_qty,
                            price=fill_result.fill_price,
                            fee=fill_result.fee,
                            notional=fill_result.notional,
                            symbol=None,
                            trade_id=self.trade_id,
                            leg_id=leg.leg_id,
                        )
                else:
                    self._apply_fee(exit_price, exit_qty)
                events.append(
                    {
                        "type": "stop",
                        "trade_id": self.trade_id,
                        "price": round(exit_price, 4),
                        "time": leg.exit_time,
                        "currency": self.quote_currency,
                        "leg": leg.name,
                        "leg_id": leg.leg_id,
                        "contracts": exit_qty,
                        "pnl": round(pnl, 4),
                        "ticks": tick_distance,
                        "direction": self.direction,
                    }
                )
            self.closed_at = candle.time
        elif all(leg.status != "open" for leg in self.legs):
            self.closed_at = candle.time
        return events

    def apply_bar(self, candle: Candle) -> List[Dict[str, str]]:
        """Advance the position with the latest candle."""

        events: List[Dict[str, str]] = []
        self._update_excursions(candle)
        leg_events = self._apply_leg_fills(candle)
        events.extend(leg_events)
        self._maybe_apply_stop_adjustments()
        self._maybe_move_breakeven()
        self._maybe_trail_stop()
        stop_events = self._apply_stop(candle)
        if stop_events:
            events.extend(stop_events)
        if not self.is_active():
            events.append(
                {
                    "type": "close",
                    "trade_id": self.trade_id,
                    "time": isoformat(self.closed_at or candle.time),
                    "gross_pnl": round(self.gross_pnl, 4),
                    "fees_paid": round(self.fees_paid, 4),
                    "net_pnl": round(self.net_pnl, 4),
                    "currency": self.quote_currency,
                    "contracts": sum(max(leg.contracts, 0) for leg in self.legs),
                    "direction": self.direction,
                    "metrics": self._metrics_snapshot(),
                }
            )
        return events

    def is_active(self) -> bool:
        return self.closed_at is None

    def serialize(self) -> Dict[str, object]:
        return {
            "trade_id": self.trade_id,
            "entry_time": isoformat(self.entry_time),
            "entry_price": round(self.entry_price, 4),
            "direction": self.direction,
            "stop_price": round(self.stop_price, 4),
            "moved_to_breakeven": self.moved_to_breakeven,
            "legs": [leg.serialize() for leg in self.legs],
            "closed_at": isoformat(self.closed_at),
            "tick_size": self.tick_size,
            "tick_value": round(self.tick_value, 6),
            "contract_size": round(self.contract_size, 6),
            "gross_pnl": round(self.gross_pnl, 4),
            "fees_paid": round(self.fees_paid, 4),
            "net_pnl": round(self.net_pnl, 4),
            "currency": self.quote_currency,
            "atr_at_entry": None if self.atr_at_entry is None else round(self.atr_at_entry, 6),
            "r_value": None if self.r_value is None else round(self.r_value, 6),
            "r_ticks": None if self.r_ticks is None else round(self.r_ticks, 4),
            "mae_ticks": round(self.mae_ticks, 4),
            "mfe_ticks": round(self.mfe_ticks, 4),
            "bars_held": self.bars_held,
            "metrics": self._metrics_snapshot(),
        }

    def _pnl_for_exit(self, exit_price: float, contracts: float) -> float:
        if contracts <= 0:
            return 0.0
        direction = 1 if self.direction == "long" else -1
        if str(self.instrument_type or "").lower() == "spot":
            return (exit_price - self.entry_price) * direction * contracts
        ticks = ((exit_price - self.entry_price) / self.tick_size) * direction
        return ticks * self.tick_value * contracts

    def _ticks_from_entry(self, price: float) -> float:
        if not self.tick_size:
            return 0.0
        direction = 1 if self.direction == "long" else -1
        return ((price - self.entry_price) / self.tick_size) * direction

    def _apply_fee(self, price: float, contracts: float) -> None:
        if contracts <= 0:
            return
        if str(self.instrument_type or "").lower() == "spot":
            notional = abs(price * contracts)
        else:
            notional = abs(price * self.contract_size * contracts)
        fee_rate = self.taker_fee_rate or 0.0
        fee = notional * fee_rate
        if fee:
            self.fees_paid += fee
            self._update_net()

    def _record_pnl(self, pnl: float) -> None:
        self.gross_pnl += pnl
        self._update_net()

    def _update_net(self) -> None:
        self.net_pnl = self.gross_pnl - self.fees_paid

    @staticmethod
    def _sanitize_for_json(value: Optional[float]) -> Optional[float]:
        """Convert NaN/Inf to None for JSON serialization."""
        if value is None:
            return None
        if isinstance(value, (int, float)) and (math.isnan(value) or math.isinf(value)):
            return None
        return value

    def _metrics_snapshot(self) -> Dict[str, object]:
        mae_r = (self.mae_ticks / self.r_ticks) if self.r_ticks else None
        mfe_r = (self.mfe_ticks / self.r_ticks) if self.r_ticks else None

        # Sanitize all numeric values to prevent NaN/Inf in JSON
        atr_clean = self._sanitize_for_json(self.atr_at_entry)
        r_val_clean = self._sanitize_for_json(self.r_value)
        r_ticks_clean = self._sanitize_for_json(self.r_ticks)
        mae_r_clean = self._sanitize_for_json(mae_r)
        mfe_r_clean = self._sanitize_for_json(mfe_r)

        return {
            "atr_at_entry": None if atr_clean is None else round(atr_clean, 6),
            "r_multiple_at_entry": self.r_multiple_at_entry,
            "r_value": None if r_val_clean is None else round(r_val_clean, 6),
            "r_ticks": None if r_ticks_clean is None else round(r_ticks_clean, 4),
            "mae_ticks": round(self.mae_ticks, 4),
            "mfe_ticks": round(self.mfe_ticks, 4),
            "mae_r": None if mae_r_clean is None else round(mae_r_clean, 6),
            "mfe_r": None if mfe_r_clean is None else round(mfe_r_clean, 6),
            "bars_held": self.bars_held,
            "pre_entry_context": dict(self.pre_entry_context or {}),
        }


class LadderRiskEngine:
    """Create and manage laddered trades for simulated bots."""

    def __init__(
        self,
        config: Optional[Dict[str, object]] = None,
        instrument: Optional[Dict[str, Any]] = None,
    ):
        provided_template = config or {}
        self.template = merge_templates(provided_template)
        self.instrument = instrument or {}

        # Always validate - same for all modes (backtest, sim_trade, paper, live)
        self._validate_template(self.template)
        self._validate_instrument(self.instrument)

        # Resolve tick_size (required)
        config_tick = coerce_float(provided_template.get("tick_size"))
        instrument_tick = coerce_float(self.instrument.get("tick_size"))
        tick_size = coalesce_numeric(config_tick, instrument_tick, default=0.0)
        if tick_size == 0:
            raise ValueError("tick_size required from either template or instrument configuration")
        self.tick_size = tick_size

        self.stop_ticks = max(int(self.template.get("stop_ticks") or 0), 0)

        initial_stop_config = self.template.get("initial_stop")
        if not isinstance(initial_stop_config, dict):
            initial_stop_config = {}
        self.r_multiple = float(initial_stop_config.get("atr_multiplier") or 1.0)

        risk_config = self.template.get("risk")
        if not isinstance(risk_config, dict):
            risk_config = {}
        self.base_risk_per_trade = coerce_float(risk_config.get("base_risk_per_trade"))
        self.stop_r_multiple = coerce_float(self.template.get("stop_r_multiple"))

        self.stop_adjustments_config: List[Dict[str, Any]] = list(self.template.get("stop_adjustments") or [])

        # Resolve contract_size (config > instrument > 1.0)
        config_contract = coerce_float(self.template.get("contract_size"))
        instrument_contract = coerce_float(self.instrument.get("contract_size"))
        self.contract_size = coalesce_numeric(config_contract, instrument_contract, default=1.0)
        # Resolve tick_value (config > instrument > calculated from tick_size * contract_size)
        config_tick_value = coerce_float(self.template.get("tick_value"))
        instrument_tick_value = coerce_float(self.instrument.get("tick_value"))
        calculated_tick_value = self.tick_size * self.contract_size
        self.tick_value = coalesce_numeric(config_tick_value, instrument_tick_value, calculated_tick_value, default=self.tick_size)
        self.instrument_type = str(self.instrument.get("instrument_type") or "").lower() or None
        if self.instrument_type == "spot":
            # Spot sizing uses price deltas; ignore contract_size/tick_value inputs.
            self.contract_size = 1.0
            self.tick_value = self.tick_size

        risk_mode = str(initial_stop_config.get("mode") or "atr").lower()
        self.risk_unit_mode = risk_mode if risk_mode in {"atr", "ticks"} else "atr"
        self.ticks_stop = int(
            self.template.get("ticks_stop")
            or self.template.get("stop_ticks")
            or self.stop_ticks
        )
        self.global_risk_multiplier = coerce_float(risk_config.get("global_risk_multiplier"), 1.0) or 1.0
        self.instrument_risk_multiplier = coerce_float(self.instrument.get("risk_multiplier"), 1.0) or 1.0
        self.min_qty, self.qty_step, self.min_notional = self._resolve_quantity_constraints(self.instrument)
        logger.debug("ladder_risk_constraints | min_qty=%.8f | qty_step=%.8f | min_notional=%.4f", self.min_qty, self.qty_step, self.min_notional)
        self.execution_model = SpotExecutionModel(
            SpotExecutionConstraints(
                tick_size=self.tick_size,
                qty_step=self.qty_step,
                min_qty=self.min_qty,
                min_notional=self.min_notional,
            )
        )
        self.last_rejection_reason: Optional[str] = None
        self.last_rejection_detail: Optional[Dict[str, Any]] = None
        self._wallet_ledger: Optional[WalletLedger] = None

        self.orders = self._orders_from_template()
        self.targets = [int(order.get("ticks") or 0) for order in self.orders]
        # Resolve quote currency
        quote_value = self.template.get("quote_currency") or self.instrument.get("quote_currency") or "USD"
        self.quote_currency = str(quote_value).upper()

        # Resolve fee rates (config > instrument > 0.0, allow_zero since 0% fees are valid)
        config_maker = coerce_float(self.template.get("maker_fee_rate"))
        instrument_maker = coerce_float(self.instrument.get("maker_fee_rate"))
        self.maker_fee = coalesce_numeric(config_maker, instrument_maker, default=0.0, allow_zero=True)

        config_taker = coerce_float(self.template.get("taker_fee_rate"))
        instrument_taker = coerce_float(self.instrument.get("taker_fee_rate"))
        self.taker_fee = coalesce_numeric(config_taker, instrument_taker, default=0.0, allow_zero=True)
        self.active_trade: Optional[LadderPosition] = None
        self.trades: List[LadderPosition] = []
        logger.info(
            "ladder_risk_configured | targets=%s | stop_ticks=%s | tick=%.5f | instrument=%s",
            ",".join(str(order.get("ticks") or order.get("r_multiple") or "?") for order in self.orders),
            self.stop_ticks,
            self.tick_size,
            self.instrument.get("symbol"),
        )

    def attach_wallet(self, ledger: WalletLedger) -> None:
        self._wallet_ledger = ledger

    def _validate_template(self, template: Dict[str, Any]) -> None:
        """Validate that required fields are present in template - same for all modes."""
        missing_fields = []

        # Validate stop configuration exists
        if not template.get("initial_stop"):
            missing_fields.append("initial_stop")

        # Validate take profit orders exist
        if not template.get("take_profit_orders"):
            missing_fields.append("take_profit_orders")

        # Validate risk configuration
        risk_config = template.get("risk")
        if not isinstance(risk_config, dict):
            missing_fields.append("risk (must be a dict)")
        elif not risk_config.get("base_risk_per_trade"):
            missing_fields.append("risk.base_risk_per_trade")

        if missing_fields:
            raise ValueError(
                f"Incomplete ATM template. Missing required fields: {', '.join(missing_fields)}. "
                f"All modes (backtest/sim_trade/paper/live) require complete templates."
            )

    def _validate_instrument(self, instrument: Dict[str, Any]) -> None:
        """Validate that instrument configuration is complete."""
        if not instrument:
            raise ValueError("Instrument configuration is required. Cannot proceed without instrument metadata.")

        if not instrument.get("tick_size"):
            raise ValueError(
                "Instrument configuration must include tick_size. "
                "This is required for accurate position sizing and PnL calculation."
            )

    def _orders_from_template(self) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []
        entries = self.template.get("take_profit_orders") or []
        base_contracts = int(self.template.get("contracts") or len(entries) or 0)
        for idx, entry in enumerate(entries):
            ticks = coerce_float(entry.get("ticks"))
            r_multiple = coerce_float(entry.get("r_multiple"))
            price = coerce_float(entry.get("price"))
            if ticks is None and r_multiple is None and price is None:
                continue
            label = entry.get("label") or f"Target {idx + 1}"
            size_fraction = coerce_float(entry.get("size_fraction"))
            size_percent = None
            if size_fraction is not None and 0 <= size_fraction <= 1:
                size_percent = size_fraction * 100

            contracts = int(entry.get("contracts") or 0)
            if contracts <= 0 and size_percent is not None and base_contracts > 0:
                contracts = int(round((size_percent / 100) * base_contracts))
            if contracts <= 0:
                continue
            orders.append(
                {
                    "label": label,
                    "ticks": int(ticks) if ticks is not None else None,
                    "r_multiple": r_multiple,
                    "price": price,
                    "contracts": contracts,
                    "size_fraction": size_fraction,
                    "id": entry.get("id"),
                }
            )
        return orders

    @staticmethod
    def _has_valid_atr(value: Optional[float]) -> bool:
        if value is None:
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        if value == 0:
            return False
        return True

    def _breakeven_threshold(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> float:
        if self.stop_adjustments_config:
            return 0.0
        breakeven = self.template.get("breakeven_trigger_ticks")
        if breakeven not in (None, "", 0):
            try:
                return float(breakeven)
            except (TypeError, ValueError):
                return 0.0
        leg_ticks = min((leg.ticks for leg in legs if leg.ticks), default=None)
        if leg_ticks is None:
            return 0.0
        return max(leg_ticks / 2, 0.0)

    def _trailing_activation_ticks(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> Optional[float]:
        trailing_activation_config = self.template.get("trailing_activation")
        trailing_activation_ticks = None
        if trailing_activation_config:
            try:
                trailing_activation_ticks = float(trailing_activation_config)
            except (TypeError, ValueError):
                trailing_activation_ticks = None
        if trailing_activation_ticks in (None, 0):
            return None
        return float(trailing_activation_ticks)

    def _trailing_distance_ticks(self, atr_at_entry: Optional[float]) -> Optional[float]:
        trailing = self.template.get("trailing_stop")
        if not isinstance(trailing, Mapping):
            trailing = {}
        trailing_ticks = coerce_float(trailing.get("ticks"))
        trailing_atr_multiple = coerce_float(trailing.get("atr_multiplier"))
        trailing_distance_ticks = None
        if trailing_atr_multiple not in (None, 0) and self._has_valid_atr(atr_at_entry):
            trailing_distance_ticks = float(trailing_atr_multiple) * float(atr_at_entry) / float(self.tick_size or 1)
        elif trailing_ticks not in (None, 0):
            trailing_distance_ticks = float(trailing_ticks)
        return trailing_distance_ticks

    def _compute_r_ticks(self, candle: Candle) -> float:
        """Compute stop distance in ticks from initial_stop config.

        Stops are ALWAYS derived from ATR * atr_multiplier from initial_stop config.
        Raises ValueError if ATR is invalid or configuration is missing.
        """
        if not self._has_valid_atr(candle.atr):
            raise ValueError(
                f"Cannot compute stop: ATR is required but got {candle.atr}. "
                f"Ensure strategy includes ATR indicator and candles have valid ATR data."
            )

        if self.tick_size in (None, 0):
            raise ValueError("tick_size is required to compute ATR-based stops")

        if self.r_multiple in (None, 0):
            raise ValueError(
                f"Cannot compute stop: initial_stop.atr_multiplier is required but got {self.r_multiple}. "
                f"Configure atr_multiplier in strategy template."
            )

        tick_stop = int(round((candle.atr * self.r_multiple) / self.tick_size))
        if tick_stop <= 0:
            raise ValueError(
                f"Computed stop is {tick_stop} ticks (ATR={candle.atr}, multiplier={self.r_multiple}, "
                f"tick_size={self.tick_size}). Stop must be > 0."
            )

        return float(tick_stop)

    def _build_stop_adjustments(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> List[Dict[str, Any]]:
        adjustments: List[Dict[str, Any]] = []
        for entry in self.stop_adjustments_config:
            if not isinstance(entry, Mapping):
                continue
            trigger_type = str(entry.get("trigger_type") or "target_id")
            trigger_target_id = entry.get("trigger_target_id")
            trigger_ticks = coerce_float(entry.get("trigger_ticks"))
            action_type = str(entry.get("action_type") or "move_to_breakeven")
            action_r = coerce_float(entry.get("action_r"))
            if trigger_type == "r_multiple" and trigger_ticks in (None, 0):
                continue
            if trigger_type != "r_multiple" and not trigger_target_id:
                continue
            adjustments.append(
                {
                    "trigger_type": trigger_type,
                    "trigger_target_id": trigger_target_id,
                    "trigger_ticks": trigger_ticks,
                    "action_type": action_type,
                    "action_r": action_r,
                }
            )
        return adjustments

    def _r_value(self, candle: Candle) -> Optional[float]:
        """Calculate the monetary value of 1R (ATR * multiplier * tick_value)."""
        if not self._has_valid_atr(candle.atr):
            return None
        return self.tick_value * candle.atr * self.r_multiple

    def _r_ticks(self, candle: Candle) -> Optional[float]:
        """Calculate R in ticks (ATR * multiplier / tick_size)."""
        if not self._has_valid_atr(candle.atr) or self.tick_size in (None, 0):
            return None
        return float((candle.atr * self.r_multiple) / self.tick_size)

    def _calculate_stop_price(self, candle: Candle, direction: str, r_ticks: float) -> float:
        """Calculate initial stop loss price for position.

        Args:
            candle: Current candle
            direction: Trade direction ("long" or "short")
            r_ticks: Stop distance in ticks (must be > 0)

        Returns:
            Stop price
        """
        if r_ticks <= 0:
            raise ValueError(f"r_ticks must be > 0, got {r_ticks}")

        stop_distance = r_ticks * self.tick_size
        if direction == "long":
            return candle.close - stop_distance
        return candle.close + stop_distance

    def _step_from_precision(self, value: Optional[object]) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)) and float(value).is_integer():
            integer = int(float(value))
            if integer >= 0:
                return float(10 ** (-integer))
        numeric = coerce_float(value)
        if numeric in (None, 0):
            return None
        return float(numeric) if 0 < float(numeric) < 1 else None

    def _resolve_quantity_constraints(self, instrument: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        min_qty = coerce_float(
            instrument.get("min_qty")
            or instrument.get("min_order_size")
            or instrument.get("min_quantity")
        )
        qty_step = coerce_float(
            instrument.get("qty_step")
            or instrument.get("order_step")
            or instrument.get("step_size")
            or instrument.get("lot_size")
        )
        min_notional = coerce_float(instrument.get("min_notional") or instrument.get("min_cost"))
        metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
        if isinstance(metadata, Mapping):
            limits = metadata.get("limits") if isinstance(metadata.get("limits"), Mapping) else {}
            amount_limits = limits.get("amount") if isinstance(limits.get("amount"), Mapping) else {}
            cost_limits = limits.get("cost") if isinstance(limits.get("cost"), Mapping) else {}
            if min_qty in (None, 0):
                min_qty = coerce_float(amount_limits.get("min"))
            if min_notional in (None, 0):
                min_notional = coerce_float(cost_limits.get("min"))
            precision = metadata.get("precision") if isinstance(metadata.get("precision"), Mapping) else {}
            if qty_step in (None, 0):
                qty_step = self._step_from_precision(precision.get("amount"))
        return min_qty, qty_step, min_notional

    def _floor_to_step(self, qty: float, step: float) -> float:
        if step in (None, 0):
            return qty
        return math.floor((qty + 1e-12) / step) * step

    def _ceil_to_step(self, qty: float, step: float) -> float:
        if step in (None, 0):
            return qty
        return math.ceil((qty - 1e-12) / step) * step

    def _apply_quantity_constraints(
        self,
        raw_qty: float,
        *,
        price: float,
    ) -> Tuple[Optional[float], Optional[str]]:
        if raw_qty <= 0:
            return None, "risk_qty_nonpositive"

        qty_step = self.qty_step
        min_qty = self.min_qty
        min_notional = self.min_notional

        min_qty_aligned = min_qty
        if min_qty not in (None, 0) and qty_step not in (None, 0):
            min_qty_aligned = self._ceil_to_step(min_qty, qty_step)

        if min_qty_aligned not in (None, 0) and raw_qty < float(min_qty_aligned) - 1e-12:
            return None, "risk_below_min_qty"

        qty = raw_qty
        if qty_step not in (None, 0):
            qty = self._floor_to_step(raw_qty, qty_step)
            if qty <= 0:
                return None, "risk_below_step_size"

        if min_qty_aligned not in (None, 0) and qty < float(min_qty_aligned) - 1e-12:
            if raw_qty >= float(min_qty_aligned):
                qty = float(min_qty_aligned)
            else:
                return None, "risk_below_min_qty"

        if min_notional not in (None, 0):
            notional = abs(price * self.contract_size * qty)
            if notional < float(min_notional) - 1e-12:
                return None, "risk_below_min_notional"

        if qty <= 0:
            return None, "risk_qty_nonpositive"

        return qty, None

    def _calculate_total_contracts(self, r_ticks: float) -> float:
        """Calculate total contracts based on base_risk_per_trade and R value.

        Formula: contracts = base_risk_per_trade / (r_ticks * tick_value)

        Returns:
            Total number of contracts to trade, or None if sizing cannot honor risk

        Raises:
            ValueError: If base_risk_per_trade is not configured
        """
        if self.base_risk_per_trade is None or self.base_risk_per_trade <= 0:
            raise ValueError(
                f"base_risk_per_trade is required but got {self.base_risk_per_trade}. "
                f"Configure risk.base_risk_per_trade in your strategy template. "
                f"This is required for dynamic position sizing."
            )

        if r_ticks <= 0:
            raise ValueError(
                f"Cannot calculate position size: r_ticks must be > 0, got {r_ticks}"
            )

        if self.tick_value <= 0:
            raise ValueError(
                f"Cannot calculate position size: tick_value must be > 0, got {self.tick_value}"
            )

        # Calculate dollar value of 1R per contract
        r_value_per_contract = r_ticks * self.tick_value

        # Calculate how many contracts fit within base_risk_per_trade
        contracts = self.base_risk_per_trade / r_value_per_contract

        # Apply global and instrument risk multipliers
        contracts = contracts * self.global_risk_multiplier * self.instrument_risk_multiplier

        logger.info(
            "position_sizing | base_risk=%.2f | r_value_per_contract=%.6f | raw_qty=%.8f | symbol=%s",
            self.base_risk_per_trade,
            r_value_per_contract,
            contracts,
            self.instrument.get("symbol"),
        )
        return float(contracts)

    def _resolve_base_quote(self) -> Tuple[str, str]:
        metadata = self.instrument.get("metadata") if isinstance(self.instrument.get("metadata"), Mapping) else {}
        base = metadata.get("base_currency") or metadata.get("base") or None
        quote = self.instrument.get("quote_currency") or metadata.get("quote_currency") or metadata.get("quote")
        symbol = str(self.instrument.get("symbol") or "")
        if not base:
            if "/" in symbol:
                parts = symbol.split("/")
                if len(parts) == 2:
                    base = parts[0]
                    quote = quote or parts[1]
            elif "-" in symbol:
                parts = symbol.split("-")
                if len(parts) == 2:
                    base = parts[0]
                    quote = quote or parts[1]
        if not base or not quote:
            raise ValueError(f"Cannot resolve base/quote currencies for instrument {symbol}")
        return str(base).upper(), str(quote).upper()

    def _build_legs(
        self,
        candle: Candle,
        direction: str,
        r_ticks: Optional[float],
        total_contracts: float,
    ) -> List[Leg]:
        """Build take-profit legs from template configuration.

        Args:
            candle: Current candle data
            direction: Trade direction ('long' or 'short')
            r_ticks: Stop distance in ticks
            total_contracts: Total number of contracts to distribute across legs
        """
        legs: List[Leg] = []

        for idx, order in enumerate(self.orders):
            ticks = order.get("ticks")
            r_multiple = order.get("r_multiple")
            price = order.get("price")
            target_ticks = ticks
            target_price = None

            # Calculate target price based on configuration type
            if r_multiple not in (None, 0) and r_ticks not in (None, 0):
                computed_ticks = float(r_multiple) * float(r_ticks)
                distance = computed_ticks * self.tick_size
                target_price = candle.close + distance if direction == "long" else candle.close - distance
                target_ticks = int(round(computed_ticks))
            elif ticks is not None:
                distance = ticks * self.tick_size
                target_price = candle.close + distance if direction == "long" else candle.close - distance
            elif price is not None:
                target_price = float(price)
                computed_ticks = risk_math.ticks_from_entry(candle.close, target_price, direction, self.tick_size)
                target_ticks = int(round(computed_ticks))

            if target_price is None:
                continue

            # Calculate contracts for this leg based on size_fraction
            size_fraction = coerce_float(order.get("size_fraction"))
            if size_fraction is not None and 0 < size_fraction <= 1:
                leg_contracts = float(total_contracts) * float(size_fraction)
            else:
                # Equal distribution if no size_fraction specified
                leg_contracts = float(total_contracts) / float(len(self.orders) or 1)

            if leg_contracts <= 0:
                continue

            legs.append(
                Leg(
                    name=order.get("label") or f"TP{target_ticks or ticks or idx + 1}",
                    ticks=target_ticks or 0,
                    target_price=target_price,
                    contracts=leg_contracts,
                    leg_id=order.get("id") or order.get("label") or f"tp-{idx + 1}",
                )
            )

        if str(self.instrument_type or "").lower() != "spot":
            return legs

        step = self.qty_step
        if step in (None, 0):
            return legs

        rounded: List[Leg] = []
        for leg in legs:
            qty = float(int((leg.contracts + 1e-12) / step)) * float(step)
            if qty <= 0:
                continue
            leg.contracts = qty
            rounded.append(leg)

        if not rounded:
            return []

        total_allocated = sum(leg.contracts for leg in rounded)
        remainder = total_contracts - total_allocated
        if remainder > 0:
            extra = float(int((remainder + 1e-12) / step)) * float(step)
            if extra > 0:
                rounded[-1].contracts += extra

        min_qty = self.min_qty
        if min_qty not in (None, 0):
            for leg in rounded:
                if leg.contracts < float(min_qty):
                    return []

        return rounded

    def _new_position(self, candle: Candle, direction: str) -> Optional[LadderPosition]:
        """Create a new ladder position from current candle and signal direction."""
        # Calculate risk metrics - _compute_r_ticks will raise if ATR invalid or config missing
        atr_at_entry = candle.atr if self._has_valid_atr(candle.atr) else None
        r_ticks = self._compute_r_ticks(candle)  # Raises ValueError if stop cannot be computed

        r_value = self._r_value(candle)
        if self.stop_r_multiple not in (None, 0) and r_value not in (None, 0):
            r_value = float(self.stop_r_multiple) * float(r_value)

        # Calculate position size based on risk (raw qty before exchange constraints)
        requested_qty = self._calculate_total_contracts(r_ticks)

        fill_result: Optional[FillResult] = None
        base_currency = None
        quote_currency = None
        if self.instrument_type == "spot":
            if not self._wallet_ledger:
                raise ValueError("Wallet ledger is required for spot execution")
            base_currency, quote_currency = self._resolve_base_quote()
            side = "buy" if direction == "long" else "sell"
            fill_result, rejection = self.execution_model.fill_market(
                side=side,
                requested_qty=requested_qty,
                price=candle.close,
                fee_rate=self.taker_fee or 0.0,
                enforce_price_tick=False,
            )
            if rejection:
                self.last_rejection_reason = rejection.reason
                self.last_rejection_detail = {
                    "requested_qty": requested_qty,
                    "price": candle.close,
                    **(rejection.metadata or {}),
                }
                logger.warning(
                    "spot_entry_rejected | reason=%s | symbol=%s | requested_qty=%.8f | price=%.4f",
                    rejection.reason,
                    self.instrument.get("symbol"),
                    requested_qty,
                    candle.close,
                )
                return None
            state = self._wallet_ledger.project()
            allowed, reason, payload = wallet_can_apply(
                state=state,
                side=side,
                base_currency=base_currency,
                quote_currency=quote_currency,
                qty=fill_result.filled_qty,
                notional=fill_result.notional,
                fee=fill_result.fee,
            )
            if not allowed:
                self._wallet_ledger.rejected(reason, payload)
                self.last_rejection_reason = reason
                self.last_rejection_detail = payload
                logger.warning(
                    "wallet_entry_rejected | reason=%s | symbol=%s | qty=%.8f | price=%.4f",
                    reason,
                    self.instrument.get("symbol"),
                    fill_result.filled_qty,
                    fill_result.fill_price,
                )
                return None
            self._wallet_ledger.trade_fill(
                side=side,
                base_currency=base_currency,
                quote_currency=quote_currency,
                qty=fill_result.filled_qty,
                price=fill_result.fill_price,
                fee=fill_result.fee,
                notional=fill_result.notional,
                symbol=self.instrument.get("symbol"),
            )

        total_contracts = fill_result.filled_qty if fill_result else requested_qty

        # Build position components
        stop_price = self._calculate_stop_price(candle, direction, r_ticks)
        legs = self._build_legs(candle, direction, r_ticks, total_contracts)
        if not legs:
            self.last_rejection_reason = "QTY_ROUNDS_TO_ZERO"
            self.last_rejection_detail = {"requested_qty": requested_qty, "symbol": self.instrument.get("symbol")}
            logger.warning(
                "spot_entry_rejected | reason=QTY_ROUNDS_TO_ZERO | symbol=%s | requested_qty=%.8f",
                self.instrument.get("symbol"),
                requested_qty,
            )
            return None

        # Configure stop management
        runtime_stop_adjustments = self._build_stop_adjustments(legs, r_ticks)
        breakeven_ticks = 0.0 if runtime_stop_adjustments else self._breakeven_threshold(legs, r_ticks)
        trailing_activation_ticks = self._trailing_activation_ticks(legs, r_ticks)
        trailing_distance_ticks = self._trailing_distance_ticks(atr_at_entry)

        # Get trailing config
        self.trailing_config = self.template.get("trailing_stop") if isinstance(self.template.get("trailing_stop"), Mapping) else {}

        # Create position
        position = LadderPosition(
            entry_time=candle.time,
            entry_price=fill_result.fill_price if fill_result else candle.close,
            direction=direction,
            stop_price=stop_price,
            tick_size=self.tick_size,
            instrument_type=self.instrument_type,
            execution_model=self.execution_model if self.instrument_type == "spot" else None,
            wallet_ledger=self._wallet_ledger if self.instrument_type == "spot" else None,
            base_currency=base_currency,
            quote_currency_code=quote_currency,
            legs=legs,
            breakeven_trigger_ticks=breakeven_ticks,
            tick_value=self.tick_value,
            contract_size=self.contract_size,
            maker_fee_rate=self.maker_fee,
            taker_fee_rate=self.taker_fee,
            quote_currency=self.quote_currency,
            atr_at_entry=atr_at_entry,
            r_multiple_at_entry=self.r_multiple,
            r_value=r_value,
            r_ticks=r_ticks,
            trailing_activation_ticks=trailing_activation_ticks,
            trailing_distance_ticks=trailing_distance_ticks,
            trailing_atr_multiple=float(self.trailing_config.get("atr_multiplier") or 0.0),
            pre_entry_context=getattr(candle, "lookback_15", None),
            stop_adjustments=runtime_stop_adjustments,
        )
        if fill_result:
            position.apply_entry_fee(fill_result.fee)
        else:
            position.register_entry_fee()
        return position

    def maybe_enter(self, candle: Candle, direction: Optional[str]) -> Optional[LadderPosition]:
        if direction is None or self.active_trade is not None:
            return None
        self.active_trade = self._new_position(candle, direction)
        if self.active_trade is None:
            return None
        self.trades.append(self.active_trade)
        return self.active_trade

    def step(self, candle: Candle) -> List[Dict[str, Any]]:
        if self.active_trade is None:
            return []
        events = self.active_trade.apply_bar(candle)
        if not self.active_trade.is_active():
            self.active_trade = None
        return events

    def serialise_trades(self) -> List[Dict[str, object]]:
        return [trade.serialize() for trade in self.trades]

    def stats(self) -> Dict[str, float]:
        legs = [leg for trade in self.trades for leg in trade.legs]
        leg_wins = sum(1 for leg in legs if leg.status == "target")
        leg_losses = sum(1 for leg in legs if leg.status == "stop")
        completed = [trade for trade in self.trades if not trade.is_active()]
        tolerance = 1e-8
        trade_wins = sum(1 for trade in completed if trade.net_pnl > tolerance)
        trade_losses = sum(1 for trade in completed if trade.net_pnl < -tolerance)
        breakeven = max(len(completed) - trade_wins - trade_losses, 0)
        completed_total = len(completed)
        denominator = completed_total or 1
        long_trades = sum(1 for trade in self.trades if trade.direction == "long")
        short_trades = sum(1 for trade in self.trades if trade.direction == "short")
        gross = sum(trade.gross_pnl for trade in self.trades)
        fees = sum(trade.fees_paid for trade in self.trades)
        net = gross - fees
        return {
            "total_trades": len(self.trades),
            "completed_trades": completed_total,
            "legs_closed": leg_wins + leg_losses,
            "wins": trade_wins,
            "losses": trade_losses,
            "breakeven_trades": breakeven,
            "win_rate": round(trade_wins / denominator, 4),
            "long_trades": long_trades,
            "short_trades": short_trades,
            "gross_pnl": round(gross, 4),
            "fees_paid": round(fees, 4),
            "net_pnl": round(net, 4),
            "quote_currency": self.quote_currency,
        }


__all__ = [
    "Candle",
    "StrategySignal",
    "Leg",
    "LadderPosition",
    "LadderRiskEngine",
    "coerce_float",
    "isoformat",
    "timeframe_duration",
    "timeframe_to_seconds",
]

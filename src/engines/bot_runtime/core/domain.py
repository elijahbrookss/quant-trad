"""Domain models and ladder risk math for bot runtime."""

from __future__ import annotations

import logging
import math
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

import risk as risk_math
from atm import merge_templates
from .execution import FillRejection, FillResult
from .execution_adapter import ExecutionAdapter
from utils.log_context import build_log_context, merge_log_context, with_log_context
from .amount_constraints import normalize_qty_with_constraints, resolve_amount_constraints
from .margin import calculate_max_qty_by_margin, resolve_instrument_type, InstrumentType
from .wallet import WalletLedger, trace_wallet_balance
from .wallet_gateway import LedgerWalletGateway, WalletGateway

logger = logging.getLogger(__name__)

_TIMEFRAME_MULTIPLIERS = {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 60 * 60 * 24,
    "w": 60 * 60 * 24 * 7,
}

if TYPE_CHECKING:
    from .execution import SpotExecutionModel


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


@dataclass
class LadderPosition:
    """Track laddered take-profit and stop-loss behaviour for a trade."""

    entry_time: datetime
    entry_price: float
    direction: str
    stop_price: float
    tick_size: float
    execution_model: Optional["SpotExecutionModel"] = None
    execution_adapter: Optional[ExecutionAdapter] = None
    wallet_gateway: Optional[WalletGateway] = None
    base_currency: Optional[str] = None
    quote_currency_code: Optional[str] = None
    legs: List[Leg] = field(default_factory=list)
    breakeven_trigger_ticks: float = 20.0
    tick_value: float = 1.0
    contract_size: float = 1.0
    maker_fee_rate: float = 0.0
    taker_fee_rate: float = 0.0
    quote_currency: str = "USD"
    short_requires_borrow: bool = False
    instrument: Optional[Dict[str, Any]] = None  # For margin-based validation
    moved_to_breakeven: bool = False
    closed_at: Optional[datetime] = None
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: isoformat(datetime.now(timezone.utc)))
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
        if self.execution_adapter:
            return self.execution_adapter.fill_market(
                side=side,
                requested_qty=contracts,
                price=price,
                fee_rate=self.taker_fee_rate or 0.0,
                enforce_price_tick=False,
            )
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
        if not self.wallet_gateway:
            return True, None, {}
        return self.wallet_gateway.can_apply(
            side=side,
            base_currency=self.base_currency or "",
            quote_currency=self.quote_currency_code or "",
            qty=fill.filled_qty,
            qty_raw=fill.metadata.get("qty_raw") if isinstance(fill.metadata, dict) else None,
            qty_final=fill.metadata.get("qty_final") if isinstance(fill.metadata, dict) else None,
            notional=fill.notional,
            fee=fill.fee,
            short_requires_borrow=bool(self.short_requires_borrow),
            instrument=self.instrument,
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

    def _uses_wallet_execution(self) -> bool:
        return bool(self.execution_adapter and self.wallet_gateway)

    def _accounting_mode(self) -> Optional[str]:
        inst_type = resolve_instrument_type(self.instrument or {})
        if inst_type in (InstrumentType.FUTURE, InstrumentType.SWAP):
            return "margin"
        return None

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
            if self._uses_wallet_execution():
                side = "sell" if self.direction == "long" else "buy"
                fill_result, rejection = self._execute_spot_fill(
                    leg.target_price, leg.contracts, side=side
                )
                if rejection:
                    context = build_log_context(
                        trade_id=self.trade_id,
                        leg_id=leg.leg_id,
                        leg=leg.name,
                        reason=rejection.reason,
                        price=round(leg.target_price, 4),
                        direction=self.direction,
                    )
                    logger.warning(with_log_context("spot_exit_rejected", context))
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
                    # CRITICAL: Position exits must ALWAYS execute to close positions
                    # Log the wallet insufficient balance but force the execution anyway
                    if self.wallet_gateway:
                        self.wallet_gateway.reject(reason, payload, trade_id=self.trade_id, leg_id=leg.leg_id)
                    context = merge_log_context(
                        build_log_context(
                            trade_id=self.trade_id,
                            leg_id=leg.leg_id,
                            leg=leg.name,
                            reason=reason,
                            price=round(leg.target_price, 4),
                            direction=self.direction,
                        ),
                        build_log_context(
                            available=payload.get("available"),
                            required=payload.get("required"),
                            required_used=payload.get("required_used"),
                            required_full_notional=payload.get("required_full_notional"),
                            available_collateral=payload.get("available_collateral"),
                            currency=payload.get("currency"),
                            qty=payload.get("qty"),
                            qty_raw=payload.get("qty_raw"),
                            qty_final=payload.get("qty_final"),
                            notional=payload.get("notional"),
                            fee=payload.get("fee"),
                            margin_total_required=payload.get("margin_total_required"),
                            margin_calc_type=payload.get("margin_calc_type"),
                            margin_method=payload.get("margin_method"),
                            margin_session=payload.get("margin_session"),
                            margin_leg=payload.get("margin_leg"),
                            margin_rate_source_path=payload.get("margin_rate_source_path"),
                            shortfall=payload.get("shortfall"),
                        ),
                    )
                    logger.warning(with_log_context("wallet_exit_forced_despite_insufficient_balance", context))
                    # Note: We do NOT continue here - we force the exit execution below

            exit_price = fill_result.fill_price if fill_result else leg.target_price
            exit_qty = fill_result.filled_qty if fill_result else leg.contracts
            pnl = self._pnl_for_exit(exit_price, exit_qty)
            leg.status = "target"
            leg.exit_price = exit_price
            leg.exit_time = isoformat(candle.time)
            leg.exit_created_at = isoformat(datetime.now(timezone.utc))
            leg.contracts = exit_qty
            leg.pnl = pnl
            self._record_pnl(pnl)
            if fill_result:
                self._apply_fee_amount(fill_result.fee)
                if self.wallet_gateway:
                    self.wallet_gateway.apply_fill(
                        event_type="EXIT_FILL",
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
                        position_direction=self.direction,
                        accounting_mode=self._accounting_mode(),
                        realized_pnl=pnl,
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
                if self._uses_wallet_execution():
                    side = "sell" if self.direction == "long" else "buy"
                    fill_result, rejection = self._execute_spot_fill(
                        self.stop_price, leg.contracts, side=side
                    )
                    if rejection:
                        context = build_log_context(
                            trade_id=self.trade_id,
                            leg_id=leg.leg_id,
                            leg=leg.name,
                            reason=rejection.reason,
                            price=round(self.stop_price, 4),
                            direction=self.direction,
                        )
                        logger.warning(with_log_context("spot_stop_rejected", context))
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
                        # CRITICAL: Stop losses must ALWAYS execute to close positions
                        # Log the wallet insufficient balance but force the execution anyway
                        if self.wallet_gateway:
                            self.wallet_gateway.reject(reason, payload, trade_id=self.trade_id, leg_id=leg.leg_id)
                        context = merge_log_context(
                            build_log_context(
                                trade_id=self.trade_id,
                                leg_id=leg.leg_id,
                                leg=leg.name,
                                reason=reason,
                                price=round(self.stop_price, 4),
                                direction=self.direction,
                            ),
                            build_log_context(
                                available=payload.get("available"),
                                required=payload.get("required"),
                                required_used=payload.get("required_used"),
                                required_full_notional=payload.get("required_full_notional"),
                                available_collateral=payload.get("available_collateral"),
                                currency=payload.get("currency"),
                                qty=payload.get("qty"),
                                qty_raw=payload.get("qty_raw"),
                                qty_final=payload.get("qty_final"),
                                notional=payload.get("notional"),
                                fee=payload.get("fee"),
                                margin_total_required=payload.get("margin_total_required"),
                                margin_calc_type=payload.get("margin_calc_type"),
                                margin_method=payload.get("margin_method"),
                                margin_session=payload.get("margin_session"),
                                margin_leg=payload.get("margin_leg"),
                                margin_rate_source_path=payload.get("margin_rate_source_path"),
                                shortfall=payload.get("shortfall"),
                            ),
                        )
                        logger.warning(with_log_context("wallet_stop_forced_despite_insufficient_balance", context))
                        # Note: We do NOT continue here - we force the stop execution below

                exit_price = fill_result.fill_price if fill_result else self.stop_price
                exit_qty = fill_result.filled_qty if fill_result else leg.contracts
                pnl = self._pnl_for_exit(exit_price, exit_qty)
                leg.status = "stop"
                leg.exit_price = exit_price
                leg.exit_time = isoformat(candle.time)
                leg.exit_created_at = isoformat(datetime.now(timezone.utc))
                leg.contracts = exit_qty
                leg.pnl = pnl
                self._record_pnl(pnl)
                if fill_result:
                    self._apply_fee_amount(fill_result.fee)
                    if self.wallet_gateway:
                        self.wallet_gateway.apply_fill(
                            event_type="EXIT_FILL",
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
                            position_direction=self.direction,
                            accounting_mode=self._accounting_mode(),
                            realized_pnl=pnl,
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
            "created_at": self.created_at,
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
        self.contract_size = coalesce_numeric(config_contract, instrument_contract, default=0.0)
        if self.contract_size in (None, 0):
            raise ValueError("contract_size required from either template or instrument configuration")
        # Resolve tick_value (config > instrument > calculated from tick_size * contract_size)
        config_tick_value = coerce_float(self.template.get("tick_value"))
        instrument_tick_value = coerce_float(self.instrument.get("tick_value"))
        calculated_tick_value = self.tick_size * self.contract_size
        self.tick_value = coalesce_numeric(config_tick_value, instrument_tick_value, calculated_tick_value, default=0.0)
        if self.tick_value in (None, 0):
            raise ValueError("tick_value required from either template or instrument configuration")

        risk_mode = str(initial_stop_config.get("mode") or "atr").lower()
        self.risk_unit_mode = risk_mode if risk_mode in {"atr", "ticks"} else "atr"
        self.ticks_stop = int(
            self.template.get("ticks_stop")
            or self.template.get("stop_ticks")
            or self.stop_ticks
        )
        self.global_risk_multiplier = coerce_float(risk_config.get("global_risk_multiplier"), 1.0) or 1.0
        self.instrument_risk_multiplier = coerce_float(self.instrument.get("risk_multiplier"), 1.0) or 1.0
        try:
            self.amount_constraints = resolve_amount_constraints(self.instrument)
        except ValueError as exc:
            context = build_log_context(
                symbol=self.instrument.get("symbol"),
                reason="AMOUNT_CONSTRAINT_CONFLICT",
                error=str(exc),
            )
            logger.error(with_log_context("ladder_risk_configuration_failed", context))
            raise
        self.min_qty = self.amount_constraints.min_qty
        self.max_qty = self.amount_constraints.max_qty
        self.qty_step = self.amount_constraints.qty_step
        self.min_notional = self.amount_constraints.min_notional
        self.amount_precision = self.amount_constraints.precision
        constraints_context = build_log_context(
            symbol=self.instrument.get("symbol"),
            min_qty=self.min_qty,
            max_qty=self.max_qty,
            qty_step=self.qty_step,
            min_notional=self.min_notional,
            amount_precision=self.amount_precision,
            qty_step_source=self.amount_constraints.step_source,
        )
        logger.debug(with_log_context("ladder_risk_constraints", constraints_context))
        self.execution_model = None
        self.execution_adapter: Optional[ExecutionAdapter] = None
        self.last_rejection_reason: Optional[str] = None
        self.last_rejection_detail: Optional[Dict[str, Any]] = None
        self._wallet_ledger: Optional[WalletLedger] = None
        self._wallet_gateway: Optional[WalletGateway] = None
        self.can_short = bool(self.instrument.get("can_short"))
        self.short_requires_borrow = bool(self.instrument.get("short_requires_borrow"))

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
        configured_context = build_log_context(
            symbol=self.instrument.get("symbol"),
            targets=",".join(str(order.get("ticks") or order.get("r_multiple") or "?") for order in self.orders),
            stop_ticks=self.stop_ticks,
            tick_size=self.tick_size,
        )
        logger.info(with_log_context("ladder_risk_configured", configured_context))

    def attach_wallet(self, ledger: WalletLedger) -> None:
        self._wallet_ledger = ledger
        self._wallet_gateway = LedgerWalletGateway(ledger)

    def attach_execution_adapter(self, adapter: ExecutionAdapter) -> None:
        """Inject a run-type specific execution adapter (backtest/paper/live)."""
        self.execution_adapter = adapter

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

    def _floor_to_step(self, qty: float, step: float) -> float:
        if step in (None, 0):
            return qty
        return math.floor((qty + 1e-12) / step) * step

    def _ceil_to_step(self, qty: float, step: float) -> float:
        if step in (None, 0):
            return qty
        return math.ceil((qty - 1e-12) / step) * step

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

        sizing_context = build_log_context(
            symbol=self.instrument.get("symbol"),
            base_risk=self.base_risk_per_trade,
            r_value_per_contract=r_value_per_contract,
            raw_qty=contracts,
        )
        logger.info(with_log_context("position_sizing", sizing_context))
        return float(contracts)

    def _cap_qty_by_margin(
        self,
        risk_qty: float,
        price: float,
        direction: str,
    ) -> Tuple[float, bool, Optional[Dict[str, Any]]]:
        """Cap risk-based qty by available margin for futures/derivatives.

        For spot instruments, returns risk_qty unchanged.
        For futures/swaps, calculates max qty allowed by wallet margin and caps.

        Args:
            risk_qty: Qty calculated from risk-based sizing
            price: Current price
            direction: "long" or "short"

        Returns:
            Tuple of (final_qty, was_capped, margin_info)
        """
        if not self._wallet_gateway:
            return risk_qty, False, None

        inst_type = resolve_instrument_type(self.instrument)

        # Only apply margin cap for futures/swaps
        if inst_type not in (InstrumentType.FUTURE, InstrumentType.SWAP):
            return risk_qty, False, None

        # Get available collateral from wallet (for backtest, same as cash balance)
        wallet_state = self._wallet_gateway.project()
        quote = self.quote_currency.upper()
        available_collateral = wallet_state.balances.get(quote, 0.0)

        if available_collateral <= 0:
            balance_trace = None
            ledger = getattr(self._wallet_gateway, "ledger", None)
            if ledger and hasattr(ledger, "events"):
                balance_trace = trace_wallet_balance(ledger.events(), quote, limit=8)
            return (
                0.0,
                True,
                {
                    "reason": "no_available_collateral",
                    "available_collateral": float(available_collateral),
                    "max_qty_by_margin": 0.0,
                    "cost_per_contract": None,
                    "margin_per_contract": None,
                    "fee_per_contract": None,
                    "margin_rate": None,
                    "calculation_method": None,
                    "balance_trace": balance_trace,
                },
            )

        try:
            margin_result = calculate_max_qty_by_margin(
                available_collateral=available_collateral,
                price=price,
                contract_size=self.contract_size,
                direction=direction,
                instrument=self.instrument,
                fee_rate=self.taker_fee or 0.0,  # Use taker (worst case) for conservative sizing
                safety_multiplier=1.05,
                qty_step=self.qty_step,
                min_order_size=self.min_qty,
            )
        except ValueError as exc:
            # Instrument misconfigured - fail loud
            return (
                0.0,
                True,
                {
                    "reason": "margin_calculation_failed",
                    "error": str(exc),
                    "available_collateral": float(available_collateral),
                    "max_qty_by_margin": 0.0,
                    "cost_per_contract": None,
                    "margin_per_contract": None,
                    "fee_per_contract": None,
                    "margin_rate": None,
                    "calculation_method": None,
                },
            )

        max_qty = margin_result.max_qty
        was_capped = risk_qty > max_qty

        margin_info = {
            "risk_qty": risk_qty,
            "max_qty_by_margin": max_qty,
            "was_capped": was_capped,
            "available_collateral": available_collateral,
            "cost_per_contract": margin_result.cost_per_contract,
            "margin_per_contract": margin_result.margin_per_contract,
            "fee_per_contract": margin_result.fee_per_contract,
            "margin_rate": margin_result.margin_rate,
            "calculation_method": margin_result.calculation_method,
        }

        final_qty = min(risk_qty, max_qty) if was_capped else risk_qty

        if was_capped:
            context = build_log_context(
                symbol=self.instrument.get("symbol"),
                risk_qty=round(risk_qty, 6),
                max_qty_by_margin=round(max_qty, 6),
                final_qty=round(final_qty, 6),
                available_collateral=round(available_collateral, 2),
                cost_per_contract=round(margin_result.cost_per_contract, 4),
                margin_rate=round(margin_result.margin_rate, 6),
            )
            logger.info(with_log_context("qty_capped_by_margin", context))

        return final_qty, was_capped, margin_info

    def _resolve_base_quote(self) -> Tuple[str, str]:
        base = self.instrument.get("base_currency")
        quote = self.instrument.get("quote_currency")
        symbol = str(self.instrument.get("symbol") or "")
        if not base or not quote:
            context = build_log_context(
                symbol=symbol,
                base_currency=base,
                quote_currency=quote,
                instrument=self.instrument,
            )
            logger.error(with_log_context("instrument_base_quote_missing", context))
            raise ValueError(f"Cannot resolve base/quote currencies for instrument {symbol}")
        return str(base).upper(), str(quote).upper()

    def _resolve_tp_step(self) -> Optional[float]:
        step = self.qty_step
        source = self.amount_constraints.step_source
        inst_type = resolve_instrument_type(self.instrument)

        if inst_type in (InstrumentType.FUTURE, InstrumentType.SWAP):
            if source in {"base_increment", "provider_product", "metadata", "metadata_info"} and step not in (None, 0):
                if step >= 1 and abs(step - round(step)) <= 1e-9:
                    return float(step)
                return None
            symbol = self.instrument.get("symbol")
            raise ValueError(f"Missing instrument metadata qty step for TP allocation: {symbol}")

        if step not in (None, 0) and step >= 1 and abs(step - round(step)) <= 1e-9:
            return float(step)
        return None

    def _allocate_tp_contracts(
        self,
        *,
        qty_final: float,
        tp_leg_count: int,
        step: float,
    ) -> Tuple[List[float], List[int]]:
        if tp_leg_count <= 0:
            return [], []
        total_units = int(math.floor((qty_final + 1e-12) / step))
        if total_units <= 0:
            return [0.0 for _ in range(tp_leg_count)], list(range(1, tp_leg_count + 1))
        if total_units < tp_leg_count:
            units = [1] * total_units + [0] * (tp_leg_count - total_units)
        else:
            base = total_units // tp_leg_count
            remainder = total_units % tp_leg_count
            units = [base + (1 if idx < remainder else 0) for idx in range(tp_leg_count)]
        contracts = [float(unit) * float(step) for unit in units]
        dropped = [idx + 1 for idx, qty in enumerate(contracts) if qty <= 0]
        return contracts, dropped

    def _build_legs(
        self,
        candle: Candle,
        direction: str,
        r_ticks: Optional[float],
        total_contracts: float,
        *,
        qty_raw: Optional[float] = None,
        qty_final: Optional[float] = None,
        order_intent_id: Optional[str] = None,
        side: Optional[str] = None,
    ) -> List[Leg]:
        """Build take-profit legs from template configuration.

        Args:
            candle: Current candle data
            direction: Trade direction ('long' or 'short')
            r_ticks: Stop distance in ticks
            total_contracts: Total number of contracts to distribute across legs
            qty_raw: Raw qty before normalization (for logging)
            qty_final: Normalized qty used for allocation (for logging)
            order_intent_id: Correlation id for log tracing
            side: Order side for log context
        """
        leg_specs: List[Dict[str, Any]] = []

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

            leg_specs.append(
                {
                    "name": order.get("label") or f"TP{target_ticks or ticks or idx + 1}",
                    "ticks": target_ticks or 0,
                    "target_price": target_price,
                    "leg_id": order.get("id") or order.get("label") or f"tp-{idx + 1}",
                    "order": order,
                }
            )

        qty_raw_value = float(qty_raw) if qty_raw is not None else float(total_contracts)
        qty_final_value = float(qty_final) if qty_final is not None else float(total_contracts)
        tp_leg_count = len(leg_specs)

        contracts_by_leg: List[float] = []
        dropped_legs: List[int] = []

        tp_step = self._resolve_tp_step()
        if tp_step is not None:
            contracts_by_leg, dropped_legs = self._allocate_tp_contracts(
                qty_final=qty_final_value,
                tp_leg_count=tp_leg_count,
                step=tp_step,
            )
        else:
            for spec in leg_specs:
                size_fraction = coerce_float(spec["order"].get("size_fraction"))
                if size_fraction is not None and 0 < size_fraction <= 1:
                    leg_contracts = float(qty_final_value) * float(size_fraction)
                else:
                    leg_contracts = float(qty_final_value) / float(tp_leg_count or 1)
                contracts_by_leg.append(leg_contracts)

            step = self.qty_step
            if step not in (None, 0):
                rounded: List[float] = []
                for qty in contracts_by_leg:
                    rounded_qty = float(int((qty + 1e-12) / step)) * float(step)
                    rounded.append(rounded_qty)
                total_allocated = sum(rounded)
                remainder = qty_final_value - total_allocated
                if remainder > 0:
                    extra = float(int((remainder + 1e-12) / step)) * float(step)
                    if extra > 0:
                        rounded[-1] += extra
                contracts_by_leg = rounded

            dropped_legs = [idx + 1 for idx, qty in enumerate(contracts_by_leg) if qty <= 0]

        if dropped_legs:
            if qty_final_value < tp_leg_count:
                drop_reason = "INSUFFICIENT_CONTRACTS_FOR_LEGS"
                drop_explain = f"qty_final {qty_final_value} < tp_leg_count {tp_leg_count}; dropped legs {dropped_legs}"
            elif qty_final_value < qty_raw_value:
                drop_reason = "QTY_NORMALIZED_DOWN"
                drop_explain = f"qty normalized down from {qty_raw_value} to {qty_final_value}; dropped legs {dropped_legs}"
            else:
                drop_reason = "INSUFFICIENT_CONTRACTS_FOR_LEGS"
                drop_explain = f"tp allocation dropped legs {dropped_legs}"
        else:
            drop_reason = "NONE"
            drop_explain = "no legs dropped"

        context = build_log_context(
            symbol=self.instrument.get("symbol"),
            order_intent_id=order_intent_id,
            side=side,
            qty_raw=qty_raw_value,
            qty_final=qty_final_value,
            qty_step=self.qty_step,
            tp_step=tp_step,
            min_order_size=self.min_qty,
            tp_leg_count=tp_leg_count,
            tp_contracts=contracts_by_leg,
            dropped_legs=dropped_legs,
            drop_reason=drop_reason,
            drop_explain=drop_explain,
        )
        logger.info(with_log_context("tp_leg_allocation_finalized", context))
        self._last_tp_allocation = dict(context)

        legs: List[Leg] = []
        for spec, contracts in zip(leg_specs, contracts_by_leg):
            if contracts <= 0:
                continue
            legs.append(
                Leg(
                    name=spec["name"],
                    ticks=spec["ticks"],
                    target_price=spec["target_price"],
                    contracts=contracts,
                    leg_id=spec["leg_id"],
                )
            )

        if not legs:
            return []

        min_qty = self.min_qty
        if min_qty not in (None, 0):
            for leg in legs:
                if leg.contracts < float(min_qty):
                    return []

        return legs

    def _new_position(self, candle: Candle, direction: str) -> Optional[LadderPosition]:
        """Create a new ladder position from current candle and signal direction."""
        # Calculate risk metrics - _compute_r_ticks will raise if ATR invalid or config missing
        atr_at_entry = candle.atr if self._has_valid_atr(candle.atr) else None
        r_ticks = self._compute_r_ticks(candle)  # Raises ValueError if stop cannot be computed

        r_value = self._r_value(candle)
        if self.stop_r_multiple not in (None, 0) and r_value not in (None, 0):
            r_value = float(self.stop_r_multiple) * float(r_value)

        # Calculate position size based on risk (raw qty before exchange constraints)
        risk_based_qty = self._calculate_total_contracts(r_ticks)

        # Cap qty by available margin for futures/derivatives
        # This prevents wallet rejection by sizing within margin constraints upfront
        capped_qty, was_margin_capped, margin_info = self._cap_qty_by_margin(
            risk_qty=risk_based_qty,
            price=candle.close,
            direction=direction,
        )

        # Check for margin calculation failure
        if margin_info and margin_info.get("reason") == "margin_calculation_failed":
            self.last_rejection_reason = "MARGIN_CALCULATION_FAILED"
            self.last_rejection_detail = margin_info
            context = build_log_context(
                symbol=self.instrument.get("symbol"),
                reason="MARGIN_CALCULATION_FAILED",
                error=margin_info.get("error"),
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        if capped_qty <= 0:
            self.last_rejection_reason = "QTY_CAPPED_TO_ZERO"
            self.last_rejection_detail = margin_info or {"risk_qty": risk_based_qty}
            context = build_log_context(
                symbol=self.instrument.get("symbol"),
                reason="QTY_CAPPED_TO_ZERO",
                risk_qty=risk_based_qty,
                capped_qty=capped_qty,
                was_margin_capped=was_margin_capped,
                price=candle.close,
                direction=direction,
                margin_reason=margin_info.get("reason") if margin_info else None,
                margin_error=margin_info.get("error") if margin_info else None,
                available_collateral=margin_info.get("available_collateral") if margin_info else None,
                max_qty_by_margin=margin_info.get("max_qty_by_margin") if margin_info else None,
                cost_per_contract=margin_info.get("cost_per_contract") if margin_info else None,
                margin_per_contract=margin_info.get("margin_per_contract") if margin_info else None,
                fee_per_contract=margin_info.get("fee_per_contract") if margin_info else None,
                margin_rate=margin_info.get("margin_rate") if margin_info else None,
                margin_method=margin_info.get("calculation_method") if margin_info else None,
                balance_trace=margin_info.get("balance_trace") if margin_info else None,
                qty_step=self.qty_step,
                min_qty=self.min_qty,
                max_qty=self.max_qty,
                min_notional=self.min_notional,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        order_intent_id = str(uuid.uuid4())
        trade_id = str(uuid.uuid4())
        qty_raw = capped_qty
        requested_qty = capped_qty
        normalization = normalize_qty_with_constraints(self.amount_constraints, requested_qty)
        if not normalization.ok:
            self.last_rejection_reason = normalization.rejected_reason or "QTY_CONSTRAINT_FAILED"
            self.last_rejection_detail = normalization.to_log_dict()
            context = merge_log_context(
                build_log_context(
                    symbol=self.instrument.get("symbol"),
                    reason=self.last_rejection_reason,
                ),
                build_log_context(**normalization.to_log_dict()),
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None
        requested_qty = float(normalization.qty_final)

        fill_result: Optional[FillResult] = None
        base_currency = None
        quote_currency = None
        side = "buy" if direction == "long" else "sell"
        use_wallet_execution = bool(self.execution_adapter and self._wallet_gateway)
        if use_wallet_execution:
            fill_result, rejection = self.execution_adapter.fill_market(
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
                context = build_log_context(
                    symbol=self.instrument.get("symbol"),
                    reason=rejection.reason,
                    requested_qty=requested_qty,
                    price=round(candle.close, 4),
                )
                logger.warning(with_log_context("entry_rejected", context))
                return None

        if use_wallet_execution:
            if not fill_result:
                raise ValueError("Execution adapter did not return a fill for spot execution")
            base_currency, quote_currency = self._resolve_base_quote()
            allowed, reason, payload = self._wallet_gateway.can_apply(
                side=side,
                base_currency=base_currency,
                quote_currency=quote_currency,
                qty=fill_result.filled_qty,
                qty_raw=qty_raw,
                qty_final=fill_result.filled_qty,
                notional=fill_result.notional,
                fee=fill_result.fee,
                short_requires_borrow=bool(self.short_requires_borrow),
                instrument=self.instrument,
            )
            if not allowed:
                self._wallet_gateway.reject(reason, payload)
                self.last_rejection_reason = reason
                self.last_rejection_detail = payload
                context = merge_log_context(
                    build_log_context(
                        symbol=self.instrument.get("symbol"),
                        reason=reason,
                        qty=fill_result.filled_qty,
                        price=round(fill_result.fill_price, 4),
                    ),
                    build_log_context(
                        available=payload.get("available"),
                        required=payload.get("required"),
                        required_used=payload.get("required_used"),
                        required_full_notional=payload.get("required_full_notional"),
                        available_collateral=payload.get("available_collateral"),
                        currency=payload.get("currency"),
                        notional=payload.get("notional"),
                        fee=payload.get("fee"),
                        qty_raw=payload.get("qty_raw"),
                        qty_final=payload.get("qty_final"),
                        instrument_type=payload.get("instrument_type"),
                        margin_rate=payload.get("margin_rate"),
                        required_margin=payload.get("required_margin"),
                        fee_buffer=payload.get("fee_buffer"),
                        safety_buffer=payload.get("safety_buffer"),
                        margin_method=payload.get("margin_method"),
                        margin_session=payload.get("margin_session"),
                        margin_leg=payload.get("margin_leg"),
                        margin_rate_source_path=payload.get("margin_rate_source_path"),
                        margin_total_required=payload.get("margin_total_required"),
                        margin_calc_type=payload.get("margin_calc_type"),
                        shortfall=payload.get("shortfall"),
                        margin_error=payload.get("margin_error"),
                    ),
                )
                logger.warning(with_log_context("wallet_entry_rejected", context))
                return None
            accounting_mode = None
            inst_type = resolve_instrument_type(self.instrument)
            if inst_type in (InstrumentType.FUTURE, InstrumentType.SWAP):
                accounting_mode = "margin"
            self._wallet_gateway.apply_fill(
                event_type="ENTRY_FILL",
                side=side,
                base_currency=base_currency,
                quote_currency=quote_currency,
                qty=fill_result.filled_qty,
                price=fill_result.fill_price,
                fee=fill_result.fee,
                notional=fill_result.notional,
                symbol=self.instrument.get("symbol"),
                trade_id=trade_id,
                position_direction=direction,
                accounting_mode=accounting_mode,
                realized_pnl=0.0,
            )

        total_contracts = fill_result.filled_qty if fill_result else requested_qty

        # Build position components
        stop_price = self._calculate_stop_price(candle, direction, r_ticks)
        legs = self._build_legs(
            candle,
            direction,
            r_ticks,
            total_contracts,
            qty_raw=qty_raw,
            qty_final=total_contracts,
            order_intent_id=order_intent_id,
            side=side,
        )
        if not legs:
            rounded_qty = (
                self._floor_to_step(requested_qty, self.qty_step)
                if self.qty_step not in (None, 0)
                else requested_qty
            )
            rejection_reason = "QTY_ROUNDS_TO_ZERO" if rounded_qty <= 0 else "TP_LEGS_EMPTY"
            self.last_rejection_reason = rejection_reason
            self.last_rejection_detail = {
                "requested_qty": requested_qty,
                "rounded_qty": rounded_qty,
                "symbol": self.instrument.get("symbol"),
                "qty_step": self.qty_step,
                "min_qty": self.min_qty,
                "min_notional": self.min_notional,
                "tp_leg_count": len(self.orders),
                "tp_allocation": self._last_tp_allocation,
            }
            context = build_log_context(
                symbol=self.instrument.get("symbol"),
                reason=rejection_reason,
                requested_qty=requested_qty,
                rounded_qty=rounded_qty,
                qty_step=self.qty_step,
                min_qty=self.min_qty,
                min_notional=self.min_notional,
                tp_leg_count=len(self.orders),
                tp_allocation=self._last_tp_allocation,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return None

        # Configure stop management
        runtime_stop_adjustments = self._build_stop_adjustments(legs, r_ticks)
        breakeven_ticks = 0.0 if runtime_stop_adjustments else self._breakeven_threshold(legs, r_ticks)
        trailing_activation_ticks = self._trailing_activation_ticks(legs, r_ticks)
        trailing_distance_ticks = self._trailing_distance_ticks(atr_at_entry)

        # Get trailing config
        self.trailing_config = self.template.get("trailing_stop") if isinstance(self.template.get("trailing_stop"), Mapping) else {}
        self._last_tp_allocation: Optional[Dict[str, Any]] = None

        # Create position
        position = LadderPosition(
            entry_time=candle.time,
            entry_price=fill_result.fill_price if fill_result else candle.close,
            direction=direction,
            stop_price=stop_price,
            tick_size=self.tick_size,
            execution_model=self.execution_model if use_wallet_execution else None,
            execution_adapter=self.execution_adapter if use_wallet_execution else None,
            wallet_gateway=self._wallet_gateway if use_wallet_execution else None,
            base_currency=base_currency,
            quote_currency_code=quote_currency,
            legs=legs,
            breakeven_trigger_ticks=breakeven_ticks,
            tick_value=self.tick_value,
            contract_size=self.contract_size,
            maker_fee_rate=self.maker_fee,
            taker_fee_rate=self.taker_fee,
            quote_currency=self.quote_currency,
            short_requires_borrow=bool(self.short_requires_borrow),
            instrument=self.instrument if use_wallet_execution else None,
            atr_at_entry=atr_at_entry,
            r_multiple_at_entry=self.r_multiple,
            r_value=r_value,
            r_ticks=r_ticks,
            trailing_activation_ticks=trailing_activation_ticks,
            trailing_distance_ticks=trailing_distance_ticks,
            trailing_atr_multiple=float(self.trailing_config.get("atr_multiplier") or 0.0),
            pre_entry_context=getattr(candle, "lookback_15", None),
            stop_adjustments=runtime_stop_adjustments,
            trade_id=trade_id,
        )
        if fill_result:
            position.apply_entry_fee(fill_result.fee)
        else:
            position.register_entry_fee()
        return position

    def maybe_enter(self, candle: Candle, direction: Optional[str]) -> Optional[LadderPosition]:
        if direction is None or self.active_trade is not None:
            return None
        if not self.execution_adapter:
            raise ValueError("Execution adapter is required for trade execution")
        if not self._wallet_gateway:
            raise ValueError("Wallet gateway is required for trade execution")
        if direction == "short" and not self.can_short:
            self.last_rejection_reason = "CAN_SHORT_DISABLED"
            self.last_rejection_detail = {"symbol": self.instrument.get("symbol"), "direction": direction}
            context = build_log_context(
                symbol=self.instrument.get("symbol"),
                reason="CAN_SHORT_DISABLED",
                direction=direction,
            )
            logger.warning(with_log_context("short_entry_rejected", context))
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
    "DecisionLedgerEvent",
    "StrategySignal",
    "Leg",
    "LadderPosition",
    "LadderRiskEngine",
    "coerce_float",
    "isoformat",
    "timeframe_duration",
    "timeframe_to_seconds",
]

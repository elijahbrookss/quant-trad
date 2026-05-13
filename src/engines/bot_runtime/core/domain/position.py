"""Ladder position domain state and per-bar progression."""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import risk as risk_math

from utils.log_context import build_log_context, with_log_context
from ..execution import FillRejection, FillResult
from ..execution_adapter import ExecutionAdapter
from ..events import ExitSettlementPayload
from ..execution_profile import SeriesExecutionProfile
from ..exit_settlement import ExitSettlement, ExitSettlementService
from ..fees import executed_fee, executed_notional
from ..margin import resolve_instrument_type, InstrumentType
from ..wallet_gateway import WalletGateway
from .models import Candle, Leg
from .time_utils import isoformat

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..execution import SpotExecutionModel


class SameBarResolutionPolicy(str, Enum):
    """Policy for bars whose range contains both a target and the active stop."""

    TARGET_FIRST = "target_first"
    PESSIMISTIC_STOP = "pessimistic_stop"

    @classmethod
    def normalize(cls, value: Optional[object]) -> "SameBarResolutionPolicy":
        if isinstance(value, SameBarResolutionPolicy):
            return value
        normalized = str(value or cls.TARGET_FIRST.value).strip().lower()
        if normalized in {"pessimistic", "pessimistic_stop", "stop_first", "stop"}:
            return cls.PESSIMISTIC_STOP
        return cls.TARGET_FIRST


@dataclass
class LadderPosition:
    """Track laddered take-profit and stop-loss behaviour for a trade."""

    entry_time: datetime
    entry_price: float
    direction: str
    stop_price: float
    tick_size: float
    entry_order: Optional[Dict[str, Any]] = None
    entry_outcome: Optional[Dict[str, Any]] = None
    execution_model: Optional["SpotExecutionModel"] = None
    execution_adapter: Optional[ExecutionAdapter] = None
    wallet_gateway: Optional[WalletGateway] = None
    exit_settlement: Optional[ExitSettlement] = None
    base_currency: Optional[str] = None
    quote_currency_code: Optional[str] = None
    legs: List[Leg] = field(default_factory=list)
    breakeven_trigger_ticks: float = 20.0
    tick_value: float = 1.0
    contract_size: float = 1.0
    maker_fee_rate: float = 0.0
    taker_fee_rate: float = 0.0
    fee_source: str = "template_or_instrument"
    fee_version: Optional[str] = None
    quote_currency: str = "USD"
    short_requires_borrow: bool = False
    instrument: Optional[Dict[str, Any]] = None  # For margin-based validation
    execution_profile: Optional[SeriesExecutionProfile] = None
    signal_id: Optional[str] = None
    decision_id: Optional[str] = None
    strategy_id: Optional[str] = None
    bar_time: Optional[datetime] = None
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
    wallet_fill_metadata: Dict[str, Any] = field(default_factory=dict)
    position_commit_seq: int = 0
    stop_adjustments: List[Dict[str, Any]] = field(default_factory=list)
    close_reason: Optional[str] = None

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

    def _exit_settlement(self) -> ExitSettlement:
        if self.exit_settlement:
            return self.exit_settlement
        return ExitSettlementService(self.wallet_gateway)

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
        if self.execution_profile is not None:
            return self.execution_profile.accounting_mode
        inst_type = resolve_instrument_type(self.instrument or {})
        if inst_type in (InstrumentType.FUTURE, InstrumentType.SWAP):
            return "margin"
        return None

    def _apply_leg_fills(self, candle: Candle) -> List[Dict[str, Any]]:
        """Check if candle price hits any target levels and process fills."""
        events: List[Dict[str, Any]] = []
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
            side = "sell" if self.direction == "long" else "buy"
            if self._uses_wallet_execution():
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
            fee_value = (
                float(fill_result.fee)
                if fill_result
                else self._fee_for_fill(exit_price, exit_qty)
            )
            notional = float(fill_result.notional) if fill_result else self._notional_for_fill(exit_price, exit_qty)
            self._apply_fee_amount(fee_value)
            fee_metadata = self._fee_metadata_for_fill(fill_result)

            settlement_payload: ExitSettlementPayload = {
                "event_type": "EXIT_FILL",
                "exit_kind": "TARGET",
                "side": side,
                "base_currency": self.base_currency or "",
                "quote_currency": self.quote_currency_code or "",
                "qty": exit_qty,
                "price": exit_price,
                "fee": fee_value,
                "notional": notional,
                "fee_rate": fee_metadata["fee_rate"],
                "fee_type": fee_metadata["fee_type"],
                "fee_source": fee_metadata["fee_source"],
                "fee_version": fee_metadata["fee_version"],
                "trade_id": self.trade_id,
                "leg_id": leg.leg_id or "",
                "position_direction": self.direction,
                "accounting_mode": self._accounting_mode(),
                "realized_pnl": pnl,
                "allow_short_borrow": bool(self.short_requires_borrow),
                "instrument": self.instrument or {},
            }

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
                    "notional": notional,
                    "fee_paid": fee_value,
                    "fee_rate": fee_metadata["fee_rate"],
                    "fee_type": fee_metadata["fee_type"],
                    "fee_source": fee_metadata["fee_source"],
                    "fee_version": fee_metadata["fee_version"],
                    "settlement": settlement_payload,
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

    def _apply_stop(self, candle: Candle) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
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
                side = "sell" if self.direction == "long" else "buy"
                if self._uses_wallet_execution():
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
                fee_value = (
                    float(fill_result.fee)
                    if fill_result
                    else self._fee_for_fill(exit_price, exit_qty)
                )
                notional = float(fill_result.notional) if fill_result else self._notional_for_fill(exit_price, exit_qty)
                self._apply_fee_amount(fee_value)
                fee_metadata = self._fee_metadata_for_fill(fill_result)
                settlement_payload: ExitSettlementPayload = {
                    "event_type": "EXIT_FILL",
                    "exit_kind": "STOP",
                    "side": side,
                    "base_currency": self.base_currency or "",
                    "quote_currency": self.quote_currency_code or "",
                    "qty": exit_qty,
                    "price": exit_price,
                    "fee": fee_value,
                    "notional": notional,
                    "fee_rate": fee_metadata["fee_rate"],
                    "fee_type": fee_metadata["fee_type"],
                    "fee_source": fee_metadata["fee_source"],
                    "fee_version": fee_metadata["fee_version"],
                    "trade_id": self.trade_id,
                    "leg_id": leg.leg_id or "",
                    "position_direction": self.direction,
                    "accounting_mode": self._accounting_mode(),
                    "realized_pnl": pnl,
                    "allow_short_borrow": bool(self.short_requires_borrow),
                    "instrument": self.instrument or {},
                }
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
                        "notional": notional,
                        "fee_paid": fee_value,
                        "fee_rate": fee_metadata["fee_rate"],
                        "fee_type": fee_metadata["fee_type"],
                        "fee_source": fee_metadata["fee_source"],
                        "fee_version": fee_metadata["fee_version"],
                        "settlement": settlement_payload,
                    }
                )
            self.closed_at = candle.time
        elif all(leg.status != "open" for leg in self.legs):
            self.closed_at = candle.time
        return events

    def hits_open_target(self, candle: Candle) -> bool:
        """Return True when the bar range reaches any open take-profit target."""

        for leg in self.legs:
            if leg.status != "open":
                continue
            if self.direction == "long" and candle.high >= leg.target_price:
                return True
            if self.direction == "short" and candle.low <= leg.target_price:
                return True
        return False

    def hits_active_stop(self, candle: Candle) -> bool:
        """Return True when the bar range reaches the active stop."""

        if self.direction == "long":
            return candle.low <= self.stop_price
        if self.direction == "short":
            return candle.high >= self.stop_price
        return False

    def hits_target_and_stop(self, candle: Candle) -> bool:
        """Return True when TP and stop are both inside the same bar range."""

        return self.hits_open_target(candle) and self.hits_active_stop(candle)

    def apply_bar(
        self,
        candle: Candle,
        *,
        same_bar_policy: SameBarResolutionPolicy | str = SameBarResolutionPolicy.TARGET_FIRST,
    ) -> List[Dict[str, Any]]:
        """Advance the position with the latest candle."""

        events: List[Dict[str, Any]] = []
        policy = SameBarResolutionPolicy.normalize(same_bar_policy)
        same_bar_target_and_stop = self.hits_target_and_stop(candle)
        self._update_excursions(candle)

        if policy == SameBarResolutionPolicy.PESSIMISTIC_STOP and same_bar_target_and_stop:
            stop_events = self._apply_stop(candle)
        else:
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

    def force_close_at_backtest_end(
        self,
        candle: Candle,
        *,
        reason_code: str = "BACKTEST_END",
    ) -> List[Dict[str, Any]]:
        """Close any remaining open legs at the final executable backtest price."""

        if not self.is_active():
            return []

        events: List[Dict[str, Any]] = []
        exit_price_source = float(candle.close)
        event_time = isoformat(candle.time)
        side = "sell" if self.direction == "long" else "buy"
        normalized_reason = str(reason_code or "BACKTEST_END").strip().upper() or "BACKTEST_END"

        for leg in self.legs:
            if leg.status != "open":
                continue
            fill_result = None
            if self._uses_wallet_execution():
                fill_result, rejection = self._execute_spot_fill(
                    exit_price_source,
                    leg.contracts,
                    side=side,
                )
                if rejection:
                    context = build_log_context(
                        trade_id=self.trade_id,
                        leg_id=leg.leg_id,
                        leg=leg.name,
                        reason=rejection.reason,
                        price=round(exit_price_source, 4),
                        direction=self.direction,
                    )
                    logger.error(with_log_context("backtest_terminal_close_rejected", context))
                    raise RuntimeError(
                        "backtest_terminal_close_rejected "
                        f"trade_id={self.trade_id} leg_id={leg.leg_id} reason={rejection.reason}"
                    )

            exit_price = fill_result.fill_price if fill_result else exit_price_source
            exit_qty = fill_result.filled_qty if fill_result else leg.contracts
            pnl = self._pnl_for_exit(exit_price, exit_qty)
            leg.status = "backtest_end"
            leg.exit_price = exit_price
            leg.exit_time = event_time
            leg.exit_created_at = isoformat(datetime.now(timezone.utc))
            leg.contracts = exit_qty
            leg.pnl = pnl
            self._record_pnl(pnl)
            fee_value = (
                float(fill_result.fee)
                if fill_result
                else self._fee_for_fill(exit_price, exit_qty)
            )
            notional = float(fill_result.notional) if fill_result else self._notional_for_fill(exit_price, exit_qty)
            self._apply_fee_amount(fee_value)
            fee_metadata = self._fee_metadata_for_fill(fill_result)
            settlement_payload: ExitSettlementPayload = {
                "event_type": "EXIT_FILL",
                "exit_kind": "CLOSE",
                "side": side,
                "base_currency": self.base_currency or "",
                "quote_currency": self.quote_currency_code or "",
                "qty": exit_qty,
                "price": exit_price,
                "fee": fee_value,
                "notional": notional,
                "fee_rate": fee_metadata["fee_rate"],
                "fee_type": fee_metadata["fee_type"],
                "fee_source": fee_metadata["fee_source"],
                "fee_version": fee_metadata["fee_version"],
                "trade_id": self.trade_id,
                "leg_id": leg.leg_id or "",
                "position_direction": self.direction,
                "accounting_mode": self._accounting_mode(),
                "realized_pnl": pnl,
                "allow_short_borrow": bool(self.short_requires_borrow),
                "instrument": self.instrument or {},
            }
            events.append(
                {
                    "type": "backtest_end",
                    "trade_id": self.trade_id,
                    "price": round(exit_price, 4),
                    "time": event_time,
                    "currency": self.quote_currency,
                    "leg": leg.name,
                    "leg_id": leg.leg_id,
                    "contracts": exit_qty,
                    "pnl": round(pnl, 4),
                    "ticks": round(self._ticks_from_entry(exit_price), 4),
                    "direction": self.direction,
                    "notional": notional,
                    "fee_paid": fee_value,
                    "fee_rate": fee_metadata["fee_rate"],
                    "fee_type": fee_metadata["fee_type"],
                    "fee_source": fee_metadata["fee_source"],
                    "fee_version": fee_metadata["fee_version"],
                    "reason_code": normalized_reason,
                    "close_reason": normalized_reason,
                    "settlement": settlement_payload,
                }
            )

        if events or all(leg.status != "open" for leg in self.legs):
            self.closed_at = candle.time
            self.close_reason = normalized_reason
            events.append(
                {
                    "type": "close",
                    "trade_id": self.trade_id,
                    "time": event_time,
                    "gross_pnl": round(self.gross_pnl, 4),
                    "fees_paid": round(self.fees_paid, 4),
                    "net_pnl": round(self.net_pnl, 4),
                    "currency": self.quote_currency,
                    "contracts": sum(max(leg.contracts, 0) for leg in self.legs),
                    "direction": self.direction,
                    "metrics": self._metrics_snapshot(),
                    "reason_code": normalized_reason,
                    "close_reason": normalized_reason,
                    "exit_price": round(exit_price_source, 4),
                }
            )
        return events

    def is_active(self) -> bool:
        return self.closed_at is None

    def serialize(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "trade_id": self.trade_id,
            "created_at": self.created_at,
            "entry_time": isoformat(self.entry_time),
            "bar_time": isoformat(self.bar_time or self.entry_time),
            "strategy_id": self.strategy_id,
            "signal_id": self.signal_id,
            "decision_id": self.decision_id,
            "entry_price": round(self.entry_price, 4),
            "entry_order": dict(self.entry_order or {}),
            "entry_outcome": dict(self.entry_outcome or {}),
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
            "position_commit_seq": int(self.position_commit_seq),
            "position_commit_seq_status": "position_scoped",
        }
        if self.close_reason:
            payload["close_reason"] = self.close_reason
            payload["reason_code"] = self.close_reason
            payload["exit_time"] = isoformat(self.closed_at)
            closed_legs = [leg for leg in self.legs if leg.status != "open" and leg.exit_price is not None]
            if closed_legs:
                total_contracts = sum(max(float(leg.contracts or 0.0), 0.0) for leg in closed_legs)
                if total_contracts > 0:
                    weighted_exit = sum(
                        float(leg.exit_price or 0.0) * max(float(leg.contracts or 0.0), 0.0)
                        for leg in closed_legs
                    )
                    payload["exit_price"] = round(
                        weighted_exit / total_contracts,
                        4,
                    )
        return payload

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
        fee = self._fee_for_fill(price, contracts)
        if fee:
            self.fees_paid += fee
            self._update_net()

    def _notional_for_fill(self, price: float, contracts: float) -> float:
        return executed_notional(
            price=price,
            quantity=contracts,
            contract_size=self.contract_size,
        )

    def _fee_for_fill(self, price: float, contracts: float) -> float:
        return executed_fee(
            price=price,
            quantity=contracts,
            contract_size=self.contract_size,
            fee_rate=self.taker_fee_rate or 0.0,
        )

    def _fee_metadata_for_fill(self, fill: Optional[FillResult]) -> Dict[str, Any]:
        fee_rate = getattr(fill, "fee_rate", None) if fill is not None else None
        fee_type = getattr(fill, "fee_role", None) if fill is not None else None
        fee_source = getattr(fill, "fee_source", None) if fill is not None else None
        fee_version = getattr(fill, "fee_version", None) if fill is not None else None
        return {
            "fee_rate": float(fee_rate if fee_rate is not None else self.taker_fee_rate or 0.0),
            "fee_type": str(fee_type or "taker"),
            "fee_source": str(fee_source or self.fee_source or "template_or_instrument"),
            "fee_version": fee_version if fee_version is not None else self.fee_version,
        }

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

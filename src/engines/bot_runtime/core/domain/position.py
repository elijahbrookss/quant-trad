"""Ladder position domain state and per-bar progression."""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

import risk as risk_math

from utils.log_context import build_log_context, merge_log_context, with_log_context
from ..execution import FillRejection, FillResult
from ..execution_adapter import ExecutionAdapter
from ..events import ExitSettlementPayload
from ..execution_profile import SeriesExecutionProfile
from ..exit_settlement import ExitSettlement, ExitSettlementService
from ..margin import resolve_instrument_type, InstrumentType
from ..wallet_gateway import WalletGateway
from .models import Candle, Leg
from .time_utils import isoformat

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..execution import SpotExecutionModel


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
    quote_currency: str = "USD"
    short_requires_borrow: bool = False
    instrument: Optional[Dict[str, Any]] = None  # For margin-based validation
    execution_profile: Optional[SeriesExecutionProfile] = None
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
            execution_profile=self.execution_profile,
            reserve=False,
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
            fee_value = (
                float(fill_result.fee)
                if fill_result
                else abs(exit_price * self.contract_size * exit_qty) * float(self.taker_fee_rate or 0.0)
            )
            notional = float(fill_result.notional) if fill_result else abs(exit_price * self.contract_size * exit_qty)
            if fill_result:
                self._apply_fee_amount(fill_result.fee)
            else:
                self._apply_fee(exit_price, exit_qty)

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
                fee_value = (
                    float(fill_result.fee)
                    if fill_result
                    else abs(exit_price * self.contract_size * exit_qty) * float(self.taker_fee_rate or 0.0)
                )
                notional = float(fill_result.notional) if fill_result else abs(exit_price * self.contract_size * exit_qty)
                if fill_result:
                    self._apply_fee_amount(fill_result.fee)
                else:
                    self._apply_fee(exit_price, exit_qty)
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
                        "settlement": settlement_payload,
                    }
                )
            self.closed_at = candle.time
        elif all(leg.status != "open" for leg in self.legs):
            self.closed_at = candle.time
        return events

    def apply_bar(self, candle: Candle) -> List[Dict[str, Any]]:
        """Advance the position with the latest candle."""

        events: List[Dict[str, Any]] = []
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


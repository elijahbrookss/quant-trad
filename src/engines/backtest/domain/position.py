"""Ladder position management for backtest engine."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from risk import clamp_stop, price_from_r

from .models import Candle, Leg
from .utils import isoformat


@dataclass
class LadderPosition:
    """Track laddered take-profit and stop-loss behaviour for a trade."""

    entry_time: datetime
    entry_price: float
    direction: str
    stop_price: float
    tick_size: float
    instrument_type: Optional[str] = None
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
        events: List[Dict[str, str]] = []
        ordered = sorted(self.legs, key=lambda leg: leg.ticks)
        if self.direction == "long":
            for leg in ordered:
                if leg.status != "open":
                    continue
                if candle.high >= leg.target_price:
                    leg.status = "target"
                    leg.exit_price = leg.target_price
                    leg.exit_time = isoformat(candle.time)
                    pnl = self._pnl_for_exit(leg.target_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(leg.target_price, leg.contracts)
                    events.append(
                        {
                            "type": "target",
                            "leg": leg.name,
                            "leg_id": leg.leg_id,
                            "trade_id": self.trade_id,
                            "price": round(leg.target_price, 4),
                            "time": leg.exit_time,
                            "pnl": round(pnl, 4),
                            "currency": self.quote_currency,
                            "contracts": leg.contracts,
                            "ticks": leg.ticks,
                            "direction": self.direction,
                        }
                    )
                    if not self.moved_to_breakeven and leg.ticks >= self.breakeven_trigger_ticks:
                        self.stop_price = self.entry_price
                        self.moved_to_breakeven = True
        else:
            for leg in ordered:
                if leg.status != "open":
                    continue
                if candle.low <= leg.target_price:
                    leg.status = "target"
                    leg.exit_price = leg.target_price
                    leg.exit_time = isoformat(candle.time)
                    pnl = self._pnl_for_exit(leg.target_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(leg.target_price, leg.contracts)
                    events.append(
                        {
                            "type": "target",
                            "leg": leg.name,
                            "trade_id": self.trade_id,
                            "price": round(leg.target_price, 4),
                            "time": leg.exit_time,
                            "pnl": round(pnl, 4),
                            "currency": self.quote_currency,
                            "contracts": leg.contracts,
                            "ticks": leg.ticks,
                            "direction": self.direction,
                        }
                    )
                    if not self.moved_to_breakeven and leg.ticks >= self.breakeven_trigger_ticks:
                        self.stop_price = self.entry_price
                        self.moved_to_breakeven = True
        return events

    def _maybe_move_breakeven(self) -> None:
        if self.moved_to_breakeven or self.trailing_active:
            return
        if self.breakeven_trigger_ticks and self.mfe_ticks >= self.breakeven_trigger_ticks:
            self.stop_price = clamp_stop(self.stop_price, self.entry_price, self.direction)
            self.moved_to_breakeven = True

    def _maybe_apply_stop_adjustments(self) -> None:
        """Apply one-time stop adjustments while trailing is inactive.

        Stop adjustments are evaluated on every bar until trailing activates; once
        trailing is active this method is skipped to honor the priority rule.
        """

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
                    candidate = price_from_r(self.entry_price, self.direction, float(self.r_value), float(action_r))

            if candidate is None:
                continue

            self.stop_price = clamp_stop(self.stop_price, candidate, self.direction)
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
        candidate = (
            self.best_price - distance_price if self.direction == "long" else self.best_price + distance_price
        )
        self.stop_price = clamp_stop(self.stop_price, candidate, self.direction)

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
                if leg.status == "open":
                    leg.status = "stop"
                    leg.exit_price = self.stop_price
                    leg.exit_time = isoformat(candle.time)
                    pnl = self._pnl_for_exit(self.stop_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(self.stop_price, leg.contracts)
                    events.append(
                        {
                            "type": "stop",
                            "trade_id": self.trade_id,
                            "price": round(self.stop_price, 4),
                            "time": leg.exit_time,
                            "currency": self.quote_currency,
                            "leg": leg.name,
                            "leg_id": leg.leg_id,
                            "contracts": leg.contracts,
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

    def _pnl_for_exit(self, exit_price: float, contracts: int) -> float:
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

    def _apply_fee(self, price: float, contracts: int) -> None:
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

    def _metrics_snapshot(self) -> Dict[str, object]:
        mae_r = (self.mae_ticks / self.r_ticks) if self.r_ticks else None
        mfe_r = (self.mfe_ticks / self.r_ticks) if self.r_ticks else None
        return {
            "atr_at_entry": None if self.atr_at_entry is None else round(self.atr_at_entry, 6),
            "r_multiple_at_entry": self.r_multiple_at_entry,
            "r_value": None if self.r_value is None else round(self.r_value, 6),
            "r_ticks": None if self.r_ticks is None else round(self.r_ticks, 4),
            "mae_ticks": round(self.mae_ticks, 4),
            "mfe_ticks": round(self.mfe_ticks, 4),
            "mae_r": None if mae_r is None else round(mae_r, 6),
            "mfe_r": None if mfe_r is None else round(mfe_r, 6),
            "bars_held": self.bars_held,
            "pre_entry_context": dict(self.pre_entry_context or {}),
        }

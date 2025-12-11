"""Ladder risk engine for creating and managing backtest positions."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from atm import merge_templates
from risk import price_from_r, r_value_from_atr, ticks_for_r, ticks_from_entry

from .models import Candle, Leg
from .position import LadderPosition
from .utils import coerce_float

logger = logging.getLogger(__name__)

DEFAULT_RISK = {
    "contracts": 3,
    "targets": [20, 40, 60],
    "stop_ticks": 30,
    "breakeven_trigger_ticks": 20,
    "tick_size": 0.01,
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
        config_tick = coerce_float(provided_template.get("tick_size"))
        instrument_tick = coerce_float(self.instrument.get("tick_size"))
        fallback_tick = coerce_float(DEFAULT_RISK.get("tick_size"), 0.01)
        if config_tick not in (None, 0):
            self.tick_size = float(config_tick)
        elif instrument_tick not in (None, 0):
            self.tick_size = float(instrument_tick)
        elif fallback_tick not in (None, 0):
            self.tick_size = float(fallback_tick)
        else:
            self.tick_size = 0.01
        self.stop_ticks = int(self.template.get("stop_ticks") or DEFAULT_RISK["stop_ticks"])

        # Schema v2: Read from nested initial_stop object
        initial_stop_config = self.template.get("initial_stop")
        if not isinstance(initial_stop_config, dict):
            initial_stop_config = {}
        self.r_multiple = float(initial_stop_config.get("atr_multiplier") or 1.0)

        # Schema v2: Read from nested risk object
        risk_config = self.template.get("risk")
        if not isinstance(risk_config, dict):
            risk_config = {}
        self.base_risk_per_trade = coerce_float(risk_config.get("base_risk_per_trade"))
        self.stop_r_multiple = coerce_float(self.template.get("stop_r_multiple"))

        # Stop adjustments (v2 nested format)
        self.stop_adjustments_config: List[Dict[str, Any]] = list(self.template.get("stop_adjustments") or [])

        config_contract = coerce_float(self.template.get("contract_size"))
        instrument_contract = coerce_float(self.instrument.get("contract_size"))
        self.contract_size = (
            float(config_contract)
            if config_contract not in (None, 0)
            else float(instrument_contract)
            if instrument_contract not in (None, 0)
            else 1.0
        )
        config_tick_value = coerce_float(self.template.get("tick_value"))
        instrument_tick_value = coerce_float(self.instrument.get("tick_value"))
        if config_tick_value not in (None, 0):
            tick_value = float(config_tick_value)
        elif instrument_tick_value not in (None, 0):
            tick_value = float(instrument_tick_value)
        else:
            tick_value = self.tick_size * self.contract_size
        self.tick_value = float(tick_value or self.tick_size)

        # Read risk mode from nested initial_stop config
        risk_mode = str(initial_stop_config.get("mode") or "atr").lower()
        self.risk_unit_mode = risk_mode if risk_mode in {"atr", "ticks"} else "atr"
        self.ticks_stop = int(
            self.template.get("ticks_stop")
            or self.template.get("stop_ticks")
            or DEFAULT_RISK.get("stop_ticks")
            or 1
        )
        self.global_risk_multiplier = coerce_float(risk_config.get("global_risk_multiplier"), 1.0) or 1.0
        self.instrument_risk_multiplier = coerce_float(self.instrument.get("risk_multiplier"), 1.0) or 1.0

        self.orders = self._orders_from_template()
        self.targets = [int(order.get("ticks") or 0) for order in self.orders]
        quote_value = self.template.get("quote_currency") or self.instrument.get("quote_currency") or "USD"
        self.quote_currency = str(quote_value).upper()
        config_maker = coerce_float(self.template.get("maker_fee_rate"))
        instrument_maker = coerce_float(self.instrument.get("maker_fee_rate"), 0.0)
        config_taker = coerce_float(self.template.get("taker_fee_rate"))
        instrument_taker = coerce_float(self.instrument.get("taker_fee_rate"), 0.0)
        self.maker_fee = (
            float(config_maker)
            if config_maker is not None
            else float(instrument_maker or 0.0)
        )
        self.taker_fee = (
            float(config_taker)
            if config_taker is not None
            else float(instrument_taker or 0.0)
        )
        self.active_trade: Optional[LadderPosition] = None
        self.trades: List[LadderPosition] = []

        logger.info(
            "ladder_risk_configured | targets=%s | stop_ticks=%s | tick=%.5f | instrument=%s",
            ",".join(str(order.get("ticks") or order.get("r_multiple") or "?") for order in self.orders),
            self.stop_ticks,
            self.tick_size,
            self.instrument.get("symbol"),
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
            # Schema v2: size_fraction (0-1 range)
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
                    "contracts": max(contracts, 1),
                    "size_percent": size_percent,
                }
            )
        if orders:
            return orders

        fallback_targets: Sequence[int] = (
            self.template.get("targets")
            or DEFAULT_RISK.get("targets")
            or [20, 40, 60]
        )
        total_contracts = int(self.template.get("contracts") or len(fallback_targets) or 1)
        distribution = self._distribute_contracts(len(fallback_targets), total_contracts)
        built: List[Dict[str, Any]] = []
        for idx, ticks in enumerate(fallback_targets):
            built.append(
                {
                    "label": f"TP +{int(ticks)}",
                    "ticks": int(ticks),
                    "contracts": distribution[idx] if idx < len(distribution) else 1,
                }
            )
        return built

    @staticmethod
    def _distribute_contracts(count: int, total: int) -> List[int]:
        if count <= 0:
            return []
        slots = [0 for _ in range(count)]
        total = total if total > 0 else count
        for idx in range(total):
            slots[idx % count] += 1
        return slots

    def _point_value(self) -> float:
        if self.tick_value not in (None, 0):
            return float(self.tick_value)
        if self.contract_size not in (None, 0):
            return float(self.contract_size)
        return 1.0

    def _apply_quantity_constraints(self, qty: float) -> float:
        min_qty = coerce_float(
            self.instrument.get("min_qty")
            or self.instrument.get("min_order_size")
            or self.instrument.get("min_quantity")
        )
        qty_step = coerce_float(
            self.instrument.get("qty_step")
            or self.instrument.get("order_step")
            or self.instrument.get("step_size")
        )
        supports_fractional = bool(self.instrument.get("supports_fractional") or self.instrument.get("fractional_orders"))

        normalized = max(qty, 0.0)
        if qty_step not in (None, 0):
            normalized = round(normalized / qty_step) * qty_step
        elif not supports_fractional:
            normalized = float(int(round(normalized)))

        if min_qty not in (None, 0) and normalized < float(min_qty):
            normalized = float(min_qty)

        if not supports_fractional:
            normalized = float(max(int(round(normalized)), 1))
        return normalized

    def _contracts_from_effective_risk(self, stop_distance_price: float) -> Optional[float]:
        if self.base_risk_per_trade in (None, ""):
            return None
        try:
            base_risk = max(float(self.base_risk_per_trade), 0.0)
        except (TypeError, ValueError):
            return None
        effective_multiplier = max(self.global_risk_multiplier or 1.0, 0.0) * max(
            self.instrument_risk_multiplier or 1.0, 0.0
        )
        effective_risk = base_risk * (effective_multiplier or 1.0)
        if effective_risk <= 0:
            return None

        if self.tick_value not in (None, 0) and self.tick_size not in (None, 0):
            ticks = abs(stop_distance_price) / abs(self.tick_size)
            risk_per_unit = ticks * abs(self.tick_value)
        else:
            risk_per_unit = abs(stop_distance_price) * abs(self._point_value())
        if risk_per_unit == 0:
            return None

        qty = effective_risk / risk_per_unit
        return self._apply_quantity_constraints(qty)

    def _orders_with_total(self, total_contracts: Optional[float]) -> List[Dict[str, Any]]:
        base_orders = self._orders_from_template()
        if total_contracts in (None, 0) or not base_orders:
            return base_orders

        total = max(int(round(total_contracts)), len(base_orders))
        distribution = self._distribute_contracts(len(base_orders), total)
        scaled: List[Dict[str, Any]] = []
        for idx, order in enumerate(base_orders):
            payload = dict(order)
            payload["contracts"] = distribution[idx] if idx < len(distribution) else max(int(round(total / len(base_orders))), 1)
            scaled.append(payload)
        return scaled

    def _build_stop_adjustments(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> List[Dict[str, Any]]:
        """Normalise stop adjustment rules for runtime evaluation."""

        runtime_rules: List[Dict[str, Any]] = []
        if r_ticks in (None, 0):
            return runtime_rules

        for idx, entry in enumerate(self.stop_adjustments_config):
            trigger_type = str(entry.get("trigger_type") or "").lower()
            action_type = str(entry.get("action_type") or "").lower()
            if trigger_type not in {"r_multiple", "target_hit"}:
                continue
            if action_type not in {"move_to_breakeven", "move_to_r"}:
                continue

            trigger_ticks: Optional[float] = None
            trigger_target_id: Optional[str] = None

            if trigger_type == "r_multiple":
                trigger_value = coerce_float(entry.get("trigger_value"))
                if trigger_value in (None, 0) or trigger_value <= 0:
                    continue
                trigger_ticks = float(trigger_value) * float(r_ticks)
            else:
                desired = entry.get("trigger_value") or entry.get("trigger_target_id") or entry.get("target_id")
                if desired is None:
                    continue
                for leg in legs:
                    if str(leg.leg_id or leg.name) == str(desired):
                        trigger_target_id = leg.leg_id or leg.name
                        break
                if trigger_target_id is None:
                    continue

            action_r: Optional[float] = None
            if action_type == "move_to_r":
                action_r = coerce_float(entry.get("action_value"))
                if action_r in (None, 0) or action_r <= 0:
                    continue

            runtime_rules.append(
                {
                    "id": entry.get("id") or f"sa-{idx + 1}",
                    "trigger_type": trigger_type,
                    "trigger_ticks": trigger_ticks,
                    "trigger_target_id": trigger_target_id,
                    "action_type": action_type,
                    "action_r": action_r,
                    "fired": False,
                }
            )

        return runtime_rules

    def _new_position(self, candle: Candle, direction: str) -> LadderPosition:
        direction = "long" if direction == "long" else "short"
        atr_at_entry = getattr(candle, "atr", None)
        r_value = r_value_from_atr(atr_at_entry, self.r_multiple)
        r_ticks = ticks_for_r(r_value, self.tick_size)
        stop_price = None
        one_r_distance = None
        stop_distance_price = None

        if self.risk_unit_mode == "ticks":
            stop_distance_price = abs(self.ticks_stop * self.tick_size)
            stop_price = (
                candle.close - stop_distance_price
                if direction == "long"
                else candle.close + stop_distance_price
            )
            one_r_distance = stop_distance_price
            r_value = one_r_distance
            r_ticks = ticks_for_r(one_r_distance, self.tick_size)
        else:
            explicit_stop = coerce_float(self.template.get("stop_price"))
            if explicit_stop not in (None, 0):
                stop_price = float(explicit_stop)
            if self.stop_r_multiple is not None and r_value is not None and stop_price is None:
                stop_price = price_from_r(candle.close, direction, r_value, self.stop_r_multiple)
            if stop_price is None:
                stop_distance = self.stop_ticks * self.tick_size
                stop_price = (
                    candle.close - stop_distance if direction == "long" else candle.close + stop_distance
                )
            stop_distance_price = abs(candle.close - stop_price)
            one_r_distance = stop_distance_price if stop_distance_price is not None else r_value
            if r_value is None:
                r_value = one_r_distance
            if r_ticks is None:
                r_ticks = ticks_for_r(r_value, self.tick_size)

        total_contracts = self._contracts_from_effective_risk(one_r_distance or 0.0)
        orders_for_position = self._orders_with_total(total_contracts)

        legs: List[Leg] = []
        for idx, order in enumerate(orders_for_position):
            ticks = order.get("ticks")
            r_multiple = order.get("r_multiple")
            price = order.get("price")
            target_price: Optional[float] = None
            target_ticks: Optional[int] = ticks if ticks is not None else None
            if r_multiple is not None and r_value is not None:
                target_price = price_from_r(candle.close, direction, r_value, r_multiple)
                computed_ticks = ticks_from_entry(
                    candle.close, target_price, direction, self.tick_size
                )
                target_ticks = int(round(computed_ticks))
            elif ticks is not None:
                distance = ticks * self.tick_size
                target_price = candle.close + distance if direction == "long" else candle.close - distance
            elif price is not None:
                target_price = float(price)
                computed_ticks = ticks_from_entry(
                    candle.close, target_price, direction, self.tick_size
                )
                target_ticks = int(round(computed_ticks))
            if target_price is None:
                continue
            legs.append(
                Leg(
                    name=order.get("label") or f"TP{target_ticks or ticks or idx + 1}",
                    ticks=target_ticks or 0,
                    target_price=target_price,
                    contracts=order.get("contracts", 1),
                    leg_id=order.get("id") or order.get("label") or f"tp-{idx + 1}",
                )
            )
        runtime_stop_adjustments = self._build_stop_adjustments(legs, r_ticks)
        # Stop adjustments replace legacy breakeven/trailing configs
        position = LadderPosition(
            entry_time=candle.time,
            entry_price=candle.close,
            direction=direction,
            stop_price=stop_price,
            tick_size=self.tick_size,
            legs=legs,
            breakeven_trigger_ticks=0.0,  # Handled by stop_adjustments
            tick_value=self.tick_value,
            contract_size=self.contract_size,
            maker_fee_rate=self.maker_fee,
            taker_fee_rate=self.taker_fee,
            quote_currency=self.quote_currency,
            atr_at_entry=atr_at_entry,
            r_multiple_at_entry=self.r_multiple,
            r_value=r_value,
            r_ticks=r_ticks,
            trailing_activation_ticks=None,  # Handled by stop_adjustments
            trailing_distance_ticks=None,  # Handled by stop_adjustments
            trailing_atr_multiple=0.0,  # Handled by stop_adjustments
            pre_entry_context=getattr(candle, "lookback_15", None),
            stop_adjustments=runtime_stop_adjustments,
        )
        position.register_entry_fee()
        return position

    def maybe_enter(self, candle: Candle, direction: Optional[str]) -> Optional[LadderPosition]:
        if direction is None or self.active_trade is not None:
            return None
        self.active_trade = self._new_position(candle, direction)
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

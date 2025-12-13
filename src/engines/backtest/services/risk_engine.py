"""Ladder risk engine for creating and managing backtest positions."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from atm import merge_templates

from ..domain.configuration import InstrumentConfig, RiskConfig
from ..domain.models import Candle
from ..domain.position import LadderPosition
from ..strategies import (
    DefaultRiskSizingStrategy,
    DefaultStopTargetStrategy,
    RiskSizingStrategy,
    StopTargetStrategy,
)
from ..utils import coerce_float
from .orders import OrderTemplateBuilder

logger = logging.getLogger(__name__)


class LadderRiskEngine:
    """Create and manage laddered trades for simulated bots."""

    def __init__(
        self,
        config: Optional[Dict[str, object]] = None,
        instrument: Optional[Dict[str, Any]] = None,
        risk_sizing_strategy: Optional[RiskSizingStrategy] = None,
        stop_target_strategy: Optional[StopTargetStrategy] = None,
        order_builder: Optional[OrderTemplateBuilder] = None,
    ):
        provided_template = config or {}
        self.template = merge_templates(provided_template)
        self.instrument = instrument or {}

        # Always validate template - same for all modes (backtest, sim_trade, paper, live)
        self._validate_template(self.template)
        self._validate_instrument(self.instrument)

        self.instrument_config = InstrumentConfig.from_dict(self.instrument)
        self.risk_config = RiskConfig.from_dict(self.template, self.instrument_config)
        self.order_builder = order_builder or OrderTemplateBuilder(self.template)
        self.risk_sizing_strategy = risk_sizing_strategy or DefaultRiskSizingStrategy()
        self.stop_target_strategy = stop_target_strategy or DefaultStopTargetStrategy()

        self.orders = self.order_builder.build_orders()
        self.targets = [int(order.get("ticks") or 0) for order in self.orders]

        self.tick_size = self.risk_config.tick_size
        self.stop_ticks = self.risk_config.stop_ticks
        self.r_multiple = self.risk_config.r_multiple
        self.base_risk_per_trade = self.risk_config.base_risk_per_trade
        self.stop_r_multiple = self.risk_config.stop_r_multiple
        self.stop_adjustments_config = self.risk_config.stop_adjustments
        self.contract_size = self.risk_config.contract_size
        self.tick_value = self.risk_config.tick_value
        self.risk_unit_mode = self.risk_config.risk_unit_mode
        self.ticks_stop = self.risk_config.ticks_stop
        self.global_risk_multiplier = self.risk_config.global_risk_multiplier
        self.instrument_risk_multiplier = self.risk_config.instrument_risk_multiplier
        self.quote_currency = self.risk_config.quote_currency
        self.maker_fee = self.risk_config.maker_fee
        self.taker_fee = self.risk_config.taker_fee

        self.active_trade: Optional[LadderPosition] = None
        self.trades: List[LadderPosition] = []

        logger.info(
            "ladder_risk_configured | targets=%s | stop_ticks=%s | tick=%.5f | instrument=%s",
            ",".join(str(order.get("ticks") or order.get("r_multiple") or "?") for order in self.orders),
            self.stop_ticks,
            self.tick_size,
            self.instrument.get("symbol"),
        )

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

    def _new_position(self, candle: Candle, direction: str) -> LadderPosition:
        direction = "long" if direction == "long" else "short"
        stop_info = self.stop_target_strategy.compute_stop(candle, direction, self.risk_config)
        total_contracts = self.risk_sizing_strategy.contracts_from_effective_risk(
            stop_info.one_r_distance or 0.0, self.risk_config, self.instrument_config
        )
        orders_for_position = self.order_builder.with_total_contracts(total_contracts)
        target_result = self.stop_target_strategy.build_targets(
            candle, direction, self.risk_config, orders_for_position, stop_info
        )

        position = LadderPosition(
            entry_time=candle.time,
            entry_price=candle.close,
            direction=direction,
            stop_price=target_result.stop_price,
            tick_size=self.tick_size,
            legs=target_result.legs,
            breakeven_trigger_ticks=0.0,
            tick_value=self.tick_value,
            contract_size=self.contract_size,
            maker_fee_rate=self.maker_fee,
            taker_fee_rate=self.taker_fee,
            quote_currency=self.quote_currency,
            atr_at_entry=target_result.atr_at_entry,
            r_multiple_at_entry=self.r_multiple,
            r_value=target_result.r_value,
            r_ticks=target_result.r_ticks,
            trailing_activation_ticks=None,
            trailing_distance_ticks=None,
            trailing_atr_multiple=0.0,
            pre_entry_context=getattr(candle, "lookback_15", None),
            stop_adjustments=target_result.stop_adjustments,
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

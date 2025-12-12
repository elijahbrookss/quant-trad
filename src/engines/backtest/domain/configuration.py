"""Configuration helpers for laddered risk engine components."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..utils import coerce_float


@dataclass
class InstrumentConfig:
    """Normalised instrument metadata with quantity safeguards."""

    tick_size: float
    contract_size: float
    tick_value: float
    risk_multiplier: float
    min_qty: Optional[float] = None
    qty_step: Optional[float] = None
    supports_fractional: bool = False
    maker_fee_rate: float = 0.0
    taker_fee_rate: float = 0.0
    quote_currency: str = "USD"

    @classmethod
    def from_dict(
        cls, instrument: Optional[Dict[str, Any]], default_tick_size: float
    ) -> "InstrumentConfig":
        instrument = instrument or {}
        tick_size = coerce_float(instrument.get("tick_size"), default_tick_size) or default_tick_size
        contract_size = coerce_float(instrument.get("contract_size"), 1.0) or 1.0
        tick_value = coerce_float(instrument.get("tick_value"))
        if tick_value in (None, 0):
            tick_value = tick_size * contract_size

        min_qty = coerce_float(
            instrument.get("min_qty")
            or instrument.get("min_order_size")
            or instrument.get("min_quantity")
        )
        qty_step = coerce_float(
            instrument.get("qty_step")
            or instrument.get("order_step")
            or instrument.get("step_size")
        )
        quote_value = instrument.get("quote_currency") or "USD"
        return cls(
            tick_size=float(tick_size),
            contract_size=float(contract_size),
            tick_value=float(tick_value) if tick_value is not None else float(default_tick_size),
            risk_multiplier=coerce_float(instrument.get("risk_multiplier"), 1.0) or 1.0,
            min_qty=float(min_qty) if min_qty not in (None, 0) else None,
            qty_step=float(qty_step) if qty_step not in (None, 0) else None,
            supports_fractional=bool(
                instrument.get("supports_fractional") or instrument.get("fractional_orders")
            ),
            maker_fee_rate=coerce_float(instrument.get("maker_fee_rate"), 0.0) or 0.0,
            taker_fee_rate=coerce_float(instrument.get("taker_fee_rate"), 0.0) or 0.0,
            quote_currency=str(quote_value).upper(),
        )

    def apply_quantity_constraints(self, qty: float) -> float:
        """Clamp order size based on instrument minimums and steps."""

        normalized = max(qty, 0.0)
        if self.qty_step not in (None, 0):
            normalized = round(normalized / self.qty_step) * self.qty_step
        elif not self.supports_fractional:
            normalized = float(int(round(normalized)))

        if self.min_qty not in (None, 0) and normalized < float(self.min_qty):
            normalized = float(self.min_qty)

        if not self.supports_fractional:
            normalized = float(max(int(round(normalized)), 1))
        return normalized

    def point_value(self) -> float:
        """Return the value of a one point move for this instrument."""

        if self.tick_value not in (None, 0):
            return float(self.tick_value)
        if self.contract_size not in (None, 0):
            return float(self.contract_size)
        return 1.0


@dataclass
class RiskConfig:
    """Normalized risk configuration ready for runtime use."""

    tick_size: float
    stop_ticks: int
    r_multiple: float
    base_risk_per_trade: Optional[float]
    stop_r_multiple: Optional[float]
    stop_price: Optional[float] = None
    stop_adjustments: List[Dict[str, Any]] = field(default_factory=list)
    risk_unit_mode: str = "atr"
    ticks_stop: int = 0
    global_risk_multiplier: float = 1.0
    instrument_risk_multiplier: float = 1.0
    tick_value: float = 0.0
    contract_size: float = 1.0
    maker_fee: float = 0.0
    taker_fee: float = 0.0
    quote_currency: str = "USD"

    @classmethod
    def from_dict(
        cls,
        template: Optional[Dict[str, Any]],
        instrument: InstrumentConfig,
        defaults: Dict[str, Any],
    ) -> "RiskConfig":
        template = template or {}
        config_tick = coerce_float(template.get("tick_size"))
        fallback_tick = coerce_float(defaults.get("tick_size"), 0.01) or 0.01
        tick_size = (
            float(config_tick)
            if config_tick not in (None, 0)
            else float(instrument.tick_size or fallback_tick)
        )

        initial_stop_config = template.get("initial_stop")
        if not isinstance(initial_stop_config, dict):
            initial_stop_config = {}
        r_multiple = float(initial_stop_config.get("atr_multiplier") or 1.0)

        risk_config = template.get("risk")
        if not isinstance(risk_config, dict):
            risk_config = {}

        stop_adjustments: List[Dict[str, Any]] = list(template.get("stop_adjustments") or [])
        risk_mode = str(initial_stop_config.get("mode") or "atr").lower()
        risk_unit_mode = risk_mode if risk_mode in {"atr", "ticks"} else "atr"
        ticks_stop = int(
            template.get("ticks_stop")
            or template.get("stop_ticks")
            or defaults.get("stop_ticks")
            or 1
        )
        config_tick_value = coerce_float(template.get("tick_value"))
        tick_value = (
            float(config_tick_value)
            if config_tick_value not in (None, 0)
            else float(instrument.tick_value or tick_size * instrument.contract_size)
        )

        config_contract = coerce_float(template.get("contract_size"))
        contract_size = (
            float(config_contract)
            if config_contract not in (None, 0)
            else float(instrument.contract_size)
        )

        config_maker = coerce_float(template.get("maker_fee_rate"))
        config_taker = coerce_float(template.get("taker_fee_rate"))
        quote_value = template.get("quote_currency") or instrument.quote_currency or "USD"

        return cls(
            tick_size=tick_size,
            stop_ticks=int(template.get("stop_ticks") or defaults.get("stop_ticks") or 1),
            r_multiple=r_multiple,
            base_risk_per_trade=coerce_float(risk_config.get("base_risk_per_trade")),
            stop_r_multiple=coerce_float(template.get("stop_r_multiple")),
            stop_price=coerce_float(template.get("stop_price")),
            stop_adjustments=stop_adjustments,
            risk_unit_mode=risk_unit_mode,
            ticks_stop=ticks_stop,
            global_risk_multiplier=coerce_float(risk_config.get("global_risk_multiplier"), 1.0) or 1.0,
            instrument_risk_multiplier=instrument.risk_multiplier or 1.0,
            tick_value=tick_value,
            contract_size=contract_size,
            maker_fee=float(config_maker) if config_maker is not None else float(instrument.maker_fee_rate),
            taker_fee=float(config_taker) if config_taker is not None else float(instrument.taker_fee_rate),
            quote_currency=str(quote_value).upper(),
        )

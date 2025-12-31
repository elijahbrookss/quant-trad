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
    instrument_type: Optional[str] = None
    min_qty: Optional[float] = None
    qty_step: Optional[float] = None
    supports_fractional: bool = False
    maker_fee_rate: float = 0.0
    taker_fee_rate: float = 0.0
    quote_currency: str = "USD"

    @classmethod
    def from_dict(cls, instrument: Optional[Dict[str, Any]]) -> "InstrumentConfig":
        instrument = instrument or {}

        # Always require tick_size from instrument - no defaults
        tick_size = coerce_float(instrument.get("tick_size"))
        if not tick_size or tick_size <= 0:
            raise ValueError(
                "Instrument configuration must include a valid tick_size. "
                "No default tick_size is provided to ensure accurate pricing."
            )

        instrument_type = str(instrument.get("instrument_type") or "").lower() or None
        if instrument_type == "spot":
            contract_size = 1.0
            tick_value = tick_size
        else:
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
            tick_value=float(tick_value) if tick_value is not None else float(tick_size),
            risk_multiplier=coerce_float(instrument.get("risk_multiplier"), 1.0) or 1.0,
            min_qty=float(min_qty) if min_qty not in (None, 0) else None,
            qty_step=float(qty_step) if qty_step not in (None, 0) else None,
            supports_fractional=bool(
                instrument.get("supports_fractional") or instrument.get("fractional_orders")
            ),
            maker_fee_rate=coerce_float(instrument.get("maker_fee_rate"), 0.0) or 0.0,
            taker_fee_rate=coerce_float(instrument.get("taker_fee_rate"), 0.0) or 0.0,
            quote_currency=str(quote_value).upper(),
            instrument_type=instrument_type,
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

        if str(self.instrument_type or "").lower() == "spot":
            return float(self.tick_size)
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
    ) -> "RiskConfig":
        template = template or {}
        config_tick = coerce_float(template.get("tick_size"))
        tick_size = (
            float(config_tick)
            if config_tick not in (None, 0)
            else float(instrument.tick_size)
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
            or 0  # Will be validated below
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

        base_risk = coerce_float(risk_config.get("base_risk_per_trade"))
        stop_ticks_value = int(template.get("stop_ticks") or 0)

        # Always validate critical fields - same for all modes (backtest, sim_trade, paper, live)
        validation_errors = []

        if base_risk is None or base_risk <= 0:
            validation_errors.append("risk.base_risk_per_trade must be a positive number")

        if stop_ticks_value <= 0:
            validation_errors.append("stop_ticks must be positive")

        if validation_errors:
            raise ValueError(
                f"Incomplete or invalid risk configuration. "
                f"Errors: {'; '.join(validation_errors)}. "
                f"All modes (backtest/sim_trade/paper/live) require complete templates."
            )

        return cls(
            tick_size=tick_size,
            stop_ticks=stop_ticks_value,
            r_multiple=r_multiple,
            base_risk_per_trade=base_risk,
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

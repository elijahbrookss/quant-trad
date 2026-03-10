"""Margin requirement calculation for different instrument types.

This module provides a pluggable abstraction for calculating margin requirements
based on instrument type and configuration. The design allows:

1. Futures/derivatives: Use exchange-provided margin rates (intraday/overnight)
2. Spot instruments: Require full cash collateral (no margin)
3. Fail-loud on misconfiguration: Don't silently fall back to spot-style

Session Context:
- Intraday margin is lower, used during active trading hours
- Overnight margin is higher (more conservative), used when session is unclear
- Default to overnight (conservative) when session state unknown
"""

from __future__ import annotations

import math
from dataclasses import dataclass
import logging
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Protocol, Tuple

from utils.log_context import build_log_context, with_log_context

logger = logging.getLogger(__name__)


class MarginSessionType(Enum):
    """Trading session type for margin rate selection."""

    INTRADAY = "intraday"
    OVERNIGHT = "overnight"
    UNKNOWN = "unknown"  # Will default to overnight (conservative)


class InstrumentType(Enum):
    """Instrument type classification."""

    SPOT = "spot"
    FUTURE = "future"
    SWAP = "swap"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MarginRates:
    """Margin rates for an instrument (long/short, intraday/overnight)."""

    intraday_long: Optional[float] = None
    intraday_short: Optional[float] = None
    overnight_long: Optional[float] = None
    overnight_short: Optional[float] = None

    def get_rate(self, direction: str, session: MarginSessionType) -> Optional[float]:
        """Get the appropriate margin rate for direction and session.

        Args:
            direction: "long" or "short"
            session: Session type (intraday, overnight, or unknown)

        Returns:
            Margin rate as a decimal (e.g., 0.10 for 10%), or None if not available
        """
        is_long = direction.lower() == "long"

        if session == MarginSessionType.INTRADAY:
            return self.intraday_long if is_long else self.intraday_short

        # For OVERNIGHT or UNKNOWN, use overnight (conservative)
        return self.overnight_long if is_long else self.overnight_short


@dataclass(frozen=True)
class MarginRequirement:
    """Calculated margin requirement for a trade."""

    required_margin: float  # The margin/collateral required
    margin_rate: float  # The rate used (for debugging/logging)
    notional: float  # The notional value of the position
    fee_buffer: float  # Fees included in requirement
    safety_buffer: float  # Optional safety margin
    calculation_method: str  # FUTURES_MARGIN_INTRADAY, FUTURES_MARGIN_OVERNIGHT, SPOT_CASH_SHORT_COVER
    session_type: str  # Which session was used

    @property
    def total_required(self) -> float:
        """Total cash required for the trade."""
        return self.required_margin + self.fee_buffer + self.safety_buffer


class MarginCalculator(Protocol):
    """Protocol for margin requirement calculators."""

    def calculate(
        self,
        *,
        notional: float,
        fee: float,
        direction: str,
        session: MarginSessionType,
    ) -> MarginRequirement:
        """Calculate margin requirement for a trade."""
        ...


class SpotMarginCalculator:
    """Margin calculator for spot instruments - requires full notional."""

    def calculate(
        self,
        *,
        notional: float,
        fee: float,
        direction: str,
        session: MarginSessionType,
    ) -> MarginRequirement:
        """Spot shorts require full notional for buyback (cash-secured)."""
        # For spot shorts, we need the full notional to cover buyback
        # Plus double fee as safety margin (entry + exit fees)
        fee_buffer = fee * 2
        return MarginRequirement(
            required_margin=notional,
            margin_rate=1.0,  # 100% - full notional
            notional=notional,
            fee_buffer=fee_buffer,
            safety_buffer=0.0,
            calculation_method="SPOT_CASH_SHORT_COVER",
            session_type="n/a",
        )


class FuturesMarginCalculator:
    """Margin calculator for futures - uses exchange margin rates."""

    def __init__(
        self,
        rates: MarginRates,
        *,
        safety_multiplier: float = 1.05,  # 5% safety buffer
    ) -> None:
        """Initialize with margin rates.

        Args:
            rates: Margin rates extracted from instrument metadata
            safety_multiplier: Multiplier for safety buffer (default 5%)
        """
        self.rates = rates
        self.safety_multiplier = safety_multiplier

    def calculate(
        self,
        *,
        notional: float,
        fee: float,
        direction: str,
        session: MarginSessionType,
    ) -> MarginRequirement:
        """Calculate margin-based requirement for futures."""
        resolved_session = (
            MarginSessionType.INTRADAY
            if session == MarginSessionType.INTRADAY
            else MarginSessionType.OVERNIGHT
        )
        rate = self.rates.get_rate(direction, resolved_session)
        if rate is None or rate <= 0:
            raise ValueError(
                f"No valid margin rate for direction={direction}, session={session}. "
                f"Available rates: intraday_long={self.rates.intraday_long}, "
                f"intraday_short={self.rates.intraday_short}, "
                f"overnight_long={self.rates.overnight_long}, "
                f"overnight_short={self.rates.overnight_short}"
            )

        base_margin = notional * rate
        safety_buffer = base_margin * (self.safety_multiplier - 1.0)

        return MarginRequirement(
            required_margin=base_margin,
            margin_rate=rate,
            notional=notional,
            fee_buffer=fee,
            safety_buffer=safety_buffer,
            calculation_method=f"FUTURES_MARGIN_{resolved_session.value.upper()}",
            session_type=resolved_session.value,
        )


def extract_margin_rates(instrument: Mapping[str, Any]) -> Optional[MarginRates]:
    """Extract margin rates from instrument metadata.

    Canonical lookup order:
    1) instrument.margin_rates
    2) instrument.metadata.instrument_fields.margin_rates

    Args:
        instrument: Instrument configuration dict

    Returns:
        MarginRates if available, None if not present
    """
    def _extract_from_margin_rates_map(rates_map: Optional[Mapping[str, Any]]) -> Tuple[Mapping[str, Any], Mapping[str, Any]]:
        if not isinstance(rates_map, Mapping):
            return {}, {}
        intraday_map = rates_map.get("intraday")
        overnight_map = rates_map.get("overnight")
        return (
            intraday_map if isinstance(intraday_map, Mapping) else {},
            overnight_map if isinstance(overnight_map, Mapping) else {},
        )

    intraday: Mapping[str, Any] = {}
    overnight: Mapping[str, Any] = {}

    # Canonical path (preferred)
    intraday, overnight = _extract_from_margin_rates_map(
        instrument.get("margin_rates") if isinstance(instrument.get("margin_rates"), Mapping) else None
    )

    # Canonical metadata fallback.
    if not intraday and not overnight:
        metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
        instrument_fields = (
            metadata.get("instrument_fields")
            if isinstance(metadata.get("instrument_fields"), Mapping)
            else {}
        )
        intraday, overnight = _extract_from_margin_rates_map(
            instrument_fields.get("margin_rates") if isinstance(instrument_fields.get("margin_rates"), Mapping) else None
        )

    def parse_rate(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            rate = float(value)
            return rate if rate > 0 else None
        except (TypeError, ValueError):
            return None

    rates = MarginRates(
        intraday_long=parse_rate(intraday.get("long_margin_rate")),
        intraday_short=parse_rate(intraday.get("short_margin_rate")),
        overnight_long=parse_rate(overnight.get("long_margin_rate")),
        overnight_short=parse_rate(overnight.get("short_margin_rate")),
    )
    if (
        rates.intraday_long is None
        and rates.intraday_short is None
        and rates.overnight_long is None
        and rates.overnight_short is None
    ):
        return None
    return rates


def resolve_instrument_type(instrument: Mapping[str, Any]) -> InstrumentType:
    """Resolve the instrument type from configuration.

    Args:
        instrument: Instrument configuration dict

    Returns:
        InstrumentType enum value
    """
    raw_type = str(instrument.get("instrument_type") or "").lower().strip()
    if not raw_type:
        metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
        instrument_fields = (
            metadata.get("instrument_fields")
            if isinstance(metadata.get("instrument_fields"), Mapping)
            else {}
        )
        raw_type = str(instrument_fields.get("instrument_type") or "").lower().strip()
    if raw_type == "spot":
        return InstrumentType.SPOT
    if raw_type == "future":
        return InstrumentType.FUTURE
    if raw_type == "swap":
        return InstrumentType.SWAP
    return InstrumentType.UNKNOWN


@dataclass(frozen=True)
class MaxQtyByMargin:
    """Result of calculating maximum qty allowed by available collateral."""

    max_qty: float  # Maximum qty that can be traded
    available_collateral: float  # Free collateral available for margin
    cost_per_contract: float  # Total cost per contract (margin + fees) after safety
    margin_per_contract: float  # Base margin per contract (before safety)
    fee_per_contract: float  # Round-trip fee per contract
    margin_rate: float  # The margin rate used
    calculation_method: str  # FUTURES_MARGIN_INTRADAY, FUTURES_MARGIN_OVERNIGHT, SPOT_CASH_SHORT_COVER


def _floor_to_step(qty: float, step: Optional[float]) -> float:
    if step in (None, 0):
        return qty
    return math.floor((qty + 1e-12) / step) * step


def _align_min_to_step(min_qty: Optional[float], step: Optional[float]) -> Optional[float]:
    if min_qty in (None, 0) or step in (None, 0):
        return min_qty
    return math.ceil(min_qty / step) * step


def calculate_max_qty_by_margin(
    *,
    available_collateral: float,
    price: float,
    contract_size: float,
    direction: str,
    instrument: Mapping[str, Any],
    execution_profile: Optional[Any] = None,
    fee_rate: float = 0.0,
    safety_multiplier: float = 1.05,
    margin_session: Optional[MarginSessionType] = None,
    qty_step: Optional[float] = None,
    min_order_size: Optional[float] = None,
) -> MaxQtyByMargin:
    """Calculate maximum qty allowed by available collateral/margin.

    Formula for futures:
        notional_per_contract = price * contract_size
        margin_per_contract = notional_per_contract * margin_rate
        fee_per_contract = notional_per_contract * fee_rate * 2  (round-trip: entry + exit)
        cost_per_contract = (margin_per_contract + fee_per_contract) * safety_multiplier
        max_qty = available_collateral / cost_per_contract

    Formula for spot (full notional):
        cost_per_contract = (notional_per_contract + fee_per_contract) * safety_multiplier
        max_qty = available_collateral / cost_per_contract

    Safety multiplier is applied to the TOTAL required (margin + fees), not just margin,
    because fees and slippage are part of "can I survive this fill" calculation.

    Args:
        available_collateral: Free collateral in quote currency (for backtest, same as cash balance)
        price: Current price
        contract_size: Contract size multiplier
        direction: "long" or "short"
        instrument: Instrument configuration
        execution_profile: Optional compiled series execution profile
        fee_rate: Taker fee rate as decimal (worst case). Use taker rate for conservative sizing.
        safety_multiplier: Safety buffer on total required (default 5%)
        margin_session: Session type for margin rate selection (intraday vs overnight)
        qty_step: Quantity step size for rounding down max qty
        min_order_size: Minimum order size; if max qty after rounding is below this, returns 0

    Returns:
        MaxQtyByMargin with max_qty and breakdown of costs

    Raises:
        ValueError: If instrument is misconfigured (e.g., future without margin rates)
    """
    session = margin_session or MarginSessionType.OVERNIGHT
    min_order_size_aligned = _align_min_to_step(min_order_size, qty_step)

    notional_per_contract = price * contract_size
    if notional_per_contract <= 0:
        raise ValueError(f"Invalid notional_per_contract: {notional_per_contract}")

    # Round-trip fees: entry + exit (worst case taker on both)
    fee_per_contract = notional_per_contract * fee_rate * 2

    # Resolve margin model from compiled profile when available.
    calculator = getattr(execution_profile, "margin_calculator", None) if execution_profile is not None else None
    calc_type = getattr(execution_profile, "margin_calc_type", None) if execution_profile is not None else None
    if calculator is None or calc_type not in {"margin", "full_notional"}:
        calculator, calc_type = create_margin_calculator(instrument, safety_multiplier=1.0)

    if calc_type == "margin":
        # Futures/derivatives: use exchange margin rate
        rates = getattr(execution_profile, "margin_rates", None) if execution_profile is not None else None
        if rates is None:
            rates = extract_margin_rates(instrument)
        if rates is None:
            raise ValueError("Margin calculator created but no rates found")

        resolved_session = (
            MarginSessionType.INTRADAY
            if session == MarginSessionType.INTRADAY
            else MarginSessionType.OVERNIGHT
        )
        margin_rate = rates.get_rate(direction, resolved_session)
        if margin_rate is None or margin_rate <= 0:
            raise ValueError(f"No valid margin rate for {direction}/{session}")

        context = build_log_context(
            symbol=instrument.get("symbol"),
            direction=direction,
            session=resolved_session.value,
            margin_rate=margin_rate,
            intraday_long=rates.intraday_long,
            intraday_short=rates.intraday_short,
            overnight_long=rates.overnight_long,
            overnight_short=rates.overnight_short,
            price=price,
            contract_size=contract_size,
            notional_per_contract=notional_per_contract,
        )
        logger.info(with_log_context("margin_rate_selected", context))

        # Base margin per contract (before safety)
        margin_per_contract = notional_per_contract * margin_rate

        # Total cost = (margin + round-trip fees) * safety
        base_cost = margin_per_contract + fee_per_contract
        cost_per_contract = base_cost * safety_multiplier

        max_qty = available_collateral / cost_per_contract if cost_per_contract > 0 else 0.0
        max_qty = _floor_to_step(max_qty, qty_step)
        if min_order_size_aligned not in (None, 0) and max_qty + 1e-12 < min_order_size_aligned:
            max_qty = 0.0

        return MaxQtyByMargin(
            max_qty=max_qty,
            available_collateral=available_collateral,
            cost_per_contract=cost_per_contract,
            margin_per_contract=margin_per_contract,
            fee_per_contract=fee_per_contract,
            margin_rate=margin_rate,
            calculation_method=f"FUTURES_MARGIN_{resolved_session.value.upper()}",
        )
    else:
        # Spot: full notional required as "margin"
        margin_per_contract = notional_per_contract  # 100% margin

        # Total cost = (full notional + round-trip fees) * safety
        base_cost = margin_per_contract + fee_per_contract
        cost_per_contract = base_cost * safety_multiplier

        max_qty = available_collateral / cost_per_contract if cost_per_contract > 0 else 0.0
        max_qty = _floor_to_step(max_qty, qty_step)
        if min_order_size_aligned not in (None, 0) and max_qty + 1e-12 < min_order_size_aligned:
            max_qty = 0.0

        return MaxQtyByMargin(
            max_qty=max_qty,
            available_collateral=available_collateral,
            cost_per_contract=cost_per_contract,
            margin_per_contract=margin_per_contract,
            fee_per_contract=fee_per_contract,
            margin_rate=1.0,
            calculation_method="SPOT_CASH_SHORT_COVER",
        )


def create_margin_calculator(
    instrument: Mapping[str, Any],
    *,
    safety_multiplier: float = 1.05,
) -> Tuple[MarginCalculator, str]:
    """Create appropriate margin calculator for an instrument.

    This is the main factory function. It:
    1. Requires an explicit instrument type
    2. Extracts canonical margin rates when needed
    3. Returns appropriate calculator or raises on misconfiguration

    Args:
        instrument: Instrument configuration dict
        safety_multiplier: Safety buffer multiplier for margin calculations

    Returns:
        Tuple of (calculator, calculation_type) where calculation_type is
        "margin" or "full_notional"

    Raises:
        ValueError: If instrument is misconfigured (e.g., future without margin rates)
    """
    inst_type = resolve_instrument_type(instrument)
    margin_rates = extract_margin_rates(instrument)

    # SPOT: Always use full notional (cash-secured)
    if inst_type == InstrumentType.SPOT:
        return SpotMarginCalculator(), "full_notional"

    # FUTURE or SWAP: Require margin rates, fail loud if missing
    if inst_type in (InstrumentType.FUTURE, InstrumentType.SWAP):
        if margin_rates is None:
            raise ValueError(
                f"Instrument type is '{inst_type.value}' but no margin rates found. "
                f"Futures/swaps must include margin_rates with intraday and/or overnight values. "
                f"Instrument: {instrument.get('symbol', 'unknown')}"
            )

        # Validate that at least overnight rates are present (conservative fallback)
        if margin_rates.overnight_long is None and margin_rates.overnight_short is None:
            if margin_rates.intraday_long is None and margin_rates.intraday_short is None:
                raise ValueError(
                    f"Instrument type is '{inst_type.value}' but margin rates are all None. "
                    f"At least one valid margin rate is required. "
                    f"Instrument: {instrument.get('symbol', 'unknown')}"
                )

        return FuturesMarginCalculator(margin_rates, safety_multiplier=safety_multiplier), "margin"

    # UNKNOWN type: fail loud instead of inferring execution semantics.
    raise ValueError(
        "Instrument type is missing or unsupported. "
        "Expected one of: spot, future, swap. "
        f"Instrument: {instrument.get('symbol', 'unknown')}"
    )


__all__ = [
    "MarginSessionType",
    "InstrumentType",
    "MarginRates",
    "MarginRequirement",
    "MaxQtyByMargin",
    "MarginCalculator",
    "SpotMarginCalculator",
    "FuturesMarginCalculator",
    "extract_margin_rates",
    "resolve_instrument_type",
    "create_margin_calculator",
    "calculate_max_qty_by_margin",
]

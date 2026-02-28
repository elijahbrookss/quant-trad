"""Canonical runtime execution profile and model contracts.

This module compiles raw instrument/template payloads into one runtime profile
object so engine semantics do not depend on ad-hoc dictionary lookups.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Protocol, Tuple

from .amount_constraints import AmountConstraints, resolve_amount_constraints
from .margin import (
    InstrumentType,
    MarginCalculator,
    MarginRates,
    create_margin_calculator,
    extract_margin_rates,
    resolve_instrument_type,
)


@dataclass(frozen=True)
class InstrumentContract:
    """Canonical instrument identity and static fields used by runtime."""

    instrument_id: Optional[str]
    symbol: str
    instrument_type: str
    datasource: Optional[str]
    exchange: Optional[str]
    base_currency: str
    quote_currency: str
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class ExecutionConstraintsContract:
    """Quantity/price constraints consumed by sizing and execution."""

    tick_size: float
    contract_size: float
    tick_value: float
    min_order_size: Optional[float]
    qty_step: Optional[float]
    max_qty: Optional[float]
    min_notional: Optional[float]
    amount_precision: Optional[int]
    amount_constraints: AmountConstraints


@dataclass(frozen=True)
class ExecutionCapabilities:
    """Capability flags that drive runtime behavior."""

    supports_margin: bool
    supports_short: bool
    short_requires_borrow: bool
    has_funding: bool
    has_expiry: bool


@dataclass(frozen=True)
class RiskConfigContract:
    """Risk inputs used by sizing logic."""

    base_risk_per_trade: Optional[float]
    global_risk_multiplier: float
    instrument_risk_multiplier: float


class CollateralModel(Protocol):
    """Collateral accounting model boundary."""

    @property
    def accounting_mode(self) -> Optional[str]:
        """Return accounting mode identifier (e.g. ``margin`` or ``None``)."""


@dataclass(frozen=True)
class SimpleCollateralModel:
    """Minimal collateral model used by runtime v1."""

    accounting_mode: Optional[str]


@dataclass(frozen=True)
class SeriesExecutionProfile:
    """Compiled runtime profile for one series/instrument."""

    instrument: InstrumentContract
    constraints: ExecutionConstraintsContract
    capabilities: ExecutionCapabilities
    risk: RiskConfigContract
    margin_calculator: MarginCalculator
    margin_calc_type: str
    margin_rates: Optional[MarginRates]
    collateral_model: CollateralModel
    validated_for_runtime: bool

    @property
    def accounting_mode(self) -> Optional[str]:
        return self.collateral_model.accounting_mode

    def is_margin_accounting(self) -> bool:
        return self.accounting_mode == "margin"

    def is_derivatives(self) -> bool:
        return self.instrument.instrument_type in {"future", "perp", "swap"}


def normalize_runtime_instrument_type(value: Optional[object]) -> str:
    """Normalize provider/runtime instrument types to canonical values."""

    text = str(value or "").strip().lower()
    if text in {"futures", "future"}:
        return "future"
    if text in {"perps", "perp"}:
        return "perp"
    if text in {"swaps", "swap"}:
        return "swap"
    if text in {"spots", "spot"}:
        return "spot"
    if text in {"derivative", "derivatives"}:
        return "future"
    return text


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_risk_contract(
    template: Optional[Mapping[str, Any]],
    instrument: Mapping[str, Any],
) -> RiskConfigContract:
    template_payload = dict(template or {})
    risk_payload = template_payload.get("risk")
    if not isinstance(risk_payload, Mapping):
        risk_payload = {}
    base_risk_per_trade = _coerce_float(
        risk_payload.get("base_risk_per_trade") or template_payload.get("base_risk_per_trade")
    )
    global_risk_multiplier = _coerce_float(risk_payload.get("global_risk_multiplier"), 1.0) or 1.0
    instrument_risk_multiplier = _coerce_float(instrument.get("risk_multiplier"), 1.0) or 1.0
    return RiskConfigContract(
        base_risk_per_trade=base_risk_per_trade,
        global_risk_multiplier=float(global_risk_multiplier),
        instrument_risk_multiplier=float(instrument_risk_multiplier),
    )


def _allowed_types(
    values: Optional[Iterable[str]],
) -> Tuple[str, ...]:
    if not values:
        return ("future", "perp")
    normalized = [normalize_runtime_instrument_type(value) for value in values]
    filtered = [value for value in normalized if value]
    if not filtered:
        return ("future", "perp")
    return tuple(sorted(set(filtered)))


def compile_series_execution_profile(
    instrument: Mapping[str, Any],
    *,
    template: Optional[Mapping[str, Any]] = None,
    runtime_requires_derivatives: bool = False,
    allowed_derivative_types: Optional[Iterable[str]] = None,
) -> SeriesExecutionProfile:
    """Compile a canonical runtime profile from instrument/template payloads."""

    if not isinstance(instrument, Mapping) or not instrument:
        raise ValueError("instrument metadata missing. Validate instrument before runtime.")

    instrument_type = normalize_runtime_instrument_type(instrument.get("instrument_type"))
    resolved_type = resolve_instrument_type(instrument)
    if not instrument_type:
        if resolved_type == InstrumentType.FUTURE:
            instrument_type = "future"
        elif resolved_type == InstrumentType.SWAP:
            instrument_type = "swap"
        elif resolved_type == InstrumentType.SPOT:
            instrument_type = "spot"

    runtime_allowed = _allowed_types(allowed_derivative_types)
    if runtime_requires_derivatives and instrument_type not in runtime_allowed:
        raise ValueError(
            "runtime v1 supports only futures/perps instruments "
            f"(got instrument_type={instrument_type or 'missing'})."
        )

    symbol = str(instrument.get("symbol") or "").strip()
    if not symbol:
        raise ValueError("instrument symbol missing. Validate instrument metadata before runtime.")

    amount_constraints = resolve_amount_constraints(instrument)
    tick_size = _coerce_float(instrument.get("tick_size"))
    contract_size = _coerce_float(instrument.get("contract_size"))
    tick_value = _coerce_float(instrument.get("tick_value"))
    if tick_value in (None, 0) and tick_size not in (None, 0) and contract_size not in (None, 0):
        tick_value = float(tick_size) * float(contract_size)

    if tick_size in (None, 0):
        raise ValueError(f"{symbol}: tick_size required for runtime execution.")
    if contract_size in (None, 0):
        raise ValueError(f"{symbol}: contract_size required for runtime execution.")
    if tick_value in (None, 0):
        raise ValueError(f"{symbol}: tick_value required for runtime execution.")

    margin_rates = extract_margin_rates(instrument)
    try:
        margin_calculator, margin_calc_type = create_margin_calculator(instrument)
    except ValueError as exc:
        if runtime_requires_derivatives:
            raise ValueError(
                f"{symbol}: missing margin_rates. Futures/perps require "
                "intraday/overnight margin configuration before runtime."
            ) from exc
        raise

    if runtime_requires_derivatives and margin_calc_type != "margin":
        raise ValueError(
            f"{symbol}: missing margin_rates. Futures/perps require "
            "intraday/overnight margin configuration before runtime."
        )

    base_currency = str(instrument.get("base_currency") or "").strip().upper()
    quote_currency = str(instrument.get("quote_currency") or "").strip().upper()
    if not base_currency or not quote_currency:
        raise ValueError(f"{symbol}: base_currency/quote_currency required for runtime execution.")

    supports_short = bool(instrument.get("can_short"))
    if instrument_type in {"future", "perp", "swap"} and not supports_short:
        supports_short = True

    capabilities = ExecutionCapabilities(
        supports_margin=(margin_calc_type == "margin"),
        supports_short=supports_short,
        short_requires_borrow=bool(instrument.get("short_requires_borrow")),
        has_funding=bool(instrument.get("has_funding")),
        has_expiry=bool(instrument.get("expiry_ts")),
    )

    constraints = ExecutionConstraintsContract(
        tick_size=float(tick_size),
        contract_size=float(contract_size),
        tick_value=float(tick_value),
        min_order_size=amount_constraints.min_qty,
        qty_step=amount_constraints.qty_step,
        max_qty=amount_constraints.max_qty,
        min_notional=amount_constraints.min_notional,
        amount_precision=amount_constraints.precision,
        amount_constraints=amount_constraints,
    )

    instrument_contract = InstrumentContract(
        instrument_id=str(instrument.get("id") or "").strip() or None,
        symbol=symbol,
        instrument_type=instrument_type or "unknown",
        datasource=str(instrument.get("datasource") or "").strip() or None,
        exchange=str(instrument.get("exchange") or "").strip() or None,
        base_currency=base_currency,
        quote_currency=quote_currency,
        raw=instrument,
    )

    risk = _extract_risk_contract(template, instrument)
    accounting_mode = "margin" if margin_calc_type == "margin" else None
    profile = SeriesExecutionProfile(
        instrument=instrument_contract,
        constraints=constraints,
        capabilities=capabilities,
        risk=risk,
        margin_calculator=margin_calculator,
        margin_calc_type=margin_calc_type,
        margin_rates=margin_rates,
        collateral_model=SimpleCollateralModel(accounting_mode=accounting_mode),
        validated_for_runtime=runtime_requires_derivatives,
    )
    return profile


def compile_runtime_profile_or_error(
    instrument: Mapping[str, Any],
    *,
    template: Optional[Mapping[str, Any]] = None,
    allowed_derivative_types: Optional[Iterable[str]] = None,
) -> SeriesExecutionProfile:
    """Compile profile with runtime derivative requirements enabled."""

    return compile_series_execution_profile(
        instrument,
        template=template,
        runtime_requires_derivatives=True,
        allowed_derivative_types=allowed_derivative_types,
    )


__all__ = [
    "InstrumentContract",
    "ExecutionConstraintsContract",
    "ExecutionCapabilities",
    "RiskConfigContract",
    "CollateralModel",
    "SimpleCollateralModel",
    "SeriesExecutionProfile",
    "normalize_runtime_instrument_type",
    "compile_series_execution_profile",
    "compile_runtime_profile_or_error",
]

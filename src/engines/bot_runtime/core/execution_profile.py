"""Canonical runtime execution profile and model contracts.

This module compiles raw instrument/template payloads into one runtime profile
object so engine semantics do not depend on ad-hoc dictionary lookups.
"""

from __future__ import annotations

from dataclasses import dataclass
from copy import deepcopy
from typing import Any, Iterable, Mapping, Optional, Protocol, Tuple

from risk import normalise_risk_config

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
    source_instrument_type: str
    execution_semantics: str
    research_market_role: str
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
class ExecutionFeeContract:
    """Fee rates consumed by deterministic execution."""

    maker_fee_rate: float
    taker_fee_rate: float
    source: str


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
    fees: ExecutionFeeContract
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
        return self.instrument.execution_semantics in {"derivative", "proxy_derivative"} or (
            self.instrument.instrument_type in {"future", "perp", "swap"}
        )

    def to_dict(self) -> dict[str, Any]:
        """Return an API/report-safe profile summary."""

        return {
            "instrument": {
                "instrument_id": self.instrument.instrument_id,
                "symbol": self.instrument.symbol,
                "instrument_type": self.instrument.instrument_type,
                "source_instrument_type": self.instrument.source_instrument_type,
                "execution_semantics": self.instrument.execution_semantics,
                "research_market_role": self.instrument.research_market_role,
                "datasource": self.instrument.datasource,
                "exchange": self.instrument.exchange,
                "base_currency": self.instrument.base_currency,
                "quote_currency": self.instrument.quote_currency,
            },
            "constraints": {
                "tick_size": self.constraints.tick_size,
                "contract_size": self.constraints.contract_size,
                "tick_value": self.constraints.tick_value,
                "min_order_size": self.constraints.min_order_size,
                "qty_step": self.constraints.qty_step,
                "max_qty": self.constraints.max_qty,
                "min_notional": self.constraints.min_notional,
                "amount_precision": self.constraints.amount_precision,
            },
            "capabilities": {
                "supports_margin": self.capabilities.supports_margin,
                "supports_short": self.capabilities.supports_short,
                "short_requires_borrow": self.capabilities.short_requires_borrow,
                "has_funding": self.capabilities.has_funding,
                "has_expiry": self.capabilities.has_expiry,
            },
            "fees": {
                "maker_fee_rate": self.fees.maker_fee_rate,
                "taker_fee_rate": self.fees.taker_fee_rate,
                "source": self.fees.source,
            },
            "risk": {
                "base_risk_per_trade": self.risk.base_risk_per_trade,
                "global_risk_multiplier": self.risk.global_risk_multiplier,
                "instrument_risk_multiplier": self.risk.instrument_risk_multiplier,
            },
            "accounting_mode": self.accounting_mode,
            "margin_calc_type": self.margin_calc_type,
            "validated_for_runtime": self.validated_for_runtime,
        }


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


def normalize_execution_semantics(value: Optional[object], *, instrument_type: Optional[str] = None) -> str:
    """Normalize the runtime execution semantics for a series.

    ``instrument_type`` remains the canonical market source from the instrument
    table. ``execution_semantics`` describes how the bot runtime should model
    fills/accounting for that source.
    """

    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"proxy", "proxy_derivative", "proxy_derivatives", "spot_proxy_derivative"}:
        return "proxy_derivative"
    if text in {"derivative", "derivatives", "future", "futures", "perp", "perps", "swap", "swaps"}:
        return "derivative"
    if text in {"spot", "cash"}:
        return "spot"

    source_type = normalize_runtime_instrument_type(instrument_type)
    if source_type in {"future", "perp", "swap"}:
        return "derivative"
    if source_type == "spot":
        return "spot"
    return "unknown"


def _extract_mapping(*values: Any) -> Optional[Mapping[str, Any]]:
    for value in values:
        if isinstance(value, Mapping) and value:
            return value
    return None


def _instrument_fields(instrument: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
    return fields


def _proxy_margin_rates(
    instrument: Mapping[str, Any],
) -> Mapping[str, Any]:
    instrument_fields = _instrument_fields(instrument)
    configured = _extract_mapping(
        instrument.get("proxy_derivative_margin_rates"),
        instrument.get("margin_rates"),
        instrument_fields.get("proxy_derivative_margin_rates"),
        instrument_fields.get("margin_rates"),
    )
    if configured:
        return configured
    symbol = str(instrument.get("symbol") or "unknown").strip() or "unknown"
    raise ValueError(
        f"{symbol}: proxy_derivative_margin_rates required for proxy_derivative execution. "
        "Attach a derivative reference or explicit proxy margin rates before runtime."
    )


def _proxy_instrument_fields(
    instrument: Mapping[str, Any],
) -> Mapping[str, Any]:
    instrument_fields = _instrument_fields(instrument)
    configured = _extract_mapping(
        instrument.get("proxy_derivative_instrument_fields"),
        instrument_fields.get("proxy_derivative_instrument_fields"),
    )
    if configured:
        return configured
    symbol = str(instrument.get("symbol") or "unknown").strip() or "unknown"
    raise ValueError(
        f"{symbol}: proxy_derivative_instrument_fields required for proxy_derivative execution. "
        "Attach a derivative reference or explicit proxy execution fields before runtime."
    )


def _execution_margin_instrument(
    instrument: Mapping[str, Any],
    *,
    execution_semantics: str,
) -> Mapping[str, Any]:
    if execution_semantics != "proxy_derivative":
        return instrument
    proxy = deepcopy(dict(instrument))
    proxy["instrument_type"] = "future"
    proxy["can_short"] = True
    proxy["short_requires_borrow"] = False
    proxy["margin_rates"] = dict(_proxy_margin_rates(instrument))
    return proxy


def _execution_instrument(
    instrument: Mapping[str, Any],
    *,
    execution_semantics: str,
) -> Mapping[str, Any]:
    if execution_semantics != "proxy_derivative":
        return instrument

    proxy = deepcopy(dict(instrument))
    metadata = dict(proxy.get("metadata") or {})
    instrument_fields = dict(metadata.get("instrument_fields") or {})

    for key, value in _proxy_instrument_fields(instrument).items():
        proxy[key] = deepcopy(value)
        instrument_fields[key] = deepcopy(value)

    proxy["instrument_type"] = "future"
    proxy["can_short"] = True
    proxy["short_requires_borrow"] = False
    proxy["margin_rates"] = dict(_proxy_margin_rates(instrument))
    for fee_key in ("maker_fee_rate", "taker_fee_rate"):
        proxy_fee_key = f"proxy_derivative_{fee_key}"
        if fee_key in proxy:
            instrument_fields[fee_key] = deepcopy(proxy.get(fee_key))
            continue
        proxy_fee = instrument.get(proxy_fee_key)
        if proxy_fee is None:
            proxy_fee = _instrument_fields(instrument).get(proxy_fee_key)
        if proxy_fee is not None:
            proxy[fee_key] = deepcopy(proxy_fee)
            instrument_fields[fee_key] = deepcopy(proxy_fee)
    instrument_fields["can_short"] = True
    instrument_fields["short_requires_borrow"] = False
    instrument_fields["margin_rates"] = deepcopy(proxy["margin_rates"])
    metadata["instrument_fields"] = instrument_fields
    proxy["metadata"] = metadata
    return proxy


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _field_value(mapping: Mapping[str, Any], key: str) -> Any:
    value = mapping.get(key)
    if value is None:
        value = _instrument_fields(mapping).get(key)
    return value


def _execution_fees(execution_instrument: Mapping[str, Any]) -> ExecutionFeeContract:
    maker = _coerce_float(_field_value(execution_instrument, "maker_fee_rate"), 0.0) or 0.0
    taker = _coerce_float(_field_value(execution_instrument, "taker_fee_rate"), 0.0) or 0.0
    source = "instrument"
    if maker == 0.0 and taker == 0.0:
        source = "default_zero"
    return ExecutionFeeContract(
        maker_fee_rate=float(maker),
        taker_fee_rate=float(taker),
        source=source,
    )


def _extract_risk_contract(
    risk_config: Optional[Mapping[str, Any]],
    instrument: Mapping[str, Any],
) -> RiskConfigContract:
    normalized = normalise_risk_config(risk_config)
    base_risk_per_trade = _coerce_float(normalized.get("base_risk_per_trade"))
    global_risk_multiplier = _coerce_float(normalized.get("global_risk_multiplier"), 1.0) or 1.0
    instrument_risk_multiplier = _coerce_float(
        normalized.get("instrument_risk_multiplier"),
        _coerce_float(instrument.get("risk_multiplier"), 1.0) or 1.0,
    ) or 1.0
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
    risk_config: Optional[Mapping[str, Any]] = None,
    require_margin_accounting: bool = False,
    allowed_source_instrument_types: Optional[Iterable[str]] = None,
    execution_semantics: Optional[object] = None,
    research_market_role: Optional[object] = None,
) -> SeriesExecutionProfile:
    """Compile a canonical runtime profile from instrument and risk contracts."""

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

    allowed_source_types = _allowed_types(allowed_source_instrument_types) if allowed_source_instrument_types else ()
    resolved_execution_semantics = normalize_execution_semantics(
        execution_semantics,
        instrument_type=instrument_type,
    )
    derivative_semantics = resolved_execution_semantics in {"derivative", "proxy_derivative"}
    if allowed_source_types and instrument_type not in allowed_source_types:
        raise ValueError(
            "source instrument type is not allowed for this runtime binding "
            f"(got instrument_type={instrument_type or 'missing'}, allowed={','.join(allowed_source_types)})."
        )
    if require_margin_accounting and not derivative_semantics:
        raise ValueError("margin-accounting runtime requires derivative or proxy_derivative execution semantics.")

    symbol = str(instrument.get("symbol") or "").strip()
    if not symbol:
        raise ValueError("instrument symbol missing. Validate instrument metadata before runtime.")

    execution_instrument = _execution_instrument(
        instrument,
        execution_semantics=resolved_execution_semantics,
    )

    amount_constraints = resolve_amount_constraints(execution_instrument)
    tick_size = _coerce_float(_field_value(execution_instrument, "tick_size"))
    contract_size = _coerce_float(_field_value(execution_instrument, "contract_size"))
    tick_value = _coerce_float(_field_value(execution_instrument, "tick_value"))
    if tick_value in (None, 0) and tick_size not in (None, 0) and contract_size not in (None, 0):
        tick_value = float(tick_size) * float(contract_size)

    if tick_size in (None, 0):
        raise ValueError(f"{symbol}: tick_size required for runtime execution.")
    if contract_size in (None, 0):
        raise ValueError(f"{symbol}: contract_size required for runtime execution.")
    if tick_value in (None, 0):
        raise ValueError(f"{symbol}: tick_value required for runtime execution.")

    margin_instrument = _execution_margin_instrument(
        execution_instrument,
        execution_semantics=resolved_execution_semantics,
    )
    margin_rates = extract_margin_rates(margin_instrument)
    try:
        margin_calculator, margin_calc_type = create_margin_calculator(margin_instrument)
    except ValueError as exc:
        if derivative_semantics or require_margin_accounting:
            raise ValueError(
                f"{symbol}: missing margin_rates. Derivative execution requires "
                "intraday/overnight margin configuration."
            ) from exc
        raise

    if (derivative_semantics or require_margin_accounting) and margin_calc_type != "margin":
        raise ValueError(
            f"{symbol}: missing margin_rates. Derivative execution requires "
            "intraday/overnight margin configuration."
        )

    base_currency = str(_field_value(instrument, "base_currency") or "").strip().upper()
    quote_currency = str(_field_value(instrument, "quote_currency") or "").strip().upper()
    if not base_currency or not quote_currency:
        raise ValueError(f"{symbol}: base_currency/quote_currency required for runtime execution.")

    supports_short = bool(_field_value(execution_instrument, "can_short"))
    if derivative_semantics and not supports_short:
        supports_short = True

    capabilities = ExecutionCapabilities(
        supports_margin=(margin_calc_type == "margin"),
        supports_short=supports_short,
        short_requires_borrow=False
        if resolved_execution_semantics == "proxy_derivative"
        else bool(_field_value(execution_instrument, "short_requires_borrow")),
        has_funding=bool(_field_value(execution_instrument, "has_funding")),
        has_expiry=bool(_field_value(execution_instrument, "expiry_ts")),
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
    fees = _execution_fees(execution_instrument)

    instrument_contract = InstrumentContract(
        instrument_id=str(instrument.get("id") or "").strip() or None,
        symbol=symbol,
        instrument_type=instrument_type or "unknown",
        source_instrument_type=instrument_type or "unknown",
        execution_semantics=resolved_execution_semantics,
        research_market_role=str(research_market_role or "").strip() or (
            "proxy_underlier" if resolved_execution_semantics == "proxy_derivative" else "execution_instrument"
        ),
        datasource=str(instrument.get("datasource") or "").strip() or None,
        exchange=str(instrument.get("exchange") or "").strip() or None,
        base_currency=base_currency,
        quote_currency=quote_currency,
        raw=instrument,
    )

    risk = _extract_risk_contract(risk_config, instrument)
    accounting_mode = "margin" if margin_calc_type == "margin" else None
    profile = SeriesExecutionProfile(
        instrument=instrument_contract,
        constraints=constraints,
        capabilities=capabilities,
        fees=fees,
        risk=risk,
        margin_calculator=margin_calculator,
        margin_calc_type=margin_calc_type,
        margin_rates=margin_rates,
        collateral_model=SimpleCollateralModel(accounting_mode=accounting_mode),
        validated_for_runtime=bool(require_margin_accounting or derivative_semantics),
    )
    return profile


__all__ = [
    "InstrumentContract",
    "ExecutionConstraintsContract",
    "ExecutionCapabilities",
    "ExecutionFeeContract",
    "RiskConfigContract",
    "CollateralModel",
    "SimpleCollateralModel",
    "SeriesExecutionProfile",
    "normalize_runtime_instrument_type",
    "normalize_execution_semantics",
    "compile_series_execution_profile",
]

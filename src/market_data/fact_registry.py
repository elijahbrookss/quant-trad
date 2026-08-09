"""Typed fact-contract registry shared by planning, freezing, and runtime."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping


NORMALIZED_FACT_PREFIX = "market.normalized."
NORMALIZED_FACT_VERSION = "market.normalized_feature.v1"


@dataclass(frozen=True)
class FactContract:
    fact_type: str
    contract_version: str
    timeframe_mode: str
    archive_policy: str
    record_time_field: str
    dataset_eligible: bool = True
    storage_shape: str = "specialized"
    numeric_type: str | None = None
    subject_type: str = "instrument"
    required_dimensions: tuple[str, ...] = ()
    optional_dimensions: tuple[str, ...] = ()
    series_identity_dimensions: tuple[str, ...] = ()
    uppercase_dimensions: tuple[str, ...] = ()
    allowed_units: tuple[str, ...] = ()
    unit_dimension: str | None = None
    minimum_value: Decimal | None = None
    minimum_inclusive: bool = True

    def validate(
        self, *, contract_version: str, timeframe_seconds: int | None
    ) -> None:
        actual_version = str(contract_version or "").strip()
        version_valid = actual_version == self.contract_version
        if self.fact_type.startswith(NORMALIZED_FACT_PREFIX):
            pattern = (
                rf"{re.escape(NORMALIZED_FACT_VERSION)}/nsp_[0-9a-f]{{31}}"
            )
            version_valid = bool(re.fullmatch(pattern, actual_version))
        if not version_valid:
            raise ValueError(
                "market_fact_contract_mismatch: "
                f"fact_type={self.fact_type} expected={self.contract_version} "
                f"actual={contract_version or '<missing>'}"
            )
        if self.timeframe_mode == "required" and (
            timeframe_seconds is None or int(timeframe_seconds) <= 0
        ):
            raise ValueError(
                f"market_fact_contract_invalid: fact_type={self.fact_type} facts "
                "require timeframe_seconds"
            )
        if self.timeframe_mode == "forbidden" and timeframe_seconds is not None:
            raise ValueError(
                f"market_fact_contract_invalid: fact_type={self.fact_type} "
                "facts do not have a timeframe"
            )

    @property
    def default_alignment(self) -> str:
        return (
            "exact_interval"
            if self.timeframe_mode == "required"
            else "latest_known"
        )

    @property
    def uses_exact_numeric_storage(self) -> bool:
        return self.storage_shape == "exact_numeric"

    def normalize_dimensions(self, dimensions: Mapping[str, Any] | None) -> dict[str, str]:
        """Validate the contract-enumerated dimensions used in series identity."""

        raw = dict(dimensions or {})
        allowed = set(self.required_dimensions) | set(self.optional_dimensions)
        unexpected = sorted(str(key) for key in raw if str(key) not in allowed)
        if unexpected:
            raise ValueError(
                "market_fact_dimensions_invalid: "
                f"fact_type={self.fact_type} unexpected={','.join(unexpected)}"
            )
        missing = sorted(key for key in self.required_dimensions if key not in raw)
        if missing:
            raise ValueError(
                "market_fact_dimensions_invalid: "
                f"fact_type={self.fact_type} missing={','.join(missing)}"
            )
        normalized: dict[str, str] = {}
        for key in sorted(raw):
            if isinstance(raw[key], bool):
                raise ValueError(
                    "market_fact_dimensions_invalid: "
                    f"fact_type={self.fact_type} dimension={key} must be a string or integer"
                )
            value = str(raw[key]).strip()
            if not value:
                raise ValueError(
                    "market_fact_dimensions_invalid: "
                    f"fact_type={self.fact_type} dimension={key} is empty"
                )
            if key in self.uppercase_dimensions:
                value = value.upper()
            normalized[str(key)] = value
        return normalized

    def validate_numeric_value(
        self,
        *,
        value: Decimal,
        unit: str,
        dimensions: Mapping[str, Any] | None,
    ) -> tuple[str, dict[str, str]]:
        """Validate an exact numeric value without admitting provider-specific shape."""

        if not self.uses_exact_numeric_storage or self.numeric_type != "decimal":
            raise ValueError(
                f"market_fact_numeric_contract_invalid: fact_type={self.fact_type}"
            )
        normalized_dimensions = self.normalize_dimensions(dimensions)
        normalized_unit = str(unit or "").strip().upper()
        if not normalized_unit:
            raise ValueError(
                f"market_fact_numeric_contract_invalid: fact_type={self.fact_type} unit is required"
            )
        allowed_units = {item.upper() for item in self.allowed_units}
        if allowed_units and normalized_unit not in allowed_units:
            raise ValueError(
                "market_fact_numeric_contract_invalid: "
                f"fact_type={self.fact_type} unit={normalized_unit}"
            )
        if self.unit_dimension is not None:
            expected_unit = normalized_dimensions[self.unit_dimension].upper()
            if normalized_unit != expected_unit:
                raise ValueError(
                    "market_fact_numeric_contract_invalid: "
                    f"fact_type={self.fact_type} unit={normalized_unit} "
                    f"expected_from_dimension={expected_unit}"
                )
        if self.minimum_value is not None:
            below = value < self.minimum_value
            excluded_equal = not self.minimum_inclusive and value == self.minimum_value
            if below or excluded_equal:
                operator = ">=" if self.minimum_inclusive else ">"
                raise ValueError(
                    "market_fact_numeric_contract_invalid: "
                    f"fact_type={self.fact_type} value must be {operator} {self.minimum_value}"
                )
        return normalized_unit, normalized_dimensions


_CONTRACTS = {
    "candle.ohlcv": FactContract(
        "candle.ohlcv", "candle.ohlcv.v1", "required", "none", "open_time"
    ),
    "derivatives.open_interest": FactContract(
        "derivatives.open_interest",
        "derivatives.open_interest.v1",
        "forbidden",
        "none",
        "sample_time",
    ),
    "derivatives.funding_rate": FactContract(
        "derivatives.funding_rate",
        "derivatives.funding_rate.v1",
        "forbidden",
        "none",
        "sample_time",
    ),
    "market.l2_book": FactContract(
        "market.l2_book",
        "market.l2_book.v1",
        "forbidden",
        "raw_required",
        "effective_at",
        False,
    ),
    "market.trade": FactContract(
        "market.trade", "market.trade.v1", "forbidden", "raw_required", "provider_event_time"
    ),
    "market.trade_flow": FactContract(
        "market.trade_flow", "market.trade_flow.v1", "required", "raw_required", "bucket_start"
    ),
    "market.bbo": FactContract(
        "market.bbo", "market.bbo.v1", "required", "raw_required", "bucket_start"
    ),
    "market.depth_observation": FactContract(
        "market.depth_observation",
        "market.depth_band.v1",
        "required",
        "raw_required",
        "bucket_start",
    ),
    "market.trade_flow_feature": FactContract(
        "market.trade_flow_feature",
        "market.trade_flow_feature.v1",
        "required",
        "raw_required",
        "bucket_start",
    ),
    "market.futures_spot_relationship": FactContract(
        "market.futures_spot_relationship",
        "market.futures_spot_basis.v1",
        "required",
        "raw_required",
        "effective_at",
    ),
    "market.derivative_state": FactContract(
        "market.derivative_state",
        "market.derivative_state.v1",
        "forbidden",
        "none",
        "effective_at",
    ),
    "market.reference_price": FactContract(
        "market.reference_price",
        "market.reference_price.v1",
        "forbidden",
        "none",
        "effective_at",
        storage_shape="exact_numeric",
        numeric_type="decimal",
        subject_type="instrument",
        required_dimensions=("quote_currency",),
        series_identity_dimensions=("quote_currency",),
        uppercase_dimensions=("quote_currency",),
        unit_dimension="quote_currency",
        minimum_value=Decimal("0"),
        minimum_inclusive=False,
    ),
    "market.reserve_balance": FactContract(
        "market.reserve_balance",
        "market.reserve_balance.v1",
        "forbidden",
        "none",
        "effective_at",
        storage_shape="exact_numeric",
        numeric_type="decimal",
        subject_type="instrument",
        required_dimensions=("reserve_unit",),
        series_identity_dimensions=("reserve_unit",),
        uppercase_dimensions=("reserve_unit",),
        unit_dimension="reserve_unit",
        minimum_value=Decimal("0"),
        minimum_inclusive=True,
    ),
    "market.market_response": FactContract(
        "market.market_response",
        "market.market_response.v1",
        "required",
        "raw_required",
        "effective_at",
    ),
}


def get_fact_contract(fact_type: str) -> FactContract:
    normalized = str(fact_type or "").strip().lower()
    if normalized.startswith(NORMALIZED_FACT_PREFIX) and len(normalized) > len(
        NORMALIZED_FACT_PREFIX
    ):
        return FactContract(
            normalized,
            NORMALIZED_FACT_VERSION,
            "optional",
            "source_dependent",
            "effective_at",
        )
    contract = _CONTRACTS.get(normalized)
    if contract is None:
        raise ValueError(
            f"market_fact_contract_unsupported: fact_type={normalized or '<missing>'}"
        )
    return contract


def supported_fact_contracts() -> tuple[FactContract, ...]:
    return tuple(_CONTRACTS[key] for key in sorted(_CONTRACTS))


__all__ = [
    "FactContract",
    "NORMALIZED_FACT_PREFIX",
    "NORMALIZED_FACT_VERSION",
    "get_fact_contract",
    "supported_fact_contracts",
]

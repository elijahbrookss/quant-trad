"""Typed fact-contract registry shared by planning, freezing, and runtime."""

from __future__ import annotations

import re
from dataclasses import dataclass


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

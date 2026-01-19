"""Execution adapter interfaces for bot runtime."""

from __future__ import annotations

from typing import Optional, Protocol, Tuple

from .execution import FillRejection, FillResult, SpotExecutionModel, DerivativesExecutionModel


class ExecutionAdapter(Protocol):
    """Abstract execution layer used by the risk engine."""

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        ...


class SpotExecutionAdapter:
    """Adapter that forwards to the existing spot execution model."""

    def __init__(self, model: SpotExecutionModel) -> None:
        self._model = model

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        return self._model.fill_market(
            side=side,
            requested_qty=requested_qty,
            price=price,
            fee_rate=fee_rate,
            enforce_price_tick=enforce_price_tick,
        )


class DerivativesExecutionAdapter:
    """Adapter that forwards to the derivatives execution model."""

    def __init__(self, model: DerivativesExecutionModel) -> None:
        self._model = model

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        return self._model.fill_market(
            side=side,
            requested_qty=requested_qty,
            price=price,
            fee_rate=fee_rate,
            enforce_price_tick=enforce_price_tick,
        )


__all__ = ["ExecutionAdapter", "SpotExecutionAdapter", "DerivativesExecutionAdapter"]

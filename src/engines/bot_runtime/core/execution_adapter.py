"""Execution adapter interfaces for bot runtime."""

from __future__ import annotations

from typing import Optional, Protocol, Tuple

from .execution import FillRejection, FillResult, SpotExecutionModel, DerivativesExecutionModel
from .execution_order import FillOrder


class ExecutionAdapter(Protocol):
    """Abstract execution layer used by the risk engine."""

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        ...


class SpotExecutionAdapter:
    """Adapter that forwards typed orders to the spot execution model."""

    def __init__(self, model: SpotExecutionModel) -> None:
        self._model = model

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        return self._model.execute_order(order)


class DerivativesExecutionAdapter:
    """Adapter that forwards typed orders to the derivatives execution model."""

    def __init__(self, model: DerivativesExecutionModel) -> None:
        self._model = model

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        return self._model.execute_order(order)


__all__ = ["ExecutionAdapter", "SpotExecutionAdapter", "DerivativesExecutionAdapter"]

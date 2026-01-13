"""Run-type adapter for deterministic backtest fills."""

from __future__ import annotations

from typing import Optional, Tuple

from ..core.execution import (
    DerivativesExecutionConstraints,
    DerivativesExecutionModel,
    FillRejection,
    FillResult,
    SpotExecutionConstraints,
    SpotExecutionModel,
)
from ..core.execution_adapter import ExecutionAdapter


class BacktestAdapter(ExecutionAdapter):
    """Deterministic execution adapter for backtest fills."""

    def __init__(
        self,
        *,
        tick_size: float,
        qty_step: float,
        min_qty: float,
        min_notional: float,
        contract_size: float,
        short_requires_borrow: bool,
        slippage_bps: float = 0.0,
    ) -> None:
        if short_requires_borrow:
            self._model = SpotExecutionModel(
                SpotExecutionConstraints(
                    tick_size=tick_size,
                    qty_step=qty_step,
                    min_qty=min_qty,
                    min_notional=min_notional,
                ),
                slippage_bps=slippage_bps,
            )
        else:
            self._model = DerivativesExecutionModel(
                DerivativesExecutionConstraints(
                    tick_size=tick_size,
                    qty_step=qty_step,
                    min_qty=min_qty,
                    min_notional=min_notional,
                    contract_size=contract_size,
                ),
                slippage_bps=slippage_bps,
            )

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


__all__ = ["BacktestAdapter"]

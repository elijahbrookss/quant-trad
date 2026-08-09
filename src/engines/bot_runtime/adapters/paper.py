"""Run-type adapter for simulated paper fills."""

from __future__ import annotations

from typing import Optional, Tuple

from ..core.execution import FillRejection, FillResult
from ..core.execution_adapter import ExecutionAdapter
from ..core.execution_assumptions import ResolvedExecutionAssumptions
from ..core.execution_context import ResolvedExecutionContext
from ..core.execution_order import FillOrder
from .backtest import BacktestAdapter


class PaperAdapter(ExecutionAdapter):
    """Execution adapter for paper trading (simulated fills)."""

    def __init__(
        self,
        *,
        tick_size: float,
        qty_step: float,
        min_qty: float,
        min_notional: float,
        contract_size: float,
        short_requires_borrow: bool,
        max_qty: Optional[float] = None,
        amount_precision: Optional[int] = None,
        slippage_bps: float = 0.0,
        assumptions: ResolvedExecutionAssumptions | None = None,
        execution_context: ResolvedExecutionContext | None = None,
    ) -> None:
        self._delegate = BacktestAdapter(
            tick_size=tick_size,
            qty_step=qty_step,
            min_qty=min_qty,
            min_notional=min_notional,
            contract_size=contract_size,
            short_requires_borrow=short_requires_borrow,
            max_qty=max_qty,
            amount_precision=amount_precision,
            slippage_bps=slippage_bps,
            assumptions=assumptions,
            execution_context=execution_context,
        )

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        return self._delegate.execute_order(order)


__all__ = ["PaperAdapter"]

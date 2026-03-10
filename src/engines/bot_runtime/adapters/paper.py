"""Run-type adapter for simulated paper fills."""

from __future__ import annotations

from typing import Optional, Tuple

from ..core.execution import FillRejection, FillResult
from ..core.execution_adapter import ExecutionAdapter
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
        return self._delegate.fill_market(
            side=side,
            requested_qty=requested_qty,
            price=price,
            fee_rate=fee_rate,
            enforce_price_tick=enforce_price_tick,
        )


__all__ = ["PaperAdapter"]

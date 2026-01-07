"""Run-type adapter for perpetual/futures fills."""

from __future__ import annotations

from typing import Optional, Tuple

from ..core.execution import FillRejection, FillResult, SpotExecutionConstraints, SpotExecutionModel
from ..core.execution_adapter import ExecutionAdapter


class PerpExecutionAdapter(ExecutionAdapter):
    """Execution adapter for derivatives that enforces size constraints."""

    def __init__(
        self,
        *,
        tick_size: float,
        qty_step: float,
        min_qty: float,
        min_notional: float,
    ) -> None:
        self._model = SpotExecutionModel(
            SpotExecutionConstraints(
                tick_size=tick_size,
                qty_step=qty_step,
                min_qty=min_qty,
                min_notional=min_notional,
            )
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


__all__ = ["PerpExecutionAdapter"]

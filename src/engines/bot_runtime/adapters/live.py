"""Run-type adapter for live execution."""

from __future__ import annotations

from typing import Optional, Tuple

from ..core.execution import FillRejection, FillResult
from ..core.execution_adapter import ExecutionAdapter


class LiveAdapter(ExecutionAdapter):
    """Execution adapter that forwards to provided live executors."""

    def __init__(
        self,
        *,
        short_requires_borrow: bool,
        spot_adapter: Optional[ExecutionAdapter] = None,
        derivatives_adapter: Optional[ExecutionAdapter] = None,
    ) -> None:
        self._short_requires_borrow = bool(short_requires_borrow)
        self._spot_adapter = spot_adapter
        self._derivatives_adapter = derivatives_adapter

    def fill_market(
        self,
        *,
        side: str,
        requested_qty: float,
        price: float,
        fee_rate: float,
        enforce_price_tick: bool,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        adapter = self._spot_adapter if self._short_requires_borrow else self._derivatives_adapter
        if not adapter:
            raise ValueError("LiveAdapter requires a configured execution adapter for this instrument.")
        return adapter.fill_market(
            side=side,
            requested_qty=requested_qty,
            price=price,
            fee_rate=fee_rate,
            enforce_price_tick=enforce_price_tick,
        )


__all__ = ["LiveAdapter"]

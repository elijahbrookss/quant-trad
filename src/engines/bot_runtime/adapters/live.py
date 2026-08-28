"""Reserved fail-closed seam for a possible future live execution mode."""

from __future__ import annotations

from typing import Optional, Tuple

from ..core.execution import FillRejection, FillResult
from ..core.execution_adapter import ExecutionAdapter
from ..core.execution_order import FillOrder


class LiveAdapter(ExecutionAdapter):
    """Keep the live composition seam closed until owner authority admits it."""

    def __init__(
        self,
        *,
        short_requires_borrow: bool,
        spot_adapter: Optional[ExecutionAdapter] = None,
        derivatives_adapter: Optional[ExecutionAdapter] = None,
    ) -> None:
        if spot_adapter is not None or derivatives_adapter is not None:
            raise ValueError(
                "external order submission is closed; live execution adapters are not admitted"
            )
        self._short_requires_borrow = bool(short_requires_borrow)

    def execute_order(
        self,
        order: FillOrder,
    ) -> Tuple[Optional[FillResult], Optional[FillRejection]]:
        del order
        raise RuntimeError(
            "external order submission is closed; live execution is a reserved seam"
        )


__all__ = ["LiveAdapter"]

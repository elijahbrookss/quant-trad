"""Execution model interface for bot runtime."""

from __future__ import annotations

from typing import Optional, Protocol, Tuple

from .execution import FillRejection
from .execution_intent import ExecutionIntent, ExecutionOutcome


class ExecutionModel(Protocol):
    """Execution abstraction used by the bot runtime domain."""

    def submit(self, intent: ExecutionIntent) -> ExecutionOutcome:
        ...

    def evaluate(
        self,
        intent: ExecutionIntent,
        *,
        candle_high: float,
        candle_low: float,
        candle_close: float,
        candle_open: float,
    ) -> Tuple[ExecutionOutcome, Optional[FillRejection]]:
        ...


__all__ = ["ExecutionModel"]

"""Signal consumption utilities for bot runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from engines.bot_runtime.core.domain import StrategySignal


@dataclass(frozen=True)
class SignalConsumption:
    epoch: int
    consumed_signals: List[Dict[str, object]]
    chosen_signal: Optional[Dict[str, object]]


def consume_signals(
    signals: Deque[StrategySignal],
    *,
    epoch: int,
    last_consumed_epoch: int,
) -> Tuple[List[StrategySignal], Optional[StrategySignal], int]:
    consumed: List[StrategySignal] = []
    while signals and signals[0].epoch <= epoch:
        signal = signals.popleft()
        if signal.epoch <= last_consumed_epoch:
            continue
        consumed.append(signal)
    chosen = consumed[-1] if consumed else None
    updated_last = last_consumed_epoch
    if consumed:
        updated_last = max(updated_last, consumed[-1].epoch)
    return consumed, chosen, updated_last

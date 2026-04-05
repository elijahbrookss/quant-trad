"""Shared runtime models and helpers."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

from engines.bot_runtime.core.domain import (
    Candle,
    StrategySignal,
    coerce_float,
    isoformat,
    timeframe_to_seconds,
)
from engines.indicator_engine.contracts import OutputType, RuntimeOutput, RuntimeOverlay
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from strategies.evaluator import DecisionEvaluationState

DEFAULT_SIM_LOOKBACK_DAYS = 7
MAX_LOG_ENTRIES = 500
MAX_WARNING_ENTRIES = 20
MAX_SIGNAL_CONSUMPTIONS = 500
INTRABAR_BASE_SECONDS = 0.4
WALK_FORWARD_SAMPLE_INTERVAL = 50
OVERLAY_SUMMARY_INTERVAL = 50


@dataclass
class SeriesExecutionState:
    series: Any
    bar_index: int = 0
    total_bars: int = 0
    next_step_at: Optional[datetime] = None
    intrabar_candles: List[Candle] = field(default_factory=list)
    intrabar_index: int = 0
    active_candle: Optional[Candle] = None
    done: bool = False
    last_evaluated_epoch: int = 0
    last_consumed_epoch: int = 0
    pending_signals: Deque[StrategySignal] = field(default_factory=deque)
    signal_consumptions: Deque[Any] = field(
        default_factory=lambda: deque(maxlen=MAX_SIGNAL_CONSUMPTIONS)
    )
    indicator_engine: Optional[IndicatorExecutionEngine] = None
    indicator_outputs: Dict[str, RuntimeOutput] = field(default_factory=dict)
    indicator_overlays: Dict[str, RuntimeOverlay] = field(default_factory=dict)
    indicator_output_types: Dict[str, OutputType] = field(default_factory=dict)
    overlay_runtime_metrics: Dict[str, float] = field(default_factory=dict)
    decision_evaluation_state: DecisionEvaluationState = field(default_factory=DecisionEvaluationState)
    decision_artifacts: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=2000))
    rejection_artifacts: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=1000))

    def intrabar_active(self) -> bool:
        return bool(self.intrabar_candles) and self.intrabar_index < len(self.intrabar_candles)


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    return coerce_float(value, default)


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    return isoformat(value)


def _timeframe_to_seconds(label: Optional[str]) -> Optional[int]:
    return timeframe_to_seconds(label)


__all__ = [
    "DEFAULT_SIM_LOOKBACK_DAYS",
    "MAX_LOG_ENTRIES",
    "MAX_WARNING_ENTRIES",
    "MAX_SIGNAL_CONSUMPTIONS",
    "INTRABAR_BASE_SECONDS",
    "WALK_FORWARD_SAMPLE_INTERVAL",
    "OVERLAY_SUMMARY_INTERVAL",
    "SeriesExecutionState",
    "_coerce_float",
    "_isoformat",
    "_timeframe_to_seconds",
]

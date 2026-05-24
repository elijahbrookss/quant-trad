"""Series-builder runtime models."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

from engines.bot_runtime.core.domain import Candle, LadderRiskEngine, StrategySignal
from engines.bot_runtime.core.execution_profile import SeriesExecutionProfile


@dataclass
class StrategySeries:
    """Runtime payload describing a single strategy stream."""

    strategy_id: str
    name: str
    symbol: str
    timeframe: str
    datasource: Optional[str]
    exchange: Optional[str]
    # NOTE: Per-series in-memory cache of candles/signals for runtime execution.
    candles: List[Candle]
    signals: Deque[StrategySignal] = field(default_factory=deque)
    overlays: List[Dict[str, Any]] = field(default_factory=list)
    risk_engine: Optional[LadderRiskEngine] = None
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    last_consumed_epoch: int = 0
    instrument: Optional[Dict[str, Any]] = None
    atm_template: Dict[str, Any] = field(default_factory=dict)
    trade_overlay: Optional[Dict[str, Any]] = None
    replay_start_index: int = 0
    bootstrap_completed: bool = False
    bootstrap_indicator_overlays: int = 0
    bootstrap_total_overlays: int = 0
    execution_profile: Optional[SeriesExecutionProfile] = None


__all__ = ["StrategySeries"]

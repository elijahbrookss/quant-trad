"""Strategy series model for backtest runtime."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

from .models import Candle, StrategySignal
from .risk_engine import LadderRiskEngine


@dataclass
class StrategySeries:
    """Runtime payload describing a single strategy stream."""

    strategy_id: str
    name: str
    symbol: str
    timeframe: str
    datasource: Optional[str]
    exchange: Optional[str]
    candles: List[Candle]
    signals: Deque[StrategySignal] = field(default_factory=deque)
    overlays: List[Dict[str, Any]] = field(default_factory=list)
    risk_engine: LadderRiskEngine = field(default_factory=LadderRiskEngine)
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    last_consumed_epoch: int = 0
    instrument: Optional[Dict[str, Any]] = None
    atm_template: Dict[str, Any] = field(default_factory=dict)
    trade_overlay: Optional[Dict[str, Any]] = None

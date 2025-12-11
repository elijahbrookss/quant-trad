"""Backtest engine for simulating trading strategies.

This module provides the core backtesting infrastructure for running
trading strategies in simulation mode.
"""

from .models import Candle, Leg, StrategySignal
from .position import LadderPosition
from .risk_engine import DEFAULT_RISK, LadderRiskEngine
from .series import StrategySeries
from .utils import (
    coerce_float,
    instrument_key,
    isoformat,
    timeframe_duration,
    timeframe_to_seconds,
)

__all__ = [
    # Models
    "Candle",
    "Leg",
    "StrategySignal",
    # Position Management
    "LadderPosition",
    # Risk Engine
    "LadderRiskEngine",
    "DEFAULT_RISK",
    # Series
    "StrategySeries",
    # Utilities
    "coerce_float",
    "instrument_key",
    "isoformat",
    "timeframe_duration",
    "timeframe_to_seconds",
]

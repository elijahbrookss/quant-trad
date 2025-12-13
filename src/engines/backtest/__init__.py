"""Backtest engine for simulating trading strategies.

This module provides the core backtesting infrastructure for running
trading strategies in simulation mode.
"""

from .domain.models import Candle, Leg, StrategySignal
from .domain.position import LadderPosition
from .domain.series import StrategySeries
from .services.risk_engine import LadderRiskEngine
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
    # Series
    "StrategySeries",
    # Utilities
    "coerce_float",
    "instrument_key",
    "isoformat",
    "timeframe_duration",
    "timeframe_to_seconds",
]

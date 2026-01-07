"""Run-type adapters for bot runtime execution."""

from .backtest import BacktestAdapter
from .perp import PerpExecutionAdapter

__all__ = ["BacktestAdapter", "PerpExecutionAdapter"]

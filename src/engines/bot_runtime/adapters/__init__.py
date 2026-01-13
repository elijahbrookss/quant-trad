"""Run-type adapters for bot runtime execution."""

from .backtest import BacktestAdapter
from .live import LiveAdapter
from .paper import PaperAdapter

__all__ = ["BacktestAdapter", "PaperAdapter", "LiveAdapter"]

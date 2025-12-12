"""Signal execution helpers for pivot level indicators."""

from __future__ import annotations

import logging
from typing import Any, Optional

import pandas as pd

from indicators.pivot_level import PivotLevelIndicator
from signals.engine.generators.base import BaseSignalGenerator
from signals.rules.pivot import (
    pivot_breakout_rule,
    pivot_retest_rule,
)

logger = logging.getLogger("PivotLevelSignalGenerator")


class PivotLevelSignalGenerator(BaseSignalGenerator):
    """Convenience wrapper around the signal engine for pivot level indicators."""

    indicator_type = PivotLevelIndicator.NAME

    def __init__(self, indicator: PivotLevelIndicator, symbol: Optional[str] = None):
        super().__init__(indicator, symbol=symbol)

    def generate_signals(self, df: pd.DataFrame, **config: Any):
        """Execute registered pivot level rules and return their signals."""

        self._require_symbol()
        logger.debug(
            "Generating pivot signals | indicator=%s | rows=%d | config_keys=%s",
            getattr(self.indicator, "NAME", type(self.indicator).__name__),
            len(df) if hasattr(df, "__len__") else -1,
            sorted(config.keys()),
        )

        return super().generate_signals(df, **config)


__all__ = [
    "PivotLevelSignalGenerator",
]

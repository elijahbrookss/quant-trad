import numpy as np
from typing import List
from classes.StockData import StockData
from classes.indicators.BaseIndicator import BaseIndicator
from classes.indicators.TrendlineIndicator import TrendlineIndicator
from classes.indicators.VWAPIndicator import VWAPIndicator
from classes.indicators.LevelsIndicator import LevelsIndicator
from classes.indicators.MarketProfileIndicator import MarketProfileIndicator
from classes.Logger import logger



class SimpleStrategy:
    """Spin up all indicators & deliver an aggregate confidence score."""

    def __init__(
        self,
        symbol: str = "AAPL",
        start: str = "2024-01-01",
        end: str = "2025-01-01",
    ) -> None:
        self.data = StockData(symbol, start, end).df
        self.indicators: List[BaseIndicator] = [
            TrendlineIndicator(self.data),
            VWAPIndicator(self.data),
            LevelsIndicator(self.data),
            MarketProfileIndicator(self.data),
        ]
        self.confidence: float | None = None

    # ------------------------------------------------------------------
    def run(self) -> float:
        logger.info("Running SimpleStrategy for %s", self.data.iloc[-1].name.date())
        scores = []
        for ind in self.indicators:
            logger.info("Computing %s…", ind.NAME)
            ind.compute()
            ind.plot()
            if ind.score is not None:
                scores.append(ind.score)
        self.confidence = float(np.mean(scores)) if scores else 0.0
        logger.info("Aggregate confidence = %.2f", self.confidence)
        return self.confidence





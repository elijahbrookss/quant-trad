from typing import List, Dict
import pandas as pd
from classes.indicators.MarketProfileIndicator import breakout_rule
from mplfinance.plotting import make_addplot
from .base import BaseSignal
import logging

logger = logging.getLogger("MarketProfileSignalGenerator")


class MarketProfileSignalGenerator:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def generate_signals(
        self,
        df: pd.DataFrame,
        value_areas: List[Dict]
    ) -> List[BaseSignal]:
        """
        Run all market profile rules against value areas and create BaseSignal objects.
        """
        context = {
            "df": df,
            "symbol": self.symbol,
        }

        rules = [
            breakout_rule,
            # Add more rule functions here if needed
        ]

        raw_signals = []
        for va in value_areas:
            for rule in rules:
                raw_signals.extend(rule(context, va))

        signals = [
            BaseSignal(
                type=meta["type"],
                symbol=meta["symbol"],
                time=meta["time"],
                confidence=1.0,
                metadata={k: v for k, v in meta.items() if k not in {"type", "symbol", "time"}}
            )
            for meta in raw_signals
        ]

        return signals

    @staticmethod
    def to_overlays(
        signals: List["BaseSignal"],
        plot_df: pd.DataFrame,
        n: int = 3,
        offset: float = .6,
    ) -> List[Dict]:
        overlays = []
        logger.info("Converting %d signals to line overlays", len(signals))

        for idx, sig in enumerate(signals):
            if sig.metadata.get("source") != "MarketProfile":
                logger.debug("Skipping signal %d: not from MarketProfile source", idx)
                continue

            ts = sig.time
            if ts not in plot_df.index:
                nearest_idx = plot_df.index.get_indexer([ts], method="nearest")[0]
                ts = plot_df.index[nearest_idx]

            direction = sig.metadata.get("direction")
            base_y = sig.metadata.get("VAH") if sig.metadata.get("level_type") == "VAH" else sig.metadata.get("VAL")

            if base_y is None or direction not in {"up", "down"}:
                logger.warning("Signal %d missing direction or price level", idx)
                continue

            # Offset breakout line based on breakout direction
            y = base_y + offset if direction == "up" else base_y - offset

            center_idx = plot_df.index.get_indexer([ts], method="nearest")[0]
            start_idx = max(0, center_idx - n)
            end_idx = min(len(plot_df.index) - 1, center_idx + n)

            short_index = plot_df.index[start_idx:end_idx + 1]
            line_series = pd.Series(index=plot_df.index, dtype=float)
            line_series.loc[short_index] = y

            logger.debug(
                "Signal %d [%s] line from %s to %s at level %.2f",
                idx, direction, short_index[0], short_index[-1], y
            )

            ap = make_addplot(
                line_series,
                color="green" if direction == "up" else "red",
                linestyle="-",
                width=1.0,
            )

            ap["zorder"] = 6

            overlays.append({
                "kind": "addplot",
                "plot": ap,
                "label": f"Breakout {direction.capitalize()} {idx}"
            })

        logger.info("Converted %d signals to overlays", len(overlays))
        return overlays

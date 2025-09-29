from typing import List, Dict, Optional
import logging

import pandas as pd
from indicators.market_profile import MarketProfileIndicator
from mplfinance.plotting import make_addplot

from signals.base import BaseSignal
from signals.engine.signal_generator import (
    build_signal_overlays,
    register_indicator_rules,
    run_indicator_rules,
)
from signals.rules.market_profile import (
    market_profile_breakout_rule,
    market_profile_retest_rule,
)

logger = logging.getLogger("MarketProfileSignalGenerator")


class MarketProfileSignalGenerator:
    def __init__(self, indicator: MarketProfileIndicator, symbol: Optional[str] = None):
        self.indicator = indicator
        self.symbol = symbol or getattr(indicator, "symbol", None)

    def generate_signals(
        self,
        df: pd.DataFrame,
        value_areas: List[Dict]
    ) -> List[BaseSignal]:
        """Run registered Market Profile rules and convert outputs into signals."""
        if self.symbol is None:
            raise ValueError("MarketProfileSignalGenerator requires a symbol for rule execution")
        return run_indicator_rules(
            self.indicator,
            df,
            rule_payloads=value_areas,
            symbol=self.symbol,
        )

    @staticmethod
    def to_overlays(
        signals: List[BaseSignal],
        plot_df: pd.DataFrame,
        **kwargs,
    ) -> List[Dict]:
        return list(
            build_signal_overlays(
                MarketProfileIndicator.NAME,
                signals,
                plot_df,
                **kwargs,
            )
        )


def _market_profile_overlay_adapter(
    signals: List[BaseSignal],
    plot_df: pd.DataFrame,
    n: int = 3,
    offset: float = 0.2,
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
        base_y = (
            sig.metadata.get("VAH")
            if sig.metadata.get("level_type") == "VAH"
            else sig.metadata.get("VAL")
        )

        if base_y is None or direction not in {"up", "down"}:
            logger.warning("Signal %d missing direction or price level", idx)
            continue

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


register_indicator_rules(
    MarketProfileIndicator.NAME,
    rules=[market_profile_breakout_rule, market_profile_retest_rule],
    overlay_adapter=_market_profile_overlay_adapter,
)

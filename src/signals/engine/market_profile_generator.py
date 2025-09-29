from typing import List, Dict, Optional, Sequence, Mapping, Any
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


def _clone_indicator_for_runtime(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    interval: Optional[str] = None,
) -> Optional[MarketProfileIndicator]:
    """Create a lightweight indicator instance for signal evaluation."""

    if df is None or df.empty:
        return None

    try:
        runtime = MarketProfileIndicator(
            df=df.copy(),
            bin_size=getattr(indicator, "bin_size", 0.1),
            mode=getattr(indicator, "mode", "tpo"),
            interval=interval or getattr(indicator, "interval", "30m"),
        )
    except Exception:
        logger.exception("Failed to initialise MarketProfileIndicator for signal payloads")
        return None

    return runtime


def build_value_area_payloads(
    indicator: MarketProfileIndicator,
    df: pd.DataFrame,
    *,
    interval: Optional[str] = None,
    use_merged: Optional[bool] = None,
    merge_threshold: Optional[float] = None,
    min_merge_sessions: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Derive value area payloads for market profile signal rules."""

    runtime = _clone_indicator_for_runtime(indicator, df, interval=interval)
    if runtime is None:
        return []

    use_merged = True if use_merged is None else bool(use_merged)

    if use_merged:
        threshold = 0.6 if merge_threshold is None else float(merge_threshold)
        min_merge = 2 if min_merge_sessions is None else int(min_merge_sessions)
        value_areas = runtime.merge_value_areas(threshold=threshold, min_merge=min_merge)
    else:
        value_areas = runtime.daily_profiles

    payloads: List[Dict[str, Any]] = []
    for area in value_areas or []:
        if isinstance(area, Mapping) and area.get("VAH") is not None and area.get("VAL") is not None:
            payloads.append(dict(area))

    return payloads


class MarketProfileSignalGenerator:
    def __init__(self, indicator: MarketProfileIndicator, symbol: Optional[str] = None):
        self.indicator = indicator
        self.symbol = symbol or getattr(indicator, "symbol", None)

    def generate_signals(
        self,
        df: pd.DataFrame,
        value_areas: Optional[Sequence[Mapping[str, Any]]] = None,
        **config: Any,
    ) -> List[BaseSignal]:
        """Run registered Market Profile rules and convert outputs into signals."""
        if self.symbol is None:
            raise ValueError("MarketProfileSignalGenerator requires a symbol for rule execution")

        payloads = (
            list(value_areas)
            if value_areas is not None
            else build_value_area_payloads(
                self.indicator,
                df,
                interval=getattr(self.indicator, "interval", None),
                use_merged=config.get("market_profile_use_merged_value_areas"),
                merge_threshold=config.get("market_profile_merge_threshold"),
                min_merge_sessions=config.get("market_profile_merge_min_sessions"),
            )
        )
        return run_indicator_rules(
            self.indicator,
            df,
            rule_payloads=payloads,
            symbol=self.symbol,
            **config,
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
    *,
    n: int = 3,
    offset: float = 0.2,
    **kwargs: Any,
) -> List[Dict]:
    n = int(kwargs.get("market_profile_overlay_half_width", n))
    offset = float(kwargs.get("market_profile_overlay_offset", offset))
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

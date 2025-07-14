# signals/market_profile_signal.py

from typing import List, Dict
import pandas as pd
from datetime import datetime
from .base import BaseSignal
import logging

logger = logging.getLogger("MarketProfileSignalGenerator")


class MarketProfileSignalGenerator:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def generate_signals(
        self,
        price_df: pd.DataFrame,
        value_areas: List[Dict]
    ) -> List[BaseSignal]:
        signals = []

        for area in value_areas:
            start, end = area["start"], area["end"]
            VAL, VAH = area["VAL"], area["VAH"]

            # Filter price data for the session window
            session_df = price_df[(price_df.index >= start) & (price_df.index <= end)]

            # Loop over the session candles
            for i in range(1, len(session_df)):
                prev_close = session_df.iloc[i - 1]["close"]
                curr_close = session_df.iloc[i]["close"]
                curr_time = session_df.index[i]

                # Skip if previous candle was already outside VA
                if not (VAL <= prev_close <= VAH):
                    continue

                # Breakout above VAH
                if curr_close > VAH:
                    distance_pct = (curr_close - VAH) / VAH
                    signals.append(BaseSignal(
                        type="breakout",
                        symbol=self.symbol,
                        time=curr_time,
                        confidence=1.0,  # can improve this later
                        metadata={
                            "source": "MarketProfile",
                            "level_type": "VAH",
                            "distance_pct": round(distance_pct, 4),
                            "session_start": start,
                            "session_end": end,
                            "VAL": VAL,
                            "VAH": VAH,
                            "POC": area.get("POC")
                        }
                    ))
                    break  # only first breakout per session

                # Breakout below VAL
                elif curr_close < VAL:
                    distance_pct = (VAL - curr_close) / VAL
                    signals.append(BaseSignal(
                        type="breakout",
                        symbol=self.symbol,
                        time=curr_time,
                        confidence=1.0,
                        metadata={
                            "source": "MarketProfile",
                            "level_type": "VAL",  # or "VAL"
                            "distance_pct": round(distance_pct, 4),
                            "session_start": start,
                            "session_end": end,
                            "VAL": VAL,
                            "VAH": VAH,
                            "POC": area.get("POC") 
                        }
                    ))

                    break

        return signals

    @staticmethod
    def to_overlays(signals: List["BaseSignal"], plot_df: pd.DataFrame, marker_size: int = 60) -> List[Dict]:
        overlays = []
        logger.info("Converting %d signals to overlays", len(signals))

        for idx, sig in enumerate(signals):
            if sig.metadata.get("source") != "MarketProfile":
                logger.debug("Signal %d skipped: source=%s", idx, sig.metadata.get("source"))
                continue

            ts = sig.time
            if ts not in plot_df.index:
                nearest_idx = plot_df.index.get_indexer([ts], method="nearest")[0]
                ts = plot_df.index[nearest_idx]
                logger.debug("Signal %d: timestamp %s not in plot_df, using nearest %s", idx, sig.time, ts)

            level_type = sig.metadata.get("level_type")
            if level_type == "VAH":
                y = sig.metadata.get("VAH")
                marker = "v"
                color = "green"
            elif level_type == "VAL":
                y = sig.metadata.get("VAL")
                marker = "^"
                color = "red"
            else:
                logger.warning("Signal %d: unknown level_type %s", idx, level_type)
                continue


            logger.debug(
                "Signal %d: time=%s, y=%s, marker=%s, color=%s, label=%s",
                idx, ts, y, marker, color, "MarketProfile breakout"
            )
            if y is None:
                logger.warning("Signal %d: y-value is None for time=%s, level_type=%s", idx, ts, level_type)

            overlays.append({
                "x": [ts],
                "y": [y],
                "marker": marker,
                "markersize": marker_size,
                "color": color,
                "label": "MarketProfile breakout",
                "kind": "scatter",
                "scatter": True,
                "zorder": 5,
            })

        logger.info("Generated %d overlays from signals", len(overlays))
        return overlays

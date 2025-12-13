from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from signals.base import BaseSignal
from signals.engine.generators.base import BaseSignalGenerator
from signals.engine.market_profile import build_value_area_payloads
from signals.rules.common.utils import format_duration
from signals.rules.market_profile import (
    market_profile_breakout_rule,
    market_profile_retest_rule,
)

logger = logging.getLogger("MarketProfileSignalGenerator")


class MarketProfileSignalGenerator(BaseSignalGenerator):
    indicator_type = MarketProfileIndicator.NAME

    def __init__(self, indicator: MarketProfileIndicator, symbol: Optional[str] = None):
        super().__init__(indicator, symbol=symbol)

    def generate_signals(
        self,
        df: pd.DataFrame,
        value_areas: Optional[Sequence[Mapping[str, Any]]] = None,
        **config: Any,
    ) -> List[BaseSignal]:
        """Run registered Market Profile rules and convert outputs into signals."""

        self._require_symbol()
        start_time = perf_counter()

        if value_areas is not None:
            payloads = list(value_areas)
            payload_source = "provided"
            payload_duration = 0.0
        else:
            payload_start = perf_counter()
            payloads = build_value_area_payloads(
                self.indicator,
                df,
                runtime_indicator=self.indicator,
                interval=getattr(self.indicator, "interval", None),
                use_merged=config.get("market_profile_use_merged_value_areas"),
                merge_threshold=config.get("market_profile_merge_threshold"),
                min_merge_sessions=config.get("market_profile_merge_min_sessions"),
            )
            payload_duration = perf_counter() - payload_start
            payload_source = "computed"

        if payloads:
            logger.info(
                "Market profile signal payload summaries (%d):",
                len(payloads),
            )
            for idx, payload in enumerate(payloads, start=1):
                logger.info(
                    "  [%d] %s",
                    idx,
                    MarketProfileIndicator.describe_profile(payload),
                )
        else:
            logger.info("Market profile signal payload summaries: none")

        rules_start = perf_counter()
        signals = super().generate_signals(
            df,
            rule_payloads=payloads,
            **config,
        )
        rules_duration = perf_counter() - rules_start
        total_duration = perf_counter() - start_time

        logger.info(
            "Market profile signals | symbol=%s | profiles=%d | signals=%d | payload_source=%s | "
            "durations[payloads=%s, rules=%s, total=%s]",
            self.symbol,
            len(payloads),
            len(signals),
            payload_source,
            "n/a" if payload_source == "provided" else format_duration(payload_duration),
            format_duration(rules_duration),
            format_duration(total_duration),
        )

        return signals


__all__ = ["MarketProfileSignalGenerator"]

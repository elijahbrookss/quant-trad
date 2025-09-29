"""Signal execution helpers for pivot level indicators."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from indicators.pivot_level import PivotLevelIndicator
from signals.base import BaseSignal
from signals.engine.signal_generator import (
    IndicatorRegistration,
    build_signal_overlays,
    register_indicator_rules,
    run_indicator_rules,
)
from signals.rules.pivot import (
    pivot_breakout_rule,
    pivot_retest_rule,
    pivot_signals_to_overlays,
)

logger = logging.getLogger("PivotLevelSignalGenerator")


def ensure_registration(force: bool = False) -> None:
    """Ensure pivot level rules are registered with the signal engine."""

    from signals.engine import signal_generator

    desired_rules: Sequence = (pivot_breakout_rule, pivot_retest_rule)

    if force:
        signal_generator._REGISTRY[PivotLevelIndicator.NAME] = IndicatorRegistration(  # type: ignore[attr-defined]
            rules=desired_rules,
            overlay_adapter=pivot_signals_to_overlays,
        )
        return

    registration = signal_generator._REGISTRY.get(PivotLevelIndicator.NAME)
    if registration is None:
        try:
            register_indicator_rules(
                PivotLevelIndicator.NAME,
                rules=desired_rules,
                overlay_adapter=pivot_signals_to_overlays,
            )
        except ValueError:
            signal_generator._REGISTRY[PivotLevelIndicator.NAME] = IndicatorRegistration(  # type: ignore[attr-defined]
                rules=desired_rules,
                overlay_adapter=pivot_signals_to_overlays,
            )
        return

    if tuple(registration.rules) != desired_rules or registration.overlay_adapter is None:
        signal_generator._REGISTRY[PivotLevelIndicator.NAME] = IndicatorRegistration(  # type: ignore[attr-defined]
            rules=desired_rules,
            overlay_adapter=pivot_signals_to_overlays,
        )


class PivotLevelSignalGenerator:
    """Convenience wrapper around the signal engine for pivot level indicators."""

    def __init__(self, indicator: PivotLevelIndicator, symbol: Optional[str] = None):
        self.indicator = indicator
        self.symbol = symbol or getattr(indicator, "symbol", None)

    def generate_signals(self, df: pd.DataFrame, **config: Any) -> List[BaseSignal]:
        """Execute registered pivot level rules and return their signals."""

        if self.symbol is None:
            raise ValueError(
                "PivotLevelSignalGenerator requires a symbol for rule execution"
            )

        logger.debug(
            "Generating pivot signals | indicator=%s | rows=%d | config_keys=%s",
            getattr(self.indicator, "NAME", type(self.indicator).__name__),
            len(df) if hasattr(df, "__len__") else -1,
            sorted(config.keys()),
        )

        return run_indicator_rules(
            self.indicator,
            df,
            symbol=self.symbol,
            **config,
        )

    @staticmethod
    def to_overlays(
        signals: Sequence[BaseSignal],
        plot_df: pd.DataFrame,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Convert pivot level signals into overlay payloads."""

        return list(
            build_signal_overlays(
                PivotLevelIndicator.NAME,
                signals,
                plot_df,
                **kwargs,
            )
        )


ensure_registration()


__all__ = [
    "PivotLevelSignalGenerator",
    "ensure_registration",
]

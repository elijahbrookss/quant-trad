"""Shared helpers for decorator-registered signal generators."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from signals.base import BaseSignal
from signals.engine.signal_generator import build_signal_overlays, run_indicator_rules

try:  # pragma: no cover - optional import for type checking only
    from pandas import DataFrame  # type: ignore
except Exception:  # pragma: no cover
    DataFrame = Any  # fallback for environments without pandas


log = logging.getLogger(__name__)


class BaseSignalGenerator:
    """Base orchestration wrapper for running registered indicator rules."""

    indicator_type: Optional[str] = None

    def __init__(self, indicator: Any, symbol: Optional[str] = None):
        self.indicator = indicator
        self.symbol = symbol or getattr(indicator, "symbol", None)
        if self.indicator_type is None:
            self.indicator_type = getattr(indicator, "NAME", indicator.__class__.__name__)

    def _require_symbol(self) -> str:
        if self.symbol is None:
            raise ValueError(
                f"{type(self).__name__} requires a symbol for rule execution"
            )
        return self.symbol

    def generate_signals(self, df: "DataFrame", **config: Any) -> List[BaseSignal]:
        symbol = self._require_symbol()

        log.debug(
            "Running indicator rules | generator=%s | rows=%s | config_keys=%s",
            type(self).__name__,
            len(df) if hasattr(df, "__len__") else "?",
            sorted(config.keys()),
        )

        return run_indicator_rules(
            self.indicator,
            df,
            symbol=symbol,
            **config,
        )

    @classmethod
    def to_overlays(
        cls,
        signals: Sequence[BaseSignal],
        plot_df: "DataFrame",
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        indicator_type = cls.indicator_type
        if not indicator_type:
            raise ValueError("indicator_type is required to build overlays")

        return list(
            build_signal_overlays(
                indicator_type,
                signals,
                plot_df,
                **kwargs,
            )
        )


__all__ = ["BaseSignalGenerator"]

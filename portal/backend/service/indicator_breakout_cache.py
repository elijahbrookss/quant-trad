"""Breakout cache management and overlay helpers for indicator signals."""

from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

from indicators.market_profile import MarketProfileIndicator
from indicators.config import DataContext
from indicators.pivot_level import PivotLevelIndicator
from signals.engine.market_profile import resolve_market_profile_params
from signals.base import BaseSignal
from signals.rules.market_profile import (
    MarketProfileBreakoutConfig,
    _BREAKOUT_CACHE_INITIALISED,
    _BREAKOUT_CACHE_KEY,
    _BREAKOUT_READY_FLAG,
)
from signals.rules.pivot import PivotBreakoutConfig, _PIVOT_BREAKOUT_READY_FLAG


@dataclass(frozen=True)
class BreakoutCacheSpec:
    breakout_rule_id: str
    retest_rule_id: str
    cache_context_key: str
    ready_flag_key: str
    initialised_flag_key: Optional[str]
    config_signature_builder: Callable[[Mapping[str, Any]], Tuple[Any, ...]]
    rule_signal_types: Dict[str, Set[str]] = field(default_factory=dict)
    context_defaults: Mapping[str, Any] = field(default_factory=dict)


class IndicatorBreakoutCache:
    """Cache breakout signals and build overlay-friendly indicators."""

    def __init__(self) -> None:
        self._cache: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
        self._specs: Dict[str, BreakoutCacheSpec] = self._build_specs()

    def spec_for(self, indicator_name: str) -> Optional[BreakoutCacheSpec]:
        return self._specs.get(str(indicator_name).lower())

    def purge_indicator(self, inst_id: str) -> None:
        if not inst_id:
            return
        stale_keys = [key for key in self._cache if key and key[0] == inst_id]
        for cache_key in stale_keys:
            self._cache.pop(cache_key, None)

    def build_cache_key(
        self,
        inst_id: str,
        indicator_name: str,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        signature: Tuple[Any, ...],
    ) -> Tuple[Any, ...]:
        return (inst_id, indicator_name, symbol, interval, start, end, signature)

    def get_cached_breakouts(
        self, cache_key: Tuple[Any, ...]
    ) -> Optional[List[Dict[str, Any]]]:
        cached = self._cache.get(cache_key)
        if cached is None:
            return None
        return deepcopy(cached)

    def store_breakout_cache(
        self, cache_key: Tuple[Any, ...], breakouts: Sequence[Mapping[str, Any]]
    ) -> None:
        self._cache[cache_key] = deepcopy(list(breakouts)) if breakouts else []

    def flatten_breakout_signal(self, signal: BaseSignal) -> Dict[str, Any]:
        metadata = dict(signal.metadata or {})
        metadata.setdefault("type", signal.type)
        metadata.setdefault("symbol", signal.symbol)
        metadata.setdefault("time", signal.time)
        metadata.setdefault("confidence", signal.confidence)
        return metadata

    def build_market_profile_overlay_indicator(
        self,
        indicator: MarketProfileIndicator,
        df: pd.DataFrame,
        *,
        interval: Optional[str] = None,
        symbol: Optional[str] = None,
        provider: Any = None,
        data_ctx: Optional[DataContext] = None,
    ) -> MarketProfileIndicator:
        """
        Clone market profile indicator for overlay rendering.

        IMPORTANT: This method reuses pre-computed profiles from the base indicator
        instead of recomputing them from the chart's df. Market Profile profiles are
        computed from 30m data and should work on any chart timeframe.

        Args:
            indicator: Base MarketProfileIndicator with pre-computed profiles
            df: Chart's plot_df (NOT used for profile computation, only for visual boundaries)
            interval: Chart's interval (for metadata only)
            symbol: Chart's symbol (for metadata only)

        Returns:
            Cloned MarketProfileIndicator sharing the same profiles
        """
        params = resolve_market_profile_params(indicator)
        indicator_symbol = getattr(indicator, "symbol", None)

        if symbol and indicator_symbol and symbol != indicator_symbol:
            if provider is None or data_ctx is None:
                raise ValueError("Market profile overlay requires provider and data_ctx for symbol mismatch.")
            runtime = MarketProfileIndicator.from_context(
                provider=provider,
                ctx=data_ctx,
                bin_size=getattr(indicator, "bin_size", None),
                use_merged_value_areas=params.use_merged_value_areas,
                merge_threshold=params.merge_threshold,
                min_merge_sessions=params.min_merge_sessions,
                extend_value_area_to_chart_end=True,
                days_back=getattr(indicator, "days_back", MarketProfileIndicator.DEFAULT_DAYS_BACK),
            )
            setattr(runtime, "symbol", symbol)
            if interval is not None:
                setattr(runtime, "interval", interval)
            return runtime

        # Clone with existing profiles (don't recompute!)
        # Always extend value areas to chart end for overlay display
        runtime = indicator.clone_for_overlay(
            use_merged_value_areas=params.use_merged_value_areas,
            merge_threshold=params.merge_threshold,
            min_merge_sessions=params.min_merge_sessions,
            extend_value_area_to_chart_end=True
        )

        # Set runtime attributes for metadata
        if symbol is not None:
            setattr(runtime, "symbol", symbol)

        if interval is not None:
            setattr(runtime, "interval", interval)

        return runtime

    def _build_specs(self) -> Dict[str, BreakoutCacheSpec]:
        return {
            PivotLevelIndicator.NAME: BreakoutCacheSpec(
                breakout_rule_id="pivot_breakout",
                retest_rule_id="pivot_retest",
                cache_context_key="pivot_breakouts",
                ready_flag_key=_PIVOT_BREAKOUT_READY_FLAG,
                initialised_flag_key=None,
                config_signature_builder=self._pivot_breakout_signature,
                rule_signal_types={
                    "pivot_breakout": {"breakout"},
                    "pivot_retest": {"retest"},
                },
            ),
            MarketProfileIndicator.NAME: BreakoutCacheSpec(
                breakout_rule_id="market_profile_breakout",
                retest_rule_id="market_profile_retest",
                cache_context_key=_BREAKOUT_CACHE_KEY,
                ready_flag_key=_BREAKOUT_READY_FLAG,
                initialised_flag_key=_BREAKOUT_CACHE_INITIALISED,
                config_signature_builder=self._market_profile_breakout_signature,
                rule_signal_types={
                    "market_profile_breakout": {"breakout"},
                    "market_profile_retest": {"retest"},
                    "market_profile_retest_v2": {"retest"},
                },
                context_defaults={_BREAKOUT_CACHE_INITIALISED: True},
            ),
        }

    def _hashable_signature(self, value: Any) -> Any:
        if isinstance(value, (str, int, float, bool, type(None))):
            return value
        if isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass
        if isinstance(value, Mapping):
            return tuple(sorted((k, self._hashable_signature(v)) for k, v in value.items()))
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return tuple(self._hashable_signature(v) for v in value)
        return str(value)

    def _coerce_int(self, value: Any, default: int, *, minimum: Optional[int] = None) -> int:
        try:
            result = int(value)
        except (TypeError, ValueError):
            return default
        if minimum is not None and result < minimum:
            return default
        return result

    def _coerce_float(
        self, value: Any, default: float, *, minimum: Optional[float] = None
    ) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return default
        if math.isnan(result) or math.isinf(result):
            return default
        if minimum is not None and result < minimum:
            return default
        return result

    def _pivot_breakout_signature(self, config: Mapping[str, Any]) -> Tuple[Any, ...]:
        cfg = config.get("pivot_breakout_config")
        if isinstance(cfg, PivotBreakoutConfig):
            confirmation = cfg.confirmation_bars
            early_window = cfg.early_confirmation_window
            early_pct = cfg.early_confirmation_distance_pct
        else:
            default_cfg = PivotBreakoutConfig()
            confirmation = self._coerce_int(
                config.get("pivot_breakout_confirmation_bars"),
                default_cfg.confirmation_bars,
                minimum=1,
            )
            early_window = self._coerce_int(
                config.get("pivot_breakout_early_window"),
                default_cfg.early_confirmation_window,
                minimum=1,
            )
            early_pct = self._coerce_float(
                config.get("pivot_breakout_early_distance_pct"),
                default_cfg.early_confirmation_distance_pct,
                minimum=0.0,
            )
        mode = str(config.get("mode", "backtest")).lower()
        return (mode, confirmation, early_window, float(early_pct))

    def _market_profile_breakout_signature(self, config: Mapping[str, Any]) -> Tuple[Any, ...]:
        cfg = config.get("market_profile_breakout_config")
        if isinstance(cfg, MarketProfileBreakoutConfig):
            confirmation = cfg.confirmation_bars
            early_window = cfg.early_confirmation_window
            early_pct = cfg.early_confirmation_distance_pct
        else:
            default_cfg = MarketProfileBreakoutConfig()
            confirmation = self._coerce_int(
                config.get("market_profile_breakout_confirmation_bars"),
                default_cfg.confirmation_bars,
                minimum=1,
            )
            early_window = self._coerce_int(
                config.get("market_profile_breakout_early_window"),
                default_cfg.early_confirmation_window,
                minimum=1,
            )
            early_pct = self._coerce_float(
                config.get("market_profile_breakout_early_distance_pct"),
                default_cfg.early_confirmation_distance_pct,
                minimum=0.0,
            )
        mode = str(config.get("mode", "backtest")).lower()
        payload_sig = self._hashable_signature(config.get("rule_payloads"))
        return (mode, confirmation, early_window, float(early_pct), payload_sig)


def default_breakout_cache() -> IndicatorBreakoutCache:
    return IndicatorBreakoutCache()

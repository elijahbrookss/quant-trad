"""SeriesBuilder mixin."""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set

from portal.backend.service.bots.bot_runtime.strategy.models import Strategy
from utils.log_context import build_log_context, merge_log_context, series_log_context, strategy_log_context
from utils.perf_log import get_obs_enabled, get_obs_slow_ms

from .models import StrategySeries

logger = logging.getLogger(__name__)

class SeriesBuilderLifecycleMixin:
    def __init__(
        self,
        bot_id: str,
        config: Mapping[str, Any],
        run_type: str,
        log_candle_sequence: Optional[Callable[..., None]] = None,
        indicator_ctx: Optional[Any] = None,
        warning_sink: Optional[Callable[[Dict[str, object]], None]] = None,
    ):
        self.bot_id = bot_id
        self.config = config
        self.run_type = run_type
        # Default to including indicator overlays during bot runs; callers can disable if they truly want a lighter path.
        self._include_indicator_overlays = bool(config.get("include_indicator_overlays", True))
        self._log_candle_sequence = log_candle_sequence
        self._indicator_ctx = indicator_ctx
        self._warning_sink = warning_sink
        self._obs_enabled = get_obs_enabled(config)
        self._obs_slow_ms = get_obs_slow_ms(config)
        configured_lookback = config.get("incremental_signal_lookback_bars", 200)
        try:
            parsed_lookback = int(configured_lookback)
        except (TypeError, ValueError):
            parsed_lookback = 200
        self._incremental_signal_lookback_bars = max(parsed_lookback, 1)
        # Disabled by default to preserve signal correctness unless explicitly enabled.
        self._indicator_incremental_eval = bool(config.get("indicator_runtime_incremental_eval", False))
        configured_indicator_source_lookback = config.get("indicator_runtime_source_lookback_bars", 2)
        try:
            parsed_indicator_source_lookback = int(configured_indicator_source_lookback)
        except (TypeError, ValueError):
            parsed_indicator_source_lookback = 2
        self._indicator_source_lookback_bars = max(parsed_indicator_source_lookback, 1)
        runtime_symbols_raw = config.get("runtime_symbols")
        if isinstance(runtime_symbols_raw, Sequence) and not isinstance(runtime_symbols_raw, (str, bytes)):
            parsed_symbols = {str(item).strip().upper() for item in runtime_symbols_raw if str(item).strip()}
            self._runtime_symbols: Optional[Set[str]] = parsed_symbols or None
        else:
            self._runtime_symbols = None
        self._regime_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        self._indicator_overlay_runtime_cache: Dict[str, Dict[str, Any]] = {}
        self._indicator_runtime_state: Dict[str, Dict[str, Any]] = {}
        self._overlay_runtime_cache_lock = threading.RLock()
        self._regime_cache_lock = threading.RLock()

    def _runtime_log_context(self, **fields: object) -> Dict[str, object]:
        return build_log_context(bot_id=self.bot_id, bot_mode=self.run_type, **fields)

    def _series_log_context(self, series: StrategySeries, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), series_log_context(series), **fields)

    def _strategy_log_context(self, strategy: Strategy, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), strategy_log_context(strategy), **fields)

    def _emit_warning(self, warning_type: str, message: str, **context: object) -> None:
        """Forward builder warnings to the runtime when configured."""

        if not self._warning_sink:
            return
        payload_context = {key: value for key, value in context.items() if value is not None and value != ""}
        self._warning_sink(
            {
                "type": warning_type,
                "message": message,
                "context": payload_context,
            }
        )

    def reset(self) -> None:
        # Overlay caching now handled by shared IndicatorOverlayCache in the service context.
        # Reset runtime-scoped caches for a clean run.
        with self._overlay_runtime_cache_lock:
            self._indicator_overlay_runtime_cache.clear()
            self._indicator_runtime_state.clear()
        with self._regime_cache_lock:
            self._regime_snapshot_cache.clear()
        return

    def build_series_by_ids(self, strategy_ids: List[str]) -> List[StrategySeries]:
        """Build series from strategy IDs (clean DB-based approach).

        Loads strategies fresh from the database with proper typing,
        avoiding config drift and confusion.

        Args:
            strategy_ids: List of strategy IDs to build series for

        Returns:
            List of StrategySeries ready for runtime execution (one per enabled instrument per strategy)

        Raises:
            ValueError: If any strategy not found
        """
        from portal.backend.service.bots.bot_runtime.strategy.strategy_loader import StrategyLoader

        series_list: List[StrategySeries] = []
        for strategy_id in strategy_ids:
            # Load strategy fresh from DB with proper typing
            strategy = StrategyLoader.fetch_strategy(strategy_id)
            # Build one series per enabled instrument
            series_per_strategy = self._build_series_for_strategy(strategy)
            series_list.extend(series_per_strategy)
        return series_list

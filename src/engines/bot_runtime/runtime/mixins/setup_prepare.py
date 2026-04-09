"""Bot runtime setup, preparation, overlay bootstrap, and intrabar support."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from queue import Queue
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from indicators.config import IndicatorExecutionContext
from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.core.domain import normalize_epoch
from engines.bot_runtime.core.runtime_events import ReasonCode, RuntimeEventName
from engines.bot_runtime.runtime.reporting import (
    TRADE_OVERLAY_SOURCE,
    TRADE_RAY_MIN_SECONDS,
    TRADE_RAY_SPAN_MULTIPLIER,
    TRADE_STOP_COLOR,
    TRADE_TARGET_COLOR,
)
from engines.bot_runtime.runtime.overlay_types import ensure_runtime_overlay_types_registered
from engines.bot_runtime.strategy.series_builder import SeriesBuilder, StrategySeries
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.runtime.indicator_overlay_cache import default_overlay_cache
from overlays.builtins import ensure_builtin_overlays_registered
from overlays.schema import build_overlay
from utils.log_context import build_log_context, merge_log_context, series_log_context, with_log_context
from utils.perf_log import get_obs_enabled, get_obs_slow_ms, get_obs_step_sample_rate, should_sample

from ..components import (
    ChartStateBuilder,
    InMemoryEventSink,
    InlineSeriesRunner,
    IntrabarManager,
    RuntimeEventSink,
    RuntimeModePolicy,
    RunContext,
    SeriesBarTelemetryBuffer,
    SeriesRunnerContext,
    SettlementApplier,
    SignalConsumption,
    StepTracePersistenceBuffer,
    TradePersistenceBuffer,
)
from ..core import (
    INTRABAR_BASE_SECONDS,
    MAX_LOG_ENTRIES,
    MAX_SIGNAL_CONSUMPTIONS,
    MAX_WARNING_ENTRIES,
    OVERLAY_SUMMARY_INTERVAL,
    WALK_FORWARD_SAMPLE_INTERVAL,
    SeriesExecutionState,
    _coerce_float,
    _isoformat,
    _timeframe_to_seconds,
)

logger = logging.getLogger(__name__)


class RuntimeSetupPrepareMixin:
    def __init__(
        self,
        bot_id: str,
        config: Dict[str, object],
        deps: BotRuntimeDeps,
        state_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        ensure_builtin_overlays_registered()
        ensure_runtime_overlay_types_registered()
        self.bot_id = bot_id
        self.config = dict(config)
        self._deps = deps
        self.mode = (self.config.get("mode") or "instant").lower()
        self.run_type = (self.config.get("run_type") or "backtest").lower()
        self.playback_speed = 0.0
        self.focus_symbol = self.config.get("focus_symbol")
        self.state: Dict[str, object] = {
            "status": "idle",
            "progress": 0.0,
            "paused": False,
            "mode": self.mode,
        }
        self._lock = threading.Lock()
        # Serialize bootstrap so concurrent callers (start thread + snapshot polling)
        # cannot interleave preparation and observe partial bootstrap state.
        self._prepare_lock = threading.RLock()
        self._series_update_lock = threading.RLock()
        self._trade_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._runner = None
        self._stop = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._paused = False
        self._series: List["StrategySeries"] = []
        self._series_states: List[SeriesExecutionState] = []
        self._series_state_map: Dict[str, SeriesExecutionState] = {}
        self._primary_series_key: Optional[str] = None
        self._total_bars: int = 0
        self._prepared: bool = False
        self._run_context: Optional[RunContext] = None
        self._chart_overlays: List[Dict[str, Any]] = []
        # NOTE: Runtime-scoped overlay summary cache; key=strategy_key, no eviction.
        self._overlay_summary_cache: Dict[str, Dict[str, Any]] = {}
        self._last_stats: Dict[str, Any] = {}
        self._next_bar_at: Optional[datetime] = None
        self._policy = RuntimeModePolicy.for_run_type(self.run_type)
        self._live_mode = self._policy.allow_live_refresh
        self._series_runner_type = self._resolve_series_runner_type(self.config.get("series_runner"))
        self._degrade_series_on_error = bool(self.config.get("degrade_series_on_error", False))
        self._logs: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._warnings: Deque[Dict[str, Any]] = deque(maxlen=MAX_WARNING_ENTRIES)
        self._decision_events: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._event_sinks: List[RuntimeEventSink] = [
            InMemoryEventSink(
                self._logs,
                self._decision_events,
                self._lock,
                on_log=self._mark_logs_mutated,
                on_decision=self._mark_decisions_mutated,
            ),
        ]
        self._subscribers: Dict[str, Queue] = {}
        self._state_callback = state_callback
        self._candle_diag_seen: Set[Tuple[str, str]] = set()
        self._candle_diag_null: Set[Tuple[str, str]] = set()
        self._prepare_error: Optional[Dict[str, Any]] = None
        self._overlay_dirty = threading.Event()
        self._overlay_aggregator_stop = threading.Event()
        self._overlay_aggregator_thread: Optional[threading.Thread] = None
        overlay_cache = default_overlay_cache()
        self._overlay_cache = overlay_cache
        self._indicator_ctx = self._deps.build_indicator_context(
            self.bot_id,
            overlay_cache,
        )
        logger.info(
            "bot_runtime_indicator_context | bot_id=%s | cache_owner=%s | cache_scope_id=%s",
            self.bot_id,
            self._indicator_ctx.cache_owner,
            self._indicator_ctx.cache_scope_id,
        )
        self._obs_enabled = get_obs_enabled(self.config)
        self._obs_step_sample_rate = get_obs_step_sample_rate(self.config)
        self._obs_slow_ms = get_obs_slow_ms(self.config)
        self._series_builder = None
        self._settlement_applier = SettlementApplier(
            obs_enabled=self._obs_enabled,
            obs_slow_ms=self._obs_slow_ms,
        )
        self._persistence_buffer = TradePersistenceBuffer.from_config(
            self.config,
            log_context_fn=self._runtime_log_context,
            record_trade=self._deps.record_bot_trade,
            record_trade_event=self._deps.record_bot_trade_event,
        )
        self._series_bar_telemetry_buffer = SeriesBarTelemetryBuffer.from_config(
            self.config,
            record_batch=self._deps.record_bot_runtime_events_batch,
        )
        self._step_trace_buffer = StepTracePersistenceBuffer.from_config(
            self.config,
            record_batch=self._deps.record_bot_run_steps_batch,
        )
        self._report_artifact_bundle = None
        self._intrabar_manager = IntrabarManager(
            self.bot_id,
            fetcher=self._deps.fetch_ohlcv,
            build_candles=self._build_candles,
            timeframe_seconds=_timeframe_to_seconds,
            strategy_key_fn=self._strategy_key,
            obs_enabled=self._obs_enabled,
            obs_sample_rate=self._obs_step_sample_rate,
        )
        self._run_started_at: Optional[datetime] = None
        self._chart_state_builder = ChartStateBuilder(
            normalise_epoch_fn=self._normalise_epoch,
            log_sequence_fn=self._log_candle_sequence,
            strategy_key_fn=self._strategy_key,
        )
        self._phase: Optional[str] = None
        # Stream payload cache: keep last derived slices so push_update emits true fact batches.
        self._push_series_cache: Dict[str, Dict[str, Any]] = {}
        self._log_revision: int = 0
        self._decision_revision: int = 0
        self._push_log_marker: Optional[str] = None
        self._push_decision_marker: Optional[str] = None
        self._push_payload_size_probe_count: int = 0
        self._push_payload_bytes_sample_every: int = self._coerce_positive_int(
            self.config.get("push_payload_bytes_sample_every")
            or self.config.get("BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY"),
            default=10,
        )
        self._runtime_regime_overlay_rebuild: bool = self._coerce_bool(
            self.config.get("runtime_regime_overlay_rebuild")
            or self.config.get("BOT_RUNTIME_REGIME_OVERLAY_REBUILD"),
            default=False,
        )

    def _ensure_series_builder(self):
        if self._series_builder is None:
            self._series_builder = SeriesBuilder(
                self.bot_id,
                self.config,
                self.run_type,
                deps=self._deps,
                log_candle_sequence=self._log_candle_sequence,
                indicator_ctx=self._indicator_ctx,
                warning_sink=self._record_runtime_warning,
            )
        return self._series_builder

    def add_event_sink(self, sink: RuntimeEventSink) -> None:
        """Attach an additional event sink for runtime tracing."""
        if sink is None:
            return
        with self._lock:
            self._event_sinks.append(sink)

    @staticmethod
    def _coerce_positive_int(value: Optional[object], *, default: int) -> int:
        try:
            parsed = int(value) if value is not None else default
        except (TypeError, ValueError):
            return max(int(default), 1)
        return max(parsed, 1)

    @staticmethod
    def _coerce_bool(value: Optional[object], *, default: bool) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    def _mark_logs_mutated(self) -> None:
        with self._lock:
            self._log_revision += 1

    def _mark_decisions_mutated(self) -> None:
        with self._lock:
            self._decision_revision += 1

    @staticmethod
    def _coerce_playback_speed(value: Optional[object]) -> float:
        return 0.0

    @staticmethod
    def _resolve_series_runner_type(value: Optional[object]) -> str:
        if value is None:
            return "inline"
        normalized = str(value).strip().lower()
        if normalized == "inline":
            return normalized
        raise ValueError(f"Unknown series_runner '{value}'. Expected 'inline'.")

    def _build_series_runner(self) -> object:
        ctx = SeriesRunnerContext(
            stop_event=self._stop,
            pause_event=self._pause_event,
            live_mode=self._live_mode,
            mode=self.mode,
            due_series_states=self._due_series_states,
            next_step_time=self._next_step_time,
            step_series_state=self._step_series_state,
            append_live_candles_if_needed=self._append_live_candles_if_needed,
            append_live_candles_for_state=self._append_live_candles_for_state,
            pace=self._pace,
            series_states=self._active_series_states,
            thread_name=self._series_thread_name,
            log_debug=self._log_runner_debug,
            log_info=self._log_runner_info,
            log_error=self._log_runner_error,
            degrade_series_on_error=self._degrade_series_on_error,
        )
        return InlineSeriesRunner(ctx)

    def _pool_worker_count(self) -> int:
        configured = self.config.get("series_runner_pool_workers")
        if isinstance(configured, int) and configured > 0:
            return configured
        cpu_count = os.cpu_count() or 1
        return min(8, cpu_count)

    def _series_thread_name(self, state: SeriesExecutionState, index: int) -> str:
        series = state.series
        symbol = getattr(series, "symbol", "series")
        timeframe = getattr(series, "timeframe", "tf")
        return f"bot-{self.bot_id}-{symbol}-{timeframe}-{index}"

    def _log_runner_debug(
        self,
        message: str,
        state: Optional[SeriesExecutionState],
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        context = self._runtime_log_context()
        if state is not None:
            context = merge_log_context(context, series_log_context(state.series))
        if extra:
            context = merge_log_context(context, extra)
        logger.debug(with_log_context(message, context))

    def _log_runner_info(
        self,
        message: str,
        state: Optional[SeriesExecutionState],
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        context = self._runtime_log_context()
        if state is not None:
            context = merge_log_context(context, series_log_context(state.series))
        if extra:
            context = merge_log_context(context, extra)
        logger.info(with_log_context(message, context))

    def _log_runner_error(
        self,
        message: str,
        state: Optional[SeriesExecutionState],
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        context = self._runtime_log_context()
        if state is not None:
            context = merge_log_context(context, series_log_context(state.series))
        if extra:
            context = merge_log_context(context, extra)
        logger.exception(with_log_context(message, context))
        if message == "series_step_degraded":
            series = state.series if state is not None else None
            error_message = (extra or {}).get("error") if isinstance(extra, Mapping) else None
            if not error_message:
                error_message = "Series execution degraded due to runtime error."
            self._set_degraded_state(
                error_message,
                strategy_id=getattr(series, "strategy_id", None),
                symbol=getattr(series, "symbol", None),
                timeframe=getattr(series, "timeframe", None),
            )
            try:
                if series is not None and self._run_context is not None:
                    self._emit_runtime_event(
                        event_name=RuntimeEventName.SYMBOL_DEGRADED,
                        series=series,
                        bar_ts=None,
                        reason_code=ReasonCode.SYMBOL_DEGRADED,
                        payload={
                            "message": "Series execution degraded due to runtime error.",
                            "error": error_message,
                        },
                    )
            except Exception:
                logger.exception(with_log_context("symbol_degraded_event_emit_failed", context))
            self._record_runtime_warning(
                {
                    "type": "series_degraded",
                    "message": "Series execution degraded due to runtime error.",
                    "context": {
                        "strategy_id": getattr(series, "strategy_id", None),
                        "symbol": getattr(series, "symbol", None),
                        "timeframe": getattr(series, "timeframe", None),
                        "error": error_message,
                    },
                }
            )
            return
        error_message = None
        if isinstance(extra, Mapping):
            error_message = extra.get("error")
        if not error_message:
            error_message = "Series execution failed"
        series = state.series if state is not None else None
        self._set_error_state(
            error_message,
            strategy_id=getattr(series, "strategy_id", None),
            symbol=getattr(series, "symbol", None),
            timeframe=getattr(series, "timeframe", None),
        )

    def _runtime_log_context(self, **fields: object) -> Dict[str, object]:
        run_id = self._run_context.run_id if self._run_context else None
        return build_log_context(bot_id=self.bot_id, bot_mode=self.run_type, run_id=run_id, **fields)

    def _set_phase(self, phase: str, message: Optional[str] = None) -> None:
        self._phase = phase
        with self._lock:
            self.state["phase"] = phase
        context = self._runtime_log_context(phase=phase)
        logger.info(with_log_context(message or "bot_runtime_phase", context))

    def _series_log_context(self, series: StrategySeries, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), series_log_context(series), **fields)

    def apply_config(self, payload: Mapping[str, Any]) -> None:
        """Apply runtime config updates (e.g., playback speed overrides)."""

        if not payload:
            return
        self.config.update(payload)
        if "OBS_ENABLED" in payload or "obs_enabled" in payload:
            self._obs_enabled = get_obs_enabled(self.config)
        if "OBS_STEP_SAMPLE_RATE" in payload or "obs_step_sample_rate" in payload:
            self._obs_step_sample_rate = get_obs_step_sample_rate(self.config)
        if "OBS_SLOW_MS" in payload or "obs_slow_ms" in payload:
            self._obs_slow_ms = get_obs_slow_ms(self.config)
        if "mode" in payload:
            self.mode = str(payload.get("mode") or "instant").lower()
            with self._lock:
                self.state["mode"] = self.mode
        if "focus_symbol" in payload:
            self.focus_symbol = payload.get("focus_symbol") or None
        if "series_runner" in payload:
            self._series_runner_type = self._resolve_series_runner_type(payload.get("series_runner"))
        if "degrade_series_on_error" in payload:
            self._degrade_series_on_error = bool(payload.get("degrade_series_on_error"))
        if "push_payload_bytes_sample_every" in payload or "BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY" in payload:
            self._push_payload_bytes_sample_every = self._coerce_positive_int(
                payload.get("push_payload_bytes_sample_every")
                or payload.get("BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY"),
                default=self._push_payload_bytes_sample_every,
            )
        if "runtime_regime_overlay_rebuild" in payload or "BOT_RUNTIME_REGIME_OVERLAY_REBUILD" in payload:
            self._runtime_regime_overlay_rebuild = self._coerce_bool(
                payload.get("runtime_regime_overlay_rebuild")
                if "runtime_regime_overlay_rebuild" in payload
                else payload.get("BOT_RUNTIME_REGIME_OVERLAY_REBUILD"),
                default=self._runtime_regime_overlay_rebuild,
            )

    def _set_error_state(
        self,
        message: str,
        *,
        strategy_id: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> Dict[str, Any]:
        error_payload: Dict[str, Any] = {"message": message}
        if strategy_id:
            error_payload["strategy_id"] = strategy_id
        if symbol:
            error_payload["symbol"] = symbol
        if timeframe:
            error_payload["timeframe"] = timeframe
        with self._lock:
            self.state.update({"status": "error", "progress": 0.0, "paused": False, "error": error_payload})
        self._log_event("error", **error_payload)
        self._broadcast(
            "delta",
            {
                "type": "delta",
                "event": "error",
                "runtime": self._state_payload(),
                "error": error_payload,
            },
        )
        return error_payload

    def _set_degraded_state(
        self,
        message: str,
        *,
        strategy_id: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> Dict[str, Any]:
        degraded_payload: Dict[str, Any] = {"message": message}
        if strategy_id:
            degraded_payload["strategy_id"] = strategy_id
        if symbol:
            degraded_payload["symbol"] = symbol
        if timeframe:
            degraded_payload["timeframe"] = timeframe
        with self._lock:
            self.state.update(
                {
                    "status": "degraded",
                    "progress": 0.0,
                    "paused": False,
                    "degradation": degraded_payload,
                }
            )
        return degraded_payload

    def _ensure_prepared(self) -> None:
        with self._prepare_lock:
            if self._prepared:
                return
            if self.state.get("status") == "error":
                message = (self._prepare_error or {}).get("message") or "Runtime is in an error state; reset before preparing"
                raise RuntimeError(message)
            self._set_phase("prepare_series", "bot_runtime_prepare_start")
            with self._lock:
                self.state.update({"status": "initialising", "progress": 0.0, "paused": False})

            # Load strategies fresh from DB by ID
            strategy_ids = self.config.get("strategy_ids")
            self._prepare_error = None

            try:
                if not strategy_ids:
                    raise ValueError("Runtime requires 'strategy_ids' to initialise")
                context = self._runtime_log_context(strategy_ids=strategy_ids)
                logger.debug(with_log_context("bot_runtime_preparing", context))
                streams = self._ensure_series_builder().build_series_by_ids(strategy_ids)
            except Exception as exc:
                details = self._prepare_error or {"message": str(exc)}
                self._prepare_error = details
                self._set_error_state(details.get("message", str(exc)), **{k: details.get(k) for k in ("strategy_id", "symbol", "timeframe")})
                raise
            if not streams:
                message = (self._prepare_error or {}).get("message") or "No strategy streams could be prepared for this bot"
                self._set_error_state(message)
                raise RuntimeError(message)
            self._series = streams
            # Calculate total bars as max across all series (for multi-instrument support)
            self._total_bars = max(len(series.candles) for series in self._series) if self._series else 0
            try:
                self._build_series_states()
                self._set_phase("prepare_indicators", "bot_runtime_indicator_bootstrap_start")
                with self._series_update_lock:
                    for state in self._series_states:
                        self._bootstrap_indicator_overlays_for_state(state)
                    self._validate_bootstrap_readiness()
                self._set_phase("prepare_indicators_done", "bot_runtime_indicator_bootstrap_done")
                self._aggregate_overlays_to_cache()
            except Exception as exc:
                details = self._prepare_error or {"message": str(exc)}
                self._prepare_error = details
                self._set_error_state(
                    details.get("message", str(exc)),
                    **{k: details.get(k) for k in ("strategy_id", "symbol", "timeframe")},
                )
                raise
            self._prepared = True
            with self._lock:
                self.state.update({"status": "idle", "progress": 0.0, "paused": False})
            context = self._runtime_log_context(series=len(self._series), total_bars=self._total_bars)
            logger.info(with_log_context("bot_runtime_prepared", context))
            self._set_phase("prepared", "bot_runtime_prepare_complete")
            self._log_event("prepared", total_bars=self._total_bars)
            self._push_update("prepared")

    def _rebuild_overlay_cache(self) -> None:
        """Rebuild overlays synchronously and signal the aggregator."""

        if self._overlay_aggregator_thread and self._overlay_aggregator_thread.is_alive():
            # Let the aggregator do the work to avoid duplicate passes.
            self._notify_overlay_aggregation_needed()
            return
        self._aggregate_overlays_to_cache()
        self._notify_overlay_aggregation_needed()

    def _aggregate_overlays_to_cache(self) -> None:
        """Collect overlays for every series while holding the update lock."""

        overlays: List[Dict[str, Any]] = []
        with self._series_update_lock:
            for series in self._series:
                overlays.extend(series.overlays or [])
                if series.trade_overlay:
                    overlays.append(series.trade_overlay)
        with self._lock:
            self._chart_overlays = overlays

    def _notify_overlay_aggregation_needed(self) -> None:
        """Signal the background aggregator that overlays need refreshing."""

        if self._overlay_aggregator_thread and self._overlay_aggregator_thread.is_alive():
            self._overlay_dirty.set()
        else:
            # Fallback to synchronous rebuild if the aggregator is not running.
            self._aggregate_overlays_to_cache()

    def _start_overlay_aggregator(self) -> None:
        """Ensure the aggregator thread is running and primed."""

        if self._overlay_aggregator_thread and self._overlay_aggregator_thread.is_alive():
            return
        self._overlay_aggregator_stop.clear()
        self._overlay_dirty.clear()
        self._overlay_aggregator_thread = threading.Thread(
            target=self._overlay_aggregator_loop,
            name=f"bot-{self.bot_id}-overlay-aggregator",
            daemon=True,
        )
        self._overlay_aggregator_thread.start()
        # Trigger an initial update so the cache is populated.
        self._overlay_dirty.set()

    def _stop_overlay_aggregator(self) -> None:
        """Shut down the overlay aggregator thread cleanly."""

        self._overlay_aggregator_stop.set()
        self._overlay_dirty.set()
        thread = self._overlay_aggregator_thread
        self._overlay_aggregator_thread = None
        if thread and thread.is_alive():
            thread.join(timeout=0.5)

    def _overlay_aggregator_loop(self) -> None:
        """Background loop that batches overlay builds while the runtime is running."""

        while not self._stop.is_set() and not self._overlay_aggregator_stop.is_set():
            triggered = self._overlay_dirty.wait(timeout=0.25)
            self._overlay_dirty.clear()
            if self._overlay_aggregator_stop.is_set() or self._stop.is_set():
                break
            if triggered:
                self._aggregate_overlays_to_cache()
        # Ensure the cache is up to date before exiting.
        self._aggregate_overlays_to_cache()

    def _build_series_states(self) -> None:
        self._series_states = []
        self._series_state_map = {}
        self._primary_series_key = None
        for series in self._series:
            start_index = int(getattr(series, "replay_start_index", 0) or 0)
            start_index = max(0, min(start_index, len(series.candles)))
            state = SeriesExecutionState(
                series=series,
                bar_index=start_index,
                total_bars=len(series.candles),
                last_consumed_epoch=max(int(getattr(series, "last_consumed_epoch", 0) or 0), 0),
            )
            key = self._strategy_key(series)
            self._series_states.append(state)
            self._series_state_map[key] = state
            self._initialize_indicator_runtime_state(state)
            if self._primary_series_key is None:
                self._primary_series_key = key

    def _initialize_indicator_runtime_state(self, state: SeriesExecutionState) -> None:
        series = state.series
        strategy_meta = series.meta or {}
        indicator_links = list(strategy_meta.get("indicator_links") or [])
        indicator_ids = strategy_meta.get("indicator_ids")
        if not indicator_links and isinstance(indicator_ids, list):
            indicator_links = [{"indicator_id": indicator_id} for indicator_id in indicator_ids if indicator_id]
        indicator_metas: Dict[str, Dict[str, Any]] = {}
        for link in indicator_links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id:
                continue
            indicator_metas[indicator_id] = self._deps.indicator_get_instance_meta(
                indicator_id,
                ctx=self._indicator_ctx,
            )

        series_start = (
            series.window_start
            or (series.candles[0].time.isoformat() if series.candles else None)
        )
        series_end = (
            series.window_end
            or (series.candles[-1].time.isoformat() if series.candles else None)
        )
        instrument_id = None
        if isinstance(series.instrument, Mapping):
            instrument_id = series.instrument.get("id")
        execution_context = IndicatorExecutionContext(
            symbol=series.symbol,
            start=series_start,
            end=series_end,
            interval=series.timeframe,
            datasource=series.datasource,
            exchange=series.exchange,
            instrument_id=str(instrument_id) if instrument_id is not None else None,
        )

        indicator_metas, indicators = self._deps.indicator_build_runtime_graph(
            list(indicator_metas.keys()),
            strategy_indicator_metas=indicator_metas,
            execution_context=execution_context,
            ctx=self._indicator_ctx,
        )
        state.indicator_engine = IndicatorExecutionEngine(indicators)
        state.indicator_output_types = state.indicator_engine.output_types
        state.indicator_outputs = {}
        state.indicator_overlays = {}

        warmup_count = max(int(state.bar_index or 0), 0)
        if warmup_count > 0 and state.indicator_engine is not None:
            last_frame = None
            for index, warmup_candle in enumerate(series.candles[:warmup_count]):
                last_frame = state.indicator_engine.step(
                    bar=warmup_candle,
                    bar_time=warmup_candle.time,
                    include_overlays=index == (warmup_count - 1),
                )
            if last_frame is not None:
                state.indicator_outputs = dict(last_frame.outputs)
                state.indicator_overlays = dict(last_frame.overlays)

        series.overlays = self._series_overlay_entries(state)
        logger.info(
            with_log_context(
                "indicator_runtime_initialized",
                self._series_log_context(
                    series,
                    warmup_candles=warmup_count,
                    indicators=len(indicators),
                    order=list(state.indicator_engine.order if state.indicator_engine else ()),
                ),
            )
        )

    def _series_state_for(self, series: Optional[StrategySeries]) -> Optional[SeriesExecutionState]:
        if series is None:
            return None
        return self._series_state_map.get(self._strategy_key(series))

    def _active_series_states(self) -> List[SeriesExecutionState]:
        with self._series_update_lock:
            return [state for state in self._series_states if not state.done]

    def _compute_progress(self) -> float:
        if not self._series_states:
            return 0.0
        progress_total = 0.0
        counted = 0
        for state in self._series_states:
            if state.total_bars <= 0:
                continue
            replay_start = int(getattr(state.series, "replay_start_index", 0) or 0)
            replay_start = max(0, min(replay_start, state.total_bars))
            effective_total = max(state.total_bars - replay_start, 1)
            effective_pos = max(min(state.bar_index, state.total_bars) - replay_start, 0)
            progress_total += effective_pos / effective_total
            counted += 1
        return round(progress_total / counted, 4) if counted else 0.0

    def _refresh_next_bar_at(self) -> None:
        next_at: Optional[datetime] = None
        with self._series_update_lock:
            for state in self._active_series_states():
                candidate = state.next_step_at
                if candidate is None:
                    continue
                if next_at is None or candidate < next_at:
                    next_at = candidate
        self._next_bar_at = next_at

    def _bar_interval(self) -> float:
        return self._compute_playback_interval()

    def _intrabar_interval(self) -> float:
        return self._compute_playback_interval(INTRABAR_BASE_SECONDS)

    def _schedule_next_step(self, state: SeriesExecutionState, interval: float) -> None:
        if self.mode != "walk-forward" or interval <= 0:
            state.next_step_at = None
            return
        state.next_step_at = datetime.now(timezone.utc) + timedelta(seconds=interval)

    def _due_series_states(self, now: datetime) -> List[SeriesExecutionState]:
        due: List[SeriesExecutionState] = []
        for state in self._active_series_states():
            if state.next_step_at is None or now >= state.next_step_at:
                due.append(state)
        return due

    def _next_step_time(self) -> Optional[datetime]:
        next_at: Optional[datetime] = None
        for state in self._active_series_states():
            if state.next_step_at is None:
                return None
            if next_at is None or state.next_step_at < next_at:
                next_at = state.next_step_at
        return next_at

    def _instrument_for(
        self,
        datasource: Optional[str],
        exchange: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        return self._ensure_series_builder()._instrument_for(datasource, exchange, symbol)

    def _resolve_live_window(self) -> Tuple[str, str]:
        return self._ensure_series_builder()._resolve_live_window()

    def _series_overlay_entries(self, state: SeriesExecutionState) -> List[Dict[str, Any]]:
        overlays: List[Dict[str, Any]] = []
        for overlay_key in sorted(state.indicator_overlays.keys()):
            runtime_overlay = state.indicator_overlays.get(overlay_key)
            if runtime_overlay is None or not runtime_overlay.ready:
                continue
            indicator_id, _, overlay_name = str(overlay_key).partition(".")
            entry = dict(runtime_overlay.value)
            entry.setdefault("overlay_id", overlay_key)
            entry.setdefault("indicator_id", indicator_id)
            entry.setdefault("overlay_name", overlay_name)
            overlays.append(entry)
        state.overlay_runtime_metrics = {
            "series_overlay_entries_ms": 0.0,
            "series_overlay_indicator_entries_ms": 0.0,
            "series_overlay_regime_build_ms": 0.0,
            "series_overlay_indicator_entries_count": float(len(overlays)),
            "series_overlay_regime_entries_count": 0.0,
            "series_overlay_total_entries_count": float(len(overlays)),
            "series_overlay_regime_mode_rebuild": 0.0,
        }
        return overlays

    def _bootstrap_indicator_overlays_for_state(self, state: SeriesExecutionState) -> None:
        series = state.series
        series.bootstrap_completed = True
        series.bootstrap_indicator_overlays = len(series.overlays or [])
        series.bootstrap_total_overlays = len(series.overlays or [])
        logger.info(
            with_log_context(
                "indicator_overlay_bootstrap_completed",
                self._series_log_context(
                    series,
                    overlays=len(series.overlays or []),
                    indicators=len(state.indicator_engine.order if state.indicator_engine else ()),
                    indicator_overlays=len(series.overlays or []),
                ),
            )
        )

    def _validate_bootstrap_readiness(self) -> None:
        for state in self._series_states:
            series = state.series
            if not bool(series.bootstrap_completed):
                raise RuntimeError("indicator_bootstrap_invalid: bootstrap_completed is required")

    def _update_trade_overlay(self, series: Optional[StrategySeries]) -> None:
        if series is None:
            return
        overlay = self._build_trade_overlay(series)
        series.trade_overlay = overlay
        self._rebuild_overlay_cache()

    def _build_trade_overlay(self, series: StrategySeries) -> Optional[Dict[str, Any]]:
        # Disabled: TP/SL price lines are now created in frontend from trade data
        return None
        engine = getattr(series, "risk_engine", None)
        trade = getattr(engine, "active_trade", None)
        if not trade or not trade.is_active():
            return None
        anchor_epoch = self._current_epoch()
        if anchor_epoch is None:
            if series.candles:
                anchor_epoch = int(series.candles[-1].time.timestamp())
            else:
                return None
        timeframe_seconds = _timeframe_to_seconds(series.timeframe) or 60
        span = max(int(timeframe_seconds) * TRADE_RAY_SPAN_MULTIPLIER, TRADE_RAY_MIN_SECONDS)
        start_epoch = anchor_epoch
        end_epoch = anchor_epoch - span

        def quantise(value: float) -> float:
            return round(float(value), 8)

        segments_map: Dict[Tuple[str, float], Dict[str, Any]] = {}

        def add_level(kind: str, price: Optional[float]) -> None:
            if price in (None, float("inf"), float("-inf")):
                return
            key = (kind, quantise(price))
            entry = segments_map.get(key)
            if entry:
                entry["count"] = entry.get("count", 1) + 1
                return
            color = TRADE_STOP_COLOR if kind == "stop" else TRADE_TARGET_COLOR
            segments_map[key] = {
                "kind": kind,
                "price": float(price),
                "color": color,
                "lineWidth": 2,
                "lineStyle": 2,
            }

        add_level("stop", trade.stop_price)
        for leg in trade.legs:
            if getattr(leg, "status", "open") == "open":
                add_level("target", leg.target_price)

        if not segments_map:
            return None

        segments = [
            {
                "x1": start_epoch,
                "x2": end_epoch,
                "y1": entry["price"],
                "y2": entry["price"],
                "color": entry["color"],
                "lineWidth": entry["lineWidth"],
                "lineStyle": entry["lineStyle"],
            }
            for entry in segments_map.values()
        ]

        if not segments:
            return None

        overlay = build_overlay("bot_trade_rays", {"segments": segments})
        overlay["source"] = TRADE_OVERLAY_SOURCE
        return overlay

    @staticmethod
    def _build_candles(df: Any, timeframe: Optional[str] = None) -> List[Candle]:
        return SeriesBuilder._build_candles(df, timeframe)

    @staticmethod
    def _build_signals_from_decision_artifacts(artifacts: Sequence[Mapping[str, Any]]) -> Deque[StrategySignal]:
        return SeriesBuilder._build_signals_from_decision_artifacts(artifacts)

    @staticmethod
    def _strategy_key(series: StrategySeries) -> str:
        strategy_id = getattr(series, "strategy_id", getattr(series, "id", id(series)))
        symbol = getattr(series, "symbol", None)
        timeframe = getattr(series, "timeframe", None)
        parts = [str(strategy_id)]
        if symbol:
            parts.append(str(symbol))
        if timeframe:
            parts.append(str(timeframe))
        return ":".join(parts)

    @staticmethod
    def _trade_entry_time(series: StrategySeries, trade_id: Optional[str]) -> Optional[str]:
        if not trade_id:
            return None
        engine = getattr(series, "risk_engine", None)
        trades = getattr(engine, "trades", None) if engine else None
        if not trades:
            return None
        for trade in trades:
            if getattr(trade, "trade_id", None) == trade_id:
                entry_time = getattr(trade, "entry_time", None)
                return _isoformat(entry_time)
        return None

    def _active_trade_for_instrument(
        self,
        instrument_id: Optional[str],
        *,
        skip_series: Optional[StrategySeries] = None,
    ) -> Optional[object]:
        if not instrument_id:
            return None
        with self._series_update_lock:
            for state in self._series_states:
                series = state.series
                if skip_series is not None and series is skip_series:
                    continue
                series_instrument_id = None
                if isinstance(series.instrument, Mapping):
                    series_instrument_id = series.instrument.get("id")
                if not series_instrument_id or series_instrument_id != instrument_id:
                    continue
                engine = getattr(series, "risk_engine", None)
                trade = getattr(engine, "active_trade", None) if engine else None
                if trade and getattr(trade, "is_active", lambda: True)():
                    return trade
        return None

    def _snapshot_candle_for_state(self, base: Candle, snapshot: Mapping[str, Any]) -> Candle:
        open_price = _coerce_float(snapshot.get("open"), base.open) or base.open
        high_price = _coerce_float(snapshot.get("high"), max(base.high, open_price)) or max(base.high, open_price)
        low_price = _coerce_float(snapshot.get("low"), min(base.low, open_price)) or min(base.low, open_price)
        close_price = _coerce_float(snapshot.get("close"), base.close) or base.close
        return Candle(
            time=base.time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            end=snapshot.get("end") or base.end,
        )

    def _prime_intrabar_or_step_bar(self, state: SeriesExecutionState, candle: Candle) -> List[Dict[str, Any]]:
        series = state.series
        engine = series.risk_engine
        if engine is None:
            return []
        if not self._policy.use_intrabar:
            return engine.step(candle)
        intrabar = self._intrabar_manager.intrabar_candles(series, candle)
        if not intrabar:
            return engine.step(candle)
        if self.mode == "instant":
            return self._run_intrabar_batch(series, intrabar)
        state.intrabar_candles = intrabar
        state.intrabar_index = 0
        self._schedule_next_step(state, self._intrabar_interval())
        context = self._series_log_context(series, bars=len(intrabar))
        logger.debug(with_log_context("intrabar_start", context))
        return []

    def _run_intrabar_batch(
        self,
        series: StrategySeries,
        intrabar: Sequence[Candle],
    ) -> List[Dict[str, Any]]:
        engine = series.risk_engine
        if engine is None:
            return []
        events: List[Dict[str, Any]] = []
        steps = 0
        for minute_bar in intrabar:
            steps += 1
            events.extend(engine.step(minute_bar))
            if engine.active_trade is None:
                break
        context = self._series_log_context(series, bars=len(intrabar), steps=steps, mode=self.mode)
        logger.debug(with_log_context("intrabar_batch", context))
        return events

    def _step_intrabar(self, state: SeriesExecutionState) -> None:
        series = state.series
        engine = series.risk_engine
        if engine is None or not state.intrabar_active():
            self._finish_intrabar(state)
            return
        if state.active_candle is None:
            if state.total_bars <= 0:
                self._finish_intrabar(state)
                return
            state.active_candle = series.candles[min(state.bar_index, state.total_bars - 1)]
        minute_bar = state.intrabar_candles[state.intrabar_index]
        state.intrabar_index += 1
        events = engine.step(minute_bar)
        snapshot = self._intrabar_manager.update_snapshot(series, state.active_candle, minute_bar)
        temp_candle = self._snapshot_candle_for_state(state.active_candle, snapshot)
        update_metrics = self._update_state(self._state_candle_for(series, temp_candle))
        self._push_update(
            "intrabar",
            series=series,
            candle=temp_candle,
            replace_last=True,
            precomputed_stats=update_metrics.get("stats"),
        )
        for event in events:
            self._log_event(
                event.get("type", "event"),
                series,
                state.active_candle,
                trade_id=event.get("trade_id"),
                leg=event.get("leg"),
                price=event.get("price"),
                event_time=event.get("time"),
                bar_index=state.bar_index,
                contracts=event.get("contracts"),
                pnl=event.get("pnl"),
                net_pnl=event.get("net_pnl"),
                gross_pnl=event.get("gross_pnl"),
                fees_paid=event.get("fees_paid"),
                currency=event.get("currency"),
                direction=event.get("direction"),
            )
            raw_subtype = event.get("type")
            event_ts = event.get("time")
            if raw_subtype and event_ts:
                event_subtype = str(raw_subtype)
                if event_subtype in {"target", "stop", "close"}:
                    self._emit_exit_filled_event(
                        series=series,
                        candle=state.active_candle,
                        event=event,
                    )
                    if event_subtype == "close":
                        self._persist_trade_close(series, event)
        if engine.active_trade is None or not state.intrabar_active():
            self._finish_intrabar(state)
        else:
            self._schedule_next_step(state, self._intrabar_interval())

    def _finish_intrabar(self, state: SeriesExecutionState) -> None:
        if state.intrabar_candles:
            self._intrabar_manager.clear_snapshot(state.series)
        if state.intrabar_candles:
            context = self._series_log_context(state.series, steps=state.intrabar_index)
            logger.debug(with_log_context("intrabar_complete", context))
        state.intrabar_candles = []
        state.intrabar_index = 0
        if state.active_candle is not None:
            self._finalize_bar_step(state, state.active_candle)

    def _finalize_bar_step(self, state: SeriesExecutionState, candle: Candle) -> Dict[str, Optional[float]]:
        finalize_started_perf = time.perf_counter()
        current_bar_index = max(int(state.bar_index or 0), 0)
        update_state_ms: Optional[float] = None
        stats_update_ms: Optional[float] = None
        push_update_ms: Optional[float] = None
        persist_ms: Optional[float] = None
        db_commit_ms: Optional[float] = None
        update_metrics: Dict[str, Any] = {}
        state.bar_index += 1
        if state.bar_index >= state.total_bars:
            state.done = True
        if state.bar_index % 50 == 0 or state.done:
            context = self._series_log_context(
                state.series,
                bar_index=state.bar_index,
                bar_time=_isoformat(candle.time),
                done=state.done,
            )
            logger.info(with_log_context("walk_forward_step", context))
        if state.done:
            state.next_step_at = None
        else:
            self._schedule_next_step(state, self._bar_interval())
        if state.done or state.bar_index % OVERLAY_SUMMARY_INTERVAL == 0:
            self._log_overlay_summary(state, candle)
        should_update_state = self._should_update_state_for(state.series)
        if should_update_state:
            update_started_perf = time.perf_counter()
            update_started = datetime.now(timezone.utc)
            try:
                update_metrics = self._update_state(self._state_candle_for(state.series, candle))
                update_metrics["series_bar_telemetry_enqueue_ms"] = self._persist_series_bar_telemetry(
                    series=state.series,
                    candle=candle,
                    bar_index=current_bar_index,
                )
                stats_update_ms = update_metrics.get("stats_update_ms")
                persist_ms = self._record_step_trace(
                    "step_update_state",
                    started_at=update_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=getattr(state.series, "strategy_id", None),
                    symbol=getattr(state.series, "symbol", None),
                    timeframe=getattr(state.series, "timeframe", None),
                    context={
                        "bar_index": state.bar_index,
                        "total_bars": state.total_bars,
                        "bar_time": _isoformat(candle.time),
                    },
                )
                db_commit_ms = persist_ms
            except Exception as exc:
                self._record_step_trace(
                    "step_update_state",
                    started_at=update_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=getattr(state.series, "strategy_id", None),
                    symbol=getattr(state.series, "symbol", None),
                    timeframe=getattr(state.series, "timeframe", None),
                    error=str(exc),
                    context={
                        "bar_index": state.bar_index,
                        "total_bars": state.total_bars,
                        "bar_time": _isoformat(candle.time),
                    },
                )
                raise
            finally:
                update_state_ms = max((time.perf_counter() - update_started_perf) * 1000.0, 0.0)
        push_metrics = self._push_update(
            "bar",
            series=state.series,
            candle=candle,
            replace_last=False,
            precomputed_stats=update_metrics.get("stats") if should_update_state else None,
        )
        push_update_ms = push_metrics.get("duration_ms")
        push_trace_persist_ms = push_metrics.get("trace_persist_ms")
        push_stats_update_ms = push_metrics.get("stats_update_ms")
        if persist_ms is not None and push_trace_persist_ms is not None:
            persist_ms = persist_ms + push_trace_persist_ms
        elif push_trace_persist_ms is not None:
            persist_ms = push_trace_persist_ms
        if db_commit_ms is not None and push_trace_persist_ms is not None:
            db_commit_ms = db_commit_ms + push_trace_persist_ms
        elif push_trace_persist_ms is not None:
            db_commit_ms = push_trace_persist_ms
        if stats_update_ms is not None and push_stats_update_ms is not None:
            stats_update_ms = stats_update_ms + push_stats_update_ms
        elif push_stats_update_ms is not None:
            stats_update_ms = push_stats_update_ms
        finalize_total_ms = max((time.perf_counter() - finalize_started_perf) * 1000.0, 0.0)
        known_ms = (update_state_ms or 0.0) + (push_update_ms or 0.0)
        finalize_residual_ms = max(finalize_total_ms - known_ms, 0.0)
        step_trace_metrics = self._step_trace_metrics()
        return {
            "finalize_residual_ms": finalize_residual_ms,
            "persist_ms": persist_ms,
            "db_commit_ms": db_commit_ms,
            "stats_update_ms": stats_update_ms,
            "delta_build_ms": push_metrics.get("delta_build_ms"),
            "delta_serialize_ms": push_metrics.get("delta_serialize_ms"),
            "stream_emit_ms": push_metrics.get("stream_emit_ms"),
            "series_bar_telemetry_enqueue_ms": update_metrics.get("series_bar_telemetry_enqueue_ms"),
            "subscribers_count": push_metrics.get("subscribers_count"),
            "overlay_points_changed": push_metrics.get("overlay_points"),
            "step_trace_queue_depth": step_trace_metrics.get("step_trace_queue_depth"),
            "step_trace_dropped_count": step_trace_metrics.get("step_trace_dropped_count"),
            "step_trace_persist_lag_ms": step_trace_metrics.get("step_trace_persist_lag_ms"),
            "step_trace_persist_batch_ms": step_trace_metrics.get("step_trace_persist_batch_ms"),
            "step_trace_persist_error_count": step_trace_metrics.get("step_trace_persist_error_count"),
        }

    def _primary_state_candle(self) -> Optional[Candle]:
        if not self._series_states:
            return None
        primary = self._series_states[0]
        if not primary.series.candles:
            return None
        idx = max(primary.bar_index - 1, 0)
        idx = min(idx, len(primary.series.candles) - 1)
        return primary.series.candles[idx]

    def _state_candle_for(self, series: StrategySeries, candle: Candle) -> Candle:
        if self._primary_series_key and self._strategy_key(series) == self._primary_series_key:
            return candle
        primary_candle = self._primary_state_candle()
        return primary_candle or candle

    def _should_update_state_for(self, series: StrategySeries) -> bool:
        if self.focus_symbol:
            return getattr(series, "symbol", None) == self.focus_symbol
        if self._primary_series_key:
            return self._strategy_key(series) == self._primary_series_key
        return True

    def _log_overlay_summary(self, state: SeriesExecutionState, candle: Candle) -> None:
        series = state.series
        overlays = list(series.overlays or [])
        if series.trade_overlay:
            overlays.append(series.trade_overlay)
        current_epoch = int(candle.time.timestamp())
        visible = self._chart_state_builder.visible_overlays(
            overlays,
            str(self.state.get("status") or "").lower(),
            current_epoch,
        )
        summary = self._overlay_summary(visible)
        signature = (
            summary.get("total_overlays"),
            tuple(sorted(summary.get("type_counts", {}).items())),
            tuple(sorted(summary.get("payload_counts", {}).items())),
            tuple(sorted(summary.get("type_payload_counts", {}).items())),
            tuple(
                (key, tuple(sorted((k, v) for k, v in (value or {}).items())))
                for key, value in (summary.get("transform_counts") or {}).items()
            ),
        )
        series_key = self._strategy_key(series)
        should_log = self._obs_enabled and should_sample(self._obs_step_sample_rate)
        get_started = time.perf_counter() if should_log else 0.0
        cached = self._overlay_summary_cache.get(series_key)
        if should_log:
            get_ms = (time.perf_counter() - get_started) * 1000.0
            cache_context = self._series_log_context(
                series,
                cache_name="overlay_summary_cache",
                cache_scope="runtime",
                cache_key_summary=series_key,
                time_taken_ms=get_ms,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
            )
            logger.debug(
                with_log_context(
                    "cache.get",
                    merge_log_context(cache_context, build_log_context(event="cache.get")),
                )
            )
            hit_event = "cache.hit" if cached is not None else "cache.miss"
            logger.debug(
                with_log_context(
                    hit_event,
                    merge_log_context(cache_context, build_log_context(event=hit_event)),
                )
            )
        if cached and cached.get("signature") == signature and not state.done:
            return
        set_started = time.perf_counter() if should_log else 0.0
        self._overlay_summary_cache[series_key] = {
            "signature": signature,
            "bar_index": state.bar_index,
        }
        if should_log:
            set_ms = (time.perf_counter() - set_started) * 1000.0
            set_context = self._series_log_context(
                series,
                cache_name="overlay_summary_cache",
                cache_scope="runtime",
                cache_key_summary=series_key,
                time_taken_ms=set_ms,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
            )
            logger.debug(
                with_log_context(
                    "cache.set",
                    merge_log_context(set_context, build_log_context(event="cache.set")),
                )
            )
        instrument = series.instrument or {}
        regime_payload = (summary.get("type_payload_counts") or {}).get("regime_overlay", {})
        regime_overlay_count = summary.get("type_counts", {}).get("regime_overlay", 0)
        regime_marker_count = summary.get("type_counts", {}).get("regime_markers", 0)
        # Extract first/last times to make log inspection easier in BotLens
        start_epoch = None
        end_epoch = None
        for ov in visible:
            if not isinstance(ov, Mapping):
                continue
            payload = ov.get("payload") if isinstance(ov, Mapping) else {}
            boxes = payload.get("boxes") if isinstance(payload, Mapping) else None
            if isinstance(boxes, list) and boxes:
                starts = [b.get("x1") or b.get("start") for b in boxes if isinstance(b, Mapping)]
                ends = [b.get("x2") or b.get("end") for b in boxes if isinstance(b, Mapping)]
                starts = [s for s in starts if isinstance(s, (int, float))]
                ends = [e for e in ends if isinstance(e, (int, float))]
                if starts:
                    start_epoch = min(start_epoch, min(starts)) if start_epoch is not None else min(starts)
                if ends:
                    end_epoch = max(end_epoch, max(ends)) if end_epoch is not None else max(ends)
        context = self._series_log_context(
            series,
            instrument_id=instrument.get("id") if isinstance(instrument, dict) else None,
            bar_index=state.bar_index,
            bar_time=_isoformat(candle.time),
            overlays=summary.get("total_overlays"),
            overlay_types=summary.get("type_counts"),
            overlay_payloads=summary.get("payload_counts"),
            overlay_profiles=summary.get("profile_counts"),
            overlay_profile_params=summary.get("profile_params_present"),
            overlay_transform=summary.get("transform_counts"),
            regime_overlay=regime_overlay_count,
            regime_overlay_boxes=regime_payload.get("boxes"),
            regime_overlay_segments=regime_payload.get("segments"),
            regime_markers=regime_marker_count,
            overlay_type_payloads=summary.get("type_payload_counts"),
            overlay_start=_isoformat(datetime.fromtimestamp(start_epoch, tz=timezone.utc)) if start_epoch else None,
            overlay_end=_isoformat(datetime.fromtimestamp(end_epoch, tz=timezone.utc)) if end_epoch else None,
        )
        logger.info(with_log_context("instrument_overlay_summary", context))
        self._log_event(
            "overlay_summary",
            series=series,
            candle=candle,
            overlays=summary.get("total_overlays"),
            overlay_types=summary.get("type_counts"),
            overlay_payloads=summary.get("payload_counts"),
            overlay_profiles=summary.get("profile_counts"),
            overlay_profile_params=summary.get("profile_params_present"),
            overlay_transform=summary.get("transform_counts"),
            regime_overlay=regime_overlay_count,
            regime_overlay_boxes=regime_payload.get("boxes"),
            regime_overlay_segments=regime_payload.get("segments"),
            regime_markers=regime_marker_count,
            overlay_type_payloads=summary.get("type_payload_counts"),
            overlay_start=_isoformat(datetime.fromtimestamp(start_epoch, tz=timezone.utc)) if start_epoch else None,
            overlay_end=_isoformat(datetime.fromtimestamp(end_epoch, tz=timezone.utc)) if end_epoch else None,
        )
        self._notify_overlay_aggregation_needed()

    @staticmethod
    def _overlay_summary(overlays: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        def _payload_has_content(payload: Mapping[str, Any]) -> bool:
            if not isinstance(payload, Mapping):
                return False
            list_keys = {
                "price_lines",
                "markers",
                "touchPoints",
                "touch_points",
                "boxes",
                "segments",
                "polylines",
                "bubbles",
            }
            for key in list_keys:
                entries = payload.get(key)
                if isinstance(entries, list) and entries:
                    return True
            for key, value in payload.items():
                if key in list_keys:
                    continue
                if isinstance(value, list) and value:
                    return True
                if isinstance(value, Mapping) and value:
                    return True
                if isinstance(value, (int, float)) and value != 0:
                    return True
                if isinstance(value, str) and value.strip():
                    return True
            return False

        type_counts: Dict[str, int] = {}
        payload_counts = {
            "boxes": 0,
            "markers": 0,
            "price_lines": 0,
            "polylines": 0,
            "segments": 0,
            "bubbles": 0,
        }
        profile_counts: Dict[str, int] = {}
        profile_params_present: Dict[str, int] = {}
        profile_params_samples: Dict[str, Dict[str, Any]] = {}
        type_payload_counts: Dict[str, Dict[str, int]] = {}
        transform_counts: Dict[str, Dict[str, int]] = {
            "known_profiles": {},
            "merged_profiles": {},
        }
        for overlay in overlays or []:
            if not isinstance(overlay, Mapping):
                continue
            overlay_type = str(overlay.get("type") or "unknown")
            type_counts[overlay_type] = type_counts.get(overlay_type, 0) + 1
            payload = overlay.get("payload")
            if not isinstance(payload, Mapping):
                continue
            if not _payload_has_content(payload):
                # Skip empty overlays so counts reflect visible artefacts
                continue
            profiles = payload.get("profiles")
            if isinstance(profiles, list):
                profile_counts[overlay_type] = profile_counts.get(overlay_type, 0) + len(profiles)
            if "profile_params" in payload:
                profile_params_present[overlay_type] = profile_params_present.get(overlay_type, 0) + 1
                if overlay_type not in profile_params_samples:
                    profile_params = payload.get("profile_params")
                    if isinstance(profile_params, Mapping):
                        profile_params_samples[overlay_type] = dict(profile_params)
            transform_summary = payload.get("transform_summary")
            if isinstance(transform_summary, Mapping):
                for key in ("known_profiles", "merged_profiles"):
                    value = transform_summary.get(key)
                    if isinstance(value, (int, float)):
                        bucket = transform_counts[key]
                        bucket[overlay_type] = bucket.get(overlay_type, 0) + int(value)
            for key in payload_counts.keys():
                entries = payload.get(key)
                if isinstance(entries, list):
                    payload_counts[key] += len(entries)
                    per_type = type_payload_counts.setdefault(
                        overlay_type,
                        {name: 0 for name in payload_counts.keys()},
                    )
                    per_type[key] += len(entries)
        return {
            "total_overlays": len(overlays or []),
            "type_counts": type_counts,
            "payload_counts": payload_counts,
            "profile_counts": profile_counts,
            "profile_params_present": profile_params_present,
            "profile_params_samples": profile_params_samples,
            "type_payload_counts": type_payload_counts,
            "transform_counts": transform_counts,
        }

    @staticmethod
    def _normalise_epoch(value: Any) -> Optional[int]:
        """Deprecated: Use normalize_epoch from domain module instead."""
        return normalize_epoch(value)

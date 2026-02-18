"""Bot runtime orchestrator combining domain, runtime, and reporting layers."""

from __future__ import annotations

import logging
import os
import threading
import uuid
import time
import json
from contextlib import nullcontext
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from queue import Empty, Full, Queue
from typing import Any, Callable, Deque, Dict, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

from engines.bot_runtime.core.domain import (
    Candle,
    DecisionLedgerEvent,
    StrategySignal,
    coerce_float,
    isoformat,
    normalize_epoch,
    timeframe_to_seconds,
)
from signals.overlays.registry import register_overlay_type
from signals.overlays.builtins import ensure_builtin_overlays_registered
from signals.overlays.schema import build_overlay, normalize_overlays
from ..reporting.reporting import (
    TRADE_OVERLAY_SOURCE,
    TRADE_STOP_COLOR,
    TRADE_TARGET_COLOR,
    TRADE_RAY_MIN_SECONDS,
    TRADE_RAY_SPAN_MULTIPLIER,
    instrument_key,
)
from .chart_state import ChartStateBuilder
from .event_sink import InMemoryEventSink, RuntimeEventSink
from .intrabar import IntrabarManager
from utils.log_context import build_log_context, merge_log_context, series_log_context, with_log_context
from .runtime_policy import RuntimeModePolicy
from ..strategy.series_builder import SeriesBuilder, StrategySeries
from .run_context import RunContext
from engines.bot_runtime.core.wallet import LockedWalletLedger, project_wallet
from engines.bot_runtime.core.indicator_state import (
    OverlayProjectionInput,
    SignalEvaluationInput,
    ensure_builtin_indicator_plugins_registered,
    evaluate_rules_from_state_snapshots,
    plugin_registry,
    project_overlay_delta,
)
from ....indicators.indicator_service.context import IndicatorServiceContext, _context as indicator_context
from indicators.runtime.indicator_overlay_cache import default_overlay_cache
from indicators.runtime.overlay_cache_registry import get_overlay_cache_types
from .persistence_buffer import TradePersistenceBuffer
from .series_runner import InlineSeriesRunner, PoolSeriesRunner, SeriesRunnerContext, ThreadedSeriesRunner
from .settlement import SettlementApplier
from .signal_consumption import SignalConsumption, consume_signals
from portal.backend.service.market.entry_context import build_entry_metrics, derive_entry_context
from portal.backend.service.market.stats_queue import REGIME_VERSION, STATS_VERSION
from utils.perf_log import (
    get_obs_enabled,
    get_obs_step_sample_rate,
    get_obs_slow_ms,
    perf_log,
    should_sample,
)

logger = logging.getLogger(__name__)

DEFAULT_SIM_LOOKBACK_DAYS = 7
MAX_LOG_ENTRIES = 500
MAX_WARNING_ENTRIES = 20
MAX_SIGNAL_CONSUMPTIONS = 500
INTRABAR_BASE_SECONDS = 0.4
WALK_FORWARD_SAMPLE_INTERVAL = 50
OVERLAY_SUMMARY_INTERVAL = 50

register_overlay_type(
    "bot_trade_rays",
    label="Trade Rays",
    pane_views=("segment",),
    description="Active trade stop/target rays for bot playback.",
    renderers={"lightweight": "segment", "mpl": "line"},
    payload_keys=("segments",),
    ui_color="#22d3ee",
)


@dataclass
class SeriesExecutionState:
    series: StrategySeries
    bar_index: int = 0
    total_bars: int = 0
    next_step_at: Optional[datetime] = None
    intrabar_candles: List[Candle] = field(default_factory=list)
    intrabar_index: int = 0
    active_candle: Optional[Candle] = None
    done: bool = False
    last_evaluated_epoch: int = 0
    last_consumed_epoch: int = 0
    pending_signals: Deque[StrategySignal] = field(default_factory=deque)
    signal_consumptions: Deque["SignalConsumption"] = field(
        default_factory=lambda: deque(maxlen=MAX_SIGNAL_CONSUMPTIONS)
    )
    indicator_state_runtime: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    indicator_projection_runtime: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def intrabar_active(self) -> bool:
        return bool(self.intrabar_candles) and self.intrabar_index < len(self.intrabar_candles)

def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    return coerce_float(value, default)


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    return isoformat(value)


def _timeframe_to_seconds(label: Optional[str]) -> Optional[int]:
    return timeframe_to_seconds(label)


class BotRuntime:
    """Simulated bot runtime that iterates over real candles and emits stats."""

    def __init__(
        self,
        bot_id: str,
        config: Dict[str, object],
        state_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        ensure_builtin_overlays_registered()
        self.bot_id = bot_id
        self.config = dict(config)
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
        self._series_update_lock = threading.RLock()
        self._trade_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._runner = None
        self._stop = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._paused = False
        self._series: List[StrategySeries] = []
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
        self._logs: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._warnings: Deque[Dict[str, Any]] = deque(maxlen=MAX_WARNING_ENTRIES)
        self._decision_events: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._signal_event_ids: Dict[Tuple[str, str, Optional[str], Optional[str]], str] = {}
        self._decision_event_ids: Dict[str, str] = {}
        self._entry_event_ids: Dict[str, str] = {}
        self._event_sinks: List[RuntimeEventSink] = [
            InMemoryEventSink(self._logs, self._decision_events, self._lock),
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
        for indicator_type in get_overlay_cache_types():
            overlay_cache.enable_type(indicator_type)
        self._overlay_cache = overlay_cache
        self._indicator_ctx = IndicatorServiceContext.fork_with_overlay_cache(
            indicator_context,
            overlay_cache,
        )
        self._obs_enabled = get_obs_enabled(self.config)
        self._obs_step_sample_rate = get_obs_step_sample_rate(self.config)
        self._obs_slow_ms = get_obs_slow_ms(self.config)
        self._series_builder = SeriesBuilder(
            self.bot_id,
            self.config,
            self.run_type,
            self._log_candle_sequence,
            indicator_ctx=self._indicator_ctx,
            warning_sink=self._record_runtime_warning,
        )
        ensure_builtin_indicator_plugins_registered()
        self._indicator_plugin_registry = plugin_registry()
        self._settlement_applier = SettlementApplier(
            obs_enabled=self._obs_enabled,
            obs_slow_ms=self._obs_slow_ms,
        )
        self._persistence_buffer = TradePersistenceBuffer.from_config(
            self.config,
            self._runtime_log_context,
        )
        self._intrabar_manager = IntrabarManager(
            self.bot_id,
            build_candles=SeriesBuilder._build_candles,
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
        # Stream payload cache: keep last derived slices so push_update emits true deltas.
        self._push_series_cache: Dict[str, Dict[str, Any]] = {}
        self._push_logs_fingerprint: Optional[Tuple[int, Optional[str], Optional[str]]] = None
        self._push_decisions_fingerprint: Optional[Tuple[int, Optional[str], Optional[str]]] = None

    def add_event_sink(self, sink: RuntimeEventSink) -> None:
        """Attach an additional event sink for runtime tracing."""
        if sink is None:
            return
        with self._lock:
            self._event_sinks.append(sink)

    @staticmethod
    def _coerce_playback_speed(value: Optional[object]) -> float:
        return 0.0

    @staticmethod
    def _resolve_series_runner_type(value: Optional[object]) -> str:
        if value is None:
            return "threaded"
        normalized = str(value).strip().lower()
        if normalized in {"inline", "threaded", "pool"}:
            return normalized
        raise ValueError(f"Unknown series_runner '{value}'. Expected 'inline', 'threaded', or 'pool'.")

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
        )
        if self._series_runner_type == "threaded":
            return ThreadedSeriesRunner(ctx)
        if self._series_runner_type == "pool":
            return PoolSeriesRunner(ctx, max_workers=self._pool_worker_count())
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

    def _ensure_prepared(self) -> None:
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
            streams = self._series_builder.build_series_by_ids(strategy_ids)
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

        from ....indicators import indicator_service

        for link in indicator_links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id:
                continue
            meta = indicator_service.get_instance_meta(indicator_id, ctx=self._indicator_ctx)
            indicator_type = str(meta.get("type") or "").strip().lower()
            if not indicator_type:
                raise RuntimeError(f"indicator_state_setup_failed: missing indicator type | indicator_id={indicator_id}")
            plugin = self._indicator_plugin_registry.resolve(indicator_type)
            engine = plugin.engine_factory(meta)
            if engine is None:
                raise RuntimeError(f"indicator_plugin_engine_missing: indicator_type={indicator_type} indicator_id={indicator_id}")
            engine_state = engine.initialize({
                "symbol": series.symbol,
                "timeframe": series.timeframe,
                "strategy_id": series.strategy_id,
                "indicator_id": indicator_id,
            })
            state.indicator_state_runtime[indicator_id] = {
                "indicator_id": indicator_id,
                "indicator_type": indicator_type,
                "indicator_meta": dict(meta),
                "plugin": plugin,
                "engine": engine,
                "engine_state": engine_state,
                "last_revision": -1,
            }
            state.indicator_projection_runtime[indicator_id] = {
                "seq": 0,
                "revision": -1,
                "entries": {},
            }

        # Prime indicator engines with warmup candles before replay start so
        # overlays/signals at bar 0 reflect known history.
        warmup_count = max(int(state.bar_index or 0), 0)
        if warmup_count <= 0:
            return
        warmup_candles = list(series.candles[:warmup_count])
        for runtime in state.indicator_state_runtime.values():
            engine = runtime.get("engine")
            engine_state = runtime.get("engine_state")
            if engine is None or not isinstance(engine_state, MutableMapping):
                continue
            for warmup_candle in warmup_candles:
                engine.apply_bar(engine_state, warmup_candle)
            snapshot = engine.snapshot(engine_state)
            runtime["last_revision"] = snapshot.revision
            indicator_type = str(runtime.get("indicator_type") or "")
            projector = getattr(runtime.get("plugin"), "overlay_projector", None)
            if projector is None:
                continue
            raw_entries = projector(
                OverlayProjectionInput(
                    snapshot=snapshot,
                    previous_projection_state={"seq": 0, "revision": -1, "entries": {}},
                )
            )
            if not isinstance(raw_entries, Mapping):
                continue
            normalized_entries: Dict[str, Dict[str, Any]] = {}
            indicator_meta = runtime.get("indicator_meta") if isinstance(runtime.get("indicator_meta"), Mapping) else {}
            overlay_color = indicator_meta.get("color") if isinstance(indicator_meta, Mapping) else None
            for entry_key, entry_value in raw_entries.items():
                if not isinstance(entry_value, Mapping):
                    continue
                normalized = normalize_overlays(indicator_type, [dict(entry_value)])
                if not normalized:
                    continue
                overlay_entry = dict(normalized[0])
                overlay_entry.update(
                    {
                        "ind_id": indicator_id,
                        "source": "indicator_state",
                        "bot_id": self.bot_id,
                        "strategy_id": series.strategy_id,
                        "symbol": series.symbol,
                        "timeframe": series.timeframe,
                    }
                )
                if isinstance(overlay_color, str) and overlay_color.strip():
                    overlay_entry["color"] = overlay_color
                normalized_entries[str(entry_key)] = overlay_entry
            indicator_id = str(runtime.get("indicator_id") or "").strip()
            if not indicator_id:
                continue
            projection_state = state.indicator_projection_runtime.setdefault(
                indicator_id,
                {"seq": 0, "revision": -1, "entries": {}},
            )
            projection_state["revision"] = snapshot.revision
            projection_state["entries"] = normalized_entries

        series.overlays = self._series_overlay_entries(state)
        logger.info(
            with_log_context(
                "indicator_state_warmup_seeded",
                self._series_log_context(
                    series,
                    warmup_candles=warmup_count,
                    indicators=len(state.indicator_state_runtime),
                    overlays=len(series.overlays or []),
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
        return self._series_builder._instrument_for(datasource, exchange, symbol)

    def _resolve_live_window(self) -> Tuple[str, str]:
        return self._series_builder._resolve_live_window()

    def _indicator_overlay_entries(
        self,
        strategy: Mapping[str, Any],
        start_iso: str,
        end_iso: str,
        timeframe: Optional[str],
        symbol: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> List[Dict[str, Any]]:
        return self._series_builder._indicator_overlay_entries(
            strategy,
            start_iso,
            end_iso,
            timeframe,
            symbol,
            datasource,
            exchange,
        )

    def _series_overlay_entries(self, state: SeriesExecutionState) -> List[Dict[str, Any]]:
        overlays: List[Dict[str, Any]] = []
        for projection_state in state.indicator_projection_runtime.values():
            entries = projection_state.get("entries")
            if not isinstance(entries, Mapping):
                continue
            overlays.extend(dict(value) for value in entries.values() if isinstance(value, Mapping))
        visible_count = min(max(int(state.bar_index) + 1, 1), len(state.series.candles))
        visible_candles = state.series.candles[:visible_count]
        overlays.extend(self._build_runtime_regime_overlays(series=state.series, candles=visible_candles))
        return overlays

    def _bootstrap_indicator_overlays_for_state(self, state: SeriesExecutionState) -> None:
        series = state.series
        strategy_meta = series.meta or {}
        indicator_links = list(strategy_meta.get("indicator_links") or [])
        replay_idx = max(int(state.bar_index or 0), 0)
        if replay_idx >= len(series.candles):
            replay_idx = max(len(series.candles) - 1, 0)
        if not series.candles:
            return
        end_iso = _isoformat(series.candles[replay_idx].time) or _isoformat(series.candles[-1].time)
        start_iso = str(series.window_start or end_iso)
        fetched: List[Dict[str, Any]] = []
        if indicator_links:
            fetched = self._indicator_overlay_entries(
                strategy_meta,
                start_iso,
                str(end_iso),
                series.timeframe,
                series.symbol,
                series.datasource,
                series.exchange,
            )
        grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}
        counters: Dict[str, int] = {}
        for overlay in fetched:
            if not isinstance(overlay, Mapping):
                continue
            indicator_id = str(overlay.get("ind_id") or "").strip()
            if not indicator_id:
                continue
            bucket = grouped.setdefault(indicator_id, {})
            idx = counters.get(indicator_id, 0)
            key = f"bootstrap:{idx}"
            counters[indicator_id] = idx + 1
            bucket[key] = dict(overlay)
        for indicator_id, entries in grouped.items():
            projection_state = state.indicator_projection_runtime.setdefault(
                indicator_id,
                {"seq": 0, "revision": -1, "entries": {}},
            )
            projection_state["entries"] = entries
        series.overlays = self._series_overlay_entries(state)
        series.bootstrap_completed = True
        series.bootstrap_indicator_overlays = int(sum(len(entries or {}) for entries in grouped.values()))
        series.bootstrap_total_overlays = len(series.overlays or [])
        logger.info(
            with_log_context(
                "indicator_overlay_bootstrap_completed",
                self._series_log_context(
                    series,
                    start=start_iso,
                    end=end_iso,
                    overlays=len(series.overlays or []),
                    indicators=len(grouped),
                    indicator_overlays=series.bootstrap_indicator_overlays,
                ),
            )
        )

    def _validate_bootstrap_readiness(self) -> None:
        min_total = max(int(self.config.get("bootstrap_min_total_overlays_per_series") or 0), 0)
        min_indicator = max(int(self.config.get("bootstrap_min_indicator_overlays_per_series") or 1), 0)
        require_indicator = bool(self.config.get("bootstrap_require_indicator_overlays", True))
        require_indicator = require_indicator and bool(self.config.get("include_indicator_overlays", True))

        failures: List[Dict[str, Any]] = []
        for state in self._series_states:
            series = state.series
            if not bool(series.bootstrap_completed):
                failures.append(
                    self._series_log_context(
                        series,
                        reason="bootstrap_incomplete",
                        bootstrap_completed=False,
                    )
                )
                continue

            total_overlays = int(series.bootstrap_total_overlays or 0)
            indicator_overlays = int(series.bootstrap_indicator_overlays or 0)
            indicator_links = list((series.meta or {}).get("indicator_links") or [])
            expected_indicators = len(indicator_links)

            if min_total > 0 and total_overlays < min_total:
                failures.append(
                    self._series_log_context(
                        series,
                        reason="bootstrap_total_overlays_below_min",
                        total_overlays=total_overlays,
                        min_total_overlays=min_total,
                    )
                )
                continue

            if require_indicator and expected_indicators > 0 and indicator_overlays < min_indicator:
                failures.append(
                    self._series_log_context(
                        series,
                        reason="bootstrap_indicator_overlays_below_min",
                        indicator_overlays=indicator_overlays,
                        min_indicator_overlays=min_indicator,
                        expected_indicators=expected_indicators,
                    )
                )

        if failures:
            self._prepare_error = {
                "message": "Indicator bootstrap validation failed.",
                "failures": failures,
            }
            context = self._runtime_log_context(
                failures=failures,
                min_total_overlays=min_total,
                min_indicator_overlays=min_indicator,
                require_indicator_overlays=require_indicator,
            )
            logger.error(with_log_context("bot_runtime_bootstrap_validation_failed", context))
            raise RuntimeError(
                "Indicator bootstrap validation failed. "
                "Check bot_runtime_bootstrap_validation_failed log for per-series details."
            )

    def _build_runtime_regime_overlays(
        self,
        *,
        series: StrategySeries,
        candles: Sequence[Candle],
    ) -> List[Dict[str, Any]]:
        instrument = series.instrument if isinstance(series.instrument, Mapping) else {}
        instrument_id = str(instrument.get("id") or "").strip()
        if not instrument_id:
            return []
        return self._series_builder._build_regime_overlays(
            instrument_id=instrument_id,
            candles=list(candles),
            timeframe=series.timeframe,
            strategy_id=series.strategy_id,
            symbol=series.symbol,
        )

    @staticmethod
    def _indicator_overlay_cache_key(
        indicator_id: str,
        start_iso: Optional[str],
        end_iso: Optional[str],
        interval: Optional[str],
        symbol: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> str:
        return SeriesBuilder._indicator_overlay_cache_key(
            indicator_id, start_iso, end_iso, interval, symbol, datasource, exchange
        )

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
    def _build_signals_from_markers(markers: Mapping[str, Any]) -> Deque[StrategySignal]:
        return SeriesBuilder._build_signals_from_markers(markers)

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
                impact_pnl = event.get("pnl") if event_subtype in {"target", "stop"} else None
                trade_net_pnl = event.get("net_pnl") if event_subtype == "close" else None
                self._record_execution_ledger_event(
                    series,
                    event_subtype=event_subtype,
                    event_ts=event_ts,
                    trade_id=event.get("trade_id"),
                    side=event.get("direction"),
                    qty=event.get("contracts"),
                    price=event.get("price"),
                    event_impact_pnl=impact_pnl,
                    trade_net_pnl=trade_net_pnl,
                    evidence_details=event,
                )
            self._persist_trade_event(series, event)
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
        return {
            "finalize_residual_ms": finalize_residual_ms,
            "persist_ms": persist_ms,
            "db_commit_ms": db_commit_ms,
            "stats_update_ms": stats_update_ms,
            "delta_build_ms": push_metrics.get("delta_build_ms"),
            "delta_serialize_ms": push_metrics.get("delta_serialize_ms"),
            "stream_emit_ms": push_metrics.get("stream_emit_ms"),
            "subscribers_count": push_metrics.get("subscribers_count"),
            "overlay_points_changed": push_metrics.get("overlay_points"),
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

    @staticmethod
    def _extract_indicator_overlays(result: Mapping[str, Any]) -> List[Dict[str, Any]]:
        # Indicator results include overlays that visualize raw signal markers.
        # The bot lens should only render the strategy's configured indicator
        # overlays, so skip signal-driven visuals entirely.
        return SeriesBuilder._extract_indicator_overlays(result)

    def reset(self) -> None:
        """Clear cached series so the runtime can restart fresh."""

        if self._thread and self._thread.is_alive():
            raise RuntimeError("Cannot reset a running bot runtime")
        with self._lock:
            self._prepared = False
            self._series = []
            self._series_states = []
            self._series_state_map = {}
            self._primary_series_key = None
            self._total_bars = 0
            self._chart_overlays = []
            self._overlay_summary_cache = {}
            self._last_stats = {}
            self._next_bar_at = None
            self._logs.clear()
            self._warnings.clear()
            self._decision_events.clear()
            self._intrabar_manager.clear_cache()
            self._run_started_at = None
            self._run_context = None
            self._runner = None
            self._push_series_cache = {}
            self._push_logs_fingerprint = None
            self._push_decisions_fingerprint = None
            self.state = {"status": "idle", "progress": 0.0, "paused": False}
        self._stop.clear()
        self._pause_event.set()
        self._paused = False
        self._series_builder.reset()
        self._overlay_cache.clear()

    def needs_reset(self) -> bool:
        """Return True when the runtime finished and can be rerun."""

        status = str(self.state.get("status") or "").lower()
        finished = status in {"completed", "stopped", "error"}
        exhausted = bool(self._series_states) and all(
            state.done or state.bar_index >= state.total_bars for state in self._series_states
        )
        thread_active = self._thread and self._thread.is_alive()
        return not thread_active and (finished or exhausted)

    def reset_if_finished(self) -> None:
        """Reset cached series if the previous run completed."""

        if self.needs_reset():
            self.reset()

    def warm_up(self) -> None:
        """Prepare strategy sessions so the lens can query data."""

        if self._prepared:
            return
        self._ensure_prepared()

    def start(self) -> None:
        """Start the execution loop in the background."""

        logger.info(
            with_log_context(
                "bot_runtime_start_invoked",
                self._runtime_log_context(
                    mode=self.mode,
                    run_type=self.run_type,
                    series_runner=self._series_runner_type,
                    thread_alive=bool(self._thread and self._thread.is_alive()),
                ),
            )
        )
        self._set_phase("prepare", "bot_runtime_prepare_start")
        self._ensure_prepared()
        if self._thread and self._thread.is_alive():
            logger.info(with_log_context("bot_runtime_start_ignored", self._runtime_log_context()))
            return
        logger.info(with_log_context("bot_runtime_run_context_start", self._runtime_log_context()))
        self._stop.clear()
        self._pause_event.set()
        self._paused = False
        self._run_started_at = datetime.now(timezone.utc)
        self._run_context = self._build_run_context()
        logger.info(with_log_context("bot_runtime_run_context_ready", self._runtime_log_context()))
        with self._lock:
            self.state.update(
                {"status": "starting", "paused": False, "started_at": _isoformat(self._run_started_at)}
            )
        self._set_phase("start_threads", "bot_runtime_thread_starting")
        logger.info(
            with_log_context(
                "bot_runtime_thread_starting",
                self._runtime_log_context(
                    mode=self.mode,
                    run_type=self.run_type,
                    series_runner=self._series_runner_type,
                    series=len(self._series_states),
                ),
            )
        )
        self._thread = threading.Thread(target=self._run, name=f"bot-{self.bot_id}", daemon=True)
        self._thread.start()
        logger.info(
            with_log_context(
                "bot_runtime_thread_started_dispatch",
                self._runtime_log_context(
                    thread_alive=bool(self._thread and self._thread.is_alive()),
                ),
            )
        )
        self._log_event("start", message="Bot runtime started", mode=self.mode, run_type=self.run_type)
        self._push_update("start")

    def _run(self) -> None:
        try:
            logger.debug(with_log_context("bot_runtime_thread_started", self._runtime_log_context()))
            self._execute_loop()
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(error=str(exc))
            logger.exception(with_log_context("bot_runtime_loop_failed", context))
            self._set_error_state(str(exc))
            self._push_update("error")
            self._persist_runtime_state("error")
            self._flush_persistence_buffer("runtime_loop_failed")

    def _execute_loop(self) -> None:
        self._ensure_prepared()
        status = "running"
        loop_started = datetime.now(timezone.utc)
        self._set_phase("running", "bot_runtime_running")
        self._log_event(
            "running",
            message="Bot execution loop started",
            series_runner=self._series_runner_type,
            series_count=len(self._series_states),
        )
        self._start_overlay_aggregator()
        try:
            self._runner = self._build_series_runner()
            self._runner.run()
        finally:
            self._stop_overlay_aggregator()
        runtime_status = str(self.state.get("status") or "").lower()
        if runtime_status == "error":
            status = "error"
        elif self._stop.is_set():
            status = "stopped"
        elif not self._live_mode:
            status = "completed"
        self._next_bar_at = None
        self._record_step_trace(
            "run_loop",
            started_at=loop_started,
            ended_at=datetime.now(timezone.utc),
            ok=(status != "error"),
            context={
                "status": status,
                "series_count": len(self._series_states),
            },
        )
        self._log_event(status, message=f"Bot runtime {status}")
        if status in {"completed", "stopped"}:
            duration_seconds = None
            if self._run_started_at is not None:
                duration_seconds = (datetime.now(timezone.utc) - self._run_started_at).total_seconds()
            summary = self._aggregate_stats()
            drawdown = self._max_drawdown_from_trades()
            context = self._runtime_log_context(
                status=status,
                trades=summary.get("completed_trades"),
                gross_pnl=summary.get("gross_pnl"),
                net_pnl=summary.get("net_pnl"),
                fees=summary.get("fees_paid"),
                drawdown=drawdown,
                duration_seconds=duration_seconds,
            )
            logger.info(with_log_context("bot_run_end_summary", context))
        # Update state with last candle from first series (backward compatibility)
        if self._series and self._series[0].candles:
            self._update_state(self._series[0].candles[-1], status=status)
        else:
            with self._lock:
                self.state.update({"status": status})
        self._push_update(status)
        self._persist_runtime_state(status)
        if status in {"completed", "stopped"}:
            self._persist_run_artifact(status)
        self._flush_persistence_buffer("runtime_loop_complete")

    def _step_series_state(self, state: SeriesExecutionState) -> None:
        if state.done:
            return
        if state.intrabar_active():
            self._step_intrabar(state)
            return
        if state.bar_index >= state.total_bars:
            state.done = True
            return
        series = state.series
        strategy_id = getattr(series, "strategy_id", None)
        symbol = getattr(series, "symbol", None)
        timeframe = getattr(series, "timeframe", None)
        step_started = datetime.now(timezone.utc)
        step_context: Dict[str, Any] = {
            "bar_index": state.bar_index,
            "total_bars": state.total_bars,
        }
        candle_update_ms: Optional[float] = None
        overlays_update_ms: Optional[float] = None
        pending_signals_ops_ms: Optional[float] = None
        execution_ms: Optional[float] = None
        stats_update_ms: Optional[float] = None
        persistence_ms: Optional[float] = None
        db_commit_ms: Optional[float] = None
        delta_build_ms: Optional[float] = None
        delta_serialize_ms: Optional[float] = None
        stream_emit_ms: Optional[float] = None
        indicators_count: Optional[float] = None
        overlays_changed_count: Optional[float] = None
        overlay_points_changed: Optional[float] = None
        signals_emitted_count: Optional[float] = None
        subscribers_count: Optional[float] = None
        trades_touched_count: float = 0.0
        decision_events_logged = 0
        execution_events_logged = 0
        trade_events_processed = 0
        entry_created = False
        sample_enabled = self._obs_enabled and should_sample(self._obs_step_sample_rate)
        base_context = self._series_log_context(
            series,
            bar_index=state.bar_index,
            total_bars=state.total_bars,
        )
        perf_context = (
            perf_log(
                "bot_runtime_step_series_state",
                logger=logger,
                base_context=base_context,
                enabled=sample_enabled,
                slow_ms=self._obs_slow_ms,
            )
            if sample_enabled
            else nullcontext()
        )
        try:
            with perf_context:
                candle_update_started = time.perf_counter()
                candle = series.candles[state.bar_index]
                state.active_candle = candle
                epoch = int(candle.time.timestamp())
                candle_update_ms = max((time.perf_counter() - candle_update_started) * 1000.0, 0.0)
                step_context["epoch"] = epoch

                context = self._series_log_context(
                    series,
                    bar_index=state.bar_index,
                    epoch=epoch,
                )
                logger.debug(with_log_context("apply_bar", context))
                if state.bar_index % WALK_FORWARD_SAMPLE_INTERVAL == 0:
                    info_context = self._series_log_context(
                        series,
                        bar_index=state.bar_index,
                        bar_time=isoformat(candle.time),
                        status=self.state.get("status"),
                    )
                    logger.info(with_log_context("walk_forward_step", info_context))

                signal_eval_started = datetime.now(timezone.utc)
                signal_event_logged = False
                next_last_evaluated_epoch = state.last_evaluated_epoch
                next_last_consumed_epoch = state.last_consumed_epoch
                try:
                    (
                        consumed_signals,
                        direction,
                        signals_pending,
                        signal_eval_metrics,
                        next_last_evaluated_epoch,
                        next_last_consumed_epoch,
                    ) = self._next_signal_for(
                        state,
                        series,
                        candle,
                        epoch,
                    )
                    overlays_update_ms = signal_eval_metrics.get("overlays_update_ms")
                    pending_signals_ops_ms = signal_eval_metrics.get("pending_signals_ops_ms")
                    indicators_count = signal_eval_metrics.get("indicators_count")
                    overlays_changed_count = signal_eval_metrics.get("overlays_changed_count")
                    overlay_points_changed = signal_eval_metrics.get("overlay_points_changed")
                    signals_emitted_count = signal_eval_metrics.get("signals_emitted_count")
                    step_context["signals_pending"] = signals_pending
                    self._record_signal_consumption(state, epoch, consumed_signals, direction)

                    # Debug: Log signal consumption result
                    if direction is not None:
                        context = self._series_log_context(
                            series,
                            bar_index=state.bar_index,
                            epoch=epoch,
                            direction=direction,
                        )
                        logger.debug(with_log_context("signal_consumed", context))
                        signal_event_id = self._record_ledger_event(
                            event_type="signal",
                            event_subtype="strategy_signal",
                            event_ts=_isoformat(candle.time),
                            reason_code="SIGNAL_STRATEGY_SIGNAL",
                            series=series,
                            side=direction,
                            price=candle.close,
                            evidence_refs=[
                                {
                                    "ref_type": "indicator",
                                    "ref_id": "strategy_signal",
                                    "summary": f"direction={direction} price={round(candle.close, 4)}",
                                }
                            ],
                        )
                        self._signal_event_ids[
                            self._signal_key(series, "strategy_signal", direction, None)
                        ] = signal_event_id
                        signal_event_logged = True
                    self._record_step_trace(
                        "step_signal_eval",
                        started_at=signal_eval_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=True,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        context={
                            "bar_index": state.bar_index,
                            "consumed_signals_count": len(consumed_signals),
                            "direction_present": bool(direction),
                            "signal_event_logged": signal_event_logged,
                            "epochs_evaluated_this_tick": signal_eval_metrics.get("epochs_evaluated_this_tick"),
                            "strategy_eval_ms": signal_eval_metrics.get("strategy_eval_ms"),
                            "indicator_eval_ms": signal_eval_metrics.get("indicator_eval_ms"),
                            "rule_eval_ms": signal_eval_metrics.get("rule_eval_ms"),
                            "indicator_state_update_ms": signal_eval_metrics.get("indicator_state_update_ms"),
                            "signal_eval_ms": signal_eval_metrics.get("signal_eval_ms"),
                            "overlay_projection_ms": signal_eval_metrics.get("overlay_projection_ms"),
                            "overlay_projection_skipped_count": signal_eval_metrics.get("overlay_projection_skipped_count"),
                            "state_revisions_changed_count": signal_eval_metrics.get("state_revisions_changed_count"),
                            "signals_emitted_count": signal_eval_metrics.get("signals_emitted_count"),
                            "overlays_update_ms": signal_eval_metrics.get("overlays_update_ms"),
                            "pending_signals_ops_ms": signal_eval_metrics.get("pending_signals_ops_ms"),
                            "indicators_count": signal_eval_metrics.get("indicators_count"),
                            "overlays_changed_count": signal_eval_metrics.get("overlays_changed_count"),
                            "overlay_points_changed": signal_eval_metrics.get("overlay_points_changed"),
                        },
                    )
                except Exception as exc:
                    self._record_step_trace(
                        "step_signal_eval",
                        started_at=signal_eval_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=False,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        error=str(exc),
                        context={
                            "bar_index": state.bar_index,
                            "signal_event_logged": signal_event_logged,
                            "epochs_evaluated_this_tick": None,
                            "strategy_eval_ms": None,
                            "indicator_eval_ms": None,
                            "rule_eval_ms": None,
                            "signals_emitted_count": None,
                            "indicator_state_update_ms": None,
                            "signal_eval_ms": None,
                            "overlay_projection_ms": None,
                            "overlay_projection_skipped_count": None,
                            "state_revisions_changed_count": None,
                            "overlays_update_ms": None,
                            "pending_signals_ops_ms": None,
                            "indicators_count": None,
                            "overlays_changed_count": None,
                            "overlay_points_changed": None,
                        },
                    )
                    raise

            execution_started_perf = time.perf_counter()
            decision_flow_started = datetime.now(timezone.utc)
            decision_flow_started_perf = time.perf_counter()
            blocking_trade = None
            new_trade = None
            try:
                # Attempt to create trade from signal
                if direction is not None:
                    instrument_id = None
                    if isinstance(series.instrument, Mapping):
                        instrument_id = series.instrument.get("id")
                    if not instrument_id:
                        decision_events_logged += 1
                        self._log_decision_event(
                            event="signal_rejected",
                            series=series,
                            candle=candle,
                            signal_type="strategy_signal",
                            signal_direction=direction,
                            signal_price=candle.close,
                            rule_id=None,
                            decision="rejected",
                            reason_code="DECISION_REJECTED_INSTRUMENT_MISSING",
                            reason_detail="Instrument id missing.",
                            context={
                                "signal_type": "strategy_signal",
                                "signal_direction": direction,
                                "signal_price": candle.close,
                                "blocked_instrument_id": None,
                            },
                            instrument_id=None,
                        )
                        direction = None
                    else:
                        with self._trade_lock:
                            blocking_trade = self._active_trade_for_instrument(
                                instrument_id,
                                skip_series=series,
                            )
                            if blocking_trade is None:
                                new_trade = series.risk_engine.maybe_enter(candle, direction)

                # Log decision event
                if direction is not None:
                    if new_trade is not None:
                        # Signal was accepted and trade was opened
                        decision_events_logged += 1
                        self._log_decision_event(
                            event="signal_accepted",
                            series=series,
                            candle=candle,
                            signal_type="strategy_signal",  # Generic type for now
                            signal_direction=direction,
                            signal_price=candle.close,
                            rule_id=None,  # Not available in current signal queue
                            decision="accepted",
                            reason_code="DECISION_ACCEPTED",
                            trade_id=new_trade.trade_id,
                            trade_time=_isoformat(new_trade.entry_time),
                        )
                    else:
                        # Signal was rejected (no trade opened)
                        # Determine rejection reason
                        rejection_reason = "Active trade already open"
                        rejection_meta: Optional[Dict[str, Any]] = None
                        blocking_trade_id: Optional[str] = None
                        rejection_code: Optional[str] = None
                        rejection_context: Dict[str, Any] = {
                            "signal_type": "strategy_signal",
                            "signal_direction": direction,
                            "signal_price": candle.close,
                        }
                        if blocking_trade is not None:
                            rejection_reason = "Active trade already open for instrument"
                            rejection_code = "DECISION_REJECTED_ACTIVE_TRADE"
                            blocked_instrument_id = None
                            if isinstance(series.instrument, Mapping):
                                blocked_instrument_id = series.instrument.get("id")
                            blocking_trade_id = getattr(blocking_trade, "trade_id", None)
                            rejection_meta = {
                                "active_trade_id": blocking_trade_id,
                                "blocked_instrument_id": blocked_instrument_id,
                            }
                            rejection_context.update(rejection_meta)
                        elif series.risk_engine.active_trade is None:
                            rejection_reason = series.risk_engine.last_rejection_reason or "Risk engine declined entry"
                            rejection_code = "DECISION_REJECTED_RISK_ENGINE"
                            rejection_meta = series.risk_engine.last_rejection_detail
                            if isinstance(rejection_meta, Mapping):
                                rejection_context.update(rejection_meta)
                            rejection_context["risk_engine_reason"] = rejection_reason
                        resolved_trade_id, metadata_payload = self._normalise_rejection_metadata(
                            rejection_meta,
                            blocking_trade_id,
                        )

                        decision_events_logged += 1
                        self._log_decision_event(
                            event="signal_rejected",
                            series=series,
                            candle=candle,
                            signal_type="strategy_signal",
                            signal_direction=direction,
                            signal_price=candle.close,
                            rule_id=None,
                            decision="rejected",
                            reason_code=rejection_code or "DECISION_REJECTED",
                            reason_detail=rejection_reason,
                            context=rejection_context,
                            trade_id=resolved_trade_id,
                            **metadata_payload,
                        )

                if new_trade is not None:
                    entry_created = True
                    targets = [
                        {"name": leg.name, "price": round(leg.target_price, 4)}
                        for leg in new_trade.legs
                    ]
                    self._log_event(
                        "entry",
                        series,
                        candle,
                        trade_id=new_trade.trade_id,
                        direction=direction,
                        entry_price=round(new_trade.entry_price, 4),
                        stop_price=round(new_trade.stop_price, 4),
                        targets=targets,
                        bar_index=state.bar_index,
                        contracts=sum(max(leg.contracts, 0) for leg in new_trade.legs),
                        trade_time=_isoformat(new_trade.entry_time),
                    )
                    execution_events_logged += 1
                    self._record_execution_ledger_event(
                        series,
                        event_subtype="entry",
                        event_ts=_isoformat(new_trade.entry_time),
                        trade_id=new_trade.trade_id,
                        side=direction,
                        qty=sum(max(leg.contracts, 0) for leg in new_trade.legs),
                        price=new_trade.entry_price,
                        evidence_details={
                            "stop_price": round(new_trade.stop_price, 4),
                            "targets": targets,
                        },
                    )
                    self._persist_trade_entry(series, new_trade)
                    self._update_trade_overlay(series)
                self._record_step_trace(
                    "step_decision_flow",
                    started_at=decision_flow_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    context={
                        "bar_index": state.bar_index,
                        "decision_events_logged": decision_events_logged,
                        "entry_created": entry_created,
                    },
                )
            except Exception as exc:
                self._record_step_trace(
                    "step_decision_flow",
                    started_at=decision_flow_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(exc),
                    context={
                        "bar_index": state.bar_index,
                        "decision_events_logged": decision_events_logged,
                        "entry_created": entry_created,
                    },
                )
                raise
            decision_flow_ms = max((time.perf_counter() - decision_flow_started_perf) * 1000.0, 0.0)
            prime_started_perf = time.perf_counter()
            trade_events = self._prime_intrabar_or_step_bar(state, candle)
            execution_prime_ms = max((time.perf_counter() - prime_started_perf) * 1000.0, 0.0)
            exit_settlement = getattr(series.risk_engine, "exit_settlement", None) if series.risk_engine else None
            settlement_started = datetime.now(timezone.utc)
            settlement_started_perf = time.perf_counter()
            try:
                self._settlement_applier.apply(trade_events, exit_settlement)
                self._record_step_trace(
                    "settlement_apply",
                    started_at=settlement_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=getattr(series, "strategy_id", None),
                    symbol=getattr(series, "symbol", None),
                    timeframe=getattr(series, "timeframe", None),
                    context={"events": len(trade_events)},
                )
            except Exception as exc:
                self._record_step_trace(
                    "settlement_apply",
                    started_at=settlement_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=getattr(series, "strategy_id", None),
                    symbol=getattr(series, "symbol", None),
                    timeframe=getattr(series, "timeframe", None),
                    error=str(exc),
                    context={"events": len(trade_events)},
                )
                raise
            settlement_ms = max((time.perf_counter() - settlement_started_perf) * 1000.0, 0.0)
            event_processing_started = datetime.now(timezone.utc)
            event_processing_started_perf = time.perf_counter()
            try:
                for event in trade_events:
                    trade_events_processed += 1
                    trade_time = self._trade_entry_time(series, event.get("trade_id"))
                    self._log_event(
                        event.get("type", "event"),
                        series,
                        candle,
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
                        trade_time=trade_time,
                    )
                    raw_subtype = event.get("type")
                    event_ts = event.get("time")
                    if raw_subtype and event_ts:
                        event_subtype = str(raw_subtype)
                        impact_pnl = event.get("pnl") if event_subtype in {"target", "stop"} else None
                        trade_net_pnl = event.get("net_pnl") if event_subtype == "close" else None
                        execution_events_logged += 1
                        self._record_execution_ledger_event(
                            series,
                            event_subtype=event_subtype,
                            event_ts=event_ts,
                            trade_id=event.get("trade_id"),
                            side=event.get("direction"),
                            qty=event.get("contracts"),
                            price=event.get("price"),
                            event_impact_pnl=impact_pnl,
                            trade_net_pnl=trade_net_pnl,
                            evidence_details=event,
                        )
                    self._persist_trade_event(series, event)
                self._update_trade_overlay(series)
                state.last_evaluated_epoch = max(state.last_evaluated_epoch, next_last_evaluated_epoch)
                state.last_consumed_epoch = max(state.last_consumed_epoch, next_last_consumed_epoch)
                series.last_consumed_epoch = state.last_consumed_epoch
                self._record_step_trace(
                    "step_trade_event_processing",
                    started_at=event_processing_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=True,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    context={
                        "bar_index": state.bar_index,
                        "trade_events_count": len(trade_events),
                        "trade_events_processed": trade_events_processed,
                        "execution_events_logged": execution_events_logged,
                    },
                )
            except Exception as exc:
                self._record_step_trace(
                    "step_trade_event_processing",
                    started_at=event_processing_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=False,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    error=str(exc),
                    context={
                        "bar_index": state.bar_index,
                        "trade_events_count": len(trade_events),
                        "trade_events_processed": trade_events_processed,
                        "execution_events_logged": execution_events_logged,
                    },
                )
                raise
            event_processing_ms = max((time.perf_counter() - event_processing_started_perf) * 1000.0, 0.0)
            execution_ms = max((time.perf_counter() - execution_started_perf) * 1000.0, 0.0)
            if not state.intrabar_active():
                finalize_started = datetime.now(timezone.utc)
                try:
                    finalize_metrics = self._finalize_bar_step(state, candle)
                    stats_update_ms = finalize_metrics.get("stats_update_ms")
                    persistence_ms = finalize_metrics.get("persist_ms")
                    db_commit_ms = finalize_metrics.get("db_commit_ms")
                    delta_build_ms = finalize_metrics.get("delta_build_ms")
                    delta_serialize_ms = finalize_metrics.get("delta_serialize_ms")
                    stream_emit_ms = finalize_metrics.get("stream_emit_ms")
                    subscribers_count = finalize_metrics.get("subscribers_count")
                    self._record_step_trace(
                        "step_finalize_bar",
                        started_at=finalize_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=True,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        context={
                            "bar_index": state.bar_index,
                            "done": bool(state.done),
                            "finalize_residual_ms": finalize_metrics.get("finalize_residual_ms"),
                            "persist_ms": finalize_metrics.get("persist_ms"),
                            "stats_update_ms": finalize_metrics.get("stats_update_ms"),
                            "db_commit_ms": finalize_metrics.get("db_commit_ms"),
                            "delta_build_ms": finalize_metrics.get("delta_build_ms"),
                            "delta_serialize_ms": finalize_metrics.get("delta_serialize_ms"),
                            "stream_emit_ms": finalize_metrics.get("stream_emit_ms"),
                            "subscribers_count": finalize_metrics.get("subscribers_count"),
                        },
                    )
                except Exception as exc:
                    self._record_step_trace(
                        "step_finalize_bar",
                        started_at=finalize_started,
                        ended_at=datetime.now(timezone.utc),
                        ok=False,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        timeframe=timeframe,
                        error=str(exc),
                        context={
                            "bar_index": state.bar_index,
                            "done": bool(state.done),
                            "finalize_residual_ms": None,
                            "persist_ms": None,
                            "stats_update_ms": None,
                            "db_commit_ms": None,
                            "delta_build_ms": None,
                            "delta_serialize_ms": None,
                            "stream_emit_ms": None,
                            "subscribers_count": None,
                        },
                    )
                    raise
            trades_touched_count = float(trade_events_processed + (1 if entry_created else 0))
            step_context["trade_events_count"] = len(trade_events)
            step_context["trade_events_processed"] = trade_events_processed
            step_context["execution_events_logged"] = execution_events_logged
            step_context["decision_events_logged"] = decision_events_logged
            step_context["entry_created"] = entry_created
            step_context["candle_update_ms"] = candle_update_ms
            step_context["overlays_update_ms"] = overlays_update_ms
            step_context["pending_signals_ops_ms"] = pending_signals_ops_ms
            step_context["execution_ms"] = execution_ms
            step_context["stats_update_ms"] = stats_update_ms
            step_context["persistence_ms"] = persistence_ms
            step_context["db_commit_ms"] = db_commit_ms
            step_context["delta_build_ms"] = delta_build_ms
            step_context["delta_serialize_ms"] = delta_serialize_ms
            step_context["stream_emit_ms"] = stream_emit_ms
            step_context["indicators_count"] = indicators_count
            step_context["overlays_changed_count"] = overlays_changed_count
            step_context["overlay_points_changed"] = overlay_points_changed
            step_context["signals_emitted_count"] = signals_emitted_count
            step_context["trades_touched_count"] = trades_touched_count
            step_context["subscribers_count"] = subscribers_count
            step_context["execution_decision_flow_ms"] = decision_flow_ms
            step_context["execution_prime_ms"] = execution_prime_ms
            step_context["execution_settlement_ms"] = settlement_ms
            step_context["execution_trade_event_processing_ms"] = event_processing_ms
            self._record_step_trace(
                "step_series_state",
                started_at=step_started,
                ended_at=datetime.now(timezone.utc),
                ok=True,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                context=step_context,
            )
        except Exception as exc:
            step_context["decision_events_logged"] = decision_events_logged
            step_context["execution_events_logged"] = execution_events_logged
            step_context["trade_events_processed"] = trade_events_processed
            step_context["entry_created"] = entry_created
            step_context["candle_update_ms"] = candle_update_ms
            step_context["overlays_update_ms"] = overlays_update_ms
            step_context["pending_signals_ops_ms"] = pending_signals_ops_ms
            step_context["execution_ms"] = execution_ms
            step_context["stats_update_ms"] = stats_update_ms
            step_context["persistence_ms"] = persistence_ms
            step_context["db_commit_ms"] = db_commit_ms
            step_context["delta_build_ms"] = delta_build_ms
            step_context["delta_serialize_ms"] = delta_serialize_ms
            step_context["stream_emit_ms"] = stream_emit_ms
            step_context["indicators_count"] = indicators_count
            step_context["overlays_changed_count"] = overlays_changed_count
            step_context["overlay_points_changed"] = overlay_points_changed
            step_context["signals_emitted_count"] = signals_emitted_count
            step_context["trades_touched_count"] = trades_touched_count
            step_context["subscribers_count"] = subscribers_count
            self._record_step_trace(
                "step_series_state",
                started_at=step_started,
                ended_at=datetime.now(timezone.utc),
                ok=False,
                strategy_id=strategy_id,
                symbol=symbol,
                timeframe=timeframe,
                error=str(exc),
                context=step_context,
            )
            raise
    
    def _next_signal_for(
        self,
        state: SeriesExecutionState,
        series: StrategySeries,
        candle: Candle,
        epoch: int,
    ) -> Tuple[List[Dict[str, object]], Optional[str], int, Dict[str, Optional[float]], int, int]:
        if epoch <= state.last_evaluated_epoch:
            consume_started = time.perf_counter()
            consumed, chosen, updated_last = consume_signals(
                state.pending_signals,
                epoch=epoch,
                last_consumed_epoch=state.last_consumed_epoch,
            )
            pending_consume_ms = max((time.perf_counter() - consume_started) * 1000.0, 0.0)
            return (
                consumed,
                chosen,
                len(state.pending_signals),
                {
                    "epochs_evaluated_this_tick": 0.0,
                    "pending_signals_append_ms": 0.0,
                    "pending_signals_consume_ms": pending_consume_ms,
                    "pending_signals_ops_ms": pending_consume_ms,
                    "signals_emitted_count": 0.0,
                    "overlays_update_ms": 0.0,
                    "indicator_state_update_ms": 0.0,
                    "signal_eval_ms": 0.0,
                    "overlay_projection_ms": 0.0,
                    "overlay_projection_skipped_count": 0.0,
                    "state_revisions_changed_count": 0.0,
                    "indicators_count": 0.0,
                    "overlays_changed_count": 0.0,
                    "overlay_points_changed": 0.0,
                },
                state.last_evaluated_epoch,
                updated_last,
            )

        indicator_started = time.perf_counter()
        snapshots: Dict[str, Any] = {}
        state_revisions_changed_count = 0
        previous_candle: Optional[Candle] = None
        if state.bar_index > 0 and state.bar_index - 1 < len(series.candles):
            previous_candle = series.candles[state.bar_index - 1]

        for indicator_id, runtime in state.indicator_state_runtime.items():
            indicator_type = str(runtime.get("indicator_type") or "")
            engine = runtime.get("engine")
            engine_state = runtime.get("engine_state")
            if engine is None or not isinstance(engine_state, MutableMapping):
                raise RuntimeError(f"indicator_state_runtime_invalid: indicator_id={indicator_id}")
            delta = engine.apply_bar(engine_state, candle)
            snapshot = engine.snapshot(engine_state)

            payload = dict(snapshot.payload)
            plugin = runtime.get("plugin")
            if plugin is None:
                raise RuntimeError(f"indicator_plugin_runtime_missing: indicator_id={indicator_id}")
            if getattr(plugin, "signal_emitter", None) is not None:
                rule_payload = plugin.signal_emitter(payload, candle, previous_candle)
            else:
                rule_payload = {"signals": []}
            enriched_payload = dict(payload)
            enriched_payload.update(dict(rule_payload or {}))
            snapshots[indicator_id] = type(snapshot)(
                revision=snapshot.revision,
                known_at=snapshot.known_at,
                formed_at=snapshot.formed_at,
                source_timeframe=snapshot.source_timeframe,
                payload=enriched_payload,
            )
            if bool(delta.changed):
                state_revisions_changed_count += 1

        indicator_state_update_ms = max((time.perf_counter() - indicator_started) * 1000.0, 0.0)

        signal_started = time.perf_counter()
        rules = (series.meta or {}).get("rules") or {}
        evaluated = evaluate_rules_from_state_snapshots(
            signal_input=SignalEvaluationInput(snapshots=snapshots),
            rules=rules,
            current_epoch=epoch,
            rule_evaluator=self._series_builder._evaluate_rule_payload,
        )
        signal_eval_ms = max((time.perf_counter() - signal_started) * 1000.0, 0.0)

        previous_overlay_count = float(len(series.overlays or []))
        previous_overlay_points = float(self._count_overlay_points(series.overlays or []))

        projection_started = time.perf_counter()
        overlay_projection_skipped_count = 0
        for indicator_id, snapshot in snapshots.items():
            runtime = state.indicator_state_runtime.get(indicator_id) or {}
            indicator_type = str(runtime.get("indicator_type") or "")
            plugin = runtime.get("plugin")
            projector = getattr(plugin, "overlay_projector", None) if plugin is not None else None
            if projector is None:
                overlay_projection_skipped_count += 1
                continue
            projection_state = state.indicator_projection_runtime.setdefault(indicator_id, {"seq": 0, "revision": -1, "entries": {}})
            projection_delta = project_overlay_delta(
                projection_input=OverlayProjectionInput(snapshot=snapshot, previous_projection_state=projection_state),
                entry_projector=projector,
            )
            if not projection_delta.ops:
                overlay_projection_skipped_count += 1
                continue
            raw_entries = projector(OverlayProjectionInput(snapshot=snapshot, previous_projection_state=projection_state))
            if not isinstance(raw_entries, Mapping):
                raise RuntimeError(
                    f"indicator_overlay_projection_invalid: indicator_type={indicator_type} indicator_id={indicator_id}"
                )
            normalized_entries: Dict[str, Dict[str, Any]] = {}
            indicator_meta = runtime.get("indicator_meta") if isinstance(runtime.get("indicator_meta"), Mapping) else {}
            overlay_color = indicator_meta.get("color") if isinstance(indicator_meta, Mapping) else None
            for entry_key, entry_value in raw_entries.items():
                if not isinstance(entry_value, Mapping):
                    continue
                normalized = normalize_overlays(indicator_type, [dict(entry_value)])
                if not normalized:
                    raise RuntimeError(
                        f"indicator_overlay_projection_normalize_failed: indicator_type={indicator_type} indicator_id={indicator_id} entry_key={entry_key}"
                    )
                overlay_entry = dict(normalized[0])
                overlay_entry.update(
                    {
                        "ind_id": indicator_id,
                        "source": "indicator_state",
                        "bot_id": self.bot_id,
                        "strategy_id": series.strategy_id,
                        "symbol": series.symbol,
                        "timeframe": series.timeframe,
                    }
                )
                if isinstance(overlay_color, str) and overlay_color.strip():
                    overlay_entry["color"] = overlay_color
                normalized_entries[str(entry_key)] = overlay_entry
            projection_state["seq"] = projection_delta.seq
            projection_state["revision"] = snapshot.revision
            projection_state["entries"] = normalized_entries

        overlay_projection_ms = max((time.perf_counter() - projection_started) * 1000.0, 0.0)
        overlays = self._series_overlay_entries(state)
        overlays_changed_count, overlay_points_changed = self._overlay_change_metrics(series.overlays or [], overlays)
        series.overlays = overlays

        append_started = time.perf_counter()
        for signal in evaluated:
            state.pending_signals.append(signal)
        pending_append_ms = max((time.perf_counter() - append_started) * 1000.0, 0.0)

        consume_started = time.perf_counter()
        consumed, chosen, updated_last = consume_signals(
            state.pending_signals,
            epoch=epoch,
            last_consumed_epoch=state.last_consumed_epoch,
        )
        pending_consume_ms = max((time.perf_counter() - consume_started) * 1000.0, 0.0)
        pending_ops_ms = pending_append_ms + pending_consume_ms

        eval_metrics = {
            "epochs_evaluated_this_tick": 1.0,
            "pending_signals_append_ms": pending_append_ms,
            "pending_signals_consume_ms": pending_consume_ms,
            "pending_signals_ops_ms": pending_ops_ms,
            "signals_emitted_count": float(len(evaluated)),
            "overlays_update_ms": overlay_projection_ms,
            "indicator_state_update_ms": indicator_state_update_ms,
            "signal_eval_ms": signal_eval_ms,
            "overlay_projection_ms": overlay_projection_ms,
            "overlay_projection_skipped_count": float(overlay_projection_skipped_count),
            "state_revisions_changed_count": float(state_revisions_changed_count),
            "indicators_count": float(len(state.indicator_state_runtime)),
            "overlays_changed_count": overlays_changed_count,
            "overlay_points_changed": overlay_points_changed,
            "overlay_count_before": previous_overlay_count,
            "overlay_count_after": float(len(overlays)),
            "overlay_points_before": previous_overlay_points,
            "overlay_points_after": float(self._count_overlay_points(overlays)),
        }

        next_last_evaluated = epoch
        return consumed, chosen, len(state.pending_signals), eval_metrics, next_last_evaluated, updated_last

    def _record_signal_consumption(
        self,
        state: SeriesExecutionState,
        epoch: int,
        consumed_signals: List[Dict[str, object]],
        chosen_direction: Optional[str],
    ) -> None:
        state.signal_consumptions.append(
            SignalConsumption(
                epoch=epoch,
                consumed_signals=list(consumed_signals),
                chosen_direction=chosen_direction,
            )
        )

    def _compute_playback_interval(self, base_seconds: float = 1.0) -> float:
        return 0.0

    def _has_open_trades(self) -> bool:
        for series in self._series or []:
            if getattr(series, "risk_engine", None) is None:
                continue
            if getattr(series.risk_engine, "active_trade", None) is not None:
                return True
        return False

    def _pace(self, interval: float, update_next_bar: bool = False) -> None:
        if interval <= 0:
            if update_next_bar:
                self._next_bar_at = None
                with self._lock:
                    self.state.update({"next_bar_at": None, "next_bar_in_seconds": None})
            return
        if update_next_bar:
            self._next_bar_at = datetime.now(timezone.utc) + timedelta(seconds=interval)
            with self._lock:
                self.state.update(
                    {
                        "next_bar_at": _isoformat(self._next_bar_at),
                        "next_bar_in_seconds": self._seconds_until_next_bar(),
                    }
                )
        target = time.time() + interval
        while not self._stop.is_set():
            if not self._pause_event.wait(timeout=0.2):
                continue
            remaining = target - time.time()
            if remaining <= 0:
                break
            time.sleep(min(0.25, remaining))

    def _append_live_candles_if_needed(self) -> bool:
        updated = False
        end_iso = _isoformat(datetime.now(timezone.utc))
        with self._series_update_lock:
            for series in self._series:
                last_time = series.candles[-1].time if series.candles else None
                if last_time is None:
                    continue
                start_iso = _isoformat(last_time + timedelta(seconds=1))
                if self._append_series_updates(series, start_iso, end_iso):
                    updated = True
        if updated:
            with self._series_update_lock:
                # Recalculate total bars as max across all series
                self._total_bars = max(len(s.candles) for s in self._series) if self._series else 0
                for state in self._series_states:
                    state.total_bars = len(state.series.candles)
                    if state.done and state.bar_index < state.total_bars:
                        state.done = False
            self._rebuild_overlay_cache()
            self._log_event("live_refresh", message="Appended live candles")
            self._push_update("live_refresh")
        return updated

    def _append_live_candles_for_state(self, state: SeriesExecutionState) -> bool:
        end_iso = _isoformat(datetime.now(timezone.utc))
        series = state.series
        last_time = series.candles[-1].time if series.candles else None
        if last_time is None:
            return False
        start_iso = _isoformat(last_time + timedelta(seconds=1))
        with self._series_update_lock:
            updated = self._append_series_updates(series, start_iso, end_iso)
            if not updated:
                return False
            state.total_bars = len(series.candles)
            if state.done and state.bar_index < state.total_bars:
                state.done = False
            self._total_bars = max(len(s.candles) for s in self._series) if self._series else 0
        self._rebuild_overlay_cache()
        self._log_event("live_refresh", message="Appended live candles")
        self._push_update("live_refresh")
        return True

    def _append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
        return self._series_builder.append_series_updates(series, start_iso, end_iso)

    def pause(self) -> None:
        self._ensure_prepared()
        self._paused = True
        self._pause_event.clear()
        self._next_bar_at = None
        with self._lock:
            self.state.update({"status": "paused", "paused": True, "next_bar_at": None, "next_bar_in_seconds": None})
        self._log_event("pause", message="Bot paused")
        self._push_update("pause")

    def resume(self) -> None:
        self._ensure_prepared()
        self._paused = False
        self._pause_event.set()
        with self._lock:
            if self.state.get("status") == "paused":
                self.state.update({"status": "running", "paused": False})
        self._log_event("resume", message="Bot resumed")
        self._push_update("resume")

    def stop(self) -> None:
        self._stop.set()
        self._pause_event.set()
        if self._runner is not None:
            self._runner.stop()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.2)
        with self._lock:
            self.state.update({"status": "stopped", "paused": False})
        self._next_bar_at = None
        self._log_event("stop", message="Bot stopped")
        self._push_update("stop")

    def _aggregate_trades(self) -> List[Dict[str, Any]]:
        trades: List[Dict[str, Any]] = []
        for series in self._series:
            for entry in series.risk_engine.serialise_trades():
                payload = dict(entry)
                payload.setdefault("strategy_id", series.strategy_id)
                payload.setdefault("symbol", series.symbol)
                trades.append(payload)
        return trades

    def _aggregate_stats(self) -> Dict[str, float]:
        summary = {
            "total_trades": 0,
            "completed_trades": 0,
            "legs_closed": 0,
            "wins": 0,
            "losses": 0,
            "breakeven_trades": 0,
            "long_trades": 0,
            "short_trades": 0,
        }
        gross = 0.0
        fees = 0.0
        net = 0.0
        currency: Optional[str] = None
        multi_currency = False
        win_pnls: List[float] = []
        loss_pnls: List[float] = []
        tolerance = 1e-8

        for series in self._series:
            stats = series.risk_engine.stats()
            for key in summary:
                try:
                    summary[key] += int(stats.get(key, 0) or 0)
                except (TypeError, ValueError):
                    continue
            gross += float(stats.get("gross_pnl", 0.0) or 0.0)
            fees += float(stats.get("fees_paid", 0.0) or 0.0)
            net += float(stats.get("net_pnl", 0.0) or 0.0)
            series_currency = stats.get("quote_currency")
            if isinstance(series_currency, str) and series_currency:
                if currency is None:
                    currency = series_currency
                elif currency != series_currency:
                    multi_currency = True

            # Collect individual trade PnLs for avg/largest calculations
            for trade in series.risk_engine.trades:
                if trade.is_active():
                    continue
                pnl = trade.net_pnl
                if pnl > tolerance:
                    win_pnls.append(pnl)
                elif pnl < -tolerance:
                    loss_pnls.append(pnl)

        total = summary.get("completed_trades") or (summary["wins"] + summary["losses"])
        summary["win_rate"] = round(summary["wins"] / total, 4) if total else 0.0
        summary["gross_pnl"] = round(gross, 4)
        summary["fees_paid"] = round(fees, 4)
        summary["net_pnl"] = round(net, 4)
        summary["total_fees"] = round(fees, 4)  # Alias for frontend compatibility

        # Avg win/loss
        summary["avg_win"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
        summary["avg_loss"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0

        # Largest win/loss
        summary["largest_win"] = round(max(win_pnls), 4) if win_pnls else 0.0
        summary["largest_loss"] = round(min(loss_pnls), 4) if loss_pnls else 0.0

        # Max drawdown
        summary["max_drawdown"] = self._max_drawdown_from_trades()

        if multi_currency:
            summary["quote_currency"] = "MULTI"
        elif currency:
            summary["quote_currency"] = currency
        return summary

    def _max_drawdown_from_trades(self) -> float:
        trades = self._aggregate_trades()
        closed = []
        for trade in trades:
            closed_at = trade.get("closed_at")
            net_pnl = trade.get("net_pnl")
            if not closed_at or net_pnl is None:
                continue
            try:
                timestamp = datetime.fromisoformat(str(closed_at))
            except ValueError:
                continue
            closed.append((timestamp, float(net_pnl)))
        if not closed:
            return 0.0
        closed.sort(key=lambda item: item[0])
        equity = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for _, pnl in closed:
            equity += pnl
            if equity > peak:
                peak = equity
            drawdown = peak - equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        return round(max_drawdown, 4)

    def _log_event(
        self,
        event: str,
        series: Optional[StrategySeries] = None,
        candle: Optional[Candle] = None,
        **fields: object,
    ) -> None:
        created_at = _isoformat(datetime.now(timezone.utc))
        entry: Dict[str, object] = {
            "id": str(uuid.uuid4()),
            "event": event,
            "timestamp": created_at,
            "created_at": created_at,
        }
        if series is not None:
            entry["strategy_id"] = series.strategy_id
            entry["symbol"] = series.symbol
        if candle is not None:
            entry["bar_time"] = _isoformat(candle.time)
            entry["chart_time"] = entry["bar_time"]
            entry.setdefault("price", round(candle.close, 4))
        for key, value in fields.items():
            if value is not None:
                entry[key] = value
        with self._lock:
            sinks = list(self._event_sinks)
        for sink in sinks:
            sink.record_log(entry)

    def _record_runtime_warning(self, warning: Optional[Mapping[str, object]]) -> None:
        """Capture runtime warnings for UI consumption."""

        if not warning:
            return
        entry: Dict[str, object] = dict(warning)
        entry.setdefault("level", "warning")
        entry.setdefault("type", entry.get("type") or "runtime_warning")
        entry.setdefault("message", entry.get("message") or "Runtime warning")
        entry.setdefault("source", entry.get("source") or "runtime")
        entry.setdefault("bot_id", self.bot_id)
        entry.setdefault("bot_mode", self.run_type)
        entry.setdefault("timestamp", _isoformat(datetime.now(timezone.utc)))
        entry.setdefault("id", str(uuid.uuid4()))
        context = dict(entry.get("context") or {})
        entry["context"] = context
        with self._lock:
            self._warnings.append(entry)
            self.state["warnings"] = list(self._warnings)

    def warnings(self) -> List[Dict[str, object]]:
        """Return the current runtime warnings."""

        with self._lock:
            return list(self._warnings)

    def _signal_key(
        self,
        series: StrategySeries,
        signal_type: str,
        signal_direction: Optional[str],
        rule_id: Optional[str],
    ) -> Tuple[str, str, Optional[str], Optional[str]]:
        return (series.strategy_id, series.symbol, signal_type, signal_direction or rule_id)

    def _record_ledger_event(
        self,
        *,
        event_type: str,
        event_ts: str,
        reason_code: str,
        series: Optional[StrategySeries] = None,
        event_subtype: Optional[str] = None,
        parent_event_id: Optional[str] = None,
        trade_id: Optional[str] = None,
        position_id: Optional[str] = None,
        side: Optional[str] = None,
        qty: Optional[float] = None,
        price: Optional[float] = None,
        event_impact_pnl: Optional[float] = None,
        trade_net_pnl: Optional[float] = None,
        reason_detail: Optional[str] = None,
        evidence_refs: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, Any]] = None,
        alternatives_rejected: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        if not reason_code:
            raise ValueError("reason_code is required for ledger events")
        instrument_id = None
        if series is not None and isinstance(series.instrument, Mapping):
            instrument_id = series.instrument.get("id")
        event_id = str(uuid.uuid4())
        created_at = _isoformat(datetime.now(timezone.utc))
        ledger_event = DecisionLedgerEvent(
            event_id=event_id,
            event_ts=event_ts,
            event_type=event_type,
            reason_code=reason_code,
            event_subtype=event_subtype,
            parent_event_id=parent_event_id,
            trade_id=trade_id,
            position_id=position_id,
            strategy_id=series.strategy_id if series else None,
            strategy_name=series.name or series.strategy_id if series else None,
            symbol=series.symbol if series else None,
            instrument_id=instrument_id,
            timeframe=getattr(series, "timeframe", None) if series else None,
            side=side,
            qty=qty,
            price=price,
            event_impact_pnl=event_impact_pnl,
            trade_net_pnl=trade_net_pnl,
            reason_detail=reason_detail,
            evidence_refs=evidence_refs,
            context=context,
            alternatives_rejected=alternatives_rejected,
            created_at=created_at,
        )
        payload = ledger_event.serialize()
        with self._lock:
            sinks = list(self._event_sinks)
            if self._run_context is not None:
                self._run_context.decision_trace.append(payload)
        for sink in sinks:
            sink.record_decision(payload)
        return event_id

    def _log_decision_event(
        self,
        event: str,
        series: StrategySeries,
        candle: Candle,
        signal_type: str,
        signal_direction: Optional[str] = None,
        signal_price: Optional[float] = None,
        rule_id: Optional[str] = None,
        decision: Optional[str] = None,
        reason_code: str = "",
        reason_detail: Optional[str] = None,
        trade_id: Optional[str] = None,
        trade_time: Optional[str] = None,
        conditions: Optional[List[Dict[str, Any]]] = None,
        evidence_refs: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, Any]] = None,
        alternatives_rejected: Optional[List[Dict[str, Any]]] = None,
        **metadata: object,
    ) -> None:
        """Log a strategy-level decision event for the decision ledger."""
        if not reason_code:
            raise ValueError("reason_code is required for decision events")
        if trade_id is not None and "trade_id" in metadata:
            metadata = {k: v for k, v in metadata.items() if k != "trade_id"}
        parent_event_id = self._signal_event_ids.get(
            self._signal_key(series, signal_type, signal_direction, rule_id),
        )
        context_payload = dict(metadata) if metadata else None
        if conditions:
            context_payload = dict(context_payload or {})
            context_payload["conditions"] = conditions
        derived_evidence: List[Dict[str, Any]] = []
        if rule_id:
            derived_evidence.append(
                {
                    "ref_type": "indicator",
                    "ref_id": rule_id,
                    "summary": "rule_id",
                }
            )
        if conditions:
            for cond in conditions:
                if not isinstance(cond, Mapping):
                    continue
                name = cond.get("name") or cond.get("condition")
                if not name:
                    continue
                derived_evidence.append(
                    {
                        "ref_type": "indicator",
                        "ref_id": str(name),
                        "summary": f"condition_passed={cond.get('passed')}",
                    }
                )
        if reason_code.startswith("DECISION_REJECTED"):
            derived_evidence.append(
                {
                    "ref_type": "risk",
                    "ref_id": reason_code,
                    "summary": reason_detail or "decision rejected",
                }
            )
        event_id = self._record_ledger_event(
            event_type="decision",
            event_subtype=event,
            event_ts=_isoformat(candle.time),
            reason_code=reason_code,
            series=series,
            parent_event_id=parent_event_id,
            trade_id=trade_id,
            side=signal_direction,
            price=signal_price or candle.close,
            reason_detail=reason_detail,
            evidence_refs=evidence_refs or (derived_evidence or None),
            context=context or context_payload,
            alternatives_rejected=alternatives_rejected,
        )
        if decision == "accepted" and trade_id:
            self._decision_event_ids[trade_id] = event_id

    @staticmethod
    def _normalise_rejection_metadata(
        rejection_meta: Optional[Mapping[str, Any]],
        blocking_trade_id: Optional[str],
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        resolved_trade_id = blocking_trade_id
        metadata_payload: Dict[str, Any] = {}
        if not isinstance(rejection_meta, Mapping):
            return resolved_trade_id, metadata_payload
        metadata_payload = {
            k: v
            for k, v in rejection_meta.items()
            if k not in {"reason", "trade_id"}
        }
        if resolved_trade_id is None:
            meta_trade_id = rejection_meta.get("trade_id")
            if meta_trade_id is not None:
                resolved_trade_id = str(meta_trade_id)
        return resolved_trade_id, metadata_payload

    def _record_execution_ledger_event(
        self,
        series: StrategySeries,
        *,
        event_subtype: str,
        event_ts: str,
        trade_id: Optional[str],
        side: Optional[str] = None,
        qty: Optional[float] = None,
        price: Optional[float] = None,
        event_impact_pnl: Optional[float] = None,
        trade_net_pnl: Optional[float] = None,
        evidence_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not trade_id:
            return
        reason_code_map = {
            "entry": "EXEC_ENTRY",
            "open": "EXEC_ENTRY",
            "fill": "EXEC_ENTRY",
            "stop": "EXEC_STOP_HIT",
            "target": "EXEC_TARGET_HIT",
            "close": "OUTCOME_TRADE_CLOSED",
            "exit": "OUTCOME_TRADE_CLOSED",
        }
        reason_code = reason_code_map.get(event_subtype)
        if not reason_code:
            raise ValueError(f"Unknown execution event subtype '{event_subtype}'")
        evidence_parts: List[str] = []
        if evidence_details:
            for key in ("leg", "ticks", "pnl", "gross_pnl", "fees_paid", "net_pnl", "currency", "stop_price"):
                if key in evidence_details and evidence_details[key] is not None:
                    evidence_parts.append(f"{key}={evidence_details[key]}")
            targets = evidence_details.get("targets") if isinstance(evidence_details, Mapping) else None
            if isinstance(targets, list) and targets:
                evidence_parts.append(f"targets={len(targets)}")
            metrics = evidence_details.get("metrics") if isinstance(evidence_details, Mapping) else None
            if isinstance(metrics, Mapping) and "bars_held" in metrics:
                evidence_parts.append(f"bars_held={metrics['bars_held']}")
        summary = " ".join(evidence_parts) if evidence_parts else "execution_event"
        evidence_refs = [
            {
                "ref_type": "execution",
                "ref_id": event_subtype,
                "summary": summary,
            }
        ]
        parent_event_id = self._entry_event_ids.get(trade_id) or self._decision_event_ids.get(trade_id)
        ledger_type = "outcome" if event_subtype in {"close", "exit"} else "execution"
        event_id = self._record_ledger_event(
            event_type=ledger_type,
            event_subtype=event_subtype,
            event_ts=event_ts,
            reason_code=reason_code,
            series=series,
            parent_event_id=parent_event_id,
            trade_id=trade_id,
            side=side,
            qty=qty,
            price=price,
            event_impact_pnl=event_impact_pnl,
            trade_net_pnl=trade_net_pnl,
            evidence_refs=evidence_refs,
        )
        if event_subtype in {"entry", "open", "fill"}:
            self._entry_event_ids[trade_id] = event_id

    def _build_run_context(self) -> RunContext:
        wallet_config = self.config.get("wallet_config")
        if not isinstance(wallet_config, dict):
            raise ValueError("wallet_config is required to start a bot run")
        balances = wallet_config.get("balances")
        if not isinstance(balances, dict) or not balances:
            raise ValueError("wallet_config.balances is required to start a bot run")
        logger.info(with_log_context("bot_runtime_run_context_wallet_init", self._runtime_log_context()))
        run_context = RunContext(bot_id=self.bot_id)
        run_context.wallet_ledger = LockedWalletLedger()
        logger.info(
            with_log_context(
                "bot_runtime_run_context_wallet_deposit",
                self._runtime_log_context(balance_currencies=list(balances.keys())),
            )
        )
        run_context.wallet_ledger.deposit(balances)
        logger.info(with_log_context("bot_runtime_run_context_wallet_deposit_done", self._runtime_log_context()))
        logger.info(
            with_log_context(
                "bot_runtime_run_context_attach_wallet_start",
                self._runtime_log_context(series=len(self._series)),
            )
        )
        for series in self._series:
            series.risk_engine.attach_wallet(run_context.wallet_ledger)
        logger.info(with_log_context("bot_runtime_run_context_attach_wallet_done", self._runtime_log_context()))
        return run_context

    def _persist_run_artifact(self, status: str) -> None:
        if self._run_context is None:
            return
        from ....storage import storage
        from ....reports import report_service

        artifact = self._run_artifact_payload(status)
        storage.update_bot_run_artifact(self.bot_id, artifact)
        report_service.record_run_report(
            bot_id=self.bot_id,
            run_id=artifact.get("run_id"),
            status=status,
            started_at=artifact.get("started_at"),
            ended_at=artifact.get("ended_at"),
            config=self.config,
            series=list(self._series),
            decision_ledger=list(artifact.get("decision_trace") or []),
        )

    def _run_artifact_payload(self, status: str) -> Dict[str, Any]:
        if self._run_context is None:
            raise ValueError("Run context is required to build artifact payload")
        self._run_context.status = status
        self._run_context.ended_at = _isoformat(datetime.now(timezone.utc))
        wallet_state = project_wallet(self._run_context.wallet_ledger.events())
        return {
            "run_id": self._run_context.run_id,
            "bot_id": self.bot_id,
            "started_at": self._run_context.started_at,
            "ended_at": self._run_context.ended_at,
            "status": status,
            "wallet_start": dict(self.config.get("wallet_config") or {}),
            "wallet_end": {"balances": wallet_state.balances},
            "wallet_ledger": [event.__dict__ for event in self._run_context.wallet_ledger.events()],
            "decision_trace": list(self._run_context.decision_trace),
        }

    def logs(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return up to *limit* recent log entries."""

        with self._lock:
            entries = list(self._logs)
        if limit and limit > 0:
            entries = entries[-limit:]
        return entries

    def decision_events(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return up to *limit* recent decision ledger events."""

        with self._lock:
            entries = list(self._decision_events)
        if limit and limit > 0:
            entries = entries[-limit:]
        return entries

    def _persist_trade_entry(self, series: StrategySeries, trade: LadderPosition) -> None:
        if not series or not trade:
            return
        run_id = self._run_context.run_id if self._run_context else None
        contracts = sum(max(leg.contracts, 0) for leg in trade.legs)
        timeframe_label = series.timeframe
        timeframe_seconds = _timeframe_to_seconds(timeframe_label)
        instrument_id = (series.instrument or {}).get("id") if isinstance(series.instrument, dict) else None
        entry_context = derive_entry_context(
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
            entry_time=trade.entry_time,
            stats_version=STATS_VERSION,
            regime_version=REGIME_VERSION,
        )
        metrics = dict(trade._metrics_snapshot())
        metrics.update(build_entry_metrics(entry_context))
        self._persistence_buffer.record_trade_entry(
            {
                "trade_id": trade.trade_id,
                "run_id": run_id,
                "bot_id": self.bot_id,
                "strategy_id": series.strategy_id,
                "symbol": series.symbol,
                "direction": trade.direction,
                "entry_time": trade.entry_time,
                "entry_price": trade.entry_price,
                "stop_price": trade.stop_price,
                "contracts": contracts,
                "status": "open",
                "quote_currency": trade.quote_currency,
                "metrics": metrics,
                "instrument_id": instrument_id,
                "timeframe": timeframe_label,
                "timeframe_seconds": timeframe_seconds,
            }
        )

    def _persist_trade_event(self, series: StrategySeries, event: Dict[str, Any]) -> None:
        trade_id = event.get("trade_id")
        if not trade_id:
            return
        run_id = self._run_context.run_id if self._run_context else None
        payload = {
            "id": event.get("id"),
            "trade_id": trade_id,
            "run_id": run_id,
            "bot_id": self.bot_id,
            "strategy_id": getattr(series, "strategy_id", None),
            "symbol": getattr(series, "symbol", None),
            "event_type": event.get("type"),
            "leg": event.get("leg"),
            "contracts": event.get("contracts"),
            "price": event.get("price"),
            "ticks": event.get("ticks"),
            "pnl": event.get("pnl"),
            "quote_currency": event.get("currency"),
            "event_time": event.get("event_time") or event.get("time"),
        }
        event_type = event.get("type")
        self._persistence_buffer.record_trade_event(payload, event_type=event_type)
        if event_type == "close":
            self._persistence_buffer.record_trade_entry(
                {
                    "trade_id": trade_id,
                    "run_id": run_id,
                    "bot_id": self.bot_id,
                    "strategy_id": getattr(series, "strategy_id", None),
                    "symbol": getattr(series, "symbol", None),
                    "direction": event.get("direction"),
                    "status": "closed",
                    "exit_time": event.get("time"),
                    "gross_pnl": event.get("gross_pnl"),
                    "fees_paid": event.get("fees_paid"),
                    "net_pnl": event.get("net_pnl"),
                    "quote_currency": event.get("currency"),
                    "metrics": event.get("metrics"),
                    "instrument_id": (series.instrument or {}).get("id") if isinstance(series.instrument, dict) else None,
                    "timeframe": series.timeframe,
                    "timeframe_seconds": _timeframe_to_seconds(series.timeframe),
                }
            )

    def _persist_runtime_state(self, status: str) -> None:
        """Send completion metadata back to the service layer for persistence."""

        if not self._state_callback:
            return
        payload = {
            "status": status,
            "last_stats": dict(self._last_stats or {}),
            "last_run_at": _isoformat(datetime.now(timezone.utc)),
        }
        try:
            self._state_callback(payload)
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(status=status, error=str(exc))
            logger.warning(with_log_context("bot_runtime_state_callback_failed", context))

    def _flush_persistence_buffer(self, reason: str) -> None:
        flush_started = datetime.now(timezone.utc)
        try:
            self._persistence_buffer.flush(reason=reason)
            self._record_step_trace(
                "persistence_flush",
                started_at=flush_started,
                ended_at=datetime.now(timezone.utc),
                ok=True,
                context={"reason": reason},
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(reason=reason, error=str(exc))
            logger.warning(with_log_context("bot_runtime_persistence_flush_failed", context))
            self._record_step_trace(
                "persistence_flush",
                started_at=flush_started,
                ended_at=datetime.now(timezone.utc),
                ok=False,
                error=str(exc),
                context={"reason": reason},
            )

    def _record_step_trace(
        self,
        step_name: str,
        *,
        started_at: datetime,
        ended_at: datetime,
        ok: bool,
        strategy_id: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        error: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[float]:
        run_id = self._run_context.run_id if self._run_context else None
        if not run_id:
            return None
        duration_ms = max((ended_at - started_at).total_seconds() * 1000.0, 0.0)
        try:
            from ....storage import storage

            persist_started = time.perf_counter()
            storage.record_bot_run_step(
                {
                    "run_id": run_id,
                    "bot_id": self.bot_id,
                    "step_name": step_name,
                    "started_at": _isoformat(started_at),
                    "ended_at": _isoformat(ended_at),
                    "duration_ms": duration_ms,
                    "ok": ok,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "error": error,
                    "context": dict(context or {}),
                }
            )
            return max((time.perf_counter() - persist_started) * 1000.0, 0.0)
        except Exception as exc:  # pragma: no cover - defensive logging
            step_context = self._runtime_log_context(step=step_name, run_id=run_id, error=str(exc))
            logger.warning(with_log_context("bot_runtime_step_trace_persist_failed", step_context))
            return None

    def _update_state(self, candle: Candle, status: str = "running") -> Dict[str, Any]:
        update_started = time.perf_counter()
        stats_started = time.perf_counter()
        stats = self._aggregate_stats()
        stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
        with self._lock:
            self._last_stats = stats
        self._refresh_next_bar_at()
        progress = self._compute_progress()
        snapshot = {
            "status": status,
            "progress": progress,
            "last_bar": candle.to_dict(),
            "stats": stats,
            "paused": self._paused,
            "next_bar_at": _isoformat(self._next_bar_at),
            "next_bar_in_seconds": self._seconds_until_next_bar(),
        }
        with self._lock:
            self.state.update(snapshot)
        if self._state_callback:
            try:
                self._state_callback({"runtime": self.snapshot()})
            except Exception as exc:  # pragma: no cover - defensive logging
                context = self._runtime_log_context(error=str(exc))
                logger.warning(with_log_context("bot_runtime_stream_callback_failed", context), exc_info=exc)
        return {
            "stats_update_ms": stats_update_ms,
            "update_state_total_ms": max((time.perf_counter() - update_started) * 1000.0, 0.0),
            "stats": dict(stats),
        }

    def _seconds_until_next_bar(self) -> Optional[float]:
        if not self._next_bar_at:
            return None
        delta = (self._next_bar_at - datetime.now(timezone.utc)).total_seconds()
        return round(delta, 2) if delta > 0 else 0.0

    def _state_payload(self) -> Dict[str, object]:
        self._refresh_next_bar_at()
        with self._lock:
            payload = dict(self.state)
        payload.setdefault("bot_id", self.bot_id)
        if self._run_context is not None:
            payload.setdefault("run_id", self._run_context.run_id)
        payload.setdefault("stats", self._last_stats)
        if "next_bar_at" not in payload:
            payload["next_bar_at"] = _isoformat(self._next_bar_at)
        if "next_bar_in_seconds" not in payload:
            payload["next_bar_in_seconds"] = self._seconds_until_next_bar()
        if "started_at" not in payload and self._run_started_at is not None:
            payload["started_at"] = _isoformat(self._run_started_at)
        payload.setdefault("warnings", self.warnings())
        payload["bootstrap"] = self._bootstrap_status_payload()
        return payload

    def _bootstrap_status_payload(self) -> Dict[str, Any]:
        with self._series_update_lock:
            per_series: List[Dict[str, Any]] = []
            for state in self._series_states:
                series = state.series
                indicator_links = list((series.meta or {}).get("indicator_links") or [])
                per_series.append(
                    {
                        "strategy_id": series.strategy_id,
                        "symbol": series.symbol,
                        "timeframe": series.timeframe,
                        "replay_start_index": int(getattr(series, "replay_start_index", 0) or 0),
                        "bootstrap_completed": bool(getattr(series, "bootstrap_completed", False)),
                        "bootstrap_total_overlays": int(getattr(series, "bootstrap_total_overlays", 0) or 0),
                        "bootstrap_indicator_overlays": int(getattr(series, "bootstrap_indicator_overlays", 0) or 0),
                        "expected_indicators": len(indicator_links),
                    }
                )
        failed = [entry for entry in per_series if not entry.get("bootstrap_completed")]
        status = "failed" if failed else ("ready" if per_series else "idle")
        details = self._prepare_error if isinstance(self._prepare_error, Mapping) else {}
        failure_details = details.get("failures") if isinstance(details, Mapping) else None
        return {
            "status": status,
            "series": per_series,
            "failed_count": len(failed),
            "failure_details": failure_details if isinstance(failure_details, list) else [],
        }

    def snapshot(self) -> Dict[str, object]:
        """Return a thread-safe snapshot of runtime state."""

        if self.state.get("status") != "error":
            self._ensure_prepared()
        return self._state_payload()

    def chart_payload(self) -> Dict[str, object]:
        """Return the latest candle, trade, overlay, and stat data for the lens."""

        self._ensure_prepared()
        payload = self._chart_state()
        payload["warnings"] = self.warnings()
        payload["bot_id"] = self.bot_id
        payload["run_id"] = self._run_context.run_id if self._run_context is not None else None
        payload["runtime"] = self.snapshot()
        overlays = payload.get("overlays")
        overlay_summary = self._overlay_summary(overlays if isinstance(overlays, list) else [])
        series_entries = payload.get("series")
        series_overlay_counts: List[Dict[str, Any]] = []
        if isinstance(series_entries, list):
            for entry in series_entries:
                if not isinstance(entry, Mapping):
                    continue
                series_overlays = entry.get("overlays")
                series_overlay_counts.append(
                    {
                        "strategy_id": entry.get("strategy_id"),
                        "symbol": entry.get("symbol"),
                        "timeframe": entry.get("timeframe"),
                        "overlays": len(series_overlays) if isinstance(series_overlays, list) else 0,
                    }
                )
        logger.info(
            with_log_context(
                "bot_overlay_snapshot_sent",
                self._runtime_log_context(
                    overlays=overlay_summary.get("total_overlays"),
                    overlay_types=overlay_summary.get("type_counts"),
                    overlay_payloads=overlay_summary.get("payload_counts"),
                    overlay_profile_params=overlay_summary.get("profile_params_samples"),
                    series_overlay_counts=series_overlay_counts,
                ),
            )
        )
        return payload

    def regime_overlay_dump(self) -> Dict[str, Any]:
        """Return raw and visible regime overlays for debugging (no trimming on raw)."""

        self._ensure_prepared()
        # Ensure overlay cache is current.
        self._aggregate_overlays_to_cache()
        raw_overlays = [
            ov
            for ov in self._chart_overlays or []
            if isinstance(ov, Mapping) and str(ov.get("type") or "").lower() in {"regime_overlay", "regime_markers"}
        ]

        current_candle = self._primary_state_candle()
        current_epoch = int(current_candle.time.timestamp()) if current_candle else None
        status = str(self.state.get("status") or "").lower()
        visible = self._chart_state_builder.visible_overlays(raw_overlays, status, current_epoch)

        def _start_end(overlay: Mapping[str, Any]) -> Tuple[Optional[int], Optional[int]]:
            payload = overlay.get("payload") if isinstance(overlay, Mapping) else {}
            boxes = payload.get("boxes") if isinstance(payload, Mapping) else None
            if isinstance(boxes, list) and boxes:
                starts = [b.get("x1") or b.get("start") for b in boxes if isinstance(b, Mapping)]
                ends = [b.get("x2") or b.get("end") for b in boxes if isinstance(b, Mapping)]
                starts = [s for s in starts if isinstance(s, (int, float))]
                ends = [e for e in ends if isinstance(e, (int, float))]
                return (int(min(starts)) if starts else None, int(max(ends)) if ends else None)
            segments = payload.get("segments") if isinstance(payload, Mapping) else None
            if isinstance(segments, list) and segments:
                starts = [s.get("x1") for s in segments if isinstance(s, Mapping)]
                ends = [s.get("x2") for s in segments if isinstance(s, Mapping)]
                starts = [s for s in starts if isinstance(s, (int, float))]
                ends = [e for e in ends if isinstance(e, (int, float))]
                return (int(min(starts)) if starts else None, int(max(ends)) if ends else None)
            return (None, None)

        def _with_meta(overlay: Mapping[str, Any]) -> Dict[str, Any]:
            start_epoch, end_epoch = _start_end(overlay)
            return {
                "type": overlay.get("type"),
                "instrument_id": overlay.get("instrument_id"),
                "symbol": overlay.get("symbol"),
                "timeframe": overlay.get("timeframe"),
                "strategy_id": overlay.get("strategy_id"),
                "start_time": _isoformat(datetime.fromtimestamp(start_epoch, tz=timezone.utc)) if start_epoch else None,
                "end_time": _isoformat(datetime.fromtimestamp(end_epoch, tz=timezone.utc)) if end_epoch else None,
                "payload": overlay.get("payload"),
            }

        return {
            "current_epoch": current_epoch,
            "raw": [_with_meta(ov) for ov in raw_overlays],
            "visible": [_with_meta(ov) for ov in visible],
        }

    def subscribe(self) -> Tuple[str, Queue]:
        """Register a streaming subscriber and return its token/queue."""

        self._ensure_prepared()
        channel: Queue = Queue(maxsize=256)
        token = str(uuid.uuid4())
        with self._lock:
            self._subscribers[token] = channel
        return token, channel

    def unsubscribe(self, token: str) -> None:
        """Remove a streaming subscriber and drain its queue."""

        with self._lock:
            channel = self._subscribers.pop(token, None)
        if not channel:
            return
        try:
            while True:
                channel.get_nowait()
        except Empty:
            pass

    def _broadcast(self, event: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, int]:
        message = dict(payload or {})
        message.setdefault("type", event)
        with self._lock:
            channels = list(self._subscribers.values())
        dropped_messages = 0
        for channel in channels:
            try:
                channel.put_nowait(message)
            except Full:
                try:
                    channel.get_nowait()
                except Empty:
                    pass
                try:
                    channel.put_nowait(message)
                except Full:
                    dropped_messages += 1
                    continue
        return len(channels), dropped_messages

    @staticmethod
    def _overlay_points_for_payload(payload: Mapping[str, Any]) -> int:
        points = 0
        for key in (
            "price_lines",
            "markers",
            "touchPoints",
            "touch_points",
            "boxes",
            "segments",
            "polylines",
            "bubbles",
            "regime_blocks",
        ):
            entries = payload.get(key)
            if isinstance(entries, list):
                points += len(entries)
        return points

    @staticmethod
    def _entry_fingerprint(entries: Sequence[Mapping[str, Any]]) -> Tuple[int, Optional[str], Optional[str]]:
        if not entries:
            return (0, None, None)
        last = entries[-1]
        marker: Optional[str] = None
        kind: Optional[str] = None
        if isinstance(last, Mapping):
            kind_value = last.get("type")
            kind = str(kind_value) if kind_value is not None else None
            for key in ("id", "event_id", "trade_id", "time", "created_at", "timestamp", "message"):
                value = last.get(key)
                if value is not None:
                    marker = str(value)
                    break
        return (len(entries), kind, marker)

    @staticmethod
    def _trade_revision(series: StrategySeries) -> Tuple[Any, ...]:
        engine = getattr(series, "risk_engine", None)
        trades = list(getattr(engine, "trades", []) or [])
        if not trades:
            return (0, None, None, None, None, None, None)
        last = trades[-1]
        legs = list(getattr(last, "legs", []) or [])
        open_legs = sum(1 for leg in legs if str(getattr(leg, "status", "")) == "open")
        active_trade = getattr(engine, "active_trade", None)
        last_closed_at = _isoformat(getattr(last, "closed_at", None))
        last_net_pnl = round(float(getattr(last, "net_pnl", 0.0) or 0.0), 4)
        return (
            len(trades),
            str(getattr(last, "trade_id", "") or ""),
            last_closed_at,
            int(getattr(last, "bars_held", 0) or 0),
            open_legs,
            last_net_pnl,
            str(getattr(active_trade, "trade_id", "") or ""),
        )

    @staticmethod
    def _overlay_cache_key(overlay: Mapping[str, Any], ordinal: int) -> str:
        if not isinstance(overlay, Mapping):
            return f"overlay:{ordinal}"
        explicit = overlay.get("id")
        if explicit:
            return str(explicit)
        parts = [
            str(overlay.get("type") or "overlay"),
            str(overlay.get("strategy_id") or ""),
            str(overlay.get("symbol") or ""),
            str(overlay.get("timeframe") or ""),
            str(overlay.get("instrument_id") or ""),
            str(overlay.get("source") or ""),
            str(ordinal),
        ]
        return "|".join(parts)

    @staticmethod
    def _overlay_payload_fingerprint(overlay: Mapping[str, Any]) -> str:
        try:
            return json.dumps(overlay, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            return str(overlay)

    def _build_overlay_delta(
        self,
        cache: Dict[str, Any],
        overlays: Sequence[Mapping[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        previous_entries = cache.get("overlay_entries")
        previous_fingerprints = cache.get("overlay_fingerprints")
        previous_order = cache.get("overlay_order")
        previous_seq = int(cache.get("overlay_seq") or 0)
        if not isinstance(previous_entries, dict) or not isinstance(previous_fingerprints, dict) or not isinstance(previous_order, list):
            previous_entries = {}
            previous_fingerprints = {}
            previous_order = []

        next_entries: Dict[str, Dict[str, Any]] = {}
        next_fingerprints: Dict[str, str] = {}
        next_order: List[str] = []
        for idx, overlay in enumerate(overlays):
            if not isinstance(overlay, Mapping):
                continue
            key = self._overlay_cache_key(overlay, idx)
            next_entries[key] = dict(overlay)
            next_fingerprints[key] = self._overlay_payload_fingerprint(overlay)
            next_order.append(key)

        if (
            previous_order == next_order
            and all(previous_fingerprints.get(key) == next_fingerprints.get(key) for key in next_order)
            and len(previous_entries) == len(next_entries)
        ):
            return None

        next_seq = previous_seq + 1
        ops: List[Dict[str, Any]] = []
        removed_keys = [key for key in previous_order if key not in next_entries]
        for key in removed_keys:
            ops.append({"op": "remove", "key": key})
        for key in next_order:
            if previous_fingerprints.get(key) != next_fingerprints.get(key):
                ops.append({"op": "upsert", "key": key, "overlay": next_entries[key]})

        cache["overlay_entries"] = next_entries
        cache["overlay_fingerprints"] = next_fingerprints
        cache["overlay_order"] = next_order
        cache["overlay_seq"] = next_seq
        return {
            "seq": next_seq,
            "base_seq": previous_seq,
            "ops": ops,
        }

    @staticmethod
    def _overlay_delta_op_counts(delta: Mapping[str, Any]) -> Dict[str, int]:
        ops = delta.get("ops")
        if not isinstance(ops, list):
            return {}
        counts: Dict[str, int] = {}
        for op in ops:
            if not isinstance(op, Mapping):
                continue
            key = str(op.get("op") or "unknown").lower()
            counts[key] = counts.get(key, 0) + 1
        return counts

    @classmethod
    def _count_overlay_points(cls, overlays: Sequence[Mapping[str, Any]]) -> int:
        points = 0
        for overlay in overlays or []:
            if not isinstance(overlay, Mapping):
                continue
            payload = overlay.get("payload")
            if isinstance(payload, Mapping):
                points += cls._overlay_points_for_payload(payload)
        return points

    @classmethod
    def _overlay_change_metrics(
        cls,
        before: Sequence[Mapping[str, Any]],
        after: Sequence[Mapping[str, Any]],
    ) -> Tuple[float, float]:
        changed = 0
        before_len = len(before or [])
        after_len = len(after or [])
        min_len = min(before_len, after_len)
        for idx in range(min_len):
            prev = before[idx] if isinstance(before[idx], Mapping) else {}
            curr = after[idx] if isinstance(after[idx], Mapping) else {}
            prev_type = str(prev.get("type") or "")
            curr_type = str(curr.get("type") or "")
            prev_points = cls._overlay_points_for_payload(prev.get("payload")) if isinstance(prev.get("payload"), Mapping) else 0
            curr_points = cls._overlay_points_for_payload(curr.get("payload")) if isinstance(curr.get("payload"), Mapping) else 0
            if prev_type != curr_type or prev_points != curr_points:
                changed += 1
        changed += abs(before_len - after_len)
        points_changed = abs(cls._count_overlay_points(after or []) - cls._count_overlay_points(before or []))
        return float(changed), float(points_changed)

    def _overlay_payload_metrics(self, payload: Mapping[str, Any]) -> Tuple[int, int]:
        overlay_count = 0
        overlay_points = 0

        def consume(overlays: Any) -> None:
            nonlocal overlay_count, overlay_points
            if not isinstance(overlays, list):
                return
            for overlay in overlays:
                if not isinstance(overlay, Mapping):
                    continue
                overlay_count += 1
                overlay_payload = overlay.get("payload")
                if isinstance(overlay_payload, Mapping):
                    overlay_points += self._overlay_points_for_payload(overlay_payload)

        consume(payload.get("overlays"))
        series_list = payload.get("series")
        if isinstance(series_list, list):
            for series_entry in series_list:
                if not isinstance(series_entry, Mapping):
                    continue
                consume(series_entry.get("overlays"))
        return overlay_count, overlay_points

    def _visible_candles(self) -> List[Dict[str, Any]]:
        # Use first series for chart state (backward compatibility)
        primary_state = self._series_states[0] if self._series_states else None
        primary = primary_state.series if primary_state else None
        return self._chart_state_builder.visible_candles(
            primary,
            self.state.get("status"),
            primary_state.bar_index if primary_state else 0,
            self._intrabar_manager,
        )

    def _log_candle_sequence(
        self,
        stage: str,
        strategy_id: Optional[str],
        candles: Sequence[Any],
    ) -> None:
        if not candles or len(candles) < 2:
            return

        key = (stage, strategy_id or "unknown")

        def epoch_from_entry(entry: Any) -> Optional[int]:
            if isinstance(entry, Candle):
                return int(entry.time.timestamp())
            if isinstance(entry, Mapping):
                return self._normalise_epoch(entry.get("time"))
            if isinstance(entry, (int, float)):
                return int(entry)
            return None

        previous: Optional[int] = None
        first_epoch: Optional[int] = None
        second_epoch: Optional[int] = None
        last_epoch: Optional[int] = None
        for idx, entry in enumerate(candles):
            epoch = epoch_from_entry(entry)
            if epoch is None:
                if key not in self._candle_diag_null:
                    self._candle_diag_null.add(key)
                    context = self._runtime_log_context(
                        strategy_id=strategy_id,
                        stage=stage,
                        index=idx,
                    )
                    logger.error(with_log_context("bot_runtime_candle_missing_time", context))
                continue
            if first_epoch is None:
                first_epoch = epoch
            elif second_epoch is None:
                second_epoch = epoch
            last_epoch = epoch
            if previous is not None and epoch < previous:
                context = self._runtime_log_context(
                    strategy_id=strategy_id,
                    stage=stage,
                    index=idx,
                    prev=previous,
                    current=epoch,
                )
                logger.error(with_log_context("bot_runtime_candle_order_violation", context))
                return
            previous = epoch

        if first_epoch is None or last_epoch is None:
            return
        start_iso = _isoformat(datetime.fromtimestamp(first_epoch, tz=timezone.utc))
        second_iso = (
            _isoformat(datetime.fromtimestamp(second_epoch, tz=timezone.utc))
            if second_epoch is not None
            else None
        )
        end_iso = _isoformat(datetime.fromtimestamp(last_epoch, tz=timezone.utc))
        if key in self._candle_diag_seen:
            return
        self._candle_diag_seen.add(key)
        context = self._runtime_log_context(
            strategy_id=strategy_id,
            stage=stage,
            count=len(candles),
            start=start_iso,
            second=second_iso,
            end=end_iso,
        )
        logger.debug(with_log_context("bot_runtime_candle_sequence_ok", context))

    def _current_epoch(self) -> Optional[int]:
        # Use first series for current epoch (backward compatibility)
        primary_state = self._series_states[0] if self._series_states else None
        primary = primary_state.series if primary_state else None
        if not primary_state or not primary or not primary.candles:
            return None
        if primary_state.bar_index <= 0:
            status = str(self.state.get("status") or "").lower()
            if status in {"idle", "initialising"}:
                return None
        idx = min(max(primary_state.bar_index - 1, 0), len(primary.candles) - 1)
        candle = primary.candles[idx]
        return int(candle.time.timestamp())

    def _current_epoch_for(self, series: Optional[StrategySeries]) -> Optional[int]:
        state = self._series_state_for(series)
        if not series or not series.candles or state is None:
            return None
        if state.bar_index <= 0:
            status = str(self.state.get("status") or "").lower()
            if status in {"idle", "initialising"}:
                return None
        idx = min(max(state.bar_index - 1, 0), len(series.candles) - 1)
        candle = series.candles[idx]
        return int(candle.time.timestamp())

    def _visible_overlays(self) -> List[Dict[str, Any]]:
        status = str(self.state.get("status") or "").lower()
        return self._chart_state_builder.visible_overlays(
            self._chart_overlays,
            status,
            self._current_epoch(),
        )

    def _series_payloads(self) -> List[Dict[str, Any]]:
        status = str(self.state.get("status") or "").lower()
        payloads: List[Dict[str, Any]] = []
        for series in self._series:
            state = self._series_state_for(series)
            bar_index = state.bar_index if state else 0
            overlays = list(series.overlays or [])
            if series.trade_overlay:
                overlays.append(series.trade_overlay)
            # Build per-series stats including avg/largest calculations
            series_stats = series.risk_engine.stats()
            series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)
            # Calculate avg and largest win/loss for this series
            tolerance = 1e-8
            win_pnls = []
            loss_pnls = []
            for trade in series.risk_engine.trades:
                if trade.is_active():
                    continue
                pnl = trade.net_pnl
                if pnl > tolerance:
                    win_pnls.append(pnl)
                elif pnl < -tolerance:
                    loss_pnls.append(pnl)
            series_stats["avg_win"] = round(sum(win_pnls) / len(win_pnls), 4) if win_pnls else 0.0
            series_stats["avg_loss"] = round(sum(loss_pnls) / len(loss_pnls), 4) if loss_pnls else 0.0
            series_stats["largest_win"] = round(max(win_pnls), 4) if win_pnls else 0.0
            series_stats["largest_loss"] = round(min(loss_pnls), 4) if loss_pnls else 0.0

            payloads.append(
                {
                    "strategy_id": series.strategy_id,
                    "symbol": series.symbol,
                    "timeframe": series.timeframe,
                    "datasource": series.datasource,
                    "exchange": series.exchange,
                    "instrument": series.instrument,
                    "candles": self._chart_state_builder.visible_candles(
                        series,
                        status,
                        bar_index,
                        self._intrabar_manager,
                    ),
                    "overlays": self._chart_state_builder.visible_overlays(
                        overlays,
                        status,
                        self._current_epoch_for(series),
                    ),
                    "trades": series.risk_engine.serialise_trades(),
                    "stats": series_stats,
                }
            )
        return payloads

    def _chart_state(self) -> Dict[str, Any]:
        candles = self._visible_candles()
        overlays = self._visible_overlays()
        payload = self._chart_state_builder.chart_state(
            candles,
            self._aggregate_trades(),
            self._last_stats or self._aggregate_stats(),
            overlays,
            self.logs(),
            self.decision_events(),
        )
        payload["series"] = self._series_payloads()
        return payload

    def _push_update(
        self,
        event: str,
        *,
        series: Optional[StrategySeries] = None,
        candle: Optional[Candle] = None,
        replace_last: bool = False,
        precomputed_stats: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Optional[float]]:
        push_started = datetime.now(timezone.utc)
        push_started_perf = time.perf_counter()
        ok = True
        payload_context: Dict[str, Any] = {
            "event": event,
            "payload_bytes": None,
            "build_state_ms": None,
            "delta_build_ms": None,
            "serialize_ms": None,
            "delta_serialize_ms": None,
            "enqueue_ms": None,
            "stream_emit_ms": None,
            "subscriber_count": None,
            "subscribers_count": None,
            "dropped_messages": None,
            "overlay_count": None,
            "overlay_points": None,
            "stats_update_ms": None,
            "stats_reused": None,
        }
        error_message: Optional[str] = None
        trace_persist_ms: Optional[float] = None
        build_state_ms: Optional[float] = None
        serialize_ms: Optional[float] = None
        enqueue_ms: Optional[float] = None
        stats_update_ms: Optional[float] = None
        overlay_count: Optional[int] = None
        overlay_points: Optional[int] = None
        subscriber_count: Optional[int] = None
        dropped_messages: Optional[int] = None
        try:
            build_started = time.perf_counter()
            payload: Dict[str, Any] = {
                "type": "delta",
                "event": event,
                "runtime": self.snapshot(),
                "stats": None,
            }
            logs_entries = self.logs()
            logs_fingerprint = self._entry_fingerprint(logs_entries)
            if logs_fingerprint != self._push_logs_fingerprint:
                payload["logs"] = logs_entries
                self._push_logs_fingerprint = logs_fingerprint
            decisions_entries = self.decision_events()
            decisions_fingerprint = self._entry_fingerprint(decisions_entries)
            if decisions_fingerprint != self._push_decisions_fingerprint:
                payload["decisions"] = decisions_entries
                self._push_decisions_fingerprint = decisions_fingerprint
            if isinstance(precomputed_stats, Mapping):
                payload["stats"] = dict(precomputed_stats)
                stats_update_ms = 0.0
                payload_context["stats_reused"] = True
            else:
                stats_started = time.perf_counter()
                payload["stats"] = self._aggregate_stats()
                stats_update_ms = max((time.perf_counter() - stats_started) * 1000.0, 0.0)
                payload_context["stats_reused"] = False
            candles_count: Optional[int] = None
            trades_count: Optional[int] = None
            if series is not None:
                series_key = self._strategy_key(series)
                cache = self._push_series_cache.setdefault(series_key, {})
                status = str(self.state.get("status") or "").lower()
                series_state = self._series_state_for(series)
                bar_index = series_state.bar_index if series_state else 0
                candles_count = min(bar_index + 1, len(series.candles))
                series_delta: Dict[str, Any] = {
                    "strategy_id": series.strategy_id,
                    "symbol": series.symbol,
                    "timeframe": series.timeframe,
                    "bar_index": bar_index,
                    "replace_last": bool(replace_last),
                }
                include_heavy_series_data = event != "intrabar"
                if include_heavy_series_data or "visible_overlays" not in cache:
                    overlays = list(series.overlays or [])
                    if series.trade_overlay:
                        overlays.append(series.trade_overlay)
                    overlay_revision = (
                        status,
                        self._current_epoch_for(series),
                        len(overlays),
                    )
                    if cache.get("overlay_revision") != overlay_revision:
                        cache["visible_overlays"] = self._chart_state_builder.visible_overlays(
                            overlays,
                            status,
                            self._current_epoch_for(series),
                        )
                        cache["overlay_revision"] = overlay_revision
                    visible_overlays = cache.get("visible_overlays")
                    if isinstance(visible_overlays, list):
                        overlay_summary = self._overlay_summary(visible_overlays)
                        overlay_delta = self._build_overlay_delta(cache, visible_overlays)
                        logger.info(
                            with_log_context(
                                "bot_overlay_emit_attempt",
                                self._series_log_context(
                                    series,
                                    bar_index=bar_index,
                                    status=status,
                                    event=event,
                                    overlays=overlay_summary.get("total_overlays"),
                                    overlay_types=overlay_summary.get("type_counts"),
                                    overlay_payloads=overlay_summary.get("payload_counts"),
                                    overlay_profile_params=overlay_summary.get("profile_params_samples"),
                                    emitted_delta=isinstance(overlay_delta, Mapping),
                                ),
                            )
                        )
                        if isinstance(overlay_delta, Mapping):
                            series_delta["overlay_delta"] = dict(overlay_delta)
                            logger.info(
                                with_log_context(
                                    "bot_overlay_delta_sent",
                                    self._series_log_context(
                                        series,
                                        bar_index=bar_index,
                                        seq=overlay_delta.get("seq"),
                                        base_seq=overlay_delta.get("base_seq"),
                                        overlay_ops=len(overlay_delta.get("ops") or []),
                                        overlay_op_counts=self._overlay_delta_op_counts(overlay_delta),
                                        overlays=overlay_summary.get("total_overlays"),
                                        overlay_types=overlay_summary.get("type_counts"),
                                        overlay_payloads=overlay_summary.get("payload_counts"),
                                        overlay_profile_params=overlay_summary.get("profile_params_samples"),
                                    ),
                                )
                            )
                trades_revision = self._trade_revision(series)
                if cache.get("trades_revision") != trades_revision:
                    trades = series.risk_engine.serialise_trades()
                    trades_count = len(trades)
                    cache["trades"] = trades
                    cache["trades_revision"] = trades_revision
                    series_stats = series.risk_engine.stats()
                    series_stats["total_fees"] = series_stats.get("fees_paid", 0.0)
                    cache["series_stats"] = series_stats
                    series_delta["trades"] = trades
                    series_delta["stats"] = series_stats
                else:
                    cached_trades = cache.get("trades")
                    if isinstance(cached_trades, list):
                        trades_count = len(cached_trades)
                if candle is not None:
                    series_delta["candle"] = candle.to_dict()
                payload["series"] = [series_delta]
            build_state_ms = max((time.perf_counter() - build_started) * 1000.0, 0.0)
            overlay_count, overlay_points = self._overlay_payload_metrics(payload)
            payload_context.update(
                {
                    "candles_count": candles_count,
                    "trades_count": trades_count,
                    "logs_count": len(logs_entries),
                    "decisions_count": len(decisions_entries),
                    "series_count": len(self._series or []),
                    "build_state_ms": build_state_ms,
                    "delta_build_ms": build_state_ms,
                    "overlay_count": overlay_count,
                    "overlay_points": overlay_points,
                    "stats_update_ms": stats_update_ms,
                }
            )
            if self._obs_enabled:
                serialize_started = time.perf_counter()
                try:
                    payload_context["payload_bytes"] = len(json.dumps(payload, separators=(",", ":"), default=str))
                except Exception:
                    payload_context["payload_bytes"] = None
                finally:
                    serialize_ms = max((time.perf_counter() - serialize_started) * 1000.0, 0.0)
                    payload_context["serialize_ms"] = serialize_ms
                    payload_context["delta_serialize_ms"] = serialize_ms
            enqueue_started = time.perf_counter()
            subscriber_count, dropped_messages = self._broadcast("delta", payload)
            enqueue_ms = max((time.perf_counter() - enqueue_started) * 1000.0, 0.0)
            payload_context["enqueue_ms"] = enqueue_ms
            payload_context["stream_emit_ms"] = enqueue_ms
            payload_context["subscriber_count"] = subscriber_count
            payload_context["subscribers_count"] = subscriber_count
            payload_context["dropped_messages"] = dropped_messages
        except Exception as exc:
            ok = False
            error_message = str(exc)
            raise
        finally:
            if event == "bar":
                trace_persist_ms = self._record_step_trace(
                    "step_push_update",
                    started_at=push_started,
                    ended_at=datetime.now(timezone.utc),
                    ok=ok,
                    error=error_message,
                    context=payload_context,
                )
        push_duration_ms = max((time.perf_counter() - push_started_perf) * 1000.0, 0.0)
        return {
            "duration_ms": push_duration_ms,
            "build_state_ms": build_state_ms,
            "delta_build_ms": build_state_ms,
            "serialize_ms": serialize_ms,
            "delta_serialize_ms": serialize_ms,
            "enqueue_ms": enqueue_ms,
            "stream_emit_ms": enqueue_ms,
            "stats_update_ms": stats_update_ms,
            "subscriber_count": float(subscriber_count) if subscriber_count is not None else None,
            "subscribers_count": float(subscriber_count) if subscriber_count is not None else None,
            "dropped_messages": float(dropped_messages) if dropped_messages is not None else None,
            "overlay_count": float(overlay_count) if overlay_count is not None else None,
            "overlay_points": float(overlay_points) if overlay_points is not None else None,
            "trace_persist_ms": trace_persist_ms,
        }


__all__ = [
    "BotRuntime",
]

"""Bot runtime orchestrator combining domain, runtime, and reporting layers."""

from __future__ import annotations

import logging
import threading
import uuid
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from queue import Empty, Full, Queue
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from ....storage import storage
from engines.bot_runtime.core.domain import (
    Candle,
    StrategySignal,
    coerce_float,
    isoformat,
    normalize_epoch,
    timeframe_to_seconds,
)
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
from engines.bot_runtime.core.wallet import project_wallet
from ....indicators.indicator_service.context import IndicatorServiceContext, _context as indicator_context
from indicators.runtime.indicator_overlay_cache import default_overlay_cache
from indicators.runtime.overlay_cache_registry import get_overlay_cache_types

logger = logging.getLogger(__name__)

DEFAULT_SIM_LOOKBACK_DAYS = 7
MAX_LOG_ENTRIES = 500
INTRABAR_BASE_SECONDS = 0.4
WALK_FORWARD_SAMPLE_INTERVAL = 50


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
        self.bot_id = bot_id
        self.config = dict(config)
        self.mode = (self.config.get("mode") or "instant").lower()
        self.run_type = (self.config.get("run_type") or "backtest").lower()
        self.playback_speed = self._coerce_playback_speed(self.config.get("playback_speed"))
        self.focus_symbol = self.config.get("focus_symbol")
        self.state: Dict[str, object] = {"status": "idle", "progress": 0.0, "paused": False}
        self.state["playback_speed"] = self.playback_speed
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
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
        self._last_stats: Dict[str, Any] = {}
        self._next_bar_at: Optional[datetime] = None
        self._policy = RuntimeModePolicy.for_run_type(self.run_type)
        self._live_mode = self._policy.allow_live_refresh
        self._logs: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._decision_events: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._event_sinks: List[RuntimeEventSink] = [
            InMemoryEventSink(self._logs, self._decision_events, self._lock),
        ]
        self._subscribers: Dict[str, Queue] = {}
        self._state_callback = state_callback
        self._candle_diag_seen: Set[Tuple[str, str]] = set()
        self._candle_diag_null: Set[Tuple[str, str]] = set()
        self._prepare_error: Optional[Dict[str, Any]] = None
        overlay_cache = default_overlay_cache()
        for indicator_type in get_overlay_cache_types():
            overlay_cache.enable_type(indicator_type)
        self._overlay_cache = overlay_cache
        self._indicator_ctx = IndicatorServiceContext.fork_with_overlay_cache(
            indicator_context,
            overlay_cache,
        )
        self._series_builder = SeriesBuilder(
            self.bot_id,
            self.config,
            self.run_type,
            self._log_candle_sequence,
            indicator_ctx=self._indicator_ctx,
        )
        self._intrabar_manager = IntrabarManager(
            self.bot_id,
            build_candles=SeriesBuilder._build_candles,
            timeframe_seconds=_timeframe_to_seconds,
            strategy_key_fn=self._strategy_key,
        )
        self._run_started_at: Optional[datetime] = None
        self._chart_state_builder = ChartStateBuilder(
            normalise_epoch_fn=self._normalise_epoch,
            log_sequence_fn=self._log_candle_sequence,
            strategy_key_fn=self._strategy_key,
        )

    def add_event_sink(self, sink: RuntimeEventSink) -> None:
        """Attach an additional event sink for runtime tracing."""
        if sink is None:
            return
        with self._lock:
            self._event_sinks.append(sink)

    @staticmethod
    def _coerce_playback_speed(value: Optional[object]) -> float:
        try:
            numeric = float(value) if value is not None else 10.0
        except (TypeError, ValueError):
            numeric = 10.0
        return numeric if numeric >= 0 else 0.0

    def _runtime_log_context(self, **fields: object) -> Dict[str, object]:
        run_id = self._run_context.run_id if self._run_context else None
        return build_log_context(bot_id=self.bot_id, bot_mode=self.run_type, run_id=run_id, **fields)

    def _series_log_context(self, series: StrategySeries, **fields: object) -> Dict[str, object]:
        return merge_log_context(self._runtime_log_context(), series_log_context(series), **fields)

    def apply_config(self, payload: Mapping[str, Any]) -> None:
        """Apply runtime config updates (e.g., playback speed overrides)."""

        if not payload:
            return
        self.config.update(payload)
        if "playback_speed" in payload:
            self.playback_speed = self._coerce_playback_speed(payload.get("playback_speed"))
            with self._lock:
                self.state["playback_speed"] = self.playback_speed
        if "focus_symbol" in payload:
            self.focus_symbol = payload.get("focus_symbol") or None

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
        self._broadcast("error", {"runtime": self._state_payload(), "error": error_payload})
        return error_payload

    def _ensure_prepared(self) -> None:
        if self._prepared:
            return
        if self.state.get("status") == "error":
            message = (self._prepare_error or {}).get("message") or "Runtime is in an error state; reset before preparing"
            raise RuntimeError(message)
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
        self._build_series_states()
        self._rebuild_overlay_cache()
        self._prepared = True
        with self._lock:
            self.state.update({"status": "idle", "progress": 0.0, "paused": False})
        context = self._runtime_log_context(series=len(self._series), total_bars=self._total_bars)
        logger.info(with_log_context("bot_runtime_prepared", context))
        self._log_event("prepared", total_bars=self._total_bars)
        self._push_update("prepared")

    def _rebuild_overlay_cache(self) -> None:
        overlays: List[Dict[str, Any]] = []
        for series in self._series:
            overlays.extend(series.overlays)
            if series.trade_overlay:
                overlays.append(series.trade_overlay)
        self._chart_overlays = overlays

    def _build_series_states(self) -> None:
        self._series_states = []
        self._series_state_map = {}
        self._primary_series_key = None
        for series in self._series:
            state = SeriesExecutionState(series=series, total_bars=len(series.candles))
            key = self._strategy_key(series)
            self._series_states.append(state)
            self._series_state_map[key] = state
            if self._primary_series_key is None:
                self._primary_series_key = key

    def _series_state_for(self, series: Optional[StrategySeries]) -> Optional[SeriesExecutionState]:
        if series is None:
            return None
        return self._series_state_map.get(self._strategy_key(series))

    def _active_series_states(self) -> List[SeriesExecutionState]:
        return [state for state in self._series_states if not state.done]

    def _compute_progress(self) -> float:
        if not self._series_states:
            return 0.0
        progress_total = 0.0
        counted = 0
        for state in self._series_states:
            if state.total_bars <= 0:
                continue
            progress_total += min(state.bar_index, state.total_bars) / state.total_bars
            counted += 1
        return round(progress_total / counted, 4) if counted else 0.0

    def _refresh_next_bar_at(self) -> None:
        next_at: Optional[datetime] = None
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

        return {
            "type": "bot_trade_rays",
            "source": TRADE_OVERLAY_SOURCE,
            "payload": {"segments": segments},
        }

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
        state.intrabar_candles = intrabar
        state.intrabar_index = 0
        self._schedule_next_step(state, self._intrabar_interval())
        context = self._series_log_context(series, bars=len(intrabar))
        logger.debug(with_log_context("intrabar_start", context))
        return []

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
        self._update_state(self._state_candle_for(series, temp_candle))
        self._push_update("intrabar")
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
            )
            self._persist_trade_event(series, event)
        if engine.active_trade is None or not state.intrabar_active():
            self._finish_intrabar(state)
        else:
            self._schedule_next_step(state, self._intrabar_interval())

    def _finish_intrabar(self, state: SeriesExecutionState) -> None:
        if state.intrabar_candles:
            self._intrabar_manager.snapshots.pop(self._strategy_key(state.series), None)
        if state.intrabar_candles:
            context = self._series_log_context(state.series, steps=state.intrabar_index)
            logger.debug(with_log_context("intrabar_complete", context))
        state.intrabar_candles = []
        state.intrabar_index = 0
        if state.active_candle is not None:
            self._finalize_bar_step(state, state.active_candle)

    def _finalize_bar_step(self, state: SeriesExecutionState, candle: Candle) -> None:
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
        self._log_overlay_summary(state, candle)
        self._update_state(self._state_candle_for(state.series, candle))
        self._push_update("bar")

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
        instrument = series.instrument or {}
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
        )
        logger.info(with_log_context("instrument_overlay_summary", context))

    @staticmethod
    def _overlay_summary(overlays: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
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
            profiles = payload.get("profiles")
            if isinstance(profiles, list):
                profile_counts[overlay_type] = profile_counts.get(overlay_type, 0) + len(profiles)
            if "profile_params" in payload:
                profile_params_present[overlay_type] = profile_params_present.get(overlay_type, 0) + 1
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
        return {
            "total_overlays": len(overlays or []),
            "type_counts": type_counts,
            "payload_counts": payload_counts,
            "profile_counts": profile_counts,
            "profile_params_present": profile_params_present,
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
            self._last_stats = {}
            self._next_bar_at = None
            self._logs.clear()
            self._decision_events.clear()
            self._intrabar_manager.clear_cache()
            self._run_started_at = None
            self._run_context = None
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

        self._ensure_prepared()
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._pause_event.set()
        self._paused = False
        self._run_started_at = datetime.now(timezone.utc)
        self._run_context = self._build_run_context()
        with self._lock:
            self.state.update(
                {"status": "starting", "paused": False, "started_at": _isoformat(self._run_started_at)}
            )
        self._thread = threading.Thread(target=self._run, name=f"bot-{self.bot_id}", daemon=True)
        self._thread.start()
        self._log_event("start", message="Bot runtime started", mode=self.mode, run_type=self.run_type)
        self._push_update("start")

    def _run(self) -> None:
        try:
            self._execute_loop()
        except Exception as exc:  # pragma: no cover - defensive logging
            context = self._runtime_log_context(error=str(exc))
            logger.exception(with_log_context("bot_runtime_loop_failed", context))
            with self._lock:
                self.state.update({"status": "error", "error": str(exc)})
            self._persist_runtime_state("error")

    def _execute_loop(self) -> None:
        self._ensure_prepared()
        status = "running"
        self._log_event("running", message="Bot execution loop started")
        while not self._stop.is_set():
            if not self._pause_event.wait(timeout=0.2):
                continue
            now = datetime.now(timezone.utc)
            due_states = self._due_series_states(now)
            if not due_states:
                if self._live_mode and self._append_live_candles_if_needed():
                    continue
                next_at = self._next_step_time()
                if next_at:
                    interval = max((next_at - now).total_seconds(), 0)
                    self._pace(interval, update_next_bar=True)
                    continue
                break
            for state in due_states:
                self._step_series_state(state)
        if self._stop.is_set():
            status = "stopped"
        elif not self._live_mode:
            status = "completed"
        self._next_bar_at = None
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
        candle = series.candles[state.bar_index]
        state.active_candle = candle
        epoch = int(candle.time.timestamp())

        # Debug: Log signal queue status
        signals_pending = len(series.signals) if series.signals else 0
        context = self._series_log_context(
            series,
            bar_index=state.bar_index,
            epoch=epoch,
            signals_pending=signals_pending,
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

        direction = self._next_signal_for(series, epoch)

        # Debug: Log signal consumption result
        if direction is not None:
            context = self._series_log_context(
                series,
                bar_index=state.bar_index,
                epoch=epoch,
                direction=direction,
            )
            logger.debug(with_log_context("signal_consumed", context))

        # Attempt to create trade from signal
        blocking_trade = None
        if direction is not None:
            instrument_id = None
            if isinstance(series.instrument, Mapping):
                instrument_id = series.instrument.get("id")
            if not instrument_id:
                self._log_decision_event(
                    event="signal_rejected",
                    series=series,
                    candle=candle,
                    signal_type="strategy_signal",
                    signal_direction=direction,
                    signal_price=candle.close,
                    rule_id=None,
                    decision="rejected",
                    reason="instrument_id_missing",
                    instrument_id=None,
                )
                direction = None
            else:
                blocking_trade = self._active_trade_for_instrument(
                    instrument_id,
                    skip_series=series,
                )

        new_trade = None
        if direction is not None and blocking_trade is None:
            new_trade = series.risk_engine.maybe_enter(candle, direction)

        # Log decision event
        if direction is not None:
            if new_trade is not None:
                # Signal was accepted and trade was opened
                self._log_decision_event(
                    event="signal_accepted",
                    series=series,
                    candle=candle,
                    signal_type="strategy_signal",  # Generic type for now
                    signal_direction=direction,
                    signal_price=candle.close,
                    rule_id=None,  # Not available in current signal queue
                    decision="accepted",
                    reason=None,
                    trade_id=new_trade.trade_id,
                    trade_time=_isoformat(new_trade.entry_time),
                )
            else:
                # Signal was rejected (no trade opened)
                # Determine rejection reason
                rejection_reason = "Active trade already open"
                rejection_meta: Optional[Dict[str, Any]] = None
                blocking_trade_id: Optional[str] = None
                if blocking_trade is not None:
                    rejection_reason = "Active trade already open for instrument"
                    blocked_instrument_id = None
                    if isinstance(series.instrument, Mapping):
                        blocked_instrument_id = series.instrument.get("id")
                    blocking_trade_id = getattr(blocking_trade, "trade_id", None)
                    rejection_meta = {
                        "active_trade_id": blocking_trade_id,
                        "blocked_instrument_id": blocked_instrument_id,
                    }
                elif series.risk_engine.active_trade is None:
                    rejection_reason = series.risk_engine.last_rejection_reason or "Risk engine declined entry"
                    rejection_meta = series.risk_engine.last_rejection_detail
                if rejection_meta and "reason" in rejection_meta:
                    rejection_meta = {k: v for k, v in rejection_meta.items() if k != "reason"}

                self._log_decision_event(
                    event="signal_rejected",
                    series=series,
                    candle=candle,
                    signal_type="strategy_signal",
                    signal_direction=direction,
                    signal_price=candle.close,
                    rule_id=None,
                    decision="rejected",
                    reason=rejection_reason,
                    **(rejection_meta or {}),
                    trade_id=blocking_trade_id,
                )

        if new_trade is not None:
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
            self._persist_trade_entry(series, new_trade)
            self._update_trade_overlay(series)
        trade_events = self._prime_intrabar_or_step_bar(state, candle)
        for event in trade_events:
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
            self._persist_trade_event(series, event)
        self._update_trade_overlay(series)
        series.last_consumed_epoch = max(series.last_consumed_epoch, epoch)
        if not state.intrabar_active():
            self._finalize_bar_step(state, candle)

    def _next_signal_for(self, series: StrategySeries, epoch: int) -> Optional[str]:
        direction: Optional[str] = None
        while series.signals and series.signals[0].epoch <= epoch:
            direction = series.signals.popleft().direction
        return direction

    def _compute_playback_interval(self, base_seconds: float = 1.0) -> float:
        speed = self.playback_speed or 0.0
        if speed <= 0:
            return 0.0
        if self._has_open_trades():
            override = self.config.get("playback_speed_open_trade")
            if override is not None:
                try:
                    speed = float(override)
                except (TypeError, ValueError):
                    speed = self.playback_speed or 0.0
            elif speed > 1.0:
                speed = 1.0
        return max(base_seconds / speed, 0.02)

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
        for series in self._series:
            last_time = series.candles[-1].time if series.candles else None
            if last_time is None:
                continue
            start_iso = _isoformat(last_time + timedelta(seconds=1))
            if self._append_series_updates(series, start_iso, end_iso):
                updated = True
        if updated:
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
        total = summary.get("completed_trades") or (summary["wins"] + summary["losses"])
        summary["win_rate"] = round(summary["wins"] / total, 4) if total else 0.0
        summary["gross_pnl"] = round(gross, 4)
        summary["fees_paid"] = round(fees, 4)
        summary["net_pnl"] = round(net, 4)
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
        reason: Optional[str] = None,
        trade_id: Optional[str] = None,
        trade_time: Optional[str] = None,
        conditions: Optional[List[Dict[str, Any]]] = None,
        **metadata: object,
    ) -> None:
        """Log a strategy-level decision event for the decision trace."""
        from engines.bot_runtime.core.domain import DecisionEvent

        decision_event = DecisionEvent(
            event_id=str(uuid.uuid4()),
            event=event,
            timestamp=_isoformat(datetime.now(timezone.utc)),
            bar_time=_isoformat(candle.time),
            chart_time=_isoformat(candle.time),
            trade_time=trade_time,
            strategy_id=series.strategy_id,
            strategy_name=series.name or series.strategy_id,
            symbol=series.symbol,
            signal_type=signal_type,
            signal_direction=signal_direction,
            signal_price=signal_price or candle.close,
            rule_id=rule_id,
            decision=decision,
            reason=reason,
            trade_id=trade_id,
            conditions=conditions,
            metadata=dict(metadata) if metadata else None,
            created_at=_isoformat(datetime.now(timezone.utc)),
        )

        payload = decision_event.serialize()
        with self._lock:
            sinks = list(self._event_sinks)
            if self._run_context is not None:
                self._run_context.decision_trace.append(payload)
        for sink in sinks:
            sink.record_decision(payload)

    def _build_run_context(self) -> RunContext:
        wallet_config = self.config.get("wallet_config")
        if not isinstance(wallet_config, dict):
            raise ValueError("wallet_config is required to start a bot run")
        balances = wallet_config.get("balances")
        if not isinstance(balances, dict) or not balances:
            raise ValueError("wallet_config.balances is required to start a bot run")
        run_context = RunContext(bot_id=self.bot_id)
        run_context.wallet_ledger.deposit(balances)
        for series in self._series:
            series.risk_engine.attach_wallet(run_context.wallet_ledger)
        return run_context

    def _persist_run_artifact(self, status: str) -> None:
        if self._run_context is None:
            return
        from ....storage import storage

        artifact = self._run_artifact_payload(status)
        storage.update_bot_run_artifact(self.bot_id, artifact)

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
        """Return up to *limit* recent decision events."""

        with self._lock:
            entries = list(self._decision_events)
        if limit and limit > 0:
            entries = entries[-limit:]
        return entries

    def _persist_trade_entry(self, series: StrategySeries, trade: LadderPosition) -> None:
        if not series or not trade:
            return
        contracts = sum(max(leg.contracts, 0) for leg in trade.legs)
        storage.record_bot_trade(
            {
                "trade_id": trade.trade_id,
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
                "metrics": trade._metrics_snapshot(),
            }
        )

    def _persist_trade_event(self, series: StrategySeries, event: Dict[str, Any]) -> None:
        trade_id = event.get("trade_id")
        if not trade_id:
            return
        payload = {
            "id": event.get("id"),
            "trade_id": trade_id,
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
        storage.record_bot_trade_event(payload)
        if event.get("type") == "close":
            storage.record_bot_trade(
                {
                    "trade_id": trade_id,
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

    def _update_state(self, candle: Candle, status: str = "running") -> None:
        stats = self._aggregate_stats()
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
            "playback_speed": self.playback_speed,
        }
        with self._lock:
            self.state.update(snapshot)
        if self._state_callback:
            try:
                self._state_callback({"runtime": self.snapshot()})
            except Exception as exc:  # pragma: no cover - defensive logging
                context = self._runtime_log_context(error=str(exc))
                logger.warning(with_log_context("bot_runtime_stream_callback_failed", context), exc_info=exc)

    def _seconds_until_next_bar(self) -> Optional[float]:
        if not self._next_bar_at:
            return None
        delta = (self._next_bar_at - datetime.now(timezone.utc)).total_seconds()
        return round(delta, 2) if delta > 0 else 0.0

    def _state_payload(self) -> Dict[str, object]:
        self._refresh_next_bar_at()
        with self._lock:
            payload = dict(self.state)
        payload.setdefault("stats", self._last_stats)
        if "next_bar_at" not in payload:
            payload["next_bar_at"] = _isoformat(self._next_bar_at)
        if "next_bar_in_seconds" not in payload:
            payload["next_bar_in_seconds"] = self._seconds_until_next_bar()
        if "started_at" not in payload and self._run_started_at is not None:
            payload["started_at"] = _isoformat(self._run_started_at)
        return payload

    def snapshot(self) -> Dict[str, object]:
        """Return a thread-safe snapshot of runtime state."""

        if self.state.get("status") != "error":
            self._ensure_prepared()
        return self._state_payload()

    def chart_payload(self) -> Dict[str, object]:
        """Return the latest candle, trade, overlay, and stat data for the lens."""

        self._ensure_prepared()
        payload = self._chart_state()
        payload["runtime"] = self.snapshot()
        return payload

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

    def _broadcast(self, event: str, payload: Optional[Dict[str, Any]] = None) -> None:
        message = dict(payload or {})
        message.setdefault("type", event)
        with self._lock:
            channels = list(self._subscribers.values())
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
                    continue

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

    def _push_update(self, event: str) -> None:
        payload = self._chart_state()
        payload["runtime"] = self.snapshot()
        self._broadcast(event, payload)


__all__ = [
    "BotRuntime",
]

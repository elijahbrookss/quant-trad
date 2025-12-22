"""Bot runtime orchestrator combining domain, runtime, and reporting layers."""

from __future__ import annotations

import logging
import re
import threading
import uuid
import time
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from queue import Empty, Full, Queue
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from .. import risk_math, storage, strategy_service
from ..candle_service import fetch_ohlcv
from .domain import (
    Candle,
    LadderRiskEngine,
    StrategySignal,
    coerce_float,
    isoformat,
    normalize_epoch,
    timeframe_duration,
    timeframe_to_seconds,
)
from .reporting import (
    TRADE_OVERLAY_SOURCE,
    TRADE_STOP_COLOR,
    TRADE_TARGET_COLOR,
    TRADE_RAY_MIN_SECONDS,
    TRADE_RAY_SPAN_MULTIPLIER,
    instrument_key,
)
from .chart_state import ChartStateBuilder
from .intrabar import IntrabarManager
from .series_builder import SeriesBuilder, StrategySeries

logger = logging.getLogger(__name__)

DEFAULT_SIM_LOOKBACK_DAYS = 7
MAX_LOG_ENTRIES = 500
INTRABAR_BASE_SECONDS = 0.4


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    return coerce_float(value, default)


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    return isoformat(value)


def _timeframe_to_seconds(label: Optional[str]) -> Optional[int]:
    return timeframe_to_seconds(label)


def _timeframe_duration(label: Optional[str]) -> Optional[timedelta]:
    return timeframe_duration(label)


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
        self.state: Dict[str, object] = {"status": "idle", "progress": 0.0, "paused": False}
        self.state["playback_speed"] = self.playback_speed
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._paused = False
        self._series: List[StrategySeries] = []
        self._total_bars: int = 0
        self._bar_index: int = 0
        self._prepared: bool = False
        self._chart_overlays: List[Dict[str, Any]] = []
        self._last_stats: Dict[str, Any] = {}
        self._next_bar_at: Optional[datetime] = None
        self._live_mode = self.run_type == "sim_trade"
        self._logs: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._subscribers: Dict[str, Queue] = {}
        self._state_callback = state_callback
        self._candle_diag_seen: Set[Tuple[str, str]] = set()
        self._candle_diag_null: Set[Tuple[str, str]] = set()
        self._prepare_error: Optional[Dict[str, Any]] = None
        self._series_builder = SeriesBuilder(self.bot_id, self.config, self.run_type, self._log_candle_sequence)
        self._intrabar_manager = IntrabarManager(
            self.bot_id,
            build_candles=SeriesBuilder._build_candles,
            timeframe_seconds=_timeframe_to_seconds,
            strategy_key_fn=self._strategy_key,
        )
        self._chart_state_builder = ChartStateBuilder(
            normalise_epoch_fn=self._normalise_epoch,
            log_sequence_fn=self._log_candle_sequence,
            strategy_key_fn=self._strategy_key,
        )

    @staticmethod
    def _coerce_playback_speed(value: Optional[object]) -> float:
        try:
            numeric = float(value) if value is not None else 10.0
        except (TypeError, ValueError):
            numeric = 10.0
        return numeric if numeric >= 0 else 0.0

    def apply_config(self, payload: Mapping[str, Any]) -> None:
        """Apply runtime config updates (e.g., playback speed overrides)."""

        if not payload:
            return
        self.config.update(payload)
        if "playback_speed" in payload:
            self.playback_speed = self._coerce_playback_speed(payload.get("playback_speed"))
            with self._lock:
                self.state["playback_speed"] = self.playback_speed

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
            logger.debug("[BotRuntime] Preparing bot runtime %s with strategy_ids: %s", self.bot_id, strategy_ids)
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
        self._bar_index = 0
        self._rebuild_overlay_cache()
        self._prepared = True
        with self._lock:
            self.state.update({"status": "idle", "progress": 0.0, "paused": False})
        logger.info(
            "Bot runtime prepared | bot=%s | series=%d | total_bars=%d",
            self.bot_id,
            len(self._series),
            self._total_bars,
        )
        self._log_event("prepared", total_bars=self._total_bars)
        self._push_update("prepared")

    def _rebuild_overlay_cache(self) -> None:
        overlays: List[Dict[str, Any]] = []
        for series in self._series:
            overlays.extend(series.overlays)
            if series.trade_overlay:
                overlays.append(series.trade_overlay)
        self._chart_overlays = overlays

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
        return str(getattr(series, "strategy_id", getattr(series, "id", id(series))))

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

    def _step_series_with_intrabar(self, series: StrategySeries, candle: Candle) -> List[Dict[str, Any]]:
        engine = series.risk_engine
        if engine is None:
            return []
        intrabar = self._intrabar_manager.intrabar_candles(series, candle)
        if not intrabar:
            return engine.step(candle)
        events: List[Dict[str, Any]] = []
        snapshot_used = False
        for minute_bar in intrabar:
            events.extend(engine.step(minute_bar))
            snapshot = self._intrabar_manager.update_snapshot(series, candle, minute_bar)
            snapshot_used = True
            temp_candle = self._snapshot_candle_for_state(candle, snapshot)
            self._update_state(temp_candle)
            self._push_update("intrabar")
            if engine.active_trade is None:
                break
            self._pace_intrabar_step()
        if snapshot_used:
            self._intrabar_manager.snapshots.pop(self._strategy_key(series), None)
        return events

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
            self._total_bars = 0
            self._bar_index = 0
            self._chart_overlays = []
            self._last_stats = {}
            self._next_bar_at = None
            self._logs.clear()
            self._intrabar_manager.clear_cache()
            self.state = {"status": "idle", "progress": 0.0, "paused": False}
        self._stop.clear()
        self._pause_event.set()
        self._paused = False
        self._series_builder.reset()

    def needs_reset(self) -> bool:
        """Return True when the runtime finished and can be rerun."""

        status = str(self.state.get("status") or "").lower()
        finished = status in {"completed", "stopped", "error"}
        exhausted = bool(self._total_bars) and self._bar_index >= self._total_bars
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
        with self._lock:
            self.state.update({"status": "starting", "paused": False})
        self._thread = threading.Thread(target=self._run, name=f"bot-{self.bot_id}", daemon=True)
        self._thread.start()
        self._log_event("start", message="Bot runtime started", mode=self.mode, run_type=self.run_type)
        self._push_update("start")

    def _run(self) -> None:
        try:
            self._execute_loop()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("bot_runtime_loop_failed | bot=%s | error=%s", self.bot_id, exc)
            with self._lock:
                self.state.update({"status": "error", "error": str(exc)})
            self._persist_runtime_state("error")

    def _execute_loop(self) -> None:
        self._ensure_prepared()
        status = "running"
        self._log_event("running", message="Bot execution loop started")
        while not self._stop.is_set():
            if self._bar_index >= self._total_bars:
                if self._live_mode and self._append_live_candles_if_needed():
                    continue
                break
            if not self._pause_event.wait(timeout=0.2):
                continue
            # Check if any series has candles remaining
            if not self._series or not any(len(s.candles) > self._bar_index for s in self._series):
                break
            self._apply_bar(self._bar_index)
            self._sleep_between_bars()
        if self._stop.is_set():
            status = "stopped"
        elif not self._live_mode:
            status = "completed"
        self._next_bar_at = None
        self._log_event(status, message=f"Bot runtime {status}")
        # Update state with last candle from first series (backward compatibility)
        if self._series and self._series[0].candles:
            self._update_state(self._series[0].candles[-1], status=status)
        else:
            with self._lock:
                self.state.update({"status": status})
        self._push_update(status)
        self._persist_runtime_state(status)

    def _apply_bar(self, index: int) -> None:
        for series in self._series:
            if index >= len(series.candles):
                continue
            candle = series.candles[index]
            epoch = int(candle.time.timestamp())
            direction = self._next_signal_for(series, epoch)
            new_trade = series.risk_engine.maybe_enter(candle, direction)
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
                    bar_index=index,
                    contracts=sum(max(leg.contracts, 0) for leg in new_trade.legs),
                )
                self._persist_trade_entry(series, new_trade)
                self._update_trade_overlay(series)
            trade_events = self._step_series_with_intrabar(series, candle)
            for event in trade_events:
                self._log_event(
                    event.get("type", "event"),
                    series,
                    candle,
                    trade_id=event.get("trade_id"),
                    leg=event.get("leg"),
                    price=event.get("price"),
                    event_time=event.get("time"),
                    bar_index=index,
                    contracts=event.get("contracts"),
                )
                self._persist_trade_event(series, event)
            self._update_trade_overlay(series)
            series.last_consumed_epoch = max(series.last_consumed_epoch, epoch)
        self._bar_index = index + 1
        # Update state with current candle from first series (backward compatibility)
        if self._series and self._series[0].candles:
            candle = self._series[0].candles[min(index, len(self._series[0].candles) - 1)]
            self._update_state(candle)
        self._push_update("bar")

    def _next_signal_for(self, series: StrategySeries, epoch: int) -> Optional[str]:
        direction: Optional[str] = None
        while series.signals and series.signals[0].epoch <= epoch:
            direction = series.signals.popleft().direction
        return direction

    def _sleep_between_bars(self) -> None:
        if self.mode != "walk-forward":
            return
        interval = self._compute_playback_interval()
        self._pace(interval, update_next_bar=True)

    def _compute_playback_interval(self, base_seconds: float = 1.0) -> float:
        speed = self.playback_speed or 0.0
        if speed <= 0:
            return 0.0
        return max(base_seconds / speed, 0.02)

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

    def _pace_intrabar_step(self) -> None:
        if self.mode != "walk-forward":
            return
        interval = self._compute_playback_interval(INTRABAR_BASE_SECONDS)
        self._pace(interval)

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

    def _log_event(
        self,
        event: str,
        series: Optional[StrategySeries] = None,
        candle: Optional[Candle] = None,
        **fields: object,
    ) -> None:
        entry: Dict[str, object] = {
            "id": str(uuid.uuid4()),
            "event": event,
            "timestamp": _isoformat(datetime.now(timezone.utc)),
        }
        if series is not None:
            entry["strategy_id"] = series.strategy_id
            entry["symbol"] = series.symbol
        if candle is not None:
            entry["bar_time"] = _isoformat(candle.time)
            entry.setdefault("price", round(candle.close, 4))
        for key, value in fields.items():
            if value is not None:
                entry[key] = value
        with self._lock:
            self._logs.append(entry)

    def logs(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return up to *limit* recent log entries."""

        with self._lock:
            entries = list(self._logs)
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
            logger.warning(
                "bot_runtime_state_callback_failed | bot=%s | error=%s",
                self.bot_id,
                exc,
            )

    def _update_state(self, candle: Candle, status: str = "running") -> None:
        stats = self._aggregate_stats()
        self._last_stats = stats
        progress = 0.0 if not self._total_bars else round(min(self._bar_index, self._total_bars) / self._total_bars, 4)
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
                logger.warning("bot_runtime_stream_callback_failed", exc_info=exc)

    def _seconds_until_next_bar(self) -> Optional[float]:
        if not self._next_bar_at:
            return None
        delta = (self._next_bar_at - datetime.now(timezone.utc)).total_seconds()
        return round(delta, 2) if delta > 0 else 0.0

    def _state_payload(self) -> Dict[str, object]:
        with self._lock:
            payload = dict(self.state)
        payload.setdefault("stats", self._last_stats)
        if "next_bar_at" not in payload:
            payload["next_bar_at"] = _isoformat(self._next_bar_at)
        if "next_bar_in_seconds" not in payload:
            payload["next_bar_in_seconds"] = self._seconds_until_next_bar()
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
        primary = self._series[0] if self._series else None
        return self._chart_state_builder.visible_candles(
            primary,
            self.state.get("status"),
            self._bar_index,
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
                    logger.error(
                        "bot_runtime_candle_missing_time | bot=%s | strategy=%s | stage=%s | index=%s",
                        self.bot_id,
                        strategy_id,
                        stage,
                        idx,
                    )
                continue
            if first_epoch is None:
                first_epoch = epoch
            elif second_epoch is None:
                second_epoch = epoch
            last_epoch = epoch
            if previous is not None and epoch < previous:
                logger.error(
                    "bot_runtime_candle_order_violation | bot=%s | strategy=%s | stage=%s | index=%s | prev=%s | current=%s",
                    self.bot_id,
                    strategy_id,
                    stage,
                    idx,
                    previous,
                    epoch,
                )
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
        logger.debug(
            "bot_runtime_candle_sequence_ok | bot=%s | strategy=%s | stage=%s | count=%s | start=%s | second=%s | end=%s",
            self.bot_id,
            strategy_id,
            stage,
            len(candles),
            start_iso,
            second_iso,
            end_iso,
        )

    def _current_epoch(self) -> Optional[int]:
        # Use first series for current epoch (backward compatibility)
        primary = self._series[0] if self._series else None
        if not primary or not primary.candles:
            return None
        if self._bar_index <= 0:
            status = str(self.state.get("status") or "").lower()
            if status in {"idle", "initialising"}:
                return None
        idx = min(max(self._bar_index - 1, 0), len(primary.candles) - 1)
        candle = primary.candles[idx]
        return int(candle.time.timestamp())

    def _visible_overlays(self) -> List[Dict[str, Any]]:
        status = str(self.state.get("status") or "").lower()
        return self._chart_state_builder.visible_overlays(
            self._chart_overlays,
            status,
            self._current_epoch(),
        )

    def _chart_state(self) -> Dict[str, Any]:
        candles = self._visible_candles()
        overlays = self._visible_overlays()
        return self._chart_state_builder.chart_state(
            candles,
            self._aggregate_trades(),
            self._last_stats or self._aggregate_stats(),
            overlays,
            self.logs(),
        )

    def _push_update(self, event: str) -> None:
        payload = self._chart_state()
        payload["runtime"] = self.snapshot()
        self._broadcast(event, payload)


__all__ = [
    "BotRuntime",
]

"""Backtesting runtime with ladder risk logic for bot simulations."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from . import strategy_service
from .candle_service import fetch_ohlcv


logger = logging.getLogger(__name__)


DEFAULT_RISK = {
    "contracts": 3,
    "targets": [20, 40, 60],
    "stop_ticks": 30,
    "breakeven_trigger_ticks": 20,
    "tick_size": 0.01,
}


@dataclass
class Candle:
    """Single OHLC datapoint used by the simulated bot."""

    time: datetime
    open: float
    high: float
    low: float
    close: float

    def to_dict(self) -> Dict[str, float]:
        return {
            "time": self.time.isoformat() + "Z",
            "open": round(self.open, 4),
            "high": round(self.high, 4),
            "low": round(self.low, 4),
            "close": round(self.close, 4),
        }


@dataclass
class StrategySignal:
    """Queued strategy action derived from rule markers."""

    epoch: int
    direction: str


@dataclass
class Leg:
    """Take-profit leg metadata."""

    name: str
    ticks: int
    target_price: float
    status: str = "open"
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None

    def serialize(self) -> Dict[str, Optional[float]]:
        return {
            "name": self.name,
            "ticks": self.ticks,
            "target_price": round(self.target_price, 4),
            "status": self.status,
            "exit_price": None if self.exit_price is None else round(self.exit_price, 4),
            "exit_time": self.exit_time,
        }


@dataclass
class LadderPosition:
    """Track laddered take-profit and stop-loss behaviour for a trade."""

    entry_time: datetime
    entry_price: float
    direction: str
    stop_price: float
    tick_size: float
    legs: List[Leg] = field(default_factory=list)
    breakeven_trigger_ticks: int = 20
    moved_to_breakeven: bool = False
    closed_at: Optional[datetime] = None

    def _apply_leg_fills(self, candle: Candle) -> List[Dict[str, str]]:
        events: List[Dict[str, str]] = []
        ordered = sorted(self.legs, key=lambda leg: leg.ticks)
        if self.direction == "long":
            for leg in ordered:
                if leg.status != "open":
                    continue
                if candle.high >= leg.target_price:
                    leg.status = "target"
                    leg.exit_price = leg.target_price
                    leg.exit_time = candle.time.isoformat() + "Z"
                    events.append({"type": "target", "leg": leg.name})
                    if not self.moved_to_breakeven and leg.ticks >= self.breakeven_trigger_ticks:
                        self.stop_price = self.entry_price
                        self.moved_to_breakeven = True
        else:
            for leg in ordered:
                if leg.status != "open":
                    continue
                if candle.low <= leg.target_price:
                    leg.status = "target"
                    leg.exit_price = leg.target_price
                    leg.exit_time = candle.time.isoformat() + "Z"
                    events.append({"type": "target", "leg": leg.name})
                    if not self.moved_to_breakeven and leg.ticks >= self.breakeven_trigger_ticks:
                        self.stop_price = self.entry_price
                        self.moved_to_breakeven = True
        return events

    def _apply_stop(self, candle: Candle) -> Optional[Dict[str, str]]:
        if self.direction == "long" and candle.low <= self.stop_price:
            for leg in self.legs:
                if leg.status == "open":
                    leg.status = "stop"
                    leg.exit_price = self.stop_price
                    leg.exit_time = candle.time.isoformat() + "Z"
            self.closed_at = candle.time
            return {"type": "stop"}
        if self.direction == "short" and candle.high >= self.stop_price:
            for leg in self.legs:
                if leg.status == "open":
                    leg.status = "stop"
                    leg.exit_price = self.stop_price
                    leg.exit_time = candle.time.isoformat() + "Z"
            self.closed_at = candle.time
            return {"type": "stop"}
        if all(leg.status != "open" for leg in self.legs):
            self.closed_at = candle.time
        return None

    def apply_bar(self, candle: Candle) -> List[Dict[str, str]]:
        """Advance the position with the latest candle."""

        events = self._apply_leg_fills(candle)
        stop_event = self._apply_stop(candle)
        if stop_event:
            events.append(stop_event)
        return events

    def is_active(self) -> bool:
        return self.closed_at is None

    def serialize(self) -> Dict[str, object]:
        return {
            "entry_time": self.entry_time.isoformat() + "Z",
            "entry_price": round(self.entry_price, 4),
            "direction": self.direction,
            "stop_price": round(self.stop_price, 4),
            "moved_to_breakeven": self.moved_to_breakeven,
            "legs": [leg.serialize() for leg in self.legs],
            "closed_at": self.closed_at.isoformat() + "Z" if self.closed_at else None,
        }


class LadderRiskEngine:
    """Create and manage laddered trades for simulated bots."""

    def __init__(self, config: Optional[Dict[str, object]] = None):
        self.config = {**DEFAULT_RISK, **(config or {})}
        self.tick_size = float(self.config.get("tick_size") or DEFAULT_RISK["tick_size"])
        targets: Sequence[int] = self.config.get("targets") or DEFAULT_RISK["targets"]
        self.targets = [int(t) for t in targets]
        self.stop_ticks = int(self.config.get("stop_ticks") or DEFAULT_RISK["stop_ticks"])
        self.breakeven_trigger = int(
            self.config.get("breakeven_trigger_ticks")
            or DEFAULT_RISK["breakeven_trigger_ticks"]
        )
        self.active_trade: Optional[LadderPosition] = None
        self.trades: List[LadderPosition] = []

    def _new_position(self, candle: Candle, direction: str) -> LadderPosition:
        direction = "long" if direction == "long" else "short"
        stop_distance = self.stop_ticks * self.tick_size
        stop_price = (
            candle.close - stop_distance if direction == "long" else candle.close + stop_distance
        )
        legs: List[Leg] = []
        for ticks in self.targets:
            distance = ticks * self.tick_size
            target = candle.close + distance if direction == "long" else candle.close - distance
            legs.append(Leg(name=f"TP{ticks}", ticks=ticks, target_price=target))
        position = LadderPosition(
            entry_time=candle.time,
            entry_price=candle.close,
            direction=direction,
            stop_price=stop_price,
            tick_size=self.tick_size,
            legs=legs,
            breakeven_trigger_ticks=self.breakeven_trigger,
        )
        return position

    def maybe_enter(self, candle: Candle, direction: Optional[str]) -> None:
        if direction is None or self.active_trade is not None:
            return
        self.active_trade = self._new_position(candle, direction)
        self.trades.append(self.active_trade)

    def step(self, candle: Candle) -> None:
        if self.active_trade is None:
            return
        self.active_trade.apply_bar(candle)
        if not self.active_trade.is_active():
            self.active_trade = None

    def serialise_trades(self) -> List[Dict[str, object]]:
        return [trade.serialize() for trade in self.trades]

    def stats(self) -> Dict[str, float]:
        legs = [leg for trade in self.trades for leg in trade.legs]
        wins = sum(1 for leg in legs if leg.status == "target")
        losses = sum(1 for leg in legs if leg.status == "stop")
        total = wins + losses if wins + losses else 1
        long_trades = sum(1 for trade in self.trades if trade.direction == "long")
        short_trades = sum(1 for trade in self.trades if trade.direction == "short")
        return {
            "total_trades": len(self.trades),
            "legs_closed": wins + losses,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / total, 4),
            "long_trades": long_trades,
            "short_trades": short_trades,
        }


@dataclass
class StrategySeries:
    """Runtime payload describing a single strategy stream."""

    strategy_id: str
    name: str
    symbol: str
    timeframe: str
    datasource: Optional[str]
    exchange: Optional[str]
    candles: List[Candle]
    signals: Deque[StrategySignal] = field(default_factory=deque)
    overlays: List[Dict[str, Any]] = field(default_factory=list)
    risk_engine: LadderRiskEngine = field(default_factory=LadderRiskEngine)
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    last_consumed_epoch: int = 0


class BotRuntime:
    """Simulated bot runtime that iterates over real candles and emits stats."""

    def __init__(self, bot_id: str, config: Dict[str, object]):
        self.bot_id = bot_id
        self.config = dict(config)
        self.mode = (self.config.get("mode") or "instant").lower()
        self.run_type = (self.config.get("run_type") or "backtest").lower()
        self.fetch_seconds = self._coerce_fetch_seconds(self.config.get("fetch_seconds"))
        self.state: Dict[str, object] = {"status": "idle", "progress": 0.0, "paused": False}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._paused = False
        self._series: List[StrategySeries] = []
        self._primary_series: Optional[StrategySeries] = None
        self._total_bars: int = 0
        self._bar_index: int = 0
        self._prepared: bool = False
        self._chart_overlays: List[Dict[str, Any]] = []
        self._last_stats: Dict[str, Any] = {}
        self._next_bar_at: Optional[datetime] = None
        self._live_mode = self.run_type == "sim_trade"

    @staticmethod
    def _coerce_fetch_seconds(value: Optional[object]) -> float:
        try:
            numeric = float(value) if value is not None else 1.0
        except (TypeError, ValueError):
            numeric = 1.0
        return numeric if numeric >= 0 else 0.0

    def _ensure_prepared(self) -> None:
        if self._prepared:
            return
        with self._lock:
            self.state.update({"status": "initialising", "progress": 0.0, "paused": False})
        meta = self.config.get("strategies_meta")
        if not meta:
            raise ValueError("Runtime requires strategy metadata to initialise")
        streams = self._build_series(meta)
        if not streams:
            raise ValueError("No strategy streams could be prepared for this bot")
        self._series = streams
        self._primary_series = self._series[0]
        self._total_bars = len(self._primary_series.candles)
        self._bar_index = 0
        self._chart_overlays = [overlay for series in self._series for overlay in series.overlays]
        self._prepared = True
        with self._lock:
            self.state.update({"status": "idle", "progress": 0.0, "paused": False})

    def _build_series(self, strategies: Sequence[Mapping[str, Any]]) -> List[StrategySeries]:
        series_list: List[StrategySeries] = []
        for strategy in strategies:
            try:
                stream = self._build_series_for_strategy(strategy)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("bot_runtime_series_failed | bot=%s | strategy=%s | error=%s", self.bot_id, strategy.get("id"), exc)
                continue
            if stream:
                series_list.append(stream)
        return series_list

    def _build_series_for_strategy(self, strategy: Mapping[str, Any]) -> Optional[StrategySeries]:
        symbol = self._resolve_symbol(strategy)
        if not symbol:
            logger.warning("bot_runtime_missing_symbol | bot=%s | strategy=%s", self.bot_id, strategy.get("id"))
            return None
        timeframe = self._resolve_timeframe(strategy)
        datasource = self._resolve_datasource(strategy)
        exchange = self._resolve_exchange(strategy)
        if self.run_type == "backtest":
            start_iso = self.config.get("backtest_start")
            end_iso = self.config.get("backtest_end")
            if not start_iso or not end_iso:
                start_iso, end_iso = self._resolve_live_window()
        else:
            start_iso, end_iso = self._resolve_live_window()

        df = fetch_ohlcv(
            symbol,
            start_iso,
            end_iso,
            timeframe,
            datasource=datasource,
            exchange=exchange,
        )
        if df is None or df.empty:
            logger.warning(
                "bot_runtime_no_candles | bot=%s | strategy=%s | symbol=%s | timeframe=%s",
                self.bot_id,
                strategy.get("id"),
                symbol,
                timeframe,
            )
            return None

        candles = self._build_candles(df)
        if not candles:
            return None

        try:
            evaluation = strategy_service.evaluate(
                strategy_id=strategy.get("id"),
                start=start_iso,
                end=end_iso,
                interval=timeframe,
                symbol=symbol,
                datasource=datasource,
                exchange=exchange,
                config={"mode": self.run_type},
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(
                "bot_runtime_strategy_eval_failed | bot=%s | strategy=%s | error=%s",
                self.bot_id,
                strategy.get("id"),
                exc,
            )
            return None

        overlays = self._extract_indicator_overlays(evaluation)
        signals = self._build_signals_from_markers(evaluation.get("chart_markers") or {})
        risk_engine = LadderRiskEngine(self.config.get("risk"))

        return StrategySeries(
            strategy_id=str(strategy.get("id")),
            name=strategy.get("name") or str(strategy.get("id")) or "strategy",
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            candles=candles,
            signals=signals,
            overlays=overlays,
            risk_engine=risk_engine,
            window_start=start_iso,
            window_end=end_iso,
            meta=dict(strategy),
        )

    @staticmethod
    def _resolve_symbol(strategy: Mapping[str, Any]) -> Optional[str]:
        symbols = strategy.get("symbols") or []
        if symbols:
            return str(symbols[0])
        return strategy.get("symbol") or None

    def _resolve_timeframe(self, strategy: Mapping[str, Any]) -> str:
        return str(strategy.get("timeframe") or self.config.get("timeframe") or "15m")

    def _resolve_datasource(self, strategy: Mapping[str, Any]) -> Optional[str]:
        return self.config.get("datasource") or strategy.get("datasource")

    def _resolve_exchange(self, strategy: Mapping[str, Any]) -> Optional[str]:
        return self.config.get("exchange") or strategy.get("exchange")

    def _resolve_live_window(self) -> Tuple[str, str]:
        lookback_days = int(self.config.get("sim_lookback_days") or DEFAULT_SIM_LOOKBACK_DAYS)
        lookback_days = max(lookback_days, 1)
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=lookback_days)
        return start_dt.isoformat() + "Z", end_dt.isoformat() + "Z"

    @staticmethod
    def _build_candles(df: pd.DataFrame) -> List[Candle]:
        frame = df.copy()
        frame.index = pd.to_datetime(frame.index, utc=True)
        candles: List[Candle] = []
        for ts, row in frame.iterrows():
            try:
                open_price = float(row.get("open", row.get("Open")))
                high_price = float(row.get("high", row.get("High")))
                low_price = float(row.get("low", row.get("Low")))
                close_price = float(row.get("close", row.get("Close")))
            except (TypeError, ValueError):
                continue
            candles.append(
                Candle(
                    time=ts.to_pydatetime(),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                )
            )
        return candles

    @staticmethod
    def _build_signals_from_markers(markers: Mapping[str, Any]) -> Deque[StrategySignal]:
        queued: List[StrategySignal] = []
        for entry in markers.get("buy", []) or []:
            epoch = BotRuntime._normalise_epoch(entry.get("time"))
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="long"))
        for entry in markers.get("sell", []) or []:
            epoch = BotRuntime._normalise_epoch(entry.get("time"))
            if epoch is not None:
                queued.append(StrategySignal(epoch=epoch, direction="short"))
        queued.sort(key=lambda signal: signal.epoch)
        return deque(queued)

    @staticmethod
    def _normalise_epoch(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            return int(text)
        try:
            return int(float(text))
        except (TypeError, ValueError):
            pass
        try:
            if text.endswith("Z"):
                text = text[:-1]
            parsed = datetime.fromisoformat(text)
            return int(parsed.timestamp())
        except ValueError:
            return None

    @staticmethod
    def _extract_indicator_overlays(result: Mapping[str, Any]) -> List[Dict[str, Any]]:
        overlays: List[Dict[str, Any]] = []
        indicator_results = result.get("indicator_results") if isinstance(result, Mapping) else None
        if not isinstance(indicator_results, Mapping):
            return overlays
        for payload in indicator_results.values():
            if isinstance(payload, Mapping):
                overlays.extend(payload.get("overlays", []) or [])
        return overlays

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

    def _run(self) -> None:
        try:
            self._execute_loop()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("bot_runtime_loop_failed | bot=%s | error=%s", self.bot_id, exc)
            with self._lock:
                self.state.update({"status": "error", "error": str(exc)})

    def _execute_loop(self) -> None:
        self._ensure_prepared()
        status = "running"
        while not self._stop.is_set():
            if self._bar_index >= self._total_bars:
                if self._live_mode and self._append_live_candles_if_needed():
                    continue
                break
            if not self._pause_event.wait(timeout=0.2):
                continue
            series = self._primary_series
            if not series or not series.candles:
                break
            self._apply_bar(self._bar_index)
            self._sleep_between_bars()
        if self._stop.is_set():
            status = "stopped"
        elif not self._live_mode:
            status = "completed"
        self._next_bar_at = None
        if self._primary_series and self._primary_series.candles:
            self._update_state(self._primary_series.candles[-1], status=status)
        else:
            with self._lock:
                self.state.update({"status": status})

    def _apply_bar(self, index: int) -> None:
        for series in self._series:
            if index >= len(series.candles):
                continue
            candle = series.candles[index]
            epoch = int(candle.time.timestamp())
            direction = self._next_signal_for(series, epoch)
            if direction:
                series.risk_engine.maybe_enter(candle, direction)
            series.risk_engine.step(candle)
            series.last_consumed_epoch = max(series.last_consumed_epoch, epoch)
        self._bar_index = index + 1
        primary = self._primary_series
        if primary and primary.candles:
            candle = primary.candles[min(index, len(primary.candles) - 1)]
            self._update_state(candle)

    def _next_signal_for(self, series: StrategySeries, epoch: int) -> Optional[str]:
        direction: Optional[str] = None
        while series.signals and series.signals[0].epoch <= epoch:
            direction = series.signals.popleft().direction
        return direction

    def _sleep_between_bars(self) -> None:
        if self.mode != "walk-forward" or self.fetch_seconds <= 0:
            return
        self._next_bar_at = datetime.utcnow() + timedelta(seconds=self.fetch_seconds)
        target = time.time() + self.fetch_seconds
        while not self._stop.is_set():
            if not self._pause_event.wait(timeout=0.2):
                continue
            remaining = target - time.time()
            if remaining <= 0:
                break
            time.sleep(min(0.25, remaining))

    def _append_live_candles_if_needed(self) -> bool:
        updated = False
        end_iso = datetime.utcnow().isoformat() + "Z"
        for series in self._series:
            last_time = series.candles[-1].time if series.candles else None
            if last_time is None:
                continue
            start_iso = (last_time + timedelta(seconds=1)).isoformat() + "Z"
            if self._append_series_updates(series, start_iso, end_iso):
                updated = True
        if updated:
            primary = self._primary_series
            if primary:
                self._total_bars = len(primary.candles)
            self._chart_overlays = [overlay for series in self._series for overlay in series.overlays]
        return updated

    def _append_series_updates(self, series: StrategySeries, start_iso: str, end_iso: str) -> bool:
        df = fetch_ohlcv(
            series.symbol,
            start_iso,
            end_iso,
            series.timeframe,
            datasource=series.datasource,
            exchange=series.exchange,
        )
        if df is None or df.empty:
            return False
        new_candles = [c for c in self._build_candles(df) if not series.candles or c.time > series.candles[-1].time]
        if not new_candles:
            return False
        series.candles.extend(new_candles)
        try:
            evaluation = strategy_service.evaluate(
                strategy_id=series.strategy_id,
                start=series.window_start or start_iso,
                end=end_iso,
                interval=series.timeframe,
                symbol=series.symbol,
                datasource=series.datasource,
                exchange=series.exchange,
                config={"mode": self.run_type},
            )
            series.overlays = self._extract_indicator_overlays(evaluation)
            signals = self._build_signals_from_markers(evaluation.get("chart_markers") or {})
            while signals and signals[0].epoch <= series.last_consumed_epoch:
                signals.popleft()
            series.signals = signals
            series.window_end = end_iso
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(
                "bot_runtime_refresh_failed | bot=%s | strategy=%s | error=%s",
                self.bot_id,
                series.strategy_id,
                exc,
            )
        return True

    def pause(self) -> None:
        if not self._prepared:
            return
        self._paused = True
        self._pause_event.clear()
        self._next_bar_at = None
        with self._lock:
            self.state.update({"status": "paused", "paused": True, "next_bar_at": None, "next_bar_in_seconds": None})

    def resume(self) -> None:
        if not self._prepared:
            return
        self._paused = False
        self._pause_event.set()
        with self._lock:
            if self.state.get("status") == "paused":
                self.state.update({"status": "running", "paused": False})

    def stop(self) -> None:
        self._stop.set()
        self._pause_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.2)
        with self._lock:
            self.state.update({"status": "stopped", "paused": False})
        self._next_bar_at = None

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
            "legs_closed": 0,
            "wins": 0,
            "losses": 0,
            "long_trades": 0,
            "short_trades": 0,
        }
        for series in self._series:
            stats = series.risk_engine.stats()
            for key in summary:
                try:
                    summary[key] += int(stats.get(key, 0) or 0)
                except (TypeError, ValueError):
                    continue
        total = summary["wins"] + summary["losses"]
        summary["win_rate"] = round(summary["wins"] / total, 4) if total else 0.0
        return summary

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
            "next_bar_at": self._next_bar_at.isoformat() + "Z" if self._next_bar_at else None,
            "next_bar_in_seconds": self._seconds_until_next_bar(),
        }
        with self._lock:
            self.state.update(snapshot)

    def _seconds_until_next_bar(self) -> Optional[float]:
        if not self._next_bar_at:
            return None
        delta = (self._next_bar_at - datetime.utcnow()).total_seconds()
        return round(delta, 2) if delta > 0 else 0.0

    def snapshot(self) -> Dict[str, object]:
        """Return a thread-safe snapshot of runtime state."""

        self._ensure_prepared()
        with self._lock:
            payload = dict(self.state)
        payload.setdefault("stats", self._last_stats)
        if "next_bar_in_seconds" not in payload:
            payload["next_bar_in_seconds"] = self._seconds_until_next_bar()
        return payload

    def chart_payload(self) -> Dict[str, object]:
        """Return the latest candle, trade, overlay, and stat data for the lens."""

        self._ensure_prepared()
        primary = self._primary_series
        candles: List[Dict[str, Any]] = []
        if primary and primary.candles:
            status = self.state.get("status")
            if status in {"idle", "initialising"}:
                visible = len(primary.candles)
            elif status in {"completed", "stopped"}:
                visible = len(primary.candles)
            else:
                visible = min(self._bar_index, len(primary.candles))
            visible = max(1, visible)
            candles = [candle.to_dict() for candle in primary.candles[:visible]]
        return {
            "candles": candles,
            "trades": self._aggregate_trades(),
            "stats": self._last_stats or self._aggregate_stats(),
            "overlays": list(self._chart_overlays),
        }


__all__ = [
    "BotRuntime",
    "DEFAULT_RISK",
]

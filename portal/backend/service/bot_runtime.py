"""Backtesting runtime with ladder risk logic for bot simulations."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from queue import Empty, Full, Queue
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from . import indicator_service, strategy_service
from .atm import DEFAULT_ATM_TEMPLATE, merge_templates
from .candle_service import fetch_ohlcv


logger = logging.getLogger(__name__)


DEFAULT_RISK = {
    "contracts": 3,
    "targets": [20, 40, 60],
    "stop_ticks": 30,
    "breakeven_trigger_ticks": 20,
    "tick_size": 0.01,
}

MAX_LOG_ENTRIES = 500


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    """Return a UTC ISO8601 string with Z suffix for *value*."""

    if value is None:
        return None
    target = value
    if target.tzinfo is None:
        return target.replace(tzinfo=None).isoformat() + "Z"
    return target.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _coerce_float(value: Optional[object], default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric


def _instrument_key(datasource: Optional[str], exchange: Optional[str], symbol: Optional[str]) -> str:
    return "::".join(
        [
            (datasource or "").strip().lower(),
            (exchange or "").strip().lower(),
            (symbol or "").strip().upper(),
        ]
    )


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
            "time": _isoformat(self.time),
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
    contracts: int = 1
    pnl: float = 0.0

    def serialize(self) -> Dict[str, Optional[float]]:
        return {
            "name": self.name,
            "ticks": self.ticks,
            "target_price": round(self.target_price, 4),
            "status": self.status,
            "exit_price": None if self.exit_price is None else round(self.exit_price, 4),
            "exit_time": self.exit_time,
            "contracts": self.contracts,
            "pnl": round(self.pnl, 4),
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
    tick_value: float = 1.0
    contract_size: float = 1.0
    maker_fee_rate: float = 0.0
    taker_fee_rate: float = 0.0
    quote_currency: str = "USD"
    moved_to_breakeven: bool = False
    closed_at: Optional[datetime] = None
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    gross_pnl: float = 0.0
    fees_paid: float = 0.0
    net_pnl: float = 0.0

    def register_entry_fee(self) -> None:
        total_contracts = sum(max(leg.contracts, 0) for leg in self.legs) or 1
        self._apply_fee(self.entry_price, total_contracts)

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
                    leg.exit_time = _isoformat(candle.time)
                    pnl = self._pnl_for_exit(leg.target_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(leg.target_price, leg.contracts)
                    events.append(
                        {
                            "type": "target",
                            "leg": leg.name,
                            "trade_id": self.trade_id,
                            "price": round(leg.target_price, 4),
                            "time": leg.exit_time,
                            "pnl": round(pnl, 4),
                            "currency": self.quote_currency,
                        }
                    )
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
                    leg.exit_time = _isoformat(candle.time)
                    pnl = self._pnl_for_exit(leg.target_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(leg.target_price, leg.contracts)
                    events.append(
                        {
                            "type": "target",
                            "leg": leg.name,
                            "trade_id": self.trade_id,
                            "price": round(leg.target_price, 4),
                            "time": leg.exit_time,
                            "pnl": round(pnl, 4),
                            "currency": self.quote_currency,
                        }
                    )
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
                    leg.exit_time = _isoformat(candle.time)
                    pnl = self._pnl_for_exit(self.stop_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(self.stop_price, leg.contracts)
            self.closed_at = candle.time
            return {
                "type": "stop",
                "trade_id": self.trade_id,
                "price": round(self.stop_price, 4),
                "time": _isoformat(candle.time),
                "currency": self.quote_currency,
            }
        if self.direction == "short" and candle.high >= self.stop_price:
            for leg in self.legs:
                if leg.status == "open":
                    leg.status = "stop"
                    leg.exit_price = self.stop_price
                    leg.exit_time = _isoformat(candle.time)
                    pnl = self._pnl_for_exit(self.stop_price, leg.contracts)
                    leg.pnl = pnl
                    self._record_pnl(pnl)
                    self._apply_fee(self.stop_price, leg.contracts)
            self.closed_at = candle.time
            return {
                "type": "stop",
                "trade_id": self.trade_id,
                "price": round(self.stop_price, 4),
                "time": _isoformat(candle.time),
                "currency": self.quote_currency,
            }
        if all(leg.status != "open" for leg in self.legs):
            self.closed_at = candle.time
        return None

    def apply_bar(self, candle: Candle) -> List[Dict[str, str]]:
        """Advance the position with the latest candle."""

        events = self._apply_leg_fills(candle)
        stop_event = self._apply_stop(candle)
        if stop_event:
            events.append(stop_event)
        if not self.is_active():
            events.append(
                {
                    "type": "close",
                    "trade_id": self.trade_id,
                    "time": _isoformat(self.closed_at or candle.time),
                    "gross_pnl": round(self.gross_pnl, 4),
                    "fees_paid": round(self.fees_paid, 4),
                    "net_pnl": round(self.net_pnl, 4),
                    "currency": self.quote_currency,
                }
            )
        return events

    def is_active(self) -> bool:
        return self.closed_at is None

    def serialize(self) -> Dict[str, object]:
        return {
            "trade_id": self.trade_id,
            "entry_time": _isoformat(self.entry_time),
            "entry_price": round(self.entry_price, 4),
            "direction": self.direction,
            "stop_price": round(self.stop_price, 4),
            "moved_to_breakeven": self.moved_to_breakeven,
            "legs": [leg.serialize() for leg in self.legs],
            "closed_at": _isoformat(self.closed_at),
            "tick_size": self.tick_size,
            "tick_value": round(self.tick_value, 6),
            "contract_size": round(self.contract_size, 6),
            "gross_pnl": round(self.gross_pnl, 4),
            "fees_paid": round(self.fees_paid, 4),
            "net_pnl": round(self.net_pnl, 4),
            "currency": self.quote_currency,
        }

    def _pnl_for_exit(self, exit_price: float, contracts: int) -> float:
        if contracts <= 0:
            return 0.0
        direction = 1 if self.direction == "long" else -1
        ticks = ((exit_price - self.entry_price) / self.tick_size) * direction
        return ticks * self.tick_value * contracts

    def _apply_fee(self, price: float, contracts: int) -> None:
        if contracts <= 0:
            return
        notional = abs(price * self.contract_size * contracts)
        fee_rate = self.taker_fee_rate or 0.0
        fee = notional * fee_rate
        if fee:
            self.fees_paid += fee
            self._update_net()

    def _record_pnl(self, pnl: float) -> None:
        self.gross_pnl += pnl
        self._update_net()

    def _update_net(self) -> None:
        self.net_pnl = self.gross_pnl - self.fees_paid


class LadderRiskEngine:
    """Create and manage laddered trades for simulated bots."""

    def __init__(
        self,
        config: Optional[Dict[str, object]] = None,
        instrument: Optional[Dict[str, Any]] = None,
    ):
        self.template = merge_templates(config)
        self.instrument = instrument or {}
        instrument_tick = _coerce_float(self.instrument.get("tick_size"))
        config_tick = _coerce_float(self.template.get("tick_size"), DEFAULT_RISK["tick_size"])
        self.tick_size = float(instrument_tick or config_tick or DEFAULT_RISK["tick_size"])
        self.orders = self._orders_from_template()
        self.targets = [int(order["ticks"]) for order in self.orders]
        self.stop_ticks = int(self.template.get("stop_ticks") or DEFAULT_RISK["stop_ticks"])
        self.breakeven_trigger = self._breakeven_threshold()
        self.contract_size = _coerce_float(self.instrument.get("contract_size"), 1.0) or 1.0
        tick_value = _coerce_float(self.instrument.get("tick_value"))
        if tick_value is None:
            tick_value = self.tick_size * self.contract_size
        self.tick_value = float(tick_value or self.tick_size)
        self.quote_currency = (self.instrument.get("quote_currency") or "USD").upper()
        self.maker_fee = _coerce_float(self.instrument.get("maker_fee_rate"), 0.0) or 0.0
        self.taker_fee = _coerce_float(self.instrument.get("taker_fee_rate"), 0.0) or 0.0
        self.active_trade: Optional[LadderPosition] = None
        self.trades: List[LadderPosition] = []
        logger.info(
            "ladder_risk_configured | targets=%s | stop_ticks=%s | tick=%.5f | instrument=%s",
            ",".join(str(order["ticks"]) for order in self.orders),
            self.stop_ticks,
            self.tick_size,
            self.instrument.get("symbol"),
        )

    def _orders_from_template(self) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []
        entries = self.template.get("take_profit_orders") or []
        for idx, entry in enumerate(entries):
            ticks = _coerce_float(entry.get("ticks"))
            if ticks is None:
                continue
            label = entry.get("label") or f"Target {idx + 1}"
            contracts = int(entry.get("contracts") or 0)
            orders.append(
                {
                    "label": label,
                    "ticks": int(ticks),
                    "contracts": max(contracts, 1),
                }
            )
        if orders:
            return orders

        fallback_targets: Sequence[int] = (
            self.template.get("targets")
            or DEFAULT_RISK.get("targets")
            or [20, 40, 60]
        )
        total_contracts = int(self.template.get("contracts") or len(fallback_targets) or 1)
        distribution = self._distribute_contracts(len(fallback_targets), total_contracts)
        built: List[Dict[str, Any]] = []
        for idx, ticks in enumerate(fallback_targets):
            built.append(
                {
                    "label": f"TP +{int(ticks)}",
                    "ticks": int(ticks),
                    "contracts": distribution[idx] if idx < len(distribution) else 1,
                }
            )
        return built

    @staticmethod
    def _distribute_contracts(count: int, total: int) -> List[int]:
        if count <= 0:
            return []
        slots = [0 for _ in range(count)]
        total = total if total > 0 else count
        for idx in range(total):
            slots[idx % count] += 1
        return slots

    def _breakeven_threshold(self) -> int:
        config = self.template.get("breakeven") or {}
        ticks = _coerce_float(config.get("ticks"))
        if ticks and ticks > 0:
            return int(ticks)
        if config.get("target_index") is not None and self.targets:
            index = max(0, min(int(config.get("target_index") or 0), len(self.targets) - 1))
            return int(self.targets[index])
        fallback = self.template.get("breakeven_trigger_ticks")
        value = _coerce_float(fallback) or DEFAULT_RISK["breakeven_trigger_ticks"]
        return int(value)

    def _new_position(self, candle: Candle, direction: str) -> LadderPosition:
        direction = "long" if direction == "long" else "short"
        stop_distance = self.stop_ticks * self.tick_size
        stop_price = (
            candle.close - stop_distance if direction == "long" else candle.close + stop_distance
        )
        legs: List[Leg] = []
        for idx, order in enumerate(self.orders):
            ticks = order.get("ticks", 0)
            distance = ticks * self.tick_size
            target = candle.close + distance if direction == "long" else candle.close - distance
            legs.append(
                Leg(
                    name=order.get("label") or f"TP{ticks}",
                    ticks=ticks,
                    target_price=target,
                    contracts=order.get("contracts", 1),
                )
            )
        position = LadderPosition(
            entry_time=candle.time,
            entry_price=candle.close,
            direction=direction,
            stop_price=stop_price,
            tick_size=self.tick_size,
            legs=legs,
            breakeven_trigger_ticks=self.breakeven_trigger,
            tick_value=self.tick_value,
            contract_size=self.contract_size,
            maker_fee_rate=self.maker_fee,
            taker_fee_rate=self.taker_fee,
            quote_currency=self.quote_currency,
        )
        position.register_entry_fee()
        return position

    def maybe_enter(self, candle: Candle, direction: Optional[str]) -> Optional[LadderPosition]:
        if direction is None or self.active_trade is not None:
            return None
        self.active_trade = self._new_position(candle, direction)
        self.trades.append(self.active_trade)
        return self.active_trade

    def step(self, candle: Candle) -> List[Dict[str, Any]]:
        if self.active_trade is None:
            return []
        events = self.active_trade.apply_bar(candle)
        if not self.active_trade.is_active():
            self.active_trade = None
        return events

    def serialise_trades(self) -> List[Dict[str, object]]:
        return [trade.serialize() for trade in self.trades]

    def stats(self) -> Dict[str, float]:
        legs = [leg for trade in self.trades for leg in trade.legs]
        wins = sum(1 for leg in legs if leg.status == "target")
        losses = sum(1 for leg in legs if leg.status == "stop")
        total = wins + losses if wins + losses else 1
        long_trades = sum(1 for trade in self.trades if trade.direction == "long")
        short_trades = sum(1 for trade in self.trades if trade.direction == "short")
        gross = sum(trade.gross_pnl for trade in self.trades)
        fees = sum(trade.fees_paid for trade in self.trades)
        net = gross - fees
        return {
            "total_trades": len(self.trades),
            "legs_closed": wins + losses,
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / total, 4),
            "long_trades": long_trades,
            "short_trades": short_trades,
            "gross_pnl": round(gross, 4),
            "fees_paid": round(fees, 4),
            "net_pnl": round(net, 4),
            "quote_currency": self.quote_currency,
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
    instrument: Optional[Dict[str, Any]] = None
    atm_template: Dict[str, Any] = field(default_factory=dict)


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
        self._logs: Deque[Dict[str, Any]] = deque(maxlen=MAX_LOG_ENTRIES)
        self._subscribers: Dict[str, Queue] = {}
        self._state_callback = state_callback

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
        self._log_event("prepared", total_bars=self._total_bars)
        self._push_update("prepared")

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
            evaluation = strategy_service.generate_strategy_signals(
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
        instrument = self._instrument_for(datasource, exchange, symbol)
        bot_override = self.config.get("risk") or {}
        override_payload = bot_override if bot_override and bot_override != DEFAULT_ATM_TEMPLATE else None
        atm_template = merge_templates(
            strategy.get("atm_template"),
            override_payload,
        )
        if instrument and instrument.get("tick_size") and not atm_template.get("tick_size"):
            atm_template["tick_size"] = instrument.get("tick_size")
        risk_engine = LadderRiskEngine(atm_template, instrument=instrument)
        series_meta = dict(strategy)
        if instrument:
            series_meta["instrument"] = instrument
        series_meta["atm_template"] = atm_template
        logger.info(
            "bot_runtime_series_ready | bot=%s | strategy=%s | contracts=%s | targets=%s",
            self.bot_id,
            strategy.get("id"),
            atm_template.get("contracts"),
            ",".join(str(order.get("ticks")) for order in atm_template.get("take_profit_orders", [])),
        )

        return StrategySeries(
            strategy_id=str(strategy.get("id")),
            name=strategy.get("name") or str(strategy.get("id")) or "strategy",
            symbol=symbol,
            timeframe=timeframe,
            datasource=datasource,
            exchange=exchange,
            candles=candles,
            signals=signals,
            overlays=overlays
            + self._indicator_overlay_entries(
                strategy,
                start_iso,
                end_iso,
                timeframe,
                symbol,
                datasource,
                exchange,
            ),
            risk_engine=risk_engine,
            window_start=start_iso,
            window_end=end_iso,
            meta=series_meta,
            instrument=instrument,
            atm_template=atm_template,
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

    def _instrument_for(
        self,
        datasource: Optional[str],
        exchange: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        index = self.config.get("instrument_index") or {}
        if not symbol:
            return None
        keys = [
            _instrument_key(datasource, exchange, symbol),
            _instrument_key(datasource, None, symbol),
            _instrument_key(None, exchange, symbol),
            _instrument_key(None, None, symbol),
        ]
        for key in keys:
            if key in index:
                return index[key]
        return None

    def _resolve_live_window(self) -> Tuple[str, str]:
        lookback_days = int(self.config.get("sim_lookback_days") or DEFAULT_SIM_LOOKBACK_DAYS)
        lookback_days = max(lookback_days, 1)
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=lookback_days)
        return _isoformat(start_dt), _isoformat(end_dt)

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
        overlays: List[Dict[str, Any]] = []
        strategy_meta = strategy or {}
        links = list(strategy_meta.get("indicator_links") or [])
        if not links and strategy_meta.get("indicator_ids"):
            links = [
                {"indicator_id": indicator_id}
                for indicator_id in strategy_meta.get("indicator_ids")
                if indicator_id
            ]
        seen: set[str] = set()
        for link in links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id or indicator_id in seen:
                continue
            seen.add(indicator_id)
            snapshot = dict(link.get("indicator_snapshot") or {})
            params = dict(snapshot.get("params") or {})
            indicator_type = snapshot.get("type") or link.get("indicator_type") or "indicator"
            color = snapshot.get("color") or link.get("color")
            window_symbol = symbol or params.get("symbol")
            interval = params.get("interval") or timeframe or self.config.get("timeframe") or "15m"
            ds = link.get("datasource") or snapshot.get("datasource") or params.get("datasource") or datasource
            ex = link.get("exchange") or snapshot.get("exchange") or params.get("exchange") or exchange
            try:
                payload = indicator_service.overlays_for_instance(
                    indicator_id,
                    start=start_iso,
                    end=end_iso,
                    interval=str(interval),
                    symbol=window_symbol,
                    datasource=ds,
                    exchange=ex,
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.debug(
                    "bot_indicator_overlay_failed | bot=%s | strategy=%s | indicator=%s | error=%s",
                    self.bot_id,
                    strategy_meta.get("id"),
                    indicator_id,
                    exc,
                )
                continue
            overlays.append(
                {
                    "ind_id": indicator_id,
                    "type": indicator_type,
                    "payload": payload,
                    "color": color,
                    "source": "indicator",
                }
            )
        return overlays

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
        # Indicator results include overlays that visualize raw signal markers.
        # The bot lens should only render the strategy's configured indicator
        # overlays, so skip signal-driven visuals entirely.
        return []

    def reset(self) -> None:
        """Clear cached series so the runtime can restart fresh."""

        if self._thread and self._thread.is_alive():
            raise RuntimeError("Cannot reset a running bot runtime")
        with self._lock:
            self._prepared = False
            self._series = []
            self._primary_series = None
            self._total_bars = 0
            self._bar_index = 0
            self._chart_overlays = []
            self._last_stats = {}
            self._next_bar_at = None
            self._logs.clear()
            self.state = {"status": "idle", "progress": 0.0, "paused": False}
        self._stop.clear()
        self._pause_event.set()
        self._paused = False

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
        self._log_event(status, message=f"Bot runtime {status}")
        if self._primary_series and self._primary_series.candles:
            self._update_state(self._primary_series.candles[-1], status=status)
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
                )
            trade_events = series.risk_engine.step(candle)
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
                )
            series.last_consumed_epoch = max(series.last_consumed_epoch, epoch)
        self._bar_index = index + 1
        primary = self._primary_series
        if primary and primary.candles:
            candle = primary.candles[min(index, len(primary.candles) - 1)]
            self._update_state(candle)
        self._push_update("bar")

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
        end_iso = _isoformat(datetime.utcnow())
        for series in self._series:
            last_time = series.candles[-1].time if series.candles else None
            if last_time is None:
                continue
            start_iso = _isoformat(last_time + timedelta(seconds=1))
            if self._append_series_updates(series, start_iso, end_iso):
                updated = True
        if updated:
            primary = self._primary_series
            if primary:
                self._total_bars = len(primary.candles)
            self._chart_overlays = [overlay for series in self._series for overlay in series.overlays]
            self._log_event("live_refresh", message="Appended live candles")
            self._push_update("live_refresh")
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
            overlays = self._extract_indicator_overlays(evaluation)
            overlays.extend(
                self._indicator_overlay_entries(
                    series.meta or {},
                    series.window_start or start_iso,
                    end_iso,
                    series.timeframe,
                    series.symbol,
                    series.datasource,
                    series.exchange,
                )
            )
            series.overlays = overlays
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
        self._log_event("pause", message="Bot paused")
        self._push_update("pause")

    def resume(self) -> None:
        if not self._prepared:
            return
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
            "legs_closed": 0,
            "wins": 0,
            "losses": 0,
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
        total = summary["wins"] + summary["losses"]
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
            "timestamp": _isoformat(datetime.utcnow()),
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

    def _persist_runtime_state(self, status: str) -> None:
        """Send completion metadata back to the service layer for persistence."""

        if not self._state_callback:
            return
        payload = {
            "status": status,
            "last_stats": dict(self._last_stats or {}),
            "last_run_at": _isoformat(datetime.utcnow()),
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
        primary = self._primary_series
        candles: List[Dict[str, Any]] = []
        if not primary or not primary.candles:
            return candles
        status = self.state.get("status")
        if status in {"idle", "initialising"}:
            visible = len(primary.candles)
        elif status in {"completed", "stopped"}:
            visible = len(primary.candles)
        else:
            visible = min(self._bar_index, len(primary.candles))
        visible = max(1, visible)
        candles = [candle.to_dict() for candle in primary.candles[:visible]]
        return candles

    def _current_epoch(self) -> Optional[int]:
        primary = self._primary_series
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
        overlays = list(self._chart_overlays)
        if not overlays:
            return []
        current_epoch = self._current_epoch()
        status = str(self.state.get("status") or "").lower()
        if current_epoch is None:
            # Hide overlays until the bot has advanced at least one bar.
            if status in {"idle", "initialising"}:
                return []
            return overlays

        visible: List[Dict[str, Any]] = []
        for overlay in overlays:
            trimmed = self._trim_overlay_to_epoch(overlay, current_epoch)
            if trimmed and self._overlay_is_ready(trimmed, current_epoch):
                visible.append(trimmed)
        return visible

    @staticmethod
    def _overlay_is_ready(overlay: Mapping[str, Any], current_epoch: int) -> bool:
        if not isinstance(overlay, Mapping):
            return False
        overlay_type = str(overlay.get("type") or "").lower()
        if overlay_type not in {"market_profile", "mpf"}:
            return True
        payload = overlay.get("payload") if isinstance(overlay.get("payload"), Mapping) else {}
        boxes = payload.get("boxes") if isinstance(payload, Mapping) else None
        if not boxes:
            return True
        latest_needed: Optional[int] = None
        for box in boxes:
            if not isinstance(box, Mapping):
                continue
            end_epoch = BotRuntime._normalise_epoch(
                box.get("end") or box.get("end_date") or box.get("endDate")
            )
            if end_epoch is None:
                end_epoch = BotRuntime._normalise_epoch(box.get("x2"))
            if end_epoch is None:
                end_epoch = BotRuntime._normalise_epoch(box.get("x1"))
            if end_epoch is None:
                continue
            if latest_needed is None or end_epoch > latest_needed:
                latest_needed = end_epoch
        if latest_needed is None:
            return True
        return current_epoch >= latest_needed

    @staticmethod
    def _trim_overlay_to_epoch(overlay: Mapping[str, Any], current_epoch: int) -> Optional[Dict[str, Any]]:
        if not isinstance(overlay, Mapping):
            return None
        payload = overlay.get("payload")
        if not isinstance(payload, Mapping):
            return dict(overlay)
        trimmed_payload, has_content = BotRuntime._trim_overlay_payload(payload, current_epoch)
        if not has_content:
            return None
        if trimmed_payload is payload:
            return dict(overlay)
        trimmed = dict(overlay)
        trimmed["payload"] = trimmed_payload
        return trimmed

    @staticmethod
    def _trim_overlay_payload(payload: Mapping[str, Any], current_epoch: int) -> Tuple[Mapping[str, Any], bool]:
        if not isinstance(payload, Mapping):
            return payload, True
        trimmed: Dict[str, Any] = dict(payload)
        changed = False

        def process_list(key: str, filter_fn: Callable[[Any], Optional[Any]]) -> None:
            nonlocal changed
            entries = payload.get(key)
            if not isinstance(entries, list):
                return
            new_entries: List[Any] = []
            entry_changed = False
            for entry in entries:
                filtered = filter_fn(entry)
                if filtered is None:
                    entry_changed = True
                    continue
                new_entries.append(filtered)
                if filtered is not entry:
                    entry_changed = True
            if entry_changed or len(new_entries) != len(entries):
                trimmed[key] = new_entries
                changed = True
            else:
                trimmed[key] = entries

        process_list("price_lines", lambda entry: BotRuntime._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("markers", lambda entry: BotRuntime._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("touchPoints", lambda entry: BotRuntime._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("touch_points", lambda entry: BotRuntime._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("bubbles", lambda entry: BotRuntime._trim_time_entry(entry, current_epoch, ("time",)))
        process_list("segments", lambda entry: BotRuntime._trim_segment_entry(entry, current_epoch))
        process_list("polylines", lambda entry: BotRuntime._trim_polyline_entry(entry, current_epoch))
        process_list("boxes", lambda entry: BotRuntime._trim_box_entry(entry, current_epoch))

        has_content = BotRuntime._payload_has_content(trimmed)
        return (trimmed if changed else payload, has_content)

    @staticmethod
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

    @staticmethod
    def _trim_time_entry(entry: Any, current_epoch: int, keys: Tuple[str, ...]) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        epoch = BotRuntime._first_epoch_from(entry, keys)
        if epoch is not None and epoch > current_epoch:
            return None
        return entry

    @staticmethod
    def _trim_box_entry(entry: Any, current_epoch: int) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        start_epoch = BotRuntime._first_epoch_from(
            entry,
            ("start", "start_date", "startDate", "x1"),
        )
        if start_epoch is not None and start_epoch > current_epoch:
            return None
        end_epoch = BotRuntime._first_epoch_from(entry, ("end", "end_date", "endDate"))
        extend_flag = bool(entry.get("extend")) if "extend" in entry else False
        if end_epoch is None and not extend_flag:
            end_epoch = BotRuntime._first_epoch_from(entry, ("x2",))
        if end_epoch is not None and end_epoch > current_epoch:
            return None
        return entry

    @staticmethod
    def _trim_segment_entry(entry: Any, current_epoch: int) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        start_epoch = BotRuntime._first_epoch_from(entry, ("x1", "start", "start_date", "startDate"))
        if start_epoch is not None and start_epoch > current_epoch:
            return None
        end_epoch = BotRuntime._first_epoch_from(entry, ("x2", "end", "end_date", "endDate"))
        if end_epoch is not None and end_epoch > current_epoch:
            trimmed = dict(entry)
            trimmed["x2"] = current_epoch
            return trimmed
        return entry

    @staticmethod
    def _trim_polyline_entry(entry: Any, current_epoch: int) -> Optional[Any]:
        if not isinstance(entry, Mapping):
            return None
        points = entry.get("points")
        if not isinstance(points, list):
            return entry
        new_points: List[Any] = []
        changed = False
        for point in points:
            if not isinstance(point, Mapping):
                continue
            epoch = BotRuntime._normalise_epoch(point.get("time"))
            if epoch is not None and epoch > current_epoch:
                changed = True
                continue
            new_points.append(point)
        if not new_points:
            return None
        if changed or len(new_points) != len(points):
            trimmed = dict(entry)
            trimmed["points"] = new_points
            return trimmed
        return entry

    @staticmethod
    def _first_epoch_from(entry: Mapping[str, Any], keys: Tuple[str, ...]) -> Optional[int]:
        for key in keys:
            if key not in entry:
                continue
            epoch = BotRuntime._normalise_epoch(entry.get(key))
            if epoch is not None:
                return epoch
        return None

    def _chart_state(self) -> Dict[str, Any]:
        candles = self._visible_candles()
        return {
            "candles": candles,
            "trades": self._aggregate_trades(),
            "stats": self._last_stats or self._aggregate_stats(),
            "overlays": self._visible_overlays(),
            "logs": self.logs(),
        }

    def _push_update(self, event: str) -> None:
        payload = self._chart_state()
        payload["runtime"] = self.snapshot()
        self._broadcast(event, payload)


__all__ = [
    "BotRuntime",
    "DEFAULT_RISK",
]

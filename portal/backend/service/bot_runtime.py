"""Backtesting runtime with ladder risk logic for bot simulations."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence


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


class BotRuntime:
    """Simulated bot runtime that iterates over candles and emits stats."""

    def __init__(self, bot_id: str, config: Dict[str, object]):
        self.bot_id = bot_id
        self.config = config
        self.mode = (config.get("mode") or "instant").lower()
        self.fetch_seconds = max(float(config.get("fetch_seconds") or 1.0), 0)
        self.risk_engine = LadderRiskEngine(config.get("risk"))
        self.candles: List[Candle] = []
        self.state: Dict[str, object] = {"status": "idle", "progress": 0.0}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def _generate_candles(self, count: int = 240) -> List[Candle]:
        start = datetime.utcnow() - timedelta(hours=count // 4)
        price = 80.0
        candles: List[Candle] = []
        rng = random.Random(self.bot_id)
        for i in range(count):
            drift = rng.uniform(-0.5, 0.5)
            open_price = price
            close_price = max(1.0, open_price + drift)
            high = max(open_price, close_price) + rng.uniform(0, 0.3)
            low = min(open_price, close_price) - rng.uniform(0, 0.3)
            candles.append(
                Candle(
                    time=start + timedelta(minutes=5 * i),
                    open=open_price,
                    high=high,
                    low=low,
                    close=close_price,
                )
            )
            price = close_price
        return candles

    def _signal_for_bar(self, idx: int, candle: Candle) -> Optional[str]:
        if idx % 9 != 0:
            return None
        return "long" if candle.close >= candle.open else "short"

    def _run(self) -> None:
        candles = self._generate_candles()
        self.candles = candles
        for idx, candle in enumerate(candles):
            if self._stop.is_set():
                break
            signal = self._signal_for_bar(idx, candle)
            self.risk_engine.maybe_enter(candle, signal)
            self.risk_engine.step(candle)
            progress = round((idx + 1) / len(candles), 4)
            with self._lock:
                self.state.update({
                    "status": "running",
                    "last_bar": candle.to_dict(),
                    "progress": progress,
                })
            if self.mode == "walk-forward" and self.fetch_seconds:
                time.sleep(min(self.fetch_seconds, 2.0))
        with self._lock:
            self.state.update({
                "status": "completed" if not self._stop.is_set() else "stopped",
                "completed_at": datetime.utcnow().isoformat() + "Z",
                "stats": self.risk_engine.stats(),
            })

    def warm_up(self) -> None:
        """Ensure candles exist even if the runtime has not been started."""

        if self.candles:
            return
        self._stop.clear()
        self.state = {"status": "initialising", "progress": 0.0}
        self.risk_engine = LadderRiskEngine(self.config.get("risk"))
        self._run()

    def start(self) -> None:
        """Start the backtest loop in the background."""

        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        with self._lock:
            self.state = {"status": "starting", "progress": 0.0}
        self.risk_engine = LadderRiskEngine(self.config.get("risk"))
        self._thread = threading.Thread(target=self._run, name=f"bot-{self.bot_id}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Request the runtime to halt."""

        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.1)
        with self._lock:
            self.state["status"] = "stopped"

    def snapshot(self) -> Dict[str, object]:
        """Return a thread-safe snapshot of runtime state."""

        with self._lock:
            base = dict(self.state)
        base.setdefault("stats", self.risk_engine.stats())
        return base

    def chart_payload(self) -> Dict[str, object]:
        """Return the latest candle and trade data for visualisation."""

        return {
            "candles": [c.to_dict() for c in self.candles],
            "trades": self.risk_engine.serialise_trades(),
            "stats": self.snapshot().get("stats", {}),
        }


__all__ = [
    "BotRuntime",
    "DEFAULT_RISK",
]

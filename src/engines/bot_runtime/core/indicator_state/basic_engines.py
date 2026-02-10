"""Additional built-in indicator state engines for plugin manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from engines.bot_runtime.core.domain import Candle

from .contracts import IndicatorStateDelta, IndicatorStateEngine, IndicatorStateSnapshot


@dataclass(frozen=True)
class RollingWindowEngineConfig:
    source_timeframe: str
    window_bars: int = 200


class RollingWindowStateEngine(IndicatorStateEngine):
    def __init__(self, config: RollingWindowEngineConfig) -> None:
        self._config = config

    def initialize(self, window_context: Mapping[str, Any]) -> MutableMapping[str, Any]:
        symbol = str(window_context.get("symbol") or "")
        if not symbol:
            raise RuntimeError("indicator_state_init_failed: symbol is required")
        return {
            "revision": 0,
            "symbol": symbol,
            "known_at": datetime.fromtimestamp(0, tz=timezone.utc),
            "formed_at": datetime.fromtimestamp(0, tz=timezone.utc),
            "bars": [],
        }

    def apply_bar(self, state: MutableMapping[str, Any], bar: Any) -> IndicatorStateDelta:
        if not isinstance(bar, Candle):
            raise RuntimeError("indicator_state_apply_failed: Candle input is required")
        bars = list(state.get("bars") or [])
        bars.append(
            {
                "time": bar.time,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume or 0.0),
            }
        )
        if len(bars) > self._config.window_bars:
            bars = bars[-self._config.window_bars :]
        state["bars"] = bars
        state["revision"] = int(state.get("revision") or 0) + 1
        known_at = bar.time.astimezone(timezone.utc) if bar.time.tzinfo else bar.time.replace(tzinfo=timezone.utc)
        state["known_at"] = known_at
        state["formed_at"] = known_at
        return IndicatorStateDelta(changed=True, revision=int(state["revision"]), known_at=known_at)

    def snapshot(self, state: Mapping[str, Any]) -> IndicatorStateSnapshot:
        known_at = state.get("known_at")
        formed_at = state.get("formed_at")
        if not isinstance(known_at, datetime):
            known_at = datetime.fromtimestamp(0, tz=timezone.utc)
        if not isinstance(formed_at, datetime):
            formed_at = known_at
        return IndicatorStateSnapshot(
            revision=int(state.get("revision") or 0),
            known_at=known_at,
            formed_at=formed_at,
            source_timeframe=self._config.source_timeframe,
            payload={
                "symbol": state.get("symbol"),
                "bars": list(state.get("bars") or []),
            },
        )


class VWAPStateEngine(IndicatorStateEngine):
    """Session-based VWAP engine (daily reset)."""

    def initialize(self, window_context: Mapping[str, Any]) -> MutableMapping[str, Any]:
        symbol = str(window_context.get("symbol") or "")
        if not symbol:
            raise RuntimeError("indicator_state_init_failed: vwap requires symbol")
        return {
            "revision": 0,
            "symbol": symbol,
            "session": None,
            "cum_pv": 0.0,
            "cum_volume": 0.0,
            "known_at": datetime.fromtimestamp(0, tz=timezone.utc),
            "formed_at": datetime.fromtimestamp(0, tz=timezone.utc),
            "vwap": None,
        }

    def apply_bar(self, state: MutableMapping[str, Any], bar: Any) -> IndicatorStateDelta:
        if not isinstance(bar, Candle):
            raise RuntimeError("indicator_state_apply_failed: vwap requires Candle input")
        ts = bar.time.astimezone(timezone.utc) if bar.time.tzinfo else bar.time.replace(tzinfo=timezone.utc)
        session = ts.date().isoformat()
        if state.get("session") != session:
            state["session"] = session
            state["cum_pv"] = 0.0
            state["cum_volume"] = 0.0
        typical = (float(bar.high) + float(bar.low) + float(bar.close)) / 3.0
        volume = float(bar.volume or 0.0)
        state["cum_pv"] = float(state.get("cum_pv") or 0.0) + (typical * volume)
        state["cum_volume"] = float(state.get("cum_volume") or 0.0) + volume
        cum_volume = float(state.get("cum_volume") or 0.0)
        state["vwap"] = (float(state.get("cum_pv") or 0.0) / cum_volume) if cum_volume > 0 else None
        state["revision"] = int(state.get("revision") or 0) + 1
        state["known_at"] = ts
        state["formed_at"] = ts
        return IndicatorStateDelta(changed=True, revision=int(state["revision"]), known_at=ts)

    def snapshot(self, state: Mapping[str, Any]) -> IndicatorStateSnapshot:
        known_at = state.get("known_at")
        formed_at = state.get("formed_at")
        if not isinstance(known_at, datetime):
            known_at = datetime.fromtimestamp(0, tz=timezone.utc)
        if not isinstance(formed_at, datetime):
            formed_at = known_at
        return IndicatorStateSnapshot(
            revision=int(state.get("revision") or 0),
            known_at=known_at,
            formed_at=formed_at,
            source_timeframe="1m",
            payload={
                "symbol": state.get("symbol"),
                "session": state.get("session"),
                "vwap": state.get("vwap"),
            },
        )


def build_pivot_engine() -> RollingWindowStateEngine:
    return RollingWindowStateEngine(RollingWindowEngineConfig(source_timeframe="1d", window_bars=64))


def build_trendline_engine() -> RollingWindowStateEngine:
    return RollingWindowStateEngine(RollingWindowEngineConfig(source_timeframe="1h", window_bars=256))

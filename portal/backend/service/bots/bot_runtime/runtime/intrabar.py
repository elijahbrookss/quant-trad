from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional

from ....market.candle_service import fetch_ohlcv
from engines.bot_runtime.core.domain import Candle, coerce_float
from utils.log_context import build_log_context, merge_log_context, series_log_context, with_log_context

logger = logging.getLogger(__name__)


class IntrabarManager:
    """Manage cached intrabar candles and snapshots for the bot runtime."""

    def __init__(
        self,
        bot_id: str,
        *,
        fetcher: Callable[..., Any] = fetch_ohlcv,
        build_candles: Callable[[Any, Optional[str]], List[Candle]],
        timeframe_seconds: Callable[[Optional[str]], Optional[float]],
        strategy_key_fn: Callable[[Any], str],
    ) -> None:
        self.bot_id = bot_id
        self._fetcher = fetcher
        self._build_candles = build_candles
        self._timeframe_seconds = timeframe_seconds
        self._strategy_key = strategy_key_fn
        self._cache: Dict[str, List[Candle]] = {}
        self._snapshots: Dict[str, Dict[str, Any]] = {}

    @property
    def snapshots(self) -> Dict[str, Dict[str, Any]]:
        return self._snapshots

    def intrabar_candles(self, series: Any, candle: Candle) -> List[Candle]:
        engine = getattr(series, "risk_engine", None)
        if engine is None or engine.active_trade is None:
            return []
        interval = self._intrabar_interval_for(series.timeframe)
        if not interval:
            return []
        start = candle.start_time
        duration = self._timeframe_seconds(series.timeframe) or 0
        end = candle.end or (start + timedelta(seconds=max(int(duration), 0)))
        if start is None or end is None or end <= start:
            return []
        key = self._intrabar_cache_key(series, start, interval)
        if key in self._cache:
            return self._cache[key]
        sub_candles = self._fetch_intrabar_candles(series, start, end, interval)
        self._cache[key] = sub_candles
        return sub_candles

    def update_snapshot(self, series: Any, candle: Candle, minute_bar: Candle) -> Dict[str, Any]:
        snapshot = self._update_intrabar_snapshot(series, candle, minute_bar)
        return snapshot

    def merge_snapshot_payload(self, existing: Mapping[str, Any], snapshot: Mapping[str, Any]) -> Dict[str, Any]:
        payload = dict(existing)
        open_price = coerce_float(snapshot.get("open"), payload.get("open", 0.0)) or 0.0
        high_price = coerce_float(snapshot.get("high"), payload.get("high", open_price)) or open_price
        low_price = coerce_float(snapshot.get("low"), payload.get("low", open_price)) or open_price
        close_price = coerce_float(snapshot.get("close"), payload.get("close", open_price)) or open_price
        payload["open"] = round(open_price, 4)
        payload["high"] = round(high_price, 4)
        payload["low"] = round(low_price, 4)
        payload["close"] = round(close_price, 4)
        end_ts = snapshot.get("end")
        if isinstance(end_ts, datetime):
            payload["end"] = end_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return payload

    def clear_cache(self) -> None:
        self._cache.clear()
        self._snapshots.clear()

    def _intrabar_interval_for(self, timeframe: Optional[str]) -> Optional[str]:
        base_seconds = self._timeframe_seconds(timeframe)
        if not base_seconds or base_seconds <= 60:
            return None
        return "1m"

    def _intrabar_cache_key(self, series: Any, start: datetime, interval: str) -> str:
        epoch = int(start.timestamp())
        strategy_key = self._strategy_key(series)
        return f"{strategy_key}:{getattr(series, 'symbol', '')}:{getattr(series, 'timeframe', '')}:{interval}:{epoch}"

    def _fetch_intrabar_candles(
        self,
        series: Any,
        start: datetime,
        end: datetime,
        interval: str,
    ) -> List[Candle]:
        start_iso = start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        end_iso = end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        try:
            df = self._fetcher(
                series.symbol,
                start_iso,
                end_iso,
                interval,
                datasource=series.datasource,
                exchange=series.exchange,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            context = merge_log_context(
                series_log_context(series),
                build_log_context(
                    bot_id=self.bot_id,
                    interval=interval,
                    error=str(exc),
                ),
            )
            logger.debug(with_log_context("bot_runtime_intrabar_fetch_failed", context))
            return []
        if df is None or getattr(df, "empty", False):
            return []
        candles = self._build_candles(df, interval)
        filtered: List[Candle] = []
        for candle in candles:
            start_ts = candle.start_time
            end_ts = candle.end_time
            if end_ts <= start:
                continue
            if start_ts >= end:
                break
            filtered.append(candle)
        return filtered

    def _ensure_intrabar_snapshot(self, series: Any, candle: Candle) -> Dict[str, Any]:
        strategy_key = self._strategy_key(series)
        snapshot = self._snapshots.get(strategy_key)
        if snapshot:
            return snapshot
        open_price = coerce_float(candle.open, 0.0) or 0.0
        entry = {
            "strategy_id": getattr(series, "strategy_id", None) or strategy_key,
            "time": candle.time,
            "open": open_price,
            "high": open_price,
            "low": open_price,
            "close": open_price,
            "end": candle.end or candle.time,
        }
        self._snapshots[strategy_key] = entry
        return entry

    def _update_intrabar_snapshot(
        self,
        series: Any,
        candle: Candle,
        minute_bar: Candle,
    ) -> Dict[str, Any]:
        snapshot = self._ensure_intrabar_snapshot(series, candle)
        close_price = coerce_float(minute_bar.close, snapshot["close"])
        high_price = coerce_float(minute_bar.high, snapshot["high"])
        low_price = coerce_float(minute_bar.low, snapshot["low"])
        if close_price is not None:
            snapshot["close"] = close_price
        if high_price is not None:
            snapshot["high"] = max(snapshot["high"], high_price)
        if low_price is not None:
            snapshot["low"] = min(snapshot["low"], low_price)
        snapshot["end"] = minute_bar.end or minute_bar.time
        return snapshot

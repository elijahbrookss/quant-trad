from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional

from engines.bot_runtime.core.domain import Candle, coerce_float
from utils.log_context import build_log_context, merge_log_context, series_log_context, with_log_context
from utils.perf_log import get_obs_enabled, get_obs_step_sample_rate, should_sample

logger = logging.getLogger(__name__)


class IntrabarManager:
    """Manage cached intrabar candles and snapshots for the bot runtime."""

    def __init__(
        self,
        bot_id: str,
        *,
        fetcher: Optional[Callable[..., Any]] = None,
        build_candles: Callable[[Any, Optional[str]], List[Candle]],
        timeframe_seconds: Callable[[Optional[str]], Optional[float]],
        strategy_key_fn: Callable[[Any], str],
        obs_enabled: Optional[bool] = None,
        obs_sample_rate: Optional[float] = None,
    ) -> None:
        self.bot_id = bot_id
        self._fetcher = fetcher if fetcher is not None else self._default_fetcher
        self._build_candles = build_candles
        self._timeframe_seconds = timeframe_seconds
        self._strategy_key = strategy_key_fn
        self._obs_enabled = get_obs_enabled() if obs_enabled is None else obs_enabled
        self._obs_sample_rate = (
            get_obs_step_sample_rate() if obs_sample_rate is None else obs_sample_rate
        )
        # NOTE: Runtime-scoped in-memory cache. Key=strategy+symbol+timeframe+interval+start epoch.
        # NOTE: No eviction; multiprocessing/container-per-bot will duplicate work.
        # NOTE: Guarded by lock but not safe for concurrent mutation outside this instance.
        self._cache: Dict[str, List[Candle]] = {}
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _default_fetcher(*args: Any, **kwargs: Any) -> Any:
        from portal.backend.service.market.candle_service import fetch_ohlcv

        return fetch_ohlcv(*args, **kwargs)

    @property
    def snapshots(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._snapshots)

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
        cache_key_summary = f"{getattr(series, 'symbol', '')}:{getattr(series, 'timeframe', '')}:{interval}:{start.date().isoformat()}"
        should_log = self._obs_enabled and should_sample(self._obs_sample_rate)
        get_started = time.perf_counter() if should_log else 0.0
        with self._lock:
            cached = self._cache.get(key)
        if should_log:
            get_ms = (time.perf_counter() - get_started) * 1000.0
            base_context = merge_log_context(
                series_log_context(series),
                build_log_context(
                    bot_id=self.bot_id,
                    cache_name="intrabar_candles",
                    cache_scope="runtime",
                    cache_key_summary=cache_key_summary,
                    time_taken_ms=get_ms,
                    pid=os.getpid(),
                    thread_name=threading.current_thread().name,
                ),
            )
            logger.debug(
                with_log_context(
                    "cache.get",
                    merge_log_context(base_context, build_log_context(event="cache.get")),
                )
            )
            hit_event = "cache.hit" if cached is not None else "cache.miss"
            logger.debug(
                with_log_context(
                    hit_event,
                    merge_log_context(base_context, build_log_context(event=hit_event)),
                )
            )
        if cached is not None:
            return cached
        fetch_started = time.perf_counter() if should_log else 0.0
        sub_candles = self._fetch_intrabar_candles(series, start, end, interval)
        with self._lock:
            self._cache[key] = sub_candles
        if should_log:
            fetch_ms = (time.perf_counter() - fetch_started) * 1000.0
            set_context = merge_log_context(
                series_log_context(series),
                build_log_context(
                    bot_id=self.bot_id,
                    cache_name="intrabar_candles",
                    cache_scope="runtime",
                    cache_key_summary=cache_key_summary,
                    time_taken_ms=fetch_ms,
                    pid=os.getpid(),
                    thread_name=threading.current_thread().name,
                ),
            )
            logger.debug(
                with_log_context(
                    "cache.set",
                    merge_log_context(set_context, build_log_context(event="cache.set")),
                )
            )
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
        with self._lock:
            self._cache.clear()
            self._snapshots.clear()

    def clear_snapshot(self, series: Any) -> None:
        strategy_key = self._strategy_key(series)
        with self._lock:
            self._snapshots.pop(strategy_key, None)

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
        with self._lock:
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
        with self._lock:
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

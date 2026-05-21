"""Container-owned live market stream for paper bot runs."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from data_providers.streams import CoinbaseAdvancedTradeStream, MarketSubscription, ProviderMarketDataStream
from engines.bot_runtime.core.series_identity import canonical_series_key
from engines.bot_runtime.live_market import LiveCandleAggregator, LiveCandleStore

from .market_data_stream_policy import normalize_market_data_stream_policy

logger = logging.getLogger(__name__)

PAPER_MARKET_STREAM_CHANNELS = ("heartbeats", "ticker", "candles")
_RECENT_RECONNECT_LIMIT = 12


class PaperMarketStreamRunner:
    """Run provider market-data intake beside the existing paper runtime."""

    def __init__(
        self,
        *,
        bot_id: str,
        run_id: str,
        store: LiveCandleStore,
        series: Sequence[Any],
        channels: Sequence[str] = PAPER_MARKET_STREAM_CHANNELS,
        provisional_candle_sink: Callable[[Mapping[str, Any]], bool] | None = None,
        provisional_emit_interval_ms: int = 1000,
        market_data_stream_policy: Mapping[str, Any] | None = None,
        stream_factory: Callable[[Sequence[MarketSubscription]], ProviderMarketDataStream] | None = None,
    ) -> None:
        self.bot_id = str(bot_id)
        self.run_id = str(run_id)
        self.store = store
        self.series = list(series or [])
        self.channels = tuple(str(channel).strip().lower() for channel in channels if str(channel).strip())
        self.provisional_candle_sink = provisional_candle_sink
        self.provisional_emit_interval_ms = max(int(provisional_emit_interval_ms or 0), 0)
        self.market_data_stream_policy = normalize_market_data_stream_policy(market_data_stream_policy)
        self._stream_factory = stream_factory
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._counts: dict[str, int] = {}
        self._stream_lock = threading.Lock()
        self._aggregators: dict[str, LiveCandleAggregator] = {}
        self._series_meta_by_symbol: dict[str, dict[str, Any]] = {}
        self._last_provisional_emit_monotonic_by_symbol: dict[str, float] = {}
        self._startup_error: str | None = None
        self._disconnect_count = 0
        self._reconnect_attempt_count = 0
        self._reconnect_success_count = 0
        self._total_disconnected_seconds = 0.0
        self._max_continuous_disconnected_seconds = 0.0
        self._current_disconnect_started_monotonic: float | None = None
        self._current_disconnect_started_at: str | None = None
        self._last_disconnect_reason: str | None = None
        self._last_disconnect_started_at: str | None = None
        self._last_reconnect_attempt_at: str | None = None
        self._last_reconnect_succeeded_at: str | None = None
        self._recent_reconnects: list[dict[str, Any]] = []

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        subscriptions = self._subscriptions()
        if not subscriptions:
            raise RuntimeError(
                f"paper market stream requires at least one subscription | bot_id={self.bot_id} run_id={self.run_id}"
            )
        self._thread = threading.Thread(
            target=self._run_thread,
            name=f"paper-market-stream-{self.bot_id}",
            args=(subscriptions,),
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=15.0):
            self._stop.set()
            raise RuntimeError(
                f"paper market stream did not become ready within 15s | bot_id={self.bot_id} run_id={self.run_id}"
            )
        if self._startup_error:
            raise RuntimeError(
                f"paper market stream failed during startup | bot_id={self.bot_id} run_id={self.run_id} error={self._startup_error}"
            )

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5.0)
        self._thread = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "event_counts": dict(self._counts),
            "store": self.store.snapshot(),
            "market_data_stream_policy": dict(self.market_data_stream_policy),
            "stream_diagnostics": self._stream_diagnostics_snapshot(),
            "provisional_emit_interval_ms": self.provisional_emit_interval_ms,
            "aggregators": {
                key: aggregator.snapshot()
                for key, aggregator in sorted(self._aggregators.items())
            },
        }

    def _run_thread(self, subscriptions: Sequence[MarketSubscription]) -> None:
        try:
            asyncio.run(self._run_stream(subscriptions))
        except Exception as exc:  # noqa: BLE001
            self._startup_error = str(exc)
            self.store.mark_failed(str(exc))
            self._ready.set()
            logger.exception(
                "paper_market_stream_failed | bot_id=%s | run_id=%s | error=%s",
                self.bot_id,
                self.run_id,
                exc,
            )
            self._increment("stream_error")

    async def _run_stream(self, subscriptions: Sequence[MarketSubscription]) -> None:
        self._ready.set()
        reconnect_attempt = 0
        while not self._stop.is_set():
            if self._current_disconnect_started_monotonic is not None:
                budget_seconds = float(self.market_data_stream_policy["continuous_disconnect_budget_seconds"])
                disconnected_seconds = self._current_disconnect_seconds()
                if disconnected_seconds >= budget_seconds:
                    self._increment("disconnect_budget_exhausted")
                    raise RuntimeError(
                        "paper market stream disconnected longer than budget "
                        f"bot_id={self.bot_id} run_id={self.run_id} "
                        f"disconnect_seconds={disconnected_seconds:.3f} budget_seconds={budget_seconds:.3f} "
                        f"reason={self._last_disconnect_reason or 'unknown'}"
                    )
                delay_seconds = min(
                    self._reconnect_backoff_seconds(reconnect_attempt),
                    max(budget_seconds - disconnected_seconds, 0.0),
                )
                self._record_reconnect_attempt(reconnect_attempt, delay_seconds)
                await self._sleep_until_stop(delay_seconds)
                if self._stop.is_set():
                    break

            stream = self._build_stream(subscriptions)
            pending_event: asyncio.Task[Any] | None = None
            first_event_after_disconnect = False
            last_event_monotonic = time.monotonic()
            try:
                await asyncio.wait_for(stream.connect(), timeout=self._stream_operation_timeout_seconds())
                self._increment("provider_connected")
                await asyncio.wait_for(stream.subscribe(subscriptions), timeout=self._stream_operation_timeout_seconds())
                iterator = stream.events().__aiter__()
                while not self._stop.is_set():
                    if pending_event is None:
                        pending_event = asyncio.create_task(anext(iterator))
                    done, _pending = await asyncio.wait({pending_event}, timeout=0.5)
                    if not done:
                        stale_seconds = float(self.market_data_stream_policy["heartbeat_stale_seconds"])
                        if stale_seconds > 0 and time.monotonic() - last_event_monotonic >= stale_seconds:
                            pending_event.cancel()
                            with contextlib.suppress(asyncio.CancelledError, Exception):
                                await pending_event
                            pending_event = None
                            raise RuntimeError(
                                "paper market stream heartbeat stale "
                                f"stale_seconds={stale_seconds:.3f}"
                            )
                        continue
                    try:
                        event = pending_event.result()
                    except StopAsyncIteration as exc:
                        raise RuntimeError("paper market provider stream ended") from exc
                    finally:
                        pending_event = None
                    last_event_monotonic = time.monotonic()
                    if self._current_disconnect_started_monotonic is not None and not first_event_after_disconnect:
                        self._record_reconnect_succeeded()
                        reconnect_attempt = 0
                        first_event_after_disconnect = True
                    self._handle_event(event.to_dict())
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    break
                if not self._should_reconnect(exc):
                    raise
                reconnect_attempt += 1
                self._record_disconnect(str(exc) or type(exc).__name__)
                continue
            finally:
                if pending_event is not None:
                    pending_event.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await pending_event
                try:
                    await asyncio.wait_for(stream.close(), timeout=5.0)
                except Exception as exc:  # noqa: BLE001
                    self._increment("provider_close_error")
                    logger.warning(
                        "paper_market_stream_close_failed | bot_id=%s | run_id=%s | error=%s",
                        self.bot_id,
                        self.run_id,
                        exc,
                    )
                self._increment("provider_closed")
                logger.info(
                    "paper_market_stream_closed | bot_id=%s | run_id=%s | counts=%s | store=%s | stream_diagnostics=%s",
                    self.bot_id,
                    self.run_id,
                    dict(self._counts),
                    self.store.snapshot(),
                    self._stream_diagnostics_snapshot(),
                )

    def _stream_diagnostics_snapshot(self) -> dict[str, Any]:
        with self._stream_lock:
            current = self._current_disconnect_seconds_locked()
            max_continuous = max(self._max_continuous_disconnected_seconds, current)
            return {
                "schema_version": "market_data_stream_diagnostics.v1",
                "disconnect_count": self._disconnect_count,
                "reconnect_attempt_count": self._reconnect_attempt_count,
                "reconnect_success_count": self._reconnect_success_count,
                "current_disconnect_started_at": self._current_disconnect_started_at,
                "current_disconnected_seconds": round(current, 6),
                "total_disconnected_seconds": round(self._total_disconnected_seconds + current, 6),
                "max_continuous_disconnected_seconds": round(max_continuous, 6),
                "last_disconnect_reason": self._last_disconnect_reason,
                "last_disconnect_started_at": self._last_disconnect_started_at,
                "last_reconnect_attempt_at": self._last_reconnect_attempt_at,
                "last_reconnect_succeeded_at": self._last_reconnect_succeeded_at,
                "recent_reconnects": list(self._recent_reconnects),
            }

    def _record_disconnect(self, reason: str) -> None:
        text = str(reason or "").strip() or "provider stream disconnected"
        now = _utc_now_iso()
        with self._stream_lock:
            self._last_disconnect_reason = text
            event_name = "reconnect_failed"
            if self._current_disconnect_started_monotonic is None:
                event_name = "disconnect"
                self._disconnect_count += 1
                self._current_disconnect_started_monotonic = time.monotonic()
                self._current_disconnect_started_at = now
                self._last_disconnect_started_at = now
            self._recent_reconnects.append(
                {
                    "event": event_name,
                    "at": now,
                    "reason": text,
                }
            )
            self._trim_recent_reconnects_locked()
        self._increment("stream_disconnect")
        logger.warning(
            "paper_market_stream_disconnected | bot_id=%s | run_id=%s | reason=%s | diagnostics=%s",
            self.bot_id,
            self.run_id,
            text,
            self._stream_diagnostics_snapshot(),
        )

    def _record_reconnect_attempt(self, attempt: int, delay_seconds: float) -> None:
        now = _utc_now_iso()
        with self._stream_lock:
            self._reconnect_attempt_count += 1
            self._last_reconnect_attempt_at = now
            self._recent_reconnects.append(
                {
                    "event": "reconnect_attempt",
                    "at": now,
                    "attempt": int(attempt),
                    "delay_seconds": round(float(delay_seconds), 6),
                    "reason": self._last_disconnect_reason,
                }
            )
            self._trim_recent_reconnects_locked()
        self._increment("stream_reconnect_attempt")
        logger.info(
            "paper_market_stream_reconnect_attempt | bot_id=%s | run_id=%s | attempt=%s | delay_seconds=%.3f | reason=%s",
            self.bot_id,
            self.run_id,
            int(attempt),
            float(delay_seconds),
            self._last_disconnect_reason,
        )

    def _record_reconnect_succeeded(self) -> None:
        now = _utc_now_iso()
        with self._stream_lock:
            disconnected_seconds = self._current_disconnect_seconds_locked()
            self._total_disconnected_seconds += disconnected_seconds
            self._max_continuous_disconnected_seconds = max(
                self._max_continuous_disconnected_seconds,
                disconnected_seconds,
            )
            self._reconnect_success_count += 1
            self._last_reconnect_succeeded_at = now
            self._recent_reconnects.append(
                {
                    "event": "reconnect_succeeded",
                    "at": now,
                    "disconnected_seconds": round(disconnected_seconds, 6),
                    "reason": self._last_disconnect_reason,
                }
            )
            self._current_disconnect_started_monotonic = None
            self._current_disconnect_started_at = None
            self._trim_recent_reconnects_locked()
        self._increment("stream_reconnect_succeeded")
        logger.info(
            "paper_market_stream_reconnect_succeeded | bot_id=%s | run_id=%s | disconnected_seconds=%.3f",
            self.bot_id,
            self.run_id,
            disconnected_seconds,
        )

    def _current_disconnect_seconds(self) -> float:
        with self._stream_lock:
            return self._current_disconnect_seconds_locked()

    def _current_disconnect_seconds_locked(self) -> float:
        if self._current_disconnect_started_monotonic is None:
            return 0.0
        return max(time.monotonic() - self._current_disconnect_started_monotonic, 0.0)

    def _trim_recent_reconnects_locked(self) -> None:
        if len(self._recent_reconnects) > _RECENT_RECONNECT_LIMIT:
            del self._recent_reconnects[: len(self._recent_reconnects) - _RECENT_RECONNECT_LIMIT]

    def _reconnect_backoff_seconds(self, attempt: int) -> float:
        if int(attempt) <= 1:
            return 0.0
        initial = float(self.market_data_stream_policy["initial_backoff_seconds"])
        cap = float(self.market_data_stream_policy["max_backoff_seconds"])
        multipliers = (1.0, 2.0, 5.0, 10.0, 30.0)
        index = min(max(int(attempt) - 2, 0), len(multipliers) - 1)
        if int(attempt) > len(multipliers) + 1:
            return cap
        return min(initial * multipliers[index], cap)

    def _stream_operation_timeout_seconds(self) -> float:
        timeout = float(self.market_data_stream_policy["heartbeat_stale_seconds"])
        if self._current_disconnect_started_monotonic is None:
            return max(timeout, 0.001)
        budget_remaining = (
            float(self.market_data_stream_policy["continuous_disconnect_budget_seconds"])
            - self._current_disconnect_seconds()
        )
        return max(min(timeout, budget_remaining), 0.001)

    async def _sleep_until_stop(self, seconds: float) -> None:
        remaining = max(float(seconds or 0.0), 0.0)
        deadline = time.monotonic() + remaining
        while not self._stop.is_set() and remaining > 0:
            await asyncio.sleep(min(remaining, 0.25))
            remaining = deadline - time.monotonic()

    def _should_reconnect(self, exc: Exception) -> bool:
        if not bool(self.market_data_stream_policy["reconnect_enabled"]):
            return False
        if isinstance(exc, ValueError):
            return False
        if isinstance(exc, TimeoutError):
            return True
        message = str(exc or "").strip().lower()
        transient_markers = (
            "no close frame",
            "connection",
            "connect",
            "closed",
            "close",
            "timeout",
            "timed out",
            "temporary",
            "network",
            "eof",
            "heartbeat stale",
            "provider stream ended",
            "ping",
        )
        return any(marker in message for marker in transient_markers)

    def _handle_event(self, event: Mapping[str, Any]) -> None:
        kind = str(event.get("event_kind") or "").strip()
        self._increment(kind or "unknown")
        symbol = str(event.get("symbol") or "").strip().upper()
        if not symbol:
            return
        aggregator = self._aggregators.get(symbol)
        if aggregator is None:
            return
        for candle in aggregator.process(event):
            appended = self.store.append(candle)
            self._increment("closed_live_candle_appended" if appended else "closed_live_candle_duplicate")
            if appended:
                logger.info(
                    "paper_live_candle_closed | bot_id=%s | run_id=%s | symbol=%s | timeframe=%s | bar_time=%s | source_event_count=%s",
                    self.bot_id,
                    self.run_id,
                    candle.symbol,
                    candle.timeframe,
                    candle.time.isoformat(),
                    candle.source_event_count,
                )
        provisional = aggregator.provisional_from_event(event)
        if provisional is not None:
            self._emit_provisional_candle(symbol, provisional.to_dict())

    def _emit_provisional_candle(self, symbol: str, provisional_candle: Mapping[str, Any]) -> None:
        sink = self.provisional_candle_sink
        if sink is None:
            return
        now = time.monotonic()
        last = float(self._last_provisional_emit_monotonic_by_symbol.get(symbol) or 0.0)
        interval_s = float(self.provisional_emit_interval_ms) / 1000.0
        if interval_s > 0 and last > 0 and now - last < interval_s:
            self._increment("provisional_candle_throttled")
            return
        meta = self._series_meta_by_symbol.get(symbol) or {}
        payload = {
            **meta,
            "symbol": str(provisional_candle.get("symbol") or symbol).strip().upper(),
            "timeframe": provisional_candle.get("timeframe") or meta.get("timeframe"),
            "provisional_candle": dict(provisional_candle),
            "known_at": provisional_candle.get("last_known_at") or provisional_candle.get("first_known_at"),
            "event_time": provisional_candle.get("last_known_at") or provisional_candle.get("first_known_at"),
        }
        try:
            emitted = bool(sink(payload))
        except Exception as exc:  # noqa: BLE001
            self._increment("provisional_candle_emit_failed")
            logger.warning(
                "paper_market_provisional_emit_failed | bot_id=%s | run_id=%s | symbol=%s | error=%s",
                self.bot_id,
                self.run_id,
                symbol,
                exc,
            )
            return
        if not emitted:
            self._increment("provisional_candle_dropped")
            return
        self._last_provisional_emit_monotonic_by_symbol[symbol] = now
        self._increment("provisional_candle_emitted")

    def _subscriptions(self) -> list[MarketSubscription]:
        subscriptions: list[MarketSubscription] = []
        for runtime_series in self.series:
            datasource = str(getattr(runtime_series, "datasource", "") or "").strip().upper()
            exchange = str(getattr(runtime_series, "exchange", "") or "").strip().upper()
            if datasource != "COINBASE" or exchange != "COINBASE_DIRECT":
                raise ValueError(
                    "paper live market-data v1 supports only COINBASE/COINBASE_DIRECT "
                    f"(got provider={datasource or None} venue={exchange or None})"
                )
            symbol = str(getattr(runtime_series, "symbol", "") or "").strip().upper()
            if not symbol:
                raise ValueError("paper live market-data series is missing symbol")
            timeframe = str(getattr(runtime_series, "timeframe", "") or "").strip()
            product_id = _product_id_from_instrument(getattr(runtime_series, "instrument", None)) or symbol
            instrument_id = _instrument_id_from_instrument(getattr(runtime_series, "instrument", None))
            series_key = canonical_series_key(instrument_id, timeframe)
            subscriptions.append(
                MarketSubscription.from_values(
                    provider="COINBASE",
                    venue="COINBASE_DIRECT",
                    symbol=symbol,
                    product_id=product_id,
                    channels=self.channels,
                    timeframe=timeframe or None,
                    auth_mode="public",
                )
            )
            self._aggregators[symbol] = LiveCandleAggregator(target_timeframe=timeframe)
            self._series_meta_by_symbol[symbol] = {
                "series_key": series_key,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_id": str(getattr(runtime_series, "strategy_id", "") or "").strip() or None,
            }
        return subscriptions

    @staticmethod
    def _stream_for(subscriptions: Sequence[MarketSubscription]) -> CoinbaseAdvancedTradeStream:
        providers = {subscription.provider for subscription in subscriptions}
        venues = {subscription.venue for subscription in subscriptions}
        if providers != {"COINBASE"} or venues != {"COINBASE_DIRECT"}:
            raise ValueError(f"unsupported paper market stream subscriptions providers={providers} venues={venues}")
        return CoinbaseAdvancedTradeStream(provider="COINBASE", venue="COINBASE_DIRECT")

    def _build_stream(self, subscriptions: Sequence[MarketSubscription]) -> ProviderMarketDataStream:
        if self._stream_factory is not None:
            return self._stream_factory(subscriptions)
        return self._stream_for(subscriptions)

    def _increment(self, key: str) -> None:
        self._counts[str(key)] = int(self._counts.get(str(key), 0)) + 1


def _product_id_from_instrument(instrument: Any) -> str | None:
    if not isinstance(instrument, Mapping):
        return None
    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    provider_metadata = metadata.get("provider_metadata") if isinstance(metadata.get("provider_metadata"), Mapping) else metadata
    for key in ("product_id", "coinbase_product_id", "venue_symbol", "provider_symbol"):
        value = provider_metadata.get(key) if isinstance(provider_metadata, Mapping) else None
        text = str(value or "").strip()
        if text:
            return text
    product = metadata.get("product") if isinstance(metadata.get("product"), Mapping) else {}
    product_id = str(product.get("product_id") or "").strip()
    return product_id or None


def _instrument_id_from_instrument(instrument: Any) -> str | None:
    if not isinstance(instrument, Mapping):
        return None
    for key in ("id", "instrument_id"):
        text = str(instrument.get(key) or "").strip()
        if text:
            return text
    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    for key in ("instrument_id", "id"):
        text = str(metadata.get(key) or "").strip()
        if text:
            return text
    return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


__all__ = ["PAPER_MARKET_STREAM_CHANNELS", "PaperMarketStreamRunner"]

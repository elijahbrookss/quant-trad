"""Live market-data aggregation contracts for paper bot runtimes.

This module converts provider stream events into closed runtime candles. It is
intentionally separate from order, fill, wallet, fee, and trade semantics.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Mapping

from data_providers.streams.contracts import CanonicalMarketEvent
from engines.bot_runtime.core.domain import Candle, isoformat, timeframe_duration


@dataclass(frozen=True)
class ClosedLiveCandle:
    """A closed provider-derived candle ready for runtime evaluation."""

    provider: str
    venue: str
    symbol: str
    product_id: str | None
    timeframe: str
    time: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    source_event_count: int = 0
    first_known_at: str | None = None
    last_known_at: str | None = None
    source: str = "provider_stream"

    def to_runtime_candle(self) -> Candle:
        return Candle(
            time=self.time,
            open=float(self.open),
            high=float(self.high),
            low=float(self.low),
            close=float(self.close),
            end=self.end,
            volume=float(self.volume) if self.volume is not None else None,
            range=float(self.high) - float(self.low),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "symbol": self.symbol,
            "product_id": self.product_id,
            "timeframe": self.timeframe,
            "time": isoformat(self.time),
            "end": isoformat(self.end),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source_event_count": self.source_event_count,
            "first_known_at": self.first_known_at,
            "last_known_at": self.last_known_at,
            "source": self.source,
        }


@dataclass(frozen=True)
class ProvisionalLiveCandle:
    """A provider-derived display candle that is not executable runtime truth."""

    provider: str
    venue: str
    symbol: str
    product_id: str | None
    timeframe: str
    time: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    source_event_count: int = 0
    first_known_at: str | None = None
    last_known_at: str | None = None
    source: str = "provider_stream"
    source_event_kind: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "symbol": self.symbol,
            "product_id": self.product_id,
            "timeframe": self.timeframe,
            "time": isoformat(self.time),
            "end": isoformat(self.end),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source_event_count": self.source_event_count,
            "first_known_at": self.first_known_at,
            "last_known_at": self.last_known_at,
            "source": self.source,
            "source_event_kind": self.source_event_kind,
            "is_closed": False,
            "provisional": True,
            "execution_eligible": False,
        }


@dataclass
class _SourceCandleSnapshot:
    provider: str
    venue: str
    symbol: str
    product_id: str | None
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    first_known_at: str | None
    last_known_at: str | None
    source_event_count: int = 1
    source_type: str | None = None


@dataclass
class _TargetAggregate:
    provider: str
    venue: str
    symbol: str
    product_id: str | None
    timeframe: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    first_known_at: str | None
    last_known_at: str | None
    source_event_count: int = 0
    source_bucket_count: int = 0

    def absorb(self, source: _SourceCandleSnapshot) -> None:
        self.high = max(self.high, source.high)
        self.low = min(self.low, source.low)
        self.close = source.close
        if source.volume is not None:
            self.volume = float(self.volume or 0.0) + float(source.volume)
        self.last_known_at = source.last_known_at
        self.source_event_count += max(int(source.source_event_count or 0), 1)
        self.source_bucket_count += 1

    def close_payload(self) -> ClosedLiveCandle:
        return ClosedLiveCandle(
            provider=self.provider,
            venue=self.venue,
            symbol=self.symbol,
            product_id=self.product_id,
            timeframe=self.timeframe,
            time=self.start,
            end=self.end,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            source_event_count=self.source_event_count,
            first_known_at=self.first_known_at,
            last_known_at=self.last_known_at,
        )


@dataclass
class _ProvisionalAggregate:
    provider: str
    venue: str
    symbol: str
    product_id: str | None
    timeframe: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    first_known_at: str | None
    last_known_at: str | None
    source_event_count: int = 0
    last_source_start: datetime | None = None
    source_event_kind: str | None = None

    def absorb_source(self, source: _SourceCandleSnapshot, *, source_event_kind: str) -> None:
        if self.last_source_start is not None and source.start < self.last_source_start:
            self.high = max(self.high, source.high)
            self.low = min(self.low, source.low)
            self.last_known_at = source.last_known_at or self.last_known_at
            self.source_event_count += max(int(source.source_event_count or 0), 1)
            self.source_event_kind = str(source_event_kind or "").strip() or self.source_event_kind
            return
        self.high = max(self.high, source.high)
        self.low = min(self.low, source.low)
        self.close = source.close
        self.volume = source.volume
        self.last_known_at = source.last_known_at or self.last_known_at
        self.source_event_count += max(int(source.source_event_count or 0), 1)
        self.last_source_start = source.start
        self.source_event_kind = str(source_event_kind or "").strip() or self.source_event_kind

    def absorb_price(self, *, price: float, known_at: str | None, source_event_kind: str) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.last_known_at = known_at or self.last_known_at
        self.source_event_count += 1
        self.source_event_kind = str(source_event_kind or "").strip() or self.source_event_kind

    def payload(self) -> ProvisionalLiveCandle:
        return ProvisionalLiveCandle(
            provider=self.provider,
            venue=self.venue,
            symbol=self.symbol,
            product_id=self.product_id,
            timeframe=self.timeframe,
            time=self.start,
            end=self.end,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            source_event_count=self.source_event_count,
            first_known_at=self.first_known_at,
            last_known_at=self.last_known_at,
            source_event_kind=self.source_event_kind,
        )


class LiveCandleStore:
    """Thread-safe closed-candle buffer consumed by paper runtime workers."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._candles: dict[tuple[str, str, int], ClosedLiveCandle] = {}
        self._duplicate_count = 0
        self._conflicting_duplicate_count = 0
        self._failure_message: str | None = None

    def append(self, candle: ClosedLiveCandle) -> bool:
        key = self._key(candle.symbol, candle.timeframe, candle.time)
        with self._lock:
            existing = self._candles.get(key)
            if existing is not None:
                self._duplicate_count += 1
                if existing.to_dict() != candle.to_dict():
                    self._conflicting_duplicate_count += 1
                return False
            self._candles[key] = candle
            return True

    def closed_after(
        self,
        *,
        symbol: str,
        timeframe: str,
        after: datetime | None,
        limit: int | None = None,
    ) -> list[ClosedLiveCandle]:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_timeframe = str(timeframe or "").strip()
        after_epoch = int(after.timestamp()) if after is not None else -1
        with self._lock:
            values = [
                candle
                for (stored_symbol, stored_timeframe, epoch), candle in self._candles.items()
                if stored_symbol == normalized_symbol
                and stored_timeframe == normalized_timeframe
                and epoch > after_epoch
            ]
        ordered = sorted(values, key=lambda candle: candle.time)
        if limit is not None and int(limit) > 0:
            return ordered[: int(limit)]
        return ordered

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "closed_candle_count": len(self._candles),
                "duplicate_count": self._duplicate_count,
                "conflicting_duplicate_count": self._conflicting_duplicate_count,
                "failure_message": self._failure_message,
            }

    def mark_failed(self, message: str) -> None:
        text = str(message or "").strip() or "live candle store failed"
        with self._lock:
            self._failure_message = text

    def raise_if_failed(self) -> None:
        with self._lock:
            message = self._failure_message
        if message:
            raise RuntimeError(message)

    @staticmethod
    def _key(symbol: str, timeframe: str, time: datetime) -> tuple[str, str, int]:
        return (_normalize_symbol(symbol), str(timeframe or "").strip(), int(_as_utc(time).timestamp()))


class LiveCandleAggregator:
    """Aggregate provider stream candle updates into closed runtime candles."""

    def __init__(
        self,
        *,
        target_timeframe: str,
        source_timeframe_seconds: int = 300,
    ) -> None:
        self.target_timeframe = str(target_timeframe or "").strip()
        target_delta = timeframe_duration(self.target_timeframe)
        if target_delta is None or target_delta.total_seconds() <= 0:
            raise ValueError(f"Unsupported target timeframe '{target_timeframe}'")
        self.target_seconds = int(target_delta.total_seconds())
        self.source_timeframe_seconds = max(int(source_timeframe_seconds or 0), 1)
        self._required_source_bucket_count = max(
            int(math.ceil(self.target_seconds / self.source_timeframe_seconds)),
            1,
        )
        self._open_source: dict[str, _SourceCandleSnapshot] = {}
        self._target: dict[str, _TargetAggregate] = {}
        self._provisional: dict[str, _ProvisionalAggregate] = {}
        self._ignored_out_of_order = 0
        self._ignored_snapshot_count = 0
        self._dropped_incomplete_target_count = 0

    def process(self, event: CanonicalMarketEvent | Mapping[str, Any]) -> list[ClosedLiveCandle]:
        payload = event.to_dict() if hasattr(event, "to_dict") else dict(event or {})
        if str(payload.get("event_kind") or "").strip() != "market_candle_update":
            return []
        market_payload = payload.get("payload") if isinstance(payload.get("payload"), Mapping) else {}
        if str(market_payload.get("type") or "").strip().lower() == "snapshot":
            self._ignored_snapshot_count += 1
            return []
        source = self._source_from_event(payload)
        if source is None:
            return []
        key = self._source_key(source)
        current = self._open_source.get(key)
        if current is None:
            self._open_source[key] = source
            return []
        if source.start == current.start:
            self._open_source[key] = _merge_source_update(current, source)
            return []
        if source.start < current.start:
            self._ignored_out_of_order += 1
            return []
        self._open_source[key] = source
        return self._ingest_closed_source(current)

    def provisional_from_event(self, event: CanonicalMarketEvent | Mapping[str, Any]) -> ProvisionalLiveCandle | None:
        payload = event.to_dict() if hasattr(event, "to_dict") else dict(event or {})
        event_kind = str(payload.get("event_kind") or "").strip()
        if event_kind == "market_candle_update":
            source = self._source_from_event(payload)
            if source is None:
                return None
            return self._provisional_from_source(source, source_event_kind=event_kind)
        if event_kind == "market_ticker":
            return self._provisional_from_ticker(payload, source_event_kind=event_kind)
        return None

    def snapshot(self) -> dict[str, Any]:
        return {
            "target_timeframe": self.target_timeframe,
            "source_timeframe_seconds": self.source_timeframe_seconds,
            "required_source_bucket_count": self._required_source_bucket_count,
            "open_source_count": len(self._open_source),
            "open_target_count": len(self._target),
            "open_provisional_count": len(self._provisional),
            "ignored_out_of_order_count": self._ignored_out_of_order,
            "ignored_snapshot_count": self._ignored_snapshot_count,
            "dropped_incomplete_target_count": self._dropped_incomplete_target_count,
        }

    def _source_from_event(self, event: Mapping[str, Any]) -> _SourceCandleSnapshot | None:
        payload = event.get("payload") if isinstance(event.get("payload"), Mapping) else {}
        symbol = _normalize_symbol(event.get("symbol") or event.get("product_id"))
        if not symbol:
            return None
        start = _parse_time(payload.get("start"))
        if start is None:
            return None
        try:
            open_price = float(payload.get("open"))
            high_price = float(payload.get("high"))
            low_price = float(payload.get("low"))
            close_price = float(payload.get("close"))
        except (TypeError, ValueError):
            return None
        volume = _coerce_float(payload.get("volume"))
        known_at = str(event.get("received_at") or event.get("provider_event_time") or "").strip() or None
        return _SourceCandleSnapshot(
            provider=str(event.get("provider") or "").strip().upper(),
            venue=str(event.get("venue") or "").strip().upper(),
            symbol=symbol,
            product_id=str(event.get("product_id") or "").strip() or None,
            start=start,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            first_known_at=known_at,
            last_known_at=known_at,
            source_type=str(payload.get("type") or "").strip().lower() or None,
        )

    def _provisional_from_source(
        self,
        source: _SourceCandleSnapshot,
        *,
        source_event_kind: str,
    ) -> ProvisionalLiveCandle:
        target_start = _floor_time(source.start, self.target_seconds)
        target_end = datetime.fromtimestamp(int(target_start.timestamp()) + self.target_seconds, tz=timezone.utc)
        key = self._target_key(source.symbol, source.product_id)
        current = self._provisional.get(key)
        if current is None or target_start > current.start:
            current = _ProvisionalAggregate(
                provider=source.provider,
                venue=source.venue,
                symbol=source.symbol,
                product_id=source.product_id,
                timeframe=self.target_timeframe,
                start=target_start,
                end=target_end,
                open=source.open,
                high=source.high,
                low=source.low,
                close=source.close,
                volume=source.volume,
                first_known_at=source.first_known_at,
                last_known_at=source.last_known_at,
                source_event_count=max(int(source.source_event_count or 0), 1),
                last_source_start=source.start,
                source_event_kind=source_event_kind,
            )
            self._provisional[key] = current
            return current.payload()
        if target_start < current.start:
            self._ignored_out_of_order += 1
            return current.payload()
        current.absorb_source(source, source_event_kind=source_event_kind)
        self._provisional[key] = current
        return current.payload()

    def _provisional_from_ticker(
        self,
        event: Mapping[str, Any],
        *,
        source_event_kind: str,
    ) -> ProvisionalLiveCandle | None:
        payload = event.get("payload") if isinstance(event.get("payload"), Mapping) else {}
        price = _coerce_float(payload.get("price"))
        symbol = _normalize_symbol(event.get("symbol") or event.get("product_id"))
        if price is None or not symbol:
            return None
        event_time = (
            _parse_time(event.get("provider_event_time"))
            or _parse_time(event.get("received_at"))
            or datetime.now(timezone.utc)
        )
        known_at = str(event.get("received_at") or event.get("provider_event_time") or "").strip() or None
        target_start = _floor_time(event_time, self.target_seconds)
        target_end = datetime.fromtimestamp(int(target_start.timestamp()) + self.target_seconds, tz=timezone.utc)
        product_id = str(event.get("product_id") or "").strip() or None
        key = self._target_key(symbol, product_id)
        current = self._provisional.get(key)
        if current is None or target_start > current.start:
            current = _ProvisionalAggregate(
                provider=str(event.get("provider") or "").strip().upper(),
                venue=str(event.get("venue") or "").strip().upper(),
                symbol=symbol,
                product_id=product_id,
                timeframe=self.target_timeframe,
                start=target_start,
                end=target_end,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=None,
                first_known_at=known_at,
                last_known_at=known_at,
                source_event_count=1,
                source_event_kind=source_event_kind,
            )
            self._provisional[key] = current
            return current.payload()
        if target_start < current.start:
            self._ignored_out_of_order += 1
            return current.payload()
        current.absorb_price(price=price, known_at=known_at, source_event_kind=source_event_kind)
        self._provisional[key] = current
        return current.payload()

    def _ingest_closed_source(self, source: _SourceCandleSnapshot) -> list[ClosedLiveCandle]:
        target_start = _floor_time(source.start, self.target_seconds)
        target_end = datetime.fromtimestamp(int(target_start.timestamp()) + self.target_seconds, tz=timezone.utc)
        key = self._target_key(source.symbol, source.product_id)
        current = self._target.get(key)
        closed: list[ClosedLiveCandle] = []
        if current is not None and target_start > current.start:
            if self._target_has_full_source_coverage(current):
                closed.append(current.close_payload())
                self._provisional.pop(key, None)
            else:
                self._dropped_incomplete_target_count += 1
            current = None
        if current is not None and target_start < current.start:
            self._ignored_out_of_order += 1
            return closed
        if current is None:
            current = _TargetAggregate(
                provider=source.provider,
                venue=source.venue,
                symbol=source.symbol,
                product_id=source.product_id,
                timeframe=self.target_timeframe,
                start=target_start,
                end=target_end,
                open=source.open,
                high=source.high,
                low=source.low,
                close=source.close,
                volume=source.volume,
                first_known_at=source.first_known_at,
                last_known_at=source.last_known_at,
                source_event_count=max(int(source.source_event_count or 0), 1),
                source_bucket_count=1,
            )
            self._target[key] = current
            if self._source_closes_target(source, current):
                if self._target_has_full_source_coverage(current):
                    closed.append(current.close_payload())
                    self._provisional.pop(key, None)
                else:
                    self._dropped_incomplete_target_count += 1
                self._target.pop(key, None)
            return closed
        current.absorb(source)
        self._target[key] = current
        if self._source_closes_target(source, current):
            if self._target_has_full_source_coverage(current):
                closed.append(current.close_payload())
                self._provisional.pop(key, None)
            else:
                self._dropped_incomplete_target_count += 1
            self._target.pop(key, None)
        return closed

    @staticmethod
    def _source_key(source: _SourceCandleSnapshot) -> str:
        return "|".join((source.provider, source.venue, source.symbol, str(source.product_id or "")))

    def _target_key(self, symbol: str, product_id: str | None) -> str:
        return "|".join((self.target_timeframe, _normalize_symbol(symbol), str(product_id or "")))

    def _source_closes_target(self, source: _SourceCandleSnapshot, target: _TargetAggregate) -> bool:
        source_end_epoch = int(source.start.timestamp()) + int(self.source_timeframe_seconds)
        return source_end_epoch >= int(target.end.timestamp())

    def _target_has_full_source_coverage(self, target: _TargetAggregate) -> bool:
        return int(target.source_bucket_count or 0) >= self._required_source_bucket_count


def append_closed_live_candles_to_series(
    *,
    store: LiveCandleStore,
    series: Any,
    after: datetime | None,
    limit: int | None = None,
) -> list[Candle]:
    closed = store.closed_after(
        symbol=getattr(series, "symbol", ""),
        timeframe=getattr(series, "timeframe", ""),
        after=after,
        limit=limit,
    )
    return [entry.to_runtime_candle() for entry in closed]


def _merge_source_update(current: _SourceCandleSnapshot, incoming: _SourceCandleSnapshot) -> _SourceCandleSnapshot:
    return _SourceCandleSnapshot(
        provider=current.provider or incoming.provider,
        venue=current.venue or incoming.venue,
        symbol=current.symbol,
        product_id=current.product_id or incoming.product_id,
        start=current.start,
        open=current.open,
        high=max(current.high, incoming.high),
        low=min(current.low, incoming.low),
        close=incoming.close,
        volume=incoming.volume,
        first_known_at=current.first_known_at or incoming.first_known_at,
        last_known_at=incoming.last_known_at or current.last_known_at,
        source_event_count=max(int(current.source_event_count or 0), 1) + 1,
        source_type=incoming.source_type or current.source_type,
    )


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        if text.isdigit():
            return datetime.fromtimestamp(int(text), tz=timezone.utc)
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    return _as_utc(parsed)


def _floor_time(value: datetime, bucket_seconds: int) -> datetime:
    epoch = int(_as_utc(value).timestamp())
    return datetime.fromtimestamp((epoch // int(bucket_seconds)) * int(bucket_seconds), tz=timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


__all__ = [
    "ClosedLiveCandle",
    "LiveCandleAggregator",
    "LiveCandleStore",
    "ProvisionalLiveCandle",
    "append_closed_live_candles_to_series",
]

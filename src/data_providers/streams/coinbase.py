"""Coinbase Advanced Trade public market-data stream adapter."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping, Sequence
from uuid import uuid4

from core.logger import logger

from .contracts import CanonicalMarketEvent, MarketSubscription

try:  # pragma: no cover - exercised through smoke path when installed.
    import websockets  # type: ignore
except ImportError:  # pragma: no cover - optional dependency guard.
    websockets = None


COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
SUPPORTED_PUBLIC_CHANNELS = {"heartbeats", "ticker", "candles"}


class CoinbaseMessageParser:
    """Translate Coinbase WebSocket messages into canonical market events."""

    def __init__(
        self,
        *,
        provider: str = "COINBASE",
        venue: str = "COINBASE_DIRECT",
        symbol_by_product_id: Mapping[str, str] | None = None,
    ) -> None:
        self.provider = str(provider or "COINBASE").upper()
        self.venue = str(venue or "COINBASE_DIRECT").upper()
        self.symbol_by_product_id = {str(key): str(value) for key, value in (symbol_by_product_id or {}).items()}
        self._last_sequence_by_key: dict[str, int] = {}

    def parse_raw(self, raw_message: str | bytes) -> list[CanonicalMarketEvent]:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            return [
                self._event(
                    "provider_malformed_message",
                    payload={"error": str(exc)},
                    raw_ref={"raw_preview": str(raw_message)[:300]},
                )
            ]
        if not isinstance(payload, dict):
            return [
                self._event(
                    "provider_malformed_message",
                    payload={"error": "message is not a JSON object"},
                    raw_ref={"raw_type": type(payload).__name__},
                )
            ]
        return self.parse(payload)

    def parse(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        events: list[CanonicalMarketEvent] = []
        events.extend(self._sequence_events(message))

        channel = str(message.get("channel") or message.get("type") or "").strip().lower()
        if channel == "subscriptions":
            events.extend(self._parse_subscriptions(message))
        elif channel == "heartbeats":
            events.extend(self._parse_heartbeats(message))
        elif channel == "ticker":
            events.extend(self._parse_ticker(message))
        elif channel == "candles":
            events.extend(self._parse_candles(message))
        else:
            events.append(
                self._event(
                    "provider_unsupported_message",
                    provider_sequence_num=_coerce_int(message.get("sequence_num")),
                    provider_event_time=_str_or_none(message.get("timestamp")),
                    payload={"channel": channel or None},
                    raw_ref=_bounded_raw_ref(message),
                )
            )
        return events

    def _parse_subscriptions(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        results: list[CanonicalMarketEvent] = []
        for event in _iter_event_objects(message):
            subscriptions = event.get("subscriptions")
            if not isinstance(subscriptions, Mapping):
                continue
            results.append(
                self._event(
                    "provider_subscription_ack",
                    provider_sequence_num=_coerce_int(message.get("sequence_num")),
                    provider_event_time=_str_or_none(message.get("timestamp")),
                    payload={"subscriptions": _json_safe(subscriptions)},
                    raw_ref={"channel": "subscriptions"},
                )
            )
        if not results:
            results.append(
                self._event(
                    "provider_subscription_ack",
                    provider_sequence_num=_coerce_int(message.get("sequence_num")),
                    provider_event_time=_str_or_none(message.get("timestamp")),
                    payload={},
                    raw_ref={"channel": "subscriptions"},
                )
            )
        return results

    def _parse_heartbeats(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        results: list[CanonicalMarketEvent] = []
        for event in _iter_event_objects(message):
            results.append(
                self._event(
                    "provider_heartbeat",
                    provider_sequence_num=_coerce_int(message.get("sequence_num")),
                    provider_event_time=_str_or_none(message.get("timestamp")),
                    payload={
                        "current_time": _str_or_none(event.get("current_time")),
                        "heartbeat_counter": _str_or_none(event.get("heartbeat_counter")),
                    },
                    raw_ref={"channel": "heartbeats"},
                )
            )
        return results

    def _parse_ticker(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        results: list[CanonicalMarketEvent] = []
        for event in _iter_event_objects(message):
            event_type = _str_or_none(event.get("type"))
            for ticker in _iter_child_objects(event, "tickers"):
                product_id = _str_or_none(ticker.get("product_id"))
                results.append(
                    self._event(
                        "market_ticker",
                        symbol=self._symbol_for_product(product_id),
                        product_id=product_id,
                        provider_sequence_num=_coerce_int(message.get("sequence_num")),
                        provider_event_time=_str_or_none(message.get("timestamp")),
                        payload={
                            "type": event_type,
                            "price": _str_or_none(ticker.get("price")),
                            "volume_24_h": _str_or_none(ticker.get("volume_24_h")),
                            "low_24_h": _str_or_none(ticker.get("low_24_h")),
                            "high_24_h": _str_or_none(ticker.get("high_24_h")),
                            "best_bid": _str_or_none(ticker.get("best_bid")),
                            "best_ask": _str_or_none(ticker.get("best_ask")),
                            "best_bid_quantity": _str_or_none(ticker.get("best_bid_quantity")),
                            "best_ask_quantity": _str_or_none(ticker.get("best_ask_quantity")),
                        },
                        raw_ref={"channel": "ticker"},
                    )
                )
        return results

    def _parse_candles(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        results: list[CanonicalMarketEvent] = []
        for event in _iter_event_objects(message):
            event_type = _str_or_none(event.get("type"))
            for candle in _iter_child_objects(event, "candles"):
                product_id = _str_or_none(candle.get("product_id"))
                results.append(
                    self._event(
                        "market_candle_update",
                        symbol=self._symbol_for_product(product_id),
                        product_id=product_id,
                        provider_sequence_num=_coerce_int(message.get("sequence_num")),
                        provider_event_time=_str_or_none(message.get("timestamp")),
                        payload={
                            "type": event_type,
                            "start": _str_or_none(candle.get("start")),
                            "open": _str_or_none(candle.get("open")),
                            "high": _str_or_none(candle.get("high")),
                            "low": _str_or_none(candle.get("low")),
                            "close": _str_or_none(candle.get("close")),
                            "volume": _str_or_none(candle.get("volume")),
                        },
                        raw_ref={"channel": "candles"},
                    )
                )
        return results

    def _sequence_events(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        sequence_num = _coerce_int(message.get("sequence_num"))
        if sequence_num is None:
            return []
        channel = str(message.get("channel") or message.get("type") or "unknown").strip().lower() or "unknown"
        keys = self._sequence_keys(message, channel)
        results: list[CanonicalMarketEvent] = []
        for key in keys:
            last = self._last_sequence_by_key.get(key)
            if last is not None and sequence_num > last + 1:
                results.append(
                    self._event(
                        "provider_sequence_gap",
                        provider_sequence_num=sequence_num,
                        provider_event_time=_str_or_none(message.get("timestamp")),
                        payload={
                            "sequence_key": key,
                            "previous_sequence_num": last,
                            "current_sequence_num": sequence_num,
                            "missing_count": sequence_num - last - 1,
                            "status": "gap",
                        },
                        raw_ref={"channel": channel},
                    )
                )
            elif last is not None and sequence_num <= last:
                results.append(
                    self._event(
                        "provider_sequence_gap",
                        provider_sequence_num=sequence_num,
                        provider_event_time=_str_or_none(message.get("timestamp")),
                        payload={
                            "sequence_key": key,
                            "previous_sequence_num": last,
                            "current_sequence_num": sequence_num,
                            "missing_count": 0,
                            "status": "out_of_order",
                        },
                        raw_ref={"channel": channel},
                    )
                )
            if last is None or sequence_num > last:
                self._last_sequence_by_key[key] = sequence_num
        return results

    def _sequence_keys(self, message: Mapping[str, Any], channel: str) -> list[str]:
        del channel
        return ["connection"]

    def _symbol_for_product(self, product_id: str | None) -> str | None:
        if not product_id:
            return None
        return self.symbol_by_product_id.get(product_id, product_id)

    def _event(
        self,
        event_kind: str,
        *,
        symbol: str | None = None,
        product_id: str | None = None,
        provider_sequence_num: int | None = None,
        provider_event_time: str | None = None,
        payload: Mapping[str, Any] | None = None,
        raw_ref: Mapping[str, Any] | None = None,
    ) -> CanonicalMarketEvent:
        return CanonicalMarketEvent.build(
            event_kind=event_kind,
            provider=self.provider,
            venue=self.venue,
            symbol=symbol,
            product_id=product_id,
            provider_sequence_num=provider_sequence_num,
            provider_event_time=provider_event_time,
            received_at=datetime.now(timezone.utc).isoformat(),
            payload=payload,
            raw_ref=raw_ref,
        )


class CoinbaseAdvancedTradeStream:
    """Read-only Coinbase Advanced Trade market-data WebSocket adapter."""

    def __init__(
        self,
        *,
        url: str = COINBASE_WS_URL,
        provider: str = "COINBASE",
        venue: str = "COINBASE_DIRECT",
    ) -> None:
        self.url = str(url or COINBASE_WS_URL)
        self.provider = str(provider or "COINBASE").upper()
        self.venue = str(venue or "COINBASE_DIRECT").upper()
        self.stream_session_id = uuid4().hex
        self._ws: Any = None
        self._parser = CoinbaseMessageParser(provider=self.provider, venue=self.venue)

    async def connect(self) -> None:
        if websockets is None:
            raise RuntimeError(
                "Coinbase WebSocket streaming requires the 'websockets' package. "
                "Install project dependencies before running provider stream smoke checks."
            )
        self._ws = await websockets.connect(self.url)
        logger.info(
            "provider_stream_connected | provider=%s venue=%s stream_session_id=%s url=%s",
            self.provider,
            self.venue,
            self.stream_session_id,
            self.url,
        )

    async def subscribe(self, subscriptions: Sequence[MarketSubscription]) -> None:
        if self._ws is None:
            raise RuntimeError("Coinbase stream is not connected.")
        symbol_by_product_id = {}
        channels_by_product_id: dict[str, list[str]] = {}
        heartbeat_requested = False
        for subscription in subscriptions:
            if subscription.provider != self.provider or subscription.venue != self.venue:
                raise ValueError(
                    "Coinbase stream received subscription for a different provider/venue: "
                    f"{subscription.provider}/{subscription.venue}"
                )
            symbol_by_product_id[subscription.product_id] = subscription.symbol
            for channel in subscription.channels:
                normalized_channel = str(channel).strip().lower()
                if normalized_channel not in SUPPORTED_PUBLIC_CHANNELS:
                    raise ValueError(
                        f"Unsupported Coinbase public channel '{channel}'. "
                        f"Supported channels: {', '.join(sorted(SUPPORTED_PUBLIC_CHANNELS))}"
                    )
                if normalized_channel == "heartbeats":
                    heartbeat_requested = True
                    continue
                channel_products = channels_by_product_id.setdefault(normalized_channel, [])
                if subscription.product_id not in channel_products:
                    channel_products.append(subscription.product_id)
        if heartbeat_requested:
            await self._send_subscribe_message(channel="heartbeats")
        for channel, product_ids in channels_by_product_id.items():
            await self._send_subscribe_message(channel=channel, product_ids=product_ids)
        self._parser = CoinbaseMessageParser(
            provider=self.provider,
            venue=self.venue,
            symbol_by_product_id=symbol_by_product_id,
        )

    async def _send_subscribe_message(self, *, channel: str, product_ids: Sequence[str] | None = None) -> None:
        message: dict[str, Any] = {"type": "subscribe", "channel": channel}
        if channel != "heartbeats":
            message["product_ids"] = list(product_ids or [])
        await self._ws.send(json.dumps(message, separators=(",", ":")))
        logger.info(
            "provider_stream_subscription_sent | provider=%s venue=%s stream_session_id=%s channel=%s product_ids=%s",
            self.provider,
            self.venue,
            self.stream_session_id,
            channel,
            list(product_ids or []),
        )

    async def events(self) -> AsyncIterator[CanonicalMarketEvent]:
        if self._ws is None:
            raise RuntimeError("Coinbase stream is not connected.")
        async for raw_message in self._ws:
            for event in self._parser.parse_raw(raw_message):
                yield event
        yield CanonicalMarketEvent.build(
            event_kind="provider_disconnected",
            provider=self.provider,
            venue=self.venue,
            payload={
                "stream_session_id": self.stream_session_id,
                "close_code": getattr(self._ws, "close_code", None),
                "close_reason": getattr(self._ws, "close_reason", None),
            },
            raw_ref={"url": self.url},
        )

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            logger.info(
                "provider_stream_closed | provider=%s venue=%s stream_session_id=%s",
                self.provider,
                self.venue,
                self.stream_session_id,
            )
        self._ws = None


def _iter_event_objects(message: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    events = message.get("events")
    if not isinstance(events, list):
        return []
    return [event for event in events if isinstance(event, Mapping)]


def _iter_child_objects(event: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = event.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


def _bounded_raw_ref(message: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "channel": _str_or_none(message.get("channel")),
        "type": _str_or_none(message.get("type")),
        "keys": sorted(str(key) for key in message.keys()),
    }


__all__ = [
    "COINBASE_WS_URL",
    "SUPPORTED_PUBLIC_CHANNELS",
    "CoinbaseAdvancedTradeStream",
    "CoinbaseMessageParser",
]

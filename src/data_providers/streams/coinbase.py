"""Coinbase Advanced Trade public market-data stream adapter."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Mapping, Sequence
from uuid import uuid4

from core.logger import logger

from .contracts import CanonicalMarketEvent, MarketSubscription, ProviderRawMessage

try:  # pragma: no cover - exercised through smoke path when installed.
    import websockets  # type: ignore
except ImportError:  # pragma: no cover - optional dependency guard.
    websockets = None


COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
COINBASE_WS_MAX_MESSAGE_BYTES = 16 * 1024 * 1024
COINBASE_WS_WIRE_LOGGER = logging.getLogger("quanttrad.provider.coinbase.websocket")
COINBASE_WS_WIRE_LOGGER.setLevel(logging.INFO)

SUPPORTED_PUBLIC_CHANNELS = {
    "candles",
    "heartbeats",
    "level2",
    "market_trades",
    "ticker",
}


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
        self._received_at: str | None = None
        self._raw_ref: dict[str, Any] = {}

    def parse_raw(
        self,
        raw_message: str | bytes,
        *,
        received_at: str | None = None,
        raw_ref: Mapping[str, Any] | None = None,
    ) -> list[CanonicalMarketEvent]:
        previous_received_at = self._received_at
        previous_raw_ref = self._raw_ref
        self._received_at = received_at
        self._raw_ref = dict(raw_ref or {})
        try:
            try:
                payload = json.loads(raw_message)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                return [
                    self._event(
                        "provider_malformed_message",
                        payload={"error": str(exc)},
                        raw_ref={"raw_type": type(raw_message).__name__},
                    )
                ]
            if not isinstance(payload, Mapping):
                return [
                    self._event(
                        "provider_malformed_message",
                        payload={"error": "Coinbase message must decode to an object."},
                        raw_ref={"raw_type": type(payload).__name__},
                    )
                ]
            return self._parse_message(payload)
        finally:
            self._received_at = previous_received_at
            self._raw_ref = previous_raw_ref

    def parse(
        self,
        message: Mapping[str, Any],
        *,
        received_at: str | None = None,
        raw_ref: Mapping[str, Any] | None = None,
    ) -> list[CanonicalMarketEvent]:
        previous_received_at = self._received_at
        previous_raw_ref = self._raw_ref
        self._received_at = received_at
        self._raw_ref = dict(raw_ref or {})
        try:
            return self._parse_message(message)
        finally:
            self._received_at = previous_received_at
            self._raw_ref = previous_raw_ref

    def _parse_message(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
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
        elif channel == "market_trades":
            events.extend(self._parse_market_trades(message))
        elif channel in {"level2", "l2_data"}:
            events.extend(self._parse_level2(message))
        else:
            events.append(
                self._event(
                    "provider_unsupported_message",
                    provider_sequence_num=_coerce_int(message.get("sequence_num")),
                    provider_message_time=_str_or_none(message.get("timestamp")),
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
                    provider_message_time=_str_or_none(message.get("timestamp")),
                    payload={"subscriptions": _json_safe(subscriptions)},
                    raw_ref={"channel": "subscriptions"},
                )
            )
        if not results:
            results.append(
                self._event(
                    "provider_subscription_ack",
                    provider_sequence_num=_coerce_int(message.get("sequence_num")),
                    provider_message_time=_str_or_none(message.get("timestamp")),
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
                    provider_message_time=_str_or_none(message.get("timestamp")),
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
                        provider_message_time=_str_or_none(message.get("timestamp")),
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
                        provider_message_time=_str_or_none(message.get("timestamp")),
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

    def _parse_market_trades(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        results: list[CanonicalMarketEvent] = []
        message_time = _str_or_none(message.get("timestamp"))
        sequence_num = _coerce_int(message.get("sequence_num"))
        for event_ordinal, event in enumerate(_iter_event_objects(message)):
            event_type = _str_or_none(event.get("type"))
            for trade_ordinal, trade in enumerate(_iter_child_objects(event, "trades")):
                product_id = _str_or_none(trade.get("product_id"))
                results.append(
                    self._event(
                        "market_trade",
                        symbol=self._symbol_for_product(product_id),
                        product_id=product_id,
                        provider_sequence_num=sequence_num,
                        provider_event_time=_str_or_none(trade.get("time")),
                        provider_message_time=message_time,
                        payload={
                            "type": event_type,
                            "event_ordinal": event_ordinal,
                            "trade_ordinal": trade_ordinal,
                            "trade_id": _str_or_none(trade.get("trade_id")),
                            "price": _str_or_none(trade.get("price")),
                            "size": _str_or_none(trade.get("size")),
                            "side": _str_or_none(trade.get("side")),
                        },
                        raw_ref={
                            "channel": "market_trades",
                            "event_ordinal": event_ordinal,
                            "trade_ordinal": trade_ordinal,
                        },
                    )
                )
        return results

    def _parse_level2(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        results: list[CanonicalMarketEvent] = []
        message_time = _str_or_none(message.get("timestamp"))
        sequence_num = _coerce_int(message.get("sequence_num"))
        for event_ordinal, event in enumerate(_iter_event_objects(message)):
            event_type = str(event.get("type") or "").strip().lower()
            product_id = _str_or_none(event.get("product_id"))
            updates: list[dict[str, Any]] = []
            for mutation_ordinal, update in enumerate(_iter_child_objects(event, "updates")):
                updates.append(
                    {
                        "mutation_ordinal": mutation_ordinal,
                        "side": _str_or_none(update.get("side")),
                        "event_time": _str_or_none(update.get("event_time")),
                        "price_level": _str_or_none(update.get("price_level")),
                        "new_quantity": _str_or_none(update.get("new_quantity")),
                    }
                )
            if event_type not in {"snapshot", "update"}:
                results.append(
                    self._event(
                        "provider_unsupported_message",
                        symbol=self._symbol_for_product(product_id),
                        product_id=product_id,
                        provider_sequence_num=sequence_num,
                        provider_message_time=message_time,
                        payload={
                            "channel": "level2",
                            "event_type": event_type or None,
                            "event_ordinal": event_ordinal,
                            "updates": updates,
                        },
                        raw_ref={"channel": "level2", "event_ordinal": event_ordinal},
                    )
                )
                continue
            kind = "market_l2_snapshot" if event_type == "snapshot" else "market_l2_update"
            results.append(
                self._event(
                    kind,
                    symbol=self._symbol_for_product(product_id),
                    product_id=product_id,
                    provider_sequence_num=sequence_num,
                    provider_message_time=message_time,
                    payload={
                        "type": event_type or None,
                        "event_ordinal": event_ordinal,
                        "updates": updates,
                    },
                    raw_ref={"channel": "level2", "event_ordinal": event_ordinal},
                )
            )
        return results

    def _sequence_events(self, message: Mapping[str, Any]) -> list[CanonicalMarketEvent]:
        sequence_num = _coerce_int(message.get("sequence_num"))
        if sequence_num is None:
            return []
        channel = str(message.get("channel") or message.get("type") or "unknown").strip().lower() or "unknown"
        results: list[CanonicalMarketEvent] = []
        for key in ("connection",):
            last = self._last_sequence_by_key.get(key)
            if last is not None and sequence_num > last + 1:
                results.append(
                    self._event(
                        "provider_sequence_gap",
                        provider_sequence_num=sequence_num,
                        provider_message_time=_str_or_none(message.get("timestamp")),
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
            elif last is not None and sequence_num == last:
                results.append(
                    self._event(
                        "provider_sequence_gap",
                        provider_sequence_num=sequence_num,
                        provider_message_time=_str_or_none(message.get("timestamp")),
                        payload={
                            "sequence_key": key,
                            "previous_sequence_num": last,
                            "current_sequence_num": sequence_num,
                            "missing_count": 0,
                            "status": "duplicate",
                        },
                        raw_ref={"channel": channel},
                    )
                )
            elif last is not None and sequence_num < last:
                results.append(
                    self._event(
                        "provider_sequence_gap",
                        provider_sequence_num=sequence_num,
                        provider_message_time=_str_or_none(message.get("timestamp")),
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

    def reset_sequence(self) -> None:
        self._last_sequence_by_key.clear()

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
        provider_message_time: str | None = None,
        payload: Mapping[str, Any] | None = None,
        raw_ref: Mapping[str, Any] | None = None,
    ) -> CanonicalMarketEvent:
        merged_raw_ref = dict(self._raw_ref)
        merged_raw_ref.update(dict(raw_ref or {}))
        return CanonicalMarketEvent.build(
            event_kind=event_kind,
            provider=self.provider,
            venue=self.venue,
            symbol=symbol,
            product_id=product_id,
            provider_sequence_num=provider_sequence_num,
            provider_event_time=provider_event_time,
            provider_message_time=provider_message_time,
            received_at=self._received_at or datetime.now(timezone.utc).isoformat(),
            payload=payload,
            raw_ref=merged_raw_ref,
        )


class CoinbaseAdvancedTradeStream:
    """Read-only Coinbase Advanced Trade market-data WebSocket adapter."""

    def __init__(
        self,
        *,
        url: str = COINBASE_WS_URL,
        provider: str = "COINBASE",
        venue: str = "COINBASE_DIRECT",
        jwt_factory: Callable[[], str] | None = None,
        max_message_bytes: int = COINBASE_WS_MAX_MESSAGE_BYTES,
        stream_session_id: str | None = None,
    ) -> None:
        self.url = str(url or COINBASE_WS_URL)
        self.provider = str(provider or "COINBASE").upper()
        self.venue = str(venue or "COINBASE_DIRECT").upper()
        self.jwt_factory = jwt_factory
        self.max_message_bytes = int(max_message_bytes)
        if self.max_message_bytes < 1:
            raise ValueError("Coinbase WebSocket max_message_bytes must be positive.")
        self.stream_session_id = str(stream_session_id or uuid4().hex).strip()
        if not self.stream_session_id:
            raise ValueError("Coinbase stream_session_id cannot be empty.")
        self._ws: Any = None
        self._parser = CoinbaseMessageParser(provider=self.provider, venue=self.venue)
        self._connection_epoch = -1
        self._receive_ordinal = 0

    async def connect(self) -> int:
        if websockets is None:
            raise RuntimeError(
                "Coinbase WebSocket streaming requires the 'websockets' package. "
                "Install project dependencies before running provider stream smoke checks."
            )
        self._ws = await websockets.connect(
            self.url,
            max_size=self.max_message_bytes,
            logger=COINBASE_WS_WIRE_LOGGER,
        )
        self._connection_epoch += 1
        self._receive_ordinal = 0
        self._parser.reset_sequence()
        logger.info(
            "provider_stream_connected | provider=%s venue=%s stream_session_id=%s url=%s",
            self.provider,
            self.venue,
            self.stream_session_id,
            self.url,
        )
        return self._connection_epoch

    async def subscribe(self, subscriptions: Sequence[MarketSubscription]) -> None:
        if self._ws is None:
            raise RuntimeError("Coinbase stream is not connected.")
        symbol_by_product_id = {}
        channels_by_product_id: dict[str, list[str]] = {}
        heartbeat_requested = False
        auth_modes: set[str] = set()
        for subscription in subscriptions:
            if subscription.provider != self.provider or subscription.venue != self.venue:
                raise ValueError(
                    "Coinbase stream received subscription for a different provider/venue: "
                    f"{subscription.provider}/{subscription.venue}"
                )
            symbol_by_product_id[subscription.product_id] = subscription.symbol
            if subscription.auth_mode not in {"public", "authenticated"}:
                raise ValueError(
                    "Coinbase stream auth_mode must be 'public' or 'authenticated': "
                    f"{subscription.auth_mode!r}"
                )
            auth_modes.add(subscription.auth_mode)
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
        if len(auth_modes) > 1:
            raise ValueError("One Coinbase stream connection cannot mix public and authenticated subscriptions.")
        auth_mode = next(iter(auth_modes), "public")
        if auth_mode == "authenticated" and self.jwt_factory is None:
            raise RuntimeError(
                "Authenticated Coinbase stream subscription requires the shared provider credential boundary."
            )
        if heartbeat_requested:
            await self._send_subscribe_message(channel="heartbeats", auth_mode=auth_mode)
        for channel, product_ids in channels_by_product_id.items():
            await self._send_subscribe_message(
                channel=channel,
                product_ids=product_ids,
                auth_mode=auth_mode,
            )
        self._parser = CoinbaseMessageParser(
            provider=self.provider,
            venue=self.venue,
            symbol_by_product_id=symbol_by_product_id,
        )

    async def _send_subscribe_message(
        self,
        *,
        channel: str,
        product_ids: Sequence[str] | None = None,
        auth_mode: str = "public",
    ) -> None:
        message: dict[str, Any] = {"type": "subscribe", "channel": channel}
        if channel != "heartbeats":
            message["product_ids"] = list(product_ids or [])
        if auth_mode == "authenticated":
            if self.jwt_factory is None:
                raise RuntimeError("Coinbase JWT factory is unavailable.")
            token = str(self.jwt_factory() or "").strip()
            if not token:
                raise RuntimeError("Coinbase provider credential boundary returned an empty WebSocket JWT.")
            message["jwt"] = token
        await self._ws.send(json.dumps(message, separators=(",", ":")))
        logger.info(
            "provider_stream_subscription_sent | provider=%s venue=%s stream_session_id=%s channel=%s product_ids=%s",
            self.provider,
            self.venue,
            self.stream_session_id,
            channel,
            list(product_ids or []),
        )

    async def raw_messages(self) -> AsyncIterator[ProviderRawMessage]:
        if self._ws is None:
            raise RuntimeError("Coinbase stream is not connected.")
        async for raw_message in self._ws:
            self._receive_ordinal += 1
            yield ProviderRawMessage.build(
                provider=self.provider,
                venue=self.venue,
                stream_session_id=self.stream_session_id,
                connection_epoch=self._connection_epoch,
                receive_ordinal=self._receive_ordinal,
                raw_frame=raw_message,
            )

    async def events(self) -> AsyncIterator[CanonicalMarketEvent]:
        async for raw_message in self.raw_messages():
            for event in self._parser.parse_raw(
                raw_message.raw_frame,
                received_at=raw_message.received_at,
                raw_ref=raw_message.evidence_ref(),
            ):
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

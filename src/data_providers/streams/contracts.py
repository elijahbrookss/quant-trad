"""Canonical provider stream contracts.

These contracts are read-only market-data contracts. They intentionally do not
model orders, fills, wallet effects, or runtime execution semantics.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping, Protocol, Sequence
from uuid import uuid4


@dataclass(frozen=True)
class MarketSubscription:
    """A provider-neutral market data subscription request."""

    provider: str
    venue: str
    symbol: str
    product_id: str
    channels: tuple[str, ...] = ("heartbeats", "ticker", "candles")
    timeframe: str | None = None
    auth_mode: str = "public"

    @classmethod
    def from_values(
        cls,
        *,
        provider: str,
        venue: str,
        symbol: str,
        product_id: str | None = None,
        channels: Sequence[str] | None = None,
        timeframe: str | None = None,
        auth_mode: str = "public",
    ) -> "MarketSubscription":
        normalized_channels = tuple(
            str(channel).strip().lower()
            for channel in (channels or ("heartbeats", "ticker", "candles"))
            if str(channel).strip()
        )
        if not normalized_channels:
            raise ValueError("At least one market data channel is required.")
        normalized_symbol = str(symbol or "").strip()
        if not normalized_symbol:
            raise ValueError("symbol is required for market data subscriptions")
        return cls(
            provider=str(provider or "").strip().upper(),
            venue=str(venue or "").strip().upper(),
            symbol=normalized_symbol,
            product_id=str(product_id or normalized_symbol).strip(),
            channels=normalized_channels,
            timeframe=str(timeframe).strip() if timeframe else None,
            auth_mode=str(auth_mode or "public").strip().lower(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "venue": self.venue,
            "symbol": self.symbol,
            "product_id": self.product_id,
            "channels": list(self.channels),
            "timeframe": self.timeframe,
            "auth_mode": self.auth_mode,
        }


@dataclass(frozen=True)
class ProviderRawMessage:
    """One exact inbound WebSocket application payload with local receipt ordering.

    This is acquisition evidence, not an archive acknowledgement. The durable
    collector assigns ``raw_record_id`` only after it knows the fenced stream
    definition and has selected a spool segment. Transport framing and
    compression bytes are outside this application-message contract.
    """

    provider: str
    venue: str
    stream_session_id: str
    connection_epoch: int
    receive_ordinal: int
    received_at: str
    raw_frame: bytes
    raw_frame_sha256: str

    @classmethod
    def build(
        cls,
        *,
        provider: str,
        venue: str,
        stream_session_id: str,
        connection_epoch: int,
        receive_ordinal: int,
        raw_frame: str | bytes,
        received_at: str | None = None,
    ) -> "ProviderRawMessage":
        frame_bytes = raw_frame.encode("utf-8") if isinstance(raw_frame, str) else bytes(raw_frame)
        return cls(
            provider=str(provider or "").strip().upper(),
            venue=str(venue or "").strip().upper(),
            stream_session_id=str(stream_session_id or "").strip(),
            connection_epoch=int(connection_epoch),
            receive_ordinal=int(receive_ordinal),
            received_at=received_at or datetime.now(timezone.utc).isoformat(),
            raw_frame=frame_bytes,
            raw_frame_sha256=hashlib.sha256(frame_bytes).hexdigest(),
        )

    def evidence_ref(self) -> dict[str, Any]:
        return {
            "stream_session_id": self.stream_session_id,
            "connection_epoch": self.connection_epoch,
            "receive_ordinal": self.receive_ordinal,
            "raw_frame_sha256": self.raw_frame_sha256,
            "raw_frame_bytes": len(self.raw_frame),
        }


@dataclass(frozen=True)
class CanonicalMarketEvent:
    """A provider-neutral market data event emitted by stream adapters."""

    event_kind: str
    provider: str
    venue: str
    symbol: str | None = None
    product_id: str | None = None
    provider_sequence_num: int | None = None
    provider_event_time: str | None = None
    provider_message_time: str | None = None
    received_at: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    raw_ref: Mapping[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: uuid4().hex)

    @classmethod
    def build(
        cls,
        *,
        event_kind: str,
        provider: str,
        venue: str,
        symbol: str | None = None,
        product_id: str | None = None,
        provider_sequence_num: int | None = None,
        provider_event_time: str | None = None,
        provider_message_time: str | None = None,
        received_at: str | None = None,
        payload: Mapping[str, Any] | None = None,
        raw_ref: Mapping[str, Any] | None = None,
    ) -> "CanonicalMarketEvent":
        return cls(
            event_kind=str(event_kind),
            provider=str(provider or "").upper(),
            venue=str(venue or "").upper(),
            symbol=symbol,
            product_id=product_id,
            provider_sequence_num=provider_sequence_num,
            provider_event_time=provider_event_time,
            provider_message_time=provider_message_time,
            received_at=received_at or datetime.now(timezone.utc).isoformat(),
            payload=dict(payload or {}),
            raw_ref=dict(raw_ref or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_kind": self.event_kind,
            "provider": self.provider,
            "venue": self.venue,
            "symbol": self.symbol,
            "product_id": self.product_id,
            "provider_sequence_num": self.provider_sequence_num,
            "provider_event_time": self.provider_event_time,
            "provider_message_time": self.provider_message_time,
            "received_at": self.received_at,
            "payload": dict(self.payload or {}),
            "raw_ref": dict(self.raw_ref or {}),
        }


class ProviderMarketDataStream(Protocol):
    """Async provider stream boundary for read-only market data."""

    async def connect(self) -> int:
        """Connect and return the epoch assigned to that successful connection."""

        ...

    async def subscribe(self, subscriptions: Sequence[MarketSubscription]) -> None:
        ...

    async def raw_messages(self) -> AsyncIterator[ProviderRawMessage]:
        ...

    async def events(self) -> AsyncIterator[CanonicalMarketEvent]:
        ...

    async def close(self) -> None:
        ...

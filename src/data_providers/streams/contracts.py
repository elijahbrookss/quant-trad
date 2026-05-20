"""Canonical provider stream contracts.

These contracts are read-only market-data contracts. They intentionally do not
model orders, fills, wallet effects, or runtime execution semantics.
"""

from __future__ import annotations

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
class CanonicalMarketEvent:
    """A provider-neutral market data event emitted by stream adapters."""

    event_kind: str
    provider: str
    venue: str
    symbol: str | None = None
    product_id: str | None = None
    provider_sequence_num: int | None = None
    provider_event_time: str | None = None
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
            "received_at": self.received_at,
            "payload": dict(self.payload or {}),
            "raw_ref": dict(self.raw_ref or {}),
        }


class ProviderMarketDataStream(Protocol):
    """Async provider stream boundary for read-only market data."""

    async def connect(self) -> None:
        ...

    async def subscribe(self, subscriptions: Sequence[MarketSubscription]) -> None:
        ...

    async def events(self) -> AsyncIterator[CanonicalMarketEvent]:
        ...

    async def close(self) -> None:
        ...

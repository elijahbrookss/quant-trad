from __future__ import annotations

import asyncio

import pytest

from data_providers.streams.contracts import CanonicalMarketEvent
from portal.backend.service.providers import stream_smoke


class _FakeStream:
    def __init__(self) -> None:
        self.connected = False
        self.closed = False
        self.subscriptions = []

    async def connect(self) -> None:
        self.connected = True

    async def subscribe(self, subscriptions) -> None:
        self.subscriptions.extend(subscriptions)

    async def events(self):
        yield CanonicalMarketEvent.build(
            event_kind="provider_subscription_ack",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            payload={"subscriptions": {"ticker": ["BIP-20DEC30-CDE"]}},
        )
        yield CanonicalMarketEvent.build(
            event_kind="provider_heartbeat",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            payload={"heartbeat_counter": "1"},
        )
        yield CanonicalMarketEvent.build(
            event_kind="market_ticker",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            symbol="BIP-20DEC30-CDE",
            product_id="BIP-20DEC30-CDE",
            payload={"price": "103000"},
        )

    async def close(self) -> None:
        self.closed = True


class _SparseStream(_FakeStream):
    async def events(self):
        yield CanonicalMarketEvent.build(
            event_kind="provider_subscription_ack",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            payload={"subscriptions": {"candles": ["BIP-20DEC30-CDE"]}},
        )
        await asyncio.sleep(1.05)
        yield CanonicalMarketEvent.build(
            event_kind="provider_heartbeat",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            payload={"heartbeat_counter": "1"},
        )
        while True:
            await asyncio.sleep(10.0)


@pytest.mark.asyncio
async def test_provider_stream_smoke_summarizes_fake_stream() -> None:
    fake = _FakeStream()

    result = await stream_smoke.run_provider_stream_smoke(
        provider_id="COINBASE",
        venue_id="COINBASE_DIRECT",
        symbol="BIP-20DEC30-CDE",
        duration_seconds=0.1,
        stream_factory=lambda: fake,
    )

    assert result["schema_version"] == "provider_stream_smoke.v1"
    assert result["status"] == "ended_early"
    assert result["counts"]["provider_connected"] == 1
    assert result["counts"]["provider_subscription_ack"] == 1
    assert result["counts"]["provider_heartbeat"] == 1
    assert result["counts"]["market_ticker"] == 1
    assert result["latest"]["ticker"]["payload"]["price"] == "103000"
    assert result["diagnostics"]["heartbeat_count"] == 1
    assert result["diagnostics"]["stream_ended_early"] is True
    assert fake.connected is True
    assert fake.closed is True
    assert fake.subscriptions[0].product_id == "BIP-20DEC30-CDE"


@pytest.mark.asyncio
async def test_provider_stream_smoke_does_not_close_sparse_stream_on_timeout() -> None:
    fake = _SparseStream()

    result = await stream_smoke.run_provider_stream_smoke(
        provider_id="COINBASE",
        venue_id="COINBASE_DIRECT",
        symbol="BIP-20DEC30-CDE",
        duration_seconds=1.2,
        stream_factory=lambda: fake,
    )

    assert result["status"] == "completed"
    assert result["diagnostics"]["timeout_count"] >= 1
    assert result["diagnostics"]["stream_ended_early"] is False
    assert result["counts"]["provider_heartbeat"] == 1
    assert fake.closed is True


@pytest.mark.asyncio
async def test_provider_stream_smoke_rejects_non_coinbase_provider() -> None:
    with pytest.raises(ValueError, match="only COINBASE"):
        await stream_smoke.run_provider_stream_smoke(
            provider_id="OTHER",
            venue_id="COINBASE_DIRECT",
            symbol="BIP-20DEC30-CDE",
            duration_seconds=0.1,
        )

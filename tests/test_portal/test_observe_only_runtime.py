from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from data_providers.streams import CanonicalMarketEvent
from portal.backend.service.bots import observe_only_runtime


class _FakeStream:
    subscriptions = []
    closed = False

    def __init__(self, *, provider: str, venue: str) -> None:
        self.provider = provider
        self.venue = venue

    async def connect(self) -> None:
        return None

    async def subscribe(self, subscriptions) -> None:
        type(self).subscriptions = list(subscriptions)

    async def events(self):
        yield CanonicalMarketEvent(
            event_kind="provider_subscription_ack",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            symbol=None,
            product_id=None,
            provider_sequence_num=1,
            provider_event_time=None,
            payload={},
            raw_ref={"channel": "subscriptions"},
        )
        yield CanonicalMarketEvent(
            event_kind="market_ticker",
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            symbol="BIP-20DEC30-CDE",
            product_id="BIP-20DEC30-CDE",
            provider_sequence_num=2,
            provider_event_time=None,
            payload={"price": "77000"},
            raw_ref={"channel": "ticker"},
        )
        while True:
            await asyncio.sleep(0.1)

    async def close(self) -> None:
        type(self).closed = True


@pytest.mark.asyncio
async def test_observe_only_runtime_records_market_intake_without_execution(monkeypatch):
    _FakeStream.subscriptions = []
    _FakeStream.closed = False
    monkeypatch.setattr(observe_only_runtime, "CoinbaseAdvancedTradeStream", _FakeStream)
    monkeypatch.setattr(
        observe_only_runtime.instrument_service,
        "get_instrument_record",
        lambda _instrument_id: {
            "symbol": "BIP-20DEC30-CDE",
            "metadata": {"provider_metadata": {"product_id": "BIP-20DEC30-CDE"}},
        },
    )
    lifecycle = []
    summaries = []

    def _record_lifecycle(bot_id, run_id, phase, owner, message, metadata, failure, status):
        lifecycle.append(
            {
                "bot_id": bot_id,
                "run_id": run_id,
                "phase": phase,
                "owner": owner,
                "message": message,
                "metadata": dict(metadata or {}),
                "failure": dict(failure or {}),
                "status": status,
            }
        )
        return lifecycle[-1]

    def _record_summary(payload):
        summaries.append(dict(payload))

    strategy = SimpleNamespace(
        datasource="COINBASE",
        exchange="COINBASE_DIRECT",
        timeframe="1h",
        instrument_links=[
            SimpleNamespace(
                symbol="BIP-20DEC30-CDE",
                instrument_id="instrument-1",
                instrument_snapshot={"symbol": "BIP-20DEC30-CDE"},
            )
        ],
    )

    exit_code = await observe_only_runtime.run_observe_only_market_intake(
        bot_id="bot-1",
        run_id="run-1",
        request_id="req-1",
        strategy=strategy,
        readiness={"datasource": "COINBASE", "exchange": "COINBASE_DIRECT", "timeframe": "1h"},
        record_lifecycle=_record_lifecycle,
        record_run_summary=_record_summary,
        duration_seconds=0.01,
    )

    assert exit_code == 0
    assert [entry["phase"] for entry in lifecycle] == ["runtime_subscribing", "live", "completed"]
    assert summaries[-1]["summary"]["orders_submitted"] == 0
    assert summaries[-1]["summary"]["fills_recorded"] == 0
    assert summaries[-1]["summary"]["wallet_mutations"] == 0
    assert summaries[-1]["summary"]["market_event_counts"]["market_ticker"] == 1
    assert _FakeStream.subscriptions[0].product_id == "BIP-20DEC30-CDE"
    assert _FakeStream.closed is True

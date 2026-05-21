from __future__ import annotations

import asyncio
import json

from data_providers.streams.coinbase import CoinbaseAdvancedTradeStream, CoinbaseMessageParser
from data_providers.streams.contracts import MarketSubscription


def test_market_subscription_defaults_product_id_to_symbol() -> None:
    subscription = MarketSubscription.from_values(
        provider="coinbase",
        venue="coinbase_direct",
        symbol="BIP-20DEC30-CDE",
    )

    assert subscription.to_dict() == {
        "provider": "COINBASE",
        "venue": "COINBASE_DIRECT",
        "symbol": "BIP-20DEC30-CDE",
        "product_id": "BIP-20DEC30-CDE",
        "channels": ["heartbeats", "ticker", "candles"],
        "timeframe": None,
        "auth_mode": "public",
    }


def test_coinbase_parser_maps_ticker_to_canonical_event() -> None:
    parser = CoinbaseMessageParser(symbol_by_product_id={"BIP-20DEC30-CDE": "BTC-PERP"})

    events = parser.parse(
        {
            "channel": "ticker",
            "timestamp": "2026-05-17T12:00:00Z",
            "sequence_num": 10,
            "events": [
                {
                    "type": "snapshot",
                    "tickers": [
                        {
                            "product_id": "BIP-20DEC30-CDE",
                            "price": "103000",
                            "best_bid": "102995",
                            "best_ask": "103005",
                            "volume_24_h": "123",
                        }
                    ],
                }
            ],
        }
    )

    assert len(events) == 1
    event = events[0].to_dict()
    assert event["event_kind"] == "market_ticker"
    assert event["provider"] == "COINBASE"
    assert event["venue"] == "COINBASE_DIRECT"
    assert event["symbol"] == "BTC-PERP"
    assert event["product_id"] == "BIP-20DEC30-CDE"
    assert event["provider_sequence_num"] == 10
    assert event["payload"]["price"] == "103000"
    assert event["payload"]["best_bid"] == "102995"
    assert event["payload"]["best_ask"] == "103005"


def test_coinbase_parser_maps_candles_and_heartbeat() -> None:
    parser = CoinbaseMessageParser()

    candle_events = parser.parse(
        {
            "channel": "candles",
            "timestamp": "2026-05-17T12:00:00Z",
            "sequence_num": 20,
            "events": [
                {
                    "type": "update",
                    "candles": [
                        {
                            "product_id": "BIP-20DEC30-CDE",
                            "start": "1779019200",
                            "open": "103000",
                            "high": "103100",
                            "low": "102900",
                            "close": "103050",
                            "volume": "10",
                        }
                    ],
                }
            ],
        }
    )
    heartbeat_events = parser.parse(
        {
            "channel": "heartbeats",
            "timestamp": "2026-05-17T12:00:01Z",
            "sequence_num": 21,
            "events": [{"current_time": "2026-05-17 12:00:01 UTC", "heartbeat_counter": "7"}],
        }
    )

    assert candle_events[0].event_kind == "market_candle_update"
    assert candle_events[0].product_id == "BIP-20DEC30-CDE"
    assert candle_events[0].payload["start"] == "1779019200"
    assert candle_events[0].payload["close"] == "103050"
    assert heartbeat_events[0].event_kind == "provider_heartbeat"
    assert heartbeat_events[0].payload["heartbeat_counter"] == "7"


def test_coinbase_parser_surfaces_subscription_ack_and_unknown_message() -> None:
    parser = CoinbaseMessageParser()

    ack_events = parser.parse(
        {
            "channel": "subscriptions",
            "timestamp": "2026-05-17T12:00:00Z",
            "sequence_num": 1,
            "events": [{"subscriptions": {"ticker": ["BIP-20DEC30-CDE"]}}],
        }
    )
    unknown_events = parser.parse({"channel": "new_channel", "sequence_num": 2, "events": []})

    assert ack_events[0].event_kind == "provider_subscription_ack"
    assert ack_events[0].payload["subscriptions"] == {"ticker": ["BIP-20DEC30-CDE"]}
    assert unknown_events[0].event_kind == "provider_unsupported_message"
    assert unknown_events[0].payload["channel"] == "new_channel"


def test_coinbase_parser_detects_sequence_gap_and_out_of_order() -> None:
    parser = CoinbaseMessageParser()

    first = parser.parse(
        {
            "channel": "ticker",
            "sequence_num": 10,
            "events": [{"tickers": [{"product_id": "BIP-20DEC30-CDE", "price": "1"}]}],
        }
    )
    gap = parser.parse(
        {
            "channel": "ticker",
            "sequence_num": 12,
            "events": [{"tickers": [{"product_id": "BIP-20DEC30-CDE", "price": "2"}]}],
        }
    )
    out_of_order = parser.parse(
        {
            "channel": "ticker",
            "sequence_num": 11,
            "events": [{"tickers": [{"product_id": "BIP-20DEC30-CDE", "price": "3"}]}],
        }
    )

    assert first[0].event_kind == "market_ticker"
    assert gap[0].event_kind == "provider_sequence_gap"
    assert gap[0].payload["missing_count"] == 1
    assert gap[0].payload["status"] == "gap"
    assert gap[1].event_kind == "market_ticker"
    assert out_of_order[0].event_kind == "provider_sequence_gap"
    assert out_of_order[0].payload["status"] == "out_of_order"
    assert out_of_order[1].event_kind == "market_ticker"


def test_coinbase_parser_handles_malformed_json() -> None:
    parser = CoinbaseMessageParser()

    events = parser.parse_raw("{not-json")

    assert events[0].event_kind == "provider_malformed_message"
    assert "error" in events[0].payload


def test_coinbase_stream_groups_and_dedupes_subscribe_frames() -> None:
    class _FakeWebSocket:
        def __init__(self) -> None:
            self.sent: list[str] = []

        async def send(self, message: str) -> None:
            self.sent.append(message)

    ws = _FakeWebSocket()
    stream = CoinbaseAdvancedTradeStream()
    stream._ws = ws

    asyncio.run(
        stream.subscribe(
            [
                MarketSubscription.from_values(
                    provider="COINBASE",
                    venue="COINBASE_DIRECT",
                    symbol="BIP-20DEC30-CDE",
                    channels=("heartbeats", "ticker", "candles"),
                ),
                MarketSubscription.from_values(
                    provider="COINBASE",
                    venue="COINBASE_DIRECT",
                    symbol="ETP-20DEC30-CDE",
                    channels=("heartbeats", "ticker", "candles"),
                ),
                MarketSubscription.from_values(
                    provider="COINBASE",
                    venue="COINBASE_DIRECT",
                    symbol="BIP-20DEC30-CDE",
                    channels=("ticker",),
                ),
            ]
        )
    )

    frames = [json.loads(message) for message in ws.sent]
    assert frames == [
        {"type": "subscribe", "channel": "heartbeats"},
        {
            "type": "subscribe",
            "channel": "ticker",
            "product_ids": ["BIP-20DEC30-CDE", "ETP-20DEC30-CDE"],
        },
        {
            "type": "subscribe",
            "channel": "candles",
            "product_ids": ["BIP-20DEC30-CDE", "ETP-20DEC30-CDE"],
        },
    ]

from __future__ import annotations

import asyncio
import hashlib
import json

from data_providers.streams import coinbase as coinbase_stream_module
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
    assert event["provider_event_time"] is None
    assert event["provider_message_time"] == "2026-05-17T12:00:00Z"
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


def test_coinbase_parser_distinguishes_duplicate_sequence_delivery() -> None:
    parser = CoinbaseMessageParser()
    message = {
        "channel": "ticker",
        "sequence_num": 10,
        "events": [{"tickers": [{"product_id": "BIP-20DEC30-CDE", "price": "1"}]}],
    }

    parser.parse(message)
    duplicate = parser.parse(message)

    assert duplicate[0].event_kind == "provider_sequence_gap"
    assert duplicate[0].payload == {
        "sequence_key": "connection",
        "previous_sequence_num": 10,
        "current_sequence_num": 10,
        "missing_count": 0,
        "status": "duplicate",
    }
    assert duplicate[1].event_kind == "market_ticker"


def test_coinbase_parser_handles_malformed_json() -> None:
    parser = CoinbaseMessageParser()

    events = parser.parse_raw(
        "{not-json",
        received_at="2026-08-02T12:00:00.300Z",
        raw_ref={"receive_ordinal": 9, "raw_frame_sha256": "abc"},
    )

    assert events[0].event_kind == "provider_malformed_message"
    assert "error" in events[0].payload
    assert events[0].received_at == "2026-08-02T12:00:00.300Z"
    assert events[0].raw_ref["receive_ordinal"] == 9

    undecodable = parser.parse_raw(b"\xff")
    assert undecodable[0].event_kind == "provider_malformed_message"


def test_coinbase_parser_preserves_market_trade_maker_side_and_batch_position() -> None:
    parser = CoinbaseMessageParser(symbol_by_product_id={"BIP-20DEC30-CDE": "BIP"})

    events = parser.parse(
        {
            "channel": "market_trades",
            "timestamp": "2026-08-02T12:00:00.250Z",
            "sequence_num": 44,
            "events": [
                {
                    "type": "update",
                    "trades": [
                        {
                            "trade_id": "trade-1",
                            "product_id": "BIP-20DEC30-CDE",
                            "price": "115000",
                            "size": "3",
                            "side": "BUY",
                            "time": "2026-08-02T12:00:00.100Z",
                        },
                        {
                            "trade_id": "trade-2",
                            "product_id": "BIP-20DEC30-CDE",
                            "price": "115005",
                            "size": "1",
                            "side": "SELL",
                            "time": "2026-08-02T12:00:00.200Z",
                        },
                    ],
                }
            ],
        },
        received_at="2026-08-02T12:00:00.300Z",
        raw_ref={"receive_ordinal": 9, "raw_frame_sha256": "abc"},
    )

    assert [event.event_kind for event in events] == ["market_trade", "market_trade"]
    assert [event.payload["side"] for event in events] == ["BUY", "SELL"]
    assert [event.payload["trade_ordinal"] for event in events] == [0, 1]
    assert events[0].provider_event_time == "2026-08-02T12:00:00.100Z"
    assert events[0].provider_message_time == "2026-08-02T12:00:00.250Z"
    assert events[0].received_at == "2026-08-02T12:00:00.300Z"
    assert events[0].raw_ref["receive_ordinal"] == 9


def test_coinbase_parser_preserves_level2_event_atomicity_and_absolute_quantities() -> None:
    parser = CoinbaseMessageParser()

    events = parser.parse(
        {
            "channel": "l2_data",
            "timestamp": "2026-08-02T12:00:00.250Z",
            "sequence_num": 91,
            "events": [
                {
                    "type": "snapshot",
                    "product_id": "BTC-USD",
                    "updates": [
                        {
                            "side": "bid",
                            "event_time": "2026-08-02T12:00:00.100Z",
                            "price_level": "114999.99",
                            "new_quantity": "0.5",
                        },
                        {
                            "side": "offer",
                            "event_time": "2026-08-02T12:00:00.100Z",
                            "price_level": "115000.01",
                            "new_quantity": "0",
                        },
                    ],
                }
            ],
        }
    )

    assert len(events) == 1
    assert events[0].event_kind == "market_l2_snapshot"
    assert events[0].product_id == "BTC-USD"
    assert events[0].provider_event_time is None
    assert events[0].provider_message_time == "2026-08-02T12:00:00.250Z"
    assert events[0].payload["updates"] == [
        {
            "mutation_ordinal": 0,
            "side": "bid",
            "event_time": "2026-08-02T12:00:00.100Z",
            "price_level": "114999.99",
            "new_quantity": "0.5",
        },
        {
            "mutation_ordinal": 1,
            "side": "offer",
            "event_time": "2026-08-02T12:00:00.100Z",
            "price_level": "115000.01",
            "new_quantity": "0",
        },
    ]


def test_coinbase_parser_does_not_reinterpret_unknown_level2_event_type() -> None:
    parser = CoinbaseMessageParser()

    events = parser.parse(
        {
            "channel": "level2",
            "timestamp": "2026-08-02T12:00:00.250Z",
            "sequence_num": 91,
            "events": [
                {
                    "type": "replacement",
                    "product_id": "BTC-USD",
                    "updates": [
                        {
                            "side": "bid",
                            "event_time": "2026-08-02T12:00:00.100Z",
                            "price_level": "114999.99",
                            "new_quantity": "0.5",
                        }
                    ],
                }
            ],
        }
    )

    assert len(events) == 1
    assert events[0].event_kind == "provider_unsupported_message"
    assert events[0].payload["channel"] == "level2"
    assert events[0].payload["event_type"] == "replacement"


def test_coinbase_sequence_is_connection_wide_across_interleaved_channels() -> None:
    parser = CoinbaseMessageParser()

    subscription = parser.parse(
        {"channel": "subscriptions", "sequence_num": 0, "events": []}
    )
    ticker = parser.parse(
        {
            "channel": "ticker",
            "sequence_num": 1,
            "events": [{"tickers": [{"product_id": "BTC-USD", "price": "1"}]}],
        }
    )
    heartbeat = parser.parse(
        {
            "channel": "heartbeats",
            "sequence_num": 2,
            "events": [{"heartbeat_counter": "1"}],
        }
    )

    assert [event.event_kind for event in subscription] == [
        "provider_subscription_ack"
    ]
    assert [event.event_kind for event in ticker] == ["market_ticker"]
    assert [event.event_kind for event in heartbeat] == ["provider_heartbeat"]


def test_coinbase_connect_uses_bounded_message_size_and_resets_sequence(
    monkeypatch,
) -> None:
    observed: dict[str, object] = {}

    class _FakeWebSockets:
        @staticmethod
        async def connect(url: str, **kwargs):
            observed.update({"url": url, **kwargs})
            return object()

    stream = CoinbaseAdvancedTradeStream()
    stream._parser.parse(
        {"channel": "subscriptions", "sequence_num": 99, "events": []}
    )
    monkeypatch.setattr(coinbase_stream_module, "websockets", _FakeWebSockets)

    asyncio.run(stream.connect())

    assert observed["max_size"] == 16 * 1024 * 1024
    assert observed["logger"] is coinbase_stream_module.COINBASE_WS_WIRE_LOGGER
    assert not observed["logger"].isEnabledFor(10)
    assert stream._parser._last_sequence_by_key == {}


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


def test_coinbase_stream_adds_fresh_jwt_to_each_authenticated_subscription() -> None:
    class _FakeWebSocket:
        def __init__(self) -> None:
            self.sent: list[str] = []

        async def send(self, message: str) -> None:
            self.sent.append(message)

    tokens = iter(["jwt-heartbeat", "jwt-trades"])
    ws = _FakeWebSocket()
    stream = CoinbaseAdvancedTradeStream(jwt_factory=lambda: next(tokens))
    stream._ws = ws

    asyncio.run(
        stream.subscribe(
            [
                MarketSubscription.from_values(
                    provider="COINBASE",
                    venue="COINBASE_DIRECT",
                    symbol="BIP",
                    product_id="BIP-20DEC30-CDE",
                    channels=("heartbeats", "market_trades"),
                    auth_mode="authenticated",
                )
            ]
        )
    )

    frames = [json.loads(message) for message in ws.sent]
    assert frames == [
        {"type": "subscribe", "channel": "heartbeats", "jwt": "jwt-heartbeat"},
        {
            "type": "subscribe",
            "channel": "market_trades",
            "product_ids": ["BIP-20DEC30-CDE"],
            "jwt": "jwt-trades",
        },
    ]


def test_coinbase_stream_exposes_exact_raw_frame_evidence_before_parsing() -> None:
    raw_frame = '{"channel":"heartbeats","sequence_num":0,"events":[]}'

    class _FakeWebSocket:
        def __init__(self) -> None:
            self._frames = iter([raw_frame])

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self._frames)
            except StopIteration as exc:
                raise StopAsyncIteration from exc

    async def _collect():
        stream = CoinbaseAdvancedTradeStream()
        stream._ws = _FakeWebSocket()
        stream._connection_epoch = 3
        return [message async for message in stream.raw_messages()]

    messages = asyncio.run(_collect())

    assert len(messages) == 1
    assert messages[0].connection_epoch == 3
    assert messages[0].receive_ordinal == 1
    assert messages[0].raw_frame == raw_frame.encode("utf-8")
    assert messages[0].raw_frame_sha256 == hashlib.sha256(raw_frame.encode("utf-8")).hexdigest()

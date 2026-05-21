from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from data_providers.streams.contracts import CanonicalMarketEvent
from engines.bot_runtime.live_market import (
    ClosedLiveCandle,
    LiveCandleAggregator,
    LiveCandleStore,
    append_closed_live_candles_to_series,
)


def _candle_event(
    *,
    start: int,
    symbol: str = "BTC-PERP",
    close: str = "101",
    payload_type: str | None = None,
) -> CanonicalMarketEvent:
    payload = {
        "start": str(start),
        "open": "100",
        "high": "105",
        "low": "95",
        "close": close,
        "volume": "2",
    }
    if payload_type is not None:
        payload["type"] = payload_type
    return CanonicalMarketEvent.build(
        event_kind="market_candle_update",
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        symbol=symbol,
        product_id="BIP-20DEC30-CDE",
        provider_event_time=datetime.fromtimestamp(start + 1, tz=timezone.utc).isoformat(),
        received_at=datetime.fromtimestamp(start + 2, tz=timezone.utc).isoformat(),
        payload=payload,
    )


def test_live_candle_aggregator_closes_target_timeframe_when_next_bucket_arrives() -> None:
    aggregator = LiveCandleAggregator(target_timeframe="15m", source_timeframe_seconds=300)
    base = 1_779_019_200

    assert aggregator.process(_candle_event(start=base, close="101")) == []
    assert aggregator.process(_candle_event(start=base + 300, close="102")) == []
    assert aggregator.process(_candle_event(start=base + 600, close="103")) == []
    closed = aggregator.process(_candle_event(start=base + 900, close="104"))

    assert len(closed) == 1
    candle = closed[0]
    assert candle.symbol == "BTC-PERP"
    assert candle.timeframe == "15m"
    assert int(candle.time.timestamp()) == base
    assert int(candle.end.timestamp()) == base + 900
    assert candle.open == 100.0
    assert candle.high == 105.0
    assert candle.low == 95.0
    assert candle.close == 103.0
    assert candle.volume == 6.0


def test_live_candle_aggregator_ignores_provider_snapshot_history() -> None:
    aggregator = LiveCandleAggregator(target_timeframe="15m", source_timeframe_seconds=300)
    base = 1_779_019_200

    assert aggregator.process(_candle_event(start=base, payload_type="snapshot")) == []

    snapshot = aggregator.snapshot()
    assert snapshot["ignored_snapshot_count"] == 1
    assert snapshot["open_source_count"] == 0
    assert snapshot["open_target_count"] == 0


def test_live_candle_aggregator_drops_incomplete_target_when_source_buckets_gap() -> None:
    aggregator = LiveCandleAggregator(target_timeframe="15m", source_timeframe_seconds=300)
    base = 1_779_019_200

    assert aggregator.process(_candle_event(start=base, close="101")) == []
    assert aggregator.process(_candle_event(start=base + 900, close="104")) == []
    closed = aggregator.process(_candle_event(start=base + 1_200, close="105"))

    assert closed == []
    snapshot = aggregator.snapshot()
    assert snapshot["required_source_bucket_count"] == 3
    assert snapshot["dropped_incomplete_target_count"] == 1


def test_live_candle_store_returns_immutable_closed_candles_after_last_seen() -> None:
    store = LiveCandleStore()
    first = ClosedLiveCandle(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        symbol="BTC-PERP",
        product_id="BIP-20DEC30-CDE",
        timeframe="1h",
        time=datetime(2026, 5, 17, 12, tzinfo=timezone.utc),
        end=datetime(2026, 5, 17, 13, tzinfo=timezone.utc),
        open=100.0,
        high=110.0,
        low=90.0,
        close=105.0,
        volume=12.0,
    )
    duplicate = ClosedLiveCandle(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        symbol="BTC-PERP",
        product_id="BIP-20DEC30-CDE",
        timeframe="1h",
        time=datetime(2026, 5, 17, 12, tzinfo=timezone.utc),
        end=datetime(2026, 5, 17, 13, tzinfo=timezone.utc),
        open=100.0,
        high=111.0,
        low=90.0,
        close=106.0,
        volume=12.0,
    )

    assert store.append(first) is True
    assert store.append(duplicate) is False

    series = SimpleNamespace(symbol="btc-perp", timeframe="1h")
    candles = append_closed_live_candles_to_series(
        store=store,
        series=series,
        after=datetime(2026, 5, 17, 11, tzinfo=timezone.utc),
    )

    assert len(candles) == 1
    assert candles[0].close == 105.0
    assert store.snapshot()["duplicate_count"] == 1
    assert store.snapshot()["conflicting_duplicate_count"] == 1


def test_live_candle_store_failure_is_loud() -> None:
    store = LiveCandleStore()
    store.mark_failed("provider stream ended")

    with pytest.raises(RuntimeError, match="provider stream ended"):
        store.raise_if_failed()

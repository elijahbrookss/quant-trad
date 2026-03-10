import datetime as dt

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("coinbase")

from data_providers.providers import coinbase as coinbase_module
from data_providers.providers.base import InstrumentType


class FakeClient:
    def __init__(self, *, product=None, candle_provider=None, transaction_summary=None):
        self.product = product
        self.candle_provider = candle_provider
        self.transaction_summary = transaction_summary
        self.calls = []

    def get_product(self, product_id, **kwargs):
        self.calls.append({"url_path": "/products", "product_id": product_id})
        if self.product is None:
            raise Exception("product_not_found")
        return dict(self.product)

    def get_candles(self, product_id, start, end, granularity, limit=None, **kwargs):
        self.calls.append(
            {
                "url_path": "/candles",
                "product_id": product_id,
                "start": start,
                "end": end,
                "granularity": granularity,
                "limit": limit,
            }
        )
        if self.candle_provider is None:
            return {"candles": []}
        return {"candles": self.candle_provider({"start": start, "end": end, "granularity": granularity})}

    def get_transaction_summary(self, **kwargs):
        self.calls.append({"url_path": "/transaction_summary", "params": kwargs})
        if self.transaction_summary is None:
            raise Exception("transaction_summary_not_found")
        return dict(self.transaction_summary)


def _make_provider(fake_client):
    provider = coinbase_module.CoinbaseProvider()
    provider._client = fake_client
    return provider


def test_interval_mapping_supports_four_hour():
    provider = coinbase_module.CoinbaseProvider()
    assert provider._interval_to_granularity("4h") == coinbase_module.Granularity.FOUR_HOUR


def test_validate_symbol_success_with_matching_venue():
    product = {
        "product_id": "BTC-USD",
        "product_type": "SPOT",
        "price_increment": "0.01",
        "base_currency_id": "BTC",
        "quote_currency_id": "USD",
        "product_venue": "CBE",
    }
    provider = _make_provider(FakeClient(product=product))

    provider.validate_symbol("CBE", "BTC-USD")


def test_validate_symbol_raises_on_missing_product():
    provider = _make_provider(FakeClient(product=None))

    with pytest.raises(ValueError):
        provider.validate_symbol("", "BTC-USD")


def test_validate_symbol_ignores_venue():
    product = {
        "product_id": "BTC-USD",
        "product_type": "SPOT",
        "price_increment": "0.01",
        "base_currency_id": "BTC",
        "quote_currency_id": "USD",
        "product_venue": "FCM",
    }
    provider = _make_provider(FakeClient(product=product))

    provider.validate_symbol("coinbase_direct", "BTC-USD")
    provider.validate_symbol("FCM", "BTC-USD")


def test_get_instrument_type_maps_spot_and_future():
    spot = _make_provider(
        FakeClient(
            product={
                "product_id": "BTC-USD",
                "product_type": "SPOT",
                "price_increment": "0.01",
                "base_currency_id": "BTC",
                "quote_currency_id": "USD",
            }
        )
    )
    future = _make_provider(
        FakeClient(
            product={
                "product_id": "BTC-PERP",
                "product_type": "FUTURE",
                "price_increment": "0.1",
                "base_currency_id": "BTC",
                "quote_currency_id": "USD",
                "future_product_details": {"contract_size": "2"},
            }
        )
    )

    assert spot.get_instrument_type("", "BTC-USD") == InstrumentType.SPOT
    assert future.get_instrument_type("", "BTC-PERP") == InstrumentType.FUTURE


def test_validate_instrument_type_raises_on_unknown():
    provider = _make_provider(
        FakeClient(
            product={
                "product_id": "BTC-USD",
                "product_type": "UNKNOWN_PRODUCT_TYPE",
                "price_increment": "0.01",
                "base_currency_id": "BTC",
                "quote_currency_id": "USD",
            }
        )
    )

    with pytest.raises(ValueError):
        provider.validate_instrument_type("", "BTC-USD")


def test_get_instrument_metadata_spot_and_future():
    transaction_summary = {
        "fee_tier": {"maker_fee_rate": "0.0001", "taker_fee_rate": "0.0002"}
    }
    spot_product = {
        "product_id": "BTC-USD",
        "product_type": "SPOT",
        "price_increment": "0.01",
        "base_currency_id": "BTC",
        "quote_currency_id": "USD",
        "base_min_size": "0.001",
    }
    future_product = {
        "product_id": "BTC-PERP",
        "product_type": "FUTURE",
        "price_increment": "0.5",
        "base_currency_id": "BTC",
        "quote_currency_id": "USD",
        "base_min_size": "0.01",
        "future_product_details": {
            "contract_size": "2",
            "contract_expiry": "2025-01-01T00:00:00Z",
            "intraday_margin_rate": {
                "long_margin_rate": "0.1000185",
                "short_margin_rate": "0.1000008",
            },
            "overnight_margin_rate": {
                "long_margin_rate": "0.245625",
                "short_margin_rate": "0.306375",
            },
            "funding_rate": "0.01",
        },
    }

    spot = _make_provider(
        FakeClient(product=spot_product, transaction_summary=transaction_summary)
    )
    future = _make_provider(
        FakeClient(product=future_product, transaction_summary=transaction_summary)
    )

    spot_meta = spot.get_instrument_metadata("", "BTC-USD")
    assert spot_meta.tick_size == 0.01
    assert spot_meta.base_currency == "BTC"
    assert spot_meta.quote_currency == "USD"
    assert spot_meta.can_short is False
    assert spot_meta.min_order_size == 0.001
    assert spot_meta.maker_fee_rate == 0.0001
    assert spot_meta.taker_fee_rate == 0.0002

    future_meta = future.get_instrument_metadata("", "BTC-PERP")
    assert future_meta.tick_size == 0.5
    assert future_meta.contract_size == 2.0
    assert future_meta.tick_value == 1.0
    assert future_meta.has_funding is True
    assert future_meta.can_short is True
    assert future_meta.expiry_ts is not None
    assert future_meta.min_order_size == 0.01
    assert future_meta.maker_fee_rate == 0.0001
    assert future_meta.taker_fee_rate == 0.0002
    assert future_meta.margin_rates is not None
    assert future_meta.margin_rates.get("intraday", {}).get("long_margin_rate") == "0.1000185"
    future_meta_payload = future_meta.metadata or {}
    future_details = future_meta_payload.get("future_product_details") or {}
    margin_rates = future_details.get("margin_rates") or {}
    assert margin_rates.get("intraday", {}).get("long_margin_rate") == "0.1000185"


def test_fetch_from_api_normalizes_candles_sorted():
    start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    end = dt.datetime(2024, 1, 1, 0, 2, tzinfo=dt.timezone.utc)
    base_start = int(start.timestamp())

    def candle_provider(_params):
        return [
            {
                "start": str(base_start + 60),
                "open": "2",
                "high": "2",
                "low": "2",
                "close": "2",
                "volume": "1",
            },
            {
                "start": str(base_start),
                "open": "1",
                "high": "1",
                "low": "1",
                "close": "1",
                "volume": "1",
            },
        ]

    provider = _make_provider(FakeClient(product={"product_id": "BTC-USD"}, candle_provider=candle_provider))

    frame = provider.fetch_from_api("BTC-USD", start, end, "1m")

    assert list(frame.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert frame["timestamp"].is_monotonic_increasing
    assert len(frame) == 2


def test_fetch_from_api_chunks_requests(monkeypatch):
    def candle_provider(params):
        return [
            {
                "start": str(params["start"]),
                "open": "1",
                "high": "1",
                "low": "1",
                "close": "1",
                "volume": "1",
            }
        ]

    fake_client = FakeClient(product={"product_id": "BTC-USD"}, candle_provider=candle_provider)
    provider = _make_provider(fake_client)
    monkeypatch.setattr(provider, "MAX_CANDLES_PER_REQUEST", 2)

    start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(minutes=5)

    frame = provider.fetch_from_api("BTC-USD", start, end, "1m")

    candle_calls = [call for call in fake_client.calls if call["url_path"] == "/candles"]
    assert len(candle_calls) > 1
    assert len(frame) == len(candle_calls)

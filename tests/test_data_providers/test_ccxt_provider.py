import importlib
import types
from datetime import datetime, timedelta, timezone

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("ccxt")

ccxt_module = importlib.import_module("data_providers.providers.ccxt")


def test_ccxt_provider_continues_pagination_when_exchange_caps(monkeypatch):
    """Providers should keep paging until the requested end timestamp is covered."""

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=5)

    candles = []
    for index in range(10):
        ts = int((start + timedelta(minutes=index)).timestamp() * 1000)
        candles.append([ts, 100 + index, 101 + index, 99 + index, 100.5 + index, 1000 + index])

    class LimitedBatchExchange:
        def __init__(self, _config):
            self.fetch_calls = []

        def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None):
            self.fetch_calls.append({"symbol": symbol, "timeframe": timeframe, "since": since, "limit": limit})

            target_since = since if since is not None else candles[0][0]
            start_idx = 0
            while start_idx < len(candles) and candles[start_idx][0] < target_since:
                start_idx += 1

            end_idx = min(start_idx + 2, len(candles))
            return candles[start_idx:end_idx]

    monkeypatch.setattr(ccxt_module, "ccxt", types.SimpleNamespace(binanceus=LimitedBatchExchange))

    provider = ccxt_module.CCXTProvider("binanceus")
    frame = provider.fetch_from_api("BTC/USDT", start, end, "1m")

    exchange = provider._exchange
    assert len(exchange.fetch_calls) >= 3  # Limited batches required several pages.
    assert len(frame) == 6
    assert frame["timestamp"].iloc[0] == pd.Timestamp(start)
    assert frame["timestamp"].iloc[-1] == pd.Timestamp(end)
    assert frame["timestamp"].is_monotonic_increasing

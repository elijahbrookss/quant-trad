import datetime as dt

import pytest

pd = pytest.importorskip('pandas')

import ccxt

from data_providers.ccxt_provider import CCXTProvider


class DummyExchange:
    """Test double that emulates paginated CCXT OHLCV responses."""

    instances = []
    max_per_call = 600
    step_ms = 15 * 60 * 1000
    until_ms = None

    def __init__(self, _params=None):
        self.calls = []
        DummyExchange.instances.append(self)

    def fetch_ohlcv(self, symbol, timeframe, since, limit):
        self.calls.append((symbol, timeframe, since, limit))

        candles = []
        cursor = since
        # Respect the configured per-call cap so the provider must paginate.
        cap = min(limit, self.max_per_call)
        while len(candles) < cap and cursor <= self.until_ms:
            candles.append([cursor, 1, 2, 0.5, 1.5, 42])
            cursor += self.step_ms

        return candles


@pytest.fixture(autouse=True)
def patch_binanceus(monkeypatch):
    DummyExchange.instances.clear()
    monkeypatch.setattr(ccxt, 'binanceus', DummyExchange)
    yield


def test_fetch_from_api_handles_multi_batch_range():
    provider = CCXTProvider('binanceus')

    start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=30)
    DummyExchange.until_ms = int(end.timestamp() * 1000)

    frame = provider.fetch_from_api('LINK/USDT', start, end, '15m')

    # More than one exchange call should be required due to the per-call cap.
    exchange = DummyExchange.instances[0]
    assert len(exchange.calls) > 1

    assert isinstance(frame, pd.DataFrame)
    assert not frame.empty
    # Ensure the dataframe spans the requested interval and is deduplicated.
    assert frame['timestamp'].is_unique
    assert frame['timestamp'].min() >= pd.Timestamp(start)
    assert frame['timestamp'].max() <= pd.Timestamp(end)

    # Roughly verify the expected row count for a 30 day window of 15 minute candles.
    expected = int((end - start).total_seconds() // (15 * 60)) + 1
    assert abs(len(frame) - expected) <= 1

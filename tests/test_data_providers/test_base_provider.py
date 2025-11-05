import datetime as dt

import pytest

pd = pytest.importorskip("pandas")

from data_providers.base_provider import BaseDataProvider
from indicators.config import DataContext


class DummyProvider(BaseDataProvider):
    def __init__(self):
        self._fetch_impl = None

    def get_datasource(self) -> str:
        return "TEST"

    def fetch_from_api(self, symbol: str, start: dt.datetime, end: dt.datetime, interval: str):
        if self._fetch_impl is None:
            raise RuntimeError("fetch_from_api was not configured")
        return self._fetch_impl(symbol, start, end, interval)


@pytest.fixture
def provider(monkeypatch):
    instance = DummyProvider()
    instance._engine = object()
    instance._table = "ohlcv_test"
    monkeypatch.setattr(DummyProvider, "_write_dataframe", lambda self, df, ctx: len(df))
    return instance


def _build_frame(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    index = pd.date_range(start=start, end=end, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": index,
            "open": range(len(index)),
            "high": range(len(index)),
            "low": range(len(index)),
            "close": range(len(index)),
            "volume": [1.0] * len(index),
        }
    )


def test_get_ohlcv_fetches_missing_segments(provider, monkeypatch):
    cached = _build_frame(
        pd.Timestamp("2024-01-05T00:00:00Z"),
        pd.Timestamp("2024-01-07T00:00:00Z"),
    )

    fetch_ranges = []

    def fake_fetch(symbol, start, end, interval):
        fetch_ranges.append((pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True)))
        return _build_frame(pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True))

    provider._fetch_impl = fake_fetch

    monkeypatch.setattr(
        pd,
        "read_sql",
        lambda query, engine, params=None: cached.copy(),
    )

    ctx = DataContext(
        symbol="LINK/USDT",
        start="2024-01-01T00:00:00Z",
        end="2024-01-10T00:00:00Z",
        interval="1d",
    )

    frame = provider.get_ohlcv(ctx)

    assert len(fetch_ranges) == 2
    starts = {rng[0] for rng in fetch_ranges}
    ends = {rng[1] for rng in fetch_ranges}
    assert pd.Timestamp("2024-01-01T00:00:00Z") in starts
    assert pd.Timestamp("2024-01-10T00:00:00Z") in ends
    timestamps = frame.index
    assert timestamps.min() <= pd.Timestamp("2024-01-01T00:00:00Z")
    assert timestamps.max() >= pd.Timestamp("2024-01-10T00:00:00Z")


def test_get_ohlcv_skips_fetch_when_range_complete(provider, monkeypatch):
    cached = _build_frame(
        pd.Timestamp("2024-01-01T00:00:00Z"),
        pd.Timestamp("2024-01-10T00:00:00Z"),
    )

    monkeypatch.setattr(
        pd,
        "read_sql",
        lambda query, engine, params=None: cached.copy(),
    )

    fetch_ranges = []

    def fake_fetch(symbol, start, end, interval):
        fetch_ranges.append((start, end))
        return _build_frame(pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True))

    provider._fetch_impl = fake_fetch

    ctx = DataContext(
        symbol="LINK/USDT",
        start="2024-01-01T00:00:00Z",
        end="2024-01-10T00:00:00Z",
        interval="1d",
    )

    frame = provider.get_ohlcv(ctx)

    assert not fetch_ranges
    timestamps = frame.index
    assert timestamps.min() <= pd.Timestamp("2024-01-01T00:00:00Z")
    assert timestamps.max() >= pd.Timestamp("2024-01-10T00:00:00Z")

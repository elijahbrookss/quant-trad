"""Tests for BaseDataProvider helpers."""

from typing import Any, Mapping


import pytest

pd = pytest.importorskip("pandas")

from data_providers.providers.base import BaseDataProvider, InstrumentMetadata, InstrumentType
from data_providers import utils
from indicators.config import DataContext


class _FakePersistence:
    engine_available = True

    def __init__(self, frame, *, closure_evidence=None):
        self.frame = frame
        self.closure_evidence = list(closure_evidence or [])
        self.recorded_closures = []

    def ensure_schema(self) -> None:
        return None

    def fetch_ohlcv(self, ctx, datasource):
        return self.frame.copy()

    def load_closure_ranges(self, ctx, datasource, requested_start, requested_end):
        return [
            (entry["start"], entry["end"])
            for entry in self.closure_evidence
        ]

    def load_closure_evidence_ranges(self, ctx, datasource, requested_start, requested_end):
        return list(self.closure_evidence)

    def record_closure_range(self, ctx, datasource, start, end, metadata: Mapping[str, Any] | None = None):
        self.recorded_closures.append(
            {
                "datasource": datasource,
                "start": start,
                "end": end,
                "metadata": dict(metadata or {}),
            }
        )

    def write_dataframe(self, df, ctx):
        return len(df)


class _FakeProvider(BaseDataProvider):
    def __init__(self, *, persistence, response=None, error: Exception | None = None):
        super().__init__(persistence=persistence)
        self.response = response
        self.error = error

    def get_datasource(self) -> str:
        return "FAKE_PROVIDER"

    def fetch_from_api(self, symbol, start, end, interval):
        if self.error is not None:
            raise self.error
        return self.response.copy() if self.response is not None else pd.DataFrame()

    def get_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        return InstrumentType.SPOT

    def validate_instrument_type(self, venue: str, symbol: str) -> InstrumentType:
        return InstrumentType.SPOT

    def get_instrument_metadata(self, venue: str, symbol: str) -> InstrumentMetadata:
        return InstrumentMetadata(
            tick_size=0.01,
            contract_size=1.0,
            tick_value=0.01,
            min_order_size=None,
            qty_step=None,
            max_qty=None,
            min_notional=None,
            maker_fee_rate=None,
            taker_fee_rate=None,
            margin_rates=None,
            can_short=False,
            short_requires_borrow=False,
            has_funding=False,
            expiry_ts=None,
            base_currency="AAA",
            quote_currency="USD",
            metadata=None,
        )

    def validate_symbol(self, venue: str, symbol: str) -> None:
        return None


def _ts_range(start: str, count: int, step: str) -> list[pd.Timestamp]:
    base = pd.Timestamp(start, tz="UTC")
    delta = pd.to_timedelta(step)
    return [base + i * delta for i in range(count)]


def _cached_frame(times: list[str]):
    return pd.DataFrame(
        {
            "timestamp": [pd.Timestamp(value, tz="UTC") for value in times],
            "open": [1.0 for _ in times],
            "high": [2.0 for _ in times],
            "low": [0.5 for _ in times],
            "close": [1.5 for _ in times],
            "volume": [10.0 for _ in times],
        }
    )


def _ctx() -> DataContext:
    return DataContext(
        symbol="AAA-USD",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T03:00:00Z",
        interval="1h",
        instrument_id="instrument-1",
    )


def test_collect_missing_ranges_handles_exclusive_end_without_gap():
    """No supplemental fetch is needed when cached candles cover the window."""

    start = pd.Timestamp("2024-01-01T00:00:00Z")
    end = pd.Timestamp("2024-01-01T05:00:00Z")
    timestamps = _ts_range("2024-01-01T00:00:00Z", 5, "1h")

    missing = utils.collect_missing_ranges(timestamps, start, end, "1h")

    assert missing == []


def test_collect_missing_ranges_reports_trailing_gap_only_when_missing():
    """Trailing gaps start at the next expected candle rather than the last seen."""

    start = pd.Timestamp("2024-01-01T00:00:00Z")
    end = pd.Timestamp("2024-01-01T05:00:00Z")
    timestamps = _ts_range("2024-01-01T00:00:00Z", 3, "1h")

    missing = utils.collect_missing_ranges(timestamps, start, end, "1h")

    assert missing == [(pd.Timestamp("2024-01-01T03:00:00Z"), end)]


def test_subtract_ranges_removes_known_closures():
    """Closures are carved out of missing windows."""

    start = pd.Timestamp("2024-01-01T00:00:00Z")
    ranges = [(start, start + pd.Timedelta(hours=6))]
    closures = [
        (start + pd.Timedelta(hours=1), start + pd.Timedelta(hours=2)),
        (start + pd.Timedelta(hours=4), start + pd.Timedelta(hours=5)),
    ]

    remaining = utils.subtract_ranges(ranges, closures)

    assert remaining == [
        (start, start + pd.Timedelta(hours=1)),
        (start + pd.Timedelta(hours=2), start + pd.Timedelta(hours=4)),
        (start + pd.Timedelta(hours=5), start + pd.Timedelta(hours=6)),
    ]


def test_subtract_ranges_drops_fully_covered_segments():
    """Missing ranges vanish once fully covered by closures."""

    start = pd.Timestamp("2024-01-01T00:00:00Z")
    ranges = [(start, start + pd.Timedelta(hours=3))]
    closures = [(start - pd.Timedelta(minutes=30), start + pd.Timedelta(hours=3))]

    remaining = utils.subtract_ranges(ranges, closures)

    assert remaining == []


def test_get_ohlcv_records_empty_provider_response_evidence():
    cached = _cached_frame(["2024-01-01T00:00:00Z", "2024-01-01T02:00:00Z"])
    empty_response = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    empty_response.attrs["provider_message"] = "exchange returned no candle for interval"
    persistence = _FakePersistence(cached)
    provider = _FakeProvider(persistence=persistence, response=empty_response)

    result = provider.get_ohlcv(_ctx())

    classification = result.attrs["gap_classification"][0]
    assert classification["classification"] == "provider_missing_data"
    assert classification["reason_code"] == "provider_response_empty"
    assert classification["evidence"] == "provider_api_empty_response"
    assert classification["provider_evidence"]["provider_response"]["provider_message"] == "exchange returned no candle for interval"
    assert persistence.recorded_closures[0]["metadata"]["reason_code"] == "provider_response_empty"


def test_get_ohlcv_records_provider_exception_stack_trace_for_missing_range():
    cached = _cached_frame(["2024-01-01T00:00:00Z", "2024-01-01T02:00:00Z"])
    persistence = _FakePersistence(cached)
    provider = _FakeProvider(persistence=persistence, error=RuntimeError("rate limit exceeded"))

    result = provider.get_ohlcv(_ctx())

    classification = result.attrs["gap_classification"][0]
    assert classification["classification"] == "ingestion_failure"
    assert classification["reason_code"] == "provider_fetch_exception"
    assert classification["provider_evidence"]["exception_type"] == "RuntimeError"
    assert "rate limit exceeded" in classification["provider_evidence"]["exception_message"]
    assert "RuntimeError: rate limit exceeded" in classification["provider_evidence"]["stack_trace"]


def test_loaded_closure_evidence_is_carried_into_gap_classification():
    cached = _cached_frame(["2024-01-01T00:00:00Z", "2024-01-01T02:00:00Z"])
    persistence = _FakePersistence(
        cached,
        closure_evidence=[
            {
                "start": pd.Timestamp("2024-01-01T01:00:00Z"),
                "end": pd.Timestamp("2024-01-01T02:00:00Z"),
                "metadata": {
                    "reason_code": "provider_response_empty",
                    "evidence": "provider_api_empty_response",
                    "provider_response": {"provider_message": "known closure"},
                },
            }
        ],
    )
    provider = _FakeProvider(persistence=persistence)

    result = provider.get_ohlcv(_ctx())

    classification = result.attrs["gap_classification"][0]
    assert classification["classification"] == "provider_missing_data"
    assert classification["reason_code"] == "provider_response_empty"
    assert classification["provider_evidence"]["provider_response"]["provider_message"] == "known closure"

"""Tests for BaseDataProvider helpers."""

from __future__ import annotations

import pandas as pd

from data_providers.base_provider import BaseDataProvider


def _ts_range(start: str, count: int, step: str) -> list[pd.Timestamp]:
    base = pd.Timestamp(start, tz="UTC")
    delta = pd.to_timedelta(step)
    return [base + i * delta for i in range(count)]


def test_collect_missing_ranges_handles_exclusive_end_without_gap():
    """No supplemental fetch is needed when cached candles cover the window."""

    start = pd.Timestamp("2024-01-01T00:00:00Z")
    end = pd.Timestamp("2024-01-01T05:00:00Z")
    timestamps = _ts_range("2024-01-01T00:00:00Z", 5, "1H")

    missing = BaseDataProvider._collect_missing_ranges(timestamps, start, end, "1h")

    assert missing == []


def test_collect_missing_ranges_reports_trailing_gap_only_when_missing():
    """Trailing gaps start at the next expected candle rather than the last seen."""

    start = pd.Timestamp("2024-01-01T00:00:00Z")
    end = pd.Timestamp("2024-01-01T05:00:00Z")
    timestamps = _ts_range("2024-01-01T00:00:00Z", 3, "1H")

    missing = BaseDataProvider._collect_missing_ranges(timestamps, start, end, "1h")

    assert missing == [(pd.Timestamp("2024-01-01T03:00:00Z"), end)]

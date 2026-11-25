from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from market_data.contracts import CANDLE_FACT_TYPE
from portal.backend.service.storage.repos import candles as repository
from portal.backend.service.storage.repos.candles import _frozen_dataset_page_flags


def _at(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def test_interior_frozen_window_exposes_both_dataset_directions() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(3),
        effective_end=_at(6),
        has_extra=False,
        prefer_latest=False,
    ) == (True, True)


def test_latest_frozen_page_stops_only_at_dataset_end() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(1),
        effective_end=_at(10),
        has_extra=True,
        prefer_latest=True,
    ) == (True, False)


def test_forward_frozen_page_keeps_prior_boundary_and_reports_extra_rows() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(4),
        effective_end=_at(10),
        has_extra=True,
        prefer_latest=False,
    ) == (True, True)


def test_forward_frozen_page_stops_when_it_reaches_dataset_end() -> None:
    assert _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(10),
        effective_start=_at(4),
        effective_end=_at(10),
        has_extra=False,
        prefer_latest=False,
    ) == (True, False)


def test_empty_windows_retain_the_direction_of_available_dataset_history() -> None:
    before_dataset = _frozen_dataset_page_flags(
        frozen_start=_at(3),
        frozen_end=_at(10),
        effective_start=_at(3),
        effective_end=_at(2),
        has_extra=False,
        prefer_latest=False,
    )
    after_dataset = _frozen_dataset_page_flags(
        frozen_start=_at(1),
        frozen_end=_at(8),
        effective_start=_at(9),
        effective_end=_at(8),
        has_extra=False,
        prefer_latest=True,
    )
    assert before_dataset == (False, True)
    assert after_dataset == (True, False)


_WINDOW_CLOCKS = ("z", "naive", "utc", "offset", "offset_text", "mixed")


def _window_clocks(kind, start, end):
    offset = timezone(timedelta(hours=5, minutes=30))
    if kind == "z":
        return tuple(value.isoformat().replace("+00:00", "Z") for value in (start, end))
    if kind == "naive":
        return start.replace(tzinfo=None), end.replace(tzinfo=None)
    if kind == "offset":
        return start.astimezone(offset), end.astimezone(offset)
    if kind == "offset_text":
        return start.astimezone(offset).isoformat(), end.astimezone(offset).isoformat()
    if kind == "mixed":
        return start.isoformat().replace("+00:00", "Z"), end
    return start, end


@pytest.fixture
def canonical_candle_reads(monkeypatch):
    start = _at(1)
    rows = [{"id": f"candle-{index}", "fact_type": CANDLE_FACT_TYPE,
             "observation_time": start + timedelta(minutes=index), "revision": 1,
             "market_commit_seq": index + 1,
             "payload": {"open": 10, "high": 20, "low": 1, "close": 10 + index, "volume": None}}
            for index in range(4)]
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = [60]
    session.execute.return_value.scalar_one_or_none.return_value = 60
    requests = []

    def visible(begin, end, commit):
        return [row for row in rows if begin <= row["observation_time"] < end
                and (commit is None or row["market_commit_seq"] <= commit)]

    def nearest(_session, **query):
        requests.append(query)
        clocks = [row["observation_time"] for row in visible(query["start"], query["end"], query["commit"])
                  if (row["observation_time"] < query["cursor"] if query["prefer_latest"]
                      else row["observation_time"] >= query["cursor"])]
        return (max(clocks) if query["prefer_latest"] else min(clocks)) if clocks else None

    monkeypatch.setattr(repository.db, "session", lambda: nullcontext(session))
    monkeypatch.setattr(repository, "_cold_enabled", lambda _: True)
    monkeypatch.setattr(repository, "_cold_candle_series", lambda *_: 1)
    monkeypatch.setattr(repository.canonical_fact_storage_repository, "cold_history",
                        lambda _: SimpleNamespace(candle_nearest_observation=nearest))
    monkeypatch.setattr(repository, "_cold_candle_rows",
                        lambda _session, *, series_id, start, end, commit=None: iter(visible(start, end, commit)))
    return SimpleNamespace(start=start, end=start + timedelta(minutes=3), rows=rows,
                           session=session, requests=requests)


def _assert_utc_queries(requests):
    assert requests
    for query in requests:
        assert all(query[key].tzinfo is timezone.utc for key in ("start", "end", "cursor"))


@pytest.mark.parametrize("kind", _WINDOW_CLOCKS)
def test_report_candle_summary_normalizes_read_bounds_only(canonical_candle_reads, kind):
    source = canonical_candle_reads
    start, end = _window_clocks(kind, source.start, source.end)
    summary = repository.get_candle_storage_summary(
        instrument_id="fixture", timeframe="1m", start=start, end=end,
    )
    assert summary["candle_count"] == 3
    assert summary["first_candle"] == source.start
    assert summary["last_candle"] == source.end - timedelta(minutes=1)
    assert summary["gap_count"] == summary["missing_count"] == 0
    _assert_utc_queries(source.requests)


@pytest.mark.parametrize("kind", _WINDOW_CLOCKS)
@pytest.mark.parametrize("latest", [False, True])
def test_candle_paging_keeps_direction_and_exclusive_end(canonical_candle_reads, kind, latest):
    source = canonical_candle_reads
    start, end = _window_clocks(kind, source.start, source.end)
    page = repository.list_candles_for_series(
        instrument_id="fixture", timeframe="1m", start=start, end=end, limit=2, prefer_latest=latest,
    )
    assert [row["time"] for row in page] == [
        int((source.start + timedelta(minutes=index)).timestamp()) for index in ((1, 2) if latest else (0, 1))
    ]
    _assert_utc_queries(source.requests)


@pytest.mark.parametrize("kind", _WINDOW_CLOCKS)
def test_excursion_candle_windows_keep_each_range_and_limit(canonical_candle_reads, kind):
    source = canonical_candle_reads
    start, end = _window_clocks(kind, source.start, source.end)
    overlap_start, overlap_end = _window_clocks(kind, source.start + timedelta(minutes=1), source.end)
    windows = [{"window_id": "all", "start": start, "end": end, "limit": 4},
               {"window_id": "overlap", "start": overlap_start, "end": overlap_end, "limit": 1}]
    original = [dict(window) for window in windows]
    result = repository.list_candles_for_series_windows(instrument_id="fixture", timeframe="1m", windows=windows)
    assert [row["close"] for row in result["all"]] == [10, 11, 12]
    assert [row["close"] for row in result["overlap"]] == [11]
    assert windows == original
    _assert_utc_queries(source.requests)


@pytest.mark.parametrize("kind", _WINDOW_CLOCKS)
def test_frozen_candle_bounds_keep_original_range_and_commit(canonical_candle_reads, kind):
    source = canonical_candle_reads
    frozen = {"range_start": source.start, "range_end": source.end, "max_commit_seq": 2}
    source.session.execute.return_value.mappings.return_value.first.return_value = frozen
    start, end = _window_clocks(kind, source.start - timedelta(minutes=1), source.end + timedelta(minutes=1))
    page = repository.read_frozen_dataset_candles(
        dataset_id="frozen", series_id=1, start=start, end=end, limit=1,
    )
    assert [row["close"] for row in page["candles"]] == [10]
    assert page["max_commit_seq"] == 2
    assert page["range_start"] == frozen["range_start"] == source.start
    assert page["range_end"] == frozen["range_end"] == source.end
    assert page["has_more_before"] is False and page["has_more_after"] is True
    assert all(query["commit"] == 2 for query in source.requests)
    _assert_utc_queries(source.requests)

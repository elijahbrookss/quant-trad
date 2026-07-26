from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from engines.bot_runtime.live_market import ClosedLiveCandle
from market_data.contracts import CandleFact, CandleRecord, SourceIdentity
from market_data.store import IngestionOutcome
from portal.backend.service.market import feed_service


UTC = timezone.utc


class FakeStore:
    def __init__(self) -> None:
        self.source = SourceIdentity("CCXT", "COINBASE", "historical_api", "adapter.v1")
        self.records: list[CandleRecord] = []
        self.gaps: list[dict] = []
        self.ingest_calls: list[dict] = []
        self.resolve_calls = 0

    def register_source(self, identity, *, lineage=None):
        self.source = identity
        return 3

    def register_series(self, **kwargs):
        return 7

    def resolve_series_id(self, **kwargs):
        self.resolve_calls += 1
        return 7

    def ingest_candles(self, *, series_id, source_id, facts, **kwargs):
        rows = list(facts)
        self.ingest_calls.append(
            {
                "series_id": series_id,
                "source_id": source_id,
                "facts": rows,
                **kwargs,
            }
        )
        self.records = [
            CandleRecord(
                series_id=series_id,
                revision=1,
                market_commit_seq=index + 1,
                ingestion_run_id="ingest-1",
                source_identity_key=self.source.identity_key,
                source=self.source,
                provenance={},
                fact=fact,
            )
            for index, fact in enumerate(rows)
        ]
        return IngestionOutcome(
            ingestion_run_id="ingest-1",
            requested_count=len(rows),
            inserted_count=len(rows),
            corrected_count=0,
            noop_count=0,
            max_commit_seq=len(rows),
        )

    def read_candles(self, **kwargs):
        start = kwargs["start"]
        end = kwargs["end"]
        return [
            record
            for record in self.records
            if start <= record.fact.open_time < end
        ]

    def read_dataset_series(self, **kwargs):
        return list(self.records)

    def record_gap_evidence(self, **kwargs):
        self.gaps.append(kwargs)
        return "gap-hash"

    def list_gap_evidence(self, **kwargs):
        return list(self.gaps)


def _fact() -> CandleFact:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    return CandleFact(
        open_time=start,
        close_time=start + timedelta(minutes=1),
        open=100,
        high=102,
        low=99,
        close=101,
        volume=10,
        trade_count=5,
        known_at=start + timedelta(minutes=1),
        known_at_method="interval_close_inferred",
        accepted_at=start + timedelta(days=1),
    )


def _instrument() -> dict:
    return {
        "id": "instrument-1",
        "datasource": "CCXT",
        "exchange": "COINBASE",
        "symbol": "BTC/USD",
    }


def test_canonical_read_never_calls_provider_and_returns_source_facts_only(monkeypatch) -> None:
    store = FakeStore()
    store.records = [
        CandleRecord(
            series_id=7,
            revision=1,
            market_commit_seq=9,
            ingestion_run_id="legacy-v1",
            source_identity_key=store.source.identity_key,
            source=store.source,
            provenance={"legacy": True},
            fact=_fact(),
        )
    ]
    monkeypatch.setattr(
        feed_service,
        "get_provider",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("read path called provider")
        ),
    )

    frame = feed_service.CanonicalCandleFeed(store).read_by_instrument(
        _instrument(),
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:02:00Z",
        interval="1m",
    )

    assert len(frame) == 1
    assert "atr" not in frame.columns
    assert frame.iloc[0]["known_at_method"] == "interval_close_inferred"
    assert frame.attrs["provider_call_performed"] is False
    assert frame.attrs["market_data_series_id"] == 7
    assert frame.iloc[0]["provenance"] == {"legacy": True}
    assert frame.attrs["market_data_provenance"]["row_count"] == 1


def test_historical_acquisition_is_explicit_and_persists_closed_facts(monkeypatch) -> None:
    class Provider:
        def fetch_from_api(self, symbol, start, end, interval):
            return pd.DataFrame(
                [
                    {
                        "timestamp": "2024-01-01T00:00:00Z",
                        "open": 100,
                        "high": 102,
                        "low": 99,
                        "close": 101,
                        "volume": 10,
                        "trade_count": 5,
                    },
                    {
                        "timestamp": "2024-01-01T00:01:00Z",
                        "open": 101,
                        "high": 103,
                        "low": 100,
                        "close": 102,
                        "volume": 11,
                        "trade_count": 6,
                    },
                ]
            )

    store = FakeStore()
    monkeypatch.setattr(feed_service, "get_provider", lambda *args, **kwargs: Provider())
    result = feed_service.HistoricalCandleIngestor(store).ingest_by_instrument(
        _instrument(),
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:02:00Z",
        interval="1m",
    )

    assert result.series_id == 7
    assert result.outcome.inserted_count == 2
    assert result.gap_evidence_count == 0
    facts = store.ingest_calls[0]["facts"]
    assert [fact.known_at for fact in facts] == [fact.close_time for fact in facts]
    assert all(fact.known_at_method == "interval_close_inferred" for fact in facts)


def test_historical_acquisition_rejects_duplicate_provider_candles(monkeypatch) -> None:
    class Provider:
        def fetch_from_api(self, symbol, start, end, interval):
            return pd.DataFrame(
                [
                    {"timestamp": "2024-01-01T00:00:00Z", "open": 1, "high": 2, "low": 1, "close": 2},
                    {"timestamp": "2024-01-01T00:00:00Z", "open": 1, "high": 2, "low": 1, "close": 2},
                ]
            )

    store = FakeStore()
    monkeypatch.setattr(feed_service, "get_provider", lambda *args, **kwargs: Provider())
    with pytest.raises(ValueError, match="duplicate provider candle"):
        feed_service.HistoricalCandleIngestor(store).ingest_by_instrument(
            _instrument(),
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T00:02:00Z",
            interval="1m",
        )
    assert store.ingest_calls == []


def test_paper_sink_persists_before_return_and_forbids_corrections() -> None:
    store = FakeStore()
    sink = feed_service.PaperCandlePersistenceSink(store)
    candle = ClosedLiveCandle(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        symbol="BTC-USD",
        product_id="BTC-USD",
        timeframe="1m",
        time=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        open=100,
        high=102,
        low=99,
        close=101,
        volume=10,
        first_known_at="2024-01-01T00:01:01Z",
        last_known_at="2024-01-01T00:01:02Z",
    )

    persisted = sink.persist(
        candle,
        instrument_id="instrument-1",
        bot_id="bot-1",
        run_id="run-1",
    )

    assert persisted.fact.row_hash == store.records[0].fact.row_hash
    assert store.ingest_calls[0]["allow_corrections"] is False
    assert persisted.fact.known_at_method == "platform_acceptance"
    assert persisted.fact.known_at >= persisted.fact.accepted_at


def test_historical_acquisition_segments_requests_without_parallel_overlap(monkeypatch) -> None:
    calls: list[tuple[datetime, datetime]] = []

    class Provider:
        def fetch_from_api(self, symbol, start, end, interval):
            calls.append((start, end))
            return pd.DataFrame(
                [
                    {
                        "timestamp": start,
                        "open": 100,
                        "high": 102,
                        "low": 99,
                        "close": 101,
                    }
                ]
            )

    store = FakeStore()
    monkeypatch.setattr(feed_service, "get_provider", lambda *args, **kwargs: Provider())
    result = feed_service.HistoricalCandleIngestor(
        store, max_segment_points=1
    ).ingest_by_instrument(
        _instrument(),
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:02:00Z",
        interval="1m",
    )

    assert len(calls) == 2
    assert calls[0][1] == calls[1][0]
    assert result.outcome.inserted_count == 2


def test_historical_acquisition_records_empty_provider_evidence(monkeypatch) -> None:
    class Provider:
        def fetch_from_api(self, symbol, start, end, interval):
            return pd.DataFrame()

    store = FakeStore()
    monkeypatch.setattr(feed_service, "get_provider", lambda *args, **kwargs: Provider())
    with pytest.raises(RuntimeError, match="missing-data evidence was recorded"):
        feed_service.HistoricalCandleIngestor(
            store, max_segment_points=1
        ).ingest_by_instrument(
            _instrument(),
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T00:02:00Z",
            interval="1m",
        )

    assert len(store.gaps) == 2
    assert {gap["classification"] for gap in store.gaps} == {"provider_missing_data"}
    assert store.ingest_calls == []

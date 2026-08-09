from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from market_data.contracts import (
    CANDLE_FACT_TYPE,
    FUNDING_RATE_FACT_TYPE,
    OPEN_INTEREST_FACT_TYPE,
    CandleFact,
    CandleRecord,
    FundingRateFact,
    FundingRateRecord,
    InstrumentRole,
    MarketDataRequirement,
    MarketDataWindow,
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
    build_candle_material_hash,
    build_dataset_identity_hash,
    dataset_series_identity_payload,
    build_funding_rate_material_hash,
    build_open_interest_material_hash,
    build_quality_hash,
)


UTC = timezone.utc


def _fact(**overrides) -> CandleFact:
    open_time = datetime(2024, 1, 1, tzinfo=UTC)
    values = {
        "open_time": open_time,
        "close_time": open_time + timedelta(minutes=1),
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 12.5,
        "trade_count": 7,
        "known_at": open_time + timedelta(minutes=1),
        "known_at_method": "interval_close_inferred",
        "accepted_at": open_time + timedelta(days=1),
    }
    values.update(overrides)
    return CandleFact(**values)


def _record(fact: CandleFact, *, revision: int = 1, commit_seq: int = 1) -> CandleRecord:
    return CandleRecord(
        series_id=11,
        revision=revision,
        market_commit_seq=commit_seq,
        ingestion_run_id="ingest-1",
        source_identity_key="source-key",
        source=SourceIdentity("CCXT", "Coinbase", "legacy_import", "v1"),
        provenance={"evidence": "fixture"},
        fact=fact,
    )


def test_candle_fact_normalizes_utc_and_has_exact_stable_hash() -> None:
    first = _fact(open_time="2024-01-01T00:00:00Z", close_time="2024-01-01T00:01:00Z")
    second = _fact(
        open_time=datetime(2023, 12, 31, 18, 0, tzinfo=timezone(timedelta(hours=-6))),
        close_time=datetime(2023, 12, 31, 18, 1, tzinfo=timezone(timedelta(hours=-6))),
        accepted_at=datetime(2025, 1, 1, tzinfo=UTC),
    )

    assert first.open_time == datetime(2024, 1, 1, tzinfo=UTC)
    assert first.row_hash == second.row_hash


def test_candle_fact_hash_includes_causal_availability() -> None:
    fact = _fact()
    later_known = replace(
        fact,
        known_at=fact.known_at + timedelta(seconds=1),
        known_at_method="provider_publication",
    )

    assert fact.row_hash != later_known.row_hash


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"close_time": datetime(2024, 1, 1, tzinfo=UTC)}, "close_time"),
        ({"known_at": datetime(2024, 1, 1, tzinfo=UTC)}, "known_at"),
        ({"high": 98.0}, "high"),
        ({"low": 103.0}, "low"),
        ({"volume": -1}, "volume"),
        ({"trade_count": -1}, "trade_count"),
    ],
)
def test_candle_fact_rejects_malformed_source_facts(overrides, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _fact(**overrides)


def test_receipt_known_at_cannot_precede_acceptance() -> None:
    accepted = datetime(2024, 1, 1, 0, 1, 2, tzinfo=UTC)
    with pytest.raises(ValueError, match="receipt-based known_at"):
        _fact(
            known_at_method="stream_receipt",
            known_at=datetime(2024, 1, 1, 0, 1, 1, tzinfo=UTC),
            accepted_at=accepted,
            received_at=datetime(2024, 1, 1, 0, 1, 1, tzinfo=UTC),
        )


def test_source_identity_is_stable_but_adapter_version_is_material() -> None:
    first = SourceIdentity("CCXT", "Coinbase", "historical_api", "v1")
    same = SourceIdentity("ccxt", "coinbase", "HISTORICAL_API", "v1")
    changed = SourceIdentity("ccxt", "coinbase", "historical_api", "v2")

    assert first.identity_key == same.identity_key
    assert first.identity_key != changed.identity_key


def test_candle_requirements_and_windows_require_a_timeframe() -> None:
    with pytest.raises(ValueError, match="require timeframe_seconds"):
        MarketDataRequirement(fact_type=CANDLE_FACT_TYPE)
    with pytest.raises(ValueError, match="require timeframe_seconds"):
        MarketDataWindow(
            instrument_id="instrument-1",
            fact_type=CANDLE_FACT_TYPE,
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
        )


def test_open_interest_requirement_requires_causal_staleness_and_no_timeframe() -> None:
    requirement = MarketDataRequirement(
        key="open_interest",
        fact_type=OPEN_INTEREST_FACT_TYPE,
        instrument_role=InstrumentRole.PRIMARY,
        max_staleness_seconds=300,
    )

    assert requirement.contract_version == "derivatives.open_interest.v1"
    assert requirement.alignment.value == "latest_known"
    assert requirement.timeframe_seconds is None
    with pytest.raises(ValueError, match="requires max_staleness"):
        MarketDataRequirement(fact_type=OPEN_INTEREST_FACT_TYPE)
    with pytest.raises(ValueError, match="do not have a timeframe"):
        MarketDataRequirement(
            fact_type=OPEN_INTEREST_FACT_TYPE,
            timeframe_seconds=60,
            max_staleness_seconds=300,
        )


def test_open_interest_fact_exposes_schedule_without_inventing_provider_time() -> None:
    sample_time = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    received_at = sample_time + timedelta(seconds=5)
    fact = OpenInterestFact(
        sample_time=sample_time,
        value=12_345,
        received_at=received_at,
        accepted_at=received_at,
        known_at=received_at,
        known_at_method="platform_acceptance",
    )
    record = OpenInterestRecord(
        series_id=12,
        revision=1,
        market_commit_seq=2,
        ingestion_run_id="poll-1",
        source_identity_key="coinbase-source",
        source=SourceIdentity("COINBASE", "COINBASE_DIRECT", "poll_api", "v1"),
        provenance={"provider_event_time_available": False},
        fact=fact,
    )

    assert fact.sample_time_method == "collector_schedule"
    assert fact.source_published_at is None
    assert fact.known_at == received_at
    identity = {
        "instrument_id": "instrument-1",
        "fact_type": OPEN_INTEREST_FACT_TYPE,
        "timeframe_seconds": None,
        "contract_version": "derivatives.open_interest.v1",
    }
    assert build_open_interest_material_hash(
        series_identity=identity,
        records=[record],
    )


def test_open_interest_fact_rejects_negative_or_future_schedule() -> None:
    sample_time = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="nonnegative"):
        OpenInterestFact(
            sample_time=sample_time,
            value=-1,
            accepted_at=sample_time,
            known_at=sample_time,
            known_at_method="platform_acceptance",
        )
    with pytest.raises(ValueError, match="must not precede sample_time"):
        OpenInterestFact(
            sample_time=sample_time,
            value=1,
            accepted_at=sample_time - timedelta(seconds=1),
            known_at=sample_time - timedelta(seconds=1),
            known_at_method="platform_acceptance",
        )


def test_funding_rate_preserves_signed_rate_and_provider_time_without_causalizing_it() -> None:
    sample_time = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    known_at = sample_time + timedelta(seconds=4)
    funding_time = sample_time + timedelta(hours=1)
    fact = FundingRateFact(
        sample_time=sample_time,
        rate=-0.000025,
        funding_time=funding_time,
        interval_seconds=3600,
        received_at=known_at,
        accepted_at=known_at,
        known_at=known_at,
        known_at_method="platform_acceptance",
    )
    record = FundingRateRecord(
        series_id=13,
        revision=1,
        market_commit_seq=3,
        ingestion_run_id="funding-poll-1",
        source_identity_key="coinbase-funding-source",
        source=SourceIdentity("COINBASE", "COINBASE_DIRECT", "poll_api", "v1"),
        provenance={
            "provider_funding_time_semantics": "provider_reported_unspecified"
        },
        fact=fact,
    )

    assert fact.rate == -0.000025
    assert fact.funding_time == funding_time
    assert fact.known_at == known_at
    assert fact.source_published_at is None
    assert build_funding_rate_material_hash(
        series_identity={
            "instrument_id": "instrument-1",
            "fact_type": FUNDING_RATE_FACT_TYPE,
            "timeframe_seconds": None,
            "contract_version": "derivatives.funding_rate.v1",
        },
        records=[record],
    )


def test_funding_rate_requirement_and_fact_reject_invalid_shape() -> None:
    requirement = MarketDataRequirement(
        key="funding_rate",
        fact_type=FUNDING_RATE_FACT_TYPE,
        max_staleness_seconds=300,
    )
    assert requirement.contract_version == "derivatives.funding_rate.v1"
    assert requirement.alignment.value == "latest_known"
    with pytest.raises(ValueError, match="do not have a timeframe"):
        MarketDataRequirement(
            fact_type=FUNDING_RATE_FACT_TYPE,
            timeframe_seconds=60,
            max_staleness_seconds=300,
        )
    for interval_seconds in (0, 1.5):
        with pytest.raises(ValueError, match="positive integer"):
            FundingRateFact(
                sample_time=datetime(2024, 1, 1, tzinfo=UTC),
                rate=0.0,
                funding_time=datetime(2024, 1, 1, 1, tzinfo=UTC),
                interval_seconds=interval_seconds,
                accepted_at=datetime(2024, 1, 1, tzinfo=UTC),
                known_at=datetime(2024, 1, 1, tzinfo=UTC),
                known_at_method="platform_acceptance",
            )
def test_material_hash_ignores_storage_revision_but_not_fact_values() -> None:
    identity = {
        "instrument_id": "instrument-1",
        "fact_type": CANDLE_FACT_TYPE,
        "timeframe_seconds": 60,
        "contract_version": "candle.ohlcv.v1",
    }
    fact = _fact()
    first = build_candle_material_hash(
        series_identity=identity,
        records=[_record(fact, revision=1, commit_seq=1)],
    )
    reimported = build_candle_material_hash(
        series_identity=identity,
        records=[_record(fact, revision=8, commit_seq=99)],
    )
    corrected = build_candle_material_hash(
        series_identity=identity,
        records=[_record(replace(fact, close=100.5), revision=2, commit_seq=2)],
    )

    assert first == reimported
    assert first != corrected


def test_quality_hash_is_order_independent_and_separate() -> None:
    first = {"classification": "missing", "start": "2024-01-01T00:01:00Z"}
    second = {"classification": "expected_sparse", "start": "2024-01-01T00:02:00Z"}

    assert build_quality_hash([first, second]) == build_quality_hash([second, first])


def test_dataset_identity_ignores_storage_watermark_but_not_evidence() -> None:
    base = {
        "series_id": 11,
        "range_start": "2024-01-01T00:00:00Z",
        "range_end": "2024-01-02T00:00:00Z",
        "max_commit_seq": 10,
        "row_count": 1440,
        "material_hash": "material",
        "provenance_hash": "provenance",
        "quality_hash": "quality",
        "source_summary": {"counts": {"source": 1440}},
        "quality_summary": {"evidence_count": 0},
    }

    first = build_dataset_identity_hash([base])
    unrelated_commit = build_dataset_identity_hash(
        [{**base, "max_commit_seq": 999}]
    )
    changed_quality = build_dataset_identity_hash(
        [{**base, "quality_hash": "changed"}]
    )

    assert first == unrelated_commit
    assert first != changed_quality


def test_dataset_series_identity_projects_quality_evidence_through_hash() -> None:
    entry = {
        "series_id": 11,
        "range_start": "2024-01-01T00:00:00Z",
        "range_end": "2024-01-02T00:00:00Z",
        "max_commit_seq": 10,
        "row_count": 1440,
        "material_hash": "material",
        "provenance_hash": "provenance",
        "quality_hash": "quality",
        "source_summary": {"counts": {"source": 1440}},
        "quality_summary": {"evidence_count": 1},
        "quality_evidence": [{"classification": "provider_missing_data"}],
        "identity_key": "joined-series-detail-not-in-v1-identity",
    }

    projected = dataset_series_identity_payload(entry)

    assert "quality_evidence" not in projected
    assert "identity_key" not in projected
    assert projected["quality_hash"] == "quality"
    assert projected["range_start"] == "2024-01-01T00:00:00.000000Z"

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import uuid

import pytest

from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    FUNDING_RATE_FACT_TYPE,
    FUNDING_RATE_FACT_VERSION,
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    CandleFact,
    DatasetSeriesRequest,
    FundingRateFact,
    OpenInterestFact,
    SourceIdentity,
)
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market.normalization_service import (
    market_normalization_service,
)
from portal.backend.service.storage.repos.market_data import market_data_repo


pytestmark = pytest.mark.db

_BASE = datetime(2024, 1, 1, tzinfo=UTC)
_TIMEFRAME_SECONDS = 3600


def _fact(index: int, *, close: float | None = None) -> CandleFact:
    open_time = _BASE + timedelta(hours=index)
    close_time = open_time + timedelta(hours=1)
    close_value = float(close if close is not None else 100.5 + index)
    return CandleFact(
        open_time=open_time,
        close_time=close_time,
        open=100.0 + index,
        high=max(102.0 + index, close_value),
        low=min(99.0 + index, close_value),
        close=close_value,
        volume=10.0 + index,
        trade_count=100 + index,
        source_published_at=None,
        received_at=None,
        accepted_at=close_time + timedelta(minutes=5),
        known_at=close_time,
        known_at_method="interval_close_inferred",
    )


@pytest.fixture
def canonical_series() -> dict[str, int | str]:
    """Create unique immutable facts in the disposable DB test profile."""

    token = uuid.uuid4().hex
    instrument_id = f"md-db-{token[:24]}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="TEST",
                exchange="ISOLATED",
                symbol=f"BTC-{token[:8].upper()}",
                instrument_type="spot",
                can_short=False,
                short_requires_borrow=False,
                has_funding=False,
                extra_metadata={},
            )
        )

    source_identity = SourceIdentity(
        provider="TEST",
        venue="ISOLATED",
        source_kind="fixture",
        adapter_version=f"dataset-db-test.{token}",
    )
    source_id = market_data_repo.register_source(
        source_identity,
        lineage={"fixture": "tests/test_market_data/test_repository_db.py"},
    )
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=_TIMEFRAME_SECONDS,
        contract_version=CANDLE_FACT_VERSION,
    )
    return {
        "instrument_id": instrument_id,
        "source_id": source_id,
        "source_identity_key": source_identity.identity_key,
        "series_id": series_id,
    }


def _ingest(
    fixture: dict[str, int | str],
    facts: list[CandleFact],
    *,
    source_revision: str,
) -> None:
    market_data_repo.ingest_candles(
        series_id=int(fixture["series_id"]),
        source_id=int(fixture["source_id"]),
        facts=facts,
        request={"fixture": "market_dataset_db_isolation"},
        source_revision=source_revision,
    )


def _request(series_id: int) -> DatasetSeriesRequest:
    return DatasetSeriesRequest(
        series_id=series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=3),
    )


def test_content_identical_freeze_reuses_exact_persisted_manifest(
    canonical_series: dict[str, int | str],
) -> None:
    series_id = int(canonical_series["series_id"])
    _ingest(
        canonical_series,
        [_fact(0), _fact(1), _fact(2)],
        source_revision="fixture-v1",
    )
    first = market_data_repo.freeze_dataset([_request(series_id)])
    assert first.reused_existing is False

    _ingest(canonical_series, [_fact(3)], source_revision="fixture-v1")
    assert market_data_repo.current_commit_seq() > first.max_commit_seq

    repeated = market_data_repo.freeze_dataset([_request(series_id)])
    persisted = market_data_repo.get_dataset(first.dataset_id)

    assert repeated.dataset_id == first.dataset_id
    assert repeated.dataset_hash == first.dataset_hash
    assert repeated == persisted
    assert repeated.reused_existing is True
    assert persisted.reused_existing is False
    assert repeated.max_commit_seq == first.max_commit_seq
    assert repeated.series == first.series


def test_freeze_persists_source_bound_quality_evidence_array(
    canonical_series: dict[str, int | str],
) -> None:
    series_id = int(canonical_series["series_id"])
    source_id = int(canonical_series["source_id"])
    _ingest(
        canonical_series,
        [_fact(0), _fact(2)],
        source_revision="fixture-gap-v1",
    )
    evidence_hash = market_data_repo.record_gap_evidence(
        series_id=series_id,
        source_id=source_id,
        start=_BASE + timedelta(hours=1),
        end=_BASE + timedelta(hours=2),
        classification="provider_missing_data",
        expected_count=1,
        observed_count=0,
        evidence={
            "schema_version": "market_gap_evidence.v1",
            "source_id": source_id,
            "reason_code": "fixture_gap",
        },
    )

    frozen = market_data_repo.freeze_dataset([_request(series_id)])
    persisted = market_data_repo.get_dataset(frozen.dataset_id)
    quality = persisted.series[0]["quality_evidence"]

    assert isinstance(quality, list)
    assert quality == frozen.series[0]["quality_evidence"]
    assert any(row["evidence_hash"] == evidence_hash for row in quality)
    assert all(
        row["source_identity_key"]
        == canonical_series["source_identity_key"]
        for row in quality
    )


def test_frozen_dataset_cannot_observe_post_freeze_correction(
    canonical_series: dict[str, int | str],
) -> None:
    series_id = int(canonical_series["series_id"])
    original = [_fact(0), _fact(1), _fact(2)]
    _ingest(canonical_series, original, source_revision="fixture-v1")
    frozen = market_data_repo.freeze_dataset([_request(series_id)])

    _ingest(
        canonical_series,
        [_fact(0, close=101.75)],
        source_revision="fixture-v2",
    )
    latest = market_data_repo.read_candles(
        series_id=series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=3),
    )
    replay = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=series_id,
    )

    assert latest[0].revision == 2
    assert latest[0].fact.row_hash != original[0].row_hash
    assert replay[0].revision == 1
    assert replay[0].fact.row_hash == original[0].row_hash
    assert market_data_repo.get_dataset(frozen.dataset_id) == frozen

    corrected = market_data_repo.freeze_dataset([_request(series_id)])
    assert corrected.dataset_id != frozen.dataset_id
    assert corrected.dataset_hash != frozen.dataset_hash

    with pytest.raises(ValueError, match="range_expansion_forbidden"):
        market_data_repo.read_dataset_series(
            dataset_id=frozen.dataset_id,
            series_id=series_id,
            start=_BASE - timedelta(hours=1),
        )


def test_frozen_source_binding_filters_before_latest_revision_selection(
    canonical_series: dict[str, int | str],
) -> None:
    series_id = int(canonical_series["series_id"])
    first_source_id = int(canonical_series["source_id"])
    first_source_identity_key = str(canonical_series["source_identity_key"])
    original = _fact(0, close=101.0)
    market_data_repo.ingest_candles(
        series_id=series_id,
        source_id=first_source_id,
        facts=[original],
        request={"fixture": "source-filter-before-revision"},
        source_revision="source-a-v1",
    )
    second_source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="TEST_B",
            venue="ISOLATED",
            source_kind="fixture",
            adapter_version=f"source-filter.{uuid.uuid4().hex}",
        ),
        lineage={"fixture": "source-filter-before-revision"},
    )
    market_data_repo.ingest_candles(
        series_id=series_id,
        source_id=second_source_id,
        facts=[_fact(0, close=202.0)],
        request={"fixture": "source-filter-before-revision"},
        source_revision="source-b-v2",
    )
    frozen = market_data_repo.freeze_dataset(
        [
            DatasetSeriesRequest(
                series_id=series_id,
                start=_BASE,
                end=_BASE + timedelta(hours=1),
            )
        ]
    )

    replay = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=series_id,
        source_identity_keys=(first_source_identity_key,),
    )

    assert len(replay) == 1
    assert replay[0].source_identity_key == first_source_identity_key
    assert replay[0].fact.close == Decimal("101.0")


def test_mixed_fact_freeze_uses_one_commit_clock_and_preserves_oi_revision(
    canonical_series: dict[str, int | str],
) -> None:
    candle_series_id = int(canonical_series["series_id"])
    source_id = int(canonical_series["source_id"])
    _ingest(
        canonical_series,
        [_fact(0), _fact(1), _fact(2)],
        source_revision="fixture-v1",
    )
    oi_series_id = market_data_repo.register_series(
        instrument_id=str(canonical_series["instrument_id"]),
        fact_type=OPEN_INTEREST_FACT_TYPE,
        timeframe_seconds=None,
        contract_version=OPEN_INTEREST_FACT_VERSION,
    )

    def oi(index: int, value: float) -> OpenInterestFact:
        sample = _BASE + timedelta(hours=index)
        known = sample + timedelta(minutes=2)
        return OpenInterestFact(
            sample_time=sample,
            value=value,
            received_at=known,
            accepted_at=known,
            known_at=known,
            known_at_method="platform_acceptance",
        )

    original = [oi(0, 1000), oi(1, 1100), oi(2, 1200)]
    first_ingest = market_data_repo.ingest_open_interest(
        series_id=oi_series_id,
        source_id=source_id,
        facts=original,
        provenance={"fixture": "mixed-fact-freeze"},
        source_revision="oi-v1",
    )
    repeated = market_data_repo.ingest_open_interest(
        series_id=oi_series_id,
        source_id=source_id,
        facts=original,
        provenance={"fixture": "mixed-fact-freeze"},
        source_revision="oi-v1",
    )
    assert first_ingest.inserted_count == 3
    assert repeated.noop_count == 3
    assert repeated.max_commit_seq == first_ingest.max_commit_seq

    frozen = market_data_repo.freeze_dataset(
        [_request(candle_series_id), _request(oi_series_id)],
        purpose="backtest",
    )
    assert len(frozen.series) == 2
    assert frozen.max_commit_seq >= max(
        int(row["max_commit_seq"]) for row in frozen.series
    )
    oi_manifest = next(
        row for row in frozen.series if row["fact_type"] == OPEN_INTEREST_FACT_TYPE
    )
    assert oi_manifest["timeframe_seconds"] is None
    assert oi_manifest["row_count"] == 3

    market_data_repo.ingest_open_interest(
        series_id=oi_series_id,
        source_id=source_id,
        facts=[oi(0, 9999)],
        provenance={"fixture": "post-freeze-correction"},
        source_revision="oi-v2",
    )
    latest = market_data_repo.read_open_interest(
        series_id=oi_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=3),
    )
    replay = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=oi_series_id,
    )
    before_known = market_data_repo.read_open_interest(
        series_id=oi_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=1),
        known_at_lte=_BASE + timedelta(minutes=1),
    )

    assert latest[0].revision == 2
    assert latest[0].fact.value == 9999
    assert replay[0].revision == 1
    assert replay[0].fact.value == 1000
    assert before_known == []


def test_funding_rate_round_trip_and_freeze_preserve_causal_observations(
    canonical_series: dict[str, int | str],
) -> None:
    source_id = int(canonical_series["source_id"])
    funding_series_id = market_data_repo.register_series(
        instrument_id=str(canonical_series["instrument_id"]),
        fact_type=FUNDING_RATE_FACT_TYPE,
        timeframe_seconds=None,
        contract_version=FUNDING_RATE_FACT_VERSION,
    )

    def funding(index: int, rate: float) -> FundingRateFact:
        sample = _BASE + timedelta(hours=index)
        known = sample + timedelta(minutes=2)
        return FundingRateFact(
            sample_time=sample,
            rate=rate,
            funding_time=sample + timedelta(hours=1),
            interval_seconds=3600,
            received_at=known,
            accepted_at=known,
            known_at=known,
            known_at_method="platform_acceptance",
        )

    facts = [funding(0, -0.00002), funding(1, 0.00003)]
    outcome = market_data_repo.ingest_funding_rates(
        series_id=funding_series_id,
        source_id=source_id,
        facts=facts,
        provenance={
            "fixture": "funding-rate-round-trip",
            "provider_funding_time_semantics": "provider_reported_unspecified",
        },
        source_revision="funding-v1",
    )
    visible = market_data_repo.read_funding_rates(
        series_id=funding_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=2),
    )
    before_known = market_data_repo.read_funding_rates(
        series_id=funding_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=1),
        known_at_lte=_BASE + timedelta(minutes=1),
    )
    frozen = market_data_repo.freeze_dataset(
        [
            DatasetSeriesRequest(
                series_id=funding_series_id,
                start=_BASE,
                end=_BASE + timedelta(hours=2),
            )
        ],
        purpose="research",
    )
    replay = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=funding_series_id,
    )

    assert outcome.inserted_count == 2
    assert [record.fact.rate for record in visible] == [-0.00002, 0.00003]
    assert visible[0].fact.funding_time == _BASE + timedelta(hours=1)
    assert before_known == []
    assert [record.fact.row_hash for record in replay] == [
        fact.row_hash for fact in facts
    ]

    installed = market_normalization_service.install_builtin_specs(
        approved_by="normalization-db-test"
    )
    bps_spec_id = next(
        row["spec_id"] for row in installed if row["feature_name"] == "funding_rate_bps"
    )
    percentile_spec_id = next(
        row["spec_id"]
        for row in installed
        if row["feature_name"] == "funding_rate_percentile_30d"
    )
    normalized = market_normalization_service.compare_persisted(
        spec_id=bps_spec_id,
        source_series_id=funding_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=2),
        known_at=_BASE + timedelta(hours=2),
    )
    assert normalized["persisted_equal"] is True
    assert normalized["provider_call_performed"] is False
    assert normalized["statuses"] == {"valid": 2}
    normalized_series_id = int(normalized["output_series_id"])

    warmup = market_normalization_service.materialize(
        spec_id=percentile_spec_id,
        source_series_id=funding_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=2),
        known_at=_BASE + timedelta(hours=2),
    )
    assert warmup["statuses"] == {"insufficient_history": 2}

    requests = [
        DatasetSeriesRequest(
            series_id=funding_series_id,
            start=_BASE,
            end=_BASE + timedelta(hours=2),
        ),
        DatasetSeriesRequest(
            series_id=normalized_series_id,
            start=_BASE,
            end=_BASE + timedelta(hours=2),
        ),
    ]
    normalized_dataset = market_data_repo.freeze_dataset(
        requests,
        purpose="research",
    )
    repeated = market_data_repo.freeze_dataset(requests, purpose="research")
    normalized_replay = market_data_repo.read_dataset_series(
        dataset_id=normalized_dataset.dataset_id,
        series_id=normalized_series_id,
    )
    correction = market_data_repo.ingest_funding_rates(
        series_id=funding_series_id,
        source_id=source_id,
        facts=[funding(0, -0.00004)],
        provenance={"fixture": "post-freeze-funding-correction"},
        source_revision="funding-v2",
    )
    recomputed = market_normalization_service.compare_persisted(
        spec_id=bps_spec_id,
        source_series_id=funding_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=2),
        known_at=_BASE + timedelta(hours=2),
    )
    latest_normalized = market_data_repo.read_series_records(
        series_id=normalized_series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=2),
    )
    frozen_after_correction = market_data_repo.read_dataset_series(
        dataset_id=normalized_dataset.dataset_id,
        series_id=normalized_series_id,
    )
    assert correction.corrected_count == 1
    assert recomputed["persisted_equal"] is True
    assert recomputed["inserted_count"] == 1
    assert [record.fact.value for record in latest_normalized] == [
        Decimal("-0.4000000000000000000000"),
        Decimal("0.3000000000000000000000"),
    ]
    assert [record.fact.value for record in frozen_after_correction] == [
        Decimal("-0.2000000000000000000000"),
        Decimal("0.3000000000000000000000"),
    ]

    assert repeated.dataset_id == normalized_dataset.dataset_id
    assert repeated.dataset_hash == normalized_dataset.dataset_hash
    assert [record.fact.value for record in normalized_replay] == [
        Decimal("-0.2000000000000000000000"),
        Decimal("0.3000000000000000000000"),
    ]
    assert normalized_dataset.metadata["normalization_refs"][0][
        "source_series_ids"
    ] == [funding_series_id]
    normalized_series = next(
        row for row in market_data_repo.list_series()
        if int(row["id"]) == normalized_series_id
    )
    assert normalized_series["feature_count"] == 2


def test_structured_canonical_fact_freezes_and_replays_with_schema_contract() -> None:
    token = uuid.uuid4().hex
    instrument_id = f"canonical-v2-{token[:20]}"
    with db.session() as session:
        session.add(
            InstrumentRecord(
                id=instrument_id,
                datasource="TEST",
                exchange="ISOLATED",
                symbol=f"CFT-{token[:8].upper()}",
                instrument_type="perpetual",
                can_short=False,
                short_requires_borrow=False,
                has_funding=True,
                extra_metadata={},
            )
        )
    source = SourceIdentity(
        provider="TEST",
        venue="ISOLATED",
        source_kind="fixture",
        adapter_version=f"canonical-funding-v2.{token}",
    )
    source_id = market_data_repo.register_source(source)
    series_id = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type=FUNDING_RATE_FACT_TYPE,
        timeframe_seconds=None,
        contract_version="derivatives.funding_rate.v2",
    )
    fact = CanonicalFact(
        fact_type=FUNDING_RATE_FACT_TYPE,
        payload_schema_id="derivatives.funding_rate.v2",
        observation_key="schedule:2024-01-01T00:00:00Z",
        observation_time=_BASE,
        observation_time_method="collector_schedule",
        received_at=_BASE + timedelta(seconds=1),
        accepted_at=_BASE + timedelta(seconds=2),
        known_at=_BASE + timedelta(seconds=2),
        known_at_method="platform_acceptance",
        source=source,
        transformation_id="fixture_funding_to_canonical.v2",
        payload={
            "rate": Decimal("0.00012500"),
            "raw_rate": "0.00012500",
            "funding_time": _BASE - timedelta(hours=1),
            "interval_seconds": 3600,
            "unit": "fraction",
        },
        provenance={"external_object": "fixture-funding-response"},
    )

    outcome = market_data_repo.ingest_facts(
        series_id=series_id,
        source_id=source_id,
        facts=[fact],
        request={"fixture": "structured_canonical_replay"},
    )
    direct = market_data_repo.read_facts(
        series_id=series_id,
        start=_BASE,
        end=_BASE + timedelta(hours=1),
    )
    frozen = market_data_repo.freeze_dataset(
        [
            DatasetSeriesRequest(
                series_id=series_id,
                start=_BASE,
                end=_BASE + timedelta(hours=1),
            )
        ]
    )
    replay = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=series_id,
    )
    listed = next(
        row
        for row in market_data_repo.list_series()
        if int(row["id"]) == series_id
    )

    assert outcome.inserted_count == 1
    assert len(direct) == 1
    assert isinstance(direct[0], CanonicalFactRecord)
    assert direct[0].fact.payload["rate"] == "0.000125"
    assert direct[0].fact.payload["funding_time"] == (
        "2023-12-31T23:00:00.000000Z"
    )
    assert frozen.series[0]["payload_schemas"] == [
        {
            "schema_id": "derivatives.funding_rate.v2",
            "contract_hash": fact.payload_contract_hash,
        }
    ]
    assert len(replay) == 1
    assert isinstance(replay[0], CanonicalFactRecord)
    assert replay[0].fact.payload_schema_id == "derivatives.funding_rate.v2"
    assert replay[0].fact.payload_contract_hash == fact.payload_contract_hash
    assert listed["version_count"] == 1
    assert listed["fact_count"] == 1
    assert listed["funding_rate_count"] == 1

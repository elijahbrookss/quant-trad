from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from market_data.contracts import FundingRateFact, FundingRateRecord, SourceIdentity, TypedFeatureRecord
from portal.backend.service.market.normalization_service import (
    MarketNormalizationService,
    builtin_normalization_specs,
)
from portal.backend.service.storage.repos.normalization import (
    _spec_from_row,
    _unreferenced_legacy_spec_from_row,
)


BASE = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


class _Repository:
    def __init__(self, spec) -> None:
        self.spec = spec
        self.records: tuple[TypedFeatureRecord, ...] = ()
        self.ingest_count = 0

    def get_spec(self, spec_id: str):
        assert spec_id == self.spec.spec_id
        return self.spec

    def ingest(self, facts):
        self.ingest_count += 1
        base_commit = 1_000 + self.ingest_count * 100
        self.records = tuple(
            TypedFeatureRecord(
                version_id=f"nfv-{self.ingest_count}-{index}",
                series_id=fact.series_id,
                revision=1,
                market_commit_seq=base_commit + index,
                provenance_hash=hashlib.sha256(fact.input_fingerprint.encode()).hexdigest(),
                quality={
                    "classification": fact.status.value,
                    "valid": fact.status.value == "valid",
                    "reason": fact.reason,
                },
                fact=fact,
            )
            for index, fact in enumerate(facts, start=1)
        )
        return SimpleNamespace(
            inserted_count=len(self.records),
            noop_count=0,
            max_commit_seq=max(record.market_commit_seq for record in self.records),
        )

    def read_records(self, *, as_of_commit_seq: int, **_kwargs):
        return tuple(
            record
            for record in self.records
            if record.market_commit_seq <= as_of_commit_seq
        )


class _Store:
    def __init__(self, records, *, gaps=()) -> None:
        self.records = list(records)
        self.gaps = list(gaps)
        self.read_calls = []
        self.gap_calls = []

    def list_series(self):
        return [
            {
                "id": 11,
                "instrument_id": "BIP-20DEC30-CDE",
                "fact_type": "derivatives.funding_rate",
                "timeframe_seconds": None,
                "contract_version": "derivatives.funding_rate.v1",
            }
        ]

    def current_commit_seq(self) -> int:
        return 77

    def read_series_records(self, **kwargs):
        self.read_calls.append(dict(kwargs))
        return list(self.records)

    def list_gap_evidence(self, **kwargs):
        self.gap_calls.append(dict(kwargs))
        return list(self.gaps)

    def register_series(self, **kwargs):
        assert kwargs == {
            "instrument_id": "BIP-20DEC30-CDE",
            "fact_type": "market.normalized.funding_rate_bps",
            "timeframe_seconds": None,
            "contract_version": f"market.normalized_feature.v1/{_funding_bps_spec().spec_id}",
        }
        return 22


def _funding_bps_spec():
    return next(
        spec
        for spec in builtin_normalization_specs()
        if spec.feature_name == "funding_rate_bps"
    )


def _funding_record(offset_minutes: int, rate: float, commit_seq: int) -> FundingRateRecord:
    sample_time = BASE + timedelta(minutes=offset_minutes)
    source = SourceIdentity(
        provider="COINBASE",
        venue="CDE",
        source_kind="funding_current",
        adapter_version="coinbase_cdp_rest.v1",
    )
    fact = FundingRateFact(
        sample_time=sample_time,
        rate=rate,
        funding_time=sample_time + timedelta(hours=1),
        interval_seconds=60,
        known_at=sample_time + timedelta(seconds=1),
        known_at_method="platform_acceptance",
        accepted_at=sample_time + timedelta(seconds=1),
    )
    return FundingRateRecord(
        series_id=11,
        revision=1,
        market_commit_seq=commit_seq,
        ingestion_run_id="run-funding",
        source_identity_key=source.identity_key,
        source=source,
        provenance={"provider": "COINBASE"},
        fact=fact,
    )


def test_materialization_uses_one_causal_watermark_and_replays_equal() -> None:
    records = [_funding_record(0, 0.001, 71), _funding_record(1, 0.002, 72)]
    store = _Store(records)
    repository = _Repository(_funding_bps_spec())
    service = MarketNormalizationService(repository=repository, store=store)

    result = service.compare_persisted(
        spec_id=repository.spec.spec_id,
        source_series_id=11,
        start=BASE,
        end=BASE + timedelta(minutes=2),
        known_at=BASE + timedelta(minutes=2),
    )

    assert result["persisted_equal"] is True
    assert result["provider_call_performed"] is False
    assert result["selection_watermark"] == 77
    assert result["source_watermark"] == 72
    assert result["statuses"] == {"valid": 2}
    assert store.read_calls[0]["as_of_commit_seq"] == 77
    assert store.read_calls[0]["known_at_lte"] == BASE + timedelta(minutes=2)
    assert store.gap_calls[0]["as_of_commit_seq"] == 77
    assert store.gap_calls[0]["known_at_lte"] == BASE + timedelta(minutes=2)


def test_gap_detection_time_propagates_to_explicit_invalid_output() -> None:
    records = [_funding_record(0, 0.001, 71), _funding_record(1, 0.002, 72)]
    gap_start = BASE + timedelta(minutes=1)
    detected_at = gap_start + timedelta(seconds=30)
    store = _Store(
        records,
        gaps=[
            {
                "start": gap_start.isoformat(),
                "end": (gap_start + timedelta(minutes=1)).isoformat(),
                "classification": "stream_unhealthy",
                "detected_as_of_commit_seq": 75,
                "detected_at": detected_at.isoformat(),
                "evidence_hash": hashlib.sha256(b"gap").hexdigest(),
            }
        ],
    )
    repository = _Repository(_funding_bps_spec())
    service = MarketNormalizationService(repository=repository, store=store)

    result = service.materialize(
        spec_id=repository.spec.spec_id,
        source_series_id=11,
        start=BASE,
        end=BASE + timedelta(minutes=2),
        known_at=BASE + timedelta(minutes=2),
    )

    assert result["statuses"] == {"invalid_input": 1, "valid": 1}
    invalid = repository.records[1].fact
    assert invalid.value is None
    assert invalid.reason == "stream_unhealthy"
    assert invalid.known_at == detected_at
    assert invalid.input_watermark == 75


def test_materialization_rejects_decision_before_complete_range() -> None:
    service = MarketNormalizationService(
        repository=_Repository(_funding_bps_spec()),
        store=_Store([_funding_record(0, 0.001, 71)]),
    )
    with pytest.raises(ValueError, match="causal range"):
        service.materialize(
            spec_id=_funding_bps_spec().spec_id,
            source_series_id=11,
            start=BASE,
            end=BASE + timedelta(minutes=2),
            known_at=BASE + timedelta(minutes=1),
        )


def _stored_spec_row(spec, *, spec_id: str, spec_hash: str | None = None, refs: int = 0):
    return {
        **spec.material(),
        "id": spec_id,
        "spec_hash": spec_hash or spec.spec_hash,
        "materialized_ref_count": refs,
        "dataset_ref_count": 0,
    }


def test_unreferenced_legacy_spec_identity_is_verified_before_quarantine() -> None:
    spec = _funding_bps_spec()
    row = _stored_spec_row(spec, spec_id=f"nsp_{spec.spec_hash[:40]}")

    with pytest.raises(RuntimeError, match="storage_corrupt"):
        _spec_from_row(row)

    assert _unreferenced_legacy_spec_from_row(row) == spec


def test_legacy_spec_quarantine_rejects_material_hash_mismatch() -> None:
    spec = _funding_bps_spec()
    row = _stored_spec_row(
        spec,
        spec_id=f"nsp_{spec.spec_hash[:40]}",
        spec_hash="0" * 64,
    )

    assert _unreferenced_legacy_spec_from_row(row) is None


def test_referenced_legacy_spec_identity_remains_fail_loud() -> None:
    spec = _funding_bps_spec()
    row = _stored_spec_row(
        spec,
        spec_id=f"nsp_{spec.spec_hash[:40]}",
        refs=1,
    )

    with pytest.raises(RuntimeError, match="legacy_identity_referenced"):
        _unreferenced_legacy_spec_from_row(row)

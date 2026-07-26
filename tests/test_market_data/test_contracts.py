from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CandleFact,
    CandleRecord,
    MarketDataRequirement,
    MarketDataWindow,
    SourceIdentity,
    build_candle_material_hash,
    build_dataset_identity_hash,
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

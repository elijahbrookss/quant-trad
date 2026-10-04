from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from market_data.candle_derivation import derive_frozen_candles
from market_data.contracts import CandleFact, CandleRecord, SourceIdentity

START = datetime(2022, 10, 1, tzinfo=timezone.utc)
SOURCE = SourceIdentity(provider="TEST", venue="ISOLATED", source_kind="fixture", adapter_version="v1")


def minute_records(count=10):
    result = []
    for index in range(count):
        opened = START + timedelta(minutes=index)
        closed = opened + timedelta(minutes=1)
        fact = CandleFact(open_time=opened, close_time=closed, open=100 + index,
            high=103 + index, low=98 + index, close=101 + index, volume=index + 1,
            trade_count=2, known_at=closed, known_at_method="interval_close_inferred",
            accepted_at=START + timedelta(days=1), source_published_at=None, received_at=None)
        result.append(CandleRecord(series_id=1, revision=1, market_commit_seq=index + 1,
            ingestion_run_id="fixture", source_identity_key=SOURCE.identity_key,
            source=SOURCE, provenance={}, fact=fact))
    return result


def derive(records, **kwargs):
    args = dict(dataset_id="frozen", dataset_hash="a" * 64, source_series_id=1,
                source_seconds=60, target_seconds=300, start=START,
                end=START + timedelta(minutes=10), accepted_at=START + timedelta(days=2))
    return derive_frozen_candles(records, **{**args, **kwargs})


def test_aggregation_preserves_exact_slots_lineage_and_late_knowledge():
    rows = minute_records()
    rows[2] = replace(rows[2], revision=2, fact=replace(rows[2].fact,
        known_at=START + timedelta(hours=1)))
    source, facts = derive(rows)
    first = facts[0]
    assert first.payload["open"] == "100.0" and first.payload["close"] == "105.0"
    assert first.payload["high"] == "107.0" and first.payload["low"] == "98.0"
    assert first.payload["volume"] == "15.0" and first.payload["trade_count"] == 10
    assert first.known_at == START + timedelta(hours=1)
    assert facts[1].known_at == START + timedelta(minutes=10)
    assert first.provenance["inputs"][2]["revision"] == 2
    assert first.provenance["inputs"][2]["row_hash"] == rows[2].fact.row_hash
    assert source.source_kind == "canonical_candle_derivation"
    assert source.identity_key != SOURCE.identity_key
    replay_source, replay = derive(rows, accepted_at=START + timedelta(days=3))
    assert replay_source == source and replay[0].row_hash == first.row_hash
    assert derive(rows, dataset_hash="b" * 64)[0] != source


def test_unknown_additive_fields_remain_unknown():
    rows = minute_records()
    rows[0] = replace(rows[0], fact=replace(rows[0].fact, volume=None, trade_count=None))
    _, facts = derive(rows)
    assert facts[0].payload["volume"] is None and facts[0].payload["trade_count"] is None
    assert facts[1].payload["volume"] == "40.0"


@pytest.mark.parametrize("damage", ["missing", "duplicate", "mixed_source", "mixed_series", "offgrid"])
def test_rejects_incomplete_or_ambiguous_buckets(damage):
    rows = minute_records()
    if damage == "missing":
        rows.pop(3)
    elif damage == "duplicate":
        rows[3] = rows[2]
    elif damage == "mixed_source":
        rows[3] = replace(rows[3], source_identity_key="different")
    elif damage == "mixed_series":
        rows[3] = replace(rows[3], series_id=2)
    else:
        rows[3] = replace(rows[3], fact=replace(rows[3].fact,
            open_time=rows[3].fact.open_time + timedelta(seconds=1)))
    with pytest.raises(ValueError, match="candle_derivation_"):
        derive(rows)


@pytest.mark.parametrize("kwargs", [{"target_seconds": 60}, {"target_seconds": 90},
    {"start": START + timedelta(minutes=1)}, {"end": START + timedelta(minutes=11)},
    {"end": START + timedelta(days=500)}, {"start": START.replace(tzinfo=None)}])
def test_rejects_noncoarsening_unaligned_or_unbounded_requests(kwargs):
    with pytest.raises(ValueError, match="candle_derivation_"):
        derive(minute_records(), **kwargs)


@pytest.mark.parametrize("corrupt", [False, True])
def test_service_verifies_frozen_manifest_before_any_write(corrupt):
    from types import SimpleNamespace
    from market_data.contracts import build_candle_material_hash, build_provenance_hash, build_quality_hash
    from market_data.store import FrozenDataset, IngestionOutcome
    from portal.backend.service.market.candle_derivation_service import derive_candles

    records = minute_records()
    identity = dict(identity_key="fixture-series", instrument_id="fixture", fact_type="candle.ohlcv",
                    timeframe_seconds=60, contract_version="candle.ohlcv.v1")
    entry = {**identity, "series_id": 1, "range_start": START,
        "range_end": START + timedelta(minutes=10), "max_commit_seq": 10, "row_count": 10,
        "material_hash": build_candle_material_hash(series_identity=identity, records=records),
        "provenance_hash": build_provenance_hash(records), "quality_hash": build_quality_hash([]),
        "quality_evidence": [], "source_summary": {}}
    if corrupt:
        entry["provenance_hash"] = "0" * 64
    dataset = FrozenDataset(dataset_id="frozen", dataset_hash="a" * 64, max_commit_seq=10, series=(entry,))
    writes = []
    def register_source(*args, **kwargs):
        writes.append("source")
        return 2
    def ingest(**kwargs):
        writes.append(kwargs)
        return IngestionOutcome("fixture", 2, 2, 0, 0, 12)
    store = SimpleNamespace(get_dataset=lambda _: dataset, read_dataset_series=lambda **kwargs: records,
        register_source=register_source, register_series=lambda **kwargs: 2, ingest_facts=ingest)
    args = dict(store=store, dataset_id="frozen", source_series_id=1, start=START.isoformat(),
                end=(START + timedelta(minutes=10)).isoformat(), target_seconds=300)
    if corrupt:
        with pytest.raises(RuntimeError, match="hash_disagreement"):
            derive_candles(**args)
        assert writes == []
    else:
        receipt = derive_candles(**args)
        assert receipt["outcome"]["inserted_count"] == 2
        assert writes[-1]["require_same_source"] is True
        assert writes[-1]["allow_corrections"] is False

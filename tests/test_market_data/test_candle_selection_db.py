"""Header projections must select the same evidence as canonical payload reads."""
from collections import Counter
from dataclasses import replace
from datetime import timedelta

import pytest

from core.execution_control import (
    ExecutionBudgetExceededError, ExecutionControl, controlled_execution,
)
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, SourceIdentity
from portal.backend.service.market.frozen_dataset_service import (
    candle_selection_times, prepare_frozen_dataset_from_requirements,
)
from tests.test_market_data.test_repository_db import _BASE, _fact
from tests.test_market_data.test_fact_storage_tiers_db import (
    storage, _verified_cold_fixture,
)

pytestmark = pytest.mark.db


def _candles(storage):
    storage.series_id = storage.repo.register_series(
        instrument_id="storage-fixture", fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=3600, contract_version=CANDLE_FACT_VERSION,
    )
    storage.repo.ingest_candles(
        series_id=storage.series_id, source_id=storage.source_id,
        facts=[_fact(0), _fact(1), _fact(3)], source_revision="fixture.v1",
    )
    return dict(series_id=storage.series_id, start=_BASE, end=_BASE + timedelta(hours=4))


def _assert_projection(repo, args):
    records = repo.read_series_records(**args)
    selection = repo.inspect_candle_selection(**args)
    assert selection["row_count"] == len(records)
    assert selection["source_summary"]["counts"] == dict(Counter(
        record.source_identity_key for record in records
    ))
    assert set(candle_selection_times(selection)) == {record.fact.open_time for record in records}
    assert selection["first_observation_at"] == min(
        (record.fact.open_time for record in records), default=None,
    )
    return selection


def test_candle_projection_preserves_watermarks_sources_invalidations_and_duplicates(storage):
    args = _candles(storage)
    repo = storage.repo
    initial_watermark = repo.current_commit_seq()
    original = repo.read_facts(**args)
    second_source = SourceIdentity(provider="SECOND", venue="ISOLATED", source_kind="fixture",
                                   adapter_version="projection.v1")
    second_id = repo.register_source(second_source)
    # Source filtering must precede latest-revision selection.
    repo.ingest_facts(series_id=storage.series_id, source_id=second_id, facts=[replace(
        original[0].fact, source=second_source,
        payload={**original[0].fact.payload, "close": "101.5"},
        known_at=_BASE + timedelta(days=1), accepted_at=_BASE + timedelta(days=1),
    )])
    repo.ingest_facts(series_id=storage.series_id, source_id=storage.source_id, facts=[replace(
        original[1].fact, state="invalidated",
        known_at=_BASE + timedelta(days=1), accepted_at=_BASE + timedelta(days=1),
    ), replace(original[0].fact, observation_key="duplicate-timestamp")])
    source_a = original[0].source_identity_key
    for selection in (
        {}, {"as_of_commit_seq": initial_watermark}, {"known_at_lte": _BASE + timedelta(hours=4)},
        {"source_identity_keys": (source_a,)}, {"source_identity_keys": (second_source.identity_key,)},
        {"source_identity_keys": ("absent-source",)},
    ):
        _assert_projection(repo, {**args, **selection})
    current = repo.inspect_candle_selection(**args)
    assert current["row_count"] == 3
    assert set(candle_selection_times(current)) == {_BASE, _BASE + timedelta(hours=3)}
    pinned = repo.inspect_candle_selection(**args, as_of_commit_seq=initial_watermark)
    assert pinned["row_count"] == 3
    assert _BASE + timedelta(hours=1) in set(candle_selection_times(pinned))


def test_candle_projection_matches_cold_selection_but_does_not_certify_archive(storage, tmp_path, monkeypatch):
    args = _candles(storage)
    hot = _assert_projection(storage.repo, args)
    archive_path = _verified_cold_fixture(storage, tmp_path, monkeypatch)
    assert _assert_projection(storage.repo, args) == hot
    archive_path.write_bytes(b"corrupt disposable fixture")
    # Planning describes selected headers. It must not certify payload integrity.
    assert storage.repo.inspect_candle_selection(**args) == hot
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        storage.repo.read_series_records(**args)
    from market_data.contracts import DatasetSeriesRequest
    with pytest.raises(RuntimeError, match="checksum_mismatch"):
        storage.repo.freeze_dataset([DatasetSeriesRequest(**args)])


def test_candle_preparation_preserves_frozen_identity_and_binding(storage):
    _candles(storage)
    requirement = {
        "alias": "primary_bars", "instrument_id": "storage-fixture",
        "fact_type": CANDLE_FACT_TYPE, "contract_version": CANDLE_FACT_VERSION,
        "timeframe_seconds": 3600, "dimensions": {},
        "required_start": (_BASE + timedelta(minutes=5)).isoformat(),
        "required_end": (_BASE + timedelta(hours=4, minutes=5)).isoformat(),
        "source_policy": {"mode": "exact"},
    }
    class RecordReader:
        inspect_candle_selection = None

        def __getattr__(self, name):
            return getattr(storage.repo, name)

    kwargs = dict(requirements=[requirement], freeze=True,
                  instrument_loader=lambda identifier: {"id": identifier})
    # Both paths must reject the fixture's undisclosed gaps during binding.
    for reader in (RecordReader(), storage.repo):
        with pytest.raises(RuntimeError, match="backtest_dataset_unacceptable_gap"):
            prepare_frozen_dataset_from_requirements(store=reader, **kwargs)
    storage.repo.ingest_candles(
        series_id=storage.series_id, source_id=storage.source_id,
        facts=[_fact(2), _fact(4)], source_revision="fixture.v1",
    )
    old = prepare_frozen_dataset_from_requirements(store=RecordReader(), **kwargs)
    new = prepare_frozen_dataset_from_requirements(store=storage.repo, **kwargs)
    assert new["status"] == old["status"] == "frozen"
    assert new["resolved_requirements"] == old["resolved_requirements"]
    assert new["dataset"]["dataset_id"] == old["dataset"]["dataset_id"]
    assert new["dataset"]["dataset_hash"] == old["dataset"]["dataset_hash"]
    assert new["binding"] == old["binding"]


def test_candle_projection_enforces_returned_byte_budget(storage):
    args = _candles(storage)
    control = ExecutionControl()
    control.limit(seconds=30, input_bytes=1)
    with pytest.raises(ExecutionBudgetExceededError, match="resource=input_bytes"):
        with controlled_execution(control):
            storage.repo.inspect_candle_selection(**args)
    # Budget interruption must release the connection and leave facts intact.
    assert len(storage.repo.read_series_records(**args)) == 3

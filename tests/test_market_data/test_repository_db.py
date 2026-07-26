from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid

import pytest

from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    CandleFact,
    DatasetSeriesRequest,
    SourceIdentity,
)
from portal.backend.db import InstrumentRecord, db
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

    source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="TEST",
            venue="ISOLATED",
            source_kind="fixture",
            adapter_version=f"dataset-db-test.{token}",
        ),
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

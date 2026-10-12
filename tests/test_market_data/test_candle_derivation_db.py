"""Disposable canonical derivation, freeze, correction and source-collision proof."""
from dataclasses import replace
from datetime import timedelta
import uuid

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, DatasetSeriesRequest, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market.candle_derivation_service import derive_candles
from portal.backend.service.storage.repos.market_data import market_data_repo
from tests.test_market_data.test_candle_derivation import START, minute_records

pytestmark = pytest.mark.db


def test_frozen_derivation_replays_and_rejects_foreign_source_atomically():
    token = uuid.uuid4().hex
    instrument_id = f"derive-{token[:24]}"
    with db.session() as session:
        session.add(InstrumentRecord(id=instrument_id, datasource="TEST", exchange="ISOLATED",
            symbol=f"DERIVE-{token[:8]}", instrument_type="spot", can_short=False,
            short_requires_borrow=False, has_funding=False, extra_metadata={"fixture": token}))
    records = minute_records()
    source_id = market_data_repo.register_source(records[0].source)
    series_id = market_data_repo.register_series(instrument_id=instrument_id,
        fact_type=CANDLE_FACT_TYPE, timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION)
    market_data_repo.ingest_candles(series_id=series_id, source_id=source_id,
                                  facts=[row.fact for row in records])
    end = START + timedelta(minutes=10)
    frozen = market_data_repo.freeze_dataset(
        requests=[DatasetSeriesRequest(series_id=series_id, start=START, end=end)])
    args = dict(store=market_data_repo, dataset_id=frozen.dataset_id, source_series_id=series_id,
                start=START.isoformat(), end=end.isoformat(), target_seconds=300)
    first = derive_candles(**args)
    assert first["outcome"]["inserted_count"] == 2 and first["provider_call_performed"] is False
    output = market_data_repo.read_candles(series_id=first["series_id"], start=START, end=end)
    assert output[0].fact.close == 105 and output[0].provenance["dataset_hash"] == frozen.dataset_hash
    output_frozen = market_data_repo.freeze_dataset(requests=[
        DatasetSeriesRequest(series_id=first["series_id"], start=START, end=end)])
    # Mutable source revisions cannot change the pinned derivation.
    market_data_repo.ingest_candles(series_id=series_id, source_id=source_id,
        facts=[replace(records[4].fact, close=106)])
    replay = derive_candles(**args)
    assert replay["outcome"]["noop_count"] == 2 and replay["source_identity_key"] == first["source_identity_key"]
    changed = market_data_repo.freeze_dataset(requests=[DatasetSeriesRequest(series_id=series_id, start=START, end=end)])
    with pytest.raises(RuntimeError, match="market_data_source_conflict"):
        derive_candles(**{**args, "dataset_id": changed.dataset_id})
    assert market_data_repo.get_dataset(output_frozen.dataset_id).dataset_hash == output_frozen.dataset_hash
    # An identical row hash from another producer must fail before the no-op branch.
    canonical = market_data_repo.read_facts(series_id=first["series_id"], start=START, end=end)
    foreign = SourceIdentity(provider="TEST", venue="ISOLATED", source_kind="fixture",
                             adapter_version=f"foreign.{token}")
    foreign_id = market_data_repo.register_source(foreign)
    collision = replace(canonical[1].fact, source=foreign)
    assert collision.row_hash == canonical[1].fact.row_hash
    earlier = replace(canonical[0].fact, source=foreign,
        observation_key=(START - timedelta(minutes=5)).isoformat(),
        observation_time=START - timedelta(minutes=5),
        payload={**canonical[0].fact.payload, "close_time": START})
    with pytest.raises(RuntimeError, match="market_data_source_conflict"):
        market_data_repo.ingest_facts(series_id=first["series_id"], source_id=foreign_id,
            facts=[earlier, collision], allow_corrections=False, require_same_source=True)
    assert len(market_data_repo.read_facts(series_id=first["series_id"],
        start=START - timedelta(minutes=5), end=end)) == 2

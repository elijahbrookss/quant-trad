"""Disposable native acquisition -> fresh coverage -> frozen binding, without a Check."""
from datetime import UTC, datetime, timedelta
import uuid

import pandas as pd
import pytest

from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market import feed_service
from portal.backend.service.research import service
from portal.backend.service.research.raw_candle_evaluator import OUTCOME_SCHEMA
from portal.backend.service.storage.repos.market_data import market_data_repo

pytestmark = pytest.mark.db


def preparation_request(instrument_id, start):
    return {"check_family": "raw_forward_outcome", "scope": {"instrument_id": instrument_id,
        "timeframe": "1m", "start": (start + timedelta(minutes=1)).isoformat(),
        "end": (start + timedelta(minutes=18)).isoformat()},
        "detector": {"all": [{"field": "close", "operator": "gt", "value_field": "open"},
                              {"field": "previous_close", "operator": "gte", "value": 0}]},
        "outcomes": {"schema_version": OUTCOME_SCHEMA, "forward_bars": [5], "entry_lag_bars": 1,
                     "min_sample_count": 1}, "gap_policy": "continue_degraded"}


def test_explicit_prepare_acquires_native_input_and_freezes_without_execution(monkeypatch):
    token = uuid.uuid4().hex
    instrument_id = f"prep-native-{token[:20]}"
    start = datetime(2024, 1, 1, tzinfo=UTC)
    with db.session() as session:
        session.add(InstrumentRecord(id=instrument_id, datasource="COINBASE", exchange="COINBASE_DIRECT",
            symbol=f"PREP-{token[:8]}", instrument_type="spot", can_short=False,
            short_requires_borrow=False, has_funding=False, extra_metadata={"fixture": token}))
    calls = []
    class Provider:
        @classmethod
        def native_candle_interval(cls, seconds):
            assert seconds == 60
            return "1m"
        def fetch_from_api(self, symbol, begin, end, interval):
            calls.append((symbol, begin, end, interval))
            return pd.DataFrame([{"timestamp": start + timedelta(minutes=index),
                "open": 100 + index, "high": 102 + index, "low": 99 + index,
                "close": 101 + index, "volume": 10} for index in range(18)])
    monkeypatch.setattr(feed_service, "get_provider", lambda *a, **k: Provider())
    monkeypatch.setattr(feed_service, "get_provider_class", lambda *a, **k: Provider)
    monkeypatch.setattr(service, "run_research_check", lambda *a, **k:
        (_ for _ in ()).throw(AssertionError("preparation executed a Check")))
    payload = preparation_request(instrument_id, start)
    inspected = service.prepare_research_check_evidence(payload)
    assert inspected["status"] == "acquisition_required" and not calls
    assert market_data_repo.list_series_metadata(instrument_id=instrument_id) == []
    payload["preparation"] = {"acquire_missing": True, "freeze": True}
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen", prepared
    assert len(calls) == 1 and calls[0][1:] == (start, start + timedelta(minutes=18), "1m")
    assert prepared["check_executed"] is prepared["observation_created"] is False
    receipt = prepared["acquisition"]["receipts"][0]
    assert receipt["outcome"]["inserted_count"] == 18
    dataset_id = prepared["dataset"]["dataset_id"]
    records = market_data_repo.read_dataset_series(dataset_id=dataset_id, series_id=receipt["series_id"])
    assert len(records) == 18 and all(row.revision == 1 for row in records)
    assert {row.source_identity_key for row in records} == {receipt["source_identity_key"]}
    assert all(row.source.source_kind == "historical_api" for row in records)
    assert all(row.fact.known_at == row.fact.close_time for row in records)
    assert prepared["binding"]["provider_access"] == "disabled"
    assert prepared["next_request"]["dataset_id"] == dataset_id

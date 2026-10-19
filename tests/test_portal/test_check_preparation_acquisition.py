from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core.execution_control import ExecutionControl, ExecutionBudgetExceededError, controlled_execution
from data_providers.registry import FeatureAuth
from market_data.contracts import SourceIdentity
from market_data.store import IngestionOutcome
from portal.backend.service.research import preparation_acquisition as acquisition, service, catalog
from tests.test_portal.test_research_check_catalog import client
from tests.test_portal.test_matched_origin_check import _plan


SOURCE = SourceIdentity("COINBASE", "COINBASE_DIRECT", "historical_api", "native.adapter.v1")


def requirement(alias="primary_bars", **changes):
    return {"alias": alias, "instrument_id": "instrument", "fact_type": "candle.ohlcv",
        "contract_version": "candle.ohlcv.v1", "dimensions": {}, "timeframe_seconds": 900,
        "source_policy": {"mode": "exact"},
        "required_start": "2025-01-01T00:00:00+00:00", "required_end": "2025-01-01T01:00:00+00:00", **changes}


@pytest.fixture
def native(monkeypatch):
    calls = []
    monkeypatch.setattr(acquisition.instrument_service, "get_instrument_record", lambda value:
        {"id": value, "datasource": "COINBASE", "exchange": "COINBASE_DIRECT", "symbol": "ETP"})
    monkeypatch.setattr(acquisition, "feature_contract", lambda *args: SimpleNamespace(auth=FeatureAuth.PUBLIC))
    monkeypatch.setattr(acquisition.historical_candle_ingestor, "native_request", lambda *args, **kwargs: ("15m", SOURCE))
    def ingest(instrument, **kwargs):
        calls.append((instrument, kwargs))
        return SimpleNamespace(source_id=2, series_id=3, gap_evidence_count=0,
            outcome=IngestionOutcome(ingestion_run_id="run", requested_count=4, inserted_count=4,
                corrected_count=0, noop_count=0, max_commit_seq=4))
    monkeypatch.setattr(acquisition.historical_candle_ingestor, "ingest_by_instrument", ingest)
    return calls


def test_alias_ranges_merge_without_duplicate_fetch_or_scope_expansion(native):
    result = acquisition.acquire_check_inputs(requirements=[requirement(), requirement("indicator")],
        missing_coverage=[{"alias": "primary_bars", "reason": "series_not_registered"},
                          {"alias": "indicator", "reason": "series_not_registered"}])
    assert result["status"] == "acquired" and len(native) == 1
    assert native[0][1] == {"start": requirement()["required_start"], "end": requirement()["required_end"], "interval": "15m"}
    assert result["receipts"][0]["aliases"] == ["indicator", "primary_bars"]
    assert result["receipts"][0]["outcome"]["inserted_count"] == 4


@pytest.mark.parametrize("changed,reason", [
    ({"fact_type": "market.trade"}, "owning Fact"),
    ({"source_policy": {"mode": "exact", "source_identity_key": "other"}}, "declared source"),
    ({"required_start": "2025-01-01T00:01:00Z"}, "grid aligned"),
    ({"required_end": "2099-01-01T01:00:00Z"}, "unclosed"),
])
def test_preflight_refuses_all_effects_if_any_alias_is_unsupported(native, changed, reason):
    result = acquisition.acquire_check_inputs(requirements=[requirement(), requirement("bad", **changed)],
        missing_coverage=[{"alias": key, "reason": "series_not_registered"} for key in ("primary_bars", "bad")])
    assert result["status"] == "acquisition_blocked" and not native
    assert reason in str(result["blockers"])


def test_no_authority_for_credential_provider(native, monkeypatch):
    monkeypatch.setattr(acquisition, "feature_contract", lambda *args: SimpleNamespace(auth=FeatureAuth.CREDENTIALS))
    result = acquisition.acquire_check_inputs(requirements=[requirement()],
        missing_coverage=[{"alias": "primary_bars", "reason": "series_not_registered"}])
    assert result["status"] == "acquisition_blocked" and not native


def test_native_interval_refusal_precedes_fetch(native, monkeypatch):
    monkeypatch.setattr(acquisition.historical_candle_ingestor, "native_request",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("unsupported native timeframe")))
    result = acquisition.acquire_check_inputs(requirements=[requirement()],
        missing_coverage=[{"alias": "primary_bars", "reason": "series_not_registered"}])
    assert result["status"] == "acquisition_blocked" and not native


def test_input_cost_budget_refuses_before_provider_call(native):
    control = ExecutionControl()
    control.limit(seconds=30, input_rows=3)
    with controlled_execution(control, check_on_exit=False):
        with pytest.raises(ExecutionBudgetExceededError, match="input_rows"):
            acquisition.acquire_check_inputs(requirements=[requirement()],
                missing_coverage=[{"alias": "primary_bars", "reason": "series_not_registered"}])
    assert not native


def test_disjoint_ranges_keep_partial_receipt_and_do_not_retry(native, monkeypatch):
    successful = acquisition.historical_candle_ingestor.ingest_by_instrument
    def ingest(*args, **kwargs):
        if native:
            raise RuntimeError("provider failed; durable gap evidence recorded")
        return successful(*args, **kwargs)
    monkeypatch.setattr(acquisition.historical_candle_ingestor, "ingest_by_instrument", ingest)
    result = acquisition.acquire_check_inputs(requirements=[requirement()], missing_coverage=[
        {"alias": "primary_bars", "reason": "source_bound_interval_unrecorded", "start": "2025-01-01T00:00:00Z", "end": "2025-01-01T00:15:00Z"},
        {"alias": "primary_bars", "reason": "source_bound_interval_unrecorded", "start": "2025-01-01T00:45:00Z", "end": "2025-01-01T01:00:00Z"}])
    assert result["status"] == "acquisition_failed" and len(native) == len(result["receipts"]) == 1
    assert result["provider_call_performed"] is True
    assert result["blockers"][0]["partial_effects_possible"]


@pytest.fixture
def preparation(monkeypatch):
    request = deepcopy(catalog.get_check_definition("event_fact_analysis", "11")["examples"][0]["request"])
    calls = []
    incomplete = replace(_plan(), plan_hash="", missing_coverage=({"alias": "primary_bars", "reason": "series_not_registered"},))
    plans = [incomplete, _plan()]
    def plan(*args, **kwargs):
        calls.append("plan")
        return plans.pop(0)
    monkeypatch.setattr(service, "plan_research_check", plan)
    def acquire(**kwargs):
        calls.append("acquire")
        return {"status": "acquired", "receipts": [{"source_id": 9}], "blockers": [],
                "provider_call_performed": True, "acquisition_performed": True}
    monkeypatch.setattr(service, "acquire_check_inputs", acquire)
    def freeze(**kwargs):
        calls.append(("freeze", kwargs["freeze"]))
        return {"status": "ready_to_freeze", "dataset": None, "unresolved_requirements": []}
    monkeypatch.setattr(service, "prepare_frozen_dataset_from_requirements", freeze)
    return request, calls, plans


def test_inspect_default_is_provider_free_and_reports_missing(preparation):
    request, calls, _ = preparation
    response = client().post("/api/research/checks/prepare", json=request)
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "acquisition_required" and calls == ["plan"]
    assert result["check_executed"] is result["provider_call_performed"] is False
    assert result["missing_coverage"]


@pytest.mark.parametrize("freeze", [False, True])
def test_explicit_acquisition_rechecks_before_optional_freeze(preparation, freeze):
    request, calls, _ = preparation
    request["preparation"] = {"acquire_missing": True, "freeze": freeze}
    response = client().post("/api/research/checks/prepare", json=request)
    assert response.status_code == 200
    assert calls == ["plan", "acquire", "plan", ("freeze", freeze)]
    result = response.json()
    assert result["acquisition"]["receipts"] == [{"source_id": 9}]
    assert result["provider_call_performed"] is True and result["check_executed"] is False


def test_receipt_is_not_coverage_proof(preparation):
    request, calls, plans = preparation
    plans[1] = plans[0]
    request["acquire_missing"] = True
    response = client().post("/api/research/checks/prepare", json=request)
    assert response.json()["status"] == "acquisition_required"
    assert calls == ["plan", "acquire", "plan"]


def test_partial_failure_is_not_frozen_or_retried(preparation, monkeypatch):
    request, calls, _ = preparation
    request["acquire_missing"] = True
    monkeypatch.setattr(service, "acquire_check_inputs", lambda **kwargs:
        {"status": "acquisition_failed", "receipts": [{"source_id": 1}], "blockers": [{"reason": "provider failure"}],
         "provider_call_performed": None, "acquisition_performed": True})
    result = client().post("/api/research/checks/prepare", json=request).json()
    assert result["status"] == "acquisition_failed" and result["dataset"] is None
    assert len(result["acquisition"]["receipts"]) == 1 and calls == ["plan"]


@pytest.mark.parametrize("value", ["false", "true", 1, [], {}])
def test_mutation_flags_require_literal_booleans(preparation, value):
    request, calls, _ = preparation
    request["preparation"] = {"acquire_missing": value}
    response = client().post("/api/research/checks/prepare", json=request)
    assert response.status_code == 400 and calls == []


def test_first_provider_failure_exposes_unknown_effects_and_retains_durable_gap(native, monkeypatch):
    from portal.backend.service.market import feed_service
    from tests.test_market_data.test_feed_service import FakeStore
    class Provider:
        def native_candle_interval(self, seconds):
            assert seconds == 900
            return "15m"
        def fetch_from_api(self, *args):
            raise RuntimeError("public provider offline")
    store = FakeStore()
    ingestor = feed_service.HistoricalCandleIngestor(store)
    monkeypatch.setattr(feed_service, "get_provider", lambda *a, **k: Provider())
    monkeypatch.setattr(acquisition, "historical_candle_ingestor", ingestor)
    result = acquisition.acquire_check_inputs(requirements=[requirement()],
        missing_coverage=[{"alias": "primary_bars", "reason": "series_not_registered"}])
    assert result["status"] == "acquisition_failed" and result["receipts"] == []
    assert result["acquisition_performed"] is result["provider_call_performed"] is None
    assert result["blockers"][0]["partial_effects_possible"]
    assert len(store.gaps) == 1 and store.gaps[0]["classification"] == "ingestion_failure"
    assert not store.ingest_calls


def test_new_http_root_flags_are_not_coerced(preparation):
    request, calls, _ = preparation
    request["acquire_missing"] = "true"
    assert client().post("/api/research/checks/prepare", json=request).status_code == 422
    assert calls == []


def test_post_acquisition_coverage_error_retains_receipts_and_does_not_freeze(preparation, monkeypatch):
    request, calls, plans = preparation
    initial = plans[0]
    def plan(*args, **kwargs):
        calls.append("plan")
        if len(calls) > 1:
            raise ValueError("source coverage verification failed")
        return initial
    monkeypatch.setattr(service, "plan_research_check", plan)
    request["preparation"] = {"acquire_missing": True, "freeze": True}
    result = client().post("/api/research/checks/prepare", json=request).json()
    assert result["status"] == "acquisition_failed"
    assert result["acquisition"]["receipts"] == [{"source_id": 9}]
    assert result["acquisition"]["blockers"][0]["stage"] == "coverage_verification"
    assert result["dataset"] is None and calls == ["plan", "acquire", "plan"]

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, DatasetSeriesRequest, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.indicators.indicator_service.api import create_instance
from portal.backend.service.research import service
from portal.backend.service.storage.repos.market_data import market_data_repo

pytestmark = pytest.mark.db


@pytest.mark.parametrize("forward_risk", [False, True])
def test_candle_only_check_uses_real_indicator_freeze_and_replay(monkeypatch, forward_risk):
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    token = uuid.uuid4().hex
    instrument_id = f"candle-check-{token[:20]}"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    with db.session() as session:
        session.add(InstrumentRecord(
            id=instrument_id, datasource="TEST", exchange="ISOLATED",
            symbol=f"CANDLE-{token[:8]}", instrument_type="spot",
            can_short=False, short_requires_borrow=False, has_funding=False,
            extra_metadata={"fixture": "candle-only-check"},
        ))
    source = SourceIdentity(provider="TEST", venue="ISOLATED", source_kind="historical",
                            adapter_version=f"candle-check.{token}")
    source_id = market_data_repo.register_source(source, lineage={"fixture": token})
    series = market_data_repo.register_series(
        instrument_id=instrument_id, fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION,
    )
    candles = []
    for index in range(800 if forward_risk else 400):
        if forward_risk and index == 300:
            continue
        opened = start + timedelta(minutes=index)
        closed = opened + timedelta(minutes=1)
        width = 20 if index in (250, 550) else 1
        candles.append(CandleFact(
            open_time=opened, close_time=closed, open=100, high=100 + width,
            low=100 - width, close=100, volume=10, trade_count=None,
            source_published_at=None, received_at=None,
            accepted_at=start + timedelta(days=1), known_at=closed,
            known_at_method="interval_close_inferred",
        ))
    market_data_repo.ingest_candles(series_id=series, source_id=source_id,
                                   facts=candles, request={"fixture": token})
    if forward_risk:
        market_data_repo.record_gap_evidence(series_id=series, source_id=source_id,
            start=start+timedelta(minutes=300), end=start+timedelta(minutes=301),
            classification="provider_missing_data", expected_count=1, observed_count=0,
            evidence={"schema_version":"market_gap_evidence.v1", "reason_code":"fixture_gap"})
    indicator = create_instance("candle_stats", f"Candle-only {token}", {})

    class ProviderCallTrap:
        def __init__(self, **kwargs):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"frozen Check attempted provider access: {name}")

    monkeypatch.setattr(runtime_market_data, "MarketDataCollectorService", ProviderCallTrap)
    payload = {
        "check_family": "event_fact_analysis",
        "scope": {"instrument_id": instrument_id, "indicator_id": indicator["id"],
                  "timeframe": "1m", "start": (start + timedelta(minutes=220)).isoformat(),
                  "end": (start + timedelta(minutes=290)).isoformat()},
        "detector": {"type": "indicator_event", "output_name": "atr_expansion",
                     "event_keys": [{"key": "atr_expansion_long", "direction": "long"}]},
        "outcomes": {"horizons": [1, 3], "primary_horizon": 1},
        "statistics": {"features": {"baseline": [], "enriched": []},
                       "eligibility": {"min_samples": 1}},
        "inputs": [], "gap_policy": "reject",
        "preparation": {"freeze": True, "name": f"candle-only-{token}"},
    }
    if forward_risk:
        payload["scope"]["end"] = (start+timedelta(minutes=790)).isoformat()
        payload["outcomes"] = {"horizon_kind":"elapsed_time", "horizons":[1800,7200,21600], "primary_horizon":7200, "entry_lag_bars":0,
            "forward_risk":{"schema_version":"candle_risk_comparison.v1", "baseline_bars":120, "readiness_contract":"candle_stats.public_outputs.v1", "outcome_boundary":"evaluation_end_exclusive"}}
        payload.update(statistics={}, gap_policy="reset_rewarm")
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen", prepared
    run = service.run_research_check(prepared["next_request"])
    assert run["replayable"] is True
    assert run["evidence"]["input_binding"]["provider_access"] == "disabled"
    evaluated = run["result"]["result"]
    if forward_risk:
        assert evaluated["schema_version"] == "event_fact_analysis_result.v9"
        analysis = evaluated["forward_risk_comparison"]
        assert analysis["coverage"]["first_blocking_stage_counts"]["missing_source_candle"] == 1
        assert analysis["coverage"]["first_blocking_stage_counts"]["indicator_not_ready"] == 200
        assert analysis["horizons"]["1800"]["cohorts"]["shock_crossing"]["candidate_count"] >= 2
        assert analysis["horizons"]["1800"]["cohorts"]["shock_crossing"]["raw_risk"]["count"] >= 2
    else:
        assert evaluated["schema_version"] == "event_fact_analysis_result.v5"
        assert evaluated["analysis_status"] == "completed"
        assert evaluated["sample_count"] > 0
        assert evaluated["descriptive_outcomes"]["population_count"] == evaluated["sample_count"]
        assert all(event["fact_references"] == {} for event in evaluated["events"])

    market_data_repo.ingest_candles(
        series_id=series, source_id=source_id,
        facts=[replace(candles[251], close=100.5)], request={"fixture": token, "correction": True},
    )
    replay = service.replay_research_check(run["check"]["id"])
    assert replay["status"] == "matched", replay
    assert replay["matches"] is True
    assert replay["provider_call_performed"] is False
    assert replay["original_result_hash"] == replay["replayed_result_hash"]
    assert replay["original_evidence_hash"] == replay["replayed_evidence_hash"]
    assert replay["original_plan_hash"] == replay["replayed_plan_hash"]


@pytest.mark.parametrize("matched_origin, derived", [(False, False), (True, False), ("shared_landmark", False), ("shared_landmark", True), ("first_return", False)])
def test_mixed_timeframe_profile_freezes_and_replays_with_delayed_entry(monkeypatch, matched_origin, derived):
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    token = uuid.uuid4().hex
    instrument_id = f"profile-check-{token[:18]}"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    with db.session() as session:
        session.add(InstrumentRecord(
            id=instrument_id, datasource="TEST", exchange="ISOLATED",
            symbol=f"CANDLE-{token[:8]}", instrument_type="spot",
            can_short=False, short_requires_borrow=False, has_funding=False,
            extra_metadata={"fixture": "candle-only-check"},
        ))
    source = SourceIdentity(provider="TEST", venue="ISOLATED", source_kind="historical",
                            adapter_version=f"candle-check.{token}")
    source_id = market_data_repo.register_source(source, lineage={"fixture": token})
    for minutes in ((1,) if derived else (5, 30)):
        series = market_data_repo.register_series(
            instrument_id=instrument_id, fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=minutes * 60, contract_version=CANDLE_FACT_VERSION,
        )
        candles = []
        for index in range(5 * 24 * 60 // minutes):
            opened = start + timedelta(minutes=index * minutes)
            closed = opened + timedelta(minutes=minutes)
            price = 150 if start + timedelta(days=2, hours=12) <= opened else 100
            if matched_origin == "first_return" and opened < start+timedelta(days=2,hours=12):
                price = 100 + (-2, 0, 2)[(index*minutes//30) % 3]
            if matched_origin == "first_return" and (
                start+timedelta(days=2,hours=11,minutes=55) <= opened < start+timedelta(days=2,hours=12)
                or start+timedelta(days=2,hours=12,minutes=5) <= opened < start+timedelta(days=2,hours=12,minutes=10)
            ):
                # Inside immediately before the 12:00 origin, then a strict
                # first return at its one-bar classification landmark.
                price = 100
            candles.append(CandleFact(
                open_time=opened, close_time=closed, open=price, high=price + 1,
                low=price - 1, close=price, volume=10 * minutes, trade_count=None,
                source_published_at=None, received_at=None,
                accepted_at=start + timedelta(days=6), known_at=closed,
                known_at_method="interval_close_inferred",
            ))
        market_data_repo.ingest_candles(series_id=series, source_id=source_id,
                                       facts=candles, request={"fixture": token})
    if derived:
        from portal.backend.service.market.candle_derivation_service import derive_candles
        source_dataset = market_data_repo.freeze_dataset(requests=[DatasetSeriesRequest(
            series_id=series, start=start, end=start + timedelta(days=5))])
        for target_seconds in (300, 1800):
            receipt = derive_candles(store=market_data_repo, dataset_id=source_dataset.dataset_id,
                source_series_id=series, start=start.isoformat(),
                end=(start + timedelta(days=5)).isoformat(), target_seconds=target_seconds)
            assert receipt["provider_call_performed"] is False
            assert receipt["outcome"]["inserted_count"] == 5 * 86400 // target_seconds
    indicator = create_instance("market_profile", f"Mixed frame {token}", {
        "bin_size": 1, "days_back": 3, "use_merged_value_areas": False,
    }, version="v2" if matched_origin == "first_return" else None)

    class ProviderCallTrap:
        def __init__(self, **kwargs):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"frozen Check attempted provider access: {name}")

    monkeypatch.setattr(runtime_market_data, "MarketDataCollectorService", ProviderCallTrap)
    payload = {
        "check_family": "event_fact_analysis",
        "scope": {"instrument_id": instrument_id, "indicator_id": indicator["id"],
                  "timeframe": "5m", "start": (start + timedelta(days=2)).isoformat(),
                  "end": (start + timedelta(days=3)).isoformat()},
        "detector": {"type": "indicator_event", "output_name": "balance_breakout",
                     "event_keys": [{"key": "balance_breakout_long", "direction": "long"}]},
        "outcomes": {"horizons": [6, 24, 72], "primary_horizon": 24, "entry_lag_bars": 1},
        "statistics": {"features": {"baseline": [], "enriched": []},
                       "eligibility": {"min_samples": 1}},
        "inputs": [], "gap_policy": "reject",
        "preparation": {"freeze": True, "name": f"candle-only-{token}"},
    }
    if matched_origin and matched_origin != "first_return":
        payload["outcomes"]["matched_origin"] = {
            "detector": {"type": "indicator_event", "output_name": "confirmed_balance_breakout",
                         "event_keys": [{"key": "confirmed_balance_breakout_long", "direction": "long"}]},
            "origin_time_path": "metadata.breakout_time",
            "origin_event_key_path": "metadata.breakout_event_key",
            "reference_path": "metadata.reference",
        }
    if matched_origin == "shared_landmark":
        payload["outcomes"]["shared_landmark"] = {
            "classification_lag_bars": 1, "sample_lag_bars": 2,
            "readiness_contract": "market_profile.value_location.v1",
            "dependence": "leave_one_original_profile_out.v1",
        }
    if matched_origin == "first_return":
        payload["outcomes"]["first_return"] = {
            "classification_lag_bars": 1, "sample_lag_bars": 2,
            "readiness_contract": "market_profile.first_return_state.v2",
            "dependence": "leave_one_original_profile_out.v1",
        }
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen", prepared
    run = service.run_research_check(prepared["next_request"])
    assert run["replayable"] is True
    assert run["evidence"]["input_binding"]["provider_access"] == "disabled"
    evaluated = run["result"]["result"]
    assert evaluated["schema_version"] == ("event_fact_analysis_result.v8" if matched_origin == "first_return" else "event_fact_analysis_result.v7" if matched_origin == "shared_landmark" else "event_fact_analysis_result.v6" if matched_origin else "event_fact_analysis_result.v5")
    assert evaluated["analysis_status"] in ({"completed", "insufficient_evidence"} if matched_origin in {"shared_landmark", "first_return"} else {"completed"})
    assert evaluated["sample_count"] > 0
    assert evaluated["descriptive_outcomes"]["population_count"] == evaluated["sample_count"]
    assert all(event["fact_references"] == {} for event in evaluated["events"])

    if matched_origin and matched_origin != "first_return":
        attribution = evaluated["matched_origin_attribution"]
        assert attribution["matched_count"] > 0
        assert attribution["horizons"]["24"]["common_pair_count"] > 0
        assert attribution["horizons"]["24"]["followup_population_reconciled"] is True
        observation = service.create_observation_from_check_evidence(
            run["check"]["id"], {"title": "Matched origin disposable proof"}
        )
        assert observation["observation"]["payload"]["check_id"] == run["check"]["id"]

    if matched_origin == "shared_landmark":
        shared = evaluated["shared_landmark_comparison"]
        assert shared["origin_count"] == evaluated["sample_count"]
        assert shared["classification_counts"].get("confirmed_by_landmark", 0) > 0
        assert shared["horizons"]["24"]["leave_one_profile_out"]["method"] == "leave_one_original_profile_out.v1"
    if matched_origin == "first_return":
        analysis = evaluated["first_return_comparison"]
        assert analysis["classification_counts"].get("returned_by_landmark", 0) > 0
        assert any(r["outcomes"]["24"]["status"] == "resolved" for r in analysis["origins"])
        assert indicator["version"] == "v2"
        assert indicator["manifest"]["version"] == "v2"
    replay = service.replay_research_check(run["check"]["id"])
    assert replay["status"] == "matched", replay
    assert replay["matches"] is True
    assert replay["provider_call_performed"] is False
    assert replay["original_result_hash"] == replay["replayed_result_hash"]
    assert replay["original_evidence_hash"] == replay["replayed_evidence_hash"]
    assert replay["original_plan_hash"] == replay["replayed_plan_hash"]

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, SourceIdentity
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.indicators.indicator_service.api import create_instance
from portal.backend.service.research import service
from portal.backend.service.storage.repos.market_data import market_data_repo

pytestmark = pytest.mark.db


def test_candle_only_check_uses_real_indicator_freeze_and_replay(monkeypatch):
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
    for index in range(400):
        opened = start + timedelta(minutes=index)
        closed = opened + timedelta(minutes=1)
        width = 20 if index == 250 else 1
        candles.append(CandleFact(
            open_time=opened, close_time=closed, open=100, high=100 + width,
            low=100 - width, close=100, volume=10, trade_count=None,
            source_published_at=None, received_at=None,
            accepted_at=start + timedelta(days=1), known_at=closed,
            known_at_method="interval_close_inferred",
        ))
    market_data_repo.ingest_candles(series_id=series, source_id=source_id,
                                   facts=candles, request={"fixture": token})
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
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen", prepared
    run = service.run_research_check(prepared["next_request"])
    assert run["replayable"] is True
    assert run["evidence"]["input_binding"]["provider_access"] == "disabled"
    evaluated = run["result"]["result"]
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


@pytest.mark.parametrize("matched_origin", [False, True, "shared_landmark"])
def test_mixed_timeframe_profile_freezes_and_replays_with_delayed_entry(monkeypatch, matched_origin):
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
    for minutes in (5, 30):
        series = market_data_repo.register_series(
            instrument_id=instrument_id, fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=minutes * 60, contract_version=CANDLE_FACT_VERSION,
        )
        candles = []
        for index in range(5 * 24 * 60 // minutes):
            opened = start + timedelta(minutes=index * minutes)
            closed = opened + timedelta(minutes=minutes)
            price = 150 if start + timedelta(days=2, hours=12) <= opened else 100
            candles.append(CandleFact(
                open_time=opened, close_time=closed, open=price, high=price + 1,
                low=price - 1, close=price, volume=10 * minutes, trade_count=None,
                source_published_at=None, received_at=None,
                accepted_at=start + timedelta(days=6), known_at=closed,
                known_at_method="interval_close_inferred",
            ))
        market_data_repo.ingest_candles(series_id=series, source_id=source_id,
                                       facts=candles, request={"fixture": token})
    indicator = create_instance("market_profile", f"Mixed frame {token}", {
        "bin_size": 1, "days_back": 3, "use_merged_value_areas": False,
    })

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
    if matched_origin:
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
    prepared = service.prepare_research_check_evidence(payload)
    assert prepared["status"] == "frozen", prepared
    run = service.run_research_check(prepared["next_request"])
    assert run["replayable"] is True
    assert run["evidence"]["input_binding"]["provider_access"] == "disabled"
    evaluated = run["result"]["result"]
    assert evaluated["schema_version"] == ("event_fact_analysis_result.v7" if matched_origin == "shared_landmark" else "event_fact_analysis_result.v6" if matched_origin else "event_fact_analysis_result.v5")
    assert evaluated["analysis_status"] in ({"completed", "insufficient_evidence"} if matched_origin == "shared_landmark" else {"completed"})
    assert evaluated["sample_count"] > 0
    assert evaluated["descriptive_outcomes"]["population_count"] == evaluated["sample_count"]
    assert all(event["fact_references"] == {} for event in evaluated["events"])

    if matched_origin:
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
    replay = service.replay_research_check(run["check"]["id"])
    assert replay["status"] == "matched", replay
    assert replay["matches"] is True
    assert replay["provider_call_performed"] is False
    assert replay["original_result_hash"] == replay["replayed_result_hash"]
    assert replay["original_evidence_hash"] == replay["replayed_evidence_hash"]
    assert replay["original_plan_hash"] == replay["replayed_plan_hash"]

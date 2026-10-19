"""Synthetic method/provenance qualification; no actual studies or acquisition."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import math
import pandas as pd
import pytest
from market_data.contracts import NumericFact, NumericFactRecord, SourceIdentity
from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research import catalog, controlled_reversal_evaluator as cr
from portal.backend.service.research.event_fact_evaluator import EventFactEvaluator
from portal.backend.service.research.event_facts import _causal_fact_records
from portal.backend.service.research.retrospective_numeric import retrospective_snapshots
from portal.backend.service.research.registry import (normalize_check_request,
    normalize_check_preparation_request, CHECK_REGISTRY, pin_check_definition_to_plan,
    reconstruct_pinned_check_definition)
from tests.test_portal.test_controlled_reversal_evidence import supported_pairs

BASE = datetime(2022, 1, 1, tzinfo=UTC)

def iso(t):
    return t.isoformat().replace("+00:00", "Z")

def comparison_payload(interval="15m"):
    p = deepcopy(catalog._EXAMPLES["raw_forward_outcome@19"])
    seconds = cr.INTERVALS[interval]
    p["scope"].update(timeframe=interval, warmup_bars=86400//seconds+1)
    p["outcomes"].update(interval=interval, trend_lookback_bars=10800//seconds,
        volatility_lookback_bars=86400//seconds, outcome_horizon_bars=10800//seconds)
    return p

def plan_for(p, request, requirements=()):
    step = cr.INTERVALS.get(p["scope"]["timeframe"], 3600)
    return ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION, request_hash=request.request_hash,
        market_data_requirements=requirements, indicator_graph=(),
        evaluation_range={"start": p["scope"]["start"], "end_exclusive": p["scope"]["end"]},
        materialization_range={"start": iso(BASE-timedelta(days=2)), "end_exclusive": p["scope"]["end"]},
        warmup={"timeframe_seconds": step, "bars": p["scope"].get("warmup_bars", 0)},
        outcome_tail={"bars": 0, "seconds": 0}, gap_policy=p["gap_policy"])

@pytest.mark.parametrize("interval,history,horizon", [("15m",97,12),("30m",49,6),("1h",25,3)])
def test_configuration_only_intervals_and_exact_replay(interval, history, horizon):
    p=comparison_payload(interval); d,q=normalize_check_request(p)
    ev=CHECK_REGISTRY.resolve_evaluator(d)
    decl=ev.declare_requirements(definition=d,request=q)
    assert ev.version == "19" and decl["feature_lookback_bars"] == history
    assert decl["outcome_horizons"] == [horizon]
    assert cr.INTERVALS[interval]*horizon == 10800
    normalize_check_preparation_request(p)
    plan=plan_for(p,q,({"alias":"primary_bars","timeframe_seconds":cr.INTERVALS[interval]},))
    d,q,plan=pin_check_definition_to_plan(d,q,plan)
    assert reconstruct_pinned_check_definition(q,plan).to_dict() == d.to_dict()

@pytest.mark.parametrize("key,value", [("interval","5m"),("trend_lookback_bars",97),
    ("entry_lag_bars",True),("comparison_window_days",32),("matching_policy","same_date_hour"),
    ("outcome_horizon_bars",10000)])
def test_incompatible_choices_rejected_before_planning(key,value):
    p=comparison_payload();p["outcomes"][key]=value
    with pytest.raises(ValueError,match="controlled_reversal_invalid"):
        normalize_check_request(p)

def comparison_candles():
    # Two similar prior histories at identical close slots two days apart.
    rows=[]
    for i in range(-97, 384):
        cycle=i%192
        close=120-cycle*.01
        opened=close
        if cycle==95: opened=close+.2
        if cycle==96:
            opened,close=(118.9,119.4) if i==96 else (119.06,119.06*(119.4/118.9))
        t=BASE+timedelta(minutes=15*i)
        rows.append({"timestamp":iso(t),"close_time":iso(t+timedelta(minutes=15)),
            "known_at":iso(t+timedelta(minutes=15)),"known_at_method":"interval_close_inferred",
            "source_identity_key":"synthetic","open":opened,"close":close,
            "high":max(opened,close)+1,"low":min(opened,close)-1})
    return pd.DataFrame(rows)

def run_comparison(frame):
    p=comparison_payload();p["scope"].update(end=iso(BASE+timedelta(days=4)))
    d,q=normalize_check_request(p);plan=plan_for(p,q,({"alias":"primary_bars","timeframe_seconds":900},))
    return CHECK_REGISTRY.resolve_evaluator(d).evaluate(plan=plan,inputs={**q.parameters,"candles":frame})

def test_current_elapsed_features_cross_date_pairs_and_missingness():
    frame=comparison_candles();original=frame.copy(deep=True);result=run_comparison(frame)
    assert result["sample_count"] == 1
    pair=result["pairs"][0]
    assert pair["treated"]["day"] != pair["control"]["day"]
    assert result["elapsed_seconds"]["entry_to_target"] == 10800
    event=pd.Timestamp(pair["treated"]["event_time"])
    history=frame[(pd.to_datetime(frame.timestamp)>=event-timedelta(minutes=15*97)) &
                  (pd.to_datetime(frame.timestamp)<event)].close.to_numpy()
    rms=math.sqrt(sum((history[1:]/history[:-1]-1)**2)/96)
    assert pair["treated"]["features"]["log_prior_rms"] == pytest.approx(math.log(rms))
    assert pd.Timestamp(pair["treated"]["target_time"])-pd.Timestamp(pair["treated"]["entry_time"]) == timedelta(hours=3)
    missing=frame[frame.timestamp != iso(event+timedelta(minutes=45))]
    changed=run_comparison(missing)
    assert changed["feature_frozen_hash"] == result["feature_frozen_hash"]
    assert changed["complete_sample_count"] == 0
    late=frame.copy();late.loc[late.timestamp==iso(event-timedelta(minutes=15)),"known_at"]=iso(event+timedelta(hours=1))
    assert run_comparison(late)["eligible_treated"] == 0
    pd.testing.assert_frame_equal(frame,original)

def test_both_arm_day_deletion_and_treated_support_preserved():
    pairs=supported_pairs()
    for p in pairs: p["control"]["day"]="2022-02-01"
    result=cr.summarize_pairs(pairs,eligible_treated=200,both_arm_days=True)
    assert result["primary"]["utc_days"]==20 and result["primary"]["both_arm_utc_days"]==21
    assert result["leave_one_utc_day_out"][-1]["complete_pairs"]==0
    assert result["decision"]=="mixed"
    historical=cr.summarize_pairs(pairs,eligible_treated=200)
    assert len(historical["leave_one_utc_day_out"])==20 and "both_arm_utc_days" not in historical["primary"]

def numeric_record(opened, index, value="2", revision=1, known=None):
    source=SourceIdentity(provider="SYNTHETIC",venue="SYNTHETIC",source_kind="history",adapter_version="test.v1")
    known=known or datetime(2026,10,13,tzinfo=UTC)
    fact=NumericFact(fact_type="derivatives.liquidation_volume",contract_version="derivatives.liquidation_volume.v1",
        value=Decimal(value),raw_value=value,unit="BTC",dimensions={"liquidated_side":"long","bucket_seconds":3600,
            "volume_unit":"BTC","exchange":"Binance","product":"BTCUSDT_PERP.A"},
        effective_at=opened,effective_at_method="provider_bucket_start",accepted_at=known,known_at=known,
        known_at_method="platform_receipt",source_event_key=f"bucket-{index}",source_published_at=None)
    return NumericFactRecord(series_id=11,revision=revision,market_commit_seq=index+revision,
        ingestion_run_id="synthetic",source_identity_key=source.identity_key,source=source,
        provenance={"bucket_end":int((opened+timedelta(hours=1)).timestamp()),"received_at":iso(known),
            "publication_time_available":False},fact=fact)

def retrospective_inputs():
    p=deepcopy(catalog._EXAMPLES["event_fact_analysis@13"])
    p["statistics"]["features"]["enriched"]=p["statistics"]["features"]["enriched"][:1]
    p["inputs"]=p["inputs"][:1]
    d,q=normalize_check_request(p)
    start=datetime(2026,7,25,tzinfo=UTC)
    rows=[];records=[]
    for i in range(-15,1442):
        opened=start+timedelta(hours=i);close=100+(.3 if i%3==0 else -.1)+i*.002
        rows.append({"open_time":iso(opened),"close_time":iso(opened+timedelta(hours=1)),
            "known_at":iso(opened+timedelta(hours=1)),"open":100.,"close":close,"high":close+1,"low":99.,"volume":2})
        if 0<=i<1440: records.append(numeric_record(opened,i,str(i%5)))
    plan=plan_for(p,q)
    return d,q,plan,{**q.parameters,"candles":rows,"fact_records_by_alias":{"long_liquidations":records},
        "fact_requirements_by_alias":{"long_liquidations":p["inputs"][0]},"data_quality":{"recorded_gaps":[]}}

def test_late_numeric_revisions_retained_and_causal_selector_unchanged():
    opened=datetime(2026,7,25,tzinfo=UTC);a=numeric_record(opened,1)
    b=numeric_record(opened,1,"7",revision=2)
    sample=opened+timedelta(hours=1);cutoff=datetime(2026,10,14,tzinfo=UTC)
    assert _causal_fact_records([a,b],evaluation_time=sample)==()
    selected=retrospective_snapshots([a,b],snapshot_requests=[(sample,sample)],where={},alias="x",analysis_as_of=cutoff)
    assert selected[(sample,sample)] is b
    assert retrospective_snapshots([a,b],snapshot_requests=[(sample,sample)],where={},alias="x",analysis_as_of=sample)[(sample,sample)] is None
    assert a.fact.known_at==b.fact.known_at==datetime(2026,10,13,tzinfo=UTC)

def test_retrospective_uses_paired_models_and_exposes_actual_provenance():
    d,q,plan,inputs=retrospective_inputs();ev=CHECK_REGISTRY.resolve_evaluator(d)
    result=ev.evaluate(plan=plan,inputs=inputs)
    assert result["analysis_sample_count"]>1000
    assert result["statistics"]["model"]["retrospective_validation_count"] > 400
    assert "oos_count" not in result["statistics"]["model"]
    assert result["hashes"]["statistics_hash"] == semantic_hash(result["statistics"])
    assert result["hashes"]["event_population_hash"] == semantic_hash({"events":result["events"]})
    assert result["evidence_class"]=="retrospective_conditional_association"
    assert not result["point_in_time_predictive"] and not result["scientific_certification_eligible"]
    row=next(r for r in result["events"] if r["analysis_eligible"])
    assert "decision_time" not in row and row["historical_decision"] is False
    ref=row["fact_references"]["long_liquidations"][0]
    assert ref["known_at"].startswith("2026-10-13") and ref["source_published_at"] is None
    assert ref["source_provenance"]["publication_time_available"] is False
    assert row["late_known_fact_count"]==1
    assert semantic_hash(result)==semantic_hash(ev.evaluate(plan=plan,inputs=inputs))
    causal=deepcopy(inputs);causal["outcomes"].pop("retrospective")
    with pytest.raises(ValueError,match="retrospective_invalid"):
        ev.evaluate(plan=plan,inputs=causal)
    with pytest.raises(ValueError,match="retrospective_invalid"):
        EventFactEvaluator().evaluate(plan=plan,inputs=inputs)

@pytest.mark.parametrize("mutation", ["trigger","interval","model","late_cutoff","enriched","extra_outcome"])
def test_retrospective_rejects_incompatible_combination(mutation):
    p=deepcopy(catalog._EXAMPLES["event_fact_analysis@13"])
    if mutation=="trigger":p["detector"]["evaluation_trigger"]="required_facts_available"
    if mutation=="interval":p["scope"]["timeframe"]="15m"
    if mutation=="model":p["statistics"]["model"]={}
    if mutation=="late_cutoff":p["outcomes"]["retrospective"]["analysis_as_of"]="2026-08-01T00:00:00Z"
    if mutation=="enriched":p["statistics"]["features"]["enriched"][0]["operator"]="age_seconds"
    if mutation=="extra_outcome":p["outcomes"]["horizons"]=[1,2]
    with pytest.raises(ValueError,match="retrospective_invalid|event_fact_check_invalid"):
        normalize_check_request(p)


def test_execution_reader_uses_analysis_cutoff_only_for_explicit_v13(monkeypatch):
    from types import SimpleNamespace
    from portal.backend.service.research import execution
    d,q,plan,inputs=retrospective_inputs();calls=[]
    resolver=SimpleNamespace(dataset_binding=None, causal_history=lambda **kw:calls.append(kw) or ())
    monkeypatch.setattr(execution.candle_service,"fetch_ohlcv_by_instrument",lambda *a,**kw:inputs["candles"])
    plan=replace(plan, market_data_requirements=(
        {"alias":"primary_bars","instrument_id":"synthetic"},
        {"alias":"long_liquidations","consumer_id":"check","required_start":q.scope["start"],"required_end":q.scope["end"]}),
        execution={"fact_history_required":True}, plan_hash="")
    execution._load_market_inputs(d,q,plan,resolver=resolver)
    assert calls[-1]["evaluation_time"] == datetime(2026,10,13,23,tzinfo=UTC)
    old=replace(q,definition_version="4", request_hash="", parameters={**q.parameters,"outcomes":{k:v for k,v in q.parameters["outcomes"].items() if k!="retrospective"}})
    execution._load_market_inputs(CHECK_REGISTRY.resolve_definition("event_fact_analysis","4"),old,plan,resolver=resolver)
    assert calls[-1]["evaluation_time"] == datetime(2026,9,23,tzinfo=UTC)

def test_retrospective_missing_bucket_and_gap_do_not_become_zero():
    d,q,plan,inputs=retrospective_inputs();ev=CHECK_REGISTRY.resolve_evaluator(d)
    baseline=ev.evaluate(plan=plan,inputs=inputs)
    missing=deepcopy(inputs);missing["fact_records_by_alias"]["long_liquidations"].pop(100)
    result=ev.evaluate(plan=plan,inputs=missing)
    assert result["sample_count"]==baseline["sample_count"]
    assert result["analysis_sample_count"]==baseline["analysis_sample_count"]-1
    gap=deepcopy(missing);gap["data_quality"]["recorded_gaps"]=[{"alias":"long_liquidations",
        "start":"2026-07-29T04:00:00Z","end":"2026-07-29T05:00:00Z"}]
    result=ev.evaluate(plan=plan,inputs=gap)
    assert result["analysis_sample_count"]==baseline["analysis_sample_count"]-1
    assert result["eligibility"]["exclusions"]["fact_gap:long_liquidations"]==1
    # Match historical exact-bucket semantics: actual present observations remain
    # usable under continue_degraded, with uncertainty retained, never filled.
    gap=deepcopy(inputs);gap["data_quality"]["recorded_gaps"]=[{"alias":"long_liquidations",
        "start":q.scope["start"],"end":q.scope["end"],"classification":"provider_completeness_unverified"}]
    result=ev.evaluate(plan=plan,inputs=gap)
    assert result["analysis_sample_count"]==baseline["analysis_sample_count"]
    assert result["gap_decision"]["degraded"] is True
    assert result["data_quality"]["recorded_gaps"]==gap["data_quality"]["recorded_gaps"]
    blocked=ev.evaluate(plan=replace(plan,gap_policy="reject",plan_hash=""),inputs=gap)
    assert blocked["status"]=="blocked" and not blocked["point_in_time_predictive"]


def test_retrospective_bucket_validation_ambiguity_and_cancellation(monkeypatch):
    import portal.backend.service.research.retrospective_numeric as retro
    opened=datetime(2026,7,25,tzinfo=UTC);a=numeric_record(opened,1);sample=opened+timedelta(hours=1)
    options={"snapshot_requests":[(sample,sample)],"where":{},"alias":"x","analysis_as_of":datetime(2026,10,14,tzinfo=UTC)}
    bad=replace(a,provenance={"bucket_end":int(sample.timestamp())+1})
    with pytest.raises(ValueError,match="inconsistent hourly bucket"):
        retro.retrospective_snapshots([bad],**options)
    with pytest.raises(ValueError,match="ambiguous exact bucket"):
        retro.retrospective_snapshots([a,numeric_record(opened,2)],**options)
    def cancelled():raise RuntimeError("cancelled")
    monkeypatch.setattr(retro,"execution_checkpoint",cancelled)
    with pytest.raises(RuntimeError,match="cancelled"):
        retro.retrospective_snapshots([a],**options)

def test_current_exact_comparison_radius_slot_and_shared_overlap_controls():
    c=comparison_payload()["outcomes"]
    def candidate(day,treated,slot="00:15:00"):
        event=BASE+timedelta(days=day)
        return {"event_time":iso(event),"event_close_time":iso(event+timedelta(minutes=15)),
            "entry_time":iso(event+timedelta(minutes=30)),"target_time":iso(event+timedelta(hours=3,minutes=30)),
            "stratum":slot,"day":event.date().isoformat(),"treated":treated,"features":dict.fromkeys(cr.FEATURES,0.)}
    assert len(cr.prepare_pairs([candidate(0,True),candidate(7,False)],choices=c)[0])==1
    assert cr.prepare_pairs([candidate(0,True),candidate(8,False)],choices=c)[0]==[]
    assert cr.prepare_pairs([candidate(0,True),candidate(7,False,"00:30:00")],choices=c)[0]==[]
    assert cr.prepare_pairs([candidate(0,True),candidate(0,False)],choices=c)[0]==[]


def test_real_planner_keeps_exact_profile_history_and_typed_numeric_inputs():
    from portal.backend.service.research.planning import plan_research_check
    def indicator_planner(ids, **kwargs):
        assert ids == []
        return {"indicators":[],"requirements":[],"warmup_bars":0}
    for p in (comparison_payload(),deepcopy(catalog._EXAMPLES["event_fact_analysis@13"])):
        d,q=normalize_check_request(p)
        plan=plan_research_check(d,q,inspect_coverage=False,instrument_loader=lambda key:{"id":key,"symbol":"SYNTHETIC"},indicator_planner=indicator_planner)
        if d.definition_id=="raw_forward_outcome":
            assert plan.warmup["bars"]==97 and plan.outcome_tail["seconds"]==0
            assert plan.materialization_range["start"].startswith("2021-12-30T23:45:00")
        else:
            assert plan.execution["fact_history_required"]
            assert len(plan.market_data_requirements)==3
            assert all(r.get("timeframe_seconds") is None for r in plan.market_data_requirements if r["alias"]!="primary_bars")
        d,q,plan=pin_check_definition_to_plan(d,q,plan)
        assert reconstruct_pinned_check_definition(q,plan).to_dict()==d.to_dict()

def test_impossible_same_hour_closed_intervals_fail_early():
    p=comparison_payload("30m");p["outcomes"].update(matching_policy="same_date_hour",comparison_window_days=0,outcome_horizon_bars=1)
    with pytest.raises(ValueError,match="room for two disjoint"):
        normalize_check_request(p)




def test_complete_paired_policy_plans_only_evaluation_buckets_and_preserves_legacy():
    from portal.backend.service.research.planning import plan_research_check
    p = deepcopy(catalog._EXAMPLES["event_fact_analysis@13"])
    def planned(payload):
        d, q = normalize_check_request(payload)
        plan = plan_research_check(d, q, inspect_coverage=False,
            instrument_loader=lambda key: {"id": key, "symbol": "SYNTHETIC"},
            indicator_planner=lambda ids, **kw: {"indicators": [], "requirements": [], "warmup_bars": 0})
        d, q, plan = pin_check_definition_to_plan(d, q, plan)
        assert reconstruct_pinned_check_definition(q, plan).to_dict() == d.to_dict()
        return d, q, plan
    old_d, old_q, old_plan = planned(p)
    assert old_plan.outcome_tail["seconds"] == 3600
    assert old_plan.market_data_requirements[1]["required_start"].startswith("2026-07-24T23:")
    p["outcomes"]["retrospective"]["input_coverage_policy"] = "complete_paired_rows"
    d, q, plan = planned(p)
    assert d.evaluator_version == "13"
    assert d.definition_hash != old_d.definition_hash
    assert plan.outcome_tail["seconds"] == 0
    assert plan.materialization_range["end_exclusive"].startswith("2026-09-23T00:")
    assert plan.market_data_requirements[0]["required_start"].startswith("2026-07-24T09:")
    for req in plan.market_data_requirements[1:]:
        assert req["required_start"].startswith("2026-07-25T00:")
        assert req["required_end"].startswith("2026-09-23T00:")
        assert req["coverage_policy"] == "recorded_partial"
    assert "input_coverage_policy" not in old_q.parameters["outcomes"]["retrospective"]
    p["outcomes"]["retrospective"]["input_coverage_policy"] = "fill_missing_zero"
    with pytest.raises(ValueError, match="unsupported input coverage policy"):
        planned(p)


def test_complete_paired_two_sides_excludes_missing_retains_zeros_and_fixed_boundary():
    d, q, plan, inputs = retrospective_inputs()
    long = inputs["fact_records_by_alias"]["long_liquidations"]
    short = [replace(rec, series_id=12, fact=replace(rec.fact,
        dimensions={**rec.fact.dimensions, "liquidated_side": "short"})) for rec in long]
    inputs["fact_records_by_alias"]["short_liquidations"] = short
    inputs["inputs"].append({**inputs["inputs"][0], "alias": "short_liquidations",
        "dimensions": {**inputs["inputs"][0]["dimensions"], "liquidated_side": "short"}})
    inputs["statistics"]["features"]["enriched"].append(
        {"name": "short_liquidations", "operator": "latest_value", "input_alias": "short_liquidations"})
    inputs["fact_requirements_by_alias"]["short_liquidations"] = inputs["inputs"][-1]
    inputs["outcomes"]["retrospective"]["input_coverage_policy"] = "complete_paired_rows"
    end = datetime(2026, 9, 23, tzinfo=UTC)
    inputs["candles"] = [row for row in inputs["candles"] if pd.Timestamp(row["open_time"]) < end]
    ev = CHECK_REGISTRY.resolve_evaluator(d)
    baseline = ev.evaluate(plan=plan, inputs=inputs)
    inputs["fact_records_by_alias"]["long_liquidations"] = long[:100] + long[101:]
    inputs["fact_records_by_alias"]["short_liquidations"] = short[:1100] + short[1101:]
    result = ev.evaluate(plan=plan, inputs=inputs)
    assert result["analysis_sample_count"] == baseline["analysis_sample_count"] - 2
    admitted = [row for row in result["events"] if row["analysis_eligible"]]
    assert any(row["features"]["long_liquidations"] == 0 for row in admitted)
    assert all(row["features"]["long_liquidations"] is not None and
               row["features"]["short_liquidations"] is not None for row in admitted)
    assert all(pd.Timestamp(row["outcomes"]["1"]["target_time"]) < end for row in admitted)
    assert not result["events"][-1]["analysis_eligible"]
    assert result["analysis_contract"]["input_coverage_policy"] == "complete_paired_rows"
    assert not result["point_in_time_predictive"]

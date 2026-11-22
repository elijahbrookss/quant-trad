from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, CheckRequest, ResolvedCheckPlan
from portal.backend.service.research import catalog, service
from portal.backend.service.research.raw_candle_evaluator import OUTCOME_SCHEMA, RawCandleEvaluator
from portal.backend.service.research.registry import (
    CHECK_REGISTRY, definition_allows_evidence, materialize_check_definition,
    normalize_check_request, normalize_check_preparation_request,
    pin_check_definition_to_plan, reconstruct_pinned_check_definition,
)

START = datetime(2022, 1, 1, tzinfo=UTC)


def time(index):
    return (START + timedelta(minutes=index)).isoformat()


def payload(volume=True):
    conditions = [{"field": "previous_close", "operator": "lt", "value_field": "previous_open"},
                  {"field": "close", "operator": "gt", "value_field": "open"},
                  {"field": "open", "operator": "lte", "value_field": "previous_close"},
                  {"field": "close", "operator": "gte", "value_field": "previous_open"}]
    if volume:
        conditions.append({"field": "volume", "operator": "gt", "value_field": "previous_volume"})
    return {"check_family": "raw_forward_outcome", "mode": "evidence", "dataset_id": "mds_"+"a"*32,
            "scope": {"instrument_id": "instrument-1", "timeframe": "1m", "start": time(1), "end": time(10)},
            "detector": {"all": conditions}, "outcomes": {"schema_version": OUTCOME_SCHEMA,
                "forward_bars": [5], "entry_lag_bars": 1, "min_sample_count": 1},
            "gap_policy": "continue_degraded"}


def candles():
    closes = [100, 101, 110, 112, 114, 116, 118, 121, 122]
    opens = [101, 100, *closes[2:]]
    return pd.DataFrame([{ "timestamp": time(i), "close_time": time(i+1), "known_at": time(i+1),
        "known_at_method": "interval_close_inferred", "source_identity_key": "source-1",
        "open": o, "high": max(o,c)+1, "low": min(o,c)-1, "close": c,
        "volume": 20 if i==1 else 10} for i,(o,c) in enumerate(zip(opens,closes))]).astype({key:"float64" for key in ("open","high","low","close","volume")})


def contract(p=None):
    p=p or payload();definition,request=normalize_check_request(p)
    declaration=RawCandleEvaluator().declare_requirements(definition=definition,request=request)
    plan=ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION,request_hash=request.request_hash,
        market_data_requirements=({"alias":"primary_bars","instrument_id":"instrument-1",
            "timeframe_seconds":60,"required_fields":declaration["primary_candle_required_fields"]},),
        indicator_graph=(),evaluation_range={"start":p["scope"]["start"],"end_exclusive":p["scope"]["end"]},
        materialization_range={"start":time(0),"end_exclusive":p["scope"]["end"]},
        warmup={"bars":1,"timeframe_seconds":60},outcome_tail={"bars":0,"seconds":0},
        gap_policy="continue_degraded")
    return definition,request,plan


def run(frame=None,p=None):
    _,request,plan=contract(p)
    return RawCandleEvaluator().evaluate(plan=plan,inputs={**request.parameters,
        "candles":candles() if frame is None else frame,"data_quality":{"status":"clean"}})


def test_explicit_version_dynamic_volume_and_pinned_reconstruction():
    definition,request,plan=contract()
    assert definition.definition_version.startswith("3+") and definition.evaluator_version=="3"
    assert definition_allows_evidence(definition)
    declaration=RawCandleEvaluator().declare_requirements(definition=definition,request=request)
    assert declaration["feature_lookback_bars"]==1 and declaration["outcome_boundary"]=="evaluation_end_exclusive"
    assert "volume" in declaration["primary_candle_required_fields"]
    definition,request,plan=pin_check_definition_to_plan(definition,request,plan)
    assert reconstruct_pinned_check_definition(request,plan).to_dict()==definition.to_dict()
    definition,request,_=contract(payload(False))
    assert "volume" not in RawCandleEvaluator().declare_requirements(definition=definition,request=request)["primary_candle_required_fields"]
    assert "evidence" in catalog.get_check_definition("raw_forward_outcome","3")["eligibility"]["modes"]
    normalize_check_preparation_request(payload())


def test_retired_v2_refuses_without_reinterpreting_unknown_settings():
    p=payload();p['outcomes'].pop('schema_version')
    with pytest.raises(ValueError,match='check_definition_retired'):
        normalize_check_request(p)
    p['outcomes']['unknown_setting']=True
    with pytest.raises(ValueError,match='check_definition_retired'):
        normalize_check_request(p)


def test_exact_five_bar_return_close_clocks_and_unchanged_inputs():
    frame=candles();before=frame.copy(deep=True);p=payload();original=deepcopy(p)
    result=run(frame,p)
    pd.testing.assert_frame_equal(frame,before);assert p==original
    assert result["sample_count"]==result["complete_sample_count"]==1
    event=result["events"][0]
    assert event["event_time"]==time(1).replace("+00:00","Z")
    assert event["entry_time"]==time(3).replace("+00:00","Z")
    outcome=event["outcomes"]["5"]
    assert outcome["target_time"]==time(8).replace("+00:00","Z")
    assert outcome["forward_return_pct"]==pytest.approx(121/110-1)
    assert result["outcomes"]["summary"]["5"]["mean_forward_return_pct"]==outcome["forward_return_pct"]
    assert result["return_unit"]=="fraction" and result["event_ownership"]=="check"
    assert semantic_hash(result)==semantic_hash(run(frame,p))
    p["outcomes"]["direction"]="short"
    assert run(frame,p)["events"][0]["outcomes"]["5"]["forward_return_pct"]==-outcome["forward_return_pct"]


@pytest.mark.parametrize("value",[None,float("nan"),-1,float("inf")])
def test_unknown_or_invalid_volume_never_becomes_low_volume(value):
    frame=candles();frame.loc[1,"volume"]=value
    p=payload();p["detector"]["all"][-1]["operator"]="lte"
    result=run(frame,p)
    assert result["sample_count"]==0 and result["candidate_exclusions"]["predicate_volume_unavailable"]>=1


def test_real_zero_and_price_only_missing_volume_are_supported():
    frame=candles();frame.loc[[0,1],"volume"]=0
    p=payload();p["detector"]["all"][-1]["operator"]="lte"
    assert run(frame,p)["complete_sample_count"]==1
    assert run(frame.drop(columns="volume"),payload(False))["complete_sample_count"]==1
    with pytest.raises(ValueError,match="missing canonical fields"):
        run(frame.drop(columns="volume"))


@pytest.mark.parametrize("missing,reason",[(0,"previous_bar_missing"),(2,"gap_in_entry_path"),(4,"gap_in_horizon")])
def test_real_elapsed_paths_do_not_bridge_missing_rows(missing,reason):
    result=run(candles().drop(index=missing).reset_index(drop=True))
    assert result["complete_sample_count"]==0
    if missing==0:
        assert result["candidate_exclusions"][reason]>=1
    else:
        assert result["events"][0]["outcomes"]["5"]["reason"]==reason


def test_target_at_exclusive_end_is_censored_and_never_uses_tail():
    p=payload();p["scope"]["end"]=time(8)
    result=run(p=p)
    assert result["events"][0]["outcomes"]["5"]["reason"]=="target_outside_evaluation"
    assert result["outcomes"]["summary"]["5"]["sample_count"]==0


@pytest.mark.parametrize("index,known,reason",[(0,4,"predicate_known_after_entry_close"),
    (1,4,"predicate_known_after_entry_close"),(2,4,"entry_price_known_after_close")])
def test_late_predicate_or_entry_prices_cannot_retroactively_enter(index,known,reason):
    frame=candles();frame.loc[index,"known_at"]=time(known)
    result=run(frame)
    assert result["events"][0]["outcomes"]["5"]["reason"]==reason
    assert result["complete_sample_count"]==0


def test_availability_boundary_and_retrospective_outcome_clock_are_distinct():
    frame=candles();frame.loc[1,"known_at"]=time(3);frame.loc[7,"known_at"]=time(12)
    result=run(frame)
    assert result["complete_sample_count"]==1
    assert result["events"][0]["outcomes"]["5"]["outcome_known_at"]==time(12).replace("+00:00","Z")
    assert result["known_at_methods"]=={"interval_close_inferred":9}


@pytest.mark.parametrize("field,value",[("high",1),("close",float("nan")),("close_time",time(4)),
    ("known_at",time(0)),("known_at_method",None),("source_identity_key",None),
    ("source_identity_key","other-source"),("known_at","2022-01-01T00:02:00")])
def test_malformed_canonical_inputs_block_instead_of_silent_repair(field,value):
    frame=candles();frame.loc[1,field]=value
    with pytest.raises(ValueError,match="raw_candle_input_invalid"):
        run(frame)


def test_duplicate_or_unordered_identity_blocks():
    frame=candles();frame.loc[1,"timestamp"]=time(0)
    with pytest.raises(ValueError,match="duplicate or unordered"):
        run(frame)
    with pytest.raises(ValueError,match="duplicate or unordered"):
        run(candles().iloc[::-1])


@pytest.mark.parametrize("field,value",[("forward_bars",[]),("forward_bars",[True]),
    ("forward_bars",[1.5]),("entry_lag_bars",True),("entry_lag_bars",10000),
    ("outcome_boundary","extend"),("min_edge_pct",float("nan")),("max_examples",1001)])
def test_new_method_bounds_fail_before_execution(field,value):
    p=payload();p["outcomes"][field]=value
    with pytest.raises(ValueError,match="raw_candle_check_invalid"):
        normalize_check_request(p)


def test_existing_generic_boolean_predicates_and_current_only_volume():
    p=payload(False);p["detector"]={"all":[p["detector"],{"any":[{"field":"volume","operator":"gt","value":10},
        {"not":{"field":"close","operator":"gt","value":200}}]}]}
    frame=candles();frame.loc[0,"volume"]=None
    assert run(frame,p)["complete_sample_count"]==1



def test_real_planner_declares_volume_lookback_and_no_future_tail():
    from portal.backend.service.research.planning import plan_research_check
    for volume in (False, True):
        definition,request,_=contract(payload(volume))
        def indicator_planner(ids, **kwargs):
            assert ids==[]
            return {"indicators":[],"requirements":[],"warmup_bars":0}
        plan=plan_research_check(definition,request,inspect_coverage=False,
            instrument_loader=lambda key:{"id":key,"symbol":"BTC-USD"},indicator_planner=indicator_planner)
        assert plan.warmup["bars"]==1
        assert plan.outcome_tail["seconds"]==0
        assert plan.outcome_tail["boundary"]=="evaluation_end_exclusive"
        assert plan.materialization_range["end_exclusive"]==plan.evaluation_range["end_exclusive"]
        assert ("volume" in plan.market_data_requirements[0]["required_fields"])==volume
        assert plan.market_data_requirements[0]["required_start"].startswith("2022-01-01T00:00:00")


def test_overflowed_outcome_is_unresolved_not_a_nonfinite_statistic():
    frame=candles()
    frame.loc[2,["open","close","high","low"]]=[1e-308,1e-308,2e-308,0.5e-308]
    result=run(frame)
    assert result["complete_sample_count"]==0
    assert result["events"][0]["outcomes"]["5"]["reason"]=="outcome_numeric_unavailable"
    assert result["outcomes"]["summary"]["5"]["mean_forward_return_pct"] is None


@pytest.mark.parametrize("detector",[{"all":[]},{"all":[],"field":"close"},
    {"field":"close","operator":"gt","value":float("nan")},
    {"field":"close","operator":"gt","value":1,"unknown":1}])
def test_new_predicate_rejects_ambiguous_empty_or_nonfinite_configuration(detector):
    p=payload();p["detector"]=detector
    with pytest.raises(ValueError,match="raw_candle_check_invalid"):
        normalize_check_request(p)


def test_v3_retained_classification_and_version_pair_are_exact():
    from dataclasses import replace
    definition,_,_=contract()
    assert service._definition_evidence_classification(definition)=="frozen_replayable"
    assert not definition_allows_evidence(replace(definition,evaluator_version="2",definition_hash=""))


def test_raw_loop_honors_the_existing_execution_checkpoint(monkeypatch):
    from portal.backend.service.research import raw_candle_evaluator
    def cancel():
        raise RuntimeError("cancelled-by-existing-owner")
    monkeypatch.setattr(raw_candle_evaluator,"execution_checkpoint",cancel)
    with pytest.raises(RuntimeError,match="cancelled-by-existing-owner"):
        run()



def test_oversized_range_rejects_before_source_or_store_inspection():
    from portal.backend.service.research.planning import plan_research_check
    p=payload();p["scope"]["end"]=(START+timedelta(days=100)).isoformat()
    definition,request=normalize_check_request(p)
    def forbidden(*args,**kwargs):
        raise AssertionError("range admission reached an external boundary")
    with pytest.raises(ValueError,match="nominal materialization"):
        plan_research_check(definition,request,instrument_loader=forbidden,indicator_planner=forbidden)


def test_bounded_predicate_rejects_before_recursive_legacy_validation():
    p=payload();node={"field":"close","operator":"gt","value":0}
    for _ in range(34):
        node={"not":node}
    p["detector"]=node
    with pytest.raises(ValueError,match="bounded mapping tree"):
        normalize_check_request(p)



def test_interleaved_off_grid_row_cannot_replace_actual_previous_interval():
    frame=candles();extra=frame.iloc[0].copy()
    extra["timestamp"]=(START+timedelta(seconds=30)).isoformat()
    extra["close_time"]=extra["known_at"]=(START+timedelta(seconds=90)).isoformat()
    frame=pd.concat([frame.iloc[:1],pd.DataFrame([extra]),frame.iloc[1:]],ignore_index=True)
    with pytest.raises(ValueError,match="inconsistent canonical candle cadence"):
        run(frame)


def test_consistent_observed_source_offset_is_preserved_without_utc_anchor():
    frame=candles()
    for field in ("timestamp","close_time","known_at"):
        frame[field]=[(pd.Timestamp(v)+timedelta(seconds=30)).isoformat() for v in frame[field]]
    result=run(frame)
    assert result["complete_sample_count"]==1
    assert result["events"][0]["entry_time"]==(START+timedelta(minutes=3,seconds=30)).isoformat().replace("+00:00","Z")

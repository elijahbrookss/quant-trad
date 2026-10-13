"""Synthetic qualification of fixed raw-v4 matching; no market-data acquisition."""
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research import catalog
from portal.backend.service.research import controlled_reversal_evaluator as cr
from portal.backend.service.research.registry import (
    normalize_check_request, normalize_check_preparation_request, definition_allows_evidence,
    pin_check_definition_to_plan, reconstruct_pinned_check_definition,
)

START = datetime(2022, 1, 1, tzinfo=UTC)


def time(i):
    return (START+timedelta(minutes=i)).isoformat().replace("+00:00","Z")


def payload():
    return {"check_family": "raw_forward_outcome", "mode": "evidence", "dataset_id": "mds_"+"a"*32,
        "scope": {"instrument_id": "synthetic", "timeframe": "1m", "start": time(21), "end": time(90)},
        "detector": dict(cr.DETECTOR), "outcomes": {"schema_version": cr.OUTCOME_SCHEMA},
        "gap_policy": "continue_degraded"}


def candles():
    rows = []
    for i in range(90):
        close = 120-(i % 30)*.1
        opened = close
        if i in (20,50):
            opened = close+.2
        elif i == 21:
            opened,close = 117.9,118.3
        elif i == 51:
            opened,close = 118.01,118.01*(118.3/117.9)
        rows.append({"timestamp": time(i), "close_time": time(i+1), "known_at": time(i+1),
            "known_at_method": "interval_close_inferred", "source_identity_key": "synthetic-source",
            "open": opened, "close": close, "high": max(opened,close)+1, "low": min(opened,close)-1})
    return pd.DataFrame(rows)


def contract(p=None):
    p = p or payload()
    definition,request = normalize_check_request(p)
    declaration = cr.ControlledReversalEvaluator().declare_requirements(definition=definition,request=request)
    plan = ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION,request_hash=request.request_hash,
        market_data_requirements=({"alias": "primary_bars", "instrument_id": "synthetic",
            "timeframe_seconds": 60, "required_fields": declaration["primary_candle_required_fields"]},),
        indicator_graph=(),evaluation_range={"start": p["scope"]["start"], "end_exclusive": p["scope"]["end"]},
        materialization_range={"start": time(0), "end_exclusive": p["scope"]["end"]},
        warmup={"bars": 21, "timeframe_seconds": 60},outcome_tail={"bars": 0,"seconds": 0},
        gap_policy="continue_degraded")
    return definition,request,plan


def run(frame=None,p=None):
    _,request,plan = contract(p)
    return cr.ControlledReversalEvaluator().evaluate(plan=plan,inputs={**request.parameters,
        "candles": candles() if frame is None else frame})


def test_explicit_version_preparation_catalog_and_reconstruction():
    definition,request,plan = contract()
    assert definition.definition_version.startswith("4+") and definition_allows_evidence(definition)
    declared = cr.ControlledReversalEvaluator().declare_requirements(definition=definition,request=request)
    assert declared["feature_lookback_bars"] == 21 and declared["indicator_ids"] == []
    assert "volume" not in declared["primary_candle_required_fields"]
    definition,request,plan = pin_check_definition_to_plan(definition,request,plan)
    assert reconstruct_pinned_check_definition(request,plan).to_dict() == definition.to_dict()
    normalize_check_preparation_request(payload())
    description = catalog.get_check_definition("raw_forward_outcome","4")
    assert description["result_shape"]["analysis_field"] == "controlled_reversal_comparison"
    assert description["settings"]["fixed"]["support"]["complete_pairs"] == 100


@pytest.mark.parametrize("section,key,value", [
    ("outcomes","forward_bars",[10]), ("outcomes","min_sample_count",1),
    ("detector","threshold",0), ("statistics","bootstrap",{}),
    ("scope","indicator_id","indicator"), ("scope","timeframe","5m"),
    ("scope","warmup_bars",20), ("scope","warmup_bars",True),
    ("scope","end","2023-01-01T00:00:00Z"), ("scope","start","2022-01-01T00:21:01Z"),
])
def test_tuning_and_underdeclared_history_rejected(section,key,value):
    p=payload();p.setdefault(section,{})[key]=value
    with pytest.raises(ValueError,match="controlled_reversal_invalid"):
        normalize_check_request(p)


def test_exact_pair_features_and_returns_and_immutable_inputs():
    frame=candles();original=frame.copy(deep=True)
    result=run(frame);pair=result["pairs"][0]
    assert result["eligible_treated"] == result["eligible_controls"] == result["sample_count"] == 1
    assert pair["treated"]["event_time"] == time(21) and pair["control"]["event_time"] == time(51)
    history=frame.loc[:20,"close"].to_numpy()
    rms=sum((history[1:]/history[:-1]-1)**2)/20
    assert pair["treated"]["features"]["log_prior_rms"] == pytest.approx(cr.math.log(cr.math.sqrt(rms)))
    assert pair["treated"]["entry_time"] == time(23) and pair["treated"]["target_time"] == time(28)
    assert pair["outcomes"]["treated"]["forward_return"] == pytest.approx(117.3/117.8-1)
    assert result["controlled_reversal_comparison"]["decision"] == "unsupported"
    assert result["complete_sample_count"] == 1
    assert semantic_hash(result) == semantic_hash(run(frame))
    pd.testing.assert_frame_equal(frame,original)


def test_future_values_and_missing_outcomes_do_not_select_replacement():
    original=run();frame=candles()
    frame.loc[27,"close"] = 118.;frame.loc[27,"high"] = 119.
    changed=run(frame)
    assert original["feature_frozen_hash"] == changed["feature_frozen_hash"]
    assert original["pairs"][0]["outcomes"] != changed["pairs"][0]["outcomes"]
    frame=candles().drop(index=25)
    missing=run(frame)
    assert missing["feature_frozen_hash"] == original["feature_frozen_hash"]
    assert missing["sample_count"] == 1 and missing["complete_sample_count"] == 0
    assert missing["pairs"][0]["joint_nonoverlap_selected"] is True
    assert missing["pairs"][0]["outcomes"]["treated"]["reason"] == "gap_in_horizon"


def test_entry_clock_checked_after_freeze_and_features_are_causal():
    original=run();frame=candles();frame.loc[22,"known_at"] = time(24)
    result=run(frame)
    assert result["feature_frozen_hash"] == original["feature_frozen_hash"]
    assert result["pairs"][0]["outcomes"]["treated"]["reason"] == "entry_price_known_after_close"
    frame=candles();frame.loc[21,"known_at"] = time(24)
    result=run(frame)
    assert result["eligible_treated"] == 0
    assert result["candidate_exclusions"]["feature_known_after_entry_close"] == 1
    frame=candles().drop(index=0)
    assert run(frame)["eligible_treated"] == 0


def candidate(i, treated, *, feature=0.):
    return {"event_time": time(i), "event_close_time": time(i+1), "decision_known_at": time(i+1),
        "entry_time": time(i+2), "target_time": time(i+7), "day": "2022-01-01",
        "stratum": "2022-01-01T00:00:00Z", "treated": treated,
        "features": dict.fromkeys(cr.FEATURES, feature)}


def test_ties_without_reuse_closed_overlap_and_fixed_nonoverlap():
    # Equal distance/score picks earlier control; the second treated cannot reuse it.
    pairs,unmatched,_=cr.prepare_pairs([candidate(20,True),candidate(10,False),candidate(30,False),candidate(21,True)])
    assert [(p["treated"]["event_time"],p["control"]["event_time"]) for p in pairs] == [(time(20),time(10)),(time(21),time(30))]
    assert not unmatched and [p["joint_nonoverlap_selected"] for p in pairs] == [True,False]
    assert cr.prepare_pairs([candidate(20,True),candidate(25,False)])[0] == []
    assert len(cr.prepare_pairs([candidate(20,True),candidate(26,False)])[0]) == 1
    wrong_hour=candidate(30,False);wrong_hour["stratum"]="2022-01-01T01:00:00Z"
    assert cr.prepare_pairs([candidate(20,True),wrong_hour])[0] == []


def test_inclusive_caliper_and_comparison_cancellation(monkeypatch):
    a,b=candidate(20,True),candidate(30,False)
    b["features"]["log_body_size"] = cr.CALIPERS[0]
    assert len(cr.prepare_pairs([a,b])[0]) == 1
    b["features"]["log_body_size"] = cr.math.nextafter(cr.CALIPERS[0],cr.math.inf)
    assert cr.prepare_pairs([a,b])[0] == []
    monkeypatch.setattr(cr,"MAX_COMPARISONS",0)
    with pytest.raises(ValueError,match="resource_limit"):
        cr.prepare_pairs([a,b])
    def cancelled():
        raise RuntimeError("cancelled")
    monkeypatch.setattr(cr,"execution_checkpoint",cancelled)
    with pytest.raises(RuntimeError,match="cancelled"):
        run()


def supported_pairs():
    pairs=[]
    for i in range(120):
        a,b=candidate(20,True,feature=float(i%6)),candidate(30,False,feature=float(i%6))
        a["day"]=b["day"]=f"2022-01-{i//6+1:02d}"
        pairs.append({"treated": a,"control": b,"joint_nonoverlap_selected": True,
            "outcomes": {"treated": {"status":"resolved","forward_return":.002},
                         "control": {"status":"resolved","forward_return":.001}}})
    return pairs


def test_support_decision_day_deletion_and_nonoverlap_not_inference():
    pairs=supported_pairs();result=cr.summarize_pairs(pairs,eligible_treated=200)
    assert result["decision"] == "positive_descriptive_lead" and all(result["support_gates"].values())
    assert len(result["leave_one_utc_day_out"]) == 20
    assert result["primary"]["mean_paired_return_difference"] == pytest.approx(.001)
    for p in pairs:
        p["joint_nonoverlap_selected"] = False
    assert cr.summarize_pairs(pairs,eligible_treated=200)["decision"] == "mixed"
    assert cr.summarize_pairs(pairs,eligible_treated=241)["decision"] == "unsupported"
    for p in pairs:
        p["outcomes"]["treated"]["forward_return"] = -.001
    assert cr.summarize_pairs(pairs,eligible_treated=200)["decision"] == "nonpositive"


def test_undefined_zero_variance_balance_and_empty_results():
    pairs=supported_pairs()
    for p in pairs:
        p["treated"]["features"]["log_body_size"] = 1.
        p["control"]["features"]["log_body_size"] = 2.
    result=cr.summarize_pairs(pairs,eligible_treated=120)
    assert result["balance"]["log_body_size"]["absolute_smd"] is None
    assert result["decision"] == "unsupported"
    assert cr.summarize_pairs([],eligible_treated=0)["decision"] == "unsupported"


def test_nominal_endpoint_strict_bound_and_outcome_availability():
    p=payload();p["scope"]["end"] = time(58)
    result=run(p=p)
    assert result["eligible_controls"] == 0
    assert result["candidate_exclusions"]["nominal_target_outside_evaluation"] == 1
    frame=candles();frame.loc[27,"known_at"] = time(95)
    result=run(frame)
    assert result["complete_sample_count"] == 1
    assert result["pairs"][0]["outcomes"]["treated"]["outcome_known_at"] == time(95)


def test_actual_planner_declares_history_and_never_requests_future_tail():
    from portal.backend.service.research.planning import plan_research_check
    definition,request,_=contract()
    def indicator_planner(ids, **kwargs):
        assert ids == []
        return {"indicators": [],"requirements": [],"warmup_bars": 0}
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{"id":key,"symbol":"SYNTHETIC"},indicator_planner=indicator_planner)
    assert plan.warmup["bars"] == 21
    assert plan.outcome_tail["seconds"] == 0
    assert plan.outcome_tail["boundary"] == "evaluation_end_exclusive"
    assert plan.market_data_requirements[0]["required_start"].startswith("2022-01-01T00:00:00")
    assert plan.materialization_range["end_exclusive"] == plan.evaluation_range["end_exclusive"]

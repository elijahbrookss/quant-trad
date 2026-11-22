from copy import deepcopy
from datetime import UTC, datetime, timedelta
import pandas as pd
import pytest

from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research.candle_distribution_evaluator import CandleDistributionEvaluator, describe
from portal.backend.service.research.raw_candle_evaluator import RawCandleEvaluator
from portal.backend.service.research.registry import normalize_check_request, reconstruct_pinned_check_definition, pin_check_definition_to_plan
from portal.backend.service.research import catalog

START = datetime(2022, 1, 1, tzinfo=UTC)

def ts(i):
    return (START+timedelta(minutes=i)).isoformat()

def fixture():
    p=deepcopy(catalog._EXAMPLES["raw_forward_outcome@6"])
    p["scope"].update(start=ts(1), end=ts(701), warmup_bars=1)
    p["outcomes"].update(forward_bars=[1,5], max_examples=2, min_sample_count=1)
    rows=[]
    for i in range(700):
        o,c=(101.,100.) if i%2==0 else (100.,101.)
        if i==650: o,c=150.,75.
        rows.append({"timestamp":ts(i),"close_time":ts(i+1),"known_at":ts(i+1),
            "known_at_method":"interval_close_inferred","source_identity_key":"source-1",
            "open":o,"close":c,"high":max(o,c)+1,"low":min(o,c)-1,
            "volume":20. if i%4==1 else 10.})
    d,r=normalize_check_request(p)
    plan=ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION,request_hash=r.request_hash,
        market_data_requirements=({"alias":"primary_bars","timeframe_seconds":60},),indicator_graph=(),
        evaluation_range={"start":ts(1),"end_exclusive":ts(701)},
        materialization_range={"start":ts(0),"end_exclusive":ts(701)},
        warmup={"bars":1,"timeframe_seconds":60},outcome_tail={"bars":0,"seconds":0},gap_policy="continue_degraded")
    return p,d,r,plan,pd.DataFrame(rows)

def test_complete_late_tail_retention_old_cohort_totals_and_determinism():
    _,_,r,plan,frame=fixture();before=frame.copy(deep=True)
    result=CandleDistributionEvaluator().evaluate(plan=plan, inputs={**r.parameters,"candles":frame})
    assert result["event_count_truncated"]==0 and len(result["events"])>250
    assert any(e["event_time"]==ts(649).replace('+00:00','Z') for e in result["events"])
    for name,op in [('increasing_volume','gt'),('non_increasing_volume','lte')]:
        parameters=deepcopy(r.parameters)
        parameters['detector']['all'][-1]={"field":"volume","operator":op,"value_field":"previous_volume"}
        old=RawCandleEvaluator().evaluate(plan=plan,inputs={**parameters,"candles":frame})
        assert sum(e['cohort']==name for e in result['events'])==old['sample_count']
        for h in ['1','5']:
            summary=result['distribution']['cohorts'][name][h]
            assert summary['sample_count']==old['outcomes']['summary'][h]['sample_count']
            assert summary['mean']==pytest.approx(old['outcomes']['summary'][h]['mean_forward_return_pct'],abs=1e-15)
    assert semantic_hash(result)==semantic_hash(CandleDistributionEvaluator().evaluate(plan=plan,inputs={**r.parameters,"candles":frame}))
    pd.testing.assert_frame_equal(frame,before)

def test_context_missingness_and_cross_cohort_overlap_are_explicit():
    _,_,r,plan,frame=fixture()
    out=CandleDistributionEvaluator().evaluate(plan=plan,inputs={**r.parameters,"candles":frame})
    assert out['events'][0]['prior_rms_return'] is None
    assert out['events'][20]['prior_context_status']=='available'
    assert out['events'][0]['overlap_components']['5']==out['events'][1]['overlap_components']['5']
    assert any(e['outcomes']['5']['status']=='unresolved' for e in out['events'])

def test_tail_contributions_keep_extremes_and_use_full_denominator():
    rows=[{'event_id':str(i),'utc_day':f'2022-01-0{1+i%2}',
        'outcomes':{'5':{'status':'resolved','forward_return_pct':v}}} for i,v in enumerate([-10.,1.,1.,20.])]
    out=describe(rows,5)
    assert out['mean']==3.
    assert out['tails'][0]['contribution_to_full_mean']==-2.5
    assert out['tails'][1]['contribution_to_full_mean']==5.
    assert len(out['leave_one_day_out'])==2
    with_empty=describe(rows,5,['2022-01-01','2022-01-02','2022-01-03'])
    assert with_empty['days'][-1]['count']==0 and with_empty['days'][-1]['mean'] is None
    assert with_empty['equal_day_population']=='days_with_resolved_events'

def test_invalid_volume_excluded_without_assigning_low_cohort():
    _,_,r,plan,frame=fixture();frame.loc[1,'volume']=float('nan')
    out=CandleDistributionEvaluator().evaluate(plan=plan,inputs={**r.parameters,"candles":frame})
    assert out['candidate_exclusions']['predicate_volume_unavailable']>=1
    assert all(e['event_time']!=ts(1).replace('+00:00','Z') for e in out['events'])

def test_registration_pinning_and_unsupported_settings():
    p,d,r,plan,_=fixture()
    assert d.evaluator_version=='6'
    pinned,request,pinned_plan=pin_check_definition_to_plan(d,r,plan)
    assert reconstruct_pinned_check_definition(request,pinned_plan).definition_hash==pinned.definition_hash
    for field,value in [('trim',.01),('quantiles',[.5])]:
        bad=deepcopy(p);bad['outcomes'][field]=value
        with pytest.raises(ValueError,match='fixed diagnostics'):normalize_check_request(bad)

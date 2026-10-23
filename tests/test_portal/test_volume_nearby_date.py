"""Synthetic qualification only; no market history or revised outcome search."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import math
import pandas as pd
import pytest
from market_data.frozen import semantic_hash
from portal.backend.service.research import catalog
from portal.backend.service.research import volume_cohort_evaluator as vc
from portal.backend.service.research.candle_distribution_evaluator import CandleDistributionEvaluator
from portal.backend.service.research.registry import (normalize_check_request,
    definition_allows_evidence,pin_check_definition_to_plan,reconstruct_pinned_check_definition)
from tests.test_portal.test_volume_cohort_evidence import fixture,candidate,ts


def multi_day(slot=3600):
    p,_,_,plan,frame=fixture()
    p['outcomes'].update(schema_version=vc.NEARBY_SCHEMA)
    p['outcomes']['comparison'].update(matching_policy='same_utc_close_slot',
        comparison_window_days=3,matching_slot_seconds=slot)
    p['scope'].update(start=ts(1),end=ts(8*1440))
    rows=[]
    for day in (0,1,3,4,7):
        for i,original in frame.iloc[:70].iterrows():
            r=original.to_dict()
            r.update(timestamp=ts(day*1440+i),close_time=ts(day*1440+i+1),known_at=ts(day*1440+i+1))
            r['volume']=20. if day==0 and i%2 else 10.
            rows.append(r)
    d,q=normalize_check_request(p)
    plan=replace(plan,plan_hash="",request_hash=q.request_hash,evaluation_range={
        'start':p['scope']['start'],'end_exclusive':p['scope']['end']},
        materialization_range={'start':ts(0),'end_exclusive':p['scope']['end']})
    return p,d,q,plan,pd.DataFrame(rows)


def run():
    _,_,q,p,f=multi_day()
    return vc.NearbyDateVolumeCohortEvaluator().evaluate(plan=p,inputs={**q.parameters,'candles':f})


def test_explicit_v8_configuration_catalog_and_frozen_reconstruction():
    p,d,q,plan,_=multi_day()
    assert d.evaluator_version=='8' and definition_allows_evidence(d)
    assert q.parameters['outcomes']['comparison']['comparison_window_days']==3
    assert vc.NearbyDateVolumeCohortEvaluator().declare_requirements(definition=d,request=q)['feature_lookback_bars']==1
    pinned,request,plan=pin_check_definition_to_plan(d,q,plan)
    assert reconstruct_pinned_check_definition(request,plan).definition_hash==pinned.definition_hash
    description=catalog.get_check_definition('raw_forward_outcome','8')
    assert description['result_shape']['analysis_field']=='volume_cohort_comparison'
    p['outcomes']['comparison']['comparison_window_days']=2
    assert normalize_check_request(p)[0].definition_hash!=d.definition_hash
    p['outcomes']['schema_version']=vc.OUTCOME_SCHEMA
    with pytest.raises(ValueError):normalize_check_request(p)


@pytest.mark.parametrize('key,value',[
    ('comparison_window_days',0),('comparison_window_days',32),('comparison_window_days',True),
    ('matching_slot_seconds',True),('matching_slot_seconds',3600.),('matching_slot_seconds',300),
    ('matching_policy','same_date_hour')])
def test_invalid_nearby_choices_fail_before_input_access(key,value):
    p,*_=multi_day();p['outcomes']['comparison'][key]=value
    with pytest.raises(ValueError):normalize_check_request(p)


def test_hour_pool_crosses_dates_and_reuses_actual_shared_matcher(monkeypatch):
    real=vc.prepare_pairs;calls=[]
    def shared(candidates,**kwargs):
        assert kwargs['choices']['comparison_window_days']==3
        assert kwargs['choices']['matching_slot_seconds']==3600
        assert kwargs['features']==vc.FEATURES
        assert kwargs['calipers']==(math.log(1.25),)*3
        assert all('outcomes' not in c for c in candidates)
        calls.append(True)
        return real(candidates,**kwargs)
    monkeypatch.setattr(vc,'prepare_pairs',shared)
    out=run()
    assert calls and out['pairs']
    assert out['schema_version']==vc.NEARBY_RESULT_SCHEMA
    assert out['comparison_rules']['support']==vc.RULES['support']
    assert len({p['control']['event_time'] for p in out['pairs']})==len(out['pairs'])
    assert any(p['treated']['day']!=p['control']['day'] for p in out['pairs'])
    assert all(abs(p['control_minus_treated_seconds'])<=3*86400 for p in out['pairs'])
    assert out['matching_time_geometry']['outside_request_controls'] is False
    # The hour slot also permits different minutes, even though this symmetric
    # fixture's nearest-time tie break happens to select exact-minute controls.
    a,b=candidate(30,True),candidate(1440+32,False)
    a['stratum']=b['stratum']='00:00:00'
    assert vc.prepare_pairs([a,b],choices={'matching_policy':'same_utc_close_slot',
        'comparison_window_days':3,'matching_slot_seconds':3600},features=vc.FEATURES,
        calipers=(math.log(1.25),)*3)[0]
    assert semantic_hash(out)==semantic_hash(run())


def test_elapsed_radius_inclusive_and_utc_hour_constraint():
    choices={'matching_policy':'same_utc_close_slot','comparison_window_days':3}
    a=candidate(30,True);b=candidate(30+3*1440,False)
    pairs,_,_=vc.prepare_pairs([a,b],choices=choices,features=vc.FEATURES,calipers=(1.,)*3)
    assert len(pairs)==1
    b=candidate(31+3*1440,False)
    assert not vc.prepare_pairs([a,b],choices=choices,features=vc.FEATURES,calipers=(1.,)*3)[0]
    b=candidate(30+1440,False);b['stratum']='another UTC hour'
    assert not vc.prepare_pairs([a,b],choices=choices,features=vc.FEATURES,calipers=(1.,)*3)[0]


def test_hour_and_minute_slots_are_configuration_not_a_new_matching_algorithm():
    _,_,q,p,f=multi_day()
    frame,rows=vc._prepare(f,step=timedelta(minutes=1),volume_required=True)
    kwargs=dict(detector=q.parameters['detector'],start=vc._clock(p.evaluation_range['start'],'start'),
        end=vc._clock(p.evaluation_range['end_exclusive'],'end'),step=timedelta(minutes=1),lag=1,horizon=5)
    choices=q.parameters['outcomes']['comparison']
    hour=vc.prepare_volume_candidates(frame,rows,choices=choices,**kwargs)[1]
    minute=vc.prepare_volume_candidates(frame,rows,choices={**choices,'matching_slot_seconds':60},**kwargs)[1]
    assert hour[0]['stratum']=='00:00:00'
    assert minute[0]['stratum']==minute[0]['event_close_time'][11:19]
    assert {c['event_time'] for c in hour}=={c['event_time'] for c in minute}


def test_future_failure_cannot_replace_a_cross_date_control(monkeypatch):
    baseline=run();evaluate=CandleDistributionEvaluator.evaluate
    chosen=baseline['pairs'][0]['control']['event_time']
    def missing(self,**kwargs):
        r=evaluate(self,**kwargs)
        next(e for e in r['events'] if e['event_time']==chosen)['outcomes']['5']={
            'status':'unresolved','reason':'synthetic_missing_future'}
        return r
    monkeypatch.setattr(CandleDistributionEvaluator,'evaluate',missing)
    out=run()
    assert out['feature_frozen_hash']==baseline['feature_frozen_hash']
    assert [(p['treated']['event_time'],p['control']['event_time']) for p in out['pairs']]==[
        (p['treated']['event_time'],p['control']['event_time']) for p in baseline['pairs']]
    assert out['volume_cohort_comparison']['missing_outcome_reasons']['control:synthetic_missing_future']==1


def test_either_arm_day_deletion_and_original_population_remain_visible():
    _,_,q,p,f=multi_day();before=f.copy(deep=True)
    out=vc.NearbyDateVolumeCohortEvaluator().evaluate(plan=p,inputs={**q.parameters,'candles':f})
    a=out['volume_cohort_comparison']
    complete=[pair for pair in out['pairs'] if all(v['status']=='resolved' for v in pair['outcomes'].values())]
    for row in a['leave_one_utc_day_out']:
        assert row['complete_pairs']==sum(all(pair[arm]['day']!=row['omitted_utc_day'] for arm in ('treated','control')) for pair in complete)
    assert out['sample_count']==out['matching_universe_count']
    assert out['feature_missing_reasons'] and out['unselected_controls']
    assert a['support_gates']['treated_matching']==(a['treated_matching_fraction']>=.5)
    assert out['dependence']['selected_pairs']['full_feature_support']['interval_count']==2*len(out['pairs'])
    pd.testing.assert_frame_equal(f,before)

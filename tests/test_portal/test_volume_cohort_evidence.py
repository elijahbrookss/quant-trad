"""Synthetic C2 protocol checks; no stored market inputs or scientific dispatch."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import math
import pandas as pd
import pytest

from market_data.frozen import semantic_hash
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan
from portal.backend.service.research import catalog
from portal.backend.service.research import volume_cohort_evaluator as vc
from portal.backend.service.research.candle_distribution_evaluator import CandleDistributionEvaluator
from portal.backend.service.research.raw_candle_evaluator import _prepare, _clock
from portal.backend.service.research.registry import (
    normalize_check_request, definition_allows_evidence, pin_check_definition_to_plan,
    reconstruct_pinned_check_definition)

START = datetime(2024, 4, 3, tzinfo=UTC)


def ts(i):
    return (START+timedelta(minutes=i)).isoformat().replace('+00:00','Z')


def fixture():
    payload = deepcopy(catalog._EXAMPLES['raw_forward_outcome@7'])
    payload['scope'].update(instrument_id='synthetic-other-asset', start=ts(1), end=ts(701))
    payload['outcomes'].update(max_examples=2, min_sample_count=1)
    rows = []
    for i in range(700):
        o,c = (101.,100.) if i%2 == 0 else (100.,101.)
        rows.append(dict(timestamp=ts(i), close_time=ts(i+1), known_at=ts(i+1),
            known_at_method='interval_close_inferred', source_identity_key='synthetic-other-venue',
            open=o,close=c,high=max(o,c)+1,low=min(o,c)-1,volume=20. if i%4 == 1 else 10.))
    d,r = normalize_check_request(payload)
    plan = ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION,request_hash=r.request_hash,
        market_data_requirements=({'alias':'primary_bars','timeframe_seconds':60,'instrument_id':'synthetic-other-asset'},),
        indicator_graph=(),evaluation_range={'start':ts(1),'end_exclusive':ts(701)},
        materialization_range={'start':ts(0),'end_exclusive':ts(701)},
        warmup={'bars':1,'timeframe_seconds':60},outcome_tail={'bars':0,'seconds':0},gap_policy='continue_degraded')
    return payload,d,r,plan,pd.DataFrame(rows)


def evaluate():
    _,_,r,p,f = fixture()
    return vc.VolumeCohortEvaluator().evaluate(plan=p, inputs={**r.parameters,'candles':f})


def test_full_universe_distribution_matches_v6_and_input_is_immutable():
    payload,d,r,p,f = fixture();before = f.copy(deep=True)
    out = vc.VolumeCohortEvaluator().evaluate(plan=p,inputs={**r.parameters,'candles':f})
    payload['outcomes'].pop('comparison');payload['outcomes']['schema_version']='raw_candle_distribution.v1'
    _,old = normalize_check_request(payload)
    baseline = CandleDistributionEvaluator().evaluate(plan=p,inputs={**old.parameters,'candles':f})
    assert out['matching_universe_count'] == baseline['sample_count']
    assert [{k:v for k,v in e.items() if k != 'matching_context'} for e in out['events']] == baseline['events']
    assert out['distribution'] == baseline['distribution']
    assert out['matching_universe_count'] > out['matching_feature_eligible_count']
    assert out['feature_missing_reasons']['prior_rms_return'] > 0
    assert out['volume_cohort_comparison']['decision'] == 'unsupported'  # Only one synthetic UTC day.
    pd.testing.assert_frame_equal(f,before)
    assert semantic_hash(out) == semantic_hash(evaluate())


def test_explicit_version_catalog_binding_and_no_new_feature_warmup():
    _,d,r,p,_ = fixture()
    assert d.evaluator_version == '7' and definition_allows_evidence(d)
    req = vc.VolumeCohortEvaluator().declare_requirements(definition=d,request=r)
    assert req['feature_lookback_bars'] == 1 and req['required_outcome_horizons'] == [5]
    assert 'volume' in req['primary_candle_required_fields']
    pinned,request,plan = pin_check_definition_to_plan(d,r,p)
    assert reconstruct_pinned_check_definition(request,plan).definition_hash == pinned.definition_hash
    description = catalog.get_check_definition('raw_forward_outcome','7')
    assert description['result_shape']['analysis_field'] == 'volume_cohort_comparison'
    assert description['settings']['fixed']['volume_cohort_rules']['support']['complete_pairs'] == 100


@pytest.mark.parametrize('key,value',[
    ('cohort','invented'),('matching_policy','outcome_nearest'),('prior_volatility_bars',True),
    ('prior_volatility_bars',1),('caliper_ratios',{'formation_body_fraction':1.25}),
    ('caliper_ratios',{f:1. for f in vc.FEATURES}),
    ('caliper_ratios',{f:float('nan') for f in vc.FEATURES}),
])
def test_invalid_or_incomplete_comparison_choices_fail(key,value):
    payload,*_ = fixture();payload['outcomes']['comparison'][key] = value
    with pytest.raises(ValueError): normalize_check_request(payload)


def test_supported_choices_change_definition_and_are_not_study_membership():
    payload,d,*_ = fixture()
    payload['outcomes']['comparison']['prior_volatility_bars']=12
    payload['outcomes']['comparison']['caliper_ratios']['formation_range_fraction']=1.5
    changed,r = normalize_check_request(payload)
    assert d.definition_hash != changed.definition_hash
    assert r.parameters['outcomes']['comparison']['prior_volatility_bars'] == 12
    payload['scope'].update(timeframe='15m',start='2025-06-01T00:00:00Z',end='2025-06-02T00:00:00Z')
    payload['outcomes'].update(entry_lag_bars=2,forward_bars=[3],direction='short')
    _,r = normalize_check_request(payload)
    assert r.parameters['outcomes']['direction'] == 'short'
    assert '2022-01' not in str(changed.material_rules)


def test_pair_freeze_precedes_outcome_resolution_and_missing_future_gets_no_replacement(monkeypatch):
    original_pair = vc.prepare_pairs; original_evaluate = CandleDistributionEvaluator.evaluate
    frozen = []
    def pair(*args,**kwargs):
        assert all('outcomes' not in c for c in args[0])
        value = original_pair(*args,**kwargs)
        frozen.append(deepcopy(value[0]))
        return value
    def resolve(self,**kwargs):
        assert frozen and frozen[-1]
        result = original_evaluate(self,**kwargs)
        chosen = frozen[-1][0]['control']['event_time']
        next(e for e in result['events'] if e['event_time']==chosen)['outcomes']['5'] = {
            'status':'unresolved','reason':'synthetic_missing_future'}
        return result
    monkeypatch.setattr(vc,'prepare_pairs',pair)
    monkeypatch.setattr(CandleDistributionEvaluator,'evaluate',resolve)
    missing = evaluate()
    monkeypatch.setattr(CandleDistributionEvaluator,'evaluate',original_evaluate)
    complete = evaluate()
    assert missing['feature_frozen_hash'] == complete['feature_frozen_hash']
    assert [(p['treated']['event_time'],p['control']['event_time']) for p in missing['pairs']] == [
        (p['treated']['event_time'],p['control']['event_time']) for p in complete['pairs']]
    assert missing['volume_cohort_comparison']['incomplete_pairs'] >= 1
    assert missing['volume_cohort_comparison']['missing_outcome_reasons']['control:synthetic_missing_future']==1


def candidate(i,treated,features=None):
    return {'event_time':ts(i),'event_close_time':ts(i+1),'entry_time':ts(i+2),'target_time':ts(i+7),
        'feature_start':ts(i-20),'day':ts(i+1)[:10],'stratum':ts(0),'treated':treated,
        'features':features or {f:0. for f in vc.FEATURES}}


def test_matching_ties_no_reuse_closed_endpoints_and_full_support_sensitivity():
    candidates=[candidate(30,True),candidate(20,False),candidate(40,False),candidate(32,True)]
    pairs,unmatched,_ = vc.prepare_pairs(candidates,features=vc.FEATURES,
        calipers=(math.log(1.25),)*3,support_start='feature_start')
    assert pairs[0]['control']['event_time']==ts(20)  # Equal distance chooses earlier.
    assert len({p['control']['event_time'] for p in pairs})==len(pairs)
    assert not pairs[0]['joint_nonoverlap_selected']  # Outcome disjoint; 20-prior-bar support overlaps.
    a,b=candidate(20,True),candidate(25,False)
    assert a['target_time']==b['entry_time']
    assert not vc.prepare_pairs([a,b],features=vc.FEATURES,calipers=(1.,)*3)[0]


def test_prior_context_definition_is_n_minus_one_returns_and_zero_stays_missing():
    payload,_,r,_,f = fixture()
    frame,rows = _prepare(f,step=timedelta(minutes=1),volume_required=True)
    choices = r.parameters['outcomes']['comparison']
    ledger,_,_,_ = vc.prepare_volume_candidates(frame,rows,detector=r.parameters['detector'],
        start=_clock(ts(1),'start'),end=_clock(ts(701),'end'),step=timedelta(minutes=1),choices=choices,lag=1,horizon=5)
    event = next(c for c in ledger if c['event_time']==ts(21))
    prior=[rows[i]['close'] for i in range(1,21)]
    expected=math.hypot(*(b/a-1 for a,b in zip(prior,prior[1:])))/math.sqrt(19)
    assert event['context']['prior_rms_return']==expected
    assert ledger[0]['feature_missing']==['prior_rms_return']
    flat=f.copy();flat['close']=100.;flat['open']=100.;flat['high']=101.;flat['low']=99.
    # A configurable event predicate is allowed, without hardcoded engulfing.
    detector={'all':[{'field':'volume','operator':'gte','value_field':'previous_volume'}]}
    frame,rows=_prepare(flat,step=timedelta(minutes=1),volume_required=True)
    ledger,candidates,_,_=vc.prepare_volume_candidates(frame,rows,detector=detector,start=_clock(ts(1),'start'),
        end=_clock(ts(701),'end'),step=timedelta(minutes=1),choices=choices,lag=1,horizon=5)
    assert ledger and not candidates
    assert 'prior_rms_return' in ledger[-1]['feature_missing']


def test_all_calendar_days_both_roles_dependence_and_extremes_are_retained():
    out=evaluate()
    assert out['coverage_by_utc_day']
    for row in out['coverage_by_utc_day'].values():
        for cohort in ('increasing_volume','non_increasing_volume'):
            assert row[cohort+':original']==row[cohort+':feature_eligible']+row[cohort+':feature_missing']
    dependence=out['dependence']['selected_pairs']
    assert dependence['full_feature_support']['component_count'] <= dependence['outcome_support']['component_count']
    assert dependence['outcome_support']['interval_count']==2*len(out['pairs'])
    assert out['volume_cohort_comparison']['selected_pair_balance']
    assert all('tails' in v for v in out['volume_cohort_comparison']['complete_pair_distributions'].values())


def test_missingness_retained_context_and_inferred_timing_are_explicit():
    _,_,r,p,f = fixture()
    f.loc[21,'known_at']=ts(24)
    f.loc[21,'volume']=0.
    out=vc.VolumeCohortEvaluator().evaluate(plan=p,inputs={**r.parameters,'candles':f})
    event=next(e for e in out['events'] if e['event_time']==ts(21))
    assert event['cohort']=='non_increasing_volume'
    assert 'formation_known_after_close' in event['matching_context']['feature_missing']
    assert event['volume']==0.
    populations=out['population_context']
    assert sum(v['event_count'] for v in populations['original'].values())==out['sample_count']
    assert sum(v['event_count'] for v in populations['selected'].values())==2*len(out['pairs'])
    assert populations['original']['increasing_volume']['context']['prior_rms_return']['unavailable_count']>0
    assert any('no dependence-adjusted uncertainty' in c for c in out['caveats'])


def test_resource_bounds_and_cancellation_fail_without_truncation(monkeypatch):
    _,_,r,p,f=fixture()
    monkeypatch.setitem(vc.RULES,'event_limit',1)
    with pytest.raises(ValueError,match='no truncation'):
        vc.VolumeCohortEvaluator().evaluate(plan=p,inputs={**r.parameters,'candles':f})
    monkeypatch.setitem(vc.RULES,'event_limit',10000)
    def cancel(): raise RuntimeError('synthetic cancellation')
    monkeypatch.setattr(vc,'execution_checkpoint',cancel)
    with pytest.raises(RuntimeError,match='synthetic cancellation'):
        vc.VolumeCohortEvaluator().evaluate(plan=p,inputs={**r.parameters,'candles':f})

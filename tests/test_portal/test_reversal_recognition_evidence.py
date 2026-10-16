"""Synthetic registered-method qualification only;no historical Check execution."""
from dataclasses import replace
import pandas as pd
import pytest
from research_science.check import ResolvedCheckPlan,CHECK_PLAN_SCHEMA_VERSION
from research_science.recognition import plain, utc
from tests.test_research_science.test_recognition import candles,t
from portal.backend.service.research import catalog
from portal.backend.service.research.registry import (normalize_check_request,definition_allows_evidence,
    pin_check_definition_to_plan,reconstruct_pinned_check_definition)
from portal.backend.service.research.reversal_recognition_evaluator import ReversalRecognitionEvaluator


def payload():
    return {'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':'mds_'+'a'*32,
        'scope':{'instrument_id':'instrument','timeframe':'15m','start':plain(t(1)),'end':plain(t(7))},
        'detector':{'type':'completed_reversal_candidates','computation_origin':plain(t(0)),
            'reference_basis':'rising_leg_only','ownership':'retain_all_provisional','max_bars':100,'max_candidates':100},
        'outcomes':{'schema_version':'reversal_recognition.v1'},'gap_policy':'reject'}


def fixture():
    p=payload();d,q=normalize_check_request(p)
    plan=ResolvedCheckPlan(schema_version=CHECK_PLAN_SCHEMA_VERSION,request_hash=q.request_hash,
        market_data_requirements=({'alias':'primary_bars','instrument_id':'instrument','timeframe_seconds':900},),
        indicator_graph=(),evaluation_range={'start':p['scope']['start'],'end_exclusive':p['scope']['end']},
        materialization_range={'start':plain(t(0)),'end_exclusive':p['scope']['end']},
        warmup={'bars':1,'timeframe_seconds':900},outcome_tail={'bars':0,'seconds':0},gap_policy='reject')
    frame=pd.DataFrame([{'timestamp':b.open_time,'close_time':b.close_time,'known_at':b.known_at,
        'source_identity_key':b.source_identity_key,'known_at_method':'interval_close_inferred',
        'open':b.open,'high':b.high,'low':b.low,'close':b.close} for b in candles()])
    return p,d,q,plan,frame


def test_new_version_is_discoverable_pinned_and_has_no_outcome_tail():
    _,d,q,plan,_=fixture();e=ReversalRecognitionEvaluator()
    assert d.evaluator_version=='9' and definition_allows_evidence(d)
    requirements=e.declare_requirements(definition=d,request=q)
    assert requirements['feature_lookback_bars']==1
    assert requirements['outcome_horizons']==[] and requirements['entry_lag_bars']==0
    pinned,request,planned=pin_check_definition_to_plan(d,q,plan)
    assert reconstruct_pinned_check_definition(request,planned).to_dict()==pinned.to_dict()
    desc=catalog.get_check_definition('raw_forward_outcome','9')
    assert desc['settings']['fixed']['recognition_rules']['recognition_status']=='provisional'
    assert desc['result_shape']['analytical_fields']['outcomes_calculated']['fixed'] is False


def test_registered_method_emits_source_linked_candidate_evidence_not_outcomes():
    p,d,q,plan,frame=fixture();before=frame.copy(deep=True)
    r=ReversalRecognitionEvaluator().evaluate(plan=plan,inputs={**q.parameters,'candles':frame})
    pd.testing.assert_frame_equal(frame,before)
    assert r['candidate_count']==1 and r['sample_count']==r['study_events_admitted']==0
    assert r['population_status']=='ownership_unresolved' and not r['outcomes_calculated']
    assert r['events'][0]['source_events'][0]['event_id']==r['reference_events'][0]['event_id']
    assert 'outcomes' not in r['events'][0]
    p['detector']['reference_basis']='rising_leg_plus_retreat'
    assert normalize_check_request(p)[0].definition_hash!=d.definition_hash


@pytest.mark.parametrize('edit',[
    lambda p:p['outcomes'].update(forward_bars=[4]),
    lambda p:p.update(statistics={'anything':1}),
    lambda p:p.update(assertions=[{'metric_path':'candidate_count','operator':'gt','threshold':0}]),
    lambda p:p.update(gap_policy='continue_degraded'),
    lambda p:p['detector'].update(ownership='latest_only'),
    lambda p:p['detector'].update(minimum_size=1),
    lambda p:p['detector'].update(computation_origin=plain(t(2))),
    lambda p:p['scope'].update(warmup_bars=3),
    lambda p:p['scope'].update(indicator_id='not_allowed'),
    lambda p:p['detector'].update(max_bars=True),
])
def test_unsupported_semantics_fail_before_input_access(edit):
    p=payload();edit(p)
    with pytest.raises(ValueError):normalize_check_request(p)


def test_missing_or_late_candles_and_changed_origin_are_not_silently_repaired():
    _,_,q,plan,frame=fixture();run=ReversalRecognitionEvaluator().evaluate
    with pytest.raises(ValueError):run(plan=plan,inputs={**q.parameters,'candles':frame.drop(index=3)})
    with pytest.raises(ValueError):run(plan=plan,inputs={**q.parameters,'candles':frame.iloc[1:]})
    delayed=frame.copy();delayed.loc[2,'known_at']=t(4)
    with pytest.raises(ValueError,match='close-available'):run(plan=plan,inputs={**q.parameters,'candles':delayed})
    changed=replace(plan,plan_hash='',materialization_range={'start':plain(t(1)),'end_exclusive':plain(t(7))})
    with pytest.raises(ValueError,match='origin'):run(plan=changed,inputs={**q.parameters,'candles':frame})


def test_real_planner_uses_fixed_origin_and_no_tail_without_market_reads():
    from portal.backend.service.research.planning import plan_research_check
    _,d,q,_,_=fixture()
    def indicators(ids,**kwargs):
        assert ids==[]
        return {'indicators':[],'requirements':[],'warmup_bars':0}
    p=plan_research_check(d,q,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'BTC-USD'},indicator_planner=indicators)
    assert utc(p.materialization_range['start'])==t(0)
    assert utc(p.materialization_range['end_exclusive'])==t(7)
    assert p.outcome_tail['seconds']==0


def test_visual_consumer_uses_identical_event_identity_and_masks_after_completion():
    from scripts.reporting.reversal_recognition_preview import prepare_preview
    from research_science.reversal_recognition import ReversalSettings
    _,_,q,plan,frame=fixture()
    result=ReversalRecognitionEvaluator().evaluate(plan=plan,inputs={**q.parameters,'candles':frame})
    data={'start_inclusive':plain(t(0)),
          'series_manifest':{'instrument_id':'instrument','timeframe_seconds':900},
          'records':[{'source_identity_key':'source','fact':{**vars(b)}} for b in candles()]}
    cases=[{'label':'synthetic','start':plain(t(0)),'end':plain(t(7)),
            'reference_known_at':plain(t(4))}]
    preview=prepare_preview(data,cases,settings=ReversalSettings('rising_leg_only','retain_all_provisional',100,100))
    shown=preview['examples'][0]
    assert shown['candidate']['event_id']==result['events'][0]['event_id']
    assert utc(shown['as_of'])==t(6) and len(shown['candles'])==6
    assert preview['study_events_admitted']==0 and not preview['outcomes_calculated']
    pending_case={**cases[0],'end':plain(t(5))}
    pending=prepare_preview(data,[pending_case],settings=ReversalSettings('rising_leg_only','retain_all_provisional',100,100))['examples'][0]
    assert pending['candidate'] is None and utc(pending['as_of'])==t(5)
    assert all(utc(s['known_at'])<=t(5) for s in pending['stages'])

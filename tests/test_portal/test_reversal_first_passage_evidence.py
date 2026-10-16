"""Prospective fixed-protocol adapter tests on synthetic candles only."""
from dataclasses import replace
import pandas as pd
import pytest
from market_data.frozen import semantic_hash
from portal.backend.service.research import catalog
from portal.backend.service.research.registry import (normalize_check_request, definition_allows_evidence,
    pin_check_definition_to_plan,reconstruct_pinned_check_definition)
from portal.backend.service.research.reversal_first_passage_evaluator import ReversalFirstPassageEvaluator
from tests.test_portal.test_reversal_recognition_evidence import payload,fixture
from tests.test_research_science.test_reversal_outcomes import path
from tests.test_research_science.test_recognition import t
from research_science.recognition import plain,utc


def configured():
    p=payload();p['outcomes']={'schema_version':'reversal_first_passage.v1'}
    p['detector']['reference_basis']='rising_leg_plus_retreat'
    p['scope']['end']=plain(t(19))
    d,q=normalize_check_request(p)
    _,_,_,plan,_=fixture()
    plan=replace(plan,request_hash=q.request_hash,plan_hash='',
        evaluation_range={'start':p['scope']['start'],'end_exclusive':p['scope']['end']},
        materialization_range={'start':plain(t(0)),'end_exclusive':p['scope']['end']})
    _,bars=path('continuation')
    frame=pd.DataFrame([{'timestamp':b.open_time,'close_time':b.close_time,'known_at':b.known_at,
        'source_identity_key':'source','known_at_method':'interval_close_inferred',
        'open':b.open,'high':b.high,'low':b.low,'close':b.close} for b in bars])
    return p,d,q,plan,frame


def test_explicit_version_and_real_planner_keep_frozen_boundary_without_tail():
    from portal.backend.service.research.planning import plan_research_check
    _,d,q,_,_=configured()
    assert d.evaluator_version=='10' and definition_allows_evidence(d)
    description=catalog.get_check_definition('raw_forward_outcome','10')
    assert description['settings']['fixed']['first_passage_rules']['horizon_bars']==12
    p=plan_research_check(d,q,inspect_coverage=False,instrument_loader=lambda key:{'id':key,'symbol':'test'},
        indicator_planner=lambda *args,**kwargs:{'indicators':[],'requirements':[],'warmup_bars':0})
    assert utc(p.materialization_range['start'])==t(0) and utc(p.materialization_range['end_exclusive'])==t(19)
    assert p.outcome_tail['seconds']==0
    pinned,request,plan=pin_check_definition_to_plan(d,q,p)
    assert reconstruct_pinned_check_definition(request,plan).definition_hash==pinned.definition_hash


def test_every_chain_is_retained_with_explicit_components_and_outcomes():
    _,_,q,plan,frame=configured()
    result=ReversalFirstPassageEvaluator().evaluate(plan=plan,inputs={**q.parameters,'candles':frame})
    assert result['candidate_count']==result['first_passage_summary']['chain_count']==1
    assert result['sample_count']==1 and 'not independent' in result['sample_count_meaning']
    assert result['events'][0]['first_passage']['classification']=='continuation'
    assert result['first_passage_summary']['equal_component_weighted_shares']['continuation']==1
    assert not result['first_passage_summary']['independence_claim']


@pytest.mark.parametrize('change',[
    lambda p:p['detector'].update(reference_basis='rising_leg_only'),
    lambda p:p['outcomes'].update(horizon_bars=24),
    lambda p:p['outcomes'].update(entry='next_open'),
    lambda p:p['scope'].update(timeframe='1m'),
    lambda p:p['detector'].update(max_bars=100000),
    lambda p:p.update(statistics={'bootstrap':100}),
    lambda p:p.update(assertions=[{'metric_path':'candidate_count','operator':'gt','threshold':0}]),
])
def test_no_unapproved_grid_or_semantic_changes(change):
    p,*_=configured();change(p)
    with pytest.raises(ValueError):normalize_check_request(p)


def test_shared_preparation_preserves_recognition_only_v9_result_exactly():
    from portal.backend.service.research.reversal_recognition_evaluator import ReversalRecognitionEvaluator
    _,_,q,p,f=fixture()
    result=ReversalRecognitionEvaluator().evaluate(plan=p,inputs={**q.parameters,'candles':f})
    assert semantic_hash(result)=='ad4ba3ee95ec4e698bbeab65fd8fc9661ea9d627bd75e6487b59fb8f7bf478e5'

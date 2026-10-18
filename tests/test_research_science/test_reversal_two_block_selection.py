"""Synthetic clean-prefix invariance; no historical ETH recognition or outcomes."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import json
import pytest
from research_science.recognition import RecognitionContext,RecognitionEvent,RecognitionStage
from research_science.reversal_two_block_selection import select_two_blocks,ORIGIN,START,END,RULES
from research_science.reversal_six_block_selection import select_six_blocks,LEGACY_CHOICES
from research_science.study import stable_hash
from tests.test_research_science.test_reversal_selection import fixture


def test_two_full_blocks_preserve_cases_control_choices_and_tie_namespace():
    bars,_=fixture(count=19104);delta=ORIGIN-bars[0].open_time
    bars=tuple(replace(b,open_time=b.open_time+delta,close_time=b.close_time+delta,known_at=b.known_at+delta) for b in bars)
    context=RecognitionContext('synthetic',900,ORIGIN,'source')
    events=[]
    for index in (4000,7000,13000):
        at=bars[index].close_time
        events.append(RecognitionEvent('synthetic','1',stable_hash({**LEGACY_CHOICES,'max_bars':19104}),context,
            'provisional_reversal_candidate',bars[index-5].open_time,at,at,
            (RecognitionStage('completion',at,at,bars[index].candle_id),),()))
    full=select_six_blocks(bars,SimpleNamespace(candidates=tuple(events),references=()))
    prefix=select_two_blocks(bars[:8352],SimpleNamespace(candidates=tuple(events[:2]),references=()))
    prior=[c for c in full['cases'] if c['block']<2]
    assert len(prefix['cases'])==len(prior)==2 and len(prefix['pairs'])==2
    for before,after in zip(prior,prefix['cases']):
        for key in before.keys()-{'full_population_moment_weight','matched_population_moment_weight'}:
            assert before[key]==after[key]
    full_pairs={p['event_id']:p for p in full['pairs']}
    for p in prefix['pairs']:
        assert p['control']==full_pairs[p['event_id']]['control']
        assert p['distance_squared']==full_pairs[p['event_id']]['distance_squared']
    assert prefix['populations']['combined']['selection_summary']==prefix['selection_summary']
    saved=json.loads(json.dumps(prefix))
    assert saved['selection_manifest_hash']==stable_hash({k:saved[k] for k in saved['selection_manifest_fields']})


def payload():
    from tests.test_portal.test_reversal_six_block_selection_evidence import payload as six_payload
    p=six_payload();p['scope']['end']='2025-03-29T00:00:00Z'
    p['outcomes']={'schema_version':'reversal_two_block_selection.v1'}
    return p


def test_scope_plan_is_two_blocks_without_changed_matching_or_event_namespace():
    from portal.backend.service.research.registry import normalize_check_request
    from portal.backend.service.research.planning import plan_research_check
    from portal.backend.service.research.catalog import get_check_definition
    from research_science.reversal_six_block_selection import RULES as six_rules
    p=payload();definition,request=normalize_check_request(p)
    assert definition.evaluator_version=='14'
    plan=plan_research_check(definition,request,inspect_coverage=False,instrument_loader=lambda key:{'id':key,'symbol':'ETH-USD'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    from research_science.recognition import utc
    assert utc(plan.materialization_range['start'])==ORIGIN and utc(plan.materialization_range['end_exclusive'])==END
    assert plan.outcome_tail['bars']==plan.outcome_tail['seconds']==0
    assert p['detector']['max_bars']==19104
    for k in six_rules.keys()-{'version','evaluation','populations'}:assert RULES[k]==six_rules[k]
    detail=get_check_definition('raw_forward_outcome','14')
    assert detail['result_shape']['analysis_field']=='selection_summary'
    assert normalize_check_request(deepcopy(detail['examples'][0]['request']))[0].evaluator_version=='14'


@pytest.mark.parametrize('change',[
 lambda p:p['scope'].update(end='2025-04-25T07:30:00Z'),
 lambda p:p['detector'].update(computation_origin='2025-04-25T08:00:00Z'),
 lambda p:p['detector'].update(max_bars=8352),
 lambda p:p.update(gap_policy='continue_degraded'),
 lambda p:p['outcomes'].update(sigma_ratio=2.),
])
def test_no_silent_window_reset_namespace_or_scientific_rule_change(change):
    from portal.backend.service.research.registry import normalize_check_request
    p=payload();change(p)
    with pytest.raises(ValueError):normalize_check_request(p)


def test_actual_loader_consumes_exact_clean_prefix_and_rejects_gap(monkeypatch):
    import pandas as pd
    from portal.backend.service.research import execution
    from portal.backend.service.research.registry import normalize_check_request
    from portal.backend.service.research.planning import plan_research_check
    from tests.test_research_science.test_recognition import candles as pattern
    definition,request=normalize_check_request(payload())
    plan=plan_research_check(definition,request,inspect_coverage=False,instrument_loader=lambda key:{'id':key,'symbol':'ETH-USD'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    opened=pd.date_range('2025-01-01',periods=8352,freq='15min',tz='UTC')
    prices=[100+i*.001 for i in range(len(opened))]
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),'known_at':opened+timedelta(minutes=15),
        'source_identity_key':'synthetic','known_at_method':'interval_close_inferred','open':prices,
        'high':[p+.01 for p in prices],'low':[p-.01 for p in prices],'close':prices})
    for start in (4000,7000):
        for offset,bar in enumerate(pattern()):
            for field in ('open','high','low','close'):frame.loc[start+offset,field]=getattr(bar,field)
    before=frame.copy(deep=True)
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**k:frame)
    loaded=execution._load_market_inputs(definition,request,plan,resolver=SimpleNamespace(dataset_binding={'recorded_gaps':[]}))
    result=execution._evaluate(definition,request,plan,loaded)
    pd.testing.assert_frame_equal(frame,before)
    assert result['processed_bars']==8352 and not result['outcomes_calculated']
    assert set(result['populations'])=={'block1','block2','combined'}
    assert all(result['populations'][n]['selection_summary']['all']['chains']>0 for n in ('block1','block2'))
    with pytest.raises(ValueError,match='8352bar'):
        execution._evaluate(definition,request,plan,{**loaded,'candles':frame.drop(index=5000)})

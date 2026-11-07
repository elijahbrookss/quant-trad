"""Exact bounded configured native input through normal planning and loader."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import pandas as pd
import pytest

from research_science.recognition import utc
from portal.backend.service.research import catalog,execution
from tests.test_portal.test_reversal_population_evidence import planned,native_binding,SOURCE
from tests.test_research_science.test_recognition import candles as pattern


def payload():
    p=deepcopy(catalog.get_check_definition('raw_forward_outcome','20')['examples'][0]['request'])
    p['scope'].update(start='2025-05-24T00:00:00Z',end='2025-07-19T00:00:00Z')
    p['detector']['computation_origin']='2025-04-25T08:00:00Z'
    p['outcomes']['populations']=[{'name':'may24_jun21','start_block':0,'block_count':1},
        {'name':'jun21_jul19','start_block':1,'block_count':1}]
    return p


def test_explicit_reusable_schema_plans_actual_suffix_without_outcome_tail():
    definition,request,plan=planned(payload())
    assert definition.evaluator_version=='20'
    assert plan.execution['warmup_floor_bars']==2752
    assert plan.outcome_tail['seconds']==0
    assert utc(plan.materialization_range['start'])==utc('2025-04-25T08:00:00Z')
    detail=catalog.get_check_definition('raw_forward_outcome','20')
    assert detail['result_shape']['analysis_field']=='populations'
    assert 'All completed' in detail['result_shape']['analytical_fields']['sample_count']['meaning']


@pytest.mark.parametrize('change',[
    lambda p:p['outcomes'].update(calipers={'r':2}),
    lambda p:p['detector'].update(max_bars=8128),
    lambda p:p['detector'].update(reference_basis='rising_leg_only'),
    lambda p:p.update(mode='preview'),lambda p:p.update(dataset_id=None),
    lambda p:p.update(assertions=[{'metric_path':'sample_count','operator':'gte','threshold':1}]),
    lambda p:p.update(gap_policy='continue_degraded'),
])
def test_new_profile_does_not_relax_native_frozen_scientific_or_identity_contracts(change):
    p=payload();change(p)
    with pytest.raises(ValueError):planned(p)


def test_actual8128_loader_all_completed_four_labels_and_three_fixed_views(monkeypatch):
    definition,request,plan=planned(payload())
    plan=replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':10})
    binding=native_binding(request,plan,8128)
    opened=pd.date_range('2025-04-25T08:00:00Z',periods=8128,freq='15min')
    prices=[100+i*.001 for i in range(8128)]
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),'known_at':opened+timedelta(minutes=15),
        'source_identity_key':SOURCE,'known_at_method':'interval_close_inferred','open':prices,'close':prices,
        'high':[v+.01 for v in prices],'low':[v-.01 for v in prices]})
    for start in (3000,6000):
        for offset,bar in enumerate(pattern()):
            for field in ('open','high','low','close'):frame.loc[start+offset,field]=getattr(bar,field)
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**k:frame)
    loaded=execution._load_market_inputs(definition,request,plan,resolver=SimpleNamespace(dataset_binding=binding))
    loaded['frozen_input_binding']=binding
    before=frame.copy(deep=True)
    result=execution._evaluate(definition,request,plan,loaded)
    assert result['processed_bars']==8128 and result['candidate_count']>0
    assert len(result['cases'])==result['candidate_count']
    assert set(result['populations'])=={'combined','may24_jun21','jun21_jul19'}
    assert 'pairs' not in result
    assert all('contrasts' not in v and 'completion_day_deletions' not in v for v in result['populations'].values())
    assert set(result['populations']['combined']['outcomes'])=={'continuation','failure','neither','unresolved'}
    assert all(r['recovery_candles']>=1 for r in result['cases'])
    pd.testing.assert_frame_equal(frame,before)
    with pytest.raises(ValueError,match='actual frozen input binding'):
        execution._evaluate(definition,request,plan,{k:v for k,v in loaded.items() if k!='frozen_input_binding'})
    bad=deepcopy(binding);bad['series'][0]['row_count']=8127
    with pytest.raises(ValueError):execution._evaluate(definition,request,plan,{**loaded,'frozen_input_binding':bad})

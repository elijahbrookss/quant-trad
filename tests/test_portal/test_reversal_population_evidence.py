"""Configurable selection uses existing frozen planning/loading without outcomes."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import pandas as pd
import pytest

from portal.backend.service.research import catalog,execution
from portal.backend.service.research.registry import normalize_check_request,definition_allows_evidence
from portal.backend.service.research.planning import plan_research_check
from tests.test_portal.test_saved_selection_measurements import synthetic,SOURCE


def payload():
    return deepcopy(catalog.get_check_definition('raw_forward_outcome','17')['examples'][0]['request'])


def planned(p):
    definition,request=normalize_check_request(p)
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'SYNTHETIC'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    return definition,request,plan


def test_same_public_recipe_admits_other_symbols_and_dates_with_no_outcome_support():
    p=payload();p['scope'].update(instrument_id='other-symbol',start='2024-07-02T00:00:00Z',end='2024-08-27T00:00:00Z')
    p['detector']['computation_origin']='2024-06-01T00:00:00Z'
    definition,request,plan=planned(p)
    assert definition.evaluator_version=='17' and definition_allows_evidence(definition)
    assert plan.execution['warmup_floor_bars']==2976 and plan.execution['outcome_horizons']==[]
    assert plan.outcome_tail['seconds']==0
    detail=catalog.get_check_definition('raw_forward_outcome','17')
    assert detail['result_shape']['analysis_field']=='selection_summary'


@pytest.mark.parametrize('change',[
    lambda p:p['outcomes'].update(sigma_ratio=2),lambda p:p['detector'].update(max_bars=8352),
    lambda p:p['detector'].update(reference_basis='rising_leg_only'),
    lambda p:p.update(gap_policy='continue_degraded'),
    lambda p:p.update(assertions=[{'metric_path':'sample_count','operator':'gte','threshold':1}]),
    lambda p:p['outcomes']['populations'].append({'name':'third','start_block':0,'block_count':1}),
])
def test_no_scientific_threshold_namespace_or_outcome_gate_tuning(change):
    p=payload();change(p)
    with pytest.raises(ValueError):planned(p)


def test_loader_matches_fixed_profile_and_fails_missing_native_binding(monkeypatch):
    from tests.test_research_science.test_recognition import candles as pattern
    p=payload();definition,request,plan=planned(p)
    plan=replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':10})
    item,_,_,_=synthetic();binding=item['payload']['evidence']['input_binding']
    opened=pd.date_range('2025-01-01',periods=8352,freq='15min',tz='UTC');prices=[100+i*.001 for i in range(8352)]
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),'known_at':opened+timedelta(minutes=15),
        'source_identity_key':SOURCE,'known_at_method':'interval_close_inferred','open':prices,'close':prices,
        'high':[v+.01 for v in prices],'low':[v-.01 for v in prices]})
    for start in (4000,7000):
        for offset,bar in enumerate(pattern()):
            for field in ('open','high','low','close'):frame.loc[start+offset,field]=getattr(bar,field)
    before=frame.copy(deep=True)
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**k:frame)
    loaded=execution._load_market_inputs(definition,request,plan,resolver=SimpleNamespace(dataset_binding=binding))
    loaded['frozen_input_binding']=binding
    result=execution._evaluate(definition,request,plan,loaded)
    fixed=deepcopy(p);fixed['outcomes']={'schema_version':'reversal_two_block_selection.v1'}
    old_definition,old_request,old_plan=planned(fixed)
    old=execution._evaluate(old_definition,old_request,old_plan,{**loaded,'outcomes':fixed['outcomes']})
    for key in ('cases','pairs','dependence','selection_summary'):assert result[key]==old[key]
    assert result['candidate_count']>0 and result['outcomes_calculated'] is False
    pd.testing.assert_frame_equal(before,frame)
    with pytest.raises(ValueError,match='actual frozen input binding'):
        execution._evaluate(definition,request,plan,{k:v for k,v in loaded.items() if k!='frozen_input_binding'})

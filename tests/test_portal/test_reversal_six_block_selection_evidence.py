"""Fixed expanded plan and actual loader boundary, using synthetic data only."""
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
import pytest
from portal.backend.service.research import catalog, execution
from portal.backend.service.research.registry import normalize_check_request, definition_allows_evidence
from portal.backend.service.research.planning import plan_research_check
from research_science.recognition import utc


def payload():
    return {'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':'mds_'+'c'*32,
        'scope':{'instrument_id':'instrument','timeframe':'15m','start':'2025-02-01T00:00:00Z','end':'2025-07-19T00:00:00Z'},
        'detector':{'type':'completed_reversal_candidates','computation_origin':'2025-01-01T00:00:00Z',
            'reference_basis':'rising_leg_plus_retreat','ownership':'retain_all_provisional',
            'max_bars':19104,'max_candidates':10000},
        'outcomes':{'schema_version':'reversal_selection_only.v2'},'gap_policy':'reject'}


def planned():
    definition,request=normalize_check_request(payload())
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'synthetic'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    return definition,request,plan


def test_expanded_catalogue_and_plan_pin_zero_outcomes_and_fixed_window():
    definition,request,plan=planned()
    assert definition.evaluator_version=='12' and definition_allows_evidence(definition)
    assert utc(plan.materialization_range['start'])==utc('2025-01-01T00:00:00Z')
    assert utc(plan.materialization_range['end_exclusive'])==utc('2025-07-19T00:00:00Z')
    assert plan.outcome_tail['bars']==plan.outcome_tail['seconds']==0
    detail=catalog.get_check_definition('raw_forward_outcome','12')
    assert detail['result_shape']['analysis_field']=='selection_summary'
    assert 'populations' in detail['result_shape']['analytical_fields']
    assert detail['result_shape']['analytical_fields']['outcomes_calculated']['fixed'] is False
    assert normalize_check_request(deepcopy(detail['examples'][0]['request']))[0].evaluator_version=='12'
    assert catalog.get_check_definition('raw_forward_outcome','11')['definition']['definition_hash']=='2d895750174e9f927dfe1941766e6a08f3b81d4f89dfb7518edffd0a0983bc78'


@pytest.mark.parametrize('change',[
    lambda p:p['scope'].update(end='2025-08-01T00:00:00Z'),
    lambda p:p['scope'].update(start='2025-02-02T00:00:00Z'),
    lambda p:p['detector'].update(computation_origin='2024-12-01T00:00:00Z'),
    lambda p:p['detector'].update(max_bars=20000),
    lambda p:p['detector'].update(max_candidates=9999),
    lambda p:p['outcomes'].update(sigma_ratio=1.5),
    lambda p:p.update(assertions=[{'metric_path':'sample_count','operator':'gte','threshold':200}]),
    lambda p:p.update(gap_policy='continue_degraded'),
])
def test_no_date_origin_limit_or_scientific_rule_tuning(change):
    p=payload();change(p)
    with pytest.raises(ValueError):normalize_check_request(p)


def test_real_loader_evaluator_boundary_consumes_full_synthetic_path_and_partitions(monkeypatch):
    import pandas as pd
    from tests.test_research_science.test_recognition import candles as pattern
    definition,request,plan=planned()
    opened=pd.date_range('2025-01-01',periods=19104,freq='15min',tz='UTC')
    prices=[100+i*.001 for i in range(len(opened))]
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),
        'known_at':opened+timedelta(minutes=15),'source_identity_key':'synthetic',
        'known_at_method':'interval_close_inferred','open':prices,'high':[p+.01 for p in prices],
        'low':[p-.01 for p in prices],'close':prices})
    for start in (4000,13000):
        for offset,bar in enumerate(pattern()):
            for field in ('open','high','low','close'):
                frame.loc[start+offset,field]=getattr(bar,field)
    before=frame.copy(deep=True)
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**k:frame)
    loaded=execution._load_market_inputs(definition,request,plan,resolver=SimpleNamespace(dataset_binding={'recorded_gaps':[]}))
    assert 'inputs' not in loaded
    result=execution._evaluate(definition,request,plan,loaded)
    pd.testing.assert_frame_equal(frame,before)
    assert result['processed_bars']==19104 and not result['outcomes_calculated']
    assert set(result['populations'])=={'old','new','combined'}
    assert result['populations']['old']['selection_summary']['all']['chains']>0
    assert result['populations']['new']['selection_summary']['all']['chains']>0
    assert result['populations']['combined']['selection_summary']==result['selection_summary']
    assert all(c['timing']['schema_version']=='reversal_stage_availability.v3' for c in result['cases'])
    with pytest.raises(ValueError,match='fixed2025six-block'):
        execution._evaluate(definition,request,plan,{**loaded,'inputs':[{'alias':'unexpected'}]})

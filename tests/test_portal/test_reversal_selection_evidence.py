"""Fixed selected protocol version/planning/cancellation tests;no2025 data."""
from copy import deepcopy
import pytest
from portal.backend.service.research import catalog
from portal.backend.service.research.registry import (normalize_check_request, CHECK_REGISTRY,
    definition_allows_evidence,pin_check_definition_to_plan,reconstruct_pinned_check_definition)
from portal.backend.service.research.planning import plan_research_check
from portal.backend.service.research.reversal_selection_evaluator import ReversalSelectionEvaluator
from research_science.recognition import utc


def payload():
    return {'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':'mds_'+'b'*32,
        'scope':{'instrument_id':'instrument','timeframe':'15m','start':'2025-02-01T00:00:00Z','end':'2025-04-26T00:00:00Z'},
        'detector':{'type':'completed_reversal_candidates','computation_origin':'2025-01-01T00:00:00Z',
            'reference_basis':'rising_leg_plus_retreat','ownership':'retain_all_provisional',
            'max_bars':12000,'max_candidates':10000},
        'outcomes':{'schema_version':'reversal_selection_only.v1'},'gap_policy':'reject'}


def test_real_selection_definition_catalogue_and_plan_never_require_outcomes():
    definition,request=normalize_check_request(payload())
    assert definition.evaluator_version=='11' and definition_allows_evidence(definition)
    desc=catalog.get_check_definition('raw_forward_outcome','11')
    assert desc['result_shape']['analysis_field']=='selection_summary'
    assert desc['result_shape']['analytical_fields']['outcomes_calculated']['fixed'] is False
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'synthetic'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    assert utc(plan.materialization_range['start'])==utc('2025-01-01T00:00:00Z')
    assert utc(plan.materialization_range['end_exclusive'])==utc('2025-04-26T00:00:00Z')
    assert plan.outcome_tail['bars']==plan.outcome_tail['seconds']==0
    pinned,q,p=pin_check_definition_to_plan(definition,request,plan)
    assert reconstruct_pinned_check_definition(q,p).definition_hash==pinned.definition_hash
    example=deepcopy(desc['examples'][0]['request'])
    assert normalize_check_request(example)[0].evaluator_version=='11'


@pytest.mark.parametrize('change',[
    lambda p:p['outcomes'].update(sigma_ratio=1.5),
    lambda p:p['outcomes'].update(forward_bars=[12]),
    lambda p:p['outcomes'].update(weighting='chain'),
    lambda p:p['detector'].update(reference_basis='rising_leg_only'),
    lambda p:p['detector'].update(expiry_bars=12),
    lambda p:p['scope'].update(end='2025-04-27T00:00:00Z'),
    lambda p:p['scope'].update(timeframe='1m'),
    lambda p:p['scope'].update(warmup_bars=97),
    lambda p:p.update(statistics={'min_pairs':200}),
    lambda p:p.update(gap_policy='continue_degraded'),
    lambda p:p.update(assertions=[{'metric_path':'sample_count','operator':'gte','threshold':200}]),
])
def test_protocol_changes_fail_before_reading_input(change):
    value=payload();change(value)
    with pytest.raises(ValueError):normalize_check_request(value)


def test_method_has_no_outcome_import_or_dispatch():
    import inspect
    from portal.backend.service.research import reversal_selection_evaluator as adapter
    from research_science import reversal_selection as domain
    assert 'reversal_outcomes' not in inspect.getsource(adapter)
    assert 'first_passage(' not in inspect.getsource(domain)
    declaration=ReversalSelectionEvaluator().declare_requirements(definition=None,request=normalize_check_request(payload())[1])
    assert declaration['outcome_horizons']==declaration['required_outcome_horizons']==[]


def test_admitted_adapter_consumes_only_its_synthetic_prefix_and_no_future_tail():
    import pandas as pd
    from datetime import timedelta
    definition,request=normalize_check_request(payload())
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'synthetic'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    opened=pd.date_range('2025-01-01',periods=11040,freq='15min',tz='UTC')
    prices=[100+i*.001 for i in range(len(opened))]
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),
        'known_at':opened+timedelta(minutes=15),'source_identity_key':'synthetic',
        'known_at_method':'interval_close_inferred','open':prices,'high':[p+.01 for p in prices],
        'low':[p-.01 for p in prices],'close':prices})
    before=frame.copy(deep=True)
    result=ReversalSelectionEvaluator().evaluate(plan=plan,inputs={**request.parameters,'candles':frame})
    pd.testing.assert_frame_equal(frame,before)
    assert not result['outcomes_calculated'] and result['cases']==result['pairs']==[]
    assert result['processed_bars']==11040
    late=frame.copy();late.loc[100,'known_at']=late.loc[100,'close_time']+timedelta(seconds=1)
    with pytest.raises(ValueError,match='close-available'):
        ReversalSelectionEvaluator().evaluate(plan=plan,inputs={**request.parameters,'candles':late})


def test_standard_loader_to_registered_evaluator_accepts_its_real_input_contract(monkeypatch):
    from datetime import timedelta
    from types import SimpleNamespace
    import pandas as pd
    from portal.backend.service.research import execution
    from tests.test_research_science.test_recognition import candles as formation_candles
    definition,request=normalize_check_request(payload())
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'synthetic'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    opened=pd.date_range('2025-01-01',periods=11040,freq='15min',tz='UTC')
    prices=[100+i*.001 for i in range(len(opened))]
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),
        'known_at':opened+timedelta(minutes=15),'source_identity_key':'synthetic',
        'known_at_method':'interval_close_inferred','open':prices,'high':[p+.01 for p in prices],
        'low':[p-.01 for p in prices],'close':prices})
    for offset,bar in enumerate(formation_candles()):
        for field in ('open','high','low','close'):
            frame.loc[4000+offset,field]=getattr(bar,field)
    before=frame.copy(deep=True)
    calls=[]
    def fetch(instrument_id,start,end,timeframe,*,frozen_alias):
        calls.append((instrument_id,start,end,timeframe,frozen_alias))
        return frame
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',fetch)
    loaded=execution._load_market_inputs(definition,request,plan,
        resolver=SimpleNamespace(dataset_binding={'recorded_gaps':[]}))
    assert 'inputs' not in loaded  # Runtime payload is not request.parameters.
    assert calls==[('instrument',plan.materialization_range['start'],
        plan.materialization_range['end_exclusive'],'15m','primary_bars')]
    result=execution._evaluate(definition,request,plan,loaded)
    pd.testing.assert_frame_equal(frame,before)
    assert result['processed_bars']==11040 and result['candidate_count']>=1
    assert result['candidate_count']==len(result['cases'])
    assert result['analysis_status']=='selection_feasibility_only'
    assert not result['outcomes_calculated']
    assert result['promotion_authority'] is result['execution_authority'] is False
    assert plan.outcome_tail['bars']==plan.outcome_tail['seconds']==0
    with pytest.raises(ValueError,match='fixed smaller-design'):
        execution._evaluate(definition,request,plan,{**loaded,'inputs':[{'alias':'unexpected'}]})

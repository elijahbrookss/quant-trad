"""Planning and saved-reference adapter tests; no historical outcome execution."""
from copy import deepcopy
from dataclasses import replace
import pytest
from portal.backend.service.research import catalog
from portal.backend.service.research.registry import normalize_check_request,definition_allows_evidence
from portal.backend.service.research.planning import plan_research_check
from portal.backend.service.research import reversal_matched_outcomes_evaluator as adapter
from research_science.recognition import utc


def payload():
    return {'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':adapter.DATASET_ID,
        'scope':{'instrument_id':adapter.INSTRUMENT_ID,'timeframe':'15m','start':'2025-02-01T00:00:00Z','end':'2025-07-19T00:00:00Z'},
        'detector':deepcopy(adapter.DETECTOR),'outcomes':deepcopy(adapter.OUTCOMES),'gap_policy':'reject'}


def test_saved_reference_catalog_and_plan_bind_original_input_without_tail_or_rematching():
    definition,request=normalize_check_request(payload())
    assert definition.evaluator_version=='13' and definition_allows_evidence(definition)
    plan=plan_research_check(definition,request,inspect_coverage=False,instrument_loader=lambda key:{'id':key,'symbol':'BTC-USD'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    assert utc(plan.materialization_range['start'])==utc('2025-01-01T00:00:00Z')
    assert utc(plan.materialization_range['end_exclusive'])==utc('2025-07-19T00:00:00Z')
    assert plan.outcome_tail['seconds']==0 and plan.execution['outcome_horizons']==[12]
    detail=catalog.get_check_definition('raw_forward_outcome','13')
    assert detail['result_shape']['analysis_field']=='matched_outcome_comparison'
    assert normalize_check_request(deepcopy(detail['examples'][0]['request']))[0].evaluator_version=='13'
    assert catalog.get_check_definition('raw_forward_outcome','10')['definition']['material_rules']['first_passage_rules']['horizon_bars']==12


@pytest.mark.parametrize('change',[
 lambda p:p['outcomes']['selection_reference'].update(result_hash='a'*64),
 lambda p:p['scope'].update(instrument_id='ETH'),
 lambda p:p['scope'].update(end='2025-07-20T00:00:00Z'),
 lambda p:p['outcomes'].update(horizon_bars=24),
 lambda p:p.update(assertions=[{'metric_path':'sample_count','operator':'gte','threshold':1}]),
])
def test_no_selection_substitution_horizon_or_scientific_gate(change):
    p=payload();change(p)
    with pytest.raises(ValueError):normalize_check_request(p)


@pytest.mark.parametrize('field,value',[('mode','preview'),('dataset_id','mds_'+'a'*32)])
def test_requirements_reject_mutable_preview_and_other_dataset(field,value):
    definition,request=normalize_check_request(payload())
    request=replace(request,request_hash='',**{field:value})
    with pytest.raises(ValueError,match='original B3 Dataset'):
        adapter.ReversalMatchedOutcomesEvaluator().declare_requirements(definition=definition,request=request)


def saved_item(monkeypatch):
    from research_science.check import CheckEvidenceBinding,CheckResult,CHECK_EVIDENCE_BINDING_SCHEMA_VERSION
    from tests.test_research_science.test_check import _frozen_binding
    from research_science.study import stable_hash
    binding=_frozen_binding();binding['max_commit_seq']=223194126;binding.pop('binding_hash',None)
    monkeypatch.setattr(adapter,'DATASET_ID',binding['dataset_id'])
    monkeypatch.setattr(adapter,'DATASET_HASH',binding['dataset_hash'])
    evidence=CheckEvidenceBinding(schema_version=CHECK_EVIDENCE_BINDING_SCHEMA_VERSION,
        definition_hash='definition',request_hash='request',plan_hash='plan',code_revision='synthetic',
        evidence_kind='frozen_market_data',input_binding=binding,indicator_graph_hash='graph',
        indicator_output_hash='output',fact_input_hash='facts',gap_transition_hash='transitions',quality_hash='quality',gaps_hash='gaps')
    selection={'rules':{},'cases':[],'pairs':[{'synthetic':i} for i in range(999)],'dependence':{},
        'populations':{n:{'selection_summary':{'chains_per_completion_clock_histogram':{1:1,10:1}}} for n in ('old','new','combined')},
        'compatibility':{}}
    manifest_hash=stable_hash(selection)
    selection.update(schema_version='reversal_selection_only_result.v2',selection_manifest_hash=manifest_hash,outcomes_calculated=False)
    result=CheckResult(schema_version='research.check_result.v2',definition_hash='definition',request_hash='request',plan_hash='plan',
        evidence_hash=evidence.evidence_hash,evaluator_id='raw_forward_outcome',evaluator_version='12',result=selection)
    reference={'check_id':'synthetic-selection','result_hash':result.result_hash,'evidence_hash':evidence.evidence_hash,'selection_manifest_hash':manifest_hash}
    monkeypatch.setattr(adapter,'SELECTION_REFERENCE',reference)
    import json
    item=json.loads(json.dumps({'id':reference['check_id'],'kind':'research_check','status':'tested',
        'payload':{'replayable':True,'observation_eligible':True,'result':result.to_dict(),'evidence':evidence.to_dict()}}))
    monkeypatch.setattr(adapter.repository,'get_item',lambda key:deepcopy(item))
    return item


def test_saved_check_typed_hashes_and_historical_integer_key_decoding(monkeypatch):
    item=saved_item(monkeypatch)
    before=deepcopy(item)
    selection,binding=adapter.load_saved_selection()
    assert len(selection['pairs'])==999 and binding['max_commit_seq']==223194126
    assert item==before


@pytest.mark.parametrize('change',[
 lambda item:item['payload']['result']['result']['pairs'][0].update(synthetic='changed'),
 lambda item:item['payload']['evidence'].update(code_revision='changed'),
 lambda item:item.update(status='draft'),
 lambda item:item['payload'].update(observation_eligible=False),
])
def test_changed_saved_result_or_evidence_fails_without_replay(monkeypatch,change):
    item=saved_item(monkeypatch);change(item)
    with pytest.raises(ValueError):adapter.load_saved_selection()


def test_actual_market_loader_consumer_boundary_never_recognizes_or_rematches(monkeypatch):
    import pandas as pd
    from types import SimpleNamespace
    from portal.backend.service.research import execution
    from tests.test_research_science.test_reversal_matched_outcomes import fixture
    selection,overrides,_=fixture()
    opened=pd.date_range('2025-01-01',periods=19104,freq='15min',tz='UTC')
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+pd.Timedelta(minutes=15),
        'known_at':opened+pd.Timedelta(minutes=15),'source_identity_key':'source','known_at_method':'interval_close_inferred',
        'open':100.,'high':100.5,'low':99.5,'close':100.})
    by_time={v:i for i,v in enumerate(opened)}
    for candle in overrides:
        i=by_time[candle.open_time]
        frame.loc[i,'high']=candle.high;frame.loc[i,'low']=candle.low
    selection['source_identity_key']='source'
    monkeypatch.setattr(adapter,'load_saved_selection',lambda:(deepcopy(selection),{'dataset_id':adapter.DATASET_ID,'dataset_hash':adapter.DATASET_HASH}))
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**k:frame)
    definition,request=normalize_check_request(payload())
    plan=plan_research_check(definition,request,inspect_coverage=False,instrument_loader=lambda key:{'id':key,'symbol':'BTC-USD'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    plan=replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':223194126})
    loaded=execution._load_market_inputs(definition,request,plan,resolver=SimpleNamespace(dataset_binding={'recorded_gaps':[]}))
    before=frame.copy(deep=True)
    result=execution._evaluate(definition,request,plan,loaded)
    pd.testing.assert_frame_equal(frame,before)
    assert result['processed_bars']==19104 and len(result['pairs'])==3 and not result['reselection_performed']
    assert result['matched_outcome_comparison']['old']['pairs']==2
    assert result['matched_outcome_comparison']['new']['pairs']==1
    import json
    from research_science.study import stable_hash
    saved=json.loads(json.dumps(result))
    assert saved['matched_outcome_manifest_hash']==stable_hash({k:saved[k] for k in saved['matched_outcome_manifest_fields']})
    # Stored CheckResult intentionally strips check_id metadata recursively;
    # the source selection identity must survive under its material field.
    from research_science.check import CheckResult, CHECK_RESULT_SCHEMA_VERSION
    typed = CheckResult(schema_version=CHECK_RESULT_SCHEMA_VERSION,
        definition_hash=definition.definition_hash, request_hash=request.request_hash,
        plan_hash=plan.plan_hash, evidence_hash='synthetic-evidence',
        evaluator_id=definition.evaluator_id, evaluator_version=definition.evaluator_version,
        result=result)
    retained = CheckResult.from_dict(json.loads(json.dumps(typed.to_dict()))).result
    assert retained['selection_reference']['source_check_id']==adapter.SELECTION_REFERENCE['check_id']
    assert 'check_id' not in retained['selection_reference']
    assert retained['matched_outcome_manifest_hash']==stable_hash({k:retained[k] for k in retained['matched_outcome_manifest_fields']})
    wrong=replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':223194127})
    with pytest.raises(ValueError,match='watermark required'):execution._evaluate(definition,request,wrong,loaded)

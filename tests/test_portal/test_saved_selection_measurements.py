"""Synthetic typed saved populations: shared binding, composition and legacy parity."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from market_data.frozen import build_frozen_market_data_read_binding, frozen_subject_snapshot_hash
from portal.backend.service.research import catalog, execution
from portal.backend.service.research import saved_selection_measurements_evaluator as adapter
from portal.backend.service.research.saved_selection_binding import load_saved_selection, MANIFEST_FIELDS
from portal.backend.service.research.registry import normalize_check_request, pin_check_definition_to_plan, definition_allows_evidence
from portal.backend.service.research.planning import plan_research_check
from research_science.check import CheckResult, CheckEvidenceBinding
from research_science.recognition import utc, plain, RecognitionCandle
from research_science.reversal_selection import moment_weights
from research_science.reversal_six_block_selection import RULES as SIX_BLOCK_RULES
from research_science.reversal_two_block_selection import RULES as TWO_BLOCK_RULES
from research_science.reversal_matched_outcomes import describe_selected_pairs
from research_science.reversal_matched_progress import describe_progress
from research_science.study import stable_hash

CHECK_ID = '11111111-1111-4111-8111-111111111111'
SOURCE = 'e'*64


def planned(payload):
    definition, request = normalize_check_request(payload)
    plan = plan_research_check(definition, request, inspect_coverage=False,
        instrument_loader=lambda key: {'id': key, 'symbol': 'SYNTHETIC'},
        indicator_planner=lambda *a, **k: {'indicators': [], 'requirements': [], 'warmup_bars': 0})
    return definition, request, plan


def seal_selection(selection, version):
    material = deepcopy({k: selection[k] for k in MANIFEST_FIELDS})
    if version == '14':
        material = json.loads(json.dumps(material))
        selection['selection_manifest_fields'] = list(MANIFEST_FIELDS)
    selection['selection_manifest_hash'] = stable_hash(material)


def reseal(item):
    payload = item['payload']
    result = CheckResult.from_dict({**payload['result'], 'result_hash': ''})
    payload['result'] = result.to_dict()
    return {'source_check_id': item['id'], 'result_hash': result.result_hash,
        'evidence_hash': payload['evidence']['evidence_hash'],
        'selection_manifest_hash': result.result['selection_manifest_hash']}


def synthetic(version='14'):
    from tests.test_research_science.test_reversal_matched_outcomes import fixture
    selection, bars, _ = fixture()
    rows = 8352 if version == '14' else 19104
    end = '2025-03-29T00:00:00Z' if version == '14' else '2025-07-19T00:00:00Z'
    if version == '14':
        selection['cases'] = selection['cases'][:2]; selection['pairs'] = selection['pairs'][:2]
        for pair in selection['pairs']: pair['completion_moment_weight'] = .5
    origin = utc('2025-01-01T00:00:00Z'); step = timedelta(minutes=15)
    weights = moment_weights(selection['cases'])
    for case in selection['cases']:
        at = utc(case['decision_at']); n = case['formation_candles']
        case.update(formation_start=plain(at-n*step), support_start=plain(max(origin,at-98*step)),
            support_end=plain(at+12*step), full_population_moment_weight=weights[case['event_id']],
            matched_population_moment_weight=weights[case['event_id']])
    for pair in selection['pairs']:
        control = pair['control']; at = utc(control['decision_at']); n = control['trailing_candles']
        control.update(trailing_start=plain(at-n*step),support_start=plain(max(origin,at-98*step)),support_end=plain(at+12*step))
    count, clocks = len(selection['cases']), len({c['decision_at'] for c in selection['cases']})
    selection['selection_summary'].update(all={'chains':count,'completion_clocks':clocks},matched={'chains':count,'completion_clocks':clocks},case_status_counts={'matched':count})
    for pop in selection['populations'].values():
        pop['selection_summary'] = {'chains_per_completion_clock_histogram': {2:1, 11:1}}
    selection.update(schema_version=('reversal_two_block_selection_result.v1' if version == '14' else 'reversal_selection_only_result.v2'),
        status='completed',outcomes_calculated=False,source_identity_key=SOURCE,computation_origin=plain(origin),
        processed_bars=rows,candidate_count=count,sample_count=clocks,evaluation_range={'start':'2025-02-01T00:00:00Z','end_exclusive':end},
        dependence={'ancestry': 'synthetic retained'},rules=deepcopy(SIX_BLOCK_RULES if version=='12' else TWO_BLOCK_RULES),compatibility={'synthetic':True})
    seal_selection(selection,version)
    payload = deepcopy(catalog.get_check_definition('raw_forward_outcome',version)['examples'][0]['request'])
    payload.update(mode='evidence',dataset_id='mds_'+'a'*32)
    payload['scope']['instrument_id'] = 'instrument-1'
    definition, request, plan = planned(payload)
    plan = replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':10})
    definition,request,plan = pin_check_definition_to_plan(definition,request,plan)
    source = {'mode':'exact','series_id':1,'resolved_source_identity_keys':[SOURCE],
        'sources':{SOURCE:{'source_kind':'historical_api','adapter_version':'SyntheticProvider:candle.ohlcv.v1'}},
        'source_summary':{'counts':{SOURCE:rows}}}
    binding = build_frozen_market_data_read_binding(dataset_id=request.dataset_id,dataset_hash='a'*64,max_commit_seq=10,
        subjects=[{'instrument_id':'instrument-1','snapshot':{'id':'instrument-1','symbol':'SYNTHETIC'},'snapshot_hash':frozen_subject_snapshot_hash({'id':'instrument-1','symbol':'SYNTHETIC'})}],series=[{'alias':'primary_bars','series_id':1,'instrument_id':'instrument-1',
            'fact_type':'candle.ohlcv','contract_version':'candle.ohlcv.v1','timeframe_seconds':900,
            'range_start':plain(origin),'range_end':end,'row_count':rows,'max_commit_seq':10,
            'material_hash':'material','provenance_hash':'provenance','quality_hash':'quality',
            'source_binding':source,'loaded_range':{'start':plain(origin),'end_exclusive':end},
            'requirement':dict(plan.market_data_requirements[0])}])
    evidence = CheckEvidenceBinding(schema_version='research.check_evidence_binding.v2',definition_hash=definition.definition_hash,
        request_hash=request.request_hash,plan_hash=plan.plan_hash,code_revision='synthetic',evidence_kind='frozen_market_data',
        input_binding=binding,indicator_graph_hash='graph',indicator_output_hash='output',fact_input_hash='facts',
        gap_transition_hash='transitions',quality_hash='quality',gaps_hash='gaps')
    result = CheckResult(schema_version='research.check_result.v2',definition_hash=definition.definition_hash,
        request_hash=request.request_hash,plan_hash=plan.plan_hash,evidence_hash=evidence.evidence_hash,
        evaluator_id='raw_forward_outcome',evaluator_version=version,result=selection)
    item = json.loads(json.dumps({'id':CHECK_ID,'kind':'research_check','status':'tested','instrument_id':'instrument-1','timeframe':'15m',
        'replayable':True,'observation_eligible':True,'payload':{'replayable':True,'observation_eligible':True,
        'definition':definition.to_dict(),'request':request.to_dict(),'plan':plan.to_dict(),'result':result.to_dict(),'evidence':evidence.to_dict()}}))
    ctx = {'dataset_id':binding['dataset_id'],'dataset_hash':binding['dataset_hash'],'input_binding_hash':binding['binding_hash'],
        'max_commit_seq':10,'instrument_id':'instrument-1','input_start':plain(origin),'input_end_exclusive':end,
        'decision_start':'2025-02-01T00:00:00Z','decision_end_exclusive':end,'source_identity_key':SOURCE,
        'input_rows':rows,'case_count':count,'pair_count':count,'completion_clock_count':clocks}
    return item,reseal(item),ctx,bars


def payload(ref,ctx):
    return {'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':ctx['dataset_id'],
        'scope':{'instrument_id':ctx['instrument_id'],'timeframe':'15m','start':ctx['decision_start'],'end':ctx['decision_end_exclusive']},
        'detector':deepcopy(adapter.DETECTOR),'outcomes':{'schema_version':adapter.SCHEMA,'selection_reference':ref,'frozen_context':ctx},'gap_policy':'reject'}


@pytest.mark.parametrize('version',['12','14'])
def test_typed_saved_context_and_manifest_round_trip_without_replay(version):
    item,ref,ctx,_ = synthetic(version);before=deepcopy(item);calls=[]
    saved=load_saved_selection(ref,ctx,item_loader=lambda key:(calls.append(key) or item))
    assert calls==[CHECK_ID] and item==before
    assert saved.evaluator_version==version and saved.context['pair_count']==len(saved.selection['pairs'])
    if version=='12':
        assert stable_hash({k:saved.selection[k] for k in MANIFEST_FIELDS}) != ref['selection_manifest_hash']


@pytest.mark.parametrize('key,value', [('dataset_hash','f'*64),('max_commit_seq',11),('instrument_id','other'),
    ('source_identity_key','f'*64),('pair_count',1),('completion_clock_count',2),('case_count',3),('input_binding_hash','f'*64)])
def test_declared_context_is_not_authority(key,value):
    item,ref,ctx,_=synthetic();ctx[key]=value
    with pytest.raises(ValueError,match='saved_selection_invalid'):
        load_saved_selection(ref,ctx,item_loader=lambda key:item)


@pytest.mark.parametrize('change',[
    lambda i:i.update(status='draft'),lambda i:i.update(replayable=False),lambda i:i['payload'].update(observation_eligible=False),
    lambda i:i['payload']['result']['result']['cases'][0].update(formation_candles=2),
    lambda i:i['payload']['evidence'].update(code_revision='changed'),
    lambda i:i['payload']['plan']['materialization_range'].update(as_of_commit_seq=11),
    lambda i:i['payload']['definition']['material_rules'].update(changed=True),
])
def test_changed_envelope_or_ineligible_source_is_rejected(change):
    item,ref,ctx,_=synthetic();change(item)
    with pytest.raises(ValueError):load_saved_selection(ref,ctx,item_loader=lambda key:item)


@pytest.mark.parametrize('change',[
    lambda s:s['cases'][0].update(support_end='2025-03-01T00:00:00Z'),
    lambda s:s['cases'][0].update(full_population_moment_weight=.1),
    lambda s:s['pairs'][0].update(completion_moment_weight=.1),
    lambda s:s['pairs'][0]['control'].update(trailing_candles=2),
    lambda s:s['pairs'][1]['control'].update(decision_at=s['pairs'][0]['control']['decision_at']),
    lambda s:s['selection_summary']['matched'].update(chains=1),
    lambda s:s['rules'].update(horizon_bars=4),
])
def test_resigned_semantically_inconsistent_selection_is_rejected(change):
    item,ref,ctx,_=synthetic();selection=item['payload']['result']['result'];change(selection)
    seal_selection(selection,'14');ref=reseal(item)
    with pytest.raises(ValueError,match='saved_selection_invalid'):
        load_saved_selection(ref,ctx,item_loader=lambda key:item)


def test_plan_catalog_and_no_tail_or_db_lookup_during_declaration():
    item,ref,ctx,_=synthetic();definition,request,plan=planned(payload(ref,ctx))
    assert definition.evaluator_version=='16' and definition_allows_evidence(definition)
    assert plan.outcome_tail['seconds']==0 and plan.execution['outcome_horizons']==[12]
    assert utc(plan.materialization_range['start'])==utc(ctx['input_start'])
    assert catalog.get_check_definition('raw_forward_outcome','16')['result_shape']['analysis_field']=='measurements'
    planned(catalog.get_check_definition('raw_forward_outcome','16')['examples'][0]['request'])


@pytest.mark.parametrize('change',[
    lambda p:p['scope'].update(end='2025-03-30T00:00:00Z'),lambda p:p['detector'].update(tune=True),
    lambda p:p['outcomes'].update(target=.5),lambda p:p.update(statistics={'bins':2}),
    lambda p:p.update(gap_policy='continue_degraded'),lambda p:p.update(assertions=[{'metric_path':'sample_count','operator':'gte','threshold':1}]),
    lambda p:p['outcomes']['frozen_context'].update(pair_count=1000),
    lambda p:p['outcomes']['frozen_context'].update(completion_clock_count=3),
    lambda p:p['scope'].update(warmup_bars=0),
])
def test_no_scope_tuning_protocol_expansion_or_scientific_gate(change):
    item,ref,ctx,_=synthetic();p=payload(ref,ctx);change(p)
    with pytest.raises(ValueError):planned(p)


def test_composed_actual_loader_matches_both_pure_kernels_and_retains_input(monkeypatch):
    item,ref,ctx,bars=synthetic();definition,request,plan=planned(payload(ref,ctx))
    plan=replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':10})
    opened=pd.date_range('2025-01-01',periods=8352,freq='15min',tz='UTC')
    frame=pd.DataFrame({'timestamp':opened,'close_time':opened+timedelta(minutes=15),'known_at':opened+timedelta(minutes=15),
        'source_identity_key':SOURCE,'known_at_method':'interval_close_inferred','open':100.,'high':100.5,'low':99.5,'close':100.})
    index={v:i for i,v in enumerate(opened)}
    for bar in bars:
        if bar.open_time in index:
            frame.loc[index[bar.open_time],'high']=bar.high;frame.loc[index[bar.open_time],'low']=bar.low
    before=frame.copy(deep=True);source_before=deepcopy(item)
    monkeypatch.setattr(adapter.repository,'get_item',lambda key:deepcopy(item))
    monkeypatch.setattr(execution.candle_service,'fetch_ohlcv_by_instrument',lambda *a,**k:frame)
    binding=item['payload']['evidence']['input_binding']
    loaded=execution._load_market_inputs(definition,request,plan,resolver=SimpleNamespace(dataset_binding=binding))
    loaded['frozen_input_binding']=binding
    result=execution._evaluate(definition,request,plan,loaded)
    pd.testing.assert_frame_equal(before,frame);assert item==source_before
    _,rows=adapter._prepare(frame,step=timedelta(minutes=15),volume_required=False)
    candles=tuple(RecognitionCandle.from_mapping(v) for v in rows)
    selection=item['payload']['result']['result']
    passage=describe_selected_pairs(selection,candles,evaluation_end=utc(ctx['input_end_exclusive']))
    progress=describe_progress(selection,passage,candles,evaluation_end=utc(ctx['input_end_exclusive']))
    assert result['data_quality']==loaded.get('data_quality',{})
    assert result['pairs']==progress['pairs']
    assert result['measurements']['first_passage']==passage['matched_outcome_comparison']['combined']
    assert result['measurements']['progress']==progress['progress_comparison']['combined']
    assert result['outcome_provenance']['kind']=='computed_in_this_check' and not result['reselection_performed']
    retained=json.loads(json.dumps(result));assert stable_hash({k:retained[k] for k in retained['measurement_manifest_fields']})==retained['measurement_manifest_hash']
    for field,value in [('max_commit_seq',11),('dataset_hash','f'*64)]:
        wrong=deepcopy(binding);wrong[field]=value
        with pytest.raises(ValueError,match='actual frozen input binding'):
            execution._evaluate(definition,request,plan,{**loaded,'frozen_input_binding':wrong})
    with pytest.raises(ValueError,match='actual frozen input binding'):
        execution._evaluate(definition,request,plan,{k:v for k,v in loaded.items() if k!='frozen_input_binding'})


def reseal_binding(item):
    evidence_payload=item['payload']['evidence']
    binding=evidence_payload['input_binding'];binding.pop('binding_hash',None)
    for series in binding['series']:series.pop('source_binding_hash',None)
    evidence=CheckEvidenceBinding.from_dict({**evidence_payload,'evidence_hash':'','input_hash':''})
    item['payload']['evidence']=evidence.to_dict()
    item['payload']['result']['evidence_hash']=evidence.evidence_hash
    return reseal(item),evidence.input_binding['binding_hash']


@pytest.mark.parametrize('change',[
    lambda b:b['series'][0]['source_binding']['sources'][SOURCE].update(source_kind='derived'),
    lambda b:b['series'][0]['source_binding']['sources'][SOURCE].update(adapter_version='coarsen-candles.v1'),
    lambda b:b['series'][0]['source_binding']['source_summary'].update(counts={SOURCE:1}),
    lambda b:b['series'][0].update(disclosed_gaps=[{'reason':'missing'}]),
    lambda b:b['series'][0]['loaded_range'].update(start='2025-01-01T00:15:00Z'),
    lambda b:b['series'][0]['requirement'].update(known_at_required=False),
])
def test_resigned_non_native_gap_or_inconsistent_frozen_binding_is_rejected(change):
    item,ref,ctx,_=synthetic();change(item['payload']['evidence']['input_binding'])
    ref,binding_hash=reseal_binding(item);ctx['input_binding_hash']=binding_hash
    with pytest.raises(ValueError,match='saved_selection_invalid'):
        load_saved_selection(ref,ctx,item_loader=lambda key:item)


def test_handoff_binding_is_not_new_historical_execution_hash_material():
    _,ref,ctx,_=synthetic();_,_,plan=planned(payload(ref,ctx))
    inputs={'indicator_evidence':{},'fact_records_by_alias':{},'fact_requirements_by_alias':{}}
    before=execution._execution_input_hashes(plan,inputs)
    assert before==execution._execution_input_hashes(plan,{**inputs,'frozen_input_binding':{'synthetic':'readonly handoff'}})


@pytest.mark.parametrize('mode,dataset',[('preview',None),('evidence','mds_'+'f'*32)])
def test_no_mutable_or_other_dataset_execution(mode,dataset):
    _,ref,ctx,_=synthetic();p=payload(ref,ctx);p.update(mode=mode,dataset_id=dataset)
    with pytest.raises(ValueError,match='original frozen Dataset'):planned(p)

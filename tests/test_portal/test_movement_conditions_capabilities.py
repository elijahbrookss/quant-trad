"""Synthetic custody, training-clock and no-replay capability regressions."""
from copy import deepcopy
from datetime import timedelta
import math
import pytest
from market_data.frozen import semantic_hash
from portal.backend.service.research import training_exceedance_evaluator as extreme
from portal.backend.service.research import saved_score_attribution_evaluator as attribution
from portal.backend.service.research.candle_target_benchmark_evaluator import TARGET_VARIANCE
from portal.backend.service.research.registry import pin_check_definition_to_plan, reconstruct_pinned_check_definition, normalize_check_request
from tests.test_portal.test_candle_state_comparison_v2 import BASE, STEP, candles, contract, iso, run
from tests.test_portal.test_candle_target_benchmark_evaluator import payload

def extreme_payload():
    p=payload();p['outcomes']['schema_version']=extreme.SCHEMA
    p['outcomes']['target']={'operator':extreme.OPERATOR,'base_target':TARGET_VARIANCE,'quantile':.9,
        'quantile_method':'linear','threshold_fit':'fold_training_only','comparison':'strict_gt','tie':'class_zero','freeze':'validation'}
    return p

def test_exceedance_worker_roundtrip_and_fold_freeze():
    p=extreme_payload();definition,request,plan=contract(p)
    definition,request,plan=pin_check_definition_to_plan(definition,request,plan)
    assert reconstruct_pinned_check_definition(type(request).from_dict(request.to_dict()),type(plan).from_dict(plan.to_dict())).to_dict()==definition.to_dict()
    result=run(p=p)
    assert result['method_version']=='25'
    assert result['comparison']['target']==extreme.OPERATOR
    for f in result['folds']:
        threshold=f['target_transform']['threshold']
        assert f['target_transform']['maximum_training_label_known_at']<f['target_transform']['known_at_cutoff']
        assert all(r['target_threshold']==threshold for r in result['scoring_rows'] if r['fold_id']==f['id'])
    for r in result['scoring_rows']:
        assert r['actual_target']==float(r['raw_continuous_target']>r['target_threshold'])
        assert r['maximum_admitted_label_known_at']<r['decision_time']

def synthetic_rows(values):
    return [{'decision_time':iso(BASE+STEP*i),'target_time':iso(BASE+STEP*(i+1)),
             'outcome_known_at':iso(BASE+STEP*(i+1)),
             'features':{'x':float(i)},'outcomes':{'primary':{'positive':False,'path_variation':v}}} for i,v in enumerate(values)]

def test_quantile_linear_strict_tie_and_future_poison():
    rows=synthetic_rows([0.,1.,2.,3.,4.,9.,100.])
    fold={'train':{'start':iso(BASE),'end':iso(BASE+STEP*5)}}
    stats={'purge_bars':0,'target_transform':{'quantile':.5}}
    transformed,receipt=extreme.transform_fold(rows,fold,stats,900)
    assert receipt['threshold']==1.5  # known-at equal cutoff is excluded
    assert receipt['training_count']==4
    poisoned=deepcopy(rows);poisoned[5]['outcomes']['primary']['path_variation']=1e100
    assert extreme.transform_fold(poisoned,fold,stats,900)[1]==receipt
    tie_rows=synthetic_rows([2.]*7)
    tied,_=extreme.transform_fold(tie_rows,fold,stats,900)
    assert all(r['outcomes']['primary']['positive'] is False for r in tied)
    later=deepcopy(fold);later['train']['end']=iso(BASE+STEP*7)
    assert extreme.transform_fold(rows,later,stats,900)[1]['threshold']!=receipt['threshold']

@pytest.mark.parametrize('mutation',[
    lambda p:p['outcomes']['target'].update(quantile=1),
    lambda p:p['outcomes']['target'].update(threshold_fit='full_sample'),
    lambda p:p['outcomes']['target'].update(comparison='gte'),
    lambda p:p['statistics']['rolling_history'].update(label_availability='known_at_lte_prediction')])
def test_exceedance_rejects_noncausal_contract(mutation):
    p=extreme_payload();mutation(p)
    with pytest.raises(ValueError):normalize_check_request(p)

@pytest.fixture
def saved():
    p=payload(target=TARGET_VARIANCE);definition,request,_=contract(p);result=run(p=p)
    membership={f['id']:attribution.training_clocks(result,f,result['statistics_configuration'],3600,native_seconds=900) for f in result['folds'] if f['status']=='valid'}
    return {'reference':{'check_id':'11111111-1111-4111-8111-111111111111','result_hash':'a'*64,'evidence_hash':'b'*64},
            'result':result,'request':request,'training_rows':membership}

def attribution_stats():
    return {'causal_strata':[{'feature':'atr3','quantiles':[.5,.9],'quantile_method':'linear'}],
            'minimum_interval_rows':100,'minimum_interval_clusters':6}

def test_original_membership_hash_is_mandatory(saved):
    result=saved['result'];fold=next(f for f in result['folds'] if f['status']=='valid')
    assert len(saved['training_rows'][fold['id']])==fold['train_count']
    bad=deepcopy(fold);bad['training_clock_hash']='0'*64
    with pytest.raises(ValueError,match='membership unavailable'):
        attribution.training_clocks(result,bad,result['statistics_configuration'],3600,native_seconds=900)

def test_full_population_attribution_reconciles_and_marks_sparse_intervals(saved):
    measured=attribution.attribute_source(saved,attribution_stats())
    for name,totals in measured['full_population'].items():
        assert totals['positive_mass']>=0 and totals['negative_mass']<=0
        assert math.isclose(totals['positive_mass']+totals['negative_mass'],totals['signed_sum'],rel_tol=1e-12)
        for dimension in {r['dimension'] for r in measured['tables']}:
            assert math.isclose(math.fsum(r['benchmarks'][name]['signed_sum'] for r in measured['tables'] if r['dimension']==dimension),totals['signed_sum'],rel_tol=1e-12,abs_tol=1e-24)
    assert all(r['interval']['status']=='unsupported' for r in measured['tables'])
    assert measured['partition_hash']==semantic_hash({'assignments':measured['assignments']})

def test_signed_zero_and_offset_shares_remain_unclipped():
    rows=[{'rolling_global_loss':3.,'forecaster_loss':0.},{'rolling_global_loss':0.,'forecaster_loss':2.}]
    assert attribution.mass(rows[:1],'rolling_global',{'signed_sum':1.,'absolute_mass':5.})['signed_share']==3.
    assert attribution.mass(rows,'rolling_global',{'signed_sum':0.,'absolute_mass':5.})['signed_share'] is None

@pytest.mark.parametrize('flag,value',[('replayable',False),('observation_eligible',False),('replayable',1),('observation_eligible','true')])
def test_source_loader_requires_literal_persisted_flags(flag,value):
    ref={'check_id':'11111111-1111-4111-8111-111111111111','result_hash':'a'*64,'evidence_hash':'b'*64}
    item={'id':ref['check_id'],'kind':'research_check','status':'tested','payload':{'replayable':True,'observation_eligible':True}}
    item['payload'][flag]=value
    with pytest.raises(ValueError,match='persisted eligibility'):
        attribution.load_source(ref,item_loader=lambda _:item)

def test_attribution_missing_feature_kept_and_unknown_feature_refused(saved):
    for row in saved['result']['selection_audit']['rows']:row['feature_context']['atr3']=None
    result=attribution.attribute_source(saved,attribution_stats())
    assert all('missing_or_unreconstructible' in r['strata']['prior'] for r in result['assignments'])
    stats=attribution_stats();stats['causal_strata'][0]['feature']='invented'
    with pytest.raises(ValueError,match='absent'):attribution.attribute_source(saved,stats)


def source_envelope():
    from dataclasses import replace
    from market_data.frozen import build_frozen_market_data_read_binding, frozen_subject_snapshot_hash
    from research_science.check import CheckEvidenceBinding, CheckResult
    from portal.backend.service.research.planning import plan_research_check
    from portal.backend.service.research.registry import CHECK_REGISTRY
    p=payload(target=TARGET_VARIANCE);definition,request=normalize_check_request(p)
    plan=plan_research_check(definition,request,inspect_coverage=False,
        instrument_loader=lambda key:{'id':key,'symbol':'SYNTHETIC'},
        indicator_planner=lambda *a,**k:{'indicators':[],'requirements':[],'warmup_bars':0})
    plan=replace(plan,plan_hash='',materialization_range={**plan.materialization_range,'as_of_commit_seq':10})
    definition,request,plan=pin_check_definition_to_plan(definition,request,plan)
    import pandas as pd
    frame=candles()
    clocks=pd.to_datetime(frame['timestamp'],utc=True)
    frame=frame[(clocks>=pd.Timestamp(plan.materialization_range['start'])) &
                (clocks<pd.Timestamp(plan.materialization_range['end_exclusive']))]
    count=len(frame)
    result=CHECK_REGISTRY.resolve_evaluator(definition).evaluate(plan=plan,inputs={**request.parameters,'candles':frame,'data_quality':{'status':'clean'}})
    source=result['selection_audit']['rows'][0]['source_identity_key']
    source_binding={'mode':'exact','series_id':1,'resolved_source_identity_keys':[source],
        'sources':{source:{'source_kind':'historical_api','adapter_version':'SyntheticProvider:candle.ohlcv.v1'}},
        'source_summary':{'counts':{source:count}}}
    snapshot={'id':'instrument-1','symbol':'SYNTHETIC'}
    binding=build_frozen_market_data_read_binding(dataset_id=request.dataset_id,dataset_hash='a'*64,max_commit_seq=10,
        subjects=[{'instrument_id':'instrument-1','snapshot':snapshot,'snapshot_hash':frozen_subject_snapshot_hash(snapshot)}],
        series=[{'alias':'primary_bars','series_id':1,'instrument_id':'instrument-1','fact_type':'candle.ohlcv',
            'contract_version':'candle.ohlcv.v1','timeframe_seconds':900,'range_start':plan.materialization_range['start'],
            'range_end':plan.materialization_range['end_exclusive'],'row_count':count,'max_commit_seq':10,
            'material_hash':'material','provenance_hash':'provenance','quality_hash':'quality',
            'source_binding':source_binding,'requirement':dict(plan.market_data_requirements[0])}])
    evidence=CheckEvidenceBinding(schema_version='research.check_evidence_binding.v2',definition_hash=definition.definition_hash,
        request_hash=request.request_hash,plan_hash=plan.plan_hash,code_revision='synthetic',evidence_kind='frozen_market_data',
        input_binding=binding,indicator_graph_hash=semantic_hash({'indicators':[]}),indicator_output_hash='output',fact_input_hash='facts',
        gap_transition_hash='transitions',quality_hash=semantic_hash(dict(binding.get('quality') or {})),gaps_hash=semantic_hash({'recorded_gaps':[]}))
    result=CheckResult(schema_version='research.check_result.v2',definition_hash=definition.definition_hash,request_hash=request.request_hash,
        plan_hash=plan.plan_hash,evidence_hash=evidence.evidence_hash,evaluator_id='raw_forward_outcome',evaluator_version='24',
        result={**result,'promotion_authority':False,'execution_authority':False})
    item={'id':'11111111-1111-4111-8111-111111111111','kind':'research_check','status':'tested','instrument_id':'instrument-1','timeframe':'15m',
          'payload':{'schema_version':'research_check_payload.v2','replayable':True,'observation_eligible':True,
              'definition':definition.to_dict(),'request':request.to_dict(),'plan':plan.to_dict(),'result':result.to_dict(),'evidence':evidence.to_dict()}}
    ref={'check_id':item['id'],'result_hash':result.result_hash,'evidence_hash':evidence.evidence_hash}
    return item,ref

def test_real_raw_envelope_and_saved_route_never_materializes_market(monkeypatch):
    from portal.backend.service.research import catalog,execution,service
    from portal.backend.service.research.planning import plan_research_check
    item,ref=source_envelope()
    monkeypatch.setattr(attribution.repository,'get_item',lambda _:item)
    loaded=attribution.load_source(ref)
    assert loaded['training_rows']
    p=deepcopy(catalog.get_check_definition('raw_forward_outcome','26')['examples'][0]['request'])
    p['scope']=deepcopy(item['payload']['request']['scope']);p['dataset_id']=item['payload']['request']['dataset_id']
    p['outcomes']['sources']=[ref];p['statistics']=attribution_stats()
    definition,request=normalize_check_request(p)
    plan=plan_research_check(definition,request,inspect_coverage=False)
    definition,request,plan=pin_check_definition_to_plan(definition,request,plan)
    assert not plan.market_data_requirements
    assert reconstruct_pinned_check_definition(request,plan).to_dict()==definition.to_dict()
    monkeypatch.setattr(execution,'_load_market_inputs',lambda *a,**k:pytest.fail('market replay forbidden'))
    monkeypatch.setattr(execution,'evidence_source_revision',lambda:'synthetic')
    final,evidence,result=execution.execute_check_evidence(definition,request,plan)
    assert evidence.evidence_kind=='saved_check_evidence'
    envelope={'schema_version':'research_check_payload.v2','definition':definition.to_dict(),'request':request.to_dict(),'plan':final.to_dict(),
              'evidence':evidence.to_dict(),'result':result.to_dict()}
    service._validate_research_check_evidence_payload(envelope)
    assert result.result['market_replay_performed'] is False
    changed={**ref,'result_hash':'0'*64}
    with pytest.raises(ValueError,match='exact eligible'):attribution.load_source(changed)

def test_original_cluster_bootstrap_preserves_undefined_draws():
    rows=[]
    for day in range(6):
        for i in range(20):
            rows.append({'decision_time':iso(BASE+timedelta(days=day,minutes=i)),
                'forecaster_loss':0.,'rolling_global_loss':0.,'rolling_calendar_loss':0.})
    config={'cluster_days':1,'anchor':iso(BASE),'replicates':100,'seed':1,'confidence':.95}
    result=attribution.conditional_interval(rows,config,attribution_stats(),list(range(6)))
    assert result['status']=='completed'
    assert result['benchmarks']['rolling_global']['skill'] is None
    assert result['benchmarks']['rolling_global']['undefined_skill_draws']==100

@pytest.mark.parametrize('version',['25','26'])
def test_supported_worker_dispatch_reconstructs_new_versions(monkeypatch,version):
    from contextlib import nullcontext
    from portal.backend.service.async_jobs import ClaimedJob
    from portal.backend.service.research import async_dispatch,catalog
    from portal.backend.service.research.planning import plan_research_check
    from portal.backend.workers import research_worker
    if version=='25':definition,request,plan=contract(extreme_payload())
    else:
        p=deepcopy(catalog.get_check_definition('raw_forward_outcome','26')['examples'][0]['request'])
        definition,request=normalize_check_request(p)
        plan=plan_research_check(definition,request,inspect_coverage=False)
    definition,request,plan=pin_check_definition_to_plan(definition,request,plan)
    job=ClaimedJob(id='synthetic-movement-worker',job_type=async_dispatch.JOB_TYPE_RESEARCH_CHECK_RUN,
        payload={'request':{'request':request.to_dict(),'plan':plan.to_dict()}},attempts=1,max_attempts=1,
        partition_key=None,partition_hash=0,lock_owner='synthetic',claim_token='synthetic',claim_generation=1)
    observed={}
    def build(serialized):
        req=type(request).from_dict(serialized['request']);resolved=type(plan).from_dict(serialized['plan'])
        observed['definition']=reconstruct_pinned_check_definition(req,resolved).to_dict()
        return {'schema_version':'research_check_evidence_build.v1'}
    monkeypatch.setattr(research_worker,'maintain_job_heartbeat',lambda *a,**k:nullcontext())
    monkeypatch.setattr(research_worker.research_service,'build_research_check_evidence',build)
    monkeypatch.setattr(research_worker.research_service,'persist_built_research_check_evidence',lambda built,*,session:{'schema_version':'research_check_run.v2','status':'completed'})
    monkeypatch.setattr(research_worker,'complete_job_with_owned_effect',lambda claimed,effect:dict(effect('synthetic-session')))
    research_worker.execute_claimed_research_job(job)
    assert observed['definition']==definition.to_dict()

def test_exceedance_shared_native_gap_mask_and_model_future_invariance():
    p=extreme_payload();frame=candles().drop(index=[190]).reset_index(drop=True)
    original=run(frame=frame,p=p)
    directional=run(frame=frame,p=payload())
    assert original['selection_audit']==directional['selection_audit']
    poisoned=deepcopy(frame)
    for field in ('open','high','low','close'):
        poisoned.loc[poisoned.index>=300,field]*=1.25
    changed=run(frame=poisoned,p=p)
    assert original['folds'][0]['model']==changed['folds'][0]['model']
    assert original['folds'][0]['target_transform']==changed['folds'][0]['target_transform']

def test_flat_native_targets_keep_zero_and_unsupported_class_support():
    frame=candles()
    for field in ('open','high','low','close'):frame[field]=100.
    result=run(frame=frame,p=extreme_payload())
    assert result['status']=='unsupported'
    assert all(f['target_transform']['threshold']==0. for f in result['folds'])
    assert all(f['target_transform']['training_event_count']==0 for f in result['folds'])
    assert not result['scoring_rows']

def test_nonfinite_quantile_labels_fail_closed():
    rows=synthetic_rows([0.,float('nan'),2.,3.,4.])
    with pytest.raises(ValueError,match='nonfinite'):
        extreme.transform_fold(rows,{'train':{'start':iso(BASE),'end':iso(BASE+STEP*5)}},
            {'purge_bars':0,'target_transform':{'quantile':.9}},900)

def test_training_availability_custody_is_required(saved):
    result=deepcopy(saved['result']);fold=next(f for f in result['folds'] if f['status']=='valid')
    first=saved['training_rows'][fold['id']][0]['decision_time']
    next(r for r in result['selection_audit']['rows'] if r['decision_time']==first)['known_at']=None
    with pytest.raises(ValueError,match='availability custody'):
        attribution.training_clocks(result,fold,result['statistics_configuration'],3600,native_seconds=900)

def test_catalog_attribution_is_metadata_only_and_export_guard_is_unchanged(monkeypatch):
    from portal.backend.service.research import catalog
    monkeypatch.setattr(attribution.repository,'get_item',lambda _:pytest.fail('catalog must not load source examples'))
    entry=catalog.get_check_definition('raw_forward_outcome','26')
    assert entry['examples']
    evaluator=attribution.SavedScoreAttributionEvaluator()
    monkeypatch.setattr(attribution,'load_source',lambda _:None)
    monkeypatch.setattr(attribution,'attribute_source',lambda *a:'x'*(8388608+1))
    with pytest.raises(ValueError,match='exceeds8MiB'):
        evaluator.evaluate(plan=None,inputs={'outcomes':{'sources':[{}]},'statistics':{}})

def test_retrospective_equal_target_ties_use_last_ascending_clock_ranks(saved):
    for row in saved['result']['scoring_rows']:row['actual_target']=1.
    result=attribution.attribute_source(saved,attribution_stats())
    count=math.ceil(.01*len(saved['result']['scoring_rows']))
    ordered=sorted((r['decision_time'],r['fold_id']) for r in saved['result']['scoring_rows'])
    actual=sorted((a['key'][1],a['key'][0]) for a in result['assignments'] if a['strata']['outcome']=='top1')
    assert actual==ordered[-count:]

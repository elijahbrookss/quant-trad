"""Exact saved-score attribution; no candle loading, label replay or refitting."""
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import json
import math
import re
from uuid import UUID
import numpy as np
from data_providers.utils.ohlcv import interval_to_timedelta
from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from research_science.check import CheckDefinition, CheckRequest, CheckResult, CheckEvidenceBinding, ResolvedCheckPlan
from . import repository
from . import candle_target_benchmark_evaluator as base

SCHEMA='saved_score_attribution.v1'
RESULT_SCHEMA='saved_score_attribution_result.v1'
RULES={'source':'exact_persisted_eligible_raw_forward_outcome@24',
       'training_membership':'original_count_and_ordered_clock_hash_required',
       'attribution':'full_population_paired_signed_loss_no_tail_removal',
       'limits':{'sources':4,'rows_per_source':19104,'result_bytes':8388608},
       'inference':'original_fixed_calendar_cluster_conditional_only'}

def invalid(message): raise ValueError('saved_score_attribution_invalid: '+message)

def reference(raw):
    if not isinstance(raw,dict) or set(raw)!={'check_id','result_hash','evidence_hash'}: invalid('exact source reference required')
    try:
        if str(UUID(raw['check_id']))!=raw['check_id']: invalid('canonical Check ID required')
    except (ValueError,TypeError,AttributeError): invalid('canonical Check ID required')
    if any(not isinstance(raw[k],str) or not re.fullmatch('[0-9a-f]{64}',raw[k]) for k in ('result_hash','evidence_hash')): invalid('exact hashes required')
    return dict(raw)

def normalize_configuration(*,detector,outcomes,statistics,inputs,scope,gap_policy):
    if detector!={'type':'saved_scoring_rows'} or inputs or gap_policy!='reject': invalid('saved-score-only route required')
    if not isinstance(outcomes,dict) or set(outcomes)!={'schema_version','sources'} or outcomes['schema_version']!=SCHEMA: invalid('versioned sources required')
    refs=[reference(r) for r in base.v2._list(outcomes['sources'],field='sources',minimum=1,maximum=4)]
    if len({r['check_id'] for r in refs})!=len(refs): invalid('duplicate source')
    if not isinstance(statistics,dict) or set(statistics)!={'causal_strata','minimum_interval_rows','minimum_interval_clusters'}: invalid('explicit strata and interval support required')
    strata=[]
    for raw in base.v2._list(statistics['causal_strata'],field='causal_strata',minimum=1,maximum=4):
        if set(raw)!={'feature','quantiles','quantile_method'} or not isinstance(raw['feature'],str) or not raw['feature'] or raw['quantile_method']!='linear': invalid('declared feature quantiles required')
        qs=[base.v2._number(q,field='stratum.quantile',minimum=0.) for q in raw['quantiles']]
        if not qs or len(qs)>4 or qs!=sorted(set(qs)) or any(not 0<q<1 for q in qs): invalid('ordered interior quantiles required')
        strata.append({'feature':raw['feature'],'quantiles':qs,'quantile_method':'linear'})
    if len({r['feature'] for r in strata})!=len(strata): invalid('duplicate stratum')
    normalized={'causal_strata':strata,'minimum_interval_rows':base.v2._integer(statistics['minimum_interval_rows'],field='minimum_interval_rows',minimum=100,maximum=19104),
                'minimum_interval_clusters':base.v2._integer(statistics['minimum_interval_clusters'],field='minimum_interval_clusters',minimum=6,maximum=19104)}
    if base.v2._clock(scope['start'],'scope.start')>=base.v2._clock(scope['end'],'scope.end'): invalid('ordered scope required')
    return {'schema_version':SCHEMA,'sources':refs},normalized

def training_clocks(result,fold,statistics,horizon_seconds,*,native_seconds):
    """Candidate audit membership is accepted only against original custody."""
    declared=next(r for r in statistics['folds'] if r['id']==fold['id'])
    start=base.v2._clock(declared['train']['start'],'train.start');end=base.v2._clock(declared['train']['end'],'train.end')
    effective=base.v2._clock(fold['effective_train_end'],'effective_train_end')
    rows=[r for r in result['selection_audit']['rows'] if r.get('selected_for_paired_model') is True
          and start<=base.v2._clock(r['decision_time'],'decision')<effective
          and base.v2._clock(r['decision_time'],'decision')+timedelta(seconds=horizon_seconds)<end]
    clocks=[r['decision_time'] for r in rows]
    if len(set(clocks))!=len(clocks) or len(clocks)!=fold['train_count'] or semantic_hash({'clocks':clocks})!=fold['training_clock_hash']:
        invalid('exact original training membership unavailable; no approximation or replay permitted')
    audit={r['decision_time']:r for r in result['selection_audit']['rows']}
    # Availability custody is retained for every observed close, including the
    # future path. This reads metadata only; no market replay or label refit.
    for index,row in enumerate(rows):
        if index%64==0: execution_checkpoint()
        decision=base.v2._clock(row['decision_time'],'decision')
        path=[audit.get(base.v2._iso(decision+timedelta(seconds=offset)))
              for offset in range(0,horizon_seconds+1,native_seconds)]
        if any(p is None or p.get('observed_native_close') is not True or p.get('known_at') is None
               or base.v2._clock(p['known_at'],'path.known_at')>=end for p in path):
            invalid('exact training-label availability custody unavailable')
    return rows

def load_source(ref,*,item_loader=None):
    from .service import _validate_research_check_evidence_payload
    item=(item_loader or repository.get_item)(ref['check_id'])
    if not isinstance(item,dict) or item.get('id')!=ref['check_id'] or item.get('kind')!='research_check' or item.get('status')!='tested': invalid('tested source Check required')
    payload=item.get('payload') or {}
    if len(json.dumps(payload.get('result') or {},allow_nan=False).encode())>16777216:
        invalid('source result exceeds existing16MiB bound')
    if payload.get('observation_eligible') is not True or payload.get('replayable') is not True: invalid('persisted eligibility flags must literally be true')
    if any(k in item and item[k] is not True for k in ('observation_eligible','replayable')): invalid('projection disagrees with persisted eligibility')
    _validate_research_check_evidence_payload(payload,item_id=item['id'])
    definition=CheckDefinition.from_dict(payload['definition']);request=CheckRequest.from_dict(payload['request'])
    evidence=CheckEvidenceBinding.from_dict(payload['evidence']);contract=CheckResult.from_dict(payload['result'])
    result=deepcopy(dict(contract.result));binding=dict(evidence.input_binding)
    if (definition.evaluator_id!='raw_forward_outcome' or definition.evaluator_version!='24' or contract.evaluator_version!='24'
        or contract.result_hash!=ref['result_hash'] or evidence.evidence_hash!=ref['evidence_hash']
        or evidence.evidence_kind!='frozen_market_data' or request.mode!='evidence'
        or result.get('schema_version')!=base.RESULT_SCHEMA or result.get('status')!='completed'
        or result['outcomes']['target'].get('operator')!=base.TARGET_VARIANCE
        or result['outcomes']['target'].get('definition_version')!='native_log_return_sum_squares.v1'): invalid('exact eligible native variance source required')
    audit=result['selection_audit'];rows=result['scoring_rows']
    if (len(rows)>19104 or len(audit['rows'])>19104 or semantic_hash({'rows':audit['rows']})!=audit['row_hash']
        or semantic_hash({'scoring_rows':rows})!=result['comparison']['prediction_hash']): invalid('saved scoring/audit integrity differs')
    series=binding.get('series') or []
    if (len(series)!=1 or series[0].get('fact_type')!='candle.ohlcv'
        or series[0].get('contract_version')!='candle.ohlcv.v1'
        or series[0].get('timeframe_seconds')!=int(interval_to_timedelta(request.scope['timeframe']).total_seconds())): invalid('native candle scope differs')
    primary=series[0];source=primary['source_binding'];keys=source.get('resolved_source_identity_keys') or []
    if (source.get('mode')!='exact' or len(keys)!=1 or primary.get('instrument_id')!=request.scope['instrument_id']
        or source['sources'][keys[0]].get('source_kind')!='historical_api'
        or not source['sources'][keys[0]].get('adapter_version','').endswith('Provider:candle.ohlcv.v1')
        or any(r.get('source_identity_key')!=keys[0] for r in audit['rows'] if r.get('observed_native_close') is True)): invalid('source/instrument provenance differs')
    if len({(r['fold_id'],r['decision_time']) for r in rows})!=len(rows): invalid('duplicate scoring pair keys')
    folds={r['id']:r for r in result['folds'] if r['status']=='valid'}
    for index,row in enumerate(rows):
        if index%64==0: execution_checkpoint()
        if row['fold_id'] not in folds or any(type(row[k]) not in (int,float) or not math.isfinite(row[k]) or row[k]<0 for k in ('actual_target','forecaster_loss','rolling_global_loss','rolling_calendar_loss')): invalid('invalid saved scoring losses')
    for name in ('rolling_global','rolling_calendar'):
        summary=base._score_summary(rows,name=name);original=result['comparison']['benchmarks'][name]
        if summary!=original: invalid('full-population loss reconciliation differs')
    membership={key:training_clocks(result,fold,result['statistics_configuration'],int(result['outcomes']['horizon_seconds']),
                native_seconds=int(interval_to_timedelta(request.scope['timeframe']).total_seconds())) for key,fold in folds.items()}
    return {'reference':ref,'result':result,'request':request,'binding':binding,'training_rows':membership}

def binding_for(request,*,load=True):
    refs=request.parameters['outcomes']['sources']
    sources=[load_source(r) for r in refs] if load else []
    if sources:
        first=sources[0]['request']
        if request.dataset_id!=first.dataset_id: invalid('request must retain first source Dataset identity')
        for saved in sources:
            scope=saved['request'].scope
            if scope['instrument_id']!=request.scope['instrument_id'] or scope['timeframe']!=request.scope['timeframe'] or base.v2._clock(scope['start'],'source.start')<base.v2._clock(request.scope['start'],'scope.start') or base.v2._clock(scope['end'],'source.end')>base.v2._clock(request.scope['end'],'scope.end'): invalid('declared scope must enclose each same-instrument/timeframe source')
    return {'schema_version':'saved_check_score_binding.v1','sources':refs,'dataset_id':request.dataset_id,'provider_access':False,'market_replay':False}

def mass(rows,name,total):
    ds=[r[name+'_loss']-r['forecaster_loss'] for r in rows];s=math.fsum(ds);absolute=math.fsum(abs(v) for v in ds)
    return {'rows':len(rows),'signed_sum':s,'signed_mean':s/len(rows) if rows else None,
            'positive_mass':math.fsum(v for v in ds if v>0),'negative_mass':math.fsum(v for v in ds if v<0),
            'absolute_mass':absolute,'signed_share':None if total['signed_sum']==0 else s/total['signed_sum'],
            'absolute_mass_share':None if total['absolute_mass']==0 else absolute/total['absolute_mass']}

def conditional_interval(rows,config,statistics,original_cluster_keys=None):
    blocks=defaultdict(list)
    for r in rows: blocks[base._block_id(r['decision_time'],days=config['cluster_days'],anchor=config['anchor'])].append(r)
    if len(rows)<statistics['minimum_interval_rows'] or len(blocks)<statistics['minimum_interval_clusters']:
        return {'status':'unsupported','reason':'minimum_rows_or_original_clusters_not_met','rows':len(rows),'cluster_count':len(blocks)}
    keys=sorted(blocks) if original_cluster_keys is None else original_cluster_keys
    nonempty_count=len(blocks)
    values=np.array([[len(blocks[key]),math.fsum(r['forecaster_loss'] for r in blocks[key]),*[math.fsum(r[n+'_loss'] for r in blocks[key]) for n in ('rolling_global','rolling_calendar')]] for key in keys])
    rng=np.random.default_rng(config['seed']);draws=[]
    for start in range(0,config['replicates'],128):
        execution_checkpoint();idx=rng.choice(len(values),size=(min(128,config['replicates']-start),len(values)),replace=True)
        draws.append(values[idx].sum(axis=1))
    samples=np.concatenate(draws);alpha=(1-config['confidence'])/2;result={}
    empty=int(np.count_nonzero(samples[:,0]==0))
    if empty:
        return {'status':'unsupported','reason':'undefined_empty_cluster_draws','undefined_draws':empty,
                'cluster_count':nonempty_count,'original_cluster_count':len(keys)}
    for i,name in enumerate(('rolling_global','rolling_calendar'),2):
        gain=(samples[:,i]-samples[:,1])/samples[:,0];undefined=int(np.count_nonzero(samples[:,i]==0))
        result[name]={'gain':dict(zip(('lower','upper'),map(float,np.quantile(gain,[alpha,1-alpha])))),
                      'skill':None if undefined else dict(zip(('lower','upper'),map(float,np.quantile(1-samples[:,1]/samples[:,i],[alpha,1-alpha])))),
                      'undefined_skill_draws':undefined}
    return {'status':'completed','cluster_count':nonempty_count,'original_cluster_count':len(keys),
            'benchmarks':result,'conditional_on_original_fitted_models':True}

def attribute_source(saved,statistics):
    result=saved['result'];rows=result['scoring_rows'];audit={r['decision_time']:r for r in result['selection_audit']['rows']}
    thresholds={};missing={}
    configured={r['name'] for r in result['statistics_configuration']['features']['baseline']}
    for fold,training in saved['training_rows'].items():
        thresholds[fold]={};missing[fold]={}
        for spec in statistics['causal_strata']:
            name=spec['feature']
            if name not in configured: invalid('causal feature absent from original feature contract')
            values=[r['feature_context'].get(name) for r in training]
            valid=[float(v) for v in values if type(v) in (int,float) and math.isfinite(v)]
            missing[fold][name]=len(values)-len(valid)
            thresholds[fold][name]=None if not valid else [float(np.quantile(valid,q,method='linear')) for q in spec['quantiles']]
    ranked=sorted(rows,key=lambda r:(r['actual_target'],r['decision_time'],r['fold_id']))
    top=math.ceil(.01*len(rows));ten=math.ceil(.10*len(rows))
    outcome_bin={(r['fold_id'],r['decision_time']):'top1' if i>=len(rows)-top else 'next9' if i>=len(rows)-ten else 'ordinary90' for i,r in enumerate(ranked)}
    ranks={(r['fold_id'],r['decision_time']):i+1 for i,r in enumerate(ranked)}
    config=result['statistics_configuration']['bootstrap'];groups=defaultdict(list);assignments=[]
    for r in rows:
        key=(r['fold_id'],r['decision_time']);a=audit.get(r['decision_time'])
        if a is None or a.get('selected_for_paired_model') is not True: invalid('scoring clock lacks original causal audit')
        prior=[]
        for spec in statistics['causal_strata']:
            name=spec['feature'];v=a['feature_context'].get(name);qs=thresholds[r['fold_id']][name]
            label='missing_or_unreconstructible' if type(v) not in (int,float) or not math.isfinite(v) or qs is None else 'bin'+str(sum(v>q for q in qs))
            prior.append(name+':'+label)
        calendar=base.v2._clock(r['decision_time'],'decision').hour//6;block=base._block_id(r['decision_time'],days=config['cluster_days'],anchor=config['anchor'])
        assignment={'fold':r['fold_id'],'calendar':str(calendar),'prior':'|'.join(prior),'outcome':outcome_bin[key], 'day':r['decision_time'][:10],'three_day':str(base._block_id(r['decision_time'],days=3,anchor=config['anchor']))}
        assignments.append({'key':list(key),'strata':assignment,'retrospective_rank':ranks[key],
                            'target_tie_id':semantic_hash({'actual_target':r['actual_target']})})
        for dimension,value in assignment.items(): groups[(dimension,value)].append(r)
        groups[('joint',json.dumps([assignment[k] for k in ('fold','calendar','prior','outcome')]))].append(r)
    totals={}
    for name in ('rolling_global','rolling_calendar'):
        ds=[r[name+'_loss']-r['forecaster_loss'] for r in rows]
        totals[name]={'signed_sum':math.fsum(ds),'absolute_mass':math.fsum(abs(v) for v in ds)}
    table=[]
    original_keys=sorted({base._block_id(r['decision_time'],days=config['cluster_days'],anchor=config['anchor']) for r in rows})
    for (dimension,value),selected in sorted(groups.items()):
        execution_checkpoint()
        table.append({'dimension':dimension,'value':value,'paired_key_hash':semantic_hash({'keys':[(r['fold_id'],r['decision_time']) for r in selected]}),
                      'benchmarks':{n:mass(selected,n,totals[n]) for n in totals},'interval':conditional_interval(selected,config,statistics,original_keys)})
    losses={'rows':len(rows),'forecaster_loss_sum':math.fsum(r['forecaster_loss'] for r in rows),
            **{n+'_loss_sum':math.fsum(r[n+'_loss'] for r in rows) for n in totals}}
    for dimension in {r['dimension'] for r in table}:
        for name in totals:
            summed=math.fsum(r['benchmarks'][name]['signed_sum'] for r in table if r['dimension']==dimension)
            tolerance=8*math.ulp(totals[name]['absolute_mass'])
            if not math.isclose(summed,totals[name]['signed_sum'],rel_tol=1e-12,abs_tol=tolerance): invalid('partition loss sums do not reconcile')
    return {'source':saved['reference'],'full_population':{n:mass(rows,n,totals[n]) for n in totals},'loss_reconciliation':losses,
            'loss_reconciliation_hash':semantic_hash(losses),'original_comparison':result['comparison'],
            'training_clock_hashes':{f['id']:f['training_clock_hash'] for f in result['folds'] if f['status']=='valid'},
            'training_quantiles':thresholds,'training_feature_missingness':missing,'assignments':assignments,
            'partition_hash':semantic_hash({'assignments':assignments}),'tables':table,
            'retrospective_rank_rule':'ascending_actual_target_then_decision_time_then_fold_id; largest_ceil_one_percent_then_next_to_ceil_ten_percent',
            'interpretation':'Outcome ranks are retrospective descriptive strata; prior-state associations are not causal effects. No rows trimmed or refit.'}

@dataclass(frozen=True)
class SavedScoreAttributionEvaluator:
    evaluator_id:str='raw_forward_outcome'
    version:str='26'
    result_schema_version:str=RESULT_SCHEMA

    def declare_requirements(self,*,definition,request):
        binding=binding_for(request,load=False)
        return {'input_kind':'saved_check_evidence','source_binding':binding,'market_input_rows':0,'provider_calls':0,
                'source_limit':4,'source_scoring_row_limit':19104,'result_bytes_limit':8388608,
                'saved_source_bounds':{'scope':dict(request.scope),'source_count':len(binding['sources']),
                    'maximum_scoring_rows':19104*len(binding['sources']),
                    'maximum_audit_rows':19104*len(binding['sources']),
                    'maximum_source_result_bytes':16777216*len(binding['sources'])}}

    def evaluate(self,*,plan,inputs):
        sources=[load_source(r) for r in inputs['outcomes']['sources']]
        result={'schema_version':RESULT_SCHEMA,'method_version':self.version,'status':'completed','rules':RULES,
                'sources':[attribute_source(s,inputs['statistics']) for s in sources],
                'market_replay_performed':False,'model_refit_performed':False}
        if len(json.dumps(result,allow_nan=False).encode())>8388608: invalid('whole result exceeds8MiB; no truncation')
        return result

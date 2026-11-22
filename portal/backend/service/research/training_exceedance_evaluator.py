"""Fold-training-defined exceedance of an unchanged native continuous target."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import math
import numpy as np
from market_data.frozen import semantic_hash
from . import candle_target_benchmark_evaluator as base

SCHEMA = 'candle_training_exceedance.v1'
RESULT_SCHEMA = 'candle_training_exceedance_result.v1'
OPERATOR = 'training_quantile_exceedance'
RULES = {**base.RULES, 'model': 'fold_training_quantile_frozen_logistic.v1',
         'training_labels': 'outcome_known_at_strictly_before_training_cutoff',
         'history_labels': 'reclassify_matured_continuous_target_at_current_fold_threshold'}

def normalize_configuration(**kwargs):
    args=deepcopy(kwargs)
    outcome=args['outcomes']; transform=outcome.get('target')
    if outcome.get('schema_version')!=SCHEMA or not isinstance(transform,dict):
        raise ValueError('training_exceedance_invalid: explicit versioned target required')
    fixed={'operator':OPERATOR,'base_target':base.TARGET_VARIANCE,'quantile_method':'linear',
           'threshold_fit':'fold_training_only','comparison':'strict_gt','tie':'class_zero','freeze':'validation'}
    if set(transform)!=set(fixed)|{'quantile'} or any(transform.get(k)!=v for k,v in fixed.items()):
        raise ValueError('training_exceedance_invalid: target transform contract differs')
    q=base.v2._number(transform['quantile'],field='target.quantile',minimum=0.)
    if not 0<q<1: raise ValueError('training_exceedance_invalid: quantile must be inside(0,1)')
    transform={**fixed,'quantile':q}
    config=args['statistics']
    canonical='target_operator' in config
    if canonical:
        if config['target_operator']!=OPERATOR or config.pop('target_transform',None)!=transform:
            raise ValueError('training_exceedance_invalid: normalized target mismatch')
        config['target_operator']=base.TARGET_DIRECTION
    outcome['schema_version']=base.SCHEMA
    outcome['target']={'operator':base.TARGET_DIRECTION,'definition_version':'terminal_direction.v1','flat':'class_zero'}
    normalized,statistics=base.normalize_configuration(**args)
    normalized.update(schema_version=SCHEMA,target=transform)
    statistics.update(target_operator=OPERATOR,target_transform=transform)
    if canonical and kwargs['statistics']!=statistics:
        raise ValueError('training_exceedance_invalid: canonical statistics disagree')
    return normalized,statistics

def transform_fold(rows, fold, statistics, seconds):
    clock=base.v2._clock
    start=clock(fold['train']['start'],'train.start');cutoff=clock(fold['train']['end'],'train.end')
    effective=cutoff-timedelta(seconds=statistics['purge_bars']*seconds)
    train=[r for r in rows if start<=clock(r['decision_time'],'decision')<effective
           and clock(r['target_time'],'target')<cutoff and clock(r['outcome_known_at'],'known')<cutoff]
    values=[float(r['outcomes']['primary']['path_variation']) for r in train]
    if any(not math.isfinite(v) or v<0 for v in values):
        raise ValueError('training_exceedance_unsupported: nonfinite eligible training labels')
    threshold=float(np.quantile(values,statistics['target_transform']['quantile'],method='linear')) if values else None
    receipt={'threshold':threshold,'training_count':len(train),'training_clock_hash':semantic_hash({'clocks':[r['decision_time'] for r in train]}),
             'training_clocks':[r['decision_time'] for r in train],
             'training_event_count':sum(v>threshold for v in values),'training_nonevent_count':sum(v<=threshold for v in values),
             'training_label_hash':semantic_hash({'labels':[(r['decision_time'],r['outcome_known_at'],v) for r,v in zip(train,values,strict=True)]}),
             'known_at_cutoff':base.v2._iso(cutoff),'maximum_training_label_known_at':max((r['outcome_known_at'] for r in train),default=None),
             'quantile_method':'linear_index_(n-1)q','comparison':'strict_gt','ties':'class_zero',
             'freeze':'validation','historical_reclassification':'threshold_known_before_current_prediction_not_historical_anchor'}
    transformed=[]
    for source in rows:
        r={**source,'outcomes':{**source['outcomes'],'primary':{**source['outcomes']['primary']}}}
        r['outcomes']['primary']['positive']=threshold is not None and float(source['outcomes']['primary']['path_variation'])>threshold
        transformed.append(r)
    return transformed,receipt

@dataclass(frozen=True)
class TrainingExceedanceEvaluator(base.CandleTargetBenchmarkEvaluator):
    version:str='25'
    result_schema_version:str=RESULT_SCHEMA

    def _score_complete_rows(self,*,complete_rows,statistics,seconds,checkpoint):
        scored=base.score_target_benchmarks(complete_rows=complete_rows,statistics=statistics,
            target=base.TARGET_DIRECTION,seconds=seconds,checkpoint=checkpoint,
            fold_transform=lambda rows,fold:transform_fold(rows,fold,statistics,seconds))
        scored['comparison']['target']=OPERATOR
        for fold in scored['folds']:
            rows=[r for r in scored['result_extensions']['scoring_rows'] if r['fold_id']==fold['id']]
            events=sum(r['actual_target']==1 for r in rows)
            alarms=[r for r in rows if r['forecaster_prediction']>.5]
            fold['event_diagnostics']={'rows':len(rows),'events':events,'nonevents':len(rows)-events,
                'event_rate':events/len(rows) if rows else None,
                'mean_forecast':float(np.mean([r['forecaster_prediction'] for r in rows])) if rows else None,
                'alarm_rule':'forecaster_probability_strictly_above_0.5_descriptive_only',
                'alarms':len(alarms),'false_alarms':sum(r['actual_target']==0 for r in alarms),
                'false_alarm_denominator':len(alarms),
                'event_utc_days':len({r['decision_time'][:10] for r in rows if r['actual_target']==1}),
                'event_primary_clusters':len({base._block_id(r['decision_time'],days=statistics['bootstrap']['cluster_days'],anchor=statistics['bootstrap']['anchor']) for r in rows if r['actual_target']==1})}
        scored['result_extensions']['target_transform']=statistics['target_transform']
        scored['result_extensions']['additional_caveats'].append(
            'Training quantile, model and scaler are frozen per fold; matured past-validation labels update rolling benchmarks at the current fold threshold.')
        return scored

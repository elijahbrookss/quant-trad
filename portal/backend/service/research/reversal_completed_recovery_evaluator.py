"""Bounded explicit composition over shared native input and event kernels."""
from dataclasses import dataclass
import json
from time import monotonic

from core.execution_control import execution_checkpoint
from research_science.recognition import plain
from research_science.reversal_completed_recovery import SCHEMA,RULES,measure_completed
from .reversal_population_evaluator import (
    SCHEMA as POPULATION_SCHEMA, normalize_configuration as normalize_population,
    population_for, prepare_population_input)
from .recognition_preparation import recognition_requirements

RESULT_SCHEMA='reversal_completed_recovery_result.v1'


def normalize_configuration(*,detector,outcomes,statistics,inputs,scope,gap_policy):
    if not isinstance(outcomes,dict) or outcomes.get('schema_version')!=SCHEMA:
        raise ValueError('completed_recovery_invalid: explicit completed recovery composition required')
    result=normalize_population(detector=detector,
        outcomes={**outcomes,'schema_version':POPULATION_SCHEMA},statistics=statistics,
        inputs=inputs,scope=scope,gap_policy=gap_policy)
    return {**result,'schema_version':SCHEMA}


@dataclass(frozen=True)
class ReversalCompletedRecoveryEvaluator:
    evaluator_id:str='raw_forward_outcome'
    version:str='20'
    result_schema_version:str=RESULT_SCHEMA

    def declare_requirements(self,*,definition,request):
        config=normalize_configuration(detector=request.parameters['detector'],outcomes=request.parameters['outcomes'],
            statistics=request.parameters['statistics'],inputs=request.parameters['inputs'],scope=request.scope,gap_policy=request.parameters['gap_policy'])
        if request.mode!='evidence' or not request.dataset_id:
            raise ValueError('completed_recovery_invalid: explicit frozen native Dataset required')
        population=population_for(request.parameters['detector'],request.scope,config['populations'])
        return recognition_requirements(population.warmup_bars)

    def evaluate(self,*,plan,inputs):
        started=monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started>=RULES['limits']['wall_seconds']:
                raise ValueError('completed_recovery_limit:120second cooperative evaluator budget exceeded')
        primary=next(r for r in plan.market_data_requirements if r['alias']=='primary_bars')
        scope={'instrument_id':primary['instrument_id'],'timeframe':'15m','start':plan.evaluation_range['start'],
            'end':plan.evaluation_range['end_exclusive']}
        config=normalize_configuration(detector=inputs['detector'],outcomes=inputs['outcomes'],statistics=inputs['statistics'],
            inputs=inputs.get('inputs',[]),scope=scope,gap_policy=plan.gap_policy)
        population=population_for(inputs['detector'],scope,config['populations'])
        settings,snapshot,candles,source_key=prepare_population_input(plan=plan,inputs=inputs,
            population=population,primary=primary,schema=SCHEMA,config=config,checkpoint=checkpoint)
        result=measure_completed(candles,snapshot,population,checkpoint=checkpoint)
        result.update(schema_version=RESULT_SCHEMA,check_family=self.evaluator_id,status='completed',
            analysis_status='descriptive_completed_formation_population',computation_origin=plain(population.origin),
            recognition_choices=settings.to_dict(),processed_bars=snapshot.processed_bars,
            reference_count=len(snapshot.references),pending_candidate_count=len(snapshot.pending),
            evaluation_range=dict(plan.evaluation_range),source_identity_key=source_key,
            snapshot_known_at=plain(snapshot.last_known_at),context_left_censored=True,
            candidate_count=len(result['cases']),sample_count=len({r['decision_at'] for r in result['cases']}),
            sample_count_meaning='All completed formation clocks;not independent observations',
            data_quality=dict(inputs.get('data_quality') or {}),
            caveats=['All completed chains retained;no matching or context-adjusted comparison.',
                'N measures inclusive observed trough-to-completion duration;not intrabar speed.',
                'All four first-passage labels and missing/unpaired mass retained;no extra input tail.',
                'Label summaries retain original all-population clock weights;no independence,inference or trading claim.'])
        checkpoint()
        if len(json.dumps(result,allow_nan=False,separators=(',',':')).encode())>RULES['limits']['result_bytes']:
            raise ValueError('completed_recovery_limit: result exceeds16MiB;no truncation')
        return result

"""Explicit two-block prefix;reuse canonical preparation, recognizer and selector."""
from dataclasses import dataclass
from time import monotonic
import json
from core.execution_control import execution_checkpoint
from research_science.recognition import plain,utc
from research_science.reversal_two_block_selection import SCHEMA,RULES,ORIGIN,START,END,INPUT_BARS,select_two_blocks
from .reversal_recognition_evaluator import choices,prepare_recognition

RESULT_SCHEMA='reversal_two_block_selection_result.v1'


def normalize_configuration(*,detector,outcomes,statistics,inputs,scope,gap_policy):
    settings=choices(detector)
    if (outcomes!={'schema_version':SCHEMA} or statistics or inputs or gap_policy!='reject'
        or scope.get('timeframe')!='15m' or scope.get('indicator_id') or scope.get('indicator_param_overrides')
        or settings.reference_basis!='rising_leg_plus_retreat' or settings.ownership!='retain_all_provisional'
        or settings.max_bars!=19104 or settings.max_candidates!=10000
        or utc(detector['computation_origin'])!=ORIGIN or utc(scope.get('start'))!=START or utc(scope.get('end'))!=END
        or ('warmup_bars' in scope and (type(scope['warmup_bars']) is not int or scope['warmup_bars']!=2976))):
        raise ValueError('reversal_selection_invalid: fixed first-two-block scope and unchanged recognition/matching settings required')
    return {'schema_version':SCHEMA}


@dataclass(frozen=True)
class ReversalTwoBlockSelectionEvaluator:
    evaluator_id:str='raw_forward_outcome'
    version:str='14'
    result_schema_version:str=RESULT_SCHEMA

    def declare_requirements(self,*,definition,request):
        normalize_configuration(detector=request.parameters['detector'],outcomes=request.parameters['outcomes'],
            statistics=request.parameters['statistics'],inputs=request.parameters['inputs'],scope=request.scope,gap_policy=request.parameters['gap_policy'])
        return {'input_kind':'market_data','indicator_ids':[],'warmup_floor_bars':2976,'feature_lookback_bars':2976,
            'feature_windows_seconds_by_alias':{},'outcome_horizons':[],'required_outcome_horizons':[],
            'horizon_kind':'bars','entry_lag_bars':0,'invalidation_max_bars':0,'outcome_boundary':'evaluation_end_exclusive',
            'primary_candle_required_fields':['open','high','low','close','known_at']}

    def evaluate(self,*,plan,inputs):
        started=monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started>=RULES['limits']['wall_seconds']:
                raise ValueError('reversal_selection_limit:120second two-block method wall budget exceeded')
        normalize_configuration(detector=inputs['detector'],outcomes=inputs['outcomes'],statistics=inputs['statistics'],inputs=inputs.get('inputs',[]),
            gap_policy=plan.gap_policy,scope={'start':plan.evaluation_range['start'],'end':plan.evaluation_range['end_exclusive'],'timeframe':'15m'})
        if int(inputs['candles'].memory_usage(index=True,deep=True).sum())>RULES['limits']['input_bytes']:
            raise ValueError('reversal_selection_limit:input exceeds256MiB')
        if len(inputs['candles'])!=INPUT_BARS:
            raise ValueError('reversal_selection_invalid:exact8352bar clean prefix required')
        settings,snapshot,rows,start,end,candles=prepare_recognition(plan=plan,inputs=inputs,schema=SCHEMA,checkpoint=checkpoint)
        result=select_two_blocks(candles,snapshot,checkpoint=checkpoint)
        result.update(schema_version=RESULT_SCHEMA,check_family=self.evaluator_id,status='completed',
            analysis_status='selection_feasibility_only',computation_origin=plain(ORIGIN),recognition_choices=settings.to_dict(),
            processed_bars=snapshot.processed_bars,evaluation_range=dict(plan.evaluation_range),source_identity_key=candles[0].source_identity_key,
            snapshot_known_at=plain(snapshot.last_known_at),context_left_censored=True,candidate_count=len(result['cases']),
            sample_count=result['selection_summary']['matched']['completion_clocks'],sample_count_meaning='Matched completion moments;not independent observations',
            data_quality=dict(inputs.get('data_quality') or {}),
            caveats=['Two complete pre-gap blocks;scope amended explicitly;no gap compression/reset.',
                'All scientific formation/matching rules and original resource namespace retained.',
                'No outcomes,coverage pass gate or independence claim.'])
        checkpoint()
        if len(json.dumps(result,allow_nan=False,separators=(',',':')).encode())>RULES['limits']['result_bytes']:
            raise ValueError('reversal_selection_limit:result exceeds16MiB;no truncation')
        return result

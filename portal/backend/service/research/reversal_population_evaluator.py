"""One supported population recipe; dates/symbols/views are configuration."""
from dataclasses import dataclass
import json
from time import monotonic

from core.execution_control import execution_checkpoint
from research_science.recognition import plain,utc
from research_science.reversal_population import SCHEMA,RULES,SelectionPopulation,select_population
from .recognition_preparation import choices,prepare_recognition,recognition_requirements
from .saved_selection_binding import native_candle_series

RESULT_SCHEMA='reversal_population_selection_result.v1'


def population_for(detector,scope,views):
    return SelectionPopulation.from_configuration(origin=detector['computation_origin'],
        start=scope['start'],end=scope['end'],views=views)


def normalize_configuration(*,detector,outcomes,statistics,inputs,scope,gap_policy):
    settings=choices(detector)
    if (not isinstance(outcomes,dict) or set(outcomes)!={'schema_version','populations'}
            or outcomes['schema_version']!=SCHEMA or statistics or inputs or gap_policy!='reject'
            or scope.get('timeframe')!='15m' or scope.get('indicator_id') or scope.get('indicator_param_overrides')
            or settings.reference_basis!='rising_leg_plus_retreat' or settings.ownership!='retain_all_provisional'
            or settings.max_bars!=19104 or settings.max_candidates!=10000):
        raise ValueError('reversal_population_invalid: fixed outcome-blind formation/matching policy and native15m input required')
    population=population_for(detector,scope,outcomes['populations'])
    if ('warmup_bars' in scope and (type(scope['warmup_bars']) is not int or scope['warmup_bars']!=population.warmup_bars)):
        raise ValueError('reversal_population_invalid: warmup must equal explicit origin distance')
    return {'schema_version':SCHEMA,'populations':population.to_dict()['views']}


@dataclass(frozen=True)
class ReversalPopulationEvaluator:
    evaluator_id:str='raw_forward_outcome'
    version:str='17'
    result_schema_version:str=RESULT_SCHEMA

    def declare_requirements(self,*,definition,request):
        config=normalize_configuration(detector=request.parameters['detector'],outcomes=request.parameters['outcomes'],
            statistics=request.parameters['statistics'],inputs=request.parameters['inputs'],scope=request.scope,gap_policy=request.parameters['gap_policy'])
        if request.mode!='evidence' or not request.dataset_id:
            raise ValueError('reversal_population_invalid: explicit frozen native Dataset required')
        population=population_for(request.parameters['detector'],request.scope,config['populations'])
        return recognition_requirements(population.warmup_bars)

    def evaluate(self,*,plan,inputs):
        started=monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started>=RULES['limits']['wall_seconds']:
                raise ValueError('reversal_population_limit:120second cooperative evaluator budget exceeded')
        primary=next(r for r in plan.market_data_requirements if r['alias']=='primary_bars')
        scope={'instrument_id':primary['instrument_id'],'timeframe':'15m','start':plan.evaluation_range['start'],
               'end':plan.evaluation_range['end_exclusive']}
        config=normalize_configuration(detector=inputs['detector'],outcomes=inputs['outcomes'],statistics=inputs['statistics'],
            inputs=inputs.get('inputs',[]),scope=scope,gap_policy=plan.gap_policy)
        population=population_for(inputs['detector'],scope,config['populations'])
        settings,snapshot,candles,source_key=prepare_population_input(plan=plan,inputs=inputs,
            population=population,primary=primary,schema=SCHEMA,config=config,checkpoint=checkpoint)
        result=select_population(candles,snapshot,population,checkpoint=checkpoint)
        result.update(schema_version=RESULT_SCHEMA,check_family=self.evaluator_id,status='completed',
            analysis_status='selection_only',computation_origin=plain(population.origin),recognition_choices=settings.to_dict(),
            processed_bars=snapshot.processed_bars,evaluation_range=dict(plan.evaluation_range),source_identity_key=source_key,
            snapshot_known_at=plain(snapshot.last_known_at),context_left_censored=True,candidate_count=len(result['cases']),
            sample_count=result['selection_summary']['matched']['completion_clocks'],
            sample_count_meaning='Matched completion clocks;not independent observations',
            data_quality=dict(inputs.get('data_quality') or {}),
            caveats=['Selection only;no forward outcome measurement,model or inference.',
                'Report populations change only summaries;root pairs and original weights are unchanged.',
                'Fixed formation/matching rules and B2tie namespace;no optimized thresholds or rescue tuning.',
                'Nominal support and common recognition ancestry remain;no independent-sample claim.'])
        checkpoint()
        if len(json.dumps(result,allow_nan=False,separators=(',',':')).encode())>RULES['limits']['result_bytes']:
            raise ValueError('reversal_population_limit: result exceeds16MiB;no truncation')
        return result


def prepare_population_input(*, plan, inputs, population, primary, schema, config, checkpoint):
    frame=inputs['candles']
    if len(frame)!=population.input_bars or int(frame.memory_usage(index=True,deep=True).sum())>RULES['limits']['input_bytes']:
        raise ValueError('reversal_population_limit: exact bounded original input required')
    binding=inputs.get('frozen_input_binding')
    if not isinstance(binding,dict):
        raise ValueError('reversal_population_invalid: actual frozen input binding required')
    series,source_key=native_candle_series(binding,dataset_id=binding['dataset_id'])
    if (series['row_count']!=population.input_bars or series['instrument_id']!=primary['instrument_id']
            or series.get('requirement')!=dict(primary) or len(plan.market_data_requirements)!=1
            or utc(series['loaded_range']['start'])!=population.origin
            or utc(series['loaded_range']['end_exclusive'])!=population.end
            or plan.materialization_range['as_of_commit_seq']!=binding['max_commit_seq']
            or plan.indicator_graph or plan.outcome_tail['seconds']!=0):
        raise ValueError('reversal_population_invalid: original frozen native input scope/watermark differs')
    settings,snapshot,rows,start,end,candles=prepare_recognition(plan=plan,inputs=inputs,schema=schema,
        expected_outcomes=config,checkpoint=checkpoint)
    if any(c.source_identity_key!=source_key or c.known_at!=c.close_time for c in candles):
        raise ValueError('reversal_population_invalid: exact source and close availability required')
    return settings,snapshot,candles,source_key

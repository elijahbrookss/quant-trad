"""Narrow saved-B3 manifest consumer on the original frozen native input."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import json
from time import monotonic

from core.execution_control import execution_checkpoint
from research_science.check import CheckEvidenceBinding, CheckResult
from research_science.recognition import RecognitionCandle, utc
from research_science.reversal_matched_outcomes import SCHEMA, RULES, describe_selected_pairs
from research_science.study import stable_hash
from .raw_candle_evaluator import _prepare
from . import repository

RESULT_SCHEMA='reversal_matched_first_passage_result.v1'
DATASET_ID='mds_e73849f18650d9a62bd7b023c368ec98'
DATASET_HASH='e73849f18650d9a62bd7b023c368ec987f044d8165175471e30d4e94d1e7aa01'
INSTRUMENT_ID='0a019a46-c679-4c30-b90c-41518c68e31f'
SELECTION_REFERENCE={
 'check_id':'c31f3aed-e4d1-47d4-ae07-3387166960f7',
 'result_hash':'f9e746c3314aa81afc3abb3b2f7c1f3b92a5839c1ccdc52021f159c688d053ea',
 'evidence_hash':'06c039a519bc12c12445dec884c73d92cc1a93b20ec108c2460fa1a99b96eac9',
 'selection_manifest_hash':'df10bf9763e46397d04996582a3c50345ed9877a38345adf8ec859e2c9be33a5'}
DETECTOR={'type':'saved_reversal_comparisons','computation_origin':'2025-01-01T00:00:00Z'}
OUTCOMES={'schema_version':SCHEMA,'selection_reference':SELECTION_REFERENCE}


def normalize_configuration(*,detector,outcomes,statistics,inputs,scope,gap_policy):
    if (detector!=DETECTOR or outcomes!=OUTCOMES or statistics or inputs or gap_policy!='reject'
        or scope.get('timeframe')!='15m' or scope.get('instrument_id')!=INSTRUMENT_ID
        or utc(scope.get('start'))!=utc('2025-02-01T00:00:00Z')
        or utc(scope.get('end'))!=utc('2025-07-19T00:00:00Z')
        or scope.get('indicator_id') or scope.get('indicator_param_overrides')
        or ('warmup_bars' in scope and (type(scope['warmup_bars']) is not int or scope['warmup_bars']!=2976))):
        raise ValueError('matched_outcomes_invalid: exact saved B3 reference, original BTC scope and fixed12bar protocol required')
    return deepcopy(OUTCOMES)


def load_saved_selection():
    """Read and validate typed immutable evidence, with no replay or calculation."""
    item=repository.get_item(SELECTION_REFERENCE['check_id'])
    payload=item['payload']
    if (item['id']!=SELECTION_REFERENCE['check_id'] or item['kind']!='research_check'
            or item['status']!='tested' or not payload.get('replayable') or not payload.get('observation_eligible')):
        raise ValueError('matched_outcomes_invalid: completed frozen replayable selection required')
    result=CheckResult.from_dict(payload['result'])
    evidence=CheckEvidenceBinding.from_dict(payload['evidence'])
    if (evidence.evidence_kind!='frozen_market_data' or result.result_hash!=SELECTION_REFERENCE['result_hash'] or evidence.evidence_hash!=SELECTION_REFERENCE['evidence_hash']
        or result.evidence_hash!=evidence.evidence_hash or result.evaluator_version!='12'
        or result.definition_hash!=evidence.definition_hash or result.request_hash!=evidence.request_hash
        or result.plan_hash!=evidence.plan_hash):
        raise ValueError('matched_outcomes_invalid: saved result/evidence identity mismatch')
    binding=evidence.input_binding
    if binding['dataset_id']!=DATASET_ID or binding['dataset_hash']!=DATASET_HASH or binding['max_commit_seq']!=223194126:
        raise ValueError('matched_outcomes_invalid: original frozen input required')
    selection=dict(result.result)
    if selection['schema_version']!='reversal_selection_only_result.v2' or selection['outcomes_calculated'] or len(selection['pairs'])!=999:
        raise ValueError('matched_outcomes_invalid: wrong saved selection population')
    manifest=deepcopy({k:selection[k] for k in ('rules','cases','pairs','dependence','populations','compatibility')})
    # Historical v12 manifest hashes integer histogram keys before JSON conversion.
    for pop in manifest['populations'].values():
        summary=pop['selection_summary']
        summary['chains_per_completion_clock_histogram']={int(k):v for k,v in summary['chains_per_completion_clock_histogram'].items()}
    if stable_hash(manifest)!=selection['selection_manifest_hash'] or selection['selection_manifest_hash']!=SELECTION_REFERENCE['selection_manifest_hash']:
        raise ValueError('matched_outcomes_invalid: selection manifest hash mismatch')
    return selection,binding


@dataclass(frozen=True)
class ReversalMatchedOutcomesEvaluator:
    evaluator_id:str='raw_forward_outcome'
    version:str='13'
    result_schema_version:str=RESULT_SCHEMA

    def declare_requirements(self,*,definition,request):
        normalize_configuration(detector=request.parameters['detector'],outcomes=request.parameters['outcomes'],
            statistics=request.parameters['statistics'],inputs=request.parameters['inputs'],scope=request.scope,gap_policy=request.parameters['gap_policy'])
        if request.dataset_id!=DATASET_ID or request.mode!='evidence':
            raise ValueError('matched_outcomes_invalid: evidence mode and original B3 Dataset required; no mutable preview')
        return {'input_kind':'market_data','indicator_ids':[],'warmup_floor_bars':2976,'feature_lookback_bars':2976,
            'feature_windows_seconds_by_alias':{},'outcome_horizons':[12],'required_outcome_horizons':[],
            'horizon_kind':'bars','entry_lag_bars':0,'invalidation_max_bars':0,'outcome_boundary':'evaluation_end_exclusive',
            'primary_candle_required_fields':['open','high','low','close','known_at']}

    def evaluate(self,*,plan,inputs):
        started=monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started>=RULES['limits']['wall_seconds']:
                raise ValueError('matched_outcomes_limit:120second method wall budget exceeded')
        primary=next(v for v in plan.market_data_requirements if v['alias']=='primary_bars')
        scope={**plan.evaluation_range,'start':plan.evaluation_range['start'],'end':plan.evaluation_range['end_exclusive'],
            'instrument_id':primary['instrument_id'],'timeframe':'15m'}
        normalize_configuration(detector=inputs['detector'],outcomes=inputs['outcomes'],statistics=inputs['statistics'],
            inputs=inputs.get('inputs',[]),scope=scope,gap_policy=plan.gap_policy)
        if (plan.materialization_range['as_of_commit_seq']!=223194126
                or utc(plan.materialization_range['start'])!=utc(DETECTOR['computation_origin'])
                or utc(plan.materialization_range['end_exclusive'])!=utc(scope['end'])):
            raise ValueError('matched_outcomes_invalid: original frozen input range and watermark required')
        frame=inputs['candles']
        if int(frame.memory_usage(index=True,deep=True).sum())>RULES['limits']['input_bytes']:
            raise ValueError('matched_outcomes_limit: input exceeds256MiB')
        _,rows=_prepare(frame,step=timedelta(minutes=15),volume_required=False)
        if len(rows)!=19104 or utc(rows[0]['open_time'])!=utc(DETECTOR['computation_origin']) or utc(rows[-1]['close_time'])!=utc(scope['end']):
            raise ValueError('matched_outcomes_invalid: incomplete original input')
        candles=tuple(RecognitionCandle.from_mapping(v) for v in rows)
        selection,binding=load_saved_selection()
        if (any(c.source_identity_key!=selection['source_identity_key'] or c.known_at!=c.close_time for c in candles)
                or len({c.open_time for c in candles})!=19104):
            raise ValueError('matched_outcomes_invalid: source or close availability mismatch')
        result=describe_selected_pairs(selection,candles,evaluation_end=utc(scope['end']),checkpoint=checkpoint)
        result.update(schema_version=RESULT_SCHEMA,check_family=self.evaluator_id,status='completed',
            analysis_status='selected_population_descriptive_only',selection_reference=deepcopy(SELECTION_REFERENCE),
            selected_dataset_id=binding['dataset_id'],selected_dataset_hash=binding['dataset_hash'],processed_bars=19104,
            data_quality=dict(inputs.get('data_quality') or {}),
            caveats=['Finite matched population only;no causal,predictive,profitability or independent-trial inference.',
                'Original pairs,barriers and weights retained;no selection rerun.',
                'One control nominal horizon crosses the frozen boundary;unresolved retained unless earlier first touch known.',
                'Historical interval-close availability is inferred,not measured receipt latency.'])
        checkpoint()
        if len(json.dumps(result,allow_nan=False,separators=(',',':')).encode())>RULES['limits']['result_bytes']:
            raise ValueError('matched_outcomes_limit: result exceeds16MiB')
        return result

"""Compose existing first-passage/progress kernels on validated saved populations."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import json
from time import monotonic
from typing import ClassVar

from core.execution_control import execution_checkpoint
from research_science.recognition import RecognitionCandle, utc
from research_science.reversal_matched_outcomes import describe_selected_pairs, summarize_matched_outcomes, RULES as PASSAGE_RULES
from research_science.reversal_matched_progress import describe_progress, summarize_progress, RULES as PROGRESS_RULES
from research_science.hashing import stable_hash
from . import repository
from .raw_candle_evaluator import _prepare
from .saved_selection_binding import context, reference, load_saved_selection, invalid

SCHEMA = 'reversal_saved_measurements.v1'
RESULT_SCHEMA = 'reversal_saved_measurements_result.v1'
DETECTOR = {'type': 'saved_reversal_selection'}
RULES = {
    'version': SCHEMA,
    'selection_contracts': ['raw_forward_outcome@12', 'raw_forward_outcome@14'],
    'population': 'Exact validated saved pairs, weights, barriers, support and ancestry; no recognition/rematching',
    'first_passage': deepcopy(PASSAGE_RULES),
    'progress': {k: deepcopy(v) for k, v in PROGRESS_RULES.items() if k != 'population'},
    'outcome_provenance': 'Computed in this Check and passed directly to the unchanged progress kernel',
    'reporting': 'Whole saved scope and original block/day labels; no pooled independence claim',
    'limits': {'candles': 19104, 'cases': 10000, 'pairs': 999, 'wall_seconds': 120,
               'input_bytes': 268435456, 'result_bytes': 16777216},
}


REUSABLE_SCHEMA = 'reversal_saved_measurements.v2'
REUSABLE_RESULT_SCHEMA = 'reversal_saved_measurements_result.v2'
# Versioned rule metadata stays fixed for existing Check18 payload identities.
# Current execution accepts only supported Check17 selections.
REUSABLE_RULES = deepcopy(RULES)
REUSABLE_RULES.update(version=REUSABLE_SCHEMA,
    selection_contracts=['raw_forward_outcome@12', 'raw_forward_outcome@14', 'raw_forward_outcome@17'],
    reporting='Whole saved scope and its declared populations;original root weights normalized only within each named reporting view')


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy, schema=REUSABLE_SCHEMA):
    if (detector != DETECTOR or not isinstance(outcomes, dict)
            or set(outcomes) != {'schema_version', 'selection_reference', 'frozen_context'}
            or outcomes['schema_version'] != schema or statistics or inputs or gap_policy != 'reject'
            or scope.get('timeframe') != '15m' or scope.get('indicator_id') or scope.get('indicator_param_overrides')):
        invalid('fixed saved-selection measurement protocol required')
    declared = context(outcomes['frozen_context'])
    warmup = int((utc(declared['decision_start'])-utc(declared['input_start']))/timedelta(minutes=15))
    if (scope.get('instrument_id') != declared['instrument_id']
            or utc(scope.get('start')) != utc(declared['decision_start'])
            or utc(scope.get('end')) != utc(declared['decision_end_exclusive'])
            or ('warmup_bars' in scope and (type(scope['warmup_bars']) is not int or scope['warmup_bars'] != warmup))):
        invalid('request scope must match the original saved input/decision scope')
    return {'schema_version': schema, 'selection_reference': reference(outcomes['selection_reference']),
            'frozen_context': declared}


@dataclass(frozen=True)
class ReusableSelectionMeasurementsEvaluator:
    evaluator_id: str = 'raw_forward_outcome'
    version: str = '18'
    result_schema_version: str = REUSABLE_RESULT_SCHEMA
    configuration_schema: str = REUSABLE_SCHEMA
    measurement_rules: ClassVar[dict] = REUSABLE_RULES

    def additional_measurements(self, selection, passage, progress):
        views={}
        for name,population in selection['populations'].items():
            scope=population['evaluation_range']
            start,end=utc(scope['start']),utc(scope['end_exclusive'])
            selected=[row for row in progress['pairs'] if start<=utc(row['decision_at'])<end]
            first=[row for row in passage['pairs'] if start<=utc(row['decision_at'])<end]
            views[name]={'evaluation_range':deepcopy(scope),'first_passage':summarize_matched_outcomes(first),
                'progress':{role:{'all':summarize_progress(selected,role),
                    'originally_neither':summarize_progress([row for row in selected
                        if row[role]['original_outcome']['classification']=='neither'],role)}
                    for role in ('case','control')}}
        return {'measurements_by_population':views}

    def declare_requirements(self, *, definition, request):
        config = normalize_configuration(detector=request.parameters['detector'], outcomes=request.parameters['outcomes'],
            statistics=request.parameters['statistics'], inputs=request.parameters['inputs'],
            scope=request.scope, gap_policy=request.parameters['gap_policy'], schema=self.configuration_schema)
        declared = config['frozen_context']
        if request.mode != 'evidence' or request.dataset_id != declared['dataset_id']:
            invalid('evidence mode and original frozen Dataset required')
        warmup = int((utc(declared['decision_start'])-utc(declared['input_start']))/timedelta(minutes=15))
        return {'input_kind': 'market_data', 'indicator_ids': [], 'warmup_floor_bars': warmup,
            'feature_lookback_bars': warmup, 'feature_windows_seconds_by_alias': {},
            'outcome_horizons': [12], 'required_outcome_horizons': [], 'horizon_kind': 'bars',
            'entry_lag_bars': 0, 'invalidation_max_bars': 0, 'outcome_boundary': 'evaluation_end_exclusive',
            'primary_candle_required_fields': ['open', 'high', 'low', 'close', 'known_at']}

    def evaluate(self, *, plan, inputs):
        started = monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started >= RULES['limits']['wall_seconds']:
                raise ValueError('saved_selection_limit:120second combined method wall budget exceeded')
        primary = next(r for r in plan.market_data_requirements if r['alias'] == 'primary_bars')
        config = normalize_configuration(detector=inputs['detector'], outcomes=inputs['outcomes'],
            statistics=inputs['statistics'], inputs=inputs.get('inputs', []), gap_policy=plan.gap_policy,
            scope={'instrument_id': primary['instrument_id'], 'timeframe': '15m',
                   'start': plan.evaluation_range['start'], 'end': plan.evaluation_range['end_exclusive']}, schema=self.configuration_schema)
        saved = load_saved_selection(config['selection_reference'], config['frozen_context'],
            item_loader=repository.get_item, checkpoint=checkpoint)
        ctx = saved.context
        if (inputs.get('frozen_input_binding') != saved.binding or plan.indicator_graph
                or utc(plan.materialization_range['start']) != utc(ctx['input_start'])
                or utc(plan.materialization_range['end_exclusive']) != utc(ctx['input_end_exclusive'])
                or plan.materialization_range['as_of_commit_seq'] != ctx['max_commit_seq']
                or plan.outcome_tail['seconds'] != 0):
            invalid('actual frozen input binding/range/watermark differs from the saved Check')
        frame = inputs['candles']
        if int(frame.memory_usage(index=True, deep=True).sum()) > RULES['limits']['input_bytes']:
            raise ValueError('saved_selection_limit: input exceeds256MiB')
        if len(frame) != ctx['input_rows']:
            invalid('actual input row count differs')
        _, rows = _prepare(frame, step=timedelta(minutes=15), volume_required=False)
        candles = tuple(RecognitionCandle.from_mapping(v) for v in rows)
        start = utc(ctx['input_start'])
        for index, candle in enumerate(candles):
            checkpoint()
            if (candle.open_time != start+index*timedelta(minutes=15)
                    or candle.source_identity_key != ctx['source_identity_key'] or candle.known_at != candle.close_time):
                invalid('complete native clock/source/close availability differs')
        passage = describe_selected_pairs(saved.selection, candles,
            evaluation_end=utc(ctx['input_end_exclusive']), checkpoint=checkpoint)
        progress = describe_progress(saved.selection, passage, candles,
            evaluation_end=utc(ctx['input_end_exclusive']), checkpoint=checkpoint)
        # New explicit composition: no fabricated saved outcome identity or historical BTC labels.
        result = {'schema_version': self.result_schema_version, 'check_family': self.evaluator_id, 'status': 'completed',
            'rules': deepcopy(self.measurement_rules), 'selection_reference': saved.reference, 'frozen_context': ctx,
            'outcome_provenance': {'kind': 'computed_in_this_check',
                'first_passage_kernel': PASSAGE_RULES['version'], 'progress_kernel': PROGRESS_RULES['version']},
            'pairs': progress['pairs'], 'measurements': {
                'first_passage': passage['matched_outcome_comparison']['combined'],
                'progress': progress['progress_comparison']['combined']},
            'by_saved_block': {str(i): passage['by_block'][str(i)] for i in range(int((utc(ctx['decision_end_exclusive'])-utc(ctx['decision_start']))/timedelta(days=28)))}, 'by_case_completion_day': passage['by_case_completion_day'],
            'selection_audit': passage['selection_audit'], 'saved_dependence': deepcopy(saved.selection['dependence']),
            'saved_population_dependence': {name: deepcopy(pop['dependence']) for name, pop in saved.selection['populations'].items()},
            'sample_count': passage['sample_count'], 'sample_count_meaning': passage['sample_count_meaning'],
            'processed_bars': len(candles), 'data_quality': deepcopy(dict(inputs.get('data_quality') or {})),
            'outcomes_calculated': True, 'reselection_performed': False,
            'inference_claim': False, 'analysis_status': 'post_outcome_descriptive_only',
            'caveats': ['Observational completion-close anchors; no executed-fill or predictive claim.',
                'Original weights and support retained; completion clocks are not independent trials.',
                'Full-window progress can follow failure and never reclassifies first passage.',
                'Pre-failure OHLC bounds retain intrabar ambiguity; censored mass remains explicit.',
                'No new tail, recognition, matching or target optimization.']}
        result.update(self.additional_measurements(saved.selection, passage, progress))
        result = json.loads(json.dumps(result, allow_nan=False))
        result['measurement_manifest_fields'] = list(result)+['measurement_manifest_fields']
        result['measurement_manifest_hash'] = stable_hash({k: result[k] for k in result['measurement_manifest_fields']})
        checkpoint()
        if len(json.dumps(result, allow_nan=False, separators=(',', ':')).encode()) > RULES['limits']['result_bytes']:
            raise ValueError('saved_selection_limit: result exceeds16MiB')
        return result


def normalize_reusable_configuration(**kwargs):
    return normalize_configuration(**kwargs, schema=REUSABLE_SCHEMA)

"""Fixed BTC progress extraction through the ordinary frozen-input Check path."""
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import timedelta
import json
from time import monotonic

from core.execution_control import execution_checkpoint
from research_science.check import CheckEvidenceBinding, CheckResult
from research_science.recognition import RecognitionCandle, utc
from research_science.reversal_matched_progress import SCHEMA, RULES, describe_progress
from research_science.study import stable_hash
from .raw_candle_evaluator import _prepare
from . import repository
from .reversal_matched_outcomes_evaluator import (
    DATASET_ID, DATASET_HASH, INSTRUMENT_ID, DETECTOR, SELECTION_REFERENCE,
    OUTCOMES as FIRST_PASSAGE_OUTCOMES, ReversalMatchedOutcomesEvaluator,
    normalize_configuration as normalize_first_passage, load_saved_selection,
)

RESULT_SCHEMA = 'reversal_matched_progress_result.v1'
OUTCOME_REFERENCE = {
    'source_check_id': 'e6fdbfb8-45d7-4191-a794-d35c74b0cfdf',
    'result_hash': '60f167594037f1c5cd61eb871e8270a59dbaa23d580d8fd146bda7eb8fecd9e7',
    'evidence_hash': '962df9cecf3b136b7c8395ce7d6c32df08765de2ee374abfcf5535f2f3c58ff3',
    'matched_outcome_manifest_hash': '4bc19fdbbad9f5bb8aeb88a6d0f0bbcbb0c508d5c8e78986aa443f56b9f5d9f9',
}
OUTCOMES = {'schema_version': SCHEMA, 'selection_reference': SELECTION_REFERENCE,
            'outcome_reference': OUTCOME_REFERENCE}


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    if outcomes != OUTCOMES:
        raise ValueError('matched_progress_invalid: exact saved references and fixed progress protocol required')
    normalize_first_passage(detector=detector, outcomes=FIRST_PASSAGE_OUTCOMES,
        statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
    return deepcopy(OUTCOMES)


def load_saved_outcomes():
    """Validate typed immutable evidence without replaying its scientific attempt."""
    item = repository.get_item(OUTCOME_REFERENCE['source_check_id'])
    payload = item['payload']
    if (item['id'] != OUTCOME_REFERENCE['source_check_id'] or item['kind'] != 'research_check'
            or item['status'] != 'tested' or not payload.get('replayable') or not payload.get('observation_eligible')):
        raise ValueError('matched_progress_invalid: completed frozen outcome evidence required')
    result = CheckResult.from_dict(payload['result'])
    evidence = CheckEvidenceBinding.from_dict(payload['evidence'])
    if (evidence.evidence_kind != 'frozen_market_data' or result.evaluator_id != 'raw_forward_outcome'
            or result.evaluator_version != '13' or result.result_hash != OUTCOME_REFERENCE['result_hash']
            or evidence.evidence_hash != OUTCOME_REFERENCE['evidence_hash']
            or result.evidence_hash != evidence.evidence_hash or result.definition_hash != evidence.definition_hash
            or result.request_hash != evidence.request_hash or result.plan_hash != evidence.plan_hash):
        raise ValueError('matched_progress_invalid: outcome result/evidence identity mismatch')
    binding = evidence.input_binding
    if (binding['dataset_id'] != DATASET_ID or binding['dataset_hash'] != DATASET_HASH
            or binding['max_commit_seq'] != 223194126):
        raise ValueError('matched_progress_invalid: original frozen BTC input required')
    saved = dict(result.result)
    reference = saved['selection_reference']
    if (saved['schema_version'] != 'reversal_matched_first_passage_result.v1'
            or len(saved['pairs']) != 999 or not saved['outcomes_calculated']
            or reference['source_check_id'] != SELECTION_REFERENCE['check_id']
            or any(reference[k] != SELECTION_REFERENCE[k] for k in ('result_hash', 'evidence_hash', 'selection_manifest_hash'))
            or saved['matched_outcome_manifest_hash'] != OUTCOME_REFERENCE['matched_outcome_manifest_hash']
            or stable_hash({k: saved[k] for k in saved['matched_outcome_manifest_fields']}) != saved['matched_outcome_manifest_hash']):
        raise ValueError('matched_progress_invalid: outcome manifest mismatch')
    return saved, binding


@dataclass(frozen=True)
class ReversalMatchedProgressEvaluator:
    evaluator_id: str = 'raw_forward_outcome'
    version: str = '15'
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        normalize_configuration(detector=request.parameters['detector'], outcomes=request.parameters['outcomes'],
            statistics=request.parameters['statistics'], inputs=request.parameters['inputs'],
            scope=request.scope, gap_policy=request.parameters['gap_policy'])
        # Exactly v13's existing original-input support; no new tail acquisition.
        original = replace(request, request_hash='', parameters={**request.parameters, 'outcomes': FIRST_PASSAGE_OUTCOMES})
        return ReversalMatchedOutcomesEvaluator().declare_requirements(definition=definition, request=original)

    def evaluate(self, *, plan, inputs):
        started = monotonic()
        def checkpoint():
            execution_checkpoint()
            if monotonic()-started >= RULES['limits']['wall_seconds']:
                raise ValueError('matched_progress_limit:120second method wall budget exceeded')
        primary = next(v for v in plan.market_data_requirements if v['alias'] == 'primary_bars')
        scope = {**plan.evaluation_range, 'end': plan.evaluation_range['end_exclusive'],
                 'instrument_id': primary['instrument_id'], 'timeframe': '15m'}
        normalize_configuration(detector=inputs['detector'], outcomes=inputs['outcomes'], statistics=inputs['statistics'],
            inputs=inputs.get('inputs', []), scope=scope, gap_policy=plan.gap_policy)
        if (plan.materialization_range['as_of_commit_seq'] != 223194126
                or utc(plan.materialization_range['start']) != utc(DETECTOR['computation_origin'])
                or utc(plan.materialization_range['end_exclusive']) != utc(scope['end'])):
            raise ValueError('matched_progress_invalid: original frozen range and watermark required')
        frame = inputs['candles']
        if int(frame.memory_usage(index=True, deep=True).sum()) > RULES['limits']['input_bytes']:
            raise ValueError('matched_progress_limit: input exceeds256MiB')
        _, rows = _prepare(frame, step=timedelta(minutes=15), volume_required=False)
        if (len(rows) != 19104 or utc(rows[0]['open_time']) != utc(DETECTOR['computation_origin'])
                or utc(rows[-1]['close_time']) != utc(scope['end'])):
            raise ValueError('matched_progress_invalid: incomplete original input')
        candles = tuple(RecognitionCandle.from_mapping(v) for v in rows)
        selection, binding = load_saved_selection()
        saved, outcome_binding = load_saved_outcomes()
        if binding != outcome_binding:
            raise ValueError('matched_progress_invalid: saved input bindings differ')
        if any(c.source_identity_key != selection['source_identity_key'] or c.known_at != c.close_time for c in candles):
            raise ValueError('matched_progress_invalid: source or close availability mismatch')
        result = describe_progress(selection, saved, candles, evaluation_end=utc(scope['end']), checkpoint=checkpoint)
        result.update(schema_version=RESULT_SCHEMA, check_family=self.evaluator_id, status='completed',
            analysis_status='post_outcome_descriptive_only',
            selection_reference={**deepcopy(SELECTION_REFERENCE), 'source_check_id': SELECTION_REFERENCE['check_id']},
            outcome_reference=deepcopy(OUTCOME_REFERENCE), selected_dataset_id=binding['dataset_id'],
            selected_dataset_hash=binding['dataset_hash'], processed_bars=19104,
            data_quality=dict(inputs.get('data_quality') or {}),
            caveats=['Post-outcome description; no prediction, optimized target or independent-trial inference.',
                'Full-window maximum can occur after failure and never changes the original class.',
                'OHLC supplies bounds on progress before failure, not intrabar ordering.',
                'Partial control retained as censored; no acquired tail or beyond-three-hour claim.',
                'Interval-close availability is inferred, not measured receipt latency.'])
        checkpoint()
        if len(json.dumps(result, allow_nan=False, separators=(',', ':')).encode()) > RULES['limits']['result_bytes']:
            raise ValueError('matched_progress_limit: result exceeds16MiB')
        return result

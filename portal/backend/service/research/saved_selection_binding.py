"""Validated saved v12/v14 populations; no replay, recognition or matching."""
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import json
import math
import re
from uuid import UUID

from research_science.check import CheckDefinition, CheckRequest, ResolvedCheckPlan, CheckResult, CheckEvidenceBinding
from research_science.recognition import utc, plain
from research_science.reversal_selection import moment_weights
from research_science.study import stable_hash

MANIFEST_FIELDS = ('rules', 'cases', 'pairs', 'dependence', 'populations', 'compatibility')
SCHEMAS = {'12': 'reversal_selection_only_result.v2', '14': 'reversal_two_block_selection_result.v1',
           '17': 'reversal_population_selection_result.v1'}
STEP = timedelta(minutes=15)
CONTEXT_KEYS = {'dataset_id', 'dataset_hash', 'input_binding_hash', 'max_commit_seq', 'instrument_id',
    'input_start', 'input_end_exclusive', 'decision_start', 'decision_end_exclusive',
    'source_identity_key', 'input_rows', 'case_count', 'pair_count', 'completion_clock_count'}
REFERENCE_KEYS = {'source_check_id', 'result_hash', 'evidence_hash', 'selection_manifest_hash'}


def invalid(message):
    raise ValueError('saved_selection_invalid: '+message)


def reference(value):
    if not isinstance(value, dict) or set(value) != REFERENCE_KEYS:
        invalid('exact saved Check/result/evidence/manifest reference required')
    try:
        if str(UUID(value['source_check_id'])) != value['source_check_id']:
            invalid('canonical source Check identity required')
    except (ValueError, TypeError, AttributeError):
        invalid('source Check identity required')
    for key in REFERENCE_KEYS-{'source_check_id'}:
        if not isinstance(value[key], str) or not re.fullmatch(r'[0-9a-f]{64}', value[key]):
            invalid('invalid '+key)
    return dict(value)


def context(value):
    if not isinstance(value, dict) or set(value) != CONTEXT_KEYS:
        invalid('exact frozen selection context required')
    result = dict(value)
    for key in ('dataset_hash', 'input_binding_hash', 'source_identity_key'):
        if not isinstance(result[key], str) or not re.fullmatch(r'[0-9a-f]{64}', result[key]):
            invalid('invalid '+key)
    for key in ('dataset_id', 'instrument_id'):
        if not isinstance(result[key], str) or not result[key].strip():
            invalid('invalid '+key)
    if type(result['pair_count']) is int and result['pair_count'] > 999:
        invalid('saved measurement supports at most999pairs; entire selection rejected without truncation or rematching')
    for key, maximum in (('input_rows', 19104), ('case_count', 10000), ('pair_count', 999),
                         ('completion_clock_count', 999), ('max_commit_seq', 2**63-1)):
        if type(result[key]) is not int or not 0 <= result[key] <= maximum:
            invalid('bounded integer '+key+' required')
    if (not result['input_rows'] or not result['max_commit_seq']
            or result['dataset_id'] != 'mds_'+result['dataset_hash'][:32]
            or not result['completion_clock_count'] <= result['pair_count'] <= result['case_count']
            or bool(result['pair_count']) != bool(result['completion_clock_count'])):
        invalid('consistent frozen identity/counts required')
    for key in ('input_start', 'input_end_exclusive', 'decision_start', 'decision_end_exclusive'):
        result[key] = plain(utc(result[key]))
    start, end, left, right = (utc(result[k]) for k in
        ('input_start', 'input_end_exclusive', 'decision_start', 'decision_end_exclusive'))
    if (not start <= left < right == end or end-start != result['input_rows']*STEP
            or (left-start) % STEP or any(t.timestamp() % 900 for t in (start,end,left,right))):
        invalid('aligned original origin/input/decision support required; no tail')
    return result


def native_candle_series(binding, *, dataset_id, source_key=None):
    """One existing frozen native15m input contract, independent of measurement."""
    if not isinstance(binding,dict):
        invalid('actual frozen input binding required')
    series = binding['series']
    if (len(series) != 1 or binding['provider_access'] != 'disabled' or binding['provider_call_performed'] is not False
            or binding['recorded_gaps'] or binding['dataset_id'] != dataset_id):
        invalid('one complete provider-free frozen series required')
    series = series[0]
    source = series['source_binding']
    if source_key is None:
        keys = source.get('resolved_source_identity_keys',[])
        if len(keys) != 1:
            invalid('one exact native source required')
        source_key = keys[0]
    if (series['alias'] != 'primary_bars' or series['fact_type'] != 'candle.ohlcv'
            or series['contract_version'] != 'candle.ohlcv.v1' or series['timeframe_seconds'] != 900
            or series.get('recorded_gaps') or series.get('disclosed_gaps')
            or source['mode'] != 'exact' or source['resolved_source_identity_keys'] != [source_key]
            or set(source['sources']) != {source_key}
            or source['sources'][source_key]['source_kind'] != 'historical_api'
            or not source['sources'][source_key]['adapter_version'].endswith('Provider:candle.ohlcv.v1')
            or source['source_summary']['counts'] != {source_key: series['row_count']}):
        invalid('exact native historical15m source and complete rows required')
    return series, source_key


@dataclass(frozen=True)
class SavedSelection:
    selection: dict
    binding: dict
    context: dict
    reference: dict
    evaluator_version: str


def load_saved_selection(raw_reference, declared_context, *, item_loader, checkpoint=lambda: None,
                         admitted_versions=('12','14')):
    """Validate immutable envelopes/context without ScientificAttempt resolution."""
    from .registry import reconstruct_pinned_check_definition, CHECK_REGISTRY
    from research_science.reversal_six_block_selection import RULES as SIX_BLOCK_RULES
    from research_science.reversal_two_block_selection import RULES as TWO_BLOCK_RULES
    from research_science.reversal_population import RULES as POPULATION_RULES, MANIFEST_FIELDS as POPULATION_FIELDS
    from .reversal_population_evaluator import population_for
    ref, expected = reference(raw_reference), context(declared_context)
    item = item_loader(ref['source_check_id'])
    if not isinstance(item, dict) or not isinstance(item.get('payload'), dict):
        invalid('saved Check unavailable: '+ref['source_check_id'])
    payload = item['payload']
    if (item['id'] != ref['source_check_id'] or item['kind'] != 'research_check' or item['status'] != 'tested'
            or item.get('replayable') is not True or item.get('observation_eligible') is not True
            or payload.get('replayable') is not True or payload.get('observation_eligible') is not True):
        invalid('completed replayable eligible frozen Check required')
    definition = CheckDefinition.from_dict(payload['definition'])
    request = CheckRequest.from_dict(payload['request'])
    plan = ResolvedCheckPlan.from_dict(payload['plan'])
    result = CheckResult.from_dict(payload['result'])
    evidence = CheckEvidenceBinding.from_dict(payload['evidence'])
    version = result.evaluator_version
    if (result.evaluator_id != 'raw_forward_outcome' or version not in admitted_versions or version not in SCHEMAS
            or definition.evaluator_id != result.evaluator_id or definition.evaluator_version != version
            or definition.definition_id != result.evaluator_id or definition.definition_version.split('+')[0] != version
            or request.mode != 'evidence' or evidence.evidence_kind != 'frozen_market_data'
            or result.result_hash != ref['result_hash'] or evidence.evidence_hash != ref['evidence_hash']
            or result.evidence_hash != evidence.evidence_hash
            or not definition.definition_hash == request.definition_hash == evidence.definition_hash == result.definition_hash
            or not request.request_hash == plan.request_hash == evidence.request_hash == result.request_hash
            or not plan.plan_hash == evidence.plan_hash == result.plan_hash
            or reconstruct_pinned_check_definition(request, plan).to_dict() != definition.to_dict()):
        invalid('typed definition/request/plan/result/evidence linkage differs')
    if (plan.indicator_graph or plan.gap_policy != 'reject' or plan.outcome_tail['seconds'] != 0
            or dict(plan.execution) != dict(CHECK_REGISTRY.resolve_evaluator(definition).declare_requirements(
                definition=definition, request=request))):
        invalid('saved selection execution/support contract differs')
    selection, binding = deepcopy(dict(result.result)), deepcopy(dict(evidence.input_binding))
    source_rules = {'12': SIX_BLOCK_RULES, '14': TWO_BLOCK_RULES, '17': POPULATION_RULES}[version]
    fields = POPULATION_FIELDS if version == '17' else MANIFEST_FIELDS
    if (selection['schema_version'] != SCHEMAS[version] or selection['outcomes_calculated'] is not False
            or selection['rules'] != source_rules
            or selection['status'] != 'completed' or selection['selection_manifest_hash'] != ref['selection_manifest_hash']
            or len(json.dumps(selection, allow_nan=False).encode()) > 16777216):
        invalid('unsupported or oversized saved selection')
    manifest = deepcopy({key: selection[key] for key in fields})
    if version == '12':
        # Preserve the historical int-key hash projection, including numeric order.
        for population in manifest['populations'].values():
            histogram = population['selection_summary']['chains_per_completion_clock_histogram']
            if any(not isinstance(k, str) or not k.isdecimal() or str(int(k)) != k for k in histogram):
                invalid('historical v12 histogram key encoding differs')
            population['selection_summary']['chains_per_completion_clock_histogram'] = {int(k): v for k,v in histogram.items()}
    else:
        if selection['selection_manifest_fields'] != list(fields):
            invalid('JSON-canonical manifest fields differ')
        manifest = json.loads(json.dumps(manifest, allow_nan=False))
    if version == '17' and selection['population_recipe'] != population_for(
            request.parameters['detector'], request.scope, request.parameters['outcomes']['populations']).to_dict():
        invalid('saved population configuration differs from its pinned request')
    if stable_hash(manifest) != ref['selection_manifest_hash']:
        invalid('selection manifest differs')
    series, source_key = native_candle_series(binding, dataset_id=request.dataset_id,
        source_key=selection['source_identity_key'])
    cases, pairs = selection['cases'], selection['pairs']
    case_ids = [c['event_id'] for c in cases]
    matched = [c for c in cases if c['match_status'] == 'matched']
    clocks = {c['decision_at'] for c in matched}
    actual = context({'dataset_id':binding['dataset_id'], 'dataset_hash':binding['dataset_hash'],
        'input_binding_hash':binding['binding_hash'], 'max_commit_seq':binding['max_commit_seq'],
        'instrument_id':series['instrument_id'], 'input_start':series['loaded_range']['start'],
        'input_end_exclusive':series['loaded_range']['end_exclusive'], 'decision_start':request.scope['start'],
        'decision_end_exclusive':request.scope['end'], 'source_identity_key':source_key,
        'input_rows':series['row_count'], 'case_count':len(cases), 'pair_count':len(pairs),
        'completion_clock_count':len(clocks)})
    if actual != expected:
        invalid('declared frozen context differs from validated saved population')
    for key, actual_value in (('start',actual['decision_start']), ('end_exclusive',actual['decision_end_exclusive'])):
        if utc(plan.evaluation_range[key]) != utc(actual_value) or utc(selection['evaluation_range'][key]) != utc(actual_value):
            invalid('saved decision scope differs')
    if (utc(plan.materialization_range['start']) != utc(actual['input_start'])
            or utc(selection['computation_origin']) != utc(actual['input_start'])
            or utc(plan.materialization_range['end_exclusive']) != utc(actual['input_end_exclusive'])
            or plan.materialization_range['as_of_commit_seq'] != actual['max_commit_seq']
            or selection['processed_bars'] != actual['input_rows'] or selection['candidate_count'] != len(cases)
            or selection['sample_count'] != len(clocks) or request.scope['instrument_id'] != actual['instrument_id']
            or request.scope['timeframe'] != '15m' or item['instrument_id'] != actual['instrument_id']
            or item['timeframe'] != '15m' or len(set(case_ids)) != len(case_ids)
            or Counter(p['event_id'] for p in pairs) != Counter(c['event_id'] for c in matched)
            or len({p['control']['control_id'] for p in pairs}) != len(pairs)
            or len({p['control']['decision_at'] for p in pairs}) != len(pairs)):
        invalid('saved scope/cardinality/identity differs')
    primary = next(r for r in plan.market_data_requirements if r['alias'] == 'primary_bars')
    if (len(plan.market_data_requirements) != 1 or primary['instrument_id'] != actual['instrument_id']
            or primary['timeframe_seconds'] != 900 or utc(primary['required_start']) != utc(actual['input_start'])
            or utc(primary['required_end']) != utc(actual['input_end_exclusive'])):
        invalid('saved primary requirement differs')
    if (series.get('requirement') != dict(primary)
            or utc(series['range_start']) != utc(actual['input_start'])
            or utc(series['range_end']) != utc(actual['input_end_exclusive'])):
        invalid('saved frozen series range/requirement differs')
    weights, all_weights = moment_weights(matched), moment_weights(cases)
    by_id = {c['event_id']:c for c in cases}
    origin, left, right = (utc(actual[k]) for k in ('input_start','decision_start','decision_end_exclusive'))
    for case in cases:
        checkpoint()
        at, n = utc(case['decision_at']), case['formation_candles']
        if (type(n) is not int or not 0 < n <= actual['input_rows'] or not left <= at < right
                or utc(case['formation_start']) != at-n*STEP or utc(case['formation_start']) < origin
                or type(case['block']) is not int or case['block'] != (at-left)//timedelta(days=28)
                or utc(case['support_start']) != max(origin, min(at-n*STEP, at-98*STEP))
                or utc(case['support_end']) != at+12*STEP
                or case['full_population_moment_weight'] != all_weights[case['event_id']]
                or case['matched_population_moment_weight'] != weights.get(case['event_id'],0.)):
            invalid('saved case/support/weights differ')
    for pair in pairs:
        checkpoint()
        case, control = by_id[pair['event_id']], pair['control']
        at, n = utc(control['decision_at']), control['trailing_candles']
        w = pair['completion_moment_weight']
        if (pair['decision_at'] != case['decision_at'] or type(n) is not int or n != case['formation_candles']
                or not left <= at < right or utc(control['trailing_start']) != at-n*STEP or at-n*STEP < origin
                or utc(control['support_start']) != max(origin,min(at-n*STEP,at-98*STEP))
                or utc(control['support_end']) != at+12*STEP or isinstance(w,bool) or not math.isfinite(w)
                or w <= 0 or w != weights[pair['event_id']]):
            invalid('saved pair/control/support/weights differ')
    summary = selection['selection_summary']
    if (summary['all'] != {'chains':len(cases),'completion_clocks':len({c['decision_at'] for c in cases})}
            or summary['matched'] != {'chains':len(pairs),'completion_clocks':len(clocks)}):
        invalid('saved summary cardinality differs')
    return SavedSelection(selection,binding,actual,ref,version)

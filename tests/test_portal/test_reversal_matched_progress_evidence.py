"""Ordinary Check planning, immutable references and synthetic loader boundary."""
from copy import deepcopy
from dataclasses import replace
import json
from types import SimpleNamespace
import pytest

from portal.backend.service.research import catalog, execution
from portal.backend.service.research import reversal_matched_progress_evaluator as adapter
from portal.backend.service.research.registry import normalize_check_request, definition_allows_evidence
from portal.backend.service.research.planning import plan_research_check
from research_science.check import CheckResult, CheckEvidenceBinding
from research_science.recognition import RecognitionCandle, utc
from research_science.reversal_matched_outcomes import describe_selected_pairs
from research_science.study import stable_hash


def payload():
    return {'check_family': 'raw_forward_outcome', 'mode': 'evidence', 'dataset_id': adapter.DATASET_ID,
        'scope': {'instrument_id': adapter.INSTRUMENT_ID, 'timeframe': '15m',
                  'start': '2025-02-01T00:00:00Z', 'end': '2025-07-19T00:00:00Z'},
        'detector': deepcopy(adapter.DETECTOR), 'outcomes': deepcopy(adapter.OUTCOMES), 'gap_policy': 'reject'}


def plan_for(p):
    definition, request = normalize_check_request(p)
    plan = plan_research_check(definition, request, inspect_coverage=False,
        instrument_loader=lambda key: {'id': key, 'symbol': 'BTC-USD'},
        indicator_planner=lambda *a, **k: {'indicators': [], 'requirements': [], 'warmup_bars': 0})
    return definition, request, plan


def test_exact_protocol_catalog_and_original_support_without_new_tail():
    definition, request, plan = plan_for(payload())
    assert definition.evaluator_version == '15' and definition_allows_evidence(definition)
    assert utc(plan.materialization_range['start']) == utc('2025-01-01T00:00:00Z')
    assert utc(plan.materialization_range['end_exclusive']) == utc('2025-07-19T00:00:00Z')
    assert plan.outcome_tail['seconds'] == 0 and plan.execution['outcome_horizons'] == [12]
    detail = catalog.get_check_definition('raw_forward_outcome', '15')
    assert detail['result_shape']['analysis_field'] == 'progress_comparison'
    assert normalize_check_request(detail['examples'][0]['request'])[0].evaluator_version == '15'


@pytest.mark.parametrize('change', [
    lambda p: p['outcomes']['outcome_reference'].update(result_hash='a'*64),
    lambda p: p['outcomes']['selection_reference'].update(result_hash='a'*64),
    lambda p: p['outcomes'].update(target_fraction=.5),
    lambda p: p['scope'].update(end='2025-07-20T00:00:00Z'),
    lambda p: p['scope'].update(instrument_id='ETH'),
    lambda p: p.update(assertions=[{'metric_path': 'sample_count', 'operator': 'gte', 'threshold': 1}]),
])
def test_no_target_reference_scope_or_gate_substitution(change):
    p = payload(); change(p)
    with pytest.raises(ValueError):
        normalize_check_request(p)


@pytest.mark.parametrize('field,value', [('mode', 'preview'), ('dataset_id', 'mds_'+'a'*32)])
def test_no_mutable_preview_or_other_frozen_dataset(field, value):
    definition, request = normalize_check_request(payload())
    wrong = replace(request, request_hash='', **{field: value})
    with pytest.raises(ValueError, match='original B3 Dataset'):
        adapter.ReversalMatchedProgressEvaluator().declare_requirements(definition=definition, request=wrong)


def saved_item(monkeypatch):
    from tests.test_research_science.test_check import _frozen_binding
    binding = _frozen_binding(); binding['max_commit_seq'] = 223194126; binding.pop('binding_hash', None)
    monkeypatch.setattr(adapter, 'DATASET_ID', binding['dataset_id'])
    monkeypatch.setattr(adapter, 'DATASET_HASH', binding['dataset_hash'])
    evidence = CheckEvidenceBinding(schema_version='research.check_evidence_binding.v2',
        definition_hash='definition', request_hash='request', plan_hash='plan', code_revision='synthetic',
        evidence_kind='frozen_market_data', input_binding=binding, indicator_graph_hash='graph',
        indicator_output_hash='output', fact_input_hash='facts', gap_transition_hash='transitions',
        quality_hash='quality', gaps_hash='gaps')
    saved = {'pairs': [{'synthetic': i} for i in range(999)], 'dependence': {}}
    saved['matched_outcome_manifest_fields'] = list(saved)+['matched_outcome_manifest_fields']
    saved['matched_outcome_manifest_hash'] = stable_hash({k: saved[k] for k in saved['matched_outcome_manifest_fields']})
    saved.update(schema_version='reversal_matched_first_passage_result.v1', outcomes_calculated=True,
        selection_reference={**adapter.SELECTION_REFERENCE, 'source_check_id': adapter.SELECTION_REFERENCE['check_id']})
    result = CheckResult(schema_version='research.check_result.v2', definition_hash='definition', request_hash='request',
        plan_hash='plan', evidence_hash=evidence.evidence_hash, evaluator_id='raw_forward_outcome', evaluator_version='13', result=saved)
    reference = {'source_check_id': 'synthetic', 'result_hash': result.result_hash,
        'evidence_hash': evidence.evidence_hash, 'matched_outcome_manifest_hash': saved['matched_outcome_manifest_hash']}
    monkeypatch.setattr(adapter, 'OUTCOME_REFERENCE', reference)
    item = json.loads(json.dumps({'id': 'synthetic', 'kind': 'research_check', 'status': 'tested',
        'payload': {'replayable': True, 'observation_eligible': True, 'result': result.to_dict(), 'evidence': evidence.to_dict()}}))
    monkeypatch.setattr(adapter.repository, 'get_item', lambda key: deepcopy(item))
    return item


def test_saved_outcome_typed_hashes_and_source_identity_survive_storage(monkeypatch):
    item = saved_item(monkeypatch); before = deepcopy(item)
    saved, binding = adapter.load_saved_outcomes()
    assert len(saved['pairs']) == 999 and binding['max_commit_seq'] == 223194126
    assert 'check_id' not in saved['selection_reference']
    assert saved['selection_reference']['source_check_id'] == adapter.SELECTION_REFERENCE['check_id']
    assert item == before


@pytest.mark.parametrize('change', [
    lambda i: i['payload']['result']['result']['pairs'][0].update(synthetic='changed'),
    lambda i: i['payload']['evidence'].update(code_revision='changed'),
    lambda i: i.update(status='draft'),
    lambda i: i['payload'].update(observation_eligible=False),
])
def test_changed_saved_evidence_fails_without_replay(monkeypatch, change):
    item = saved_item(monkeypatch); change(item)
    with pytest.raises(ValueError):
        adapter.load_saved_outcomes()


def test_actual_loader_boundary_uses_exact_saved_pairs_and_preserves_classifications(monkeypatch):
    import pandas as pd
    from tests.test_research_science.test_reversal_matched_outcomes import fixture
    selection, overrides, _ = fixture(); selection['source_identity_key'] = 'source'
    opened = pd.date_range('2025-01-01', periods=19104, freq='15min', tz='UTC')
    frame = pd.DataFrame({'timestamp': opened, 'close_time': opened+pd.Timedelta(minutes=15),
        'known_at': opened+pd.Timedelta(minutes=15), 'source_identity_key': 'source',
        'known_at_method': 'interval_close_inferred', 'open': 100., 'high': 100.5, 'low': 99.5, 'close': 100.})
    indices = {v: i for i, v in enumerate(opened)}
    for bar in overrides:
        frame.loc[indices[bar.open_time], 'high'] = bar.high
        frame.loc[indices[bar.open_time], 'low'] = bar.low
    _, records = adapter._prepare(frame, step=pd.Timedelta(minutes=15), volume_required=False)
    candles = tuple(RecognitionCandle.from_mapping(v) for v in records)
    saved = describe_selected_pairs(selection, candles, evaluation_end=utc('2025-07-19T00:00:00Z'))
    binding = {'dataset_id': adapter.DATASET_ID, 'dataset_hash': adapter.DATASET_HASH}
    monkeypatch.setattr(adapter, 'load_saved_selection', lambda: (deepcopy(selection), binding))
    monkeypatch.setattr(adapter, 'load_saved_outcomes', lambda: (deepcopy(saved), binding))
    monkeypatch.setattr(execution.candle_service, 'fetch_ohlcv_by_instrument', lambda *a, **k: frame)
    definition, request, plan = plan_for(payload())
    plan = replace(plan, plan_hash='', materialization_range={**plan.materialization_range, 'as_of_commit_seq': 223194126})
    loaded = execution._load_market_inputs(definition, request, plan, resolver=SimpleNamespace(dataset_binding={'recorded_gaps': []}))
    before = frame.copy(deep=True)
    result = execution._evaluate(definition, request, plan, loaded)
    pd.testing.assert_frame_equal(frame, before)
    assert result['processed_bars'] == 19104 and len(result['pairs']) == 3
    assert not result['reselection_performed'] and result['original_outcomes_preserved']
    typed = CheckResult(schema_version='research.check_result.v2', definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, evidence_hash='synthetic',
        evaluator_id=definition.evaluator_id, evaluator_version='15', result=result)
    retained = CheckResult.from_dict(json.loads(json.dumps(typed.to_dict()))).result
    assert retained['outcome_reference']['source_check_id'] == adapter.OUTCOME_REFERENCE['source_check_id']
    assert stable_hash({k: retained[k] for k in retained['progress_manifest_fields']}) == retained['progress_manifest_hash']
    wrong = replace(plan, plan_hash='', materialization_range={**plan.materialization_range, 'as_of_commit_seq': 223194127})
    with pytest.raises(ValueError, match='watermark required'):
        execution._evaluate(definition, request, wrong, loaded)

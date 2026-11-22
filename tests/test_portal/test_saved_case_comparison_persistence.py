"""Saved-request custody across real result projection and four-episode reuse."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from market_data.frozen import semantic_hash
from portal.backend.db.models import ResearchItemRecord
from portal.backend.service.research import saved_case_comparison_evaluator as adapter
from portal.backend.service.research.registry import normalize_check_request
from research_science.check import CheckResult, CheckEvidenceBinding
from research_science.recognition import plain, utc
from tests.test_portal.test_saved_case_comparison import payload, native_plan, native_inputs, exercise_direct_publication_export
from tests.test_portal.test_reversal_population_evidence import SOURCE
from tests.test_research_science.test_saved_case_comparison import row


def persist(definition, request, plan, inputs, result, identifier):
    binding = inputs['frozen_input_binding']
    evidence = CheckEvidenceBinding(schema_version='research.check_evidence_binding.v2', definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, code_revision='synthetic', evidence_kind='frozen_market_data',
        input_binding=binding, indicator_graph_hash=semantic_hash({'indicators': list(plan.indicator_graph)}),
        indicator_output_hash='output', fact_input_hash='facts', gap_transition_hash='transitions',
        quality_hash=semantic_hash(dict(binding.get('quality') or {})), gaps_hash=semantic_hash({'recorded_gaps': []}))
    contract = CheckResult(schema_version='research.check_result.v2', definition_hash=definition.definition_hash,
        request_hash=request.request_hash, plan_hash=plan.plan_hash, evidence_hash=evidence.evidence_hash,
        evaluator_id='event_fact_analysis', evaluator_version='16',
        result={**result, 'promotion_authority': False, 'execution_authority': False})
    p = {'schema_version': 'research_check_payload.v2', 'observation_eligible': True, 'replayable': True,
         'definition': definition.to_dict(), 'request': request.to_dict(), 'plan': plan.to_dict(),
         'result': contract.to_dict(), 'evidence': evidence.to_dict()}
    record = ResearchItemRecord(id=identifier, kind='research_check', status='tested', instrument_id=request.scope['instrument_id'],
        timeframe='15m', payload=json.loads(json.dumps(p)), created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1))
    item = adapter.repository.get_item(identifier, session=SimpleNamespace(get=lambda *args: record))
    return item, {'check_id': identifier, 'result_hash': contract.result_hash, 'evidence_hash': evidence.evidence_hash}


def event(raw, index):
    at = utc(raw['outcomes']['input_origin']) + timedelta(minutes=1500)
    identity = f'synthetic-comparison-{index}'
    return {'indicator_id': raw['scope']['indicator_id'], 'output_type': 'signal', 'output_name': 'provisional_reversals',
        'event_key': 'provisional_reversal_completed', 'time': plain(at-timedelta(minutes=15)), 'known_at': plain(at),
        'event': {'pattern_id': identity, 'known_at': plain(at), 'direction': 'long', 'metadata': {
            'recovery_candles': 3, 'context': {'sigma': .01, 'r': .02, 'q': 1., 'z_pre12': 0.},
            'recognition': {'event_id': identity, 'definition_id': 'completed_reversal_candidates', 'definition_version': 'provisional.1',
                'formation_start': plain(at-timedelta(hours=3)), 'known_at': plain(at), 'source_events': [],
                'context': {'source_identity_key': SOURCE, 'instrument_id': raw['scope']['instrument_id'],
                            'timeframe_seconds': 900, 'computation_origin': raw['outcomes']['input_origin']}}}}}


@pytest.fixture
def training(monkeypatch):
    raw = payload()
    origin = datetime(2023, 12, 1, tzinfo=UTC)
    rows = [row(i, label=adapter.math.CLASSES[i % 3], clock=origin+timedelta(minutes=15*i)) for i in range(210)]
    for r in rows:
        r['features'] = {**r['comparison_features'], 'recovery_candles': r['comparison_features']['N']}
        r['original_weight'] = 1/210
    saved = {'bytes': 1, 'cases': len(rows), 'binding_hash': 'frozen', 'input_identity': {'synthetic': 'development'},
        'source': SOURCE, 'request': SimpleNamespace(scope={'instrument_id': raw['scope']['instrument_id']}),
        'result': {'cases': rows, 'input_origin': plain(origin), 'evaluation_range': {'end_exclusive': '2023-12-10T00:00:00Z'}}}
    items = {}
    original = adapter.load_source
    def load(ref, *, kind, item_loader=None):
        if kind == 'training':
            return {**saved, 'reference': ref}
        return original(ref, kind=kind, item_loader=lambda key: items[key])
    monkeypatch.setattr(adapter, 'load_source', load)
    return raw, items


def test_persisted_first_model_reused_through_fourth_episode_and_exact_aggregate(training, monkeypatch):
    raw, items = training
    config = raw['statistics']['comparison']
    prior_refs = []
    first_model = first_ref = None
    results = []
    replayed = []
    original_barriers = adapter.barriers.evaluate
    def current_only(**kwargs):
        replayed.append(kwargs['inputs']['outcomes']['input_origin'])
        return original_barriers(**kwargs)
    monkeypatch.setattr(adapter.barriers, 'evaluate', current_only)
    for index, episode in enumerate(config['validation_episodes']):
        current = deepcopy(raw)
        c = current['statistics']['comparison']
        current['outcomes']['input_origin'] = episode['origin']
        current['scope'].update(start=plain(utc(episode['origin'])+timedelta(minutes=1470)), end=episode['end_exclusive'])
        if index:
            c['frozen_model_source'] = {**first_ref, 'model_hash': first_model['model_hash']}
        if index == 3:
            c['prior_scoring_sources'] = deepcopy(prior_refs)
        definition, request, plan = native_plan(current)
        inputs = native_inputs(request, plan)
        inputs['indicator_evidence']['outputs'] = [event(current, index)]
        result = adapter.evaluate(plan=plan, inputs=inputs)
        item, ref = persist(definition, request, plan, inputs, result, f'00000000-0000-0000-0000-{index+2:012d}')
        items[ref['check_id']] = item
        if not index:
            first_model, first_ref = deepcopy(result['model']), ref
            monkeypatch.setattr(adapter.math, 'fit_models', lambda *args, **kwargs: pytest.fail('refit forbidden'))
            assert 'check_id' not in item['payload']['result']['result']['comparison_configuration']['training_sources'][0]
            assert 'check_id' in item['payload']['request']['parameters']['statistics']['comparison']['training_sources'][0]
        assert item['payload']['result']['result']['model'] == first_model
        assert result['model'] == first_model
        assert adapter.load_source({**first_ref, 'model_hash': first_model['model_hash']}, kind='model')['result']['model'] == first_model
        assert items[first_ref['check_id']]['payload']['result']['result_hash'] == first_ref['result_hash']
        prior_refs.append({**ref, 'scored_evidence_hash': result['scored_evidence_hash']})
        results.append(result)
    assert results[-1]['comparison']['episode_count'] == 4
    assert [e['origin'] for e in results[-1]['comparison']['episodes']] == [e['origin'] for e in config['validation_episodes']]
    assert all(b['complete'] for b in results[-1]['comparison']['chronological_blocks'])
    assert replayed == [e['origin'] for e in config['validation_episodes']]
    original_item = deepcopy(items[first_ref['check_id']])
    mutations = [
        lambda p: p['request']['parameters']['statistics']['comparison']['training_sources'][0].update(check_id='00000000-0000-0000-0000-000000000099'),
        lambda p: p['result']['result']['comparison_configuration']['training_sources'][0].update(result_hash='f'*64),
        lambda p: p['result']['result']['comparison_configuration']['comparison_recipe']['model'].update(C=2.),
        lambda p: p['result']['result']['model']['arms']['base']['intercepts'].__setitem__(0, 99.),
        lambda p: p['result']['result']['cases'][0]['outcome'].update(classification='failure'),
        lambda p: p['result']['result'].update(input_origin='2024-01-01T00:15:00Z'),
    ]
    for mutate in mutations:
        tampered = deepcopy(original_item)
        mutate(tampered['payload'])
        items[first_ref['check_id']] = tampered
        with pytest.raises(ValueError):
            adapter.load_source({**first_ref, 'model_hash': first_model['model_hash']}, kind='model')
    items[first_ref['check_id']] = original_item
    changed = deepcopy(raw)
    episode = config['validation_episodes'][1]
    changed['outcomes']['input_origin'] = episode['origin']
    changed['scope'].update(start=plain(utc(episode['origin'])+timedelta(minutes=1470)), end=episode['end_exclusive'])
    changed['statistics']['comparison']['frozen_model_source'] = {**first_ref, 'model_hash': first_model['model_hash']}
    changed['statistics']['comparison']['training_sources'][0]['check_id'] = '00000000-0000-0000-0000-000000000099'
    _, changed_request, changed_plan = native_plan(changed)
    with pytest.raises(ValueError, match='frozen training/configuration differs'):
        adapter.evaluate(plan=changed_plan, inputs=native_inputs(changed_request, changed_plan))
    assert replayed == [e['origin'] for e in config['validation_episodes']]
    # Exercise the final projected configuration through direct publication and
    # native export too; it must not require fabricated Run identities.
    exercise_direct_publication_export(monkeypatch, definition, request, plan, inputs, result)


def test_projection_does_not_hide_scientific_or_authoritative_identity_changes():
    from research_science.check import _semantic_result
    raw = payload()
    _, request = normalize_check_request(raw)
    config = request.parameters['statistics']['comparison']
    saved = {'comparison_configuration': _semantic_result(config)}
    before = deepcopy(saved)
    assert adapter.saved_comparison_configuration(saved, request) == config
    assert saved == before
    for key in ('training_cutoff', 'comparison_recipe', 'validation_episodes'):
        changed = deepcopy(saved)
        changed['comparison_configuration'][key] = 'changed'
        with pytest.raises(ValueError, match='projection'):
            adapter.saved_comparison_configuration(changed, request)
    changed_parameters = deepcopy(request.parameters)
    changed_parameters['statistics']['comparison']['training_sources'][0]['check_id'] = '00000000-0000-0000-0000-000000000099'
    from dataclasses import replace
    changed_request = replace(request, parameters=changed_parameters, request_hash='')
    # IDs are intentionally absent from the result projection, but the complete
    # authoritative request differs and therefore cannot equal the live contract.
    assert adapter.saved_comparison_configuration(saved, changed_request) != config

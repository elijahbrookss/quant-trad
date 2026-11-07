"""Stored historical evidence has custody without an installed executor."""
from copy import deepcopy
import json
from pathlib import Path

import pytest
from research_science.check import CheckDefinition,CheckRequest,ResolvedCheckPlan,CheckEvidenceBinding,CheckResult
from portal.backend.service.research import historical_check_custody as custody
from portal.backend.service.research import service

ID='synthetic-original-historical-check'


@pytest.fixture
def historical_payload(monkeypatch):
    payload=json.loads((Path(__file__).parent/'fixtures/historical_check_v2_synthetic.json').read_text())
    archive=deepcopy(custody._archive())
    archive['receipts'].append({'id':ID,'definition_id':payload['definition']['definition_id'],
        'definition_version':payload['definition']['definition_version'],
        'definition_hash':payload['definition']['definition_hash'],
        'evidence_hash':payload['evidence']['evidence_hash'],'result_hash':payload['result']['result_hash'],
        'observation_eligible_at_creation':False})
    monkeypatch.setattr(custody,'_archive',lambda:archive)
    def no_executor(*args,**kwargs):
        raise AssertionError('Historical integrity must not resolve an installed evaluator or definition')
    monkeypatch.setattr(service.CHECK_REGISTRY,'resolve_evaluator',no_executor)
    monkeypatch.setattr(service.CHECK_REGISTRY,'resolve_definition',no_executor)
    return payload,archive


def test_complete_frozen_envelope_validates_without_installed_method(historical_payload):
    payload,_=historical_payload
    before=deepcopy(payload)
    values=service._validate_research_check_evidence_payload(payload,item_id=ID)
    assert values[-1].result_hash==payload['result']['result_hash']
    assert payload==before


@pytest.mark.parametrize('item_id',[None,'copied-new-record'])
def test_missing_or_copied_item_identity_refused(historical_payload,item_id):
    payload,_=historical_payload
    with pytest.raises(ValueError,match='no original custody receipt'):
        service._validate_research_check_evidence_payload(payload,item_id=item_id)


def test_coordinated_rehash_cannot_substitute_saved_result(historical_payload):
    payload,_=historical_payload
    changed=CheckResult.from_dict({**payload['result'],'result':{**payload['result']['result'],'new_field':'changed'},'result_hash':''})
    payload['result']=changed.to_dict()
    with pytest.raises(ValueError,match='no original custody receipt'):
        service._validate_research_check_evidence_payload(payload,item_id=ID)


def test_verified_historical_projection_is_not_execution_or_replay(historical_payload):
    payload,_=historical_payload
    item={'id':ID,'kind':'research_check','status':'tested','payload':payload}
    projected=service._project_evidence_classification(item)
    assert projected['stored_evidence_integrity']=='verified'
    assert projected['execution_available'] is False
    assert projected['replayable'] is False
    assert projected['observation_eligible'] is False  # raw2 was never durable-eligible
    assert projected['evidence_classification']=='historical_frozen_verified'


def test_stored_plan_semantics_still_rederived_even_with_trusted_test_receipt(historical_payload):
    payload,archive=historical_payload
    changed_plan=ResolvedCheckPlan.from_dict({**payload['plan'],'evaluation_range':{
        **payload['plan']['evaluation_range'],'start':'2026-01-01T06:00:00.000000Z'},'plan_hash':''})
    evidence=CheckEvidenceBinding.from_dict({**payload['evidence'],'plan_hash':changed_plan.plan_hash,'evidence_hash':''})
    result=CheckResult.from_dict({**payload['result'],'plan_hash':changed_plan.plan_hash,
        'evidence_hash':evidence.evidence_hash,'result_hash':''})
    payload.update(plan=changed_plan.to_dict(),evidence=evidence.to_dict(),result=result.to_dict())
    archive['receipts'][-1].update(evidence_hash=evidence.evidence_hash,result_hash=result.result_hash)
    with pytest.raises(ValueError,match='request/plan semantic disagreement'):
        service._validate_research_check_evidence_payload(payload,item_id=ID)


@pytest.fixture(params=['12','14'])
def historical_selection(request,monkeypatch):
    frozen=json.loads((Path(__file__).parent/f'fixtures/historical_saved_selection_v{request.param}.json').read_text())
    item=frozen['item'];payload=item['payload'];archive=deepcopy(custody._archive())
    archive['receipts'].append({'id':item['id'],'definition_id':payload['definition']['definition_id'],
        'definition_version':payload['definition']['definition_version'],'definition_hash':payload['definition']['definition_hash'],
        'evidence_hash':payload['evidence']['evidence_hash'],'result_hash':payload['result']['result_hash'],
        'observation_eligible_at_creation':True})
    monkeypatch.setattr(custody,'_archive',lambda:archive)
    def no_executor(*args,**kwargs):
        raise AssertionError('Historical saved read must not resolve evaluator')
    monkeypatch.setattr(service.CHECK_REGISTRY,'resolve_evaluator',no_executor)
    monkeypatch.setattr(service.CHECK_REGISTRY,'resolve_definition',no_executor)
    return frozen


def test_saved_population_reader_preserves_original_manifest_without_executor(historical_selection):
    from portal.backend.service.research.saved_selection_binding import load_saved_selection
    frozen=historical_selection;before=deepcopy(frozen)
    raw=deepcopy(frozen['item']);raw.pop('replayable');raw.pop('observation_eligible')
    saved=load_saved_selection(frozen['reference'],frozen['context'],item_loader=lambda key:raw)
    assert saved.selection['selection_manifest_hash']==frozen['reference']['selection_manifest_hash']
    assert frozen==before
    projected=service._project_evidence_classification(frozen['item'])
    assert projected['replayable'] is False and projected['observation_eligible'] is True
    assert load_saved_selection(frozen['reference'],frozen['context'],item_loader=lambda key:projected).reference==saved.reference


def test_explicit_ineligible_projection_still_rejected(historical_selection):
    from portal.backend.service.research.saved_selection_binding import load_saved_selection
    frozen=historical_selection;item=service._project_evidence_classification(frozen['item'])
    item['observation_eligible']=False
    with pytest.raises(ValueError,match='saved_selection_invalid'):
        load_saved_selection(frozen['reference'],frozen['context'],item_loader=lambda key:item)


def test_original_historical_images_binding_remains_readable(historical_selection,monkeypatch):
    from portal.backend.service.research import images,publication
    frozen=historical_selection;item=frozen['item'];ref=frozen['reference']
    monkeypatch.setattr(publication,'_assert_access',lambda *args:None)
    binding=images._binding(item,{'result_hash':ref['result_hash'],'evidence_hash':ref['evidence_hash']},None)
    assert binding['item_id']==item['id'] and binding['result_hash']==ref['result_hash']


def test_original_historical_publication_reference_classification(historical_selection,monkeypatch):
    from types import SimpleNamespace
    from portal.backend.service.research import publication
    from portal.backend.service.storage.repos.market_data import market_data_repo
    from portal.backend.service.market import backtest_dataset_service
    from market_data import contracts
    frozen=historical_selection;item=frozen['item'];ref=frozen['reference'];binding=item['payload']['evidence']['input_binding']
    monkeypatch.setattr(publication,'_assert_access',lambda *args:None)
    monkeypatch.setattr(publication.repository,'get_item',lambda *args,**kwargs:item)
    dataset=SimpleNamespace(dataset_id=binding['dataset_id'],dataset_hash=binding['dataset_hash'],series=[])
    monkeypatch.setattr(market_data_repo,'get_dataset',lambda *args:dataset)
    monkeypatch.setattr(contracts,'build_dataset_identity_hash',lambda *args:dataset.dataset_hash)
    monkeypatch.setattr(backtest_dataset_service,'validate_frozen_dataset_series',lambda **kwargs:None)
    session=SimpleNamespace(scalars=lambda *args:SimpleNamespace(all=lambda:[]))
    result=publication.resolve_reference({'item_id':item['id'],'kind':'research_check','role':'supports',
        'result_hash':ref['result_hash'],'evidence_hash':ref['evidence_hash']},session)
    assert result['evidence_classification']=='historical_frozen_verified'
    assert result['replay_evidence']=='not_established_by_publication'



def test_historical_replay_refuses_without_resolving_or_executing(historical_selection,monkeypatch):
    item=historical_selection['item']
    monkeypatch.setattr(service.repository,'get_item',lambda *args,**kwargs:item)
    monkeypatch.setattr(service,'execute_check_evidence',lambda *args,**kwargs:pytest.fail('retired replay cannot execute'))
    outcome=service.replay_research_check(item['id'])
    assert outcome['status']=='not_replayable' and outcome['provider_call_performed'] is False
    assert 'retired' in outcome['reasons'][0]


def test_eligible_historical_observation_uses_original_hashes_without_replay(historical_selection,monkeypatch):
    item=historical_selection['item'];writes=[]
    monkeypatch.setattr(service.repository,'get_item',lambda *args,**kwargs:item)
    monkeypatch.setattr(service.repository,'create_item',lambda **kwargs:(writes.append(kwargs) or {'id':'new-observation',**kwargs}))
    monkeypatch.setattr(service.repository,'create_link',lambda **kwargs:kwargs)
    result=service.create_observation_from_check_evidence(item['id'],{'title':'Preserved historical finding'})
    assert writes[0]['payload']['result_hash']==item['payload']['result']['result_hash']
    assert writes[0]['payload']['evidence_hash']==item['payload']['evidence']['evidence_hash']
    assert result['link']['source_item_id']==item['id']

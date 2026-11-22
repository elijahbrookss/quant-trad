"""Retired metadata custody never supplies execution admission."""
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from market_data.frozen import semantic_hash
from research_science.check import CheckDefinition
from portal.backend.service.research.historical_check_custody import (
    _custody, historical_base, historical_identity, validate_historical_definition,
)


def configured(version='12'):
    base = historical_base('raw_forward_outcome', version)
    parameters = dict(inputs=[], detector={'type':'historical'},outcomes={},statistics={},
                      assertions=[],gap_policy='reject',gap_rewarm_bars=0)
    graph = []
    rules = {**dict(base.material_rules),'registered_definition':{
        'definition_id':base.definition_id,'definition_version':base.definition_version,
        'definition_hash':base.definition_hash},**parameters,'indicator':{},
        'resolved_indicator_graph':graph,'resolved_indicator_graph_hash':semantic_hash({'indicators':graph})}
    definition = replace(base,definition_version=version+'+'+semantic_hash(rules)[:16],
                         material_rules=rules,definition_hash='')
    return definition,SimpleNamespace(parameters=parameters,scope={}),SimpleNamespace(indicator_graph=())


def test_archive_is_bounded_data_only_and_every_base_verifies():
    assert len(_custody()) == 17
    for row in _custody().values():
        base = CheckDefinition.from_dict(row['definition'])
        assert historical_identity(base)
        assert historical_base(base.definition_id,base.definition_version).to_dict() == base.to_dict()


def test_unknown_identity_fails_closed():
    with pytest.raises(ValueError,match='not in custody'):
        historical_base('raw_forward_outcome','999')


def test_configured_historical_definition_validates_without_any_registry(monkeypatch):
    definition,request,plan = configured()
    validate_historical_definition(definition,request,plan)


@pytest.mark.parametrize('tamper',['base','graph','request','lineage','version','extra'])
def test_coherently_rehashed_configured_tampering_rejected(tamper):
    definition,request,plan = configured()
    rules = deepcopy(dict(definition.material_rules))
    if tamper == 'base': rules['input_policy'] = 'different'
    elif tamper == 'graph': plan.indicator_graph = ({'indicator_id':'other'},)
    elif tamper == 'request': request.parameters = {**request.parameters,'detector':{'type':'different'}}
    elif tamper == 'lineage': rules['registered_definition']['definition_hash']='0'*64
    elif tamper == 'extra': rules['new_rule']='different'
    changed = replace(definition,material_rules=rules,
        definition_version='12+'+('0'*16 if tamper=='version' else semantic_hash(rules)[:16]),definition_hash='')
    with pytest.raises(ValueError,match='check_evidence_payload_invalid'):
        validate_historical_definition(changed,request,plan)


@pytest.mark.parametrize('tamper',[None,'definition','version','evidence','result','item_id'])
def test_original_receipt_anchors_full_hash_chain_and_rejects_rehash(tamper):
    from portal.backend.service.research.historical_check_custody import _archive,validate_historical_receipt
    receipt = _archive()['receipts'][0]
    definition=SimpleNamespace(definition_id=receipt['definition_id'],definition_version=receipt['definition_version'],
        definition_hash=receipt['definition_hash'])
    evidence=SimpleNamespace(evidence_hash=receipt['evidence_hash'])
    result=SimpleNamespace(result_hash=receipt['result_hash'])
    item_id=receipt['id'] if tamper!='item_id' else 'copied-record'
    if tamper=='definition': definition.definition_hash='0'*64
    elif tamper=='version': definition.definition_version=definition.definition_version.split('+')[0]+'+'+'0'*16
    elif tamper=='evidence': evidence.evidence_hash='0'*64
    elif tamper=='result': result.result_hash='0'*64
    if tamper:
        with pytest.raises(ValueError,match='no original custody receipt'):
            validate_historical_receipt(definition,evidence,result,item_id=item_id)
    else:
        assert validate_historical_receipt(definition,evidence,result,item_id=item_id)==receipt['id']


def test_custody_receipts_are_exact_finite_source_census():
    from portal.backend.service.research.historical_check_custody import _archive
    receipts=_archive()['receipts']
    assert len(receipts)==8 and len({r['id'] for r in receipts})==8
    assert {r['definition_version'].split('+')[0] for r in receipts}=={'4','5','10','11','12','13','14','15'}

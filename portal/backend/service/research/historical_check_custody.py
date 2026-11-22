"""Data-only identities of retired Check definitions; never execution admission.

The archive validates stored metadata, not scientific results against removed
code. It contains no evaluator factory, provider, storage or replay operation.
"""
from copy import deepcopy
from functools import lru_cache
import json
from pathlib import Path

from market_data.frozen import semantic_hash
from research_science.check import CheckDefinition

_CONFIGURED = frozenset({'registered_definition','inputs','indicator','detector','outcomes',
    'statistics','assertions','gap_policy','gap_rewarm_bars','resolved_indicator_graph','resolved_indicator_graph_hash'})


@lru_cache(maxsize=1)
def _archive():
    return json.loads(Path(__file__).with_suffix('.json').read_text())


@lru_cache(maxsize=1)
def _custody():
    return _archive()['definitions']


def historical_identity(definition):
    base = str(definition.definition_version).split('+', 1)[0]
    return f'{definition.definition_id}@{base}' in _custody()


def historical_base(definition_id, version):
    key = f'{definition_id}@{version}'
    if key not in _custody():
        raise ValueError('check_evidence_payload_invalid: historical identity is not in custody')
    return CheckDefinition.from_dict(deepcopy(_custody()[key]['definition']))


def validate_historical_definition(definition, request, plan):
    rules = dict(definition.material_rules)
    registered = rules.get('registered_definition')
    base_version = definition.definition_version.split('+', 1)[0]
    base = historical_base(definition.definition_id, base_version)
    if not isinstance(registered, dict):
        if definition.to_dict() != base.to_dict():
            raise ValueError('check_evidence_payload_invalid: historical definition is not in custody')
        return
    expected_lineage = {'definition_id':base.definition_id,'definition_version':base.definition_version,
                        'definition_hash':base.definition_hash}
    stripped = {k:v for k,v in rules.items() if k not in _CONFIGURED}
    if (registered != expected_lineage or stripped != dict(base.material_rules)
            or definition.evaluator_id != base.evaluator_id
            or definition.evaluator_version != base.evaluator_version
            or definition.request_schema_version != base.request_schema_version
            or definition.result_schema_version != base.result_schema_version):
        raise ValueError('check_evidence_payload_invalid: registered definition lineage disagrees')
    expected = dict(base.material_rules)
    expected.update(registered_definition=expected_lineage,
        **{k:request.parameters.get(k) for k in ('inputs','detector','outcomes','statistics',
            'assertions','gap_policy','gap_rewarm_bars')},
        indicator={k:request.scope[k] for k in ('indicator_id','indicator_param_overrides') if k in request.scope})
    if 'resolved_indicator_graph' in rules or 'resolved_indicator_graph_hash' in rules:
        graph = [dict(row) for row in plan.indicator_graph]
        expected.update(resolved_indicator_graph=graph,
                        resolved_indicator_graph_hash=semantic_hash({'indicators':graph}))
    if rules != expected or definition.definition_version != f'{base_version}+{semantic_hash(expected)[:16]}':
        raise ValueError('check_evidence_payload_invalid: definition is not the canonical request/graph definition')


def validate_historical_receipt(definition, evidence, result, *, item_id):
    """Anchor the entire typed plan/request/input/result chain to known saved custody.

    Unknown historical evidence fails closed. This grants neither execution nor
    exact-source reproducibility; it only admits the eight inventoried originals.
    """
    for receipt in _archive()['receipts']:
        if (receipt['id'] == item_id
                and receipt['definition_id'] == definition.definition_id
                and receipt['definition_version'] == definition.definition_version
                and receipt['definition_hash'] == definition.definition_hash
                and receipt['evidence_hash'] == evidence.evidence_hash
                and receipt['result_hash'] == result.result_hash):
            return receipt['id']
    raise ValueError('check_evidence_payload_invalid: retired evidence has no original custody receipt')


def historical_observation_eligible(item_id):
    """Read original receipt eligibility only after full envelope/ID validation."""
    return any(row['id'] == item_id and row['observation_eligible_at_creation'] is True
               for row in _archive()['receipts'])

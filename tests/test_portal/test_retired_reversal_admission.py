"""Retired research adapters are unavailable; immutable definitions remain data."""
import pytest
from dataclasses import replace
from portal.backend.service.research.registry import CHECK_REGISTRY, normalize_check_request

SCHEMAS = {
    '9': 'reversal_recognition.v1', '10': 'reversal_first_passage.v1',
    '11': 'reversal_selection_only.v1', '12': 'reversal_selection_only.v2',
    '13': 'reversal_matched_first_passage.v1', '14': 'reversal_two_block_selection.v1',
    '15': 'reversal_matched_progress.v1',
}


@pytest.mark.parametrize('version,schema', SCHEMAS.items())
def test_retired_reversal_schemas_never_admit_execution_(version,schema):
    base=replace(CHECK_REGISTRY.resolve_definition('raw_forward_outcome','17'),
        definition_version=version,evaluator_version=version,definition_hash='')
    assert base.evaluator_version == version
    with pytest.raises(ValueError,match='check_definition_unavailable'):
        CHECK_REGISTRY.resolve_definition('raw_forward_outcome',version)
    with pytest.raises(ValueError,match='check_evaluator_unavailable'):
        CHECK_REGISTRY.resolve_evaluator(base)
    payload={'check_family':'raw_forward_outcome','mode':'evidence','dataset_id':'mds_'+'a'*32,
        'scope':{'instrument_id':'instrument','timeframe':'15m',
            'start':'2025-02-01T00:00:00Z','end':'2025-03-01T00:00:00Z'},
        'detector':{'type':'completed_reversal_candidates','computation_origin':'2025-01-01T00:00:00Z',
            'reference_basis':'rising_leg_plus_retreat','ownership':'retain_all_provisional',
            'max_bars':19104,'max_candidates':10000},
        'outcomes':{'schema_version':schema},'gap_policy':'reject'}
    with pytest.raises(ValueError,match='check_definition_retired'):
        normalize_check_request(payload)

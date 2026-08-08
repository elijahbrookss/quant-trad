from __future__ import annotations

import pytest

from portal.backend.service.research import checks
from portal.backend.service.research.registry import (
    CHECK_REGISTRY,
    LEGACY_CHECK_FAMILIES,
    materialize_check_definition,
    normalize_check_request,
)


def _payload() -> dict:
    return {
        "title": "Indicator evidence",
        "check_family": checks.INDICATOR_FORWARD_OUTCOME,
        "dataset_id": "mds_" + "a" * 32,
        "scope": {
            "indicator_id": "indicator-1",
            "instrument_id": "instrument-1",
            "timeframe": "1h",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "detector": {"type": "record_match", "output_name": "entry"},
        "outcomes": {"forward_bars": [1, 3]},
        "gap_policy": "reject",
    }


def test_all_existing_check_families_are_exact_version_registered() -> None:
    for family in LEGACY_CHECK_FAMILIES:
        definition, evaluator = CHECK_REGISTRY.resolve(family, "2")
        assert definition.definition_id == family
        assert evaluator.evaluator_id == family
        assert evaluator.version == "2"


def test_material_definition_hash_covers_configuration_and_assertions() -> None:
    base = materialize_check_definition(_payload(), mode="evidence")
    changed_outcome = materialize_check_definition(
        {**_payload(), "outcomes": {"forward_bars": [1, 6]}},
        mode="evidence",
    )
    changed_assertion = materialize_check_definition(
        {
            **_payload(),
            "assertions": [
                {"metric_path": "sample_count", "operator": "gte", "threshold": 30}
            ],
        },
        mode="evidence",
    )

    assert changed_outcome.definition_hash != base.definition_hash
    assert changed_assertion.definition_hash != base.definition_hash


def test_evidence_rejects_unconstrained_provider_source() -> None:
    with pytest.raises(ValueError, match="unconstrained_source_forbidden"):
        normalize_check_request(
            {
                **_payload(),
                "inputs": [
                    {
                        "alias": "reference_price",
                        "fact_type": "market.reference_price",
                        "contract_version": "market.reference_price.v1",
                        "source_policy": {"mode": "current"},
                    }
                ],
            },
            mode="evidence",
        )


def test_same_semantic_fact_can_use_distinct_exact_aliases() -> None:
    _definition, request = normalize_check_request(
        {
            **_payload(),
            "inputs": [
                {
                    "alias": "reference_a",
                    "fact_type": "market.reference_price",
                    "contract_version": "market.reference_price.v1",
                    "source_policy": {
                        "mode": "exact",
                        "source_identity_key": "source-a",
                    },
                },
                {
                    "alias": "reference_b",
                    "fact_type": "market.reference_price",
                    "contract_version": "market.reference_price.v1",
                    "source_policy": {
                        "mode": "exact",
                        "source_identity_key": "source-b",
                    },
                },
            ],
        },
        mode="evidence",
    )

    assert [row["alias"] for row in request.parameters["inputs"]] == [
        "reference_a",
        "reference_b",
    ]
    assert all(
        row["fact_type"] == "market.reference_price"
        for row in request.parameters["inputs"]
    )

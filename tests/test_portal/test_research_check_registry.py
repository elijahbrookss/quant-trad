from __future__ import annotations

import pytest

from portal.backend.service.research import checks as _checks  # noqa: F401
from portal.backend.service.research.event_fact_evaluator import (
    EVENT_FACT_ANALYSIS,
    EVENT_FACT_EVALUATOR_VERSION,
    EVENT_FACT_RESULT_VERSION,
)
from portal.backend.service.research.registry import (
    CHECK_REGISTRY,
    LEGACY_CHECK_FAMILIES,
    materialize_check_definition,
    normalize_check_request,
)


def _payload() -> dict:
    return {
        "title": "Indicator evidence",
        "check_family": EVENT_FACT_ANALYSIS,
        "dataset_id": "mds_" + "a" * 32,
        "scope": {
            "indicator_id": "indicator-1",
            "instrument_id": "instrument-1",
            "timeframe": "1h",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "detector": {
            "type": "indicator_event",
            "output_name": "atr_expansion",
            "event_keys": [
                {"key": "atr_expansion_long", "direction": "long"}
            ],
        },
        "outcomes": {"horizons": [1, 3]},
        "inputs": [
            {
                "alias": "reference_price",
                "fact_type": "market.reference_price",
                "contract_version": "market.reference_price.v1",
                "dimensions": {"quote_currency": "USD"},
                "source_policy": {
                    "mode": "exact",
                    "source_identity_key": "source-a",
                },
            }
        ],
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
        {**_payload(), "outcomes": {"horizons": [1, 6]}},
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
    changed_gap_rewarm = materialize_check_definition(
        {**_payload(), "gap_policy": "reset_rewarm", "gap_rewarm_bars": 48},
        mode="evidence",
    )

    assert changed_outcome.definition_hash != base.definition_hash
    assert changed_assertion.definition_hash != base.definition_hash
    assert changed_gap_rewarm.definition_hash != base.definition_hash


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


def test_event_fact_definition_uses_current_operation_versions() -> None:
    definition, evaluator = CHECK_REGISTRY.resolve(EVENT_FACT_ANALYSIS, "3")

    assert evaluator.version == EVENT_FACT_EVALUATOR_VERSION == "2"
    assert definition.evaluator_version == "2"
    assert EVENT_FACT_RESULT_VERSION == "event_fact_analysis_result.v2"


def test_event_fact_rejects_misnested_or_unknown_statistics_fields() -> None:
    payload = {
        **_payload(),
        "detector": {
            "type": "indicator_event",
            "output_name": "entry",
            "event_keys": [{"key": "long", "direction": "long"}],
        },
        "outcomes": {"horizons": [1]},
        "inputs": [
            {
                "alias": "reference_price",
                "fact_type": "market.reference_price",
                "contract_version": "market.reference_price.v1",
                "source_policy": {
                    "mode": "exact",
                    "source_identity_key": "source-a",
                },
            }
        ],
        "statistics": {"baseline_features": []},
    }

    with pytest.raises(ValueError, match="unsupported statistics fields"):
        normalize_check_request(payload, mode="evidence")

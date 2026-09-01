from __future__ import annotations

import pytest

from portal.backend.service.research import checks as _checks  # noqa: F401
from portal.backend.service.research.event_fact_evaluator import (
    EVENT_FACT_ANALYSIS,
    EVENT_FACT_EVALUATOR_VERSION,
    EVENT_FACT_RESULT_VERSION,
    LEGACY_EVENT_FACT_EVALUATOR_VERSION,
    LEGACY_EVENT_FACT_RESULT_VERSION,
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


def _l2_payload() -> dict:
    return {
        "check_family": EVENT_FACT_ANALYSIS,
        "dataset_id": "mds_" + "b" * 32,
        "scope": {
            "instrument_id": "instrument-1",
            "timeframe": "30m",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
        },
        "detector": {
            "type": "fact_snapshot",
            "input_alias": "depth",
            "where": {"payload.band_bps": 5},
        },
        "outcomes": {"horizons": [1]},
        "statistics": {
            "features": {
                "baseline": [
                    {
                        "name": "lagged_return",
                        "operator": "lagged_return",
                        "lookback_bars": 1,
                    }
                ],
                "enriched": [
                    {
                        "name": "depth_imbalance_5bps",
                        "operator": "latest_payload_number",
                        "input_alias": "depth",
                        "path": "payload.imbalance",
                        "where": {"payload.band_bps": 5},
                    }
                ],
            }
        },
        "inputs": [
            {
                "alias": "depth",
                "fact_type": "market.depth_observation",
                "contract_version": "market.depth_band.v1",
                "timeframe_seconds": 1,
                "max_staleness_seconds": 120,
                "source_policy": {
                    "mode": "exact",
                    "source_identity_key": "source-a",
                },
            }
        ],
        "gap_policy": "continue_degraded",
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
    legacy_definition, legacy_evaluator = CHECK_REGISTRY.resolve(
        EVENT_FACT_ANALYSIS, "3"
    )
    definition, evaluator = CHECK_REGISTRY.resolve(EVENT_FACT_ANALYSIS, "4")

    assert legacy_evaluator.version == LEGACY_EVENT_FACT_EVALUATOR_VERSION == "2"
    assert legacy_evaluator.result_schema_version == LEGACY_EVENT_FACT_RESULT_VERSION
    assert legacy_definition.evaluator_version == "2"
    assert legacy_definition.definition_hash == (
        "dc408552a837090a783c905f0e39670bed21dcdf59c5652d157f0b6c5b72602b"
    )
    assert evaluator.version == EVENT_FACT_EVALUATOR_VERSION == "3"
    assert evaluator.result_schema_version == EVENT_FACT_RESULT_VERSION
    assert definition.evaluator_version == "3"
    assert EVENT_FACT_RESULT_VERSION == "event_fact_analysis_result.v3"


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


def test_l2_fact_snapshot_is_schema_bound_and_normalizes_query_values() -> None:
    definition, request = normalize_check_request(_l2_payload(), mode="evidence")

    assert definition.definition_version.startswith("4+")
    assert definition.evaluator_version == "3"
    assert request.parameters["detector"]["where"] == {"payload.band_bps": 5}
    assert request.parameters["statistics"]["features"]["enriched"][0] == {
        "name": "depth_imbalance_5bps",
        "operator": "latest_payload_number",
        "input_alias": "depth",
        "path": "payload.imbalance",
        "where": {"payload.band_bps": 5},
    }


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload: payload["detector"].update(input_alias="missing"),
            "undeclared input_alias=missing",
        ),
        (
            lambda payload: payload["statistics"]["features"]["enriched"][0].update(
                path="payload.bid_quantity"
            ),
            "not a declared query field",
        ),
        (
            lambda payload: payload["statistics"]["features"]["enriched"][0].update(
                path="payload.bucket_end"
            ),
            "must be numeric",
        ),
        (
            lambda payload: payload["inputs"][0].pop("max_staleness_seconds"),
            "requires explicit max_staleness_seconds",
        ),
        (
            lambda payload: payload.update(gap_policy="reset_rewarm"),
            "does not support reset_rewarm",
        ),
        (
            lambda payload: payload["inputs"][0].update(
                fact_type="market.l2_book",
                contract_version="market.l2_book.v1",
                timeframe_seconds=None,
            ),
            "not Dataset eligible",
        ),
    ],
)
def test_l2_fact_snapshot_rejects_unsafe_admission(mutate, message: str) -> None:
    payload = _l2_payload()
    mutate(payload)

    with pytest.raises(ValueError, match=message):
        normalize_check_request(payload, mode="evidence")

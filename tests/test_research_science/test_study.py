from __future__ import annotations

from dataclasses import replace

import pytest

from research_science import (
    PROTOCOL_SCHEMA_VERSION,
    RESEARCH_BRIEF_SCHEMA_VERSION,
    RESEARCH_RUN_SCHEMA_VERSION,
    STUDY_DEFINITION_SCHEMA_VERSION,
    AvailabilityTransform,
    FactRequirement,
    ResearchBrief,
    ResearchBundleRegistry,
    ResearchRun,
    ScientificProtocol,
    StudyDefinition,
    TemporalJoinSpec,
    preflight_study,
)


class _Bundle:
    def __init__(self, bundle_id: str, version: str) -> None:
        self.bundle_id = bundle_id
        self.version = version


class _AvailabilityResolver:
    transform_id = "market.candle.close_availability"
    version = "v1"


def _study() -> StudyDefinition:
    return StudyDefinition(
        schema_version=STUDY_DEFINITION_SCHEMA_VERSION,
        study_id="price-volume-dislocation",
        brief=ResearchBrief(
            schema_version=RESEARCH_BRIEF_SCHEMA_VERSION,
            brief_id="brief-price-volume",
            objective="Test whether volume dislocation changes short-horizon returns.",
            economic_claim="Volume adds information beyond price alone.",
            economic_claim_intent="selection",
            requested_by="human:owner",
        ),
        instrument_ids=("instrument-1",),
        fact_requirements=(
            FactRequirement(
                fact_key="price",
                fact_type="market.candle",
                role="primary",
                timeframe_seconds=60,
            ),
            FactRequirement(
                fact_key="volume_profile",
                fact_type="market.volume_profile",
                role="context",
                timeframe_seconds=60,
            ),
        ),
        availability_transforms=(
            AvailabilityTransform(
                transform_id="market.candle.close_availability",
                transform_version="v1",
                output_fact_key="price",
                input_fact_keys=("price",),
                parameters={"processing_latency_ms": 25},
            ),
        ),
        temporal_joins=(
            TemporalJoinSpec(
                left_fact_key="price",
                right_fact_key="volume_profile",
                output_key="volume_context",
                missing_policy="exclude_frame",
            ),
        ),
        feature_bundle_id="features.price_volume",
        feature_bundle_version="v1",
        search_space_bundle_id="search.price_volume",
        search_space_bundle_version="v1",
        evaluator_bundle_id="evaluator.forward_return",
        evaluator_bundle_version="v1",
        benchmark_ids=("no_trade", "price_only"),
    )


def _protocol() -> ScientificProtocol:
    return ScientificProtocol.from_dict(
        {
            "schema_version": PROTOCOL_SCHEMA_VERSION,
            "protocol_id": "protocol-price-volume",
            "family_name": "price-volume-dislocation",
            "economic_claim_intent": "selection",
            "datasets": [
                {
                    "dataset_id": f"dataset-{role}",
                    "dataset_hash": f"hash-{role}",
                    "role": role,
                    "window_start": f"202{index}-01-01T00:00:00Z",
                    "window_end": f"202{index}-02-01T00:00:00Z",
                    **(
                        {"blind_alias": "sealed-final"}
                        if role == "holdout"
                        else {}
                    ),
                }
                for index, role in enumerate(
                    ("train", "validation", "holdout"),
                    start=0,
                )
            ],
            "blindness": "PLATFORM_CONTROLLED_HISTORICAL",
            "budget": {
                "max_attempts": 12,
                "max_runtime_seconds": 600,
                "max_compute_units": 12,
                "max_validation_feedback_uses": 1,
            },
            "leakage": {
                "max_feature_lookback_bars": 5,
                "label_horizon_bars": 3,
                "max_holding_period_bars": 3,
                "order_expiry_bars": 1,
                "purge_bars": 5,
                "embargo_bars": 5,
            },
            "walk_forward": {
                "train_bars": 40,
                "validation_bars": 10,
                "step_bars": 10,
                "fold_count": 1,
                "folds": [
                    {
                        "fold_index": 0,
                        "train_start_bar": 0,
                        "train_end_bar": 40,
                        "validation_start_bar": 45,
                        "validation_end_bar": 55,
                        "embargo_end_bar": 60,
                    }
                ],
            },
            "instrument_ids": ["instrument-1"],
            "allowed_mutation_dimensions": ["expressions", "parameters"],
            "benchmark_ids": ["no_trade", "price_only"],
            "primary_metric": "mean_return_bps",
            "primary_metric_direction": "maximize",
            "minimum_effect_size": 0.0,
            "secondary_metrics": ["trade_count"],
            "safety_metrics": ["max_drawdown"],
            "alpha": 0.05,
            "minimum_sample_count": 10,
            "minimum_trade_count": 2,
            "minimum_calendar_days": 1,
            "minimum_exposure": 0.01,
            "minimum_execution_quality_class": "X2",
            "execution_stress_ids": ["base", "double_cost"],
            "multiple_testing_method": "holm",
            "robustness_requirements": ["walk_forward"],
            "statistical_method_versions": {"multiplicity": "holm.v1"},
            "policy_versions": {"study_definition": _study().definition_hash},
            "created_by": "research:author",
            "authorized_by": "research:authority",
            "authorization_request_id": "authorize-price-volume",
        }
    )


def _registry() -> ResearchBundleRegistry:
    registry = ResearchBundleRegistry()
    for identity in (
        ("features.price_volume", "v1"),
        ("search.price_volume", "v1"),
        ("evaluator.forward_return", "v1"),
    ):
        registry.register(_Bundle(*identity))
    registry.register_availability(_AvailabilityResolver())
    return registry


def test_study_definition_is_generic_and_hash_stable() -> None:
    study = _study()
    restored = StudyDefinition.from_dict(study.to_dict())

    assert restored == study
    assert {row.fact_type for row in study.fact_requirements} == {
        "market.candle",
        "market.volume_profile",
    }
    assert study.definition_hash == restored.definition_hash


def test_study_preflight_resolves_exact_bundles_and_role_fact_sets() -> None:
    facts = {
        role: ("market.candle", "market.volume_profile")
        for role in ("train", "validation", "holdout")
    }
    result = preflight_study(
        study=_study(),
        protocol=_protocol(),
        dataset_fact_types=facts,
        registry=_registry(),
    )

    assert result["provider_fetch_allowed"] is False
    assert result["external_trading_allowed"] is False
    assert result["dataset_fact_set_hashes"].keys() == facts.keys()


def test_study_preflight_fails_when_any_role_omits_a_required_fact() -> None:
    with pytest.raises(ValueError, match="role=validation"):
        preflight_study(
            study=_study(),
            protocol=_protocol(),
            dataset_fact_types={
                "train": ("market.candle", "market.volume_profile"),
                "validation": ("market.candle",),
                "holdout": ("market.candle", "market.volume_profile"),
            },
            registry=_registry(),
        )


def test_study_hash_rejects_mutation() -> None:
    study = _study()
    with pytest.raises(ValueError, match="study_definition_hash_mismatch"):
        replace(study, benchmark_ids=("no_trade",))


def test_research_run_pins_generic_inputs_and_bundles() -> None:
    study = _study()
    protocol = _protocol()
    run = ResearchRun(
        schema_version=RESEARCH_RUN_SCHEMA_VERSION,
        run_id="run-price-volume-001",
        study_definition_hash=study.definition_hash,
        protocol_hash=protocol.protocol_hash,
        code_revision="abcdef123456",
        dataset_binding_hashes=(
            ("train", "a" * 64),
            ("validation", "b" * 64),
            ("holdout", "c" * 64),
        ),
        bundle_versions=tuple(study.bundle_versions.items()),
        availability_binding_hashes=("d" * 64,),
        created_by="research:runner",
    )

    assert run.run_hash
    with pytest.raises(ValueError, match="research_run_hash_mismatch"):
        replace(run, code_revision="different-revision")

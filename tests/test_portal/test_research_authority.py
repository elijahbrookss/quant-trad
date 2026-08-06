from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from market_data.store import FrozenDataset
from portal.backend.db.session import db
from portal.backend.service.research import (
    authority,
    campaign_runner,
    governance,
    governance_repository,
)
from portal.backend.service.research import authority_repository as repository
from portal.backend.service.research import repository as research_repository
from research_science import (
    CAMPAIGN_CHARTER_SCHEMA_VERSION,
    CANDIDATE_SCHEMA_VERSION,
    PROTOCOL_SCHEMA_VERSION,
    CampaignCharter,
    CampaignExecutionCosts,
    CandidateSnapshot,
    FrozenCampaignBar,
    ResearchReplayAvailabilityArtifact,
    ResearchReplayAvailabilityPolicy,
    ScientificProtocol,
    resolve_campaign_charter,
)
from strategies.typed_graph import TYPED_STRATEGY_GRAPH_VERSION

pytestmark = pytest.mark.integration


def _protocol_payload(protocol_id: str) -> dict:
    return {
        "schema_version": PROTOCOL_SCHEMA_VERSION,
        "protocol_id": protocol_id,
        "family_name": "fixture-family",
        "economic_claim_intent": "selection",
        "datasets": [
            {
                "dataset_id": f"mds_{'a' * 32}",
                "dataset_hash": "a" * 64,
                "role": "train",
                "window_start": "2020-01-01T00:00:00Z",
                "window_end": "2021-01-01T00:00:00Z",
            },
            {
                "dataset_id": f"mds_{'b' * 32}",
                "dataset_hash": "b" * 64,
                "role": "validation",
                "window_start": "2021-01-01T00:00:00Z",
                "window_end": "2022-01-01T00:00:00Z",
            },
            {
                "dataset_id": f"mds_{'c' * 32}",
                "dataset_hash": "c" * 64,
                "role": "holdout",
                "window_start": "2022-01-01T00:00:00Z",
                "window_end": "2023-01-01T00:00:00Z",
                "blind_alias": "final-a",
            },
        ],
        "blindness": "PLATFORM_CONTROLLED_HISTORICAL",
        "budget": {
            "max_attempts": 5,
            "max_runtime_seconds": 500,
            "max_compute_units": 50,
            "max_validation_feedback_uses": 2,
        },
        "leakage": {
            "max_feature_lookback_bars": 20,
            "label_horizon_bars": 5,
            "max_holding_period_bars": 12,
            "order_expiry_bars": 3,
        },
        "walk_forward": {
            "train_bars": 100,
            "validation_bars": 25,
            "step_bars": 25,
            "fold_count": 3,
        },
        "instrument_ids": ["BTC-USD"],
        "allowed_mutation_dimensions": [
            "initial_graph",
            "expressions",
            "parameter_values",
        ],
        "benchmark_ids": ["buy-and-hold"],
        "primary_metric": "net_sharpe",
        "primary_metric_direction": "maximize",
        "minimum_effect_size": 0.1,
        "secondary_metrics": ["net_return"],
        "safety_metrics": ["max_drawdown"],
        "alpha": 0.05,
        "minimum_sample_count": 30,
        "minimum_trade_count": 20,
        "minimum_calendar_days": 180,
        "minimum_exposure": 1000,
        "minimum_execution_quality_class": "X2",
        "execution_stress_ids": ["cost-plus-25bps", "latency-plus-one-bar"],
        "multiple_testing_method": "bonferroni",
        "robustness_requirements": [
            "cost_stress",
            "latency_stress",
            "parameter_neighborhood",
            "subperiod",
        ],
        "statistical_method_versions": {
            "bootstrap": "moving-block-bootstrap.v1",
            "multiple_testing": "bonferroni.v1",
            "walk_forward": "chronological-walk-forward.v1",
        },
        "policy_versions": {
            "dataset": "frozen-dataset-policy.v1",
            "execution": "execution-quality-policy.v1",
            "science": "scientific-quality-policy.v1",
            "strategy": "typed-strategy-policy.v1",
        },
        "created_by": "human:owner",
        "authorized_by": "human:owner",
        "authorization_request_id": "fixture-authorization",
    }


def _graph_payload(graph_id: str, *, parents: list[str] | None = None) -> dict:
    return {
        "schema_version": TYPED_STRATEGY_GRAPH_VERSION,
        "graph_id": graph_id,
        "timeframe": "5m",
        "facts": [
            {"name": "indicator.rsi.value", "value_type": "number"},
            {"name": "position.is_flat", "value_type": "boolean"},
        ],
        "rules": [
            {
                "rule_id": "enter",
                "priority": 10,
                "condition": {
                    "op": "all",
                    "args": [
                        {"op": "fact", "name": "position.is_flat"},
                        {
                            "op": "lt",
                            "args": [
                                {"op": "fact", "name": "indicator.rsi.value"},
                                {"op": "const", "value_type": "number", "value": 30},
                            ],
                        },
                    ],
                },
                "action": "enter",
                "side": "long",
                "sizing": {"mode": "risk_budget_fraction", "value": 0.01},
                "execution": {
                    "style": "aggressive_limit",
                    "time_in_force": "ioc",
                    "expiration_bars": 0,
                    "price_offset_bps": 2,
                    "chase_limit": 0,
                    "stage_count": 1,
                },
            }
        ],
        "risk": {
            "max_position_notional": 10000,
            "max_risk_fraction": 0.02,
            "allow_short": False,
        },
        "parent_graph_ids": list(parents or []),
    }


@pytest.fixture
def authority_transaction(monkeypatch: pytest.MonkeyPatch):
    if not db.ensure_schema() or db._engine is None:
        pytest.skip("PostgreSQL is unavailable")
    connection = db._engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, expire_on_commit=False, autoflush=False, future=True)

    @contextmanager
    def session_scope():
        nested = session.begin_nested()
        try:
            yield session
            session.flush()
            nested.commit()
        except Exception:
            nested.rollback()
            raise

    monkeypatch.setattr(repository, "db", SimpleNamespace(session=session_scope))
    monkeypatch.setattr(
        governance_repository, "db", SimpleNamespace(session=session_scope)
    )
    monkeypatch.setattr(
        research_repository, "db", SimpleNamespace(session=session_scope)
    )
    hashes = {
        f"mds_{value * 32}": value * 64 for value in ("a", "b", "c")
    }

    def frozen_dataset(dataset_id: str) -> FrozenDataset:
        return FrozenDataset(
            dataset_id=dataset_id,
            dataset_hash=hashes[dataset_id],
            max_commit_seq=100,
            series=(
                {
                    "identity_key": f"series:{dataset_id}",
                    "instrument_id": "BTC-USD",
                    "range_start": "2019-01-01T00:00:00Z",
                    "range_end": "2024-01-01T00:00:00Z",
                },
            ),
        )

    monkeypatch.setattr(
        authority,
        "market_data_repo",
        SimpleNamespace(get_dataset=frozen_dataset),
    )
    try:
        yield session
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()


def test_protocol_and_trial_fence_reject_agent_selected_data(authority_transaction) -> None:
    suffix = uuid4().hex
    protocol = authority.create_protocol(
        {
            "actor_id": "human:owner",
            "actor_role": "research_authority",
            "request_id": f"request-protocol-{suffix}",
            "protocol": _protocol_payload(f"protocol-{suffix}"),
        }
    )
    holdout = next(row for row in protocol["manifest"]["datasets"] if row["role"] == "holdout")
    assert holdout["dataset_id"] is None
    assert holdout["window_start"] is None
    private_protocol = repository.protocol_private(protocol["id"])
    assert private_protocol.authorized_by == "human:owner"
    assert private_protocol.authorization_request_id == f"request-protocol-{suffix}"
    family = authority.create_family(
        {
            "actor_id": "agent:researcher",
            "actor_role": "research_agent",
            "request_id": f"request-family-{suffix}",
            "protocol_id": protocol["id"],
            "family_id": f"family-{suffix}",
            "name": "fixture-family",
        }
    )
    with pytest.raises(ValueError, match="may not choose datasets"):
        authority.register_attempt(
            {
                "actor_id": "agent:researcher",
                "actor_role": "research_agent",
                "request_id": f"request-bad-{suffix}",
                "family_id": family["id"],
                "dataset_role": "train",
                "trial_inputs": {"strategy": "fixture", "dataset_id": "agent-choice"},
                "estimated_runtime_seconds": 1,
                "estimated_compute_units": 1,
            }
        )
    with pytest.raises(ValueError, match="one-use holdout authority"):
        authority.register_attempt(
            {
                "actor_id": "agent:researcher",
                "actor_role": "research_agent",
                "request_id": f"request-holdout-{suffix}",
                "family_id": family["id"],
                "dataset_role": "holdout",
                "trial_inputs": {"strategy": "fixture"},
                "estimated_runtime_seconds": 1,
                "estimated_compute_units": 1,
            }
        )


def test_protocol_admission_requires_frozen_instrument_universe_coverage(
    authority_transaction,
) -> None:
    suffix = uuid4().hex
    payload = _protocol_payload(f"protocol-{suffix}")
    payload["instrument_ids"] = ["ETH-USD"]
    with pytest.raises(
        ValueError, match="scientific_protocol_instrument_universe_not_covered"
    ):
        authority.create_protocol(
            {
                "actor_id": "human:owner",
                "actor_role": "research_authority",
                "request_id": f"request-protocol-{suffix}",
                "protocol": payload,
            }
        )


def _reserved_fixture_holdout(suffix: str) -> tuple[dict, dict, str]:
    protocol = ScientificProtocol.from_dict(_protocol_payload(f"protocol-{suffix}"))
    repository.create_protocol(
        protocol,
        actor_id="human:owner",
        actor_role="research_authority",
        request_id=f"request-protocol-{suffix}",
    )
    family = repository.create_family(
        protocol_id=protocol.protocol_id,
        family_id=f"family-{suffix}",
        name="fixture-family",
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"request-family-{suffix}",
    )
    attempt = repository.register_attempt(
        family_id=family["id"],
        request_id=f"request-validation-{suffix}",
        dataset_role="validation",
        trial_inputs={"strategy_graph_hash": "1" * 64},
        estimated_runtime_seconds=1,
        estimated_compute_units=1,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    evidence = {
        "artifact_hash": "2" * 64,
        "strategy_artifact_hash": "3" * 64,
        "parameter_artifact_hash": "4" * 64,
        "execution_model_hash": "5" * 64,
        "metric_contract_hash": "6" * 64,
        "reproducible": True,
        "sample_count": 80,
        "trade_count": 60,
        "calendar_days": 365,
        "exposure": 4000,
        "execution_quality_class": "X5",
        "execution_stress_ids_passed": [
            "cost-plus-25bps",
            "latency-plus-one-bar",
        ],
        "metric_results": {
            "net_sharpe": 1.1,
            "net_return": 0.12,
            "max_drawdown": -0.08,
        },
        "benchmark_metric_results": {
            "buy-and-hold": {"net_sharpe": 0.5},
        },
        "walk_forward_fold_count": 3,
        "purge_bars": 20,
        "embargo_bars": 20,
        "context_only_warmup": True,
        "flat_at_scoring_start": True,
        "no_pending_orders_at_scoring_start": True,
        "signals_not_before_scoring_start": True,
    }
    repository.complete_attempt(
        attempt_id=attempt["id"],
        status="completed",
        result_evidence=evidence,
        error=None,
        actual_runtime_seconds=1,
        actual_compute_units=1,
        actor_id="runner:offline",
        actor_role="experiment_runner",
        request_id=f"request-validation-complete-{suffix}",
    )
    candidate = repository.freeze_candidate(
        CandidateSnapshot(
            schema_version=CANDIDATE_SCHEMA_VERSION,
            candidate_id=f"candidate-{suffix}",
            family_id=family["id"],
            protocol_hash=protocol.protocol_hash,
            source_attempt_id=attempt["id"],
            strategy_artifact_hash="3" * 64,
            parameter_artifact_hash="4" * 64,
            execution_model_hash="5" * 64,
            metric_contract_hash="6" * 64,
            research_dataset_hashes=("a" * 64, "b" * 64),
            evidence_hashes=("2" * 64,),
            frozen_by="agent:researcher",
        ),
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"request-candidate-{suffix}",
    )
    repository.close_family(
        family_id=family["id"],
        actor_id="human:holdout-authority",
        actor_role="research_authority",
        request_id=f"request-close-{suffix}",
    )
    holdout, token = repository.reserve_holdout(
        family_id=family["id"],
        candidate_id=candidate["id"],
        actor_id="human:holdout-authority",
        actor_role="research_authority",
        request_id=f"request-holdout-{suffix}",
    )
    return family, holdout, token


def test_rejected_holdout_is_consumed_and_remains_sealed(
    authority_transaction,
) -> None:
    suffix = uuid4().hex
    family, holdout, token = _reserved_fixture_holdout(suffix)
    rejected = authority.reject_holdout_internal(
        holdout_use_id=holdout["id"],
        reservation_token=token,
        result_evidence={
            "artifact_hash": "7" * 64,
            "sample_count": 5,
            "trade_count": 1,
        },
        reason_codes=("sample_count_below_minimum", "trade_count_below_minimum"),
        executor_actor="runner:sealed-holdout",
        request_id=f"request-holdout-reject-{suffix}",
    )
    assert rejected["status"] == "rejected"
    public = repository.family_evidence(family["id"])
    assert public["family"]["status"] == "holdout_rejected"
    assert public["holdout"]["status"] == "rejected"
    assert public["holdout"]["result_evidence"] is None
    private = repository.family_evidence(family["id"], private=True)
    assert private["holdout"]["result_evidence"][
        "holdout_rejection_reason_codes"
    ] == ["sample_count_below_minimum", "trade_count_below_minimum"]
    archived = authority.archive_rejected_family(
        {
            "actor_id": "human:holdout-authority",
            "actor_role": "research_authority",
            "request_id": f"request-family-archive-{suffix}",
            "family_id": family["id"],
            "reason": "sealed_holdout_gate_rejection",
        }
    )
    assert archived["status"] == "archived"
    assert repository.family_evidence(family["id"])["events"][-1][
        "event_type"
    ] == "REJECTED_FAMILY_ARCHIVED"
    with pytest.raises(ValueError, match="holdout_use_not_reserved"):
        repository.execute_holdout_internal(
            holdout_use_id=holdout["id"],
            reservation_token=token,
            result_evidence={},
            executor_actor="runner:sealed-holdout",
            request_id=f"request-holdout-retry-{suffix}",
        )
    with pytest.raises(ValueError, match="scientific certification requires"):
        authority.certify_family(
            {
                "actor_id": "human:independent-certifier",
                "actor_role": "research_authority",
                "request_id": f"request-certificate-{suffix}",
                "family_id": family["id"],
                "robustness": {},
            }
        )


def test_complete_family_flow_retains_failures_and_enforces_one_holdout_use(
    authority_transaction,
) -> None:
    suffix = uuid4().hex
    protocol_contract = ScientificProtocol.from_dict(
        _protocol_payload(f"protocol-{suffix}")
    )
    repository.create_protocol(
        protocol_contract,
        actor_id="human:owner",
        actor_role="research_authority",
        request_id=f"request-protocol-{suffix}",
    )
    family = repository.create_family(
        protocol_id=protocol_contract.protocol_id,
        family_id=f"family-{suffix}",
        name="fixture-family",
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"request-family-{suffix}",
    )
    failed = repository.register_attempt(
        family_id=family["id"],
        request_id=f"request-attempt-failed-{suffix}",
        dataset_role="train",
        trial_inputs={"strategy_graph_hash": "1" * 64},
        estimated_runtime_seconds=10,
        estimated_compute_units=1,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    idempotent_retry = repository.register_attempt(
        family_id=family["id"],
        request_id=f"request-attempt-failed-{suffix}",
        dataset_role="train",
        trial_inputs={"strategy_graph_hash": "1" * 64},
        estimated_runtime_seconds=10,
        estimated_compute_units=1,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    assert idempotent_retry["id"] == failed["id"]
    repository.complete_attempt(
        attempt_id=failed["id"],
        status="failed",
        result_evidence={"failure_evidence_hash": "2" * 64},
        error="bounded failure",
        actual_runtime_seconds=5,
        actual_compute_units=0.5,
        actor_id="runner:offline",
        actor_role="experiment_runner",
        request_id=f"request-complete-failed-{suffix}",
    )
    duplicate = repository.register_attempt(
        family_id=family["id"],
        request_id=f"request-attempt-duplicate-{suffix}",
        dataset_role="train",
        trial_inputs={"strategy_graph_hash": "1" * 64},
        estimated_runtime_seconds=999,
        estimated_compute_units=999,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    assert duplicate["status"] == "duplicate"
    assert duplicate["estimated_runtime_seconds"] == 0.0
    assert duplicate["estimated_compute_units"] == 0.0
    assert duplicate["result_evidence"]["duplicate_of_attempt_id"] == failed["id"]
    validation = repository.register_attempt(
        family_id=family["id"],
        request_id=f"request-attempt-validation-{suffix}",
        dataset_role="validation",
        trial_inputs={"strategy_graph_hash": "3" * 64},
        estimated_runtime_seconds=10,
        estimated_compute_units=1,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    validation_evidence = {
        "artifact_hash": "4" * 64,
        "strategy_artifact_hash": "5" * 64,
        "parameter_artifact_hash": "6" * 64,
        "execution_model_hash": "8" * 64,
        "metric_contract_hash": "9" * 64,
        "reproducible": True,
        "sample_count": 80,
        "trade_count": 60,
        "calendar_days": 365,
        "exposure": 4000,
        "execution_quality_class": "X5",
        "execution_stress_ids_passed": [
            "cost-plus-25bps",
            "latency-plus-one-bar",
        ],
        "metric_results": {
            "net_sharpe": 1.1,
            "net_return": 0.12,
            "max_drawdown": -0.08,
        },
        "benchmark_metric_results": {
            "buy-and-hold": {"net_sharpe": 0.5},
        },
        "p_value": 0.001,
        "walk_forward_fold_count": 3,
        "purge_bars": 20,
        "embargo_bars": 20,
        "context_only_warmup": True,
        "flat_at_scoring_start": True,
        "no_pending_orders_at_scoring_start": True,
        "signals_not_before_scoring_start": True,
    }
    insufficient_execution = {
        **validation_evidence,
        "execution_quality_class": "X1",
    }
    with pytest.raises(
        ValueError, match="validation_execution_quality_below_protocol_minimum"
    ):
        repository.complete_attempt(
            attempt_id=validation["id"],
            status="completed",
            result_evidence=insufficient_execution,
            error=None,
            actual_runtime_seconds=8,
            actual_compute_units=0.8,
            actor_id="runner:offline",
            actor_role="experiment_runner",
            request_id=f"request-complete-validation-insufficient-{suffix}",
        )
    insufficient_effect = {
        **validation_evidence,
        "benchmark_metric_results": {
            "buy-and-hold": {"net_sharpe": 1.05},
        },
    }
    with pytest.raises(
        ValueError, match="validation_effect_size_below_protocol_minimum"
    ):
        repository.complete_attempt(
            attempt_id=validation["id"],
            status="completed",
            result_evidence=insufficient_effect,
            error=None,
            actual_runtime_seconds=8,
            actual_compute_units=0.8,
            actor_id="runner:offline",
            actor_role="experiment_runner",
            request_id=f"request-complete-validation-effect-{suffix}",
        )
    repository.complete_attempt(
        attempt_id=validation["id"],
        status="completed",
        result_evidence=validation_evidence,
        error=None,
        actual_runtime_seconds=8,
        actual_compute_units=0.8,
        actor_id="runner:offline",
        actor_role="experiment_runner",
        request_id=f"request-complete-validation-{suffix}",
    )
    candidate_contract = CandidateSnapshot(
        schema_version=CANDIDATE_SCHEMA_VERSION,
        candidate_id=f"candidate-{suffix}",
        family_id=family["id"],
        protocol_hash=protocol_contract.protocol_hash,
        source_attempt_id=validation["id"],
        strategy_artifact_hash="5" * 64,
        parameter_artifact_hash="6" * 64,
        execution_model_hash="8" * 64,
        metric_contract_hash="9" * 64,
        research_dataset_hashes=("a" * 64, "b" * 64),
        evidence_hashes=(validation_evidence["artifact_hash"],),
        frozen_by="agent:researcher",
    )
    candidate = repository.freeze_candidate(
        candidate_contract,
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"request-candidate-{suffix}",
    )
    repository.close_family(
        family_id=family["id"],
        actor_id="human:holdout-authority",
        actor_role="research_authority",
        request_id=f"request-close-{suffix}",
    )
    holdout, token = repository.reserve_holdout(
        family_id=family["id"],
        candidate_id=candidate["id"],
        actor_id="human:holdout-authority",
        actor_role="research_authority",
        request_id=f"request-holdout-{suffix}",
    )
    assert token
    assert repository.internal_holdout_binding(
        holdout["id"], reservation_token=token
    )["dataset_id"] == f"mds_{'c' * 32}"
    with pytest.raises(ValueError, match="already_used"):
        repository.reserve_holdout(
            family_id=family["id"],
            candidate_id=candidate["id"],
            actor_id="human:other",
            actor_role="research_authority",
            request_id=f"request-holdout-second-{suffix}",
        )
    holdout_result = {
        "artifact_hash": "7" * 64,
        "reproducible": True,
        "sample_count": 75,
        "trade_count": 55,
        "calendar_days": 365,
        "exposure": 3500,
        "execution_quality_class": "X5",
        "execution_stress_ids_passed": [
            "cost-plus-25bps",
            "latency-plus-one-bar",
        ],
        "metric_results": {
            "net_sharpe": 1.0,
            "net_return": 0.10,
            "max_drawdown": -0.09,
        },
        "benchmark_metric_results": {
            "buy-and-hold": {"net_sharpe": 0.5},
        },
        "p_value": 0.001,
        "confidence_interval_low": 0.1,
        "robustness_passed": [
            "cost_stress",
            "latency_stress",
            "parameter_neighborhood",
            "subperiod",
        ],
        "cost_stress_passed": True,
        "latency_stress_passed": True,
    }
    insufficient_holdout = {
        **holdout_result,
        "execution_stress_ids_passed": ["cost-plus-25bps"],
    }
    with pytest.raises(ValueError, match="holdout_execution_stresses_incomplete"):
        repository.execute_holdout_internal(
            holdout_use_id=holdout["id"],
            reservation_token=token,
            result_evidence=insufficient_holdout,
            executor_actor="runner:sealed-holdout",
            request_id=f"request-holdout-insufficient-{suffix}",
        )
    repository.execute_holdout_internal(
        holdout_use_id=holdout["id"],
        reservation_token=token,
        result_evidence=holdout_result,
        executor_actor="runner:sealed-holdout",
        request_id=f"request-holdout-complete-{suffix}",
    )
    sealed = repository.family_evidence(family["id"])
    assert sealed["holdout"]["result_evidence"] is None
    certificate = authority.certify_family(
        {
            "actor_id": "human:independent-certifier",
            "actor_role": "research_authority",
            "request_id": f"request-certificate-{suffix}",
            "family_id": family["id"],
            "robustness": {},
        }
    )
    assert certificate["scientific_quality_class"] == "S4", certificate["evidence"]
    assert certificate["status"] == "qualified"
    released = repository.family_evidence(family["id"])
    assert released["holdout"]["result_evidence"]["artifact_hash"] == "7" * 64
    assert [row["status"] for row in released["attempts"]] == [
        "failed",
        "duplicate",
        "completed",
    ]
    assert released["budget_usage"] == {
        "schema_version": "research_search_budget_usage.v1",
        "attempts": {"maximum": 5, "used": 3, "remaining": 2},
        "runtime_seconds": {"maximum": 500.0, "reserved": 20.0, "remaining": 480.0},
        "compute_units": {"maximum": 50.0, "reserved": 2.0, "remaining": 48.0},
        "validation_feedback_uses": {"maximum": 2, "used": 0, "remaining": 2},
        "attempt_status_counts": {"completed": 1, "duplicate": 1, "failed": 1},
        "rejected_proposal_count": 0,
    }


def test_search_budget_denial_does_not_create_an_unaccounted_trial(
    authority_transaction,
) -> None:
    suffix = uuid4().hex
    raw = _protocol_payload(f"protocol-{suffix}")
    raw["budget"]["max_attempts"] = 1
    protocol = ScientificProtocol.from_dict(raw)
    repository.create_protocol(
        protocol,
        actor_id="human:owner",
        actor_role="research_authority",
        request_id=f"request-protocol-{suffix}",
    )
    family = repository.create_family(
        protocol_id=protocol.protocol_id,
        family_id=f"family-{suffix}",
        name="fixture-family",
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"request-family-{suffix}",
    )
    repository.register_attempt(
        family_id=family["id"],
        request_id=f"request-attempt-1-{suffix}",
        dataset_role="train",
        trial_inputs={"variant": 1},
        estimated_runtime_seconds=1,
        estimated_compute_units=1,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    with pytest.raises(ValueError, match="attempt_budget_exhausted"):
        repository.register_attempt(
            family_id=family["id"],
            request_id=f"request-attempt-2-{suffix}",
            dataset_role="train",
            trial_inputs={"variant": 2},
            estimated_runtime_seconds=1,
            estimated_compute_units=1,
            actor_id="agent:researcher",
            actor_role="research_agent",
        )
    evidence = repository.family_evidence(family["id"])
    assert len(evidence["attempts"]) == 1
    assert evidence["events"][-1]["event_type"] == "ATTEMPT_REGISTERED"


def test_typed_graph_creation_is_family_bound_immutable_and_budgeted(
    authority_transaction,
) -> None:
    suffix = uuid4().hex
    protocol_contract = ScientificProtocol.from_dict(
        _protocol_payload(f"protocol-{suffix}")
    )
    repository.create_protocol(
        protocol_contract,
        actor_id="human:owner",
        actor_role="research_authority",
        request_id=f"protocol-{suffix}",
    )
    family = repository.create_family(
        protocol_id=protocol_contract.protocol_id,
        family_id=f"family-{suffix}",
        name="fixture-family",
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"family-{suffix}",
    )
    admission = authority.create_typed_strategy_graph(
        {
            "actor_id": "agent:researcher",
            "actor_role": "research_agent",
            "request_id": f"graph-request-{suffix}",
            "family_id": family["id"],
            "graph": _graph_payload(f"graph-{suffix}"),
            "mutation_dimensions": ["initial_graph"],
            "estimated_runtime_seconds": 5,
            "estimated_compute_units": 1,
        }
    )
    graph = admission["strategy_graph"]
    attempt = admission["search_attempt"]
    assert graph["search_attempt_id"] == attempt["id"]
    assert attempt["trial_manifest"]["dataset_binding"]["role"] == "train"
    assert (
        attempt["trial_manifest"]["trial_inputs"]["strategy_graph_hash"]
        == graph["graph_hash"]
    )
    evidence = repository.family_evidence(family["id"])
    assert [row["id"] for row in evidence["strategy_graphs"]] == [graph["id"]]

    unsafe = _graph_payload(f"unsafe-{suffix}")
    unsafe["network"] = "https://venue.invalid"
    with pytest.raises(ValueError, match="capability is forbidden"):
        authority.create_typed_strategy_graph(
            {
                "actor_id": "agent:researcher",
                "actor_role": "research_agent",
                "request_id": f"unsafe-request-{suffix}",
                "family_id": family["id"],
                "graph": unsafe,
                "mutation_dimensions": ["expressions"],
            }
        )
    assert len(repository.family_evidence(family["id"])["attempts"]) == 1


def test_offline_governance_reaches_research_certified_and_no_further(
    authority_transaction,
) -> None:
    suffix = uuid4().hex
    observation = research_repository.create_item(
        kind="observation",
        title="Causal observation",
        status="active",
        payload={"evidence_hash": "a" * 64},
        session=authority_transaction,
    )
    hypothesis = research_repository.create_item(
        kind="hypothesis",
        title="Typed hypothesis",
        status="active",
        payload={"evidence_hash": "b" * 64},
        session=authority_transaction,
    )
    protocol_contract = ScientificProtocol.from_dict(
        _protocol_payload(f"protocol-{suffix}")
    )
    repository.create_protocol(
        protocol_contract,
        actor_id="human:protocol-owner",
        actor_role="research_authority",
        request_id=f"protocol-{suffix}",
    )
    family = repository.create_family(
        protocol_id=protocol_contract.protocol_id,
        family_id=f"family-{suffix}",
        name="fixture-family",
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"family-{suffix}",
    )
    case = governance.create_case(
        {
            "actor_id": "agent:governance-proposer",
            "actor_role": "research_agent",
            "request_id": f"case-{suffix}",
            "case_id": f"case-{suffix}",
            "observation_id": observation["id"],
        }
    )
    assert governance.create_case(
        {
            "actor_id": "agent:governance-proposer",
            "actor_role": "research_agent",
            "request_id": f"case-{suffix}",
            "observation_id": observation["id"],
        }
    )["id"] == case["id"]

    def advance(target: str, *, bindings: dict | None = None) -> dict:
        nonlocal case
        proposal = governance.propose_transition(
            {
                "actor_id": "agent:governance-proposer",
                "actor_role": "research_agent",
                "request_id": f"proposal-{target.lower()}-{suffix}",
                "case_id": case["id"],
                "expected_state_version": case["state_version"],
                "target_state": target,
                "binding_updates": dict(bindings or {}),
                "evidence_hashes": [f"{case['state_version'] + 1:064x}"],
                "rationale": f"advance offline evidence to {target}",
            }
        )
        decision = governance.decide_transition(
            {
                "actor_id": "human:governance-authorizer",
                "actor_role": "research_authority",
                "request_id": f"decision-{target.lower()}-{suffix}",
                "proposal_id": proposal["id"],
                "disposition": "approve",
            }
        )
        case = governance.case_trail(case["id"])["case"]
        assert decision["resulting_state"] == target
        return proposal

    first = governance.propose_transition(
        {
            "actor_id": "agent:governance-proposer",
            "actor_role": "research_agent",
            "request_id": f"proposal-hypothesis-{suffix}",
            "case_id": case["id"],
            "expected_state_version": 0,
            "target_state": "HYPOTHESIS",
            "binding_updates": {"hypothesis_id": hypothesis["id"]},
            "evidence_hashes": ["1" * 64],
            "rationale": "link persisted hypothesis",
        }
    )
    assert governance.propose_transition(
        {
            "actor_id": "agent:governance-proposer",
            "actor_role": "research_agent",
            "request_id": f"proposal-hypothesis-{suffix}",
            "case_id": case["id"],
            "expected_state_version": 0,
            "target_state": "HYPOTHESIS",
            "binding_updates": {"hypothesis_id": hypothesis["id"]},
            "evidence_hashes": ["1" * 64],
            "rationale": "link persisted hypothesis",
        }
    )["id"] == first["id"]
    with pytest.raises(ValueError, match="self-authorized"):
        governance.decide_transition(
            {
                "actor_id": "agent:governance-proposer",
                "actor_role": "research_authority",
                "request_id": f"self-decision-{suffix}",
                "proposal_id": first["id"],
                "disposition": "approve",
            }
        )
    governance.decide_transition(
        {
            "actor_id": "human:governance-authorizer",
            "actor_role": "research_authority",
            "request_id": f"decision-hypothesis-{suffix}",
            "proposal_id": first["id"],
            "disposition": "approve",
        }
    )
    case = governance.case_trail(case["id"])["case"]
    with pytest.raises(ValueError, match="structurally closed"):
        governance.propose_transition(
            {
                "actor_id": "agent:governance-proposer",
                "actor_role": "research_agent",
                "request_id": f"proposal-live-{suffix}",
                "case_id": case["id"],
                "expected_state_version": case["state_version"],
                "target_state": "LIVE",
                "binding_updates": {},
                "evidence_hashes": ["2" * 64],
                "rationale": "must fail",
            }
        )
    advance(
        "PROTOCOL_PROPOSED",
        bindings={
            "protocol_id": protocol_contract.protocol_id,
            "family_id": family["id"],
        },
    )
    advance("PROTOCOL_APPROVED")
    advance("TRIALS_RUNNING")

    validation = repository.register_attempt(
        family_id=family["id"],
        request_id=f"validation-{suffix}",
        dataset_role="validation",
        trial_inputs={"strategy_graph_hash": "3" * 64},
        estimated_runtime_seconds=10,
        estimated_compute_units=1,
        actor_id="agent:researcher",
        actor_role="research_agent",
    )
    validation_evidence = {
        "artifact_hash": "4" * 64,
        "reproducible": True,
        "strategy_artifact_hash": "5" * 64,
        "parameter_artifact_hash": "6" * 64,
        "execution_model_hash": "8" * 64,
        "metric_contract_hash": "9" * 64,
        "sample_count": 80,
        "trade_count": 60,
        "calendar_days": 365,
        "exposure": 4000,
        "execution_quality_class": "X5",
        "execution_stress_ids_passed": [
            "cost-plus-25bps",
            "latency-plus-one-bar",
        ],
        "metric_results": {
            "net_sharpe": 1.1,
            "net_return": 0.12,
            "max_drawdown": -0.08,
        },
        "benchmark_metric_results": {
            "buy-and-hold": {"net_sharpe": 0.5},
        },
        "p_value": 0.001,
        "walk_forward_fold_count": 3,
        "purge_bars": 20,
        "embargo_bars": 20,
        "context_only_warmup": True,
        "flat_at_scoring_start": True,
        "no_pending_orders_at_scoring_start": True,
        "signals_not_before_scoring_start": True,
    }
    repository.complete_attempt(
        attempt_id=validation["id"],
        status="completed",
        result_evidence=validation_evidence,
        error=None,
        actual_runtime_seconds=8,
        actual_compute_units=0.8,
        actor_id="runner:offline",
        actor_role="experiment_runner",
        request_id=f"complete-validation-{suffix}",
    )
    advance("EVIDENCE_PRODUCED")
    candidate = repository.freeze_candidate(
        CandidateSnapshot(
            schema_version=CANDIDATE_SCHEMA_VERSION,
            candidate_id=f"candidate-{suffix}",
            family_id=family["id"],
            protocol_hash=protocol_contract.protocol_hash,
            source_attempt_id=validation["id"],
            strategy_artifact_hash="5" * 64,
            parameter_artifact_hash="6" * 64,
            execution_model_hash="8" * 64,
            metric_contract_hash="9" * 64,
            research_dataset_hashes=("a" * 64, "b" * 64),
            evidence_hashes=(validation_evidence["artifact_hash"],),
            frozen_by="agent:researcher",
        ),
        actor_id="agent:researcher",
        actor_role="research_agent",
        request_id=f"candidate-{suffix}",
    )
    advance("CANDIDATE_NOMINATED", bindings={"candidate_id": candidate["id"]})
    advance("VALIDATION_PASSED")
    repository.close_family(
        family_id=family["id"],
        actor_id="human:holdout-authority",
        actor_role="research_authority",
        request_id=f"close-{suffix}",
    )
    advance("HOLDOUT_ELIGIBLE")
    holdout, token = repository.reserve_holdout(
        family_id=family["id"],
        candidate_id=candidate["id"],
        actor_id="human:holdout-authority",
        actor_role="research_authority",
        request_id=f"holdout-{suffix}",
    )
    repository.execute_holdout_internal(
        holdout_use_id=holdout["id"],
        reservation_token=token,
        result_evidence={
            "artifact_hash": "7" * 64,
            "reproducible": True,
            "sample_count": 75,
            "trade_count": 55,
            "calendar_days": 365,
            "exposure": 3500,
            "execution_quality_class": "X5",
            "execution_stress_ids_passed": [
                "cost-plus-25bps",
                "latency-plus-one-bar",
            ],
            "metric_results": {
                "net_sharpe": 1.0,
                "net_return": 0.10,
                "max_drawdown": -0.09,
            },
            "benchmark_metric_results": {
                "buy-and-hold": {"net_sharpe": 0.5},
            },
            "p_value": 0.001,
            "confidence_interval_low": 0.1,
            "robustness_passed": [],
            "cost_stress_passed": False,
            "latency_stress_passed": False,
        },
        executor_actor="runner:sealed-holdout",
        request_id=f"execute-holdout-{suffix}",
    )
    advance("HOLDOUT_EVALUATED")
    certificate = authority.certify_family(
        {
            "actor_id": "human:science-certifier",
            "actor_role": "research_authority",
            "request_id": f"certificate-{suffix}",
            "family_id": family["id"],
            "robustness": {},
        }
    )
    assert certificate["status"] == "qualified"
    assert certificate["scientific_quality_class"] == "S3"
    advance(
        "RESEARCH_CERTIFIED",
        bindings={"certificate_id": certificate["id"]},
    )
    trail = governance.case_trail(case["id"])
    assert trail["case"]["current_state"] == "RESEARCH_CERTIFIED"
    assert trail["maximum_state"] == "RESEARCH_CERTIFIED"
    assert trail["operational_trading_authority"] is False
    assert len(trail["decisions"]) == 10


def _campaign_runner_charter(suffix: str) -> CampaignCharter:
    path = (
        Path(__file__).parents[2]
        / "config"
        / "research_campaigns"
        / "btc_perp_market_structure_v3.json"
    )
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["schema_version"] = CAMPAIGN_CHARTER_SCHEMA_VERSION
    raw["eligible_fact_types"] = [*raw["eligible_fact_types"], "market.trade"]
    raw["replay_availability_policy"] = ResearchReplayAvailabilityPolicy().to_dict()
    campaign_id = f"btc_perp_runner_e2e_{suffix[:8]}"
    raw["campaign_id"] = campaign_id
    raw["instrument_id"] = "BTC-USD"
    raw["instrument_symbol"] = "BTC-PERP"
    raw["datasets"] = [
        {
            "role": "train",
            "dataset_id": f"mds_{'a' * 32}",
            "dataset_hash": "a" * 64,
            "window_start": "2020-01-01T00:00:00Z",
            "window_end": "2020-01-01T01:59:00Z",
        },
        {
            "role": "validation",
            "dataset_id": f"mds_{'b' * 32}",
            "dataset_hash": "b" * 64,
            "window_start": "2021-01-01T00:00:00Z",
            "window_end": "2021-01-01T01:59:00Z",
        },
        {
            "role": "holdout",
            "blind_alias": f"sealed-{suffix}",
            "sealed": True,
        },
    ]
    return resolve_campaign_charter(
        raw,
        sealed_holdout_binding={
            "dataset_id": f"mds_{'c' * 32}",
            "dataset_hash": "c" * 64,
            "window_start": "2022-01-01T00:00:00Z",
            "window_end": "2022-01-01T01:59:00Z",
        },
    )


def _campaign_runner_bars(
    start: datetime,
    count: int = 120,
) -> tuple[FrozenCampaignBar, ...]:
    rows = []
    price = 60_000.0
    for index in range(count):
        prior = price
        direction = 1.0 if (index // 20) % 2 == 0 else -1.0
        price = prior * (1.0 + direction * 0.002)
        bucket_start = start + timedelta(minutes=index)
        rows.append(
            FrozenCampaignBar(
                bucket_start=bucket_start,
                bucket_end=bucket_start + timedelta(minutes=1),
                known_at=bucket_start + timedelta(minutes=1, milliseconds=50),
                open_price=prior,
                high_price=max(prior, price) * 1.0001,
                low_price=min(prior, price) * 0.9999,
                close_price=price,
                trade_count=20 + index % 5,
                base_volume=100.0,
                quote_notional=6_000_000.0 + index * 1000.0,
                cvd_delta=50.0 * direction,
                open_interest=100_000.0 + index,
                funding_rate=0.00001,
                source_hashes=(f"source-{index}",),
            )
        )
    return tuple(rows)


@pytest.mark.parametrize("force_validation_rejection", [False, True])
def test_campaign_runner_completes_persisted_sealed_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    authority_transaction,
    force_validation_rejection: bool,
) -> None:
    suffix = uuid4().hex
    charter = _campaign_runner_charter(suffix)
    bars = {
        "train": _campaign_runner_bars(datetime(2020, 1, 1, tzinfo=UTC)),
        "validation": _campaign_runner_bars(datetime(2021, 1, 1, tzinfo=UTC)),
        "holdout": _campaign_runner_bars(datetime(2022, 1, 1, tzinfo=UTC)),
    }
    costs = CampaignExecutionCosts(
        market_slippage_bps=charter.market_slippage_bps,
        taker_fee_rate=charter.taker_fee_rate,
        execution_quality_class="X2",
        execution_model_hash="e" * 64,
        fee_schedule_hash="f" * 64,
        stress_scenarios=charter.cost_stress_scenarios,
    )
    monkeypatch.setattr(campaign_runner, "load_private_charter", lambda _path: charter)
    monkeypatch.setattr(
        campaign_runner,
        "preflight_campaign",
        lambda _path, *, code_revision: {
            "preflight_hash": "d" * 64,
            "code_revision": code_revision,
        },
    )
    monkeypatch.setattr(
        campaign_runner,
        "_load_replay_role_inputs",
        lambda _charter, role: campaign_runner.CampaignReplayRoleInputs(
            role=role,
            bars=bars[role],
            replay_artifact=ResearchReplayAvailabilityArtifact(
                schema_version="research_replay_availability.v1",
                policy_hash=charter.replay_availability_policy.policy_hash,
                bucket_count=len(bars[role]),
                eligible_bucket_count=len(bars[role]),
                excluded_bucket_count=0,
                exclusion_counts={},
                coverage_material_hashes=("a" * 64,),
                replay_bucket_hashes=tuple(
                    f"{role}-bucket-{index}" for index in range(len(bars[role]))
                ),
                replay_semantic_hash=(
                    {"train": "a", "validation": "b", "holdout": "c"}[role]
                    * 64
                ),
            ),
            replay_binding_hash=(
                {"train": "d", "validation": "e", "holdout": "f"}[role]
                * 64
            ),
            dataset_manifest_hash=(
                {"train": "1", "validation": "2", "holdout": "3"}[role]
                * 64
            ),
        ),
    )
    monkeypatch.setattr(
        campaign_runner,
        "_execution_bundle",
        lambda _charter: (costs, {"context_hash": "e" * 64}),
    )
    monkeypatch.setattr(
        campaign_runner,
        "campaign_graph_specs",
        # Strategy-space construction has separate exhaustive tests; use a
        # reliably passing family here to isolate persisted lifecycle wiring.
        lambda: tuple(
            {
                "family": "flow_continuation",
                "flow": 0.1,
                "price": 0.0,
                "ordinal": ordinal,
            }
            for ordinal in range(1, 25)
        ),
    )
    if force_validation_rejection:
        monkeypatch.setattr(
            campaign_runner,
            "_gate_failures",
            lambda _charter, _evaluation: ("forced_validation_rejection",),
        )

    result = campaign_runner.execute_campaign(
        "private-charter-not-read.json",
        code_revision="runner-e2e-test-revision",
    )

    persisted_protocol = repository.protocol_private(result["protocol_id"])
    replay_policy_versions = dict(persisted_protocol.policy_versions)
    assert replay_policy_versions["research_replay_availability"] == (
        charter.replay_availability_policy.policy_hash
    )
    assert replay_policy_versions["research_replay_train_binding"] == "d" * 64
    assert replay_policy_versions["research_replay_validation_binding"] == "e" * 64
    assert replay_policy_versions["research_replay_holdout_binding"] == "f" * 64

    diagnostic = repository.family_evidence(result["family_id"], private=True)
    if force_validation_rejection:
        assert result["outcome"] == "rejected_before_holdout"
        assert result["holdout_opened"] is False
        assert diagnostic["family"]["status"] == "archived"
        assert diagnostic["holdout"] is None
        assert all(
            attempt["status"] in {"completed", "invalid", "failed", "abandoned"}
            for attempt in diagnostic["attempts"]
        )
        trail = governance.case_trail(result["governance_case_id"])
        assert trail["case"]["current_state"] == "ARCHIVED"
        assert trail["operational_trading_authority"] is False
        return
    assert result["outcome"] == "research_certified", [
        (
            attempt["dataset_role"],
            attempt["status"],
            attempt["error"],
            (attempt["result_evidence"] or {}).get("gate_failures"),
        )
        for attempt in diagnostic["attempts"]
        if attempt["dataset_role"] == "validation"
    ]
    assert result["holdout_opened"] is True
    assert result["holdout_consumed"] is True
    assert result["promotion_eligible"] is False
    assert result["external_trading_authority"] is False
    evidence = diagnostic
    assert evidence["family"]["feedback_released"] is True
    assert evidence["holdout"]["status"] == "completed"
    assert len(evidence["certificates"]) == 1
    assert evidence["certificates"][0]["status"] == "qualified"
    assert evidence["attempts"]
    assert all(
        attempt["status"] in {"completed", "invalid", "failed", "abandoned"}
        for attempt in evidence["attempts"]
    )
    trail = governance.case_trail(result["governance_case_id"])
    assert trail["case"]["current_state"] == "RESEARCH_CERTIFIED"
    assert trail["maximum_state"] == "RESEARCH_CERTIFIED"
    assert trail["operational_trading_authority"] is False

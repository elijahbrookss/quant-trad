"""Application service for protocol-bound offline research authority."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Mapping

from research_science import (
    CANDIDATE_SCHEMA_VERSION,
    SCIENTIFIC_EVIDENCE_SCHEMA_VERSION,
    CandidateSnapshot,
    BlindnessClass,
    ScientificEvidence,
    ScientificProtocol,
    adjusted_p_values,
    classify_scientific_quality,
)
from portal.backend.service.storage.repos.market_data import market_data_repo
from strategies.typed_graph import (
    TYPED_STRATEGY_GRAPH_VERSION,
    TypedStrategyGraph,
)

from . import authority_repository as repository


RESEARCHER_ROLES = {"researcher", "research_agent"}
AUTHORITY_ROLES = {"research_authority", "human_research_owner"}
RUNNER_ROLES = {"experiment_runner"}
HOLDOUT_EXECUTOR_ROLE = "holdout_executor"
_FORBIDDEN_TRIAL_INPUT_KEYS = {
    "dataset_id",
    "dataset_hash",
    "dataset_binding",
    "dataset_role",
    "economic_claim_intent",
    "holdout_dataset",
    "provider_fetch",
}
_ALLOWED_GRAPH_MUTATION_DIMENSIONS = {
    "initial_graph",
    "facts",
    "expressions",
    "actions",
    "execution_policy",
    "sizing",
    "risk_constraints",
    "parameter_values",
}
_EXECUTION_QUALITY_RANK = {f"X{index}": index for index in range(6)}


def _required(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _generated_id(prefix: str, *, actor_id: str, request_id: str) -> str:
    stable = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"quant-trad:{prefix}:{actor_id}:{request_id}",
    )
    return f"{prefix}:{stable}"


def _identity(payload: Mapping[str, Any], *, admitted_roles: set[str]) -> tuple[str, str, str]:
    actor_id = _required(payload.get("actor_id"), field="actor_id")
    actor_role = _required(payload.get("actor_role"), field="actor_role").lower()
    request_id = _required(payload.get("request_id"), field="request_id")
    if actor_role not in admitted_roles:
        raise ValueError(
            f"actor_role_not_authorized role={actor_role} expected={','.join(sorted(admitted_roles))}"
        )
    return actor_id, actor_role, request_id


def _forbidden_trial_keys(value: Any, *, path: str = "trial_inputs") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).strip().lower()
            nested_path = f"{path}.{normalized}"
            if normalized in _FORBIDDEN_TRIAL_INPUT_KEYS:
                found.append(nested_path)
            found.extend(_forbidden_trial_keys(nested, path=nested_path))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            found.extend(_forbidden_trial_keys(nested, path=f"{path}[{index}]"))
    return found


def _utc(value: Any) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _validate_protocol_datasets(protocol: ScientificProtocol) -> None:
    """Resolve every declared role to an existing provider-free frozen artifact."""

    if protocol.blindness in {
        BlindnessClass.EXTERNALLY_ATTESTED,
        BlindnessClass.FORWARD_UNSEEN,
    }:
        raise ValueError(
            "requested holdout blindness requires an attestation/allocation authority not implemented in this campaign"
        )
    ordered = [protocol.assignment(role) for role in ("train", "validation", "holdout")]
    if not (
        _utc(ordered[0].window_end) <= _utc(ordered[1].window_start)
        and _utc(ordered[1].window_end) <= _utc(ordered[2].window_start)
    ):
        raise ValueError(
            "scientific_protocol_dataset_windows_must_be_chronological_and_disjoint"
        )
    for assignment in ordered:
        dataset = market_data_repo.get_dataset(assignment.dataset_id)
        if dataset.contract_version != "market_dataset.v1":
            raise ValueError("scientific_protocol_dataset_contract_unsupported")
        if dataset.dataset_hash != assignment.dataset_hash:
            raise ValueError(
                f"scientific_protocol_dataset_hash_mismatch role={assignment.role.value}"
            )
        if dataset.dataset_id != f"mds_{dataset.dataset_hash[:32]}":
            raise RuntimeError("scientific_protocol_dataset_identity_disagreement")
        if not dataset.series:
            raise ValueError("scientific_protocol_dataset_has_no_frozen_series")
        dataset_instruments = {
            str(row.get("instrument_id") or "").strip()
            for row in dataset.series
            if str(row.get("instrument_id") or "").strip()
        }
        if not set(protocol.instrument_ids) <= dataset_instruments:
            raise ValueError(
                "scientific_protocol_instrument_universe_not_covered: "
                + ",".join(sorted(set(protocol.instrument_ids) - dataset_instruments))
            )
        start = _utc(assignment.window_start)
        end = _utc(assignment.window_end)
        uncovered = [
            str(row.get("identity_key") or row.get("series_id") or "unknown")
            for row in dataset.series
            if _utc(row.get("range_start")) > start
            or _utc(row.get("range_end")) < end
        ]
        if uncovered:
            raise ValueError(
                "scientific_protocol_dataset_window_not_covered: "
                + ",".join(sorted(uncovered))
            )


def create_protocol(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, admitted_roles=AUTHORITY_ROLES)
    manifest = dict(payload.get("protocol") or {})
    if not manifest.get("protocol_id"):
        manifest["protocol_id"] = _generated_id(
            "protocol", actor_id=actor_id, request_id=request_id
        )
    manifest["created_by"] = actor_id
    manifest["authorized_by"] = actor_id
    manifest["authorization_request_id"] = request_id
    protocol = ScientificProtocol.from_dict(manifest)
    _validate_protocol_datasets(protocol)
    return repository.create_protocol(
        protocol,
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )


def get_protocol(protocol_id: str) -> dict[str, Any]:
    """Research-facing read: the final holdout binding stays redacted."""

    return repository.get_protocol(protocol_id, private=False)


def create_family(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(
        payload, admitted_roles=RESEARCHER_ROLES | AUTHORITY_ROLES
    )
    return repository.create_family(
        protocol_id=_required(payload.get("protocol_id"), field="protocol_id"),
        family_id=str(
            payload.get("family_id")
            or _generated_id("family", actor_id=actor_id, request_id=request_id)
        ),
        name=_required(payload.get("name"), field="name"),
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )


def register_attempt(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, admitted_roles=RESEARCHER_ROLES)
    trial_inputs = dict(payload.get("trial_inputs") or {})
    family_id = _required(payload.get("family_id"), field="family_id")
    forbidden = _forbidden_trial_keys(trial_inputs)
    if forbidden:
        error = ValueError(
            "trial inputs may not choose datasets, claim intent, or provider fetches: "
            + ",".join(sorted(forbidden))
        )
        repository.record_rejected_proposal(
            family_id=family_id,
            request_id=request_id,
            actor_id=actor_id,
            actor_role=actor_role,
            reason=str(error),
            proposal={"dataset_role": payload.get("dataset_role"), "trial_inputs": trial_inputs},
        )
        raise error
    try:
        return repository.register_attempt(
            family_id=family_id,
            request_id=request_id,
            dataset_role=_required(payload.get("dataset_role"), field="dataset_role"),
            trial_inputs=trial_inputs,
            estimated_runtime_seconds=float(payload.get("estimated_runtime_seconds") or 0.0),
            estimated_compute_units=float(payload.get("estimated_compute_units") or 0.0),
            actor_id=actor_id,
            actor_role=actor_role,
        )
    except ValueError as exc:
        repository.record_rejected_proposal(
            family_id=family_id,
            request_id=request_id,
            actor_id=actor_id,
            actor_role=actor_role,
            reason=str(exc),
            proposal={"dataset_role": payload.get("dataset_role"), "trial_inputs": trial_inputs},
        )
        raise


def complete_attempt(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, admitted_roles=RUNNER_ROLES)
    status = _required(payload.get("status"), field="status").lower()
    evidence = dict(payload.get("result_evidence") or {})
    required = (
        {"artifact_hash", "reproducible", "sample_count", "exposure"}
        if status == "completed"
        else set()
    )
    missing = sorted(field for field in required if evidence.get(field) is None)
    if missing:
        raise ValueError("attempt result evidence is incomplete: " + ",".join(missing))
    return repository.complete_attempt(
        attempt_id=_required(payload.get("attempt_id"), field="attempt_id"),
        status=status,
        result_evidence=evidence,
        error=str(payload.get("error") or "").strip() or None,
        actual_runtime_seconds=float(payload.get("actual_runtime_seconds") or 0.0),
        actual_compute_units=float(payload.get("actual_compute_units") or 0.0),
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )


def freeze_candidate(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, admitted_roles=RESEARCHER_ROLES)
    manifest = dict(payload.get("candidate") or {})
    manifest.setdefault("schema_version", CANDIDATE_SCHEMA_VERSION)
    manifest.setdefault(
        "candidate_id",
        _generated_id("candidate", actor_id=actor_id, request_id=request_id),
    )
    manifest["frozen_by"] = actor_id
    candidate = CandidateSnapshot.from_dict(manifest)
    return repository.freeze_candidate(
        candidate,
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )


def create_typed_strategy_graph(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Admit one bounded graph and charge its creation to family search budget."""

    actor_id, actor_role, request_id = _identity(
        payload, admitted_roles=RESEARCHER_ROLES
    )
    family_id = _required(payload.get("family_id"), field="family_id")
    context = repository.family_context(family_id)
    family = context["family"]
    protocol_manifest = context["protocol"]["manifest"]
    if family.get("status") != "open":
        raise ValueError("research_family_not_open_for_strategy_generation")
    manifest = dict(payload.get("graph") or {})
    manifest.setdefault("schema_version", TYPED_STRATEGY_GRAPH_VERSION)
    manifest.setdefault(
        "graph_id",
        _generated_id("strategy_graph", actor_id=actor_id, request_id=request_id),
    )
    manifest["family_id"] = family_id
    manifest["protocol_hash"] = context["protocol"]["protocol_hash"]
    manifest["created_by"] = actor_id
    graph = TypedStrategyGraph.from_dict(manifest)
    mutation_dimensions = tuple(
        sorted(
            {
                _required(value, field="mutation_dimension")
                for value in payload.get("mutation_dimensions") or ()
            }
        )
    )
    protocol_mutations = set(
        protocol_manifest.get("allowed_mutation_dimensions") or ()
    )
    if (
        not mutation_dimensions
        or not set(mutation_dimensions) <= _ALLOWED_GRAPH_MUTATION_DIMENSIONS
        or not set(mutation_dimensions) <= protocol_mutations
    ):
        raise ValueError("typed strategy mutation dimensions are empty or unsupported")
    graph_by_id = {row["id"]: row for row in context["strategy_graphs"]}
    missing_parents = sorted(set(graph.parent_graph_ids) - set(graph_by_id))
    if missing_parents:
        raise ValueError("typed_strategy_graph_parent_outside_family")
    parent_attempt_ids = [
        graph_by_id[parent_id]["search_attempt_id"]
        for parent_id in graph.parent_graph_ids
    ]
    attempt = repository.register_attempt(
        family_id=family_id,
        request_id=request_id,
        dataset_role="train",
        trial_inputs={
            "strategy_graph_hash": graph.graph_hash,
            "strategy_graph_id": graph.graph_id,
            "parent_attempt_ids": parent_attempt_ids,
            "influenced_by_attempt_ids": list(
                payload.get("influenced_by_attempt_ids") or ()
            ),
            "mutation_dimensions": list(mutation_dimensions),
            "protocol_family_name": protocol_manifest.get("family_name"),
        },
        estimated_runtime_seconds=float(
            payload.get("estimated_runtime_seconds") or 0.0
        ),
        estimated_compute_units=float(
            payload.get("estimated_compute_units") or 0.0
        ),
        actor_id=actor_id,
        actor_role=actor_role,
    )
    record = repository.create_strategy_graph(
        graph,
        search_attempt_id=attempt["id"],
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )
    return {
        "schema_version": "typed_strategy_graph_admission.v1",
        "strategy_graph": record,
        "search_attempt": attempt,
    }


def reserve_holdout(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Reserve one family holdout without disclosing its binding or token."""

    actor_id, actor_role, request_id = _identity(payload, admitted_roles=AUTHORITY_ROLES)
    record, _internal_token = repository.reserve_holdout(
        family_id=_required(payload.get("family_id"), field="family_id"),
        candidate_id=_required(payload.get("candidate_id"), field="candidate_id"),
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )
    return {
        **record,
        "holdout_binding": None,
        "reservation_token": None,
        "blindness_claim": "workflow reservation only; prior external exposure is not provable",
    }


def execute_holdout_internal(
    *,
    holdout_use_id: str,
    reservation_token: str,
    result_evidence: Mapping[str, Any],
    executor_actor: str,
    request_id: str,
) -> dict[str, Any]:
    """Internal executor seam; deliberately absent from the public controller."""

    return repository.execute_holdout_internal(
        holdout_use_id=holdout_use_id,
        reservation_token=reservation_token,
        result_evidence=result_evidence,
        executor_actor=executor_actor,
        request_id=request_id,
    )


def close_family(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, admitted_roles=AUTHORITY_ROLES)
    return repository.close_family(
        family_id=_required(payload.get("family_id"), field="family_id"),
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )


def family_evidence(family_id: str) -> dict[str, Any]:
    return repository.family_evidence(family_id)


def _scientific_evidence_from_family(
    family: Mapping[str, Any],
    *,
    robustness: Mapping[str, Any],
) -> tuple[ScientificEvidence, dict[str, Any]]:
    protocol = repository.protocol_private(family["family"]["protocol_id"])
    attempts = list(family.get("attempts") or [])
    terminal = [row for row in attempts if row.get("status") in repository.TERMINAL_ATTEMPT_STATUSES]
    completed = [row for row in attempts if row.get("status") == "completed"]
    validation = [row for row in completed if row.get("dataset_role") == "validation"]
    candidate = next(
        (row for row in family.get("candidates") or [] if row.get("id") == family["family"].get("current_candidate_id")),
        None,
    )
    holdout = dict(family.get("holdout") or {})
    holdout_result = dict(holdout.get("result_evidence") or {})
    candidate_result = {}
    if candidate is not None:
        source_id = candidate.get("source_attempt_id")
        source = next((row for row in attempts if row.get("id") == source_id), None)
        candidate_result = dict((source or {}).get("result_evidence") or {})
    p_values = [
        float(row["result_evidence"]["p_value"])
        for row in completed
        if isinstance(row.get("result_evidence"), Mapping)
        and row["result_evidence"].get("p_value") is not None
    ]
    holdout_p = holdout_result.get("p_value")
    adjusted_holdout = None
    if holdout_p is not None:
        values = [*p_values, float(holdout_p)]
        adjusted_holdout = adjusted_p_values(
            values,
            method=protocol.multiple_testing_method,
        )[-1]
    budget_runtime = sum(float(row.get("actual_runtime_seconds") or row.get("estimated_runtime_seconds") or 0.0) for row in attempts)
    budget_compute = sum(float(row.get("actual_compute_units") or row.get("estimated_compute_units") or 0.0) for row in attempts)
    holdout_used_once = bool(holdout) and holdout.get("status") == "completed"
    candidate_created = str((candidate or {}).get("created_at") or "")
    holdout_reserved = str(holdout.get("reserved_at") or "")
    candidate_before_holdout = bool(candidate_created and holdout_reserved and candidate_created <= holdout_reserved)
    required_robustness = protocol.robustness_requirements
    passed_robustness = tuple(robustness.get("passed") or holdout_result.get("robustness_passed") or ())
    evidence = ScientificEvidence(
        schema_version=SCIENTIFIC_EVIDENCE_SCHEMA_VERSION,
        reproducible=bool(candidate_result.get("reproducible")) and bool(holdout_result.get("reproducible")),
        protocol_bound=True,
        attempts_registered=len(attempts),
        attempts_accounted=len(terminal),
        budget_compliant=(
            len(attempts) <= protocol.budget.max_attempts
            and budget_runtime <= protocol.budget.max_runtime_seconds
            and budget_compute <= protocol.budget.max_compute_units
        ),
        benchmark_present=bool(protocol.benchmark_ids),
        walk_forward_complete=bool(validation) and all(
            int(dict(row.get("result_evidence") or {}).get("walk_forward_fold_count") or 0)
            == protocol.walk_forward.fold_count
            for row in validation
        ),
        leakage_controls_applied=bool(validation) and all(
            int(dict(row.get("result_evidence") or {}).get("purge_bars") or -1)
            == protocol.leakage.purge_bars
            and int(dict(row.get("result_evidence") or {}).get("embargo_bars") or -1)
            == protocol.leakage.embargo_bars
            and dict(row.get("result_evidence") or {}).get("context_only_warmup") is True
            and dict(row.get("result_evidence") or {}).get("flat_at_scoring_start") is True
            and dict(row.get("result_evidence") or {}).get("no_pending_orders_at_scoring_start") is True
            and dict(row.get("result_evidence") or {}).get("signals_not_before_scoring_start") is True
            for row in validation
        ),
        candidate_frozen_before_holdout=candidate_before_holdout,
        holdout_used_once=holdout_used_once,
        blindness=protocol.blindness,
        sample_count=int(holdout_result.get("sample_count") or candidate_result.get("sample_count") or 0),
        trade_count=int(holdout_result.get("trade_count") or candidate_result.get("trade_count") or 0),
        calendar_days=int(holdout_result.get("calendar_days") or candidate_result.get("calendar_days") or 0),
        exposure=float(holdout_result.get("exposure") or candidate_result.get("exposure") or 0.0),
        minimum_sample_count=protocol.minimum_sample_count,
        minimum_trade_count=protocol.minimum_trade_count,
        minimum_calendar_days=protocol.minimum_calendar_days,
        minimum_exposure=protocol.minimum_exposure,
        execution_quality_sufficient=(
            _EXECUTION_QUALITY_RANK.get(
                str(
                    holdout_result.get("execution_quality_class")
                    or candidate_result.get("execution_quality_class")
                    or ""
                ).upper(),
                -1,
            )
            >= _EXECUTION_QUALITY_RANK[protocol.minimum_execution_quality_class]
        ),
        safety_metrics_passed=set(protocol.safety_metrics)
        <= set(
            dict(
                holdout_result.get("metric_results")
                or candidate_result.get("metric_results")
                or {}
            )
        ),
        raw_p_value=float(holdout_p) if holdout_p is not None else None,
        adjusted_p_value=adjusted_holdout,
        alpha=protocol.alpha,
        confidence_interval_low=(
            float(holdout_result["confidence_interval_low"])
            if holdout_result.get("confidence_interval_low") is not None
            else None
        ),
        robustness_passed=passed_robustness,
        robustness_required=required_robustness,
        cost_stress_passed=bool(robustness.get("cost_stress_passed", holdout_result.get("cost_stress_passed"))),
        latency_stress_passed=bool(robustness.get("latency_stress_passed", holdout_result.get("latency_stress_passed"))),
        failed_trials_retained=len(attempts) == len(terminal),
        family_closed_before_holdout=bool(
            family["family"].get("closed_at")
            and holdout_reserved
            and str(family["family"]["closed_at"]) <= holdout_reserved
        ),
    )
    supplemental = {
        "protocol_hash": protocol.protocol_hash,
        "family_hash": family["family"].get("family_hash"),
        "candidate_hash": (candidate or {}).get("candidate_hash"),
        "attempt_ids": [row.get("id") for row in attempts],
        "holdout_use_id": holdout.get("id"),
        "budget_runtime_used": budget_runtime,
        "budget_compute_used": budget_compute,
    }
    return evidence, supplemental


def certify_family(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, admitted_roles=AUTHORITY_ROLES)
    family_id = _required(payload.get("family_id"), field="family_id")
    evidence_bundle = repository.family_evidence(family_id, private=True)
    family = evidence_bundle["family"]
    if family.get("status") != "holdout_completed":
        raise ValueError("scientific certification requires a consumed holdout")
    candidate_id = _required(family.get("current_candidate_id"), field="current_candidate_id")
    candidate = next(row for row in evidence_bundle["candidates"] if row["id"] == candidate_id)
    if candidate.get("frozen_by") == actor_id:
        raise ValueError("candidate freezer cannot certify its own candidate")
    evidence, supplemental = _scientific_evidence_from_family(
        evidence_bundle,
        robustness=dict(payload.get("robustness") or {}),
    )
    assessment = classify_scientific_quality(evidence)
    required_quality = {
        "exploration": "S0",
        "economic": "S2",
        "selection": "S3",
        "promotion": "S4",
    }[repository.protocol_private(family["protocol_id"]).economic_claim_intent]
    quality_rank = {f"S{index}": index for index in range(5)}
    meets_claim = (
        assessment.qualified
        and quality_rank[assessment.scientific_quality_class.value]
        >= quality_rank[required_quality]
    )
    certificate_evidence = {
        "schema_version": "scientific_certificate_evidence.v1",
        "scientific_evidence": {
            **evidence.__dict__,
            "blindness": evidence.blindness.value,
        },
        "assessment": assessment.to_dict(),
        "required_scientific_quality_class": required_quality,
        "economic_claim_intent_qualified": meets_claim,
        "lineage": supplemental,
    }
    certificate = repository.create_certificate(
        protocol_id=family["protocol_id"],
        family_id=family_id,
        candidate_id=candidate_id,
        scientific_quality_class=assessment.scientific_quality_class.value,
        status="qualified" if meets_claim else "blocked",
        evidence=certificate_evidence,
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
    )
    repository.release_holdout_feedback(
        family_id=family_id,
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=f"{request_id}:feedback",
    )
    return certificate


__all__ = [
    "AUTHORITY_ROLES",
    "HOLDOUT_EXECUTOR_ROLE",
    "RESEARCHER_ROLES",
    "RUNNER_ROLES",
    "certify_family",
    "close_family",
    "complete_attempt",
    "create_family",
    "create_protocol",
    "create_typed_strategy_graph",
    "execute_holdout_internal",
    "family_evidence",
    "freeze_candidate",
    "get_protocol",
    "register_attempt",
    "reserve_holdout",
]

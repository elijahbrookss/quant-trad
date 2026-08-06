"""Transactional persistence for scientific research authority."""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from portal.backend.db.models import (
    ResearchAttemptRecord,
    ResearchAuthorityEventRecord,
    ResearchCandidateRecord,
    ResearchCertificateRecord,
    ResearchFamilyRecord,
    ResearchHoldoutUseRecord,
    ResearchProtocolRecord,
    ResearchStrategyGraphRecord,
)
from portal.backend.db.session import db
from research_science import CandidateSnapshot, DatasetRole, ScientificProtocol
from strategies.typed_graph import TypedStrategyGraph, compile_typed_strategy_graph


TERMINAL_ATTEMPT_STATUSES = {"completed", "failed", "abandoned", "invalid"}
_EXECUTION_QUALITY_RANK = {f"X{index}": index for index in range(6)}


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _required(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _event(
    session: Session,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    actor_id: str,
    actor_role: str,
    request_id: str,
    idempotency_key: str,
    payload: Mapping[str, Any],
) -> ResearchAuthorityEventRecord:
    prior = session.scalar(
        select(ResearchAuthorityEventRecord).where(
            ResearchAuthorityEventRecord.aggregate_type == aggregate_type,
            ResearchAuthorityEventRecord.aggregate_id == aggregate_id,
            ResearchAuthorityEventRecord.idempotency_key == idempotency_key,
        )
    )
    if prior is not None:
        return prior
    latest = session.scalar(
        select(func.max(ResearchAuthorityEventRecord.event_seq)).where(
            ResearchAuthorityEventRecord.aggregate_type == aggregate_type,
            ResearchAuthorityEventRecord.aggregate_id == aggregate_id,
        )
    )
    event_seq = int(latest or 0) + 1
    material = {
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "event_seq": event_seq,
        "event_type": event_type,
        "actor_id": actor_id,
        "actor_role": actor_role,
        "request_id": request_id,
        "idempotency_key": idempotency_key,
        "payload": dict(payload),
    }
    record = ResearchAuthorityEventRecord(
        id=f"research_event:{_stable_hash(material)}",
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        event_seq=event_seq,
        event_type=event_type,
        actor_id=actor_id,
        actor_role=actor_role,
        request_id=request_id,
        idempotency_key=idempotency_key,
        payload=dict(payload),
        evidence_hash=_stable_hash(material),
        created_at=_now(),
    )
    session.add(record)
    session.flush()
    return record


def create_protocol(
    protocol: ScientificProtocol,
    *,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        existing = session.get(ResearchProtocolRecord, protocol.protocol_id)
        if existing is not None:
            if existing.protocol_hash != protocol.protocol_hash:
                raise ValueError("scientific_protocol_id_conflict")
            return existing.to_dict()
        record = ResearchProtocolRecord(
            id=protocol.protocol_id,
            schema_version=protocol.schema_version,
            protocol_hash=protocol.protocol_hash,
            status="active",
            blindness_class=protocol.blindness.value,
            private_manifest=protocol.to_private_dict(),
            public_manifest=protocol.to_public_dict(),
            created_by=actor_id,
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        _event(
            session,
            aggregate_type="protocol",
            aggregate_id=record.id,
            event_type="PROTOCOL_CREATED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"create:{request_id}",
            payload={"protocol_hash": record.protocol_hash},
        )
        return record.to_dict()


def get_protocol(protocol_id: str, *, private: bool = False) -> dict[str, Any]:
    with db.session() as session:
        record = session.get(ResearchProtocolRecord, _required(protocol_id, field="protocol_id"))
        if record is None:
            raise KeyError(f"scientific protocol not found: {protocol_id}")
        return record.to_dict(private=private)


def create_family(
    *,
    protocol_id: str,
    family_id: str,
    name: str,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        protocol = session.scalar(
            select(ResearchProtocolRecord)
            .where(ResearchProtocolRecord.id == protocol_id)
            .with_for_update()
        )
        if protocol is None:
            raise KeyError(f"scientific protocol not found: {protocol_id}")
        if protocol.status != "active":
            raise ValueError("scientific_protocol_not_active")
        protocol_manifest = ScientificProtocol.from_dict(protocol.private_manifest)
        if _required(name, field="family.name") != protocol_manifest.family_name:
            raise ValueError("research_family_name_must_match_protocol")
        existing = session.get(ResearchFamilyRecord, family_id)
        family_hash = _stable_hash(
            {"protocol_hash": protocol.protocol_hash, "family_id": family_id, "name": name}
        )
        if existing is not None:
            if existing.family_hash != family_hash:
                raise ValueError("research_family_id_conflict")
            return existing.to_dict()
        record = ResearchFamilyRecord(
            id=family_id,
            protocol_id=protocol.id,
            family_hash=family_hash,
            name=_required(name, field="family.name"),
            status="open",
            feedback_released=False,
            created_by=actor_id,
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=record.id,
            event_type="FAMILY_CREATED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"create:{request_id}",
            payload={"protocol_id": protocol.id, "family_hash": family_hash},
        )
        return record.to_dict()


def family_context(family_id: str) -> dict[str, Any]:
    """Internal immutable context used to validate generated graph admission."""

    with db.session() as session:
        family = session.get(ResearchFamilyRecord, _required(family_id, field="family_id"))
        if family is None:
            raise KeyError(f"research family not found: {family_id}")
        protocol = session.get(ResearchProtocolRecord, family.protocol_id)
        if protocol is None:
            raise RuntimeError("research_family_protocol_missing")
        graphs = session.scalars(
            select(ResearchStrategyGraphRecord)
            .where(ResearchStrategyGraphRecord.family_id == family.id)
            .order_by(ResearchStrategyGraphRecord.graph_version)
        ).all()
        return {
            "family": family.to_dict(),
            "protocol": protocol.to_dict(private=True),
            "strategy_graphs": [row.to_dict() for row in graphs],
        }


def create_strategy_graph(
    graph: TypedStrategyGraph,
    *,
    search_attempt_id: str,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    """Persist one immutable graph after its creation consumed search budget."""

    compiled = compile_typed_strategy_graph(graph)
    with db.session() as session:
        family = session.scalar(
            select(ResearchFamilyRecord)
            .where(ResearchFamilyRecord.id == graph.family_id)
            .with_for_update()
        )
        if family is None:
            raise KeyError(f"research family not found: {graph.family_id}")
        if family.status != "open":
            raise ValueError("research_family_not_open_for_strategy_generation")
        protocol = session.get(ResearchProtocolRecord, family.protocol_id)
        if protocol is None or protocol.protocol_hash != graph.protocol_hash:
            raise ValueError("typed_strategy_graph_protocol_mismatch")
        attempt = session.get(ResearchAttemptRecord, search_attempt_id)
        if (
            attempt is None
            or attempt.family_id != family.id
            or attempt.status != "registered"
            or dict(attempt.trial_manifest.get("trial_inputs") or {}).get(
                "strategy_graph_hash"
            )
            != graph.graph_hash
        ):
            raise ValueError("typed_strategy_graph_search_attempt_invalid")
        parent_ids = set(graph.parent_graph_ids)
        existing_parents = set(
            session.scalars(
                select(ResearchStrategyGraphRecord.id).where(
                    ResearchStrategyGraphRecord.family_id == family.id,
                    ResearchStrategyGraphRecord.id.in_(parent_ids),
                )
            ).all()
        ) if parent_ids else set()
        if existing_parents != parent_ids:
            raise ValueError("typed_strategy_graph_parent_outside_family")
        existing = session.get(ResearchStrategyGraphRecord, graph.graph_id)
        if existing is not None:
            if existing.graph_hash != graph.graph_hash:
                raise ValueError("typed_strategy_graph_id_conflict")
            return existing.to_dict()
        version = int(
            session.scalar(
                select(func.max(ResearchStrategyGraphRecord.graph_version)).where(
                    ResearchStrategyGraphRecord.family_id == family.id
                )
            )
            or 0
        ) + 1
        record = ResearchStrategyGraphRecord(
            id=graph.graph_id,
            protocol_id=family.protocol_id,
            family_id=family.id,
            search_attempt_id=attempt.id,
            parent_graph_ids=list(graph.parent_graph_ids),
            graph_version=version,
            graph_hash=graph.graph_hash,
            compiled_hash=compiled.compiled_hash,
            manifest=graph.to_dict(),
            created_by=actor_id,
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="TYPED_STRATEGY_GRAPH_CREATED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"strategy-graph:{request_id}",
            payload={
                "graph_id": graph.graph_id,
                "graph_hash": graph.graph_hash,
                "compiled_hash": compiled.compiled_hash,
                "search_attempt_id": attempt.id,
                "parent_graph_ids": list(graph.parent_graph_ids),
            },
        )
        return record.to_dict()


def register_attempt(
    *,
    family_id: str,
    request_id: str,
    dataset_role: str,
    trial_inputs: Mapping[str, Any],
    estimated_runtime_seconds: float,
    estimated_compute_units: float,
    actor_id: str,
    actor_role: str,
) -> dict[str, Any]:
    normalized_role = DatasetRole(str(dataset_role or "").strip().lower())
    if normalized_role is DatasetRole.HOLDOUT:
        raise ValueError("holdout trials require the one-use holdout authority")
    with db.session() as session:
        family = session.scalar(
            select(ResearchFamilyRecord)
            .where(ResearchFamilyRecord.id == family_id)
            .with_for_update()
        )
        if family is None:
            raise KeyError(f"research family not found: {family_id}")
        if family.status != "open":
            raise ValueError("research_family_not_open_for_trials")
        existing = session.scalar(
            select(ResearchAttemptRecord).where(
                ResearchAttemptRecord.family_id == family.id,
                ResearchAttemptRecord.request_id == request_id,
            )
        )
        if existing is not None:
            return existing.to_dict()
        protocol_record = session.get(ResearchProtocolRecord, family.protocol_id)
        if protocol_record is None:
            raise RuntimeError("research_family_protocol_missing")
        protocol = ScientificProtocol.from_dict(protocol_record.private_manifest)
        prior_attempts = session.scalars(
            select(ResearchAttemptRecord).where(
                ResearchAttemptRecord.family_id == family.id
            )
        ).all()
        prior_by_id = {row.id: row for row in prior_attempts}
        lineage = {
            key: tuple(str(value).strip() for value in trial_inputs.get(key, ()) or ())
            for key in ("parent_attempt_ids", "influenced_by_attempt_ids")
        }
        for key, identifiers in lineage.items():
            if len(set(identifiers)) != len(identifiers) or any(
                not identifier for identifier in identifiers
            ):
                raise ValueError(f"{key} must contain unique non-empty attempt IDs")
            missing = sorted(set(identifiers) - set(prior_by_id))
            if missing:
                raise ValueError(f"{key} references attempts outside the family")
        for identifier in lineage["influenced_by_attempt_ids"]:
            source = prior_by_id[identifier]
            if source.dataset_role != DatasetRole.VALIDATION.value or source.status != "completed":
                raise ValueError(
                    "validation feedback lineage requires completed validation attempts"
                )
        feedback_used = sum(
            len(
                tuple(
                    dict(row.trial_manifest.get("lineage") or {}).get(
                        "influenced_by_attempt_ids"
                    )
                    or ()
                )
            )
            for row in prior_attempts
        )
        feedback_requested = len(lineage["influenced_by_attempt_ids"])
        if (
            feedback_used + feedback_requested
            > protocol.budget.max_validation_feedback_uses
        ):
            raise ValueError("validation_feedback_budget_exhausted")
        attempt_count = int(
            session.scalar(
                select(func.count()).select_from(ResearchAttemptRecord).where(
                    ResearchAttemptRecord.family_id == family.id
                )
            )
            or 0
        )
        estimated_runtime_used, estimated_compute_used = session.execute(
            select(
                func.coalesce(func.sum(ResearchAttemptRecord.estimated_runtime_seconds), 0.0),
                func.coalesce(func.sum(ResearchAttemptRecord.estimated_compute_units), 0.0),
            ).where(ResearchAttemptRecord.family_id == family.id)
        ).one()
        runtime = float(estimated_runtime_seconds)
        compute = float(estimated_compute_units)
        if runtime < 0.0 or compute < 0.0:
            raise ValueError("attempt budget estimates must be non-negative")
        budget_blockers = []
        if attempt_count >= protocol.budget.max_attempts:
            budget_blockers.append("attempt_budget_exhausted")
        if float(estimated_runtime_used or 0.0) + runtime > protocol.budget.max_runtime_seconds:
            budget_blockers.append("runtime_budget_exhausted")
        if float(estimated_compute_used or 0.0) + compute > protocol.budget.max_compute_units:
            budget_blockers.append("compute_budget_exhausted")
        if budget_blockers:
            raise ValueError("research_search_budget_exhausted: " + ",".join(budget_blockers))
        assignment = protocol.assignment(normalized_role)
        ordinal = attempt_count + 1
        manifest = {
            "schema_version": "research_trial_manifest.v1",
            "protocol_id": protocol.protocol_id,
            "protocol_hash": protocol.protocol_hash,
            "family_id": family.id,
            "family_hash": family.family_hash,
            "attempt_ordinal": ordinal,
            "dataset_role": normalized_role.value,
            "dataset_binding": assignment.to_private_dict(),
            "trial_inputs": dict(trial_inputs),
            "lineage": {key: list(value) for key, value in lineage.items()},
            "estimated_runtime_seconds": runtime,
            "estimated_compute_units": compute,
        }
        manifest_hash = _stable_hash(manifest)
        record = ResearchAttemptRecord(
            id=f"attempt:{uuid.uuid4()}",
            protocol_id=protocol.protocol_id,
            family_id=family.id,
            attempt_ordinal=ordinal,
            request_id=request_id,
            dataset_role=normalized_role.value,
            status="registered",
            trial_manifest_hash=manifest_hash,
            trial_manifest=manifest,
            estimated_runtime_seconds=runtime,
            estimated_compute_units=compute,
            created_by=actor_id,
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="ATTEMPT_REGISTERED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"attempt:{request_id}",
            payload={"attempt_id": record.id, "trial_manifest_hash": manifest_hash, "dataset_role": normalized_role.value},
        )
        return record.to_dict()


def record_rejected_proposal(
    *,
    family_id: str,
    request_id: str,
    actor_id: str,
    actor_role: str,
    reason: str,
    proposal: Mapping[str, Any],
) -> dict[str, Any]:
    """Retain a rejected search decision without admitting it as a trial."""

    with db.session() as session:
        family = session.scalar(
            select(ResearchFamilyRecord)
            .where(ResearchFamilyRecord.id == _required(family_id, field="family_id"))
            .with_for_update()
        )
        if family is None:
            raise KeyError(f"research family not found: {family_id}")
        event = _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="TRIAL_PROPOSAL_REJECTED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"trial-rejected:{request_id}",
            payload={
                "reason": _required(reason, field="reason"),
                "proposal_hash": _stable_hash(dict(proposal)),
            },
        )
        return event.to_dict()


def complete_attempt(
    *,
    attempt_id: str,
    status: str,
    result_evidence: Mapping[str, Any],
    error: str | None,
    actual_runtime_seconds: float,
    actual_compute_units: float,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in TERMINAL_ATTEMPT_STATUSES:
        raise ValueError("attempt status must be completed, failed, abandoned, or invalid")
    with db.session() as session:
        attempt = session.scalar(
            select(ResearchAttemptRecord)
            .where(ResearchAttemptRecord.id == attempt_id)
            .with_for_update()
        )
        if attempt is None:
            raise KeyError(f"research attempt not found: {attempt_id}")
        if attempt.status in TERMINAL_ATTEMPT_STATUSES:
            return attempt.to_dict()
        if normalized_status == "completed" and attempt.dataset_role == DatasetRole.VALIDATION.value:
            protocol_record = session.get(ResearchProtocolRecord, attempt.protocol_id)
            if protocol_record is None:
                raise RuntimeError("research_attempt_protocol_missing")
            protocol = ScientificProtocol.from_dict(protocol_record.private_manifest)
            expected = {
                "walk_forward_fold_count": protocol.walk_forward.fold_count,
                "purge_bars": protocol.leakage.purge_bars,
                "embargo_bars": protocol.leakage.embargo_bars,
                "context_only_warmup": True,
                "flat_at_scoring_start": True,
                "no_pending_orders_at_scoring_start": True,
                "signals_not_before_scoring_start": True,
            }
            disagreements = [
                key
                for key, value in expected.items()
                if result_evidence.get(key) != value
            ]
            if disagreements:
                raise ValueError(
                    "validation_boundary_evidence_incomplete: "
                    + ",".join(sorted(disagreements))
                )
            result_quality = str(
                result_evidence.get("execution_quality_class") or ""
            ).strip().upper()
            if (
                result_quality not in _EXECUTION_QUALITY_RANK
                or _EXECUTION_QUALITY_RANK[result_quality]
                < _EXECUTION_QUALITY_RANK[
                    protocol.minimum_execution_quality_class
                ]
            ):
                raise ValueError(
                    "validation_execution_quality_below_protocol_minimum"
                )
            passed_stresses = {
                str(value).strip()
                for value in result_evidence.get("execution_stress_ids_passed") or ()
                if str(value).strip()
            }
            missing_stresses = sorted(
                set(protocol.execution_stress_ids) - passed_stresses
            )
            if missing_stresses:
                raise ValueError(
                    "validation_execution_stresses_incomplete: "
                    + ",".join(missing_stresses)
                )
            metric_results = dict(result_evidence.get("metric_results") or {})
            required_metrics = {
                protocol.primary_metric,
                *protocol.secondary_metrics,
                *protocol.safety_metrics,
            }
            missing_metrics = sorted(required_metrics - set(metric_results))
            if missing_metrics:
                raise ValueError(
                    "validation_metric_results_incomplete: "
                    + ",".join(missing_metrics)
                )
            for field, minimum in (
                ("sample_count", protocol.minimum_sample_count),
                ("trade_count", protocol.minimum_trade_count),
                ("calendar_days", protocol.minimum_calendar_days),
            ):
                if int(result_evidence.get(field) or 0) < minimum:
                    raise ValueError(f"validation_{field}_below_protocol_minimum")
            if float(result_evidence.get("exposure") or 0.0) < protocol.minimum_exposure:
                raise ValueError("validation_exposure_below_protocol_minimum")
        attempt.status = normalized_status
        attempt.result_evidence = dict(result_evidence)
        attempt.error = str(error or "").strip() or None
        attempt.actual_runtime_seconds = float(actual_runtime_seconds)
        attempt.actual_compute_units = float(actual_compute_units)
        if attempt.actual_runtime_seconds < 0.0 or attempt.actual_compute_units < 0.0:
            raise ValueError("attempt actual usage must be non-negative")
        attempt.finished_at = _now()
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=attempt.family_id,
            event_type=f"ATTEMPT_{normalized_status.upper()}",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"attempt-complete:{attempt.id}:{request_id}",
            payload={"attempt_id": attempt.id, "status": normalized_status, "result_evidence_hash": _stable_hash(dict(result_evidence))},
        )
        return attempt.to_dict()


def freeze_candidate(
    candidate: CandidateSnapshot,
    *,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        family = session.scalar(
            select(ResearchFamilyRecord)
            .where(ResearchFamilyRecord.id == candidate.family_id)
            .with_for_update()
        )
        if family is None:
            raise KeyError(f"research family not found: {candidate.family_id}")
        protocol = session.get(ResearchProtocolRecord, family.protocol_id)
        if protocol is None or protocol.protocol_hash != candidate.protocol_hash:
            raise ValueError("candidate_protocol_hash_mismatch")
        protocol_contract = ScientificProtocol.from_dict(protocol.private_manifest)
        expected_dataset_hashes = tuple(
            sorted(
                protocol_contract.assignment(role).dataset_hash
                for role in (DatasetRole.TRAIN, DatasetRole.VALIDATION)
            )
        )
        if candidate.research_dataset_hashes != expected_dataset_hashes:
            raise ValueError("candidate_research_dataset_hashes_mismatch")
        attempt = session.get(ResearchAttemptRecord, candidate.source_attempt_id)
        if attempt is None or attempt.family_id != family.id:
            raise ValueError("candidate_source_attempt_invalid")
        if attempt.status != "completed" or attempt.dataset_role != DatasetRole.VALIDATION.value:
            raise ValueError("candidate requires a completed validation attempt")
        result = dict(attempt.result_evidence or {})
        pinned = {
            "strategy_artifact_hash": candidate.strategy_artifact_hash,
            "parameter_artifact_hash": candidate.parameter_artifact_hash,
            "execution_model_hash": candidate.execution_model_hash,
            "metric_contract_hash": candidate.metric_contract_hash,
        }
        disagreements = [
            key for key, value in pinned.items() if result.get(key) != value
        ]
        if disagreements:
            raise ValueError(
                "candidate_validation_artifacts_mismatch: "
                + ",".join(sorted(disagreements))
            )
        prior = session.get(ResearchCandidateRecord, candidate.candidate_id)
        if prior is not None:
            if prior.candidate_hash != candidate.candidate_hash:
                raise ValueError("research_candidate_id_conflict")
            return prior.to_dict()
        if session.scalar(select(ResearchHoldoutUseRecord.id).where(ResearchHoldoutUseRecord.family_id == family.id)):
            raise ValueError("candidate cannot change after holdout reservation")
        version = int(
            session.scalar(
                select(func.max(ResearchCandidateRecord.candidate_version)).where(
                    ResearchCandidateRecord.family_id == family.id
                )
            )
            or 0
        ) + 1
        record = ResearchCandidateRecord(
            id=candidate.candidate_id,
            protocol_id=family.protocol_id,
            family_id=family.id,
            source_attempt_id=attempt.id,
            candidate_version=version,
            candidate_hash=candidate.candidate_hash,
            manifest=candidate.to_dict(),
            frozen_by=actor_id,
            created_at=_now(),
        )
        session.add(record)
        family.current_candidate_id = record.id
        family.status = "candidate_frozen"
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="CANDIDATE_FROZEN",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"candidate:{request_id}",
            payload={"candidate_id": record.id, "candidate_hash": record.candidate_hash, "candidate_version": version},
        )
        return record.to_dict()


def reserve_holdout(
    *,
    family_id: str,
    candidate_id: str,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> tuple[dict[str, Any], str]:
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    try:
        with db.session() as session:
            family = session.scalar(
                select(ResearchFamilyRecord)
                .where(ResearchFamilyRecord.id == family_id)
                .with_for_update()
            )
            if family is None:
                raise KeyError(f"research family not found: {family_id}")
            existing = session.scalar(
                select(ResearchHoldoutUseRecord).where(
                    ResearchHoldoutUseRecord.family_id == family.id
                )
            )
            if existing is not None:
                if existing.request_id == request_id and existing.candidate_id == candidate_id:
                    return existing.to_dict(), ""
                raise ValueError("family_holdout_already_used")
            candidate = session.get(ResearchCandidateRecord, candidate_id)
            if candidate is None or candidate.family_id != family.id:
                raise ValueError("holdout candidate is not the frozen family candidate")
            if family.current_candidate_id != candidate.id or family.status != "family_closed":
                raise ValueError("family is not eligible for holdout reservation")
            if candidate.frozen_by == actor_id:
                raise ValueError("candidate freezer cannot reserve its own holdout")
            protocol = session.get(ResearchProtocolRecord, family.protocol_id)
            if protocol is None:
                raise RuntimeError("research_family_protocol_missing")
            record = ResearchHoldoutUseRecord(
                id=f"holdout:{uuid.uuid4()}",
                protocol_id=protocol.id,
                family_id=family.id,
                candidate_id=candidate.id,
                request_id=request_id,
                status="reserved",
                blindness_class=protocol.blindness_class,
                reservation_token_hash=token_hash,
                reserved_by=actor_id,
                feedback_released=False,
                reserved_at=_now(),
            )
            session.add(record)
            family.status = "holdout_reserved"
            session.flush()
            _event(
                session,
                aggregate_type="family",
                aggregate_id=family.id,
                event_type="HOLDOUT_RESERVED",
                actor_id=actor_id,
                actor_role=actor_role,
                request_id=request_id,
                idempotency_key=f"holdout:{request_id}",
                payload={"holdout_use_id": record.id, "candidate_id": candidate.id, "blindness_class": protocol.blindness_class},
            )
            return record.to_dict(), token
    except IntegrityError as exc:
        raise ValueError("family_holdout_already_used") from exc


def execute_holdout_internal(
    *,
    holdout_use_id: str,
    reservation_token: str,
    result_evidence: Mapping[str, Any],
    executor_actor: str,
    request_id: str,
) -> dict[str, Any]:
    """Record internally produced holdout evidence; never returns the dataset binding."""

    supplied_hash = hashlib.sha256(_required(reservation_token, field="reservation_token").encode("utf-8")).hexdigest()
    with db.session() as session:
        holdout = session.scalar(
            select(ResearchHoldoutUseRecord)
            .where(ResearchHoldoutUseRecord.id == holdout_use_id)
            .with_for_update()
        )
        if holdout is None:
            raise KeyError(f"holdout use not found: {holdout_use_id}")
        if not secrets.compare_digest(holdout.reservation_token_hash, supplied_hash):
            raise ValueError("holdout_reservation_token_invalid")
        if holdout.status == "completed":
            return holdout.to_dict(include_result=True)
        if holdout.status != "reserved":
            raise ValueError("holdout_use_not_reserved")
        family = session.get(ResearchFamilyRecord, holdout.family_id)
        if family is None:
            raise RuntimeError("holdout_family_missing")
        protocol_record = session.get(ResearchProtocolRecord, family.protocol_id)
        if protocol_record is None:
            raise RuntimeError("holdout_protocol_missing")
        protocol = ScientificProtocol.from_dict(protocol_record.private_manifest)
        result_quality = str(
            result_evidence.get("execution_quality_class") or ""
        ).strip().upper()
        if (
            result_quality not in _EXECUTION_QUALITY_RANK
            or _EXECUTION_QUALITY_RANK[result_quality]
            < _EXECUTION_QUALITY_RANK[protocol.minimum_execution_quality_class]
        ):
            raise ValueError("holdout_execution_quality_below_protocol_minimum")
        passed_stresses = {
            str(value).strip()
            for value in result_evidence.get("execution_stress_ids_passed") or ()
            if str(value).strip()
        }
        missing_stresses = sorted(
            set(protocol.execution_stress_ids) - passed_stresses
        )
        if missing_stresses:
            raise ValueError(
                "holdout_execution_stresses_incomplete: "
                + ",".join(missing_stresses)
            )
        metric_results = dict(result_evidence.get("metric_results") or {})
        required_metrics = {
            protocol.primary_metric,
            *protocol.secondary_metrics,
            *protocol.safety_metrics,
        }
        missing_metrics = sorted(required_metrics - set(metric_results))
        if missing_metrics:
            raise ValueError(
                "holdout_metric_results_incomplete: " + ",".join(missing_metrics)
            )
        for field, minimum in (
            ("sample_count", protocol.minimum_sample_count),
            ("trade_count", protocol.minimum_trade_count),
            ("calendar_days", protocol.minimum_calendar_days),
        ):
            if int(result_evidence.get(field) or 0) < minimum:
                raise ValueError(f"holdout_{field}_below_protocol_minimum")
        if float(result_evidence.get("exposure") or 0.0) < protocol.minimum_exposure:
            raise ValueError("holdout_exposure_below_protocol_minimum")
        holdout.status = "completed"
        holdout.executor_actor = _required(executor_actor, field="executor_actor")
        holdout.result_evidence = dict(result_evidence)
        holdout.completed_at = _now()
        family.status = "holdout_completed"
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="HOLDOUT_COMPLETED_SEALED",
            actor_id=executor_actor,
            actor_role="holdout_executor",
            request_id=request_id,
            idempotency_key=f"holdout-complete:{request_id}",
            payload={"holdout_use_id": holdout.id, "result_evidence_hash": _stable_hash(dict(result_evidence))},
        )
        return holdout.to_dict(include_result=False)


def close_family(
    *,
    family_id: str,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        family = session.scalar(
            select(ResearchFamilyRecord)
            .where(ResearchFamilyRecord.id == family_id)
            .with_for_update()
        )
        if family is None:
            raise KeyError(f"research family not found: {family_id}")
        if family.status == "family_closed":
            return family.to_dict()
        if family.status != "candidate_frozen" or not family.current_candidate_id:
            raise ValueError("family requires a frozen candidate before closure")
        nonterminal = int(
            session.scalar(
                select(func.count()).select_from(ResearchAttemptRecord).where(
                    ResearchAttemptRecord.family_id == family.id,
                    ResearchAttemptRecord.status.not_in(TERMINAL_ATTEMPT_STATUSES),
                )
            )
            or 0
        )
        if nonterminal:
            raise ValueError("family_has_unaccounted_attempts")
        family.status = "family_closed"
        family.feedback_released = False
        family.closed_at = _now()
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="FAMILY_CLOSED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"family-close:{request_id}",
            payload={"candidate_id": family.current_candidate_id},
        )
        return family.to_dict()


def release_holdout_feedback(
    *,
    family_id: str,
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    """Release final metrics only after an immutable certificate exists."""

    with db.session() as session:
        family = session.scalar(
            select(ResearchFamilyRecord)
            .where(ResearchFamilyRecord.id == family_id)
            .with_for_update()
        )
        if family is None:
            raise KeyError(f"research family not found: {family_id}")
        holdout = session.scalar(
            select(ResearchHoldoutUseRecord).where(
                ResearchHoldoutUseRecord.family_id == family.id
            )
        )
        certificate = session.scalar(
            select(ResearchCertificateRecord.id).where(
                ResearchCertificateRecord.family_id == family.id
            )
        )
        if holdout is None or holdout.status != "completed" or certificate is None:
            raise ValueError("certified completed holdout required before feedback release")
        if family.feedback_released and holdout.feedback_released:
            return {
                "family": family.to_dict(),
                "holdout": holdout.to_dict(include_result=True),
            }
        family.feedback_released = True
        holdout.feedback_released = True
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="HOLDOUT_FEEDBACK_RELEASED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"feedback-release:{request_id}",
            payload={"holdout_use_id": holdout.id},
        )
        return {
            "family": family.to_dict(),
            "holdout": holdout.to_dict(include_result=True),
        }


def create_certificate(
    *,
    protocol_id: str,
    family_id: str,
    candidate_id: str,
    scientific_quality_class: str,
    status: str,
    evidence: Mapping[str, Any],
    actor_id: str,
    actor_role: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        family = session.get(ResearchFamilyRecord, family_id)
        candidate = session.get(ResearchCandidateRecord, candidate_id)
        if family is None or candidate is None or candidate.family_id != family.id:
            raise ValueError("certificate family/candidate lineage invalid")
        material = {
            "protocol_id": protocol_id,
            "family_id": family_id,
            "candidate_id": candidate_id,
            "candidate_hash": candidate.candidate_hash,
            "scientific_quality_class": scientific_quality_class,
            "status": status,
            "evidence": dict(evidence),
        }
        certificate_hash = _stable_hash(material)
        existing = session.scalar(
            select(ResearchCertificateRecord).where(
                ResearchCertificateRecord.certificate_hash == certificate_hash
            )
        )
        if existing is not None:
            return existing.to_dict()
        record = ResearchCertificateRecord(
            id=f"science_certificate:{uuid.uuid4()}",
            protocol_id=protocol_id,
            family_id=family_id,
            candidate_id=candidate_id,
            scientific_quality_class=scientific_quality_class,
            status=status,
            evidence=dict(evidence),
            certificate_hash=certificate_hash,
            created_by=actor_id,
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        _event(
            session,
            aggregate_type="family",
            aggregate_id=family.id,
            event_type="SCIENTIFIC_CERTIFICATE_CREATED",
            actor_id=actor_id,
            actor_role=actor_role,
            request_id=request_id,
            idempotency_key=f"certificate:{request_id}",
            payload={"certificate_id": record.id, "certificate_hash": certificate_hash, "scientific_quality_class": scientific_quality_class},
        )
        return record.to_dict()


def family_evidence(family_id: str, *, private: bool = False) -> dict[str, Any]:
    with db.session() as session:
        family = session.get(ResearchFamilyRecord, _required(family_id, field="family_id"))
        if family is None:
            raise KeyError(f"research family not found: {family_id}")
        protocol = session.get(ResearchProtocolRecord, family.protocol_id)
        attempts = session.scalars(
            select(ResearchAttemptRecord)
            .where(ResearchAttemptRecord.family_id == family.id)
            .order_by(ResearchAttemptRecord.attempt_ordinal)
        ).all()
        candidates = session.scalars(
            select(ResearchCandidateRecord)
            .where(ResearchCandidateRecord.family_id == family.id)
            .order_by(ResearchCandidateRecord.candidate_version)
        ).all()
        strategy_graphs = session.scalars(
            select(ResearchStrategyGraphRecord)
            .where(ResearchStrategyGraphRecord.family_id == family.id)
            .order_by(ResearchStrategyGraphRecord.graph_version)
        ).all()
        holdout = session.scalar(
            select(ResearchHoldoutUseRecord).where(ResearchHoldoutUseRecord.family_id == family.id)
        )
        certificates = session.scalars(
            select(ResearchCertificateRecord)
            .where(ResearchCertificateRecord.family_id == family.id)
            .order_by(ResearchCertificateRecord.created_at)
        ).all()
        events = session.scalars(
            select(ResearchAuthorityEventRecord)
            .where(
                ResearchAuthorityEventRecord.aggregate_type == "family",
                ResearchAuthorityEventRecord.aggregate_id == family.id,
            )
            .order_by(ResearchAuthorityEventRecord.event_seq)
        ).all()
        return {
            "schema_version": "research_family_evidence.v1",
            "family": family.to_dict(),
            "protocol": protocol.to_dict() if protocol is not None else None,
            "attempts": [row.to_dict() for row in attempts],
            "candidates": [row.to_dict() for row in candidates],
            "strategy_graphs": [row.to_dict() for row in strategy_graphs],
            "holdout": (
                holdout.to_dict(
                    include_result=bool(private or holdout.feedback_released)
                )
                if holdout is not None
                else None
            ),
            "certificates": [row.to_dict() for row in certificates],
            "events": [row.to_dict() for row in events],
        }


def protocol_private(protocol_id: str) -> ScientificProtocol:
    payload = get_protocol(protocol_id, private=True)
    return ScientificProtocol.from_dict(payload["manifest"])


def internal_holdout_binding(holdout_use_id: str, *, reservation_token: str) -> dict[str, Any]:
    """Resolve the sealed binding only for an in-process holdout executor."""

    supplied_hash = hashlib.sha256(_required(reservation_token, field="reservation_token").encode("utf-8")).hexdigest()
    with db.session() as session:
        holdout = session.get(ResearchHoldoutUseRecord, holdout_use_id)
        if holdout is None:
            raise KeyError(f"holdout use not found: {holdout_use_id}")
        if holdout.status != "reserved" or not secrets.compare_digest(holdout.reservation_token_hash, supplied_hash):
            raise ValueError("holdout_reservation_token_invalid")
        protocol = session.get(ResearchProtocolRecord, holdout.protocol_id)
        if protocol is None:
            raise RuntimeError("holdout_protocol_missing")
        manifest = ScientificProtocol.from_dict(protocol.private_manifest)
        return manifest.assignment(DatasetRole.HOLDOUT).to_private_dict()


__all__ = [
    "TERMINAL_ATTEMPT_STATUSES",
    "close_family",
    "complete_attempt",
    "create_certificate",
    "create_family",
    "create_strategy_graph",
    "create_protocol",
    "execute_holdout_internal",
    "family_evidence",
    "family_context",
    "freeze_candidate",
    "get_protocol",
    "internal_holdout_binding",
    "protocol_private",
    "register_attempt",
    "record_rejected_proposal",
    "release_holdout_feedback",
    "reserve_holdout",
]

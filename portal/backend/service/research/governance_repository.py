"""Transactional append-only authority for offline research promotion."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any, Mapping

from sqlalchemy import func, select

from portal.backend.db.models import (
    ResearchAttemptRecord,
    ResearchCandidateRecord,
    ResearchCertificateRecord,
    ResearchFamilyRecord,
    ResearchGovernanceCaseRecord,
    ResearchGovernanceDecisionRecord,
    ResearchGovernanceProposalRecord,
    ResearchHoldoutUseRecord,
    ResearchItemRecord,
    ResearchProtocolRecord,
)
from portal.backend.db.session import db
from research_governance import GovernanceState, validate_offline_transition

from .authority_repository import TERMINAL_ATTEMPT_STATUSES


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def create_case(
    *,
    case_id: str,
    observation_id: str,
    actor_id: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        normalized_request = _required(request_id, "request_id")
        prior_request = session.scalar(
            select(ResearchGovernanceCaseRecord).where(
                ResearchGovernanceCaseRecord.creation_request_id
                == normalized_request
            )
        )
        if prior_request is not None:
            if prior_request.observation_id != str(observation_id):
                raise ValueError("research_governance_case_request_conflict")
            return prior_request.to_dict()
        observation = session.get(
            ResearchItemRecord, _required(observation_id, "observation_id")
        )
        if observation is None or observation.kind != "observation":
            raise ValueError("governance case requires a persisted observation")
        existing = session.get(ResearchGovernanceCaseRecord, case_id)
        if existing is not None:
            if existing.observation_id != observation.id:
                raise ValueError("research_governance_case_id_conflict")
            return existing.to_dict()
        record = ResearchGovernanceCaseRecord(
            id=_required(case_id, "case_id"),
            current_state=GovernanceState.OBSERVATION.value,
            state_version=0,
            observation_id=observation.id,
            created_by=_required(actor_id, "actor_id"),
            creation_request_id=normalized_request,
            created_at=_now(),
            updated_at=_now(),
        )
        session.add(record)
        session.flush()
        # The case itself is immutable lineage; subsequent mutations require
        # proposal plus a separate decision record.
        return record.to_dict()


def propose_transition(
    *,
    proposal_id: str,
    case_id: str,
    expected_state_version: int,
    target_state: str,
    binding_updates: Mapping[str, Any],
    evidence_hashes: tuple[str, ...],
    rationale: str,
    proposed_by: str,
    proposed_role: str,
    request_id: str,
) -> dict[str, Any]:
    with db.session() as session:
        case = session.scalar(
            select(ResearchGovernanceCaseRecord)
            .where(ResearchGovernanceCaseRecord.id == case_id)
            .with_for_update()
        )
        if case is None:
            raise KeyError(f"research governance case not found: {case_id}")
        if int(expected_state_version) != int(case.state_version):
            raise ValueError("research_governance_stale_state_version")
        source, target = validate_offline_transition(case.current_state, target_state)
        hashes = tuple(sorted({_required(value, "evidence_hash") for value in evidence_hashes}))
        if not hashes:
            raise ValueError("governance transition requires immutable evidence hashes")
        bindings = {str(key): _required(value, f"binding.{key}") for key, value in binding_updates.items()}
        allowed_binding = {
            GovernanceState.HYPOTHESIS: {"hypothesis_id"},
            GovernanceState.PROTOCOL_PROPOSED: {"protocol_id", "family_id"},
            GovernanceState.CANDIDATE_NOMINATED: {"candidate_id"},
            GovernanceState.RESEARCH_CERTIFIED: {"certificate_id"},
        }.get(target, set())
        if set(bindings) != allowed_binding:
            raise ValueError(
                f"governance binding set invalid for {target.value}: {sorted(bindings)}"
            )
        normalized_request = _required(request_id, "request_id")
        prior_request = session.scalar(
            select(ResearchGovernanceProposalRecord).where(
                ResearchGovernanceProposalRecord.case_id == case.id,
                ResearchGovernanceProposalRecord.request_id
                == normalized_request,
            )
        )
        if prior_request is not None:
            same = (
                prior_request.source_state == source.value
                and prior_request.target_state == target.value
                and dict(prior_request.binding_updates or {}) == bindings
                and tuple(prior_request.evidence_hashes or ()) == hashes
                and prior_request.proposed_by == proposed_by
            )
            if not same:
                raise ValueError("research_governance_proposal_request_conflict")
            return prior_request.to_dict()
        material = {
            "schema_version": "offline_governance_transition_proposal.v1",
            "proposal_id": proposal_id,
            "case_id": case.id,
            "expected_state_version": int(case.state_version),
            "source_state": source.value,
            "target_state": target.value,
            "binding_updates": bindings,
            "evidence_hashes": list(hashes),
            "rationale": _required(rationale, "rationale"),
            "proposed_by": _required(proposed_by, "proposed_by"),
            "proposed_role": _required(proposed_role, "proposed_role"),
            "request_id": normalized_request,
        }
        proposal_hash = _stable_hash(material)
        existing = session.get(ResearchGovernanceProposalRecord, proposal_id)
        if existing is not None:
            if existing.proposal_hash != proposal_hash:
                raise ValueError("research_governance_proposal_id_conflict")
            return existing.to_dict()
        record = ResearchGovernanceProposalRecord(
            id=_required(proposal_id, "proposal_id"),
            case_id=case.id,
            expected_state_version=case.state_version,
            source_state=source.value,
            target_state=target.value,
            binding_updates=bindings,
            evidence_hashes=list(hashes),
            rationale=material["rationale"],
            proposed_by=material["proposed_by"],
            proposed_role=material["proposed_role"],
            request_id=material["request_id"],
            proposal_hash=proposal_hash,
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        return record.to_dict()


def _policy_evidence(
    session,
    *,
    case: ResearchGovernanceCaseRecord,
    proposal: ResearchGovernanceProposalRecord,
    authorized_by: str,
    authorized_role: str,
) -> dict[str, Any]:
    target = GovernanceState(proposal.target_state)
    bindings = dict(proposal.binding_updates or {})
    evidence: dict[str, Any] = {
        "schema_version": "offline_governance_policy_evidence.v1",
        "target_state": target.value,
        "evidence_hashes": list(proposal.evidence_hashes or []),
        "offline_ceiling": GovernanceState.RESEARCH_CERTIFIED.value,
        "operational_trading_authority": False,
    }
    if target is GovernanceState.HYPOTHESIS:
        hypothesis = session.get(ResearchItemRecord, bindings["hypothesis_id"])
        if hypothesis is None or hypothesis.kind != "hypothesis":
            raise ValueError("governance hypothesis binding is invalid")
        evidence["hypothesis_id"] = hypothesis.id
    elif target is GovernanceState.PROTOCOL_PROPOSED:
        protocol = session.get(ResearchProtocolRecord, bindings["protocol_id"])
        family = session.get(ResearchFamilyRecord, bindings["family_id"])
        if protocol is None or family is None or family.protocol_id != protocol.id:
            raise ValueError("governance protocol/family binding is invalid")
        evidence.update(protocol_hash=protocol.protocol_hash, family_hash=family.family_hash)
    elif target is GovernanceState.PROTOCOL_APPROVED:
        protocol = session.get(ResearchProtocolRecord, case.protocol_id)
        if protocol is None or protocol.status != "active":
            raise ValueError("governance protocol is not active")
        evidence["protocol_hash"] = protocol.protocol_hash
    elif target is GovernanceState.TRIALS_RUNNING:
        family = session.get(ResearchFamilyRecord, case.family_id)
        if family is None or family.status != "open":
            raise ValueError("governance family is not open for trials")
        evidence["family_hash"] = family.family_hash
    elif target is GovernanceState.EVIDENCE_PRODUCED:
        attempts = session.scalars(
            select(ResearchAttemptRecord).where(
                ResearchAttemptRecord.family_id == case.family_id
            )
        ).all()
        if not attempts or any(row.status not in TERMINAL_ATTEMPT_STATUSES for row in attempts):
            raise ValueError("governance search accounting is incomplete")
        evidence["attempt_ids"] = [row.id for row in attempts]
    elif target is GovernanceState.CANDIDATE_NOMINATED:
        candidate = session.get(ResearchCandidateRecord, bindings["candidate_id"])
        if candidate is None or candidate.family_id != case.family_id:
            raise ValueError("governance candidate binding is invalid")
        evidence["candidate_hash"] = candidate.candidate_hash
    elif target is GovernanceState.VALIDATION_PASSED:
        candidate = session.get(ResearchCandidateRecord, case.candidate_id)
        attempt = session.get(ResearchAttemptRecord, candidate.source_attempt_id) if candidate else None
        if attempt is None or attempt.status != "completed" or attempt.dataset_role != "validation":
            raise ValueError("governance candidate has not passed validation")
        if candidate.frozen_by == authorized_by:
            raise ValueError("candidate freezer cannot authorize validation promotion")
        evidence["validation_attempt_id"] = attempt.id
    elif target is GovernanceState.HOLDOUT_ELIGIBLE:
        family = session.get(ResearchFamilyRecord, case.family_id)
        existing = session.scalar(
            select(ResearchHoldoutUseRecord.id).where(
                ResearchHoldoutUseRecord.family_id == case.family_id
            )
        )
        if (
            family is None
            or family.status != "family_closed"
            or family.current_candidate_id != case.candidate_id
            or existing is not None
        ):
            raise ValueError("governance family is not holdout eligible")
        evidence["family_closed_at"] = family.closed_at.isoformat() if family.closed_at else None
    elif target is GovernanceState.HOLDOUT_EVALUATED:
        holdout = session.scalar(
            select(ResearchHoldoutUseRecord).where(
                ResearchHoldoutUseRecord.family_id == case.family_id
            )
        )
        if holdout is None or holdout.status != "completed" or holdout.candidate_id != case.candidate_id:
            raise ValueError("governance final holdout is not consumed")
        evidence["holdout_use_id"] = holdout.id
    elif target is GovernanceState.RESEARCH_CERTIFIED:
        certificate = session.get(ResearchCertificateRecord, bindings["certificate_id"])
        if (
            certificate is None
            or certificate.family_id != case.family_id
            or certificate.candidate_id != case.candidate_id
            or certificate.status != "qualified"
        ):
            raise ValueError("governance scientific certificate is not qualified")
        if certificate.created_by == authorized_by:
            raise ValueError("certificate issuer cannot authorize its own research promotion")
        evidence.update(
            certificate_hash=certificate.certificate_hash,
            scientific_quality_class=certificate.scientific_quality_class,
        )
    elif target is GovernanceState.RESEARCH_DEGRADED:
        if not proposal.evidence_hashes:
            raise ValueError("research degradation requires deterioration evidence")
        evidence["deterioration_evidence_retained"] = True
    elif target in {GovernanceState.REJECTED, GovernanceState.ARCHIVED}:
        evidence["terminal_reason_evidence_retained"] = True
    if authorized_role not in {"research_authority", "human_research_owner", "offline_policy_engine"}:
        raise ValueError("governance authorization role is not admitted")
    return evidence


def decide_transition(
    *,
    proposal_id: str,
    disposition: str,
    authorized_by: str,
    authorized_role: str,
    request_id: str,
) -> dict[str, Any]:
    normalized_disposition = str(disposition or "").strip().lower()
    if normalized_disposition not in {"approve", "reject"}:
        raise ValueError("governance disposition must be approve or reject")
    with db.session() as session:
        proposal = session.get(ResearchGovernanceProposalRecord, proposal_id)
        if proposal is None:
            raise KeyError(f"research governance proposal not found: {proposal_id}")
        case = session.scalar(
            select(ResearchGovernanceCaseRecord)
            .where(ResearchGovernanceCaseRecord.id == proposal.case_id)
            .with_for_update()
        )
        if case is None:
            raise RuntimeError("research_governance_case_missing")
        existing = session.scalar(
            select(ResearchGovernanceDecisionRecord).where(
                ResearchGovernanceDecisionRecord.proposal_id == proposal.id
            )
        )
        if existing is not None:
            return existing.to_dict()
        authorizer = _required(authorized_by, "authorized_by")
        if authorizer == proposal.proposed_by:
            raise ValueError("governance proposal cannot be self-authorized")
        if case.state_version != proposal.expected_state_version or case.current_state != proposal.source_state:
            raise ValueError("research_governance_proposal_is_stale")
        normalized_role = _required(authorized_role, "authorized_role")
        if normalized_role not in {
            "research_authority", "human_research_owner", "offline_policy_engine"
        }:
            raise ValueError("governance authorization role is not admitted")
        policy = (
            _policy_evidence(
                session,
                case=case,
                proposal=proposal,
                authorized_by=authorizer,
                authorized_role=normalized_role,
            )
            if normalized_disposition == "approve"
            else {
                "schema_version": "offline_governance_policy_evidence.v1",
                "target_state": proposal.target_state,
                "proposal_rejected": True,
                "operational_trading_authority": False,
            }
        )
        if normalized_disposition == "approve":
            target = GovernanceState(proposal.target_state)
            for key, value in dict(proposal.binding_updates or {}).items():
                setattr(case, key, value)
            case.current_state = target.value
            case.state_version += 1
            case.updated_at = _now()
            resulting_state = target.value
            resulting_version = case.state_version
        else:
            resulting_state = case.current_state
            resulting_version = case.state_version
        material = {
            "schema_version": "offline_governance_transition_decision.v1",
            "proposal_hash": proposal.proposal_hash,
            "disposition": normalized_disposition,
            "resulting_state": resulting_state,
            "resulting_state_version": resulting_version,
            "policy_evidence": policy,
            "authorized_by": authorizer,
            "authorized_role": authorized_role,
            "request_id": _required(request_id, "request_id"),
        }
        record = ResearchGovernanceDecisionRecord(
            id=f"governance_decision:{uuid.uuid4()}",
            proposal_id=proposal.id,
            case_id=case.id,
            disposition=normalized_disposition,
            resulting_state=resulting_state,
            resulting_state_version=resulting_version,
            policy_evidence=policy,
            authorized_by=authorizer,
            authorized_role=authorized_role,
            request_id=material["request_id"],
            decision_hash=_stable_hash(material),
            created_at=_now(),
        )
        session.add(record)
        session.flush()
        return record.to_dict()


def case_trail(case_id: str) -> dict[str, Any]:
    with db.session() as session:
        case = session.get(ResearchGovernanceCaseRecord, _required(case_id, "case_id"))
        if case is None:
            raise KeyError(f"research governance case not found: {case_id}")
        proposals = session.scalars(
            select(ResearchGovernanceProposalRecord)
            .where(ResearchGovernanceProposalRecord.case_id == case.id)
            .order_by(ResearchGovernanceProposalRecord.created_at, ResearchGovernanceProposalRecord.id)
        ).all()
        decisions = session.scalars(
            select(ResearchGovernanceDecisionRecord)
            .where(ResearchGovernanceDecisionRecord.case_id == case.id)
            .order_by(ResearchGovernanceDecisionRecord.created_at, ResearchGovernanceDecisionRecord.id)
        ).all()
        return {
            "schema_version": "offline_research_governance_trail.v1",
            "case": case.to_dict(),
            "proposals": [row.to_dict() for row in proposals],
            "decisions": [row.to_dict() for row in decisions],
            "operational_trading_authority": False,
            "maximum_state": GovernanceState.RESEARCH_CERTIFIED.value,
        }


__all__ = ["case_trail", "create_case", "decide_transition", "propose_transition"]

"""Application boundary for auditable offline research governance."""

from __future__ import annotations

import uuid
from typing import Any, Mapping

from . import governance_repository as repository


PROPOSER_ROLES = {"researcher", "research_agent", "research_authority"}
AUTHORIZER_ROLES = {
    "research_authority", "human_research_owner", "offline_policy_engine"
}


def _required(value: Any, field: str) -> str:
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


def _identity(payload: Mapping[str, Any], roles: set[str]) -> tuple[str, str, str]:
    actor_id = _required(payload.get("actor_id"), "actor_id")
    actor_role = _required(payload.get("actor_role"), "actor_role").lower()
    request_id = _required(payload.get("request_id"), "request_id")
    if actor_role not in roles:
        raise ValueError(f"offline governance role not authorized: {actor_role}")
    return actor_id, actor_role, request_id


def create_case(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, _actor_role, request_id = _identity(payload, PROPOSER_ROLES)
    return repository.create_case(
        case_id=str(
            payload.get("case_id")
            or _generated_id(
                "governance_case", actor_id=actor_id, request_id=request_id
            )
        ),
        observation_id=_required(payload.get("observation_id"), "observation_id"),
        actor_id=actor_id,
        request_id=request_id,
    )


def propose_transition(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, PROPOSER_ROLES)
    return repository.propose_transition(
        proposal_id=str(
            payload.get("proposal_id")
            or _generated_id(
                "governance_proposal", actor_id=actor_id, request_id=request_id
            )
        ),
        case_id=_required(payload.get("case_id"), "case_id"),
        expected_state_version=int(payload.get("expected_state_version")),
        target_state=_required(payload.get("target_state"), "target_state"),
        binding_updates=dict(payload.get("binding_updates") or {}),
        evidence_hashes=tuple(payload.get("evidence_hashes") or ()),
        rationale=_required(payload.get("rationale"), "rationale"),
        proposed_by=actor_id,
        proposed_role=actor_role,
        request_id=request_id,
    )


def decide_transition(payload: Mapping[str, Any]) -> dict[str, Any]:
    actor_id, actor_role, request_id = _identity(payload, AUTHORIZER_ROLES)
    return repository.decide_transition(
        proposal_id=_required(payload.get("proposal_id"), "proposal_id"),
        disposition=_required(payload.get("disposition"), "disposition"),
        authorized_by=actor_id,
        authorized_role=actor_role,
        request_id=request_id,
    )


def case_trail(case_id: str) -> dict[str, Any]:
    return repository.case_trail(case_id)


__all__ = [
    "AUTHORIZER_ROLES", "PROPOSER_ROLES", "case_trail", "create_case",
    "decide_transition", "propose_transition",
]

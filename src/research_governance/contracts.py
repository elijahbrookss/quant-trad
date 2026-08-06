"""Pure Phase 6 offline governance transition policy."""

from __future__ import annotations

from enum import Enum


GOVERNANCE_SCHEMA_VERSION = "offline_research_governance.v1"


class GovernanceState(str, Enum):
    OBSERVATION = "OBSERVATION"
    HYPOTHESIS = "HYPOTHESIS"
    PROTOCOL_PROPOSED = "PROTOCOL_PROPOSED"
    PROTOCOL_APPROVED = "PROTOCOL_APPROVED"
    TRIALS_RUNNING = "TRIALS_RUNNING"
    EVIDENCE_PRODUCED = "EVIDENCE_PRODUCED"
    CANDIDATE_NOMINATED = "CANDIDATE_NOMINATED"
    VALIDATION_PASSED = "VALIDATION_PASSED"
    HOLDOUT_ELIGIBLE = "HOLDOUT_ELIGIBLE"
    HOLDOUT_EVALUATED = "HOLDOUT_EVALUATED"
    RESEARCH_CERTIFIED = "RESEARCH_CERTIFIED"
    RESEARCH_DEGRADED = "RESEARCH_DEGRADED"
    REJECTED = "REJECTED"
    ARCHIVED = "ARCHIVED"


_FORBIDDEN_OPERATIONAL_STATES = {
    "SHADOW", "SHADOW_APPROVED", "PAPER", "PAPER_APPROVED",
    "CONTROLLED_LIVE", "CONTROLLED_LIVE_APPROVED", "LIVE", "DEPLOYED",
    "EXTERNAL_SUBMISSION", "CAPITAL_APPROVED",
}

_ALLOWED: dict[GovernanceState, frozenset[GovernanceState]] = {
    GovernanceState.OBSERVATION: frozenset({GovernanceState.HYPOTHESIS, GovernanceState.ARCHIVED}),
    GovernanceState.HYPOTHESIS: frozenset({GovernanceState.PROTOCOL_PROPOSED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.PROTOCOL_PROPOSED: frozenset({GovernanceState.PROTOCOL_APPROVED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.PROTOCOL_APPROVED: frozenset({GovernanceState.TRIALS_RUNNING, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.TRIALS_RUNNING: frozenset({GovernanceState.EVIDENCE_PRODUCED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.EVIDENCE_PRODUCED: frozenset({GovernanceState.CANDIDATE_NOMINATED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.CANDIDATE_NOMINATED: frozenset({GovernanceState.VALIDATION_PASSED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.VALIDATION_PASSED: frozenset({GovernanceState.HOLDOUT_ELIGIBLE, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.HOLDOUT_ELIGIBLE: frozenset({GovernanceState.HOLDOUT_EVALUATED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.HOLDOUT_EVALUATED: frozenset({GovernanceState.RESEARCH_CERTIFIED, GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.RESEARCH_CERTIFIED: frozenset({GovernanceState.RESEARCH_DEGRADED, GovernanceState.ARCHIVED}),
    GovernanceState.RESEARCH_DEGRADED: frozenset({GovernanceState.REJECTED, GovernanceState.ARCHIVED}),
    GovernanceState.REJECTED: frozenset({GovernanceState.ARCHIVED}),
    GovernanceState.ARCHIVED: frozenset(),
}


def allowed_transition(source: GovernanceState, target: GovernanceState) -> bool:
    return target in _ALLOWED[source]


def validate_offline_transition(source: str, target: str) -> tuple[GovernanceState, GovernanceState]:
    source_text = str(source or "").strip().upper()
    target_text = str(target or "").strip().upper()
    if target_text in _FORBIDDEN_OPERATIONAL_STATES:
        raise ValueError(
            f"operational transition structurally closed for offline research: {target_text}"
        )
    try:
        source_state = GovernanceState(source_text)
        target_state = GovernanceState(target_text)
    except ValueError as exc:
        raise ValueError("unsupported offline research governance state") from exc
    if not allowed_transition(source_state, target_state):
        raise ValueError(
            f"offline governance transition not allowed: {source_state.value}->{target_state.value}"
        )
    return source_state, target_state

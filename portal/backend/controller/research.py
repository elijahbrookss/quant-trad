"""FastAPI routes for research memory and lightweight checks."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from ..service import research as research_service
from ..service.research import async_dispatch as research_async_dispatch
from ..service.research import authority as research_authority
from ..service.research import governance as research_governance


router = APIRouter()


class ResearchItemRequest(BaseModel):
    kind: str
    title: str
    status: str = "draft"
    body: Optional[str] = None
    instrument_id: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    payload: Dict[str, Any] = Field(default_factory=dict)


class ResearchLinkRequest(BaseModel):
    source_item_id: str
    target_type: str
    target_id: str
    relation: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ResearchCheckRunRequest(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None
    observation_id: Optional[str] = None
    observation: Optional[Dict[str, Any]] = None
    check_family: Optional[str] = None
    mode: Optional[str] = None
    dataset_id: Optional[str] = None
    scope: Dict[str, Any]
    detector: Dict[str, Any]
    outcomes: Dict[str, Any] = Field(default_factory=dict)
    inputs: List[Dict[str, Any]] = Field(default_factory=list)
    statistics: Dict[str, Any] = Field(default_factory=dict)
    assertions: List[Dict[str, Any]] = Field(default_factory=list)
    gap_policy: Optional[str] = None
    preparation: Dict[str, Any] = Field(default_factory=dict)
    freeze: Optional[bool] = None
    acquire_missing: Optional[bool] = None
    dataset_name: Optional[str] = None
    created_by: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class ResearchObservationFromCheckRequest(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None
    status: str = "active"
    tags: List[str] = Field(default_factory=list)


class ResearchCheckSweepRequest(BaseModel):
    title: Optional[str] = None
    check_family: str
    scope: Optional[Dict[str, Any]] = None
    scopes: Optional[List[Dict[str, Any]]] = None
    detector: Dict[str, Any]
    outcomes: Dict[str, Any] = Field(default_factory=dict)
    variants: List[Dict[str, Any]]
    ranking: Dict[str, Any]


class ResearchAuthorityRequest(BaseModel):
    actor_id: str
    actor_role: str
    request_id: str


class ScientificProtocolRequest(ResearchAuthorityRequest):
    protocol: Dict[str, Any]


class ResearchFamilyRequest(ResearchAuthorityRequest):
    protocol_id: str
    family_id: Optional[str] = None
    name: str


class ResearchAttemptRequest(ResearchAuthorityRequest):
    family_id: str
    dataset_role: str = Field(pattern="^(train|validation|holdout)$")
    trial_inputs: Dict[str, Any]
    estimated_runtime_seconds: float = Field(ge=0)
    estimated_compute_units: float = Field(ge=0)


class ResearchAttemptCompletionRequest(ResearchAuthorityRequest):
    status: str = Field(pattern="^(completed|failed|abandoned|invalid)$")
    result_evidence: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    actual_runtime_seconds: float = Field(default=0, ge=0)
    actual_compute_units: float = Field(default=0, ge=0)


class ResearchCandidateRequest(ResearchAuthorityRequest):
    candidate: Dict[str, Any]


class TypedStrategyGraphRequest(ResearchAuthorityRequest):
    family_id: str
    graph: Dict[str, Any]
    mutation_dimensions: List[str]
    influenced_by_attempt_ids: List[str] = Field(default_factory=list)
    estimated_runtime_seconds: float = Field(default=0, ge=0)
    estimated_compute_units: float = Field(default=0, ge=0)


class HoldoutReservationRequest(ResearchAuthorityRequest):
    family_id: str
    candidate_id: str


class FamilyAuthorityRequest(ResearchAuthorityRequest):
    robustness: Dict[str, Any] = Field(default_factory=dict)


class GovernanceCaseRequest(ResearchAuthorityRequest):
    case_id: Optional[str] = None
    observation_id: str


class GovernanceTransitionProposalRequest(ResearchAuthorityRequest):
    proposal_id: Optional[str] = None
    case_id: str
    expected_state_version: int = Field(ge=0)
    target_state: str
    binding_updates: Dict[str, Any] = Field(default_factory=dict)
    evidence_hashes: List[str]
    rationale: str


class GovernanceTransitionDecisionRequest(ResearchAuthorityRequest):
    disposition: str = Field(pattern="^(approve|reject)$")


def _model_payload(model: BaseModel) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _authority_call(handler, payload: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        return handler(payload)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/authority/protocols", status_code=201)
def create_scientific_protocol(body: ScientificProtocolRequest) -> Dict[str, Any]:
    return _authority_call(research_authority.create_protocol, _model_payload(body))


@router.get("/authority/protocols/{protocol_id}")
def get_scientific_protocol(protocol_id: str) -> Dict[str, Any]:
    try:
        return research_authority.get_protocol(protocol_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/authority/families", status_code=201)
def create_research_family(body: ResearchFamilyRequest) -> Dict[str, Any]:
    return _authority_call(research_authority.create_family, _model_payload(body))


@router.post("/authority/attempts", status_code=201)
def register_research_attempt(body: ResearchAttemptRequest) -> Dict[str, Any]:
    return _authority_call(research_authority.register_attempt, _model_payload(body))


@router.post("/authority/attempts/{attempt_id}/complete")
def complete_research_attempt(
    attempt_id: str,
    body: ResearchAttemptCompletionRequest,
) -> Dict[str, Any]:
    payload = _model_payload(body)
    payload["attempt_id"] = attempt_id
    return _authority_call(research_authority.complete_attempt, payload)


@router.post("/authority/candidates", status_code=201)
def freeze_research_candidate(body: ResearchCandidateRequest) -> Dict[str, Any]:
    return _authority_call(research_authority.freeze_candidate, _model_payload(body))


@router.post("/authority/strategy-graphs", status_code=201)
def create_typed_strategy_graph(body: TypedStrategyGraphRequest) -> Dict[str, Any]:
    return _authority_call(
        research_authority.create_typed_strategy_graph, _model_payload(body)
    )


@router.post("/authority/holdouts/reserve", status_code=201)
def reserve_research_holdout(body: HoldoutReservationRequest) -> Dict[str, Any]:
    return _authority_call(research_authority.reserve_holdout, _model_payload(body))


@router.post("/authority/families/{family_id}/close")
def close_research_family(
    family_id: str,
    body: FamilyAuthorityRequest,
) -> Dict[str, Any]:
    payload = _model_payload(body)
    payload["family_id"] = family_id
    return _authority_call(research_authority.close_family, payload)


@router.post("/authority/families/{family_id}/certify", status_code=201)
def certify_research_family(
    family_id: str,
    body: FamilyAuthorityRequest,
) -> Dict[str, Any]:
    payload = _model_payload(body)
    payload["family_id"] = family_id
    return _authority_call(research_authority.certify_family, payload)


@router.get("/authority/families/{family_id}/evidence")
def get_research_family_evidence(family_id: str) -> Dict[str, Any]:
    try:
        return research_authority.family_evidence(family_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/governance/cases", status_code=201)
def create_governance_case(body: GovernanceCaseRequest) -> Dict[str, Any]:
    return _authority_call(research_governance.create_case, _model_payload(body))


@router.post("/governance/proposals", status_code=201)
def propose_governance_transition(
    body: GovernanceTransitionProposalRequest,
) -> Dict[str, Any]:
    return _authority_call(
        research_governance.propose_transition, _model_payload(body)
    )


@router.post("/governance/proposals/{proposal_id}/decide", status_code=201)
def decide_governance_transition(
    proposal_id: str,
    body: GovernanceTransitionDecisionRequest,
) -> Dict[str, Any]:
    payload = _model_payload(body)
    payload["proposal_id"] = proposal_id
    return _authority_call(research_governance.decide_transition, payload)


@router.get("/governance/cases/{case_id}")
def get_governance_case(case_id: str) -> Dict[str, Any]:
    try:
        return research_governance.case_trail(case_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/items", status_code=201)
def create_research_item(body: ResearchItemRequest) -> Dict[str, Any]:
    try:
        return research_service.create_research_item(_model_payload(body))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/items")
def list_research_items(
    kind: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    symbol: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> Dict[str, Any]:
    try:
        items = research_service.list_research_items(
            kind=kind,
            status=status,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
        return {"schema_version": "research_item_list.v1", "items": items, "total": len(items)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/activity")
def get_research_activity(
    type: str = Query("checks_completed", alias="type"),
    days: int = Query(182, ge=1, le=366),
) -> Dict[str, Any]:
    """Return a complete zero-filled UTC activity projection."""

    try:
        return research_service.get_research_activity(
            activity_type=type,
            days=days,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/links", status_code=201)
def create_research_link(body: ResearchLinkRequest) -> Dict[str, Any]:
    try:
        return research_service.create_research_link(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/run", status_code=201)
def run_research_check(body: ResearchCheckRunRequest) -> Dict[str, Any]:
    try:
        return research_service.run_research_check(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/requirements")
def get_research_check_requirements(
    body: ResearchCheckRunRequest,
) -> Dict[str, Any]:
    try:
        return research_service.get_research_check_requirements(
            _model_payload(body)
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/prepare")
def prepare_research_check_evidence(
    body: ResearchCheckRunRequest,
) -> Dict[str, Any]:
    try:
        return research_service.prepare_research_check_evidence(
            _model_payload(body)
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/evaluate")
def evaluate_research_check(body: ResearchCheckRunRequest) -> Dict[str, Any]:
    try:
        return research_service.evaluate_research_check(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/sweep")
def sweep_research_checks(body: ResearchCheckSweepRequest) -> Dict[str, Any]:
    try:
        return research_service.sweep_research_checks(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/jobs/checks/run", status_code=202)
def dispatch_research_check(body: ResearchCheckRunRequest) -> Dict[str, Any]:
    try:
        return research_async_dispatch.dispatch_research_check_run(_model_payload(body))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/jobs/checks/sweep", status_code=202)
def dispatch_research_check_sweep(body: ResearchCheckSweepRequest) -> Dict[str, Any]:
    try:
        return research_async_dispatch.dispatch_research_check_sweep(_model_payload(body))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/jobs/{job_id}")
def get_research_job_status(job_id: str) -> Dict[str, Any]:
    try:
        return research_async_dispatch.get_research_job_status(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/jobs/{job_id}/result")
def get_research_job_result(job_id: str) -> Dict[str, Any]:
    try:
        return research_async_dispatch.get_research_job_result(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/checks/compare")
def compare_research_checks(left_check_id: str, right_check_id: str) -> Dict[str, Any]:
    try:
        return research_service.compare_research_checks(left_check_id, right_check_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/{check_id}/replay")
def replay_research_check(check_id: str) -> Dict[str, Any]:
    try:
        return research_service.replay_research_check(check_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/{check_id}/observations", status_code=201)
def create_observation_from_check(
    check_id: str,
    body: ResearchObservationFromCheckRequest,
) -> Dict[str, Any]:
    try:
        return research_service.create_observation_from_check_evidence(
            check_id, _model_payload(body)
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/evidence")
async def get_run_research_evidence(run_id: str) -> Dict[str, Any]:
    try:
        return await run_in_threadpool(
            research_service.get_run_research_evidence,
            run_id,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/items/{item_id}")
def get_research_item(item_id: str) -> Dict[str, Any]:
    try:
        return research_service.get_research_item(item_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/items/{item_id}/trail")
def get_research_trail(item_id: str) -> Dict[str, Any]:
    try:
        return research_service.get_research_trail(item_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/items/{item_id}/links")
def list_research_links(item_id: str, include_inbound: bool = True) -> Dict[str, Any]:
    try:
        links = research_service.list_research_links(item_id, include_inbound=include_inbound)
        return {"schema_version": "research_link_list.v1", "item_id": item_id, "items": links, "total": len(links)}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

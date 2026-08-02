"""FastAPI routes for research memory and lightweight checks."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..service import research as research_service
from ..service.research import async_dispatch as research_async_dispatch


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
    title: str
    body: Optional[str] = None
    observation_id: Optional[str] = None
    observation: Optional[Dict[str, Any]] = None
    check_family: Optional[str] = None
    scope: Dict[str, Any]
    detector: Dict[str, Any]
    outcomes: Dict[str, Any] = Field(default_factory=dict)
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


def _model_payload(model: BaseModel) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


@router.post("/items", status_code=201)
async def create_research_item(body: ResearchItemRequest) -> Dict[str, Any]:
    try:
        return research_service.create_research_item(_model_payload(body))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/items")
async def list_research_items(
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
async def get_research_activity(
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
async def create_research_link(body: ResearchLinkRequest) -> Dict[str, Any]:
    try:
        return research_service.create_research_link(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/run", status_code=201)
async def run_research_check(body: ResearchCheckRunRequest) -> Dict[str, Any]:
    try:
        return research_service.run_research_check(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/evaluate")
async def evaluate_research_check(body: ResearchCheckRunRequest) -> Dict[str, Any]:
    try:
        return research_service.evaluate_research_check(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/checks/sweep")
async def sweep_research_checks(body: ResearchCheckSweepRequest) -> Dict[str, Any]:
    try:
        return research_service.sweep_research_checks(_model_payload(body))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/jobs/checks/run", status_code=202)
async def dispatch_research_check(body: ResearchCheckRunRequest) -> Dict[str, Any]:
    try:
        return research_async_dispatch.dispatch_research_check_run(_model_payload(body))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/jobs/checks/sweep", status_code=202)
async def dispatch_research_check_sweep(body: ResearchCheckSweepRequest) -> Dict[str, Any]:
    try:
        return research_async_dispatch.dispatch_research_check_sweep(_model_payload(body))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/jobs/{job_id}")
async def get_research_job_status(job_id: str) -> Dict[str, Any]:
    try:
        return research_async_dispatch.get_research_job_status(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/jobs/{job_id}/result")
async def get_research_job_result(job_id: str) -> Dict[str, Any]:
    try:
        return research_async_dispatch.get_research_job_result(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/checks/compare")
async def compare_research_checks(left_check_id: str, right_check_id: str) -> Dict[str, Any]:
    try:
        return research_service.compare_research_checks(left_check_id, right_check_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/runs/{run_id}/evidence")
async def get_run_research_evidence(run_id: str) -> Dict[str, Any]:
    try:
        return research_service.get_run_research_evidence(run_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/items/{item_id}")
async def get_research_item(item_id: str) -> Dict[str, Any]:
    try:
        return research_service.get_research_item(item_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/items/{item_id}/trail")
async def get_research_trail(item_id: str) -> Dict[str, Any]:
    try:
        return research_service.get_research_trail(item_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/items/{item_id}/links")
async def list_research_links(item_id: str, include_inbound: bool = True) -> Dict[str, Any]:
    try:
        links = research_service.list_research_links(item_id, include_inbound=include_inbound)
        return {"schema_version": "research_link_list.v1", "item_id": item_id, "items": links, "total": len(links)}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

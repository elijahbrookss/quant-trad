"""FastAPI routes for research memory and lightweight checks."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..service import research as research_service


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


@router.get("/items/{item_id}")
async def get_research_item(item_id: str) -> Dict[str, Any]:
    try:
        return research_service.get_research_item(item_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/items/{item_id}/links")
async def list_research_links(item_id: str, include_inbound: bool = True) -> Dict[str, Any]:
    try:
        links = research_service.list_research_links(item_id, include_inbound=include_inbound)
        return {"schema_version": "research_link_list.v1", "item_id": item_id, "items": links, "total": len(links)}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

"""FastAPI router for strategy CRUD and signal orchestration."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..service import strategy_service


router = APIRouter()
logger = logging.getLogger(__name__)


class StrategyRuleOut(BaseModel):
    """Response model describing a stored strategy rule."""

    id: str
    name: str
    indicator_id: Optional[str] = None
    signal_type: str
    min_confidence: float
    action: str
    description: Optional[str] = None
    enabled: bool
    created_at: str
    updated_at: str


class StrategyOut(BaseModel):
    """Response model representing a strategy record."""

    id: str
    name: str
    description: Optional[str] = None
    symbols: List[str]
    timeframe: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    indicator_ids: List[str]
    rules: List[StrategyRuleOut]
    created_at: str
    updated_at: str


class StrategyCreateRequest(BaseModel):
    """Payload for creating a new strategy."""

    name: str
    symbols: List[str] = Field(default_factory=list)
    timeframe: str
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    indicator_ids: List[str] = Field(default_factory=list)


class StrategyUpdateRequest(BaseModel):
    """Payload for updating a strategy."""

    name: Optional[str] = None
    symbols: Optional[List[str]] = None
    timeframe: Optional[str] = None
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    indicator_ids: Optional[List[str]] = None


class StrategyRuleCreateRequest(BaseModel):
    """Payload for creating a strategy rule."""

    name: str
    signal_type: str
    action: str
    indicator_id: Optional[str] = None
    min_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    description: Optional[str] = None
    enabled: bool = True


class StrategyRuleUpdateRequest(BaseModel):
    """Payload for updating a strategy rule."""

    name: Optional[str] = None
    signal_type: Optional[str] = None
    action: Optional[str] = None
    indicator_id: Optional[str] = None
    min_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    description: Optional[str] = None
    enabled: Optional[bool] = None


class StrategySignalRequest(BaseModel):
    """Request payload for generating strategy signals."""

    start: str
    end: str
    interval: str
    symbol: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)


@router.get("/", response_model=List[StrategyOut])
async def list_strategies() -> List[Dict[str, Any]]:
    """Return all stored strategies."""

    return strategy_service.list_strategies()


@router.post("/", response_model=StrategyOut, status_code=201)
async def create_strategy(body: StrategyCreateRequest) -> Dict[str, Any]:
    """Create a new strategy record."""

    try:
        return strategy_service.create_strategy(
            body.name,
            symbols=body.symbols,
            timeframe=body.timeframe,
            description=body.description,
            datasource=body.datasource,
            exchange=body.exchange,
            indicator_ids=body.indicator_ids,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_create_failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/{strategy_id}", response_model=StrategyOut)
async def get_strategy(strategy_id: str) -> Dict[str, Any]:
    """Retrieve a single strategy."""

    try:
        return strategy_service.get_strategy(strategy_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/{strategy_id}", response_model=StrategyOut)
async def update_strategy(strategy_id: str, body: StrategyUpdateRequest) -> Dict[str, Any]:
    """Update an existing strategy."""

    try:
        payload = body.dict(exclude_unset=True)
        return strategy_service.update_strategy(strategy_id, **payload)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_update_failed")
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}", status_code=204)
async def delete_strategy(strategy_id: str) -> None:
    """Delete a strategy."""

    try:
        strategy_service.delete_strategy(strategy_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{strategy_id}/indicators/{indicator_id}", response_model=StrategyOut)
async def attach_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Attach an indicator to a strategy."""

    try:
        return strategy_service.register_indicator(strategy_id, indicator_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}/indicators/{indicator_id}", response_model=StrategyOut)
async def detach_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Detach an indicator from a strategy."""

    try:
        return strategy_service.unregister_indicator(strategy_id, indicator_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{strategy_id}/rules", response_model=StrategyOut, status_code=201)
async def create_rule(strategy_id: str, body: StrategyRuleCreateRequest) -> Dict[str, Any]:
    """Create a rule for a strategy."""

    try:
        return strategy_service.create_rule(
            strategy_id,
            name=body.name,
            signal_type=body.signal_type,
            action=body.action,
            indicator_id=body.indicator_id,
            min_confidence=body.min_confidence,
            description=body.description,
            enabled=body.enabled,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_rule_create_failed")
        raise HTTPException(400, str(exc)) from exc


@router.put("/{strategy_id}/rules/{rule_id}", response_model=StrategyOut)
async def update_rule(strategy_id: str, rule_id: str, body: StrategyRuleUpdateRequest) -> Dict[str, Any]:
    """Update an existing rule."""

    try:
        payload = body.dict(exclude_unset=True)
        return strategy_service.update_rule(strategy_id, rule_id, **payload)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}/rules/{rule_id}", response_model=StrategyOut)
async def delete_rule(strategy_id: str, rule_id: str) -> Dict[str, Any]:
    """Delete a strategy rule."""

    try:
        return strategy_service.delete_rule(strategy_id, rule_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{strategy_id}/signals")
async def generate_signals(strategy_id: str, body: StrategySignalRequest) -> Dict[str, Any]:
    """Generate buy/sell signal summaries for a strategy."""

    try:
        return strategy_service.generate_strategy_signals(
            strategy_id,
            start=body.start,
            end=body.end,
            interval=body.interval,
            symbol=body.symbol,
            datasource=body.datasource,
            exchange=body.exchange,
            config=body.config,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_signal_failed")
        raise HTTPException(400, str(exc)) from exc


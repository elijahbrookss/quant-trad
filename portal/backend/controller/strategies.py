"""FastAPI router for strategy CRUD and signal orchestration."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..service import strategy_service


router = APIRouter()
logger = logging.getLogger(__name__)


class RuleConditionOut(BaseModel):
    """Condition that must be satisfied for a rule."""

    indicator_id: str
    signal_type: str
    rule_id: Optional[str] = None
    direction: Optional[str] = None


class StrategyRuleOut(BaseModel):
    """Response model describing a stored strategy rule."""

    id: str
    name: str
    action: str
    conditions: List[RuleConditionOut]
    match: str
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
    indicators: List[Dict[str, Any]]
    missing_indicators: List[str]
    instruments: List[Dict[str, Any]] = Field(default_factory=list)
    instrument_messages: List[Dict[str, Any]] = Field(default_factory=list)
    rules: List[StrategyRuleOut]
    atm_template: Dict[str, Any] = Field(default_factory=dict)
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
    atm_template: Optional[Dict[str, Any]] = None


class StrategyUpdateRequest(BaseModel):
    """Payload for updating a strategy."""

    name: Optional[str] = None
    symbols: Optional[List[str]] = None
    timeframe: Optional[str] = None
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    indicator_ids: Optional[List[str]] = None
    atm_template: Optional[Dict[str, Any]] = None


class RuleConditionCreate(BaseModel):
    """Definition of a single rule condition."""

    indicator_id: str
    signal_type: str
    rule_id: Optional[str] = None
    direction: Optional[str] = Field(default=None)


class StrategyRuleCreateRequest(BaseModel):
    """Payload for creating a strategy rule."""

    name: str
    action: str
    conditions: List[RuleConditionCreate] = Field(default_factory=list)
    match: str = Field(default="all")
    description: Optional[str] = None
    enabled: bool = True


class RuleConditionUpdate(BaseModel):
    """Mutable condition definition."""

    indicator_id: str
    signal_type: str
    rule_id: Optional[str] = None
    direction: Optional[str] = None


class StrategyRuleUpdateRequest(BaseModel):
    """Payload for updating a strategy rule."""

    name: Optional[str] = None
    action: Optional[str] = None
    conditions: Optional[List[RuleConditionUpdate]] = None
    match: Optional[str] = None
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


class SymbolPresetRequest(BaseModel):
    """Payload describing a datasource/exchange/timeframe/symbol combination."""

    id: Optional[str] = None
    label: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    timeframe: str
    symbol: str


class SymbolPresetOut(SymbolPresetRequest):
    """Response payload for stored symbol presets."""

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


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
            atm_template=body.atm_template,
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
            action=body.action,
            conditions=[condition.dict(exclude_none=True) for condition in body.conditions],
            match=body.match,
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
        if "conditions" in payload and payload["conditions"] is not None:
            payload["conditions"] = [
                {k: v for k, v in condition.items() if v is not None}
                for condition in payload["conditions"]
            ]
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


@router.get("/presets/symbols", response_model=List[SymbolPresetOut])
async def list_symbol_presets() -> List[Dict[str, Any]]:
    """Return saved symbol presets."""

    return strategy_service.list_symbol_presets_service()


@router.post("/presets/symbols", response_model=SymbolPresetOut, status_code=201)
async def save_symbol_preset(body: SymbolPresetRequest) -> Dict[str, Any]:
    """Create or update a symbol preset."""

    try:
        return strategy_service.save_symbol_preset_service(
            preset_id=body.id,
            label=body.label,
            datasource=body.datasource,
            exchange=body.exchange,
            timeframe=body.timeframe,
            symbol=body.symbol,
        )
    except RuntimeError as exc:
        raise HTTPException(500, str(exc)) from exc


@router.delete("/presets/symbols/{preset_id}", status_code=204)
async def delete_symbol_preset(preset_id: str) -> None:
    """Delete a stored symbol preset."""

    strategy_service.delete_symbol_preset_service(preset_id)


"""FastAPI router for strategy CRUD and signal orchestration."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from ..service import provider_service
from ..service.strategy_service import facade as strategy_service


router = APIRouter()
logger = logging.getLogger(__name__)


def _apply_market_aliases(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Translate provider/venue identifiers into legacy datasource/exchange fields."""

    provider_id = payload.pop("provider_id", None)
    venue_id = payload.pop("venue_id", None)
    datasource = payload.get("datasource")
    exchange = payload.get("exchange")

    if provider_id or venue_id:
        provider, venue_exchange = provider_service.translate_market(provider_id, venue_id)
        if provider:
            payload["datasource"] = datasource or provider
        if venue_exchange:
            payload["exchange"] = exchange or venue_exchange
    return payload


def _attach_market_aliases(record: Dict[str, Any]) -> Dict[str, Any]:
    """Add provider/venue hints to strategy responses."""

    datasource = (record.get("datasource") or "").strip().upper() or None
    exchange = (record.get("exchange") or "").strip().lower() or None
    venue_id = provider_service.venue_from_exchange_slug(exchange)
    provider_id = datasource
    _, _, normalized = provider_service.validate_provider_venue(provider_id, venue_id)
    record["provider_id"] = normalized.get("provider_id") or provider_id
    record["venue_id"] = normalized.get("venue_id") or exchange or None
    return record


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


class InstrumentSlotIn(BaseModel):
    """Lightweight instrument slot definition for strategies."""

    symbol: str
    enabled: bool = Field(default=True)
    risk_multiplier: Optional[float] = Field(default=None)


class StrategyOut(BaseModel):
    """Response model representing a strategy record."""

    id: str
    name: str
    description: Optional[str] = None
    symbols: List[str]
    instrument_slots: List[Dict[str, Any]] = Field(default_factory=list)
    timeframe: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    indicator_ids: List[str]
    indicators: List[Dict[str, Any]]
    missing_indicators: List[str]
    instruments: List[Dict[str, Any]] = Field(default_factory=list)
    instrument_messages: List[Dict[str, Any]] = Field(default_factory=list)
    rules: List[StrategyRuleOut]
    atm_template: Dict[str, Any] = Field(default_factory=dict)
    atm_template_id: Optional[str] = None
    base_risk_per_trade: Optional[float] = None
    global_risk_multiplier: Optional[float] = None
    risk_overrides: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class StrategyCreateRequest(BaseModel):
    """Payload for creating a new strategy."""

    name: str
    symbols: List[Any] = Field(default_factory=list)
    instrument_slots: List[InstrumentSlotIn] = Field(default_factory=list)
    timeframe: str
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    indicator_ids: List[str] = Field(default_factory=list)
    atm_template: Optional[Dict[str, Any]] = None
    atm_template_id: Optional[str] = None
    base_risk_per_trade: Optional[float] = None
    global_risk_multiplier: Optional[float] = None
    risk_overrides: Optional[Dict[str, Any]] = None


class StrategyUpdateRequest(BaseModel):
    """Payload for updating a strategy."""

    name: Optional[str] = None
    symbols: Optional[List[Any]] = None
    instrument_slots: Optional[List[InstrumentSlotIn]] = None
    timeframe: Optional[str] = None
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    indicator_ids: Optional[List[str]] = None
    atm_template: Optional[Dict[str, Any]] = None
    atm_template_id: Optional[str] = None
    base_risk_per_trade: Optional[float] = None
    global_risk_multiplier: Optional[float] = None
    risk_overrides: Optional[Dict[str, Any]] = None


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


class ATMTemplateRequest(BaseModel):
    """Payload for saving an ATM template."""

    id: Optional[str] = None
    name: str
    template: Dict[str, Any]
    owner_id: Optional[str] = None


class ATMTemplateOut(ATMTemplateRequest):
    """Response payload for ATM templates."""

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class StrategySignalRequest(BaseModel):
    """Request payload for generating strategy signals."""

    start: str
    end: str
    interval: str
    symbol: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)


class SymbolPresetRequest(BaseModel):
    """Payload describing a datasource/exchange/timeframe/symbol combination."""

    id: Optional[str] = None
    label: str
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
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

    records = strategy_service.list_strategies()
    return [_attach_market_aliases(record) for record in records]


@router.post("/", response_model=StrategyOut, status_code=201)
async def create_strategy(body: StrategyCreateRequest) -> Dict[str, Any]:
    """Create a new strategy record."""

    try:
        payload = _apply_market_aliases(body.dict())
        symbols_payload = (
            payload.get("instrument_slots")
            or body.instrument_slots
            or payload.get("symbols")
            or body.symbols
        )
        record = strategy_service.create_strategy(
            payload.get("name") or body.name,
            symbols=symbols_payload,
            timeframe=payload.get("timeframe") or body.timeframe,
            description=payload.get("description"),
            datasource=payload.get("datasource"),
            exchange=payload.get("exchange"),
            indicator_ids=payload.get("indicator_ids") or [],
            atm_template=payload.get("atm_template"),
            atm_template_id=payload.get("atm_template_id"),
            base_risk_per_trade=payload.get("base_risk_per_trade"),
            global_risk_multiplier=payload.get("global_risk_multiplier"),
            risk_overrides=payload.get("risk_overrides"),
        )
        return _attach_market_aliases(record)
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_create_failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/{strategy_id}", response_model=StrategyOut)
async def get_strategy(strategy_id: str) -> Dict[str, Any]:
    """Retrieve a single strategy."""

    try:
        record = strategy_service.get_strategy(strategy_id)
        return _attach_market_aliases(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/{strategy_id}", response_model=StrategyOut)
async def update_strategy(strategy_id: str, body: StrategyUpdateRequest) -> Dict[str, Any]:
    """Update an existing strategy."""

    try:
        payload = _apply_market_aliases(body.dict(exclude_unset=True))
        record = strategy_service.update_strategy(strategy_id, **payload)
        return _attach_market_aliases(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_update_failed")
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}", status_code=204, response_class=Response)
async def delete_strategy(strategy_id: str) -> Response:
    """Delete a strategy."""

    try:
        strategy_service.delete_strategy(strategy_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

    return Response(status_code=204)


@router.get("/atm-templates", response_model=List[ATMTemplateOut])
async def list_atm_templates() -> List[Dict[str, Any]]:
    """Return all saved ATM templates."""

    return strategy_service.list_atm_templates()


@router.post("/atm-templates", response_model=ATMTemplateOut, status_code=201)
async def save_atm_template(body: ATMTemplateRequest) -> Dict[str, Any]:
    """Create or update an ATM template."""

    try:
        return strategy_service.save_atm_template(body.dict())
    except Exception as exc:  # noqa: BLE001
        logger.exception("atm_template_save_failed")
        raise HTTPException(400, str(exc)) from exc


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
        market = _apply_market_aliases(body.dict())
        return strategy_service.generate_strategy_signals(
            strategy_id,
            start=body.start,
            end=body.end,
            interval=body.interval,
            symbol=market.get("symbol") or body.symbol,
            datasource=market.get("datasource"),
            exchange=market.get("exchange"),
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

    presets = strategy_service.list_symbol_presets_service()
    return [_attach_market_aliases(preset) for preset in presets]


@router.post("/presets/symbols", response_model=SymbolPresetOut, status_code=201)
async def save_symbol_preset(body: SymbolPresetRequest) -> Dict[str, Any]:
    """Create or update a symbol preset."""

    try:
        payload = _apply_market_aliases(body.dict())
        record = strategy_service.save_symbol_preset_service(
            preset_id=payload.get("id"),
            label=payload.get("label"),
            datasource=payload.get("datasource"),
            exchange=payload.get("exchange"),
            timeframe=payload.get("timeframe"),
            symbol=payload.get("symbol"),
        )
        return _attach_market_aliases(record)
    except RuntimeError as exc:
        raise HTTPException(500, str(exc)) from exc


@router.delete("/presets/symbols/{preset_id}", status_code=204, response_class=Response)
async def delete_symbol_preset(preset_id: str) -> Response:
    """Delete a stored symbol preset."""

    strategy_service.delete_symbol_preset_service(preset_id)

    return Response(status_code=204)


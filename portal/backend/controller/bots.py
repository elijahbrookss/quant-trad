"""FastAPI router exposing bot CRUD and runtime controls."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..service import bot_service


router = APIRouter()


class RiskSettings(BaseModel):
    """Risk template for ladder exits."""

    contracts: Optional[int] = Field(default=3, ge=1)
    targets: Optional[List[int]] = Field(default_factory=lambda: [20, 40, 60])
    stop_ticks: Optional[int] = Field(default=30, ge=1)
    breakeven_trigger_ticks: Optional[int] = Field(default=20, ge=1)
    tick_size: Optional[float] = Field(default=0.01, gt=0)


class BotBase(BaseModel):
    """Shared bot attributes."""

    name: str
    strategy_ids: List[str] = Field(default_factory=list)
    strategy_id: Optional[str] = Field(
        default=None,
        description="Deprecated single-strategy field maintained for backwards compatibility.",
    )
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    timeframe: str = "15m"
    mode: str = Field(default="instant", pattern="^(instant|walk-forward)$")
    run_type: str = Field(default="backtest", pattern="^(backtest|sim_trade)$")
    fetch_seconds: int = Field(default=1, ge=0)
    backtest_start: Optional[str] = None
    backtest_end: Optional[str] = None
    risk: RiskSettings = Field(default_factory=RiskSettings)


class BotCreateRequest(BotBase):
    """Payload for creating a bot."""

    pass


class BotUpdateRequest(BaseModel):
    """Patch payload for updating a bot."""

    name: Optional[str] = None
    strategy_ids: Optional[List[str]] = None
    strategy_id: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    timeframe: Optional[str] = None
    mode: Optional[str] = Field(default=None, pattern="^(instant|walk-forward)$")
    fetch_seconds: Optional[int] = Field(default=None, ge=0)
    risk: Optional[RiskSettings] = None


class BotResponse(BotBase):
    """Response payload describing a bot."""

    id: str
    status: str
    last_run_at: Optional[str] = None
    last_stats: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    runtime: Optional[Dict[str, Any]] = None


class BotPerformanceResponse(BaseModel):
    """Chart payload for bot lens."""

    candles: List[Dict[str, Any]]
    trades: List[Dict[str, Any]]
    stats: Dict[str, Any]
    overlays: List[Dict[str, Any]] = Field(default_factory=list)
    logs: List[Dict[str, Any]] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)
    runtime: Optional[Dict[str, Any]] = None


class BotLogsResponse(BaseModel):
    """Runtime log payload."""

    logs: List[Dict[str, Any]] = Field(default_factory=list)


@router.get("/", response_model=List[BotResponse])
async def list_bots() -> List[Dict[str, Any]]:
    """Return all bot configs."""

    return bot_service.list_bots()


@router.post("/", response_model=BotResponse, status_code=201)
async def create_bot(body: BotCreateRequest) -> Dict[str, Any]:
    """Create a new bot configuration."""

    try:
        return bot_service.create_bot(**body.dict())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/{bot_id}", response_model=BotResponse)
async def get_bot(bot_id: str) -> Dict[str, Any]:
    """Return a single bot configuration."""

    try:
        return bot_service.get_bot(bot_id)
    except KeyError as exc:  # pragma: no cover - FastAPI path
        raise HTTPException(404, str(exc)) from exc


@router.put("/{bot_id}", response_model=BotResponse)
async def update_bot(bot_id: str, body: BotUpdateRequest) -> Dict[str, Any]:
    """Update bot attributes."""

    try:
        payload = body.dict(exclude_unset=True)
        return bot_service.update_bot(bot_id, **payload)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{bot_id}", status_code=204)
async def delete_bot(bot_id: str) -> None:
    """Delete a bot."""

    bot_service.delete_bot_record(bot_id)


@router.post("/{bot_id}/start", response_model=BotResponse)
async def start_bot(bot_id: str) -> Dict[str, Any]:
    """Start the bot runtime."""

    try:
        return bot_service.start_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/{bot_id}/stop", response_model=BotResponse)
async def stop_bot(bot_id: str) -> Dict[str, Any]:
    """Stop the bot runtime."""

    try:
        return bot_service.stop_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/status")
async def get_bot_status(bot_id: str) -> Dict[str, Any]:
    """Return live runtime status."""

    try:
        return bot_service.runtime_status(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/logs", response_model=BotLogsResponse)
async def get_bot_logs(bot_id: str, limit: int = 200) -> Dict[str, Any]:
    """Return recent runtime logs for a bot."""

    try:
        logs = bot_service.runtime_logs(bot_id, limit=limit)
        return {"logs": logs}
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{bot_id}/pause", response_model=BotResponse)
async def pause_bot(bot_id: str) -> Dict[str, Any]:
    """Pause a running bot."""

    try:
        return bot_service.pause_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{bot_id}/resume", response_model=BotResponse)
async def resume_bot(bot_id: str) -> Dict[str, Any]:
    """Resume a paused bot."""

    try:
        return bot_service.resume_bot(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{bot_id}/performance", response_model=BotPerformanceResponse)
async def get_bot_performance(bot_id: str) -> Dict[str, Any]:
    """Return candle and trade payloads for the lens chart."""

    try:
        return bot_service.performance(bot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc

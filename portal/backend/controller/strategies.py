"""FastAPI routes enabling strategy authoring flows."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..service import strategy_service, StrategyRecord


router = APIRouter()


class IndicatorPayload(BaseModel):
    """Minimal indicator description referenced by a strategy."""

    id: str = Field(..., description="Indicator identifier from the chart workspace")
    name: Optional[str] = Field(default=None, description="Display name for the indicator")
    type: Optional[str] = Field(default=None, description="Indicator type identifier")


class StrategyPayload(BaseModel):
    """Payload describing a strategy blueprint from the UI."""

    strategy_id: Optional[str] = Field(default=None, description="Existing identifier when updating")
    name: str
    symbol: Optional[str] = Field(default=None)
    timeframe: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    indicators: List[IndicatorPayload] = Field(default_factory=list)
    selected_signals: Dict[str, List[str]] = Field(default_factory=dict)


class YamlUpload(BaseModel):
    """Wrapper for YAML configuration uploads."""

    yaml_text: str = Field(..., description="Raw YAML content")


class BacktestRequest(BaseModel):
    """Optional parameters for a backtest placeholder."""

    start: Optional[str] = None
    end: Optional[str] = None
    timeframe: Optional[str] = None


class LaunchRequest(BaseModel):
    """Placeholder request for launching a strategy."""

    mode: Optional[str] = Field(default="simulation")


def _serialize(record: StrategyRecord) -> Dict[str, Any]:
    """Convert a strategy record to a JSON-serializable dict."""

    return {
        "strategy_id": record.strategy_id,
        "name": record.name,
        "symbol": record.symbol,
        "timeframe": record.timeframe,
        "description": record.description,
        "indicators": record.indicators,
        "selected_signals": record.selected_signals,
        "yaml_config": record.yaml_config,
        "created_at": record.created_at.isoformat() + "Z",
        "updated_at": record.updated_at.isoformat() + "Z",
        "last_backtest": record.last_backtest,
        "launch_status": record.launch_status,
    }


@router.get("/")
def list_strategies() -> Dict[str, Any]:
    """Return all saved strategies for the workspace."""

    records = strategy_service.list_strategies()
    return {"strategies": [_serialize(rec) for rec in records]}


@router.post("/")
def create_strategy(payload: StrategyPayload) -> Dict[str, Any]:
    """Create or update a strategy and return its serialized form."""

    record = strategy_service.save_strategy(payload.dict())
    return {"strategy": _serialize(record)}


@router.post("/{strategy_id}/yaml")
def upload_yaml(strategy_id: str, upload: YamlUpload) -> Dict[str, Any]:
    """Attach YAML metadata to the specified strategy."""

    try:
        record = strategy_service.attach_yaml(strategy_id, upload.yaml_text)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    yaml_summary = list(record.yaml_config.keys()) if record.yaml_config else []
    return {"strategy": _serialize(record), "yaml_summary": yaml_summary}


@router.get("/{strategy_id}/order-signals")
def get_order_signals(strategy_id: str) -> Dict[str, Any]:
    """Return derived order signals for the strategy."""

    try:
        signals = strategy_service.generate_order_signals(strategy_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {"order_signals": signals}


@router.post("/{strategy_id}/backtest")
def request_backtest(strategy_id: str, params: BacktestRequest | None = None) -> Dict[str, Any]:
    """Record a placeholder backtest request."""

    try:
        payload = strategy_service.request_backtest(strategy_id, (params or BacktestRequest()).dict(exclude_none=True))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return payload


@router.post("/{strategy_id}/launch")
def launch_strategy(strategy_id: str, request: LaunchRequest | None = None) -> Dict[str, Any]:
    """Record a placeholder strategy launch request."""

    mode = (request or LaunchRequest()).mode or "simulation"
    try:
        payload = strategy_service.launch_strategy(strategy_id, mode=mode)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return payload


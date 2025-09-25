# routers/indicators.py
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..service.indicator_service import (
    list_types, get_type_details,
    list_instances_meta, get_instance_meta, delete_instance,
    create_instance, update_instance, overlays_for_instance,
    generate_signals_for_instance,
)

router = APIRouter()
logger = logging.getLogger(__name__)

# ===== Schemas =====
class IndicatorInstanceIn(BaseModel):
    type: str
    name: Optional[str] = None
    params: Dict[str, Any]  # must include symbol/start/end/interval on create
    color: Optional[str] = None

class IndicatorInstanceOut(BaseModel):
    id: str
    type: str
    name: str
    params: Dict[str, Any]
    enabled: bool
    color: Optional[str] = None

class OverlayRequest(BaseModel):
    start: str
    end: str
    interval: str
    symbol: Optional[str] = None  # optional override; defaults to stored

class SignalRequest(BaseModel):
    start: str
    end: str
    interval: str
    symbol: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)

# ===== Instances =====
@router.get("/", response_model=List[IndicatorInstanceOut])
async def list_instances():
    return list_instances_meta()

@router.post("/", response_model=IndicatorInstanceOut, status_code=201)
async def create(body: IndicatorInstanceIn):
    try:
        return create_instance(body.type, body.name, dict(body.params), body.color)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))

@router.put("/{inst_id}", response_model=IndicatorInstanceOut)
async def update(inst_id: str, body: IndicatorInstanceIn):
    try:
        color_provided = "color" in body.__fields_set__
        return update_instance(
            inst_id,
            body.type,
            dict(body.params),
            body.name,
            color=body.color,
            color_provided=color_provided,
        )
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))

@router.get("/{inst_id}", response_model=IndicatorInstanceOut)
async def get_one(inst_id: str):
    try:
        return get_instance_meta(inst_id)
    except KeyError:
        raise HTTPException(404, "Indicator not found")

@router.delete("/{inst_id}", status_code=204)
async def delete(inst_id: str):
    try:
        delete_instance(inst_id)
    except KeyError:
        raise HTTPException(404, "Indicator not found")

# ===== Types =====
@router.get("-types", response_model=List[str])
async def list_indicator_types():
    return list_types()

@router.get("-types/{type_id}")
async def get_indicator_type(type_id: str):
    try:
        return get_type_details(type_id)
    except KeyError as e:
        raise HTTPException(404, str(e))

# ===== Overlays by UUID =====
@router.post("/{inst_id}/overlays")
async def overlays(inst_id: str, req: OverlayRequest):
    """
    Returns TradingView Lightweight-Charts overlays for a stored indicator UUID
    over the requested chart window. Does not accept indicator params.
    """
    try:
        payload = overlays_for_instance(
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
        )
        return payload
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    except LookupError as e:
        # no candles or no overlays
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))
    except Exception as e:
        logger.exception("Unexpected overlay error")
        raise HTTPException(500, "Unexpected error computing overlays")


@router.post("/{inst_id}/signals")
async def signals(inst_id: str, req: SignalRequest):
    try:
        return generate_signals_for_instance(
            inst_id=inst_id,
            start=req.start,
            end=req.end,
            interval=req.interval,
            symbol=req.symbol,
            config=req.config,
        )
    except KeyError:
        raise HTTPException(404, "Indicator not found")
    except LookupError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(500, str(e))
    except Exception:
        logger.exception("Unexpected signal generation error")
        raise HTTPException(500, "Unexpected error generating signals")

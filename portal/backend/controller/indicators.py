import uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import inspect

from data_providers.alpaca_provider import AlpacaProvider
from indicators.config           import DataContext
from indicators.vwap             import VWAPIndicator
from indicators.pivot_level      import PivotLevelIndicator
from indicators.trendline        import TrendlineIndicator
from indicators.market_profile   import MarketProfileIndicator

router = APIRouter()

#
# ── Request & Response Schemas ─────────────────────────────────────────────────
#
class IndicatorInstanceIn(BaseModel):
    type: str
    name: str  # optional, but useful for UI
    # must include at least: symbol, start, end, interval
    params: Dict[str, Any]

class IndicatorInstanceOut(BaseModel):
    name: str
    id: str
    type: str
    params: Dict[str, Any]
    enabled: bool

#
# ── In‐Memory Store ─────────────────────────────────────────────────────────────
#
# We store both the Pydantic metadata AND the actual Python instance,
# so later you can call instance.merge_value_areas(), .to_overlays(), etc.
_instances: Dict[str, Dict[str, Any]] = {}

#
# ── Mapping from type‐string → Indicator Class ─────────────────────────────────
#
_INDICATOR_MAP = {
    "vwap":           VWAPIndicator,
    "pivot_level":    PivotLevelIndicator,
    "trendline":      TrendlineIndicator,
    "market_profile": MarketProfileIndicator,
}

def extract_params(inst) -> Dict[str, Any]:
    sig = inspect.signature(inst.__class__.__init__)
    out: Dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
            continue
        if hasattr(inst, name):
            out[name] = getattr(inst, name)
    return out

#
# ── List all created instances ─────────────────────────────────────────────────
#
@router.get("/", response_model=List[IndicatorInstanceOut])
async def list_instances():
    return [ meta for meta in (_instances[k]["meta"] for k in _instances) ]

#
# ── Create a new instance ───────────────────────────────────────────────────────
#
@router.post("/", response_model=IndicatorInstanceOut, status_code=201)
async def create_instance(body: IndicatorInstanceIn):
    # 1) Lookup the class
    Cls = _INDICATOR_MAP.get(body.type)
    if not Cls:
        raise HTTPException(400, f"Unknown indicator type: {body.type}")

    # 2) Extract the "context" params
    ctx_keys = ("symbol", "start", "end", "interval")
    try:
        ctx_kwargs = { k: body.params.pop(k) for k in ctx_keys }
    except KeyError as e:
        raise HTTPException(400, f"Missing required context param: {e.args[0]}")

    # 3) Build DataContext & Provider
    ctx = DataContext(**ctx_kwargs)
    ctx.validate()
    provider = AlpacaProvider()

    # 4) Instantiate the indicator (this may fetch data, compute profiles, etc.)
    try:
        inst = Cls.from_context(
            provider=provider,
            ctx=ctx,
            **body.params  # the remaining params go straight into the constructor
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to instantiate indicator: {str(e)}")

    ctor_params = extract_params(inst)
    all_params = { **ctor_params }


    # 5) Record it in memory
    inst_id = str(uuid.uuid4())
    meta = IndicatorInstanceOut(
        id=inst_id,
        type=body.type,
        params=all_params,                       # echo back everything you used
        enabled=True,                           # default to off until toggled
        name=body.name or body.type.replace("_", " ").title()  # nice display name
    )
    _instances[inst_id] = { "meta": meta, "instance": inst }

    return meta

#
# ── Update params for an existing instance ─────────────────────────────────────
#
@router.put("/{inst_id}", response_model=IndicatorInstanceOut)
async def update_instance(inst_id: str, body: IndicatorInstanceIn):
    entry = _instances.get(inst_id)
    if not entry:
        raise HTTPException(404, "Indicator not found")

    # 1) Check for updated params compared to the original
    original_params = entry["meta"].params
    if body.params == original_params:
        # No changes, just return the existing metadata
        return entry["meta"]
    
    # 2) If params changed, we need to re‐instantiate the indicator
    #    This is necessary to ensure the indicator reflects the new parameters.
    if body.type != entry["meta"].type:
        raise HTTPException(400, "Cannot change indicator type; create a new instance instead")

    # 3) Validate the new params against the class constructor
    #    This ensures the new params are compatible with the indicator type.
    sig = inspect.signature(_INDICATOR_MAP[body.type].__init__)
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
            continue
        if name not in body.params:
            if param.default is inspect._empty:
                raise HTTPException(400, f"Missing required parameter: {name}")
            body.params[name] = param.default
    
    # 4) If we reach here, the params are valid and we can proceed


    # Re‐instantiate with the new params
    Cls = _INDICATOR_MAP.get(body.type)
    if not Cls:
        raise HTTPException(400, f"Unknown indicator type: {body.type}")

    # same context extraction as above
    try:
        ctx_kwargs = { k: body.params.pop(k) for k in ("symbol","start","end","interval") }
    except KeyError as e:
        raise HTTPException(400, f"Missing required context param: {e.args[0]}")

    ctx = DataContext(**ctx_kwargs)
    ctx.validate()
    provider = AlpacaProvider()

    try:
        new_inst = Cls.from_context(provider=provider, ctx=ctx, **body.params)
    except Exception as e:
        raise HTTPException(500, f"Failed to re‐instantiate indicator: {str(e)}")

    ctor_params = extract_params(new_inst)
    all_params = { **ctor_params }

    # update store
    entry["instance"] = new_inst
    entry["meta"].params = all_params
    entry["meta"].name = body.name or entry["meta"].name

    return entry["meta"]

@router.get("/{inst_id}", response_model=IndicatorInstanceOut)
async def get_instance(inst_id: str):
    entry = _instances.get(inst_id)
    print(_instances)
    if not entry:
        raise HTTPException(404, "Indicator not found")

    return entry["meta"]

#
# ── Delete an existing instance ─────────────────────────────────────────────────
#
@router.delete("/{inst_id}", status_code=204)
async def delete_instance(inst_id: str):
    if inst_id not in _instances:
        raise HTTPException(404, "Indicator not found")
    del _instances[inst_id]
    return


@router.get("/types", response_model=List[IndicatorInstanceOut])
async def list_indicators():
    out: List[Dict[str, Any]] = []

    for Cls in _INDICATOR_CLASSES:
        # 2) Inspect its __init__ signature for default params
        sig = inspect.signature(Cls.__init__)
        defaults = {
            name: param.default
            for name, param in sig.parameters.items()
            # skip `self` and the DataFrame arg
            if name not in ("self", "df") and param.default is not inspect._empty
        }

        # 3) Build your payload fields
        #    Use a class‐level NAME if present, otherwise fallback to the class name
        type_str = getattr(Cls, "NAME", Cls.__name__)
        id_str   = type_str.lower().replace(" ", "-")

        out.append({
            "id":      id_str,
            "name":    type_str,
            "type":    type_str,
            "enabled": False,    # or pull from your own feature‐flag store
            "params":  defaults,
        })

    return out

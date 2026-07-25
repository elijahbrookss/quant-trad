"""FastAPI router for strategy CRUD and signal orchestration."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field

from ..service.market import instrument_service
from ..service.providers import provider_service
from ..service.strategies.strategy_service import facade as strategy_service


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


def _slot_payload(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if hasattr(raw, "model_dump"):
        return raw.model_dump(exclude_unset=True)
    return {"symbol": str(raw or "").strip()}


def _resolve_slot_instrument(payload: Dict[str, Any], slot_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    metadata = dict(slot_payload.get("metadata") or {})
    inst_id = str(slot_payload.get("instrument_id") or metadata.get("instrument_id") or "").strip()
    if inst_id:
        return instrument_service.get_instrument_record(inst_id)

    symbol = str(slot_payload.get("symbol") or "").strip()
    if not symbol:
        return None
    provider_id = slot_payload.get("provider_id") or metadata.get("provider_id")
    venue_id = slot_payload.get("venue_id") or metadata.get("venue_id")
    translated_provider = None
    translated_exchange = None
    if provider_id or venue_id:
        translated_provider, translated_exchange = provider_service.translate_market(provider_id, venue_id)
    datasource = (
        slot_payload.get("datasource")
        or metadata.get("datasource")
        or translated_provider
        or payload.get("datasource")
    )
    exchange = (
        slot_payload.get("exchange")
        or metadata.get("exchange")
        or translated_exchange
        or payload.get("exchange")
    )
    inst_rec = instrument_service.resolve_instrument(datasource, exchange, symbol)
    if inst_rec:
        return inst_rec
    enriched, _err = instrument_service.validate_instrument(
        datasource,
        exchange,
        symbol,
        provider_id=provider_id,
        venue_id=venue_id,
    )
    return enriched


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


def _strategy_core(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": record.get("id"),
        "name": record.get("name"),
        "description": record.get("description"),
        "timeframe": record.get("timeframe"),
        "datasource": record.get("datasource"),
        "exchange": record.get("exchange"),
        "provider_id": record.get("provider_id"),
        "venue_id": record.get("venue_id"),
        "atm_template_id": record.get("atm_template_id"),
        "atm_template": dict(record.get("atm_template") or {}),
        "risk_config": dict(record.get("risk_config") or {}),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
    }


def _strategy_bindings(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbols": list(record.get("symbols") or []),
        "instrument_slots": list(record.get("instrument_slots") or []),
        "instruments": list(record.get("instruments") or []),
        "indicator_ids": list(record.get("indicator_ids") or []),
        "indicators": list(record.get("indicators") or []),
    }


def _indicator_binding_summary(entry: Dict[str, Any]) -> Dict[str, Any]:
    meta = dict(entry.get("meta") or {})
    outputs = [dict(output) for output in (meta.get("typed_outputs") or []) if isinstance(output, dict)]
    output_counts = {"signal": 0, "context": 0, "metric": 0, "other": 0}
    for output in outputs:
        output_type = str(output.get("type") or "").strip()
        if output_type in output_counts:
            output_counts[output_type] += 1
        else:
            output_counts["other"] += 1
    return {
        "id": entry.get("id") or meta.get("id"),
        "status": entry.get("status") or ("active" if meta else "missing"),
        "type": meta.get("type"),
        "name": meta.get("name"),
        "runtime_supported": bool(meta.get("runtime_supported", False)),
        "compute_supported": bool(meta.get("compute_supported", False)),
        "output_counts": output_counts,
    }


def _compact_bindings(record: Dict[str, Any]) -> Dict[str, Any]:
    shaped = _attach_market_aliases(dict(record))
    return {
        "symbols": list(shaped.get("symbols") or []),
        "instrument_slots": list(shaped.get("instrument_slots") or []),
        "instruments": list(shaped.get("instruments") or []),
        "indicator_ids": list(shaped.get("indicator_ids") or []),
        "indicators": [
            _indicator_binding_summary(entry)
            for entry in (shaped.get("indicators") or [])
            if isinstance(entry, dict)
        ],
    }


def _read_context(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "missing_indicators": list(record.get("missing_indicators") or []),
        "instrument_messages": list(record.get("instrument_messages") or []),
    }


def _build_strategy_inventory_item(record: Dict[str, Any], *, variant_count: int = 0) -> Dict[str, Any]:
    shaped = _attach_market_aliases(dict(record))
    bindings = _compact_bindings(shaped)
    read_context = _read_context(shaped)
    return {
        **_strategy_core(shaped),
        "symbols": list(shaped.get("symbols") or []),
        "instrument_count": len(bindings["instruments"]),
        "indicator_count": len(bindings["indicators"]),
        "rule_count": len(shaped.get("rules") or []),
        "variant_count": int(variant_count),
        "readiness": {
            "missing_indicator_count": len(read_context["missing_indicators"]),
            "instrument_message_count": len(read_context["instrument_messages"]),
        },
    }


def _build_strategy_definition(record: Dict[str, Any]) -> Dict[str, Any]:
    shaped = _attach_market_aliases(dict(record))
    return {
        "schema_version": "strategy_definition.v1",
        "strategy": _strategy_core(shaped),
        "read_context": _read_context(shaped),
        "counts": {
            "instrument_count": len(shaped.get("instruments") or []),
            "indicator_count": len(shaped.get("indicators") or []),
            "rule_count": len(shaped.get("rules") or []),
        },
    }


def _build_strategy_bindings_doc(record: Dict[str, Any]) -> Dict[str, Any]:
    shaped = _attach_market_aliases(dict(record))
    bindings = _compact_bindings(shaped)
    return {
        "schema_version": "strategy_bindings.v1",
        "strategy_id": shaped.get("id"),
        "bindings": bindings,
        "read_context": _read_context(shaped),
        "counts": {
            "instrument_count": len(bindings["instruments"]),
            "indicator_count": len(bindings["indicators"]),
        },
    }


def _build_strategy_rules_doc(record: Dict[str, Any]) -> Dict[str, Any]:
    shaped = _attach_market_aliases(dict(record))
    rules = list(shaped.get("rules") or [])
    return {
        "schema_version": "strategy_rules.v1",
        "strategy_id": shaped.get("id"),
        "rules": rules,
        "total": len(rules),
    }


def _build_strategy_variants_doc(strategy_id: str, variants: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "schema_version": "strategy_variants.v1",
        "strategy_id": strategy_id,
        "variants": list(variants or []),
        "total": len(variants or []),
    }


class StrategyRuleOut(BaseModel):
    """Response model describing a stored strategy rule."""

    id: str
    name: str
    intent: str
    priority: int = 0
    trigger: Dict[str, Any]
    guards: List[Dict[str, Any]] = Field(default_factory=list)
    description: Optional[str] = None
    enabled: bool
    created_at: str
    updated_at: str


class InstrumentSlotIn(BaseModel):
    """Lightweight instrument slot definition for strategies."""

    symbol: str
    risk_multiplier: Optional[float] = Field(default=None)
    instrument_id: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    provider_id: Optional[str] = None
    venue_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StrategyCreateRequest(BaseModel):
    """Payload for creating a new strategy."""

    name: str
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
    risk_config: Optional[Dict[str, Any]] = None


class StrategyUpdateRequest(BaseModel):
    """Payload for updating a strategy."""

    name: Optional[str] = None
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
    risk_config: Optional[Dict[str, Any]] = None


class StrategyRuleCreateRequest(BaseModel):
    """Payload for creating a strategy rule."""

    name: str
    intent: str
    priority: int = 0
    trigger: Dict[str, Any]
    guards: List[Dict[str, Any]] = Field(default_factory=list)
    description: Optional[str] = None
    enabled: bool = True


class StrategyRuleUpdateRequest(BaseModel):
    """Payload for updating a strategy rule."""

    name: Optional[str] = None
    intent: Optional[str] = None
    priority: Optional[int] = None
    trigger: Optional[Dict[str, Any]] = None
    guards: Optional[List[Dict[str, Any]]] = None
    description: Optional[str] = None
    enabled: Optional[bool] = None


class StrategyVariantRequest(BaseModel):
    """Payload for creating a saved strategy variant."""

    name: str
    description: Optional[str] = None
    output_filters: List[Dict[str, Any]] = Field(default_factory=list)
    is_default: bool = False


class StrategyVariantUpdateRequest(BaseModel):
    """Patch payload for updating a saved strategy variant."""

    name: Optional[str] = None
    description: Optional[str] = None
    output_filters: Optional[List[Dict[str, Any]]] = None
    is_default: Optional[bool] = None


class StrategyVariantOut(BaseModel):
    """Response model representing a saved strategy variant."""

    id: str
    strategy_id: str
    name: str
    description: Optional[str] = None
    output_filters: List[Dict[str, Any]] = Field(default_factory=list)
    is_default: bool
    created_at: str
    updated_at: str


class ATMTemplateRequest(BaseModel):
    """Payload for saving an ATM template."""

    id: Optional[str] = None
    name: str
    template: Dict[str, Any]


class ATMTemplateOut(ATMTemplateRequest):
    """Response payload for ATM templates."""

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class StrategyVariantSelectionRequest(BaseModel):
    """Payload for selecting the effective strategy variant."""

    variant_id: Optional[str] = None
    variant_name: Optional[str] = None


class StrategyPreviewRequest(StrategyVariantSelectionRequest):
    """Request payload for generating a strategy preview."""

    start: str
    end: str
    interval: str
    instrument_ids: List[str] = Field(default_factory=list)


class StrategyPreviewSummaryRequest(StrategyPreviewRequest):
    """Request payload for generating a compact strategy preview summary."""

    max_examples: int = Field(default=5, ge=0, le=100)
    include_signals: bool = False


class StrategyPreviewCompareCaseRequest(StrategyVariantSelectionRequest):
    """One strategy preview case inside a comparison request."""

    label: Optional[str] = None
    strategy_id: str
    instrument_ids: List[str] = Field(default_factory=list)


class StrategyPreviewCompareRequest(BaseModel):
    """Request payload for comparing compact strategy preview summaries."""

    start: str
    end: str
    interval: str
    cases: List[StrategyPreviewCompareCaseRequest] = Field(default_factory=list)
    max_examples: int = Field(default=5, ge=0, le=100)
    include_signals: bool = False


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


@router.get("/")
async def list_strategies() -> Dict[str, Any]:
    """Return all stored strategies."""

    records = strategy_service.list_strategies()
    items = []
    for record in records:
        strategy_id = str(record.get("id") or "").strip()
        variant_count = 0
        if strategy_id:
            variant_count = len(strategy_service.list_strategy_variants(strategy_id))
        items.append(_build_strategy_inventory_item(record, variant_count=variant_count))
    return {
        "schema_version": "strategy_inventory.v1",
        "items": items,
        "total": len(items),
    }


@router.post("/", status_code=201)
async def create_strategy(body: StrategyCreateRequest) -> Dict[str, Any]:
    """Create a new strategy record."""

    try:
        payload = _apply_market_aliases(body.model_dump())
        slots = payload.get("instrument_slots") or body.instrument_slots or []

        # Resolve or create instruments for each provided slot and embed instrument_id in metadata
        resolved_slots = []
        for raw in slots:
            slot_payload = _slot_payload(raw)
            symbol = slot_payload.get("symbol")
            if not symbol:
                continue
            symbol = str(symbol).strip()
            inst_rec = None
            try:
                inst_rec = _resolve_slot_instrument(payload, slot_payload)
            except Exception:
                metadata = dict(slot_payload.get("metadata") or {})
                if slot_payload.get("instrument_id") or metadata.get("instrument_id"):
                    raise
                inst_rec = None

            # If we persisted a minimal instrument (or found one without tick metadata),
            # attempt to enrich it immediately so the UI can display tick/contract data
            # on the next page. This is non-blocking: if enrichment fails, we continue.
            if inst_rec:
                try:
                    enrich_datasource = inst_rec.get("datasource") or slot_payload.get("datasource") or payload.get("datasource")
                    enrich_exchange = inst_rec.get("exchange") or slot_payload.get("exchange") or payload.get("exchange")
                    enriched, err = instrument_service.validate_instrument(
                        enrich_datasource, enrich_exchange, symbol
                    )
                    if enriched and enriched.get("id") == inst_rec.get("id"):
                        inst_rec = enriched
                except Exception:
                    # don't block strategy creation on enrichment failures
                    pass

            if inst_rec and isinstance(slot_payload, dict):
                slot_payload.setdefault("metadata", {})["instrument_id"] = inst_rec.get("id")
                slot_payload["metadata"].setdefault("datasource", inst_rec.get("datasource"))
                slot_payload["metadata"].setdefault("exchange", inst_rec.get("exchange"))
            resolved_slots.append(slot_payload)

        record = strategy_service.create_strategy(
            payload.get("name") or body.name,
            symbols=resolved_slots,
            timeframe=payload.get("timeframe") or body.timeframe,
            description=payload.get("description"),
            datasource=payload.get("datasource"),
            exchange=payload.get("exchange"),
            indicator_ids=payload.get("indicator_ids") or [],
            atm_template=payload.get("atm_template"),
            atm_template_id=payload.get("atm_template_id"),
            risk_config=payload.get("risk_config"),
        )
        return _build_strategy_definition(record)
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_create_failed")
        raise HTTPException(400, str(exc)) from exc


# Static endpoints (place before parameterised routes to avoid path collisions)
@router.get("/atm-templates", response_model=List[ATMTemplateOut])
async def list_atm_templates() -> List[Dict[str, Any]]:
    """Return all saved ATM templates."""

    return strategy_service.list_atm_templates()


@router.post("/atm-templates", response_model=ATMTemplateOut, status_code=201)
async def save_atm_template(body: ATMTemplateRequest) -> Dict[str, Any]:
    """Create or update an ATM template."""

    try:
        # `owner_id` was removed from the schema; do not pass it through.
        payload = body.model_dump()
        payload.pop("owner_id", None)
        return strategy_service.save_atm_template(payload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("atm_template_save_failed")
        raise HTTPException(400, str(exc)) from exc




@router.get("/{strategy_id}/variants")
async def list_strategy_variants(strategy_id: str) -> Dict[str, Any]:
    """Return saved output-filter variants for a strategy."""

    try:
        return _build_strategy_variants_doc(
            strategy_id,
            strategy_service.list_strategy_variants(strategy_id),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{strategy_id}/variants", response_model=StrategyVariantOut, status_code=201)
async def create_strategy_variant(strategy_id: str, body: StrategyVariantRequest) -> Dict[str, Any]:
    """Create a saved output-filter variant for a strategy."""

    try:
        return strategy_service.create_strategy_variant(
            strategy_id,
            name=body.name,
            description=body.description,
            output_filters=body.output_filters,
            is_default=body.is_default,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_variant_create_failed")
        raise HTTPException(400, str(exc)) from exc


@router.put("/{strategy_id}/variants/{variant_id}", response_model=StrategyVariantOut)
async def update_strategy_variant(
    strategy_id: str,
    variant_id: str,
    body: StrategyVariantUpdateRequest,
) -> Dict[str, Any]:
    """Update a saved strategy variant."""

    try:
        return strategy_service.update_strategy_variant(
            strategy_id,
            variant_id,
            **body.model_dump(exclude_unset=True),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_variant_update_failed")
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}/variants/{variant_id}", status_code=204, response_class=Response)
async def delete_strategy_variant(strategy_id: str, variant_id: str) -> Response:
    """Delete a saved non-default strategy variant."""

    try:
        strategy_service.delete_strategy_variant(strategy_id, variant_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return Response(status_code=204)


@router.get("/{strategy_id}/definition")
async def get_strategy_definition(strategy_id: str) -> Dict[str, Any]:
    """Retrieve the core strategy definition."""

    try:
        record = strategy_service.get_strategy(strategy_id)
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{strategy_id}/bindings")
async def get_strategy_bindings(strategy_id: str) -> Dict[str, Any]:
    """Retrieve instrument and indicator bindings without full indicator manifests."""

    try:
        record = strategy_service.get_strategy(strategy_id)
        return _build_strategy_bindings_doc(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{strategy_id}/rules")
async def get_strategy_rules(strategy_id: str) -> Dict[str, Any]:
    """Retrieve stored strategy rules only."""

    try:
        record = strategy_service.get_strategy(strategy_id)
        return _build_strategy_rules_doc(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/{strategy_id}/effective")
async def get_effective_strategy_contract(
    strategy_id: str,
    variant_id: Optional[str] = Query(default=None),
    variant_name: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Retrieve the runtime-effective strategy contract for a selected variant."""

    try:
        return strategy_service.get_effective_strategy_contract(
            strategy_id,
            variant_id=variant_id,
            variant_name=variant_name,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_effective_read_failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/{strategy_id}/decision-inputs")
async def get_strategy_decision_inputs(
    strategy_id: str,
    variant_id: Optional[str] = Query(default=None),
    variant_name: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Retrieve attached indicator decision inputs and effective rule references."""

    try:
        return strategy_service.get_strategy_decision_inputs(
            strategy_id,
            variant_id=variant_id,
            variant_name=variant_name,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_decision_inputs_read_failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/{strategy_id}")
async def get_strategy(strategy_id: str) -> Dict[str, Any]:
    """Retrieve the core strategy definition."""

    try:
        record = strategy_service.get_strategy(strategy_id)
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/{strategy_id}")
async def update_strategy(strategy_id: str, body: StrategyUpdateRequest) -> Dict[str, Any]:
    """Update an existing strategy."""

    try:
        payload = _apply_market_aliases(body.model_dump(exclude_unset=True))
        record = strategy_service.update_strategy(strategy_id, **payload)
        return _build_strategy_definition(record)
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
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    return Response(status_code=204)


@router.post("/{strategy_id}/indicators/{indicator_id}")
async def attach_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Attach an indicator to a strategy."""

    try:
        record = strategy_service.register_indicator(strategy_id, indicator_id)
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}/indicators/{indicator_id}")
async def detach_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Detach an indicator from a strategy."""

    try:
        record = strategy_service.unregister_indicator(strategy_id, indicator_id)
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{strategy_id}/rules", status_code=201)
async def create_rule(strategy_id: str, body: StrategyRuleCreateRequest) -> Dict[str, Any]:
    """Create a rule for a strategy."""

    try:
        record = strategy_service.create_rule(
            strategy_id,
            name=body.name,
            intent=body.intent,
            priority=body.priority,
            trigger=body.trigger,
            guards=body.guards,
            description=body.description,
            enabled=body.enabled,
        )
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_rule_create_failed")
        raise HTTPException(400, str(exc)) from exc


@router.put("/{strategy_id}/rules/{rule_id}")
async def update_rule(strategy_id: str, rule_id: str, body: StrategyRuleUpdateRequest) -> Dict[str, Any]:
    """Update an existing rule."""

    try:
        payload = body.model_dump(exclude_unset=True)
        record = strategy_service.update_rule(strategy_id, rule_id, **payload)
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{strategy_id}/rules/{rule_id}")
async def delete_rule(strategy_id: str, rule_id: str) -> Dict[str, Any]:
    """Delete a strategy rule."""

    try:
        record = strategy_service.delete_rule(strategy_id, rule_id)
        return _build_strategy_definition(record)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/{strategy_id}/compile")
async def compile_strategy_contract(
    strategy_id: str,
    body: Optional[StrategyVariantSelectionRequest] = None,
) -> Dict[str, Any]:
    """Validate and compile a strategy using the selected or default variant."""

    try:
        return strategy_service.compile_strategy_contract(
            strategy_id,
            variant_id=body.variant_id if body is not None else None,
            variant_name=body.variant_name if body is not None else None,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_compile_failed")
        raise HTTPException(400, str(exc)) from exc


@router.post("/{strategy_id}/preview")
async def run_preview(strategy_id: str, body: StrategyPreviewRequest) -> Dict[str, Any]:
    """Run a rule-logic preview for a strategy."""

    try:
        return strategy_service.run_strategy_preview(
            strategy_id,
            start=body.start,
            end=body.end,
            interval=body.interval,
            instrument_ids=body.instrument_ids,
            variant_id=body.variant_id,
            variant_name=body.variant_name,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_preview_failed")
        raise HTTPException(400, str(exc)) from exc


@router.post("/{strategy_id}/preview/summary")
async def run_preview_summary(strategy_id: str, body: StrategyPreviewSummaryRequest) -> Dict[str, Any]:
    """Run a rule-logic preview and return its compact summary."""

    try:
        return strategy_service.run_strategy_preview_summary(
            strategy_id,
            start=body.start,
            end=body.end,
            interval=body.interval,
            instrument_ids=body.instrument_ids,
            variant_id=body.variant_id,
            variant_name=body.variant_name,
            max_examples=body.max_examples,
            include_signals=body.include_signals,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_preview_summary_failed")
        raise HTTPException(400, str(exc)) from exc


@router.post("/preview/compare")
async def compare_previews(body: StrategyPreviewCompareRequest) -> Dict[str, Any]:
    """Run and compare compact previews for several strategies."""

    try:
        return strategy_service.compare_strategy_previews(
            start=body.start,
            end=body.end,
            interval=body.interval,
            cases=[case.model_dump() for case in body.cases],
            max_examples=body.max_examples,
            include_signals=body.include_signals,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("strategy_preview_compare_failed")
        raise HTTPException(400, str(exc)) from exc


@router.get("/{strategy_id}/previews/{preview_id}/signals/{signal_id}")
async def get_preview_signal_detail(strategy_id: str, preview_id: str, signal_id: str) -> Dict[str, Any]:
    """Return one retained strategy preview signal with audit context."""

    try:
        return strategy_service.get_strategy_preview_signal_detail(strategy_id, preview_id, signal_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/presets/symbols", response_model=List[SymbolPresetOut])
async def list_symbol_presets() -> List[Dict[str, Any]]:
    """Return saved symbol presets."""

    presets = strategy_service.list_symbol_presets_service()
    return [_attach_market_aliases(preset) for preset in presets]


@router.post("/presets/symbols", response_model=SymbolPresetOut, status_code=201)
async def save_symbol_preset(body: SymbolPresetRequest) -> Dict[str, Any]:
    """Create or update a symbol preset."""

    try:
        payload = _apply_market_aliases(body.model_dump())
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

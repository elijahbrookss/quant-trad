"""Research memory service and check orchestration."""

from __future__ import annotations

import logging
from typing import Any, Mapping
import uuid

from portal.backend.service.market import candle_service, instrument_service
from portal.backend.service.provenance import source_revision

from . import repository
from .checks import (
    SUPPORTED_CHECK_FAMILY,
    blocked_check_result,
    evaluate_candle_event_check,
)


logger = logging.getLogger(__name__)

RESEARCH_ITEM_KINDS = {"observation", "research_check", "hypothesis", "study"}
RESEARCH_ITEM_STATUSES = {"draft", "active", "tested", "promoted", "rejected", "archived", "blocked"}


def create_research_item(payload: Mapping[str, Any]) -> dict[str, Any]:
    kind = _normalize_choice(payload.get("kind"), "kind", RESEARCH_ITEM_KINDS)
    status = str(payload.get("status") or "draft").strip()
    if status not in RESEARCH_ITEM_STATUSES:
        raise ValueError(f"unsupported research item status: {status}")
    return repository.create_item(
        kind=kind,
        status=status,
        title=str(payload.get("title") or "").strip(),
        body=_optional(payload.get("body")),
        instrument_id=_optional(payload.get("instrument_id")),
        symbol=_optional(payload.get("symbol")),
        timeframe=_optional(payload.get("timeframe")),
        datasource=_optional(payload.get("datasource")),
        exchange=_optional(payload.get("exchange")),
        window_start=payload.get("window_start"),
        window_end=payload.get("window_end"),
        tags=_tags(payload.get("tags")),
        payload=_mapping_or_empty(payload.get("payload")),
        source_revision=_source_revision(),
    )


def get_research_item(item_id: str) -> dict[str, Any]:
    return repository.get_item(item_id)


def list_research_items(
    *,
    kind: str | None = None,
    status: str | None = None,
    symbol: str | None = None,
    timeframe: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if kind:
        _normalize_choice(kind, "kind", RESEARCH_ITEM_KINDS)
    if status and status not in RESEARCH_ITEM_STATUSES:
        raise ValueError(f"unsupported research item status: {status}")
    return repository.list_items(
        kind=kind,
        status=status,
        symbol=symbol,
        timeframe=timeframe,
        limit=limit,
    )


def create_research_link(payload: Mapping[str, Any]) -> dict[str, Any]:
    return repository.create_link(
        source_item_id=str(payload.get("source_item_id") or "").strip(),
        target_type=str(payload.get("target_type") or "").strip(),
        target_id=str(payload.get("target_id") or "").strip(),
        relation=str(payload.get("relation") or "").strip(),
        metadata=_mapping_or_empty(payload.get("metadata")),
    )


def list_research_links(item_id: str, *, include_inbound: bool = True) -> list[dict[str, Any]]:
    return repository.list_links(item_id, include_inbound=include_inbound)


def run_research_check(payload: Mapping[str, Any]) -> dict[str, Any]:
    request = dict(payload or {})
    check_family = str(request.get("check_family") or SUPPORTED_CHECK_FAMILY).strip()
    if check_family != SUPPORTED_CHECK_FAMILY:
        raise ValueError(f"unsupported research check family: {check_family}")
    title = str(request.get("title") or "").strip()
    if not title:
        raise ValueError("title is required")
    scope = _mapping(request.get("scope"), "scope")
    detector = _mapping(request.get("detector"), "detector")
    outcomes = _mapping_or_empty(request.get("outcomes"))
    observation = _resolve_observation(request, scope=scope, title=title)
    normalized_scope = _normalize_scope(scope)
    coverage = candle_service.preflight_candle_coverage_by_instrument(
        normalized_scope["instrument_id"],
        normalized_scope["start"],
        normalized_scope["end"],
        normalized_scope["timeframe"],
    )
    data_quality = _data_quality_from_coverage(coverage)
    if data_quality["status"] == "blocked":
        result = blocked_check_result(
            reason=str(coverage.get("message") or "candle coverage is blocked"),
            detector=detector,
            outcomes=outcomes,
            data_quality=data_quality,
        )
    else:
        try:
            candles = candle_service.fetch_ohlcv_by_instrument(
                normalized_scope["instrument_id"],
                normalized_scope["start"],
                normalized_scope["end"],
                normalized_scope["timeframe"],
            )
            if candles is None or candles.empty:
                result = blocked_check_result(
                    reason="no candles returned for research check window",
                    detector=detector,
                    outcomes=outcomes,
                    data_quality={**data_quality, "status": "blocked"},
                )
            else:
                result = evaluate_candle_event_check(
                    candles,
                    detector=detector,
                    outcomes=outcomes,
                    data_quality=data_quality,
                )
        except Exception as exc:  # noqa: BLE001 - stored as blocked analytical evidence.
            logger.warning(
                "research_check_candle_fetch_or_eval_blocked | title=%s instrument_id=%s timeframe=%s start=%s end=%s error=%s",
                title,
                normalized_scope["instrument_id"],
                normalized_scope["timeframe"],
                normalized_scope["start"],
                normalized_scope["end"],
                exc,
            )
            result = blocked_check_result(
                reason=f"research check evaluation failed: {exc}",
                detector=detector,
                outcomes=outcomes,
                data_quality={**data_quality, "status": "blocked"},
            )

    check_id = str(uuid.uuid4())
    normalized_request = {
        "schema_version": "research_check_request.v1",
        "check_family": check_family,
        "title": title,
        "body": request.get("body"),
        "observation_id": observation["id"],
        "scope": normalized_scope,
        "detector": detector,
        "outcomes": outcomes,
    }
    status = "tested" if result.get("status") == "completed" else "blocked"
    check_item = repository.create_item(
        item_id=check_id,
        kind="research_check",
        status=status,
        title=title,
        body=_optional(request.get("body")),
        instrument_id=normalized_scope.get("instrument_id"),
        symbol=normalized_scope.get("symbol"),
        timeframe=normalized_scope.get("timeframe"),
        datasource=normalized_scope.get("datasource"),
        exchange=normalized_scope.get("exchange"),
        window_start=normalized_scope.get("start"),
        window_end=normalized_scope.get("end"),
        tags=sorted(set(["research-check", *_tags(request.get("tags"))])),
        payload={
            "schema_version": "research_check_payload.v1",
            "request": normalized_request,
            "result": {**result, "check_id": check_id},
        },
        source_revision=_source_revision(),
    )
    link = repository.create_link(
        source_item_id=check_item["id"],
        target_type="research_item",
        target_id=observation["id"],
        relation="tests",
        metadata={"target_kind": observation.get("kind")},
    )
    return {
        "schema_version": "research_check_run.v1",
        "status": result.get("status"),
        "observation": observation,
        "check": check_item,
        "links": [link],
        "result": {**result, "check_id": check_id},
    }


def _resolve_observation(
    request: Mapping[str, Any],
    *,
    scope: Mapping[str, Any],
    title: str,
) -> dict[str, Any]:
    observation_id = str(request.get("observation_id") or "").strip()
    if observation_id:
        observation = repository.get_item(observation_id)
        if observation.get("kind") != "observation":
            raise ValueError("observation_id must reference an observation item")
        return observation
    raw_observation = request.get("observation")
    observation_payload = _mapping_or_empty(raw_observation)
    observation_title = str(observation_payload.get("title") or f"Ad hoc observation: {title}").strip()
    return repository.create_item(
        kind="observation",
        status="active",
        title=observation_title,
        body=_optional(observation_payload.get("body") or request.get("body")),
        instrument_id=_optional(scope.get("instrument_id")),
        symbol=_optional(scope.get("symbol")),
        timeframe=_optional(scope.get("timeframe")),
        datasource=_optional(scope.get("datasource")),
        exchange=_optional(scope.get("exchange")),
        window_start=scope.get("start"),
        window_end=scope.get("end"),
        tags=sorted(set(["auto-observation", *_tags(observation_payload.get("tags") or request.get("tags"))])),
        payload={
            "schema_version": "research_observation_payload.v1",
            "created_from": "research_check",
        },
        source_revision=_source_revision(),
    )


def _normalize_scope(scope: Mapping[str, Any]) -> dict[str, Any]:
    timeframe = str(scope.get("timeframe") or scope.get("interval") or "").strip()
    start = str(scope.get("start") or "").strip()
    end = str(scope.get("end") or "").strip()
    if not timeframe:
        raise ValueError("scope.timeframe is required")
    if not start or not end:
        raise ValueError("scope.start and scope.end are required")
    instrument_id = str(scope.get("instrument_id") or "").strip()
    symbol = _optional(scope.get("symbol"))
    datasource = _optional(scope.get("datasource"))
    exchange = _optional(scope.get("exchange"))
    if not instrument_id:
        if not symbol:
            raise ValueError("scope.instrument_id or scope.symbol is required")
        instrument_id = instrument_service.require_instrument_id(datasource, exchange, symbol)
    instrument = instrument_service.get_instrument_record(instrument_id)
    return {
        "instrument_id": instrument_id,
        "symbol": symbol or instrument.get("symbol"),
        "datasource": datasource or instrument.get("datasource"),
        "exchange": exchange or instrument.get("exchange"),
        "timeframe": timeframe,
        "start": start,
        "end": end,
    }


def _data_quality_from_coverage(coverage: Mapping[str, Any]) -> dict[str, Any]:
    status = str(coverage.get("status") or "").strip().lower()
    continuity = coverage.get("continuity") if isinstance(coverage.get("continuity"), Mapping) else {}
    continuity_status = str(continuity.get("final_status") or "unknown")
    if status == "error":
        quality_status = "blocked"
    elif status == "warning" or continuity_status in {"defect", "unknown", "missing"}:
        quality_status = "degraded"
    else:
        quality_status = "clean"
    return {
        "status": quality_status,
        "coverage_status": status or "unknown",
        "continuity_status": continuity_status,
        "instrument_id": coverage.get("instrument_id"),
        "provider": coverage.get("provider"),
        "exchange": coverage.get("exchange"),
        "symbol": coverage.get("symbol"),
        "timeframe": coverage.get("timeframe"),
        "row_count": coverage.get("row_count"),
        "missing_ranges": list(coverage.get("missing_ranges") or []),
        "message": coverage.get("message"),
        "coverage": dict(coverage),
    }


def _normalize_choice(value: Any, label: str, allowed: set[str]) -> str:
    normalized = str(value or "").strip()
    if normalized not in allowed:
        raise ValueError(f"unsupported {label}: {normalized or '<empty>'}")
    return normalized


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("expected object payload")
    return dict(value)


def _tags(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw = [item.strip() for item in value.split(",")]
    elif isinstance(value, list):
        raw = [str(item).strip() for item in value]
    else:
        raise ValueError("tags must be a list or comma-separated string")
    return [item for item in raw if item]


def _optional(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _source_revision() -> str:
    return source_revision()

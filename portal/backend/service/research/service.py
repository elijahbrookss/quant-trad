"""Research memory service and check orchestration."""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Mapping
import uuid

from portal.backend.service.indicators.indicator_service.runtime_validation import (
    collect_runtime_output_evidence_for_instance,
)
from portal.backend.service.market import candle_service, instrument_service
from portal.backend.service.provenance import source_revision
from portal.backend.service.reports import contract as reports_contract

from . import repository
from .checks import (
    INDICATOR_FORWARD_OUTCOME,
    RAW_FORWARD_OUTCOME,
    RUN_DECISION_TRADE_COMPARISON,
    RUN_SIGNAL_SUMMARY,
    SUPPORTED_CHECK_FAMILY,
    SUPPORTED_CHECK_FAMILIES,
    blocked_check_result,
    evaluate_indicator_forward_outcome,
    evaluate_raw_event_check,
    evaluate_run_decision_trade_comparison,
    evaluate_run_signal_summary,
    normalize_run_signal_records,
    validate_check_detector,
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


def get_research_trail(item_id: str) -> dict[str, Any]:
    item = repository.get_item(item_id)
    links: list[dict[str, Any]] = []
    related_items: list[dict[str, Any]] = []
    runs: list[dict[str, Any]] = []
    seen_links: set[str] = set()
    seen_items: set[str] = set()
    seen_runs: set[str] = set()

    def add_link(link: Mapping[str, Any]) -> None:
        link_key = str(link.get("id") or "").strip()
        if not link_key:
            link_key = "|".join(
                str(link.get(key) or "")
                for key in ("source_item_id", "target_type", "target_id", "relation")
            )
        if link_key in seen_links:
            return
        seen_links.add(link_key)
        links.append(dict(link))

    def add_related(related_id: str) -> None:
        normalized = str(related_id or "").strip()
        if not normalized or normalized == item_id or normalized in seen_items:
            return
        related_items.append(repository.get_item(normalized))
        seen_items.add(normalized)

    def collect_link(link: Mapping[str, Any]) -> None:
        add_link(link)
        source_id = str(link.get("source_item_id") or "").strip()
        target_type = str(link.get("target_type") or "").strip()
        target_id = str(link.get("target_id") or "").strip()
        if target_type == "research_item":
            add_related(source_id)
            add_related(target_id)
        elif source_id != item_id:
            add_related(source_id)
        if link.get("target_type") == "run":
            if target_id and target_id not in seen_runs:
                runs.append(_run_evidence_summary(target_id))
                seen_runs.add(target_id)

    for link in repository.list_links(item_id, include_inbound=True):
        collect_link(link)
    for related_id in list(seen_items):
        for link in repository.list_links(related_id, include_inbound=False):
            collect_link(link)

    checks = [
        related
        for related in related_items
        if str(related.get("kind") or "") == "research_check"
    ]
    observations = [
        related
        for related in related_items
        if str(related.get("kind") or "") == "observation"
    ]
    hypotheses = [
        related
        for related in related_items
        if str(related.get("kind") or "") == "hypothesis"
    ]
    return {
        "schema_version": "research_trail.v1",
        "item": item,
        "links": links,
        "related_items": related_items,
        "observations": observations,
        "checks": checks,
        "hypotheses": hypotheses,
        "runs": runs,
        "summary": {
            "link_count": len(links),
            "related_item_count": len(related_items),
            "check_count": len(checks),
            "observation_count": len(observations),
            "hypothesis_count": len(hypotheses),
            "run_count": len(runs),
        },
    }


def get_run_research_evidence(run_id: str) -> dict[str, Any]:
    return _run_evidence_summary(run_id, include_dataset_context=True)


def compare_research_checks(left_check_id: str, right_check_id: str) -> dict[str, Any]:
    left = repository.get_item(left_check_id)
    right = repository.get_item(right_check_id)
    if left.get("kind") != "research_check" or right.get("kind") != "research_check":
        raise ValueError("research compare requires two research_check items")
    left_result = _check_result(left)
    right_result = _check_result(right)
    left_family = str(left_result.get("check_family") or "")
    right_family = str(right_result.get("check_family") or "")
    if left_family != right_family:
        raise ValueError(f"check families differ: {left_family} != {right_family}")
    return {
        "schema_version": "research_check_comparison.v1",
        "check_family": left_family,
        "left": _check_comparison_side(left, left_result),
        "right": _check_comparison_side(right, right_result),
        "deltas": {
            "sample_count": _numeric_delta(left_result.get("sample_count"), right_result.get("sample_count")),
            "eligible_bars": _numeric_delta(left_result.get("eligible_bars"), right_result.get("eligible_bars")),
            "eligible_events": _numeric_delta(left_result.get("eligible_events"), right_result.get("eligible_events")),
            "eligible_decisions": _numeric_delta(left_result.get("eligible_decisions"), right_result.get("eligible_decisions")),
            "recommendation_changed": left_result.get("recommendation") != right_result.get("recommendation"),
            "status_changed": left_result.get("status") != right_result.get("status"),
            "forward_summary": _forward_summary_delta(left_result, right_result),
        },
    }


def run_research_check(payload: Mapping[str, Any]) -> dict[str, Any]:
    request = dict(payload or {})
    check_family = str(request.get("check_family") or SUPPORTED_CHECK_FAMILY).strip()
    if check_family not in SUPPORTED_CHECK_FAMILIES:
        raise ValueError(f"unsupported research check family: {check_family}")
    title = str(request.get("title") or "").strip()
    if not title:
        raise ValueError("title is required")
    scope = _mapping(request.get("scope"), "scope")
    detector = _mapping(request.get("detector"), "detector")
    outcomes = _mapping_or_empty(request.get("outcomes"))
    validate_check_detector(check_family=check_family, detector=detector)
    existing_observation = _existing_observation(request)
    if check_family == RAW_FORWARD_OUTCOME:
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
                check_family=check_family,
            )
        else:
            try:
                candles = candle_service.fetch_ohlcv_by_instrument(
                    normalized_scope["instrument_id"],
                    normalized_scope["start"],
                    normalized_scope["end"],
                    normalized_scope["timeframe"],
                )
            except Exception as exc:  # noqa: BLE001 - source data unavailability is analytical evidence.
                logger.warning(
                    "research_check_candle_fetch_blocked | title=%s instrument_id=%s timeframe=%s start=%s end=%s error=%s",
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
                    check_family=check_family,
                )
            else:
                if candles is None or candles.empty:
                    result = blocked_check_result(
                        reason="no candles returned for research check window",
                        detector=detector,
                        outcomes=outcomes,
                        data_quality={**data_quality, "status": "blocked"},
                        check_family=check_family,
                    )
                else:
                    result = evaluate_raw_event_check(
                        candles,
                        detector=detector,
                        outcomes=outcomes,
                        data_quality=data_quality,
                    )
    elif check_family == INDICATOR_FORWARD_OUTCOME:
        normalized_scope = _normalize_indicator_scope(scope)
        coverage = candle_service.preflight_candle_coverage_by_instrument(
            normalized_scope["instrument_id"],
            normalized_scope["start"],
            normalized_scope["end"],
            normalized_scope["timeframe"],
        )
        data_quality = _data_quality_from_coverage(coverage)
        if data_quality["status"] == "blocked":
            result = blocked_check_result(
                reason=str(coverage.get("message") or "indicator check source candle coverage is blocked"),
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
                check_family=check_family,
            )
        else:
            try:
                evidence = collect_runtime_output_evidence_for_instance(
                    normalized_scope["indicator_id"],
                    normalized_scope["start"],
                    normalized_scope["end"],
                    normalized_scope["timeframe"],
                    symbol=normalized_scope.get("symbol"),
                    datasource=normalized_scope.get("datasource"),
                    exchange=normalized_scope.get("exchange"),
                    instrument_id=normalized_scope.get("instrument_id"),
                )
            except LookupError as exc:
                logger.warning(
                    "research_check_indicator_evidence_blocked | title=%s indicator_id=%s instrument_id=%s timeframe=%s start=%s end=%s error=%s",
                    title,
                    normalized_scope["indicator_id"],
                    normalized_scope["instrument_id"],
                    normalized_scope["timeframe"],
                    normalized_scope["start"],
                    normalized_scope["end"],
                    exc,
                )
                result = blocked_check_result(
                    reason=f"indicator check evidence failed: {exc}",
                    detector=detector,
                    outcomes=outcomes,
                    data_quality={**data_quality, "status": "blocked"},
                    check_family=check_family,
                )
            else:
                result = evaluate_indicator_forward_outcome(
                    evidence,
                    detector=detector,
                    outcomes=outcomes,
                    data_quality=data_quality,
                )
    else:
        normalized_scope = _normalize_report_scope(scope)
        dataset = reports_contract.get_run_research_dataset(normalized_scope["run_id"])
        normalized_scope = _merge_scope_context(normalized_scope, _report_scope_context(dataset))
        data_quality = _data_quality_from_report_dataset(dataset)
        if data_quality["status"] == "blocked":
            logger.warning(
                "research_check_report_data_blocked | title=%s run_id=%s check_family=%s readiness_status=%s",
                title,
                normalized_scope["run_id"],
                check_family,
                data_quality.get("readiness_status"),
            )
            result = blocked_check_result(
                reason=str(data_quality.get("readiness_status") or "run research dataset is not analyzable"),
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
                check_family=check_family,
            )
        elif check_family == RUN_SIGNAL_SUMMARY:
            result = evaluate_run_signal_summary(
                dataset,
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        elif check_family == RUN_DECISION_TRADE_COMPARISON:
            result = evaluate_run_decision_trade_comparison(
                dataset,
                detector=detector,
                outcomes=outcomes,
                data_quality=data_quality,
            )
        else:
            raise ValueError(f"unsupported research check family: {check_family}")

    observation = existing_observation or _create_auto_observation(
        request,
        scope=normalized_scope,
        title=title,
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
    links = [
        repository.create_link(
            source_item_id=check_item["id"],
            target_type="research_item",
            target_id=observation["id"],
            relation="tests",
            metadata={"target_kind": observation.get("kind")},
        )
    ]
    if normalized_scope.get("run_id"):
        links.append(
            repository.create_link(
                source_item_id=check_item["id"],
                target_type="run",
                target_id=str(normalized_scope["run_id"]),
                relation="analyzes",
                metadata={"check_family": check_family},
            )
        )
    return {
        "schema_version": "research_check_run.v1",
        "status": result.get("status"),
        "observation": observation,
        "check": check_item,
        "links": links,
        "result": {**result, "check_id": check_id},
    }

def _existing_observation(request: Mapping[str, Any]) -> dict[str, Any] | None:
    observation_id = str(request.get("observation_id") or "").strip()
    if observation_id:
        observation = repository.get_item(observation_id)
        if observation.get("kind") != "observation":
            raise ValueError("observation_id must reference an observation item")
        return observation
    return None


def _create_auto_observation(
    request: Mapping[str, Any],
    *,
    scope: Mapping[str, Any],
    title: str,
) -> dict[str, Any]:
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
            "scope": dict(scope),
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


def _normalize_indicator_scope(scope: Mapping[str, Any]) -> dict[str, Any]:
    indicator_id = str(scope.get("indicator_id") or "").strip()
    if not indicator_id:
        raise ValueError("scope.indicator_id is required for indicator research checks")
    normalized = _normalize_scope(scope)
    normalized["indicator_id"] = indicator_id
    return normalized


def _normalize_report_scope(scope: Mapping[str, Any]) -> dict[str, Any]:
    run_id = str(scope.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("scope.run_id is required for report-backed research checks")
    return {
        "run_id": run_id,
        "symbol": _optional(scope.get("symbol")),
        "timeframe": _optional(scope.get("timeframe")),
        "start": scope.get("start"),
        "end": scope.get("end"),
    }


def _merge_scope_context(scope: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(scope)
    for key, value in context.items():
        if value is None:
            continue
        if key in {"symbols", "timeframes", "instrument_ids"}:
            merged[key] = list(value) if isinstance(value, list) else value
            continue
        if merged.get(key) in (None, ""):
            merged[key] = value
    return merged


def _report_scope_context(dataset: Mapping[str, Any]) -> dict[str, Any]:
    metadata = dataset.get("metadata") if isinstance(dataset.get("metadata"), Mapping) else {}
    simulated_window = metadata.get("simulated_window") if isinstance(metadata.get("simulated_window"), Mapping) else {}
    symbols = [str(symbol) for symbol in metadata.get("symbols") or [] if str(symbol or "").strip()]
    timeframes = [str(timeframe) for timeframe in metadata.get("timeframes") or [] if str(timeframe or "").strip()]
    instrument_ids = [str(item) for item in metadata.get("instrument_ids") or [] if str(item or "").strip()]
    return {
        "symbol": symbols[0] if len(symbols) == 1 else metadata.get("symbol"),
        "symbols": symbols,
        "instrument_ids": instrument_ids,
        "timeframe": metadata.get("timeframe") or (timeframes[0] if len(timeframes) == 1 else None),
        "timeframes": timeframes,
        "start": simulated_window.get("start"),
        "end": simulated_window.get("end"),
        "strategy_id": metadata.get("strategy_id"),
        "bot_id": metadata.get("bot_id"),
        "datasource": metadata.get("datasource") or metadata.get("provider"),
        "exchange": metadata.get("exchange"),
    }


def _run_evidence_summary(run_id: str, *, include_dataset_context: bool = False) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    dataset = reports_contract.get_run_research_dataset(normalized_run_id)
    metadata = dataset.get("metadata") if isinstance(dataset.get("metadata"), Mapping) else {}
    readiness = dataset.get("readiness") if isinstance(dataset.get("readiness"), Mapping) else {}
    summary = dataset.get("summary") if isinstance(dataset.get("summary"), Mapping) else {}
    signals = normalize_run_signal_records(dataset.get("signals"))
    decisions = [dict(row) for row in dataset.get("decisions") or [] if isinstance(row, Mapping)]
    trades = [dict(row) for row in dataset.get("trades") or [] if isinstance(row, Mapping)]
    output_names: Counter[str] = Counter()
    event_keys: Counter[str] = Counter()
    decision_states: Counter[str] = Counter()
    reason_codes: Counter[str] = Counter()
    for signal in signals:
        for output_name in signal.get("output_names") or [signal.get("output_name")]:
            text = str(output_name or "").strip()
            if text:
                output_names[text] += 1
        for event_key in signal.get("event_keys") or [signal.get("event_key")]:
            text = str(event_key or "").strip()
            if text:
                event_keys[text] += 1
    for decision in decisions:
        state = _decision_state(decision)
        if state:
            decision_states[state] += 1
        reason = str(decision.get("reason_code") or decision.get("reason") or "").strip()
        if reason:
            reason_codes[reason] += 1

    payload = {
        "schema_version": "run_research_evidence.v1",
        "run_id": normalized_run_id,
        "metadata": {
            "bot_id": metadata.get("bot_id"),
            "strategy_id": metadata.get("strategy_id"),
            "symbols": list(metadata.get("symbols") or []),
            "instrument_ids": list(metadata.get("instrument_ids") or []),
            "timeframe": metadata.get("timeframe"),
            "simulated_window": dict(metadata.get("simulated_window") or {}),
            "datasource": metadata.get("datasource") or metadata.get("provider"),
            "exchange": metadata.get("exchange"),
        },
        "readiness": {
            "dataset_status": readiness.get("dataset_status") or readiness.get("reason"),
            "safe_to_compare": bool(readiness.get("safe_to_compare", False)),
            "caveats": list(readiness.get("caveats") or []),
        },
        "counts": {
            "signals": len(signals),
            "decisions": len(decisions),
            "trades": len(trades),
            "accepted_decisions": summary.get("accepted_decisions"),
            "rejected_decisions": summary.get("rejected_decisions"),
            "closed_trades": summary.get("closed_trades") or summary.get("trades"),
            "open_trades": summary.get("open_trades"),
        },
        "signals": {
            "output_names": dict(sorted(output_names.items())),
            "event_keys": dict(sorted(event_keys.items())),
        },
        "decisions": {
            "states": dict(sorted(decision_states.items())),
            "reason_codes": dict(sorted(reason_codes.items())),
        },
        "supported_checks": [
            {
                "command": "qt research check signal",
                "check_family": RUN_SIGNAL_SUMMARY,
                "requires": ["run_id", "output_name or event_key"],
            },
            {
                "command": "qt research check decision",
                "check_family": RUN_DECISION_TRADE_COMPARISON,
                "requires": ["run_id", "state or reason_code"],
            },
        ],
    }
    if include_dataset_context:
        payload["data_quality"] = _data_quality_from_report_dataset(dataset)
    return payload


def _check_result(item: Mapping[str, Any]) -> dict[str, Any]:
    payload = item.get("payload") if isinstance(item.get("payload"), Mapping) else {}
    result = payload.get("result") if isinstance(payload.get("result"), Mapping) else {}
    return dict(result)


def _check_comparison_side(item: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "title": item.get("title"),
        "status": result.get("status"),
        "sample_count": result.get("sample_count"),
        "eligible_bars": result.get("eligible_bars"),
        "eligible_events": result.get("eligible_events"),
        "eligible_decisions": result.get("eligible_decisions"),
        "recommendation": result.get("recommendation"),
        "detector": dict(result.get("detector") or {}),
        "outcomes": dict(result.get("outcomes") or {}),
    }


def _numeric_delta(left: Any, right: Any) -> dict[str, Any] | None:
    left_number = _float_or_none(left)
    right_number = _float_or_none(right)
    if left_number is None and right_number is None:
        return None
    delta = None if left_number is None or right_number is None else right_number - left_number
    return {"left": left, "right": right, "delta": delta}


def _forward_summary_delta(left_result: Mapping[str, Any], right_result: Mapping[str, Any]) -> dict[str, Any]:
    left_outcomes = left_result.get("outcomes") if isinstance(left_result.get("outcomes"), Mapping) else {}
    right_outcomes = right_result.get("outcomes") if isinstance(right_result.get("outcomes"), Mapping) else {}
    left_summary = left_outcomes.get("summary") if isinstance(left_outcomes.get("summary"), Mapping) else {}
    right_summary = right_outcomes.get("summary") if isinstance(right_outcomes.get("summary"), Mapping) else {}
    deltas: dict[str, Any] = {}
    for window in sorted(set(left_summary) | set(right_summary), key=str):
        left_window = left_summary.get(window) if isinstance(left_summary.get(window), Mapping) else {}
        right_window = right_summary.get(window) if isinstance(right_summary.get(window), Mapping) else {}
        window_delta: dict[str, Any] = {}
        for key in sorted(set(left_window) | set(right_window), key=str):
            delta = _numeric_delta(left_window.get(key), right_window.get(key))
            if delta is not None:
                window_delta[key] = delta
        if window_delta:
            deltas[str(window)] = window_delta
    return deltas


def _float_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _decision_state(record: Mapping[str, Any]) -> str | None:
    for key in ("decision_state", "state", "status", "decision"):
        value = str(record.get(key) or "").strip().lower()
        if value:
            return value
    if record.get("accepted") is True:
        return "accepted"
    if record.get("rejected") is True:
        return "rejected"
    return None


def _data_quality_from_report_dataset(dataset: Mapping[str, Any]) -> dict[str, Any]:
    readiness = dataset.get("readiness") if isinstance(dataset.get("readiness"), Mapping) else {}
    diagnostics = dataset.get("diagnostics") if isinstance(dataset.get("diagnostics"), Mapping) else {}
    readiness_status = str(readiness.get("dataset_status") or readiness.get("reason") or "unknown").strip()
    safe_to_compare = bool(readiness.get("safe_to_compare", False))
    quality_status = "clean" if safe_to_compare else "degraded"
    if readiness_status in {"missing", "blocked", "error", "failed"}:
        quality_status = "blocked"
    return {
        "status": quality_status,
        "readiness_status": readiness_status,
        "safe_to_compare": safe_to_compare,
        "caveats": list(readiness.get("caveats") or []),
        "diagnostic_summary": dict(diagnostics.get("summary") or {}),
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

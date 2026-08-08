"""Research memory service and check orchestration."""

from __future__ import annotations

import logging
import uuid
from collections import Counter
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from time import perf_counter
from typing import Any, Mapping

from sqlalchemy.orm import Session
from research_science.check import (
    CHECK_MODE_EVIDENCE,
    CHECK_MODE_PREVIEW,
    CheckDefinition,
    CheckRequest,
    CheckResult,
    ResolvedCheckPlan,
    verify_check_replay,
)

from portal.backend.service.bots.startup_lifecycle import (
    is_active_run_state,
    is_terminal_run_state,
)

from portal.backend.service.indicators.indicator_service.runtime_validation import (
    collect_runtime_output_evidence_for_instance,
)
from portal.backend.service.market import candle_service, instrument_service
from portal.backend.service.provenance import source_revision
from portal.backend.service.reports import contract as reports_contract
from portal.backend.service.storage.repos import lifecycle as lifecycle_repo
from portal.backend.service.storage.repos import runs as runs_repo
from market_data.frozen import semantic_hash

from . import repository
from .checks import (
    CANDIDATE_LIFECYCLE,
    INDICATOR_FORWARD_OUTCOME,
    RAW_FORWARD_OUTCOME,
    RUN_DECISION_TRADE_COMPARISON,
    RUN_SIGNAL_SUMMARY,
    SIGNAL_AUDIT,
    SUPPORTED_CHECK_FAMILY,
    SUPPORTED_CHECK_FAMILIES,
    blocked_check_result,
    evaluate_candidate_lifecycle,
    evaluate_indicator_forward_outcome,
    evaluate_raw_event_check,
    evaluate_run_decision_trade_comparison,
    evaluate_run_signal_summary,
    evaluate_signal_audit,
    normalize_run_signal_records,
    validate_check_detector,
)
from .metrics import build_leaderboard, extract_numeric_metrics
from .execution import execute_check_evidence, execute_check_preview
from .planning import plan_research_check
from .registry import normalize_check_request


logger = logging.getLogger(__name__)

RESEARCH_ITEM_KINDS = {"observation", "research_check", "hypothesis", "study"}
RESEARCH_ITEM_STATUSES = {"draft", "active", "tested", "promoted", "rejected", "archived", "blocked"}
INDICATOR_CHECK_FAMILIES = {INDICATOR_FORWARD_OUTCOME, SIGNAL_AUDIT, CANDIDATE_LIFECYCLE}

RESEARCH_ACTIVITY_TYPES: dict[str, tuple[str, tuple[str, ...], str]] = {
    "checks_completed": (
        "research_check",
        ("tested", "blocked"),
        "Research checks persisted after evaluation; created_at is the completion day.",
    ),
    "hypotheses_created": (
        "hypothesis",
        (),
        "Hypotheses use the UTC day of their persisted created_at timestamp.",
    ),
    "observations_recorded": (
        "observation",
        (),
        "Observations use the UTC day of their persisted created_at timestamp.",
    ),
}
_RESEARCH_ACTIVITY_MAX_DAYS = 366


class ResearchEvaluationCache:
    """Per-request cache for repeated research check evaluation."""

    def __init__(self) -> None:
        self.coverage_by_scope: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self.candles_by_scope: dict[tuple[str, str, str, str], Any] = {}
        self.source_frames_by_request: dict[tuple[str, ...], Any] = {}
        self.stats: Counter[str] = Counter()

    def coverage(self, scope: Mapping[str, Any]) -> dict[str, Any]:
        key = _scope_cache_key(scope)
        if key in self.coverage_by_scope:
            self.stats["coverage_hits"] += 1
            return self.coverage_by_scope[key]
        self.stats["coverage_misses"] += 1
        coverage = candle_service.preflight_candle_coverage_by_instrument(
            scope["instrument_id"],
            scope["start"],
            scope["end"],
            scope["timeframe"],
        )
        self.coverage_by_scope[key] = dict(coverage)
        return dict(coverage)

    def candles(self, scope: Mapping[str, Any]) -> Any:
        key = _scope_cache_key(scope)
        if key in self.candles_by_scope:
            self.stats["candle_hits"] += 1
            return self.candles_by_scope[key]
        self.stats["candle_misses"] += 1
        frame = candle_service.fetch_ohlcv_by_instrument(
            scope["instrument_id"],
            scope["start"],
            scope["end"],
            scope["timeframe"],
        )
        self.candles_by_scope[key] = frame
        return frame

    def snapshot(self) -> dict[str, Any]:
        return {
            "coverage_entries": len(self.coverage_by_scope),
            "candle_entries": len(self.candles_by_scope),
            "source_frame_entries": len(self.source_frames_by_request),
            "stats": dict(sorted(self.stats.items())),
        }


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
    return _project_evidence_classification(repository.get_item(item_id))


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
    return [
        _project_evidence_classification(item)
        for item in repository.list_items(
            kind=kind,
            status=status,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
    ]


def _project_evidence_classification(item: Mapping[str, Any]) -> dict[str, Any]:
    projected = dict(item)
    if str(projected.get("kind") or "") != "research_check":
        return projected
    payload = projected.get("payload")
    schema_version = (
        str(payload.get("schema_version") or "")
        if isinstance(payload, Mapping)
        else ""
    )
    if schema_version == "research_check_payload.v2":
        projected["evidence_classification"] = str(
            payload.get("evidence_classification") or "frozen_replayable"
        )
        projected["replayable"] = bool(payload.get("replayable", True))
        projected["observation_eligible"] = bool(
            payload.get("observation_eligible", False)
        )
    else:
        projected["evidence_classification"] = "legacy_unpinned"
        projected["replayable"] = False
        projected["observation_eligible"] = False
    return projected


def get_research_activity(
    *,
    activity_type: str = "checks_completed",
    days: int = 182,
) -> dict[str, Any]:
    """Return one complete, zero-filled UTC research-activity series."""

    normalized_type = str(activity_type or "checks_completed").strip().lower()
    definition = RESEARCH_ACTIVITY_TYPES.get(normalized_type)
    if definition is None:
        raise ValueError(
            "unsupported research activity type: "
            f"{normalized_type or '<empty>'}; expected one of "
            f"{', '.join(sorted(RESEARCH_ACTIVITY_TYPES))}"
        )
    kind, statuses, description = definition
    bounded_days = max(1, min(int(days or 182), _RESEARCH_ACTIVITY_MAX_DAYS))
    today = datetime.now(UTC).date()
    since_date = today - timedelta(days=bounded_days - 1)
    since = datetime.combine(
        since_date,
        datetime.min.time(),
        tzinfo=UTC,
    ).replace(tzinfo=None)
    rows = repository.count_items_by_day(
        kind=kind,
        statuses=statuses,
        since=since,
    )

    by_day: dict[date, dict[str, int]] = defaultdict(dict)
    for row in rows:
        raw_day = row.get("day")
        day_value = raw_day.date() if isinstance(raw_day, datetime) else raw_day
        if isinstance(day_value, date):
            by_day[day_value][str(row.get("status") or "unknown")] = int(
                row.get("total") or 0
            )

    payload_days: list[dict[str, Any]] = []
    cursor = since_date
    while cursor <= today:
        by_status = dict(sorted(by_day.get(cursor, {}).items()))
        payload_days.append(
            {
                "date": cursor.isoformat(),
                "total": sum(by_status.values()),
                "by_status": by_status,
            }
        )
        cursor += timedelta(days=1)

    return {
        "schema_version": "research_activity.v1",
        "activity_type": normalized_type,
        "kind": kind,
        "qualifying_statuses": list(statuses),
        "timestamp_field": "created_at",
        "timezone": "UTC",
        "description": description,
        "since": since_date.isoformat(),
        "days": payload_days,
    }


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
    item = _project_evidence_classification(repository.get_item(item_id))
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
        related_items.append(
            _project_evidence_classification(repository.get_item(normalized))
        )
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


def _active_run_research_evidence(run_id: str) -> dict[str, Any] | None:
    run = runs_repo.get_bot_run(run_id) or {}
    if not isinstance(run, Mapping) or not run:
        return None
    lifecycle = lifecycle_repo.get_bot_run_lifecycle(run_id) or {}
    persisted_status = str(run.get("status") or "").strip().lower()
    phase = str(lifecycle.get("phase") or "").strip().lower()
    lifecycle_status = str(lifecycle.get("status") or "").strip().lower()
    if is_terminal_run_state(status=persisted_status, phase=phase):
        return None
    if not is_active_run_state(
        status=lifecycle_status or persisted_status,
        phase=phase,
    ):
        return None
    summary = run.get("summary") if isinstance(run.get("summary"), Mapping) else {}
    config = run.get("config_snapshot") if isinstance(run.get("config_snapshot"), Mapping) else {}
    symbols = config.get("symbols") if isinstance(config.get("symbols"), list) else []
    return {
        "schema_version": "run_research_evidence.v1",
        "run_id": run_id,
        "metadata": {
            "bot_id": run.get("bot_id"),
            "strategy_id": config.get("strategy_id"),
            "symbols": list(symbols),
            "instrument_ids": [],
            "timeframe": config.get("timeframe"),
            "simulated_window": {},
            "datasource": config.get("datasource") or config.get("provider"),
            "exchange": config.get("exchange"),
        },
        "readiness": {
            "dataset_status": "deferred_while_run_active",
            "safe_to_compare": False,
            "caveats": ["Research evidence is materialized after the run reaches a terminal state."],
        },
        "counts": {
            "signals": None,
            "decisions": None,
            "trades": (
                summary.get("total_trades")
                if summary.get("total_trades") is not None
                else summary.get("trades")
            ),
            "accepted_decisions": None,
            "rejected_decisions": None,
            "closed_trades": (
                summary.get("closed_trades")
                if summary.get("closed_trades") is not None
                else summary.get("trades")
            ),
            "open_trades": summary.get("open_trades"),
        },
        "signals": {"output_names": {}, "event_keys": {}},
        "decisions": {"states": {}, "reason_codes": {}},
        "supported_checks": [],
        "data_quality": {"status": "deferred_while_run_active"},
    }


def get_run_research_evidence(run_id: str) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    active_summary = _active_run_research_evidence(normalized_run_id)
    if active_summary is not None:
        return active_summary
    return _run_evidence_summary(normalized_run_id, include_dataset_context=True)


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


def _evaluate_legacy_research_check(
    payload: Mapping[str, Any],
    *,
    cache: ResearchEvaluationCache | None = None,
) -> dict[str, Any]:
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
    normalized_scope, result = _evaluate_research_check_request(
        title=title,
        check_family=check_family,
        scope=scope,
        detector=detector,
        outcomes=outcomes,
        cache=cache,
    )
    return {
        "schema_version": "research_check_evaluation.v1",
        "status": result.get("status"),
        "check_family": check_family,
        "scope": normalized_scope,
        "detector": detector,
        "outcomes": outcomes,
        "result": result,
    }


def get_research_check_requirements(payload: Mapping[str, Any]) -> dict[str, Any]:
    request_payload, _run_evidence = _prepare_check_request_payload(
        payload, mode=str(payload.get("mode") or CHECK_MODE_PREVIEW)
    )
    definition, request = normalize_check_request(request_payload)
    plan = plan_research_check(
        definition,
        request,
        inspect_coverage=request.mode == CHECK_MODE_PREVIEW,
    )
    return {
        "schema_version": "research_check_requirements.v1",
        "mode": request.mode,
        "definition": definition.to_dict(),
        "request": request.to_dict(),
        "plan": plan.to_dict(),
        "provider_call_performed": False,
        "preparation_authorized": False,
    }


def evaluate_research_check(
    payload: Mapping[str, Any],
    *,
    cache: ResearchEvaluationCache | None = None,
) -> dict[str, Any]:
    """Run a watermark-pinned ephemeral preview; never persist evidence."""

    _ = cache
    request_payload, run_evidence = _prepare_check_request_payload(
        {**dict(payload or {}), "mode": CHECK_MODE_PREVIEW},
        mode=CHECK_MODE_PREVIEW,
    )
    definition, request = normalize_check_request(
        request_payload, mode=CHECK_MODE_PREVIEW
    )
    plan = plan_research_check(definition, request, inspect_coverage=True)
    return execute_check_preview(
        definition, request, plan, run_evidence=run_evidence
    )


def run_research_check(
    payload: Mapping[str, Any],
    *,
    session: Session | None = None,
) -> dict[str, Any]:
    """Run preview by default or persist explicit frozen evidence."""

    mode = str(payload.get("mode") or CHECK_MODE_PREVIEW).strip().lower()
    request_payload, run_evidence = _prepare_check_request_payload(
        payload, mode=mode
    )
    definition, request = normalize_check_request(request_payload, mode=mode)
    if mode == CHECK_MODE_PREVIEW:
        plan = plan_research_check(definition, request, inspect_coverage=True)
        preview = execute_check_preview(
            definition, request, plan, run_evidence=run_evidence
        )
        return {
            **preview,
            "compatibility": {
                "route": "/api/research/checks/run",
                "status": "deprecated_for_preview",
                "replacement": "/api/research/checks/evaluate",
                "message": (
                    "Unqualified Check runs are now ephemeral previews; set mode=evidence "
                    "and provide dataset_id for durable evidence."
                ),
            },
        }
    plan = plan_research_check(definition, request, inspect_coverage=False)
    bound_plan, evidence, result = execute_check_evidence(
        definition, request, plan, run_evidence=run_evidence
    )
    return persist_research_check_evidence(
        request_payload,
        definition=definition,
        request=request,
        plan=bound_plan,
        evidence=evidence.to_dict(),
        result=result.to_dict(),
        session=session,
    )


def persist_research_check(
    payload: Mapping[str, Any],
    *,
    evaluation: Mapping[str, Any],
    session: Session | None = None,
) -> dict[str, Any]:
    """Persist a precomputed check, optionally inside an owned job transaction."""

    request = dict(payload or {})
    existing_observation = _existing_observation(request, session=session)
    check_family = evaluation["check_family"]
    title = str(request.get("title") or "").strip()
    detector = dict(evaluation["detector"])
    outcomes = dict(evaluation["outcomes"])
    normalized_scope = dict(evaluation["scope"])
    result = dict(evaluation["result"])

    observation = existing_observation or _create_auto_observation(
        request,
        scope=normalized_scope,
        title=title,
        session=session,
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
        **({"session": session} if session is not None else {}),
    )
    links = [
        repository.create_link(
            source_item_id=check_item["id"],
            target_type="research_item",
            target_id=observation["id"],
            relation="tests",
            metadata={"target_kind": observation.get("kind")},
            **({"session": session} if session is not None else {}),
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
                **({"session": session} if session is not None else {}),
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


def persist_research_check_evidence(
    payload: Mapping[str, Any],
    *,
    definition: CheckDefinition,
    request: CheckRequest,
    plan: ResolvedCheckPlan,
    evidence: Mapping[str, Any],
    result: Mapping[str, Any],
    session: Session | None = None,
) -> dict[str, Any]:
    """Persist one replayable v2 Check without auto-creating an Observation."""

    if request.mode != CHECK_MODE_EVIDENCE:
        raise ValueError("check_evidence_persistence_requires_evidence_mode")
    normalized_result = CheckResult.from_dict(result)
    result_payload = dict(normalized_result.result)
    status = "tested" if result_payload.get("status") == "completed" else "blocked"
    scope = dict(request.scope)
    check_id = str(uuid.uuid4())
    create_args = {"session": session} if session is not None else {}
    item = repository.create_item(
        item_id=check_id,
        kind="research_check",
        status=status,
        title=str(payload.get("title") or definition.definition_id).strip(),
        body=_optional(payload.get("body")),
        instrument_id=_optional(scope.get("instrument_id")),
        symbol=_optional(scope.get("symbol")),
        timeframe=_optional(scope.get("timeframe") or scope.get("interval")),
        datasource=_optional(scope.get("datasource")),
        exchange=_optional(scope.get("exchange")),
        window_start=scope.get("start"),
        window_end=scope.get("end"),
        tags=sorted(set(["research-check", "frozen-evidence", *_tags(payload.get("tags"))])),
        payload={
            "schema_version": "research_check_payload.v2",
            "evidence_classification": "frozen_replayable",
            "definition": definition.to_dict(),
            "request": request.to_dict(),
            "plan": plan.to_dict(),
            "evidence": dict(evidence),
            "result": normalized_result.to_dict(),
            "replayable": True,
            "observation_eligible": status == "tested",
        },
        source_revision=str(evidence.get("code_revision") or _source_revision()),
        **create_args,
    )
    target_type = (
        "run"
        if str(evidence.get("evidence_kind") or "")
        == "immutable_run_evidence"
        else "market_dataset"
    )
    target_id = (
        str(evidence.get("input_binding", {}).get("run_id") or "")
        if target_type == "run"
        else str(request.dataset_id)
    )
    links = [
        repository.create_link(
            source_item_id=item["id"],
            target_type=target_type,
            target_id=target_id,
            relation="uses_evidence",
            metadata={
                "dataset_hash": evidence.get("input_binding", {}).get(
                    "dataset_hash"
                ),
                "binding_hash": evidence.get("input_binding", {}).get(
                    "binding_hash"
                ),
                "result_hash": normalized_result.result_hash,
            },
            **create_args,
        )
    ]
    return {
        "schema_version": "research_check_run.v2",
        "mode": CHECK_MODE_EVIDENCE,
        "status": result_payload.get("status"),
        "check": item,
        "links": links,
        "result": normalized_result.to_dict(),
        "evidence": dict(evidence),
        "replayable": True,
        "observation_eligible": status == "tested",
    }


def create_observation_from_check_evidence(
    check_id: str,
    payload: Mapping[str, Any],
    *,
    session: Session | None = None,
) -> dict[str, Any]:
    """Create an Observation only from completed, frozen, replayable Check evidence."""

    check = _project_evidence_classification(
        repository.get_item(
            str(check_id or "").strip(),
            **({"session": session} if session is not None else {}),
        )
    )
    if check.get("kind") != "research_check":
        raise ValueError("observation_evidence_invalid: item is not a Check")
    if not check.get("replayable") or not check.get("observation_eligible"):
        raise ValueError(
            "observation_evidence_invalid: Check is not durable replayable evidence"
        )
    check_payload = dict(check.get("payload") or {})
    result = CheckResult.from_dict(check_payload["result"])
    evidence = dict(check_payload["evidence"])
    request = CheckRequest.from_dict(check_payload["request"])
    observation_title = str(
        payload.get("title") or f"Observation from Check: {check.get('title')}"
    ).strip()
    create_args = {"session": session} if session is not None else {}
    observation = repository.create_item(
        kind="observation",
        status=str(payload.get("status") or "active"),
        title=observation_title,
        body=_optional(payload.get("body")),
        instrument_id=_optional(request.scope.get("instrument_id")),
        symbol=_optional(request.scope.get("symbol")),
        timeframe=_optional(
            request.scope.get("timeframe") or request.scope.get("interval")
        ),
        datasource=_optional(request.scope.get("datasource")),
        exchange=_optional(request.scope.get("exchange")),
        window_start=request.scope.get("start"),
        window_end=request.scope.get("end"),
        tags=sorted(
            set(["evidence-backed", *_tags(payload.get("tags"))])
        ),
        payload={
            "schema_version": "research_observation_payload.v2",
            "created_from": "research_check_evidence",
            "check_id": check["id"],
            "result_hash": result.result_hash,
            "evidence_hash": result.evidence_hash,
            "input_hash": evidence.get("input_hash"),
            "scope": dict(request.scope),
        },
        source_revision=str(evidence.get("code_revision") or _source_revision()),
        **create_args,
    )
    link = repository.create_link(
        source_item_id=check["id"],
        target_type="research_item",
        target_id=observation["id"],
        relation="supports",
        metadata={
            "target_kind": "observation",
            "result_hash": result.result_hash,
            "evidence_hash": result.evidence_hash,
        },
        **create_args,
    )
    return {
        "schema_version": "research_observation_from_check.v1",
        "check_id": check["id"],
        "observation": observation,
        "link": link,
    }


def replay_research_check(check_id: str) -> dict[str, Any]:
    """Replay v2 evidence through the same provider-free canonical execution path."""

    item = _project_evidence_classification(
        repository.get_item(str(check_id or "").strip())
    )
    if item.get("kind") != "research_check":
        raise ValueError("check_replay_invalid: item is not a Check")
    if not item.get("replayable"):
        return {
            "schema_version": "research_check_replay.v1",
            "check_id": item.get("id"),
            "status": "not_replayable",
            "evidence_classification": item.get("evidence_classification"),
            "matches": False,
            "provider_call_performed": False,
            "reasons": ["legacy Check has no immutable input binding"],
        }
    payload = dict(item.get("payload") or {})
    definition = CheckDefinition.from_dict(payload["definition"])
    request = CheckRequest.from_dict(payload["request"])
    original_plan = ResolvedCheckPlan.from_dict(payload["plan"])
    original_result = CheckResult.from_dict(payload["result"])
    original_evidence = dict(payload["evidence"])
    current_revision = _source_revision()
    if str(original_evidence.get("code_revision") or "") != current_revision:
        return {
            "schema_version": "research_check_replay.v1",
            "check_id": item.get("id"),
            "status": "source_revision_unavailable",
            "matches": False,
            "provider_call_performed": False,
            "original_source_revision": original_evidence.get("code_revision"),
            "current_source_revision": current_revision,
            "reasons": ["exact producing code revision is not running"],
        }
    replayed_plan, replayed_evidence, replayed_result = execute_check_evidence(
        definition, request, original_plan
    )
    verification = verify_check_replay(original_result, replayed_result)
    evidence_matches = (
        replayed_plan.plan_hash == original_plan.plan_hash
        and replayed_evidence.evidence_hash
        == str(original_evidence.get("evidence_hash") or "")
    )
    return {
        **verification,
        "check_id": item.get("id"),
        "status": "matched" if verification["matches"] and evidence_matches else "mismatch",
        "matches": bool(verification["matches"] and evidence_matches),
        "original_evidence_hash": original_evidence.get("evidence_hash"),
        "replayed_evidence_hash": replayed_evidence.evidence_hash,
        "original_plan_hash": original_plan.plan_hash,
        "replayed_plan_hash": replayed_plan.plan_hash,
    }


def _immutable_run_binding(dataset: Mapping[str, Any]) -> dict[str, Any]:
    metadata = dict(dataset.get("metadata") or {})
    readiness = dict(dataset.get("readiness") or {})
    semantic_fingerprint = str(
        metadata.get("report_semantic_fingerprint") or ""
    ).strip()
    if not semantic_fingerprint:
        # Historical test fixtures and older terminal reports can lack the
        # explicit field. Their complete canonical projection is still pinned
        # for this request; current production reports always expose it.
        semantic_fingerprint = semantic_hash(
            {
                key: value
                for key, value in dict(dataset).items()
                if key not in {"narrative_summary", "performance"}
            }
        )
    run_id = str(metadata.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("check_run_evidence_invalid: metadata.run_id is required")
    return {
        "schema_version": "immutable_run_research_binding.v1",
        "run_id": run_id,
        "report_schema_version": str(dataset.get("schema_version") or ""),
        "report_semantic_fingerprint": semantic_fingerprint,
        "dataset_id": metadata.get("dataset_id"),
        "dataset_hash": metadata.get("dataset_hash"),
        "strategy_id": metadata.get("strategy_id"),
        "strategy_hash": metadata.get("strategy_hash"),
        "readiness": {
            "dataset_status": readiness.get("dataset_status"),
            "results_status": readiness.get("results_status"),
            "safe_to_compare": bool(readiness.get("safe_to_compare", False)),
        },
        "provider_access": False,
    }


def _prepare_check_request_payload(
    payload: Mapping[str, Any], *, mode: str
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    request = dict(payload or {})
    family = str(
        request.get("check_family") or SUPPORTED_CHECK_FAMILY
    ).strip()
    if family not in {RUN_SIGNAL_SUMMARY, RUN_DECISION_TRADE_COMPARISON}:
        return request, None
    validate_check_detector(
        check_family=family,
        detector=_mapping(request.get("detector"), "detector"),
    )
    if request.get("dataset_id") not in (None, ""):
        raise ValueError(
            "check_run_evidence_invalid: run-backed Checks do not accept a market Dataset"
        )
    if request.get("immutable_run_evidence") not in (None, ""):
        raise ValueError(
            "check_run_evidence_invalid: immutable run evidence is resolved by QT"
        )
    scope = _normalize_report_scope(
        _mapping(request.get("scope"), "scope")
    )
    dataset = dict(
        reports_contract.get_run_research_dataset(scope["run_id"])
    )
    scope = _merge_scope_context(scope, _report_scope_context(dataset))
    binding = _immutable_run_binding(dataset)
    return (
        {
            **request,
            "mode": str(mode or CHECK_MODE_PREVIEW).strip().lower(),
            "scope": scope,
            "immutable_run_evidence": binding,
        },
        dataset,
    )


def sweep_research_checks(payload: Mapping[str, Any]) -> dict[str, Any]:
    request = dict(payload or {})
    check_family = str(request.get("check_family") or "").strip()
    if check_family not in INDICATOR_CHECK_FAMILIES:
        raise ValueError(
            "research check sweep supports indicator-backed check families: "
            f"{', '.join(sorted(INDICATOR_CHECK_FAMILIES))}"
        )
    detector = _mapping(request.get("detector"), "detector")
    outcomes = _mapping_or_empty(request.get("outcomes"))
    validate_check_detector(check_family=check_family, detector=detector)
    scopes = _normalize_sweep_scopes(request)
    variants = _normalize_sweep_variants(request.get("variants"))
    ranking = _mapping(request.get("ranking"), "ranking")
    rank_by = _required_text(ranking.get("rank_by"), "ranking.rank_by")
    rank_direction = _required_text(ranking.get("direction"), "ranking.direction")
    display_metrics = _string_list(ranking.get("display_metrics") or [])
    title = str(request.get("title") or "Research check sweep").strip()
    cache = ResearchEvaluationCache()
    evaluations: list[dict[str, Any]] = []
    progress: list[dict[str, Any]] = []

    for scope in scopes:
        scope_id = str(scope["id"])
        evaluation_scope = {key: value for key, value in scope.items() if key != "id"}
        for variant in variants:
            variant_scope = dict(evaluation_scope)
            if variant["param_overrides"]:
                variant_scope["indicator_param_overrides"] = dict(variant["param_overrides"])
            started_at = perf_counter()
            evaluation = _evaluate_legacy_research_check(
                {
                    "title": f"{title}: {variant['id']} @ {scope_id}",
                    "check_family": check_family,
                    "scope": variant_scope,
                    "detector": detector,
                    "outcomes": outcomes,
                },
                cache=cache,
            )
            duration_ms = round((perf_counter() - started_at) * 1000.0, 3)
            result = dict(evaluation["result"])
            normalized_scope = {**dict(evaluation["scope"]), "id": scope_id}
            normalized_variant = dict(variant)
            row = {
                "schema_version": "research_check_sweep_evaluation.v1",
                "variant": normalized_variant,
                "scope": normalized_scope,
                "status": result.get("status"),
                "result": result,
                "metrics": extract_numeric_metrics(result),
                "duration_ms": duration_ms,
            }
            evaluations.append(row)
            progress.append(
                {
                    "variant_id": variant["id"],
                    "scope_id": scope_id,
                    "status": result.get("status"),
                    "sample_count": result.get("sample_count"),
                    "duration_ms": duration_ms,
                }
            )

    leaderboard = build_leaderboard(
        evaluations,
        rank_by=rank_by,
        rank_direction=rank_direction,
        display_metrics=display_metrics,
    )
    return {
        "schema_version": "research_check_sweep.v1",
        "check_family": check_family,
        "scope_count": len(scopes),
        "variant_count": len(variants),
        "evaluation_count": len(evaluations),
        "ranking": {
            "rank_by": rank_by,
            "direction": rank_direction,
            "display_metrics": display_metrics,
        },
        "leaderboard": leaderboard,
        "evaluations": evaluations,
        "progress": progress,
        "cache": cache.snapshot(),
    }


def _evaluate_research_check_request(
    *,
    title: str,
    check_family: str,
    scope: Mapping[str, Any],
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any],
    cache: ResearchEvaluationCache | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if check_family == RAW_FORWARD_OUTCOME:
        normalized_scope = _normalize_scope(scope)
        coverage = _coverage_for_scope(normalized_scope, cache)
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
                candles = _candles_for_scope(normalized_scope, cache)
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
        return normalized_scope, result

    if check_family in INDICATOR_CHECK_FAMILIES:
        normalized_scope = _normalize_indicator_scope(scope)
        coverage = _coverage_for_scope(normalized_scope, cache)
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
                    indicator_param_overrides=normalized_scope.get("indicator_param_overrides"),
                    candle_frame=_candles_for_scope(normalized_scope, cache) if cache is not None else None,
                    source_frame_cache=cache.source_frames_by_request if cache is not None else None,
                    source_frame_cache_stats=cache.stats if cache is not None else None,
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
                if check_family == INDICATOR_FORWARD_OUTCOME:
                    result = evaluate_indicator_forward_outcome(
                        evidence,
                        detector=detector,
                        outcomes=outcomes,
                        data_quality=data_quality,
                    )
                elif check_family == SIGNAL_AUDIT:
                    result = evaluate_signal_audit(
                        evidence,
                        detector=detector,
                        outcomes=outcomes,
                        data_quality=data_quality,
                    )
                else:
                    result = evaluate_candidate_lifecycle(
                        evidence,
                        detector=detector,
                        outcomes=outcomes,
                        data_quality=data_quality,
                    )
        return normalized_scope, result

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
    return normalized_scope, result


def _existing_observation(
    request: Mapping[str, Any],
    *,
    session: Session | None = None,
) -> dict[str, Any] | None:
    observation_id = str(request.get("observation_id") or "").strip()
    if observation_id:
        observation = repository.get_item(
            observation_id,
            **({"session": session} if session is not None else {}),
        )
        if observation.get("kind") != "observation":
            raise ValueError("observation_id must reference an observation item")
        return observation
    return None


def _create_auto_observation(
    request: Mapping[str, Any],
    *,
    scope: Mapping[str, Any],
    title: str,
    session: Session | None = None,
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
        **({"session": session} if session is not None else {}),
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
    if "indicator_param_overrides" in scope:
        normalized["indicator_param_overrides"] = _mapping(
            scope.get("indicator_param_overrides"),
            "scope.indicator_param_overrides",
        )
    return normalized


def _normalize_sweep_scopes(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    has_scopes = request.get("scopes") not in (None, "")
    has_scope = request.get("scope") not in (None, "")
    if has_scopes and has_scope:
        raise ValueError("research check sweep accepts scope or scopes, not both")
    if has_scopes:
        raw_scopes = request.get("scopes")
        if not isinstance(raw_scopes, list) or not raw_scopes:
            raise ValueError("scopes must be a non-empty list")
        scopes = [_mapping(raw, "scopes item") for raw in raw_scopes]
        if any(not str(scope.get("id") or "").strip() for scope in scopes):
            raise ValueError("each sweep scope requires id")
        return [{**dict(scope), "id": str(scope["id"]).strip()} for scope in scopes]
    if not has_scope:
        raise ValueError("research check sweep requires scope or scopes")
    scope = dict(_mapping(request.get("scope"), "scope"))
    scope["id"] = str(scope.get("id") or "scope").strip()
    return [scope]


def _normalize_sweep_variants(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("research check sweep requires non-empty variants")
    allowed = {"id", "label", "description", "param_overrides"}
    variants: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in value:
        variant = dict(_mapping(raw, "variants item"))
        unsupported = sorted(str(key) for key in variant if str(key) not in allowed)
        if unsupported:
            raise ValueError(f"unsupported sweep variant fields: {', '.join(unsupported)}")
        variant_id = _required_text(variant.get("id"), "variant.id")
        if variant_id in seen:
            raise ValueError(f"duplicate sweep variant id: {variant_id}")
        seen.add(variant_id)
        param_overrides = _mapping_or_empty(variant.get("param_overrides"))
        variants.append(
            {
                "id": variant_id,
                "label": _optional(variant.get("label")) or variant_id,
                "description": _optional(variant.get("description")),
                "param_overrides": param_overrides,
            }
        )
    return variants


def _scope_cache_key(scope: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(scope.get("instrument_id") or ""),
        str(scope.get("start") or ""),
        str(scope.get("end") or ""),
        str(scope.get("timeframe") or ""),
    )


def _coverage_for_scope(scope: Mapping[str, Any], cache: ResearchEvaluationCache | None) -> dict[str, Any]:
    if cache is not None:
        return cache.coverage(scope)
    return candle_service.preflight_candle_coverage_by_instrument(
        scope["instrument_id"],
        scope["start"],
        scope["end"],
        scope["timeframe"],
    )


def _candles_for_scope(scope: Mapping[str, Any], cache: ResearchEvaluationCache | None) -> Any:
    if cache is not None:
        return cache.candles(scope)
    return candle_service.fetch_ohlcv_by_instrument(
        scope["instrument_id"],
        scope["start"],
        scope["end"],
        scope["timeframe"],
    )


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


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError("expected a string or list of strings")


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

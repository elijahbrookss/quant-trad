"""Research memory service and check orchestration."""

from __future__ import annotations
from .execution_limits import bounded_research_execution, research_execution_scope
from core.execution_control import measured_execution, consume_execution_json, execution_checkpoint
from portal.backend.db.session import db

import logging
import uuid
from collections import Counter
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping

from sqlalchemy.orm import Session
from research_science.check import (
    CHECK_MODE_EVIDENCE,
    CHECK_MODE_PREVIEW,
    CheckDefinition,
    CheckEvidenceBinding,
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
from portal.backend.service.market.frozen_dataset_service import (
    prepare_frozen_dataset_from_requirements,
)
from portal.backend.service.provenance import evidence_source_revision, source_revision
from portal.backend.service.reports import contract as reports_contract
from portal.backend.service.storage.repos import lifecycle as lifecycle_repo
from portal.backend.service.storage.repos import runs as runs_repo
from market_data.frozen import semantic_hash

from . import repository
from .historical_check_custody import (historical_identity, validate_historical_definition, validate_historical_receipt, historical_observation_eligible)
from .checks import (
    CANDIDATE_LIFECYCLE,
    INDICATOR_FORWARD_OUTCOME,
    RAW_FORWARD_OUTCOME,
    RUN_DECISION_TRADE_COMPARISON,
    RUN_SIGNAL_SUMMARY,
    SIGNAL_AUDIT,
    SUPPORTED_CHECK_FAMILY,
    SUPPORTED_CHECK_FAMILIES,
    normalize_run_signal_records,
)
from .metrics import build_leaderboard, extract_numeric_metrics
from .execution import execute_check_evidence, execute_check_preview
from .planning import (
    plan_research_check,
    rederive_research_check_plan_from_pinned_inputs,
)
from .registry import (
    CHECK_REGISTRY,
    definition_allows_evidence,
    normalize_check_preparation_request,
    normalize_check_request,
    pin_check_definition_to_plan,
    reconstruct_pinned_check_definition,
)


logger = logging.getLogger(__name__)

RESEARCH_ITEM_KINDS = {"observation", "research_check", "hypothesis", "study"}
RESEARCH_ITEM_STATUSES = {"draft", "active", "tested", "promoted", "rejected", "archived", "blocked"}

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




def create_research_item(payload: Mapping[str, Any]) -> dict[str, Any]:
    from .publication import KEY
    if KEY in _mapping_or_empty(payload.get("payload")):
        raise ValueError("question_contract_write_reserved: use explicit question adoption/publication")
    kind = _normalize_choice(payload.get("kind"), "kind", RESEARCH_ITEM_KINDS)
    if kind == "research_check":
        raise ValueError(
            "research_check_creation_reserved: invoke the registered Check preview "
            "or evidence operation"
        )
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
    from .publication import content_identity
    item = _project_evidence_classification(repository.get_item(item_id))
    if item.get("kind") != "research_check":
        item["content_hash"] = content_identity(item)
    return item


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


def _validate_research_check_evidence_payload(
    payload: Mapping[str, Any],
    *, item_id: str | None = None,
) -> tuple[
    CheckDefinition,
    CheckRequest,
    ResolvedCheckPlan,
    CheckEvidenceBinding,
    CheckResult,
]:
    if str(payload.get("schema_version") or "") != "research_check_payload.v2":
        raise ValueError("check_evidence_payload_invalid: unsupported payload schema")
    definition = CheckDefinition.from_dict(payload["definition"])
    request = CheckRequest.from_dict(payload["request"])
    plan = ResolvedCheckPlan.from_dict(payload["plan"])
    evidence = CheckEvidenceBinding.from_dict(payload["evidence"])
    result = CheckResult.from_dict(payload["result"])
    if request.mode != CHECK_MODE_EVIDENCE:
        raise ValueError("check_evidence_payload_invalid: request is not evidence mode")
    if (
        request.definition_id != definition.definition_id
        or request.definition_version != definition.definition_version
        or request.definition_hash != definition.definition_hash
    ):
        raise ValueError("check_evidence_payload_invalid: definition/request disagreement")
    if plan.request_hash != request.request_hash:
        raise ValueError("check_evidence_payload_invalid: request/plan disagreement")
    material_parameters = {
        key: definition.material_rules.get(key)
        for key in (
            "detector",
            "outcomes",
            "statistics",
            "assertions",
            "inputs",
            "gap_policy",
            "gap_rewarm_bars",
        )
    }
    if dict(request.parameters) != material_parameters:
        raise ValueError(
            "check_evidence_payload_invalid: request parameters differ from material definition"
        )
    if plan.gap_policy != str(request.parameters.get("gap_policy") or ""):
        raise ValueError(
            "check_evidence_payload_invalid: request/plan gap policy disagreement"
        )
    if (
        evidence.definition_hash != definition.definition_hash
        or evidence.request_hash != request.request_hash
        or evidence.plan_hash != plan.plan_hash
    ):
        raise ValueError("check_evidence_payload_invalid: plan/evidence disagreement")
    if (
        result.definition_hash != definition.definition_hash
        or result.request_hash != request.request_hash
        or result.plan_hash != plan.plan_hash
        or result.evidence_hash != evidence.evidence_hash
        or result.evaluator_id != definition.evaluator_id
        or result.evaluator_version != definition.evaluator_version
    ):
        raise ValueError("check_evidence_payload_invalid: evidence/result disagreement")
    historical = historical_identity(definition)
    if historical:
        # Only exact inventoried original envelopes can use recorded declarations.
        # This does not install an executor or admit new historical requests.
        validate_historical_receipt(definition, evidence, result, item_id=item_id)
        validate_historical_definition(definition, request, plan)
        declared_execution = dict(plan.execution)
    else:
        registered = definition.material_rules.get("registered_definition")
        if isinstance(registered, Mapping):
            base = CHECK_REGISTRY.resolve_definition(
                str(registered.get("definition_id") or ""),
                str(registered.get("definition_version") or ""),
            )
            if (
                base.definition_id != definition.definition_id
                or base.definition_hash != str(registered.get("definition_hash") or "")
                or base.evaluator_id != definition.evaluator_id
                or base.evaluator_version != definition.evaluator_version
            ):
                raise ValueError(
                    "check_evidence_payload_invalid: registered definition lineage disagrees"
                )
        else:
            base = CHECK_REGISTRY.resolve_definition(
                definition.definition_id, definition.definition_version
            )
            if base.definition_hash != definition.definition_hash:
                raise ValueError(
                    "check_evidence_payload_invalid: definition is not registered"
                )
        evaluator = CHECK_REGISTRY.resolve_evaluator(definition)
        declared_execution = dict(
            evaluator.declare_requirements(definition=definition, request=request)
        )
        if dict(plan.execution) != declared_execution:
            raise ValueError(
                "check_evidence_payload_invalid: evaluator execution declaration disagrees "
                "with plan"
            )
    input_kind = str(declared_execution.get("input_kind") or "").strip()
    expected_evidence_kind = {
        "market_data": "frozen_market_data",
        "immutable_run_evidence": "immutable_run_evidence",
    }.get(input_kind)
    if expected_evidence_kind is None or evidence.evidence_kind != expected_evidence_kind:
        raise ValueError(
            "check_evidence_payload_invalid: evaluator input kind disagrees with "
            f"evidence binding input_kind={input_kind or '<missing>'}"
        )
    if not historical:
        reconstructed_definition = reconstruct_pinned_check_definition(request, plan)
        if reconstructed_definition.to_dict() != definition.to_dict():
            raise ValueError(
                "check_evidence_payload_invalid: definition is not the canonical request/graph definition"
            )
    expected_graph_hash = semantic_hash(
        {"indicators": [dict(row) for row in plan.indicator_graph]}
    )
    if evidence.indicator_graph_hash != expected_graph_hash:
        raise ValueError(
            "check_evidence_payload_invalid: Indicator graph hash disagrees with plan"
        )
    if evidence.evidence_kind == "frozen_market_data":
        if str(request.dataset_id or "") != str(evidence.input_binding.get("dataset_id") or ""):
            raise ValueError("check_evidence_payload_invalid: Dataset binding disagrees")
        expected_quality_hash = semantic_hash(
            dict(evidence.input_binding.get("quality") or {})
        )
        expected_gaps_hash = semantic_hash(
            {
                "recorded_gaps": list(
                    evidence.input_binding.get("recorded_gaps") or []
                )
            }
        )
        if evidence.quality_hash != expected_quality_hash:
            raise ValueError(
                "check_evidence_payload_invalid: quality hash disagrees with binding"
            )
        if evidence.gaps_hash != expected_gaps_hash:
            raise ValueError(
                "check_evidence_payload_invalid: gaps hash disagrees with binding"
            )
        subject_snapshots = {
            str(row["instrument_id"]): dict(row.get("snapshot") or {})
            for row in evidence.input_binding.get("subjects") or []
        }
        rederived_plan = rederive_research_check_plan_from_pinned_inputs(
            definition,
            request,
            plan,
            subject_snapshots=subject_snapshots,
            historical_execution=declared_execution if historical else None,
        )
        for field in (
            "market_data_requirements",
            "indicator_graph",
            "evaluation_range",
            "warmup",
            "outcome_tail",
            "gap_policy",
            "execution",
        ):
            if getattr(rederived_plan, field) != getattr(plan, field):
                raise ValueError(
                    "check_evidence_payload_invalid: request/plan semantic "
                    f"disagreement field={field}"
                )
        expected_materialization = dict(rederived_plan.materialization_range)
        actual_materialization = dict(plan.materialization_range)
        expected_materialization.pop("as_of_commit_seq", None)
        actual_watermark = actual_materialization.pop("as_of_commit_seq", None)
        if expected_materialization != actual_materialization:
            raise ValueError(
                "check_evidence_payload_invalid: request/plan materialization range disagreement"
            )
        if int(actual_watermark or 0) != int(evidence.input_binding["max_commit_seq"]):
            raise ValueError(
                "check_evidence_payload_invalid: plan/Dataset watermark disagreement"
            )
        if plan.missing_coverage:
            raise ValueError(
                "check_evidence_payload_invalid: durable plan contains missing coverage"
            )
        if list(plan.quality_evidence) != list(
            evidence.input_binding.get("recorded_gaps") or []
        ):
            raise ValueError(
                "check_evidence_payload_invalid: plan/Dataset quality evidence disagreement"
            )
        _validate_plan_binding_alignment(plan, evidence.input_binding)
    else:
        if dict(request.immutable_run_evidence or {}) != dict(evidence.input_binding):
            raise ValueError(
                "check_evidence_payload_invalid: immutable run binding disagrees"
            )
        rederived_plan = rederive_research_check_plan_from_pinned_inputs(
            definition,
            request,
            plan,
            subject_snapshots={},
            historical_execution=declared_execution if historical else None,
        )
        for field in (
            "market_data_requirements",
            "indicator_graph",
            "evaluation_range",
            "materialization_range",
            "warmup",
            "outcome_tail",
            "gap_policy",
            "execution",
        ):
            if getattr(rederived_plan, field) != getattr(plan, field):
                raise ValueError(
                    "check_evidence_payload_invalid: immutable run request/plan "
                    f"disagreement field={field}"
                )
    semantic_result = dict(result.result)
    if semantic_result.get("promotion_authority") is not False:
        raise ValueError("check_evidence_payload_invalid: Check cannot grant promotion authority")
    if semantic_result.get("execution_authority") is not False:
        raise ValueError("check_evidence_payload_invalid: Check cannot grant execution authority")
    return definition, request, plan, evidence, result


def _validate_plan_binding_alignment(
    plan: ResolvedCheckPlan,
    binding: Mapping[str, Any],
) -> None:
    requirements = {
        str(row.get("alias") or ""): dict(row)
        for row in plan.market_data_requirements
    }
    bound_series = {
        str(row.get("alias") or ""): dict(row)
        for row in binding.get("series") or []
    }
    if not requirements or set(bound_series) != set(requirements):
        raise ValueError(
            "check_evidence_payload_invalid: Dataset binding aliases differ from plan"
        )
    for alias, requirement in requirements.items():
        series = bound_series[alias]
        if dict(series.get("requirement") or {}) != requirement:
            raise ValueError(
                "check_evidence_payload_invalid: Dataset binding requirement "
                f"disagrees alias={alias}"
            )
        for field in (
            "instrument_id",
            "fact_type",
            "contract_version",
            "timeframe_seconds",
        ):
            if series.get(field) != requirement.get(field):
                raise ValueError(
                    "check_evidence_payload_invalid: Dataset binding semantic "
                    f"identity disagrees alias={alias} field={field}"
                )
        source_policy = dict(requirement.get("source_policy") or {})
        source_binding = dict(series.get("source_binding") or {})
        mode = str(source_policy.get("mode") or "").strip().lower()
        resolved_keys = [
            str(value)
            for value in source_binding.get("resolved_source_identity_keys") or []
        ]
        if str(source_binding.get("mode") or "") != mode or not resolved_keys:
            raise ValueError(
                "check_evidence_payload_invalid: Dataset source binding is unresolved "
                f"alias={alias}"
            )
        if mode == "exact" and source_policy.get("source_identity_key"):
            if resolved_keys != [str(source_policy["source_identity_key"])]:
                raise ValueError(
                    "check_evidence_payload_invalid: exact source binding disagrees "
                    f"alias={alias}"
                )
        if mode == "allowlist" and not set(resolved_keys).issubset(
            {str(value) for value in source_policy.get("source_identity_keys") or []}
        ):
            raise ValueError(
                "check_evidence_payload_invalid: allowlisted source binding disagrees "
                f"alias={alias}"
            )


def _definition_evidence_classification(definition: CheckDefinition) -> str:
    if historical_identity(definition):
        return "historical_frozen_verified"
    if not definition_allows_evidence(definition):
        return "frozen_replayable_unqualified"
    if definition.definition_id in {SIGNAL_AUDIT, CANDIDATE_LIFECYCLE}:
        return "frozen_replayable_diagnostic"
    return "frozen_replayable"


def _project_evidence_classification(item: Mapping[str, Any]) -> dict[str, Any]:
    projected = dict(item)
    if str(projected.get("kind") or "") != "research_check":
        if projected.get("kind") == "study":
            from .publication import KEY, SCHEMA, verify_history, _assert_access
            contract = (projected.get("payload") or {}).get(KEY)
            projected["question_classification"] = "legacy_uncontracted"
            if isinstance(contract, Mapping) and contract.get("schema_version") == SCHEMA:
                verify_history(contract, str(projected["id"]))
                with db.session() as session:
                    _assert_access(projected, session)
                projected["question_classification"] = "contracted_question"
        return projected
    payload = projected.get("payload")
    schema_version = (
        str(payload.get("schema_version") or "")
        if isinstance(payload, Mapping)
        else ""
    )
    if schema_version == "research_check_payload.v2":
        raw_plan = payload.get("plan") if isinstance(payload, Mapping) else None
        raw_evidence = (
            payload.get("evidence") if isinstance(payload, Mapping) else None
        )
        if (
            isinstance(raw_plan, Mapping)
            and str(raw_plan.get("schema_version") or "")
            == "research.check_plan.v1"
        ) or (
            isinstance(raw_evidence, Mapping)
            and str(raw_evidence.get("schema_version") or "")
            == "research.check_evidence_binding.v1"
        ):
            projected["evidence_classification"] = "legacy_frozen_unverifiable"
            projected["replayable"] = False
            projected["observation_eligible"] = False
            projected["evidence_integrity_error"] = (
                "historical frozen Check predates the complete execution-input hash contract"
            )
            return projected
        raw_definition = (
            payload.get("definition") if isinstance(payload, Mapping) else None
        )
        material_rules = (
            raw_definition.get("material_rules")
            if isinstance(raw_definition, Mapping)
            else None
        )
        if not isinstance(material_rules, Mapping) or not str(
            material_rules.get("resolved_indicator_graph_hash") or ""
        ).strip():
            projected["evidence_classification"] = "legacy_frozen_unverifiable"
            projected["replayable"] = False
            projected["observation_eligible"] = False
            projected["evidence_integrity_error"] = (
                "historical frozen Check predates definition-pinned Indicator graph semantics"
            )
            return projected
        try:
            definition, _request, _plan, _evidence, result = (
                _validate_research_check_evidence_payload(payload, item_id=str(projected["id"]))
            )
        except (KeyError, TypeError, ValueError) as exc:
            projected["evidence_classification"] = "invalid_v2"
            projected["replayable"] = False
            projected["observation_eligible"] = False
            projected["evidence_integrity_error"] = str(exc)
        else:
            historical = historical_identity(definition)
            projected["evidence_classification"] = (
                "historical_frozen_verified" if historical else _definition_evidence_classification(definition)
            )
            projected["stored_evidence_integrity"] = "verified"
            projected["execution_available"] = not historical
            projected["replayable"] = not historical
            projected["observation_eligible"] = (
                str(result.result.get("status") or "") == "completed"
                and (historical_observation_eligible(str(projected["id"])) if historical else definition_allows_evidence(definition))
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
    return compare_research_check_records(repository.get_item(left_check_id), repository.get_item(right_check_id))


def compare_research_check_records(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    """Same saved-output comparison math, over caller-admitted exact snapshots."""
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




@bounded_research_execution
def get_research_check_requirements(payload: Mapping[str, Any]) -> dict[str, Any]:
    mode = str(payload.get("mode") or CHECK_MODE_PREVIEW).strip().lower()
    family = str(payload.get("check_family") or SUPPORTED_CHECK_FAMILY).strip()
    planning_only = (
        mode == CHECK_MODE_EVIDENCE
        and not payload.get("dataset_id")
        and family not in {RUN_SIGNAL_SUMMARY, RUN_DECISION_TRADE_COMPARISON}
    )
    if planning_only:
        request_payload = dict(payload)
        definition, request = normalize_check_preparation_request(request_payload)
    else:
        request_payload, _run_evidence = _prepare_check_request_payload(
            payload, mode=mode
        )
        definition, request = normalize_check_request(request_payload)
    plan = plan_research_check(
        definition,
        request,
        inspect_coverage=request.mode == CHECK_MODE_PREVIEW,
        require_durable_sources=planning_only,
    )
    definition, request, plan = pin_check_definition_to_plan(
        definition, request, plan
    )
    return {
        "schema_version": "research_check_requirements.v1",
        "mode": mode,
        "planning_only": planning_only,
        "definition": definition.to_dict(),
        "request": request.to_dict(),
        "plan": plan.to_dict(),
        "provider_call_performed": False,
        "preparation_authorized": False,
    }


@bounded_research_execution
def prepare_research_check_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve a durable Check plan and optionally freeze its exact inputs."""

    request_payload = dict(payload or {})
    family = str(
        request_payload.get("check_family") or SUPPORTED_CHECK_FAMILY
    ).strip()
    if family in {RUN_SIGNAL_SUMMARY, RUN_DECISION_TRADE_COMPARISON}:
        raise ValueError(
            "check_preparation_invalid: run-backed Checks already bind immutable run evidence"
        )
    if request_payload.get("dataset_id") not in (None, ""):
        raise ValueError(
            "check_preparation_invalid: preparation creates the Dataset; omit dataset_id"
        )
    preparation = _mapping_or_empty(request_payload.get("preparation"))
    freeze = bool(preparation.get("freeze", request_payload.get("freeze", False)))
    acquire_missing = bool(
        preparation.get(
            "acquire_missing", request_payload.get("acquire_missing", False)
        )
    )
    definition, planning_request = normalize_check_preparation_request(
        request_payload
    )
    plan = plan_research_check(
        definition,
        planning_request,
        inspect_coverage=True,
        require_durable_sources=True,
    )
    definition, planning_request, plan = pin_check_definition_to_plan(
        definition, planning_request, plan
    )
    if plan.missing_coverage:
        return {
            "schema_version": "research_check_preparation.v2",
            "status": "acquisition_required",
            "provider_call_performed": False,
            "acquisition_performed": False,
            "acquisition_authorized": False,
            "missing_coverage": [dict(row) for row in plan.missing_coverage],
            "plan": plan.to_dict(),
            "dataset": None,
            "binding": None,
            "evidence_request": None,
            "message": (
                "Requirement planning found source-bound coverage without durable "
                "gap evidence. Use an explicitly authorized acquisition primitive "
                "and repeat preparation."
            ),
        }
    prepared = prepare_frozen_dataset_from_requirements(
        requirements=plan.market_data_requirements,
        freeze=freeze,
        name=_optional(
            preparation.get("name") or request_payload.get("dataset_name")
        ),
        purpose=str(preparation.get("purpose") or "research"),
        created_by=_optional(
            preparation.get("created_by") or request_payload.get("created_by")
        ),
        metadata={
            **_mapping_or_empty(preparation.get("metadata")),
            "check_definition_id": definition.definition_id,
            "check_definition_version": definition.definition_version,
            "check_definition_hash": definition.definition_hash,
            "planning_request_hash": planning_request.request_hash,
            "planning_plan_hash": plan.plan_hash,
        },
    )
    if acquire_missing and prepared["unresolved_requirements"]:
        prepared = {
            **prepared,
            "status": "acquisition_required",
            "acquisition_authorized": False,
            "message": (
                "Preparation never infers provider acquisition. Use the existing "
                "authorized data acquisition primitive for each unresolved requirement, "
                "then repeat preparation."
            ),
        }
    response: dict[str, Any] = {
        "schema_version": "research_check_preparation.v2",
        "status": prepared["status"],
        "definition": definition.to_dict(),
        "planning_request": planning_request.to_dict(),
        "plan": plan.to_dict(),
        "preparation": prepared,
        "provider_call_performed": False,
        "check_executed": False,
        "observation_created": False,
    }
    dataset = prepared.get("dataset")
    if not isinstance(dataset, Mapping):
        return response
    evidence_payload = {
        **request_payload,
        "mode": CHECK_MODE_EVIDENCE,
        "dataset_id": str(dataset["dataset_id"]),
    }
    evidence_payload.pop("preparation", None)
    evidence_payload.pop("freeze", None)
    evidence_payload.pop("acquire_missing", None)
    evidence_definition, evidence_request = normalize_check_request(
        evidence_payload, mode=CHECK_MODE_EVIDENCE
    )
    evidence_plan = plan_research_check(
        evidence_definition,
        evidence_request,
        inspect_coverage=False,
    )
    evidence_definition, evidence_request, evidence_plan = (
        pin_check_definition_to_plan(
            evidence_definition,
            evidence_request,
            evidence_plan,
        )
    )
    return {
        **response,
        "next_request": evidence_payload,
        "evidence_request": evidence_request.to_dict(),
        "evidence_plan": evidence_plan.to_dict(),
        "next_operation": {
            "operation": "research_check_evidence",
            "dataset_id": dataset["dataset_id"],
            "request_hash": evidence_request.request_hash,
            "request_field": "next_request",
        },
    }


@bounded_research_execution
def evaluate_research_check(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Run a watermark-pinned ephemeral preview; never persist evidence."""

    request_payload, run_evidence = _prepare_check_request_payload(
        {**dict(payload or {}), "mode": CHECK_MODE_PREVIEW},
        mode=CHECK_MODE_PREVIEW,
    )
    definition, request = normalize_check_request(
        request_payload, mode=CHECK_MODE_PREVIEW
    )
    plan = plan_research_check(definition, request, inspect_coverage=True)
    definition, request, plan = pin_check_definition_to_plan(
        definition, request, plan
    )
    return execute_check_preview(
        definition, request, plan, run_evidence=run_evidence
    )


def run_research_check(
    payload: Mapping[str, Any],
    *,
    session: Session | None = None,
) -> dict[str, Any]:
    """Persist explicit frozen evidence; preview has its own evaluate operation."""

    # One deadline/admission slot spans computation and the owned transaction.
    # Validate before commit; do not reinterpret an acknowledged commit as a
    # failure merely because the deadline expires on the return path.
    with research_execution_scope(check_on_exit=False):
        built = build_research_check_evidence(payload)
        if session is not None:
            result = persist_built_research_check_evidence(built, session=session)
            consume_execution_json("result_bytes", result)
            execution_checkpoint()
            return result
        with db.session() as owned_session:
            result = persist_built_research_check_evidence(built, session=owned_session)
            consume_execution_json("result_bytes", result)
            execution_checkpoint()
        return result


@bounded_research_execution
def build_research_check_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Compute one evidence envelope without holding a persistence transaction."""

    mode = str(payload.get("mode") or "").strip().lower()
    if mode != CHECK_MODE_EVIDENCE:
        raise ValueError(
            "check_evidence_mode_required: use the Check preview operation for ephemeral evaluation"
        )
    request_payload, run_evidence = _prepare_check_request_payload(
        payload, mode=mode
    )
    definition, request = normalize_check_request(request_payload, mode=mode)
    plan = plan_research_check(definition, request, inspect_coverage=False)
    definition, request, plan = pin_check_definition_to_plan(
        definition, request, plan
    )
    bound_plan, evidence, result = execute_check_evidence(
        definition, request, plan, run_evidence=run_evidence
    )
    return {
        "schema_version": "research_check_evidence_build.v1",
        "payload": request_payload,
        "definition": definition.to_dict(),
        "request": request.to_dict(),
        "plan": bound_plan.to_dict(),
        "evidence": evidence.to_dict(),
        "result": result.to_dict(),
    }


@measured_execution("persistence")
def persist_built_research_check_evidence(
    built: Mapping[str, Any],
    *,
    session: Session | None = None,
) -> dict[str, Any]:
    """Persist a precomputed, internally verified evidence envelope."""

    if str(built.get("schema_version") or "") != "research_check_evidence_build.v1":
        raise ValueError("check_evidence_build_invalid: unsupported schema")
    definition = CheckDefinition.from_dict(built["definition"])
    request = CheckRequest.from_dict(built["request"])
    plan = ResolvedCheckPlan.from_dict(built["plan"])
    evidence = CheckEvidenceBinding.from_dict(built["evidence"])
    result = CheckResult.from_dict(built["result"])
    return persist_research_check_evidence(
        dict(built.get("payload") or {}),
        definition=definition,
        request=request,
        plan=plan,
        evidence=evidence.to_dict(),
        result=result.to_dict(),
        session=session,
    )




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
    if not definition_allows_evidence(definition):
        raise ValueError(
            "check_evidence_family_preview_only: this evaluator cannot create new "
            f"durable evidence family={definition.definition_id}"
        )
    normalized_evidence = CheckEvidenceBinding.from_dict(evidence)
    normalized_result = CheckResult.from_dict(result)
    result_payload = dict(normalized_result.result)
    status = "tested" if result_payload.get("status") == "completed" else "blocked"
    evidence_classification = _definition_evidence_classification(definition)
    observation_eligible = (
        status == "tested"
        and definition_allows_evidence(definition)
    )
    envelope = {
        "schema_version": "research_check_payload.v2",
        "evidence_classification": evidence_classification,
        "definition": definition.to_dict(),
        "request": request.to_dict(),
        "plan": plan.to_dict(),
        "evidence": normalized_evidence.to_dict(),
        "result": normalized_result.to_dict(),
        "replayable": True,
        "observation_eligible": observation_eligible,
    }
    _validate_research_check_evidence_payload(envelope)
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
        payload=envelope,
        source_revision=normalized_evidence.code_revision,
        **create_args,
    )
    target_type = (
        "run"
        if normalized_evidence.evidence_kind
        == "immutable_run_evidence"
        else "market_dataset"
    )
    target_id = (
        str(normalized_evidence.input_binding.get("run_id") or "")
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
                "dataset_hash": normalized_evidence.input_binding.get(
                    "dataset_hash"
                ),
                "binding_hash": normalized_evidence.input_binding.get(
                    "binding_hash"
                ),
                "result_hash": normalized_result.result_hash,
            },
            **create_args,
        )
    ]
    projected_item = _project_evidence_classification(item)
    return {
        "schema_version": "research_check_run.v2",
        "mode": CHECK_MODE_EVIDENCE,
        "status": result_payload.get("status"),
        "check": projected_item,
        "links": links,
        "result": normalized_result.to_dict(),
        "evidence": normalized_evidence.to_dict(),
        "evidence_classification": evidence_classification,
        "replayable": True,
        "observation_eligible": observation_eligible,
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
    verified_historical = (
        check.get("stored_evidence_integrity") == "verified"
        and check.get("execution_available") is False
    )
    if not check.get("observation_eligible") or not (check.get("replayable") or verified_historical):
        raise ValueError(
            "observation_evidence_invalid: Check is not durable replayable evidence"
        )
    check_payload = dict(check.get("payload") or {})
    _definition, request, _plan, evidence_contract, result = (
        _validate_research_check_evidence_payload(check_payload, item_id=str(check["id"]))
    )
    evidence = evidence_contract.to_dict()
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


@bounded_research_execution
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
            "reasons": (["historical method retired; stored integrity does not provide current execution or exact-source replay"]
                        if item.get("stored_evidence_integrity") == "verified" and item.get("execution_available") is False
                        else ["legacy Check has no immutable input binding"]),
        }
    payload = dict(item.get("payload") or {})
    definition, request, original_plan, original_evidence_contract, original_result = (
        _validate_research_check_evidence_payload(payload, item_id=str(item["id"]))
    )
    original_evidence = original_evidence_contract.to_dict()
    try:
        current_revision = _evidence_source_revision()
    except RuntimeError as exc:
        return {
            "schema_version": "research_check_replay.v1",
            "check_id": item.get("id"),
            "status": "source_revision_unavailable",
            "matches": False,
            "provider_call_performed": False,
            "original_source_revision": original_evidence.get("code_revision"),
            "current_source_revision": None,
            "reasons": [str(exc)],
        }
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
        definition,
        request,
        original_plan,
        expected_input_binding=original_evidence_contract.input_binding,
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




def _prepare_check_request_payload(payload: Mapping[str, Any], *, mode: str):
    request = dict(payload or {})
    if request.get("immutable_run_evidence") not in (None, ""):
        raise ValueError("check_definition_retired: run-backed Check execution is unavailable")
    return request, None


@bounded_research_execution
def sweep_research_checks(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Retired legacy sweep: current Checks require explicit admitted requests."""
    raise ValueError("check_definition_retired: legacy indicator sweeps are unavailable; use the current Check catalog and requirements")
























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
        "supported_checks": [],
    }
    if include_dataset_context:
        payload["data_quality"] = _data_quality_from_report_dataset(dataset)
    return payload


def _check_result(item: Mapping[str, Any]) -> dict[str, Any]:
    payload = item.get("payload") if isinstance(item.get("payload"), Mapping) else {}
    result = payload.get("result") if isinstance(payload.get("result"), Mapping) else {}
    if isinstance(result.get("result"), Mapping):
        result = result["result"]
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
        "descriptive_outcomes": dict(result.get("descriptive_outcomes") or {}),
        "outcome_resolution": dict(result.get("outcome_resolution") or {}),
        "timeframe": item.get("timeframe"),
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


def _evidence_source_revision() -> str:
    return evidence_source_revision()

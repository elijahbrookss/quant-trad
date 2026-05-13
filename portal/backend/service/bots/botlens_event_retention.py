from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Tuple

from .botlens_domain_events import BotLensDomainEvent, BotLensDomainEventName


class RuntimeEventRetentionTier(str, Enum):
    CANONICAL_RUN_TRUTH = "tier_1_canonical_run_truth"
    RESEARCH_CONTEXT = "tier_2_research_context"
    OBSERVABILITY_METRIC = "tier_3_observability_metric"
    LIVE_TRANSPORT = "tier_4_live_transport"


class RuntimeEventRetentionAction(str, Enum):
    PERSIST = "persist"
    PERSIST_IF_MATERIAL = "persist_if_material"
    SUMMARIZE = "summarize"
    AGGREGATE = "aggregate"
    TRANSPORT_ONLY = "transport_only"


@dataclass(frozen=True)
class RuntimeEventRetentionPolicy:
    tier: RuntimeEventRetentionTier
    action: RuntimeEventRetentionAction
    reason: str

    @property
    def persist_raw(self) -> bool:
        return self.action == RuntimeEventRetentionAction.PERSIST


_CANONICAL_RUN_TRUTH_NAMES = frozenset(
    {
        "RUN_STARTED",
        "RUN_PHASE_REPORTED",
        "RUN_LIFECYCLE_CHANGED",
        "RUN_READY",
        "RUN_DEGRADED",
        "RUN_COMPLETED",
        "RUN_FAILED",
        "RUN_STOPPED",
        "RUN_CANCELLED",
        "SIGNAL_EMITTED",
        "DECISION_EMITTED",
        "DECISION_ACCEPTED",
        "DECISION_REJECTED",
        "ENTRY_FILLED",
        "EXIT_FILLED",
        "TRADE_EXECUTION_OBSERVED",
        "TRADE_OPENED",
        "TRADE_UPDATED",
        "TRADE_CLOSED",
        "WALLET_INITIALIZED",
        "WALLET_DEPOSITED",
        "MARGIN_RESERVED",
        "MARGIN_REJECTED",
        "MARGIN_RELEASED",
        "FEE_APPLIED",
        "REALIZED_PNL_APPLIED",
        "POSITION_OPENED",
        "POSITION_CLOSED",
        "EQUITY_UPDATED",
        "FAULT_RECORDED",
        "RUNTIME_ERROR",
        "SYMBOL_DEGRADED",
        "SYMBOL_RECOVERED",
        "EXECUTION_INTRABAR_FALLBACK_PESSIMISTIC",
    }
)
_RESEARCH_CONTEXT_PERSIST_NAMES = frozenset({"SERIES_METADATA_REPORTED"})
_RESEARCH_CONTEXT_SUMMARY_NAMES = frozenset(
    {
        "CANDLE_OBSERVED",
        "CANDLE_UPSERTED",
        "SERIES_STATS_REPORTED",
    }
)
_LIVE_TRANSPORT_NAMES = frozenset({"OVERLAY_STATE_CHANGED"})
_OBSERVABILITY_NAMES = frozenset({"HEALTH_STATUS_REPORTED", "DIAGNOSTIC_RECORDED"})
_MATERIAL_DIAGNOSTIC_CODES = frozenset(
    {
        "candle_continuity_summary",
        "candle_gap_observed",
        "data_quality_degraded",
        "execution_intrabar_fallback_pessimistic",
        "lifecycle_contradiction",
        "margin_rejection_evidence_incomplete",
        "projection_replay_completed",
        "projection_replay_failed",
        "run_notification_queue_overflow",
        "run_projector_failed",
        "wallet_ledger_state_malformed",
        "wallet_ledger_state_mismatch",
        "wallet_replay_failed",
    }
)
_MATERIAL_DIAGNOSTIC_PREFIXES = ("wallet_", "projection_", "candle_", "lifecycle_", "execution_")
_MATERIAL_LEVELS = frozenset({"WARN", "WARNING", "ERROR", "CRITICAL", "FATAL"})


def _name(value: Any) -> str:
    if isinstance(value, BotLensDomainEventName):
        return value.value.upper()
    return str(value or "").strip().upper()


def _context_from_event(event: BotLensDomainEvent) -> Dict[str, Any]:
    context = event.context.to_dict() if hasattr(event.context, "to_dict") else {}
    return dict(context) if isinstance(context, Mapping) else {}


def event_name_from_row(row: Mapping[str, Any]) -> str:
    payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
    return _name(payload.get("event_name") or row.get("event_name") or row.get("event_type"))


def event_context_from_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
    context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
    return dict(context)


def _material_diagnostic(context: Mapping[str, Any]) -> bool:
    level = str(context.get("level") or "").strip().upper()
    if level in _MATERIAL_LEVELS:
        return True
    status = str(context.get("status") or "").strip().lower()
    if status in {"degraded", "blocked", "failed", "error"}:
        return True
    failure_mode = str(context.get("failure_mode") or "").strip()
    if failure_mode:
        return True
    for value in (
        context.get("diagnostic_code"),
        context.get("diagnostic_event"),
        context.get("reason_code"),
    ):
        code = str(value or "").strip().lower()
        if not code:
            continue
        if code in _MATERIAL_DIAGNOSTIC_CODES:
            return True
        if code.startswith(_MATERIAL_DIAGNOSTIC_PREFIXES):
            return True
    return False


def retention_policy_for_event_name(
    event_name: Any,
    *,
    context: Mapping[str, Any] | None = None,
) -> RuntimeEventRetentionPolicy:
    name = _name(event_name)
    if name in _CANONICAL_RUN_TRUTH_NAMES:
        return RuntimeEventRetentionPolicy(
            tier=RuntimeEventRetentionTier.CANONICAL_RUN_TRUTH,
            action=RuntimeEventRetentionAction.PERSIST,
            reason="material runtime truth",
        )
    if name in _RESEARCH_CONTEXT_PERSIST_NAMES:
        return RuntimeEventRetentionPolicy(
            tier=RuntimeEventRetentionTier.RESEARCH_CONTEXT,
            action=RuntimeEventRetentionAction.PERSIST,
            reason="compact research/catalog context",
        )
    if name in _RESEARCH_CONTEXT_SUMMARY_NAMES:
        return RuntimeEventRetentionPolicy(
            tier=RuntimeEventRetentionTier.RESEARCH_CONTEXT,
            action=RuntimeEventRetentionAction.SUMMARIZE,
            reason="raw per-bar research context is replaced by compact summaries or source catalog references",
        )
    if name == "DIAGNOSTIC_RECORDED":
        if _material_diagnostic(context or {}):
            return RuntimeEventRetentionPolicy(
                tier=RuntimeEventRetentionTier.CANONICAL_RUN_TRUTH,
                action=RuntimeEventRetentionAction.PERSIST,
                reason="diagnostic affects trust/readiness",
            )
        return RuntimeEventRetentionPolicy(
            tier=RuntimeEventRetentionTier.OBSERVABILITY_METRIC,
            action=RuntimeEventRetentionAction.AGGREGATE,
            reason="nonmaterial diagnostic belongs in observability aggregation",
        )
    if name in _OBSERVABILITY_NAMES:
        return RuntimeEventRetentionPolicy(
            tier=RuntimeEventRetentionTier.OBSERVABILITY_METRIC,
            action=RuntimeEventRetentionAction.AGGREGATE,
            reason="runtime health and latency telemetry are observability metrics",
        )
    if name in _LIVE_TRANSPORT_NAMES:
        return RuntimeEventRetentionPolicy(
            tier=RuntimeEventRetentionTier.LIVE_TRANSPORT,
            action=RuntimeEventRetentionAction.TRANSPORT_ONLY,
            reason="live projection/UI transport delta",
        )
    return RuntimeEventRetentionPolicy(
        tier=RuntimeEventRetentionTier.OBSERVABILITY_METRIC,
        action=RuntimeEventRetentionAction.PERSIST_IF_MATERIAL,
        reason="unknown event requires explicit material classification before permanent retention",
    )


def retention_policy_for_event(event: BotLensDomainEvent) -> RuntimeEventRetentionPolicy:
    return retention_policy_for_event_name(event.event_name, context=_context_from_event(event))


def should_persist_event(event: BotLensDomainEvent) -> bool:
    return retention_policy_for_event(event).persist_raw


def split_events_by_retention(
    events: Sequence[BotLensDomainEvent],
) -> tuple[tuple[BotLensDomainEvent, ...], tuple[BotLensDomainEvent, ...]]:
    retained = []
    dropped = []
    for event in events:
        if should_persist_event(event):
            retained.append(event)
        else:
            dropped.append(event)
    return tuple(retained), tuple(dropped)


def retention_summary_for_events(events: Sequence[BotLensDomainEvent]) -> Dict[str, Any]:
    by_event_name: Counter[str] = Counter()
    by_tier: Counter[str] = Counter()
    by_action: Counter[str] = Counter()
    retained = 0
    dropped = 0
    for event in events:
        event_name = _name(event.event_name)
        policy = retention_policy_for_event(event)
        by_event_name[event_name] += 1
        by_tier[policy.tier.value] += 1
        by_action[policy.action.value] += 1
        if policy.persist_raw:
            retained += 1
        else:
            dropped += 1
    return {
        "event_count": len(events),
        "retained_count": retained,
        "dropped_or_summarized_count": dropped,
        "by_event_name": dict(sorted(by_event_name.items())),
        "by_tier": dict(sorted(by_tier.items())),
        "by_action": dict(sorted(by_action.items())),
    }


def tier_map() -> Tuple[Dict[str, str], ...]:
    names = sorted(
        _CANONICAL_RUN_TRUTH_NAMES
        | _RESEARCH_CONTEXT_PERSIST_NAMES
        | _RESEARCH_CONTEXT_SUMMARY_NAMES
        | _OBSERVABILITY_NAMES
        | _LIVE_TRANSPORT_NAMES
    )
    rows = []
    for event_name in names:
        policy = retention_policy_for_event_name(event_name)
        rows.append(
            {
                "event_name": event_name,
                "tier": policy.tier.value,
                "action": policy.action.value,
                "reason": policy.reason,
            }
        )
    return tuple(rows)


__all__ = [
    "RuntimeEventRetentionAction",
    "RuntimeEventRetentionPolicy",
    "RuntimeEventRetentionTier",
    "event_context_from_row",
    "event_name_from_row",
    "retention_policy_for_event",
    "retention_policy_for_event_name",
    "retention_summary_for_events",
    "should_persist_event",
    "split_events_by_retention",
    "tier_map",
]

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat() + "Z"


class BotLensRuntimeState(str, Enum):
    INITIALIZING = "initializing"
    AWAITING_FIRST_SNAPSHOT = "awaiting_first_snapshot"
    LIVE = "live"
    DEGRADED = "degraded"
    STOPPED = "stopped"
    STARTUP_FAILED = "startup_failed"
    CRASHED = "crashed"


_ALLOWED_TRANSITIONS: Dict[Optional[BotLensRuntimeState], frozenset[BotLensRuntimeState]] = {
    None: frozenset({BotLensRuntimeState.INITIALIZING}),
    BotLensRuntimeState.INITIALIZING: frozenset(
        {
            BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT,
            BotLensRuntimeState.LIVE,
            BotLensRuntimeState.STARTUP_FAILED,
            BotLensRuntimeState.CRASHED,
            BotLensRuntimeState.STOPPED,
        }
    ),
    BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT: frozenset(
        {
            BotLensRuntimeState.LIVE,
            BotLensRuntimeState.DEGRADED,
            BotLensRuntimeState.STARTUP_FAILED,
            BotLensRuntimeState.CRASHED,
            BotLensRuntimeState.STOPPED,
        }
    ),
    BotLensRuntimeState.LIVE: frozenset(
        {
            BotLensRuntimeState.DEGRADED,
            BotLensRuntimeState.STOPPED,
            BotLensRuntimeState.CRASHED,
        }
    ),
    BotLensRuntimeState.DEGRADED: frozenset(
        {
            BotLensRuntimeState.LIVE,
            BotLensRuntimeState.STOPPED,
            BotLensRuntimeState.CRASHED,
        }
    ),
    BotLensRuntimeState.STOPPED: frozenset({BotLensRuntimeState.INITIALIZING}),
    BotLensRuntimeState.STARTUP_FAILED: frozenset({BotLensRuntimeState.INITIALIZING}),
    BotLensRuntimeState.CRASHED: frozenset({BotLensRuntimeState.INITIALIZING}),
}

_TERMINAL_STATES = frozenset(
    {
        BotLensRuntimeState.STOPPED,
        BotLensRuntimeState.STARTUP_FAILED,
        BotLensRuntimeState.CRASHED,
    }
)
_STARTUP_BOOTSTRAP_STATES = frozenset(
    {
        BotLensRuntimeState.INITIALIZING,
        BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT,
    }
)
_CONTINUITY_RECOVERY_STATES = frozenset(
    {
        BotLensRuntimeState.LIVE,
        BotLensRuntimeState.DEGRADED,
    }
)
_STARTUP_LIFECYCLE_PHASES = frozenset(
    {
        "start_requested",
        "validating_configuration",
        "resolving_strategy",
        "resolving_runtime_dependencies",
        "preparing_run",
        "stamping_starting_state",
        "launching_container",
        "container_launched",
        "awaiting_container_boot",
        "container_booting",
        "loading_bot_config",
        "claiming_run",
        "loading_strategy_snapshot",
        "preparing_wallet",
        "planning_series_workers",
        "spawning_series_workers",
        "waiting_for_series_bootstrap",
        "warming_up_runtime",
        "runtime_subscribing",
    }
)
_TERMINAL_LIFECYCLE_PHASES = frozenset({"startup_failed", "crashed", "stopped", "completed", "cancelled", "canceled"})


class InvalidRuntimeStateTransition(RuntimeError):
    pass


@dataclass(frozen=True)
class RuntimeStateTransition:
    from_state: Optional[str]
    to_state: str
    transition_reason: str
    source_component: str
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_state": self.from_state,
            "to_state": self.to_state,
            "transition_reason": self.transition_reason,
            "source_component": self.source_component,
            "timestamp": self.timestamp,
        }


@dataclass(frozen=True)
class StartupBootstrapAdmission:
    allowed: bool
    runtime_state: Optional[str]


def normalize_runtime_state(value: Any) -> Optional[BotLensRuntimeState]:
    if value in (None, ""):
        return None
    if isinstance(value, BotLensRuntimeState):
        return value
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    try:
        return BotLensRuntimeState(normalized)
    except ValueError as exc:
        raise ValueError(f"unsupported runtime state {value!r}") from exc


def runtime_state_value(value: Any) -> Optional[str]:
    state = normalize_runtime_state(value)
    return state.value if state is not None else None


def allowed_runtime_transitions() -> Dict[str, tuple[str, ...]]:
    mapping: Dict[str, tuple[str, ...]] = {}
    for state, targets in _ALLOWED_TRANSITIONS.items():
        if state is None:
            continue
        mapping[state.value] = tuple(target.value for target in sorted(targets, key=lambda item: item.value))
    return mapping


def guard_runtime_state_transition(
    *,
    current_state: Any,
    next_state: Any,
    transition_reason: Any,
    source_component: Any,
    timestamp: Any = None,
    allow_restart: bool = False,
) -> RuntimeStateTransition:
    current = normalize_runtime_state(current_state)
    target = normalize_runtime_state(next_state)
    if target is None:
        raise ValueError("next_state is required")
    reason_text = str(transition_reason or "").strip()
    if not reason_text:
        raise ValueError("transition_reason is required")
    source_text = str(source_component or "").strip()
    if not source_text:
        raise ValueError("source_component is required")
    event_time = str(timestamp or "").strip() or _utc_now_iso()

    if current == target:
        return RuntimeStateTransition(
            from_state=current.value if current is not None else None,
            to_state=target.value,
            transition_reason=reason_text,
            source_component=source_text,
            timestamp=event_time,
        )

    allowed_targets = _ALLOWED_TRANSITIONS.get(current, frozenset())
    if target not in allowed_targets:
        raise InvalidRuntimeStateTransition(
            f"illegal runtime state transition {current.value if current is not None else 'none'} -> {target.value} "
            f"| reason={reason_text} | source_component={source_text}"
        )
    if current in _TERMINAL_STATES and target == BotLensRuntimeState.INITIALIZING and not allow_restart:
        raise InvalidRuntimeStateTransition(
            f"runtime restart must be explicit for transition {current.value} -> {target.value}"
        )
    return RuntimeStateTransition(
        from_state=current.value if current is not None else None,
        to_state=target.value,
        transition_reason=reason_text,
        source_component=source_text,
        timestamp=event_time,
    )


def is_startup_bootstrap_state(value: Any) -> bool:
    state = normalize_runtime_state(value)
    return state in _STARTUP_BOOTSTRAP_STATES


def infer_runtime_state(
    *,
    runtime_state: Any = None,
    lifecycle_phase: Any = None,
    projection_seq: Any = None,
) -> Optional[str]:
    state = normalize_runtime_state(runtime_state)
    if state is not None:
        return state.value

    phase = str(lifecycle_phase or "").strip().lower()
    if phase == "awaiting_first_snapshot":
        return BotLensRuntimeState.AWAITING_FIRST_SNAPSHOT.value
    if phase == "live":
        return BotLensRuntimeState.LIVE.value
    if phase in {"degraded", "telemetry_degraded"}:
        return BotLensRuntimeState.DEGRADED.value
    if phase == "startup_failed":
        return BotLensRuntimeState.STARTUP_FAILED.value
    if phase == "crashed":
        return BotLensRuntimeState.CRASHED.value
    if phase in {"stopped", "completed", "cancelled", "canceled"}:
        return BotLensRuntimeState.STOPPED.value
    if phase in _STARTUP_LIFECYCLE_PHASES:
        return BotLensRuntimeState.INITIALIZING.value

    try:
        seq = int(projection_seq) if projection_seq is not None else None
    except (TypeError, ValueError):
        seq = None
    if seq is not None and seq <= 0:
        return BotLensRuntimeState.INITIALIZING.value
    return None


def startup_bootstrap_admission(
    *,
    runtime_state: Any = None,
    lifecycle_phase: Any = None,
    projection_seq: Any = None,
) -> StartupBootstrapAdmission:
    effective_state = infer_runtime_state(
        runtime_state=runtime_state,
        lifecycle_phase=lifecycle_phase,
        projection_seq=projection_seq,
    )
    if effective_state is None:
        try:
            seq = int(projection_seq) if projection_seq is not None else None
        except (TypeError, ValueError):
            seq = None
        return StartupBootstrapAdmission(allowed=bool(seq is None or seq <= 0), runtime_state=None)
    return StartupBootstrapAdmission(
        allowed=is_startup_bootstrap_state(effective_state),
        runtime_state=effective_state,
    )


def is_continuity_recovery_state(value: Any) -> bool:
    state = normalize_runtime_state(value)
    return state in _CONTINUITY_RECOVERY_STATES


def should_reset_projector_scope(value: Any) -> bool:
    return is_startup_bootstrap_state(value)


def summarize_transition_history(
    entries: Iterable[Mapping[str, Any]] | None,
    *,
    limit: int = 12,
) -> tuple[Dict[str, Any], ...]:
    if entries is None:
        return ()
    history = [dict(entry) for entry in entries if isinstance(entry, Mapping)]
    if int(limit) > 0 and len(history) > int(limit):
        history = history[-int(limit) :]
    return tuple(history)


__all__ = [
    "BotLensRuntimeState",
    "InvalidRuntimeStateTransition",
    "RuntimeStateTransition",
    "StartupBootstrapAdmission",
    "allowed_runtime_transitions",
    "guard_runtime_state_transition",
    "infer_runtime_state",
    "is_continuity_recovery_state",
    "is_startup_bootstrap_state",
    "normalize_runtime_state",
    "runtime_state_value",
    "startup_bootstrap_admission",
    "should_reset_projector_scope",
    "summarize_transition_history",
]

"""Pure lifecycle diagnostics projection for run-level UI consumers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Dict, Optional

from .startup_lifecycle import BotLifecyclePhase

_FAILURE_PHASES = {
    BotLifecyclePhase.STARTUP_FAILED.value,
    BotLifecyclePhase.CRASHED.value,
}
_ACTIVE_RUN_STATUSES = {"starting", "running", "degraded", "telemetry_degraded", "paused"}
_TERMINAL_SUCCESS_PHASES = {
    BotLifecyclePhase.COMPLETED.value,
    BotLifecyclePhase.STOPPED.value,
}
_CONTAINER_BOOTED_PHASES = {
    BotLifecyclePhase.CONTAINER_BOOTING.value,
    BotLifecyclePhase.LOADING_BOT_CONFIG.value,
    BotLifecyclePhase.CLAIMING_RUN.value,
    BotLifecyclePhase.LOADING_STRATEGY_SNAPSHOT.value,
    BotLifecyclePhase.PREPARING_WALLET.value,
    BotLifecyclePhase.PLANNING_SERIES_WORKERS.value,
    BotLifecyclePhase.SPAWNING_SERIES_WORKERS.value,
    BotLifecyclePhase.WAITING_FOR_SERIES_BOOTSTRAP.value,
    BotLifecyclePhase.WARMING_UP_RUNTIME.value,
    BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
    BotLifecyclePhase.AWAITING_FIRST_SNAPSHOT.value,
    BotLifecyclePhase.LIVE.value,
    BotLifecyclePhase.DEGRADED.value,
    BotLifecyclePhase.TELEMETRY_DEGRADED.value,
    BotLifecyclePhase.STARTUP_FAILED.value,
    BotLifecyclePhase.CRASHED.value,
    BotLifecyclePhase.STOPPED.value,
    BotLifecyclePhase.COMPLETED.value,
}
_PHASE_ORDER = [phase.value for phase in BotLifecyclePhase]
_PHASE_INDEX = {phase: index for index, phase in enumerate(_PHASE_ORDER)}
_LIVE_PHASE_INDEX = _PHASE_INDEX.get(BotLifecyclePhase.LIVE.value, len(_PHASE_ORDER))


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    return list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) else []


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _checkpoint_status(*, event: Mapping[str, Any], latest_phase: str, latest_run_status: str) -> str:
    phase = str(event.get("phase") or "").strip()
    status = _normalize_status(event.get("status"))
    if phase in _FAILURE_PHASES or status in {"failed", "error", "startup_failed", "crashed"} or _mapping(event.get("failure")):
        return "failed"
    if phase in _TERMINAL_SUCCESS_PHASES:
        return "completed"
    if phase == latest_phase and latest_run_status in _ACTIVE_RUN_STATUSES:
        return "running"
    return "completed"


def _normalize_failure(event: Mapping[str, Any]) -> Dict[str, Any]:
    payload = _mapping(event.get("failure"))
    if not payload and _checkpoint_status(event=event, latest_phase="", latest_run_status="") != "failed":
        return {}
    normalized = dict(payload)
    normalized.setdefault("message", str(event.get("message") or "").strip() or None)
    normalized.setdefault("phase", str(event.get("phase") or "").strip() or None)
    normalized.setdefault("owner", str(event.get("owner") or "").strip() or None)
    normalized.setdefault("at", event.get("checkpoint_at") or event.get("created_at"))
    if normalized.get("error_type") and not normalized.get("exception_type"):
        normalized["exception_type"] = normalized.get("error_type")
    return {key: value for key, value in normalized.items() if value not in (None, "", [], {})}


def _failure_rank(event: Mapping[str, Any], failure: Mapping[str, Any]) -> tuple[int, int]:
    score = 0
    if failure.get("worker_id"):
        score += 100
    if failure.get("symbol") or _sequence(failure.get("symbols")):
        score += 20
    if failure.get("reason_code"):
        score += 20
    if failure.get("exception_type"):
        score += 25
    if failure.get("traceback"):
        score += 25
    if failure.get("stderr_tail"):
        score += 10
    if failure.get("component"):
        score += 10
    if failure.get("operation"):
        score += 5
    owner = str(event.get("owner") or failure.get("owner") or "").strip().lower()
    if owner == "runtime":
        score += 5
    message = str(failure.get("message") or event.get("message") or "").strip().lower()
    if "at least one worker reported degraded terminal state" in message:
        score -= 50
    if "worker exited with code" in message and not failure.get("exception_type") and not failure.get("traceback"):
        score -= 10
    return score, -int(event.get("seq") or 0)


def _root_failure_entry(events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    candidates: list[tuple[tuple[int, int], Dict[str, Any]]] = []
    for event in events:
        failure = _normalize_failure(event)
        if not failure:
            continue
        candidates.append((_failure_rank(event, failure), {**failure, "_seq": int(event.get("seq") or 0)}))
    if not candidates:
        return {}
    candidates.sort(key=lambda entry: entry[0], reverse=True)
    return dict(candidates[0][1])


def _series_progress(event: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = _mapping(event.get("metadata"))
    return _mapping(metadata.get("series_progress"))


def _worker_snapshot(series_progress: Mapping[str, Any]) -> Dict[str, Any]:
    series_map = _mapping(series_progress.get("series"))
    worker_symbols: dict[str, set[str]] = {}
    any_live = False
    for symbol, raw_entry in series_map.items():
        entry = _mapping(raw_entry)
        worker_id = str(entry.get("worker_id") or "").strip()
        normalized_symbol = str(symbol or "").strip().upper()
        if worker_id and normalized_symbol:
            worker_symbols.setdefault(worker_id, set()).add(normalized_symbol)
        if str(entry.get("status") or "").strip() == "live":
            any_live = True
    failed_symbols = sorted(
        str(symbol).strip().upper()
        for symbol in _sequence(series_progress.get("failed_series"))
        if str(symbol).strip()
    )
    live_symbols = sorted(
        str(symbol).strip().upper()
        for symbol in _sequence(series_progress.get("live_series"))
        if str(symbol).strip()
    )
    return {
        "planned": max(0, int(series_progress.get("workers_planned") or 0)),
        "spawned": max(0, int(series_progress.get("workers_spawned") or 0)),
        "live": len(live_symbols),
        "failed": len(failed_symbols),
        "failed_symbols": failed_symbols,
        "worker_symbols": {worker_id: sorted(symbols) for worker_id, symbols in worker_symbols.items()},
        "series": series_map,
        "any_worker_live": any_live or bool(live_symbols),
    }


def _project_checkpoints(events: Sequence[Mapping[str, Any]], *, latest_phase: str, latest_run_status: str) -> list[Dict[str, Any]]:
    selected_by_phase: dict[str, Dict[str, Any]] = {}
    latest_phase_index = max((_PHASE_INDEX.get(str(event.get("phase") or "").strip(), -1) for event in events), default=-1)
    failure_index = min(
        (_PHASE_INDEX.get(str(event.get("phase") or "").strip(), len(_PHASE_ORDER)) for event in events if _normalize_failure(event)),
        default=None,
    )
    for event in events:
        phase = str(event.get("phase") or "").strip()
        if not phase:
            continue
        current = selected_by_phase.get(phase)
        next_row = {
            "phase": phase,
            "owner": event.get("owner"),
            "message": event.get("message"),
            "at": event.get("checkpoint_at") or event.get("created_at"),
            "seq": int(event.get("seq") or 0),
            "checkpoint_status": _checkpoint_status(event=event, latest_phase=latest_phase, latest_run_status=latest_run_status),
        }
        if current is None:
            selected_by_phase[phase] = next_row
            continue
        if next_row["checkpoint_status"] == "failed":
            continue
        selected_by_phase[phase] = next_row

    checkpoints: list[Dict[str, Any]] = []
    for index, phase in enumerate(_PHASE_ORDER):
        row = selected_by_phase.get(phase)
        if row is not None:
            checkpoints.append(row)
            continue
        if latest_phase_index < 0:
            continue
        if failure_index is not None:
            if index > latest_phase_index:
                continue
            checkpoints.append({"phase": phase, "checkpoint_status": "skipped"})
            continue
        if index <= latest_phase_index:
            checkpoints.append({"phase": phase, "checkpoint_status": "skipped"})
            continue
        if index <= _LIVE_PHASE_INDEX:
            checkpoints.append({"phase": phase, "checkpoint_status": "pending"})
    return checkpoints


def project_bot_run_diagnostics(
    *,
    run_id: str,
    lifecycle: Mapping[str, Any] | None,
    events: Sequence[Mapping[str, Any]] | None,
    run_health: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    lifecycle_row = _mapping(lifecycle)
    health_row = _mapping(run_health)
    raw_events = [dict(event) for event in (events or []) if isinstance(event, Mapping)]
    latest_event = raw_events[-1] if raw_events else lifecycle_row
    latest_phase = str((latest_event or {}).get("phase") or lifecycle_row.get("phase") or "").strip()
    latest_run_status = _normalize_status((latest_event or {}).get("status") or lifecycle_row.get("status"))

    projected_events: list[Dict[str, Any]] = []
    first_failure: Optional[Dict[str, Any]] = None
    last_successful_checkpoint: Optional[Dict[str, Any]] = None
    worker_summary = {
        "planned": 0,
        "spawned": 0,
        "live": 0,
        "failed": 0,
        "failed_symbols": [],
        "first_failed_worker_id": None,
        "first_failed_symbol": None,
        "failed_worker_count": 0,
        "any_worker_live": False,
    }
    failed_workers: set[str] = set()
    seen_failure = False

    for event in raw_events:
        normalized_failure = _normalize_failure(event)
        checkpoint_status = _checkpoint_status(event=event, latest_phase=latest_phase, latest_run_status=latest_run_status)
        projected_event = {
            **event,
            "checkpoint_status": checkpoint_status,
            "failure_details": normalized_failure,
        }
        projected_events.append(projected_event)

        if checkpoint_status == "completed" and not seen_failure:
            last_successful_checkpoint = {
                "phase": str(event.get("phase") or "").strip() or None,
                "owner": str(event.get("owner") or "").strip() or None,
                "message": str(event.get("message") or "").strip() or None,
                "at": event.get("checkpoint_at") or event.get("created_at"),
                "seq": int(event.get("seq") or 0),
            }
        if checkpoint_status == "failed" and first_failure is None:
            first_failure = {
                "phase": str(event.get("phase") or "").strip() or None,
                "owner": str(event.get("owner") or "").strip() or None,
                "message": str(
                    normalized_failure.get("message")
                    or event.get("message")
                    or ""
                ).strip()
                or None,
                "at": normalized_failure.get("at") or event.get("checkpoint_at") or event.get("created_at"),
                "worker_id": normalized_failure.get("worker_id"),
                "symbol": normalized_failure.get("symbol"),
                "reason_code": normalized_failure.get("reason_code"),
                "type": normalized_failure.get("type"),
            }
            seen_failure = True
        series_progress = _series_progress(event)
        if series_progress:
            snapshot = _worker_snapshot(series_progress)
            worker_summary["planned"] = max(worker_summary["planned"], snapshot["planned"])
            worker_summary["spawned"] = max(worker_summary["spawned"], snapshot["spawned"])
            worker_summary["live"] = max(worker_summary["live"], snapshot["live"])
            worker_summary["failed"] = max(worker_summary["failed"], snapshot["failed"])
            worker_summary["failed_symbols"] = sorted(
                set(worker_summary["failed_symbols"]) | set(snapshot["failed_symbols"])
            )
            worker_summary["any_worker_live"] = bool(worker_summary["any_worker_live"] or snapshot["any_worker_live"])
        if normalized_failure.get("worker_id"):
            failed_workers.add(str(normalized_failure["worker_id"]))
            if worker_summary["first_failed_worker_id"] is None:
                worker_summary["first_failed_worker_id"] = normalized_failure.get("worker_id")
        if normalized_failure.get("symbol") and worker_summary["first_failed_symbol"] is None:
            worker_summary["first_failed_symbol"] = str(normalized_failure["symbol"]).strip().upper()
        if worker_summary["first_failed_symbol"] is None:
            symbols = _sequence(normalized_failure.get("symbols"))
            if symbols:
                worker_summary["first_failed_symbol"] = str(symbols[0]).strip().upper()
        if worker_summary["first_failed_symbol"] is None and worker_summary["failed_symbols"]:
            worker_summary["first_failed_symbol"] = worker_summary["failed_symbols"][0]
        if worker_summary["first_failed_worker_id"] is None and worker_summary["first_failed_symbol"]:
            failed_symbol = str(worker_summary["first_failed_symbol"]).strip().upper()
            entry = _mapping(snapshot.get("series", {}).get(failed_symbol))
            if entry.get("worker_id"):
                worker_summary["first_failed_worker_id"] = str(entry.get("worker_id"))

    worker_summary["failed_worker_count"] = max(len(failed_workers), len(worker_summary["failed_symbols"]))
    root_failure = _root_failure_entry(projected_events)
    runtime_degraded = _mapping(health_row.get("degraded"))
    runtime_churn = _mapping(health_row.get("churn"))
    runtime_pressure = _mapping(health_row.get("pressure"))
    runtime_transitions = [
        dict(entry)
        for entry in _sequence(health_row.get("recent_transitions"))
        if isinstance(entry, Mapping)
    ]
    runtime_terminal = _mapping(health_row.get("terminal"))
    runtime = {
        "state": str(health_row.get("runtime_state") or "").strip() or None,
        "progress_state": str(health_row.get("progress_state") or "").strip() or None,
        "last_useful_progress_at": health_row.get("last_useful_progress_at"),
        "degraded": runtime_degraded,
        "churn": runtime_churn,
        "pressure": runtime_pressure,
        "top_pressure": _mapping(runtime_pressure.get("top_pressure")),
        "recent_transitions": runtime_transitions,
        "terminal": runtime_terminal,
        "current_status": str(health_row.get("status") or latest_run_status or "").strip() or None,
        "current_phase": health_row.get("phase") or latest_phase or None,
        "is_churning": bool(runtime_churn.get("active")),
        "is_degraded": bool(runtime_degraded.get("active")) or str(health_row.get("runtime_state") or "") == "degraded",
        "is_progressing": str(health_row.get("progress_state") or "").strip().lower() == "progressing",
    }
    summary = {
        "run_status": latest_run_status or None,
        "current_phase": latest_phase or None,
        "root_failure_phase": root_failure.get("phase") or (first_failure or {}).get("phase"),
        "root_failure_owner": root_failure.get("owner") or (first_failure or {}).get("owner"),
        "root_failure_message": root_failure.get("message") or (first_failure or {}).get("message"),
        "root_failure_reason_code": root_failure.get("reason_code"),
        "root_failure_type": root_failure.get("type"),
        "root_failure_worker_id": root_failure.get("worker_id"),
        "root_failure_symbol": root_failure.get("symbol"),
        "root_failure_exception_type": root_failure.get("exception_type"),
        "first_failure_at": (first_failure or {}).get("at"),
        "root_failure_at": root_failure.get("at") or (first_failure or {}).get("at"),
        "last_successful_checkpoint": last_successful_checkpoint,
        "container_launched": any(str(event.get("phase") or "").strip() == BotLifecyclePhase.CONTAINER_LAUNCHED.value for event in raw_events),
        "container_booted": any(str(event.get("phase") or "").strip() in _CONTAINER_BOOTED_PHASES for event in raw_events),
        "workers_planned": int(worker_summary["planned"] or 0),
        "workers_spawned": int(worker_summary["spawned"] or 0),
        "workers_live": int(worker_summary["live"] or 0),
        "workers_failed": int(worker_summary["failed"] or 0),
        "failed_symbols": list(worker_summary["failed_symbols"]),
        "first_failed_worker_id": worker_summary["first_failed_worker_id"],
        "first_failed_symbol": worker_summary["first_failed_symbol"],
        "failed_worker_count": int(worker_summary["failed_worker_count"] or 0),
        "any_worker_live": bool(worker_summary["any_worker_live"]),
        "crash_before_any_series_live": bool(first_failure) and not bool(worker_summary["any_worker_live"]),
        "runtime_state": runtime["state"],
        "progress_state": runtime["progress_state"],
        "last_useful_progress_at": runtime["last_useful_progress_at"],
        "degraded_since": runtime_degraded.get("started_at"),
        "degraded_cleared_at": runtime_degraded.get("cleared_at"),
        "is_churning": runtime["is_churning"],
        "top_pressure": runtime["top_pressure"] or None,
        "latest_runtime_transition": runtime_transitions[-1] if runtime_transitions else None,
        "final_observation": {
            "phase": str((latest_event or {}).get("phase") or "").strip() or None,
            "owner": str((latest_event or {}).get("owner") or "").strip() or None,
            "message": str((latest_event or {}).get("message") or "").strip() or None,
            "at": (latest_event or {}).get("checkpoint_at") or (latest_event or {}).get("created_at"),
            "status": latest_run_status or None,
        },
        "root_failure": {
            key: value
            for key, value in root_failure.items()
            if key != "_seq"
        }
        if root_failure
        else None,
    }

    return {
        "run_id": str(run_id or "").strip(),
        "run_status": latest_run_status or None,
        "summary": summary,
        "runtime": runtime,
        "checkpoints": _project_checkpoints(projected_events, latest_phase=latest_phase, latest_run_status=latest_run_status),
        "events": projected_events,
    }


__all__ = ["project_bot_run_diagnostics"]

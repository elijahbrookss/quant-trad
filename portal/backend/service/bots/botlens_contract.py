from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict

from .botlens_projection import bounded_projection, canonicalize_projection, find_series, normalize_series_key

SCHEMA_VERSION = 3

BRIDGE_BOOTSTRAP_KIND = "botlens_runtime_bootstrap_facts"
BRIDGE_FACTS_KIND = "botlens_runtime_facts"
LIFECYCLE_KIND = "botlens_lifecycle_event"
PROJECTION_REFRESH_KIND = "bot_projection_refresh"

EVENT_TYPE_RUNTIME_BOOTSTRAP = "botlens.runtime_bootstrap_facts"
EVENT_TYPE_RUNTIME_FACTS = "botlens.runtime_facts"
EVENT_TYPE_LIFECYCLE = "botlens.lifecycle_event"

FACT_TYPE_RUNTIME_STATE = "runtime_state_observed"
FACT_TYPE_SERIES_STATE = "series_state_observed"
FACT_TYPE_CANDLE_UPSERTED = "candle_upserted"
FACT_TYPE_OVERLAY_OPS = "overlay_ops_emitted"
FACT_TYPE_SERIES_STATS = "series_stats_updated"
FACT_TYPE_TRADE_UPSERTED = "trade_upserted"
FACT_TYPE_LOG_EMITTED = "log_emitted"
FACT_TYPE_DECISION_EMITTED = "decision_emitted"

CONTINUITY_READY = "ready"
CONTINUITY_BOOTSTRAP_REQUIRED = "bootstrap_required"
CONTINUITY_RESYNC_REQUIRED = "resync_required"


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def normalize_ingest_kind(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_bridge_session_id(payload: Mapping[str, Any]) -> str:
    session_id = str(
        payload.get("bridge_session_id")
        or payload.get("stream_session_id")
        or payload.get("session_id")
        or "legacy"
    ).strip()
    return session_id or "legacy"


def normalize_bridge_seq(payload: Mapping[str, Any]) -> int:
    for key in ("bridge_seq", "transport_seq"):
        try:
            value = int(payload.get(key) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return 0


def normalize_fact_entries(payload: Any) -> list[Dict[str, Any]]:
    entries = payload if isinstance(payload, list) else []
    normalized: list[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        fact_type = str(entry.get("fact_type") or "").strip().lower()
        if not fact_type:
            continue
        normalized.append({"fact_type": fact_type, **dict(entry)})
    return normalized


def normalize_lifecycle_payload(payload: Any) -> Dict[str, Any]:
    source = _mapping(payload)
    checkpoint_at = source.get("checkpoint_at") or source.get("updated_at") or source.get("known_at")
    phase = str(source.get("phase") or "").strip()
    status = str(source.get("status") or "").strip().lower()
    lifecycle = {
        "run_id": str(source.get("run_id") or "").strip() or None,
        "phase": phase or None,
        "status": status or None,
        "owner": str(source.get("owner") or "").strip() or None,
        "message": str(source.get("message") or "").strip() or None,
        "checkpoint_at": checkpoint_at,
        "updated_at": source.get("updated_at") or checkpoint_at,
        "metadata": _mapping(source.get("metadata")),
        "failure": _mapping(source.get("failure")),
    }
    lifecycle["live"] = bool(
        lifecycle["phase"] == "live"
        or lifecycle["status"] in {"running", "degraded", "telemetry_degraded", "paused"}
    )
    return {key: value for key, value in lifecycle.items() if value not in (None, "", [])}


def default_continuity(
    *,
    status: str = CONTINUITY_BOOTSTRAP_REQUIRED,
    reason: str | None = None,
    bridge_session_id: str | None = None,
    bridge_seq: int = 0,
    details: Mapping[str, Any] | None = None,
    invalidated_at: Any = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": str(status or CONTINUITY_BOOTSTRAP_REQUIRED),
        "bridge_session_id": str(bridge_session_id or "").strip() or None,
        "last_bridge_seq": int(bridge_seq or 0),
        "details": _mapping(details),
    }
    if reason:
        payload["reason"] = str(reason)
    if invalidated_at is not None:
        payload["invalidated_at"] = invalidated_at
    return payload


def projection_state_payload(
    *,
    projection: Mapping[str, Any],
    lifecycle: Mapping[str, Any] | None = None,
    continuity: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "projection": canonicalize_projection(projection),
        "lifecycle": normalize_lifecycle_payload(lifecycle),
        "continuity": dict(continuity or default_continuity()),
    }


def read_projection_state(payload: Any) -> Dict[str, Any]:
    source = _mapping(payload)
    if "projection" in source or "continuity" in source or "lifecycle" in source:
        projection = canonicalize_projection(source.get("projection"))
        continuity = _mapping(source.get("continuity")) or default_continuity(
            status=CONTINUITY_READY if projection.get("series") else CONTINUITY_BOOTSTRAP_REQUIRED
        )
        lifecycle = normalize_lifecycle_payload(source.get("lifecycle"))
        return {
            "schema_version": int(source.get("schema_version") or SCHEMA_VERSION),
            "projection": projection,
            "continuity": continuity,
            "lifecycle": lifecycle,
        }
    projection = canonicalize_projection(source)
    return {
        "schema_version": int(source.get("schema_version") or 1),
        "projection": projection,
        "continuity": default_continuity(
            status=CONTINUITY_READY if projection.get("series") else CONTINUITY_BOOTSTRAP_REQUIRED
        ),
        "lifecycle": {},
    }


def projection_only(payload: Any) -> Dict[str, Any]:
    return read_projection_state(payload)["projection"]


def continuity_only(payload: Any) -> Dict[str, Any]:
    return read_projection_state(payload)["continuity"]


def lifecycle_only(payload: Any) -> Dict[str, Any]:
    return read_projection_state(payload)["lifecycle"]


def build_window_payload(
    *,
    run_id: str,
    series_key: str,
    seq: int,
    event_time: Any,
    payload: Any,
    limit: int,
) -> Dict[str, Any]:
    state = read_projection_state(payload)
    bounded = bounded_projection(state["projection"], candle_limit=limit)
    selected_series = find_series(bounded, series_key) or {}
    candles = list(selected_series.get("candles") or []) if isinstance(selected_series.get("candles"), list) else []
    trades = [dict(trade) for trade in bounded.get("trades") if isinstance(trade, Mapping)] if isinstance(bounded.get("trades"), list) else []
    logs = list(bounded.get("logs") or []) if isinstance(bounded.get("logs"), list) else []
    decisions = list(bounded.get("decisions") or []) if isinstance(bounded.get("decisions"), list) else []
    warnings = list(bounded.get("warnings") or []) if isinstance(bounded.get("warnings"), list) else []
    runtime = dict(bounded.get("runtime") or {}) if isinstance(bounded.get("runtime"), Mapping) else {}
    return {
        "run_id": str(run_id),
        "series_key": normalize_series_key(series_key),
        "schema_version": SCHEMA_VERSION,
        "seq": int(seq),
        "event_time": event_time,
        "cursor": {"projection_seq": int(seq)},
        "continuity": dict(state["continuity"] or {}),
        "lifecycle": dict(state["lifecycle"] or {}),
        "window": {
            "projection": bounded,
            "selected_series": dict(selected_series) if isinstance(selected_series, Mapping) else {},
            "candles": candles,
            "trades": trades,
            "logs": logs,
            "decisions": decisions,
            "warnings": warnings,
            "runtime": runtime,
            "markers": [],
            "status": str(runtime.get("status") or "waiting"),
        },
    }


__all__ = [
    "BRIDGE_BOOTSTRAP_KIND",
    "BRIDGE_FACTS_KIND",
    "CONTINUITY_BOOTSTRAP_REQUIRED",
    "CONTINUITY_READY",
    "CONTINUITY_RESYNC_REQUIRED",
    "EVENT_TYPE_LIFECYCLE",
    "EVENT_TYPE_RUNTIME_FACTS",
    "EVENT_TYPE_RUNTIME_BOOTSTRAP",
    "FACT_TYPE_CANDLE_UPSERTED",
    "FACT_TYPE_DECISION_EMITTED",
    "FACT_TYPE_LOG_EMITTED",
    "FACT_TYPE_OVERLAY_OPS",
    "FACT_TYPE_RUNTIME_STATE",
    "FACT_TYPE_SERIES_STATE",
    "FACT_TYPE_SERIES_STATS",
    "FACT_TYPE_TRADE_UPSERTED",
    "LIFECYCLE_KIND",
    "PROJECTION_REFRESH_KIND",
    "SCHEMA_VERSION",
    "build_window_payload",
    "continuity_only",
    "default_continuity",
    "lifecycle_only",
    "normalize_fact_entries",
    "normalize_bridge_seq",
    "normalize_bridge_session_id",
    "normalize_ingest_kind",
    "normalize_lifecycle_payload",
    "projection_only",
    "projection_state_payload",
    "read_projection_state",
]

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict

from engines.bot_runtime.core.series_identity import normalize_series_key as normalize_public_series_key

SCHEMA_VERSION = 4
RUN_SCOPE_KEY = "__run__"

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

STREAM_CONNECTED_TYPE = "botlens_run_connected"
STREAM_SYMBOL_SNAPSHOT_TYPE = "botlens_symbol_snapshot"
STREAM_SUMMARY_DELTA_TYPE = "botlens_run_summary_delta"
STREAM_OPEN_TRADES_DELTA_TYPE = "botlens_open_trades_delta"
STREAM_SYMBOL_CANDLE_DELTA_TYPE = "symbol_candle_delta"
STREAM_SYMBOL_OVERLAY_DELTA_TYPE = "symbol_overlay_delta"
STREAM_SYMBOL_TRADE_DELTA_TYPE = "symbol_trade_delta"
STREAM_SYMBOL_LOG_DELTA_TYPE = "symbol_log_delta"
STREAM_SYMBOL_DECISION_DELTA_TYPE = "symbol_decision_delta"
STREAM_SYMBOL_RUNTIME_DELTA_TYPE = "symbol_runtime_delta"
STREAM_SYMBOL_DELTA_TYPES = (
    STREAM_SYMBOL_CANDLE_DELTA_TYPE,
    STREAM_SYMBOL_OVERLAY_DELTA_TYPE,
    STREAM_SYMBOL_TRADE_DELTA_TYPE,
    STREAM_SYMBOL_LOG_DELTA_TYPE,
    STREAM_SYMBOL_DECISION_DELTA_TYPE,
    STREAM_SYMBOL_RUNTIME_DELTA_TYPE,
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def normalize_series_key(value: Any) -> str:
    return normalize_public_series_key(value)


def is_run_scope_key(value: Any) -> bool:
    return str(value or "").strip() == RUN_SCOPE_KEY


def normalize_view_scope_key(value: Any) -> str:
    if is_run_scope_key(value):
        return RUN_SCOPE_KEY
    return normalize_series_key(value)


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


__all__ = [
    "BRIDGE_BOOTSTRAP_KIND",
    "BRIDGE_FACTS_KIND",
    "EVENT_TYPE_LIFECYCLE",
    "EVENT_TYPE_RUNTIME_BOOTSTRAP",
    "EVENT_TYPE_RUNTIME_FACTS",
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
    "RUN_SCOPE_KEY",
    "SCHEMA_VERSION",
    "STREAM_CONNECTED_TYPE",
    "STREAM_SYMBOL_SNAPSHOT_TYPE",
    "STREAM_OPEN_TRADES_DELTA_TYPE",
    "STREAM_SYMBOL_CANDLE_DELTA_TYPE",
    "STREAM_SYMBOL_DELTA_TYPES",
    "STREAM_SYMBOL_DECISION_DELTA_TYPE",
    "STREAM_SYMBOL_LOG_DELTA_TYPE",
    "STREAM_SYMBOL_OVERLAY_DELTA_TYPE",
    "STREAM_SYMBOL_RUNTIME_DELTA_TYPE",
    "STREAM_SYMBOL_TRADE_DELTA_TYPE",
    "STREAM_SUMMARY_DELTA_TYPE",
    "is_run_scope_key",
    "normalize_bridge_seq",
    "normalize_bridge_session_id",
    "normalize_fact_entries",
    "normalize_ingest_kind",
    "normalize_lifecycle_payload",
    "normalize_series_key",
    "normalize_view_scope_key",
]

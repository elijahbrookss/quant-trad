from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from core.events import EventEnvelope, normalize_utc_datetime, parse_optional_datetime, serialize_value

from .botlens_contract import (
    FACT_TYPE_CANDLE_UPSERTED,
    FACT_TYPE_DECISION_EMITTED,
    FACT_TYPE_LOG_EMITTED,
    FACT_TYPE_RUNTIME_STATE,
    FACT_TYPE_SERIES_STATE,
    FACT_TYPE_TRADE_CLOSED,
    FACT_TYPE_TRADE_OPENED,
    FACT_TYPE_TRADE_UPDATED,
    normalize_fact_entries,
    normalize_series_key,
)


SCHEMA_VERSION = 1
BOTLENS_DOMAIN_PREFIX = "botlens_domain."
_CORRELATION_ID_MAX_LEN = 128
_SERIES_SCOPED_EVENT_NAMES = frozenset(
    {
        "SERIES_METADATA_REPORTED",
        "CANDLE_OBSERVED",
        "OVERLAY_STATE_CHANGED",
        "SERIES_STATS_REPORTED",
        "SIGNAL_EMITTED",
        "DECISION_EMITTED",
        "TRADE_OPENED",
        "TRADE_UPDATED",
        "TRADE_CLOSED",
        "DIAGNOSTIC_RECORDED",
        "FAULT_RECORDED",
    }
)
_LIFECYCLE_EVENT_NAMES = frozenset(
    {
        "RUN_PHASE_REPORTED",
        "RUN_STARTED",
        "RUN_READY",
        "RUN_DEGRADED",
        "RUN_COMPLETED",
        "RUN_FAILED",
        "RUN_STOPPED",
        "RUN_CANCELLED",
    }
)
_CLOSED_DECISION_STATES = frozenset({"accepted", "rejected"})
_OVERLAY_PAYLOAD_LIST_KEYS = (
    "price_lines",
    "markers",
    "touchPoints",
    "touch_points",
    "boxes",
    "segments",
    "polylines",
    "bubbles",
    "regime_blocks",
)
_COMPACT_SERIES_STATS_KEYS = frozenset(
    {
        "total_trades",
        "completed_trades",
        "wins",
        "losses",
        "win_rate",
        "gross_pnl",
        "fees_paid",
        "total_fees",
        "net_pnl",
        "avg_win",
        "avg_loss",
        "largest_win",
        "largest_loss",
        "max_drawdown",
        "quote_currency",
    }
)
_MAX_DURABLE_HEALTH_TRANSITIONS = 4
_DURABLE_OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT = 160
_WARNING_SEVERITY_RANK = {
    "critical": 0,
    "error": 1,
    "warning": 2,
    "warn": 2,
    "info": 3,
}


class BotLensDomainEventName(str, Enum):
    RUN_PHASE_REPORTED = "RUN_PHASE_REPORTED"
    RUN_STARTED = "RUN_STARTED"
    RUN_READY = "RUN_READY"
    RUN_DEGRADED = "RUN_DEGRADED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"
    RUN_STOPPED = "RUN_STOPPED"
    RUN_CANCELLED = "RUN_CANCELLED"
    SERIES_METADATA_REPORTED = "SERIES_METADATA_REPORTED"
    CANDLE_OBSERVED = "CANDLE_OBSERVED"
    OVERLAY_STATE_CHANGED = "OVERLAY_STATE_CHANGED"
    SERIES_STATS_REPORTED = "SERIES_STATS_REPORTED"
    SIGNAL_EMITTED = "SIGNAL_EMITTED"
    DECISION_EMITTED = "DECISION_EMITTED"
    TRADE_OPENED = "TRADE_OPENED"
    TRADE_UPDATED = "TRADE_UPDATED"
    TRADE_CLOSED = "TRADE_CLOSED"
    DIAGNOSTIC_RECORDED = "DIAGNOSTIC_RECORDED"
    HEALTH_STATUS_REPORTED = "HEALTH_STATUS_REPORTED"
    FAULT_RECORDED = "FAULT_RECORDED"


def botlens_domain_event_type(value: BotLensDomainEventName | str) -> str:
    name = value.value if isinstance(value, BotLensDomainEventName) else str(value or "")
    normalized = name.strip().lower()
    if not normalized:
        raise ValueError("botlens domain event type requires a non-empty event name")
    return f"{BOTLENS_DOMAIN_PREFIX}{normalized}"


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_or_none(value: Any) -> Optional[Dict[str, Any]]:
    normalized = _mapping(value)
    return normalized or None


def _mapping_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(entry) for entry in value if isinstance(entry, Mapping)]


def _optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _require_text(value: Any, *, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required")
    return text


def _require_distinct_signal_id(
    *,
    signal_id: Optional[str],
    decision_id: Optional[str],
    field_prefix: str,
) -> None:
    normalized_signal_id = _optional_text(signal_id)
    normalized_decision_id = _optional_text(decision_id)
    if (
        normalized_signal_id is not None
        and normalized_decision_id is not None
        and normalized_signal_id == normalized_decision_id
    ):
        raise ValueError(
            f"{field_prefix}.signal_id must not equal {field_prefix}.decision_id "
            f"value={normalized_signal_id}"
        )


def _finite_float(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"{field_name} must be a finite number")
    return numeric


def _require_float_field(payload: Mapping[str, Any], key: str, *, field_name: str) -> float:
    value = payload.get(key)
    if value in (None, ""):
        raise ValueError(f"{field_name} is required")
    return _finite_float(value, field_name=field_name)


def _optional_float_field(payload: Mapping[str, Any], key: str, *, field_name: str) -> Optional[float]:
    value = payload.get(key)
    if value in (None, ""):
        return None
    return _require_float_field({key: value}, key, field_name=field_name)


def _event_hash(*parts: Any) -> str:
    payload = json.dumps([serialize_value(part) for part in parts], sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def _durable_overlay_payload_summary(payload: Any) -> Dict[str, Any]:
    mapping = _mapping(payload)
    counts: Dict[str, int] = {}
    geometry_keys: List[str] = []
    point_count = 0
    for key in _OVERLAY_PAYLOAD_LIST_KEYS:
        entries = mapping.get(key)
        if not isinstance(entries, list) or not entries:
            continue
        counts[key] = len(entries)
        geometry_keys.append(key)
        if key == "polylines":
            point_count += sum(
                len(entry.get("points") or [])
                for entry in entries
                if isinstance(entry, Mapping)
            )
    summary: Dict[str, Any] = {}
    if geometry_keys:
        summary["geometry_keys"] = geometry_keys
    if counts:
        summary["payload_counts"] = counts
    if point_count > 0:
        summary["point_count"] = int(point_count)
    return summary


def _durable_overlay_payload_point_limit() -> int:
    from core.settings import get_settings

    return max(
        1,
        int(
            get_settings().bot_runtime.botlens.max_overlay_points
            or _DURABLE_OVERLAY_PAYLOAD_FALLBACK_POINT_LIMIT
        ),
    )


def _compact_durable_overlay_payload(value: Any, *, max_items: int, path: tuple[str, ...] = ()) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _compact_durable_overlay_payload(
                entry,
                max_items=max_items,
                path=(*path, str(key)),
            )
            for key, entry in value.items()
        }
    if isinstance(value, list):
        polyline_history_limit = max(max_items, max_items * 4)
        preserve_full_list = bool(
            path
            and path[-1] == "points"
            and "polylines" in path
            and len(value) <= polyline_history_limit
        )
        subset = value if preserve_full_list or len(value) <= max_items else value[-max_items:]
        return [
            _compact_durable_overlay_payload(entry, max_items=max_items, path=path)
            for entry in subset
        ]
    return value


def _durable_overlay_payload(payload: Any) -> Dict[str, Any]:
    mapping = _mapping(payload)
    if not mapping:
        return {}
    serialized = serialize_value(mapping)
    if not isinstance(serialized, Mapping):
        return {}
    compacted = _compact_durable_overlay_payload(
        serialized,
        max_items=_durable_overlay_payload_point_limit(),
    )
    return dict(compacted) if isinstance(compacted, Mapping) else {}


def _durable_overlay_entry(key: Any, overlay: Any) -> Dict[str, Any]:
    mapping = _mapping(overlay)
    overlay_id = _optional_text(mapping.get("overlay_id")) or _optional_text(key)
    payload = _durable_overlay_payload(mapping.get("payload"))
    pane_views = [
        str(entry).strip()
        for entry in (mapping.get("pane_views") if isinstance(mapping.get("pane_views"), list) else [])
        if str(entry).strip()
    ]
    durable = {
        "overlay_id": overlay_id,
        "type": _optional_text(mapping.get("type")),
        "strategy_id": _optional_text(mapping.get("strategy_id")),
        "source": _optional_text(mapping.get("source")),
        "pane_key": _optional_text(mapping.get("pane_key")),
        "pane_views": pane_views or None,
        "detail_level": "bounded_render",
        "payload": payload or None,
        "payload_summary": _durable_overlay_payload_summary(payload),
    }
    durable["overlay_revision"] = _event_hash("overlay", overlay_id or key, mapping)
    return {
        entry_key: entry_value
        for entry_key, entry_value in durable.items()
        if entry_value not in (None, "", {}, [])
    }


def _durable_overlay_delta(value: Any) -> Dict[str, Any]:
    payload = _mapping(value)
    durable_ops: List[Dict[str, Any]] = []
    op_counts: Dict[str, int] = {}
    point_count = 0
    for op in payload.get("ops") if isinstance(payload.get("ops"), list) else []:
        if not isinstance(op, Mapping):
            continue
        op_name = str(op.get("op") or "").strip().lower()
        key = _optional_text(op.get("key"))
        if not op_name or not key:
            continue
        durable_op: Dict[str, Any] = {"op": op_name, "key": key}
        if op_name == "upsert":
            overlay_summary = _durable_overlay_entry(key, op.get("overlay"))
            if overlay_summary:
                durable_op["overlay"] = overlay_summary
                point_count += int(_mapping(overlay_summary.get("payload_summary")).get("point_count") or 0)
        durable_ops.append(durable_op)
        op_counts[op_name] = op_counts.get(op_name, 0) + 1
    durable = {
        "seq": _coerce_int(payload.get("seq")) if payload.get("seq") is not None else None,
        "base_seq": _coerce_int(payload.get("base_seq")) if payload.get("base_seq") is not None else None,
        "ops": durable_ops,
        "op_counts": op_counts or None,
        "point_count": point_count or None,
    }
    return {
        entry_key: entry_value
        for entry_key, entry_value in durable.items()
        if entry_value not in (None, "", {}, [])
    }


def _durable_series_stats(value: Any) -> Dict[str, Any]:
    payload = _mapping(value)
    durable: Dict[str, Any] = {}
    for key in sorted(_COMPACT_SERIES_STATS_KEYS):
        raw_value = payload.get(key)
        if raw_value in (None, "", [], {}, ()):
            continue
        if key == "quote_currency":
            normalized = _optional_text(raw_value)
            if normalized:
                durable[key] = normalized.upper()
            continue
        numeric = _coerce_float(raw_value)
        if numeric is None:
            continue
        if key in {"total_trades", "completed_trades", "wins", "losses"}:
            durable[key] = int(numeric)
        else:
            durable[key] = round(numeric, 4)
    return durable


def _semantic_pressure_fingerprint(payload: Mapping[str, Any]) -> Dict[str, Any]:
    pressure = _mapping(payload)
    top_pressure = _mapping(pressure.get("top_pressure"))
    semantic: Dict[str, Any] = {}
    if pressure.get("trigger") not in (None, ""):
        semantic["trigger"] = _optional_text(pressure.get("trigger"))
    if pressure.get("trigger_event") not in (None, ""):
        semantic["trigger_event"] = _optional_text(pressure.get("trigger_event"))
    if top_pressure:
        semantic["top_pressure"] = {
            "reason_code": _optional_text(top_pressure.get("reason_code")),
            "value": _coerce_float(top_pressure.get("value")),
            "unit": _optional_text(top_pressure.get("unit")),
        }
    return {key: value for key, value in semantic.items() if value not in (None, "", {}, [])}


def _compact_degraded_payload(payload: Any) -> Dict[str, Any]:
    degraded = _mapping(payload)
    durable = {
        "active": bool(degraded.get("active")),
        "started_at": _optional_text(degraded.get("started_at")),
        "reason_code": _optional_text(degraded.get("reason_code")),
        "trigger_event": _optional_text(degraded.get("trigger_event")),
        "cleared_at": _optional_text(degraded.get("cleared_at")),
        "recovery_reason": _optional_text(degraded.get("recovery_reason")),
        "duration_ms": _coerce_int(degraded.get("duration_ms")) if degraded.get("duration_ms") is not None else None,
    }
    return {key: value for key, value in durable.items() if value not in (None, "", {}, [])}


def _compact_churn_payload(payload: Any) -> Dict[str, Any]:
    churn = _mapping(payload)
    durable = {
        "active": bool(churn.get("active")),
        "detected_at": _optional_text(churn.get("detected_at")),
        "reason_code": _optional_text(churn.get("reason_code")),
        "activity_without_progress_count": (
            _coerce_int(churn.get("activity_without_progress_count"))
            if churn.get("activity_without_progress_count") is not None
            else None
        ),
        "last_useful_progress_at": _optional_text(churn.get("last_useful_progress_at")),
    }
    return {key: value for key, value in durable.items() if value not in (None, "", {}, [])}


def _compact_terminal_payload(payload: Any) -> Dict[str, Any]:
    terminal = _mapping(payload)
    worker_terminal_statuses = _mapping(terminal.get("worker_terminal_statuses"))
    durable = {
        "status": _optional_text(terminal.get("status")),
        "source": _optional_text(terminal.get("source")),
        "actor": _optional_text(terminal.get("actor")),
        "reason": _optional_text(terminal.get("reason")),
        "expected_workers": _coerce_int(terminal.get("expected_workers")) if terminal.get("expected_workers") is not None else None,
        "reported_workers": _coerce_int(terminal.get("reported_workers")) if terminal.get("reported_workers") is not None else None,
        "worker_terminal_status_count": len(worker_terminal_statuses) if worker_terminal_statuses else None,
    }
    return {key: value for key, value in durable.items() if value not in (None, "", {}, [])}


def _durable_candle(value: Any) -> Dict[str, Any]:
    candle = BotLensCandle.from_payload(_mapping(value))
    durable = {
        "time": candle.time,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
    }
    if candle.volume is not None:
        durable["volume"] = candle.volume
    return dict(serialize_value(durable))


def _recent_transition_fingerprint(entries: Any) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, Mapping):
            continue
        normalized.append(
            {
                "from_state": _optional_text(entry.get("from_state")),
                "to_state": _optional_text(entry.get("to_state")),
                "transition_reason": _optional_text(entry.get("transition_reason")),
                "source_component": _optional_text(entry.get("source_component")),
                "timestamp": _optional_text(entry.get("timestamp")),
            }
        )
    return normalized[-_MAX_DURABLE_HEALTH_TRANSITIONS:]


def _health_event_fingerprint(context_payload: Mapping[str, Any]) -> Dict[str, Any]:
    payload = _mapping(context_payload)
    durable_context = _durable_context_payload(
        BotLensDomainEventName.HEALTH_STATUS_REPORTED,
        payload,
    )
    return {
        "status": _optional_text(durable_context.get("status")),
        "warning_count": max(int(durable_context.get("warning_count") or 0), 0),
        "warning_types": [str(entry) for entry in durable_context.get("warning_types", []) if str(entry).strip()],
        "highest_warning_severity": _optional_text(durable_context.get("highest_warning_severity")),
        "worker_count": durable_context.get("worker_count"),
        "active_workers": durable_context.get("active_workers"),
        "trigger_event": _optional_text(durable_context.get("trigger_event")),
        "warnings": _warning_fingerprint_entries(payload.get("warnings")),
        "runtime_state": _optional_text(durable_context.get("runtime_state")),
        "progress_state": _optional_text(durable_context.get("progress_state")),
        "last_useful_progress_at": _optional_text(durable_context.get("last_useful_progress_at")),
        "degraded": _mapping_or_none(durable_context.get("degraded")),
        "churn": _mapping_or_none(durable_context.get("churn")),
        "pressure": _semantic_pressure_fingerprint(durable_context.get("pressure")),
        "recent_transitions": _recent_transition_fingerprint(durable_context.get("recent_transitions")),
        "terminal": _mapping_or_none(durable_context.get("terminal")),
    }


def _normalize_symbol(value: Any) -> Optional[str]:
    text = _optional_text(value)
    return text.upper() if text else None


def _normalize_timeframe(value: Any) -> Optional[str]:
    text = _optional_text(value)
    return text.lower() if text else None


def _normalize_warning_severity(value: Any) -> str:
    text = _optional_text(value)
    return text.lower() if text else "warning"


def canonicalize_health_warning(payload: Any) -> Dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    warning_id = _optional_text(payload.get("warning_id"))
    warning_type = _optional_text(payload.get("warning_type"))
    if not warning_id or not warning_type:
        return None
    normalized: Dict[str, Any] = {
        "warning_id": warning_id,
        "warning_type": warning_type,
        "severity": _normalize_warning_severity(payload.get("severity") or payload.get("level")),
        "message": _optional_text(payload.get("message")) or "Runtime warning",
        "count": max(_coerce_int(payload.get("count"), 1), 1),
    }
    title = _optional_text(payload.get("title"))
    if title:
        normalized["title"] = title
    indicator_id = _optional_text(payload.get("indicator_id"))
    if indicator_id:
        normalized["indicator_id"] = indicator_id
    symbol_key = normalize_series_key(payload.get("symbol_key"))
    if symbol_key:
        normalized["symbol_key"] = symbol_key
    symbol = _normalize_symbol(payload.get("symbol"))
    if symbol:
        normalized["symbol"] = symbol
    timeframe = _normalize_timeframe(payload.get("timeframe"))
    if timeframe:
        normalized["timeframe"] = timeframe
    first_seen_at = _optional_text(payload.get("first_seen_at") or payload.get("timestamp"))
    if first_seen_at:
        normalized["first_seen_at"] = first_seen_at
    last_seen_at = _optional_text(
        payload.get("last_seen_at") or payload.get("updated_at") or payload.get("timestamp")
    )
    if last_seen_at:
        normalized["last_seen_at"] = last_seen_at
    return normalized


def canonicalize_health_warnings(payload: Any) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for warning in payload if isinstance(payload, list) else []:
        canonical = canonicalize_health_warning(warning)
        if canonical is not None:
            normalized.append(canonical)
    normalized.sort(key=lambda entry: str(entry.get("warning_id") or ""))
    return normalized


def _warning_fingerprint_entries(payload: Any) -> List[Dict[str, Any]]:
    fingerprint: List[Dict[str, Any]] = []
    for warning in canonicalize_health_warnings(payload):
        fingerprint.append(
            {
                "warning_id": warning.get("warning_id"),
                "warning_type": warning.get("warning_type"),
                "severity": warning.get("severity"),
                "indicator_id": warning.get("indicator_id"),
                "symbol_key": warning.get("symbol_key"),
                "symbol": warning.get("symbol"),
                "timeframe": warning.get("timeframe"),
                "title": warning.get("title"),
                "message": warning.get("message"),
            }
        )
    return fingerprint


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _coerce_optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return bool(value)


def _coerce_epoch(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(normalize_utc_datetime(value).timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        parsed = parse_optional_datetime(value)
        if parsed is None:
            return None
        return int(parsed.timestamp())


def _normalize_symbol_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    normalized: List[str] = []
    for entry in value:
        symbol = _normalize_symbol(entry)
        if symbol and symbol not in normalized:
            normalized.append(symbol)
    return normalized


@dataclass(frozen=True, kw_only=True)
class BotLensDomainContextBase:
    run_id: str
    bot_id: str

    def __post_init__(self) -> None:
        if not str(self.run_id or "").strip():
            raise ValueError("context.run_id is required")
        if not str(self.bot_id or "").strip():
            raise ValueError("context.bot_id is required")

    def to_dict(self) -> Dict[str, Any]:
        return dict(serialize_value(self))


@dataclass(frozen=True, kw_only=True)
class BotLensSeriesContextBase(BotLensDomainContextBase):
    series_key: Optional[str] = None
    instrument_id: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    strategy_id: Optional[str] = None
    trade_id: Optional[str] = None
    bar_time: Optional[datetime] = None
    observed_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        object.__setattr__(self, "series_key", normalize_series_key(self.series_key) or None)
        object.__setattr__(self, "instrument_id", _optional_text(self.instrument_id))
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "timeframe", _normalize_timeframe(self.timeframe))
        object.__setattr__(self, "strategy_id", _optional_text(self.strategy_id))
        object.__setattr__(self, "trade_id", _optional_text(self.trade_id))
        if self.bar_time is not None:
            object.__setattr__(self, "bar_time", normalize_utc_datetime(self.bar_time))
        if self.observed_at is not None:
            object.__setattr__(self, "observed_at", normalize_utc_datetime(self.observed_at))

    def _require_series_key(self, *, event_name: str) -> None:
        if self.series_key is None:
            raise ValueError(f"context.series_key is required for {event_name}")


@dataclass(frozen=True)
class BotLensCandle:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    end: Optional[datetime] = None
    atr: Optional[float] = None
    volume: Optional[float] = None
    range: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "time", normalize_utc_datetime(self.time))
        object.__setattr__(self, "open", _finite_float(self.open, field_name="context.candle.open"))
        object.__setattr__(self, "high", _finite_float(self.high, field_name="context.candle.high"))
        object.__setattr__(self, "low", _finite_float(self.low, field_name="context.candle.low"))
        object.__setattr__(self, "close", _finite_float(self.close, field_name="context.candle.close"))
        if self.end is not None:
            object.__setattr__(self, "end", normalize_utc_datetime(self.end))
        for field_name in ("atr", "volume", "range"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite_float(value, field_name=f"context.candle.{field_name}"))

    def to_dict(self) -> Dict[str, Any]:
        return dict(serialize_value(self))

    @classmethod
    def from_payload(cls, value: Mapping[str, Any]) -> "BotLensCandle":
        if not isinstance(value, Mapping):
            raise ValueError("context.candle is required")
        candle_time = parse_optional_datetime(value.get("time"))
        if candle_time is None:
            raise ValueError("context.candle.time is required")
        return cls(
            time=candle_time,
            open=_require_float_field(value, "open", field_name="context.candle.open"),
            high=_require_float_field(value, "high", field_name="context.candle.high"),
            low=_require_float_field(value, "low", field_name="context.candle.low"),
            close=_require_float_field(value, "close", field_name="context.candle.close"),
            end=parse_optional_datetime(value.get("end")),
            atr=_optional_float_field(value, "atr", field_name="context.candle.atr"),
            volume=_optional_float_field(value, "volume", field_name="context.candle.volume"),
            range=_optional_float_field(value, "range", field_name="context.candle.range"),
        )


@dataclass(frozen=True, kw_only=True)
class RunLifecycleContext(BotLensDomainContextBase):
    phase: str
    status: str
    component: Optional[str] = None
    message: Optional[str] = None
    live: bool = False
    metadata: Dict[str, Any] | None = None
    failure: Dict[str, Any] | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        phase = _require_text(self.phase, field_name="context.phase")
        status = _require_text(self.status, field_name="context.status")
        object.__setattr__(self, "phase", phase.lower())
        object.__setattr__(self, "status", status.lower())
        object.__setattr__(self, "component", _optional_text(self.component))
        object.__setattr__(self, "message", _optional_text(self.message))
        object.__setattr__(self, "metadata", _mapping_or_none(self.metadata))
        object.__setattr__(self, "failure", _mapping_or_none(self.failure))

    def to_dict(self) -> Dict[str, Any]:
        payload = super().to_dict()
        if payload.get("metadata") in (None, {}, []):
            payload.pop("metadata", None)
        if payload.get("failure") in (None, {}, []):
            payload.pop("failure", None)
        return payload


@dataclass(frozen=True, kw_only=True)
class CandleObservedContext(BotLensSeriesContextBase):
    candle: BotLensCandle

    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="CANDLE_OBSERVED")
        if not isinstance(self.candle, BotLensCandle):
            raise ValueError("context.candle is required")
        if self.bar_time is None:
            object.__setattr__(self, "bar_time", self.candle.time)


@dataclass(frozen=True, kw_only=True)
class SeriesMetadataReportedContext(BotLensSeriesContextBase):
    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="SERIES_METADATA_REPORTED")


@dataclass(frozen=True, kw_only=True)
class OverlayStateChangedContext(BotLensSeriesContextBase):
    overlay_delta: Dict[str, Any]

    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="OVERLAY_STATE_CHANGED")
        if not isinstance(self.overlay_delta, Mapping):
            raise ValueError("context.overlay_delta is required")
        object.__setattr__(self, "overlay_delta", _mapping(self.overlay_delta))


@dataclass(frozen=True, kw_only=True)
class SeriesStatsReportedContext(BotLensSeriesContextBase):
    stats: Dict[str, Any]

    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="SERIES_STATS_REPORTED")
        if not isinstance(self.stats, Mapping):
            raise ValueError("context.stats is required")
        object.__setattr__(self, "stats", _mapping(self.stats))


@dataclass(frozen=True, kw_only=True)
class SignalEmittedContext(BotLensSeriesContextBase):
    signal_id: str
    signal_type: str
    direction: str
    signal_price: float
    strategy_hash: Optional[str] = None
    bar_epoch: Optional[int] = None
    decision_id: Optional[str] = None
    rule_id: Optional[str] = None
    intent: Optional[str] = None
    event_key: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="SIGNAL_EMITTED")
        signal_id = _require_text(self.signal_id, field_name="context.signal_id")
        decision_id = _optional_text(self.decision_id)
        _require_distinct_signal_id(
            signal_id=signal_id,
            decision_id=decision_id,
            field_prefix="context",
        )
        signal_type = _require_text(self.signal_type, field_name="context.signal_type")
        direction = _require_text(self.direction, field_name="context.direction")
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "signal_type", signal_type)
        object.__setattr__(self, "direction", direction.lower())
        object.__setattr__(self, "signal_price", _finite_float(self.signal_price, field_name="context.signal_price"))
        object.__setattr__(self, "strategy_hash", _optional_text(self.strategy_hash))
        bar_epoch = _coerce_epoch(self.bar_epoch)
        if bar_epoch is None and self.bar_time is not None:
            bar_epoch = int(self.bar_time.timestamp())
        if bar_epoch is None:
            raise ValueError("context.bar_epoch or context.bar_time is required for SIGNAL_EMITTED")
        object.__setattr__(self, "bar_epoch", bar_epoch)
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(self, "rule_id", _optional_text(self.rule_id))
        object.__setattr__(self, "intent", _optional_text(self.intent))
        object.__setattr__(self, "event_key", _optional_text(self.event_key))


@dataclass(frozen=True, kw_only=True)
class DecisionEmittedContext(BotLensSeriesContextBase):
    decision_state: str
    decision_id: str
    strategy_hash: Optional[str] = None
    bar_epoch: Optional[int] = None
    signal_id: Optional[str] = None
    direction: Optional[str] = None
    signal_price: Optional[float] = None
    reason_code: Optional[str] = None
    message: Optional[str] = None
    attempt_id: Optional[str] = None
    order_request_id: Optional[str] = None
    entry_request_id: Optional[str] = None
    settlement_attempt_id: Optional[str] = None
    blocking_trade_id: Optional[str] = None
    intent: Optional[str] = None
    rule_id: Optional[str] = None
    event_key: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="DECISION_EMITTED")
        decision_state = _require_text(self.decision_state, field_name="context.decision_state")
        decision_id = _require_text(self.decision_id, field_name="context.decision_id")
        signal_id = _optional_text(self.signal_id)
        _require_distinct_signal_id(
            signal_id=signal_id,
            decision_id=decision_id,
            field_prefix="context",
        )
        normalized_state = decision_state.lower()
        if normalized_state not in _CLOSED_DECISION_STATES:
            raise ValueError("context.decision_state must be one of: accepted, rejected")
        object.__setattr__(self, "decision_state", normalized_state)
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(self, "strategy_hash", _optional_text(self.strategy_hash))
        bar_epoch = _coerce_epoch(self.bar_epoch)
        if bar_epoch is None and self.bar_time is not None:
            bar_epoch = int(self.bar_time.timestamp())
        if bar_epoch is None:
            raise ValueError("context.bar_epoch or context.bar_time is required for DECISION_EMITTED")
        object.__setattr__(self, "bar_epoch", bar_epoch)
        object.__setattr__(self, "signal_id", signal_id)
        direction = _optional_text(self.direction)
        object.__setattr__(self, "direction", direction.lower() if direction else None)
        if self.signal_price is not None:
            object.__setattr__(self, "signal_price", _finite_float(self.signal_price, field_name="context.signal_price"))
        object.__setattr__(self, "reason_code", _optional_text(self.reason_code))
        object.__setattr__(self, "message", _optional_text(self.message))
        object.__setattr__(self, "attempt_id", _optional_text(self.attempt_id))
        object.__setattr__(self, "order_request_id", _optional_text(self.order_request_id))
        object.__setattr__(self, "entry_request_id", _optional_text(self.entry_request_id))
        object.__setattr__(self, "settlement_attempt_id", _optional_text(self.settlement_attempt_id))
        object.__setattr__(self, "blocking_trade_id", _optional_text(self.blocking_trade_id))
        object.__setattr__(self, "intent", _optional_text(self.intent))
        object.__setattr__(self, "rule_id", _optional_text(self.rule_id))
        object.__setattr__(self, "event_key", _optional_text(self.event_key))
        if self.decision_state == "accepted":
            if self.direction is None:
                raise ValueError("context.direction is required for accepted decisions")
            if self.signal_price is None:
                raise ValueError("context.signal_price is required for accepted decisions")
        if self.decision_state == "rejected":
            if self.reason_code is None:
                raise ValueError("context.reason_code is required for rejected decisions")
            if self.message is None:
                raise ValueError("context.message is required for rejected decisions")


@dataclass(frozen=True, kw_only=True)
class TradeLifecycleContext(BotLensSeriesContextBase):
    trade_state: str
    side: Optional[str] = None
    direction: Optional[str] = None
    qty: Optional[float] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    realized_pnl: Optional[float] = None
    event_impact_pnl: Optional[float] = None
    trade_net_pnl: Optional[float] = None
    signal_id: Optional[str] = None
    decision_id: Optional[str] = None
    event_time: Optional[datetime] = None
    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self._require_series_key(event_name="TRADE")
        trade_state = _optional_text(self.trade_state)
        if trade_state is None:
            raise ValueError("context.trade_state is required")
        object.__setattr__(self, "trade_state", trade_state.lower())
        if self.bar_time is None:
            raise ValueError("context.bar_time is required for TRADE")
        side = _optional_text(self.side)
        direction = _optional_text(self.direction)
        object.__setattr__(self, "side", side.lower() if side else None)
        object.__setattr__(self, "direction", direction.lower() if direction else None)
        for field_name in ("qty", "entry_price", "exit_price", "realized_pnl", "event_impact_pnl", "trade_net_pnl"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite_float(value, field_name=f"context.{field_name}"))
        signal_id = _optional_text(self.signal_id)
        decision_id = _optional_text(self.decision_id)
        _require_distinct_signal_id(
            signal_id=signal_id,
            decision_id=decision_id,
            field_prefix="context",
        )
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "decision_id", decision_id)
        if self.event_time is not None:
            object.__setattr__(self, "event_time", normalize_utc_datetime(self.event_time))
        if self.opened_at is not None:
            object.__setattr__(self, "opened_at", normalize_utc_datetime(self.opened_at))
        if self.closed_at is not None:
            object.__setattr__(self, "closed_at", normalize_utc_datetime(self.closed_at))


@dataclass(frozen=True, kw_only=True)
class DiagnosticRecordedContext(BotLensSeriesContextBase):
    diagnostic_id: Optional[str]
    level: str
    message: str
    diagnostic_code: Optional[str] = None
    diagnostic_event: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    status: Optional[str] = None
    failure_mode: Optional[str] = None
    request_id: Optional[str] = None
    trace_id: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        level = _require_text(self.level, field_name="context.level")
        message = _require_text(self.message, field_name="context.message")
        object.__setattr__(self, "diagnostic_id", _optional_text(self.diagnostic_id))
        object.__setattr__(self, "level", level.upper())
        object.__setattr__(self, "diagnostic_code", _optional_text(self.diagnostic_code))
        object.__setattr__(self, "diagnostic_event", _optional_text(self.diagnostic_event))
        object.__setattr__(self, "message", message)
        object.__setattr__(self, "component", _optional_text(self.component))
        object.__setattr__(self, "operation", _optional_text(self.operation))
        status = _optional_text(self.status)
        object.__setattr__(self, "status", status.lower() if status else None)
        failure_mode = _optional_text(self.failure_mode)
        object.__setattr__(self, "failure_mode", failure_mode.lower() if failure_mode else None)
        object.__setattr__(self, "request_id", _optional_text(self.request_id))
        object.__setattr__(self, "trace_id", _optional_text(self.trace_id))


@dataclass(frozen=True, kw_only=True)
class HealthStatusReportedContext(BotLensDomainContextBase):
    status: str
    warning_count: int = 0
    warning_types: List[str] | None = None
    highest_warning_severity: Optional[str] = None
    worker_count: Optional[int] = None
    active_workers: Optional[int] = None
    trigger_event: Optional[str] = None
    warnings: List[Dict[str, Any]] | None = None
    runtime_state: Optional[str] = None
    last_useful_progress_at: Optional[str] = None
    progress_state: Optional[str] = None
    degraded: Dict[str, Any] | None = None
    churn: Dict[str, Any] | None = None
    pressure: Dict[str, Any] | None = None
    recent_transitions: List[Dict[str, Any]] | None = None
    terminal: Dict[str, Any] | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        status = _optional_text(self.status)
        if status is None:
            raise ValueError("context.status is required")
        object.__setattr__(self, "status", status.lower())
        normalized_warnings = canonicalize_health_warnings(self.warnings)
        object.__setattr__(self, "warning_count", max(int(self.warning_count or 0), len(normalized_warnings), 0))
        normalized_warning_types = [
            str(entry).strip().lower()
            for entry in (self.warning_types or [])
            if str(entry).strip()
        ]
        if not normalized_warning_types and normalized_warnings:
            normalized_warning_types = sorted(
                {
                    str(entry.get("warning_type") or "").strip().lower()
                    for entry in normalized_warnings
                    if str(entry.get("warning_type") or "").strip()
                }
            )
        highest_warning_severity = _optional_text(self.highest_warning_severity)
        if highest_warning_severity:
            highest_warning_severity = highest_warning_severity.lower()
        if highest_warning_severity is None and normalized_warnings:
            highest_warning_severity = min(
                (
                    str(entry.get("severity") or "warning").strip().lower()
                    for entry in normalized_warnings
                ),
                key=lambda value: (_WARNING_SEVERITY_RANK.get(value, 99), value),
            )
        object.__setattr__(self, "warning_types", normalized_warning_types or None)
        object.__setattr__(self, "highest_warning_severity", highest_warning_severity)
        if self.worker_count is not None:
            object.__setattr__(self, "worker_count", max(int(self.worker_count), 0))
        if self.active_workers is not None:
            object.__setattr__(self, "active_workers", max(int(self.active_workers), 0))
        object.__setattr__(self, "trigger_event", _optional_text(self.trigger_event))
        object.__setattr__(self, "warnings", normalized_warnings or None)
        object.__setattr__(self, "runtime_state", _optional_text(self.runtime_state))
        object.__setattr__(self, "last_useful_progress_at", _optional_text(self.last_useful_progress_at))
        object.__setattr__(self, "progress_state", _optional_text(self.progress_state))
        object.__setattr__(self, "degraded", _mapping_or_none(self.degraded))
        object.__setattr__(self, "churn", _mapping_or_none(self.churn))
        object.__setattr__(self, "pressure", _mapping_or_none(self.pressure))
        object.__setattr__(self, "recent_transitions", _mapping_list(self.recent_transitions) or None)
        object.__setattr__(self, "terminal", _mapping_or_none(self.terminal))

    def to_dict(self) -> Dict[str, Any]:
        payload = super().to_dict()
        for key in (
            "warning_types",
            "highest_warning_severity",
            "runtime_state",
            "last_useful_progress_at",
            "progress_state",
            "degraded",
            "churn",
            "pressure",
            "recent_transitions",
            "terminal",
        ):
            if payload.get(key) in (None, {}, []):
                payload.pop(key, None)
        return payload


@dataclass(frozen=True, kw_only=True)
class FaultRecordedContext(BotLensSeriesContextBase):
    fault_code: str
    severity: str
    message: str
    source: str
    component: Optional[str] = None
    failure_type: Optional[str] = None
    failure_phase: Optional[str] = None
    recoverable: Optional[bool] = None
    exception_type: Optional[str] = None
    location: Optional[str] = None
    worker_id: Optional[str] = None
    affected_symbols: List[str] | None = None
    exit_code: Optional[int] = None
    from_state: Optional[str] = None
    attempted_to_state: Optional[str] = None
    transition_reason: Optional[str] = None

    def __post_init__(self) -> None:
        super().__post_init__()
        fault_code = _require_text(self.fault_code, field_name="context.fault_code")
        severity = _require_text(self.severity, field_name="context.severity")
        message = _require_text(self.message, field_name="context.message")
        source = _require_text(self.source, field_name="context.source")
        object.__setattr__(self, "fault_code", fault_code.lower())
        object.__setattr__(self, "severity", severity.upper())
        object.__setattr__(self, "message", message)
        object.__setattr__(self, "source", source.lower())
        object.__setattr__(self, "component", _optional_text(self.component))
        object.__setattr__(self, "failure_type", _optional_text(self.failure_type))
        failure_phase = _optional_text(self.failure_phase)
        object.__setattr__(self, "failure_phase", failure_phase.lower() if failure_phase else None)
        object.__setattr__(self, "recoverable", _coerce_optional_bool(self.recoverable))
        object.__setattr__(self, "exception_type", _optional_text(self.exception_type))
        object.__setattr__(self, "location", _optional_text(self.location))
        object.__setattr__(self, "worker_id", _optional_text(self.worker_id))
        object.__setattr__(self, "affected_symbols", _normalize_symbol_list(self.affected_symbols))
        if self.exit_code is not None:
            object.__setattr__(self, "exit_code", int(self.exit_code))
        object.__setattr__(self, "from_state", _optional_text(self.from_state))
        object.__setattr__(self, "attempted_to_state", _optional_text(self.attempted_to_state))
        object.__setattr__(self, "transition_reason", _optional_text(self.transition_reason))


BotLensDomainContext = (
    RunLifecycleContext
    | SeriesMetadataReportedContext
    | CandleObservedContext
    | OverlayStateChangedContext
    | SeriesStatsReportedContext
    | SignalEmittedContext
    | DecisionEmittedContext
    | TradeLifecycleContext
    | DiagnosticRecordedContext
    | HealthStatusReportedContext
    | FaultRecordedContext
)


@dataclass(frozen=True)
class BotLensDomainEvent(EventEnvelope):
    event_name: BotLensDomainEventName
    context: BotLensDomainContext

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.event_name, BotLensDomainEventName):
            raise ValueError("event_name must be a BotLensDomainEventName")


def _new_event(
    *,
    event_name: BotLensDomainEventName,
    event_id: str,
    event_ts: datetime,
    correlation_id: str,
    context: BotLensDomainContext,
    root_id: Optional[str] = None,
    parent_id: Optional[str] = None,
) -> BotLensDomainEvent:
    return BotLensDomainEvent(
        schema_version=SCHEMA_VERSION,
        event_id=event_id,
        event_ts=normalize_utc_datetime(event_ts),
        event_name=event_name,
        root_id=str(root_id or event_id),
        parent_id=_optional_text(parent_id),
        correlation_id=correlation_id,
        context=context,
    )


def serialize_botlens_domain_event(event: BotLensDomainEvent) -> Dict[str, Any]:
    payload = event.serialize()
    payload["context"] = _durable_context_payload(
        event.event_name,
        _mapping(payload.get("context")),
    )
    return payload


def _durable_context_payload(
    event_name: BotLensDomainEventName,
    context_payload: Mapping[str, Any],
) -> Dict[str, Any]:
    context = _mapping(context_payload)
    if event_name == BotLensDomainEventName.CANDLE_OBSERVED:
        durable = {
            key: context.get(key)
            for key in ("run_id", "bot_id", "series_key", "instrument_id", "symbol", "timeframe", "strategy_id", "trade_id", "bar_time")
            if context.get(key) not in (None, "", [], {}, ())
        }
        durable["candle"] = _durable_candle(context.get("candle"))
        return durable
    if event_name == BotLensDomainEventName.OVERLAY_STATE_CHANGED:
        durable = {
            key: context.get(key)
            for key in ("run_id", "bot_id", "series_key", "instrument_id", "symbol", "timeframe", "strategy_id", "trade_id", "bar_time")
            if context.get(key) not in (None, "", [], {}, ())
        }
        durable["overlay_delta"] = _durable_overlay_delta(context.get("overlay_delta"))
        return durable
    if event_name == BotLensDomainEventName.SERIES_STATS_REPORTED:
        durable = {
            key: context.get(key)
            for key in ("run_id", "bot_id", "series_key", "instrument_id", "symbol", "timeframe", "strategy_id", "trade_id", "bar_time")
            if context.get(key) not in (None, "", [], {}, ())
        }
        durable["stats"] = _durable_series_stats(context.get("stats"))
        return durable
    if event_name == BotLensDomainEventName.HEALTH_STATUS_REPORTED:
        warnings = canonicalize_health_warnings(context.get("warnings"))
        warning_types = sorted(
            {
                str(entry).strip().lower()
                for entry in context.get("warning_types", [])
                if str(entry).strip()
            }
        ) or sorted(
            {
                str(entry.get("warning_type") or "").strip().lower()
                for entry in warnings
                if str(entry.get("warning_type") or "").strip()
            }
        )
        highest_warning_severity = _optional_text(context.get("highest_warning_severity"))
        if highest_warning_severity:
            highest_warning_severity = highest_warning_severity.lower()
        if highest_warning_severity is None:
            highest_warning_severity = min(
                (
                    str(entry.get("severity") or "warning").strip().lower()
                    for entry in warnings
                ),
                key=lambda value: (_WARNING_SEVERITY_RANK.get(value, 99), value),
                default=None,
            )
        durable = {
            "run_id": context.get("run_id"),
            "bot_id": context.get("bot_id"),
            "status": _optional_text(context.get("status")),
            "warning_count": max(int(context.get("warning_count") or 0), len(warnings), 0),
            "warning_types": warning_types or None,
            "highest_warning_severity": highest_warning_severity,
            "worker_count": _coerce_int(context.get("worker_count")) if context.get("worker_count") is not None else None,
            "active_workers": _coerce_int(context.get("active_workers")) if context.get("active_workers") is not None else None,
            "trigger_event": _optional_text(context.get("trigger_event")),
            "warnings": warnings or None,
            "runtime_state": _optional_text(context.get("runtime_state")),
            "last_useful_progress_at": _optional_text(context.get("last_useful_progress_at")),
            "progress_state": _optional_text(context.get("progress_state")),
            "degraded": _compact_degraded_payload(context.get("degraded")),
            "churn": _compact_churn_payload(context.get("churn")),
            "pressure": _semantic_pressure_fingerprint(context.get("pressure")),
            "recent_transitions": _recent_transition_fingerprint(context.get("recent_transitions")),
            "terminal": _compact_terminal_payload(context.get("terminal")),
        }
        return {
            key: value
            for key, value in durable.items()
            if value not in (None, "", [], {}, ())
        }
    return context


def canonicalize_botlens_candle(value: Any) -> Dict[str, Any]:
    candle_payload = dict(value) if isinstance(value, Mapping) else value
    candle = BotLensCandle.from_payload(candle_payload)
    normalized = dict(candle_payload) if isinstance(candle_payload, Mapping) else {}
    normalized["time"] = int(candle.time.timestamp())
    normalized["open"] = float(candle.open)
    normalized["high"] = float(candle.high)
    normalized["low"] = float(candle.low)
    normalized["close"] = float(candle.close)
    if candle.end is not None:
        normalized["end"] = int(candle.end.timestamp())
    for field_name in ("atr", "volume", "range"):
        value = getattr(candle, field_name)
        if value is not None:
            normalized[field_name] = float(value)
    return normalized


def _identity_fields(identity: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "series_key": normalize_series_key(identity.get("series_key")),
        "instrument_id": _optional_text(identity.get("instrument_id")),
        "symbol": _normalize_symbol(identity.get("symbol")),
        "timeframe": _normalize_timeframe(identity.get("timeframe")),
    }


def _base_context(
    *,
    bot_id: str,
    run_id: str,
    identity: Mapping[str, Any],
    strategy_id: Any = None,
    trade_id: Any = None,
    bar_time: Any = None,
    observed_at: Any = None,
) -> Dict[str, Any]:
    fields = _identity_fields(identity)
    return {
        "bot_id": str(bot_id),
        "run_id": str(run_id),
        "series_key": fields["series_key"],
        "instrument_id": fields["instrument_id"],
        "symbol": fields["symbol"],
        "timeframe": fields["timeframe"],
        "strategy_id": _optional_text(strategy_id),
        "trade_id": _optional_text(trade_id),
        "bar_time": parse_optional_datetime(bar_time),
        "observed_at": parse_optional_datetime(observed_at),
    }


def _series_identity_from_facts(payload: Mapping[str, Any]) -> Dict[str, Any]:
    identity = {
        "series_key": normalize_series_key(payload.get("series_key")),
        "instrument_id": None,
        "symbol": None,
        "timeframe": None,
    }
    for fact in normalize_fact_entries(payload.get("facts")):
        if str(fact.get("fact_type") or "").strip().lower() != FACT_TYPE_SERIES_STATE:
            continue
        if fact.get("series_key"):
            identity["series_key"] = normalize_series_key(fact.get("series_key"))
        if fact.get("instrument_id"):
            identity["instrument_id"] = _optional_text(fact.get("instrument_id"))
        if fact.get("symbol"):
            identity["symbol"] = _normalize_symbol(fact.get("symbol"))
        if fact.get("timeframe"):
            identity["timeframe"] = _normalize_timeframe(fact.get("timeframe"))
    return identity


def _correlation_id(*, run_id: str, series_key: Optional[str], scope: str, event_ts: datetime) -> str:
    normalized_run_id = str(run_id or "").strip()
    normalized_series_key = str(series_key or "__run__").strip() or "__run__"
    normalized_scope = str(scope or "").strip() or "event"
    normalized_event_ts = normalize_utc_datetime(event_ts).isoformat().replace("+00:00", "Z")
    natural = f"{normalized_run_id}:{normalized_series_key}:{normalized_scope}:{normalized_event_ts}"
    if len(natural) <= _CORRELATION_ID_MAX_LEN:
        return natural
    scope_label = normalized_scope.split(":", 1)[0].strip() or "event"
    prefix = f"{normalized_run_id}:{normalized_series_key}:{scope_label}"
    digest = hashlib.sha1(natural.encode("utf-8")).hexdigest()[:20]
    prefix_budget = _CORRELATION_ID_MAX_LEN - len(digest) - 1
    if prefix_budget <= 0:
        return digest[:_CORRELATION_ID_MAX_LEN]
    return f"{prefix[:prefix_budget]}:{digest}"


def _decision_fact_root(fact: Mapping[str, Any]) -> Dict[str, Any]:
    payload = _mapping(fact.get("decision"))
    if not payload:
        raise ValueError("decision_emitted fact missing decision payload")
    return payload


def _decision_fact_context(decision_payload: Mapping[str, Any]) -> Dict[str, Any]:
    context = _mapping(decision_payload.get("context"))
    if not context:
        raise ValueError("decision_emitted fact missing decision context")
    return context


def _decision_event_name(decision_payload: Mapping[str, Any]) -> str:
    return _require_text(decision_payload.get("event_name"), field_name="decision.event_name").upper()


def _decision_event_ts(decision_payload: Mapping[str, Any]) -> datetime:
    event_ts = parse_optional_datetime(decision_payload.get("event_ts"))
    if event_ts is None:
        raise ValueError("decision.event_ts is required")
    return event_ts


def _decision_bar_time(decision_context: Mapping[str, Any]) -> Optional[datetime]:
    return (
        parse_optional_datetime(decision_context.get("bar_ts"))
        or parse_optional_datetime(decision_context.get("bar_time"))
        or parse_optional_datetime(_mapping(decision_context.get("bar")).get("time"))
    )


def _decision_bar_epoch(decision_context: Mapping[str, Any]) -> Optional[int]:
    decision_artifact = _mapping(decision_context.get("decision_artifact"))
    return (
        _coerce_epoch(decision_context.get("bar_epoch"))
        or _coerce_epoch(decision_artifact.get("bar_epoch"))
        or _coerce_epoch(_decision_bar_time(decision_context))
    )


def _signal_identifier(decision_context: Mapping[str, Any]) -> Optional[str]:
    return _optional_text(decision_context.get("signal_id"))


def _decision_identifier(decision_context: Mapping[str, Any]) -> Optional[str]:
    return _optional_text(decision_context.get("decision_id"))


def _decision_reference_prefix(decision_name: str) -> str:
    if decision_name in {"SIGNAL_EMITTED", "DECISION_ACCEPTED", "DECISION_REJECTED"}:
        return BotLensDomainEventName.SIGNAL_EMITTED.value.lower()
    if decision_name == "RUNTIME_ERROR":
        return BotLensDomainEventName.FAULT_RECORDED.value.lower()
    raise ValueError(f"unsupported decision event reference prefix for {decision_name}")


def _trade_event_bar_time(
    *,
    fact_type: str,
    trade: Mapping[str, Any],
    opened_at: Optional[datetime],
    closed_at: Optional[datetime],
) -> Optional[datetime]:
    if fact_type == FACT_TYPE_TRADE_OPENED:
        return parse_optional_datetime(trade.get("bar_time")) or opened_at
    if fact_type == FACT_TYPE_TRADE_CLOSED:
        return closed_at or parse_optional_datetime(trade.get("bar_time"))
    return (
        parse_optional_datetime(trade.get("bar_time"))
        or parse_optional_datetime(trade.get("updated_at"))
        or closed_at
        or opened_at
    )


def _decision_base_context(
    *,
    bot_id: str,
    run_id: str,
    identity: Mapping[str, Any],
    decision_context: Mapping[str, Any],
    observed_at: Any = None,
) -> Dict[str, Any]:
    return _base_context(
        bot_id=bot_id,
        run_id=run_id,
        identity=identity
        | {
            "symbol": decision_context.get("symbol") or identity.get("symbol"),
            "timeframe": decision_context.get("timeframe") or identity.get("timeframe"),
        },
        strategy_id=decision_context.get("strategy_id"),
        trade_id=decision_context.get("trade_id"),
        bar_time=_decision_bar_time(decision_context),
        observed_at=observed_at,
    )


def _event_name(value: Any) -> BotLensDomainEventName:
    name = str(value or "").strip().upper()
    if not name:
        raise ValueError("event_name is required")
    try:
        return BotLensDomainEventName(name)
    except ValueError as exc:
        raise ValueError(f"unsupported BotLens domain event_name={name!r}") from exc


def _lifecycle_event_name(*, phase: Any, status: Any) -> BotLensDomainEventName:
    normalized_phase = str(phase or "").strip().lower()
    normalized_status = str(status or "").strip().lower()
    if normalized_phase == "start_requested":
        return BotLensDomainEventName.RUN_STARTED
    if normalized_phase == "live":
        return BotLensDomainEventName.RUN_READY
    if normalized_phase in {"degraded", "telemetry_degraded"} or normalized_status in {"degraded", "telemetry_degraded"}:
        return BotLensDomainEventName.RUN_DEGRADED
    if normalized_phase == "completed" or normalized_status == "completed":
        return BotLensDomainEventName.RUN_COMPLETED
    if normalized_phase == "stopped" or normalized_status == "stopped":
        return BotLensDomainEventName.RUN_STOPPED
    if normalized_phase in {"cancelled", "canceled"} or normalized_status in {"cancelled", "canceled"}:
        return BotLensDomainEventName.RUN_CANCELLED
    if normalized_phase in {"startup_failed", "crashed", "failed", "error"} or normalized_status in {"startup_failed", "crashed", "failed", "error"}:
        return BotLensDomainEventName.RUN_FAILED
    return BotLensDomainEventName.RUN_PHASE_REPORTED


def _serialized_context_base_fields(context_payload: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "run_id": _require_text(context_payload.get("run_id"), field_name="context.run_id"),
        "bot_id": _require_text(context_payload.get("bot_id"), field_name="context.bot_id"),
    }


def _serialized_series_context_base_fields(context_payload: Mapping[str, Any]) -> Dict[str, Any]:
    return _serialized_context_base_fields(context_payload) | {
        "series_key": context_payload.get("series_key"),
        "instrument_id": context_payload.get("instrument_id"),
        "symbol": context_payload.get("symbol"),
        "timeframe": context_payload.get("timeframe"),
        "strategy_id": context_payload.get("strategy_id"),
        "trade_id": context_payload.get("trade_id"),
        "bar_time": parse_optional_datetime(context_payload.get("bar_time")),
        "observed_at": parse_optional_datetime(context_payload.get("observed_at")),
    }


def _reject_unknown_mapping_keys(
    payload: Mapping[str, Any],
    *,
    allowed_keys: frozenset[str],
    object_name: str,
) -> None:
    unexpected = sorted(str(key) for key in payload.keys() if str(key) not in allowed_keys)
    if unexpected:
        raise ValueError(f"{object_name} contains unsupported fields: {', '.join(unexpected)}")


_SERIES_CONTEXT_BASE_KEYS = frozenset(
    {
        "run_id",
        "bot_id",
        "series_key",
        "instrument_id",
        "symbol",
        "timeframe",
        "strategy_id",
        "trade_id",
        "bar_time",
        "observed_at",
    }
)
_RUN_CONTEXT_BASE_KEYS = frozenset({"run_id", "bot_id"})
_EVENT_ENVELOPE_KEYS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_ts",
        "event_name",
        "root_id",
        "parent_id",
        "correlation_id",
        "context",
    }
)
_ALLOWED_CONTEXT_KEYS_BY_EVENT = {
    BotLensDomainEventName.SERIES_METADATA_REPORTED: _SERIES_CONTEXT_BASE_KEYS,
    BotLensDomainEventName.CANDLE_OBSERVED: _SERIES_CONTEXT_BASE_KEYS | frozenset({"candle"}),
    BotLensDomainEventName.OVERLAY_STATE_CHANGED: _SERIES_CONTEXT_BASE_KEYS | frozenset({"overlay_delta"}),
    BotLensDomainEventName.SERIES_STATS_REPORTED: _SERIES_CONTEXT_BASE_KEYS | frozenset({"stats"}),
    BotLensDomainEventName.SIGNAL_EMITTED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset({"signal_id", "signal_type", "direction", "signal_price", "strategy_hash", "bar_epoch", "decision_id", "rule_id", "intent", "event_key"}),
    BotLensDomainEventName.DECISION_EMITTED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset(
        {
            "decision_state",
            "decision_id",
            "strategy_hash",
            "bar_epoch",
            "signal_id",
            "direction",
            "signal_price",
            "reason_code",
            "message",
            "attempt_id",
            "order_request_id",
            "entry_request_id",
            "settlement_attempt_id",
            "blocking_trade_id",
            "intent",
            "rule_id",
            "event_key",
        }
    ),
    BotLensDomainEventName.TRADE_OPENED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset({"trade_state", "side", "direction", "qty", "entry_price", "exit_price", "realized_pnl", "event_impact_pnl", "trade_net_pnl", "signal_id", "decision_id", "event_time", "opened_at", "closed_at"}),
    BotLensDomainEventName.TRADE_UPDATED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset({"trade_state", "side", "direction", "qty", "entry_price", "exit_price", "realized_pnl", "event_impact_pnl", "trade_net_pnl", "signal_id", "decision_id", "event_time", "opened_at", "closed_at"}),
    BotLensDomainEventName.TRADE_CLOSED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset({"trade_state", "side", "direction", "qty", "entry_price", "exit_price", "realized_pnl", "event_impact_pnl", "trade_net_pnl", "signal_id", "decision_id", "event_time", "opened_at", "closed_at"}),
    BotLensDomainEventName.RUN_PHASE_REPORTED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_STARTED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_READY: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_DEGRADED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_COMPLETED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_FAILED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_STOPPED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.RUN_CANCELLED: _RUN_CONTEXT_BASE_KEYS | frozenset({"phase", "status", "component", "message", "live", "metadata", "failure"}),
    BotLensDomainEventName.DIAGNOSTIC_RECORDED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset({"diagnostic_id", "level", "message", "diagnostic_code", "diagnostic_event", "component", "operation", "status", "failure_mode", "request_id", "trace_id"}),
    BotLensDomainEventName.HEALTH_STATUS_REPORTED: _RUN_CONTEXT_BASE_KEYS
    | frozenset(
        {
            "status",
            "warning_count",
            "warning_types",
            "highest_warning_severity",
            "worker_count",
            "active_workers",
            "trigger_event",
            "warnings",
            "runtime_state",
            "last_useful_progress_at",
            "progress_state",
            "degraded",
            "churn",
            "pressure",
            "recent_transitions",
            "terminal",
        }
    ),
    BotLensDomainEventName.FAULT_RECORDED: _SERIES_CONTEXT_BASE_KEYS
    | frozenset({"fault_code", "severity", "message", "source", "component", "failure_type", "failure_phase", "recoverable", "exception_type", "location", "worker_id", "affected_symbols", "exit_code", "from_state", "attempted_to_state", "transition_reason"}),
}


def deserialize_botlens_domain_context(
    *,
    event_name: BotLensDomainEventName,
    context_payload: Mapping[str, Any],
) -> BotLensDomainContext:
    if not isinstance(context_payload, Mapping):
        raise ValueError("context is required")
    if event_name == BotLensDomainEventName.HEALTH_STATUS_REPORTED and context_payload.get("event") not in (None, ""):
        raise ValueError("context.event is not allowed for HEALTH_STATUS_REPORTED; use context.trigger_event")
    _reject_unknown_mapping_keys(
        context_payload,
        allowed_keys=_ALLOWED_CONTEXT_KEYS_BY_EVENT[event_name],
        object_name=f"{event_name.value} context",
    )
    if event_name.value in _SERIES_SCOPED_EVENT_NAMES and "series_key" not in context_payload:
        raise ValueError(f"context.series_key is required for {event_name.value}")
    if event_name == BotLensDomainEventName.CANDLE_OBSERVED:
        return CandleObservedContext(
            **_serialized_series_context_base_fields(context_payload),
            candle=BotLensCandle.from_payload(context_payload.get("candle")),
        )
    if event_name == BotLensDomainEventName.SERIES_METADATA_REPORTED:
        return SeriesMetadataReportedContext(**_serialized_series_context_base_fields(context_payload))
    if event_name == BotLensDomainEventName.OVERLAY_STATE_CHANGED:
        return OverlayStateChangedContext(
            **_serialized_series_context_base_fields(context_payload),
            overlay_delta=_mapping(context_payload.get("overlay_delta")),
        )
    if event_name == BotLensDomainEventName.SERIES_STATS_REPORTED:
        return SeriesStatsReportedContext(
            **_serialized_series_context_base_fields(context_payload),
            stats=_mapping(context_payload.get("stats")),
        )
    if event_name == BotLensDomainEventName.SIGNAL_EMITTED:
        return SignalEmittedContext(
            **_serialized_series_context_base_fields(context_payload),
            signal_id=_require_text(context_payload.get("signal_id"), field_name="context.signal_id"),
            signal_type=_require_text(context_payload.get("signal_type"), field_name="context.signal_type"),
            direction=_require_text(context_payload.get("direction"), field_name="context.direction"),
            signal_price=_require_float_field(context_payload, "signal_price", field_name="context.signal_price"),
            strategy_hash=context_payload.get("strategy_hash"),
            bar_epoch=context_payload.get("bar_epoch"),
            decision_id=context_payload.get("decision_id"),
            rule_id=context_payload.get("rule_id"),
            intent=context_payload.get("intent"),
            event_key=context_payload.get("event_key"),
        )
    if event_name == BotLensDomainEventName.DECISION_EMITTED:
        return DecisionEmittedContext(
            **_serialized_series_context_base_fields(context_payload),
            decision_state=_require_text(context_payload.get("decision_state"), field_name="context.decision_state"),
            decision_id=_require_text(context_payload.get("decision_id"), field_name="context.decision_id"),
            strategy_hash=context_payload.get("strategy_hash"),
            bar_epoch=context_payload.get("bar_epoch"),
            signal_id=context_payload.get("signal_id"),
            direction=context_payload.get("direction"),
            signal_price=_optional_float_field(context_payload, "signal_price", field_name="context.signal_price"),
            reason_code=context_payload.get("reason_code"),
            message=context_payload.get("message"),
            attempt_id=context_payload.get("attempt_id"),
            order_request_id=context_payload.get("order_request_id"),
            entry_request_id=context_payload.get("entry_request_id"),
            settlement_attempt_id=context_payload.get("settlement_attempt_id"),
            blocking_trade_id=context_payload.get("blocking_trade_id"),
            intent=context_payload.get("intent"),
            rule_id=context_payload.get("rule_id"),
            event_key=context_payload.get("event_key"),
        )
    if event_name in {
        BotLensDomainEventName.TRADE_OPENED,
        BotLensDomainEventName.TRADE_UPDATED,
        BotLensDomainEventName.TRADE_CLOSED,
    }:
        return TradeLifecycleContext(
            **_serialized_series_context_base_fields(context_payload),
            trade_state=_require_text(context_payload.get("trade_state"), field_name="context.trade_state"),
            side=context_payload.get("side"),
            direction=context_payload.get("direction"),
            qty=_optional_float_field(context_payload, "qty", field_name="context.qty"),
            entry_price=_optional_float_field(context_payload, "entry_price", field_name="context.entry_price"),
            exit_price=_optional_float_field(context_payload, "exit_price", field_name="context.exit_price"),
            realized_pnl=_optional_float_field(context_payload, "realized_pnl", field_name="context.realized_pnl"),
            event_impact_pnl=_optional_float_field(
                context_payload,
                "event_impact_pnl",
                field_name="context.event_impact_pnl",
            ),
            trade_net_pnl=_optional_float_field(context_payload, "trade_net_pnl", field_name="context.trade_net_pnl"),
            signal_id=context_payload.get("signal_id"),
            decision_id=context_payload.get("decision_id"),
            event_time=parse_optional_datetime(context_payload.get("event_time")),
            opened_at=parse_optional_datetime(context_payload.get("opened_at")),
            closed_at=parse_optional_datetime(context_payload.get("closed_at")),
        )
    if event_name.value in _LIFECYCLE_EVENT_NAMES:
        return RunLifecycleContext(
            **_serialized_context_base_fields(context_payload),
            phase=_require_text(context_payload.get("phase"), field_name="context.phase"),
            status=_require_text(context_payload.get("status"), field_name="context.status"),
            component=context_payload.get("component"),
            message=context_payload.get("message"),
            live=bool(context_payload.get("live")),
            metadata=_mapping_or_none(context_payload.get("metadata")),
            failure=_mapping_or_none(context_payload.get("failure")),
        )
    if event_name == BotLensDomainEventName.DIAGNOSTIC_RECORDED:
        return DiagnosticRecordedContext(
            **_serialized_series_context_base_fields(context_payload),
            diagnostic_id=context_payload.get("diagnostic_id"),
            level=_require_text(context_payload.get("level"), field_name="context.level"),
            message=_require_text(context_payload.get("message"), field_name="context.message"),
            diagnostic_code=context_payload.get("diagnostic_code"),
            diagnostic_event=context_payload.get("diagnostic_event"),
            component=context_payload.get("component"),
            operation=context_payload.get("operation"),
            status=context_payload.get("status"),
            failure_mode=context_payload.get("failure_mode"),
            request_id=context_payload.get("request_id"),
            trace_id=context_payload.get("trace_id"),
        )
    if event_name == BotLensDomainEventName.HEALTH_STATUS_REPORTED:
        return HealthStatusReportedContext(
            **_serialized_context_base_fields(context_payload),
            status=_require_text(context_payload.get("status"), field_name="context.status"),
            warning_count=int(context_payload.get("warning_count") or 0),
            warning_types=[
                str(entry).strip().lower()
                for entry in context_payload.get("warning_types", [])
                if str(entry).strip()
            ],
            highest_warning_severity=context_payload.get("highest_warning_severity"),
            worker_count=int(context_payload["worker_count"]) if context_payload.get("worker_count") is not None else None,
            active_workers=(
                int(context_payload["active_workers"])
                if context_payload.get("active_workers") is not None
                else None
            ),
            trigger_event=context_payload.get("trigger_event"),
            warnings=[dict(entry) for entry in context_payload.get("warnings", []) if isinstance(entry, Mapping)],
            runtime_state=context_payload.get("runtime_state"),
            last_useful_progress_at=context_payload.get("last_useful_progress_at"),
            progress_state=context_payload.get("progress_state"),
            degraded=_mapping_or_none(context_payload.get("degraded")),
            churn=_mapping_or_none(context_payload.get("churn")),
            pressure=_mapping_or_none(context_payload.get("pressure")),
            recent_transitions=_mapping_list(context_payload.get("recent_transitions")),
            terminal=_mapping_or_none(context_payload.get("terminal")),
        )
    if event_name == BotLensDomainEventName.FAULT_RECORDED:
        return FaultRecordedContext(
            **_serialized_series_context_base_fields(context_payload),
            fault_code=_require_text(context_payload.get("fault_code"), field_name="context.fault_code"),
            severity=_require_text(context_payload.get("severity"), field_name="context.severity"),
            message=_require_text(context_payload.get("message"), field_name="context.message"),
            source=_require_text(context_payload.get("source"), field_name="context.source"),
            component=context_payload.get("component"),
            failure_type=context_payload.get("failure_type"),
            failure_phase=context_payload.get("failure_phase"),
            recoverable=context_payload.get("recoverable"),
            exception_type=context_payload.get("exception_type"),
            location=context_payload.get("location"),
            worker_id=context_payload.get("worker_id"),
            affected_symbols=context_payload.get("affected_symbols"),
            exit_code=context_payload.get("exit_code"),
            from_state=context_payload.get("from_state"),
            attempted_to_state=context_payload.get("attempted_to_state"),
            transition_reason=context_payload.get("transition_reason"),
        )
    raise ValueError(f"unsupported BotLens domain event_name={event_name.value!r}")


def deserialize_botlens_domain_event(payload: Mapping[str, Any]) -> BotLensDomainEvent:
    if not isinstance(payload, Mapping):
        raise ValueError("payload is required")
    _reject_unknown_mapping_keys(payload, allowed_keys=_EVENT_ENVELOPE_KEYS, object_name="BotLens domain event")
    event_name = _event_name(payload.get("event_name"))
    event_ts = parse_optional_datetime(payload.get("event_ts"))
    if event_ts is None:
        raise ValueError("event_ts is required")
    context = deserialize_botlens_domain_context(
        event_name=event_name,
        context_payload=_mapping(payload.get("context")),
    )
    return _new_event(
        event_name=event_name,
        event_id=_require_text(payload.get("event_id"), field_name="event_id"),
        event_ts=event_ts,
        correlation_id=_require_text(payload.get("correlation_id"), field_name="correlation_id"),
        root_id=_require_text(payload.get("root_id"), field_name="root_id"),
        parent_id=_optional_text(payload.get("parent_id")),
        context=context,
    )


def build_botlens_domain_events_from_fact_batch(
    *,
    bot_id: str,
    run_id: str,
    payload: Mapping[str, Any],
) -> List[BotLensDomainEvent]:
    identity = _series_identity_from_facts(payload)
    known_at = parse_optional_datetime(payload.get("known_at") or payload.get("event_time"))
    if known_at is None:
        raise ValueError("BotLens fact batch known_at/event_time is required")
    observed_at = parse_optional_datetime(payload.get("observed_at") or payload.get("ingested_at"))
    events: List[BotLensDomainEvent] = []
    for fact in normalize_fact_entries(payload.get("facts")):
        fact_type = str(fact.get("fact_type") or "").strip().lower()
        if fact_type == FACT_TYPE_SERIES_STATE:
            context = SeriesMetadataReportedContext(
                **_base_context(
                    bot_id=bot_id,
                    run_id=run_id,
                    identity=identity
                    | {
                        "series_key": normalize_series_key(fact.get("series_key")) or identity.get("series_key"),
                        "instrument_id": fact.get("instrument_id") or identity.get("instrument_id"),
                        "symbol": fact.get("symbol") or identity.get("symbol"),
                        "timeframe": fact.get("timeframe") or identity.get("timeframe"),
                    },
                    observed_at=observed_at,
                )
            )
            events.append(
                _new_event(
                    event_name=BotLensDomainEventName.SERIES_METADATA_REPORTED,
                    event_id=f"botlens:{_event_hash('series_meta', run_id, context.series_key, context.instrument_id, context.symbol, context.timeframe)}",
                    event_ts=known_at,
                    correlation_id=_correlation_id(
                        run_id=run_id,
                        series_key=context.series_key,
                        scope="series_meta",
                        event_ts=known_at,
                    ),
                    context=context,
                )
            )
            continue

        if fact_type == FACT_TYPE_RUNTIME_STATE:
            runtime = _mapping(fact.get("runtime"))
            status = _optional_text(runtime.get("status"))
            if status:
                context = HealthStatusReportedContext(
                    bot_id=str(bot_id),
                    run_id=str(run_id),
                    status=status,
                    warning_count=len(runtime.get("warnings") or []) if isinstance(runtime.get("warnings"), list) else 0,
                    worker_count=_coerce_int(runtime.get("worker_count"), 0) if runtime.get("worker_count") is not None else None,
                    active_workers=_coerce_int(runtime.get("active_workers"), 0) if runtime.get("active_workers") is not None else None,
                    trigger_event=_optional_text(fact.get("event")),
                    warnings=[dict(entry) for entry in runtime.get("warnings", []) if isinstance(entry, Mapping)],
                    runtime_state=_optional_text(runtime.get("runtime_state")),
                    last_useful_progress_at=_optional_text(runtime.get("last_useful_progress_at")),
                    progress_state=_optional_text(runtime.get("progress_state")),
                    degraded=_mapping_or_none(runtime.get("degraded")),
                    churn=_mapping_or_none(runtime.get("churn")),
                    pressure=_mapping_or_none(runtime.get("pressure")),
                    recent_transitions=_mapping_list(runtime.get("recent_transitions")),
                    terminal=_mapping_or_none(runtime.get("terminal")),
                )
                event_id = f"botlens:{_event_hash('health', run_id, _health_event_fingerprint(context.to_dict()))}"
                events.append(
                    _new_event(
                        event_name=BotLensDomainEventName.HEALTH_STATUS_REPORTED,
                        event_id=event_id,
                        event_ts=known_at,
                        correlation_id=_correlation_id(
                            run_id=run_id,
                            series_key=identity.get("series_key"),
                            scope="health",
                            event_ts=known_at,
                        ),
                        context=context,
                    )
                )
            continue

        if fact_type == FACT_TYPE_CANDLE_UPSERTED:
            candle_payload = _mapping(fact.get("candle"))
            candle = BotLensCandle.from_payload(candle_payload)
            context = CandleObservedContext(
                **_base_context(
                    bot_id=bot_id,
                    run_id=run_id,
                    identity=identity,
                    bar_time=candle.time,
                    observed_at=observed_at,
                ),
                candle=candle,
            )
            durable_context = _durable_context_payload(
                BotLensDomainEventName.CANDLE_OBSERVED,
                context.to_dict(),
            )
            candle_revision = _event_hash(durable_context.get("candle"))
            event_id = f"botlens:{_event_hash('candle', run_id, context.series_key, context.candle.time, candle_revision)}"
            events.append(
                _new_event(
                    event_name=BotLensDomainEventName.CANDLE_OBSERVED,
                    event_id=event_id,
                    event_ts=context.candle.time,
                    correlation_id=_correlation_id(
                        run_id=run_id,
                        series_key=context.series_key,
                        scope="candle",
                        event_ts=context.candle.time,
                    ),
                    context=context,
                )
            )
            continue

        if fact_type == "overlay_ops_emitted":
            context = OverlayStateChangedContext(
                **_base_context(
                    bot_id=bot_id,
                    run_id=run_id,
                    identity=identity,
                    bar_time=known_at,
                    observed_at=observed_at,
                ),
                overlay_delta=_mapping(fact.get("overlay_delta")),
            )
            durable_context = _durable_context_payload(
                BotLensDomainEventName.OVERLAY_STATE_CHANGED,
                context.to_dict(),
            )
            events.append(
                _new_event(
                    event_name=BotLensDomainEventName.OVERLAY_STATE_CHANGED,
                    event_id=f"botlens:{_event_hash('overlay', run_id, context.series_key, known_at, durable_context.get('overlay_delta'))}",
                    event_ts=known_at,
                    correlation_id=_correlation_id(
                        run_id=run_id,
                        series_key=context.series_key,
                        scope="overlay",
                        event_ts=known_at,
                    ),
                    context=context,
                )
            )
            continue

        if fact_type == "series_stats_updated":
            stats = _mapping(fact.get("stats"))
            context = SeriesStatsReportedContext(
                **_base_context(
                    bot_id=bot_id,
                    run_id=run_id,
                    identity=identity,
                    bar_time=known_at,
                    observed_at=observed_at,
                ),
                stats=stats,
            )
            durable_context = _durable_context_payload(
                BotLensDomainEventName.SERIES_STATS_REPORTED,
                context.to_dict(),
            )
            events.append(
                _new_event(
                    event_name=BotLensDomainEventName.SERIES_STATS_REPORTED,
                    event_id=f"botlens:{_event_hash('series_stats', run_id, context.series_key, known_at, durable_context.get('stats'))}",
                    event_ts=known_at,
                    correlation_id=_correlation_id(
                        run_id=run_id,
                        series_key=context.series_key,
                        scope="series_stats",
                        event_ts=known_at,
                    ),
                    context=context,
                )
            )
            continue

        if fact_type == FACT_TYPE_DECISION_EMITTED:
            decision_root = _decision_fact_root(fact)
            decision_context = _decision_fact_context(decision_root)
            decision_name = _decision_event_name(decision_root)
            event_ts = _decision_event_ts(decision_root)
            base = _decision_base_context(
                bot_id=bot_id,
                run_id=run_id,
                identity=identity,
                decision_context=decision_context,
                observed_at=observed_at,
            )
            rejection_artifact = _mapping(decision_context.get("rejection_artifact"))
            rejection_context = _mapping(rejection_artifact.get("context"))
            if decision_name == "SIGNAL_EMITTED":
                context = SignalEmittedContext(
                    **base,
                    signal_id=_signal_identifier(decision_context) or "",
                    signal_type=_require_text(decision_context.get("signal_type"), field_name="decision.context.signal_type"),
                    direction=_require_text(decision_context.get("direction"), field_name="decision.context.direction"),
                    signal_price=float(decision_context.get("signal_price")),
                    strategy_hash=_optional_text(decision_context.get("strategy_hash")),
                    bar_epoch=_decision_bar_epoch(decision_context),
                    decision_id=_optional_text(decision_context.get("decision_id")),
                    rule_id=_optional_text(decision_context.get("rule_id")),
                    intent=_optional_text(decision_context.get("intent")),
                    event_key=_optional_text(decision_context.get("event_key")),
                )
                event_name = BotLensDomainEventName.SIGNAL_EMITTED
            elif decision_name in {"DECISION_ACCEPTED", "DECISION_REJECTED"}:
                if decision_name == "DECISION_REJECTED":
                    base["trade_id"] = None
                context = DecisionEmittedContext(
                    **base,
                    decision_state="accepted" if decision_name == "DECISION_ACCEPTED" else "rejected",
                    decision_id=_decision_identifier(decision_context) or "",
                    strategy_hash=_optional_text(decision_context.get("strategy_hash")),
                    bar_epoch=_decision_bar_epoch(decision_context),
                    signal_id=_optional_text(decision_context.get("signal_id")),
                    direction=_optional_text(decision_context.get("direction")),
                    signal_price=_coerce_float(decision_context.get("signal_price")),
                    reason_code=_optional_text(decision_context.get("reason_code")),
                    message=_optional_text(decision_context.get("message")),
                    attempt_id=(
                        _optional_text(decision_context.get("attempt_id"))
                        or _optional_text(decision_context.get("entry_request_id"))
                        or _optional_text(rejection_context.get("attempt_id"))
                        or _optional_text(rejection_context.get("entry_request_id"))
                        or _optional_text(rejection_context.get("settlement_attempt_id"))
                        or _optional_text(rejection_context.get("order_request_id"))
                    ),
                    order_request_id=(
                        _optional_text(decision_context.get("order_request_id"))
                        or _optional_text(rejection_context.get("order_request_id"))
                    ),
                    entry_request_id=(
                        _optional_text(decision_context.get("entry_request_id"))
                        or _optional_text(rejection_context.get("entry_request_id"))
                    ),
                    settlement_attempt_id=(
                        _optional_text(decision_context.get("settlement_attempt_id"))
                        or _optional_text(rejection_context.get("settlement_attempt_id"))
                    ),
                    blocking_trade_id=(
                        _optional_text(decision_context.get("blocking_trade_id"))
                        or _optional_text(rejection_context.get("blocking_trade_id"))
                        or _optional_text(rejection_context.get("active_trade_id"))
                    ),
                    intent=_optional_text(decision_context.get("intent")),
                    rule_id=_optional_text(decision_context.get("rule_id")),
                    event_key=_optional_text(decision_context.get("event_key")),
                )
                event_name = BotLensDomainEventName.DECISION_EMITTED
            elif decision_name == "RUNTIME_ERROR":
                context = FaultRecordedContext(
                    **base,
                    fault_code=_optional_text(decision_context.get("reason_code")) or "runtime_exception",
                    severity="ERROR",
                    message=_require_text(decision_context.get("message"), field_name="decision.context.message"),
                    source="runtime_event",
                    component="runtime",
                    failure_type="runtime_error",
                    exception_type=_optional_text(decision_context.get("exception_type")),
                    location=_optional_text(decision_context.get("location")),
                )
                event_name = BotLensDomainEventName.FAULT_RECORDED
            else:
                continue
            decision_event_id = _require_text(decision_root.get("event_id"), field_name="decision.event_id")
            decision_root_id = _optional_text(decision_root.get("root_id"))
            decision_parent_id = _optional_text(decision_root.get("parent_id"))
            correlation_id = _require_text(decision_root.get("correlation_id"), field_name="decision.correlation_id")
            decision_ref_prefix = _decision_reference_prefix(decision_name)
            event_id = f"botlens:{event_name.value.lower()}:{decision_event_id}"
            events.append(
                _new_event(
                    event_name=event_name,
                    event_id=event_id,
                    event_ts=event_ts,
                    correlation_id=correlation_id,
                    root_id=(
                        f"botlens:{decision_ref_prefix}:{decision_root_id}"
                        if decision_root_id
                        else None
                    ),
                    parent_id=(
                        f"botlens:{decision_ref_prefix}:{decision_parent_id}"
                        if decision_parent_id
                        else None
                    ),
                    context=context,
                )
            )
            continue

        if fact_type in {
            FACT_TYPE_TRADE_OPENED,
            FACT_TYPE_TRADE_UPDATED,
            FACT_TYPE_TRADE_CLOSED,
        }:
            trade = _mapping(fact.get("trade"))
            trade_id = _optional_text(trade.get("trade_id") or trade.get("id"))
            if trade_id is None:
                raise ValueError(f"{fact_type} fact missing trade_id")
            opened_at = parse_optional_datetime(trade.get("opened_at") or trade.get("entry_time"))
            closed_at = parse_optional_datetime(trade.get("closed_at") or trade.get("exit_time"))
            status = _optional_text(trade.get("status"))
            normalized_status = str(status or "").strip().lower()
            if closed_at is None and normalized_status in {"closed", "completed", "complete"}:
                raise ValueError(
                    f"{fact_type} fact marks trade closed without closed_at "
                    f"trade_id={trade_id} status={normalized_status}"
                )
            trade_state = "closed" if closed_at is not None else "open"
            if fact_type == FACT_TYPE_TRADE_OPENED and trade_state == "closed":
                raise ValueError(f"{fact_type} fact requires an open trade payload trade_id={trade_id}")
            if fact_type == FACT_TYPE_TRADE_CLOSED and trade_state != "closed":
                raise ValueError(f"{fact_type} fact requires closed_at trade_id={trade_id}")
            event_name = {
                FACT_TYPE_TRADE_OPENED: BotLensDomainEventName.TRADE_OPENED,
                FACT_TYPE_TRADE_UPDATED: BotLensDomainEventName.TRADE_UPDATED,
                FACT_TYPE_TRADE_CLOSED: BotLensDomainEventName.TRADE_CLOSED,
            }[fact_type]
            trade_bar_time = _trade_event_bar_time(
                fact_type=fact_type,
                trade=trade,
                opened_at=opened_at,
                closed_at=closed_at,
            )
            if trade_bar_time is None:
                raise ValueError(f"{fact_type} fact missing simulated trade bar_time trade_id={trade_id}")
            explicit_event_time = parse_optional_datetime(trade.get("event_time"))
            if explicit_event_time is not None and explicit_event_time != trade_bar_time:
                raise ValueError(
                    f"{fact_type} fact event_time must match simulated trade bar_time "
                    f"trade_id={trade_id}"
                )
            context = TradeLifecycleContext(
                **_base_context(
                    bot_id=bot_id,
                    run_id=run_id,
                    identity=identity
                    | {
                        "symbol": trade.get("symbol") or identity.get("symbol"),
                        "timeframe": trade.get("timeframe") or identity.get("timeframe"),
                    },
                    strategy_id=trade.get("strategy_id"),
                    trade_id=trade_id,
                    bar_time=trade_bar_time,
                    observed_at=observed_at,
                ),
                trade_state=trade_state,
                side=_optional_text(trade.get("side")),
                direction=_optional_text(trade.get("direction")),
                qty=_coerce_float(trade.get("qty") or trade.get("filled_qty")),
                entry_price=_coerce_float(trade.get("entry_price")),
                exit_price=_coerce_float(trade.get("exit_price")),
                realized_pnl=_coerce_float(trade.get("realized_pnl")),
                event_impact_pnl=_coerce_float(trade.get("event_impact_pnl")),
                trade_net_pnl=_coerce_float(trade.get("trade_net_pnl")),
                signal_id=_optional_text(trade.get("signal_id")),
                decision_id=_optional_text(trade.get("decision_id")),
                event_time=explicit_event_time or trade_bar_time,
                opened_at=opened_at,
                closed_at=closed_at,
            )
            revision = _event_hash(
                trade_id,
                trade_state,
                trade.get("status"),
                trade.get("entry_price"),
                trade.get("exit_price"),
                trade.get("qty"),
                trade.get("closed_at"),
                trade.get("realized_pnl"),
            )
            event_id = f"botlens:{_event_hash('trade', run_id, context.series_key, trade_id, revision)}"
            event_ts = context.event_time or context.bar_time
            events.append(
                _new_event(
                    event_name=event_name,
                    event_id=event_id,
                    event_ts=event_ts,
                    correlation_id=_correlation_id(
                        run_id=run_id,
                        series_key=context.series_key,
                        scope=f"trade:{trade_id}",
                        event_ts=event_ts,
                    ),
                    context=context,
                )
            )
            continue

        if fact_type == FACT_TYPE_LOG_EMITTED:
            log = _mapping(fact.get("log"))
            log_context = _mapping(log.get("context"))
            level = _optional_text(log.get("level")) or "INFO"
            message = _optional_text(log.get("message") or log.get("event")) or "diagnostic"
            context = DiagnosticRecordedContext(
                **_base_context(
                    bot_id=bot_id,
                    run_id=run_id,
                    identity=identity
                    | {
                        "symbol": _optional_text(log.get("symbol")) or identity.get("symbol"),
                        "timeframe": _optional_text(log.get("timeframe")) or identity.get("timeframe"),
                    },
                    bar_time=log.get("bar_time") or log.get("created_at") or known_at,
                    observed_at=observed_at,
                ),
                diagnostic_id=_optional_text(log.get("id") or log.get("event_id")),
                level=level,
                diagnostic_code=(
                    _optional_text(log.get("diagnostic_code"))
                    or _optional_text(log_context.get("diagnostic_code"))
                    or _optional_text(log_context.get("reason_code"))
                ),
                diagnostic_event=_optional_text(log.get("event")),
                message=message,
                component=(
                    _optional_text(log.get("component"))
                    or _optional_text(log_context.get("component"))
                    or _optional_text(log.get("owner"))
                ),
                operation=_optional_text(log_context.get("operation")),
                status=_optional_text(log_context.get("status")),
                failure_mode=_optional_text(log_context.get("failure_mode")),
                request_id=_optional_text(log_context.get("request_id")),
                trace_id=_optional_text(log_context.get("trace_id")),
            )
            event_ts = context.bar_time or known_at
            natural_id = context.diagnostic_id or _event_hash(
                level,
                context.diagnostic_code,
                context.diagnostic_event,
                message,
                context.component,
                context.operation,
                context.status,
                context.failure_mode,
                context.request_id,
                context.trace_id,
                event_ts,
            )
            event_id = f"botlens:{_event_hash('diagnostic', run_id, context.series_key, natural_id)}"
            events.append(
                _new_event(
                    event_name=BotLensDomainEventName.DIAGNOSTIC_RECORDED,
                    event_id=event_id,
                    event_ts=event_ts,
                    correlation_id=_correlation_id(
                        run_id=run_id,
                        series_key=context.series_key,
                        scope="diagnostic",
                        event_ts=event_ts,
                    ),
                    context=context,
                )
            )
            continue
    return events


def build_botlens_domain_events_from_lifecycle(
    *,
    bot_id: str,
    run_id: str,
    lifecycle: Mapping[str, Any],
) -> List[BotLensDomainEvent]:
    checkpoint_at = parse_optional_datetime(lifecycle.get("checkpoint_at") or lifecycle.get("updated_at"))
    if checkpoint_at is None:
        raise ValueError("lifecycle checkpoint_at/updated_at is required")
    phase = str(lifecycle.get("phase") or "")
    status = str(lifecycle.get("status") or "")
    event_name = _lifecycle_event_name(phase=phase, status=status)
    context = RunLifecycleContext(
        bot_id=str(bot_id),
        run_id=str(run_id),
        phase=phase,
        status=status,
        component=_optional_text(lifecycle.get("owner")),
        message=_optional_text(lifecycle.get("message")),
        live=bool(lifecycle.get("live")),
        metadata=_mapping_or_none(lifecycle.get("metadata")),
        failure=_mapping_or_none(lifecycle.get("failure")),
    )
    lifecycle_event = _new_event(
        event_name=event_name,
        event_id=f"botlens:{_event_hash('lifecycle', run_id, event_name.value, context.phase, context.status, checkpoint_at, context.component, context.message)}",
        event_ts=checkpoint_at,
        correlation_id=_correlation_id(
            run_id=run_id,
            series_key=None,
            scope=f"lifecycle:{event_name.value.lower()}",
            event_ts=checkpoint_at,
        ),
        context=context,
    )
    events: List[BotLensDomainEvent] = [lifecycle_event]
    failure = _mapping(lifecycle.get("failure"))
    if failure:
        severity = "ERROR" if context.status in {"failed", "error", "crashed", "startup_failed"} else "WARN"
        fault_context = FaultRecordedContext(
            bot_id=str(bot_id),
            run_id=str(run_id),
            fault_code=_optional_text(failure.get("reason_code") or failure.get("type")) or "lifecycle_fault",
            severity=severity,
            message=_optional_text(failure.get("message")) or context.message or "lifecycle fault",
            source="lifecycle",
            component=_optional_text(failure.get("owner") or context.component),
            failure_type=_optional_text(failure.get("type") or failure.get("error_type")),
            failure_phase=_optional_text(failure.get("phase")),
            recoverable=_coerce_optional_bool(failure.get("recoverable")),
            exception_type=_optional_text(failure.get("exception_type") or failure.get("error_type")),
            worker_id=_optional_text(failure.get("worker_id")),
            symbol=_normalize_symbol(failure.get("symbol")),
            affected_symbols=_normalize_symbol_list(failure.get("symbols")),
            exit_code=_coerce_int(failure.get("exit_code"), 0) if failure.get("exit_code") is not None else None,
            from_state=_optional_text(failure.get("from_state")),
            attempted_to_state=_optional_text(failure.get("attempted_to_state")),
            transition_reason=_optional_text(failure.get("transition_reason")),
        )
        events.append(
            _new_event(
                event_name=BotLensDomainEventName.FAULT_RECORDED,
                event_id=f"botlens:{_event_hash('fault', run_id, checkpoint_at, fault_context.fault_code, fault_context.message)}",
                event_ts=checkpoint_at,
                correlation_id=_correlation_id(run_id=run_id, series_key=None, scope="fault", event_ts=checkpoint_at),
                context=fault_context,
                root_id=lifecycle_event.event_id,
            )
        )
    return events


__all__ = [
    "BOTLENS_DOMAIN_PREFIX",
    "BotLensCandle",
    "BotLensDomainEvent",
    "BotLensDomainEventName",
    "botlens_domain_event_type",
    "build_botlens_domain_events_from_fact_batch",
    "build_botlens_domain_events_from_lifecycle",
    "canonicalize_botlens_candle",
    "deserialize_botlens_domain_context",
    "deserialize_botlens_domain_event",
    "serialize_botlens_domain_event",
]

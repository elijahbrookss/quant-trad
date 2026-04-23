from __future__ import annotations

import json
import math
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.settings import get_settings

from .botlens_contract import BRIDGE_BOOTSTRAP_KIND, RUN_SCOPE_KEY, SCHEMA_VERSION, normalize_series_key
from .botlens_domain_events import (
    BotLensDomainEvent,
    BotLensDomainEventName,
    canonicalize_botlens_candle,
    canonicalize_health_warning,
)
from .botlens_runtime_state import summarize_transition_history
from .startup_lifecycle import status_for_phase

_SETTINGS = get_settings()
_BOTLENS = _SETTINGS.bot_runtime.botlens
_MAX_CANDLES = max(1, int(_BOTLENS.max_candles))
_MAX_LOGS = max(1, int(_BOTLENS.max_logs))
_MAX_DECISIONS = max(1, int(_BOTLENS.max_decisions))
_MAX_TRADES = max(1, int(_BOTLENS.max_closed_trades))
_MAX_WARNINGS = max(1, int(_BOTLENS.max_warnings))
_MAX_OVERLAYS = max(1, int(_BOTLENS.max_overlays))


@dataclass(frozen=True)
class ProjectionBatch:
    batch_kind: str
    run_id: str
    bot_id: str
    seq: int
    event_time: Any
    known_at: Any
    symbol_key: Optional[str] = None
    bridge_session_id: Optional[str] = None
    events: Tuple[BotLensDomainEvent, ...] = ()


@dataclass(frozen=True)
class SymbolIdentityState:
    symbol_key: str
    instrument_id: Optional[str]
    symbol: Optional[str]
    timeframe: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol_key": self.symbol_key,
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
        }


@dataclass(frozen=True)
class SymbolCandlesState:
    candles: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SymbolOverlaysState:
    overlays: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SymbolSignalsState:
    signals: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SymbolDecisionsState:
    decisions: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SymbolTradesState:
    trades: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SymbolDiagnosticsState:
    diagnostics: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SymbolStatsState:
    stats: Dict[str, Any]


@dataclass(frozen=True)
class SymbolReadinessState:
    snapshot_ready: bool
    symbol_live: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "snapshot_ready": self.snapshot_ready,
            "symbol_live": self.symbol_live,
        }


@dataclass(frozen=True)
class SymbolProjectionSnapshot:
    schema_version: int
    symbol_key: str
    seq: int
    last_event_at: Optional[str]
    identity: SymbolIdentityState
    candles: SymbolCandlesState
    overlays: SymbolOverlaysState
    signals: SymbolSignalsState
    decisions: SymbolDecisionsState
    trades: SymbolTradesState
    diagnostics: SymbolDiagnosticsState
    stats: SymbolStatsState
    readiness: SymbolReadinessState


@dataclass(frozen=True)
class RunLifecycleState:
    run_id: str
    phase: Optional[str]
    status: Optional[str]
    owner: Optional[str]
    message: Optional[str]
    metadata: Dict[str, Any]
    failure: Dict[str, Any]
    checkpoint_at: Optional[str]
    updated_at: Optional[str]
    live: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "phase": self.phase,
            "status": self.status,
            "owner": self.owner,
            "message": self.message,
            "metadata": dict(self.metadata),
            "failure": dict(self.failure),
            "checkpoint_at": self.checkpoint_at,
            "updated_at": self.updated_at,
            "live": self.live,
        }


@dataclass(frozen=True)
class RunHealthState:
    status: str
    phase: Optional[str]
    warning_count: int
    warnings: Tuple[Dict[str, Any], ...]
    last_event_at: Optional[str]
    worker_count: int
    active_workers: int
    warning_types: Tuple[str, ...] = ()
    highest_warning_severity: Optional[str] = None
    trigger_event: Optional[str] = None
    runtime_state: Optional[str] = None
    last_useful_progress_at: Optional[str] = None
    progress_state: Optional[str] = None
    degraded: Dict[str, Any] = field(default_factory=dict)
    churn: Dict[str, Any] = field(default_factory=dict)
    pressure: Dict[str, Any] = field(default_factory=dict)
    recent_transitions: Tuple[Dict[str, Any], ...] = ()
    terminal: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "status": self.status,
            "phase": self.phase,
            "warning_count": self.warning_count,
            "warnings": [dict(entry) for entry in self.warnings],
            "last_event_at": self.last_event_at,
            "worker_count": self.worker_count,
            "active_workers": self.active_workers,
        }
        if self.warning_types:
            payload["warning_types"] = list(self.warning_types)
        if self.highest_warning_severity:
            payload["highest_warning_severity"] = self.highest_warning_severity
        if self.trigger_event:
            payload["trigger_event"] = self.trigger_event
        if self.runtime_state:
            payload["runtime_state"] = self.runtime_state
        if self.last_useful_progress_at:
            payload["last_useful_progress_at"] = self.last_useful_progress_at
        if self.progress_state:
            payload["progress_state"] = self.progress_state
        if self.degraded:
            payload["degraded"] = dict(self.degraded)
        if self.churn:
            payload["churn"] = dict(self.churn)
        if self.pressure:
            payload["pressure"] = dict(self.pressure)
        if self.recent_transitions:
            payload["recent_transitions"] = [dict(entry) for entry in self.recent_transitions]
        if self.terminal:
            payload["terminal"] = dict(self.terminal)
        return payload


@dataclass(frozen=True)
class RunFaultsState:
    faults: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class RunSymbolCatalogState:
    entries: Dict[str, Dict[str, Any]]


@dataclass(frozen=True)
class RunOpenTradesState:
    entries: Dict[str, Dict[str, Any]]


@dataclass(frozen=True)
class RunReadinessState:
    catalog_discovered: bool
    run_live: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "catalog_discovered": self.catalog_discovered,
            "run_live": self.run_live,
        }


@dataclass(frozen=True)
class RunProjectionSnapshot:
    schema_version: int
    bot_id: str
    run_id: str
    seq: int
    lifecycle: RunLifecycleState
    health: RunHealthState
    faults: RunFaultsState
    symbol_catalog: RunSymbolCatalogState
    open_trades: RunOpenTradesState
    readiness: RunReadinessState


@dataclass(frozen=True)
class SymbolIdentityDelta:
    symbol_key: str
    seq: int
    event_time: Any
    identity: Dict[str, Any]


@dataclass(frozen=True)
class CandleDelta:
    symbol_key: str
    seq: int
    event_time: Any
    candle: Dict[str, Any]


@dataclass(frozen=True)
class OverlayDelta:
    symbol_key: str
    seq: int
    event_time: Any
    overlay_ops: Dict[str, Any]


@dataclass(frozen=True)
class SignalDelta:
    symbol_key: str
    seq: int
    event_time: Any
    appended_signals: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class DecisionDelta:
    symbol_key: str
    seq: int
    event_time: Any
    appended_decisions: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class TradeDelta:
    symbol_key: str
    seq: int
    event_time: Any
    trade_upserts: Tuple[Dict[str, Any], ...]
    trade_removals: Tuple[str, ...]


@dataclass(frozen=True)
class DiagnosticDelta:
    symbol_key: str
    seq: int
    event_time: Any
    appended_diagnostics: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class SeriesStatsDelta:
    symbol_key: str
    seq: int
    event_time: Any
    stats: Dict[str, Any]


@dataclass(frozen=True)
class RunLifecycleDelta:
    seq: int
    event_time: Any
    lifecycle: Dict[str, Any]


@dataclass(frozen=True)
class RunHealthDelta:
    seq: int
    event_time: Any
    health: Dict[str, Any]


@dataclass(frozen=True)
class RunFaultDelta:
    seq: int
    event_time: Any
    appended_faults: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class RunSymbolCatalogDelta:
    seq: int
    event_time: Any
    symbol_upserts: Tuple[Dict[str, Any], ...]
    symbol_removals: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RunOpenTradesDelta:
    seq: int
    event_time: Any
    upserts: Tuple[Dict[str, Any], ...]
    removals: Tuple[str, ...]


SymbolConcernDelta = (
    SymbolIdentityDelta
    | CandleDelta
    | OverlayDelta
    | SignalDelta
    | DecisionDelta
    | TradeDelta
    | DiagnosticDelta
    | SeriesStatsDelta
)
RunConcernDelta = (
    RunLifecycleDelta
    | RunHealthDelta
    | RunFaultDelta
    | RunSymbolCatalogDelta
    | RunOpenTradesDelta
)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return normalized.isoformat().replace("+00:00", "Z")
    text = str(value or "").strip()
    return text or None


def _normalize_scalar_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_scalar_value(entry) for key, entry in value.items()}
    if isinstance(value, list):
        return [_normalize_scalar_value(entry) for entry in value]
    if isinstance(value, tuple):
        return [_normalize_scalar_value(entry) for entry in value]
    if isinstance(value, datetime):
        normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return normalized.isoformat().replace("+00:00", "Z")
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _copy_entries(entries: Iterable[Mapping[str, Any]]) -> Tuple[Dict[str, Any], ...]:
    return tuple(dict(entry) for entry in entries if isinstance(entry, Mapping))


_UNSET = object()


def _mapping_copy(value: Any) -> Dict[str, Any]:
    return {str(key): _normalize_scalar_value(entry) for key, entry in _mapping(value).items()}


def _transition_history(value: Any) -> Tuple[Dict[str, Any], ...]:
    return tuple(summarize_transition_history(value, limit=12))


def _warning_severity_rank(value: Any) -> int:
    normalized = str(value or "").strip().lower()
    if normalized in {"critical", "error"}:
        return 0
    if normalized in {"warning", "warn"}:
        return 1
    return 2


def _warning_sort_timestamp(value: Any) -> float:
    text = _iso_or_none(value)
    if not text:
        return 0.0
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.timestamp()


def _sort_warning_conditions(entries: Iterable[Mapping[str, Any]]) -> Tuple[Dict[str, Any], ...]:
    normalized = [dict(entry) for entry in entries if isinstance(entry, Mapping)]
    normalized.sort(
        key=lambda entry: (
            _warning_severity_rank(entry.get("severity")),
            -_warning_sort_timestamp(entry.get("last_seen_at") or entry.get("first_seen_at")),
            -max(int(entry.get("count") or 0), 0),
            str(entry.get("warning_id") or ""),
        )
    )
    return tuple(normalized[:_MAX_WARNINGS])


def _warning_types_summary(entries: Iterable[Mapping[str, Any]]) -> Tuple[str, ...]:
    values = sorted(
        {
            str(entry.get("warning_type") or "").strip().lower()
            for entry in entries
            if isinstance(entry, Mapping) and str(entry.get("warning_type") or "").strip()
        }
    )
    return tuple(values)


def _highest_warning_severity(entries: Iterable[Mapping[str, Any]]) -> Optional[str]:
    normalized = [
        str(entry.get("severity") or "warning").strip().lower()
        for entry in entries
        if isinstance(entry, Mapping)
    ]
    if not normalized:
        return None
    return min(normalized, key=lambda value: (_warning_severity_rank(value), value))


def _merge_run_health_warnings(
    current: Tuple[Dict[str, Any], ...],
    incoming: Any,
    *,
    observed_at: Any,
) -> Tuple[Dict[str, Any], ...]:
    if incoming is _UNSET:
        return current
    observed_at_text = _iso_or_none(observed_at)
    previous_by_id = {
        str(entry.get("warning_id") or "").strip(): dict(entry)
        for entry in current
        if isinstance(entry, Mapping) and str(entry.get("warning_id") or "").strip()
    }
    merged: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for raw_warning in incoming if isinstance(incoming, list) else []:
        canonical = canonicalize_health_warning(raw_warning)
        if canonical is None:
            continue
        warning_id = str(canonical.get("warning_id") or "").strip()
        if not warning_id:
            continue
        previous = merged.get(warning_id) or previous_by_id.get(warning_id) or {}
        explicit_count = None
        if isinstance(raw_warning, Mapping) and raw_warning.get("count") is not None:
            try:
                explicit_count = max(int(raw_warning.get("count") or 0), 1)
            except (TypeError, ValueError):
                explicit_count = 1
        previous_count = max(int(previous.get("count") or 0), 1) if previous else 0
        if explicit_count is None:
            canonical["count"] = previous_count + 1 if previous_count > 0 else max(int(canonical.get("count") or 0), 1)
        else:
            canonical["count"] = max(int(canonical.get("count") or 0), explicit_count, previous_count + 1)

        first_seen_at = _iso_or_none(canonical.get("first_seen_at"))
        if not first_seen_at:
            first_seen_at = _iso_or_none(previous.get("first_seen_at")) or observed_at_text
        if first_seen_at:
            canonical["first_seen_at"] = first_seen_at

        last_seen_at = _iso_or_none(canonical.get("last_seen_at"))
        previous_last_seen_at = _iso_or_none(previous.get("last_seen_at"))
        if not last_seen_at and observed_at_text:
            last_seen_at = observed_at_text
        elif previous and observed_at_text and last_seen_at and last_seen_at < observed_at_text:
            last_seen_at = observed_at_text
        if not last_seen_at:
            last_seen_at = previous_last_seen_at or first_seen_at or observed_at_text
        if last_seen_at:
            canonical["last_seen_at"] = last_seen_at

        if not canonical.get("title") and previous.get("title"):
            canonical["title"] = previous.get("title")
        if not canonical.get("indicator_id") and previous.get("indicator_id"):
            canonical["indicator_id"] = previous.get("indicator_id")
        if not canonical.get("symbol_key") and previous.get("symbol_key"):
            canonical["symbol_key"] = previous.get("symbol_key")
        if not canonical.get("symbol") and previous.get("symbol"):
            canonical["symbol"] = previous.get("symbol")
        if not canonical.get("timeframe") and previous.get("timeframe"):
            canonical["timeframe"] = previous.get("timeframe")
        if not canonical.get("context") and previous.get("context"):
            canonical["context"] = _mapping_copy(previous.get("context"))
        merged[warning_id] = canonical
    return _sort_warning_conditions(merged.values())


def _build_run_health_state(
    state: RunHealthState,
    *,
    status: Any = _UNSET,
    phase: Any = _UNSET,
    warning_count: Any = _UNSET,
    warnings: Any = _UNSET,
    last_event_at: Any = _UNSET,
    worker_count: Any = _UNSET,
    active_workers: Any = _UNSET,
    trigger_event: Any = _UNSET,
    runtime_state: Any = _UNSET,
    last_useful_progress_at: Any = _UNSET,
    progress_state: Any = _UNSET,
    degraded: Any = _UNSET,
    churn: Any = _UNSET,
    pressure: Any = _UNSET,
    recent_transitions: Any = _UNSET,
    terminal: Any = _UNSET,
) -> RunHealthState:
    next_status = state.status if status is _UNSET else str(status or state.status or "waiting")
    next_phase = state.phase if phase is _UNSET else phase
    next_warning_count = state.warning_count if warning_count is _UNSET else max(int(warning_count or 0), 0)
    next_last_event_at = state.last_event_at if last_event_at is _UNSET else _iso_or_none(last_event_at)
    next_warnings = _merge_run_health_warnings(
        state.warnings,
        warnings,
        observed_at=next_last_event_at or state.last_event_at,
    )
    next_worker_count = state.worker_count if worker_count is _UNSET else max(int(worker_count or 0), 0)
    next_active_workers = state.active_workers if active_workers is _UNSET else max(int(active_workers or 0), 0)
    next_trigger_event = state.trigger_event if trigger_event is _UNSET else (str(trigger_event or "").strip() or None)
    next_runtime_state = state.runtime_state if runtime_state is _UNSET else (str(runtime_state or "").strip() or None)
    next_last_useful_progress_at = (
        state.last_useful_progress_at
        if last_useful_progress_at is _UNSET
        else _iso_or_none(last_useful_progress_at)
    )
    next_progress_state = state.progress_state if progress_state is _UNSET else (str(progress_state or "").strip() or None)
    next_degraded = dict(state.degraded) if degraded is _UNSET else _mapping_copy(degraded)
    next_churn = dict(state.churn) if churn is _UNSET else _mapping_copy(churn)
    next_pressure = dict(state.pressure) if pressure is _UNSET else _mapping_copy(pressure)
    next_recent_transitions = state.recent_transitions if recent_transitions is _UNSET else _transition_history(recent_transitions)
    next_terminal = dict(state.terminal) if terminal is _UNSET else _mapping_copy(terminal)
    next_warning_types = _warning_types_summary(next_warnings)
    next_highest_warning_severity = _highest_warning_severity(next_warnings)
    resolved_warning_count = (
        max(int(next_warning_count or 0), len(next_warnings))
        if warnings is _UNSET
        else len(next_warnings)
    )
    return RunHealthState(
        status=next_status,
        phase=next_phase,
        warning_count=resolved_warning_count,
        warnings=next_warnings,
        last_event_at=next_last_event_at,
        worker_count=next_worker_count,
        active_workers=next_active_workers,
        warning_types=next_warning_types,
        highest_warning_severity=next_highest_warning_severity,
        trigger_event=next_trigger_event,
        runtime_state=next_runtime_state,
        last_useful_progress_at=next_last_useful_progress_at,
        progress_state=next_progress_state,
        degraded=next_degraded,
        churn=next_churn,
        pressure=next_pressure,
        recent_transitions=next_recent_transitions,
        terminal=next_terminal,
    )


def normalize_candle_time(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        return int(math.floor(numeric))
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
    except ValueError:
        numeric = float("nan")
    if math.isfinite(numeric):
        if abs(numeric) > 2e10:
            numeric /= 1000.0
        return int(math.floor(numeric))
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return None


def canonicalize_candle(candle: Any) -> Dict[str, Any]:
    if isinstance(candle, Mapping):
        time_value = normalize_candle_time(candle.get("time"))
        if time_value is not None and all(key in candle for key in ("open", "high", "low", "close")):
            try:
                return {
                    **dict(candle),
                    "time": int(time_value),
                    "open": float(candle.get("open")),
                    "high": float(candle.get("high")),
                    "low": float(candle.get("low")),
                    "close": float(candle.get("close")),
                }
            except (TypeError, ValueError):
                pass
    return canonicalize_botlens_candle(candle)


def merge_candles(*streams: Any, limit: int = _MAX_CANDLES) -> Tuple[Dict[str, Any], ...]:
    by_time: Dict[int, Dict[str, Any]] = {}
    for stream in streams:
        values = stream if isinstance(stream, (list, tuple)) else []
        for candle in values:
            normalized = canonicalize_candle(candle)
            by_time[int(normalized["time"])] = normalized
    ordered = [by_time[key] for key in sorted(by_time.keys())]
    if int(limit) > 0 and len(ordered) > int(limit):
        ordered = ordered[-int(limit) :]
    return tuple(ordered)


def overlay_identity(overlay: Any, index: int) -> str:
    if not isinstance(overlay, Mapping):
        return f"index:{index}"
    explicit = str(overlay.get("overlay_id") or "").strip()
    if explicit:
        return explicit
    for key in ("id", "name", "key", "slug", "indicator_id", "type"):
        value = str(overlay.get(key) or "").strip()
        if value:
            return f"{key}:{value}"
    return f"index:{index}"


def overlay_revision(overlay: Any) -> str:
    return json.dumps(_normalize_scalar_value(overlay), sort_keys=True, separators=(",", ":"))


def project_overlay_state(overlays: Any) -> Tuple[Dict[str, Any], ...]:
    projected: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for index, overlay in enumerate(overlays if isinstance(overlays, (list, tuple)) else []):
        if not isinstance(overlay, Mapping):
            continue
        identity = overlay_identity(overlay, index)
        normalized = dict(overlay)
        normalized["overlay_id"] = identity
        normalized["overlay_revision"] = overlay_revision(
            {key: value for key, value in normalized.items() if key != "overlay_revision"}
        )
        projected[identity] = normalized
    return tuple(projected.values())


def apply_overlay_delta(overlays: Any, delta: Any) -> Tuple[Dict[str, Any], ...]:
    current = project_overlay_state(overlays)
    payload = delta if isinstance(delta, Mapping) else {}
    ops = payload.get("ops") if isinstance(payload.get("ops"), list) else []
    overlay_map: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for index, overlay in enumerate(current):
        overlay_id = str(overlay.get("overlay_id") or overlay_identity(overlay, index))
        normalized = dict(overlay)
        normalized["overlay_id"] = overlay_id
        overlay_map[overlay_id] = normalized
    for op in ops:
        if not isinstance(op, Mapping):
            continue
        op_name = str(op.get("op") or "").strip().lower()
        key = str(op.get("key") or "").strip()
        if not key:
            continue
        if op_name == "remove":
            overlay_map.pop(key, None)
            continue
        if op_name != "upsert":
            continue
        overlay = op.get("overlay")
        if not isinstance(overlay, Mapping):
            continue
        normalized = dict(overlay)
        normalized["overlay_id"] = key
        normalized["overlay_revision"] = overlay_revision(
            {entry_key: entry_value for entry_key, entry_value in normalized.items() if entry_key != "overlay_revision"}
        )
        overlay_map[key] = normalized
    overlays_out = list(overlay_map.values())
    if len(overlays_out) > _MAX_OVERLAYS:
        overlays_out = overlays_out[-_MAX_OVERLAYS:]
    return tuple(overlays_out)


def _upsert_key(entry: Mapping[str, Any], key_fields: tuple[str, ...]) -> str:
    for field in key_fields:
        value = str(entry.get(field) or "").strip()
        if value:
            return f"{field}:{value}"
    return ""


def _upsert_tail(
    entries: Iterable[Mapping[str, Any]],
    items: Iterable[Mapping[str, Any]],
    *,
    key_fields: tuple[str, ...],
    limit: int,
) -> Tuple[Dict[str, Any], ...]:
    ordered: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        key = _upsert_key(entry, key_fields)
        if not key:
            continue
        ordered[key] = dict(entry)
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = _upsert_key(item, key_fields)
        if not key:
            continue
        ordered[key] = dict(item)
    values = list(ordered.values())
    if int(limit) > 0 and len(values) > int(limit):
        values = values[-int(limit) :]
    return tuple(values)


def is_open_trade(trade: Any) -> bool:
    if not isinstance(trade, Mapping):
        return False
    if trade.get("closed_at"):
        return False
    trade_state = str(trade.get("trade_state") or "").strip().lower()
    if trade_state:
        return trade_state != "closed"
    status = str(trade.get("status") or "").strip().lower()
    if status in {"closed", "completed", "complete"}:
        trade_id = str(trade.get("trade_id") or "").strip() or "<missing>"
        raise RuntimeError(
            "botlens_trade_projection_invalid: closed trade missing closing event fields "
            f"trade_id={trade_id} status={status}"
        )
    return True


def normalize_trade(trade: Any, *, symbol_key: str) -> Optional[Dict[str, Any]]:
    if not isinstance(trade, Mapping):
        return None
    trade_id = str(trade.get("trade_id") or trade.get("id") or "").strip()
    if not trade_id:
        return None
    normalized = dict(trade)
    normalized["trade_id"] = trade_id
    normalized["symbol_key"] = normalize_series_key(symbol_key)
    return normalized


def display_label(*, symbol: str, timeframe: str, symbol_key: str) -> str:
    display_symbol = str(symbol or "").strip() or str(symbol_key.split("|", 1)[0] if "|" in symbol_key else symbol_key).strip()
    display_timeframe = str(timeframe or "").strip()
    if display_timeframe:
        return f"{display_symbol} · {display_timeframe}"
    return display_symbol or "Unknown symbol"


def empty_symbol_identity_state(symbol_key: str) -> SymbolIdentityState:
    instrument_id, timeframe = str(symbol_key).split("|", 1) if "|" in str(symbol_key) else ("", "")
    return SymbolIdentityState(
        symbol_key=normalize_series_key(symbol_key),
        instrument_id=instrument_id or None,
        symbol=None,
        timeframe=timeframe or None,
    )


def empty_symbol_projection_snapshot(symbol_key: str) -> SymbolProjectionSnapshot:
    normalized_symbol_key = normalize_series_key(symbol_key)
    return SymbolProjectionSnapshot(
        schema_version=SCHEMA_VERSION,
        symbol_key=normalized_symbol_key,
        seq=0,
        last_event_at=None,
        identity=empty_symbol_identity_state(normalized_symbol_key),
        candles=SymbolCandlesState(candles=()),
        overlays=SymbolOverlaysState(overlays=()),
        signals=SymbolSignalsState(signals=()),
        decisions=SymbolDecisionsState(decisions=()),
        trades=SymbolTradesState(trades=()),
        diagnostics=SymbolDiagnosticsState(diagnostics=()),
        stats=SymbolStatsState(stats={}),
        readiness=SymbolReadinessState(snapshot_ready=False, symbol_live=False),
    )


def empty_run_lifecycle_state(run_id: str) -> RunLifecycleState:
    return RunLifecycleState(
        run_id=str(run_id),
        phase=None,
        status=None,
        owner=None,
        message=None,
        metadata={},
        failure={},
        checkpoint_at=None,
        updated_at=None,
        live=False,
    )


def empty_run_health_state() -> RunHealthState:
    return RunHealthState(
        status="waiting",
        phase=None,
        warning_count=0,
        warnings=(),
        last_event_at=None,
        worker_count=0,
        active_workers=0,
        warning_types=(),
        highest_warning_severity=None,
        trigger_event=None,
        runtime_state=None,
        last_useful_progress_at=None,
        progress_state=None,
        degraded={},
        churn={},
        pressure={},
        recent_transitions=(),
        terminal={},
    )


def empty_run_projection_snapshot(*, bot_id: str, run_id: str) -> RunProjectionSnapshot:
    return RunProjectionSnapshot(
        schema_version=SCHEMA_VERSION,
        bot_id=str(bot_id),
        run_id=str(run_id),
        seq=0,
        lifecycle=empty_run_lifecycle_state(run_id),
        health=empty_run_health_state(),
        faults=RunFaultsState(faults=()),
        symbol_catalog=RunSymbolCatalogState(entries={}),
        open_trades=RunOpenTradesState(entries={}),
        readiness=RunReadinessState(catalog_discovered=False, run_live=False),
    )


def _assert_lifecycle_consistency(lifecycle: RunLifecycleState, *, bot_id: str, run_id: str) -> None:
    phase = str(lifecycle.phase or "").strip().lower()
    status = str(lifecycle.status or "").strip().lower()
    if not phase and not status:
        return
    if not phase or not status:
        raise RuntimeError(
            "botlens_lifecycle_state_invalid: lifecycle phase/status must be present together "
            f"bot_id={bot_id} run_id={run_id} phase={phase or '<missing>'} status={status or '<missing>'}"
        )
    expected_status = str(status_for_phase(phase) or "").strip().lower()
    if expected_status and status != expected_status:
        raise RuntimeError(
            "botlens_lifecycle_state_invalid: lifecycle status does not match phase "
            f"bot_id={bot_id} run_id={run_id} phase={phase} status={status} expected_status={expected_status}"
        )


def _assert_run_projection_invariants(snapshot: RunProjectionSnapshot) -> None:
    _assert_lifecycle_consistency(snapshot.lifecycle, bot_id=snapshot.bot_id, run_id=snapshot.run_id)
    phase = str(snapshot.lifecycle.phase or "").strip().lower()
    status = str(snapshot.lifecycle.status or "").strip().lower()
    if phase == "completed" or status == "completed":
        if snapshot.open_trades.entries:
            open_trade_ids = ",".join(sorted(str(trade_id) for trade_id in snapshot.open_trades.entries))
            raise RuntimeError(
                "botlens_run_projection_invalid: completed run retains open trades "
                f"bot_id={snapshot.bot_id} run_id={snapshot.run_id} trade_ids={open_trade_ids}"
            )


def read_symbol_projection_snapshot(payload: Any, *, symbol_key: str) -> SymbolProjectionSnapshot:
    source = _mapping(payload)
    projection = _mapping(source.get("projection")) if "projection" in source else {}
    concerns = _mapping(projection.get("concerns"))
    base = empty_symbol_projection_snapshot(symbol_key)
    identity_payload = _mapping(concerns.get("identity"))
    readiness_payload = _mapping(concerns.get("readiness"))
    return SymbolProjectionSnapshot(
        schema_version=int(projection.get("schema_version") or source.get("schema_version") or SCHEMA_VERSION),
        symbol_key=normalize_series_key(projection.get("symbol_key") or symbol_key),
        seq=int(projection.get("seq") or 0),
        last_event_at=_iso_or_none(projection.get("last_event_at")),
        identity=SymbolIdentityState(
            symbol_key=normalize_series_key(identity_payload.get("symbol_key") or base.identity.symbol_key),
            instrument_id=str(identity_payload.get("instrument_id") or base.identity.instrument_id or "").strip() or None,
            symbol=str(identity_payload.get("symbol") or "").strip().upper() or None,
            timeframe=str(identity_payload.get("timeframe") or base.identity.timeframe or "").strip().lower() or None,
        ),
        candles=SymbolCandlesState(candles=merge_candles(_mapping(concerns.get("candles")).get("items"), limit=_MAX_CANDLES)),
        overlays=SymbolOverlaysState(overlays=project_overlay_state(_mapping(concerns.get("overlays")).get("items"))),
        signals=SymbolSignalsState(signals=_copy_entries(entry for entry in _mapping(concerns.get("signals")).get("items", []) if isinstance(entry, Mapping))),
        decisions=SymbolDecisionsState(decisions=_copy_entries(entry for entry in _mapping(concerns.get("decisions")).get("items", []) if isinstance(entry, Mapping))),
        trades=SymbolTradesState(trades=_copy_entries(entry for entry in _mapping(concerns.get("trades")).get("items", []) if isinstance(entry, Mapping))),
        diagnostics=SymbolDiagnosticsState(diagnostics=_copy_entries(entry for entry in _mapping(concerns.get("diagnostics")).get("items", []) if isinstance(entry, Mapping))),
        stats=SymbolStatsState(stats=dict(_mapping(concerns.get("stats")).get("payload") or {})),
        readiness=SymbolReadinessState(
            snapshot_ready=bool(readiness_payload.get("snapshot_ready") or int(projection.get("seq") or 0) > 0),
            symbol_live=bool(readiness_payload.get("symbol_live")),
        ),
    )


def read_run_projection_snapshot(payload: Any, *, bot_id: str, run_id: str) -> RunProjectionSnapshot:
    source = _mapping(payload)
    projection = _mapping(source.get("projection")) if "projection" in source else {}
    concerns = _mapping(projection.get("concerns"))
    lifecycle_payload = _mapping(concerns.get("lifecycle"))
    health_payload = _mapping(concerns.get("health"))
    raw_health_warnings = health_payload.get("warnings")
    warnings = _merge_run_health_warnings(
        (),
        raw_health_warnings,
        observed_at=health_payload.get("last_event_at"),
    )
    readiness_payload = _mapping(concerns.get("readiness"))
    catalog_entries = {
        normalize_series_key(key): dict(value)
        for key, value in _mapping(_mapping(concerns.get("symbol_catalog")).get("entries")).items()
        if normalize_series_key(key) and isinstance(value, Mapping)
    }
    open_trade_entries = {
        str(key): dict(value)
        for key, value in _mapping(_mapping(concerns.get("open_trades")).get("entries")).items()
        if str(key).strip() and isinstance(value, Mapping)
    }
    snapshot = RunProjectionSnapshot(
        schema_version=int(projection.get("schema_version") or source.get("schema_version") or SCHEMA_VERSION),
        bot_id=str(projection.get("bot_id") or bot_id),
        run_id=str(projection.get("run_id") or run_id),
        seq=int(projection.get("seq") or 0),
        lifecycle=RunLifecycleState(
            run_id=str(lifecycle_payload.get("run_id") or run_id),
            phase=lifecycle_payload.get("phase"),
            status=lifecycle_payload.get("status"),
            owner=lifecycle_payload.get("owner"),
            message=lifecycle_payload.get("message"),
            metadata=_mapping(lifecycle_payload.get("metadata")),
            failure=_mapping(lifecycle_payload.get("failure")),
            checkpoint_at=_iso_or_none(lifecycle_payload.get("checkpoint_at")),
            updated_at=_iso_or_none(lifecycle_payload.get("updated_at")),
            live=bool(lifecycle_payload.get("live")),
        ),
        health=RunHealthState(
            status=str(health_payload.get("status") or "waiting"),
            phase=health_payload.get("phase"),
            warning_count=(
                len(warnings)
                if isinstance(raw_health_warnings, list) and raw_health_warnings
                else max(int(health_payload.get("warning_count") or 0), len(warnings))
            ),
            warnings=warnings,
            last_event_at=_iso_or_none(health_payload.get("last_event_at")),
            worker_count=int(health_payload.get("worker_count") or 0),
            active_workers=int(health_payload.get("active_workers") or 0),
            warning_types=tuple(
                str(entry).strip().lower()
                for entry in health_payload.get("warning_types", [])
                if str(entry).strip()
            )
            or _warning_types_summary(warnings),
            highest_warning_severity=(
                str(health_payload.get("highest_warning_severity") or "").strip().lower() or None
            )
            or _highest_warning_severity(warnings),
            trigger_event=str(health_payload.get("trigger_event") or "").strip() or None,
            runtime_state=str(health_payload.get("runtime_state") or "").strip() or None,
            last_useful_progress_at=_iso_or_none(health_payload.get("last_useful_progress_at")),
            progress_state=str(health_payload.get("progress_state") or "").strip() or None,
            degraded=_mapping(health_payload.get("degraded")),
            churn=_mapping(health_payload.get("churn")),
            pressure=_mapping(health_payload.get("pressure")),
            recent_transitions=tuple(summarize_transition_history(health_payload.get("recent_transitions"), limit=12)),
            terminal=_mapping(health_payload.get("terminal")),
        ),
        faults=RunFaultsState(
            faults=_copy_entries(entry for entry in _mapping(concerns.get("faults")).get("items", []) if isinstance(entry, Mapping))
        ),
        symbol_catalog=RunSymbolCatalogState(entries=catalog_entries),
        open_trades=RunOpenTradesState(entries=open_trade_entries),
        readiness=RunReadinessState(
            catalog_discovered=bool(readiness_payload.get("catalog_discovered") or catalog_entries),
            run_live=bool(readiness_payload.get("run_live") or lifecycle_payload.get("live")),
        ),
    )
    _assert_run_projection_invariants(snapshot)
    return snapshot


def serialize_symbol_projection_snapshot(state: SymbolProjectionSnapshot) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "symbol_projection_snapshot",
        "projection": {
            "schema_version": SCHEMA_VERSION,
            "symbol_key": state.symbol_key,
            "seq": int(state.seq),
            "last_event_at": state.last_event_at,
            "concerns": {
                "identity": state.identity.to_dict(),
                "candles": {"items": [dict(entry) for entry in state.candles.candles]},
                "overlays": {"items": [dict(entry) for entry in state.overlays.overlays]},
                "signals": {"items": [dict(entry) for entry in state.signals.signals]},
                "decisions": {"items": [dict(entry) for entry in state.decisions.decisions]},
                "trades": {"items": [dict(entry) for entry in state.trades.trades]},
                "diagnostics": {"items": [dict(entry) for entry in state.diagnostics.diagnostics]},
                "stats": {"payload": dict(state.stats.stats)},
                "readiness": state.readiness.to_dict(),
            },
        },
    }


def serialize_run_projection_snapshot(state: RunProjectionSnapshot) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "run_projection_snapshot",
        "projection": {
            "schema_version": SCHEMA_VERSION,
            "bot_id": state.bot_id,
            "run_id": state.run_id,
            "seq": int(state.seq),
            "concerns": {
                "lifecycle": state.lifecycle.to_dict(),
                "health": state.health.to_dict(),
                "faults": {"items": [dict(entry) for entry in state.faults.faults]},
                "symbol_catalog": {"entries": {key: dict(value) for key, value in state.symbol_catalog.entries.items()}},
                "open_trades": {"entries": {key: dict(value) for key, value in state.open_trades.entries.items()}},
                "readiness": state.readiness.to_dict(),
            },
        },
    }


def reset_run_symbol_scope(snapshot: RunProjectionSnapshot, *, symbol_key: str) -> RunProjectionSnapshot:
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        return snapshot
    next_catalog = dict(snapshot.symbol_catalog.entries)
    next_catalog.pop(normalized_symbol_key, None)
    next_open_trades = {
        trade_id: dict(trade)
        for trade_id, trade in snapshot.open_trades.entries.items()
        if normalize_series_key(trade.get("symbol_key")) != normalized_symbol_key
    }
    return replace(
        snapshot,
        symbol_catalog=RunSymbolCatalogState(entries=next_catalog),
        open_trades=RunOpenTradesState(entries=next_open_trades),
        readiness=RunReadinessState(
            catalog_discovered=bool(next_catalog),
            run_live=bool(snapshot.readiness.run_live),
        ),
    )


def _series_context_identity(event: BotLensDomainEvent, *, fallback_symbol_key: str) -> SymbolIdentityState:
    context = event.context.to_dict()
    return SymbolIdentityState(
        symbol_key=normalize_series_key(context.get("series_key") or fallback_symbol_key),
        instrument_id=str(context.get("instrument_id") or "").strip() or None,
        symbol=str(context.get("symbol") or "").strip().upper() or None,
        timeframe=str(context.get("timeframe") or "").strip().lower() or None,
    )


def _signal_entry(event: BotLensDomainEvent) -> Dict[str, Any]:
    context = event.context.to_dict()
    return {
        "event_id": event.event_id,
        "root_id": event.root_id,
        "parent_id": event.parent_id,
        "event_ts": _iso_or_none(event.event_ts),
        **context,
    }


def _decision_entry(event: BotLensDomainEvent) -> Dict[str, Any]:
    context = event.context.to_dict()
    return {
        "event_id": event.event_id,
        "root_id": event.root_id,
        "parent_id": event.parent_id,
        "event_ts": _iso_or_none(event.event_ts),
        **context,
    }


def _trade_entry(event: BotLensDomainEvent) -> Dict[str, Any]:
    context = event.context.to_dict()
    return {
        "event_id": event.event_id,
        "event_name": event.event_name.value,
        "event_ts": _iso_or_none(event.event_ts),
        "trade_id": context.get("trade_id"),
        "symbol_key": context.get("series_key"),
        "instrument_id": context.get("instrument_id"),
        "symbol": context.get("symbol"),
        "timeframe": context.get("timeframe"),
        "trade_state": context.get("trade_state"),
        "side": context.get("side"),
        "direction": context.get("direction"),
        "qty": context.get("qty"),
        "entry_price": context.get("entry_price"),
        "exit_price": context.get("exit_price"),
        "realized_pnl": context.get("realized_pnl"),
        "event_impact_pnl": context.get("event_impact_pnl"),
        "trade_net_pnl": context.get("trade_net_pnl"),
        "opened_at": context.get("opened_at"),
        "closed_at": context.get("closed_at"),
        "updated_at": _iso_or_none(event.event_ts),
        "status": context.get("trade_state"),
    }


def _diagnostic_entry(event: BotLensDomainEvent) -> Dict[str, Any]:
    context = event.context.to_dict()
    return {
        "event_id": event.event_id,
        "id": context.get("diagnostic_id") or event.event_id,
        "event_ts": _iso_or_none(event.event_ts),
        **context,
    }


def _fault_entry(event: BotLensDomainEvent) -> Dict[str, Any]:
    context = event.context.to_dict()
    return {
        "event_id": event.event_id,
        "event_ts": _iso_or_none(event.event_ts),
        **context,
    }


def _catalog_entry(existing: Mapping[str, Any] | None, *, identity: SymbolIdentityState, event: BotLensDomainEvent) -> Dict[str, Any]:
    current = dict(existing) if isinstance(existing, Mapping) else {}
    next_entry = {
        "symbol_key": identity.symbol_key,
        "instrument_id": identity.instrument_id,
        "symbol": identity.symbol,
        "timeframe": identity.timeframe,
        "last_event_at": _iso_or_none(event.event_ts),
        "last_activity_at": _iso_or_none(event.event_ts),
        "last_bar_time": current.get("last_bar_time"),
        "last_price": current.get("last_price"),
        "candle_count": int(current.get("candle_count") or 0),
        "last_trade_at": current.get("last_trade_at"),
        "stats": dict(current.get("stats") or {}) if isinstance(current.get("stats"), Mapping) else {},
    }
    if event.event_name == BotLensDomainEventName.CANDLE_OBSERVED:
        candle = canonicalize_candle(event.context.to_dict().get("candle"))
        next_entry["last_bar_time"] = candle.get("time")
        next_entry["last_price"] = candle.get("close")
        next_entry["candle_count"] = max(int(next_entry["candle_count"] or 0) + 1, 1)
    elif event.event_name in {
        BotLensDomainEventName.TRADE_OPENED,
        BotLensDomainEventName.TRADE_UPDATED,
        BotLensDomainEventName.TRADE_CLOSED,
    }:
        next_entry["last_trade_at"] = _iso_or_none(event.event_ts)
    elif event.event_name == BotLensDomainEventName.SERIES_STATS_REPORTED:
        next_entry["stats"] = dict(event.context.to_dict().get("stats") or {})
    return next_entry


def apply_symbol_identity_projector(
    state: SymbolIdentityState,
    *,
    batch: ProjectionBatch,
) -> tuple[SymbolIdentityState, Tuple[SymbolIdentityDelta, ...]]:
    next_state = state
    deltas: List[SymbolIdentityDelta] = []
    for event in batch.events:
        identity = _series_context_identity(event, fallback_symbol_key=state.symbol_key)
        if not identity.symbol_key:
            continue
        merged_identity = SymbolIdentityState(
            symbol_key=identity.symbol_key,
            instrument_id=identity.instrument_id or next_state.instrument_id,
            symbol=identity.symbol or next_state.symbol,
            timeframe=identity.timeframe or next_state.timeframe,
        )
        if merged_identity != next_state:
            next_state = merged_identity
            deltas.append(
                SymbolIdentityDelta(
                    symbol_key=merged_identity.symbol_key,
                    seq=batch.seq,
                    event_time=_iso_or_none(event.event_ts) or batch.event_time,
                    identity=merged_identity.to_dict(),
                )
            )
    return next_state, tuple(deltas)


def apply_symbol_candle_projector(
    state: SymbolCandlesState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolCandlesState, Tuple[CandleDelta, ...]]:
    next_state = state
    deltas: List[CandleDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.CANDLE_OBSERVED:
            continue
        candle = canonicalize_candle(event.context.to_dict().get("candle"))
        next_state = SymbolCandlesState(candles=merge_candles(next_state.candles, [candle], limit=_MAX_CANDLES))
        deltas.append(CandleDelta(symbol_key=symbol_key, seq=batch.seq, event_time=_iso_or_none(event.event_ts) or batch.event_time, candle=dict(candle)))
    return next_state, tuple(deltas)


def apply_symbol_overlay_projector(
    state: SymbolOverlaysState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolOverlaysState, Tuple[OverlayDelta, ...]]:
    next_state = state
    deltas: List[OverlayDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.OVERLAY_STATE_CHANGED:
            continue
        overlay_ops = _mapping(event.context.to_dict().get("overlay_delta"))
        next_state = SymbolOverlaysState(overlays=apply_overlay_delta(next_state.overlays, overlay_ops))
        deltas.append(
            OverlayDelta(
                symbol_key=symbol_key,
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                overlay_ops=dict(overlay_ops),
            )
        )
    return next_state, tuple(deltas)


def apply_symbol_signal_projector(
    state: SymbolSignalsState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolSignalsState, Tuple[SignalDelta, ...]]:
    next_state = state
    deltas: List[SignalDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.SIGNAL_EMITTED:
            continue
        signal_entry = _signal_entry(event)
        next_state = SymbolSignalsState(
            signals=_upsert_tail(next_state.signals, [signal_entry], key_fields=("event_id", "signal_id"), limit=_MAX_DECISIONS)
        )
        deltas.append(
            SignalDelta(
                symbol_key=symbol_key,
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                appended_signals=(dict(signal_entry),),
            )
        )
    return next_state, tuple(deltas)


def apply_symbol_decision_projector(
    state: SymbolDecisionsState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolDecisionsState, Tuple[DecisionDelta, ...]]:
    next_state = state
    deltas: List[DecisionDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.DECISION_EMITTED:
            continue
        decision_entry = _decision_entry(event)
        next_state = SymbolDecisionsState(
            decisions=_upsert_tail(next_state.decisions, [decision_entry], key_fields=("event_id", "decision_id"), limit=_MAX_DECISIONS)
        )
        deltas.append(
            DecisionDelta(
                symbol_key=symbol_key,
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                appended_decisions=(dict(decision_entry),),
            )
        )
    return next_state, tuple(deltas)


def apply_symbol_trade_projector(
    state: SymbolTradesState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolTradesState, Tuple[TradeDelta, ...]]:
    next_state = state
    deltas: List[TradeDelta] = []
    for event in batch.events:
        if event.event_name not in {
            BotLensDomainEventName.TRADE_OPENED,
            BotLensDomainEventName.TRADE_UPDATED,
            BotLensDomainEventName.TRADE_CLOSED,
        }:
            continue
        trade_entry = _trade_entry(event)
        next_state = SymbolTradesState(
            trades=_upsert_tail(next_state.trades, [trade_entry], key_fields=("trade_id", "event_id"), limit=_MAX_TRADES)
        )
        normalized_trade = normalize_trade(trade_entry, symbol_key=symbol_key)
        upserts: Tuple[Dict[str, Any], ...] = ()
        removals: Tuple[str, ...] = ()
        if normalized_trade is not None and is_open_trade(normalized_trade):
            upserts = (normalized_trade,)
        else:
            trade_id = str(trade_entry.get("trade_id") or "").strip()
            removals = (trade_id,) if trade_id else ()
        deltas.append(
            TradeDelta(
                symbol_key=symbol_key,
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                trade_upserts=upserts,
                trade_removals=removals,
            )
        )
    return next_state, tuple(deltas)


def apply_symbol_diagnostic_projector(
    state: SymbolDiagnosticsState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolDiagnosticsState, Tuple[DiagnosticDelta, ...]]:
    next_state = state
    deltas: List[DiagnosticDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.DIAGNOSTIC_RECORDED:
            continue
        diagnostic_entry = _diagnostic_entry(event)
        next_state = SymbolDiagnosticsState(
            diagnostics=_upsert_tail(next_state.diagnostics, [diagnostic_entry], key_fields=("id", "event_id"), limit=_MAX_LOGS)
        )
        deltas.append(
            DiagnosticDelta(
                symbol_key=symbol_key,
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                appended_diagnostics=(dict(diagnostic_entry),),
            )
        )
    return next_state, tuple(deltas)


def apply_symbol_stats_projector(
    state: SymbolStatsState,
    *,
    batch: ProjectionBatch,
    symbol_key: str,
) -> tuple[SymbolStatsState, Tuple[SeriesStatsDelta, ...]]:
    next_state = state
    deltas: List[SeriesStatsDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.SERIES_STATS_REPORTED:
            continue
        stats = dict(event.context.to_dict().get("stats") or {})
        next_state = SymbolStatsState(stats=stats)
        deltas.append(
            SeriesStatsDelta(
                symbol_key=symbol_key,
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                stats=dict(stats),
            )
        )
    return next_state, tuple(deltas)


def apply_symbol_batch(
    snapshot: SymbolProjectionSnapshot,
    *,
    batch: ProjectionBatch,
) -> tuple[SymbolProjectionSnapshot, Tuple[SymbolConcernDelta, ...]]:
    symbol_key = snapshot.symbol_key
    identity_state, identity_deltas = apply_symbol_identity_projector(snapshot.identity, batch=batch)
    if identity_state.symbol_key:
        symbol_key = identity_state.symbol_key
    candle_state, candle_deltas = apply_symbol_candle_projector(snapshot.candles, batch=batch, symbol_key=symbol_key)
    overlay_state, overlay_deltas = apply_symbol_overlay_projector(snapshot.overlays, batch=batch, symbol_key=symbol_key)
    signal_state, signal_deltas = apply_symbol_signal_projector(snapshot.signals, batch=batch, symbol_key=symbol_key)
    decision_state, decision_deltas = apply_symbol_decision_projector(snapshot.decisions, batch=batch, symbol_key=symbol_key)
    trade_state, trade_deltas = apply_symbol_trade_projector(snapshot.trades, batch=batch, symbol_key=symbol_key)
    diagnostic_state, diagnostic_deltas = apply_symbol_diagnostic_projector(snapshot.diagnostics, batch=batch, symbol_key=symbol_key)
    stats_state, stats_deltas = apply_symbol_stats_projector(snapshot.stats, batch=batch, symbol_key=symbol_key)
    readiness_state = SymbolReadinessState(
        snapshot_ready=bool(snapshot.readiness.snapshot_ready or batch.events),
        symbol_live=bool(snapshot.readiness.symbol_live or batch.batch_kind != BRIDGE_BOOTSTRAP_KIND),
    )
    next_snapshot = replace(
        snapshot,
        symbol_key=symbol_key,
        seq=max(int(snapshot.seq), int(batch.seq)),
        last_event_at=_iso_or_none(batch.known_at or batch.event_time) or snapshot.last_event_at,
        identity=identity_state,
        candles=candle_state,
        overlays=overlay_state,
        signals=signal_state,
        decisions=decision_state,
        trades=trade_state,
        diagnostics=diagnostic_state,
        stats=stats_state,
        readiness=readiness_state,
    )
    deltas: Tuple[SymbolConcernDelta, ...] = (
        *identity_deltas,
        *candle_deltas,
        *overlay_deltas,
        *signal_deltas,
        *decision_deltas,
        *trade_deltas,
        *diagnostic_deltas,
        *stats_deltas,
    )
    return next_snapshot, deltas


def apply_run_lifecycle_projector(
    state: RunLifecycleState,
    *,
    batch: ProjectionBatch,
) -> tuple[RunLifecycleState, Tuple[RunLifecycleDelta, ...]]:
    next_state = state
    deltas: List[RunLifecycleDelta] = []
    for event in batch.events:
        if event.event_name.value not in {
            BotLensDomainEventName.RUN_PHASE_REPORTED.value,
            BotLensDomainEventName.RUN_STARTED.value,
            BotLensDomainEventName.RUN_READY.value,
            BotLensDomainEventName.RUN_DEGRADED.value,
            BotLensDomainEventName.RUN_COMPLETED.value,
            BotLensDomainEventName.RUN_FAILED.value,
            BotLensDomainEventName.RUN_STOPPED.value,
            BotLensDomainEventName.RUN_CANCELLED.value,
        }:
            continue
        context = event.context.to_dict()
        next_state = RunLifecycleState(
            run_id=str(context.get("run_id") or state.run_id),
            phase=context.get("phase"),
            status=context.get("status"),
            owner=context.get("component"),
            message=context.get("message"),
            metadata=_mapping(context.get("metadata")),
            failure=_mapping(context.get("failure")),
            checkpoint_at=_iso_or_none(event.event_ts) or _iso_or_none(batch.event_time),
            updated_at=_iso_or_none(event.event_ts) or _iso_or_none(batch.event_time),
            live=bool(context.get("live")),
        )
        deltas.append(RunLifecycleDelta(seq=batch.seq, event_time=_iso_or_none(event.event_ts) or batch.event_time, lifecycle=next_state.to_dict()))
    return next_state, tuple(deltas)


def apply_run_health_projector(
    state: RunHealthState,
    *,
    batch: ProjectionBatch,
) -> tuple[RunHealthState, Tuple[RunHealthDelta, ...]]:
    next_state = state
    deltas: List[RunHealthDelta] = []
    for event in batch.events:
        event_time = _iso_or_none(event.event_ts) or batch.event_time
        if event.event_name.value in {
            BotLensDomainEventName.RUN_PHASE_REPORTED.value,
            BotLensDomainEventName.RUN_STARTED.value,
            BotLensDomainEventName.RUN_READY.value,
            BotLensDomainEventName.RUN_DEGRADED.value,
            BotLensDomainEventName.RUN_COMPLETED.value,
            BotLensDomainEventName.RUN_FAILED.value,
            BotLensDomainEventName.RUN_STOPPED.value,
            BotLensDomainEventName.RUN_CANCELLED.value,
        }:
            context = event.context.to_dict()
            observability = _mapping(_mapping(context.get("metadata")).get("runtime_observability"))
            lifecycle_churn = observability.get("churn", _UNSET)
            if lifecycle_churn is _UNSET and str(observability.get("progress_state") or "").strip().lower() != "churning":
                lifecycle_churn = {}
            candidate_state = _build_run_health_state(
                next_state,
                status=str(context.get("status") or next_state.status or "waiting"),
                phase=context.get("phase"),
                last_event_at=event_time,
                runtime_state=observability.get("runtime_state", _UNSET),
                last_useful_progress_at=observability.get("last_useful_progress_at", _UNSET),
                progress_state=observability.get("progress_state", _UNSET),
                degraded=observability.get("degraded", _UNSET),
                churn=lifecycle_churn,
                pressure=observability.get("pressure", _UNSET),
                recent_transitions=observability.get("recent_transitions", _UNSET),
                terminal=observability.get("terminal", _UNSET),
            )
            if candidate_state != next_state:
                next_state = candidate_state
                deltas.append(RunHealthDelta(seq=batch.seq, event_time=event_time, health=next_state.to_dict()))
            continue
        if event.event_name != BotLensDomainEventName.HEALTH_STATUS_REPORTED:
            continue
        context = event.context.to_dict()
        health_churn = context.get("churn", _UNSET)
        if health_churn is _UNSET and str(context.get("progress_state") or "").strip().lower() != "churning":
            health_churn = {}
        candidate_state = _build_run_health_state(
            next_state,
            status=str(context.get("status") or next_state.status or "waiting"),
            phase=next_state.phase,
            warning_count=int(context.get("warning_count") or 0),
            warnings=context.get("warnings", []),
            last_event_at=event_time,
            worker_count=(
                int(context.get("worker_count") or 0)
                if context.get("worker_count") is not None
                else _UNSET
            ),
            active_workers=(
                int(context.get("active_workers") or 0)
                if context.get("active_workers") is not None
                else _UNSET
            ),
            trigger_event=str(context.get("trigger_event") or "").strip() or _UNSET,
            runtime_state=context.get("runtime_state", _UNSET),
            last_useful_progress_at=context.get("last_useful_progress_at", _UNSET),
            progress_state=context.get("progress_state", _UNSET),
            degraded=context.get("degraded", _UNSET),
            churn=health_churn,
            pressure=context.get("pressure", _UNSET),
            recent_transitions=context.get("recent_transitions", _UNSET),
            terminal=context.get("terminal", _UNSET),
        )
        if candidate_state != next_state:
            next_state = candidate_state
            deltas.append(RunHealthDelta(seq=batch.seq, event_time=event_time, health=next_state.to_dict()))
    return next_state, tuple(deltas)


def apply_run_fault_projector(
    state: RunFaultsState,
    *,
    batch: ProjectionBatch,
) -> tuple[RunFaultsState, Tuple[RunFaultDelta, ...]]:
    next_state = state
    deltas: List[RunFaultDelta] = []
    for event in batch.events:
        if event.event_name != BotLensDomainEventName.FAULT_RECORDED:
            continue
        fault_entry = _fault_entry(event)
        next_state = RunFaultsState(
            faults=_upsert_tail(next_state.faults, [fault_entry], key_fields=("event_id", "fault_code"), limit=_MAX_WARNINGS)
        )
        deltas.append(
            RunFaultDelta(
                seq=batch.seq,
                event_time=_iso_or_none(event.event_ts) or batch.event_time,
                appended_faults=(dict(fault_entry),),
            )
        )
    return next_state, tuple(deltas)


def apply_run_open_trades_projector(
    state: RunOpenTradesState,
    *,
    batch: ProjectionBatch,
) -> tuple[RunOpenTradesState, Tuple[RunOpenTradesDelta, ...]]:
    next_entries = dict(state.entries)
    deltas: List[RunOpenTradesDelta] = []
    for event in batch.events:
        if event.event_name not in {
            BotLensDomainEventName.TRADE_OPENED,
            BotLensDomainEventName.TRADE_UPDATED,
            BotLensDomainEventName.TRADE_CLOSED,
        }:
            continue
        trade_entry = normalize_trade(_trade_entry(event), symbol_key=str(event.context.to_dict().get("series_key") or ""))
        if trade_entry is None:
            continue
        upserts: Tuple[Dict[str, Any], ...] = ()
        removals: Tuple[str, ...] = ()
        trade_id = str(trade_entry.get("trade_id") or "").strip()
        if is_open_trade(trade_entry):
            next_entries[trade_id] = dict(trade_entry)
            upserts = (dict(trade_entry),)
        elif trade_id in next_entries:
            next_entries.pop(trade_id, None)
            removals = (trade_id,)
        if upserts or removals:
            deltas.append(
                RunOpenTradesDelta(
                    seq=batch.seq,
                    event_time=_iso_or_none(event.event_ts) or batch.event_time,
                    upserts=upserts,
                    removals=removals,
                )
            )
    return RunOpenTradesState(entries=next_entries), tuple(deltas)


def apply_run_symbol_catalog_projector(
    state: RunSymbolCatalogState,
    *,
    batch: ProjectionBatch,
) -> tuple[RunSymbolCatalogState, Tuple[RunSymbolCatalogDelta, ...]]:
    next_entries = dict(state.entries)
    upserts: List[Dict[str, Any]] = []
    for event in batch.events:
        context = event.context.to_dict() if hasattr(event.context, "to_dict") else {}
        if not context.get("series_key"):
            continue
        identity = _series_context_identity(event, fallback_symbol_key=str(context.get("series_key") or ""))
        if not identity.symbol_key:
            continue
        catalog_entry = _catalog_entry(next_entries.get(identity.symbol_key), identity=identity, event=event)
        next_entries[identity.symbol_key] = catalog_entry
        upserts.append(dict(catalog_entry))
    if not upserts:
        return RunSymbolCatalogState(entries=next_entries), ()
    deduped: OrderedDict[str, Dict[str, Any]] = OrderedDict()
    for entry in upserts:
        symbol_key = normalize_series_key(entry.get("symbol_key"))
        if symbol_key:
            deduped[symbol_key] = dict(entry)
    return RunSymbolCatalogState(entries=next_entries), (
        RunSymbolCatalogDelta(
            seq=batch.seq,
            event_time=batch.event_time,
            symbol_upserts=tuple(deduped.values()),
        ),
    )


def apply_run_batch(
    snapshot: RunProjectionSnapshot,
    *,
    batch: ProjectionBatch,
) -> tuple[RunProjectionSnapshot, Tuple[RunConcernDelta, ...]]:
    lifecycle_state, lifecycle_deltas = apply_run_lifecycle_projector(snapshot.lifecycle, batch=batch)
    health_state, health_deltas = apply_run_health_projector(snapshot.health, batch=batch)
    faults_state, fault_deltas = apply_run_fault_projector(snapshot.faults, batch=batch)
    open_trades_state, open_trade_deltas = apply_run_open_trades_projector(snapshot.open_trades, batch=batch)
    symbol_catalog_state, symbol_catalog_deltas = apply_run_symbol_catalog_projector(snapshot.symbol_catalog, batch=batch)
    readiness_state = RunReadinessState(
        catalog_discovered=bool(symbol_catalog_state.entries),
        run_live=bool(lifecycle_state.live),
    )
    next_snapshot = replace(
        snapshot,
        seq=max(int(snapshot.seq), int(batch.seq)),
        lifecycle=lifecycle_state,
        health=health_state,
        faults=faults_state,
        symbol_catalog=symbol_catalog_state,
        open_trades=open_trades_state,
        readiness=readiness_state,
    )
    _assert_run_projection_invariants(next_snapshot)
    deltas: Tuple[RunConcernDelta, ...] = (
        *lifecycle_deltas,
        *health_deltas,
        *fault_deltas,
        *symbol_catalog_deltas,
        *open_trade_deltas,
    )
    return next_snapshot, deltas


def select_default_symbol_key(
    *,
    symbol_catalog: Mapping[str, Any],
    open_trades: Mapping[str, Any],
) -> str | None:
    open_trade_by_symbol: Dict[str, list[Mapping[str, Any]]] = {}
    for trade in open_trades.values():
        if not isinstance(trade, Mapping):
            continue
        symbol_key = normalize_series_key(trade.get("symbol_key"))
        if not symbol_key:
            continue
        open_trade_by_symbol.setdefault(symbol_key, []).append(trade)

    candidates = []
    for symbol_key, summary in symbol_catalog.items():
        if not isinstance(summary, Mapping):
            continue
        symbol = str(summary.get("symbol") or "").strip().upper()
        timeframe = str(summary.get("timeframe") or "").strip().lower()
        last_activity = str(summary.get("last_activity_at") or "").strip()
        last_trade_at = str(summary.get("last_trade_at") or "").strip()
        candidates.append(
            {
                "symbol_key": symbol_key,
                "has_open_trade": bool(open_trade_by_symbol.get(symbol_key)),
                "last_trade_at": last_trade_at,
                "last_activity_at": last_activity,
                "symbol": symbol,
                "timeframe": timeframe,
            }
        )
    if not candidates:
        return None
    with_open_trade = [entry for entry in candidates if entry["has_open_trade"]]
    if with_open_trade:
        with_open_trade.sort(
            key=lambda entry: (
                entry["last_trade_at"],
                entry["last_activity_at"],
                entry["symbol"],
                entry["timeframe"],
                entry["symbol_key"],
            ),
            reverse=True,
        )
        return with_open_trade[0]["symbol_key"]
    candidates.sort(
        key=lambda entry: (
            entry["last_activity_at"],
            entry["symbol"],
            entry["timeframe"],
            entry["symbol_key"],
        ),
        reverse=True,
    )
    return candidates[0]["symbol_key"]


__all__ = [
    "CandleDelta",
    "DecisionDelta",
    "DiagnosticDelta",
    "OverlayDelta",
    "ProjectionBatch",
    "RunConcernDelta",
    "RunFaultDelta",
    "RunFaultsState",
    "RunHealthDelta",
    "RunHealthState",
    "RunLifecycleDelta",
    "RunLifecycleState",
    "RunOpenTradesDelta",
    "RunOpenTradesState",
    "RunProjectionSnapshot",
    "RunReadinessState",
    "RunSymbolCatalogDelta",
    "RunSymbolCatalogState",
    "RUN_SCOPE_KEY",
    "SCHEMA_VERSION",
    "SeriesStatsDelta",
    "SignalDelta",
    "SymbolCandlesState",
    "SymbolConcernDelta",
    "SymbolDecisionsState",
    "SymbolDiagnosticsState",
    "SymbolIdentityDelta",
    "SymbolIdentityState",
    "SymbolOverlaysState",
    "SymbolProjectionSnapshot",
    "SymbolReadinessState",
    "SymbolSignalsState",
    "SymbolStatsState",
    "SymbolTradesState",
    "TradeDelta",
    "apply_overlay_delta",
    "apply_run_batch",
    "apply_run_fault_projector",
    "apply_run_health_projector",
    "apply_run_lifecycle_projector",
    "apply_run_open_trades_projector",
    "apply_run_symbol_catalog_projector",
    "apply_symbol_batch",
    "apply_symbol_candle_projector",
    "apply_symbol_decision_projector",
    "apply_symbol_diagnostic_projector",
    "apply_symbol_identity_projector",
    "apply_symbol_overlay_projector",
    "apply_symbol_signal_projector",
    "apply_symbol_stats_projector",
    "apply_symbol_trade_projector",
    "canonicalize_candle",
    "display_label",
    "empty_run_health_state",
    "empty_run_lifecycle_state",
    "empty_run_projection_snapshot",
    "empty_symbol_identity_state",
    "empty_symbol_projection_snapshot",
    "is_open_trade",
    "merge_candles",
    "normalize_candle_time",
    "normalize_trade",
    "overlay_identity",
    "overlay_revision",
    "project_overlay_state",
    "read_run_projection_snapshot",
    "read_symbol_projection_snapshot",
    "reset_run_symbol_scope",
    "select_default_symbol_key",
    "serialize_run_projection_snapshot",
    "serialize_symbol_projection_snapshot",
]

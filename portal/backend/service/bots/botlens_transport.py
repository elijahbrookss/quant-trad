from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Dict, Iterable

from .botlens_candle_continuity import continuity_summary_from_candles
from .botlens_contract import (
    SCHEMA_VERSION,
    STREAM_CONNECTED_TYPE,
    STREAM_RESET_REQUIRED_TYPE,
    STREAM_RUN_FAULT_DELTA_TYPE,
    STREAM_RUN_HEALTH_DELTA_TYPE,
    STREAM_RUN_LIFECYCLE_DELTA_TYPE,
    STREAM_RUN_OPEN_TRADES_DELTA_TYPE,
    STREAM_RUN_SYMBOL_CATALOG_DELTA_TYPE,
    STREAM_SYMBOL_CANDLE_DELTA_TYPE,
    STREAM_SYMBOL_OVERLAY_DELTA_TYPE,
    STREAM_SYMBOL_DECISION_DELTA_TYPE,
    STREAM_SYMBOL_DIAGNOSTIC_DELTA_TYPE,
    STREAM_SYMBOL_SIGNAL_DELTA_TYPE,
    STREAM_SYMBOL_STATS_DELTA_TYPE,
    STREAM_SYMBOL_TRADE_DELTA_TYPE,
    normalize_series_key,
)
from .botlens_state import (
    CandleDelta,
    DecisionDelta,
    DiagnosticDelta,
    OverlayDelta,
    RunConcernDelta,
    RunFaultDelta,
    RunHealthDelta,
    RunLifecycleDelta,
    RunOpenTradesDelta,
    RunProjectionSnapshot,
    RunSymbolCatalogDelta,
    SeriesStatsDelta,
    SignalDelta,
    SymbolConcernDelta,
    SymbolProjectionSnapshot,
    TradeDelta,
    display_label,
)


def _json_size_bytes(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"))


def _runtime_payload(health_state: Mapping[str, Any] | None) -> Dict[str, Any]:
    source = health_state if isinstance(health_state, Mapping) else {}
    warning_types = [
        str(entry).strip().lower()
        for entry in source.get("warning_types", [])
        if str(entry).strip()
    ]
    payload = {
        "status": str(source.get("status") or "").strip() or None,
        "phase": source.get("phase"),
        "warning_count": int(source.get("warning_count") or 0),
        "warnings": [dict(entry) for entry in source.get("warnings", []) if isinstance(entry, Mapping)],
        "last_event_at": source.get("last_event_at"),
        "worker_count": int(source.get("worker_count") or 0),
        "active_workers": int(source.get("active_workers") or 0),
    }
    if warning_types:
        payload["warning_types"] = warning_types
    highest_warning_severity = str(source.get("highest_warning_severity") or "").strip().lower() or None
    if highest_warning_severity:
        payload["highest_warning_severity"] = highest_warning_severity
    if source.get("trigger_event"):
        payload["trigger_event"] = source.get("trigger_event")
    if source.get("runtime_state"):
        payload["runtime_state"] = source.get("runtime_state")
    if source.get("last_useful_progress_at"):
        payload["last_useful_progress_at"] = source.get("last_useful_progress_at")
    if source.get("progress_state"):
        payload["progress_state"] = source.get("progress_state")
    if isinstance(source.get("degraded"), Mapping) and source.get("degraded"):
        payload["degraded"] = dict(source.get("degraded"))
    if isinstance(source.get("churn"), Mapping) and source.get("churn"):
        payload["churn"] = dict(source.get("churn"))
    if isinstance(source.get("pressure"), Mapping) and source.get("pressure"):
        payload["pressure"] = dict(source.get("pressure"))
    if isinstance(source.get("terminal"), Mapping) and source.get("terminal"):
        payload["terminal"] = dict(source.get("terminal"))
    if isinstance(source.get("recent_transitions"), list) and source.get("recent_transitions"):
        payload["recent_transitions"] = [
            dict(entry) for entry in source.get("recent_transitions", []) if isinstance(entry, Mapping)
        ]
    return payload


def _symbol_identity_payload(state: SymbolProjectionSnapshot) -> Dict[str, Any]:
    identity = state.identity.to_dict()
    symbol_key = normalize_series_key(identity.get("symbol_key") or state.symbol_key)
    symbol = str(identity.get("symbol") or "").strip().upper()
    timeframe = str(identity.get("timeframe") or "").strip().lower()
    return {
        "symbol_key": symbol_key,
        "instrument_id": identity.get("instrument_id"),
        "symbol": symbol or None,
        "timeframe": timeframe or None,
        "display_label": display_label(symbol=symbol, timeframe=timeframe, symbol_key=symbol_key),
    }


def _symbol_detail_payload(state: SymbolProjectionSnapshot, *, run_health: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    identity = _symbol_identity_payload(state)
    return {
        **identity,
        "status": str((run_health or {}).get("status") or "waiting").strip() or "waiting",
        "last_event_at": state.last_event_at,
        "candles": [dict(entry) for entry in state.candles.candles],
        "overlays": [dict(entry) for entry in state.overlays.overlays],
        "recent_trades": [dict(entry) for entry in state.trades.trades],
        "logs": [dict(entry) for entry in state.diagnostics.diagnostics],
        "decisions": [dict(entry) for entry in state.decisions.decisions],
        "stats": dict(state.stats.stats),
        "runtime": _runtime_payload(run_health),
    }


def _symbol_current_payload(
    state: SymbolProjectionSnapshot,
    *,
    run_health: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    continuity = continuity_summary_from_candles(
        state.candles.candles,
        timeframe=state.identity.timeframe,
        series_key=state.symbol_key,
    ).to_dict()
    return {
        "candles": [dict(entry) for entry in state.candles.candles],
        "overlays": [dict(entry) for entry in state.overlays.overlays],
        "signals": [dict(entry) for entry in state.signals.signals],
        "decisions": [dict(entry) for entry in state.decisions.decisions],
        "recent_trades": [dict(entry) for entry in state.trades.trades],
        "logs": [dict(entry) for entry in state.diagnostics.diagnostics],
        "stats": dict(state.stats.stats),
        "runtime": _runtime_payload(run_health),
        "continuity": continuity,
    }


def _run_catalog_entry_payload(
    *,
    catalog_entry: Mapping[str, Any],
    health_state: Mapping[str, Any] | None,
    open_trades: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    identity = dict(catalog_entry)
    symbol_key = normalize_series_key(identity.get("symbol_key"))
    symbol = str(identity.get("symbol") or "").strip().upper()
    timeframe = str(identity.get("timeframe") or "").strip().lower()
    open_trade_list = [dict(entry) for entry in open_trades if isinstance(entry, Mapping)]
    readiness = dict(identity.get("readiness") or {}) if isinstance(identity.get("readiness"), Mapping) else {}
    return {
        "symbol_key": symbol_key,
        "identity": {
            "instrument_id": identity.get("instrument_id"),
            "symbol": symbol or None,
            "timeframe": timeframe or None,
            "display_label": display_label(symbol=symbol, timeframe=timeframe, symbol_key=symbol_key),
        },
        "activity": {
            "status": str((health_state or {}).get("status") or "waiting").strip() or "waiting",
            "last_event_at": identity.get("last_event_at"),
            "last_bar_time": identity.get("last_bar_time"),
            "last_price": identity.get("last_price"),
            "candle_count": int(identity.get("candle_count") or 0),
            "last_trade_at": identity.get("last_trade_at"),
            "last_activity_at": identity.get("last_activity_at"),
        },
        "open_trade": {
            "present": bool(open_trade_list),
            "count": len(open_trade_list),
        },
        "stats": dict(identity.get("stats") or {}) if isinstance(identity.get("stats"), Mapping) else {},
        "readiness": {
            "catalog_discovered": True,
            "snapshot_ready": bool(readiness.get("snapshot_ready")),
            "symbol_live": bool(readiness.get("symbol_live")),
        },
    }


def _live_symbol_summary_payload(
    *,
    catalog_entry: Mapping[str, Any],
    health_state: Mapping[str, Any] | None,
    open_trades: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    identity = dict(catalog_entry)
    symbol_key = normalize_series_key(identity.get("symbol_key"))
    symbol = str(identity.get("symbol") or "").strip().upper()
    timeframe = str(identity.get("timeframe") or "").strip().lower()
    open_trade_list = [dict(entry) for entry in open_trades if isinstance(entry, Mapping)]
    readiness = dict(identity.get("readiness") or {}) if isinstance(identity.get("readiness"), Mapping) else {}
    return {
        "symbol_key": symbol_key,
        "instrument_id": identity.get("instrument_id"),
        "symbol": symbol or None,
        "timeframe": timeframe or None,
        "display_label": display_label(symbol=symbol, timeframe=timeframe, symbol_key=symbol_key),
        "status": str((health_state or {}).get("status") or "waiting").strip() or "waiting",
        "last_event_at": identity.get("last_event_at"),
        "last_bar_time": identity.get("last_bar_time"),
        "last_price": identity.get("last_price"),
        "candle_count": int(identity.get("candle_count") or 0),
        "has_open_trade": bool(open_trade_list),
        "open_trade_count": len(open_trade_list),
        "last_trade_at": identity.get("last_trade_at"),
        "last_activity_at": identity.get("last_activity_at"),
        "stats": dict(identity.get("stats") or {}) if isinstance(identity.get("stats"), Mapping) else {},
        "readiness": {
            "catalog_discovered": True,
            "snapshot_ready": bool(readiness.get("snapshot_ready")),
            "symbol_live": bool(readiness.get("symbol_live")),
        },
    }


def _symbol_identity_from_catalog_entry(
    *,
    symbol_key: str,
    catalog_entry: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    entry = dict(catalog_entry or {})
    normalized_symbol_key = normalize_series_key(symbol_key or entry.get("symbol_key"))
    symbol = str(entry.get("symbol") or "").strip().upper()
    timeframe = str(entry.get("timeframe") or "").strip().lower()
    return {
        "symbol_key": normalized_symbol_key,
        "instrument_id": entry.get("instrument_id"),
        "symbol": symbol or None,
        "timeframe": timeframe or None,
        "display_label": display_label(symbol=symbol, timeframe=timeframe, symbol_key=normalized_symbol_key),
    }


def _selected_symbol_readiness_payload(
    *,
    symbol_key: str | None,
    symbol_state: SymbolProjectionSnapshot | None,
    symbol_catalog_entry: Mapping[str, Any] | None,
    run_live: bool,
) -> Dict[str, bool]:
    catalog_entry = dict(symbol_catalog_entry or {}) if isinstance(symbol_catalog_entry, Mapping) else {}
    catalog_discovered = bool(
        normalize_series_key(symbol_key)
        or normalize_series_key(catalog_entry.get("symbol_key"))
        or (symbol_state is not None and normalize_series_key(symbol_state.symbol_key))
    )
    snapshot_ready = bool(symbol_state is not None and symbol_state.readiness.snapshot_ready)
    symbol_live = bool(
        (symbol_state is not None and symbol_state.readiness.symbol_live)
        or (catalog_entry.get("readiness", {}) if isinstance(catalog_entry.get("readiness"), Mapping) else {}).get("symbol_live")
    )
    return {
        "catalog_discovered": catalog_discovered,
        "snapshot_ready": snapshot_ready,
        "symbol_live": symbol_live,
        "run_live": bool(run_live),
    }


def symbol_detail_response_contract(
    *,
    run_id: str,
    symbol_state: SymbolProjectionSnapshot,
    run_health: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    return {
        "contract": "botlens_symbol_detail",
        "schema_version": SCHEMA_VERSION,
        "scope": {
            "run_id": str(run_id),
            "symbol_key": symbol_state.symbol_key,
        },
        "detail_seq": int(symbol_state.seq),
        "detail": _symbol_detail_payload(symbol_state, run_health=run_health),
    }


def symbol_catalog_response_contract(*, run_id: str, symbol_catalog: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "contract": "botlens_symbol_catalog",
        "schema_version": SCHEMA_VERSION,
        "run_id": str(run_id),
        "symbols": [
            {
                "symbol_key": normalize_series_key(value.get("symbol_key")),
                "symbol": str(value.get("symbol") or "").strip().upper() or None,
                "timeframe": str(value.get("timeframe") or "").strip().lower() or None,
                "display_label": display_label(
                    symbol=str(value.get("symbol") or "").strip().upper(),
                    timeframe=str(value.get("timeframe") or "").strip().lower(),
                    symbol_key=normalize_series_key(value.get("symbol_key")),
                ),
            }
            for _, value in sorted(
                (
                    (key, dict(value))
                    for key, value in symbol_catalog.items()
                    if normalize_series_key(key) and isinstance(value, Mapping)
                ),
                key=lambda item: (
                    str(item[1].get("symbol") or ""),
                    str(item[1].get("timeframe") or ""),
                    item[0],
                ),
            )
        ],
    }


def run_bootstrap_contract(
    *,
    bot_id: str,
    run_id: str | None,
    run_meta: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    health: Mapping[str, Any] | None,
    symbol_catalog: Mapping[str, Any],
    open_trades: Mapping[str, Any],
    selected_symbol_key: str | None,
    state: str,
    run_live: bool,
    transport_eligible: bool,
    message: str,
    bootstrap_seq: int,
    base_seq: int,
    stream_session_id: str | None,
    selected_symbol_state: SymbolProjectionSnapshot | None = None,
) -> Dict[str, Any]:
    normalized_selected_symbol_key = normalize_series_key(selected_symbol_key) or None
    selected_catalog_entry = (
        dict(symbol_catalog.get(normalized_selected_symbol_key) or {})
        if normalized_selected_symbol_key and isinstance(symbol_catalog.get(normalized_selected_symbol_key), Mapping)
        else None
    )
    readiness = _selected_symbol_readiness_payload(
        symbol_key=normalized_selected_symbol_key,
        symbol_state=selected_symbol_state,
        symbol_catalog_entry=selected_catalog_entry,
        run_live=run_live,
    )
    embedded_selected_symbol = (
        selected_symbol_state is not None and selected_symbol_state.readiness.snapshot_ready
    )
    open_trade_by_symbol: Dict[str, list[Mapping[str, Any]]] = {}
    for trade in open_trades.values():
        if not isinstance(trade, Mapping):
            continue
        symbol_key = normalize_series_key(trade.get("symbol_key"))
        if not symbol_key:
            continue
        open_trade_by_symbol.setdefault(symbol_key, []).append(trade)
    summaries = []
    for _, entry in sorted(
        (
            (key, value)
            for key, value in symbol_catalog.items()
            if normalize_series_key(key) and isinstance(value, Mapping)
        ),
        key=lambda item: (
            str(item[1].get("symbol") or ""),
            str(item[1].get("timeframe") or ""),
            item[0],
        ),
    ):
        summaries.append(
            _run_catalog_entry_payload(
                catalog_entry=entry,
                health_state=health,
                open_trades=open_trade_by_symbol.get(normalize_series_key(entry.get("symbol_key")), []),
            )
        )
    return {
        "contract": "botlens_run_bootstrap",
        "schema_version": SCHEMA_VERSION,
        "scope": {
            "bot_id": str(bot_id),
            "run_id": str(run_id).strip() or None,
        },
        "state": str(state),
        "contract_state": "bootstrap_ready" if str(state) == "ready" else str(state),
        "message": str(message),
        "readiness": readiness,
        "bootstrap": {
            "scope": "run",
            "ready": str(state) == "ready",
            "bootstrap_seq": int(bootstrap_seq),
            "base_seq": int(base_seq),
            "selected_symbol_snapshot_required": bool(
                normalized_selected_symbol_key and not embedded_selected_symbol
            ),
        },
        "run": {
            "meta": dict(run_meta or {}) if isinstance(run_meta, Mapping) else None,
            "lifecycle": dict(lifecycle or {}) if isinstance(lifecycle, Mapping) else {},
            "health": _runtime_payload(health),
            "open_trades": [dict(value) for _, value in sorted(open_trades.items()) if isinstance(value, Mapping)],
            "readiness": {
                "catalog_discovered": bool(symbol_catalog),
                "run_live": bool(run_live),
            },
        },
        "selected_symbol": (
            {
                "metadata": {
                    **_symbol_identity_payload(selected_symbol_state),
                    "status": str((health or {}).get("status") or "waiting").strip() or "waiting",
                    "last_event_at": selected_symbol_state.last_event_at,
                    "seq": int(selected_symbol_state.seq),
                    "readiness": readiness,
                },
                "current": _symbol_current_payload(selected_symbol_state, run_health=health),
            }
            if embedded_selected_symbol
            else None
        ),
        "navigation": {
            "selected_symbol_key": normalized_selected_symbol_key,
            "symbols": summaries,
        },
        "live_transport": {
            "eligible": bool(transport_eligible) and str(state) == "ready",
            "channel": "botlens_run_live",
            "subscription_scope": "run",
            "selection_mode": "set_selected_symbol",
            "stream_session_id": str(stream_session_id or "").strip() or None,
            "resume_mode": "resume_from_seq",
            "requires": ["run_bootstrap"],
        },
    }


def selected_symbol_snapshot_contract(
    *,
    bot_id: str,
    run_id: str,
    symbol_key: str,
    symbol_state: SymbolProjectionSnapshot | None,
    symbol_catalog_entry: Mapping[str, Any] | None,
    run_health: Mapping[str, Any] | None,
    run_bootstrap_seq: int,
    base_seq: int,
    stream_session_id: str | None,
    run_live: bool,
    transport_eligible: bool,
    state: str = "ready",
    message: str,
    unavailable_reason: str | None = None,
) -> Dict[str, Any]:
    readiness = _selected_symbol_readiness_payload(
        symbol_key=symbol_key,
        symbol_state=symbol_state,
        symbol_catalog_entry=symbol_catalog_entry,
        run_live=run_live,
    )
    if symbol_state is not None and symbol_state.readiness.snapshot_ready:
        identity = _symbol_identity_payload(symbol_state)
        seq = int(symbol_state.seq)
        selected_symbol_payload: Dict[str, Any] | None = {
            "metadata": {
                **identity,
                "status": str((run_health or {}).get("status") or "waiting").strip() or "waiting",
                "last_event_at": symbol_state.last_event_at,
                "seq": seq,
                "readiness": readiness,
            },
            "current": _symbol_current_payload(symbol_state, run_health=run_health),
        }
    else:
        identity = _symbol_identity_from_catalog_entry(symbol_key=symbol_key, catalog_entry=symbol_catalog_entry)
        seq = 0
        selected_symbol_payload = None

    payload = {
        "contract": "botlens_selected_symbol_snapshot",
        "schema_version": SCHEMA_VERSION,
        "scope": {
            "bot_id": str(bot_id),
            "run_id": str(run_id),
            "symbol_key": normalize_series_key(symbol_key) or identity.get("symbol_key"),
        },
        "state": str(state),
        "contract_state": (
            "snapshot_ready"
            if str(state) == "ready"
            else "snapshot_unavailable"
            if str(state) == "unavailable"
            else str(state)
        ),
        "message": str(message),
        "readiness": readiness,
        "bootstrap": {
            "scope": "selected_symbol_snapshot",
            "ready": str(state) == "ready" and selected_symbol_payload is not None,
            "bootstrap_seq": seq,
            "run_bootstrap_seq": int(run_bootstrap_seq),
            "base_seq": int(base_seq),
        },
        "selection": {
            "selected_symbol_key": normalize_series_key(symbol_key) or identity.get("symbol_key"),
            "display_label": identity.get("display_label"),
        },
        "selected_symbol": selected_symbol_payload,
        "live_transport": {
            "eligible": bool(transport_eligible),
            "channel": "botlens_run_live",
            "subscription_scope": "run",
            "selection_mode": "set_selected_symbol",
            "selected_symbol_key": normalize_series_key(symbol_key) or identity.get("symbol_key"),
            "stream_session_id": str(stream_session_id or "").strip() or None,
            "resume_mode": "resume_from_seq",
        },
    }
    if unavailable_reason:
        payload["unavailable_reason"] = str(unavailable_reason)
    return payload


def selected_symbol_visual_contract(**kwargs: Any) -> Dict[str, Any]:
    return selected_symbol_snapshot_contract(**kwargs)


def stream_connected_message(*, run_id: str, stream_session_id: str, replayed_count: int = 0) -> Dict[str, Any]:
    return {
        "type": STREAM_CONNECTED_TYPE,
        "schema_version": SCHEMA_VERSION,
        "run_id": str(run_id),
        "scope": "run",
        "concern": "connection",
        "stream_session_id": str(stream_session_id),
        "stream_seq": 0,
        "replayed_count": max(int(replayed_count or 0), 0),
        "resume_mode": "resume_from_seq",
    }


def stream_reset_required_message(
    *,
    run_id: str,
    stream_session_id: str,
    reason: str,
    requested_stream_session_id: str | None = None,
    requested_resume_from_seq: int = 0,
    current_stream_seq: int = 0,
) -> Dict[str, Any]:
    return {
        "type": STREAM_RESET_REQUIRED_TYPE,
        "schema_version": SCHEMA_VERSION,
        "run_id": str(run_id),
        "scope": "run",
        "concern": "connection",
        "stream_session_id": str(stream_session_id),
        "stream_seq": 0,
        "reason": str(reason),
        "requested_stream_session_id": str(requested_stream_session_id or "").strip() or None,
        "requested_resume_from_seq": max(int(requested_resume_from_seq or 0), 0),
        "current_stream_seq": max(int(current_stream_seq or 0), 0),
    }


@dataclass(frozen=True)
class LiveDeltaEvent:
    message_type: str
    scope: str
    concern: str
    run_id: str
    scope_seq: int
    event_time: Any
    payload: Dict[str, Any]
    symbol_key: str | None = None

    def to_message(self, *, stream_session_id: str, stream_seq: int) -> Dict[str, Any]:
        message = {
            "type": self.message_type,
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "scope": self.scope,
            "concern": self.concern,
            "scope_seq": int(self.scope_seq),
            "stream_session_id": str(stream_session_id),
            "stream_seq": int(stream_seq),
            "event_time": self.event_time,
            "payload": dict(self.payload),
        }
        if self.symbol_key:
            message["symbol_key"] = self.symbol_key
        return message


@dataclass(frozen=True)
class PreparedLiveDelta:
    event: LiveDeltaEvent
    payload_bytes: int
    build_ms: float


@dataclass(frozen=True)
class LiveDeliveryStats:
    emit_ms: float
    viewer_count: int
    filtered_viewer_count: int
    stale_viewer_count: int


class BotLensTransport:
    @staticmethod
    def _build_prepared(
        *,
        message_type: str,
        scope: str,
        concern: str,
        run_id: str,
        scope_seq: int,
        event_time: Any,
        payload: Mapping[str, Any],
        symbol_key: str | None = None,
    ) -> PreparedLiveDelta:
        started = time.perf_counter()
        event = LiveDeltaEvent(
            message_type=str(message_type),
            scope=str(scope),
            concern=str(concern),
            run_id=str(run_id),
            symbol_key=normalize_series_key(symbol_key) or None,
            scope_seq=int(scope_seq),
            event_time=event_time,
            payload=dict(payload or {}),
        )
        return PreparedLiveDelta(
            event=event,
            payload_bytes=_json_size_bytes(event.payload),
            build_ms=max((time.perf_counter() - started) * 1000.0, 0.0),
        )

    def build_symbol_prepared_deltas(
        self,
        *,
        run_id: str,
        deltas: Iterable[SymbolConcernDelta],
    ) -> tuple[PreparedLiveDelta, ...]:
        prepared: list[PreparedLiveDelta] = []
        for delta in deltas:
            if isinstance(delta, CandleDelta):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_CANDLE_DELTA_TYPE,
                        scope="symbol",
                        concern="candles",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"candle": dict(delta.candle)},
                    )
                )
            elif isinstance(delta, OverlayDelta):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_OVERLAY_DELTA_TYPE,
                        scope="symbol",
                        concern="overlays",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"ops": [dict(entry) for entry in delta.overlay_ops.get("ops", []) if isinstance(entry, Mapping)]},
                    )
                )
            elif isinstance(delta, SignalDelta) and delta.appended_signals:
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_SIGNAL_DELTA_TYPE,
                        scope="symbol",
                        concern="signals",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"entries": [dict(entry) for entry in delta.appended_signals]},
                    )
                )
            elif isinstance(delta, DecisionDelta) and delta.appended_decisions:
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_DECISION_DELTA_TYPE,
                        scope="symbol",
                        concern="decisions",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"entries": [dict(entry) for entry in delta.appended_decisions]},
                    )
                )
            elif isinstance(delta, DiagnosticDelta) and delta.appended_diagnostics:
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_DIAGNOSTIC_DELTA_TYPE,
                        scope="symbol",
                        concern="diagnostics",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"entries": [dict(entry) for entry in delta.appended_diagnostics]},
                    )
                )
            elif isinstance(delta, TradeDelta) and (delta.trade_upserts or delta.trade_removals):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_TRADE_DELTA_TYPE,
                        scope="symbol",
                        concern="trades",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={
                            "upserts": [dict(entry) for entry in delta.trade_upserts],
                            "removals": [str(entry) for entry in delta.trade_removals if str(entry).strip()],
                        },
                    )
                )
            elif isinstance(delta, SeriesStatsDelta):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_SYMBOL_STATS_DELTA_TYPE,
                        scope="symbol",
                        concern="stats",
                        run_id=run_id,
                        symbol_key=delta.symbol_key,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"stats": dict(delta.stats)},
                    )
                )
        return tuple(prepared)

    def build_run_prepared_deltas(
        self,
        *,
        state: RunProjectionSnapshot,
        deltas: Iterable[RunConcernDelta],
    ) -> tuple[PreparedLiveDelta, ...]:
        prepared: list[PreparedLiveDelta] = []
        open_trade_by_symbol: Dict[str, list[Mapping[str, Any]]] = {}
        for trade in state.open_trades.entries.values():
            if not isinstance(trade, Mapping):
                continue
            symbol_key = normalize_series_key(trade.get("symbol_key"))
            if not symbol_key:
                continue
            open_trade_by_symbol.setdefault(symbol_key, []).append(trade)

        for delta in deltas:
            if isinstance(delta, RunLifecycleDelta):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_RUN_LIFECYCLE_DELTA_TYPE,
                        scope="run",
                        concern="lifecycle",
                        run_id=state.run_id,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"lifecycle": dict(delta.lifecycle)},
                    )
                )
            elif isinstance(delta, RunHealthDelta):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_RUN_HEALTH_DELTA_TYPE,
                        scope="run",
                        concern="health",
                        run_id=state.run_id,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"health": _runtime_payload(delta.health)},
                    )
                )
            elif isinstance(delta, RunFaultDelta) and delta.appended_faults:
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_RUN_FAULT_DELTA_TYPE,
                        scope="run",
                        concern="faults",
                        run_id=state.run_id,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={"entries": [dict(entry) for entry in delta.appended_faults]},
                    )
                )
            elif isinstance(delta, RunSymbolCatalogDelta) and (delta.symbol_upserts or delta.symbol_removals):
                summaries = []
                for entry in delta.symbol_upserts:
                    symbol_key = normalize_series_key(entry.get("symbol_key"))
                    if not symbol_key:
                        continue
                    summaries.append(
                        _live_symbol_summary_payload(
                            catalog_entry=state.symbol_catalog.entries.get(symbol_key, entry),
                            health_state=state.health.to_dict(),
                            open_trades=open_trade_by_symbol.get(symbol_key, ()),
                        )
                    )
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_RUN_SYMBOL_CATALOG_DELTA_TYPE,
                        scope="run",
                        concern="symbol_catalog",
                        run_id=state.run_id,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={
                            "upserts": summaries,
                            "removals": [str(entry) for entry in delta.symbol_removals if str(entry).strip()],
                        },
                    )
                )
            elif isinstance(delta, RunOpenTradesDelta) and (delta.upserts or delta.removals):
                prepared.append(
                    self._build_prepared(
                        message_type=STREAM_RUN_OPEN_TRADES_DELTA_TYPE,
                        scope="run",
                        concern="open_trades",
                        run_id=state.run_id,
                        scope_seq=delta.seq,
                        event_time=delta.event_time,
                        payload={
                            "upserts": [dict(entry) for entry in delta.upserts],
                            "removals": [str(entry) for entry in delta.removals if str(entry).strip()],
                        },
                    )
                )
        return tuple(prepared)


class LiveDeltaInstrumentation:
    @staticmethod
    def emission_summary(
        prepared_deltas: Iterable[PreparedLiveDelta],
        deliveries: Iterable[LiveDeliveryStats] | None = None,
    ) -> Dict[str, Any]:
        delivery_entries = list(deliveries or [])
        summary_events = []
        counts_by_type: Dict[str, int] = {}
        total_payload_bytes = 0
        total_build_ms = 0.0
        total_emit_ms = 0.0
        total_filtered = 0
        total_stale = 0
        max_viewers = 0
        for index, prepared in enumerate(prepared_deltas):
            delivery = delivery_entries[index] if index < len(delivery_entries) else None
            event = prepared.event
            counts_by_type[event.message_type] = counts_by_type.get(event.message_type, 0) + 1
            total_payload_bytes += int(prepared.payload_bytes)
            total_build_ms += float(prepared.build_ms)
            emit_ms = float(delivery.emit_ms) if delivery is not None else 0.0
            viewer_count = int(delivery.viewer_count) if delivery is not None else 0
            filtered_count = int(delivery.filtered_viewer_count) if delivery is not None else 0
            stale_count = int(delivery.stale_viewer_count) if delivery is not None else 0
            total_emit_ms += emit_ms
            total_filtered += filtered_count
            total_stale += stale_count
            max_viewers = max(max_viewers, viewer_count)
            summary_events.append(
                {
                    "type": event.message_type,
                    "scope": event.scope,
                    "concern": event.concern,
                    "symbol_key": event.symbol_key,
                    "scope_seq": int(event.scope_seq),
                    "payload_bytes": int(prepared.payload_bytes),
                    "build_ms": round(float(prepared.build_ms), 6),
                    "emit_ms": round(emit_ms, 6),
                    "viewer_count": viewer_count,
                    "filtered_viewer_count": filtered_count,
                    "stale_viewer_count": stale_count,
                }
            )
        return {
            "event_count": len(summary_events),
            "counts_by_type": counts_by_type,
            "events": summary_events,
            "total_payload_bytes": total_payload_bytes,
            "total_build_ms": round(total_build_ms, 6),
            "total_emit_ms": round(total_emit_ms, 6),
            "filtered_viewer_count": total_filtered,
            "stale_viewer_count": total_stale,
            "max_viewer_count": max_viewers,
        }


__all__ = [
    "BotLensTransport",
    "LiveDeliveryStats",
    "LiveDeltaEvent",
    "LiveDeltaInstrumentation",
    "PreparedLiveDelta",
    "run_bootstrap_contract",
    "selected_symbol_snapshot_contract",
    "selected_symbol_visual_contract",
    "stream_connected_message",
    "stream_reset_required_message",
    "symbol_catalog_response_contract",
    "symbol_detail_response_contract",
]

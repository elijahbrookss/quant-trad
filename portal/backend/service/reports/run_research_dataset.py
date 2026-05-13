"""Canonical run research dataset builder.

The dataset is rebuilt from durable run/trade/runtime-event read models. Export
bundles are generated from this contract rather than treated as report truth.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import math
import re
import statistics
from typing import Any, Dict, List, Optional

from utils.log_context import build_log_context, with_log_context

from ..storage import storage
from . import report_data
from .metrics import compute_expectancy, compute_max_drawdown, compute_profit_factor
from .summary_metrics import ANNUALIZATION_PERIODS, compute_summary as compute_portfolio_metric_summary


logger = logging.getLogger(__name__)

DATASET_SCHEMA_VERSION = "run_research_dataset.v1"
_EPSILON = 1e-6
_WALLET_EVENT_NAMES = {
    "wallet_initialized",
    "wallet_deposited",
    "entry_filled",
    "exit_filled",
    "margin_reserved",
    "margin_rejected",
    "margin_released",
    "fee_applied",
    "realized_pnl_applied",
    "position_opened",
    "position_closed",
    "equity_updated",
}
_POSITION_TRADE_EVENT_NAMES = {"trade_opened", "trade_updated", "trade_closed"}
_CANDLE_GAP_BLOCKING_TYPES = frozenset(
    {
        "unknown_gap",
        "runtime_missing",
        "projection_missing",
        "ingestion_failure",
    }
)
_CANDLE_GAP_PROVIDER_TYPES = frozenset({"provider_missing_data"})
_CANDLE_GAP_EXPECTED_TYPES = frozenset({"expected_session_gap"})


@dataclass(frozen=True)
class RunResearchMetadata:
    run_id: str
    bot_id: Optional[str]
    strategy_id: Optional[str]
    strategy_name: Optional[str]
    strategy_hash: Optional[str]
    run_type: Optional[str]
    status: Optional[str]
    started_at: Optional[str]
    ended_at: Optional[str]
    completed_at: Optional[str]
    symbols: List[str]
    instrument_ids: List[str]
    timeframe: Optional[str]
    timeframes: List[str]
    datasource: Optional[str]
    provider: Optional[str]
    exchange: Optional[str]
    simulated_window: Dict[str, Optional[str]]
    wall_clock_window: Dict[str, Optional[str]]
    execution_mode: str
    playback_mode: Optional[str]
    starting_capital: Optional[float]
    config_hash: Optional[str]
    material_config_hash: Optional[str]
    data_snapshot_hash: Optional[str]
    report_material_fingerprint: Optional[str]
    dataset_schema_version: str
    generated_at: str


@dataclass(frozen=True)
class RunResearchReadiness:
    dataset_ready: bool
    results_ready: bool
    safe_to_compare: bool
    reason: str
    conditions: Dict[str, bool]
    export_status: str
    dataset_status: str
    caveats: List[str] = field(default_factory=list)
    results_status: str = "blocked"
    comparison_status: str = "blocked"
    data_quality_status: str = "unknown"
    execution_quality_status: str = "unknown"
    blocking_reasons: List[str] = field(default_factory=list)
    degraded_sections: List[str] = field(default_factory=list)
    unavailable_sections: List[str] = field(default_factory=list)
    golden_candidate_status: str = "unknown"
    golden_blocking_reasons: List[str] = field(default_factory=list)
    repeatability_status: str = "unknown"
    material_fingerprint: Optional[str] = None


@dataclass(frozen=True)
class RunResearchSummary:
    total_decisions: int
    accepted_decisions: int
    rejected_decisions: int
    trades: int
    closed_trades: int
    open_trades: int
    wins: int
    losses: int
    win_rate: Optional[float]
    gross_pnl: float
    fees: float
    net_pnl: float
    equity_start: Optional[float]
    equity_end: Optional[float]
    return_pct: Optional[float]
    max_drawdown: Optional[float]
    max_drawdown_pct: Optional[float]
    profit_factor: Optional[float]
    expectancy: Optional[float]
    avg_win: Optional[float]
    avg_loss: Optional[float]
    largest_win: Optional[float]
    largest_loss: Optional[float]
    average_holding_seconds: Optional[float]
    initial_capital: Optional[float] = None
    final_equity: Optional[float] = None
    loss_rate: Optional[float] = None
    drawdown_duration_seconds: Optional[float] = None
    exposure_pct: Optional[float] = None
    cagr: Optional[float] = None
    annualized_volatility: Optional[float] = None
    sharpe: Optional[float] = None
    sortino: Optional[float] = None
    calmar: Optional[float] = None
    unavailable_metrics: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ReportDiagnostic:
    severity: str
    source: str
    code: str
    message: str
    affected_identity: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[str] = None
    known_at: Optional[str] = None
    readiness_impact: str = "none"
    suggested_next_step: Optional[str] = None


@dataclass(frozen=True)
class RunResearchDataset:
    schema_version: str
    metadata: RunResearchMetadata
    readiness: RunResearchReadiness
    summary: RunResearchSummary
    sections: Dict[str, Any]
    timeseries: Dict[str, Any]
    diagnostics: Dict[str, Any]
    decisions: List[Dict[str, Any]]
    signals: List[Dict[str, Any]]
    trades: List[Dict[str, Any]]
    context: Dict[str, Any]
    candle_catalog: Dict[str, Any]
    fee_accounting: Dict[str, Any]
    wallet_accounting: Dict[str, Any]
    wallet_diagnostics: Dict[str, Any]
    execution: Dict[str, Any]
    candle_gaps: Dict[str, Any]
    portfolio_metrics: Dict[str, Any]
    performance: Dict[str, Any]
    operational_health: Dict[str, Any]
    strategy_insights: Dict[str, Any]
    narrative_summary: str

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else None


def _duration_seconds(start: Any, end: Any) -> Optional[float]:
    started = _parse_iso(start)
    ended = _parse_iso(end)
    if not started or not ended:
        return None
    return max((ended - started).total_seconds(), 0.0)


def _payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    return _mapping(row.get("payload"))


def _context(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload = _payload(row)
    return _mapping(payload.get("context"))


def _event_name(row: Mapping[str, Any]) -> str:
    payload = _payload(row)
    return str(payload.get("event_name") or row.get("event_name") or "").strip()


def _event_name_key(row: Mapping[str, Any]) -> str:
    return _event_name(row).strip().lower()


def _config_hash(config: Mapping[str, Any]) -> Optional[str]:
    if not config:
        return None
    encoded = json.dumps(_json_safe(dict(config)), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


_NON_MATERIAL_CONFIG_KEYS = {
    "generated_at",
    "report_generated_at",
    "report_warnings",
    "request_id",
    "runtime_warnings",
    "updated_at",
    "warnings",
}


def _material_config_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _material_config_payload(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
            if str(key) not in _NON_MATERIAL_CONFIG_KEYS
        }
    if isinstance(value, list):
        return [_material_config_payload(item) for item in value]
    return _json_safe(value)


def _material_config_hash(config: Mapping[str, Any]) -> Optional[str]:
    explicit = str(
        config.get("material_config_hash")
        or config.get("strategy_material_config_hash")
        or ""
    ).strip()
    if explicit:
        return explicit
    payload = _material_config_payload(config)
    if not payload:
        return None
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _execution_mode(run: Mapping[str, Any]) -> str:
    config = _mapping(run.get("config_snapshot"))
    bot = _mapping(config.get("bot"))
    risk = _mapping(config.get("risk_settings")) or _mapping(bot.get("risk")) or _mapping(config.get("risk"))
    normalized = str(
        run.get("execution_mode")
        or config.get("execution_mode")
        or bot.get("execution_mode")
        or risk.get("execution_mode")
        or "fast"
    ).strip().lower()
    return normalized if normalized in {"fast", "full"} else "fast"


def _playback_mode(run: Mapping[str, Any]) -> Optional[str]:
    config = _mapping(run.get("config_snapshot"))
    runtime_metadata = _mapping(config.get("runtime_metadata"))
    value = (
        run.get("playback_mode")
        or runtime_metadata.get("playback_mode")
        or config.get("playback_mode")
        or config.get("mode")
    )
    text = str(value or "").strip()
    return text or None


def _starting_capital(config: Mapping[str, Any]) -> Optional[float]:
    wallet = _mapping(config.get("wallet_start")) or _mapping(config.get("wallet_config"))
    balances = _mapping(wallet.get("balances"))
    if not balances:
        return None
    values = [_safe_float(value) for value in balances.values()]
    clean = [float(value) for value in values if value is not None]
    return sum(clean) if clean else None


def _clean_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _unique_text(values: Sequence[Any]) -> List[str]:
    seen: set[str] = set()
    rows: List[str] = []
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows


def _first_trace_value(rows: Sequence[Mapping[str, Any]], key: str) -> Optional[str]:
    for row in rows:
        text = _clean_text(row.get(key))
        if text:
            return text
    return None


def _strategy_hash(run: Mapping[str, Any]) -> Optional[str]:
    config = _mapping(run.get("config_snapshot"))
    strategy = next((item for item in config.get("strategies") or [] if isinstance(item, Mapping)), {})
    for value in (
        run.get("strategy_hash"),
        config.get("strategy_hash"),
        config.get("material_config_hash"),
        config.get("strategy_material_config_hash"),
        strategy.get("strategy_hash") if isinstance(strategy, Mapping) else None,
        strategy.get("hash") if isinstance(strategy, Mapping) else None,
    ):
        text = _clean_text(value)
        if text:
            return text
    return None


def _metadata_symbols(run: Mapping[str, Any]) -> List[str]:
    config = _mapping(run.get("config_snapshot"))
    symbols = list(run.get("symbols") or []) + list(config.get("symbols") or [])
    for strategy in config.get("strategies") or []:
        if not isinstance(strategy, Mapping):
            continue
        for instrument in strategy.get("instruments") or []:
            if isinstance(instrument, Mapping):
                symbols.append(instrument.get("symbol"))
    return _unique_text(symbols)


def _metadata_instrument_ids(run: Mapping[str, Any]) -> List[str]:
    config = _mapping(run.get("config_snapshot"))
    values: List[Any] = []
    for strategy in config.get("strategies") or []:
        if not isinstance(strategy, Mapping):
            continue
        for instrument in strategy.get("instruments") or []:
            if not isinstance(instrument, Mapping):
                continue
            values.extend(
                [
                    instrument.get("instrument_id"),
                    instrument.get("id"),
                ]
            )
    return _unique_text(values)


def _metadata_timeframes(run: Mapping[str, Any]) -> List[str]:
    config = _mapping(run.get("config_snapshot"))
    values: List[Any] = [run.get("timeframe"), config.get("timeframe")]
    for strategy in config.get("strategies") or []:
        if isinstance(strategy, Mapping):
            values.append(strategy.get("timeframe"))
    return _unique_text(values)


def _trade_metrics(trade: Mapping[str, Any]) -> Dict[str, Any]:
    return _mapping(trade.get("metrics"))


def _trade_id(trade: Mapping[str, Any]) -> str:
    return str(trade.get("trade_id") or trade.get("id") or "").strip()


def _trade_close_reason(trade: Mapping[str, Any], trade_events_by_id: Mapping[str, Mapping[str, Any]]) -> Optional[str]:
    metrics = _trade_metrics(trade)
    event_context = _mapping(trade_events_by_id.get(_trade_id(trade)))
    value = (
        metrics.get("close_reason")
        or metrics.get("reason_code")
        or metrics.get("exit_reason")
        or event_context.get("close_reason")
        or event_context.get("reason_code")
    )
    text = str(value or "").strip().upper()
    return text or None


def _trade_legs(trade: Mapping[str, Any], trade_events_by_id: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    metrics = _trade_metrics(trade)
    event_context = _mapping(trade_events_by_id.get(_trade_id(trade)))
    legs = metrics.get("legs") or event_context.get("legs") or []
    return [dict(leg) for leg in legs if isinstance(leg, Mapping)]


def _quantity(trade: Mapping[str, Any], event_context: Mapping[str, Any]) -> Optional[float]:
    for value in (
        trade.get("quantity"),
        trade.get("qty"),
        trade.get("contracts"),
        event_context.get("quantity"),
        event_context.get("qty"),
    ):
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None


def _trade_closed_context_by_id(events: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_id: Dict[str, tuple[int, int, Dict[str, Any]]] = {}
    for row in events:
        name = _event_name_key(row)
        if name != "trade_closed":
            continue
        context = _context(row)
        trade_id = str(context.get("trade_id") or row.get("trade_id") or "").strip()
        if trade_id:
            position_seq = _position_commit_seq_value(row) or 0
            run_seq = _event_run_seq_value(row) or 0
            selected = context | {
                "event_id": row.get("event_id") or _payload(row).get("event_id"),
                "position_commit_seq": position_seq or context.get("position_commit_seq"),
                "position_commit_seq_status": row.get("position_commit_seq_status") or context.get("position_commit_seq_status"),
                "run_seq": run_seq or context.get("run_seq"),
            }
            previous = by_id.get(trade_id)
            if previous is None or (position_seq, run_seq) > (previous[0], previous[1]):
                by_id[trade_id] = (position_seq, run_seq, selected)
    return {trade_id: context for trade_id, (_position_seq, _run_seq, context) in by_id.items()}


def _decision_row(entry: Mapping[str, Any]) -> Dict[str, Any]:
    context = _mapping(entry.get("context"))
    decision_state = str(entry.get("decision_state") or "").strip().lower()
    accepted = decision_state == "accepted" or "accepted" in str(entry.get("event_subtype") or "").lower()
    trade_id = str(entry.get("trade_id") or context.get("trade_id") or "").strip() or None
    signal_price = entry.get("price") or context.get("signal_price") or context.get("price")
    quantity = context.get("quantity") or context.get("qty")
    action = context.get("action") or entry.get("intent") or context.get("intent") or context.get("direction") or entry.get("side")
    status = "accepted" if accepted else "rejected"
    reason = entry.get("reason_code") or context.get("reason_code") or context.get("message")
    decision_id = entry.get("decision_id") or context.get("decision_id") or entry.get("event_id")
    signal_id = entry.get("signal_id") or context.get("signal_id")
    return {
        "decision_id": decision_id,
        "seq": _safe_int(entry.get("seq")),
        "run_seq": _safe_int(entry.get("run_seq")),
        "run_seq_status": entry.get("run_seq_status") or context.get("run_seq_status"),
        "signal_id": signal_id,
        "run_id": entry.get("run_id") or context.get("run_id"),
        "bot_id": entry.get("bot_id") or context.get("bot_id"),
        "rule_id": entry.get("rule_id") or context.get("rule_id"),
        "rule_name": entry.get("rule_name") or context.get("rule_name"),
        "strategy_id": entry.get("strategy_id") or context.get("strategy_id"),
        "strategy_hash": entry.get("strategy_hash") or context.get("strategy_hash"),
        "instrument_id": entry.get("instrument_id") or context.get("instrument_id"),
        "series_key": entry.get("series_key") or context.get("series_key"),
        "symbol": entry.get("symbol") or context.get("symbol"),
        "timeframe": entry.get("timeframe") or context.get("timeframe"),
        "bar_time": entry.get("bar_time") or context.get("bar_time") or entry.get("event_ts"),
        "known_at": entry.get("known_at") or context.get("known_at") or entry.get("created_at") or entry.get("event_ts"),
        "action": action,
        "status": status,
        "verdict": status,
        "accepted": accepted,
        "rejected": not accepted,
        "reason": reason,
        "reason_code": reason,
        "rejection_reason": reason if not accepted else None,
        "skipped_reason": reason if status == "skipped" else None,
        "trade_id": trade_id if accepted else None,
        "entry_request_id": context.get("entry_request_id"),
        "attempt_id": context.get("attempt_id") or (None if accepted else context.get("source_id")),
        "selected_quantity": quantity,
        "selected_price": signal_price,
        "quantity": quantity,
        "price": signal_price,
        "decision_context": context,
        "artifact_summary": {
            "source_type": entry.get("source_type") or context.get("source_type"),
            "source_id": entry.get("source_id") or context.get("source_id"),
            "intent": entry.get("intent") or context.get("intent"),
            "side": entry.get("side") or context.get("side") or context.get("direction"),
        },
        "source_refs": [
            {"section": "runtime_events", "event_id": entry.get("event_id")}
        ] if entry.get("event_id") else [],
    }


def _signal_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    context = _context(row)
    payload = _payload(row)
    signal_id = row.get("signal_id") or context.get("signal_id")
    decision_id = row.get("decision_id") or context.get("decision_id")
    return {
        "event_id": row.get("event_id") or payload.get("event_id"),
        "seq": _safe_int(row.get("seq")),
        "run_seq": _safe_int(row.get("run_seq") or context.get("run_seq")),
        "run_seq_status": row.get("run_seq_status") or context.get("run_seq_status"),
        "signal_id": signal_id,
        "decision_id": decision_id,
        "trade_id": row.get("trade_id") or context.get("trade_id"),
        "run_id": row.get("run_id") or context.get("run_id"),
        "bot_id": row.get("bot_id") or context.get("bot_id"),
        "strategy_id": context.get("strategy_id"),
        "strategy_hash": context.get("strategy_hash"),
        "rule_id": context.get("rule_id"),
        "rule_name": context.get("rule_name"),
        "instrument_id": row.get("instrument_id") or context.get("instrument_id"),
        "series_key": row.get("series_key") or context.get("series_key"),
        "symbol": row.get("symbol") or context.get("symbol"),
        "timeframe": row.get("timeframe") or context.get("timeframe"),
        "bar_time": row.get("bar_time") or context.get("bar_time"),
        "known_at": row.get("known_at") or payload.get("known_at") or payload.get("event_ts"),
        "signal_type": context.get("signal_type") or context.get("action"),
        "action": context.get("intent") or context.get("action") or context.get("signal_type"),
        "direction": context.get("direction") or context.get("side"),
        "price": context.get("signal_price") or context.get("price"),
        "quantity": context.get("quantity") or context.get("qty"),
        "reason_code": row.get("reason_code") or context.get("reason_code"),
        "context": context,
        "source_refs": [
            {"section": "runtime_events", "event_id": row.get("event_id") or payload.get("event_id")}
        ] if (row.get("event_id") or payload.get("event_id")) else [],
    }


def _signal_rows(events: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows = [_signal_row(row) for row in events if _event_name_key(row) == "signal_emitted"]
    return sorted(
        rows,
        key=lambda item: (
            int(item.get("run_seq") or 0),
            str(item.get("bar_time") or item.get("known_at") or ""),
            int(item.get("seq") or 0),
            str(item.get("signal_id") or ""),
        ),
    )


def _normalize_trades(
    trades: Sequence[Mapping[str, Any]],
    *,
    trade_closed_context_by_id: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for trade in trades:
        trade_id = _trade_id(trade)
        event_context = _mapping(trade_closed_context_by_id.get(trade_id))
        entry_time = trade.get("entry_time") or event_context.get("opened_at")
        exit_time = trade.get("exit_time") or event_context.get("exit_time") or event_context.get("closed_at")
        holding_seconds = _duration_seconds(entry_time, exit_time)
        close_reason = _trade_close_reason(trade, trade_closed_context_by_id)
        legs = _trade_legs(trade, trade_closed_context_by_id)
        terminal = close_reason == "BACKTEST_END" or any(
            str(leg.get("status") or "").strip().lower() == "backtest_end" for leg in legs
        )
        normalized.append(
            {
                "trade_id": trade_id,
                "run_id": trade.get("run_id") or event_context.get("run_id"),
                "bot_id": trade.get("bot_id") or event_context.get("bot_id"),
                "strategy_id": trade.get("strategy_id") or event_context.get("strategy_id"),
                "strategy_hash": trade.get("strategy_hash") or event_context.get("strategy_hash"),
                "instrument_id": trade.get("instrument_id") or event_context.get("instrument_id"),
                "series_key": trade.get("series_key") or event_context.get("series_key"),
                "symbol": trade.get("symbol") or event_context.get("symbol"),
                "timeframe": trade.get("timeframe") or event_context.get("timeframe"),
                "side": trade.get("side") or trade.get("direction") or event_context.get("side") or event_context.get("direction"),
                "direction": trade.get("direction") or trade.get("side") or event_context.get("direction") or event_context.get("side"),
                "entry_time": entry_time,
                "entry_price": trade.get("entry_price") or event_context.get("entry_price"),
                "exit_time": exit_time,
                "exit_price": trade.get("exit_price") or event_context.get("exit_price"),
                "exit_reason": close_reason,
                "close_reason": close_reason,
                "position_commit_seq": _safe_int(trade.get("position_commit_seq")) or _safe_int(event_context.get("position_commit_seq")),
                "position_commit_seq_status": trade.get("position_commit_seq_status") or event_context.get("position_commit_seq_status"),
                "gross_pnl": _safe_float(trade.get("gross_pnl") if trade.get("gross_pnl") is not None else event_context.get("gross_pnl")),
                "fees": _safe_float(trade.get("fees_paid") if trade.get("fees_paid") is not None else event_context.get("fees_paid")),
                "fees_paid": _safe_float(trade.get("fees_paid") if trade.get("fees_paid") is not None else event_context.get("fees_paid")),
                "net_pnl": _safe_float(trade.get("net_pnl") if trade.get("net_pnl") is not None else event_context.get("net_pnl")),
                "quantity": _quantity(trade, event_context),
                "decision_id": trade.get("decision_id") or event_context.get("decision_id"),
                "signal_id": trade.get("signal_id") or event_context.get("signal_id"),
                "legs": legs,
                "holding_seconds": holding_seconds,
                "duration_seconds": holding_seconds,
                "duration": holding_seconds,
                "terminal_backtest_end": terminal,
                "status": trade.get("status"),
                "source_refs": [
                    {"section": "trades", "trade_id": trade_id},
                    {"section": "runtime_events", "event_id": event_context.get("event_id")},
                ] if trade_id else [],
            }
        )
    return normalized


def _link_trace_rows(
    *,
    decisions: Sequence[Mapping[str, Any]],
    signals: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    decisions_out = [dict(row) for row in decisions]
    signals_out = [dict(row) for row in signals]
    trades_out = [dict(row) for row in trades]
    decision_by_id = {str(row.get("decision_id") or ""): row for row in decisions_out if row.get("decision_id")}
    decision_by_signal = {str(row.get("signal_id") or ""): row for row in decisions_out if row.get("signal_id")}
    decision_by_trade = {str(row.get("trade_id") or ""): row for row in decisions_out if row.get("trade_id")}
    trade_by_id = {str(row.get("trade_id") or ""): row for row in trades_out if row.get("trade_id")}

    for trade in trades_out:
        decision = decision_by_trade.get(str(trade.get("trade_id") or ""))
        if not decision:
            continue
        for key in ("decision_id", "signal_id", "strategy_hash", "instrument_id", "series_key", "timeframe"):
            if not trade.get(key) and decision.get(key):
                trade[key] = decision.get(key)

    for signal in signals_out:
        decision = decision_by_id.get(str(signal.get("decision_id") or "")) or decision_by_signal.get(str(signal.get("signal_id") or ""))
        if not decision:
            continue
        if not signal.get("decision_id"):
            signal["decision_id"] = decision.get("decision_id")
        if not signal.get("trade_id") and decision.get("trade_id"):
            signal["trade_id"] = decision.get("trade_id")

    for decision in decisions_out:
        trade = trade_by_id.get(str(decision.get("trade_id") or ""))
        if trade and not decision.get("trade_id"):
            decision["trade_id"] = trade.get("trade_id")
    return decisions_out, signals_out, trades_out


def _summary(
    *,
    decisions: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    starting_capital: Optional[float],
) -> RunResearchSummary:
    closed = [trade for trade in trades if trade.get("exit_time") and _safe_float(trade.get("net_pnl")) is not None]
    open_count = sum(1 for trade in trades if not trade.get("exit_time"))
    net_values = [_safe_float(trade.get("net_pnl")) or 0.0 for trade in closed]
    gross_values = [_safe_float(trade.get("gross_pnl")) or 0.0 for trade in closed]
    fees = [_safe_float(trade.get("fees_paid")) or 0.0 for trade in closed]
    wins = [value for value in net_values if value > 0]
    losses = [value for value in net_values if value < 0]
    equity_curve: List[float] = []
    equity = float(starting_capital or 0.0)
    if starting_capital is not None:
        equity_curve.append(equity)
    for trade in sorted(closed, key=lambda item: str(item.get("exit_time") or "")):
        equity += _safe_float(trade.get("net_pnl")) or 0.0
        equity_curve.append(equity)
    max_drawdown_pct, max_drawdown_abs = compute_max_drawdown(equity_curve)
    holding_times = [
        float(value)
        for value in (_safe_float(trade.get("holding_seconds")) for trade in closed)
        if value is not None
    ]
    accepted = sum(1 for decision in decisions if decision.get("accepted"))
    rejected = sum(1 for decision in decisions if decision.get("rejected"))
    total_net = sum(net_values)
    equity_end = (starting_capital + total_net) if starting_capital is not None else None
    return_pct = (total_net / starting_capital) if starting_capital else None
    unavailable_metrics = {
        "cagr": "requires simulated window of at least 90 days",
        "annualized_volatility": "requires daily return series",
        "sharpe": "requires daily return series",
        "sortino": "requires downside daily return series",
        "calmar": "requires CAGR and max drawdown",
        "exposure_pct": "requires entry and exit times plus simulated window",
        "drawdown_duration_seconds": "requires daily equity series",
    }
    return RunResearchSummary(
        total_decisions=accepted + rejected,
        accepted_decisions=accepted,
        rejected_decisions=rejected,
        trades=len(trades),
        closed_trades=len(closed),
        open_trades=open_count,
        wins=len(wins),
        losses=len(losses),
        win_rate=(len(wins) / len(closed)) if closed else None,
        gross_pnl=sum(gross_values),
        fees=sum(fees),
        net_pnl=total_net,
        equity_start=starting_capital,
        equity_end=equity_end,
        return_pct=return_pct,
        max_drawdown=max_drawdown_abs,
        max_drawdown_pct=max_drawdown_pct,
        profit_factor=compute_profit_factor(net_values),
        expectancy=compute_expectancy(net_values) if net_values else None,
        avg_win=statistics.mean(wins) if wins else None,
        avg_loss=statistics.mean(losses) if losses else None,
        largest_win=max(wins) if wins else None,
        largest_loss=min(losses) if losses else None,
        average_holding_seconds=statistics.mean(holding_times) if holding_times else None,
        initial_capital=starting_capital,
        final_equity=equity_end,
        loss_rate=(len(losses) / len(closed)) if closed else None,
        unavailable_metrics=unavailable_metrics,
    )


def _fee_accounting(trades: Sequence[Mapping[str, Any]], summary: RunResearchSummary) -> Dict[str, Any]:
    per_symbol: Dict[str, Dict[str, Any]] = {}
    roles = Counter()
    sources = Counter()
    rates: List[float] = []
    suspicious: List[Dict[str, Any]] = []
    net_mismatch: List[str] = []
    for trade in trades:
        symbol = str(trade.get("symbol") or "UNKNOWN")
        fees = _safe_float(trade.get("fees_paid")) or 0.0
        gross = _safe_float(trade.get("gross_pnl")) or 0.0
        net = _safe_float(trade.get("net_pnl")) or 0.0
        stats = per_symbol.setdefault(symbol, {"symbol": symbol, "fees": 0.0, "trades": 0, "gross_pnl": 0.0, "net_pnl": 0.0})
        stats["fees"] += fees
        stats["trades"] += 1
        stats["gross_pnl"] += gross
        stats["net_pnl"] += net
        metrics = _mapping(trade.get("metrics"))
        if metrics.get("fee_role"):
            roles[str(metrics.get("fee_role"))] += 1
        if metrics.get("fee_source"):
            sources[str(metrics.get("fee_source"))] += 1
        rate = _safe_float(metrics.get("fee_rate"))
        if rate is not None:
            rates.append(rate)
        if fees < -_EPSILON:
            suspicious.append({"trade_id": trade.get("trade_id"), "reason": "negative_fee", "fees_paid": fees})
        if abs(gross) > _EPSILON and abs(fees / gross) > 1.0:
            suspicious.append({"trade_id": trade.get("trade_id"), "reason": "fee_exceeds_gross_pnl", "fees_paid": fees, "gross_pnl": gross})
        if abs((gross - fees) - net) > 0.01:
            net_mismatch.append(str(trade.get("trade_id") or ""))
    return {
        "per_symbol_fees": sorted(per_symbol.values(), key=lambda item: item["symbol"]),
        "fee_rate": {
            "min": min(rates) if rates else None,
            "max": max(rates) if rates else None,
            "avg": statistics.mean(rates) if rates else None,
        },
        "fee_role_distribution": dict(roles),
        "fee_source_distribution": dict(sources),
        "fee_sanity_checks": {
            "fees_non_negative": not any((_safe_float(trade.get("fees_paid")) or 0.0) < -_EPSILON for trade in trades),
            "net_equals_gross_minus_fees": not net_mismatch,
            "net_mismatch_trade_ids": net_mismatch,
            "total_fees": summary.fees,
        },
        "suspicious_fee_outliers": suspicious,
    }


def _wallet_accounting(
    *,
    run: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    summary: Optional[RunResearchSummary] = None,
) -> Dict[str, Any]:
    warnings = []
    margin_warnings = []
    missing_wallet_trace = []
    wallet_events = []
    for row in events:
        name = _event_name_key(row)
        context = _context(row)
        if name in _WALLET_EVENT_NAMES:
            wallet_events.append(_payload(row))
    wallet_events_by_decision: Dict[str, List[Dict[str, Any]]] = {}
    for payload in wallet_events:
        context = _mapping(_mapping(payload).get("context"))
        decision_id = str(context.get("decision_id") or "").strip()
        if decision_id:
            wallet_events_by_decision.setdefault(decision_id, []).append(dict(payload))

    def wallet_trace_has_evidence(value: Mapping[str, Any]) -> bool:
        context = _mapping(value)
        if _mapping(context.get("wallet_snapshot")) or _mapping(context.get("wallet_before")):
            return True
        if _safe_float(context.get("balance_before")) is not None:
            return True
        if _safe_float(context.get("margin_available")) is not None:
            return True
        return False

    for decision in decisions:
        reason = str(decision.get("reason_code") or "").strip().upper()
        if reason.startswith("WALLET_"):
            warnings.append({"decision_id": decision.get("decision_id"), "reason_code": reason})
        if "MARGIN" in reason:
            margin_warnings.append({"decision_id": decision.get("decision_id"), "reason_code": reason})
        context = _mapping(decision.get("decision_context"))
        decision_id = str(decision.get("decision_id") or "").strip()
        matching_wallet_events = wallet_events_by_decision.get(decision_id, [])
        has_wallet_snapshot = wallet_trace_has_evidence(context) or any(
            wallet_trace_has_evidence(_mapping(event).get("context") or {})
            for event in matching_wallet_events
        )
        needs_wallet_trace = bool(decision.get("accepted")) or reason.startswith("WALLET_") or "MARGIN" in reason
        if needs_wallet_trace and not has_wallet_snapshot:
            missing_wallet_trace.append(
                {
                    "decision_id": decision.get("decision_id"),
                    "bar_time": decision.get("bar_time"),
                    "symbol": decision.get("symbol"),
                    "instrument_id": decision.get("instrument_id"),
                    "action": decision.get("action"),
                    "accepted": bool(decision.get("accepted")),
                    "reason_code": reason or None,
                    "status": decision.get("status") or decision.get("verdict"),
                    "signal_id": decision.get("signal_id"),
                    "signal_exists": bool(decision.get("signal_id")),
                    "trade_id": decision.get("trade_id"),
                    "trade_exists": bool(decision.get("trade_id")),
                    "wallet_event_exists": bool(matching_wallet_events),
                }
            )

    caveats = []
    locked_margin_final = None
    reservation_leaks = None
    projection_error = None
    replay_consistency_status = "unavailable"
    replay_projection = None
    wallet_replay_status = "unavailable"
    first_wallet_replay_error = None
    first_wallet_ledger_state_issue = None
    event_counts = Counter(
        _mapping(event).get("event_name")
        for event in wallet_events
        if str(_mapping(event).get("event_name") or "").strip()
    )
    margin_rejection_trace = []
    incomplete_margin_rejection_trace = []
    drift_indicators = []
    if wallet_events:
        try:
            from engines.bot_runtime.core.wallet import canonical_wallet_ledger_events
            from engines.bot_runtime.core.wallet import first_wallet_ledger_state_issue as _first_wallet_ledger_state_issue
            from engines.bot_runtime.core.wallet import project_wallet_from_events

            wallet_events = [dict(event) for event in canonical_wallet_ledger_events(wallet_events)]
            wallet_state = project_wallet_from_events(wallet_events)
            first_wallet_ledger_state_issue = _first_wallet_ledger_state_issue(wallet_events)
            replay_projection = {
                "balances": dict(getattr(wallet_state, "balances", {}) or {}),
                "locked_margin": dict(getattr(wallet_state, "locked_margin", {}) or {}),
                "free_collateral": dict(getattr(wallet_state, "free_collateral", {}) or {}),
                "margin_positions": dict(getattr(wallet_state, "margin_positions", {}) or {}),
            }
            locked_margin = dict(getattr(wallet_state, "locked_margin", {}) or {})
            locked_margin_final = locked_margin
            reservation_leaks = {
                currency: value
                for currency, value in locked_margin.items()
                if abs(_safe_float(value) or 0.0) > _EPSILON
            }
            replay_consistency_status = "consistent" if not reservation_leaks else "drift_detected"
            final_equity = None
            if summary is not None:
                final_equity = _safe_float(getattr(summary, "equity_end", None))
                if final_equity is None:
                    final_equity = _safe_float(getattr(summary, "final_equity", None))
            final_usd = _safe_float(_mapping(replay_projection.get("balances")).get("USD"))
            if final_equity is not None and final_usd is not None and abs(final_usd - final_equity) > 0.01:
                replay_consistency_status = "drift_detected"
                drift_indicators.append(
                    {
                        "reason": "final_balance_mismatch",
                        "currency": "USD",
                        "wallet_balance": final_usd,
                        "report_equity": final_equity,
                    }
                )
            if first_wallet_ledger_state_issue:
                replay_consistency_status = "error"
                first_wallet_replay_error = first_wallet_ledger_state_issue
                caveats.append(str(first_wallet_ledger_state_issue.get("code") or "wallet_ledger_state_malformed"))
            wallet_replay_status = "passed" if replay_consistency_status == "consistent" else "failed"
        except Exception as exc:  # noqa: BLE001 - dataset caveat, not trading path
            projection_error = str(exc)
            first_wallet_replay_error = projection_error
            replay_consistency_status = "error"
            wallet_replay_status = "failed"
            caveats.append("wallet_projection_unavailable")
            caveats.append("wallet_replay_failed")
    else:
        caveats.append("wallet_runtime_events_unavailable")
    for event in wallet_events:
        payload = _mapping(event)
        context = _mapping(payload.get("context"))
        name = str(payload.get("event_name") or "").strip().upper()
        if name == "MARGIN_REJECTED":
            trace = {
                "decision_id": context.get("decision_id"),
                "trade_id": context.get("trade_id"),
                "instrument_id": context.get("instrument_id"),
                "symbol": context.get("symbol"),
                "bar_time": context.get("bar_time"),
                "known_at": context.get("known_at") or payload.get("event_ts"),
                "margin_required": context.get("margin_required"),
                "margin_available": context.get("margin_available"),
                "balance_before": context.get("balance_before"),
                "balance_after": context.get("balance_after"),
                "free_collateral_before": context.get("free_collateral_before"),
                "free_collateral_after": context.get("free_collateral_after"),
                "locked_margin_before": context.get("locked_margin_before"),
                "locked_margin_after": context.get("locked_margin_after"),
                "selected_quantity": context.get("selected_quantity"),
                "signal_id": context.get("signal_id"),
                "reason": context.get("reason"),
                "wallet_before": context.get("wallet_before"),
                "wallet_after": context.get("wallet_after"),
                "margin_requirement": context.get("margin_requirement"),
                "source_refs": context.get("source_refs"),
            }
            margin_rejection_trace.append(trace)
            if (
                not trace.get("decision_id")
                or not trace.get("signal_id")
                or _safe_float(trace.get("margin_available")) is None
                or _safe_float(trace.get("margin_required")) in (None, 0.0)
                or _safe_float(trace.get("selected_quantity")) is None
                or not wallet_trace_has_evidence(context)
                or not _mapping(trace.get("wallet_after"))
                or not trace.get("source_refs")
            ):
                incomplete_margin_rejection_trace.append(trace)
        balance_before = _safe_float(context.get("balance_before"))
        balance_after = _safe_float(context.get("balance_after"))
        if balance_before is not None and balance_after is not None:
            fee = _safe_float(context.get("fee")) or 0.0
            if name == "FEE_APPLIED" and abs((balance_before - fee) - balance_after) > 0.01:
                drift_indicators.append(
                    {
                        "event_id": payload.get("event_id"),
                        "event_name": name,
                        "reason": "fee_balance_delta_mismatch",
                    }
                )
    config = _mapping(run.get("config_snapshot"))
    if not _mapping(config.get("wallet_start")) and not _mapping(config.get("wallet_config")):
        caveats.append("wallet_artifact_unavailable")
    if missing_wallet_trace:
        caveats.append("wallet_decision_trace_incomplete")
    if incomplete_margin_rejection_trace:
        caveats.append("wallet_margin_rejection_trace_incomplete")
        caveats.append("margin_rejection_evidence_incomplete")
    if replay_consistency_status == "drift_detected":
        caveats.append("wallet_drift_detected")
    if first_wallet_ledger_state_issue and "wallet_replay_failed" not in caveats:
        caveats.append("wallet_replay_failed")
    return {
        "locked_margin_final": locked_margin_final,
        "reservation_leaks": reservation_leaks,
        "wallet_warnings": warnings,
        "margin_warnings": margin_warnings,
        "missing_wallet_trace": missing_wallet_trace,
        "missing_wallet_trace_count": len(missing_wallet_trace),
        "first_wallet_trace_gap": missing_wallet_trace[0] if missing_wallet_trace else None,
        "projection_error": projection_error,
        "wallet_replay_status": wallet_replay_status,
        "first_wallet_replay_error": first_wallet_replay_error,
        "first_wallet_ledger_state_issue": first_wallet_ledger_state_issue,
        "wallet_diagnostics": {
            "wallet_event_count": len(wallet_events),
            "wallet_event_counts": dict(sorted(event_counts.items())),
            "margin_rejection_trace": margin_rejection_trace,
            "margin_rejection_trace_complete": not incomplete_margin_rejection_trace,
            "incomplete_margin_rejection_trace": incomplete_margin_rejection_trace,
            "wallet_drift_indicators": drift_indicators,
            "wallet_replay_status": wallet_replay_status,
            "first_wallet_replay_error": first_wallet_replay_error,
            "first_wallet_ledger_state_issue": first_wallet_ledger_state_issue,
            "replay_consistency_status": replay_consistency_status,
            "replay_projection": replay_projection,
        },
        "caveats": caveats,
    }


def _execution_section(
    *,
    run: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    warnings = []
    config = _mapping(run.get("config_snapshot"))
    for key in ("report_warnings", "runtime_warnings", "warnings"):
        warnings.extend(dict(entry) for entry in config.get(key) or [] if isinstance(entry, Mapping))
    fallback_rows: Dict[str, Dict[str, Any]] = {}
    for row in events:
        name = _event_name_key(row)
        context = _context(row)
        if name == "execution_intrabar_fallback_pessimistic":
            event_id = str(row.get("event_id") or _payload(row).get("event_id") or len(fallback_rows))
            fallback_rows[event_id] = {
                "symbol": context.get("symbol") or row.get("symbol"),
                "timeframe": context.get("timeframe") or row.get("timeframe"),
                "bar_time": context.get("bar_time") or row.get("bar_time") or _payload(row).get("event_ts"),
                "reason": context.get("reason"),
                "raw_reason": context.get("raw_reason"),
            }
    for warning in warnings:
        if str(warning.get("warning_type") or "").strip() != "execution_intrabar_fallback_pessimistic":
            continue
        warning_context = _mapping(warning.get("context"))
        warning_id = str(warning.get("warning_id") or f"warning-{len(fallback_rows)}")
        fallback_rows.setdefault(
            warning_id,
            {
                "symbol": warning.get("symbol"),
                "timeframe": warning.get("timeframe"),
                "bar_time": warning_context.get("bar_time"),
                "reason": warning_context.get("reason"),
                "raw_reason": warning_context.get("raw_reason"),
            },
        )
    reason_distribution = Counter(str(row.get("reason") or "unknown") for row in fallback_rows.values())
    fast_full_caveats = []
    if fallback_rows and _execution_mode(run) == "full":
        fast_full_caveats.append("FULL mode used pessimistic same-bar fallback for some bars.")
    return {
        "execution_mode": _execution_mode(run),
        "intrabar_fallback_count": len(fallback_rows),
        "fallback_reason_distribution": dict(reason_distribution),
        "fallback_bars": list(fallback_rows.values()),
        "fast_full_caveats": fast_full_caveats,
    }


def _candle_gaps(
    events: Sequence[Mapping[str, Any]],
    observability_events: Sequence[Mapping[str, Any]] = (),
    metadata: Optional[RunResearchMetadata] = None,
) -> Dict[str, Any]:
    instrument_symbols: Dict[str, str] = {}
    if metadata is not None and len(metadata.instrument_ids) == len(metadata.symbols):
        instrument_symbols = {
            str(instrument_id): str(symbol)
            for instrument_id, symbol in zip(metadata.instrument_ids, metadata.symbols)
            if instrument_id and symbol
        }
    by_symbol: Dict[str, Dict[str, Any]] = {}
    classification = Counter()
    facts: List[Dict[str, Any]] = []
    sources: List[tuple[Mapping[str, Any], Dict[str, Any], str]] = []
    observed_summary = False
    for row in events:
        context = _context(row)
        sources.append((row, context if "gap_count_by_type" in context else _mapping(_payload(row).get("details")), _event_name_key(row)))
    for row in observability_events:
        sources.append((row, _mapping(row.get("details")), str(row.get("event_name") or "").strip().lower()))
    closure_cache: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row, summary_context, name in sources:
        if name not in {"candle_continuity_summary", "candle_gap_observed"} and "gap_count_by_type" not in summary_context:
            continue
        observed_summary = True
        instrument_id = summary_context.get("instrument_id") or row.get("instrument_id")
        series_key = summary_context.get("series_key") or row.get("series_key")
        if not instrument_id and series_key and "|" in str(series_key):
            instrument_id = str(series_key).split("|", 1)[0]
        symbol = str(summary_context.get("symbol") or row.get("symbol") or "").strip()
        if not symbol and instrument_id:
            symbol = instrument_symbols.get(str(instrument_id), "")
        if not symbol and series_key:
            symbol = str(series_key)
        if not symbol:
            symbol = "UNKNOWN"
        gap_counts = _mapping(summary_context.get("gap_count_by_type"))
        gaps = summary_context.get("gaps") if isinstance(summary_context.get("gaps"), list) else []
        gaps = _reclassify_unknown_candle_gaps_from_closures(
            gaps,
            instrument_id=str(instrument_id or ""),
            timeframe=str(summary_context.get("timeframe") or row.get("timeframe") or ""),
            metadata=metadata,
            closure_cache=closure_cache,
        )
        detected = _safe_int(summary_context.get("detected_gap_count")) or sum(
            int(_safe_int(value) or 0) for value in gap_counts.values()
        )
        derived_gap_counts = Counter(
            str(gap.get("classification") or "unknown_gap")
            for gap in gaps
            if isinstance(gap, Mapping)
        )
        if gaps and int(sum(derived_gap_counts.values())) == int(detected or 0) and int(gap_counts.get("unknown_gap", 0) or 0) > 0:
            gap_counts = dict(derived_gap_counts)
        if detected <= 0:
            continue
        stats = by_symbol.setdefault(symbol, {"symbol": symbol, "gap_count": 0, "gap_count_by_type": {}})
        stats["gap_count"] += detected
        for gap_type, count in gap_counts.items():
            normalized_count = int(_safe_int(count) or 0)
            classification[str(gap_type)] += normalized_count
            stats["gap_count_by_type"][str(gap_type)] = stats["gap_count_by_type"].get(str(gap_type), 0) + normalized_count
        facts.append(
            {
                "symbol": symbol,
                "resolved_symbol": symbol,
                "instrument_id": instrument_id,
                "timeframe": summary_context.get("timeframe") or row.get("timeframe"),
                "series_key": series_key,
                "boundary_name": summary_context.get("boundary_name"),
                "source_reason": summary_context.get("source_reason"),
                "gap_count_by_type": gap_counts,
                "detected_gap_count": detected,
                "candle_count": summary_context.get("candle_count"),
                "missing_candle_estimate": summary_context.get("missing_candle_estimate"),
                "gaps": gaps,
                "observed_at": row.get("observed_at"),
            }
        )
    return {
        "gap_counts_by_symbol": sorted(by_symbol.values(), key=lambda item: item["symbol"]),
        "classification_distribution": dict(classification),
        "unknown_gaps": int(classification.get("unknown_gap", 0)),
        "blocking_gap_count": sum(int(classification.get(gap_type, 0) or 0) for gap_type in _CANDLE_GAP_BLOCKING_TYPES),
        "provider_gap_count": sum(int(classification.get(gap_type, 0) or 0) for gap_type in _CANDLE_GAP_PROVIDER_TYPES),
        "expected_gap_count": sum(int(classification.get(gap_type, 0) or 0) for gap_type in _CANDLE_GAP_EXPECTED_TYPES),
        "facts": facts,
        "provider_missing_facts": [fact for fact in facts if int(_mapping(fact.get("gap_count_by_type")).get("provider_missing_data", 0) or 0) > 0],
        "source_sparse_facts": [fact for fact in facts if str(fact.get("source_reason") or "").strip().lower() == "source_sparse"],
        "caveats": [] if observed_summary else ["candle_gap_observability_unavailable"],
    }


def _candle_gap_missing_window(gap: Mapping[str, Any]) -> tuple[Optional[datetime], Optional[datetime]]:
    previous_ts = _parse_iso(gap.get("previous_ts") or gap.get("previous_time") or gap.get("previous"))
    current_ts = _parse_iso(gap.get("current_ts") or gap.get("current_time") or gap.get("current"))
    expected_seconds = _safe_int(gap.get("expected_interval_seconds"))
    if previous_ts is not None and current_ts is not None and expected_seconds and expected_seconds > 0:
        return previous_ts + timedelta(seconds=int(expected_seconds)), current_ts
    return (
        _parse_iso(gap.get("start") or gap.get("start_ts") or gap.get("missing_start")),
        _parse_iso(gap.get("end") or gap.get("end_ts") or gap.get("missing_end")),
    )


def _load_candle_closure_evidence(
    *,
    instrument_id: str,
    timeframe: str,
    metadata: Optional[RunResearchMetadata],
    closure_cache: Dict[tuple[str, str], List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    instrument = str(instrument_id or "").strip()
    interval = str(timeframe or "").strip()
    if not instrument or not interval or metadata is None:
        return []
    key = (instrument, interval)
    if key in closure_cache:
        return closure_cache[key]
    loader = getattr(storage, "list_candle_closure_evidence", None)
    if not callable(loader):
        closure_cache[key] = []
        return []
    try:
        rows = loader(
            instrument_id=instrument,
            timeframe=interval,
            start=metadata.simulated_window.get("start"),
            end=metadata.simulated_window.get("end"),
        )
    except Exception as exc:  # noqa: BLE001 - diagnostics must not hide report construction
        logger.warning(
            with_log_context(
                "run_research_dataset_candle_closure_evidence_unavailable",
                build_log_context(
                    run_id=metadata.run_id,
                    instrument_id=instrument,
                    timeframe=interval,
                    error=str(exc),
                ),
            )
        )
        rows = []
    closure_cache[key] = [dict(row) for row in rows if isinstance(row, Mapping)]
    return closure_cache[key]


def _closure_covers_gap(closure: Mapping[str, Any], gap_start: datetime, gap_end: datetime) -> bool:
    closure_start = _parse_iso(closure.get("start") or closure.get("start_ts"))
    closure_end = _parse_iso(closure.get("end") or closure.get("end_ts"))
    if closure_start is None or closure_end is None:
        return False
    return closure_start <= gap_start and closure_end >= gap_end


def _reclassify_unknown_candle_gaps_from_closures(
    gaps: Sequence[Any],
    *,
    instrument_id: str,
    timeframe: str,
    metadata: Optional[RunResearchMetadata],
    closure_cache: Dict[tuple[str, str], List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    normalized = [dict(gap) for gap in gaps if isinstance(gap, Mapping)]
    if not normalized or not any(str(gap.get("classification") or "unknown_gap") == "unknown_gap" for gap in normalized):
        return normalized
    closures = _load_candle_closure_evidence(
        instrument_id=instrument_id,
        timeframe=timeframe,
        metadata=metadata,
        closure_cache=closure_cache,
    )
    if not closures:
        return normalized
    reclassified: List[Dict[str, Any]] = []
    for gap in normalized:
        classification = str(gap.get("classification") or "unknown_gap")
        if classification != "unknown_gap":
            reclassified.append(gap)
            continue
        gap_start, gap_end = _candle_gap_missing_window(gap)
        if gap_start is None or gap_end is None:
            reclassified.append(gap)
            continue
        closure = next((row for row in closures if _closure_covers_gap(row, gap_start, gap_end)), None)
        if closure is None:
            reclassified.append(gap)
            continue
        closure_metadata = _mapping(closure.get("metadata"))
        provider_evidence = _mapping(closure_metadata.get("provider_evidence"))
        evidence = {
            **gap,
            "classification": "provider_missing_data",
            "reason_code": str(closure_metadata.get("reason_code") or "source_sparse"),
            "evidence": str(closure_metadata.get("evidence") or "portal_candle_closure"),
            "closure_start": _iso(_parse_iso(closure.get("start"))),
            "closure_end": _iso(_parse_iso(closure.get("end"))),
        }
        if provider_evidence:
            evidence["provider_evidence"] = provider_evidence
        reclassified.append(evidence)
    return reclassified


def _normalized_gap_counts(value: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for gap_type, count in _mapping(value).items():
        normalized = int(_safe_int(count) or 0)
        if normalized > 0:
            counts[str(gap_type)] = normalized
    return counts


def _candle_gap_semantics(
    *,
    gap_count: Optional[int],
    gap_count_by_type: Mapping[str, Any],
) -> Dict[str, Any]:
    counts = _normalized_gap_counts(gap_count_by_type)
    detected = max(int(gap_count or 0), sum(counts.values()))
    classified = sum(counts.values())
    unclassified = max(detected - classified, 0)
    blocking = unclassified + sum(int(counts.get(gap_type, 0) or 0) for gap_type in _CANDLE_GAP_BLOCKING_TYPES)
    provider = sum(int(counts.get(gap_type, 0) or 0) for gap_type in _CANDLE_GAP_PROVIDER_TYPES)
    expected = sum(int(counts.get(gap_type, 0) or 0) for gap_type in _CANDLE_GAP_EXPECTED_TYPES)
    if detected <= 0:
        status = "clean"
        impact = "none"
    elif blocking > 0:
        status = "degraded"
        impact = "blocks_golden"
    elif provider > 0:
        status = "source_sparse"
        impact = "degrades_metrics"
    elif expected > 0:
        status = "expected_sparse"
        impact = "none"
    else:
        status = "degraded"
        impact = "blocks_golden"
    return {
        "status": status,
        "readiness_impact": impact,
        "gap_count_by_type": counts,
        "blocking_gap_count": blocking,
        "provider_gap_count": provider,
        "expected_gap_count": expected,
        "unclassified_gap_count": unclassified,
    }


def _diagnostic(
    *,
    severity: str,
    source: str,
    code: str,
    message: str,
    affected_identity: Optional[Mapping[str, Any]] = None,
    timestamp: Optional[str] = None,
    known_at: Optional[str] = None,
    readiness_impact: str = "none",
    suggested_next_step: Optional[str] = None,
) -> Dict[str, Any]:
    return _json_safe(
        asdict(
            ReportDiagnostic(
                severity=severity,
                source=source,
                code=code,
                message=message,
                affected_identity=dict(affected_identity or {}),
                timestamp=timestamp,
                known_at=known_at,
                readiness_impact=readiness_impact,
                suggested_next_step=suggested_next_step,
            )
        )
    )


def _diagnostic_summary(items: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    by_severity = Counter(str(item.get("severity") or "unknown") for item in items)
    by_source = Counter(str(item.get("source") or "unknown") for item in items)
    by_code = Counter(str(item.get("code") or "unknown") for item in items)
    by_impact = Counter(str(item.get("readiness_impact") or "none") for item in items)
    affected_symbols = set()
    affected_instruments = set()
    for item in items:
        identity = _mapping(item.get("affected_identity"))
        if identity.get("symbol"):
            affected_symbols.add(str(identity.get("symbol")))
        if identity.get("instrument_id"):
            affected_instruments.add(str(identity.get("instrument_id")))
        for row in identity.get("symbols") or []:
            if isinstance(row, Mapping) and row.get("symbol"):
                affected_symbols.add(str(row.get("symbol")))
    blocking_impacts = {"blocks_results", "blocks_comparison", "blocks_export", "blocks_golden"}
    blocking_codes = sorted(
        {
            str(item.get("code"))
            for item in items
            if str(item.get("readiness_impact") or "none") in blocking_impacts and item.get("code")
        }
    )
    degraded_codes = sorted(
        {
            str(item.get("code"))
            for item in items
            if str(item.get("readiness_impact") or "none").startswith("degrades_") and item.get("code")
        }
    )
    return {
        "total": len(items),
        "by_severity": dict(by_severity),
        "by_source": dict(by_source),
        "by_code": dict(by_code),
        "readiness_impact": dict(by_impact),
        "affected_symbols": sorted(affected_symbols),
        "affected_instruments": sorted(affected_instruments),
        "blocking_codes": blocking_codes,
        "degraded_codes": degraded_codes,
    }


def _observability_events_for_run(run_id: str) -> List[Dict[str, Any]]:
    list_events = getattr(storage, "list_observability_events", None)
    if not callable(list_events):
        return []
    try:
        return [dict(row) for row in list_events(run_id=run_id, limit=2000)]
    except TypeError:
        rows = list_events(limit=5000)
        return [dict(row) for row in rows if str(row.get("run_id") or "") == run_id]
    except Exception as exc:  # noqa: BLE001 - diagnostics should report source gaps, not fail report builds.
        logger.warning(
            with_log_context(
                "run_research_dataset_observability_events_unavailable",
                build_log_context(run_id=run_id, error=str(exc)),
            )
        )
        return []


def _failure_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    context = _context(row)
    failure = _mapping(context.get("failure"))
    if failure:
        return failure
    if _event_name_key(row) == "fault_recorded":
        return context
    return {}


def _is_recoverable_watchdog_failure(row: Mapping[str, Any]) -> bool:
    failure = _failure_payload(row)
    reason_code = str(failure.get("reason_code") or failure.get("fault_code") or "").strip().lower()
    failure_type = str(failure.get("type") or failure.get("failure_type") or "").strip().lower()
    component = str(failure.get("owner") or failure.get("component") or failure.get("source") or "").strip().lower()
    reason = str(failure.get("reason") or failure.get("message") or "").strip().lower()
    recoverable = failure.get("recoverable") is True or str(failure.get("recoverable") or "").strip().lower() == "true"
    recoverable_reason = reason_code in {"stale_heartbeat", "startup_container_ambiguous"}
    return bool(
        recoverable
        and recoverable_reason
        and (component == "watchdog" or failure_type.startswith("watchdog_") or f"{reason_code}:" in reason)
    )


def _unclassified_runtime_failures(events: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [
        row
        for row in events
        if _event_name_key(row) in {"run_failed", "fault_recorded"}
        and not _is_recoverable_watchdog_failure(row)
    ]


def _recoverable_watchdog_failures(events: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [
        row
        for row in events
        if _event_name_key(row) == "fault_recorded" and _is_recoverable_watchdog_failure(row)
    ]


def _observability_time(row: Mapping[str, Any]) -> Optional[datetime]:
    return _parse_iso(row.get("observed_at") or row.get("created_at") or row.get("timestamp"))


def _details(row: Mapping[str, Any]) -> Dict[str, Any]:
    return _mapping(row.get("details"))


def _projection_replay_resolved(observability_events: Sequence[Mapping[str, Any]]) -> bool:
    blocking_events = [
        row
        for row in observability_events
        if str(row.get("event_name") or "").strip()
        in {"run_projector_failed", "run_projector_reconcile_failed", "run_notification_queue_overflow"}
    ]
    reconciliations = [
        row
        for row in observability_events
        if str(row.get("event_name") or "").strip() in {"run_projector_reconciled", "projection_replay_completed"}
    ]
    if not blocking_events or not reconciliations:
        return False
    block_times = [value for value in (_observability_time(row) for row in blocking_events) if value is not None]
    reconciliation_times = [value for value in (_observability_time(row) for row in reconciliations) if value is not None]
    latest_block = max(block_times, default=None)
    latest_reconciliation = max(reconciliation_times, default=None)
    if latest_block and latest_reconciliation and latest_reconciliation < latest_block:
        return False
    latest = max(reconciliations, key=lambda row: _observability_time(row) or datetime.min.replace(tzinfo=timezone.utc))
    details = _details(latest)
    open_count = _safe_int(details.get("open_trade_count") or latest.get("open_trade_count"))
    projection_state = str(details.get("projection_state") or latest.get("projection_state") or "").strip().lower()
    return bool((open_count in (None, 0)) and projection_state in {"", "reconciled"})


_TRADE_ID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|trade[-_][A-Za-z0-9_.:-]+"
)


def _projection_failure_trade_ids(event: Mapping[str, Any]) -> List[str]:
    details = _details(event)
    candidates: List[str] = []
    for key in ("trade_ids", "open_trade_ids", "prior_open_trade_ids"):
        raw = details.get(key) or event.get(key)
        if isinstance(raw, (list, tuple)):
            candidates.extend(str(item).strip() for item in raw if str(item).strip())
        elif raw:
            candidates.extend(_TRADE_ID_RE.findall(str(raw)))
    for key in ("error", "message"):
        raw = details.get(key) or event.get(key)
        if raw:
            candidates.extend(_TRADE_ID_RE.findall(str(raw)))
    return sorted(dict.fromkeys(candidates))


def _closed_trade_ids_from_events(events: Sequence[Mapping[str, Any]]) -> set[str]:
    closed: set[str] = set()
    for row in events:
        if _event_name_key(row) != "trade_closed":
            continue
        context = _context(row)
        trade_id = str(row.get("trade_id") or context.get("trade_id") or "").strip()
        if trade_id:
            closed.add(trade_id)
    return closed


def _event_order_value(row: Mapping[str, Any]) -> int:
    return int(_safe_int(row.get("run_seq")) or _safe_int(_context(row).get("run_seq")) or _safe_int(row.get("seq")) or 0)


def _event_run_seq_value(row: Mapping[str, Any]) -> Optional[int]:
    return _safe_int(row.get("run_seq")) or _safe_int(_context(row).get("run_seq"))


def _event_run_seq_status(row: Mapping[str, Any]) -> Optional[str]:
    value = row.get("run_seq_status") or _context(row).get("run_seq_status")
    text = str(value or "").strip().lower()
    return text or None


def _position_commit_seq_value(row: Mapping[str, Any]) -> Optional[int]:
    context = _context(row)
    return _safe_int(row.get("position_commit_seq")) or _safe_int(context.get("position_commit_seq"))


def _position_commit_seq_status(row: Mapping[str, Any]) -> Optional[str]:
    context = _context(row)
    value = row.get("position_commit_seq_status") or context.get("position_commit_seq_status")
    text = str(value or "").strip().lower()
    return text or None


def _runtime_ordering_health(events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = [row for row in events if _event_name(row)]
    total = len(rows)
    if total == 0:
        return {
            "status": "unavailable",
            "mode": "unavailable",
            "total_events": 0,
            "missing_count": 0,
            "duplicate_values": [],
            "gap_count": 0,
            "gaps": [],
            "non_monotonic_count": 0,
            "fallback_ordering_used": True,
            "mixed_ordering_modes": False,
        }
    values = [_event_run_seq_value(row) for row in rows]
    missing_count = sum(1 for value in values if value is None or value <= 0)
    present = [int(value) for value in values if value is not None and value > 0]
    counts = Counter(present)
    duplicate_values = sorted(value for value, count in counts.items() if count > 1)
    expected = set(range(1, max(present, default=0) + 1))
    gaps = sorted(expected - set(present))
    non_monotonic_count = 0
    previous = 0
    for value in present:
        if value < previous:
            non_monotonic_count += 1
        previous = value
    statuses = sorted({status for row in rows if (status := _event_run_seq_status(row))})
    fallback_ordering_used = bool(missing_count)
    mixed_ordering_modes = len(statuses) > 1 or (bool(statuses) and missing_count > 0)
    if missing_count == total:
        status = "unavailable"
        mode = "unavailable"
    elif missing_count or duplicate_values or gaps or non_monotonic_count or mixed_ordering_modes:
        status = "inconsistent"
        mode = "mixed" if mixed_ordering_modes else (statuses[0] if statuses else "fallback")
    else:
        mode = statuses[0] if statuses else "backfilled"
        status = "ready" if mode == "runtime_assigned" else "backfilled"
    return {
        "status": status,
        "mode": mode,
        "total_events": total,
        "missing_count": missing_count,
        "duplicate_values": duplicate_values[:50],
        "gap_count": len(gaps),
        "gaps": gaps[:50],
        "non_monotonic_count": non_monotonic_count,
        "fallback_ordering_used": fallback_ordering_used,
        "mixed_ordering_modes": mixed_ordering_modes,
        "min_run_seq": min(present) if present else None,
        "max_run_seq": max(present) if present else None,
    }


def _position_ordering_health(events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = [row for row in events if _event_name_key(row) in _POSITION_TRADE_EVENT_NAMES]
    if not rows:
        return {
            "status": "unavailable",
            "mode": "unavailable",
            "trade_event_count": 0,
            "missing_count": 0,
            "invalid_status_count": 0,
            "duplicate_count": 0,
            "gap_count": 0,
            "non_monotonic_count": 0,
            "open_seq_invalid_count": 0,
            "replay_ordering_key": "trade_id,position_commit_seq",
            "caveats": [],
        }

    missing: List[Dict[str, Any]] = []
    invalid_status: List[Dict[str, Any]] = []
    duplicates: List[Dict[str, Any]] = []
    gaps: List[Dict[str, Any]] = []
    non_monotonic: List[Dict[str, Any]] = []
    open_seq_invalid: List[Dict[str, Any]] = []
    grouped: Dict[str, List[tuple[int, int, Mapping[str, Any]]]] = defaultdict(list)
    seq_counts: Counter[tuple[str, int]] = Counter()

    for row in rows:
        context = _context(row)
        trade_id = str(row.get("trade_id") or context.get("trade_id") or "").strip()
        seq = _position_commit_seq_value(row)
        status = _position_commit_seq_status(row) or ""
        event_name = _event_name_key(row)
        identity = {
            "event_id": row.get("event_id") or context.get("event_id"),
            "event_name": event_name.upper(),
            "trade_id": trade_id or None,
            "bar_time": row.get("bar_time") or context.get("bar_time"),
            "run_seq": _event_run_seq_value(row),
        }
        if not trade_id or seq is None or seq <= 0:
            missing.append(identity)
            continue
        if status != "position_scoped":
            invalid_status.append(identity | {"position_commit_seq": seq, "position_commit_seq_status": status or None})
        grouped[trade_id].append((_event_order_value(row), seq, row))
        seq_counts[(trade_id, seq)] += 1
        if event_name == "trade_opened" and seq != 1:
            open_seq_invalid.append(identity | {"position_commit_seq": seq})

    for (trade_id, seq), count in seq_counts.items():
        if count > 1:
            duplicates.append({"trade_id": trade_id, "position_commit_seq": seq, "count": count})

    for trade_id, entries in grouped.items():
        seqs = {seq for _order, seq, _row in entries}
        max_seq = max(seqs, default=0)
        missing_seqs = sorted(set(range(1, max_seq + 1)) - seqs)
        if missing_seqs:
            gaps.append(
                {
                    "trade_id": trade_id,
                    "missing_position_commit_seq": missing_seqs[:20],
                    "max_position_commit_seq": max_seq,
                }
            )
        previous_seq = 0
        for _order, seq, row in sorted(entries, key=lambda entry: (entry[0], entry[1])):
            if seq < previous_seq:
                context = _context(row)
                non_monotonic.append(
                    {
                        "trade_id": trade_id,
                        "event_name": _event_name_key(row).upper(),
                        "position_commit_seq": seq,
                        "previous_position_commit_seq": previous_seq,
                        "run_seq": _event_run_seq_value(row),
                        "bar_time": row.get("bar_time") or context.get("bar_time"),
                    }
                )
            previous_seq = max(previous_seq, seq)

    caveats: List[str] = []
    if missing:
        caveats.append("position_ordering_missing")
    if invalid_status:
        caveats.append("position_commit_seq_status_invalid")
    if duplicates:
        caveats.append("position_ordering_duplicate")
    if open_seq_invalid:
        caveats.append("position_open_seq_invalid")
    status = "ready" if not caveats else "inconsistent"
    return {
        "status": status,
        "mode": "position_scoped" if status == "ready" else "mixed",
        "trade_event_count": len(rows),
        "missing_count": len(missing),
        "invalid_status_count": len(invalid_status),
        "duplicate_count": len(duplicates),
        "gap_count": len(gaps),
        "non_monotonic_count": len(non_monotonic),
        "open_seq_invalid_count": len(open_seq_invalid),
        "replay_ordering_key": "trade_id,position_commit_seq",
        "missing": missing[:20],
        "invalid_status": invalid_status[:20],
        "duplicates": duplicates[:20],
        "gaps": gaps[:20],
        "non_monotonic": non_monotonic[:20],
        "open_seq_invalid": open_seq_invalid[:20],
        "caveats": caveats,
    }


def _lifecycle_events_for_run(run_id: str) -> List[Dict[str, Any]]:
    list_events = getattr(storage, "list_bot_run_lifecycle_events", None)
    if not callable(list_events):
        return []
    try:
        return [dict(row) for row in list_events(run_id)]
    except Exception as exc:  # noqa: BLE001 - diagnostics should report source gaps, not fail report builds.
        logger.warning(
            with_log_context(
                "run_research_dataset_lifecycle_events_unavailable",
                build_log_context(run_id=run_id, error=str(exc)),
            )
        )
        return []


def _report_diagnostics(
    *,
    run: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    readiness: RunResearchReadiness,
    execution: Mapping[str, Any],
    candle_gaps: Mapping[str, Any],
    wallet_accounting: Mapping[str, Any],
    portfolio_metrics: Mapping[str, Any],
    performance: Mapping[str, Any],
    summary: RunResearchSummary,
    position_ordering: Mapping[str, Any],
    observability_events: Sequence[Mapping[str, Any]] = (),
) -> Dict[str, Any]:
    run_id = str(run.get("run_id") or "")
    bot_id = str(run.get("bot_id") or "").strip() or None
    items: List[Dict[str, Any]] = []

    if not readiness.results_ready:
        items.append(
            _diagnostic(
                severity="warning",
                source="report_readiness",
                code=readiness.reason or "results_not_ready",
                message=f"Report results are not ready: {readiness.reason or 'unknown reason'}.",
                affected_identity={"run_id": run_id, "bot_id": bot_id},
                readiness_impact="blocks_results",
                suggested_next_step="Inspect readiness conditions and resolve the first failed prerequisite.",
            )
        )
    if not readiness.safe_to_compare:
        items.append(
            _diagnostic(
                severity="warning",
                source="comparison_readiness",
                code="comparison_blocked",
                message=f"Run is not safe to compare: {readiness.reason or 'unknown reason'}.",
                affected_identity={"run_id": run_id, "bot_id": bot_id},
                readiness_impact="blocks_comparison",
                suggested_next_step="Compare only after report readiness and compatibility checks pass.",
            )
        )
    for section_name in readiness.degraded_sections:
        items.append(
            _diagnostic(
                severity="info",
                source="report_readiness",
                code=f"{section_name}_degraded",
                message=f"Report section is degraded: {section_name}.",
                affected_identity={"run_id": run_id, "bot_id": bot_id, "section": section_name},
                readiness_impact="degrades_section",
            )
        )
    for section_name in readiness.unavailable_sections:
        items.append(
            _diagnostic(
                severity="info",
                source="report_readiness",
                code=f"{section_name}_unavailable".replace(".", "_"),
                message=f"Report section is unavailable: {section_name}.",
                affected_identity={"run_id": run_id, "bot_id": bot_id, "section": section_name},
                readiness_impact="degrades_section",
            )
        )

    if readiness.export_status != "available":
        items.append(
            _diagnostic(
                severity="info",
                source="export",
                code=readiness.export_status or "export_status_unknown",
                message="Report export is unavailable until the dataset is ready.",
                affected_identity={"run_id": run_id, "bot_id": bot_id},
                readiness_impact="blocks_export",
                suggested_next_step="Resolve report readiness blockers, then request the export manifest again.",
            )
        )

    fallback_count = int(execution.get("intrabar_fallback_count") or 0)
    if fallback_count:
        items.append(
            _diagnostic(
                severity="warning",
                source="execution",
                code="intrabar_fallback_pessimistic",
                message=f"{fallback_count} bars used pessimistic intrabar fallback.",
                affected_identity={
                    "run_id": run_id,
                    "reason_distribution": execution.get("fallback_reason_distribution") or {},
                },
                readiness_impact="degrades_metrics",
                suggested_next_step="Review missing or ambiguous lower-timeframe candles before treating execution quality as final.",
            )
        )

    gap_rows = candle_gaps.get("gap_counts_by_symbol") or []
    if gap_rows:
        total_gaps = sum(int(row.get("gap_count") or 0) for row in gap_rows if isinstance(row, Mapping))
        if total_gaps:
            first_gap_evidence: List[Dict[str, Any]] = []
            for fact in candle_gaps.get("facts") or []:
                if not isinstance(fact, Mapping):
                    continue
                for gap in fact.get("gaps") or []:
                    if isinstance(gap, Mapping):
                        first_gap_evidence.append(
                            {
                                "symbol": fact.get("symbol"),
                                "instrument_id": fact.get("instrument_id"),
                                "series_key": fact.get("series_key"),
                                "timeframe": fact.get("timeframe"),
                                "gap": _json_safe(gap),
                            }
                        )
                        break
                if len(first_gap_evidence) >= 3:
                    break
            items.append(
                _diagnostic(
                    severity="warning",
                    source="data_quality",
                    code="candle_gaps_detected",
                    message=f"{total_gaps} candle continuity gaps were detected.",
                    affected_identity={
                        "run_id": run_id,
                        "symbols": gap_rows,
                        "first_gap_evidence": first_gap_evidence,
                    },
                    readiness_impact="degrades_metrics",
                    suggested_next_step="Review data quality before comparing or exporting results for analysis.",
                )
            )
    elif "candle_gap_observability_unavailable" in set(candle_gaps.get("caveats") or []):
        items.append(
            _diagnostic(
                severity="info",
                source="data_quality",
                code="candle_gap_observability_unavailable",
                message="No candle continuity summary was available for this run.",
                affected_identity={"run_id": run_id},
                readiness_impact="degrades_diagnostics",
            )
        )

    wallet_blocking_caveats = {
        "wallet_decision_trace_incomplete",
        "wallet_drift_detected",
        "wallet_ledger_state_malformed",
        "wallet_ledger_state_mismatch",
        "margin_rejection_evidence_incomplete",
        "wallet_margin_rejection_trace_incomplete",
        "wallet_replay_failed",
    }
    for caveat in wallet_accounting.get("caveats") or []:
        impact = "blocks_golden" if str(caveat) in wallet_blocking_caveats else "degrades_section"
        items.append(
            _diagnostic(
                severity="warning" if impact == "blocks_golden" else "info",
                source="wallet",
                code=str(caveat),
                message=f"Wallet accounting caveat: {caveat}.",
                affected_identity={"run_id": run_id},
                readiness_impact=impact,
            )
        )

    if performance.get("performance_caveats"):
        for caveat in performance.get("performance_caveats") or []:
            items.append(
                _diagnostic(
                    severity="info",
                    source="performance",
                    code=str(caveat),
                    message=f"Performance diagnostics caveat: {caveat}.",
                    affected_identity={"run_id": run_id},
                    readiness_impact="degrades_section",
                )
            )
    for caveat in portfolio_metrics.get("caveats") or []:
        items.append(
            _diagnostic(
                severity="info",
                source="portfolio_metrics",
                code=str(caveat),
                message=f"Portfolio metric caveat: {caveat}.",
                affected_identity={"run_id": run_id},
                readiness_impact="degrades_metrics",
            )
        )

    missing_metrics = [
        name
        for name, value in {
            "win_rate": summary.win_rate,
            "profit_factor": summary.profit_factor,
            "expectancy": summary.expectancy,
            "max_drawdown_pct": summary.max_drawdown_pct,
        }.items()
        if value is None
    ]
    if missing_metrics:
        items.append(
            _diagnostic(
                severity="info",
                source="metrics",
                code="metrics_partially_unavailable",
                message=f"Some metrics are unavailable: {', '.join(missing_metrics)}.",
                affected_identity={"run_id": run_id, "metrics": missing_metrics},
                readiness_impact="degrades_section",
            )
        )

    ordering = _runtime_ordering_health(events)
    if ordering["status"] != "ready":
        impact = "blocks_golden" if ordering["status"] in {"unavailable", "inconsistent"} else "degrades_diagnostics"
        code = {
            "unavailable": "runtime_ordering_unavailable",
            "inconsistent": "runtime_ordering_inconsistent",
            "backfilled": "runtime_ordering_backfilled",
        }.get(str(ordering["status"]), "runtime_ordering_degraded")
        items.append(
            _diagnostic(
                severity="warning" if impact == "blocks_golden" else "info",
                source="runtime_events",
                code=code,
                message="Runtime event replay ordering is not fully runtime-assigned and dense.",
                affected_identity={"run_id": run_id, "bot_id": bot_id, **ordering},
                readiness_impact=impact,
                suggested_next_step="Use runtime-assigned run_seq for new runs; backfill old runs only with explicit legacy caveats.",
            )
        )

    if str(position_ordering.get("status") or "") == "inconsistent":
        for caveat in position_ordering.get("caveats") or []:
            items.append(
                _diagnostic(
                    severity="warning",
                    source="position_ordering",
                    code=str(caveat),
                    message=f"Trade lifecycle position ordering caveat: {caveat}.",
                    affected_identity={"run_id": run_id, "bot_id": bot_id, **dict(position_ordering)},
                    readiness_impact="blocks_golden",
                    suggested_next_step="Use position_commit_seq for material trade lifecycle replay; timestamps alone are not causal ordering.",
                )
            )

    lifecycle_events = _lifecycle_events_for_run(run_id)
    runtime_failures = _unclassified_runtime_failures(events)
    recoverable_watchdog_failures = _recoverable_watchdog_failures(events)
    for recoverable in recoverable_watchdog_failures:
        failure = _failure_payload(recoverable)
        reason_code = str(failure.get("reason_code") or "").strip().lower()
        diagnostic_code = (
            "recoverable_watchdog_startup_ambiguity"
            if reason_code == "startup_container_ambiguous"
            else "recoverable_watchdog_stale_heartbeat"
        )
        items.append(
            _diagnostic(
                severity="warning",
                source="lifecycle",
                code=diagnostic_code,
                message="Recoverable watchdog condition was classified as degraded lifecycle health.",
                affected_identity={
                    "run_id": run_id,
                    "bot_id": bot_id,
                    "seq": recoverable.get("seq"),
                    "reason": failure.get("reason"),
                },
                timestamp=recoverable.get("event_time") or recoverable.get("created_at"),
                readiness_impact="degrades_diagnostics",
                suggested_next_step="Confirm a later lifecycle checkpoint proves the run continued before certifying operational health.",
            )
        )
    if not lifecycle_events:
        items.append(
            _diagnostic(
                severity="info",
                source="lifecycle",
                code="lifecycle_events_unavailable",
                message="No lifecycle event rows were available for this run.",
                affected_identity={"run_id": run_id, "bot_id": bot_id},
                readiness_impact="degrades_diagnostics",
            )
        )
        if str(run.get("status") or "").strip().lower() == "completed" and runtime_failures:
            first_failure = runtime_failures[0]
            items.append(
                _diagnostic(
                    severity="warning",
                    source="lifecycle",
                    code="lifecycle_contradiction",
                    message="Run contains failure/fault runtime facts and a later completed terminal state.",
                    affected_identity={
                        "run_id": run_id,
                        "bot_id": bot_id,
                        "failure_seq": first_failure.get("seq"),
                        "failure_status": _event_name(first_failure),
                        "failure": _mapping(_payload(first_failure).get("context")).get("failure"),
                    },
                    timestamp=first_failure.get("event_time") or first_failure.get("created_at"),
                    readiness_impact="blocks_golden",
                    suggested_next_step="Classify or eliminate the failed lifecycle transition before certifying this as a golden run.",
                )
            )
    else:
        latest = max(lifecycle_events, key=lambda row: int(row.get("seq") or 0))
        run_status = str(run.get("status") or "").strip().lower()
        latest_status = str(latest.get("status") or "").strip().lower()
        failure_rows = [
            row
            for row in lifecycle_events
            if str(row.get("status") or "").strip().lower() in {"failed", "crashed"}
            or str(row.get("phase") or "").strip().lower() in {"failed", "crashed"}
        ]
        if run_status == "completed" and (failure_rows or runtime_failures):
            first_failure = (failure_rows or runtime_failures)[0]
            items.append(
                _diagnostic(
                    severity="warning",
                    source="lifecycle",
                    code="lifecycle_contradiction",
                    message="Run contains failure/fault lifecycle facts and a later completed terminal state.",
                    affected_identity={
                        "run_id": run_id,
                        "bot_id": bot_id,
                        "failure_seq": first_failure.get("seq"),
                        "failure_status": first_failure.get("status") or _event_name(first_failure),
                        "failure": first_failure.get("failure") or _mapping(_payload(first_failure).get("context")).get("failure"),
                    },
                    timestamp=first_failure.get("checkpoint_at") or first_failure.get("created_at") or first_failure.get("event_time"),
                    readiness_impact="blocks_golden",
                    suggested_next_step="Classify or eliminate the failed lifecycle transition before certifying this as a golden run.",
                )
            )
        if run_status and latest_status and run_status != latest_status:
            items.append(
                _diagnostic(
                    severity="warning",
                    source="lifecycle",
                    code="lifecycle_status_mismatch",
                    message=f"Run status `{run_status}` differs from latest lifecycle status `{latest_status}`.",
                    affected_identity={"run_id": run_id, "bot_id": bot_id, "seq": latest.get("seq")},
                    timestamp=latest.get("timestamp") or latest.get("created_at"),
                    readiness_impact="degrades_diagnostics",
                    suggested_next_step="Inspect lifecycle events before using the report for comparison.",
                )
            )

    projection_replay_resolved = _projection_replay_resolved(observability_events)
    closed_trade_ids = _closed_trade_ids_from_events(events)
    for event in observability_events:
        level = str(event.get("level") or "INFO").strip().upper()
        event_name = str(event.get("event_name") or "observability_event")
        source = str(event.get("component") or "observability")
        failure_mode = str(event.get("failure_mode") or "").strip()
        is_projection_failure = "projector_failed" in event_name or "projection" in failure_mode
        is_queue_overflow = event_name == "run_notification_queue_overflow"
        is_botlens_projection = is_projection_failure and ("botlens" in source.lower() or event_name == "run_projector_failed")
        if level not in {"WARN", "WARNING", "ERROR", "CRITICAL"} and not is_projection_failure:
            continue
        severity = "warning" if is_botlens_projection else "critical" if level in {"ERROR", "CRITICAL"} else "warning"
        if (is_botlens_projection or is_queue_overflow) and not projection_replay_resolved:
            readiness_impact = "blocks_golden"
        elif is_botlens_projection or is_queue_overflow:
            readiness_impact = "degrades_diagnostics"
        else:
            readiness_impact = "degrades_diagnostics"
        message = str(event.get("message") or failure_mode or event_name)
        if is_botlens_projection:
            message = f"Supporting BotLens projection failed: {message}."
        items.append(
            _diagnostic(
                severity=severity,
                source=source,
                code=event_name,
                message=message,
                affected_identity={
                    "run_id": run_id,
                    "bot_id": event.get("bot_id") or bot_id,
                    "instrument_id": event.get("instrument_id"),
                    "series_key": event.get("series_key"),
                    "details": event.get("details") or {},
                },
                timestamp=event.get("observed_at"),
                readiness_impact=readiness_impact,
                suggested_next_step="Inspect projection and observability records if this diagnostic affects the report section being used.",
            )
        )
        if is_botlens_projection:
            trade_ids = _projection_failure_trade_ids(event)
            closed_projection_trade_ids = sorted(trade_id for trade_id in trade_ids if trade_id in closed_trade_ids)
            if closed_projection_trade_ids:
                items.append(
                    _diagnostic(
                        severity="warning",
                        source="botlens_projection",
                        code="projection_truth_mismatch",
                        message="BotLens projection reported open trades that canonical runtime events show as closed.",
                        affected_identity={
                            "run_id": run_id,
                            "bot_id": event.get("bot_id") or bot_id,
                            "trade_ids": closed_projection_trade_ids,
                            "projection_event": event_name,
                            "replay_resolved": projection_replay_resolved,
                        },
                        timestamp=event.get("observed_at"),
                        readiness_impact="degrades_diagnostics" if projection_replay_resolved else "blocks_golden",
                        suggested_next_step="Rebuild the projection from canonical runtime events and verify open-trade state is empty.",
                    )
                )

    items.sort(
        key=lambda item: (
            {"critical": 0, "warning": 1, "info": 2}.get(str(item.get("severity") or ""), 3),
            str(item.get("source") or ""),
            str(item.get("code") or ""),
        )
    )
    return {
        "schema_version": "report_diagnostics.v1",
        "run_id": run_id,
        "items": items,
        "summary": _diagnostic_summary(items),
    }


def _sections(
    *,
    readiness: RunResearchReadiness,
    decisions: Sequence[Mapping[str, Any]],
    signals: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    timeseries: Mapping[str, Any],
    context: Mapping[str, Any],
    candle_catalog: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    execution: Mapping[str, Any],
    candle_gaps: Mapping[str, Any],
    wallet_diagnostics: Optional[Mapping[str, Any]] = None,
    operational_health: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    def section(
        name: str,
        *,
        available: bool,
        row_count: Optional[int] = None,
        reason: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": name,
            "available": bool(available),
            "status": status or ("available" if available else "unavailable"),
        }
        if row_count is not None:
            payload["row_count"] = int(row_count)
        if reason:
            payload["reason"] = reason
        return payload

    timeseries_items = _mapping(timeseries.get("items"))
    timeseries_rows = sum(int(_mapping(section_payload).get("row_count") or 0) for section_payload in timeseries_items.values())
    context_sections = [
        _mapping(context.get("indicator_snapshots")),
        _mapping(context.get("decision_context")),
        _mapping(context.get("trade_context")),
        _mapping(context.get("market_state")),
    ]
    context_rows = sum(int(section_payload.get("row_count") or 0) for section_payload in context_sections)
    wallet = _mapping(wallet_diagnostics)
    operational = _mapping(operational_health)

    return {
        "schema_version": "report_sections.v1",
        "items": [
            section("metadata", available=True),
            section("readiness", available=True),
            section("summary", available=readiness.results_ready or readiness.dataset_ready, status=readiness.results_status),
            section("timeseries", available=timeseries_rows > 0, row_count=timeseries_rows, reason=None if timeseries_rows else "timeseries_sections_unavailable"),
            section("trades", available=True, row_count=len(trades)),
            section("decisions", available=True, row_count=len(decisions)),
            section("signals", available=True, row_count=len(signals)),
            section("indicator_context", available=bool(context.get("indicator_snapshots", {}).get("available")), row_count=int(context.get("indicator_snapshots", {}).get("row_count") or 0), reason=context.get("indicator_snapshots", {}).get("reason")),
            section("decision_context", available=bool(context.get("decision_context", {}).get("available")), row_count=int(context.get("decision_context", {}).get("row_count") or 0), reason=context.get("decision_context", {}).get("reason")),
            section("trade_context", available=bool(context.get("trade_context", {}).get("available")), row_count=int(context.get("trade_context", {}).get("row_count") or 0), reason=context.get("trade_context", {}).get("reason")),
            section("market_state", available=bool(context.get("market_state", {}).get("available")), row_count=int(context.get("market_state", {}).get("row_count") or 0), reason=context.get("market_state", {}).get("reason")),
            section("candle_catalog", available=bool(candle_catalog.get("items")), row_count=len(candle_catalog.get("items") or []), reason=None if candle_catalog.get("items") else "candle_catalog_unavailable"),
            section("diagnostics", available=True, row_count=int(_mapping(diagnostics.get("summary")).get("total") or 0)),
            section("wallet_diagnostics", available=bool(wallet), row_count=int(wallet.get("wallet_event_count") or 0), reason=None if wallet else "wallet_diagnostics_unavailable"),
            section("metrics", available=True),
            section("execution", available=True, row_count=int(execution.get("intrabar_fallback_count") or 0)),
            section("data_quality", available=True, row_count=len(candle_gaps.get("gap_counts_by_symbol") or []), status=readiness.data_quality_status),
            section("operational_health", available=bool(operational), row_count=len(operational.get("per_stage_latency") or []), reason=None if operational else "operational_health_unavailable"),
            section("export", available=readiness.export_status in {"available", "partial"}, status=readiness.export_status, reason=None if readiness.export_status == "available" else readiness.reason),
            section("comparison", available=readiness.comparison_status != "blocked", status=readiness.comparison_status, reason=None if readiness.comparison_status != "blocked" else readiness.reason),
        ],
    }


def _percentile(values: Sequence[float], percentile: float) -> Optional[float]:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    idx = (len(clean) - 1) * percentile
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return clean[int(idx)]
    return clean[lower] + ((clean[upper] - clean[lower]) * (idx - lower))


def _portfolio_metrics(
    *,
    run: Mapping[str, Any],
    trades: Sequence[Mapping[str, Any]],
    summary: RunResearchSummary,
) -> Dict[str, Any]:
    config = _mapping(run.get("config_snapshot"))
    date_range = _mapping(config.get("date_range"))
    start_time = _parse_iso(run.get("backtest_start") or date_range.get("start") or run.get("started_at"))
    end_time = _parse_iso(run.get("backtest_end") or date_range.get("end") or run.get("ended_at"))
    metric_config = dict(config)
    wallet = _mapping(metric_config.get("wallet_start")) or _mapping(metric_config.get("wallet_config"))
    balances = _mapping(wallet.get("balances"))
    if summary.equity_start is not None and len(balances) != 1:
        metric_config["wallet_start"] = {"balances": {"portfolio": summary.equity_start}}

    values = compute_portfolio_metric_summary(
        trades,
        metric_config,
        start_time=start_time,
        end_time=end_time,
    )
    caveats: List[str] = []
    if summary.closed_trades < 2:
        caveats.append("risk_metrics_require_multiple_closed_trades")
    if start_time is None or end_time is None:
        caveats.append("simulated_window_unavailable")
    if summary.equity_start in (None, 0):
        caveats.append("starting_capital_unavailable")
    for metric_name in ("sharpe", "sortino", "calmar", "annualized_volatility"):
        if values.get(metric_name) is None:
            caveats.append(f"{metric_name}_unavailable")

    return {
        "schema_version": "portfolio_metrics.v1",
        "basis": {
            "return_series": "daily_closed_trade_net_pnl_over_starting_equity",
            "pnl_source": "closed_trades.net_pnl",
            "window_start": _iso(start_time),
            "window_end": _iso(end_time),
            "risk_free_rate": 0.0,
        },
        "annualization_periods": ANNUALIZATION_PERIODS,
        **values,
        "caveats": sorted(dict.fromkeys(caveats)),
    }


def _summary_with_portfolio_metrics(summary: RunResearchSummary, portfolio_metrics: Mapping[str, Any]) -> RunResearchSummary:
    unavailable = dict(summary.unavailable_metrics)
    metric_map = {
        "cagr": portfolio_metrics.get("cagr"),
        "annualized_volatility": portfolio_metrics.get("annualized_volatility"),
        "sharpe": portfolio_metrics.get("sharpe"),
        "sortino": portfolio_metrics.get("sortino"),
        "calmar": portfolio_metrics.get("calmar"),
        "exposure_pct": portfolio_metrics.get("exposure_pct"),
    }
    for key, value in metric_map.items():
        if value is not None:
            unavailable.pop(key, None)
    drawdown_days = _safe_float(portfolio_metrics.get("drawdown_duration_days"))
    drawdown_seconds = drawdown_days * 86400.0 if drawdown_days is not None else None
    if drawdown_seconds is not None:
        unavailable.pop("drawdown_duration_seconds", None)
    return replace(
        summary,
        cagr=_safe_float(metric_map["cagr"]),
        annualized_volatility=_safe_float(metric_map["annualized_volatility"]),
        sharpe=_safe_float(metric_map["sharpe"]),
        sortino=_safe_float(metric_map["sortino"]),
        calmar=_safe_float(metric_map["calmar"]),
        exposure_pct=_safe_float(metric_map["exposure_pct"]),
        drawdown_duration_seconds=drawdown_seconds,
        unavailable_metrics=unavailable,
    )


def _series_payload(*, schema_version: str, rows: Sequence[Mapping[str, Any]], reason: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_version": schema_version,
        "available": bool(rows),
        "row_count": len(rows),
        "items": [dict(row) for row in rows],
    }
    if reason and not rows:
        payload["reason"] = reason
    return payload


def _rolling(values: Sequence[float], *, window: int, index: int) -> List[float]:
    start = max(0, index + 1 - window)
    return [float(value) for value in values[start : index + 1]]


def _timeseries(
    *,
    metadata: RunResearchMetadata,
    trades: Sequence[Mapping[str, Any]],
    summary: RunResearchSummary,
    window: int = 10,
) -> Dict[str, Any]:
    run_id = metadata.run_id
    closed = [
        dict(trade)
        for trade in trades
        if trade.get("exit_time") and _safe_float(trade.get("net_pnl")) is not None
    ]
    closed.sort(key=lambda item: str(item.get("exit_time") or ""))

    equity_rows: List[Dict[str, Any]] = []
    returns_rows: List[Dict[str, Any]] = []
    drawdown_rows: List[Dict[str, Any]] = []
    rolling_win_rate: List[Dict[str, Any]] = []
    rolling_expectancy: List[Dict[str, Any]] = []
    rolling_profit_factor: List[Dict[str, Any]] = []
    rolling_sharpe: List[Dict[str, Any]] = []
    rolling_volatility: List[Dict[str, Any]] = []
    rolling_drawdown: List[Dict[str, Any]] = []

    equity = _safe_float(summary.equity_start) or 0.0
    peak = equity
    start_ts = metadata.simulated_window.get("start") or metadata.started_at
    if summary.equity_start is not None and start_ts:
        equity_rows.append(
            {
                "run_id": run_id,
                "timestamp": start_ts,
                "value": equity,
                "equity": equity,
                "source": "starting_capital",
            }
        )
        drawdown_rows.append(
            {
                "run_id": run_id,
                "timestamp": start_ts,
                "value": 0.0,
                "drawdown": 0.0,
                "drawdown_pct": 0.0,
                "source": "starting_capital",
            }
        )

    trade_returns: List[float] = []
    net_values: List[float] = []
    for index, trade in enumerate(closed):
        timestamp = trade.get("exit_time")
        previous_equity = equity
        net = _safe_float(trade.get("net_pnl")) or 0.0
        equity += net
        peak = max(peak, equity)
        drawdown = equity - peak
        drawdown_pct = (drawdown / peak) if peak else None
        ret = net / previous_equity if previous_equity else 0.0
        trade_returns.append(ret)
        net_values.append(net)
        base = {
            "run_id": run_id,
            "instrument_id": trade.get("instrument_id"),
            "symbol": trade.get("symbol"),
            "timeframe": trade.get("timeframe"),
            "timestamp": timestamp,
            "trade_id": trade.get("trade_id"),
            "source": "closed_trades",
        }
        equity_rows.append(base | {"value": equity, "equity": equity, "net_pnl": net})
        returns_rows.append(base | {"value": ret, "return": ret, "net_pnl": net})
        drawdown_rows.append(base | {"value": drawdown, "drawdown": drawdown, "drawdown_pct": drawdown_pct})
        window_net = _rolling(net_values, window=window, index=index)
        window_returns = _rolling(trade_returns, window=window, index=index)
        wins = [value for value in window_net if value > 0]
        rolling_win_rate.append(base | {"value": (len(wins) / len(window_net)) if window_net else None, "window": len(window_net)})
        rolling_expectancy.append(base | {"value": compute_expectancy(window_net) if window_net else None, "window": len(window_net)})
        rolling_profit_factor.append(base | {"value": compute_profit_factor(window_net), "window": len(window_net)})
        rolling_sharpe.append(
            base
            | {
                "value": (statistics.mean(window_returns) / statistics.pstdev(window_returns)) if len(window_returns) >= 2 and statistics.pstdev(window_returns) else None,
                "window": len(window_returns),
            }
        )
        rolling_volatility.append(
            base
            | {
                "value": statistics.pstdev(window_returns) if len(window_returns) >= 2 else None,
                "window": len(window_returns),
            }
        )
        rolling_drawdown.append(base | {"value": drawdown, "drawdown": drawdown, "drawdown_pct": drawdown_pct, "window": len(window_net)})

    position_events: List[Dict[str, Any]] = []
    for trade in trades:
        if trade.get("entry_time"):
            position_events.append(
                {
                    "run_id": run_id,
                    "instrument_id": trade.get("instrument_id"),
                    "symbol": trade.get("symbol"),
                    "timeframe": trade.get("timeframe"),
                    "timestamp": trade.get("entry_time"),
                    "trade_id": trade.get("trade_id"),
                    "open_delta": 1,
                    "quantity_delta": _safe_float(trade.get("quantity")) or 0.0,
                    "source": "trade_entry",
                }
            )
        if trade.get("exit_time"):
            position_events.append(
                {
                    "run_id": run_id,
                    "instrument_id": trade.get("instrument_id"),
                    "symbol": trade.get("symbol"),
                    "timeframe": trade.get("timeframe"),
                    "timestamp": trade.get("exit_time"),
                    "trade_id": trade.get("trade_id"),
                    "open_delta": -1,
                    "quantity_delta": -(_safe_float(trade.get("quantity")) or 0.0),
                    "source": "trade_exit",
                }
            )
    position_events.sort(key=lambda item: str(item.get("timestamp") or ""))
    open_positions = 0
    quantity = 0.0
    positions_rows: List[Dict[str, Any]] = []
    exposure_rows: List[Dict[str, Any]] = []
    for event in position_events:
        open_positions += int(event.get("open_delta") or 0)
        quantity += float(event.get("quantity_delta") or 0.0)
        row = dict(event) | {
            "value": open_positions,
            "open_positions": open_positions,
            "open_quantity": quantity,
        }
        positions_rows.append(row)
        exposure_rows.append(row | {"exposed": open_positions > 0})

    no_trade_reason = "requires closed trades"
    return {
        "schema_version": "report_timeseries.v1",
        "window": window,
        "items": {
            "equity_curve": _series_payload(schema_version="equity_curve.v1", rows=equity_rows, reason="requires starting capital or closed trades"),
            "drawdown_curve": _series_payload(schema_version="drawdown_curve.v1", rows=drawdown_rows, reason=no_trade_reason),
            "returns_series": _series_payload(schema_version="returns_series.v1", rows=returns_rows, reason=no_trade_reason),
            "capital_timeline": _series_payload(schema_version="capital_timeline.v1", rows=equity_rows, reason="requires starting capital"),
            "exposure_timeline": _series_payload(schema_version="exposure_timeline.v1", rows=exposure_rows, reason="requires trade entry and exit times"),
            "positions_timeline": _series_payload(schema_version="positions_timeline.v1", rows=positions_rows, reason="requires trade entry and exit times"),
            "rolling_win_rate": _series_payload(schema_version="rolling_win_rate.v1", rows=rolling_win_rate, reason=no_trade_reason),
            "rolling_expectancy": _series_payload(schema_version="rolling_expectancy.v1", rows=rolling_expectancy, reason=no_trade_reason),
            "rolling_profit_factor": _series_payload(schema_version="rolling_profit_factor.v1", rows=rolling_profit_factor, reason=no_trade_reason),
            "rolling_sharpe": _series_payload(schema_version="rolling_sharpe.v1", rows=rolling_sharpe, reason="requires at least two return observations"),
            "rolling_volatility": _series_payload(schema_version="rolling_volatility.v1", rows=rolling_volatility, reason="requires at least two return observations"),
            "rolling_drawdown": _series_payload(schema_version="rolling_drawdown.v1", rows=rolling_drawdown, reason=no_trade_reason),
        },
    }


def _performance(run: Mapping[str, Any], events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    steps_func = getattr(storage, "list_bot_run_steps_for_run", None)
    steps = steps_func(str(run.get("run_id") or "")) if callable(steps_func) else []
    durations = [_safe_float(step.get("p95_value", step.get("duration_ms"))) for step in steps]
    clean_durations = [float(value) for value in durations if value is not None]
    by_step: Dict[str, Dict[str, Any]] = {}
    step_count = 0
    for step in steps:
        name = str(step.get("step_name") or "unknown")
        sample_count = int(_safe_float(step.get("sample_count")) or 1)
        duration = _safe_float(step.get("p95_value", step.get("duration_ms"))) or 0.0
        total_ms = _safe_float(step.get("value_sum")) or duration * sample_count
        stats = by_step.setdefault(name, {"step_name": name, "count": 0, "total_ms": 0.0, "durations": []})
        stats["count"] += sample_count
        step_count += sample_count
        stats["total_ms"] += total_ms
        stats["durations"].append(duration)
    major = []
    for stats in by_step.values():
        values = list(stats.pop("durations"))
        stats["avg_ms"] = statistics.mean(values) if values else None
        stats["p95_ms"] = _percentile(values, 0.95)
        major.append(stats)
    major.sort(key=lambda item: item.get("total_ms") or 0.0, reverse=True)
    return {
        "wall_clock_duration_seconds": _duration_seconds(run.get("started_at"), run.get("ended_at")),
        "event_count": len(events),
        "step_count": step_count or len(steps),
        "p50_ms": _percentile(clean_durations, 0.50),
        "p95_ms": _percentile(clean_durations, 0.95),
        "p99_ms": _percentile(clean_durations, 0.99),
        "major_step_timings": major[:10],
        "performance_caveats": [] if steps else ["runtime_step_timings_unavailable"],
    }


def _per_symbol_performance(trades: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for trade in trades:
        symbol = str(trade.get("symbol") or "UNKNOWN")
        net = _safe_float(trade.get("net_pnl")) or 0.0
        gross = _safe_float(trade.get("gross_pnl")) or 0.0
        fees = _safe_float(trade.get("fees_paid")) or 0.0
        stats = grouped.setdefault(symbol, {"symbol": symbol, "trades": 0, "wins": 0, "losses": 0, "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0})
        stats["trades"] += 1
        stats["wins"] += 1 if net > 0 else 0
        stats["losses"] += 1 if net < 0 else 0
        stats["gross_pnl"] += gross
        stats["fees"] += fees
        stats["net_pnl"] += net
    rows = []
    for stats in grouped.values():
        stats["win_rate"] = stats["wins"] / stats["trades"] if stats["trades"] else None
        rows.append(stats)
    return sorted(rows, key=lambda item: item["net_pnl"])


def _close_reason_breakdown(trades: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for trade in trades:
        reason = str(trade.get("close_reason") or "UNKNOWN").upper()
        net = _safe_float(trade.get("net_pnl")) or 0.0
        gross = _safe_float(trade.get("gross_pnl")) or 0.0
        fees = _safe_float(trade.get("fees_paid")) or 0.0
        stats = grouped.setdefault(reason, {"close_reason": reason, "trades": 0, "wins": 0, "losses": 0, "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0})
        stats["trades"] += 1
        stats["wins"] += 1 if net > 0 else 0
        stats["losses"] += 1 if net < 0 else 0
        stats["gross_pnl"] += gross
        stats["fees"] += fees
        stats["net_pnl"] += net
    return sorted(grouped.values(), key=lambda item: item["net_pnl"])


def _rule_breakdown(decisions: Sequence[Mapping[str, Any]], trades_by_id: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for decision in decisions:
        key = str(decision.get("rule_id") or decision.get("rule_name") or "UNKNOWN")
        stats = grouped.setdefault(key, {"rule": key, "decisions": 0, "accepted": 0, "rejected": 0, "net_pnl": 0.0})
        stats["decisions"] += 1
        if decision.get("accepted"):
            stats["accepted"] += 1
            trade = trades_by_id.get(str(decision.get("trade_id") or ""))
            if trade:
                stats["net_pnl"] += _safe_float(trade.get("net_pnl")) or 0.0
        else:
            stats["rejected"] += 1
    return sorted(grouped.values(), key=lambda item: item["net_pnl"])


def _loss_clusters(trades: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    losses = [
        trade
        for trade in sorted(trades, key=lambda item: str(item.get("exit_time") or ""))
        if (_safe_float(trade.get("net_pnl")) or 0.0) < 0 and _parse_iso(trade.get("exit_time"))
    ]
    clusters: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for trade in losses:
        start = _parse_iso(trade.get("exit_time"))
        if not start:
            continue
        end = start + timedelta(days=7)
        window = [candidate for candidate in losses if start <= (_parse_iso(candidate.get("exit_time")) or start) <= end]
        if len(window) < 2:
            continue
        close_reasons = Counter(str(candidate.get("close_reason") or "UNKNOWN") for candidate in window)
        symbols = Counter(str(candidate.get("symbol") or "UNKNOWN") for candidate in window)
        key = (_iso(start) or "", _iso(end) or "", ",".join(sorted(close_reasons)))
        if key in seen:
            continue
        seen.add(key)
        clusters.append(
            {
                "start": _iso(start),
                "end": _iso(end),
                "trades": len(window),
                "net_pnl": sum(_safe_float(candidate.get("net_pnl")) or 0.0 for candidate in window),
                "symbols": dict(symbols),
                "close_reasons": dict(close_reasons),
                "trade_ids": [candidate.get("trade_id") for candidate in window[:10]],
            }
        )
    clusters.sort(key=lambda item: item["net_pnl"])
    return clusters[:5]


def _strategy_insights(
    *,
    trades: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    execution: Mapping[str, Any],
    summary: RunResearchSummary,
) -> Dict[str, Any]:
    trades_by_id = {str(trade.get("trade_id") or ""): trade for trade in trades}
    per_symbol = _per_symbol_performance(trades)
    close_reasons = _close_reason_breakdown(trades)
    fee_burden = {
        "fees": summary.fees,
        "fees_to_abs_gross_pnl": (summary.fees / sum(abs(_safe_float(trade.get("gross_pnl")) or 0.0) for trade in trades))
        if trades and sum(abs(_safe_float(trade.get("gross_pnl")) or 0.0) for trade in trades)
        else None,
        "fees_to_gross_profit": (summary.fees / sum((_safe_float(trade.get("net_pnl")) or 0.0) for trade in trades if (_safe_float(trade.get("net_pnl")) or 0.0) > 0))
        if sum((_safe_float(trade.get("net_pnl")) or 0.0) for trade in trades if (_safe_float(trade.get("net_pnl")) or 0.0) > 0)
        else None,
    }
    stop_rows = [row for row in close_reasons if str(row.get("close_reason") or "").upper() in {"STOP", "STOP_LOSS", "SL"}]
    stop_loss_burden = {
        "trades": sum(int(row.get("trades") or 0) for row in stop_rows),
        "gross_pnl": sum(float(row.get("gross_pnl") or 0.0) for row in stop_rows),
        "fees": sum(float(row.get("fees") or 0.0) for row in stop_rows),
        "net_pnl": sum(float(row.get("net_pnl") or 0.0) for row in stop_rows),
    }
    best = sorted(trades, key=lambda item: _safe_float(item.get("net_pnl")) or 0.0, reverse=True)[:5]
    worst = sorted(trades, key=lambda item: _safe_float(item.get("net_pnl")) or 0.0)[:5]
    investigations = []
    if stop_loss_burden["trades"] and stop_loss_burden["net_pnl"] < 0:
        investigations.append("Inspect stop-loss exits and clustered stop periods.")
    if fee_burden["fees_to_abs_gross_pnl"] is not None and fee_burden["fees_to_abs_gross_pnl"] > 0.20:
        investigations.append("Review fee burden by symbol and position sizing.")
    if int(execution.get("intrabar_fallback_count") or 0) > 0:
        investigations.append("Audit FULL-mode intrabar fallback bars for missing or ambiguous 1m data.")
    weak_symbols = [row["symbol"] for row in per_symbol if float(row.get("net_pnl") or 0.0) < 0]
    if weak_symbols:
        investigations.append(f"Compare rule behavior on weakest symbols: {', '.join(weak_symbols[:5])}.")
    if any(decision.get("rejected") for decision in decisions):
        investigations.append("Review rejected decision reasons for wallet/risk gating pressure.")
    return {
        "per_symbol_performance": per_symbol,
        "close_reason_breakdown": close_reasons,
        "rule_breakdown": _rule_breakdown(decisions, trades_by_id),
        "loss_clusters": _loss_clusters(trades),
        "worst_trades": [_research_trade_compact(trade) for trade in worst],
        "best_trades": [_research_trade_compact(trade) for trade in best],
        "fee_burden": fee_burden,
        "stop_loss_burden": stop_loss_burden,
        "candidate_next_investigations": investigations,
    }


_MARKET_CONTEXT_KEYS = {
    "market_profile",
    "value_area",
    "pivot",
    "support",
    "resistance",
    "regime",
    "trend",
    "vwap",
    "confluence",
    "level_proximity",
}


_OUTPUT_CONTEXT_MAP_KEYS = (
    "indicator_outputs",
    "runtime_outputs",
    "outputs",
    "resolved_indicator_values",
    "context_values",
    "observed_outputs",
    "referenced_outputs",
)


def _context_output_maps(context: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    maps: List[Mapping[str, Any]] = []
    containers: List[Mapping[str, Any]] = [context]
    for nested_key in ("decision_context", "decision_artifact", "rejection_artifact"):
        nested = context.get(nested_key)
        if isinstance(nested, Mapping):
            containers.append(nested)
            nested_context = nested.get("decision_context") or nested.get("context")
            if isinstance(nested_context, Mapping):
                containers.append(nested_context)
    for container in containers:
        for key in _OUTPUT_CONTEXT_MAP_KEYS:
            value = container.get(key)
            if isinstance(value, Mapping):
                maps.append(value)
    return maps


def _indicator_snapshot_value(raw: Any) -> Any:
    if not isinstance(raw, Mapping):
        return raw
    if "value" in raw:
        return raw.get("value")
    if "fields" in raw:
        result: Dict[str, Any] = {}
        if raw.get("state_key") not in (None, ""):
            result["state_key"] = raw.get("state_key")
        result["fields"] = raw.get("fields")
        return result
    if "event_keys" in raw:
        return {"event_keys": raw.get("event_keys")}
    return raw


def _context_output_is_market_state(output_key: Any, raw: Any) -> bool:
    if not isinstance(raw, Mapping):
        return False
    output_type = str(raw.get("output_type") or raw.get("type") or "").strip().lower()
    if output_type == "context":
        return True
    lowered = str(output_key or "").lower()
    if any(token in lowered for token in _MARKET_CONTEXT_KEYS):
        return True
    fields = raw.get("fields")
    if isinstance(fields, Mapping):
        return any(any(token in str(key).lower() for token in _MARKET_CONTEXT_KEYS) for key in fields)
    return False


def _compact_context_values(context: Mapping[str, Any]) -> Dict[str, Any]:
    allowed = {
        "trigger_output_ref",
        "event_key",
        "intent",
        "direction",
        "reason_code",
        "message",
        "signal_price",
        "price",
        "qty",
        "quantity",
        "rule_id",
        "rule_name",
        "source_type",
        "source_id",
        "rejection_stage",
        "attempt_id",
        "order_request_id",
        "entry_request_id",
        "settlement_attempt_id",
        "blocking_trade_id",
        "wallet_snapshot",
        "wallet_before",
        "margin_requirement",
        "required_delta",
    }
    result: Dict[str, Any] = {}
    for key in allowed:
        value = context.get(key)
        if value in (None, "", [], {}):
            continue
        result[key] = _json_safe(value)
    return result


def _market_context_values(context: Mapping[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in context.items():
        lowered = str(key).lower()
        if not any(token in lowered for token in _MARKET_CONTEXT_KEYS):
            continue
        if value in (None, "", [], {}):
            continue
        result[str(key)] = _json_safe(value)
    for output_map in _context_output_maps(context):
        for output_key, raw in output_map.items():
            if not _context_output_is_market_state(output_key, raw):
                continue
            result[str(output_key)] = _json_safe(_indicator_snapshot_value(raw))
    return result


def _indicator_rows_from_context(
    *,
    run_id: str,
    context: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_output_keys: set[str] = set()
    for output_map in _context_output_maps(context):
        for output_key, raw in output_map.items():
            normalized_output_key = str(output_key)
            if normalized_output_key in seen_output_keys:
                continue
            seen_output_keys.add(normalized_output_key)
            indicator_id, _, output_name = str(output_key).partition(".")
            value = _indicator_snapshot_value(raw)
            output_type = None
            if isinstance(raw, Mapping):
                output_type = raw.get("output_type") or raw.get("type")
                indicator_id = str(raw.get("indicator_id") or indicator_id or "")
                output_name = str(raw.get("output_name") or output_name or str(output_key))
            rows.append(
                {
                    "run_id": run_id,
                    "indicator_id": indicator_id or None,
                    "indicator_name": indicator_id or None,
                    "output_name": output_name or str(output_key),
                    "output_type": output_type,
                    "instrument_id": identity.get("instrument_id") or context.get("instrument_id"),
                    "symbol": identity.get("symbol") or context.get("symbol"),
                    "timeframe": identity.get("timeframe") or context.get("timeframe"),
                    "bar_time": (raw.get("bar_time") if isinstance(raw, Mapping) else None) or identity.get("bar_time") or context.get("bar_time"),
                    "known_at": (raw.get("known_at") if isinstance(raw, Mapping) else None) or identity.get("known_at") or context.get("known_at"),
                    "strategy_hash": identity.get("strategy_hash") or context.get("strategy_hash"),
                    "signal_id": identity.get("signal_id") or context.get("signal_id"),
                    "decision_id": identity.get("decision_id") or context.get("decision_id"),
                    "trade_id": identity.get("trade_id") or context.get("trade_id"),
                    "indicator_commit_seq": raw.get("indicator_commit_seq") if isinstance(raw, Mapping) else None,
                    "indicator_commit_seq_status": raw.get("indicator_commit_seq_status") if isinstance(raw, Mapping) else None,
                    "values": _json_safe(value),
                    "source": "decision_context",
                }
            )
    return rows


def _context_dataset(
    *,
    metadata: RunResearchMetadata,
    decisions: Sequence[Mapping[str, Any]],
    signals: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    indicator_rows: List[Dict[str, Any]] = []
    decision_context_rows: List[Dict[str, Any]] = []
    trade_context_rows: List[Dict[str, Any]] = []
    market_state_rows: List[Dict[str, Any]] = []
    decisions_by_trade_id = {str(row.get("trade_id") or ""): dict(row) for row in decisions if row.get("trade_id")}
    decisions_by_decision_id = {str(row.get("decision_id") or ""): dict(row) for row in decisions if row.get("decision_id")}

    for decision in decisions:
        context = _mapping(decision.get("decision_context"))
        identity = {
            "run_id": metadata.run_id,
            "bot_id": decision.get("bot_id") or metadata.bot_id,
            "strategy_id": decision.get("strategy_id") or metadata.strategy_id,
            "strategy_hash": decision.get("strategy_hash") or metadata.strategy_hash,
            "instrument_id": decision.get("instrument_id"),
            "symbol": decision.get("symbol"),
            "timeframe": decision.get("timeframe"),
            "bar_time": decision.get("bar_time"),
            "known_at": decision.get("known_at"),
            "signal_id": decision.get("signal_id"),
            "decision_id": decision.get("decision_id"),
            "trade_id": decision.get("trade_id"),
        }
        values = _compact_context_values(context)
        decision_context_rows.append(
            {
                **identity,
                "action": decision.get("action"),
                "status": decision.get("status"),
                "reason": decision.get("reason"),
                "context_values": values,
                "source_refs": decision.get("source_refs") or [],
            }
        )
        indicator_rows.extend(_indicator_rows_from_context(run_id=metadata.run_id, context=context, identity=identity))
        market_values = _market_context_values(context)
        if market_values:
            market_state_rows.append(identity | {"context_values": market_values, "source": "decision_context"})

    for signal in signals:
        context = _mapping(signal.get("context"))
        identity = {
            "run_id": metadata.run_id,
            "bot_id": signal.get("bot_id") or metadata.bot_id,
            "strategy_id": signal.get("strategy_id") or metadata.strategy_id,
            "strategy_hash": signal.get("strategy_hash") or metadata.strategy_hash,
            "instrument_id": signal.get("instrument_id"),
            "symbol": signal.get("symbol"),
            "timeframe": signal.get("timeframe"),
            "bar_time": signal.get("bar_time"),
            "known_at": signal.get("known_at"),
            "signal_id": signal.get("signal_id"),
            "decision_id": signal.get("decision_id"),
            "trade_id": signal.get("trade_id"),
        }
        indicator_rows.extend(_indicator_rows_from_context(run_id=metadata.run_id, context=context, identity=identity))
        market_values = _market_context_values(context)
        if market_values:
            market_state_rows.append(identity | {"context_values": market_values, "source": "signal_context"})

    for trade in trades:
        decision = decisions_by_trade_id.get(str(trade.get("trade_id") or "")) or decisions_by_decision_id.get(str(trade.get("decision_id") or ""))
        decision_context = _mapping(decision.get("decision_context")) if decision else {}
        trade_context_rows.append(
            {
                "run_id": metadata.run_id,
                "bot_id": trade.get("bot_id") or metadata.bot_id,
                "strategy_id": trade.get("strategy_id") or metadata.strategy_id,
                "strategy_hash": trade.get("strategy_hash") or metadata.strategy_hash,
                "instrument_id": trade.get("instrument_id"),
                "symbol": trade.get("symbol"),
                "timeframe": trade.get("timeframe"),
                "entry_time": trade.get("entry_time"),
                "exit_time": trade.get("exit_time"),
                "signal_id": trade.get("signal_id"),
                "decision_id": trade.get("decision_id") or (decision or {}).get("decision_id"),
                "trade_id": trade.get("trade_id"),
                "exit_reason": trade.get("exit_reason") or trade.get("close_reason"),
                "net_pnl": trade.get("net_pnl"),
                "context_values": _compact_context_values(decision_context),
                "source_refs": trade.get("source_refs") or [],
            }
        )

    caveats = []
    if not indicator_rows:
        caveats.append("indicator_snapshot_runtime_capture_unavailable")
    if not market_state_rows:
        caveats.append("market_state_runtime_capture_unavailable")
    return {
        "schema_version": "report_context.v1",
        "indicator_snapshots": _series_payload(
            schema_version="indicator_snapshot_dataset.v1",
            rows=indicator_rows,
            reason="requires captured indicator output values in runtime decision context",
        ),
        "decision_context": _series_payload(
            schema_version="decision_context_dataset.v1",
            rows=decision_context_rows,
            reason="requires decision rows",
        ),
        "trade_context": _series_payload(
            schema_version="trade_context_dataset.v1",
            rows=trade_context_rows,
            reason="requires trade rows",
        ),
        "market_state": _series_payload(
            schema_version="market_state_dataset.v1",
            rows=market_state_rows,
            reason="requires captured market-state context values",
        ),
        "caveats": caveats,
    }


def _candle_catalog(
    *,
    metadata: RunResearchMetadata,
    traces: Sequence[Mapping[str, Any]],
    candle_gaps: Mapping[str, Any],
) -> Dict[str, Any]:
    facts = [dict(row) for row in candle_gaps.get("facts") or [] if isinstance(row, Mapping)]
    gap_by_series = {str(row.get("series_key") or ""): row for row in facts if row.get("series_key")}
    combos: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in traces:
        instrument_id = str(row.get("instrument_id") or "").strip()
        timeframe = str(row.get("timeframe") or metadata.timeframe or "").strip()
        symbol = _clean_text(row.get("symbol"))
        if not instrument_id and not symbol:
            continue
        key = (instrument_id, timeframe)
        existing = combos.get(key)
        if existing and not existing.get("symbol") and symbol:
            existing["symbol"] = symbol
            continue
        combos.setdefault(
            key,
            {
                "run_id": metadata.run_id,
                "instrument_id": instrument_id or None,
                "symbol": symbol,
                "timeframe": timeframe or None,
            },
        )
    if not combos:
        paired_symbols = list(metadata.symbols)
        paired_instruments = list(metadata.instrument_ids)
        timeframes = metadata.timeframes or ([metadata.timeframe] if metadata.timeframe else [])
        if len(paired_symbols) == len(paired_instruments):
            for instrument_id, symbol in zip(paired_instruments, paired_symbols):
                for timeframe in timeframes or [None]:
                    key = (str(instrument_id or ""), str(timeframe or ""))
                    combos.setdefault(
                        key,
                        {
                            "run_id": metadata.run_id,
                            "instrument_id": instrument_id,
                            "symbol": symbol,
                            "timeframe": timeframe,
                        },
                    )
    items: List[Dict[str, Any]] = []
    storage_summary = getattr(storage, "get_candle_storage_summary", None)
    for entry in combos.values():
        series_key = f"{entry.get('instrument_id')}|{entry.get('timeframe')}" if entry.get("instrument_id") and entry.get("timeframe") else None
        fact = gap_by_series.get(str(series_key or "")) or {}
        stored: Dict[str, Any] = {}
        if callable(storage_summary) and entry.get("instrument_id") and entry.get("timeframe"):
            stored = _mapping(
                storage_summary(
                    instrument_id=str(entry.get("instrument_id")),
                    timeframe=str(entry.get("timeframe")),
                    start=metadata.simulated_window.get("start"),
                    end=metadata.simulated_window.get("end"),
                )
            )
        gap_count = _safe_int(stored.get("gap_count"))
        if gap_count is None:
            gap_count = _safe_int(fact.get("detected_gap_count"))
        fact_gap_counts = _normalized_gap_counts(fact.get("gap_count_by_type"))
        if stored and gap_count == 0:
            fact_gap_counts = {}
        candle_count = _safe_int(stored.get("candle_count"))
        if candle_count is None:
            candle_count = _safe_int(fact.get("candle_count"))
        missing_count = _safe_int(stored.get("missing_count"))
        if missing_count is None:
            missing_count = _safe_int(fact.get("missing_candle_estimate") or fact.get("missing_count"))
        semantics = _candle_gap_semantics(gap_count=gap_count, gap_count_by_type=fact_gap_counts)
        continuity_status = "unknown"
        if gap_count is not None:
            continuity_status = str(semantics.get("status") or "unknown")
        if not entry.get("instrument_id"):
            continuity_status = "unavailable"
        available_resolutions = list(stored.get("available_resolutions") or [])
        if not available_resolutions and entry.get("timeframe"):
            available_resolutions = [entry.get("timeframe")]
        items.append(
            {
                **entry,
                "provider": metadata.provider or metadata.datasource,
                "source": metadata.datasource or metadata.provider,
                "start_time": metadata.simulated_window.get("start"),
                "end_time": metadata.simulated_window.get("end"),
                "candle_count": candle_count,
                "missing_count": missing_count,
                "gap_count": gap_count,
                "gap_count_by_type": fact_gap_counts,
                "blocking_gap_count": semantics.get("blocking_gap_count"),
                "provider_gap_count": semantics.get("provider_gap_count"),
                "expected_gap_count": semantics.get("expected_gap_count"),
                "unclassified_gap_count": semantics.get("unclassified_gap_count"),
                "readiness_impact": semantics.get("readiness_impact"),
                "first_gap_evidence": next((gap for gap in fact.get("gaps") or [] if isinstance(gap, Mapping)), None),
                "continuity_status": continuity_status,
                "available_resolutions": available_resolutions,
                "storage_source": "market_candles_raw" if stored else "candle_continuity_summary" if fact else "unresolved_instrument",
                "series_key": series_key,
            }
        )
    caveats = []
    if any(not item.get("instrument_id") for item in items):
        caveats.append("instrument_identity_unavailable_for_some_candle_sections")
    symbols_by_instrument: Dict[str, set[str]] = defaultdict(set)
    for item in items:
        if item.get("instrument_id") and item.get("symbol"):
            symbols_by_instrument[str(item["instrument_id"])].add(str(item["symbol"]))
    if any(len(symbols) > 1 for symbols in symbols_by_instrument.values()):
        caveats.append("candle_catalog_identity_conflict")
    if any(str(item.get("continuity_status") or "") in {"unknown", "unavailable"} for item in items):
        caveats.append("candle_continuity_catalog_unavailable")
    return {
        "schema_version": "candle_catalog.v1",
        "run_id": metadata.run_id,
        "items": sorted(items, key=lambda item: (str(item.get("symbol") or ""), str(item.get("timeframe") or ""), str(item.get("instrument_id") or ""))),
        "caveats": caveats,
    }


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(_json_safe(dict(payload)), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _data_snapshot_hash(candle_catalog: Mapping[str, Any]) -> Optional[str]:
    rows = []
    for item in candle_catalog.get("items") or []:
        if not isinstance(item, Mapping):
            continue
        rows.append(_candle_catalog_material_row(item))
    if not rows:
        return None
    rows.sort(key=lambda row: (str(row.get("instrument_id") or ""), str(row.get("timeframe") or "")))
    return _stable_hash({"candle_catalog": rows})


def _candle_catalog_material_row(item: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "instrument_id": item.get("instrument_id"),
        "symbol": item.get("symbol"),
        "timeframe": item.get("timeframe"),
        "start_time": item.get("start_time"),
        "end_time": item.get("end_time"),
        "candle_count": item.get("candle_count"),
        "missing_count": item.get("missing_count"),
        "gap_count": item.get("gap_count"),
        "gap_count_by_type": _normalized_gap_counts(item.get("gap_count_by_type")),
        "blocking_gap_count": item.get("blocking_gap_count"),
        "provider_gap_count": item.get("provider_gap_count"),
        "expected_gap_count": item.get("expected_gap_count"),
        "unclassified_gap_count": item.get("unclassified_gap_count"),
        "readiness_impact": item.get("readiness_impact"),
        "continuity_status": item.get("continuity_status"),
        "available_resolutions": item.get("available_resolutions") or [],
        "first_gap_evidence": _candle_gap_material_evidence(_mapping(item.get("first_gap_evidence"))),
    }


def _candle_gap_material_evidence(gap: Mapping[str, Any]) -> Dict[str, Any]:
    if not gap:
        return {}
    provider_evidence = _mapping(gap.get("provider_evidence"))
    provider_response = _mapping(provider_evidence.get("provider_response"))
    stable_provider = {
        key: provider_response.get(key)
        for key in ("provider_message", "status", "status_code", "error_code", "reason")
        if provider_response.get(key) not in (None, "", [], {})
    }
    for key in ("exception_type", "exception_message"):
        if provider_evidence.get(key) not in (None, "", [], {}):
            stable_provider[key] = provider_evidence.get(key)
    payload = {
        "previous_ts": gap.get("previous_ts"),
        "current_ts": gap.get("current_ts"),
        "start": gap.get("start") or gap.get("start_ts") or gap.get("missing_start"),
        "end": gap.get("end") or gap.get("end_ts") or gap.get("missing_end"),
        "expected_interval_seconds": gap.get("expected_interval_seconds"),
        "actual_interval_seconds": gap.get("actual_interval_seconds"),
        "missing_candle_estimate": gap.get("missing_candle_estimate"),
        "classification": gap.get("classification"),
        "reason_code": gap.get("reason_code"),
        "evidence": gap.get("evidence"),
        "provider_evidence": stable_provider,
    }
    return {key: _json_safe(value) for key, value in payload.items() if value not in (None, "", [], {})}


def _candle_gaps_material_rows(candle_gaps: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for fact in candle_gaps.get("facts") or []:
        if not isinstance(fact, Mapping):
            continue
        rows.append(
            {
                "symbol": fact.get("symbol"),
                "instrument_id": fact.get("instrument_id"),
                "timeframe": fact.get("timeframe"),
                "series_key": fact.get("series_key"),
                "boundary_name": fact.get("boundary_name"),
                "source_reason": fact.get("source_reason"),
                "gap_count_by_type": _normalized_gap_counts(fact.get("gap_count_by_type")),
                "detected_gap_count": fact.get("detected_gap_count"),
                "missing_candle_estimate": fact.get("missing_candle_estimate"),
                "gaps": [
                    _candle_gap_material_evidence(gap)
                    for gap in fact.get("gaps") or []
                    if isinstance(gap, Mapping)
                ],
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("instrument_id") or ""),
            str(row.get("timeframe") or ""),
            str(row.get("series_key") or ""),
            str(row.get("symbol") or ""),
        )
    )
    return rows


def _context_material_rows(context: Mapping[str, Any]) -> Dict[str, Any]:
    indicator_rows: List[Dict[str, Any]] = []
    for row in _mapping(context.get("indicator_snapshots")).get("items") or []:
        if not isinstance(row, Mapping):
            continue
        indicator_rows.append(
            {
                "instrument_id": row.get("instrument_id"),
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "bar_time": row.get("bar_time"),
                "known_at": row.get("known_at"),
                "signal_id": row.get("signal_id"),
                "decision_id": row.get("decision_id"),
                "trade_id": row.get("trade_id"),
                "indicator_id": row.get("indicator_id"),
                "output_name": row.get("output_name"),
                "output_type": row.get("output_type"),
                "indicator_commit_seq": row.get("indicator_commit_seq"),
                "indicator_commit_seq_status": row.get("indicator_commit_seq_status"),
                "values": row.get("values"),
            }
        )
    market_rows: List[Dict[str, Any]] = []
    for row in _mapping(context.get("market_state")).get("items") or []:
        if not isinstance(row, Mapping):
            continue
        market_rows.append(
            {
                "instrument_id": row.get("instrument_id"),
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "bar_time": row.get("bar_time"),
                "known_at": row.get("known_at"),
                "signal_id": row.get("signal_id"),
                "decision_id": row.get("decision_id"),
                "trade_id": row.get("trade_id"),
                "context_values": row.get("context_values"),
            }
        )
    indicator_rows.sort(
        key=lambda row: (
            str(row.get("decision_id") or ""),
            str(row.get("signal_id") or ""),
            str(row.get("indicator_id") or ""),
            str(row.get("output_name") or ""),
            str(row.get("bar_time") or ""),
        )
    )
    market_rows.sort(
        key=lambda row: (
            str(row.get("decision_id") or ""),
            str(row.get("signal_id") or ""),
            str(row.get("bar_time") or ""),
            str(row.get("symbol") or ""),
        )
    )
    return {
        "indicator_snapshots": indicator_rows,
        "market_state": market_rows,
    }


def _report_material_fingerprint(
    *,
    metadata: RunResearchMetadata,
    readiness: RunResearchReadiness,
    summary: RunResearchSummary,
    decisions: Sequence[Mapping[str, Any]],
    signals: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    candle_catalog: Mapping[str, Any],
    candle_gaps: Mapping[str, Any],
    context: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    sections: Mapping[str, Any],
) -> str:
    decision_rows = [
        {
            "decision_id": row.get("decision_id"),
            "run_seq": row.get("run_seq"),
            "signal_id": row.get("signal_id"),
            "strategy_id": row.get("strategy_id"),
            "strategy_hash": row.get("strategy_hash"),
            "instrument_id": row.get("instrument_id"),
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
            "bar_time": row.get("bar_time"),
            "known_at": row.get("known_at"),
            "action": row.get("action"),
            "status": row.get("status"),
            "reason_code": row.get("reason_code"),
        }
        for row in decisions
    ]
    signal_rows = [
        {
            "signal_id": row.get("signal_id"),
            "run_seq": row.get("run_seq"),
            "decision_id": row.get("decision_id"),
            "strategy_id": row.get("strategy_id"),
            "strategy_hash": row.get("strategy_hash"),
            "instrument_id": row.get("instrument_id"),
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
            "bar_time": row.get("bar_time"),
            "known_at": row.get("known_at"),
            "action": row.get("action"),
            "direction": row.get("direction"),
            "price": row.get("price"),
            "quantity": row.get("quantity"),
        }
        for row in signals
    ]
    trade_rows = [
        {
            "trade_id": row.get("trade_id"),
            "strategy_id": row.get("strategy_id"),
            "strategy_hash": row.get("strategy_hash"),
            "instrument_id": row.get("instrument_id"),
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
            "side": row.get("side"),
            "entry_time": row.get("entry_time"),
            "entry_price": row.get("entry_price"),
            "exit_time": row.get("exit_time"),
            "exit_price": row.get("exit_price"),
            "close_reason": row.get("close_reason"),
            "position_commit_seq": row.get("position_commit_seq"),
            "gross_pnl": row.get("gross_pnl"),
            "fees_paid": row.get("fees_paid"),
            "net_pnl": row.get("net_pnl"),
            "quantity": row.get("quantity"),
            "decision_id": row.get("decision_id"),
            "signal_id": row.get("signal_id"),
        }
        for row in trades
    ]
    section_rows = [
        {
            "name": row.get("name"),
            "available": row.get("available"),
            "status": row.get("status"),
            "row_count": row.get("row_count"),
            "reason": row.get("reason"),
        }
        for row in sections.get("items") or []
        if isinstance(row, Mapping)
    ]
    payload = {
        "schema_version": DATASET_SCHEMA_VERSION,
        "identity": {
            "strategy_id": metadata.strategy_id,
            "strategy_hash": metadata.strategy_hash,
            "material_config_hash": metadata.material_config_hash,
            "data_snapshot_hash": metadata.data_snapshot_hash,
            "execution_mode": metadata.execution_mode,
            "simulated_window": metadata.simulated_window,
            "symbols": metadata.symbols,
            "instrument_ids": metadata.instrument_ids,
            "timeframes": metadata.timeframes,
        },
        "summary": {
            "total_decisions": summary.total_decisions,
            "accepted_decisions": summary.accepted_decisions,
            "rejected_decisions": summary.rejected_decisions,
            "closed_trades": summary.closed_trades,
            "open_trades": summary.open_trades,
            "gross_pnl": summary.gross_pnl,
            "fees": summary.fees,
            "net_pnl": summary.net_pnl,
            "equity_start": summary.equity_start,
            "equity_end": summary.equity_end,
            "max_drawdown_pct": summary.max_drawdown_pct,
        },
        "decisions": sorted(decision_rows, key=lambda row: (int(row.get("run_seq") or 0), str(row.get("bar_time") or ""), str(row.get("decision_id") or ""))),
        "signals": sorted(signal_rows, key=lambda row: (int(row.get("run_seq") or 0), str(row.get("bar_time") or ""), str(row.get("signal_id") or ""))),
        "trades": sorted(
            trade_rows,
            key=lambda row: (
                str(row.get("trade_id") or ""),
                int(row.get("position_commit_seq") or 0),
                str(row.get("entry_time") or ""),
                str(row.get("decision_id") or ""),
                str(row.get("symbol") or ""),
            ),
        ),
        "candle_catalog": [
            _candle_catalog_material_row(item)
            for item in candle_catalog.get("items") or []
            if isinstance(item, Mapping)
        ],
        "candle_gaps": _candle_gaps_material_rows(candle_gaps),
        "context": _context_material_rows(context),
        "diagnostics": {
            "by_code": _mapping(_mapping(diagnostics.get("summary")).get("by_code")),
            "readiness_impact": _mapping(_mapping(diagnostics.get("summary")).get("readiness_impact")),
        },
        "readiness": {
            "dataset_status": readiness.dataset_status,
            "results_status": readiness.results_status,
            "comparison_status": readiness.comparison_status,
            "data_quality_status": readiness.data_quality_status,
            "execution_quality_status": readiness.execution_quality_status,
            "degraded_sections": readiness.degraded_sections,
            "unavailable_sections": readiness.unavailable_sections,
        },
        "sections": sorted(section_rows, key=lambda row: str(row.get("name") or "")),
    }
    return _stable_hash(payload)


def _golden_blocking_reasons(
    *,
    metadata: RunResearchMetadata,
    readiness: RunResearchReadiness,
    diagnostics: Mapping[str, Any],
    candle_catalog: Mapping[str, Any],
    context: Mapping[str, Any],
) -> List[str]:
    reasons: List[str] = []
    if not readiness.results_ready:
        reasons.append(readiness.reason or "results_not_ready")
    if not metadata.strategy_hash:
        reasons.append("missing_strategy_hash")
    if not metadata.material_config_hash:
        reasons.append("missing_material_config_hash")
    if not metadata.data_snapshot_hash:
        reasons.append("missing_data_snapshot_hash")
    if not readiness.material_fingerprint:
        reasons.append("missing_material_fingerprint")
    for caveat in readiness.caveats:
        normalized = str(caveat or "").strip()
        if normalized in {
            "candle_catalog_identity_conflict",
            "candle_continuity_degraded",
            "candle_continuity_catalog_unavailable",
            "lifecycle_contradiction",
            "projection_failure",
            "run_notification_queue_overflow",
            "runtime_ordering_unavailable",
            "runtime_ordering_inconsistent",
            "position_ordering_missing",
            "position_commit_seq_status_invalid",
            "position_ordering_duplicate",
            "position_ordering_gap",
            "position_ordering_non_monotonic",
            "position_open_seq_invalid",
            "wallet_runtime_events_unavailable",
            "wallet_artifact_unavailable",
            "wallet_decision_trace_incomplete",
            "wallet_drift_detected",
            "wallet_ledger_state_malformed",
            "wallet_ledger_state_mismatch",
            "margin_rejection_evidence_incomplete",
            "wallet_margin_rejection_trace_incomplete",
            "wallet_replay_failed",
        }:
            reasons.append(normalized)
    if any(str(item.get("continuity_status") or "") in {"unknown", "unavailable"} for item in candle_catalog.get("items") or [] if isinstance(item, Mapping)):
        reasons.append("unknown_candle_continuity")
    if any(str(item.get("continuity_status") or "") == "degraded" for item in candle_catalog.get("items") or [] if isinstance(item, Mapping)):
        reasons.append("candle_continuity_degraded")
    if not _mapping(context.get("indicator_snapshots")).get("available"):
        reasons.append("indicator_context_unavailable")
    if not _mapping(context.get("market_state")).get("available"):
        reasons.append("market_state_unavailable")
    for item in diagnostics.get("items") or []:
        if not isinstance(item, Mapping):
            continue
        impact = str(item.get("readiness_impact") or "")
        if impact == "blocks_golden":
            reasons.append(str(item.get("code") or "golden_blocker"))
    return sorted(dict.fromkeys(reason for reason in reasons if reason))


def _with_golden_status(
    *,
    readiness: RunResearchReadiness,
    metadata: RunResearchMetadata,
    diagnostics: Mapping[str, Any],
    candle_catalog: Mapping[str, Any],
    context: Mapping[str, Any],
    material_fingerprint: Optional[str],
) -> RunResearchReadiness:
    staged = replace(readiness, material_fingerprint=material_fingerprint)
    reasons = _golden_blocking_reasons(
        metadata=metadata,
        readiness=staged,
        diagnostics=diagnostics,
        candle_catalog=candle_catalog,
        context=context,
    )
    status = "certified" if not reasons and staged.safe_to_compare else "blocked" if reasons else "failed"
    repeatability_status = "fingerprinted" if material_fingerprint and not reasons else "blocked" if reasons else "unknown"
    return replace(
        staged,
        golden_candidate_status=status,
        golden_blocking_reasons=reasons,
        repeatability_status=repeatability_status,
    )


def _operational_health(
    *,
    metadata: RunResearchMetadata,
    events: Sequence[Mapping[str, Any]],
    performance: Mapping[str, Any],
    sections: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    observability_events: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    event_type_counts = Counter(str(row.get("event_type") or "unknown") for row in events)
    event_name_counts = Counter(_event_name_key(row) or "unknown" for row in events)
    symbol_counts = Counter(str(row.get("symbol") or _context(row).get("symbol") or "UNKNOWN") for row in events)
    rows_by_section = {
        str(section.get("name")): section.get("row_count")
        for section in sections.get("items") or []
        if isinstance(section, Mapping) and section.get("row_count") is not None
    }
    diagnostics_items = [dict(row) for row in diagnostics.get("items") or [] if isinstance(row, Mapping)]
    slow_write_rows = [
        dict(row)
        for row in observability_events
        if "write" in str(row.get("event_name") or row.get("component") or "").lower()
        and ("slow" in str(row.get("event_name") or row.get("message") or "").lower() or str(row.get("level") or "").upper() in {"WARN", "WARNING", "ERROR"})
    ]
    projection_rows = [
        {
            "timestamp": row.get("observed_at"),
            "source": row.get("component"),
            "code": row.get("event_name"),
            "level": row.get("level"),
            "message": row.get("message"),
        }
        for row in observability_events
        if "projection" in str(row.get("event_name") or row.get("component") or row.get("failure_mode") or "").lower()
    ]
    diagnostic_timeline = [
        {
            "timestamp": item.get("timestamp") or item.get("known_at"),
            "severity": item.get("severity"),
            "source": item.get("source"),
            "code": item.get("code"),
            "readiness_impact": item.get("readiness_impact"),
        }
        for item in diagnostics_items
    ]
    return {
        "schema_version": "operational_health.v1",
        "run_id": metadata.run_id,
        "runtime_step_latency_summary": {
            "p50_ms": performance.get("p50_ms"),
            "p95_ms": performance.get("p95_ms"),
            "p99_ms": performance.get("p99_ms"),
            "wall_clock_duration_seconds": performance.get("wall_clock_duration_seconds"),
        },
        "per_stage_latency": performance.get("major_step_timings") or [],
        "event_volume_summary": {
            "total": len(events),
            "by_event_type": dict(event_type_counts),
            "by_event_name": dict(event_name_counts),
        },
        "rows_produced_by_section": rows_by_section,
        "slow_write_diagnostics": slow_write_rows,
        "persistence_query_caveats": list(performance.get("performance_caveats") or []),
        "symbol_level_runtime_load": dict(symbol_counts),
        "diagnostic_timeline": diagnostic_timeline,
        "event_volume_timeline": [],
        "projection_health_timeline": projection_rows,
        "caveats": list(performance.get("performance_caveats") or []),
    }


def _research_trade_compact(trade: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "trade_id": trade.get("trade_id"),
        "symbol": trade.get("symbol"),
        "entry_time": trade.get("entry_time"),
        "exit_time": trade.get("exit_time"),
        "close_reason": trade.get("close_reason"),
        "gross_pnl": trade.get("gross_pnl"),
        "fees_paid": trade.get("fees_paid"),
        "net_pnl": trade.get("net_pnl"),
    }


def _narrative_summary(
    *,
    metadata: RunResearchMetadata,
    readiness: RunResearchReadiness,
    summary: RunResearchSummary,
    insights: Mapping[str, Any],
    execution: Mapping[str, Any],
) -> str:
    lines = [
        f"# RunResearchDataset v1: {metadata.run_id}",
        "",
        (
            f"Run `{metadata.run_id}` is `{readiness.dataset_status}` with "
            f"results_ready={str(readiness.results_ready).lower()} and "
            f"safe_to_compare={str(readiness.safe_to_compare).lower()}."
        ),
        (
            f"It traded {summary.closed_trades} closed trades from {summary.total_decisions} decisions "
            f"({summary.accepted_decisions} accepted, {summary.rejected_decisions} rejected)."
        ),
        (
            f"Gross PnL was {summary.gross_pnl:.4f}, fees were {summary.fees:.4f}, "
            f"and net PnL was {summary.net_pnl:.4f}."
        ),
    ]
    if summary.win_rate is not None:
        lines.append(f"Win rate was {summary.win_rate:.2%}; profit factor was {summary.profit_factor}.")
    if readiness.caveats:
        lines.extend(["", "## Caveats"])
        lines.extend(f"- {caveat}" for caveat in readiness.caveats)
    close_reasons = insights.get("close_reason_breakdown") or []
    if close_reasons:
        lines.extend(["", "## Strategy Insights"])
        for row in close_reasons[:5]:
            lines.append(
                f"- {row.get('close_reason')}: trades={row.get('trades')}, "
                f"net_pnl={float(row.get('net_pnl') or 0.0):.4f}"
            )
    if int(execution.get("intrabar_fallback_count") or 0) > 0:
        lines.append(
            f"- FULL-mode intrabar fallbacks: {execution.get('intrabar_fallback_count')} "
            f"({execution.get('fallback_reason_distribution')})."
        )
    investigations = insights.get("candidate_next_investigations") or []
    if investigations:
        lines.extend(["", "## Recommended Next Research Actions"])
        lines.extend(f"- {item}" for item in investigations[:6])
    return "\n".join(lines).strip() + "\n"


def _metadata(run: Mapping[str, Any]) -> RunResearchMetadata:
    config = _mapping(run.get("config_snapshot"))
    material_hash = str(
        config.get("material_config_hash")
        or config.get("strategy_material_config_hash")
        or run.get("material_config_hash")
        or ""
    ).strip() or _material_config_hash(config)
    explicit_hash = str(config.get("config_hash") or run.get("config_hash") or "").strip() or None
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    timeframes = _metadata_timeframes(run)
    datasource = str(run.get("provider") or run.get("datasource") or config.get("datasource") or "").strip() or None
    return RunResearchMetadata(
        run_id=str(run.get("run_id") or ""),
        bot_id=str(run.get("bot_id") or "").strip() or None,
        strategy_id=str(run.get("strategy_id") or "").strip() or None,
        strategy_name=str(run.get("strategy_name") or "").strip() or None,
        strategy_hash=_strategy_hash(run),
        run_type=str(run.get("run_type") or "").strip() or None,
        status=str(run.get("status") or "").strip() or None,
        started_at=run.get("started_at"),
        ended_at=run.get("ended_at"),
        completed_at=run.get("completed_at") or run.get("ended_at"),
        symbols=_metadata_symbols(run),
        instrument_ids=_metadata_instrument_ids(run),
        timeframe=str(run.get("timeframe") or config.get("timeframe") or "").strip() or None,
        timeframes=timeframes,
        datasource=datasource,
        provider=datasource,
        exchange=str(run.get("exchange") or config.get("exchange") or "").strip() or None,
        simulated_window={"start": run.get("backtest_start"), "end": run.get("backtest_end")},
        wall_clock_window={"start": run.get("started_at"), "end": run.get("ended_at")},
        execution_mode=_execution_mode(run),
        playback_mode=_playback_mode(run),
        starting_capital=_starting_capital(config),
        config_hash=explicit_hash or _config_hash(config),
        material_config_hash=material_hash,
        data_snapshot_hash=None,
        report_material_fingerprint=None,
        dataset_schema_version=DATASET_SCHEMA_VERSION,
        generated_at=generated_at,
    )


def _readiness(
    *,
    run_id: str,
    run: Mapping[str, Any],
    decision_summary: Mapping[str, Any],
    financial_summary: Mapping[str, Any],
) -> RunResearchReadiness:
    readiness = report_data.get_result_readiness(
        run_id,
        decision_summary=decision_summary,
        financial_summary=financial_summary,
    )
    caveats = list(readiness.get("caveats") or [])
    caveats.append("botlens_snapshots_rebuildable_from_material_event_ledger_and_compact_context")
    conditions = {
        str(key): bool(value)
        for key, value in _mapping(readiness.get("conditions")).items()
    }
    return RunResearchReadiness(
        dataset_ready=bool(readiness.get("dataset_ready")),
        results_ready=bool(readiness.get("results_ready")),
        safe_to_compare=bool(readiness.get("safe_to_compare")),
        reason=str(readiness.get("reason") or "not_ready"),
        conditions=conditions,
        export_status=str(
            readiness.get("export_status")
            or ("available" if readiness.get("dataset_ready") else "blocked")
        ),
        dataset_status=str(
            readiness.get("dataset_status")
            or ("ready" if readiness.get("dataset_ready") else "incomplete")
        ),
        caveats=sorted(dict.fromkeys(caveats)),
    )


def _finalize_readiness(
    *,
    run: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    observability_events: Sequence[Mapping[str, Any]],
    readiness: RunResearchReadiness,
    execution: Mapping[str, Any],
    candle_gaps: Mapping[str, Any],
    wallet_accounting: Mapping[str, Any],
    portfolio_metrics: Mapping[str, Any],
    performance: Mapping[str, Any],
    context: Mapping[str, Any],
    candle_catalog: Mapping[str, Any],
    timeseries: Mapping[str, Any],
    position_ordering: Mapping[str, Any],
) -> RunResearchReadiness:
    run_status = str(run.get("status") or "").strip().lower()
    failed = run_status in {"failed", "crashed", "startup_failed", "degraded_terminal"}
    dataset_status = "failed" if failed else "ready" if readiness.dataset_ready else "partial" if run else "blocked"
    results_status = "failed" if failed else "ready" if readiness.results_ready else "partial" if readiness.dataset_ready else "blocked"
    data_quality_status = "clean"
    candle_caveats = set(candle_gaps.get("caveats") or [])
    candle_blocking_gap_count = int(candle_gaps.get("blocking_gap_count") or 0)
    candle_provider_gap_count = int(candle_gaps.get("provider_gap_count") or 0)
    if candle_blocking_gap_count > 0 or candle_provider_gap_count > 0:
        data_quality_status = "degraded"
    elif candle_gaps.get("gap_counts_by_symbol"):
        data_quality_status = "clean"
    elif "candle_gap_observability_unavailable" in candle_caveats:
        data_quality_status = "unknown"

    execution_quality_status = "degraded" if int(execution.get("intrabar_fallback_count") or 0) > 0 else "clean"
    caveats = list(readiness.caveats)
    degraded_sections: List[str] = []
    unavailable_sections: List[str] = []
    blocking_reasons: List[str] = []
    if not readiness.results_ready and readiness.reason != "ready":
        blocking_reasons.append(readiness.reason)
    if data_quality_status in {"degraded", "unknown"}:
        degraded_sections.append("data_quality")
    if execution_quality_status == "degraded":
        degraded_sections.append("execution_quality")
    runtime_event_names = Counter(_event_name_key(row) for row in _unclassified_runtime_failures(events))
    if runtime_event_names.get("run_failed") or runtime_event_names.get("fault_recorded"):
        degraded_sections.append("lifecycle")
        caveats.append("lifecycle_contradiction")
    ordering = _runtime_ordering_health(events)
    if ordering["status"] in {"unavailable", "inconsistent", "backfilled"}:
        degraded_sections.append("runtime_ordering")
        caveats.append(f"runtime_ordering_{ordering['status']}")
    if str(position_ordering.get("status") or "") == "inconsistent":
        degraded_sections.append("position_ordering")
        for caveat in position_ordering.get("caveats") or []:
            caveats.append(str(caveat))
    projection_replay_resolved = _projection_replay_resolved(observability_events)
    if any(str(row.get("event_name") or "").strip() == "run_projector_failed" for row in observability_events):
        degraded_sections.append("projection_health")
        caveats.append("projection_replay_resolved" if projection_replay_resolved else "projection_failure")
    if any(str(row.get("event_name") or "").strip() == "run_notification_queue_overflow" for row in observability_events):
        degraded_sections.append("projection_health")
        caveats.append("projection_replay_resolved" if projection_replay_resolved else "run_notification_queue_overflow")
    for caveat in wallet_accounting.get("caveats") or []:
        degraded_sections.append("wallet_accounting")
        caveats.append(str(caveat))
    for caveat in portfolio_metrics.get("caveats") or []:
        degraded_sections.append("summary_metrics")
        caveats.append(str(caveat))
    for caveat in performance.get("performance_caveats") or []:
        degraded_sections.append("operational_health")
        caveats.append(str(caveat))
    for caveat in context.get("caveats") or []:
        if str(caveat).endswith("_unavailable"):
            unavailable_sections.append("indicator_context" if "indicator" in str(caveat) else "market_state")
        caveats.append(str(caveat))
    for caveat in candle_catalog.get("caveats") or []:
        degraded_sections.append("candle_catalog")
        caveats.append(str(caveat))
    catalog_items = [dict(row) for row in candle_catalog.get("items") or [] if isinstance(row, Mapping)]
    if catalog_items and any(str(row.get("continuity_status") or "") == "degraded" for row in catalog_items):
        data_quality_status = "degraded"
        degraded_sections.append("data_quality")
        caveats.append("candle_continuity_degraded")
    if catalog_items and any(str(row.get("continuity_status") or "") == "source_sparse" for row in catalog_items):
        if data_quality_status == "clean":
            data_quality_status = "degraded"
        degraded_sections.append("data_quality")
        caveats.append("candle_continuity_provider_sparse")
    if catalog_items and any(str(row.get("continuity_status") or "") == "expected_sparse" for row in catalog_items):
        caveats.append("candle_continuity_expected_sparse")
    if catalog_items and any(str(row.get("continuity_status") or "") in {"unknown", "unavailable"} for row in catalog_items):
        if data_quality_status == "clean":
            data_quality_status = "unknown"
        degraded_sections.append("data_quality")
        caveats.append("candle_continuity_catalog_unavailable")
    for name, section in _mapping(timeseries.get("items")).items():
        if isinstance(section, Mapping) and not section.get("available"):
            unavailable_sections.append(f"timeseries.{name}")

    export_status = "available" if readiness.dataset_ready else "partial" if dataset_status == "partial" else "unavailable"
    comparison_status = "blocked"
    if readiness.safe_to_compare:
        comparison_status = "ready_with_caveats" if degraded_sections or data_quality_status != "clean" or execution_quality_status != "clean" else "ready"
    return RunResearchReadiness(
        dataset_ready=readiness.dataset_ready,
        results_ready=readiness.results_ready,
        safe_to_compare=readiness.safe_to_compare,
        reason=readiness.reason,
        conditions=readiness.conditions,
        export_status=export_status,
        dataset_status=dataset_status,
        caveats=sorted(dict.fromkeys(caveats)),
        results_status=results_status,
        comparison_status=comparison_status,
        data_quality_status=data_quality_status,
        execution_quality_status=execution_quality_status,
        blocking_reasons=sorted(dict.fromkeys(blocking_reasons)),
        degraded_sections=sorted(dict.fromkeys(degraded_sections)),
        unavailable_sections=sorted(dict.fromkeys(unavailable_sections)),
    )


def build_run_research_dataset(run_id: str) -> Dict[str, Any]:
    """Build the canonical research dataset for a bot run from durable DB truth."""

    run = storage.get_bot_run(run_id)
    if not run:
        raise KeyError(f"Run {run_id} was not found")
    run = dict(run)
    log_context = build_log_context(run_id=run_id, bot_id=run.get("bot_id"))
    logger.info(with_log_context("run_research_dataset_build_start", log_context))
    events = report_data.list_run_events(run_id)
    decisions = [_decision_row(entry) for entry in report_data.list_decision_ledger(run_id)]
    signals = _signal_rows(events)
    trade_closed_by_id = _trade_closed_context_by_id(events)
    raw_trades = [dict(row) for row in storage.list_bot_trades_for_run(run_id)]
    trades = _normalize_trades(raw_trades, trade_closed_context_by_id=trade_closed_by_id)
    decisions, signals, trades = _link_trace_rows(decisions=decisions, signals=signals, trades=trades)
    metadata = _metadata(run)
    trace_rows = [*decisions, *signals, *trades]
    metadata = replace(
        metadata,
        symbols=_unique_text([*metadata.symbols, *(row.get("symbol") for row in trace_rows)]),
        instrument_ids=_unique_text([*metadata.instrument_ids, *(row.get("instrument_id") for row in trace_rows)]),
        timeframes=_unique_text([*metadata.timeframes, *(row.get("timeframe") for row in trace_rows)]),
        strategy_hash=metadata.strategy_hash or _first_trace_value(trace_rows, "strategy_hash"),
    )
    summary = _summary(decisions=decisions, trades=trades, starting_capital=metadata.starting_capital)
    decision_summary = {
        "total": summary.total_decisions,
        "accepted": summary.accepted_decisions,
        "rejected": summary.rejected_decisions,
    }
    financial_summary = {
        "net_pnl": summary.net_pnl,
        "total_trades": summary.closed_trades,
        "fees": summary.fees,
        "gross_pnl": summary.gross_pnl,
        "profit_factor": summary.profit_factor,
        "win_rate": summary.win_rate,
    }
    readiness = _readiness(
        run_id=run_id,
        run=run,
        decision_summary=decision_summary,
        financial_summary=financial_summary,
    )
    execution = _execution_section(run=run, events=events)
    fee_accounting = _fee_accounting(trades, summary)
    wallet_accounting = _wallet_accounting(run=run, decisions=decisions, events=events, summary=summary)
    wallet_diagnostics = _mapping(wallet_accounting.get("wallet_diagnostics"))
    observability_events = _observability_events_for_run(run_id)
    candle_gaps = _candle_gaps(events, observability_events, metadata=metadata)
    portfolio_metrics = _portfolio_metrics(run=run, trades=trades, summary=summary)
    summary = _summary_with_portfolio_metrics(summary, portfolio_metrics)
    timeseries = _timeseries(metadata=metadata, trades=trades, summary=summary)
    performance = _performance(run, events)
    context = _context_dataset(metadata=metadata, decisions=decisions, signals=signals, trades=trades)
    candle_catalog = _candle_catalog(metadata=metadata, traces=trace_rows, candle_gaps=candle_gaps)
    position_ordering = _position_ordering_health(events)
    execution = dict(execution)
    execution["position_ordering"] = dict(position_ordering)
    insights = _strategy_insights(
        trades=trades,
        decisions=decisions,
        execution=execution,
        summary=summary,
    )
    readiness = _finalize_readiness(
        run=run,
        events=events,
        observability_events=observability_events,
        readiness=readiness,
        execution=execution,
        candle_gaps=candle_gaps,
        wallet_accounting=wallet_accounting,
        portfolio_metrics=portfolio_metrics,
        performance=performance,
        context=context,
        candle_catalog=candle_catalog,
        timeseries=timeseries,
        position_ordering=position_ordering,
    )
    diagnostics = _report_diagnostics(
        run=run,
        events=events,
        readiness=readiness,
        execution=execution,
        candle_gaps=candle_gaps,
        wallet_accounting=wallet_accounting,
        portfolio_metrics=portfolio_metrics,
        performance=performance,
        summary=summary,
        position_ordering=position_ordering,
        observability_events=observability_events,
    )
    sections = _sections(
        readiness=readiness,
        decisions=decisions,
        signals=signals,
        trades=trades,
        timeseries=timeseries,
        context=context,
        candle_catalog=candle_catalog,
        diagnostics=diagnostics,
        execution=execution,
        candle_gaps=candle_gaps,
        wallet_diagnostics=wallet_diagnostics,
    )
    operational_health = _operational_health(
        metadata=metadata,
        events=events,
        performance=performance,
        sections=sections,
        diagnostics=diagnostics,
        observability_events=observability_events,
    )
    sections = _sections(
        readiness=readiness,
        decisions=decisions,
        signals=signals,
        trades=trades,
        timeseries=timeseries,
        context=context,
        candle_catalog=candle_catalog,
        diagnostics=diagnostics,
        execution=execution,
        candle_gaps=candle_gaps,
        wallet_diagnostics=wallet_diagnostics,
        operational_health=operational_health,
    )
    data_snapshot_hash = _data_snapshot_hash(candle_catalog)
    metadata = replace(metadata, data_snapshot_hash=data_snapshot_hash)
    material_fingerprint = _report_material_fingerprint(
        metadata=metadata,
        readiness=readiness,
        summary=summary,
        decisions=decisions,
        signals=signals,
        trades=trades,
        candle_catalog=candle_catalog,
        candle_gaps=candle_gaps,
        context=context,
        diagnostics=diagnostics,
        sections=sections,
    )
    metadata = replace(metadata, report_material_fingerprint=material_fingerprint)
    readiness = _with_golden_status(
        readiness=readiness,
        metadata=metadata,
        diagnostics=diagnostics,
        candle_catalog=candle_catalog,
        context=context,
        material_fingerprint=material_fingerprint,
    )
    sections = _sections(
        readiness=readiness,
        decisions=decisions,
        signals=signals,
        trades=trades,
        timeseries=timeseries,
        context=context,
        candle_catalog=candle_catalog,
        diagnostics=diagnostics,
        execution=execution,
        candle_gaps=candle_gaps,
        wallet_diagnostics=wallet_diagnostics,
        operational_health=operational_health,
    )
    narrative_summary = _narrative_summary(
        metadata=metadata,
        readiness=readiness,
        summary=summary,
        insights=insights,
        execution=execution,
    )
    dataset = RunResearchDataset(
        schema_version=DATASET_SCHEMA_VERSION,
        metadata=metadata,
        readiness=readiness,
        summary=summary,
        sections=sections,
        timeseries=timeseries,
        diagnostics=diagnostics,
        decisions=decisions,
        signals=signals,
        trades=trades,
        context=context,
        candle_catalog=candle_catalog,
        fee_accounting=fee_accounting,
        wallet_accounting=wallet_accounting,
        wallet_diagnostics=wallet_diagnostics,
        execution=execution,
        candle_gaps=candle_gaps,
        portfolio_metrics=portfolio_metrics,
        performance=performance,
        operational_health=operational_health,
        strategy_insights=insights,
        narrative_summary=narrative_summary,
    )
    logger.info(
        with_log_context(
            "run_research_dataset_build_done",
            log_context
            | {
                "dataset_ready": readiness.dataset_ready,
                "results_ready": readiness.results_ready,
                "safe_to_compare": readiness.safe_to_compare,
                "trades": summary.closed_trades,
                "decisions": summary.total_decisions,
            },
        )
    )
    return dataset.to_dict()


__all__ = [
    "DATASET_SCHEMA_VERSION",
    "ReportDiagnostic",
    "RunResearchDataset",
    "RunResearchMetadata",
    "RunResearchReadiness",
    "RunResearchSummary",
    "build_run_research_dataset",
]

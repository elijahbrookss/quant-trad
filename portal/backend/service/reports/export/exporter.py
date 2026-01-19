"""Generate bounded, deterministic run exports for LLM analysis."""

from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import bindparam, create_engine, text

from data_providers.config.runtime import runtime_config_from_env
from utils.log_context import build_log_context, with_log_context

from ...storage import storage
from ...storage.storage import _parse_optional_timestamp


logger = logging.getLogger(__name__)


def _isoformat(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if value in (None, ""):
        return None
    parsed = _parse_optional_timestamp(value)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True)


def _write_csv(zip_file: zipfile.ZipFile, name: str, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    zip_file.writestr(name, buffer.getvalue())


def _flatten_stats(stats: Dict[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key in keys:
        value = stats.get(key)
        if isinstance(value, (dict, list)):
            flattened[key] = _json_dumps(value)
        else:
            flattened[key] = value
    return flattened


def _resolve_run_window(run: Dict[str, Any]) -> Tuple[datetime, datetime]:
    start = _parse_iso(run.get("backtest_start"))
    end = _parse_iso(run.get("backtest_end"))
    if not start or not end:
        config_range = (run.get("config_snapshot") or {}).get("date_range") or {}
        start = start or _parse_iso(config_range.get("start"))
        end = end or _parse_iso(config_range.get("end"))
    if not start or not end:
        raise ValueError("Run is missing backtest_start/backtest_end window.")
    return start, end


def _resolve_symbols(run: Dict[str, Any], trades: Sequence[Dict[str, Any]]) -> List[str]:
    symbols = list(run.get("symbols") or [])
    for trade in trades:
        symbol = trade.get("symbol")
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _resolve_instruments(run: Dict[str, Any], symbols: Sequence[str]) -> Tuple[List[Dict[str, Any]], Dict[str, Optional[str]]]:
    instruments: List[Dict[str, Any]] = []
    instrument_by_symbol: Dict[str, Optional[str]] = {}
    snapshots: List[Dict[str, Any]] = []
    for strategy in (run.get("config_snapshot") or {}).get("strategies") or []:
        snapshots.extend(strategy.get("instruments") or [])

    snapshot_map = {}
    for entry in snapshots:
        symbol = entry.get("symbol")
        if symbol:
            snapshot_map[str(symbol)] = entry

    for symbol in symbols:
        record = snapshot_map.get(symbol)
        if record is None:
            record = storage.find_instrument(run.get("datasource"), run.get("exchange"), symbol)
        if record is None:
            raise KeyError(f"Instrument not found for symbol {symbol}")
        instruments.append(record)
        instrument_by_symbol[symbol] = record.get("id")

    return instruments, instrument_by_symbol


def _build_trade_rows(trades: Sequence[Dict[str, Any]], instrument_by_symbol: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for trade in trades:
        entry = _parse_iso(trade.get("entry_time"))
        exit_time = _parse_iso(trade.get("exit_time"))
        duration = None
        if entry and exit_time:
            duration = int((exit_time - entry).total_seconds())
        net_pnl = trade.get("net_pnl")
        was_win = None if net_pnl is None else bool(net_pnl > 0)
        rows.append(
            {
                "trade_id": trade.get("id"),
                "run_id": trade.get("run_id"),
                "bot_id": trade.get("bot_id"),
                "strategy_id": trade.get("strategy_id"),
                "instrument_id": instrument_by_symbol.get(trade.get("symbol")),
                "symbol": trade.get("symbol"),
                "direction": trade.get("direction"),
                "status": trade.get("status"),
                "contracts": trade.get("contracts"),
                "entry_time": trade.get("entry_time"),
                "entry_price": trade.get("entry_price"),
                "stop_price": trade.get("stop_price"),
                "exit_time": trade.get("exit_time"),
                "gross_pnl": trade.get("gross_pnl"),
                "fees_paid": trade.get("fees_paid"),
                "net_pnl": net_pnl,
                "duration_seconds": duration,
                "was_win": was_win,
                "metrics_json": _json_dumps(trade.get("metrics") or {}),
                "created_at": trade.get("created_at"),
                "updated_at": trade.get("updated_at"),
            }
        )
    return rows


def _build_trade_event_rows(
    events: Sequence[Dict[str, Any]],
    instrument_by_symbol: Dict[str, Optional[str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        rows.append(
            {
                "event_id": event.get("id"),
                "trade_id": event.get("trade_id"),
                "bot_id": event.get("bot_id"),
                "strategy_id": event.get("strategy_id"),
                "instrument_id": instrument_by_symbol.get(event.get("symbol")),
                "symbol": event.get("symbol"),
                "event_type": event.get("event_type"),
                "reason_code": event.get("reason_code"),
                "leg": event.get("leg"),
                "contracts": event.get("contracts"),
                "price": event.get("price"),
                "pnl": event.get("pnl"),
                "event_time": event.get("event_time"),
                "created_at": event.get("created_at"),
            }
        )
    return rows


def _build_ledger_rows(
    events: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for event in events:
        rows.append(
            {
                "event_id": event.get("event_id"),
                "parent_event_id": event.get("parent_event_id"),
                "reason_code": event.get("reason_code"),
                "event_type": event.get("event_type"),
                "event_subtype": event.get("event_subtype"),
                "event_ts": event.get("event_ts"),
                "created_at": event.get("created_at"),
                "symbol": event.get("symbol"),
                "instrument_id": event.get("instrument_id"),
                "trade_id": event.get("trade_id"),
                "strategy_id": event.get("strategy_id"),
                "strategy_name": event.get("strategy_name"),
                "timeframe": event.get("timeframe"),
                "side": event.get("side"),
                "qty": event.get("qty"),
                "price": event.get("price"),
                "event_impact_pnl": event.get("event_impact_pnl"),
                "trade_net_pnl": event.get("trade_net_pnl"),
                "reason_detail": event.get("reason_detail"),
                "evidence_refs_json": _json_dumps(event.get("evidence_refs") or []),
                "alternatives_rejected_json": _json_dumps(event.get("alternatives_rejected") or []),
                "context_json": _json_dumps(event.get("context") or {}),
            }
        )
    return rows


def _build_instrument_rows(instruments: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for instrument in instruments:
        flags = {
            "instrument_type": instrument.get("instrument_type"),
            "can_short": instrument.get("can_short"),
            "short_requires_borrow": instrument.get("short_requires_borrow"),
            "has_funding": instrument.get("has_funding"),
            "expiry_ts": instrument.get("expiry_ts"),
        }
        fees = {
            "maker_fee_rate": instrument.get("maker_fee_rate"),
            "taker_fee_rate": instrument.get("taker_fee_rate"),
            "tick_size": instrument.get("tick_size"),
            "tick_value": instrument.get("tick_value"),
            "contract_size": instrument.get("contract_size"),
            "min_order_size": instrument.get("min_order_size"),
        }
        rows.append(
            {
                "instrument_id": instrument.get("id"),
                "symbol": instrument.get("symbol"),
                "datasource": instrument.get("datasource"),
                "exchange": instrument.get("exchange"),
                "flags": _json_dumps(flags),
                "fees": _json_dumps(fees),
                "metadata_json": _json_dumps(instrument.get("metadata") or {}),
            }
        )
    return rows


def _build_run_rows(run: Dict[str, Any], duration_seconds: Optional[int]) -> List[Dict[str, Any]]:
    return [
        {
            "run_id": run.get("run_id"),
            "bot_id": run.get("bot_id"),
            "bot_name": run.get("bot_name"),
            "strategy_id": run.get("strategy_id"),
            "strategy_name": run.get("strategy_name"),
            "run_type": run.get("run_type"),
            "status": run.get("status"),
            "timeframe": run.get("timeframe"),
            "datasource": run.get("datasource"),
            "exchange": run.get("exchange"),
            "symbols": _json_dumps(run.get("symbols") or []),
            "backtest_start": run.get("backtest_start"),
            "backtest_end": run.get("backtest_end"),
            "started_at": run.get("started_at"),
            "ended_at": run.get("ended_at"),
            "duration_seconds": duration_seconds,
            "summary_json": _json_dumps(run.get("summary") or {}),
            "config_snapshot_json": _json_dumps(run.get("config_snapshot") or {}),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
        }
    ]


def _build_readme(
    run_id: str,
    start: datetime,
    end: datetime,
    pre_roll_hours: int,
    post_roll_hours: int,
    stats_versions: Sequence[str],
    stats_key_limit: int,
) -> str:
    lines = [
        f"Run export for run_id={run_id}",
        "",
        "Sheets/files:",
        "- run: Core run metadata and JSON snapshots.",
        "- instruments: Instruments referenced by the run symbols.",
        "- trades: Trades for the run with derived fields (duration_seconds, was_win).",
        "- trade_events: Trade stop/target events for run trades.",
        "- ledger_events: Decision ledger events captured during the run (if any).",
        "- candles_raw: Raw OHLCV candles bounded to the requested window.",
        "- derivatives_state: Per-instrument funding/derivatives state (bounded).",
        "- candle_stats_flat: Flattened candle stats for selected versions (optional).",
        "",
        "Time window:",
        f"- backtest_start={_isoformat(start)}",
        f"- backtest_end={_isoformat(end)}",
        f"- pre_roll_hours={pre_roll_hours}",
        f"- post_roll_hours={post_roll_hours}",
        "",
        f"Stats versions: {', '.join(stats_versions) if stats_versions else 'none'}",
        f"Stats key limit: {stats_key_limit}",
        "",
        "Notes:",
        "- All timestamps are UTC ISO8601.",
        "- JSON columns are raw JSON strings.",
    ]
    return "\n".join(lines)


def _fetch_candles(
    engine,
    table: str,
    instrument_id: str,
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    query = text(
        f"""
        SELECT instrument_id, timeframe_seconds, candle_time, close_time, open, high, low, close, volume, trade_count,
               is_closed, source_time, inserted_at
        FROM {table}
        WHERE instrument_id = :instrument_id
          AND timeframe_seconds = :timeframe_seconds
          AND candle_time BETWEEN :start AND :end
        ORDER BY candle_time
        """
    )
    rows: List[Dict[str, Any]] = []
    with engine.begin() as conn:
        result = conn.execute(
            query,
            {
                "instrument_id": instrument_id,
                "timeframe_seconds": timeframe_seconds,
                "start": start,
                "end": end,
            },
        )
        for row in result.mappings():
            rows.append(
                {
                    "instrument_id": row["instrument_id"],
                    "timeframe_seconds": row["timeframe_seconds"],
                    "candle_time": _isoformat(row["candle_time"]),
                    "close_time": _isoformat(row["close_time"]),
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "trade_count": row["trade_count"],
                    "is_closed": row["is_closed"],
                    "source_time": _isoformat(row["source_time"]),
                    "inserted_at": _isoformat(row["inserted_at"]),
                }
            )
    return rows


def _fetch_derivatives_state(
    engine,
    table: str,
    instrument_id: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    query = text(
        f"""
        SELECT instrument_id, observed_at, source_time, open_interest, open_interest_value, funding_rate,
               funding_time, mark_price, index_price, premium_rate, premium_index, next_funding_time, inserted_at
        FROM {table}
        WHERE instrument_id = :instrument_id
          AND observed_at BETWEEN :start AND :end
        ORDER BY observed_at
        """
    )
    rows: List[Dict[str, Any]] = []
    with engine.begin() as conn:
        result = conn.execute(
            query,
            {
                "instrument_id": instrument_id,
                "start": start,
                "end": end,
            },
        )
        for row in result.mappings():
            rows.append(
                {
                    "instrument_id": row["instrument_id"],
                    "observed_at": _isoformat(row["observed_at"]),
                    "source_time": _isoformat(row["source_time"]),
                    "open_interest": row["open_interest"],
                    "open_interest_value": row["open_interest_value"],
                    "funding_rate": row["funding_rate"],
                    "funding_time": _isoformat(row["funding_time"]),
                    "mark_price": row["mark_price"],
                    "index_price": row["index_price"],
                    "premium_rate": row["premium_rate"],
                    "premium_index": row["premium_index"],
                    "next_funding_time": _isoformat(row["next_funding_time"]),
                    "inserted_at": _isoformat(row["inserted_at"]),
                }
            )
    return rows


def _fetch_candle_stats(
    engine,
    table: str,
    instrument_id: str,
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
    stats_versions: Sequence[str],
    stats_key_limit: int,
) -> List[Dict[str, Any]]:
    if not stats_versions:
        return []
    query = text(
        f"""
        SELECT instrument_id, timeframe_seconds, candle_time, stats_version, computed_at, stats
        FROM {table}
        WHERE instrument_id = :instrument_id
          AND timeframe_seconds = :timeframe_seconds
          AND candle_time BETWEEN :start AND :end
          AND stats_version IN :stats_versions
        ORDER BY candle_time
        """
    ).bindparams(bindparam("stats_versions", expanding=True))
    rows: List[Dict[str, Any]] = []
    with engine.begin() as conn:
        result = conn.execute(
            query,
            {
                "instrument_id": instrument_id,
                "timeframe_seconds": timeframe_seconds,
                "start": start,
                "end": end,
                "stats_versions": list(stats_versions),
            },
        )
        for row in result.mappings():
            stats = dict(row["stats"] or {})
            keys = sorted(stats.keys())[:stats_key_limit]
            flattened = _flatten_stats(stats, keys)
            payload = {
                "instrument_id": row["instrument_id"],
                "timeframe_seconds": row["timeframe_seconds"],
                "candle_time": _isoformat(row["candle_time"]),
                "stats_version": row["stats_version"],
                "computed_at": _isoformat(row["computed_at"]),
                "stats_json": _json_dumps(stats),
            }
            for key, value in flattened.items():
                payload[f"stat_{key}"] = value
            rows.append(payload)
    return rows


def build_run_export(
    run_id: str,
    *,
    pre_roll_hours: int = 48,
    post_roll_hours: int = 48,
    stats_versions: Optional[Sequence[str]] = None,
    stats_key_limit: int = 20,
) -> Tuple[bytes, str]:
    """Return a ZIP export payload and filename for the requested run."""

    context = build_log_context(
        run_id=run_id,
        pre_roll_hours=pre_roll_hours,
        post_roll_hours=post_roll_hours,
        stats_versions=stats_versions or [],
        stats_key_limit=stats_key_limit,
    )
    logger.info(with_log_context("report_export_start", context))

    if pre_roll_hours < 0 or post_roll_hours < 0:
        raise ValueError("pre_roll_hours and post_roll_hours must be >= 0")

    run = storage.get_bot_run(run_id)
    if not run:
        raise KeyError(f"Run not found: {run_id}")

    trades = storage.list_bot_trades_for_run(run_id)
    trade_ids = [trade.get("id") for trade in trades if trade.get("id")]
    trade_events = storage.list_bot_trade_events_for_trades(trade_ids)
    decision_ledger = list(run.get("decision_ledger") or [])

    start, end = _resolve_run_window(run)
    window_start = start - timedelta(hours=pre_roll_hours)
    window_end = end + timedelta(hours=post_roll_hours)

    symbols = _resolve_symbols(run, trades)
    instruments, instrument_by_symbol = _resolve_instruments(run, symbols)

    duration_seconds = None
    started_at = _parse_iso(run.get("started_at"))
    ended_at = _parse_iso(run.get("ended_at"))
    if started_at and ended_at:
        duration_seconds = int((ended_at - started_at).total_seconds())

    export_rows = {
        "run": _build_run_rows(run, duration_seconds),
        "instruments": _build_instrument_rows(instruments),
        "trades": _build_trade_rows(trades, instrument_by_symbol),
        "trade_events": _build_trade_event_rows(trade_events, instrument_by_symbol),
        "ledger_events": _build_ledger_rows(decision_ledger),
    }

    runtime_config = runtime_config_from_env()
    if not runtime_config.persistence.dsn:
        raise RuntimeError("PG_DSN is required for candle export.")

    engine = create_engine(runtime_config.persistence.dsn)
    timeframe = run.get("timeframe")
    if not timeframe:
        raise ValueError("Run timeframe is required for candle export.")

    from data_providers.utils.ohlcv import interval_to_timedelta

    timeframe_seconds = int(interval_to_timedelta(timeframe).total_seconds())
    candles_rows: List[Dict[str, Any]] = []
    derivatives_rows: List[Dict[str, Any]] = []
    stats_rows: List[Dict[str, Any]] = []
    for instrument in instruments:
        instrument_id = instrument.get("id")
        symbol = instrument.get("symbol")
        if not instrument_id:
            continue
        candles = _fetch_candles(
            engine,
            runtime_config.persistence.candles_raw_table,
            instrument_id,
            timeframe_seconds,
            window_start,
            window_end,
        )
        for row in candles:
            row["symbol"] = symbol
            row["timeframe"] = timeframe
        candles_rows.extend(candles)
        if instrument.get("has_funding"):
            derivatives = _fetch_derivatives_state(
                engine,
                runtime_config.persistence.derivatives_state_table,
                instrument_id,
                window_start,
                window_end,
            )
            for row in derivatives:
                row["symbol"] = symbol
            derivatives_rows.extend(derivatives)
        stats = _fetch_candle_stats(
            engine,
            runtime_config.persistence.candle_stats_table,
            instrument_id,
            timeframe_seconds,
            window_start,
            window_end,
            stats_versions or [],
            stats_key_limit,
        )
        for row in stats:
            row["symbol"] = symbol
            row["timeframe"] = timeframe
        stats_rows.extend(stats)

    export_rows["candles_raw"] = candles_rows
    export_rows["derivatives_state"] = derivatives_rows
    if stats_rows:
        export_rows["candle_stats_flat"] = stats_rows

    readme = _build_readme(
        run_id,
        start,
        end,
        pre_roll_hours,
        post_roll_hours,
        stats_versions or [],
        stats_key_limit,
    )

    filename = f"run_{run_id}_llm_export.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("README.md", readme)
        _write_csv(
            zip_file,
            "run.csv",
            export_rows["run"],
            [
                "run_id",
                "bot_id",
                "bot_name",
                "strategy_id",
                "strategy_name",
                "run_type",
                "status",
                "timeframe",
                "datasource",
                "exchange",
                "symbols",
                "backtest_start",
                "backtest_end",
                "started_at",
                "ended_at",
                "duration_seconds",
                "summary_json",
                "config_snapshot_json",
                "created_at",
                "updated_at",
            ],
        )
        _write_csv(
            zip_file,
            "instruments.csv",
            export_rows["instruments"],
            ["instrument_id", "symbol", "datasource", "exchange", "flags", "fees", "metadata_json"],
        )
        _write_csv(
            zip_file,
            "trades.csv",
            export_rows["trades"],
            [
                "trade_id",
                "run_id",
                "bot_id",
                "strategy_id",
                "instrument_id",
                "symbol",
                "direction",
                "status",
                "contracts",
                "entry_time",
                "entry_price",
                "stop_price",
                "exit_time",
                "gross_pnl",
                "fees_paid",
                "net_pnl",
                "duration_seconds",
                "was_win",
                "metrics_json",
                "created_at",
                "updated_at",
            ],
        )
        _write_csv(
            zip_file,
            "trade_events.csv",
            export_rows["trade_events"],
            [
                "event_id",
                "trade_id",
                "bot_id",
                "strategy_id",
                "instrument_id",
                "symbol",
                "event_type",
                "reason_code",
                "leg",
                "contracts",
                "price",
                "pnl",
                "event_time",
                "created_at",
            ],
        )
        _write_csv(
            zip_file,
            "ledger_events.csv",
            export_rows["ledger_events"],
            [
                "event_id",
                "parent_event_id",
                "reason_code",
                "event_type",
                "event_subtype",
                "event_ts",
                "created_at",
                "symbol",
                "instrument_id",
                "trade_id",
                "strategy_id",
                "strategy_name",
                "timeframe",
                "side",
                "qty",
                "price",
                "event_impact_pnl",
                "trade_net_pnl",
                "reason_detail",
                "evidence_refs_json",
                "alternatives_rejected_json",
                "context_json",
            ],
        )
        _write_csv(
            zip_file,
            "candles_raw.csv",
            export_rows["candles_raw"],
            [
                "instrument_id",
                "symbol",
                "timeframe",
                "timeframe_seconds",
                "candle_time",
                "close_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trade_count",
                "is_closed",
                "source_time",
                "inserted_at",
            ],
        )
        _write_csv(
            zip_file,
            "derivatives_state.csv",
            export_rows["derivatives_state"],
            [
                "instrument_id",
                "symbol",
                "observed_at",
                "source_time",
                "open_interest",
                "open_interest_value",
                "funding_rate",
                "funding_time",
                "mark_price",
                "index_price",
                "premium_rate",
                "premium_index",
                "next_funding_time",
                "inserted_at",
            ],
        )
        if "candle_stats_flat" in export_rows:
            _write_csv(
                zip_file,
                "candle_stats_flat.csv",
                export_rows["candle_stats_flat"],
                list(export_rows["candle_stats_flat"][0].keys()) if export_rows["candle_stats_flat"] else [],
            )

    logger.info(with_log_context("report_export_success", context))
    return buffer.getvalue(), filename

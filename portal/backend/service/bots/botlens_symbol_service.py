from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .botlens_contract import EVENT_TYPE_RUNTIME_BOOTSTRAP, EVENT_TYPE_RUNTIME_FACTS, RUN_SCOPE_KEY, SCHEMA_VERSION, normalize_series_key
from .botlens_state import (
    canonicalize_candle,
    detail_snapshot_contract,
    merge_candles,
    read_run_summary_state,
    read_symbol_detail_state,
)
from ..storage.storage import (
    get_bot_run,
    get_latest_bot_run_view_state,
    list_bot_runtime_events,
)

_BOTLENS_EVENT_TYPES = (EVENT_TYPE_RUNTIME_BOOTSTRAP, EVENT_TYPE_RUNTIME_FACTS)
_MAX_SCAN_EVENTS = 5000
logger = logging.getLogger(__name__)


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _run_bot_id(*, run_id: str) -> str:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    return bot_id


def _detail_row(*, run_id: str, symbol_key: str) -> tuple[str, Optional[Dict[str, Any]]]:
    bot_id = _run_bot_id(run_id=run_id)
    row = get_latest_bot_run_view_state(
        bot_id=bot_id,
        run_id=str(run_id),
        series_key=normalize_series_key(symbol_key),
    )
    return bot_id, dict(row) if isinstance(row, Mapping) else None


def get_symbol_detail(*, run_id: str, symbol_key: str, limit: int = 320) -> Dict[str, Any]:
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        raise ValueError("canonical symbol_key is required")
    bot_id, row = _detail_row(run_id=run_id, symbol_key=normalized_symbol_key)
    if row is None:
        summary_row = get_latest_bot_run_view_state(bot_id=bot_id, run_id=str(run_id), series_key=RUN_SCOPE_KEY)
        summary_state = read_run_summary_state(
            _mapping(summary_row).get("payload"),
            bot_id=bot_id,
            run_id=str(run_id),
        )
        if normalized_symbol_key not in summary_state.get("symbol_index", {}):
            raise ValueError(f"symbol {normalized_symbol_key!r} was not found for run_id={run_id}")
        detail = read_symbol_detail_state({}, symbol_key=normalized_symbol_key)
        return detail_snapshot_contract(run_id=str(run_id), detail=detail)

    detail = read_symbol_detail_state(row.get("payload"), symbol_key=normalized_symbol_key)
    detail["candles"] = merge_candles(detail.get("candles"), limit=max(1, int(limit or 320)))
    return detail_snapshot_contract(run_id=str(run_id), detail=detail)


def get_symbol_history(*, run_id: str, symbol_key: str, before_ts: Optional[str], limit: int) -> Dict[str, Any]:
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        raise ValueError("canonical symbol_key is required")
    started = time.perf_counter()
    bot_id, latest_row = _detail_row(run_id=run_id, symbol_key=normalized_symbol_key)
    before_dt = _to_datetime(before_ts)
    if latest_row is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": str(run_id),
            "symbol_key": normalized_symbol_key,
            "before_ts": before_ts,
            "next_before_ts": None,
            "has_more": False,
            "candles": [],
        }

    latest_detail = read_symbol_detail_state(latest_row.get("payload"), symbol_key=normalized_symbol_key)
    scan_limit = max(1, min(int(limit or 320) * 20, _MAX_SCAN_EVENTS))
    rows = list_bot_runtime_events(
        bot_id=bot_id,
        run_id=str(run_id),
        after_seq=0,
        limit=scan_limit,
        event_types=list(_BOTLENS_EVENT_TYPES),
        series_key=normalized_symbol_key,
    )
    candle_map: Dict[int, Dict[str, Any]] = {}
    matching_rows = 0
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
        matching_rows += 1
        for fact in payload.get("facts") if isinstance(payload.get("facts"), list) else []:
            if not isinstance(fact, Mapping):
                continue
            if str(fact.get("fact_type") or "").strip().lower() != "candle_upserted":
                continue
            normalized = canonicalize_candle(fact.get("candle"))
            if normalized is None:
                continue
            candle_map[int(normalized["time"])] = normalized

    if not candle_map:
        for candle in latest_detail.get("candles") if isinstance(latest_detail.get("candles"), list) else []:
            normalized = canonicalize_candle(candle)
            if normalized is None:
                continue
            candle_map[int(normalized["time"])] = normalized

    ordered = [candle_map[key] for key in sorted(candle_map.keys())]
    if before_dt is not None:
        cutoff = int(before_dt.timestamp())
        ordered = [candle for candle in ordered if int(candle.get("time") or 0) < cutoff]
    page = ordered[-max(1, int(limit)) :]
    oldest = page[0]["time"] if page else None
    response = {
        "schema_version": SCHEMA_VERSION,
        "run_id": str(run_id),
        "symbol_key": normalized_symbol_key,
        "before_ts": before_ts,
        "next_before_ts": oldest,
        "has_more": len(ordered) > len(page),
        "candles": page,
    }
    logger.info(
        "botlens_symbol_history_loaded | run_id=%s | symbol_key=%s | scan_limit=%s | scanned_rows=%s | matching_rows=%s | candles_returned=%s | has_more=%s | elapsed_ms=%.3f",
        run_id,
        normalized_symbol_key,
        scan_limit,
        len(rows),
        matching_rows,
        len(page),
        len(ordered) > len(page),
        (time.perf_counter() - started) * 1000.0,
    )
    return response


def list_run_symbols(*, run_id: str) -> Dict[str, Any]:
    bot_id = _run_bot_id(run_id=run_id)
    row = get_latest_bot_run_view_state(bot_id=bot_id, run_id=str(run_id), series_key=RUN_SCOPE_KEY)
    summary = read_run_summary_state(_mapping(row).get("payload"), bot_id=bot_id, run_id=str(run_id))
    symbols = []
    for _, value in sorted(
        (summary.get("symbol_index") or {}).items(),
        key=lambda item: (
            str(item[1].get("symbol") or ""),
            str(item[1].get("timeframe") or ""),
            item[0],
        ),
    ):
        if isinstance(value, Mapping):
            symbols.append(dict(value))
    return {"schema_version": SCHEMA_VERSION, "run_id": str(run_id), "symbols": symbols}


__all__ = ["get_symbol_detail", "get_symbol_history", "list_run_symbols"]

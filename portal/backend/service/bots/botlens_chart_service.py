from __future__ import annotations

from collections import OrderedDict
import hashlib
import json
from datetime import datetime, timezone
import threading
from typing import Any, Dict, Mapping, Optional

from .botlens_chart_contracts import chart_history_response_contract
from .botlens_contract import normalize_series_key
from .botlens_overlay_history import build_chart_overlay_history
from .botlens_domain_events import BotLensDomainEventName, canonicalize_botlens_candle
from .botlens_retrieval_queries import DomainTruthEvent, iter_all_run_domain_truth
from .botlens_state import (
    merge_trade_projection_entry,
    normalize_trade,
    trade_projection_entry_from_context,
)
from data_providers.utils.ohlcv import interval_to_timedelta
from market_data.contracts import CANDLE_FACT_TYPE

from ..storage.repos.candles import (
    list_candles_for_series,
    read_frozen_dataset_candles,
)

_CANDLE_EVENT_NAMES = ("CANDLE_OBSERVED",)
_TRADE_EVENT_NAMES = ("TRADE_OPENED", "TRADE_UPDATED", "TRADE_CLOSED")
_OVERLAY_EVENT_NAMES = ("OVERLAY_STATE_CHANGED",)
_TERMINAL_RUN_STATUSES = {"completed", "cancelled", "canceled", "failed", "stopped"}
_TERMINAL_OVERLAY_TIMELINE_CACHE_MAX = 8
_TERMINAL_OVERLAY_TIMELINE_CACHE: OrderedDict[
    tuple[str, str, str],
    tuple[DomainTruthEvent, ...],
] = OrderedDict()
_TERMINAL_OVERLAY_TIMELINE_CACHE_LOCK = threading.RLock()


def get_bot_run(run_id: str):
    from ..storage.repos.runs import get_bot_run as _get_bot_run

    return _get_bot_run(run_id)


def _to_datetime(value: Any, *, field_name: str) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _isoformat_or_none(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _run_row(*, run_id: str) -> Dict[str, Any]:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    return dict(run_row)


def _series_identity(symbol_key: str) -> tuple[str | None, str | None]:
    if "|" not in str(symbol_key):
        return None, None
    instrument_id, timeframe = str(symbol_key).split("|", 1)
    instrument_id = instrument_id.strip()
    timeframe = timeframe.strip()
    return instrument_id or None, timeframe or None


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def clear_terminal_overlay_timeline_cache() -> None:
    with _TERMINAL_OVERLAY_TIMELINE_CACHE_LOCK:
        _TERMINAL_OVERLAY_TIMELINE_CACHE.clear()


def _terminal_overlay_checkpoint_present(event: DomainTruthEvent) -> bool:
    context = _mapping(getattr(event, "context", None))
    delta = _mapping(context.get("overlay_delta"))
    projection = _mapping(delta.get("projection"))
    return bool(projection.get("terminal"))


def _terminal_overlay_timeline(
    *,
    bot_id: str,
    run_id: str,
    symbol_key: str,
) -> tuple[DomainTruthEvent, ...]:
    cache_key = (str(bot_id), str(run_id), str(symbol_key))
    with _TERMINAL_OVERLAY_TIMELINE_CACHE_LOCK:
        cached = _TERMINAL_OVERLAY_TIMELINE_CACHE.get(cache_key)
        if cached is not None:
            _TERMINAL_OVERLAY_TIMELINE_CACHE.move_to_end(cache_key)
            return cached
    events = tuple(
        event
        for event in iter_all_run_domain_truth(
            bot_id=bot_id,
            run_id=run_id,
            event_names=_OVERLAY_EVENT_NAMES,
            series_key=symbol_key,
        )
        if str(getattr(event, "event_name", "")).strip().upper() in _OVERLAY_EVENT_NAMES
    )
    if not events or not _terminal_overlay_checkpoint_present(events[-1]):
        return events
    with _TERMINAL_OVERLAY_TIMELINE_CACHE_LOCK:
        _TERMINAL_OVERLAY_TIMELINE_CACHE[cache_key] = events
        _TERMINAL_OVERLAY_TIMELINE_CACHE.move_to_end(cache_key)
        while len(_TERMINAL_OVERLAY_TIMELINE_CACHE) > _TERMINAL_OVERLAY_TIMELINE_CACHE_MAX:
            _TERMINAL_OVERLAY_TIMELINE_CACHE.popitem(last=False)
    return events


def _overlay_events_before(
    events: tuple[DomainTruthEvent, ...],
    *,
    range_end: datetime,
) -> list[DomainTruthEvent]:
    retained: list[DomainTruthEvent] = []
    for event in events:
        context = _mapping(getattr(event, "context", None))
        event_time = _to_datetime(
            context.get("bar_time") or getattr(event, "event_ts", None),
            field_name="overlay_event.bar_time",
        )
        if event_time is None or event_time < range_end:
            retained.append(event)
    return retained


def _frozen_candle_series(
    *,
    run_row: Mapping[str, Any],
    instrument_id: str,
    timeframe: str,
) -> Optional[tuple[Dict[str, Any], Dict[str, Any]]]:
    config_snapshot = _mapping(run_row.get("config_snapshot"))
    binding = _mapping(config_snapshot.get("dataset_binding"))
    if not binding:
        return None
    dataset_id = str(binding.get("dataset_id") or "").strip()
    if not dataset_id:
        raise ValueError("botlens_chart_dataset_binding_missing_id")
    timeframe_seconds = int(interval_to_timedelta(timeframe).total_seconds())
    for candidate in binding.get("series") or []:
        series = _mapping(candidate)
        if (
            str(series.get("instrument_id") or "") == instrument_id
            and str(series.get("fact_type") or "") == CANDLE_FACT_TYPE
            and int(series.get("timeframe_seconds") or 0) == timeframe_seconds
        ):
            return binding, series
    raise ValueError(
        "botlens_chart_frozen_series_missing: "
        f"run_id={run_row.get('run_id')} dataset_id={dataset_id} "
        f"instrument_id={instrument_id} timeframe={timeframe}"
    )


def _fits_range(*, candle_time: int, start_epoch: Optional[int], end_epoch: Optional[int]) -> bool:
    if start_epoch is not None and candle_time < start_epoch:
        return False
    if end_epoch is not None and candle_time >= end_epoch:
        return False
    return True


def _insert_latest_window(window: OrderedDict[int, Dict[str, Any]], *, candle: Dict[str, Any], limit: int) -> bool:
    candle_time = int(candle["time"])
    window[candle_time] = candle
    if len(window) <= limit:
        return False
    oldest_time = min(window.keys())
    del window[oldest_time]
    return True


def _insert_earliest_window(window: OrderedDict[int, Dict[str, Any]], *, candle: Dict[str, Any], limit: int) -> bool:
    candle_time = int(candle["time"])
    window[candle_time] = candle
    if len(window) <= limit:
        return False
    newest_time = max(window.keys())
    del window[newest_time]
    return True


def _stable_payload_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _trade_history_payload(
    *,
    events: list[DomainTruthEvent],
    symbol_key: str,
    run_status: str,
    range_start: datetime,
    range_end: datetime,
) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    if len(events) > 10_000:
        raise RuntimeError(
            "botlens_chart_trade_history_limit_exceeded: "
            f"symbol_key={symbol_key} event_count={len(events)} limit=10000"
        )
    trades_by_id: Dict[str, Dict[str, Any]] = {}
    ordering_assured = True
    for event in events:
        event_name = BotLensDomainEventName(str(event.event_name))
        entry = trade_projection_entry_from_context(
            context=event.context,
            event_name=event_name,
            event_id=event.event_id,
            event_ts=event.event_ts,
        )
        trade_id = str(entry.get("trade_id") or "").strip()
        if not trade_id:
            raise RuntimeError(
                "botlens_chart_trade_history_invalid: "
                f"event_id={event.event_id} trade_id is required"
            )
        ordering_assured = ordering_assured and (
            str(event.context.get("run_seq_status") or "") == "runtime_assigned"
            and int(event.context.get("run_seq") or 0) > 0
            and str(event.context.get("position_commit_seq_status") or "") == "position_scoped"
            and int(event.context.get("position_commit_seq") or 0) > 0
        )
        merged = merge_trade_projection_entry(
            trades_by_id.get(trade_id),
            entry,
            event_name=event_name,
        )
        normalized = normalize_trade(merged, symbol_key=symbol_key)
        if normalized is None:
            raise RuntimeError(
                "botlens_chart_trade_history_invalid: "
                f"event_id={event.event_id} trade projection is invalid"
            )
        trades_by_id[trade_id] = normalized
    trades = sorted(
        trades_by_id.values(),
        key=lambda entry: (
            str(entry.get("entry_time") or entry.get("opened_at") or ""),
            str(entry.get("trade_id") or ""),
        ),
    )
    terminal = str(run_status or "").strip().lower() in _TERMINAL_RUN_STATUSES
    complete = terminal and ordering_assured
    fingerprint = _stable_payload_hash(
        {
            "schema_version": "botlens_chart_trade_evidence.v1",
            "symbol_key": symbol_key,
            "range_start": _isoformat_or_none(range_start),
            "range_end": _isoformat_or_none(range_end),
            "trades": trades,
        }
    )
    return trades, {
        "schema_version": "botlens_chart_trade_evidence.v1",
        "source": "domain_event_ledger",
        "coverage": "complete" if complete else "provisional",
        "complete_for_returned_candles": complete,
        "ordering_assured": ordering_assured,
        "run_status": str(run_status or "").strip().lower() or None,
        "range_start": _isoformat_or_none(range_start),
        "range_end": _isoformat_or_none(range_end),
        "event_count": len(events),
        "trade_count": len(trades),
        "fingerprint": fingerprint,
    }


def _chart_trade_window(
    *,
    bot_id: str,
    run_id: str,
    symbol_key: str,
    timeframe: str,
    run_status: str,
    candles: list[Mapping[str, Any]],
) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    if not candles:
        return [], {
            "schema_version": "botlens_chart_trade_evidence.v1",
            "source": "domain_event_ledger",
            "coverage": "empty_range",
            "complete_for_returned_candles": False,
            "ordering_assured": False,
            "event_count": 0,
            "trade_count": 0,
            "fingerprint": None,
        }
    timeframe_seconds = int(interval_to_timedelta(timeframe).total_seconds())
    range_start = datetime.fromtimestamp(int(candles[0]["time"]), tz=timezone.utc)
    range_end = datetime.fromtimestamp(
        int(candles[-1]["time"]) + timeframe_seconds,
        tz=timezone.utc,
    )
    events = [
        event
        for event in iter_all_run_domain_truth(
            bot_id=bot_id,
            run_id=run_id,
            event_names=_TRADE_EVENT_NAMES,
            series_key=symbol_key,
            bar_time_gte=_isoformat_or_none(range_start),
            bar_time_lt=_isoformat_or_none(range_end),
        )
        if str(getattr(event, "event_name", "")).strip().upper() in _TRADE_EVENT_NAMES
    ]
    return _trade_history_payload(
        events=events,
        symbol_key=symbol_key,
        run_status=run_status,
        range_start=range_start,
        range_end=range_end,
    )


def _chart_overlay_window(
    *,
    bot_id: str,
    run_id: str,
    symbol_key: str,
    timeframe: str,
    run_status: str,
    candles: list[Mapping[str, Any]],
    has_more_after: bool,
) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    if not candles:
        return [], {
            "schema_version": "botlens_chart_overlay_evidence.v2",
            "source": "domain_event_ledger",
            "coverage": "empty_range",
            "complete_for_returned_candles": False,
            "ordering_assured": False,
            "reason_codes": ["no_returned_candles"],
            "event_count": 0,
            "overlay_count": 0,
            "fingerprint": None,
        }
    timeframe_seconds = int(interval_to_timedelta(timeframe).total_seconds())
    range_start_epoch = int(candles[0]["time"])
    range_end_epoch = int(candles[-1]["time"]) + timeframe_seconds
    range_end = datetime.fromtimestamp(range_end_epoch, tz=timezone.utc)
    terminal_run = str(run_status or "").strip().lower() in _TERMINAL_RUN_STATUSES
    if terminal_run:
        events = _overlay_events_before(
            _terminal_overlay_timeline(
                bot_id=bot_id,
                run_id=run_id,
                symbol_key=symbol_key,
            ),
            range_end=range_end,
        )
    else:
        events = [
            event
            for event in iter_all_run_domain_truth(
                bot_id=bot_id,
                run_id=run_id,
                event_names=_OVERLAY_EVENT_NAMES,
                series_key=symbol_key,
                bar_time_lt=_isoformat_or_none(range_end),
            )
            if str(getattr(event, "event_name", "")).strip().upper() in _OVERLAY_EVENT_NAMES
        ]
    return build_chart_overlay_history(
        events=events,
        symbol_key=symbol_key,
        run_status=run_status,
        range_start_epoch=range_start_epoch,
        range_end_epoch=range_end_epoch,
        timeframe_seconds=timeframe_seconds,
        has_more_after=has_more_after,
    )


def get_symbol_chart_history(
    *,
    run_id: str,
    symbol_key: str,
    start_time: Optional[str],
    end_time: Optional[str],
    limit: int,
) -> Dict[str, Any]:
    normalized_symbol_key = normalize_series_key(symbol_key)
    if not normalized_symbol_key:
        raise ValueError("canonical symbol_key is required")
    normalized_limit = max(1, min(int(limit or 320), 2000))
    start_dt = _to_datetime(start_time, field_name="start_time")
    end_dt = _to_datetime(end_time, field_name="end_time")
    if start_dt is None and end_dt is None:
        raise ValueError("chart retrieval requires start_time or end_time")
    if start_dt is not None and end_dt is not None and start_dt >= end_dt:
        raise ValueError("start_time must be earlier than end_time")

    run_row = _run_row(run_id=run_id)
    bot_id = str(run_row["bot_id"])
    instrument_id, timeframe = _series_identity(normalized_symbol_key)
    if not instrument_id or not timeframe:
        raise ValueError("canonical symbol_key must contain instrument_id|timeframe")
    start_epoch = int(start_dt.timestamp()) if start_dt is not None else None
    end_epoch = int(end_dt.timestamp()) if end_dt is not None else None
    has_more_before = False
    has_more_after = False
    candles_by_time: OrderedDict[int, Dict[str, Any]] = OrderedDict()

    prefer_latest_window = start_epoch is None
    for event in iter_all_run_domain_truth(
        bot_id=bot_id,
        run_id=str(run_id),
        event_names=_CANDLE_EVENT_NAMES,
        series_key=normalized_symbol_key,
        bar_time_gte=_isoformat_or_none(start_dt),
        bar_time_lt=_isoformat_or_none(end_dt),
    ):
        normalized = canonicalize_botlens_candle(event.context.get("candle"))
        candle_time = int(normalized["time"])
        if not _fits_range(candle_time=candle_time, start_epoch=start_epoch, end_epoch=end_epoch):
            if start_epoch is not None and candle_time < start_epoch:
                has_more_before = True
            if end_epoch is not None and candle_time >= end_epoch:
                has_more_after = True
            continue
        dropped = (
            _insert_latest_window(candles_by_time, candle=normalized, limit=normalized_limit)
            if prefer_latest_window
            else _insert_earliest_window(candles_by_time, candle=normalized, limit=normalized_limit)
        )
        if dropped:
            if prefer_latest_window:
                has_more_before = True
            else:
                has_more_after = True

    evidence_source: Dict[str, Any] = {"kind": "domain_event_ledger"}
    if not candles_by_time:
        frozen_source = _frozen_candle_series(
            run_row=run_row,
            instrument_id=instrument_id,
            timeframe=timeframe,
        )
        if frozen_source is not None:
            binding, series = frozen_source
            frozen_page = read_frozen_dataset_candles(
                dataset_id=str(binding["dataset_id"]),
                series_id=int(series["series_id"]),
                start=start_dt,
                end=end_dt,
                limit=normalized_limit,
                prefer_latest=prefer_latest_window,
            )
            source_candles = list(frozen_page.get("candles") or [])
            has_more_before = bool(frozen_page.get("has_more_before"))
            has_more_after = bool(frozen_page.get("has_more_after"))
            evidence_source = {
                "kind": "frozen_dataset",
                "dataset_id": str(binding["dataset_id"]),
                "dataset_hash": str(binding.get("dataset_hash") or ""),
                "series_id": int(series["series_id"]),
                "max_commit_seq": int(series.get("max_commit_seq") or frozen_page.get("max_commit_seq") or 0),
            }
        else:
            source_candles = list_candles_for_series(
                instrument_id=instrument_id,
                timeframe=timeframe,
                start=start_dt,
                end=end_dt,
                limit=normalized_limit,
                prefer_latest=prefer_latest_window,
            )
            evidence_source = {"kind": "canonical_hot_store"}
        for candle in source_candles:
            candle_time = int(candle["time"])
            candles_by_time[candle_time] = candle

    ordered = [candles_by_time[key] for key in sorted(candles_by_time.keys())]
    trades, trade_evidence = _chart_trade_window(
        bot_id=bot_id,
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        timeframe=timeframe,
        run_status=str(run_row.get("status") or ""),
        candles=ordered,
    )
    overlays, overlay_evidence = _chart_overlay_window(
        bot_id=bot_id,
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        timeframe=timeframe,
        run_status=str(run_row.get("status") or ""),
        candles=ordered,
        has_more_after=has_more_after,
    )
    return chart_history_response_contract(
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        start_time=_isoformat_or_none(start_dt),
        end_time=_isoformat_or_none(end_dt),
        limit=normalized_limit,
        candles=ordered,
        trades=trades,
        overlays=overlays,
        trade_evidence=trade_evidence,
        overlay_evidence=overlay_evidence,
        has_more_before=has_more_before,
        has_more_after=has_more_after,
        evidence_source=evidence_source,
    )


__all__ = ["get_symbol_chart_history"]

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .botlens_chart_contracts import chart_history_response_contract
from .botlens_contract import normalize_series_key
from .botlens_domain_events import canonicalize_botlens_candle
from .botlens_retrieval_queries import iter_all_run_domain_truth

_CANDLE_EVENT_NAMES = ("CANDLE_OBSERVED",)


def get_bot_run(run_id: str):
    from ..storage.storage import get_bot_run as _get_bot_run

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


def _run_bot_id(*, run_id: str) -> str:
    run_row = get_bot_run(str(run_id)) or {}
    bot_id = str(run_row.get("bot_id") or "").strip()
    if not bot_id:
        raise ValueError(f"bot_id missing for run_id={run_id}")
    return bot_id


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

    bot_id = _run_bot_id(run_id=run_id)
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

    ordered = [candles_by_time[key] for key in sorted(candles_by_time.keys())]
    return chart_history_response_contract(
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        start_time=_isoformat_or_none(start_dt),
        end_time=_isoformat_or_none(end_dt),
        limit=normalized_limit,
        candles=ordered,
        has_more_before=has_more_before,
        has_more_after=has_more_after,
    )


__all__ = ["get_symbol_chart_history"]

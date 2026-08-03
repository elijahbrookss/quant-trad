from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

from .botlens_chart_contracts import chart_history_response_contract
from .botlens_contract import normalize_series_key
from .botlens_domain_events import canonicalize_botlens_candle
from .botlens_retrieval_queries import iter_all_run_domain_truth
from data_providers.utils.ohlcv import interval_to_timedelta
from market_data.contracts import CANDLE_FACT_TYPE

from ..storage.repos.candles import (
    list_candles_for_series,
    read_frozen_dataset_candles,
)

_CANDLE_EVENT_NAMES = ("CANDLE_OBSERVED",)


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
    return chart_history_response_contract(
        run_id=str(run_id),
        symbol_key=normalized_symbol_key,
        start_time=_isoformat_or_none(start_dt),
        end_time=_isoformat_or_none(end_dt),
        limit=normalized_limit,
        candles=ordered,
        has_more_before=has_more_before,
        has_more_after=has_more_after,
        evidence_source=evidence_source,
    )


__all__ = ["get_symbol_chart_history"]

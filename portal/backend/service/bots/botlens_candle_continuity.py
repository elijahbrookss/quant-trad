from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Dict, Optional

from core.candle_continuity import (
    CandleContinuityAccumulator,
    CandleContinuitySummary,
    expected_interval_seconds,
    summarize_candle_continuity,
)

from ..observability import BackendObserver
from .botlens_contract import FACT_TYPE_CANDLE_UPSERTED, normalize_series_key


def _timeframe_from_series_key(series_key: Any) -> Optional[str]:
    normalized = normalize_series_key(series_key)
    if not normalized or "|" not in normalized:
        return None
    _, timeframe = normalized.split("|", 1)
    return str(timeframe).strip().lower() or None


def continuity_summary_from_candles(
    candles: Iterable[Mapping[str, Any]] | None,
    *,
    timeframe: Any = None,
    series_key: Any = None,
    source_reason: Any = None,
    gap_classification: Any = None,
) -> CandleContinuitySummary:
    resolved_series_key = normalize_series_key(series_key)
    resolved_timeframe = str(timeframe or "").strip().lower() or _timeframe_from_series_key(resolved_series_key)
    return summarize_candle_continuity(
        (
            candle
            for candle in (candles or [])
            if isinstance(candle, Mapping)
        ),
        expected_interval_seconds_value=expected_interval_seconds(
            timeframe=resolved_timeframe,
            series_key=resolved_series_key,
        ),
        source_reason=source_reason,
        gap_classification=gap_classification,
    )


def continuity_candles_from_fact_payload(
    facts: Sequence[Mapping[str, Any]] | None,
) -> tuple[Mapping[str, Any], ...]:
    candles: list[Mapping[str, Any]] = []
    for fact in facts or []:
        if not isinstance(fact, Mapping):
            continue
        if str(fact.get("fact_type") or "").strip().lower() != FACT_TYPE_CANDLE_UPSERTED:
            continue
        candle = fact.get("candle")
        if isinstance(candle, Mapping):
            candles.append(candle)
    return tuple(candles)


def continuity_summary_from_fact_payload(
    *,
    facts: Sequence[Mapping[str, Any]] | None,
    timeframe: Any = None,
    series_key: Any = None,
    source_reason: Any = None,
    gap_classification: Any = None,
) -> CandleContinuitySummary:
    resolved_series_key = normalize_series_key(series_key)
    resolved_timeframe = str(timeframe or "").strip().lower() or _timeframe_from_series_key(resolved_series_key)
    return summarize_candle_continuity(
        continuity_candles_from_fact_payload(facts),
        expected_interval_seconds_value=expected_interval_seconds(
            timeframe=resolved_timeframe,
            series_key=resolved_series_key,
        ),
        source_reason=source_reason,
        gap_classification=gap_classification,
    )


def continuity_summary_from_runtime_event_rows(
    rows: Sequence[Mapping[str, Any]] | None,
    *,
    timeframe: Any = None,
    series_key: Any = None,
    source_reason: Any = None,
    gap_classification: Any = None,
) -> CandleContinuitySummary:
    resolved_series_key = normalize_series_key(series_key)
    resolved_timeframe = str(timeframe or "").strip().lower() or _timeframe_from_series_key(resolved_series_key)
    return summarize_candle_continuity(
        (
            (
                row.get("payload", {}).get("context", {}).get("candle", {})
                if isinstance(row.get("payload"), Mapping)
                else None
            )
            for row in (rows or [])
            if isinstance(row, Mapping)
        ),
        expected_interval_seconds_value=expected_interval_seconds(
            timeframe=resolved_timeframe,
            series_key=resolved_series_key,
        ),
        source_reason=source_reason,
        gap_classification=gap_classification,
    )


def emit_candle_continuity_summary(
    observer: BackendObserver,
    *,
    stage: str,
    summary: CandleContinuitySummary,
    bot_id: Any = None,
    run_id: Any = None,
    series_key: Any = None,
    instrument_id: Any = None,
    symbol: Any = None,
    timeframe: Any = None,
    message_kind: Any = None,
    storage_target: Any = None,
    source_reason: Any = None,
    boundary_name: Any = None,
    extra: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    resolved_series_key = normalize_series_key(series_key)
    resolved_timeframe = str(timeframe or "").strip().lower() or _timeframe_from_series_key(resolved_series_key)
    summary_payload = {
        **summary.to_dict(),
        "boundary_name": str(boundary_name or stage).strip() or stage,
        "symbol": str(symbol or "").strip().upper() or None,
        "instrument_id": str(instrument_id or "").strip() or None,
        "timeframe": resolved_timeframe or None,
        "series_key": resolved_series_key or None,
        "source_reason": str(source_reason or "").strip().lower() or None,
    }
    if extra:
        for key, value in dict(extra).items():
            if value is not None:
                summary_payload[str(key)] = value

    observer.observe(
        "candle_continuity_candle_count",
        float(summary.candle_count),
        bot_id=bot_id,
        run_id=run_id,
        instrument_id=instrument_id,
        series_key=resolved_series_key,
        pipeline_stage=stage,
        message_kind=message_kind,
        storage_target=storage_target,
        source_reason=source_reason,
    )
    observer.observe(
        "candle_continuity_gap_count",
        float(summary.detected_gap_count),
        bot_id=bot_id,
        run_id=run_id,
        instrument_id=instrument_id,
        series_key=resolved_series_key,
        pipeline_stage=stage,
        message_kind=message_kind,
        storage_target=storage_target,
        source_reason=source_reason,
    )
    observer.observe(
        "candle_continuity_defect_gap_count",
        float(summary.defect_gap_count),
        bot_id=bot_id,
        run_id=run_id,
        instrument_id=instrument_id,
        series_key=resolved_series_key,
        pipeline_stage=stage,
        message_kind=message_kind,
        storage_target=storage_target,
        source_reason=source_reason,
    )
    observer.observe(
        "candle_continuity_missing_candle_estimate",
        float(summary.missing_candle_estimate),
        bot_id=bot_id,
        run_id=run_id,
        instrument_id=instrument_id,
        series_key=resolved_series_key,
        pipeline_stage=stage,
        message_kind=message_kind,
        storage_target=storage_target,
        source_reason=source_reason,
    )
    observer.observe(
        "candle_continuity_max_gap_multiple",
        float(summary.max_gap_multiple or 0.0),
        bot_id=bot_id,
        run_id=run_id,
        instrument_id=instrument_id,
        series_key=resolved_series_key,
        pipeline_stage=stage,
        message_kind=message_kind,
        storage_target=storage_target,
        source_reason=source_reason,
    )
    if summary.continuity_ratio is not None:
        observer.observe(
            "candle_continuity_ratio",
            float(summary.continuity_ratio),
            bot_id=bot_id,
            run_id=run_id,
            instrument_id=instrument_id,
            series_key=resolved_series_key,
            pipeline_stage=stage,
            message_kind=message_kind,
            storage_target=storage_target,
            source_reason=source_reason,
        )
    for gap_type, count in sorted((summary.gap_count_by_type or {}).items()):
        observer.observe(
            "candle_continuity_gap_count_by_type",
            float(count),
            bot_id=bot_id,
            run_id=run_id,
            instrument_id=instrument_id,
            series_key=resolved_series_key,
            pipeline_stage=stage,
            message_kind=message_kind,
            storage_target=storage_target,
            source_reason=source_reason,
            gap_type=gap_type,
        )

    observer.event(
        "candle_continuity_summary",
        bot_id=bot_id,
        run_id=run_id,
        pipeline_stage=stage,
        message_kind=message_kind,
        storage_target=storage_target,
        message=(
            f"Candle continuity summary for {summary_payload['boundary_name']} "
            f"(gaps={summary.detected_gap_count}, candles={summary.candle_count})."
        ),
        **summary_payload,
    )
    return summary_payload


__all__ = [
    "CandleContinuityAccumulator",
    "continuity_candles_from_fact_payload",
    "continuity_summary_from_candles",
    "continuity_summary_from_fact_payload",
    "continuity_summary_from_runtime_event_rows",
    "emit_candle_continuity_summary",
]

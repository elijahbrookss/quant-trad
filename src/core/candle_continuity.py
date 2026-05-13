"""Compact candle continuity summaries for observability and contract payloads."""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from engines.bot_runtime.core.domain.time_utils import normalize_epoch, timeframe_to_seconds

EXPECTED_SESSION_GAP = "expected_session_gap"
PROVIDER_MISSING_DATA = "provider_missing_data"
INGESTION_FAILURE = "ingestion_failure"
RUNTIME_MISSING = "runtime_missing"
PROJECTION_MISSING = "projection_missing"
UNKNOWN_GAP = "unknown_gap"
GAP_CLASSIFICATION_TYPES = (
    EXPECTED_SESSION_GAP,
    PROVIDER_MISSING_DATA,
    INGESTION_FAILURE,
    RUNTIME_MISSING,
    PROJECTION_MISSING,
    UNKNOWN_GAP,
)


def _iso_or_none(epoch_seconds: Optional[int]) -> Optional[str]:
    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def expected_interval_seconds(*, timeframe: Any = None, series_key: Any = None) -> Optional[int]:
    timeframe_label = str(timeframe or "").strip().lower()
    if not timeframe_label and "|" in str(series_key or ""):
        _, timeframe_label = str(series_key).split("|", 1)
        timeframe_label = timeframe_label.strip().lower()
    seconds = timeframe_to_seconds(timeframe_label)
    return int(seconds) if seconds and int(seconds) > 0 else None


def _extract_epoch(entry: Any) -> Optional[int]:
    if isinstance(entry, Mapping):
        for key in ("time", "timestamp", "bar_time", "bar_ts", "event_time"):
            value = entry.get(key)
            if value not in (None, ""):
                return normalize_epoch(value)
        return None
    return normalize_epoch(entry)


def _missing_ohlcv(entry: Any) -> bool:
    if not isinstance(entry, Mapping):
        return False
    required = ("open", "high", "low", "close")
    if not any(key in entry for key in (*required, "volume")):
        return False
    if any(entry.get(key) in (None, "") for key in required):
        return True
    return "volume" in entry and entry.get("volume") in (None, "")


def _classification_from_text(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in GAP_CLASSIFICATION_TYPES:
        return text
    if "session" in text or "market_closed" in text or "market_holiday" in text:
        return EXPECTED_SESSION_GAP
    if (
        "provider" in text
        or "source_missing" in text
        or "source_sparse" in text
        or "exchange_no_candle" in text
        or "no_candle" in text
        or "vendor" in text
        or "closure" in text
    ):
        return PROVIDER_MISSING_DATA
    if (
        "ingest_failed" in text
        or "ingest_error" in text
        or "ingestion_failure" in text
        or "persist" in text
        or "storage" in text
        or "write_failed" in text
    ):
        return INGESTION_FAILURE
    if "runtime_missing" in text or "runtime_gap" in text or "runtime_skipped" in text:
        return RUNTIME_MISSING
    if "projection_missing" in text or "botlens_missing" in text or "chart_missing" in text:
        return PROJECTION_MISSING
    return None


def _classification_range_entries(value: Any) -> tuple[Mapping[str, Any], ...]:
    if isinstance(value, Mapping):
        return (value,)
    if isinstance(value, (str, bytes)) or value is None:
        return ()
    try:
        entries = list(value)
    except TypeError:
        return ()
    return tuple(entry for entry in entries if isinstance(entry, Mapping))


def _range_epoch(entry: Mapping[str, Any], *keys: str) -> Optional[int]:
    for key in keys:
        value = entry.get(key)
        if value not in (None, ""):
            return normalize_epoch(value)
    return None


def _classify_gap(
    *,
    source_reason: Any = None,
    gap_classification: Any = None,
    gap_start_epoch: Optional[int] = None,
    gap_end_epoch: Optional[int] = None,
) -> str:
    return str(
        _classify_gap_evidence(
            source_reason=source_reason,
            gap_classification=gap_classification,
            gap_start_epoch=gap_start_epoch,
            gap_end_epoch=gap_end_epoch,
        ).get("classification")
        or UNKNOWN_GAP
    )


def _classify_gap_evidence(
    *,
    source_reason: Any = None,
    gap_classification: Any = None,
    gap_start_epoch: Optional[int] = None,
    gap_end_epoch: Optional[int] = None,
) -> Dict[str, Any]:
    if gap_start_epoch is not None and gap_end_epoch is not None:
        for entry in _classification_range_entries(gap_classification):
            start_epoch = _range_epoch(entry, "start", "start_ts", "missing_start", "previous_ts")
            end_epoch = _range_epoch(entry, "end", "end_ts", "missing_end", "current_ts")
            if start_epoch is None or end_epoch is None:
                continue
            if int(start_epoch) <= int(gap_start_epoch) and int(end_epoch) >= int(gap_end_epoch):
                explicit = _classification_from_text(
                    entry.get("classification")
                    or entry.get("gap_classification")
                    or entry.get("reason_code")
                    or entry.get("source_reason")
                )
                if explicit is not None:
                    evidence = {
                        "classification": explicit,
                        "reason_code": entry.get("reason_code"),
                        "evidence": entry.get("evidence"),
                        "provider_evidence": entry.get("provider_evidence"),
                    }
                    return {key: value for key, value in evidence.items() if value not in (None, "", {}, [])}
    if not _classification_range_entries(gap_classification):
        explicit = _classification_from_text(gap_classification)
        if explicit is not None:
            return {"classification": explicit, "reason_code": str(gap_classification)}
    inferred = _classification_from_text(source_reason)
    if inferred is not None:
        return {"classification": inferred, "reason_code": str(source_reason)}
    return {"classification": UNKNOWN_GAP}


@dataclass(frozen=True)
class CandleGap:
    previous_epoch: int
    current_epoch: int
    expected_interval_seconds: int
    actual_interval_seconds: int
    missing_candle_estimate: int
    classification: str
    reason_code: Optional[str] = None
    evidence: Optional[str] = None
    provider_evidence: Optional[Mapping[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "previous_ts": _iso_or_none(self.previous_epoch),
            "current_ts": _iso_or_none(self.current_epoch),
            "expected_interval_seconds": int(self.expected_interval_seconds),
            "actual_interval_seconds": int(self.actual_interval_seconds),
            "missing_candle_estimate": int(self.missing_candle_estimate),
            "classification": str(self.classification),
            "reason_code": self.reason_code,
            "evidence": self.evidence,
            "provider_evidence": dict(self.provider_evidence or {}),
        }
        return {key: value for key, value in payload.items() if value not in (None, "", {}, [])}


@dataclass(frozen=True)
class CandleContinuitySummary:
    candle_count: int
    first_epoch: Optional[int]
    last_epoch: Optional[int]
    expected_interval_seconds: Optional[int]
    detected_gap_count: int
    defect_gap_count: int
    missing_candle_estimate: int
    largest_gap_seconds: int
    max_gap_seconds: int
    max_gap_multiple: Optional[float]
    continuity_ratio: Optional[float]
    duplicate_count: int = 0
    out_of_order_count: int = 0
    missing_ohlcv_count: int = 0
    gap_count_by_type: Dict[str, int] = field(default_factory=dict)
    final_status: str = "healthy"
    gaps: Tuple[CandleGap, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        gap_count_by_type = {
            gap_type: int((self.gap_count_by_type or {}).get(gap_type, 0))
            for gap_type in GAP_CLASSIFICATION_TYPES
        }
        payload: Dict[str, Any] = {
            "candle_count": self.candle_count,
            "first_ts": _iso_or_none(self.first_epoch),
            "last_ts": _iso_or_none(self.last_epoch),
            "expected_interval_seconds": self.expected_interval_seconds,
            "detected_gap_count": self.detected_gap_count,
            "defect_gap_count": self.defect_gap_count,
            "missing_candle_estimate": self.missing_candle_estimate,
            "largest_gap_seconds": self.largest_gap_seconds,
            "max_gap_seconds": self.max_gap_seconds,
            "max_gap": self.max_gap_seconds,
            "max_gap_multiple": self.max_gap_multiple,
            "continuity_ratio": self.continuity_ratio,
            "duplicate_count": self.duplicate_count,
            "out_of_order_count": self.out_of_order_count,
            "missing_ohlcv_count": self.missing_ohlcv_count,
            "gap_count_by_type": gap_count_by_type,
            "gaps": [gap.to_dict() for gap in self.gaps],
            "final_status": self.final_status,
        }
        return {key: value for key, value in payload.items() if value is not None}


def summarize_candle_continuity(
    candle_times: Iterable[Any],
    *,
    expected_interval_seconds_value: Optional[int] = None,
    source_reason: Any = None,
    gap_classification: Any = None,
) -> CandleContinuitySummary:
    raw_epochs: list[int] = []
    previous_epoch: Optional[int] = None
    duplicate_counter: Counter[int] = Counter()
    out_of_order_count = 0
    missing_ohlcv_count = 0
    for entry in candle_times:
        epoch = _extract_epoch(entry)
        if epoch is None:
            continue
        if _missing_ohlcv(entry):
            missing_ohlcv_count += 1
        epoch_int = int(epoch)
        if previous_epoch is not None and epoch_int < previous_epoch:
            out_of_order_count += 1
        previous_epoch = epoch_int
        raw_epochs.append(epoch_int)
        duplicate_counter[epoch_int] += 1

    duplicate_count = sum(max(count - 1, 0) for count in duplicate_counter.values())
    ordered_epochs = sorted(duplicate_counter.keys())
    candle_count = len(raw_epochs)
    first_epoch = ordered_epochs[0] if ordered_epochs else None
    last_epoch = ordered_epochs[-1] if ordered_epochs else None
    expected_interval = (
        int(expected_interval_seconds_value)
        if expected_interval_seconds_value is not None and int(expected_interval_seconds_value) > 0
        else None
    )
    if candle_count <= 1 or expected_interval is None:
        return CandleContinuitySummary(
            candle_count=candle_count,
            first_epoch=first_epoch,
            last_epoch=last_epoch,
            expected_interval_seconds=expected_interval,
            detected_gap_count=0,
            defect_gap_count=0,
            missing_candle_estimate=0,
            largest_gap_seconds=0,
            max_gap_seconds=0,
            max_gap_multiple=1.0 if candle_count > 0 and expected_interval is not None else None,
            continuity_ratio=1.0 if candle_count > 0 and expected_interval is not None else None,
            duplicate_count=duplicate_count,
            out_of_order_count=out_of_order_count,
            missing_ohlcv_count=missing_ohlcv_count,
            gap_count_by_type={gap_type: 0 for gap_type in GAP_CLASSIFICATION_TYPES},
            final_status=(
                "defect"
                if duplicate_count > 0 or out_of_order_count > 0 or missing_ohlcv_count > 0
                else "healthy"
            ),
        )

    gaps: list[CandleGap] = []
    gap_count_by_type: Counter[str] = Counter({gap_type: 0 for gap_type in GAP_CLASSIFICATION_TYPES})
    missing_candle_estimate = 0
    largest_gap_seconds = 0
    max_gap_seconds = 0
    max_gap_multiple = 1.0
    for previous_epoch, current_epoch in zip(ordered_epochs, ordered_epochs[1:]):
        delta_seconds = max(int(current_epoch) - int(previous_epoch), 0)
        if delta_seconds <= expected_interval:
            continue
        gap_missing = max(int(math.floor(delta_seconds / expected_interval)) - 1, 1)
        gap_start_epoch = int(previous_epoch) + int(expected_interval)
        gap_evidence = _classify_gap_evidence(
            source_reason=source_reason,
            gap_classification=gap_classification,
            gap_start_epoch=gap_start_epoch,
            gap_end_epoch=int(current_epoch),
        )
        classification = str(gap_evidence.get("classification") or UNKNOWN_GAP)
        gaps.append(
            CandleGap(
                previous_epoch=int(previous_epoch),
                current_epoch=int(current_epoch),
                expected_interval_seconds=int(expected_interval),
                actual_interval_seconds=int(delta_seconds),
                missing_candle_estimate=int(gap_missing),
                classification=classification,
                reason_code=str(gap_evidence.get("reason_code")) if gap_evidence.get("reason_code") else None,
                evidence=str(gap_evidence.get("evidence")) if gap_evidence.get("evidence") else None,
                provider_evidence=(
                    gap_evidence.get("provider_evidence")
                    if isinstance(gap_evidence.get("provider_evidence"), Mapping)
                    else None
                ),
            )
        )
        gap_count_by_type[classification] += 1
        largest_gap_seconds = max(largest_gap_seconds, delta_seconds - expected_interval)
        max_gap_seconds = max(max_gap_seconds, delta_seconds)
        max_gap_multiple = max(max_gap_multiple, float(delta_seconds) / float(expected_interval))
        missing_candle_estimate += gap_missing

    expected_count = candle_count
    if first_epoch is not None and last_epoch is not None and last_epoch >= first_epoch:
        expected_count = max(int(math.floor((last_epoch - first_epoch) / expected_interval)) + 1, candle_count)

    continuity_ratio = (
        min(float(candle_count) / float(expected_count), 1.0)
        if expected_count > 0
        else None
    )
    detected_gap_count = len(gaps)
    defect_gap_count = sum(
        int(gap_count_by_type.get(gap_type, 0))
        for gap_type in (PROVIDER_MISSING_DATA, INGESTION_FAILURE, RUNTIME_MISSING, PROJECTION_MISSING, UNKNOWN_GAP)
    )
    data_defect_count = duplicate_count + out_of_order_count + missing_ohlcv_count
    if defect_gap_count > 0 or data_defect_count > 0:
        final_status = "defect"
    elif int(gap_count_by_type.get(EXPECTED_SESSION_GAP, 0)) > 0:
        final_status = "expected_sparse"
    else:
        final_status = "healthy"
    return CandleContinuitySummary(
        candle_count=candle_count,
        first_epoch=first_epoch,
        last_epoch=last_epoch,
        expected_interval_seconds=expected_interval,
        detected_gap_count=detected_gap_count,
        defect_gap_count=defect_gap_count,
        missing_candle_estimate=missing_candle_estimate,
        largest_gap_seconds=largest_gap_seconds,
        max_gap_seconds=max_gap_seconds,
        max_gap_multiple=max_gap_multiple,
        continuity_ratio=continuity_ratio,
        duplicate_count=duplicate_count,
        out_of_order_count=out_of_order_count,
        missing_ohlcv_count=missing_ohlcv_count,
        gap_count_by_type=dict(gap_count_by_type),
        final_status=final_status,
        gaps=tuple(gaps),
    )


class CandleContinuityAccumulator:
    """Run-scoped continuity accumulator for final per-series summaries."""

    def __init__(self, *, expected_interval_seconds_value: Optional[int] = None) -> None:
        self._expected_interval_seconds_value = expected_interval_seconds_value
        self._entries: list[Any] = []
        self._source_reasons: set[str] = set()
        self._gap_classifications: set[str] = set()
        self._gap_classification_ranges: list[Mapping[str, Any]] = []

    def add(
        self,
        entries: Iterable[Any],
        *,
        expected_interval_seconds_value: Optional[int] = None,
        source_reason: Any = None,
        gap_classification: Any = None,
    ) -> None:
        if expected_interval_seconds_value is not None and int(expected_interval_seconds_value) > 0:
            self._expected_interval_seconds_value = int(expected_interval_seconds_value)
        if source_reason:
            self._source_reasons.add(str(source_reason).strip().lower())
        if gap_classification:
            range_entries = _classification_range_entries(gap_classification)
            if range_entries:
                self._gap_classification_ranges.extend(range_entries)
            else:
                classification = _classification_from_text(gap_classification)
                if classification:
                    self._gap_classifications.add(classification)
        self._entries.extend(list(entries or []))

    def summary(self) -> CandleContinuitySummary:
        classification: Any = None
        if self._gap_classification_ranges:
            classification = tuple(self._gap_classification_ranges)
        elif len(self._gap_classifications) == 1:
            classification = next(iter(self._gap_classifications))
        source_reason = None
        if len(self._source_reasons) == 1:
            source_reason = next(iter(self._source_reasons))
        return summarize_candle_continuity(
            self._entries,
            expected_interval_seconds_value=self._expected_interval_seconds_value,
            source_reason=source_reason,
            gap_classification=classification,
        )


__all__ = [
    "CandleContinuityAccumulator",
    "CandleGap",
    "CandleContinuitySummary",
    "EXPECTED_SESSION_GAP",
    "GAP_CLASSIFICATION_TYPES",
    "INGESTION_FAILURE",
    "PROJECTION_MISSING",
    "PROVIDER_MISSING_DATA",
    "RUNTIME_MISSING",
    "UNKNOWN_GAP",
    "expected_interval_seconds",
    "summarize_candle_continuity",
]

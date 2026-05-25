"""Pure helpers for compact runtime step-trace rollups."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

STEP_TRACE_ROLLUP_SENTINEL = "_step_trace_rollup_v1"
STEP_ROLLUP_BUCKET_SECONDS = 60

STEP_CONTEXT_METRIC_EXACT = frozenset(
    {
        "build_state_ms",
        "canonical_append_ms",
        "canonical_fact_overflow_count",
        "canonical_fact_persist_batch_ms",
        "canonical_fact_persist_error_count",
        "canonical_fact_persist_lag_ms",
        "canonical_fact_queue_depth",
        "canonical_fact_queued_count",
        "candle_update_ms",
        "db_commit_ms",
        "delta_build_ms",
        "delta_serialize_ms",
        "dispatch_ms",
        "enqueue_ms",
        "execution_decision_flow_ms",
        "execution_ms",
        "execution_prime_ms",
        "execution_settlement_ms",
        "execution_trade_event_processing_ms",
        "finalize_residual_ms",
        "indicator_eval_ms",
        "indicator_state_update_ms",
        "max_overlay_payload_bytes",
        "overlay_payload_bytes",
        "overlay_projection_delta_ms",
        "overlay_projection_entries_total",
        "overlay_projection_ms",
        "overlay_projection_ops_count",
        "overlays_update_ms",
        "payload_bytes",
        "pending_signals_ops_ms",
        "persist_ms",
        "persistence_ms",
        "rule_eval_ms",
        "serialize_ms",
        "series_overlay_entries_ms",
        "signal_eval_ms",
        "stats_update_ms",
        "step_trace_dropped_count",
        "step_trace_persist_batch_ms",
        "step_trace_persist_error_count",
        "step_trace_persist_lag_ms",
        "step_trace_queue_depth",
        "strategy_eval_ms",
        "stream_emit_ms",
        "trace_persist_ms",
        "trade_lock_hold_ms",
        "trade_lock_wait_ms",
        "worker_count",
    }
)
STEP_CONTEXT_METRIC_SUFFIXES: tuple[str, ...] = ()
STEP_CONTEXT_METRIC_SKIP = frozenset(
    {
        "bar_epoch",
        "bar_index",
        "bar_time",
        "event",
        "run_id",
        "symbol",
        "timeframe",
    }
)
STEP_HISTOGRAM_BOUNDS = (
    0.0,
    1.0,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    200.0,
    500.0,
    1_000.0,
    2_000.0,
    5_000.0,
    10_000.0,
    30_000.0,
    60_000.0,
    300_000.0,
    1_000_000.0,
    1_000_000_000.0,
    1_000_000_000_000.0,
)


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_optional_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, timezone.utc).replace(tzinfo=None)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def step_bucket_start(value: datetime, bucket_seconds: int = STEP_ROLLUP_BUCKET_SECONDS) -> datetime:
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(timezone.utc).replace(tzinfo=None)
    epoch = int(normalized.replace(tzinfo=timezone.utc).timestamp())
    bucket_epoch = epoch - (epoch % max(int(bucket_seconds or STEP_ROLLUP_BUCKET_SECONDS), 1))
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc).replace(tzinfo=None)


def clean_step_identity(value: Any, max_len: int) -> str:
    return str(value or "").strip()[:max_len]


def clean_step_metric_name(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "."} else "_" for ch in raw)
    return cleaned[:128]


def finite_step_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def should_rollup_step_context_metric(key: str) -> bool:
    metric_name = clean_step_metric_name(key)
    if not metric_name or metric_name in STEP_CONTEXT_METRIC_SKIP:
        return False
    return metric_name in STEP_CONTEXT_METRIC_EXACT or metric_name.endswith(STEP_CONTEXT_METRIC_SUFFIXES)


def step_histogram_counts(values: Sequence[float]) -> List[int]:
    counts = [0 for _ in STEP_HISTOGRAM_BOUNDS]
    for raw_value in values:
        value = float(raw_value)
        bucket_index = len(STEP_HISTOGRAM_BOUNDS) - 1
        for index, bound in enumerate(STEP_HISTOGRAM_BOUNDS):
            if value <= bound:
                bucket_index = index
                break
        counts[bucket_index] += 1
    return counts


def step_histogram_quantile(
    bounds: Sequence[float],
    counts: Sequence[int],
    quantile: float,
    *,
    value_max: Optional[float] = None,
) -> float:
    total = sum(max(int(count), 0) for count in counts)
    if total <= 0:
        return 0.0
    threshold = max(int(math.ceil(total * min(max(float(quantile), 0.0), 1.0))), 1)
    cumulative = 0
    max_value = float(value_max) if value_max is not None else None
    for bound, count in zip(bounds, counts):
        cumulative += max(int(count), 0)
        if cumulative >= threshold:
            value = float(bound)
            return min(value, max_value) if max_value is not None else value
    return max_value if max_value is not None else float(bounds[-1])


def step_metric_samples(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    run_id = clean_step_identity(payload.get("run_id"), 64)
    step_name = clean_step_identity(payload.get("step_name"), 64)
    started_at = parse_optional_timestamp(payload.get("started_at"))
    ended_at = parse_optional_timestamp(payload.get("ended_at"))
    duration_ms = finite_step_float(payload.get("duration_ms"))
    if not run_id or not step_name or started_at is None or ended_at is None or duration_ms is None:
        return []
    base = {
        "bucket_start": step_bucket_start(started_at),
        "bucket_seconds": STEP_ROLLUP_BUCKET_SECONDS,
        "first_seen": started_at,
        "last_seen": ended_at,
        "run_id": run_id,
        "bot_id": clean_step_identity(payload.get("bot_id"), 64),
        "step_name": step_name,
        "strategy_id": clean_step_identity(payload.get("strategy_id"), 64),
        "symbol": clean_step_identity(payload.get("symbol"), 64),
        "timeframe": clean_step_identity(payload.get("timeframe"), 32),
        "status": "ok" if bool(payload.get("ok", True)) else "failed",
        "error_count": 1 if payload.get("error") else 0,
    }
    samples = [{**base, "metric_name": "duration_ms", "value": float(duration_ms)}]
    context = payload.get("context") if isinstance(payload.get("context"), Mapping) else {}
    for key, raw_value in context.items():
        metric_name = clean_step_metric_name(key)
        if not should_rollup_step_context_metric(metric_name):
            continue
        value = finite_step_float(raw_value)
        if value is None:
            continue
        samples.append({**base, "metric_name": metric_name, "value": value})
    return samples


def step_rollup_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row["bucket_start"],
        int(row["bucket_seconds"]),
        row["run_id"],
        row["bot_id"],
        row["step_name"],
        row["metric_name"],
        row["strategy_id"],
        row["symbol"],
        row["timeframe"],
        row["status"],
    )


def rollup_step_metric_samples(payloads: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for sample in step_metric_samples(payload):
            grouped[step_rollup_identity(sample)].append(sample)

    rollups: List[Dict[str, Any]] = []
    now = utcnow_naive()
    for key, samples in grouped.items():
        values = [float(sample["value"]) for sample in samples]
        latest = max(samples, key=lambda sample: sample["last_seen"])
        histogram_counts = step_histogram_counts(values)
        value_max = float(max(values))
        rollups.append(
            {
                STEP_TRACE_ROLLUP_SENTINEL: True,
                "bucket_start": key[0],
                "bucket_seconds": int(key[1]),
                "run_id": key[2],
                "bot_id": key[3],
                "step_name": key[4],
                "metric_name": key[5],
                "strategy_id": key[6],
                "symbol": key[7],
                "timeframe": key[8],
                "status": key[9],
                "first_seen": min(sample["first_seen"] for sample in samples),
                "last_seen": max(sample["last_seen"] for sample in samples),
                "sample_count": len(values),
                "value_sum": float(sum(values)),
                "value_min": float(min(values)),
                "value_max": value_max,
                "latest_value": float(latest["value"]),
                "p95_value": step_histogram_quantile(
                    STEP_HISTOGRAM_BOUNDS,
                    histogram_counts,
                    0.95,
                    value_max=value_max,
                ),
                "p99_value": step_histogram_quantile(
                    STEP_HISTOGRAM_BOUNDS,
                    histogram_counts,
                    0.99,
                    value_max=value_max,
                ),
                "histogram_bounds": list(STEP_HISTOGRAM_BOUNDS),
                "histogram_counts": histogram_counts,
                "raw_sample_count": len(values),
                "error_count": sum(int(sample.get("error_count") or 0) for sample in samples),
                "created_at": now,
                "updated_at": now,
            }
        )
    return rollups


def merge_step_rollup_rows(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> Dict[str, Any]:
    if not existing:
        row = dict(incoming)
        row[STEP_TRACE_ROLLUP_SENTINEL] = True
        return row
    current = dict(existing)
    incoming_counts = [int(value or 0) for value in incoming.get("histogram_counts") or []]
    current_counts = [int(value or 0) for value in current.get("histogram_counts") or []]
    count_len = max(len(current_counts), len(incoming_counts), len(STEP_HISTOGRAM_BOUNDS))
    current_counts.extend([0] * max(count_len - len(current_counts), 0))
    incoming_counts.extend([0] * max(count_len - len(incoming_counts), 0))
    merged_counts = [current_counts[index] + incoming_counts[index] for index in range(count_len)]
    value_max = max(float(current.get("value_max") or 0.0), float(incoming.get("value_max") or 0.0))
    current["first_seen"] = min(
        parse_optional_timestamp(current.get("first_seen")) or utcnow_naive(),
        parse_optional_timestamp(incoming.get("first_seen")) or utcnow_naive(),
    )
    current["last_seen"] = max(
        parse_optional_timestamp(current.get("last_seen")) or utcnow_naive(),
        parse_optional_timestamp(incoming.get("last_seen")) or utcnow_naive(),
    )
    current["sample_count"] = int(current.get("sample_count") or 0) + int(incoming.get("sample_count") or 0)
    current["value_sum"] = float(current.get("value_sum") or 0.0) + float(incoming.get("value_sum") or 0.0)
    current["value_min"] = min(float(current.get("value_min") or 0.0), float(incoming.get("value_min") or 0.0))
    current["value_max"] = value_max
    current["latest_value"] = float(incoming.get("latest_value") or 0.0)
    current["histogram_bounds"] = list(incoming.get("histogram_bounds") or STEP_HISTOGRAM_BOUNDS)
    current["histogram_counts"] = merged_counts[: len(current["histogram_bounds"])]
    current["p95_value"] = step_histogram_quantile(
        current["histogram_bounds"],
        current["histogram_counts"],
        0.95,
        value_max=value_max,
    )
    current["p99_value"] = step_histogram_quantile(
        current["histogram_bounds"],
        current["histogram_counts"],
        0.99,
        value_max=value_max,
    )
    current["raw_sample_count"] = int(current.get("raw_sample_count") or 0) + int(incoming.get("raw_sample_count") or 0)
    current["error_count"] = int(current.get("error_count") or 0) + int(incoming.get("error_count") or 0)
    current["updated_at"] = incoming.get("updated_at") or utcnow_naive()
    current[STEP_TRACE_ROLLUP_SENTINEL] = True
    return current


def is_step_trace_rollup_payload(payload: Mapping[str, Any]) -> bool:
    return bool(isinstance(payload, Mapping) and payload.get(STEP_TRACE_ROLLUP_SENTINEL) is True)


def coerce_step_trace_rollup_payloads(payloads: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for payload in payloads:
        if not isinstance(payload, Mapping) or not is_step_trace_rollup_payload(payload):
            continue
        row = dict(payload)
        row["bucket_start"] = parse_optional_timestamp(row.get("bucket_start"))
        row["first_seen"] = parse_optional_timestamp(row.get("first_seen"))
        row["last_seen"] = parse_optional_timestamp(row.get("last_seen"))
        row["created_at"] = parse_optional_timestamp(row.get("created_at")) or utcnow_naive()
        row["updated_at"] = parse_optional_timestamp(row.get("updated_at")) or utcnow_naive()
        if row["bucket_start"] is None or row["first_seen"] is None or row["last_seen"] is None:
            continue
        row["bucket_seconds"] = int(row.get("bucket_seconds") or STEP_ROLLUP_BUCKET_SECONDS)
        row["run_id"] = clean_step_identity(row.get("run_id"), 64)
        row["bot_id"] = clean_step_identity(row.get("bot_id"), 64)
        row["step_name"] = clean_step_identity(row.get("step_name"), 64)
        row["metric_name"] = clean_step_metric_name(row.get("metric_name"))
        row["strategy_id"] = clean_step_identity(row.get("strategy_id"), 64)
        row["symbol"] = clean_step_identity(row.get("symbol"), 64)
        row["timeframe"] = clean_step_identity(row.get("timeframe"), 32)
        row["status"] = clean_step_identity(row.get("status") or "ok", 32) or "ok"
        if not row["run_id"] or not row["step_name"] or not row["metric_name"]:
            continue
        rows.append(row)
    return rows


__all__ = [
    "STEP_HISTOGRAM_BOUNDS",
    "STEP_ROLLUP_BUCKET_SECONDS",
    "STEP_TRACE_ROLLUP_SENTINEL",
    "coerce_step_trace_rollup_payloads",
    "is_step_trace_rollup_payload",
    "merge_step_rollup_rows",
    "rollup_step_metric_samples",
    "step_histogram_counts",
    "step_histogram_quantile",
    "step_metric_samples",
    "step_rollup_identity",
]

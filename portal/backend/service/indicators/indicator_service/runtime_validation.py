from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence

from core.events import serialize_value
from data_providers.utils.ohlcv import interval_to_timedelta
from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import configure_indicator_overlay_history
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import IndicatorExecutionContext
from market_data.frozen import semantic_hash

from ...market import candle_service, instrument_service
from .context import IndicatorServiceContext, _context
from .runtime_graph import (
    build_runtime_indicator_graph,
    collect_runtime_indicator_diagnostics,
)
from .utils import build_meta_from_record, load_indicator_record

logger = logging.getLogger(__name__)

RUNTIME_VALIDATION_PATH = "typed_indicator_engine.v1"


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _matching_recorded_gaps(
    gap: Mapping[str, Any], evidence: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    start = _utc(gap["start"])
    end = _utc(gap["end"])
    matches: list[dict[str, Any]] = []
    for raw in evidence:
        if raw.get("start") is None or raw.get("end") is None:
            continue
        recorded_start = _utc(raw["start"])
        recorded_end = _utc(raw["end"])
        if recorded_start < end and recorded_end > start:
            matches.append(dict(raw))
    return matches


def _build_runtime_candles(df: Any) -> list[Candle]:
    import pandas as pd

    if df is None or getattr(df, "empty", False):
        return []
    candles: list[Candle] = []
    timestamps = pd.to_datetime(df.index, utc=True)
    for timestamp, (_, row) in zip(timestamps, df.iterrows()):
        candles.append(
            Candle(
                time=timestamp.to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
            )
        )
    return candles


def _resolve_market_selection(
    meta: Mapping[str, Any],
    *,
    symbol: Optional[str],
    datasource: Optional[str],
    exchange: Optional[str],
    instrument_id: Optional[str],
) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    resolved_symbol = str(symbol or "").strip()
    resolved_datasource = str(datasource or meta.get("datasource") or "").strip() or None
    resolved_exchange = exchange if exchange is not None else meta.get("exchange")
    resolved_instrument_id = str(instrument_id or "").strip() or None

    if resolved_instrument_id:
        instrument = instrument_service.get_instrument_record(resolved_instrument_id)
        if not instrument:
            raise ValueError(f"Instrument record not found: {resolved_instrument_id}")
        if not resolved_symbol:
            resolved_symbol = str(instrument.get("symbol") or "").strip()
        if not resolved_datasource:
            resolved_datasource = str(instrument.get("datasource") or "").strip() or None
        if resolved_exchange is None:
            instrument_exchange = str(instrument.get("exchange") or "").strip()
            resolved_exchange = instrument_exchange or None

    if not resolved_symbol:
        raise ValueError("Indicator runtime validation requires symbol or instrument_id.")
    if not resolved_datasource and not resolved_instrument_id:
        raise ValueError("Indicator runtime validation requires datasource when instrument_id is not provided.")
    return resolved_symbol, resolved_datasource, resolved_exchange, resolved_instrument_id


def _load_candle_frame(
    *,
    start: str,
    end: str,
    interval: str,
    symbol: str,
    datasource: Optional[str],
    exchange: Optional[str],
    instrument_id: Optional[str],
) -> Any:
    if instrument_id:
        return candle_service.fetch_ohlcv_by_instrument(instrument_id, start, end, interval)
    return candle_service.fetch_ohlcv(
        symbol,
        start,
        end,
        interval,
        datasource=datasource,
        exchange=exchange,
    )


def _meta_with_param_overrides(
    meta: Mapping[str, Any],
    param_overrides: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    resolved = dict(meta)
    if param_overrides is None:
        return resolved
    if not isinstance(param_overrides, Mapping):
        raise ValueError("indicator_param_overrides must be an object")
    params = dict(resolved.get("params") or {})
    params.update(dict(param_overrides))
    resolved["params"] = params
    resolved["param_overrides"] = dict(param_overrides)
    return resolved


def _new_output_summary(*, output_key: str, output_type: str, target_indicator_id: str) -> Dict[str, Any]:
    indicator_id, _, output_name = output_key.partition(".")
    return {
        "indicator_id": indicator_id,
        "output_name": output_name,
        "type": output_type,
        "target_indicator": indicator_id == target_indicator_id,
        "present_bars": 0,
        "ready_bars": 0,
        "not_ready_bars": 0,
        "first_ready_at": None,
        "last_ready_at": None,
        "first_not_ready_at": None,
        "ready_on_last_bar": False,
        "event_counts": Counter(),
        "observed_fields": set(),
        "first_indicator_commit_seq": None,
        "last_indicator_commit_seq": None,
        "indicator_commit_seq_status": None,
    }


def _observe_fields(summary: Dict[str, Any], *, output_type: str, value: Mapping[str, Any]) -> None:
    fields = summary["observed_fields"]
    if output_type in {"signal", "lifecycle"}:
        return
    if output_type == "context":
        if "state_key" in value:
            fields.add("state_key")
        nested = value.get("fields")
        if isinstance(nested, Mapping):
            fields.update(str(key) for key in nested.keys())
        else:
            fields.update(str(key) for key in value.keys())
        return
    fields.update(str(key) for key in value.keys())


def _observe_ready_value(summary: Dict[str, Any], *, output_type: str, value: Mapping[str, Any]) -> None:
    if output_type in {"signal", "lifecycle"}:
        events = value.get("events")
        if isinstance(events, list):
            for event in events:
                if isinstance(event, Mapping):
                    key = str(event.get("key") or event.get("stage") or "").strip()
                    if key:
                        summary["event_counts"][key] += 1
        return
    _observe_fields(summary, output_type=output_type, value=value)


def _warning_payload(warning: Any) -> Dict[str, Any]:
    return {
        "warning_type": str(getattr(warning, "warning_type", "") or ""),
        "severity": str(getattr(warning, "severity", "") or ""),
        "indicator_id": str(getattr(warning, "indicator_id", "") or ""),
        "manifest_type": str(getattr(warning, "manifest_type", "") or ""),
        "version": str(getattr(warning, "version", "") or ""),
        "title": str(getattr(warning, "title", "") or ""),
        "message": str(getattr(warning, "message", "") or ""),
        "context": dict(getattr(warning, "context", {}) or {}),
    }


def validate_runtime_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    *,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    instrument_id: Optional[str] = None,
    require_ready_by_end: bool = False,
    min_ready_bars: Optional[int] = None,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    t0 = perf_counter()
    record = load_indicator_record(inst_id, ctx=ctx)
    meta = build_meta_from_record(record, ctx=ctx)
    if not bool(meta.get("runtime_supported")):
        raise RuntimeError(f"Indicator is not runtime-supported: {inst_id}")

    (
        resolved_symbol,
        resolved_datasource,
        resolved_exchange,
        resolved_instrument_id,
    ) = _resolve_market_selection(
        meta,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        instrument_id=instrument_id,
    )
    execution_context = IndicatorExecutionContext(
        symbol=resolved_symbol,
        start=start,
        end=end,
        interval=interval,
        datasource=resolved_datasource,
        exchange=resolved_exchange,
        instrument_id=resolved_instrument_id,
    )

    _, indicators = build_runtime_indicator_graph(
        [inst_id],
        execution_context=execution_context,
        ctx=ctx,
        preloaded_metas={inst_id: meta},
    )
    diagnostics = collect_runtime_indicator_diagnostics(indicators)
    frame = _load_candle_frame(
        start=start,
        end=end,
        interval=interval,
        symbol=resolved_symbol,
        datasource=resolved_datasource,
        exchange=resolved_exchange,
        instrument_id=resolved_instrument_id,
    )
    candles = _build_runtime_candles(frame)
    if not candles:
        raise LookupError("No candles available for indicator runtime validation")
    configure_indicator_overlay_history(indicators, history_bars=len(candles))

    engine = IndicatorExecutionEngine(indicators)
    declared_output_types = engine.output_types
    output_summaries = {
        output_key: _new_output_summary(
            output_key=output_key,
            output_type=str(output_type),
            target_indicator_id=inst_id,
        )
        for output_key, output_type in sorted(declared_output_types.items())
    }
    guard_warning_counts: dict[str, Dict[str, Any]] = {}
    guard_metrics = {
        "frames": 0,
        "max_execution_time_ms": 0.0,
        "max_overlay_points": 0,
        "max_overlay_payload_bytes": 0,
        "overlay_suppressed_count": 0,
    }

    declared_keys = set(declared_output_types.keys())
    for candle in candles:
        engine_frame = engine.step(
            bar=candle,
            bar_time=candle.time,
            include_overlays=False,
            include_details=False,
        )
        frame_keys = set(engine_frame.outputs.keys())
        missing_keys = sorted(declared_keys - frame_keys)
        if missing_keys:
            raise RuntimeError(
                "indicator_runtime_validation_failed: declared outputs missing "
                f"indicator_id={inst_id} bar_time={_iso_utc(candle.time)} outputs={missing_keys}"
            )
        for output_key in sorted(declared_keys):
            runtime_output = engine_frame.outputs[output_key]
            output_type = str(declared_output_types[output_key])
            summary = output_summaries[output_key]
            summary["present_bars"] += 1
            ready = bool(getattr(runtime_output, "ready", False))
            bar_time = getattr(runtime_output, "bar_time", candle.time)
            bar_time_text = _iso_utc(bar_time)
            summary["ready_on_last_bar"] = ready
            commit_seq = int(getattr(runtime_output, "indicator_commit_seq", 0) or 0)
            if summary["first_indicator_commit_seq"] is None and commit_seq:
                summary["first_indicator_commit_seq"] = commit_seq
            if commit_seq:
                summary["last_indicator_commit_seq"] = commit_seq
            commit_status = str(getattr(runtime_output, "indicator_commit_seq_status", "") or "")
            if commit_status:
                summary["indicator_commit_seq_status"] = commit_status
            if ready:
                summary["ready_bars"] += 1
                summary["first_ready_at"] = summary["first_ready_at"] or bar_time_text
                summary["last_ready_at"] = bar_time_text
                value = getattr(runtime_output, "value", {}) or {}
                if isinstance(value, Mapping):
                    _observe_ready_value(summary, output_type=output_type, value=value)
            else:
                summary["not_ready_bars"] += 1
                summary["first_not_ready_at"] = summary["first_not_ready_at"] or bar_time_text

        for metric in engine_frame.guard_metrics:
            guard_metrics["frames"] += 1
            guard_metrics["max_execution_time_ms"] = max(
                float(guard_metrics["max_execution_time_ms"]),
                float(getattr(metric, "execution_time_ms", 0.0) or 0.0),
            )
            guard_metrics["max_overlay_points"] = max(
                int(guard_metrics["max_overlay_points"]),
                int(getattr(metric, "overlay_points", 0) or 0),
            )
            guard_metrics["max_overlay_payload_bytes"] = max(
                int(guard_metrics["max_overlay_payload_bytes"]),
                int(getattr(metric, "overlay_payload_bytes", 0) or 0),
            )
            if bool(getattr(metric, "overlay_suppressed", False)):
                guard_metrics["overlay_suppressed_count"] += 1
        for warning in engine_frame.guard_warnings:
            payload = _warning_payload(warning)
            warning_key = "|".join(
                [
                    payload["warning_type"],
                    payload["severity"],
                    payload["indicator_id"],
                    payload["message"],
                ]
            )
            existing = guard_warning_counts.setdefault(warning_key, {**payload, "count": 0})
            existing["count"] += 1

    validation_errors: list[dict[str, Any]] = []
    bars_evaluated = len(candles)
    ready_threshold = int(min_ready_bars) if min_ready_bars is not None else None
    for output_key, summary in output_summaries.items():
        if int(summary["present_bars"]) != bars_evaluated:
            validation_errors.append(
                {
                    "code": "OUTPUT_NOT_PRESENT_EVERY_BAR",
                    "output_ref": output_key,
                    "present_bars": summary["present_bars"],
                    "bars_evaluated": bars_evaluated,
                }
            )
        if not bool(summary["target_indicator"]):
            continue
        if require_ready_by_end and not bool(summary["ready_on_last_bar"]):
            validation_errors.append(
                {
                    "code": "OUTPUT_NOT_READY_BY_END",
                    "output_ref": output_key,
                    "last_ready_at": summary["last_ready_at"],
                }
            )
        if ready_threshold is not None and int(summary["ready_bars"]) < ready_threshold:
            validation_errors.append(
                {
                    "code": "OUTPUT_MIN_READY_BARS_NOT_MET",
                    "output_ref": output_key,
                    "ready_bars": summary["ready_bars"],
                    "min_ready_bars": ready_threshold,
                }
            )

    outputs_payload: dict[str, dict[str, Any]] = {}
    for output_key, summary in output_summaries.items():
        payload = dict(summary)
        payload["event_counts"] = dict(sorted(payload["event_counts"].items()))
        payload["observed_fields"] = sorted(payload["observed_fields"])
        outputs_payload[output_key] = payload

    status = "passed" if not validation_errors else "failed"
    duration_ms = (perf_counter() - t0) * 1000.0
    logger.info(
        "event=indicator_runtime_validation_complete indicator_id=%s indicator_type=%s status=%s symbol=%s timeframe=%s bars=%s duration_ms=%.3f validation_errors=%s",
        inst_id,
        meta.get("type"),
        status,
        resolved_symbol,
        interval,
        bars_evaluated,
        duration_ms,
        len(validation_errors),
    )
    return {
        "schema_version": "indicator_runtime_validation.v1",
        "indicator_id": inst_id,
        "indicator_type": meta.get("type"),
        "runtime_path": RUNTIME_VALIDATION_PATH,
        "status": status,
        "validation_errors": validation_errors,
        "window": {
            "start": start,
            "end": end,
            "interval": interval,
            "first_bar_time": _iso_utc(candles[0].time),
            "last_bar_time": _iso_utc(candles[-1].time),
        },
        "market": {
            "symbol": resolved_symbol,
            "datasource": resolved_datasource,
            "exchange": resolved_exchange,
            "instrument_id": resolved_instrument_id,
        },
        "bars_evaluated": bars_evaluated,
        "outputs": outputs_payload,
        "guard": {
            "metrics": guard_metrics,
            "warnings": list(guard_warning_counts.values()),
        },
        "diagnostics": {
            "indicators": diagnostics,
        },
        "perf": {
            "duration_ms": round(duration_ms, 3),
        },
    }


def collect_runtime_output_evidence_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    *,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    instrument_id: Optional[str] = None,
    indicator_param_overrides: Mapping[str, Any] | None = None,
    candle_frame: Any = None,
    source_frame_cache: MutableMapping[tuple[str, ...], Any] | None = None,
    source_frame_cache_stats: MutableMapping[str, int] | None = None,
    market_data_resolver: Any = None,
    market_data_requirements_by_consumer: Mapping[
        str, Sequence[Mapping[str, Any]]
    ]
    | None = None,
    gap_policy: str | None = None,
    gap_rewarm_bars: int = 0,
    recorded_gap_evidence: Sequence[Mapping[str, Any]] | None = None,
    expected_indicator_graph: Sequence[Mapping[str, Any]] | None = None,
    indicator_plan_start: str | None = None,
    indicator_plan_end: str | None = None,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    """Collect per-bar declared output evidence from the canonical runtime path."""

    t0 = perf_counter()
    record = load_indicator_record(inst_id, ctx=ctx)
    base_meta = build_meta_from_record(record, ctx=ctx)
    meta = _meta_with_param_overrides(base_meta, indicator_param_overrides)
    if not bool(meta.get("runtime_supported")):
        raise RuntimeError(f"Indicator is not runtime-supported: {inst_id}")

    (
        resolved_symbol,
        resolved_datasource,
        resolved_exchange,
        resolved_instrument_id,
    ) = _resolve_market_selection(
        meta,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        instrument_id=instrument_id,
    )
    execution_context = IndicatorExecutionContext(
        symbol=resolved_symbol,
        start=start,
        end=end,
        interval=interval,
        datasource=resolved_datasource,
        exchange=resolved_exchange,
        instrument_id=resolved_instrument_id,
    )

    metas, indicators = build_runtime_indicator_graph(
        [inst_id],
        execution_context=execution_context,
        ctx=ctx,
        preloaded_metas={inst_id: meta},
        source_frame_cache=source_frame_cache,
        source_frame_cache_stats=source_frame_cache_stats,
    )
    actual_indicator_graph: list[dict[str, Any]] = []
    if expected_indicator_graph is not None:
        from .requirements import plan_runtime_requirements_for_indicators

        actual_plan = plan_runtime_requirements_for_indicators(
            [inst_id],
            timeframe=interval,
            start=str(indicator_plan_start or start),
            end=str(indicator_plan_end or end),
            param_overrides_by_id=(
                {inst_id: dict(indicator_param_overrides or {})}
                if indicator_param_overrides is not None
                else {}
            ),
            preloaded_metas=metas,
            ctx=ctx,
        )
        actual_indicator_graph = [
            dict(row) for row in actual_plan.get("indicators") or []
        ]
        expected_graph = [dict(row) for row in expected_indicator_graph]
        if semantic_hash({"indicators": actual_indicator_graph}) != semantic_hash(
            {"indicators": expected_graph}
        ):
            raise RuntimeError(
                "indicator_evidence_graph_substitution: actual runtime graph differs from planned graph"
            )
    diagnostics = collect_runtime_indicator_diagnostics(indicators)
    frame = candle_frame
    if frame is None:
        frame = _load_candle_frame(
            start=start,
            end=end,
            interval=interval,
            symbol=resolved_symbol,
            datasource=resolved_datasource,
            exchange=resolved_exchange,
            instrument_id=resolved_instrument_id,
        )
    candles = _build_runtime_candles(frame)
    if not candles:
        raise LookupError("No candles available for indicator output evidence")
    configure_indicator_overlay_history(indicators, history_bars=len(candles))

    engine = IndicatorExecutionEngine(indicators)
    output_types = {str(key): str(value) for key, value in engine.output_types.items()}
    target_output_refs = sorted(
        output_ref for output_ref in output_types if output_ref.startswith(f"{inst_id}.")
    )
    if not target_output_refs:
        raise RuntimeError(f"Indicator declared no target outputs: {inst_id}")

    output_rows: list[dict[str, Any]] = []
    candle_rows: list[dict[str, Any]] = []
    ready_counts: Counter[str] = Counter()
    not_ready_counts: Counter[str] = Counter()
    gap_transitions: list[dict[str, Any]] = []
    discontinuities: list[dict[str, Any]] = []

    interval_seconds = int(interval_to_timedelta(interval).total_seconds())
    previous_candle_time: datetime | None = None
    for bar_index, candle in enumerate(candles):
        if previous_candle_time is not None:
            expected_time = previous_candle_time + timedelta(
                seconds=interval_seconds
            )
            if candle.time != expected_time:
                gap = {
                    "start": _iso_utc(expected_time),
                    "end": _iso_utc(candle.time),
                    "missing_bars": max(
                        0,
                        int(
                            (candle.time - expected_time).total_seconds()
                            // interval_seconds
                        ),
                    ),
                    "timeframe_seconds": interval_seconds,
                }
                matching_evidence = (
                    _matching_recorded_gaps(gap, recorded_gap_evidence)
                    if recorded_gap_evidence is not None
                    else []
                )
                is_declared_gap = (
                    bool(matching_evidence)
                    if recorded_gap_evidence is not None
                    else True
                )
                discontinuities.append(
                    {
                        **gap,
                        "classification": (
                            "recorded_data_gap"
                            if is_declared_gap
                            else "expected_or_unclassified_discontinuity"
                        ),
                        "recorded_evidence": matching_evidence,
                    }
                )
                if gap_policy is not None and is_declared_gap:
                    actions = engine.handle_gap(
                        policy=gap_policy,
                        gap=gap,
                        next_bar_time=candle.time,
                        rewarm_bars=gap_rewarm_bars,
                    )
                    gap_transitions.append(
                        {**gap, "actions": [dict(row) for row in actions]}
                    )
        candle_time = _iso_utc(candle.time)
        candle_close_time = _iso_utc(
            candle.time + timedelta(seconds=interval_seconds)
        )
        candle_rows.append(
            {
                "bar_index": bar_index,
                "time": candle_time,
                "open_time": candle_time,
                "close_time": candle_close_time,
                "known_at": candle_close_time,
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "volume": float(candle.volume or 0.0),
            }
        )
        market_data_inputs = (
            market_data_resolver.resolve(
                requirements_by_consumer=dict(
                    market_data_requirements_by_consumer or {}
                ),
                primary_instrument_id=resolved_instrument_id,
                evaluation_time=candle.time,
            )
            if market_data_resolver is not None
            else None
        )
        step_args = {
            "bar": candle,
            "bar_time": candle.time,
            "include_overlays": False,
            "include_details": False,
        }
        if market_data_inputs is not None:
            step_args["market_data_inputs"] = market_data_inputs
        engine_frame = engine.step(**step_args)
        previous_candle_time = candle.time
        frame_outputs = getattr(engine_frame, "outputs", {}) or {}
        missing_keys = sorted(set(output_types) - set(frame_outputs))
        if missing_keys:
            raise RuntimeError(
                "indicator_output_evidence_failed: declared outputs missing "
                f"indicator_id={inst_id} bar_time={candle_time} outputs={missing_keys}"
            )
        for output_ref in target_output_refs:
            runtime_output = frame_outputs[output_ref]
            output_type = output_types[output_ref]
            indicator_id, _, output_name = output_ref.partition(".")
            ready = bool(getattr(runtime_output, "ready", False))
            if not ready:
                not_ready_counts[output_name] += 1
                continue
            ready_counts[output_name] += 1
            value = serialize_value(getattr(runtime_output, "value", {}) or {})
            if output_type in {"signal", "lifecycle"}:
                events = value.get("events") if isinstance(value, Mapping) else None
                if not isinstance(events, list):
                    continue
                for event_index, event in enumerate(events):
                    if not isinstance(event, Mapping):
                        continue
                    event_key = str(event.get("key") or event.get("stage") or "")
                    output_rows.append(
                        {
                            "bar_index": bar_index,
                            "time": candle_time,
                            "indicator_id": indicator_id,
                            "indicator_type": meta.get("type"),
                            "output_ref": output_ref,
                            "output_name": output_name,
                            "output_type": output_type,
                            "event_index": event_index,
                            "event_key": event_key,
                            "event": serialize_value(dict(event)),
                            "value": value,
                        }
                    )
                continue
            output_rows.append(
                {
                    "bar_index": bar_index,
                    "time": candle_time,
                    "indicator_id": indicator_id,
                    "indicator_type": meta.get("type"),
                    "output_ref": output_ref,
                    "output_name": output_name,
                    "output_type": output_type,
                    "event_index": None,
                    "event_key": None,
                    "event": None,
                    "value": value,
                }
            )

    duration_ms = (perf_counter() - t0) * 1000.0
    return {
        "schema_version": "indicator_output_evidence.v1",
        "runtime_path": RUNTIME_VALIDATION_PATH,
        "indicator": {
            "id": inst_id,
            "type": meta.get("type"),
            "name": meta.get("name"),
            "params": dict(meta.get("params") or {}),
            "base_params": dict(base_meta.get("params") or {}),
            "param_overrides": dict(indicator_param_overrides or {}),
            "dependencies": list(meta.get("dependencies") or []),
            "updated_at": meta.get("updated_at"),
        },
        "market": {
            "symbol": resolved_symbol,
            "datasource": resolved_datasource,
            "exchange": resolved_exchange,
            "instrument_id": resolved_instrument_id,
        },
        "window": {
            "start": start,
            "end": end,
            "interval": interval,
            "first_bar_time": _iso_utc(candles[0].time),
            "last_bar_time": _iso_utc(candles[-1].time),
        },
        "bars_evaluated": len(candles),
        "output_types": output_types,
        "ready_counts": dict(sorted(ready_counts.items())),
        "not_ready_counts": dict(sorted(not_ready_counts.items())),
        "gap_policy": gap_policy,
        "gap_transitions": gap_transitions,
        "continuity_discontinuities": discontinuities,
        "indicator_graph": actual_indicator_graph,
        "indicator_graph_hash": semantic_hash(
            {"indicators": actual_indicator_graph}
        ),
        "candles": candle_rows,
        "outputs": output_rows,
        "diagnostics": {
            "indicators": diagnostics,
        },
        "perf": {
            "duration_ms": round(duration_ms, 3),
        },
    }


__all__ = [
    "RUNTIME_VALIDATION_PATH",
    "collect_runtime_output_evidence_for_instance",
    "validate_runtime_for_instance",
]

"""Readonly walk-forward strategy preview using the canonical decision evaluator."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import IndicatorExecutionContext
from overlays.schema import build_overlay
from overlays.transformers import apply_overlay_transform
from portal.backend.service.indicators.indicator_service import (
    build_runtime_indicator_graph,
    get_instance_meta,
)
from portal.backend.service.market import candle_service, instrument_service
from strategies.contracts import CompiledStrategySpec
from strategies.evaluator import DecisionEvaluationState, evaluate_strategy_bar


logger = logging.getLogger(__name__)


def _isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_candles(df: Any) -> List[Candle]:
    import pandas as pd

    if df is None or getattr(df, "empty", False):
        return []
    candles: List[Candle] = []
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


def _build_marker(*, artifact: Mapping[str, Any], candle: Candle) -> Dict[str, Any]:
    intent = str(artifact.get("emitted_intent") or "")
    is_long = intent == "enter_long"
    trigger = artifact.get("trigger") if isinstance(artifact.get("trigger"), Mapping) else {}
    return {
        "time": int(candle.time.timestamp()),
        "price": float(candle.close),
        "color": "#10b981" if is_long else "#f87171",
        "shape": "arrowUp" if is_long else "arrowDown",
        "position": "belowBar" if is_long else "aboveBar",
        "text": str(artifact.get("rule_name") or trigger.get("event_key") or intent).strip(),
        "subtype": "strategy_signal",
        "decision_id": artifact.get("decision_id"),
        "strategy_rule_id": artifact.get("rule_id"),
        "indicator_id": trigger.get("output_ref"),
        "event_key": trigger.get("event_key"),
    }


def _strategy_signal_direction(intent: str) -> str:
    normalized = str(intent or "").strip().lower()
    if normalized == "enter_long":
        return "long"
    if normalized == "enter_short":
        return "short"
    raise RuntimeError(f"strategy_preview_signal_invalid: unsupported intent={intent!r}")


def _serialize_output_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return _isoformat(value)
    if isinstance(value, Mapping):
        return {
            str(key): _serialize_output_value(item)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_serialize_output_value(item) for item in value]
    return str(value)


def _serialize_signal_events(events: Any) -> List[Dict[str, Any]]:
    if not isinstance(events, list):
        return []
    serialized: List[Dict[str, Any]] = []
    for event in events:
        if not isinstance(event, Mapping):
            continue
        event_payload: Dict[str, Any] = {
            "key": str(event.get("key") or "").strip(),
        }
        if event.get("direction") is not None:
            event_payload["direction"] = event.get("direction")
        if event.get("known_at") is not None:
            event_payload["known_at"] = event.get("known_at")
        if event.get("pattern_id") is not None:
            event_payload["pattern_id"] = event.get("pattern_id")
        if event.get("confidence") is not None:
            event_payload["confidence"] = event.get("confidence")
        metadata = event.get("metadata")
        if metadata is not None:
            event_payload["metadata"] = _serialize_output_value(metadata)
        serialized.append(event_payload)
    return serialized


def _serialize_runtime_output(
    *,
    output_key: str,
    runtime_output: Any,
    output_type: str,
) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "output_ref": output_key,
        "type": output_type,
        "ready": bool(getattr(runtime_output, "ready", False)),
        "bar_time": _isoformat(getattr(runtime_output, "bar_time")),
    }
    if not getattr(runtime_output, "ready", False):
        return snapshot
    value = dict(getattr(runtime_output, "value", {}) or {})
    if output_type == "signal":
        events = _serialize_signal_events(value.get("events"))
        snapshot["event_count"] = len(events)
        snapshot["event_keys"] = [str(event.get("key") or "").strip() for event in events if str(event.get("key") or "").strip()]
        snapshot["events"] = events
        return snapshot
    snapshot["fields"] = _serialize_output_value(value)
    return snapshot


def _serialize_observed_outputs(
    *,
    outputs: Mapping[str, Any],
    output_types: Mapping[str, str],
) -> Dict[str, Dict[str, Any]]:
    observed: Dict[str, Dict[str, Any]] = {}
    for output_key in sorted(outputs.keys()):
        runtime_output = outputs.get(output_key)
        if runtime_output is None:
            continue
        output_type = str(output_types.get(output_key) or "").strip().lower()
        if output_type not in {"signal", "context", "metric"}:
            continue
        observed[output_key] = _serialize_runtime_output(
            output_key=output_key,
            runtime_output=runtime_output,
            output_type=output_type,
        )
    return observed


def _collect_referenced_output_refs(artifact: Mapping[str, Any]) -> List[str]:
    referenced: List[str] = []

    def add(value: Any) -> None:
        output_ref = str(value or "").strip()
        if output_ref and output_ref not in referenced:
            referenced.append(output_ref)

    trigger = artifact.get("trigger")
    if isinstance(trigger, Mapping):
        add(trigger.get("output_ref"))
    guard_results = artifact.get("guard_results")
    if isinstance(guard_results, list):
        for guard in guard_results:
            if not isinstance(guard, Mapping):
                continue
            add(guard.get("output_ref"))
            nested_guard = guard.get("guard")
            if isinstance(nested_guard, Mapping):
                add(nested_guard.get("output_ref"))
    return referenced


def _annotate_selected_artifact_outputs(
    *,
    artifacts: Sequence[Dict[str, Any]],
    selected_artifact: Mapping[str, Any],
    outputs: Mapping[str, Any],
    output_types: Mapping[str, str],
) -> None:
    observed_outputs = _serialize_observed_outputs(outputs=outputs, output_types=output_types)
    referenced_output_refs = _collect_referenced_output_refs(selected_artifact)
    referenced_outputs = {
        output_key: observed_outputs[output_key]
        for output_key in referenced_output_refs
        if output_key in observed_outputs
    }
    decision_id = str(selected_artifact.get("decision_id") or "").strip()
    targets: List[Dict[str, Any]] = []
    for artifact in artifacts:
        if artifact is selected_artifact:
            targets.append(artifact)
            continue
        if decision_id and str(artifact.get("decision_id") or "").strip() == decision_id:
            targets.append(artifact)
    if isinstance(selected_artifact, dict) and selected_artifact not in targets:
        targets.append(selected_artifact)
    for artifact in targets:
        artifact["observed_outputs"] = observed_outputs
        artifact["referenced_outputs"] = referenced_outputs


def _build_strategy_preview_signal(
    *,
    preview_id: str,
    artifact: Mapping[str, Any],
) -> Dict[str, Any]:
    decision_id = str(artifact.get("decision_id") or "").strip()
    if not decision_id:
        raise RuntimeError("strategy_preview_signal_invalid: decision artifact missing decision_id")
    intent = str(artifact.get("emitted_intent") or artifact.get("intent") or "").strip()
    trigger = artifact.get("trigger") if isinstance(artifact.get("trigger"), Mapping) else {}
    return {
        "signal_id": decision_id,
        "source_type": "strategy_preview",
        "source_id": preview_id,
        "decision_id": decision_id,
        "strategy_id": artifact.get("strategy_id"),
        "strategy_hash": artifact.get("strategy_hash"),
        "instrument_id": artifact.get("instrument_id"),
        "symbol": artifact.get("symbol"),
        "timeframe": artifact.get("timeframe"),
        "bar_epoch": artifact.get("bar_epoch"),
        "bar_time": artifact.get("bar_time"),
        "decision_time": artifact.get("decision_time"),
        "rule_id": artifact.get("rule_id"),
        "rule_name": artifact.get("rule_name"),
        "intent": intent,
        "direction": _strategy_signal_direction(intent),
        "event_key": trigger.get("event_key"),
    }


def _build_strategy_signal_overlay(
    *,
    strategy_id: str,
    instrument_id: str,
    markers: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    overlay = dict(build_overlay("strategy_signal", {"markers": [dict(marker) for marker in markers]}))
    overlay["overlay_id"] = f"strategy-{strategy_id}-{instrument_id}-signals"
    overlay["source"] = "strategy"
    overlay["strategy_id"] = strategy_id
    return overlay


def _collect_ready_overlays(
    overlays: Mapping[str, Any],
    *,
    current_epoch: int,
) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for overlay_key in sorted(overlays.keys()):
        runtime_overlay = overlays.get(overlay_key)
        if runtime_overlay is None or not getattr(runtime_overlay, "ready", False):
            continue
        indicator_id, _, overlay_name = str(overlay_key).partition(".")
        payload = dict(getattr(runtime_overlay, "value", {}) or {})
        transformed = apply_overlay_transform(payload, current_epoch=current_epoch)
        if transformed is None:
            continue
        payload = dict(transformed)
        payload.setdefault("overlay_id", overlay_key)
        payload.setdefault("indicator_id", indicator_id)
        payload.setdefault("overlay_name", overlay_name)
        collected.append(payload)
    return collected


def _configure_replay_window(indicators: Sequence[Any], *, history_bars: int) -> None:
    for indicator in indicators:
        configure = getattr(indicator, "configure_replay_window", None)
        if callable(configure):
            configure(history_bars=history_bars)


def evaluate_strategy_preview(
    *,
    record: Any,
    strategy_id: str,
    preview_id: str,
    start: str,
    end: str,
    interval: str,
    instrument_ids: Sequence[str],
    compiled_strategy: CompiledStrategySpec,
    selected_variant: Mapping[str, Any],
    resolved_params: Mapping[str, Any],
) -> Dict[str, Any]:
    if not instrument_ids:
        raise ValueError("instrument_ids is required for strategy preview")
    if not record.indicator_ids:
        raise ValueError("Strategy has no indicators attached")

    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if end_dt <= start_dt:
        raise ValueError("end must be after start")

    result: Dict[str, Any] = {
        "preview_id": preview_id,
        "source_type": "strategy_preview",
        "source_id": preview_id,
        "strategy_id": strategy_id,
        "strategy_name": record.name,
        "strategy_hash": compiled_strategy.strategy_hash,
        "variant": {
            "id": str(selected_variant.get("id") or "").strip(),
            "name": str(selected_variant.get("name") or "").strip(),
            "description": selected_variant.get("description"),
            "param_overrides": dict(selected_variant.get("param_overrides") or {}),
            "resolved_params": dict(resolved_params or {}),
            "atm_template_id": str(selected_variant.get("atm_template_id") or "").strip() or None,
            "is_default": bool(selected_variant.get("is_default", False)),
        },
        "instruments": {},
    }

    for instrument_id in instrument_ids:
        instrument = instrument_service.get_instrument_record(instrument_id)
        if not instrument:
            raise ValueError(f"Instrument record not found: {instrument_id}")
        symbol = str(instrument.get("symbol") or "").strip()
        datasource = str(instrument.get("datasource") or "").strip()
        exchange = instrument.get("exchange")
        if not symbol or not datasource:
            raise ValueError(f"Instrument {instrument_id} is missing symbol or datasource")

        indicator_metas: Dict[str, Dict[str, Any]] = {}
        missing_indicators: List[str] = []
        for indicator_id in record.indicator_ids:
            try:
                meta = get_instance_meta(indicator_id)
            except KeyError:
                missing_indicators.append(indicator_id)
                continue
            if not bool(meta.get("runtime_supported")):
                raise RuntimeError(f"Indicator is not runtime-supported: {indicator_id}")
            indicator_metas[indicator_id] = meta

        _, indicators = build_runtime_indicator_graph(
            list(indicator_metas.keys()),
            execution_context=IndicatorExecutionContext(
                symbol=symbol,
                start=start,
                end=end,
                interval=interval,
                datasource=datasource,
                exchange=exchange,
                instrument_id=instrument_id,
            ),
            preloaded_metas=indicator_metas,
        )
        engine = IndicatorExecutionEngine(indicators)

        fetch_started = time.perf_counter()
        df = candle_service.fetch_ohlcv_by_instrument(instrument_id, start, end, interval)
        candles = _build_candles(df)
        candle_fetch_ms = max((time.perf_counter() - fetch_started) * 1000.0, 0.0)
        if not candles:
            raise ValueError(f"No candles returned for {instrument_id}")
        _configure_replay_window(indicators, history_bars=len(candles))

        strategy_markers: List[Dict[str, Any]] = []
        strategy_signals: List[Dict[str, Any]] = []
        decision_artifacts: List[Dict[str, Any]] = []
        preview_overlays: List[Dict[str, Any]] = []
        decision_state = DecisionEvaluationState()

        replay_started = time.perf_counter()
        last_index = len(candles) - 1
        for index, candle in enumerate(candles):
            frame = engine.step(bar=candle, bar_time=candle.time, include_overlays=index == last_index)
            preview_overlays = _collect_ready_overlays(frame.overlays, current_epoch=int(candle.time.timestamp()))
            decision_result = evaluate_strategy_bar(
                compiled_strategy=compiled_strategy,
                state=decision_state,
                outputs=frame.outputs,
                output_types=engine.output_types,
                instrument_id=instrument_id,
                symbol=symbol,
                timeframe=interval,
                bar_time=candle.time,
            )
            artifacts_for_bar = [dict(artifact) for artifact in decision_result.artifacts]
            selected = decision_result.selected_artifact
            if selected is not None and str(selected.get("evaluation_result") or "") == "matched_selected":
                _annotate_selected_artifact_outputs(
                    artifacts=artifacts_for_bar,
                    selected_artifact=selected,
                    outputs=frame.outputs,
                    output_types=engine.output_types,
                )
                signal = _build_strategy_preview_signal(preview_id=preview_id, artifact=selected)
                strategy_signals.append(signal)
                marker = _build_marker(artifact=selected, candle=candle)
                marker["signal_id"] = signal["signal_id"]
                marker["source_type"] = signal["source_type"]
                marker["source_id"] = signal["source_id"]
                strategy_markers.append(marker)
            decision_artifacts.extend(artifacts_for_bar)
        preview_replay_ms = max((time.perf_counter() - replay_started) * 1000.0, 0.0)

        decision_artifacts.sort(
            key=lambda artifact: (
                -int(artifact.get("bar_epoch") or 0),
                str(artifact.get("rule_id") or ""),
            )
        )
        overlays = list(preview_overlays)
        if strategy_markers:
            overlays.append(
                _build_strategy_signal_overlay(
                    strategy_id=str(strategy_id),
                    instrument_id=str(instrument_id),
                    markers=strategy_markers,
                )
            )
        machine_payload = {
            "signals": strategy_signals,
            "decision_artifacts": decision_artifacts,
            "rule_matches": len(strategy_signals),
        }
        ui_payload = {
            "overlays": overlays,
        }
        result["instruments"][instrument_id] = {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "window": {
                "start": start,
                "end": end,
                "interval": interval,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "datasource": datasource,
                "exchange": exchange,
            },
            "machine": machine_payload,
            "ui": ui_payload,
            "signals": strategy_signals,
            "decision_artifacts": decision_artifacts,
            "rule_matches": len(strategy_signals),
            "overlays": overlays,
            "missing_indicators": missing_indicators,
            "status": "missing_indicators" if missing_indicators else "ok",
            "perf": {
                "candle_fetch_ms": candle_fetch_ms,
                "preview_replay_ms": preview_replay_ms,
            },
        }

    return result


__all__ = ["evaluate_strategy_preview"]

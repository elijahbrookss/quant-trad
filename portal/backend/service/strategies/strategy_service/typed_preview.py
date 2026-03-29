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
from strategies.compiler import compile_strategy
from strategies.evaluator import DecisionEvaluationState, evaluate_strategy_bar


logger = logging.getLogger(__name__)


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
    start: str,
    end: str,
    interval: str,
    instrument_ids: Sequence[str],
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    _ = config
    if not instrument_ids:
        raise ValueError("instrument_ids is required for strategy preview")
    if not record.indicator_ids:
        raise ValueError("Strategy has no indicators attached")

    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if end_dt <= start_dt:
        raise ValueError("end must be after start")

    compiled_strategy = compile_strategy(
        strategy_id=strategy_id,
        timeframe=interval,
        rules=[rule.to_dict() for rule in record.rules.values()],
        attached_indicator_ids=record.indicator_ids,
        indicator_meta_getter=get_instance_meta,
    )

    result: Dict[str, Any] = {
        "strategy_id": strategy_id,
        "strategy_name": record.name,
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
            decision_artifacts.extend(decision_result.artifacts)
            selected = decision_result.selected_artifact
            if selected is not None and str(selected.get("evaluation_result") or "") == "matched_selected":
                strategy_markers.append(_build_marker(artifact=selected, candle=candle))
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
            "decision_artifacts": decision_artifacts,
            "rule_matches": sum(
                1
                for artifact in decision_artifacts
                if str(artifact.get("evaluation_result") or "") == "matched_selected"
            ),
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

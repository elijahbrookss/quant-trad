"""Typed-output strategy preview replay."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import IndicatorExecutionContext
from signals.overlays.transformers import apply_overlay_transform
from signals.overlays.schema import build_overlay
from portal.backend.service.indicators.indicator_service import (
    build_runtime_indicator_graph,
    get_instance_meta,
)
from portal.backend.service.market import candle_service, instrument_service
from strategies.evaluator import evaluate_typed_rules


logger = logging.getLogger(__name__)


def _parse_iso(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_candles(df: pd.DataFrame) -> List[Candle]:
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


def _extract_clauses(when: Mapping[str, Any]) -> List[Dict[str, Any]]:
    if str(when.get("type") or "").strip().lower() == "all":
        conditions = when.get("conditions")
        if isinstance(conditions, list):
            return [dict(item) for item in conditions if isinstance(item, Mapping)]
    return [dict(when)]


def _flow_from_when(when: Mapping[str, Any]) -> Dict[str, Any]:
    clauses = _extract_clauses(when)
    trigger = next((dict(clause) for clause in clauses if clause.get("type") == "signal_match"), {})
    guards = [dict(clause) for clause in clauses if clause.get("type") in {"context_match", "metric_match"}]
    return {"trigger": trigger, "guards": guards}


def _build_marker(*, action: str, candle: Candle, rule: Mapping[str, Any], trigger: Mapping[str, Any]) -> Dict[str, Any]:
    is_buy = str(action).lower() == "buy"
    return {
        "time": int(candle.time.timestamp()),
        "price": float(candle.close),
        "color": "#10b981" if is_buy else "#f87171",
        "shape": "arrowUp" if is_buy else "arrowDown",
        "position": "belowBar" if is_buy else "aboveBar",
        "text": str(rule.get("name") or trigger.get("event_key") or action).strip(),
        "subtype": "strategy_signal",
        "rule_id": rule.get("id"),
        "indicator_id": trigger.get("indicator_id"),
        "output_name": trigger.get("output_name"),
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


def _build_trigger_rows(
    *,
    rule: Mapping[str, Any],
    candle: Candle,
    outputs: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    when = rule.get("when")
    if not isinstance(when, Mapping):
        return []
    flow = _flow_from_when(when)
    trigger = flow.get("trigger") if isinstance(flow.get("trigger"), Mapping) else {}
    guards = flow.get("guards") if isinstance(flow.get("guards"), list) else []
    output_key = f"{trigger.get('indicator_id')}.{trigger.get('output_name')}"
    runtime_output = outputs.get(output_key)
    if runtime_output is None or not runtime_output.ready:
        return []
    events = runtime_output.value.get("events") if isinstance(runtime_output.value, Mapping) else None
    if not isinstance(events, list):
        return []

    guard_rows: List[Dict[str, Any]] = []
    for guard in guards:
        guard_key = f"{guard.get('indicator_id')}.{guard.get('output_name')}"
        guard_output = outputs.get(guard_key)
        if guard_output is None or not guard_output.ready:
            continue
        row: Dict[str, Any] = {
            "type": guard.get("type"),
            "indicator_id": guard.get("indicator_id"),
            "output_name": guard.get("output_name"),
        }
        if guard.get("type") == "context_match":
            row["expected"] = guard.get("state_key")
            row["actual"] = guard_output.value.get("state_key")
        elif guard.get("type") == "metric_match":
            field = str(guard.get("field") or "")
            row["field"] = field
            row["operator"] = guard.get("operator")
            row["expected"] = guard.get("value")
            row["actual"] = guard_output.value.get(field)
        guard_rows.append(row)

    rows: List[Dict[str, Any]] = []
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            continue
        if str(event.get("key") or "") != str(trigger.get("event_key") or ""):
            continue
        epoch = int(candle.time.timestamp())
        rows.append(
            {
                "row_id": f"{rule.get('id')}|{epoch}|{index}",
                "epoch": epoch,
                "timestamp": candle.time.isoformat(),
                "action": rule.get("action"),
                "side": "BUY" if str(rule.get("action")).lower() == "buy" else "SELL",
                "rule_id": rule.get("id"),
                "rule_name": rule.get("name"),
                "trigger_indicator_id": trigger.get("indicator_id"),
                "trigger_output_name": trigger.get("output_name"),
                "event_key": trigger.get("event_key"),
                "guards": guard_rows,
            }
        )
    return rows


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
    if not instrument_ids:
        raise ValueError("instrument_ids is required for strategy preview")
    if not record.indicator_ids:
        raise ValueError("Strategy has no indicators attached")

    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if end_dt <= start_dt:
        raise ValueError("end must be after start")

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
        df = candle_service.fetch_ohlcv_by_instrument(
            instrument_id,
            start,
            end,
            interval,
        )
        candles = _build_candles(df)
        candle_fetch_ms = max((time.perf_counter() - fetch_started) * 1000.0, 0.0)
        if not candles:
            raise ValueError(f"No candles returned for {instrument_id}")
        _configure_replay_window(indicators, history_bars=len(candles))

        strategy_markers: List[Dict[str, Any]] = []
        trigger_rows: List[Dict[str, Any]] = []
        preview_overlays: List[Dict[str, Any]] = []

        replay_started = time.perf_counter()
        last_index = len(candles) - 1
        for index, candle in enumerate(candles):
            frame = engine.step(
                bar=candle,
                bar_time=candle.time,
                include_overlays=index == last_index,
            )
            preview_overlays = _collect_ready_overlays(
                frame.overlays,
                current_epoch=int(candle.time.timestamp()),
            )
            matches = evaluate_typed_rules(
                rules=record.rules,
                outputs=frame.outputs,
                output_types=engine.output_types,
                current_epoch=int(candle.time.timestamp()),
            )
            for match in matches:
                rule = record.rules.get(match.get("rule_id"))
                if not isinstance(rule, Mapping):
                    continue
                rows = _build_trigger_rows(rule=rule, candle=candle, outputs=frame.outputs)
                if not rows:
                    continue
                trigger_rows.extend(rows)
                marker = _build_marker(action=match.get("action") or "", candle=candle, rule=rule, trigger=rows[0] | {
                    "indicator_id": rows[0].get("trigger_indicator_id"),
                    "output_name": rows[0].get("trigger_output_name"),
                    "event_key": rows[0].get("event_key"),
                })
                strategy_markers.append(marker)
        preview_replay_ms = max((time.perf_counter() - replay_started) * 1000.0, 0.0)

        trigger_rows.sort(key=lambda row: int(row.get("epoch") or 0), reverse=True)
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
            "trigger_rows": trigger_rows,
            "rule_matches": len(trigger_rows),
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

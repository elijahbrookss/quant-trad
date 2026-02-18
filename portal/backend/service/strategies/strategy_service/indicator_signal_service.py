"""Indicator signal generation helpers for strategy evaluation."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Set, Tuple

from ...indicators.indicator_service import generate_signals_for_instance
from ...indicators.indicator_service import runtime_input_plan_for_instance
from strategies import evaluator


logger = logging.getLogger(__name__)

_ensure_signal_direction = evaluator._ensure_signal_direction
_summarise_signal_population = evaluator._summarise_signal_population
_format_counter = evaluator._format_counter
_extract_signal_epoch = evaluator._extract_signal_epoch


def _parse_epoch_seconds(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except ValueError:
        return None


def _filter_signals_for_window(
    signals: Sequence[Any],
    *,
    start_epoch: int | None,
    end_epoch: int | None,
) -> tuple[List[Any], int, int]:
    if start_epoch is None or end_epoch is None:
        return list(signals), 0, 0

    kept: List[Any] = []
    dropped_outside = 0
    dropped_untimed = 0
    for candidate in signals:
        if not isinstance(candidate, Mapping):
            dropped_untimed += 1
            continue
        epoch = _extract_signal_epoch(candidate)
        if epoch is None:
            dropped_untimed += 1
            continue
        if epoch < start_epoch or epoch > end_epoch:
            dropped_outside += 1
            continue
        kept.append(candidate)
    return kept, dropped_outside, dropped_untimed


def _merge_enabled_rules(existing: Any, extras: Iterable[str]) -> List[str]:
    ordered: List[str] = []
    seen: Set[str] = set()

    sources: List[Any] = []
    if existing is not None:
        sources.append(existing)
    sources.append(extras)

    for source in sources:
        if not source:
            continue
        if isinstance(source, Mapping):
            iterable = source.values()
        elif isinstance(source, (str, bytes)):
            iterable = [source]
        else:
            iterable = source

        for item in iterable:
            text = str(item).strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(text)

    return ordered


def _config_diff(base: Mapping[str, Any], derived: Mapping[str, Any]) -> Dict[str, Any]:
    diff: Dict[str, Any] = {}
    base_keys = set(base.keys())
    for key, value in derived.items():
        if key not in base_keys or base.get(key) != value:
            diff[key] = value
    removed = [key for key in base_keys if key not in derived]
    if removed:
        diff["_removed"] = sorted(removed)
    return diff


def generate_indicator_payloads(
    *,
    strategy_id: str,
    instrument_id: str,
    indicator_ids: Sequence[str],
    indicator_rule_map: Mapping[str, Sequence[str]],
    start: str,
    end: str,
    interval: str,
    symbol: str,
    datasource: str,
    exchange: str | None,
    base_config: Mapping[str, Any],
    run_id: str,
) -> Tuple[Dict[str, Dict[str, Any]], List[str], int]:
    indicator_payloads: Dict[str, Dict[str, Any]] = {}
    missing_indicators: List[str] = []
    total_signals = 0

    logger.info(
        "strategy_signal_preview_start | run_id=%s strategy=%s instrument_id=%s start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s config_keys=%s indicator_count=%d",
        run_id,
        strategy_id,
        instrument_id,
        start,
        end,
        interval,
        symbol,
        datasource,
        exchange,
        sorted(base_config.keys()),
        len(indicator_ids),
    )
    start_epoch = _parse_epoch_seconds(start)
    end_epoch = _parse_epoch_seconds(end)

    for inst_id in indicator_ids:
        try:
            per_config = dict(base_config)
            runtime_overrides = base_config.get("runtime_input_plan_overrides")
            per_override = None
            if isinstance(runtime_overrides, Mapping):
                candidate = runtime_overrides.get(inst_id)
                if isinstance(candidate, Mapping):
                    per_override = dict(candidate)
            rule_filters = indicator_rule_map.get(inst_id)
            if rule_filters:
                merged_rules = _merge_enabled_rules(per_config.get("enabled_rules"), rule_filters)
                if merged_rules:
                    per_config["enabled_rules"] = merged_rules
                else:
                    per_config.pop("enabled_rules", None)
            logger.info(
                "strategy_signal_preview_generate | run_id=%s strategy=%s instrument_id=%s indicator=%s start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s enabled_rules=%s config_diff=%s",
                run_id,
                strategy_id,
                instrument_id,
                inst_id,
                start,
                end,
                interval,
                symbol,
                datasource,
                exchange,
                per_config.get("enabled_rules"),
                _config_diff(base_config, per_config),
            )
            runtime_input_plan = runtime_input_plan_for_instance(
                inst_id,
                strategy_interval=interval,
                start=start,
                end=end,
            )
            if per_override:
                if per_override.get("start"):
                    runtime_input_plan["start"] = str(per_override.get("start"))
                if per_override.get("end"):
                    runtime_input_plan["end"] = str(per_override.get("end"))
                if per_override.get("source_timeframe"):
                    runtime_input_plan["source_timeframe"] = str(per_override.get("source_timeframe"))
            plan_start = str(runtime_input_plan.get("start") or start)
            plan_end = str(runtime_input_plan.get("end") or end)
            plan_interval = str(runtime_input_plan.get("source_timeframe") or interval)
            per_config["runtime_input_plan"] = dict(runtime_input_plan)
            logger.info(
                "strategy_signal_preview_runtime_input_plan | run_id=%s strategy=%s instrument_id=%s indicator=%s strategy_interval=%s source_timeframe=%s start=%s end=%s session_scope=%s alignment=%s normalization=%s",
                run_id,
                strategy_id,
                instrument_id,
                inst_id,
                interval,
                plan_interval,
                plan_start,
                plan_end,
                runtime_input_plan.get("session_scope"),
                runtime_input_plan.get("alignment"),
                runtime_input_plan.get("normalization"),
            )
            payload = generate_signals_for_instance(
                inst_id,
                start=plan_start,
                end=plan_end,
                interval=interval,
                symbol=symbol,
                datasource=datasource,
                exchange=exchange,
                config=per_config,
            )
            payload["runtime_input_plan"] = dict(runtime_input_plan)
            if isinstance(payload, dict):
                payload["requested_window"] = {"start": start, "end": end}
            indicator_payloads[inst_id] = payload
            signals_obj = payload.get("signals") if isinstance(payload, Mapping) else None
            if isinstance(signals_obj, Sequence) and not isinstance(signals_obj, (str, bytes)):
                filtered_signals, dropped_outside, dropped_untimed = _filter_signals_for_window(
                    signals_obj,
                    start_epoch=start_epoch,
                    end_epoch=end_epoch,
                )
                if isinstance(payload, dict):
                    payload["signals"] = filtered_signals
                signals_obj = filtered_signals
                if dropped_outside or dropped_untimed:
                    logger.info(
                        "strategy_signal_preview_window_filter | run_id=%s strategy=%s instrument_id=%s indicator=%s kept=%d dropped_outside=%d dropped_untimed=%d start=%s end=%s",
                        run_id,
                        strategy_id,
                        instrument_id,
                        inst_id,
                        len(filtered_signals),
                        dropped_outside,
                        dropped_untimed,
                        start,
                        end,
                    )
            signal_count = len(signals_obj) if isinstance(signals_obj, list) else 0
            total_signals += signal_count
            error_hint = payload.get("error") if isinstance(payload, Mapping) else None
            logger.info(
                "strategy_signal_preview_result | run_id=%s strategy=%s instrument_id=%s indicator=%s signals=%d start=%s end=%s interval=%s error=%s",
                run_id,
                strategy_id,
                instrument_id,
                inst_id,
                signal_count,
                start,
                end,
                interval,
                error_hint,
            )
            if isinstance(signals_obj, list):
                for signal in signals_obj:
                    if isinstance(signal, dict):
                        _ensure_signal_direction(signal)
                summary = _summarise_signal_population(signals_obj)
                logger.debug(
                    "strategy_indicator_signal_summary | strategy=%s instrument_id=%s indicator=%s total=%d types=[%s] rules=[%s] directions=[%s]",
                    strategy_id,
                    instrument_id,
                    inst_id,
                    len(signals_obj),
                    _format_counter(summary["types"]),
                    _format_counter(summary["rules"]),
                    _format_counter(summary["directions"]),
                )
        except KeyError:
            missing_indicators.append(inst_id)
            indicator_payloads[inst_id] = {"error": "Indicator not available"}
            logger.warning(
                "strategy_indicator_missing | strategy=%s instrument_id=%s indicator=%s",
                strategy_id,
                instrument_id,
                inst_id,
            )
            continue
        except Exception as exc:  # noqa: BLE001 - propagate failures as payload errors
            logger.warning(
                "strategy_signal_preview_failed | run_id=%s strategy=%s instrument_id=%s indicator=%s error=%s",
                run_id,
                strategy_id,
                instrument_id,
                inst_id,
                exc,
            )
            indicator_payloads[inst_id] = {"error": str(exc)}

    logger.info(
        "strategy_signal_preview_complete | run_id=%s strategy=%s instrument_id=%s start=%s end=%s interval=%s symbol=%s datasource=%s exchange=%s indicators=%d missing=%s total_signals=%d",
        run_id,
        strategy_id,
        instrument_id,
        start,
        end,
        interval,
        symbol,
        datasource,
        exchange,
        len(indicator_ids),
        missing_indicators,
        total_signals,
    )

    return indicator_payloads, missing_indicators, total_signals

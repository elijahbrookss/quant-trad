"""Strategy evaluation orchestration helpers."""

from __future__ import annotations

import logging
import uuid
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence

from data_providers.utils.ohlcv import interval_to_timedelta

from ...market import instrument_service
from .filter_runtime import apply_filter_gates, build_filter_gate_snapshot
from .filters import FilterDefinition
from .indicator_signal_service import generate_indicator_payloads
from strategies import markers


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvaluationInputs:
    strategy_id: str
    start: str
    end: str
    interval: str
    instrument_ids: Sequence[str]
    config: Mapping[str, Any]


@dataclass(frozen=True)
class EvaluationContext:
    record: Any
    timeframe_seconds: int
    start_dt: datetime
    end_dt: datetime
    indicator_rule_map: Mapping[str, List[str]]
    rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]]
    stats_snapshot: Any


class StrategyEvaluationDependencies(Protocol):
    """Dependency boundary for strategy evaluation adapters."""

    def build_filter_gate_snapshot(
        self,
        *,
        instrument_ids: Iterable[str],
        timeframe_seconds: int,
        start: datetime,
        end: datetime,
        global_filters: Sequence[FilterDefinition],
        rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]],
    ) -> Any: ...

    def generate_indicator_payloads(
        self,
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
    ) -> tuple[Dict[str, Dict[str, Any]], List[str], int]: ...

    def apply_filter_gates(
        self,
        *,
        rule_results: List[Mapping[str, Any]],
        instrument_id: str,
        timeframe_seconds: int,
        stats_snapshot: Any,
        global_filters: Sequence[FilterDefinition],
        rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]],
    ) -> None: ...


@dataclass(frozen=True)
class DefaultStrategyEvaluationDependencies:
    """Default strategy evaluation dependencies for portal runtime."""

    def build_filter_gate_snapshot(
        self,
        *,
        instrument_ids: Iterable[str],
        timeframe_seconds: int,
        start: datetime,
        end: datetime,
        global_filters: Sequence[FilterDefinition],
        rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]],
    ) -> Any:
        return build_filter_gate_snapshot(
            instrument_ids=instrument_ids,
            timeframe_seconds=timeframe_seconds,
            start=start,
            end=end,
            global_filters=global_filters,
            rule_filters_by_rule=rule_filters_by_rule,
        )

    def generate_indicator_payloads(
        self,
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
    ) -> tuple[Dict[str, Dict[str, Any]], List[str], int]:
        return generate_indicator_payloads(
            strategy_id=strategy_id,
            instrument_id=instrument_id,
            indicator_ids=indicator_ids,
            indicator_rule_map=indicator_rule_map,
            start=start,
            end=end,
            interval=interval,
            symbol=symbol,
            datasource=datasource,
            exchange=exchange,
            base_config=base_config,
            run_id=run_id,
        )

    def apply_filter_gates(
        self,
        *,
        rule_results: List[Mapping[str, Any]],
        instrument_id: str,
        timeframe_seconds: int,
        stats_snapshot: Any,
        global_filters: Sequence[FilterDefinition],
        rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]],
    ) -> None:
        apply_filter_gates(
            rule_results=rule_results,
            instrument_id=instrument_id,
            timeframe_seconds=timeframe_seconds,
            stats_snapshot=stats_snapshot,
            global_filters=global_filters,
            rule_filters_by_rule=rule_filters_by_rule,
        )


class StrategyEvaluationOrchestrator:
    """Coordinate strategy preview evaluation for indicators, rules, and filters."""

    def __init__(
        self,
        record: Any,
        *,
        dependencies: Optional[StrategyEvaluationDependencies] = None,
    ) -> None:
        self._record = record
        self._dependencies = dependencies or DefaultStrategyEvaluationDependencies()

    def build_inputs(
        self,
        *,
        strategy_id: str,
        start: str,
        end: str,
        interval: str,
        instrument_ids: Optional[Iterable[str]],
        config: Optional[Dict[str, Any]],
    ) -> EvaluationInputs:
        requested_ids = [str(item).strip() for item in (instrument_ids or []) if item]
        if not requested_ids:
            raise ValueError("instrument_ids is required for signal preview")
        return EvaluationInputs(
            strategy_id=strategy_id,
            start=start,
            end=end,
            interval=interval,
            instrument_ids=requested_ids,
            config=dict(config or {}),
        )

    def build_context(self, inputs: EvaluationInputs) -> EvaluationContext:
        timeframe_delta = interval_to_timedelta(inputs.interval)
        timeframe_seconds = int(timeframe_delta.total_seconds())
        if timeframe_seconds <= 0:
            raise ValueError(f"Invalid interval: {inputs.interval}")
        start_dt = _parse_timestamp(inputs.start)
        end_dt = _parse_timestamp(inputs.end)

        self._validate_instruments(inputs.instrument_ids)
        self._validate_indicators(inputs.strategy_id, inputs.instrument_ids)

        indicator_rule_map = self._build_indicator_rule_map()
        rule_filters_by_rule = {
            rule_id: list(rule.filters) for rule_id, rule in self._record.rules.items()
        }
        stats_snapshot = self._dependencies.build_filter_gate_snapshot(
            instrument_ids=inputs.instrument_ids,
            timeframe_seconds=timeframe_seconds,
            start=start_dt,
            end=end_dt,
            global_filters=self._record.global_filters,
            rule_filters_by_rule=rule_filters_by_rule,
        )

        self._validate_orphaned_indicators(inputs.strategy_id, indicator_rule_map)

        return EvaluationContext(
            record=self._record,
            timeframe_seconds=timeframe_seconds,
            start_dt=start_dt,
            end_dt=end_dt,
            indicator_rule_map=indicator_rule_map,
            rule_filters_by_rule=rule_filters_by_rule,
            stats_snapshot=stats_snapshot,
        )

    def evaluate(
        self,
        inputs: EvaluationInputs,
        context: EvaluationContext,
    ) -> Dict[str, Any]:
        instrument_payloads = {
            instrument_id: self._evaluate_for_instrument(
                instrument_id=instrument_id,
                inputs=inputs,
                context=context,
            )
            for instrument_id in inputs.instrument_ids
        }
        return {
            "strategy_id": self._record.id,
            "strategy_name": self._record.name,
            "instruments": instrument_payloads,
        }

    def _build_indicator_rule_map(self) -> Dict[str, List[str]]:
        indicator_rule_map: Dict[str, List[str]] = {}
        for rule in self._record.rules.values():
            for condition in rule.conditions:
                indicator_id = condition.indicator_id
                rule_id = condition.rule_id
                if not indicator_id or not rule_id:
                    continue
                bucket = indicator_rule_map.setdefault(indicator_id, [])
                if rule_id not in bucket:
                    bucket.append(rule_id)
        return indicator_rule_map

    def _validate_instruments(self, requested_ids: Sequence[str]) -> None:
        allowed_instruments: Dict[str, Any] = {}
        for slot in self._record.instruments:
            inst_id = None
            if isinstance(slot.metadata, dict):
                inst_id = slot.metadata.get("instrument_id")
            if not inst_id:
                raise ValueError(f"Instrument id missing for strategy slot {slot.symbol}")
            allowed_instruments[str(inst_id)] = slot
        for inst_id in requested_ids:
            if inst_id not in allowed_instruments:
                raise ValueError(f"Instrument {inst_id} is not attached to this strategy")

    def _validate_indicators(self, strategy_id: str, requested_ids: Sequence[str]) -> None:
        if self._record.indicator_ids:
            return
        logger.error(
            "strategy_signal_preview_no_indicators | strategy=%s instrument_ids=%s rules=%d "
            "message='Strategy has no indicators attached. Please attach indicators to this strategy before running signal preview.'",
            strategy_id,
            requested_ids,
            len(self._record.rules),
        )
        raise ValueError(
            "Strategy has no indicators attached. Please attach indicators to this strategy before running signal preview."
        )

    def _validate_orphaned_indicators(
        self, strategy_id: str, indicator_rule_map: Mapping[str, Sequence[str]]
    ) -> None:
        orphaned_indicators = [
            ind_id for ind_id in indicator_rule_map.keys() if ind_id not in self._record.indicator_ids
        ]
        if not orphaned_indicators:
            return
        logger.error(
            "strategy_signal_preview_orphaned_indicators | strategy=%s orphaned_indicators=%s attached_indicators=%s "
            "message='Strategy rules reference indicators that are not attached to the strategy.'",
            strategy_id,
            orphaned_indicators,
            self._record.indicator_ids,
        )
        raise ValueError(
            f"Strategy rules reference indicators that are not attached to the strategy: {', '.join(orphaned_indicators)}. "
            "Please attach these indicators to the strategy or update the rules."
        )

    def _evaluate_for_instrument(
        self,
        *,
        instrument_id: str,
        inputs: EvaluationInputs,
        context: EvaluationContext,
    ) -> Dict[str, Any]:
        instrument_rec = instrument_service.get_instrument_record(instrument_id)
        if not instrument_rec:
            raise ValueError(f"Instrument record not found: {instrument_id}")

        effective_symbol = instrument_rec.get("symbol")
        effective_datasource = instrument_rec.get("datasource")
        effective_exchange = instrument_rec.get("exchange")
        if not effective_symbol:
            raise ValueError(f"Instrument {instrument_id} is missing a symbol")
        if not effective_datasource:
            raise ValueError(f"Instrument {instrument_id} is missing a datasource")

        run_id = uuid.uuid4().hex
        indicator_started = time.perf_counter()
        indicator_payloads, missing_indicators, total_signals = self._dependencies.generate_indicator_payloads(
            strategy_id=inputs.strategy_id,
            instrument_id=instrument_id,
            indicator_ids=self._record.indicator_ids,
            indicator_rule_map=context.indicator_rule_map,
            start=inputs.start,
            end=inputs.end,
            interval=inputs.interval,
            symbol=effective_symbol,
            datasource=effective_datasource,
            exchange=effective_exchange,
            base_config=inputs.config,
            run_id=run_id,
        )
        indicator_eval_ms = max((time.perf_counter() - indicator_started) * 1000.0, 0.0)

        rule_started = time.perf_counter()
        rule_results = [rule.evaluate(indicator_payloads) for rule in self._record.rules.values()]
        for res in rule_results:
            res["signal_conditions"] = {
                "matched": bool(res.get("matched")),
                "reason": res.get("reason"),
                "direction": res.get("direction"),
            }
            res["global_filters"] = []
            res["rule_filters"] = []
            res["final_decision"] = {
                "allowed": False,
                "reason": "signal_conditions_failed",
            }

        self._dependencies.apply_filter_gates(
            rule_results=rule_results,
            instrument_id=instrument_id,
            timeframe_seconds=context.timeframe_seconds,
            stats_snapshot=context.stats_snapshot,
            global_filters=self._record.global_filters,
            rule_filters_by_rule=context.rule_filters_by_rule,
        )

        for res in rule_results:
            conditions = res.get("conditions") or []
            matched_count = sum(1 for cond in conditions if cond.get("matched"))
            total_conditions = len(conditions)
            logger.debug(
                "strategy_rule_evaluated | strategy=%s instrument_id=%s rule=%s action=%s matched=%s matched_conditions=%d/%d reason=%s",
                inputs.strategy_id,
                instrument_id,
                res.get("rule_id"),
                res.get("action"),
                res.get("matched"),
                matched_count,
                total_conditions,
                res.get("reason"),
            )
            for cond in conditions:
                logger.debug(
                    "strategy_rule_condition | strategy=%s instrument_id=%s rule=%s indicator=%s signal_type=%s expected_direction=%s detected_direction=%s matched=%s reason=%s stats=%s observed_rules=%s observed_directions=%s",
                    inputs.strategy_id,
                    instrument_id,
                    res.get("rule_id"),
                    cond.get("indicator_id"),
                    cond.get("signal_type"),
                    cond.get("direction"),
                    cond.get("direction_detected"),
                    cond.get("matched"),
                    cond.get("reason"),
                    cond.get("stats"),
                    cond.get("observed_rules"),
                    cond.get("observed_directions"),
                )

        buy_signals = [
            res
            for res in rule_results
            if res["matched"]
            and res["action"] == "buy"
            and res.get("final_decision", {}).get("allowed")
        ]
        sell_signals = [
            res
            for res in rule_results
            if res["matched"]
            and res["action"] == "sell"
            and res.get("final_decision", {}).get("allowed")
        ]

        chart_markers = markers.build_chart_markers(buy_signals, sell_signals)
        rule_eval_ms = max((time.perf_counter() - rule_started) * 1000.0, 0.0)

        logger.info(
            "strategy_signals_generated | strategy=%s instrument_id=%s symbol=%s interval=%s start=%s end=%s buys=%d sells=%d",
            inputs.strategy_id,
            instrument_id,
            effective_symbol,
            inputs.interval,
            inputs.start,
            inputs.end,
            len(buy_signals),
            len(sell_signals),
        )

        if not buy_signals and not sell_signals:
            aggregate_stats = {
                "signals": 0,
                "type_matches": 0,
                "rule_matches": 0,
                "direction_matches": 0,
                "final_matches": 0,
            }
            aggregate_rules: set[str] = set()
            aggregate_directions: set[str] = set()
            for res in rule_results:
                for cond in res.get("conditions") or []:
                    stats = cond.get("stats") or {}
                    for key in aggregate_stats:
                        try:
                            aggregate_stats[key] += int(stats.get(key, 0) or 0)
                        except (TypeError, ValueError):  # pragma: no cover - defensive
                            continue
                    observed_rules = cond.get("observed_rules") or []
                    observed_directions = cond.get("observed_directions") or []
                    aggregate_rules.update(map(str, observed_rules))
                    aggregate_directions.update(map(str, observed_directions))

            logger.info(
                "strategy_signals_none | strategy=%s instrument_id=%s symbol=%s interval=%s start=%s end=%s indicators=%d rules=%d stats=%s observed_rules=%s observed_directions=%s",
                inputs.strategy_id,
                instrument_id,
                effective_symbol,
                inputs.interval,
                inputs.start,
                inputs.end,
                len(indicator_payloads),
                len(rule_results),
                aggregate_stats,
                sorted(aggregate_rules),
                sorted(aggregate_directions),
            )
            for res in rule_results:
                conditions = res.get("conditions") or []
                matched_count = sum(1 for cond in conditions if cond.get("matched"))
                total_conditions = len(conditions)
                logger.info(
                    "strategy_rule_trace | strategy=%s instrument_id=%s rule=%s action=%s matched=%s matched_conditions=%d/%d reason=%s",
                    inputs.strategy_id,
                    instrument_id,
                    res.get("rule_id"),
                    res.get("action"),
                    res.get("matched"),
                    matched_count,
                    total_conditions,
                    res.get("reason"),
                )
                for cond in conditions:
                    logger.info(
                        "strategy_condition_trace | strategy=%s instrument_id=%s rule=%s indicator=%s signal_type=%s expected_direction=%s detected_direction=%s matched=%s reason=%s stats=%s observed_rules=%s observed_directions=%s",
                        inputs.strategy_id,
                        instrument_id,
                        res.get("rule_id"),
                        cond.get("indicator_id"),
                        cond.get("signal_type"),
                        cond.get("direction"),
                        cond.get("direction_detected"),
                        cond.get("matched"),
                        cond.get("reason"),
                        cond.get("stats"),
                        cond.get("observed_rules"),
                        cond.get("observed_directions"),
                    )

        status = "ok"
        if missing_indicators:
            status = "missing_indicators"

        return {
            "instrument_id": instrument_id,
            "symbol": effective_symbol,
            "window": {
                "start": inputs.start,
                "end": inputs.end,
                "interval": inputs.interval,
                "instrument_id": instrument_id,
                "symbol": effective_symbol,
                "datasource": effective_datasource,
                "exchange": effective_exchange,
            },
            "indicator_results": indicator_payloads,
            "rule_results": rule_results,
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "chart_markers": chart_markers,
            "applied_inputs": {
                "instrument_id": instrument_id,
                "symbol": effective_symbol,
                "timeframe": self._record.timeframe,
                "datasource": effective_datasource,
                "exchange": effective_exchange,
            },
            "missing_indicators": missing_indicators,
            "status": status,
            "total_signals": total_signals,
            "perf": {
                "indicator_eval_ms": indicator_eval_ms,
                "rule_eval_ms": rule_eval_ms,
            },
        }


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1]
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
    return datetime.utcnow()

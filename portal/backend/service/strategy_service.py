"""In-memory strategy rule orchestration for the portal."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from .indicator_service import generate_signals_for_instance, get_instance_meta


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a naive UTC timestamp for metadata fields."""

    return datetime.utcnow()


def _normalise_direction(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip().lower()
    if text in {"long", "buy", "bull", "bullish", "above", "up"}:
        return "long"
    if text in {"short", "sell", "bear", "bearish", "below", "down"}:
        return "short"
    return None


def _infer_signal_direction(signal: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(signal, dict):
        return None

    def _iter_sources() -> Iterable[Mapping[str, Any]]:
        yield signal
        metadata = signal.get("metadata")
        if isinstance(metadata, Mapping):
            yield metadata

    for source in _iter_sources():
        direct = _normalise_direction(source.get("direction"))
        if direct:
            return direct

        breakout_direction = _normalise_direction(source.get("breakout_direction"))
        if breakout_direction:
            return breakout_direction

        role_value = str(source.get("retest_role", "")).strip().lower()
        if role_value == "support":
            return "long"
        if role_value == "resistance":
            return "short"

        level_kind = str(
            source.get("level_type")
            or source.get("level_kind")
            or source.get("level_role")
            or ""
        ).strip().lower()
        if level_kind in {"vah", "value_area_high", "resistance"}:
            return "short"
        if level_kind in {"val", "value_area_low", "support"}:
            return "long"

    # Fallback to rule-level hints using either top-level or metadata identifiers.
    rule_id = str(signal.get("pattern_id") or "").lower()
    if not rule_id:
        metadata = signal.get("metadata")
        if isinstance(metadata, Mapping):
            rule_id = str(metadata.get("pattern_id") or "").lower()

    if rule_id.endswith("breakout"):
        for source in _iter_sources():
            candidate = _normalise_direction(source.get("breakout_direction"))
            if candidate:
                return candidate

    if rule_id.endswith("retest"):
        for source in _iter_sources():
            role_value = str(source.get("retest_role", "")).strip().lower()
            if role_value == "support":
                return "long"
            if role_value == "resistance":
                return "short"

    return None


def _extract_signal_price(signal: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(signal, dict):
        return None
    metadata = signal.get("metadata")
    candidates = []
    if isinstance(metadata, dict):
        candidates.extend(
            metadata.get(key)
            for key in (
                "price",
                "close",
                "retest_close",
                "trigger_price",
                "level_price",
                "poc",
            )
        )
    candidates.append(signal.get("price"))
    for value in candidates:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if not (number is None or number != number):  # NaN check
            return number
    return None


def _iso_to_epoch_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    try:
        return int(dt.timestamp())
    except (OverflowError, OSError, ValueError):
        return None


def _normalise_match_mode(value: Any) -> str:
    if isinstance(value, str) and value.strip().lower() == "any":
        return "any"
    return "all"


def _normalise_action(value: Any) -> str:
    action_value = str(value).strip().lower()
    if action_value not in {"buy", "sell"}:
        raise ValueError("Action must be 'buy' or 'sell'")
    return action_value


def _parse_conditions(
    strategy: "StrategyDefinition", raw_conditions: Optional[Iterable[Mapping[str, Any]]]
) -> List["RuleCondition"]:
    if not raw_conditions:
        raise ValueError("At least one condition must be provided")

    parsed: List[RuleCondition] = []
    for idx, condition in enumerate(raw_conditions):
        if not isinstance(condition, Mapping):
            raise ValueError(f"Condition at index {idx} must be an object")

        indicator_id = str(condition.get("indicator_id", "")).strip()
        if not indicator_id:
            raise ValueError(f"Condition {idx + 1} is missing indicator_id")
        if strategy.indicator_ids and indicator_id not in strategy.indicator_ids:
            raise ValueError(
                f"Indicator {indicator_id} is not attached to this strategy"
            )

        signal_type = str(condition.get("signal_type", "")).strip()
        if not signal_type:
            raise ValueError(f"Condition {idx + 1} is missing signal_type")

        rule_id = str(condition.get("rule_id", "")).strip() or None
        direction = _normalise_direction(condition.get("direction"))

        # Validate indicator exists.
        get_instance_meta(indicator_id)

        parsed.append(
            RuleCondition(
                indicator_id=indicator_id,
                signal_type=signal_type,
                rule_id=rule_id,
                direction=direction,
            )
        )

    return parsed


def _evaluate_condition(
    condition: "RuleCondition", indicator_payloads: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "indicator_id": condition.indicator_id,
        "signal_type": condition.signal_type,
        "rule_id": condition.rule_id,
        "direction": condition.direction,
        "matched": False,
        "signal": None,
        "reason": None,
    }

    payload = indicator_payloads.get(condition.indicator_id)
    if payload is None:
        info["reason"] = "No signals for indicator"
        return info

    if isinstance(payload, dict) and payload.get("error"):
        info["reason"] = str(payload.get("error"))
        return info

    signals = payload.get("signals") if isinstance(payload, dict) else None
    if not isinstance(signals, list):
        info["reason"] = "Indicator returned no signals"
        return info

    desired_type = str(condition.signal_type or "").lower()
    desired_rule = str(condition.rule_id or "").lower()
    desired_direction = _normalise_direction(condition.direction)

    for candidate in signals:
        if not isinstance(candidate, dict):
            continue
        cand_type = str(candidate.get("type", "")).lower()
        if desired_type and cand_type != desired_type:
            continue

        metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
        pattern_id = str(metadata.get("pattern_id", "")).lower()
        if desired_rule and pattern_id != desired_rule:
            continue

        cand_direction = _infer_signal_direction(candidate)
        if desired_direction and cand_direction != desired_direction:
            continue

        info["matched"] = True
        info["signal"] = candidate
        info["direction_detected"] = cand_direction
        info["reason"] = None
        return info

    info["reason"] = "No matching signals"
    return info


def _build_markers_for_results(
    results: Sequence[Mapping[str, Any]],
    *,
    action: str,
) -> List[Dict[str, Any]]:
    color = "#10b981" if action == "buy" else "#f87171"
    shape = "arrowUp" if action == "buy" else "arrowDown"
    markers: List[Dict[str, Any]] = []

    for res in results:
        rule_name = str(res.get("rule_name") or res.get("rule_id") or action.title())
        for signal in res.get("signals") or []:
            if not isinstance(signal, Mapping):
                continue
            epoch = _iso_to_epoch_seconds(signal.get("time"))
            price = _extract_signal_price(signal)
            if epoch is None or price is None:
                continue
            direction = _infer_signal_direction(signal) or ("long" if action == "buy" else "short")
            label = f"{rule_name} ({direction})" if direction else rule_name
            markers.append(
                {
                    "time": epoch,
                    "price": price,
                    "color": color,
                    "shape": shape,
                    "text": label,
                    "size": 1,
                    "subtype": "strategy_signal",
                    "direction": direction,
                    "rule_id": res.get("rule_id"),
                }
            )

    return markers


@dataclass
class RuleCondition:
    """Represents a single indicator signal requirement for a rule."""

    indicator_id: str
    signal_type: str
    rule_id: Optional[str] = None
    direction: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "indicator_id": self.indicator_id,
            "signal_type": self.signal_type,
            "rule_id": self.rule_id,
            "direction": self.direction,
        }


@dataclass
class StrategyRule:
    """Represents a rule composed of one or more indicator signal conditions."""

    id: str
    name: str
    action: str
    conditions: List[RuleCondition]
    match: str = "all"
    description: Optional[str] = None
    enabled: bool = True
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the rule for API responses."""

        return {
            "id": self.id,
            "name": self.name,
            "action": self.action,
            "conditions": [condition.to_dict() for condition in self.conditions],
            "match": self.match,
            "description": self.description,
            "enabled": self.enabled,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }

    def evaluate(
        self,
        indicator_payloads: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Evaluate the rule against collected indicator payloads."""

        matched = False
        reason: Optional[str] = None
        condition_results: List[Dict[str, Any]] = []
        trigger_signals: List[Dict[str, Any]] = []

        if not self.enabled:
            reason = "Rule disabled"
        elif not self.conditions:
            reason = "Rule has no conditions"
        else:
            match_results: List[bool] = []
            for condition in self.conditions:
                result = _evaluate_condition(condition, indicator_payloads)
                condition_results.append(result)
                match_results.append(result["matched"])
                if result["matched"] and result.get("signal"):
                    trigger_signals.append(result["signal"])

            if self.match == "any":
                matched = any(match_results)
            else:
                matched = bool(match_results) and all(match_results)

            if not matched and not reason:
                reason = "No matching signals"

        direction = None
        if trigger_signals:
            direction = _infer_signal_direction(trigger_signals[-1])

        return {
            "rule_id": self.id,
            "rule_name": self.name,
            "action": self.action,
            "matched": matched,
            "conditions": condition_results,
            "signals": trigger_signals if matched else [],
            "direction": direction,
            "reason": reason,
        }


@dataclass
class StrategyDefinition:
    """Domain model describing a user-defined strategy."""

    id: str
    name: str
    symbols: List[str]
    timeframe: str
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    indicator_ids: List[str] = field(default_factory=list)
    rules: MutableMapping[str, StrategyRule] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the strategy for API responses."""

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_ids": list(self.indicator_ids),
            "rules": [rule.to_dict() for rule in self.rules.values()],
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }

    def update(self, **fields: Any) -> None:
        """Apply partial updates to the strategy metadata."""

        if "name" in fields and fields["name"] is not None:
            self.name = str(fields["name"]).strip()
        if "description" in fields:
            description = fields["description"]
            self.description = str(description).strip() if description else None
        if "symbols" in fields and fields["symbols"] is not None:
            symbols = [
                str(symbol).strip()
                for symbol in fields["symbols"]
                if str(symbol).strip()
            ]
            if symbols:
                self.symbols = symbols[:1]
        if "timeframe" in fields and fields["timeframe"] is not None:
            self.timeframe = str(fields["timeframe"]).strip()
        if "datasource" in fields:
            datasource = fields["datasource"]
            self.datasource = str(datasource).strip() if datasource else None
        if "exchange" in fields:
            exchange = fields["exchange"]
            self.exchange = str(exchange).strip() if exchange else None
        if "indicator_ids" in fields and fields["indicator_ids"] is not None:
            indicator_ids = [
                str(identifier).strip()
                for identifier in fields["indicator_ids"]
                if str(identifier).strip()
            ]
            if indicator_ids:
                # Preserve ordering while dropping duplicates.
                self.indicator_ids = list(dict.fromkeys(indicator_ids))
        self.updated_at = _utcnow()

    def add_rule(self, rule: StrategyRule) -> None:
        """Attach a rule to the strategy."""

        self.rules[rule.id] = rule
        self.updated_at = _utcnow()

    def remove_rule(self, rule_id: str) -> None:
        """Detach a rule from the strategy."""

        if rule_id in self.rules:
            del self.rules[rule_id]
            self.updated_at = _utcnow()


class StrategyRegistry:
    """Holds all strategies for the running FastAPI instance."""

    def __init__(self) -> None:
        self._records: Dict[str, StrategyDefinition] = {}

    def list(self) -> List[Dict[str, Any]]:
        """Return serialised strategies for API responses."""

        return [record.to_dict() for record in self._records.values()]

    def get(self, strategy_id: str) -> StrategyDefinition:
        """Return the internal model for *strategy_id*."""

        record = self._records.get(strategy_id)
        if record is None:
            raise KeyError("Strategy not found")
        return record

    def create(
        self,
        name: str,
        *,
        symbols: Iterable[str],
        timeframe: str,
        description: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        indicator_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """Create a new strategy record and return its payload."""

        strategy_id = str(uuid.uuid4())
        clean_name = str(name).strip()
        clean_symbols = [
            str(symbol).strip()
            for symbol in symbols
            if str(symbol).strip()
        ]
        indicators = [
            str(identifier).strip()
            for identifier in (indicator_ids or [])
            if str(identifier).strip()
        ]

        record = StrategyDefinition(
            id=strategy_id,
            name=clean_name,
            description=str(description).strip() if description else None,
            symbols=clean_symbols[:1] or ["Unknown"],
            timeframe=str(timeframe).strip(),
            datasource=str(datasource).strip() if datasource else None,
            exchange=str(exchange).strip() if exchange else None,
            indicator_ids=list(dict.fromkeys(indicators)),
        )
        self._records[strategy_id] = record
        logger.info("strategy_created | id=%s name=%s", strategy_id, clean_name)
        return record.to_dict()

    def update(self, strategy_id: str, **fields: Any) -> Dict[str, Any]:
        """Update an existing strategy and return its payload."""

        record = self.get(strategy_id)
        record.update(**fields)
        logger.info("strategy_updated | id=%s", strategy_id)
        return record.to_dict()

    def delete(self, strategy_id: str) -> None:
        """Remove a strategy from the registry."""

        if strategy_id not in self._records:
            raise KeyError("Strategy not found")
        del self._records[strategy_id]
        logger.info("strategy_deleted | id=%s", strategy_id)

    def register_indicator(self, strategy_id: str, indicator_id: str) -> Dict[str, Any]:
        """Attach an indicator instance to a strategy."""

        record = self.get(strategy_id)
        inst_id = str(indicator_id).strip()
        if not inst_id:
            raise ValueError("Indicator id must be provided")

        get_instance_meta(inst_id)

        if inst_id not in record.indicator_ids:
            record.indicator_ids.append(inst_id)
            record.updated_at = _utcnow()

        logger.info(
            "strategy_indicator_registered | strategy=%s indicator=%s",
            strategy_id,
            inst_id,
        )
        return record.to_dict()

    def unregister_indicator(self, strategy_id: str, indicator_id: str) -> Dict[str, Any]:
        """Detach an indicator from a strategy."""

        record = self.get(strategy_id)
        inst_id = str(indicator_id).strip()
        if inst_id in record.indicator_ids:
            record.indicator_ids = [
                identifier
                for identifier in record.indicator_ids
                if identifier != inst_id
            ]
            record.updated_at = _utcnow()

        logger.info(
            "strategy_indicator_unregistered | strategy=%s indicator=%s",
            strategy_id,
            inst_id,
        )
        return record.to_dict()

    def add_rule(
        self,
        strategy_id: str,
        *,
        name: str,
        action: str,
        conditions: Iterable[Mapping[str, Any]],
        match: str = "all",
        description: Optional[str] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Create a rule for the strategy."""

        record = self.get(strategy_id)
        parsed_conditions = _parse_conditions(record, conditions)
        rule = StrategyRule(
            id=str(uuid.uuid4()),
            name=str(name).strip(),
            action=_normalise_action(action),
            conditions=parsed_conditions,
            match=_normalise_match_mode(match),
            description=str(description).strip() if description else None,
            enabled=bool(enabled),
        )
        record.add_rule(rule)

        logger.info(
            "strategy_rule_created | strategy=%s rule=%s",
            strategy_id,
            rule.id,
        )
        return record.to_dict()

    def update_rule(self, strategy_id: str, rule_id: str, **fields: Any) -> Dict[str, Any]:
        """Update a rule for a strategy."""

        record = self.get(strategy_id)
        rule = record.rules.get(rule_id)
        if rule is None:
            raise KeyError("Rule not found")

        if "name" in fields and fields["name"] is not None:
            rule.name = str(fields["name"]).strip()
        if "action" in fields and fields["action"] is not None:
            rule.action = _normalise_action(fields["action"])
        if "match" in fields and fields["match"] is not None:
            rule.match = _normalise_match_mode(fields["match"])
        if "conditions" in fields and fields["conditions"] is not None:
            rule.conditions = _parse_conditions(record, fields["conditions"])
        if "description" in fields:
            description = fields["description"]
            rule.description = str(description).strip() if description else None
        if "enabled" in fields and fields["enabled"] is not None:
            rule.enabled = bool(fields["enabled"])

        rule.updated_at = _utcnow()
        record.updated_at = _utcnow()

        logger.info(
            "strategy_rule_updated | strategy=%s rule=%s",
            strategy_id,
            rule_id,
        )
        return record.to_dict()

    def remove_rule(self, strategy_id: str, rule_id: str) -> Dict[str, Any]:
        """Remove a rule and return the updated strategy payload."""

        record = self.get(strategy_id)
        if rule_id not in record.rules:
            raise KeyError("Rule not found")
        record.remove_rule(rule_id)

        logger.info(
            "strategy_rule_deleted | strategy=%s rule=%s",
            strategy_id,
            rule_id,
        )
        return record.to_dict()

    def evaluate(
        self,
        strategy_id: str,
        *,
        start: str,
        end: str,
        interval: str,
        symbol: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Evaluate a strategy against current indicator signals."""

        record = self.get(strategy_id)

        effective_symbol = symbol or (record.symbols[0] if record.symbols else None)
        if not effective_symbol:
            raise ValueError("Strategy has no symbol defined")

        effective_datasource = datasource or record.datasource
        effective_exchange = exchange or record.exchange

        indicator_payloads: Dict[str, Dict[str, Any]] = {}
        for inst_id in record.indicator_ids:
            try:
                payload = generate_signals_for_instance(
                    inst_id,
                    start=start,
                    end=end,
                    interval=interval,
                    symbol=effective_symbol,
                    datasource=effective_datasource,
                    exchange=effective_exchange,
                    config=config or {},
                )
                indicator_payloads[inst_id] = payload
                signals_obj = payload.get("signals") if isinstance(payload, Mapping) else None
                signal_count = len(signals_obj) if isinstance(signals_obj, list) else 0
                error_hint = payload.get("error") if isinstance(payload, Mapping) else None
                logger.debug(
                    "strategy_indicator_payload | strategy=%s indicator=%s signals=%d error=%s",
                    strategy_id,
                    inst_id,
                    signal_count,
                    error_hint,
                )
            except Exception as exc:  # noqa: BLE001 - propagate failures as payload errors
                logger.warning(
                    "strategy_indicator_signal_failed | strategy=%s indicator=%s error=%s",
                    strategy_id,
                    inst_id,
                    exc,
                )
                indicator_payloads[inst_id] = {"error": str(exc)}

        rule_results = [rule.evaluate(indicator_payloads) for rule in record.rules.values()]
        for res in rule_results:
            conditions = res.get("conditions") or []
            matched_count = sum(1 for cond in conditions if cond.get("matched"))
            total_conditions = len(conditions)
            logger.debug(
                "strategy_rule_evaluated | strategy=%s rule=%s action=%s matched=%s matched_conditions=%d/%d reason=%s",
                strategy_id,
                res.get("rule_id"),
                res.get("action"),
                res.get("matched"),
                matched_count,
                total_conditions,
                res.get("reason"),
            )
            for cond in conditions:
                logger.debug(
                    "strategy_rule_condition | strategy=%s rule=%s indicator=%s signal_type=%s expected_direction=%s detected_direction=%s matched=%s reason=%s",
                    strategy_id,
                    res.get("rule_id"),
                    cond.get("indicator_id"),
                    cond.get("signal_type"),
                    cond.get("direction"),
                    cond.get("direction_detected"),
                    cond.get("matched"),
                    cond.get("reason"),
                )

        buy_signals = [res for res in rule_results if res["matched"] and res["action"] == "buy"]
        sell_signals = [res for res in rule_results if res["matched"] and res["action"] == "sell"]

        buy_markers = _build_markers_for_results(buy_signals, action="buy")
        sell_markers = _build_markers_for_results(sell_signals, action="sell")

        logger.info(
            "strategy_signals_generated | strategy=%s symbol=%s interval=%s start=%s end=%s buys=%d sells=%d",
            strategy_id,
            effective_symbol,
            interval,
            start,
            end,
            len(buy_signals),
            len(sell_signals),
        )

        if not buy_signals and not sell_signals:
            logger.info(
                "strategy_signals_none | strategy=%s symbol=%s interval=%s start=%s end=%s indicators=%d rules=%d",
                strategy_id,
                effective_symbol,
                interval,
                start,
                end,
                len(indicator_payloads),
                len(rule_results),
            )
            for res in rule_results:
                conditions = res.get("conditions") or []
                matched_count = sum(1 for cond in conditions if cond.get("matched"))
                total_conditions = len(conditions)
                logger.info(
                    "strategy_rule_trace | strategy=%s rule=%s action=%s matched=%s matched_conditions=%d/%d reason=%s",
                    strategy_id,
                    res.get("rule_id"),
                    res.get("action"),
                    res.get("matched"),
                    matched_count,
                    total_conditions,
                    res.get("reason"),
                )
                for cond in conditions:
                    logger.info(
                        "strategy_condition_trace | strategy=%s rule=%s indicator=%s signal_type=%s expected_direction=%s detected_direction=%s matched=%s reason=%s",
                        strategy_id,
                        res.get("rule_id"),
                        cond.get("indicator_id"),
                        cond.get("signal_type"),
                        cond.get("direction"),
                        cond.get("direction_detected"),
                        cond.get("matched"),
                        cond.get("reason"),
                    )

        return {
            "strategy_id": record.id,
            "strategy_name": record.name,
            "window": {
                "start": start,
                "end": end,
                "interval": interval,
                "symbol": effective_symbol,
                "datasource": effective_datasource,
                "exchange": effective_exchange,
            },
            "indicator_results": indicator_payloads,
            "rule_results": rule_results,
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "chart_markers": {
                "buy": buy_markers,
                "sell": sell_markers,
            },
            "applied_inputs": {
                "symbol": effective_symbol,
                "timeframe": record.timeframe,
                "datasource": effective_datasource,
                "exchange": effective_exchange,
            },
        }


_REGISTRY = StrategyRegistry()


def list_strategies() -> List[Dict[str, Any]]:
    """Return all registered strategies."""

    return _REGISTRY.list()


def get_strategy(strategy_id: str) -> Dict[str, Any]:
    """Return the serialised strategy record."""

    return _REGISTRY.get(strategy_id).to_dict()


def create_strategy(
    name: str,
    *,
    symbols: Iterable[str],
    timeframe: str,
    description: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    indicator_ids: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Create a new strategy using the global registry."""

    return _REGISTRY.create(
        name,
        symbols=symbols,
        timeframe=timeframe,
        description=description,
        datasource=datasource,
        exchange=exchange,
        indicator_ids=indicator_ids,
    )


def update_strategy(strategy_id: str, **fields: Any) -> Dict[str, Any]:
    """Update the specified strategy."""

    return _REGISTRY.update(strategy_id, **fields)


def delete_strategy(strategy_id: str) -> None:
    """Delete a strategy from the registry."""

    _REGISTRY.delete(strategy_id)


def register_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Attach an indicator instance to the strategy."""

    return _REGISTRY.register_indicator(strategy_id, indicator_id)


def unregister_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Detach an indicator instance from the strategy."""

    return _REGISTRY.unregister_indicator(strategy_id, indicator_id)


def create_rule(
    strategy_id: str,
    *,
    name: str,
    action: str,
    conditions: Iterable[Mapping[str, Any]],
    match: str = "all",
    description: Optional[str] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Create a strategy rule."""

    return _REGISTRY.add_rule(
        strategy_id,
        name=name,
        action=action,
        conditions=conditions,
        match=match,
        description=description,
        enabled=enabled,
    )


def update_rule(strategy_id: str, rule_id: str, **fields: Any) -> Dict[str, Any]:
    """Update an existing rule."""

    return _REGISTRY.update_rule(strategy_id, rule_id, **fields)


def delete_rule(strategy_id: str, rule_id: str) -> Dict[str, Any]:
    """Remove a rule from a strategy."""

    return _REGISTRY.remove_rule(strategy_id, rule_id)


def generate_strategy_signals(
    strategy_id: str,
    *,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluate the strategy rules for the requested window."""

    return _REGISTRY.evaluate(
        strategy_id,
        start=start,
        end=end,
        interval=interval,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        config=config,
    )


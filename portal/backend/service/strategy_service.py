"""In-memory strategy rule orchestration for the portal."""

from __future__ import annotations

import logging
from collections import Counter
from copy import deepcopy
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set

from . import instrument_service
from .indicator_service import generate_signals_for_instance, get_instance_meta
from .storage import (
    delete_strategy as storage_delete_strategy,
    delete_strategy_indicator as storage_delete_strategy_indicator,
    delete_strategy_rule as storage_delete_strategy_rule,
    delete_symbol_preset,
    list_symbol_presets,
    load_strategies as storage_load_strategies,
    upsert_strategy as storage_upsert_strategy,
    upsert_strategy_indicator as storage_upsert_strategy_indicator,
    upsert_strategy_rule as storage_upsert_strategy_rule,
    upsert_symbol_preset,
)


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a naive UTC timestamp for metadata fields."""

    return datetime.utcnow()


def _parse_timestamp(value: Any) -> datetime:
    """Parse ISO8601 strings into naive UTC datetimes."""

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
    return _utcnow()


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

        bias_hint = _normalise_direction(source.get("bias"))
        if bias_hint:
            return bias_hint

        bias_label = _normalise_direction(source.get("bias_label"))
        if bias_label:
            return bias_label

        trade_direction = _normalise_direction(source.get("trade_direction"))
        if trade_direction:
            return trade_direction

        pointer_direction = _normalise_direction(source.get("pointer_direction"))
        if pointer_direction:
            return pointer_direction

        active_side = _normalise_direction(source.get("active_side"))
        if active_side:
            return active_side

        side_hint = _normalise_direction(source.get("side"))
        if side_hint:
            return side_hint

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


def _promote_signal_metadata(signal: MutableMapping[str, Any]) -> None:
    """Lift useful metadata fields to the top level for easier inspection."""

    metadata = signal.get("metadata")
    if not isinstance(metadata, Mapping):
        return

    preferred_keys = (
        "rule_id",
        "pattern_id",
        "signal_id",
        "pattern",
        "id",
        "direction",
        "bias",
        "breakout_direction",
        "pointer_direction",
        "retest_role",
    )
    for key in preferred_keys:
        if signal.get(key) in (None, "", []):
            value = metadata.get(key)
            if value not in (None, "", []):
                signal[key] = value

    alias_keys = ("aliases", "rule_aliases", "pattern_aliases")
    alias_values: list[str] = []

    def _ingest(value: Any) -> None:
        if isinstance(value, str):
            normalised = value.strip()
            if normalised and normalised not in alias_values:
                alias_values.append(normalised)
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
            for item in value:
                _ingest(item)

    for key in alias_keys:
        _ingest(signal.get(key))
    for key in alias_keys:
        _ingest(metadata.get(key))

    if alias_values:
        signal["rule_aliases"] = alias_values


def _ensure_signal_direction(signal: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(signal, dict):
        return None

    _promote_signal_metadata(signal)

    direction = _infer_signal_direction(signal)
    if not direction:
        return None

    existing = _normalise_direction(signal.get("direction"))
    if existing is None:
        signal["direction"] = direction

    metadata = signal.get("metadata")
    if isinstance(metadata, MutableMapping):
        meta_direction = _normalise_direction(metadata.get("direction"))
        if meta_direction is None:
            metadata["direction"] = direction

    return direction


def _summarise_signal_population(signals: Iterable[Mapping[str, Any]]) -> Dict[str, Counter]:
    """Return aggregated counts for signal types, rules, and directions."""

    type_counter: Counter[str] = Counter()
    rule_counter: Counter[str] = Counter()
    direction_counter: Counter[str] = Counter()

    for candidate in signals:
        if not isinstance(candidate, Mapping):
            continue

        signal_type = str(candidate.get("type", "")).strip().lower()
        if signal_type:
            type_counter[signal_type] += 1

        for identifier in _collect_rule_identifiers(candidate):
            rule_counter[identifier] += 1

        direction = _infer_signal_direction(dict(candidate))
        if direction:
            direction_counter[direction] += 1

    return {
        "types": type_counter,
        "rules": rule_counter,
        "directions": direction_counter,
    }


def _format_counter(counter: Counter[str]) -> str:
    if not counter:
        return "-"
    parts = [f"{key}:{counter[key]}" for key in sorted(counter)]
    return ", ".join(parts)


def _collect_rule_identifiers(signal: Mapping[str, Any]) -> List[str]:
    """Return all rule identifier aliases embedded within *signal*."""

    identifiers: List[str] = []

    def _append(value: Any) -> None:
        if isinstance(value, str):
            normalised = value.strip().lower()
            if normalised and normalised not in identifiers:
                identifiers.append(normalised)
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
            for item in value:
                _append(item)

    if isinstance(signal, MutableMapping):
        _promote_signal_metadata(signal)

    sources: List[Mapping[str, Any]] = [signal]
    metadata = signal.get("metadata")
    if isinstance(metadata, Mapping):
        sources.append(metadata)

    keys = ("rule_id", "pattern_id", "signal_id", "pattern", "id")
    alias_keys = ("aliases", "rule_aliases", "pattern_aliases")

    for source in sources:
        for key in keys:
            _append(source.get(key))
        for alias_key in alias_keys:
            _append(source.get(alias_key))

    return identifiers


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


def _extract_signal_epoch(signal: Optional[Mapping[str, Any]]) -> Optional[int]:
    """Return the first usable epoch timestamp from a signal payload."""

    if not isinstance(signal, Mapping):
        return None

    candidates: List[Any] = []
    if "time" in signal:
        candidates.append(signal.get("time"))
    if "timestamp" in signal:
        candidates.append(signal.get("timestamp"))

    metadata = signal.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "time",
            "timestamp",
            "bar_time",
            "bar_timestamp",
            "candle_time",
            "event_time",
            "retest_time",
            "signal_time",
        ):
            if key in metadata:
                candidates.append(metadata.get(key))

    for value in candidates:
        epoch = _iso_to_epoch_seconds(value)
        if epoch is not None:
            return epoch

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
        "signals": [],
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

    total_signals = len(signals)
    observed_rules: set[str] = set()
    observed_directions: set[str] = set()
    type_candidates: List[Dict[str, Any]] = []
    rule_candidates: List[Dict[str, Any]] = []
    direction_candidates: List[Dict[str, Any]] = []
    matched_candidates: List[Dict[str, Any]] = []

    for candidate in signals:
        if not isinstance(candidate, dict):
            continue

        candidate_rules = _collect_rule_identifiers(candidate)
        if candidate_rules:
            observed_rules.update(candidate_rules)

        cand_direction = _infer_signal_direction(candidate)
        if cand_direction:
            observed_directions.add(cand_direction)

        cand_type = str(candidate.get("type", "")).lower()
        if desired_type and cand_type != desired_type:
            continue
        type_candidates.append(candidate)

        if desired_rule:
            if desired_rule not in candidate_rules:
                continue
        rule_candidates.append(candidate)

        if desired_direction:
            if cand_direction != desired_direction:
                continue
            direction_candidates.append(candidate)
        else:
            direction_candidates.append(candidate)

        matched_candidates.append(candidate)

    matched_candidates.sort(key=lambda entry: (_extract_signal_epoch(entry) or 0))

    info["observed_rules"] = sorted(observed_rules)
    info["observed_directions"] = sorted(observed_directions)
    info["stats"] = {
        "signals": total_signals,
        "type_matches": len(type_candidates),
        "rule_matches": len(rule_candidates),
        "direction_matches": len(direction_candidates) if desired_direction else len(rule_candidates),
        "final_matches": len(matched_candidates),
    }

    if matched_candidates:
        terminal_signal = matched_candidates[-1]
        info["matched"] = True
        info["signal"] = terminal_signal
        info["signals"] = matched_candidates
        info["direction_detected"] = _infer_signal_direction(terminal_signal)
        info["reason"] = None
        return info

    if not type_candidates:
        info["reason"] = "No matching signals (type mismatch)"
    elif desired_rule and not rule_candidates:
        info["reason"] = "No matching signals (rule mismatch)"
    elif desired_direction and not direction_candidates:
        info["reason"] = "No matching signals (direction mismatch)"
    else:
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
        seen_keys = set()
        signals = list(res.get("signals") or [])
        signals.sort(key=lambda entry: (_extract_signal_epoch(entry) or 0))
        for signal in signals:
            if not isinstance(signal, Mapping):
                continue
            epoch = _extract_signal_epoch(signal)
            price = _extract_signal_price(signal)
            if epoch is None or price is None:
                continue
            direction = _infer_signal_direction(signal) or ("long" if action == "buy" else "short")
            label = f"{rule_name} ({direction})" if direction else rule_name
            dedupe_key = (epoch, price, res.get("rule_id"), direction)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
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
                    "position": "belowBar" if action == "buy" else "aboveBar",
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

    def to_storage_payload(self, strategy_id: str) -> Dict[str, Any]:
        """Return a simplified payload for persistence."""

        return {
            "id": self.id,
            "strategy_id": strategy_id,
            "name": self.name,
            "action": self.action,
            "match": self.match,
            "description": self.description,
            "enabled": self.enabled,
            "conditions": [condition.to_dict() for condition in self.conditions],
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
                if result["matched"]:
                    signals = result.get("signals") or []
                    if signals:
                        trigger_signals.extend(signals)
                    elif result.get("signal"):
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
    indicator_snapshots: MutableMapping[str, Dict[str, Any]] = field(default_factory=dict)
    rules: MutableMapping[str, StrategyRule] = field(default_factory=dict)
    instrument_messages: List[Dict[str, str]] = field(default_factory=list)
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the strategy for API responses."""

        indicators: List[Dict[str, Any]] = []
        missing: List[str] = []
        for identifier in self.indicator_ids:
            snapshot = self.indicator_snapshots.get(identifier, {})
            active_meta: Optional[Dict[str, Any]] = None
            try:
                active_meta = get_instance_meta(identifier)
            except KeyError:
                active_meta = None
            payload = {
                "id": identifier,
                "status": "active" if active_meta else "missing",
                "meta": active_meta or snapshot or {"id": identifier},
                "snapshot": snapshot,
            }
            indicators.append(payload)
            if not active_meta:
                missing.append(identifier)
        instruments: List[Dict[str, Any]] = []
        instrument_messages = list(self.instrument_messages)

        def _message_exists(symbol: str) -> bool:
            symbol_key = (symbol or "").upper()
            for entry in instrument_messages:
                if (entry.get("symbol") or "").upper() == symbol_key:
                    return True
            return False

        for symbol in self.symbols:
            record: Optional[Dict[str, Any]] = None
            try:
                record = instrument_service.resolve_instrument(
                    self.datasource,
                    self.exchange,
                    symbol,
                )
            except Exception:
                record = None
            if record:
                instruments.append(record)
            else:
                instruments.append({"symbol": symbol})
                if not _message_exists(symbol):
                    instrument_messages.append(
                        {
                            "symbol": symbol,
                            "message": "No instrument metadata stored",
                        }
                    )

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_ids": list(self.indicator_ids),
            "indicators": indicators,
            "missing_indicators": missing,
            "instruments": instruments,
            "instrument_messages": instrument_messages,
            "rules": [rule.to_dict() for rule in self.rules.values()],
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }

    def to_storage_payload(self) -> Dict[str, Any]:
        """Return a minimal dict suitable for persistence."""

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_ids": list(self.indicator_ids),
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
                new_ids = list(dict.fromkeys(indicator_ids))
                removed = set(self.indicator_ids) - set(new_ids)
                for obsolete in removed:
                    self.indicator_snapshots.pop(obsolete, None)
                self.indicator_ids = new_ids
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
        self._bootstrap_from_storage()

    def _bootstrap_from_storage(self) -> None:
        """Load persisted strategies into the in-memory registry."""

        records = storage_load_strategies()
        for entry in records:
            strategy_id = str(entry.get("id") or "").strip()
            if not strategy_id:
                continue
            base = StrategyDefinition(
                id=strategy_id,
                name=str(entry.get("name") or strategy_id).strip(),
                description=entry.get("description"),
                symbols=list(entry.get("symbols") or []),
                timeframe=str(entry.get("timeframe") or "15m"),
                datasource=entry.get("datasource"),
                exchange=entry.get("exchange"),
                indicator_ids=list(entry.get("indicator_ids") or []),
            )
            base.created_at = _parse_timestamp(entry.get("created_at"))
            base.updated_at = _parse_timestamp(entry.get("updated_at"))

            for link in entry.get("indicator_links", []):
                indicator_id = str(link.get("indicator_id") or "").strip()
                if not indicator_id:
                    continue
                if indicator_id not in base.indicator_ids:
                    base.indicator_ids.append(indicator_id)
                snapshot = link.get("indicator_snapshot") or {}
                base.indicator_snapshots[indicator_id] = snapshot

            for rule_entry in entry.get("rules_raw", []):
                rule_id = str(rule_entry.get("id") or "").strip()
                if not rule_id:
                    continue
                conds = []
                for cond in rule_entry.get("conditions") or []:
                    try:
                        conds.append(
                            RuleCondition(
                                indicator_id=str(cond.get("indicator_id")),
                                signal_type=str(cond.get("signal_type")),
                                rule_id=str(cond.get("rule_id")) if cond.get("rule_id") else None,
                                direction=_normalise_direction(cond.get("direction")),
                            )
                        )
                    except Exception:
                        continue
                rule = StrategyRule(
                    id=rule_id,
                    name=str(rule_entry.get("name") or rule_id),
                    action=_normalise_action(rule_entry.get("action", "buy")),
                    conditions=conds,
                    match=_normalise_match_mode(rule_entry.get("match")),
                    description=rule_entry.get("description"),
                    enabled=bool(rule_entry.get("enabled", True)),
                    created_at=_parse_timestamp(rule_entry.get("created_at")),
                    updated_at=_parse_timestamp(rule_entry.get("updated_at")),
                )
                base.rules[rule_id] = rule

            self._records[strategy_id] = base

    def _sync_instruments(self, record: StrategyDefinition) -> None:
        """Auto-load instrument metadata for CCXT strategies."""

        record.instrument_messages = []
        datasource = (record.datasource or "").strip().upper()
        if datasource != "CCXT":
            return
        for symbol in record.symbols:
            _, error = instrument_service.auto_sync_instrument(
                record.datasource,
                record.exchange,
                symbol,
            )
            if error:
                record.instrument_messages.append({
                    "symbol": symbol,
                    "message": error,
                })

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
        for inst_id in record.indicator_ids:
            try:
                meta = deepcopy(get_instance_meta(inst_id))
            except KeyError:
                meta = {}
            record.indicator_snapshots[inst_id] = meta
        self._sync_instruments(record)
        self._records[strategy_id] = record
        storage_upsert_strategy(record.to_storage_payload())
        for inst_id in record.indicator_ids:
            storage_upsert_strategy_indicator(
                strategy_id=strategy_id,
                indicator_id=inst_id,
                snapshot=record.indicator_snapshots.get(inst_id, {}),
            )
        logger.info("strategy_created | id=%s name=%s", strategy_id, clean_name)
        return record.to_dict()

    def update(self, strategy_id: str, **fields: Any) -> Dict[str, Any]:
        """Update an existing strategy and return its payload."""

        record = self.get(strategy_id)
        record.update(**fields)
        self._sync_instruments(record)
        storage_upsert_strategy(record.to_storage_payload())
        logger.info("strategy_updated | id=%s", strategy_id)
        return record.to_dict()

    def delete(self, strategy_id: str) -> None:
        """Remove a strategy from the registry."""

        if strategy_id not in self._records:
            raise KeyError("Strategy not found")
        del self._records[strategy_id]
        storage_delete_strategy(strategy_id)
        logger.info("strategy_deleted | id=%s", strategy_id)

    def register_indicator(self, strategy_id: str, indicator_id: str) -> Dict[str, Any]:
        """Attach an indicator instance to a strategy."""

        record = self.get(strategy_id)
        inst_id = str(indicator_id).strip()
        if not inst_id:
            raise ValueError("Indicator id must be provided")

        meta = deepcopy(get_instance_meta(inst_id))

        if inst_id not in record.indicator_ids:
            record.indicator_ids.append(inst_id)
            record.updated_at = _utcnow()
        record.indicator_snapshots[inst_id] = meta
        storage_upsert_strategy(record.to_storage_payload())
        storage_upsert_strategy_indicator(
            strategy_id=strategy_id,
            indicator_id=inst_id,
            snapshot=meta,
        )

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
            record.indicator_snapshots.pop(inst_id, None)
            storage_upsert_strategy(record.to_storage_payload())
            storage_delete_strategy_indicator(strategy_id, inst_id)

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
        rule_id = str(uuid.uuid4())
        rule = StrategyRule(
            id=rule_id,
            name=str(name).strip(),
            action=_normalise_action(action),
            conditions=parsed_conditions,
            match=_normalise_match_mode(match),
            description=str(description).strip() if description else None,
            enabled=bool(enabled),
        )
        record.add_rule(rule)
        storage_upsert_strategy_rule(rule.to_storage_payload(strategy_id))
        storage_upsert_strategy(record.to_storage_payload())

        logger.info(
            "strategy_rule_created | strategy=%s rule=%s",
            strategy_id,
            rule_id,
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
        storage_upsert_strategy_rule(rule.to_storage_payload(strategy_id))
        storage_upsert_strategy(record.to_storage_payload())

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
        storage_delete_strategy_rule(rule_id)
        storage_upsert_strategy(record.to_storage_payload())

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

        indicator_rule_map: Dict[str, List[str]] = {}
        for rule in record.rules.values():
            for condition in rule.conditions:
                indicator_id = condition.indicator_id
                rule_id = condition.rule_id
                if not indicator_id or not rule_id:
                    continue
                bucket = indicator_rule_map.setdefault(indicator_id, [])
                if rule_id not in bucket:
                    bucket.append(rule_id)

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

        indicator_payloads: Dict[str, Dict[str, Any]] = {}
        missing_indicators: List[str] = []
        base_config = dict(config or {})
        for inst_id in record.indicator_ids:
            try:
                per_config = dict(base_config)
                rule_filters = indicator_rule_map.get(inst_id)
                if rule_filters:
                    merged_rules = _merge_enabled_rules(per_config.get("enabled_rules"), rule_filters)
                    if merged_rules:
                        per_config["enabled_rules"] = merged_rules
                    else:
                        per_config.pop("enabled_rules", None)
                payload = generate_signals_for_instance(
                    inst_id,
                    start=start,
                    end=end,
                    interval=interval,
                    symbol=effective_symbol,
                    datasource=effective_datasource,
                    exchange=effective_exchange,
                    config=per_config,
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
                if isinstance(signals_obj, list):
                    for signal in signals_obj:
                        if isinstance(signal, dict):
                            _ensure_signal_direction(signal)
                    summary = _summarise_signal_population(signals_obj)
                    logger.debug(
                        "strategy_indicator_signal_summary | strategy=%s indicator=%s total=%d types=[%s] rules=[%s] directions=[%s]",
                        strategy_id,
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
                    "strategy_indicator_missing | strategy=%s indicator=%s",
                    strategy_id,
                    inst_id,
                )
                continue
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
                    "strategy_rule_condition | strategy=%s rule=%s indicator=%s signal_type=%s expected_direction=%s detected_direction=%s matched=%s reason=%s stats=%s observed_rules=%s observed_directions=%s",
                    strategy_id,
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
                "strategy_signals_none | strategy=%s symbol=%s interval=%s start=%s end=%s indicators=%d rules=%d stats=%s observed_rules=%s observed_directions=%s",
                strategy_id,
                effective_symbol,
                interval,
                start,
                end,
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
                        "strategy_condition_trace | strategy=%s rule=%s indicator=%s signal_type=%s expected_direction=%s detected_direction=%s matched=%s reason=%s stats=%s observed_rules=%s observed_directions=%s",
                        strategy_id,
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
            "missing_indicators": missing_indicators,
            "status": status,
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


def list_symbol_presets_service() -> List[Dict[str, Any]]:
    """Return saved datasource/exchange/timeframe/symbol presets."""

    return list_symbol_presets()


def save_symbol_preset_service(
    *,
    label: str,
    datasource: Optional[str],
    exchange: Optional[str],
    timeframe: str,
    symbol: str,
    preset_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a symbol preset and return the stored payload."""

    payload = {
        "id": preset_id,
        "label": label,
        "datasource": datasource,
        "exchange": exchange,
        "timeframe": timeframe,
        "symbol": symbol,
    }
    result = upsert_symbol_preset(payload)
    if result is None:
        raise RuntimeError("Failed to persist symbol preset")
    return result


def delete_symbol_preset_service(preset_id: str) -> None:
    """Delete a stored preset."""

    delete_symbol_preset(preset_id)


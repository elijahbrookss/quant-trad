"""In-memory strategy management and signal aggregation helpers."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Tuple

from .indicator_service import (
    generate_signals_for_instance,
    get_instance_meta,
)


logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a timezone-naive UTC timestamp for metadata stamps."""

    return datetime.utcnow()


@dataclass
class StrategyRule:
    """Represents a single strategy rule bound to an indicator output."""

    id: str
    name: str
    signal_type: str
    action: str
    indicator_id: Optional[str] = None
    min_confidence: float = 0.0
    description: Optional[str] = None
    enabled: bool = True
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the rule to a JSON-serialisable dictionary."""

        return {
            "id": self.id,
            "name": self.name,
            "indicator_id": self.indicator_id,
            "signal_type": self.signal_type,
            "min_confidence": self.min_confidence,
            "action": self.action,
            "description": self.description,
            "enabled": self.enabled,
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }


@dataclass
class StrategyRecord:
    """Stores mutable state for a single user-defined strategy."""

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
        """Serialise the strategy to a JSON payload for API responses."""

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
        """Apply partial updates to mutable strategy attributes."""

        if "name" in fields and fields["name"] is not None:
            self.name = str(fields["name"]).strip()
        if "description" in fields:
            desc = fields["description"]
            self.description = str(desc).strip() if desc is not None else None
        if "symbols" in fields and fields["symbols"] is not None:
            symbols = [str(sym).strip() for sym in fields["symbols"] if str(sym).strip()]
            if symbols:
                self.symbols = symbols
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
                str(inst).strip()
                for inst in fields["indicator_ids"]
                if str(inst).strip()
            ]
            if indicator_ids:
                self.indicator_ids = list(dict.fromkeys(indicator_ids))
        self.updated_at = _utcnow()

    def add_rule(self, rule: StrategyRule) -> None:
        """Attach a rule to the strategy in insertion order."""

        self.rules[rule.id] = rule
        self.updated_at = _utcnow()

    def remove_rule(self, rule_id: str) -> None:
        """Remove a rule and bump the update timestamp."""

        if rule_id in self.rules:
            del self.rules[rule_id]
            self.updated_at = _utcnow()


_STRATEGIES: Dict[str, StrategyRecord] = {}


def list_strategies() -> List[Dict[str, Any]]:
    """Return all registered strategies as serialised dictionaries."""

    return [record.to_dict() for record in _STRATEGIES.values()]


def get_strategy(strategy_id: str) -> Dict[str, Any]:
    """Return the serialised strategy for *strategy_id*."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    return record.to_dict()


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
    """Register a new strategy and return its serialised record."""

    strategy_id = str(uuid.uuid4())
    clean_name = str(name).strip()
    clean_symbols = [str(sym).strip() for sym in symbols if str(sym).strip()]
    indicators = [
        str(inst).strip()
        for inst in (indicator_ids or [])
        if str(inst).strip()
    ]
    record = StrategyRecord(
        id=strategy_id,
        name=clean_name,
        description=str(description).strip() if description else None,
        symbols=clean_symbols or ["Unknown"],
        timeframe=str(timeframe).strip(),
        datasource=str(datasource).strip() if datasource else None,
        exchange=str(exchange).strip() if exchange else None,
        indicator_ids=list(dict.fromkeys(indicators)),
    )
    _STRATEGIES[strategy_id] = record
    logger.info("strategy_created | id=%s name=%s", strategy_id, clean_name)
    return record.to_dict()


def update_strategy(strategy_id: str, **fields: Any) -> Dict[str, Any]:
    """Update mutable fields on the specified strategy."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    record.update(**fields)
    logger.info("strategy_updated | id=%s", strategy_id)
    return record.to_dict()


def delete_strategy(strategy_id: str) -> None:
    """Remove a strategy from the in-memory registry."""

    if strategy_id not in _STRATEGIES:
        raise KeyError("Strategy not found")
    del _STRATEGIES[strategy_id]
    logger.info("strategy_deleted | id=%s", strategy_id)


def register_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Attach an indicator instance to the strategy."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    inst_id = str(indicator_id).strip()
    if not inst_id:
        raise ValueError("Indicator id must be provided")
    # Validate indicator exists
    get_instance_meta(inst_id)
    if inst_id not in record.indicator_ids:
        record.indicator_ids.append(inst_id)
        record.updated_at = _utcnow()
    logger.info("strategy_indicator_registered | strategy=%s indicator=%s", strategy_id, inst_id)
    return record.to_dict()


def unregister_indicator(strategy_id: str, indicator_id: str) -> Dict[str, Any]:
    """Detach an indicator from the strategy."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    inst_id = str(indicator_id).strip()
    if inst_id in record.indicator_ids:
        record.indicator_ids = [iid for iid in record.indicator_ids if iid != inst_id]
        record.updated_at = _utcnow()
    logger.info("strategy_indicator_unregistered | strategy=%s indicator=%s", strategy_id, inst_id)
    return record.to_dict()


def create_rule(
    strategy_id: str,
    *,
    name: str,
    signal_type: str,
    action: str,
    indicator_id: Optional[str] = None,
    min_confidence: float = 0.0,
    description: Optional[str] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Create a rule for a strategy and return the updated strategy."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    rule_id = str(uuid.uuid4())
    indicator = str(indicator_id).strip() if indicator_id else None
    if indicator:
        # Validate indicator exists before attaching
        get_instance_meta(indicator)
    action_value = str(action).strip().lower()
    if action_value not in {"buy", "sell"}:
        raise ValueError("Action must be 'buy' or 'sell'")
    rule = StrategyRule(
        id=rule_id,
        name=str(name).strip(),
        signal_type=str(signal_type).strip(),
        action=action_value,
        indicator_id=indicator,
        min_confidence=float(min_confidence or 0.0),
        description=str(description).strip() if description else None,
        enabled=bool(enabled),
    )
    record.add_rule(rule)
    logger.info("strategy_rule_created | strategy=%s rule=%s", strategy_id, rule_id)
    return record.to_dict()


def update_rule(strategy_id: str, rule_id: str, **fields: Any) -> Dict[str, Any]:
    """Update an existing rule for a strategy."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    rule = record.rules.get(rule_id)
    if rule is None:
        raise KeyError("Rule not found")
    if "name" in fields and fields["name"] is not None:
        rule.name = str(fields["name"]).strip()
    if "signal_type" in fields and fields["signal_type"] is not None:
        rule.signal_type = str(fields["signal_type"]).strip()
    if "action" in fields and fields["action"] is not None:
        action_value = str(fields["action"]).strip().lower()
        if action_value not in {"buy", "sell"}:
            raise ValueError("Action must be 'buy' or 'sell'")
        rule.action = action_value
    if "indicator_id" in fields:
        indicator_id = fields["indicator_id"]
        indicator = str(indicator_id).strip() if indicator_id else None
        if indicator:
            get_instance_meta(indicator)
        rule.indicator_id = indicator
    if "min_confidence" in fields and fields["min_confidence"] is not None:
        rule.min_confidence = float(fields["min_confidence"])
    if "description" in fields:
        description = fields["description"]
        rule.description = str(description).strip() if description else None
    if "enabled" in fields and fields["enabled"] is not None:
        rule.enabled = bool(fields["enabled"])
    rule.updated_at = _utcnow()
    record.updated_at = _utcnow()
    logger.info("strategy_rule_updated | strategy=%s rule=%s", strategy_id, rule_id)
    return record.to_dict()


def delete_rule(strategy_id: str, rule_id: str) -> Dict[str, Any]:
    """Delete a rule from a strategy."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")
    if rule_id not in record.rules:
        raise KeyError("Rule not found")
    record.remove_rule(rule_id)
    logger.info("strategy_rule_deleted | strategy=%s rule=%s", strategy_id, rule_id)
    return record.to_dict()


def _evaluate_rule(
    rule: StrategyRule,
    indicator_payloads: Dict[str, Dict[str, Any]],
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """Return whether the rule matched against collected indicator signals."""

    if not rule.enabled:
        return False, None, "Rule disabled"
    if not rule.indicator_id:
        return False, None, "No indicator attached"
    payload = indicator_payloads.get(rule.indicator_id)
    if payload is None:
        return False, None, "No signals for indicator"
    error = payload.get("error") if isinstance(payload, dict) else None
    if error:
        return False, None, str(error)
    signals = payload.get("signals") if isinstance(payload, dict) else None
    if not isinstance(signals, list):
        return False, None, "Indicator returned no signals"
    target_type = rule.signal_type.lower()
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        sig_type = str(signal.get("type", "")).lower()
        if sig_type != target_type:
            continue
        try:
            confidence = float(signal.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence >= rule.min_confidence:
            return True, signal, None
    return False, None, "No matching signals"


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
    """Compute strategy-level buy/sell signals based on rule matches."""

    record = _STRATEGIES.get(strategy_id)
    if record is None:
        raise KeyError("Strategy not found")

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
        except Exception as exc:  # noqa: BLE001 - bubble unexpected errors as payload
            logger.warning(
                "strategy_indicator_signal_failed | strategy=%s indicator=%s error=%s",
                strategy_id,
                inst_id,
                exc,
            )
            indicator_payloads[inst_id] = {"error": str(exc)}

    rule_results: List[Dict[str, Any]] = []
    for rule in record.rules.values():
        matched, signal, failure = _evaluate_rule(rule, indicator_payloads)
        entry = {
            "rule_id": rule.id,
            "rule_name": rule.name,
            "indicator_id": rule.indicator_id,
            "action": rule.action,
            "matched": matched,
            "signal": signal,
            "reason": failure,
        }
        rule_results.append(entry)

    buy_signals = [res for res in rule_results if res["matched"] and res["action"] == "buy"]
    sell_signals = [res for res in rule_results if res["matched"] and res["action"] == "sell"]

    logger.info(
        "strategy_signals_generated | strategy=%s buys=%d sells=%d",
        strategy_id,
        len(buy_signals),
        len(sell_signals),
    )

    return {
        "strategy_id": record.id,
        "strategy_name": record.name,
        "window": {
            "start": start,
            "end": end,
            "interval": interval,
            "symbol": effective_symbol,
        },
        "indicator_results": indicator_payloads,
        "rule_results": rule_results,
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
    }


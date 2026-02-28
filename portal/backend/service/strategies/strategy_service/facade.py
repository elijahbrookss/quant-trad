"""In-memory strategy rule orchestration for the portal."""

from __future__ import annotations

import logging
from copy import deepcopy
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from ...market import instrument_service
from ...risk.atm import normalise_template
from ...indicators.indicator_service import get_instance_meta
from engines.bot_runtime.core.execution_profile import compile_runtime_profile_or_error
from strategies import evaluator
from . import persistence
from .filters import (
    FilterDefinition,
    validate_filter_dsl,
)
from .evaluation_orchestrator import StrategyEvaluationDependencies, StrategyEvaluationOrchestrator


logger = logging.getLogger(__name__)
_RUNTIME_ALLOWED_DERIVATIVE_TYPES = {"future", "futures", "perp", "perps"}


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
_normalise_direction = evaluator._normalise_direction
_infer_signal_direction = evaluator._infer_signal_direction
_promote_signal_metadata = evaluator._promote_signal_metadata
_ensure_signal_direction = evaluator._ensure_signal_direction
_summarise_signal_population = evaluator._summarise_signal_population
_format_counter = evaluator._format_counter
_collect_rule_identifiers = evaluator._collect_rule_identifiers
_normalise_match_mode = evaluator._normalise_match_mode
_normalise_action = evaluator._normalise_action
_evaluate_condition = evaluator._evaluate_condition

storage_load_strategies = persistence.load_strategies
storage_upsert_strategy = persistence.upsert_strategy
storage_delete_strategy = persistence.delete_strategy
storage_upsert_strategy_indicator = persistence.upsert_strategy_indicator
storage_delete_strategy_indicator = persistence.delete_strategy_indicator
storage_upsert_strategy_rule = persistence.upsert_strategy_rule
storage_delete_strategy_rule = persistence.delete_strategy_rule
storage_list_strategy_filters = persistence.list_strategy_filters
storage_list_rule_filters = persistence.list_rule_filters
storage_upsert_strategy_filter = persistence.upsert_strategy_filter
storage_upsert_rule_filter = persistence.upsert_rule_filter
storage_delete_strategy_filter = persistence.delete_strategy_filter
storage_delete_rule_filter = persistence.delete_rule_filter
list_symbol_presets = persistence.list_symbol_presets
upsert_symbol_preset = persistence.upsert_symbol_preset
delete_symbol_preset = persistence.delete_symbol_preset
load_atm_templates = persistence.list_atm_templates
get_atm_template = persistence.get_atm_template
upsert_atm_template = persistence.upsert_atm_template
storage_upsert_strategy_instrument = persistence.upsert_strategy_instrument
storage_delete_strategy_instrument = persistence.delete_strategy_instrument
storage_list_strategy_instrument_symbols = persistence.list_strategy_instrument_symbols


def _risk_fields_from_template(template: Optional[Mapping[str, Any]]) -> Dict[str, Optional[float]]:
    """Extract risk settings from a template payload."""

    if not isinstance(template, Mapping):
        return {
            "base_risk_per_trade": None,
            "global_risk_multiplier": None,
        }

    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    # Read from nested risk object or flat fields
    risk_config = template.get("risk") if isinstance(template.get("risk"), dict) else {}

    return {
        "base_risk_per_trade": _safe_float(
            risk_config.get("base_risk_per_trade") or template.get("base_risk_per_trade")
        ),
        "global_risk_multiplier": _safe_float(
            risk_config.get("global_risk_multiplier") or template.get("global_risk_multiplier")
        ),
    }


@dataclass
class InstrumentSlot:
    """Represents a symbol attached to a strategy along with runtime hints."""

    symbol: str
    enabled: bool = True
    risk_multiplier: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable representation of the slot."""

        payload: Dict[str, Any] = {
            "symbol": self.symbol,
            "enabled": bool(self.enabled),
        }
        if self.risk_multiplier is not None:
            payload["risk_multiplier"] = float(self.risk_multiplier)
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload

    @staticmethod
    def from_any(value: Any) -> "InstrumentSlot":
        """Normalise raw payloads into :class:`InstrumentSlot` instances."""

        if isinstance(value, InstrumentSlot):
            return value
        if isinstance(value, Mapping):
            return InstrumentSlot(
                symbol=str(value.get("symbol") or "").strip(),
                enabled=bool(value.get("enabled", True)),
                risk_multiplier=float(value["risk_multiplier"]) if value.get("risk_multiplier") is not None else None,
                metadata=dict(value.get("metadata") or {}),
            )
        return InstrumentSlot(symbol=str(value or "").strip())


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
    filters: List[FilterDefinition] = field(default_factory=list)
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
            "filters": [flt.to_dict() for flt in self.filters],
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
    instruments: List[InstrumentSlot]
    timeframe: str
    description: Optional[str] = None
    datasource: Optional[str] = None
    exchange: Optional[str] = None
    indicator_ids: List[str] = field(default_factory=list)
    # REMOVED: indicator_snapshots - strategies now load indicators fresh from DB
    rules: MutableMapping[str, StrategyRule] = field(default_factory=dict)
    global_filters: List[FilterDefinition] = field(default_factory=list)
    instrument_messages: List[Dict[str, str]] = field(default_factory=list)
    atm_template_id: Optional[str] = None
    base_risk_per_trade: Optional[float] = None
    global_risk_multiplier: Optional[float] = None
    risk_overrides: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    @property
    def symbols(self) -> List[str]:
        """Return ordered list of attached symbol identifiers."""

        return [slot.symbol for slot in self.instruments]

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the strategy for API responses."""

        indicators: List[Dict[str, Any]] = []
        missing: List[str] = []
        for identifier in self.indicator_ids:
            # Load fresh indicator metadata from DB (no snapshots)
            active_meta: Optional[Dict[str, Any]] = None
            try:
                active_meta = get_instance_meta(identifier)
                logger.debug(
                    "Strategy indicator meta | indicator_id=%s | has_signal_rules=%s | count=%d",
                    identifier,
                    "signal_rules" in (active_meta or {}),
                    len(active_meta.get("signal_rules", [])) if active_meta else 0
                )
            except KeyError:
                logger.warning("⚠ Indicator %s not found for strategy", identifier)
                active_meta = None
            payload = {
                "id": identifier,
                "status": "active" if active_meta else "missing",
                "meta": active_meta or {"id": identifier},
                # REMOVED: snapshot field - no longer storing snapshots
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

        for slot in self.instruments:
            symbol = slot.symbol
            record: Optional[Dict[str, Any]] = None
            # Prefer an explicit instrument id stored on the slot metadata.
            inst_id = None
            if isinstance(slot.metadata, dict):
                inst_id = slot.metadata.get("instrument_id")
            if inst_id:
                try:
                    record = instrument_service.get_instrument_record(str(inst_id))
                except Exception:
                    record = None

            # Fallback to resolving by provider identifiers and symbol
            if record is None:
                try:
                    record = instrument_service.resolve_instrument(
                        self.datasource,
                        self.exchange,
                        symbol,
                    )
                except Exception:
                    record = None

            if record:
                instruments.append({**slot.to_dict(), **record})
            else:
                instruments.append(slot.to_dict())
                if not _message_exists(symbol):
                    instrument_messages.append(
                        {
                            "symbol": symbol,
                            "message": "No instrument metadata stored",
                        }
                    )

        # Derive a simple `symbols` array for frontend consumption. Prefer DB-derived
        # authoritative symbols from the persisted instrument rows (via strategy links).
        symbols_list: List[str] = []
        try:
            symbols_list = storage_list_strategy_instrument_symbols(self.id)
        except Exception:
            symbols_list = []
        # Fallback to in-memory slot symbols if DB-derived list is empty
        if not symbols_list:
            symbols_list = [slot.symbol for slot in self.instruments if slot.symbol]

        # Fetch ATM template from storage if template_id is set
        atm_template = None
        if self.atm_template_id:
            stored_template = get_atm_template(self.atm_template_id)
            if stored_template:
                atm_template = normalise_template(stored_template.get("template"))

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "symbols": self.symbols,
            "instrument_slots": [slot.to_dict() for slot in self.instruments],
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_ids": list(self.indicator_ids),
            "indicators": indicators,
            "missing_indicators": missing,
            "instruments": instruments,
            "instrument_messages": instrument_messages,
            "rules": [rule.to_dict() for rule in self.rules.values()],
            "global_filters": [flt.to_dict() for flt in self.global_filters],
            "atm_template": atm_template or {},
            "atm_template_id": self.atm_template_id,
            "base_risk_per_trade": self.base_risk_per_trade,
            "global_risk_multiplier": self.global_risk_multiplier,
            "risk_overrides": dict(self.risk_overrides or {}),
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }

    def to_storage_payload(self) -> Dict[str, Any]:
        """Return a minimal dict suitable for persistence."""

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            # Legacy `symbols` column removed from storage; instrument links are persisted separately.
            "timeframe": self.timeframe,
            "datasource": self.datasource,
            "exchange": self.exchange,
            "indicator_ids": list(self.indicator_ids),
            "atm_template_id": self.atm_template_id,
            "base_risk_per_trade": self.base_risk_per_trade,
            "global_risk_multiplier": self.global_risk_multiplier,
            "risk_overrides": dict(self.risk_overrides or {}),
        }

    def update(self, **fields: Any) -> None:
        """Apply partial updates to the strategy metadata."""

        if "name" in fields and fields["name"] is not None:
            self.name = str(fields["name"]).strip()
        if "description" in fields:
            description = fields["description"]
            self.description = str(description).strip() if description else None
        # legacy `symbols` field removed; prefer `instrument_slots`
        if "instrument_slots" in fields and fields["instrument_slots"] is not None:
            slots: List[InstrumentSlot] = []
            for raw_slot in fields["instrument_slots"]:
                slot = InstrumentSlot.from_any(raw_slot)
                if slot.symbol:
                    slots.append(slot)
            if slots:
                self.instruments = slots
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
                # REMOVED: indicator_snapshots cleanup - no longer storing snapshots
                self.indicator_ids = new_ids
        if "atm_template_id" in fields:
            self.atm_template_id = fields.get("atm_template_id") or None
        if "base_risk_per_trade" in fields:
            value = fields.get("base_risk_per_trade")
            self.base_risk_per_trade = float(value) if value is not None else None
        if "global_risk_multiplier" in fields:
            value = fields.get("global_risk_multiplier")
            self.global_risk_multiplier = float(value) if value is not None else None
        if "risk_overrides" in fields and fields["risk_overrides"] is not None:
            self.risk_overrides = dict(fields.get("risk_overrides") or {})
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
        # NOTE: In-memory registry cache (per-process). Requires reload to sync external updates.
        # NOTE: No locks; concurrent mutations may race.
        self._records: Dict[str, StrategyDefinition] = {}
        self._bootstrap_from_storage()

    def _bootstrap_from_storage(self) -> None:
        """Load persisted strategies into the in-memory registry."""

        records = storage_load_strategies()
        for entry in records:
            strategy_id = str(entry.get("id") or "").strip()
            if not strategy_id:
                continue
            # Prefer instrument links when available (normalised many-to-many).
            raw_inst_links = entry.get("instrument_links") or []
            slots = []
            if raw_inst_links:
                for link in raw_inst_links:
                    inst_id = str(link.get("instrument_id") or "").strip()
                    snapshot = link.get("instrument_snapshot") or {}
                    symbol = snapshot.get("symbol") or None
                    if not symbol and inst_id:
                        try:
                            rec = instrument_service.get_instrument_record(inst_id)
                            symbol = rec.get("symbol")
                        except Exception:
                            symbol = None
                    if not symbol:
                        continue
                    slot = InstrumentSlot(symbol=symbol)
                    # Preserve instrument identity in slot metadata for future ops
                    slot.metadata = {**(slot.metadata or {}), "instrument_id": inst_id, **(snapshot or {})}
                    slots.append(slot)
            else:
                raw_symbols = entry.get("symbols") or []
                slots = [InstrumentSlot.from_any(symbol) for symbol in raw_symbols]
            base = StrategyDefinition(
                id=strategy_id,
                name=str(entry.get("name") or strategy_id).strip(),
                description=entry.get("description"),
                instruments=[slot for slot in slots if slot.symbol] or [InstrumentSlot(symbol="Unknown")],
                timeframe=str(entry.get("timeframe") or "15m"),
                datasource=entry.get("datasource"),
                exchange=entry.get("exchange"),
                indicator_ids=[],
            )
            base.created_at = _parse_timestamp(entry.get("created_at"))
            base.updated_at = _parse_timestamp(entry.get("updated_at"))
            base.atm_template_id = entry.get("atm_template_id")
            base.base_risk_per_trade = entry.get("base_risk_per_trade")
            base.global_risk_multiplier = entry.get("global_risk_multiplier")
            base.risk_overrides = entry.get("risk_overrides") or {}

            for link in entry.get("indicator_links", []):
                indicator_id = str(link.get("indicator_id") or "").strip()
                if not indicator_id:
                    continue
                if indicator_id not in base.indicator_ids:
                    base.indicator_ids.append(indicator_id)
                # REMOVED: indicator_snapshots assignment - no longer storing snapshots

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
                    except Exception as exc:
                        logger.warning(
                            "strategy_rule_condition_skipped | strategy_id=%s rule_id=%s condition=%s error=%s",
                            entry.get("id"),
                            rule_id,
                            cond,
                            exc,
                        )
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

            for filter_entry in entry.get("strategy_filters_raw", []):
                filter_id = str(filter_entry.get("id") or "").strip()
                if not filter_id:
                    continue
                dsl_payload = filter_entry.get("dsl") or {}
                base.global_filters.append(
                    FilterDefinition(
                        id=filter_id,
                        scope=str(filter_entry.get("scope") or "GLOBAL").upper(),
                        name=str(filter_entry.get("name") or filter_id),
                        description=filter_entry.get("description"),
                        dsl=dict(dsl_payload),
                        enabled=bool(filter_entry.get("enabled", True)),
                        created_at=_parse_timestamp(filter_entry.get("created_at")),
                        updated_at=_parse_timestamp(filter_entry.get("updated_at")),
                    )
                )

            rule_filters_by_rule: Dict[str, List[FilterDefinition]] = {}
            for filter_entry in entry.get("rule_filters_raw", []):
                filter_id = str(filter_entry.get("id") or "").strip()
                rule_id = str(filter_entry.get("rule_id") or "").strip()
                if not filter_id or not rule_id:
                    continue
                dsl_payload = filter_entry.get("dsl") or {}
                rule_filters_by_rule.setdefault(rule_id, []).append(
                    FilterDefinition(
                        id=filter_id,
                        scope=str(filter_entry.get("scope") or "RULE").upper(),
                        name=str(filter_entry.get("name") or filter_id),
                        description=filter_entry.get("description"),
                        dsl=dict(dsl_payload),
                        enabled=bool(filter_entry.get("enabled", True)),
                        created_at=_parse_timestamp(filter_entry.get("created_at")),
                        updated_at=_parse_timestamp(filter_entry.get("updated_at")),
                    )
                )

            for rule_id, filters in rule_filters_by_rule.items():
                rule = base.rules.get(rule_id)
                if rule:
                    rule.filters = filters

            self._records[strategy_id] = base

    def _sync_instruments(self, record: StrategyDefinition) -> None:
        """Ensure instrument metadata exists for each slot regardless of provider."""

        record.instrument_messages = []
        for slot in record.instruments:
            instrument_rec, error = instrument_service.validate_instrument(
                record.datasource,
                record.exchange,
                slot.symbol,
            )
            if instrument_rec:
                inst_id = str(instrument_rec.get("id") or "").strip()
                if inst_id:
                    slot.metadata = {
                        **(slot.metadata or {}),
                        "instrument_id": inst_id,
                        **instrument_rec,
                    }
                symbol = slot.symbol
                try:
                    compile_runtime_profile_or_error(
                        instrument_rec,
                        allowed_derivative_types=_RUNTIME_ALLOWED_DERIVATIVE_TYPES,
                    )
                except ValueError as exc:
                    record.instrument_messages.append(
                        {
                            "symbol": symbol,
                            "message": str(exc),
                        }
                    )
            if error:
                record.instrument_messages.append(
                    {
                        "symbol": slot.symbol,
                        "message": error,
                    }
                )

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
        atm_template: Optional[Mapping[str, Any]] = None,
        atm_template_id: Optional[str] = None,
        base_risk_per_trade: Optional[float] = None,
        global_risk_multiplier: Optional[float] = None,
        risk_overrides: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a new strategy record and return its payload."""

        strategy_id = str(uuid.uuid4())
        clean_name = str(name).strip()
        clean_slots = [
            InstrumentSlot.from_any(symbol)
            for symbol in symbols
            if str(symbol or "").strip()
        ]
        clean_slots = [slot for slot in clean_slots if slot.symbol]
        indicators = [
            str(identifier).strip()
            for identifier in (indicator_ids or [])
            if str(identifier).strip()
        ]

        template_payload = atm_template
        if template_payload is None and atm_template_id:
            stored_template = get_atm_template(atm_template_id)
            if not stored_template:
                raise ValueError("ATM template not found.")
            template_payload = stored_template.get("template")
        if template_payload is None:
            raise ValueError("ATM template is required when creating a strategy.")

        normalised_template = normalise_template(template_payload, require_template=True)
        risk_fields = _risk_fields_from_template(normalised_template)

        # Save or get the ATM template ID
        if atm_template_id:
            final_template_id = atm_template_id
        else:
            saved_template = upsert_atm_template({"name": normalised_template.get("name") or clean_name, "template": normalised_template})
            final_template_id = saved_template.get("id")

        record = StrategyDefinition(
            id=strategy_id,
            name=clean_name,
            description=str(description).strip() if description else None,
            instruments=clean_slots or [InstrumentSlot(symbol="Unknown")],
            timeframe=str(timeframe).strip(),
            datasource=str(datasource).strip() if datasource else None,
            exchange=str(exchange).strip() if exchange else None,
            indicator_ids=list(dict.fromkeys(indicators)),
            atm_template_id=final_template_id,
            base_risk_per_trade=base_risk_per_trade if base_risk_per_trade is not None else risk_fields.get("base_risk_per_trade"),
            global_risk_multiplier=global_risk_multiplier if global_risk_multiplier is not None else risk_fields.get("global_risk_multiplier"),
            risk_overrides={
                **({slot.symbol: slot.risk_multiplier for slot in clean_slots if slot.risk_multiplier is not None}),
                **(dict(risk_overrides or {})),
            },
        )
        for inst_id in record.indicator_ids:
            try:
                meta = deepcopy(get_instance_meta(inst_id))
            except KeyError:
                meta = {}
            # REMOVED: indicator_snapshots assignment - no longer storing snapshots
        self._sync_instruments(record)
        self._records[strategy_id] = record
        storage_upsert_strategy(record.to_storage_payload())
        for inst_id in record.indicator_ids:
            storage_upsert_strategy_indicator(
                strategy_id=strategy_id,
                indicator_id=inst_id,
                # REMOVED: snapshot parameter - no longer storing snapshots
            )
        # Persist instrument links for any resolved instruments
        for slot in record.instruments:
            inst_id = slot.metadata.get("instrument_id") if isinstance(slot.metadata, dict) else None
            instrument_rec = None
            if inst_id:
                try:
                    instrument_rec = instrument_service.get_instrument_record(inst_id)
                except Exception:
                    instrument_rec = None
            else:
                try:
                    instrument_rec = instrument_service.resolve_instrument(record.datasource, record.exchange, slot.symbol)
                except Exception:
                    instrument_rec = None
            if instrument_rec:
                storage_upsert_strategy_instrument(
                    strategy_id=strategy_id,
                    instrument_id=instrument_rec.get("id"),
                    snapshot=instrument_rec,
                )
        logger.info("strategy_created | id=%s name=%s", strategy_id, clean_name)
        return record.to_dict()

    def update(self, strategy_id: str, **fields: Any) -> Dict[str, Any]:
        """Update an existing strategy and return its payload."""
        record = self.get(strategy_id)
        # Capture previous instrument slots and provider context for diffing
        old_slots = [InstrumentSlot.from_any(slot.to_dict() if hasattr(slot, "to_dict") else slot) for slot in record.instruments]
        old_datasource = record.datasource
        old_exchange = record.exchange
        if fields.get("atm_template") is not None:
            normalised_template = normalise_template(fields.get("atm_template"), require_template=True)
            risk_fields = _risk_fields_from_template(normalised_template)
            record.base_risk_per_trade = risk_fields.get("base_risk_per_trade")
            record.global_risk_multiplier = risk_fields.get("global_risk_multiplier")
            candidate_template_id = fields.get("atm_template_id") or record.atm_template_id
            saved_template = upsert_atm_template(
                {
                    "id": candidate_template_id,
                    "name": normalised_template.get("name") or record.name,
                    "template": normalised_template,
                }
            )
            record.atm_template_id = saved_template.get("id")
        record.update(**fields)
        if record.instruments:
            record.risk_overrides = {
                slot.symbol: slot.risk_multiplier
                for slot in record.instruments
                if slot.risk_multiplier is not None
            }
        self._sync_instruments(record)
        storage_upsert_strategy(record.to_storage_payload())
        # Persist instrument link changes: upsert new links, delete removed links
        try:
            # Resolve previous instrument ids
            def _resolve_slot_id(slot: InstrumentSlot, datasource: Optional[str], exchange: Optional[str]) -> Optional[str]:
                if isinstance(slot.metadata, dict) and slot.metadata.get("instrument_id"):
                    return str(slot.metadata.get("instrument_id"))
                try:
                    rec = instrument_service.resolve_instrument(datasource, exchange, slot.symbol)
                    return rec.get("id") if rec else None
                except Exception as exc:
                    logger.warning(
                        "strategy_instrument_resolution_failed | strategy_id=%s symbol=%s datasource=%s exchange=%s error=%s",
                        strategy_id,
                        slot.symbol,
                        datasource,
                        exchange,
                        exc,
                    )
                    return None

            old_ids = {i for i in (_resolve_slot_id(s, old_datasource, old_exchange) for s in old_slots) if i}
            new_ids = set()
            for slot in record.instruments:
                inst_id = None
                if isinstance(slot.metadata, dict) and slot.metadata.get("instrument_id"):
                    inst_id = str(slot.metadata.get("instrument_id"))
                else:
                    try:
                        rec = instrument_service.resolve_instrument(record.datasource, record.exchange, slot.symbol)
                        inst_id = rec.get("id") if rec else None
                    except Exception as exc:
                        logger.warning(
                            "strategy_instrument_resolution_failed | strategy_id=%s symbol=%s datasource=%s exchange=%s error=%s",
                            strategy_id,
                            slot.symbol,
                            record.datasource,
                            record.exchange,
                            exc,
                        )
                        inst_id = None
                if inst_id:
                    new_ids.add(inst_id)
                    # upsert snapshot
                    try:
                        rec = instrument_service.get_instrument_record(inst_id)
                    except Exception:
                        rec = None
                    if rec:
                        storage_upsert_strategy_instrument(strategy_id=strategy_id, instrument_id=inst_id, snapshot=rec)

            # delete removed links
            for removed in (old_ids - new_ids):
                storage_delete_strategy_instrument(strategy_id, removed)
        except Exception as exc:
            logger.exception(
                "strategy_update_instrument_link_sync_failed | strategy=%s",
                strategy_id,
            )
            raise RuntimeError(
                f"strategy_update_instrument_link_sync_failed: strategy={strategy_id}"
            ) from exc
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
        # REMOVED: indicator_snapshots assignment - no longer storing snapshots
        storage_upsert_strategy(record.to_storage_payload())
        storage_upsert_strategy_indicator(
            strategy_id=strategy_id,
            indicator_id=inst_id,
            # REMOVED: snapshot parameter - no longer storing snapshots
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
            # REMOVED: indicator_snapshots cleanup - no longer storing snapshots
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

    def list_strategy_filters(self, strategy_id: str) -> List[Dict[str, Any]]:
        record = self.get(strategy_id)
        return [flt.to_dict() for flt in record.global_filters]

    def list_rule_filters(self, strategy_id: str, rule_id: str) -> List[Dict[str, Any]]:
        record = self.get(strategy_id)
        rule = record.rules.get(rule_id)
        if rule is None:
            raise KeyError("Rule not found")
        return [flt.to_dict() for flt in rule.filters]

    def add_strategy_filter(
        self,
        strategy_id: str,
        *,
        name: str,
        dsl: Mapping[str, Any],
        description: Optional[str] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        record = self.get(strategy_id)
        validate_filter_dsl(dsl)
        filter_id = str(uuid.uuid4())
        now = _utcnow()
        flt = FilterDefinition(
            id=filter_id,
            scope="GLOBAL",
            name=str(name).strip() or filter_id,
            description=str(description).strip() if description else None,
            dsl=dict(dsl),
            enabled=bool(enabled),
            created_at=now,
            updated_at=now,
        )
        record.global_filters.append(flt)
        storage_upsert_strategy_filter(flt.to_storage_payload(strategy_id))
        storage_upsert_strategy(record.to_storage_payload())
        logger.info(
            "strategy_filter_created | strategy=%s filter=%s",
            strategy_id,
            filter_id,
        )
        return flt.to_dict()

    def update_strategy_filter(
        self,
        strategy_id: str,
        filter_id: str,
        **fields: Any,
    ) -> Dict[str, Any]:
        record = self.get(strategy_id)
        target = next((flt for flt in record.global_filters if flt.id == filter_id), None)
        if target is None:
            raise KeyError("Filter not found")
        if "name" in fields and fields["name"] is not None:
            target.name = str(fields["name"]).strip()
        if "description" in fields:
            description = fields.get("description")
            target.description = str(description).strip() if description else None
        if "enabled" in fields and fields["enabled"] is not None:
            target.enabled = bool(fields["enabled"])
        if "dsl" in fields and fields["dsl"] is not None:
            validate_filter_dsl(fields["dsl"])
            target.dsl = dict(fields["dsl"])
        target.updated_at = _utcnow()
        storage_upsert_strategy_filter(target.to_storage_payload(strategy_id))
        storage_upsert_strategy(record.to_storage_payload())
        logger.info(
            "strategy_filter_updated | strategy=%s filter=%s",
            strategy_id,
            filter_id,
        )
        return target.to_dict()

    def remove_strategy_filter(self, strategy_id: str, filter_id: str) -> None:
        record = self.get(strategy_id)
        before = len(record.global_filters)
        record.global_filters = [flt for flt in record.global_filters if flt.id != filter_id]
        if len(record.global_filters) == before:
            raise KeyError("Filter not found")
        storage_delete_strategy_filter(filter_id)
        storage_upsert_strategy(record.to_storage_payload())
        logger.info(
            "strategy_filter_deleted | strategy=%s filter=%s",
            strategy_id,
            filter_id,
        )

    def add_rule_filter(
        self,
        strategy_id: str,
        rule_id: str,
        *,
        name: str,
        dsl: Mapping[str, Any],
        description: Optional[str] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        record = self.get(strategy_id)
        rule = record.rules.get(rule_id)
        if rule is None:
            raise KeyError("Rule not found")
        validate_filter_dsl(dsl)
        filter_id = str(uuid.uuid4())
        now = _utcnow()
        flt = FilterDefinition(
            id=filter_id,
            scope="RULE",
            name=str(name).strip() or filter_id,
            description=str(description).strip() if description else None,
            dsl=dict(dsl),
            enabled=bool(enabled),
            created_at=now,
            updated_at=now,
        )
        rule.filters.append(flt)
        storage_upsert_rule_filter(flt.to_storage_payload(rule_id))
        storage_upsert_strategy(record.to_storage_payload())
        logger.info(
            "rule_filter_created | strategy=%s rule=%s filter=%s",
            strategy_id,
            rule_id,
            filter_id,
        )
        return flt.to_dict()

    def update_rule_filter(
        self,
        strategy_id: str,
        rule_id: str,
        filter_id: str,
        **fields: Any,
    ) -> Dict[str, Any]:
        record = self.get(strategy_id)
        rule = record.rules.get(rule_id)
        if rule is None:
            raise KeyError("Rule not found")
        target = next((flt for flt in rule.filters if flt.id == filter_id), None)
        if target is None:
            raise KeyError("Filter not found")
        if "name" in fields and fields["name"] is not None:
            target.name = str(fields["name"]).strip()
        if "description" in fields:
            description = fields.get("description")
            target.description = str(description).strip() if description else None
        if "enabled" in fields and fields["enabled"] is not None:
            target.enabled = bool(fields["enabled"])
        if "dsl" in fields and fields["dsl"] is not None:
            validate_filter_dsl(fields["dsl"])
            target.dsl = dict(fields["dsl"])
        target.updated_at = _utcnow()
        storage_upsert_rule_filter(target.to_storage_payload(rule_id))
        storage_upsert_strategy(record.to_storage_payload())
        logger.info(
            "rule_filter_updated | strategy=%s rule=%s filter=%s",
            strategy_id,
            rule_id,
            filter_id,
        )
        return target.to_dict()

    def remove_rule_filter(self, strategy_id: str, rule_id: str, filter_id: str) -> None:
        record = self.get(strategy_id)
        rule = record.rules.get(rule_id)
        if rule is None:
            raise KeyError("Rule not found")
        before = len(rule.filters)
        rule.filters = [flt for flt in rule.filters if flt.id != filter_id]
        if len(rule.filters) == before:
            raise KeyError("Filter not found")
        storage_delete_rule_filter(filter_id)
        storage_upsert_strategy(record.to_storage_payload())
        logger.info(
            "rule_filter_deleted | strategy=%s rule=%s filter=%s",
            strategy_id,
            rule_id,
            filter_id,
        )

    def evaluate(
        self,
        strategy_id: str,
        *,
        start: str,
        end: str,
        interval: str,
        instrument_ids: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
        dependencies: Optional[StrategyEvaluationDependencies] = None,
    ) -> Dict[str, Any]:
        """Evaluate a strategy against current indicator signals."""

        record = self.get(strategy_id)
        orchestrator = StrategyEvaluationOrchestrator(record, dependencies=dependencies)
        inputs = orchestrator.build_inputs(
            strategy_id=strategy_id,
            start=start,
            end=end,
            interval=interval,
            instrument_ids=instrument_ids,
            config=config,
        )
        context = orchestrator.build_context(inputs)
        return orchestrator.evaluate(inputs, context)


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
    symbols: Iterable[Any],
    timeframe: str,
    description: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    indicator_ids: Optional[Iterable[str]] = None,
    atm_template: Optional[Mapping[str, Any]] = None,
    atm_template_id: Optional[str] = None,
    base_risk_per_trade: Optional[float] = None,
    global_risk_multiplier: Optional[float] = None,
    risk_overrides: Optional[Mapping[str, Any]] = None,
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
        atm_template=atm_template,
        atm_template_id=atm_template_id,
        base_risk_per_trade=base_risk_per_trade,
        global_risk_multiplier=global_risk_multiplier,
        risk_overrides=risk_overrides,
    )


def update_strategy(strategy_id: str, **fields: Any) -> Dict[str, Any]:
    """Update the specified strategy."""

    return _REGISTRY.update(strategy_id, **fields)


def list_atm_templates() -> List[Dict[str, Any]]:
    """Return all persisted ATM templates."""

    return load_atm_templates()


def save_atm_template(template: Mapping[str, Any]) -> Dict[str, Any]:
    """Persist a standalone ATM template for reuse."""

    payload_template = template.get("template") if isinstance(template, Mapping) else None
    normalized = normalise_template(payload_template or template, require_template=True)
    name = str(template.get("name") or normalized.get("name") or "ATM template").strip()
    request_payload = {
        "id": template.get("id"),
        "name": name,
        "template": normalized,
    }
    return upsert_atm_template(request_payload)


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


def list_strategy_filters(strategy_id: str) -> List[Dict[str, Any]]:
    """Return global filters for a strategy."""

    return _REGISTRY.list_strategy_filters(strategy_id)


def list_rule_filters(strategy_id: str, rule_id: str) -> List[Dict[str, Any]]:
    """Return filters for a specific rule."""

    return _REGISTRY.list_rule_filters(strategy_id, rule_id)


def create_strategy_filter(
    strategy_id: str,
    *,
    name: str,
    dsl: Mapping[str, Any],
    description: Optional[str] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Create a strategy-wide filter."""

    return _REGISTRY.add_strategy_filter(
        strategy_id,
        name=name,
        dsl=dsl,
        description=description,
        enabled=enabled,
    )


def update_strategy_filter(strategy_id: str, filter_id: str, **fields: Any) -> Dict[str, Any]:
    """Update a strategy-wide filter."""

    return _REGISTRY.update_strategy_filter(strategy_id, filter_id, **fields)


def delete_strategy_filter(strategy_id: str, filter_id: str) -> None:
    """Remove a strategy-wide filter."""

    _REGISTRY.remove_strategy_filter(strategy_id, filter_id)


def create_rule_filter(
    strategy_id: str,
    rule_id: str,
    *,
    name: str,
    dsl: Mapping[str, Any],
    description: Optional[str] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Create a rule-scoped filter."""

    return _REGISTRY.add_rule_filter(
        strategy_id,
        rule_id,
        name=name,
        dsl=dsl,
        description=description,
        enabled=enabled,
    )


def update_rule_filter(strategy_id: str, rule_id: str, filter_id: str, **fields: Any) -> Dict[str, Any]:
    """Update a rule-scoped filter."""

    return _REGISTRY.update_rule_filter(strategy_id, rule_id, filter_id, **fields)


def delete_rule_filter(strategy_id: str, rule_id: str, filter_id: str) -> None:
    """Remove a rule-scoped filter."""

    _REGISTRY.remove_rule_filter(strategy_id, rule_id, filter_id)


def generate_strategy_signals(
    strategy_id: str,
    *,
    start: str,
    end: str,
    interval: str,
    instrument_ids: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
    dependencies: Optional[StrategyEvaluationDependencies] = None,
) -> Dict[str, Any]:
    """Evaluate the strategy rules for the requested window."""

    return _REGISTRY.evaluate(
        strategy_id,
        start=start,
        end=end,
        interval=interval,
        instrument_ids=instrument_ids,
        config=config,
        dependencies=dependencies,
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

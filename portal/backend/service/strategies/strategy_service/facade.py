"""In-memory strategy rule orchestration for the portal."""

from __future__ import annotations

import logging
from collections import OrderedDict
from copy import deepcopy
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from ...market import instrument_service
from ...risk.atm import normalise_template
from ...indicators.indicator_service import get_instance_meta
from engines.bot_runtime.core.execution_profile import compile_runtime_profile_or_error
from risk import normalise_risk_config
from strategies.contracts import CompiledStrategySpec, DecisionRuleSpec
from strategies.compiler import compile_strategy, normalize_rule_intent
from . import persistence
from .typed_preview import evaluate_strategy_preview


logger = logging.getLogger(__name__)
_RUNTIME_ALLOWED_DERIVATIVE_TYPES = {"future", "futures", "perp", "perps"}


def _utcnow() -> datetime:
    """Return a naive UTC timestamp for metadata fields."""

    return datetime.now(UTC).replace(tzinfo=None)


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
_LEGACY_TYPED_RULE_V1 = "typed_rule_v1"

storage_load_strategies = persistence.load_strategies
storage_upsert_strategy = persistence.upsert_strategy
storage_delete_strategy = persistence.delete_strategy
storage_upsert_strategy_indicator = persistence.upsert_strategy_indicator
storage_delete_strategy_indicator = persistence.delete_strategy_indicator
storage_upsert_strategy_rule = persistence.upsert_strategy_rule
storage_delete_strategy_rule = persistence.delete_strategy_rule
storage_list_strategy_variants = persistence.list_strategy_variants
storage_get_strategy_variant = persistence.get_strategy_variant
storage_ensure_default_strategy_variant = persistence.ensure_default_strategy_variant
storage_upsert_strategy_variant = persistence.upsert_strategy_variant
storage_delete_strategy_variant = persistence.delete_strategy_variant
list_symbol_presets = persistence.list_symbol_presets
upsert_symbol_preset = persistence.upsert_symbol_preset
delete_symbol_preset = persistence.delete_symbol_preset
load_atm_templates = persistence.list_atm_templates
get_atm_template = persistence.get_atm_template
upsert_atm_template = persistence.upsert_atm_template
storage_upsert_strategy_instrument = persistence.upsert_strategy_instrument
storage_delete_strategy_instrument = persistence.delete_strategy_instrument
storage_list_strategy_instrument_symbols = persistence.list_strategy_instrument_symbols


@dataclass
class InstrumentSlot:
    """Represents a symbol attached to a strategy along with runtime hints."""

    symbol: str
    risk_multiplier: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a serialisable representation of the slot."""

        payload: Dict[str, Any] = {
            "symbol": self.symbol,
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
                risk_multiplier=float(value["risk_multiplier"]) if value.get("risk_multiplier") is not None else None,
                metadata=dict(value.get("metadata") or {}),
            )
        return InstrumentSlot(symbol=str(value or "").strip())

def _serialize_trigger(trigger: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "type": "signal_match",
        "indicator_id": str(trigger.get("indicator_id") or "").strip(),
        "output_name": str(trigger.get("output_name") or "").strip(),
        "event_key": str(trigger.get("event_key") or "").strip(),
    }


def _serialize_guard(guard: Mapping[str, Any]) -> Dict[str, Any]:
    guard_type = str(guard.get("type") or "").strip().lower()
    if guard_type == "context_match":
        return {
            "type": "context_match",
            "indicator_id": str(guard.get("indicator_id") or "").strip(),
            "output_name": str(guard.get("output_name") or "").strip(),
            "field": str(guard.get("field") or "").strip(),
            "value": str(guard.get("value") or "").strip(),
        }
    if guard_type == "metric_match":
        return {
            "type": "metric_match",
            "indicator_id": str(guard.get("indicator_id") or "").strip(),
            "output_name": str(guard.get("output_name") or "").strip(),
            "field": str(guard.get("field") or "").strip(),
            "operator": str(guard.get("operator") or "").strip(),
            "value": float(guard.get("value")),
        }
    if guard_type == "holds_for_bars":
        inner = guard.get("guard")
        return {
            "type": "holds_for_bars",
            "bars": int(guard.get("bars") or 0),
            "guard": _serialize_guard(inner if isinstance(inner, Mapping) else {}),
        }
    if guard_type in {"signal_seen_within_bars", "signal_absent_within_bars"}:
        return {
            "type": guard_type,
            "indicator_id": str(guard.get("indicator_id") or "").strip(),
            "output_name": str(guard.get("output_name") or "").strip(),
            "event_key": str(guard.get("event_key") or "").strip(),
            "lookback_bars": int(guard.get("lookback_bars") or 0),
        }
    raise ValueError(f"Unsupported guard type: {guard_type or 'unknown'}")


def _normalize_rule_contract(
    strategy: "StrategyDefinition",
    raw_rule: Mapping[str, Any],
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    compiled = compile_strategy(
        strategy_id=strategy.id,
        timeframe=strategy.timeframe,
        rules=[raw_rule],
        attached_indicator_ids=strategy.indicator_ids,
        indicator_meta_getter=get_instance_meta,
        params=params,
    )
    rule = compiled.rules[0]
    return {
        "id": str(raw_rule.get("id") or rule.id),
        "name": str(raw_rule.get("name") or rule.name).strip(),
        "intent": str(rule.intent),
        "priority": int(rule.priority),
        "trigger": _serialize_trigger(rule.trigger.__dict__),
        "guards": [_serialize_guard(_guard_to_raw_dict(guard)) for guard in rule.guards],
        "description": str(raw_rule.get("description")).strip() if raw_rule.get("description") else None,
        "enabled": bool(raw_rule.get("enabled", rule.enabled)),
    }


def _guard_to_raw_dict(guard: Any) -> Dict[str, Any]:
    payload = dict(guard.__dict__)
    if payload.get("guard") is not None:
        payload["guard"] = _guard_to_raw_dict(payload["guard"])
    payload.pop("output_key", None)
    return payload


def _rule_to_storage_conditions(rule: "StrategyRule") -> Dict[str, Any]:
    return {
        "intent": rule.intent,
        "priority": int(rule.priority),
        "trigger": deepcopy(rule.trigger),
        "guards": deepcopy(rule.guards),
    }


def _select_default_strategy_variant(strategy_id: str) -> Dict[str, Any]:
    return storage_ensure_default_strategy_variant(strategy_id)


def _resolve_strategy_variant_payload(strategy_id: str, variant_id: Optional[str]) -> Dict[str, Any]:
    if variant_id:
        variant = storage_get_strategy_variant(variant_id)
        if not variant or str(variant.get("strategy_id") or "").strip() != strategy_id:
            raise KeyError("Strategy variant not found")
        return dict(variant)
    return dict(_select_default_strategy_variant(strategy_id))


def _resolve_effective_variant_params(
    strategy_id: str,
    *,
    selected_variant: Mapping[str, Any],
) -> Dict[str, Any]:
    default_variant = _select_default_strategy_variant(strategy_id)
    default_variant_id = str(default_variant.get("id") or "").strip()
    selected_variant_id = str(selected_variant.get("id") or "").strip()
    default_params = dict(default_variant.get("param_overrides") or {})
    selected_params = dict(selected_variant.get("param_overrides") or {})
    if bool(selected_variant.get("is_default", False)):
        return selected_params
    if default_variant_id and selected_variant_id == default_variant_id:
        return default_params
    return {
        **default_params,
        **selected_params,
    }


def _compile_strategy_definition(
    strategy: "StrategyDefinition",
    *,
    rules: Optional[Sequence[Mapping[str, Any]]] = None,
    variant_id: Optional[str] = None,
    selected_variant: Optional[Mapping[str, Any]] = None,
    timeframe: Optional[str] = None,
) -> tuple[CompiledStrategySpec, Dict[str, Any], Dict[str, Any]]:
    resolved_variant = (
        dict(selected_variant)
        if selected_variant is not None
        else _resolve_strategy_variant_payload(strategy.id, variant_id)
    )
    resolved_params = _resolve_effective_variant_params(
        strategy.id,
        selected_variant=resolved_variant,
    )
    compiled = compile_strategy(
        strategy_id=strategy.id,
        timeframe=str(timeframe or strategy.timeframe).strip(),
        rules=list(rules) if rules is not None else [rule.to_dict() for rule in strategy.rules.values()],
        attached_indicator_ids=strategy.indicator_ids,
        indicator_meta_getter=get_instance_meta,
        params=resolved_params,
    )
    return compiled, resolved_variant, resolved_params


def _validate_rule_set(
    strategy: "StrategyDefinition",
    rules: Sequence[Mapping[str, Any]],
    *,
    variant_id: Optional[str] = None,
) -> None:
    _compile_strategy_definition(
        strategy,
        rules=rules,
        variant_id=variant_id,
    )


def _serialize_compiled_rule(rule: DecisionRuleSpec) -> Dict[str, Any]:
    return {
        "id": rule.id,
        "name": rule.name,
        "intent": rule.intent,
        "priority": int(rule.priority),
        "enabled": bool(rule.enabled),
        "description": rule.description,
        "trigger": _serialize_trigger(rule.trigger.__dict__),
        "guards": [_serialize_guard(_guard_to_raw_dict(guard)) for guard in rule.guards],
    }


def _serialize_variant_payload(variant: Mapping[str, Any], *, resolved_params: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(variant.get("id") or "").strip(),
        "name": str(variant.get("name") or "").strip(),
        "description": variant.get("description"),
        "param_overrides": dict(variant.get("param_overrides") or {}),
        "resolved_params": dict(resolved_params or {}),
        "atm_template_id": str(variant.get("atm_template_id") or "").strip() or None,
        "is_default": bool(variant.get("is_default", False)),
    }


def _serialize_compiled_strategy_payload(
    strategy: "StrategyDefinition",
    *,
    compiled_strategy: CompiledStrategySpec,
    selected_variant: Mapping[str, Any],
    resolved_params: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "strategy_id": strategy.id,
        "strategy_name": strategy.name,
        "timeframe": compiled_strategy.timeframe,
        "strategy_hash": compiled_strategy.strategy_hash,
        "variant": _serialize_variant_payload(selected_variant, resolved_params=resolved_params),
        "compiled": {
            "strategy_hash": compiled_strategy.strategy_hash,
            "timeframe": compiled_strategy.timeframe,
            "max_history_bars": int(compiled_strategy.max_history_bars),
            "rule_count": len(compiled_strategy.rules),
            "rules": [_serialize_compiled_rule(rule) for rule in compiled_strategy.rules],
        },
    }


def _legacy_rule_payload(rule_entry: Mapping[str, Any]) -> Dict[str, Any]:
    raw_conditions = rule_entry.get("conditions")
    if isinstance(raw_conditions, Mapping) and str(raw_conditions.get("kind") or "").strip() == _LEGACY_TYPED_RULE_V1:
        raw_when = raw_conditions.get("when")
        payload: Dict[str, Any] = {
            "id": rule_entry.get("id"),
            "name": rule_entry.get("name"),
            "intent": normalize_rule_intent(rule_entry.get("action", "buy")),
            "description": rule_entry.get("description"),
            "enabled": bool(rule_entry.get("enabled", True)),
        }
        if isinstance(raw_when, Mapping):
            payload["when"] = raw_when
        return payload
    if isinstance(raw_conditions, Mapping):
        payload = dict(raw_conditions)
        payload.setdefault("id", rule_entry.get("id"))
        payload.setdefault("name", rule_entry.get("name"))
        payload.setdefault("intent", raw_conditions.get("intent") or normalize_rule_intent(rule_entry.get("action", "buy")))
        payload.setdefault("description", rule_entry.get("description"))
        payload.setdefault("enabled", bool(rule_entry.get("enabled", True)))
        payload.setdefault("priority", raw_conditions.get("priority") or 0)
        return payload
    raise ValueError("Rule record missing canonical conditions payload")


@dataclass
class StrategyRule:
    """Represents a canonical strategy rule."""

    id: str
    name: str
    intent: str
    trigger: Dict[str, Any]
    guards: List[Dict[str, Any]]
    priority: int = 0
    description: Optional[str] = None
    enabled: bool = True
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the rule for API responses."""

        return {
            "id": self.id,
            "name": self.name,
            "intent": self.intent,
            "priority": int(self.priority),
            "trigger": deepcopy(self.trigger),
            "guards": deepcopy(self.guards),
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
            "action": "buy" if self.intent == "enter_long" else "sell",
            "match": "all",
            "description": self.description,
            "enabled": self.enabled,
            "conditions": _rule_to_storage_conditions(self),
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
    instrument_messages: List[Dict[str, str]] = field(default_factory=list)
    atm_template_id: Optional[str] = None
    risk_config: Dict[str, Any] = field(default_factory=dict)
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
                    "Strategy indicator meta | indicator_id=%s | has_typed_outputs=%s | count=%d",
                    identifier,
                    "typed_outputs" in (active_meta or {}),
                    len(active_meta.get("typed_outputs", [])) if active_meta else 0,
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
            "atm_template": atm_template or {},
            "atm_template_id": self.atm_template_id,
            "risk_config": normalise_risk_config(self.risk_config),
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
            "risk_config": normalise_risk_config(self.risk_config),
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
        if "risk_config" in fields and fields["risk_config"] is not None:
            self.risk_config = normalise_risk_config(fields.get("risk_config"))
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
            base.risk_config = normalise_risk_config(entry.get("risk_config"))

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
                try:
                    normalized_rule = _normalize_rule_contract(base, _legacy_rule_payload(rule_entry))
                except Exception as exc:
                    logger.warning(
                        "strategy_rule_skipped | strategy_id=%s rule_id=%s error=%s",
                        entry.get("id"),
                        rule_id,
                        exc,
                    )
                    continue
                rule = StrategyRule(
                    id=rule_id,
                    name=str(normalized_rule.get("name") or rule_id),
                    intent=str(normalized_rule.get("intent") or "enter_long"),
                    priority=int(normalized_rule.get("priority") or 0),
                    trigger=dict(normalized_rule.get("trigger") or {}),
                    guards=[dict(item) for item in (normalized_rule.get("guards") or []) if isinstance(item, Mapping)],
                    description=normalized_rule.get("description"),
                    enabled=bool(normalized_rule.get("enabled", True)),
                    created_at=_parse_timestamp(rule_entry.get("created_at")),
                    updated_at=_parse_timestamp(rule_entry.get("updated_at")),
                )
                base.rules[rule_id] = rule

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
        risk_config: Optional[Mapping[str, Any]] = None,
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
            risk_config=normalise_risk_config(
                {
                    **dict(risk_config or {}),
                    "instrument_multipliers": {
                        **dict((risk_config or {}).get("instrument_multipliers") or {}),
                        **{slot.symbol: slot.risk_multiplier for slot in clean_slots if slot.risk_multiplier is not None},
                    },
                }
            ),
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
        storage_ensure_default_strategy_variant(strategy_id)
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
            instrument_multipliers = {
                slot.symbol: slot.risk_multiplier
                for slot in record.instruments
                if slot.risk_multiplier is not None
            }
            record.risk_config = normalise_risk_config(
                {
                    **record.risk_config,
                    "instrument_multipliers": instrument_multipliers,
                }
            )
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
        intent: str,
        trigger: Mapping[str, Any],
        guards: Sequence[Mapping[str, Any]] | None = None,
        priority: int = 0,
        description: Optional[str] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Create a rule for the strategy."""

        record = self.get(strategy_id)
        _, _, default_params = _compile_strategy_definition(
            record,
            rules=[],
        )
        rule_id = str(uuid.uuid4())
        normalized_rule = _normalize_rule_contract(
            record,
            {
                "id": rule_id,
                "name": name,
                "intent": intent,
                "priority": priority,
                "trigger": trigger,
                "guards": list(guards or []),
                "description": description,
                "enabled": enabled,
            },
            params=default_params,
        )
        _validate_rule_set(
            record,
            [*(existing.to_dict() for existing in record.rules.values()), normalized_rule],
        )
        rule = StrategyRule(
            id=rule_id,
            name=str(normalized_rule.get("name") or rule_id),
            intent=str(normalized_rule.get("intent") or "enter_long"),
            priority=int(normalized_rule.get("priority") or 0),
            trigger=dict(normalized_rule.get("trigger") or {}),
            guards=[dict(item) for item in (normalized_rule.get("guards") or []) if isinstance(item, Mapping)],
            description=normalized_rule.get("description"),
            enabled=bool(normalized_rule.get("enabled", True)),
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
        _, _, default_params = _compile_strategy_definition(
            record,
            rules=[],
        )
        rule = record.rules.get(rule_id)
        if rule is None:
            raise KeyError("Rule not found")

        normalized_rule = _normalize_rule_contract(
            record,
            {
                "id": rule.id,
                "name": fields.get("name", rule.name),
                "intent": fields.get("intent", rule.intent),
                "priority": fields.get("priority", rule.priority),
                "trigger": fields.get("trigger", rule.trigger),
                "guards": fields.get("guards", rule.guards),
                "description": fields.get("description", rule.description),
                "enabled": fields.get("enabled", rule.enabled),
            },
            params=default_params,
        )
        _validate_rule_set(
            record,
            [
                *(existing.to_dict() for existing_id, existing in record.rules.items() if existing_id != rule_id),
                normalized_rule,
            ],
        )
        rule.name = str(normalized_rule.get("name") or rule.id)
        rule.intent = str(normalized_rule.get("intent") or "enter_long")
        rule.priority = int(normalized_rule.get("priority") or 0)
        rule.trigger = dict(normalized_rule.get("trigger") or {})
        rule.guards = [dict(item) for item in (normalized_rule.get("guards") or []) if isinstance(item, Mapping)]
        rule.description = normalized_rule.get("description")
        rule.enabled = bool(normalized_rule.get("enabled", True))

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
        instrument_ids: Optional[List[str]] = None,
        variant_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run a rule-logic strategy preview against typed indicator outputs."""
        record = self.get(strategy_id)
        requested_ids = [str(item).strip() for item in (instrument_ids or []) if str(item).strip()]
        preview_id = str(uuid.uuid4())
        compiled_strategy, selected_variant, resolved_params = _compile_strategy_definition(
            record,
            variant_id=variant_id,
            timeframe=interval,
        )
        payload = evaluate_strategy_preview(
            record=record,
            strategy_id=strategy_id,
            preview_id=preview_id,
            start=start,
            end=end,
            interval=interval,
            instrument_ids=requested_ids,
            compiled_strategy=compiled_strategy,
            selected_variant=selected_variant,
            resolved_params=resolved_params,
        )
        _PREVIEW_RESULTS.put(payload)
        return payload


@dataclass(frozen=True)
class StrategyPreviewResult:
    preview_id: str
    strategy_id: str
    payload: Dict[str, Any]
    created_at: datetime = field(default_factory=_utcnow)


class StrategyPreviewStore:
    """Per-process retained strategy preview results for follow-up inspection."""

    def __init__(self, *, max_entries: int = 100) -> None:
        self._max_entries = max(1, int(max_entries))
        self._results: OrderedDict[str, StrategyPreviewResult] = OrderedDict()

    def put(self, payload: Mapping[str, Any]) -> StrategyPreviewResult:
        preview_id = str(payload.get("preview_id") or "").strip()
        strategy_id = str(payload.get("strategy_id") or "").strip()
        if not preview_id or not strategy_id:
            raise ValueError("strategy_preview_result_invalid: preview_id and strategy_id are required")
        result = StrategyPreviewResult(
            preview_id=preview_id,
            strategy_id=strategy_id,
            payload=deepcopy(dict(payload)),
        )
        self._results[preview_id] = result
        self._results.move_to_end(preview_id)
        while len(self._results) > self._max_entries:
            self._results.popitem(last=False)
        return result

    def get(self, strategy_id: str, preview_id: str) -> StrategyPreviewResult:
        result = self._results.get(str(preview_id))
        if result is None or result.strategy_id != str(strategy_id):
            raise KeyError("Strategy preview not found")
        return result

    def get_signal_detail(self, strategy_id: str, preview_id: str, signal_id: str) -> Dict[str, Any]:
        result = self.get(strategy_id, preview_id)
        target_signal_id = str(signal_id or "").strip()
        if not target_signal_id:
            raise KeyError("Strategy preview signal not found")
        instruments = result.payload.get("instruments")
        if not isinstance(instruments, Mapping):
            raise KeyError("Strategy preview signal not found")
        for instrument_payload in instruments.values():
            if not isinstance(instrument_payload, Mapping):
                continue
            machine_payload = instrument_payload.get("machine")
            signals = machine_payload.get("signals") if isinstance(machine_payload, Mapping) else instrument_payload.get("signals")
            if not isinstance(signals, list):
                continue
            matched_signal = next(
                (
                    dict(item)
                    for item in signals
                    if isinstance(item, Mapping)
                    and str(item.get("signal_id") or "").strip() == target_signal_id
                ),
                None,
            )
            if matched_signal is None:
                continue
            decision_artifacts = (
                machine_payload.get("decision_artifacts")
                if isinstance(machine_payload, Mapping)
                else instrument_payload.get("decision_artifacts")
            )
            matched_artifact = next(
                (
                    dict(item)
                    for item in (decision_artifacts or [])
                    if isinstance(item, Mapping)
                    and str(item.get("decision_id") or "").strip() == str(matched_signal.get("decision_id") or "").strip()
                ),
                None,
            )
            overlays = instrument_payload.get("overlays")
            ui_signal_markers: List[Dict[str, Any]] = []
            if isinstance(overlays, list):
                for overlay in overlays:
                    if not isinstance(overlay, Mapping):
                        continue
                    payload = overlay.get("payload")
                    markers = payload.get("markers") if isinstance(payload, Mapping) else None
                    if not isinstance(markers, list):
                        continue
                    ui_signal_markers.extend(
                        dict(marker)
                        for marker in markers
                        if isinstance(marker, Mapping)
                        and str(marker.get("signal_id") or "").strip() == target_signal_id
                    )
            return {
                "preview_id": result.preview_id,
                "strategy_id": result.strategy_id,
                "signal": matched_signal,
                "audit": {
                    "decision_artifact": matched_artifact,
                    "window": deepcopy(dict(instrument_payload.get("window") or {})),
                    "instrument_id": instrument_payload.get("instrument_id"),
                    "symbol": instrument_payload.get("symbol"),
                    "status": instrument_payload.get("status"),
                    "missing_indicators": list(instrument_payload.get("missing_indicators") or []),
                },
                "ui": {
                    "markers": ui_signal_markers,
                },
            }
        raise KeyError("Strategy preview signal not found")


_REGISTRY = StrategyRegistry()
_PREVIEW_RESULTS = StrategyPreviewStore()


def list_strategies() -> List[Dict[str, Any]]:
    """Return all registered strategies."""

    return _REGISTRY.list()


def get_strategy(strategy_id: str) -> Dict[str, Any]:
    """Return the serialised strategy record."""

    return _REGISTRY.get(strategy_id).to_dict()


def list_strategy_variants(strategy_id: str) -> List[Dict[str, Any]]:
    """Return persisted variants for a strategy, ensuring the default exists."""

    _REGISTRY.get(strategy_id)
    storage_ensure_default_strategy_variant(strategy_id)
    return storage_list_strategy_variants(strategy_id)


def get_strategy_variant(strategy_id: str, variant_id: str) -> Dict[str, Any]:
    """Return one strategy variant scoped to a strategy."""

    _REGISTRY.get(strategy_id)
    variant = storage_get_strategy_variant(variant_id)
    if not variant or str(variant.get("strategy_id") or "") != strategy_id:
        raise KeyError("Strategy variant not found")
    return variant


def create_strategy_variant(
    strategy_id: str,
    *,
    name: str,
    description: Optional[str] = None,
    param_overrides: Optional[Mapping[str, Any]] = None,
    atm_template_id: Optional[str] = None,
    is_default: bool = False,
) -> Dict[str, Any]:
    """Create a saved variant for a strategy."""

    record = _REGISTRY.get(strategy_id)
    if atm_template_id and not get_atm_template(atm_template_id):
        raise ValueError("ATM template not found.")
    storage_ensure_default_strategy_variant(strategy_id)
    payload = {
        "strategy_id": strategy_id,
        "name": name,
        "description": description,
        "param_overrides": dict(param_overrides or {}),
        "atm_template_id": atm_template_id,
        "is_default": is_default,
    }
    selected_variant = {
        **payload,
        "id": str(payload.get("id") or ""),
    }
    _compile_strategy_definition(
        record,
        selected_variant=selected_variant,
    )
    return storage_upsert_strategy_variant(payload)


def update_strategy_variant(
    strategy_id: str,
    variant_id: str,
    **fields: Any,
) -> Dict[str, Any]:
    """Update a saved strategy variant."""

    record = _REGISTRY.get(strategy_id)
    current = get_strategy_variant(strategy_id, variant_id)
    next_atm_template_id = fields.get("atm_template_id", current.get("atm_template_id"))
    if next_atm_template_id and not get_atm_template(next_atm_template_id):
        raise ValueError("ATM template not found.")
    payload = {
        "id": variant_id,
        "strategy_id": strategy_id,
        "name": fields.get("name", current.get("name")),
        "description": fields.get("description", current.get("description")),
        "param_overrides": fields.get("param_overrides", current.get("param_overrides") or {}),
        "atm_template_id": next_atm_template_id,
        "is_default": fields.get("is_default", current.get("is_default", False)),
    }
    _compile_strategy_definition(
        record,
        selected_variant=payload,
    )
    return storage_upsert_strategy_variant(payload)


def delete_strategy_variant(strategy_id: str, variant_id: str) -> None:
    """Delete a saved non-default strategy variant."""

    get_strategy_variant(strategy_id, variant_id)
    storage_delete_strategy_variant(variant_id)


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
    risk_config: Optional[Mapping[str, Any]] = None,
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
        risk_config=risk_config,
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
    intent: str,
    trigger: Mapping[str, Any],
    guards: Optional[Sequence[Mapping[str, Any]]] = None,
    priority: int = 0,
    description: Optional[str] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Create a strategy rule."""

    return _REGISTRY.add_rule(
        strategy_id,
        name=name,
        intent=intent,
        trigger=trigger,
        guards=guards,
        priority=priority,
        description=description,
        enabled=enabled,
    )


def update_rule(strategy_id: str, rule_id: str, **fields: Any) -> Dict[str, Any]:
    """Update an existing rule."""

    return _REGISTRY.update_rule(strategy_id, rule_id, **fields)


def delete_rule(strategy_id: str, rule_id: str) -> Dict[str, Any]:
    """Remove a rule from a strategy."""

    return _REGISTRY.remove_rule(strategy_id, rule_id)


def run_strategy_preview(
    strategy_id: str,
    *,
    start: str,
    end: str,
    interval: str,
    instrument_ids: Optional[List[str]] = None,
    variant_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Run a strategy preview for the requested window."""

    return _REGISTRY.evaluate(
        strategy_id,
        start=start,
        end=end,
        interval=interval,
        instrument_ids=instrument_ids,
        variant_id=variant_id,
    )


def compile_strategy_contract(
    strategy_id: str,
    *,
    variant_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate and compile a strategy using the selected or default variant."""

    record = _REGISTRY.get(strategy_id)
    compiled_strategy, selected_variant, resolved_params = _compile_strategy_definition(
        record,
        variant_id=variant_id,
    )
    return _serialize_compiled_strategy_payload(
        record,
        compiled_strategy=compiled_strategy,
        selected_variant=selected_variant,
        resolved_params=resolved_params,
    )


def get_strategy_preview_signal_detail(
    strategy_id: str,
    preview_id: str,
    signal_id: str,
) -> Dict[str, Any]:
    """Return one retained strategy preview signal with audit context."""

    _REGISTRY.get(strategy_id)
    return _PREVIEW_RESULTS.get_signal_detail(strategy_id, preview_id, signal_id)


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

"""Bounded deterministic strategy graph and canonical action-intent compiler.

The graph is data, not code.  It has no callable, import, file, provider,
network, shell, credential, deployment, or order-submission capability.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence


TYPED_STRATEGY_GRAPH_VERSION = "typed_strategy_graph.v1"
COMPILED_TYPED_STRATEGY_VERSION = "compiled_typed_strategy.v1"
CANONICAL_ACTION_INTENT_VERSION = "canonical_action_intent.v1"

_FACT_PREFIXES = (
    "market.",
    "indicator.",
    "regime.",
    "time.",
    "signal.current.",
    "signal.previous.",
    "position.",
    "risk.",
    "order.",
)
_FORBIDDEN_KEYS = {
    "code", "python", "callable", "module", "import", "eval", "exec",
    "file", "path", "shell", "command", "network", "url", "provider",
    "credential", "secret", "token", "deploy", "runtime_mutation",
    "external_order", "capital",
}


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload), sort_keys=True, separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()


def _required(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite")
    return parsed


def _reject_capabilities(value: Any, path: str = "graph") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).strip().lower()
            if normalized in _FORBIDDEN_KEYS:
                raise ValueError(f"typed strategy capability is forbidden: {path}.{normalized}")
            _reject_capabilities(nested, f"{path}.{normalized}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_capabilities(nested, f"{path}[{index}]")


class ValueType(str, Enum):
    BOOLEAN = "boolean"
    NUMBER = "number"
    STRING = "string"


class ActionType(str, Enum):
    ENTER = "enter"
    EXIT = "exit"
    ADD = "add"
    REDUCE = "reduce"
    REVERSE = "reverse"
    HOLD = "hold"
    CANCEL = "cancel"


class ExecutionStyle(str, Enum):
    MARKET = "market"
    AGGRESSIVE_LIMIT = "aggressive_limit"
    PASSIVE_LIMIT = "passive_limit"
    STOP = "stop"
    STAGED = "staged"


_ORDER_TYPE_BY_STYLE = {
    ExecutionStyle.MARKET: "market",
    ExecutionStyle.AGGRESSIVE_LIMIT: "limit_aggressive",
    ExecutionStyle.PASSIVE_LIMIT: "limit_resting",
    ExecutionStyle.STOP: "stop_market",
    ExecutionStyle.STAGED: "staged",
}


@dataclass(frozen=True)
class FactDeclaration:
    name: str
    value_type: ValueType | str

    def __post_init__(self) -> None:
        name = _required(self.name, "fact.name")
        if not name.startswith(_FACT_PREFIXES):
            raise ValueError(f"fact namespace is not approved: {name}")
        object.__setattr__(self, "name", name)
        value_type = (
            self.value_type
            if isinstance(self.value_type, ValueType)
            else ValueType(str(self.value_type).lower())
        )
        object.__setattr__(self, "value_type", value_type)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "value_type": self.value_type.value}


@dataclass(frozen=True)
class SizingPolicy:
    mode: str
    value: float

    def __post_init__(self) -> None:
        mode = str(self.mode or "").strip().lower()
        if mode not in {"fixed_quantity", "position_fraction", "risk_budget_fraction"}:
            raise ValueError(f"unsupported sizing mode: {mode}")
        value = _finite(self.value, "sizing.value")
        if value <= 0.0 or (mode != "fixed_quantity" and value > 1.0):
            raise ValueError("sizing value is outside its bounded range")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "value", value)

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "value": self.value}


@dataclass(frozen=True)
class RiskConstraints:
    max_position_notional: float
    max_risk_fraction: float
    allow_short: bool

    def __post_init__(self) -> None:
        notional = _finite(self.max_position_notional, "risk.max_position_notional")
        fraction = _finite(self.max_risk_fraction, "risk.max_risk_fraction")
        if notional <= 0.0 or fraction <= 0.0 or fraction > 1.0:
            raise ValueError("risk constraints are outside bounded ranges")
        if not isinstance(self.allow_short, bool):
            raise ValueError("risk.allow_short must be boolean")
        object.__setattr__(self, "max_position_notional", notional)
        object.__setattr__(self, "max_risk_fraction", fraction)

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_position_notional": self.max_position_notional,
            "max_risk_fraction": self.max_risk_fraction,
            "allow_short": self.allow_short,
        }


@dataclass(frozen=True)
class ExecutionPolicy:
    style: ExecutionStyle | str
    time_in_force: str
    expiration_bars: int
    price_offset_bps: float
    chase_limit: int
    stage_count: int

    def __post_init__(self) -> None:
        style = self.style if isinstance(self.style, ExecutionStyle) else ExecutionStyle(str(self.style).lower())
        tif = str(self.time_in_force or "").strip().lower()
        if tif not in {"gtc", "ioc", "fok", "bar"}:
            raise ValueError(f"unsupported time in force: {tif}")
        expiration = int(self.expiration_bars)
        chase = int(self.chase_limit)
        stages = int(self.stage_count)
        if expiration < 0 or chase < 0 or stages <= 0:
            raise ValueError("execution policy counts must be non-negative and stages positive")
        if style is ExecutionStyle.PASSIVE_LIMIT and tif in {"ioc", "fok"}:
            raise ValueError("passive execution cannot use immediate-only time in force")
        if style is not ExecutionStyle.STAGED and stages != 1:
            raise ValueError("stage_count greater than one requires staged execution")
        if style is ExecutionStyle.STAGED and stages < 2:
            raise ValueError("staged execution requires at least two stages")
        offset = _finite(self.price_offset_bps, "execution.price_offset_bps")
        if abs(offset) > 10_000:
            raise ValueError("execution price offset exceeds bounded range")
        object.__setattr__(self, "style", style)
        object.__setattr__(self, "time_in_force", tif)
        object.__setattr__(self, "expiration_bars", expiration)
        object.__setattr__(self, "chase_limit", chase)
        object.__setattr__(self, "stage_count", stages)
        object.__setattr__(self, "price_offset_bps", offset)

    def to_dict(self) -> dict[str, Any]:
        return {
            "style": self.style.value,
            "order_type": _ORDER_TYPE_BY_STYLE[self.style],
            "time_in_force": self.time_in_force,
            "expiration_bars": self.expiration_bars,
            "price_offset_bps": self.price_offset_bps,
            "chase_limit": self.chase_limit,
            "stage_count": self.stage_count,
        }


def _expression_type(node: Any, facts: Mapping[str, ValueType], path: str) -> ValueType:
    if not isinstance(node, Mapping):
        raise ValueError(f"{path} must be a typed expression object")
    allowed = {"op", "value", "value_type", "name", "args"}
    unknown = set(node) - allowed
    if unknown:
        raise ValueError(f"unsupported expression fields at {path}: {','.join(sorted(unknown))}")
    op = str(node.get("op") or "").strip().lower()
    if op == "const":
        declared = ValueType(str(node.get("value_type") or "").lower())
        value = node.get("value")
        actual = (
            ValueType.BOOLEAN if isinstance(value, bool)
            else ValueType.NUMBER if isinstance(value, (int, float)) and not isinstance(value, bool)
            else ValueType.STRING if isinstance(value, str)
            else None
        )
        if actual is not declared or (actual is ValueType.NUMBER and not math.isfinite(float(value))):
            raise ValueError(f"constant type mismatch at {path}")
        return declared
    if op == "fact":
        name = _required(node.get("name"), f"{path}.name")
        if name not in facts:
            raise ValueError(f"undeclared fact reference at {path}: {name}")
        return facts[name]
    args = node.get("args")
    if not isinstance(args, list):
        raise ValueError(f"{path}.args must be a list")
    arg_types = [_expression_type(value, facts, f"{path}.args[{index}]") for index, value in enumerate(args)]
    if op in {"all", "any"}:
        if len(arg_types) < 2 or any(value is not ValueType.BOOLEAN for value in arg_types):
            raise ValueError(f"{op} requires at least two boolean arguments")
        return ValueType.BOOLEAN
    if op == "not":
        if arg_types != [ValueType.BOOLEAN]:
            raise ValueError("not requires one boolean argument")
        return ValueType.BOOLEAN
    if op in {"add", "sub", "mul", "div", "min", "max"}:
        if len(arg_types) != 2 or any(value is not ValueType.NUMBER for value in arg_types):
            raise ValueError(f"{op} requires two numeric arguments")
        return ValueType.NUMBER
    if op in {"gt", "gte", "lt", "lte"}:
        if arg_types != [ValueType.NUMBER, ValueType.NUMBER]:
            raise ValueError(f"{op} requires two numeric arguments")
        return ValueType.BOOLEAN
    if op in {"eq", "ne"}:
        if len(arg_types) != 2 or arg_types[0] is not arg_types[1]:
            raise ValueError(f"{op} requires two arguments of the same type")
        return ValueType.BOOLEAN
    raise ValueError(f"unsupported expression operation at {path}: {op}")


def _evaluate_expression(node: Mapping[str, Any], facts: Mapping[str, Any]) -> Any:
    op = str(node["op"]).lower()
    if op == "const":
        return node["value"]
    if op == "fact":
        name = str(node["name"])
        if name not in facts:
            raise ValueError(f"runtime fact missing: {name}")
        return facts[name]
    args = [_evaluate_expression(value, facts) for value in node["args"]]
    if op == "all": return all(args)
    if op == "any": return any(args)
    if op == "not": return not args[0]
    if op == "add": return args[0] + args[1]
    if op == "sub": return args[0] - args[1]
    if op == "mul": return args[0] * args[1]
    if op == "div":
        if args[1] == 0:
            raise ValueError("typed strategy division by zero")
        return args[0] / args[1]
    if op == "min": return min(args)
    if op == "max": return max(args)
    if op == "gt": return args[0] > args[1]
    if op == "gte": return args[0] >= args[1]
    if op == "lt": return args[0] < args[1]
    if op == "lte": return args[0] <= args[1]
    if op == "eq": return args[0] == args[1]
    if op == "ne": return args[0] != args[1]
    raise RuntimeError(f"compiled expression op unsupported: {op}")


@dataclass(frozen=True)
class TypedActionRule:
    rule_id: str
    priority: int
    condition: Mapping[str, Any]
    action: ActionType | str
    side: str | None
    sizing: SizingPolicy | None
    execution: ExecutionPolicy | None

    def validate(self, facts: Mapping[str, ValueType]) -> None:
        _required(self.rule_id, "rule.rule_id")
        if isinstance(self.priority, bool):
            raise ValueError("rule.priority must be an integer")
        if _expression_type(self.condition, facts, f"rule.{self.rule_id}.condition") is not ValueType.BOOLEAN:
            raise ValueError("rule condition must evaluate to boolean")
        action = self.action if isinstance(self.action, ActionType) else ActionType(str(self.action).lower())
        object.__setattr__(self, "action", action)
        side = str(self.side or "").lower() or None
        if side not in {None, "long", "short"}:
            raise ValueError("action side must be long or short")
        object.__setattr__(self, "side", side)
        trading = action in {ActionType.ENTER, ActionType.ADD, ActionType.REVERSE}
        sized = trading or action in {ActionType.EXIT, ActionType.REDUCE}
        if trading and side is None:
            raise ValueError(f"{action.value} requires a side")
        if sized and self.sizing is None:
            raise ValueError(f"{action.value} requires sizing")
        if action not in {ActionType.HOLD, ActionType.CANCEL} and self.execution is None:
            raise ValueError(f"{action.value} requires execution policy")
        if action in {ActionType.HOLD, ActionType.CANCEL} and (self.sizing or self.execution):
            raise ValueError(f"{action.value} cannot carry sizing or execution")

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "priority": int(self.priority),
            "condition": dict(self.condition),
            "action": self.action.value,
            "side": self.side,
            "sizing": self.sizing.to_dict() if self.sizing else None,
            "execution": self.execution.to_dict() if self.execution else None,
        }


@dataclass(frozen=True)
class TypedStrategyGraph:
    schema_version: str
    graph_id: str
    family_id: str
    protocol_hash: str
    timeframe: str
    facts: tuple[FactDeclaration, ...]
    rules: tuple[TypedActionRule, ...]
    risk: RiskConstraints
    parent_graph_ids: tuple[str, ...]
    created_by: str
    graph_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != TYPED_STRATEGY_GRAPH_VERSION:
            raise ValueError(f"unsupported typed strategy graph schema: {self.schema_version}")
        for name in ("graph_id", "family_id", "protocol_hash", "timeframe", "created_by"):
            object.__setattr__(self, name, _required(getattr(self, name), f"graph.{name}"))
        facts = tuple(self.facts)
        if not facts or len({row.name for row in facts}) != len(facts):
            raise ValueError("graph facts must be non-empty and unique")
        fact_types = {row.name: row.value_type for row in facts}
        rules = tuple(self.rules)
        if not rules or len({row.rule_id for row in rules}) != len(rules):
            raise ValueError("graph rules must be non-empty and unique")
        for rule in rules:
            rule.validate(fact_types)
            if rule.side == "short" and not self.risk.allow_short:
                raise ValueError("short action conflicts with risk.allow_short=false")
        parents = tuple(sorted({_required(row, "graph.parent_graph_id") for row in self.parent_graph_ids}))
        if self.graph_id in parents:
            raise ValueError("graph cannot parent itself")
        object.__setattr__(self, "facts", tuple(sorted(facts, key=lambda row: row.name)))
        object.__setattr__(self, "rules", tuple(sorted(rules, key=lambda row: row.rule_id)))
        object.__setattr__(self, "parent_graph_ids", parents)
        expected = _stable_hash(self._material())
        if self.graph_hash and self.graph_hash != expected:
            raise ValueError("typed_strategy_graph_hash_mismatch")
        object.__setattr__(self, "graph_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "graph_id": self.graph_id,
            "family_id": self.family_id,
            "protocol_hash": self.protocol_hash,
            "timeframe": self.timeframe,
            "facts": [row.to_dict() for row in self.facts],
            "rules": [row.to_dict() for row in self.rules],
            "risk": self.risk.to_dict(),
            "parent_graph_ids": list(self.parent_graph_ids),
            "created_by": self.created_by,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "graph_hash": self.graph_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "TypedStrategyGraph":
        _reject_capabilities(raw)
        allowed = {
            "schema_version", "graph_id", "family_id", "protocol_hash", "timeframe",
            "facts", "rules", "risk", "parent_graph_ids", "created_by", "graph_hash",
        }
        unknown = set(raw) - allowed
        if unknown:
            raise ValueError("unsupported typed graph fields: " + ",".join(sorted(unknown)))
        rules = []
        for row in raw.get("rules") or ():
            sizing_raw = row.get("sizing")
            execution_raw = row.get("execution")
            rules.append(
                TypedActionRule(
                    rule_id=str(row.get("rule_id") or ""),
                    priority=int(row.get("priority") or 0),
                    condition=dict(row.get("condition") or {}),
                    action=str(row.get("action") or ""),
                    side=row.get("side"),
                    sizing=SizingPolicy(**sizing_raw) if isinstance(sizing_raw, Mapping) else None,
                    execution=(
                        ExecutionPolicy(
                            style=execution_raw.get("style"),
                            time_in_force=execution_raw.get("time_in_force", "gtc"),
                            expiration_bars=execution_raw.get("expiration_bars", 0),
                            price_offset_bps=execution_raw.get("price_offset_bps", 0.0),
                            chase_limit=execution_raw.get("chase_limit", 0),
                            stage_count=execution_raw.get("stage_count", 1),
                        )
                        if isinstance(execution_raw, Mapping)
                        else None
                    ),
                )
            )
        risk_raw = dict(raw.get("risk") or {})
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            graph_id=str(raw.get("graph_id") or ""),
            family_id=str(raw.get("family_id") or ""),
            protocol_hash=str(raw.get("protocol_hash") or ""),
            timeframe=str(raw.get("timeframe") or ""),
            facts=tuple(FactDeclaration(**row) for row in raw.get("facts") or ()),
            rules=tuple(rules),
            risk=RiskConstraints(**risk_raw),
            parent_graph_ids=tuple(raw.get("parent_graph_ids") or ()),
            created_by=str(raw.get("created_by") or ""),
            graph_hash=str(raw.get("graph_hash") or ""),
        )


@dataclass(frozen=True)
class CanonicalActionIntent:
    schema_version: str
    graph_id: str
    graph_hash: str
    rule_id: str
    action: str
    side: str | None
    sizing: Mapping[str, Any] | None
    execution_policy: Mapping[str, Any] | None
    risk_constraints: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "graph_id": self.graph_id,
            "graph_hash": self.graph_hash,
            "rule_id": self.rule_id,
            "action": self.action,
            "side": self.side,
            "sizing": dict(self.sizing) if self.sizing else None,
            "execution_policy": dict(self.execution_policy) if self.execution_policy else None,
            "risk_constraints": dict(self.risk_constraints),
        }


@dataclass(frozen=True)
class CompiledTypedStrategy:
    schema_version: str
    graph: TypedStrategyGraph
    compiled_hash: str

    def evaluate(self, facts: Mapping[str, Any]) -> tuple[CanonicalActionIntent | None, dict[str, Any]]:
        declared = {row.name: row.value_type for row in self.graph.facts}
        unknown = sorted(set(facts) - set(declared))
        missing = sorted(set(declared) - set(facts))
        if unknown or missing:
            raise ValueError(f"typed strategy fact set mismatch unknown={unknown} missing={missing}")
        for name, value_type in declared.items():
            actual = (
                ValueType.BOOLEAN if isinstance(facts[name], bool)
                else ValueType.NUMBER if isinstance(facts[name], (int, float)) and not isinstance(facts[name], bool)
                else ValueType.STRING if isinstance(facts[name], str)
                else None
            )
            if actual is not value_type:
                raise ValueError(f"runtime fact type mismatch: {name}")
        evaluated = [
            (rule, bool(_evaluate_expression(rule.condition, facts)))
            for rule in self.graph.rules
        ]
        matched = [row for row in evaluated if row[1]]
        selected = sorted(matched, key=lambda row: (-row[0].priority, row[0].rule_id))[0][0] if matched else None
        trace = {
            "schema_version": "typed_strategy_decision_trace.v1",
            "graph_hash": self.graph.graph_hash,
            "evaluations": [
                {
                    "rule_id": rule.rule_id,
                    "matched": matched_value,
                    "selected": selected is rule,
                }
                for rule, matched_value in evaluated
            ],
        }
        if selected is None:
            return None, trace
        intent = CanonicalActionIntent(
            schema_version=CANONICAL_ACTION_INTENT_VERSION,
            graph_id=self.graph.graph_id,
            graph_hash=self.graph.graph_hash,
            rule_id=selected.rule_id,
            action=selected.action.value,
            side=selected.side,
            sizing=selected.sizing.to_dict() if selected.sizing else None,
            execution_policy=selected.execution.to_dict() if selected.execution else None,
            risk_constraints=self.graph.risk.to_dict(),
        )
        return intent, trace


def compile_typed_strategy_graph(graph: TypedStrategyGraph) -> CompiledTypedStrategy:
    material = {
        "schema_version": COMPILED_TYPED_STRATEGY_VERSION,
        "graph_hash": graph.graph_hash,
        "canonical_action_intent_version": CANONICAL_ACTION_INTENT_VERSION,
    }
    return CompiledTypedStrategy(
        schema_version=COMPILED_TYPED_STRATEGY_VERSION,
        graph=graph,
        compiled_hash=_stable_hash(material),
    )


__all__ = [
    "ActionType", "CANONICAL_ACTION_INTENT_VERSION", "CanonicalActionIntent",
    "CompiledTypedStrategy", "ExecutionPolicy", "ExecutionStyle", "FactDeclaration",
    "RiskConstraints", "SizingPolicy", "TYPED_STRATEGY_GRAPH_VERSION",
    "TypedActionRule", "TypedStrategyGraph", "ValueType", "compile_typed_strategy_graph",
]

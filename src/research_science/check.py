"""Pure contracts for versioned, replayable research Check execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

from market_data.frozen import normalize_frozen_market_data_read_binding

from .study import stable_hash


CHECK_DEFINITION_SCHEMA_VERSION = "research.check_definition.v1"
CHECK_REQUEST_SCHEMA_VERSION = "research.check_request.v2"
CHECK_PLAN_SCHEMA_VERSION = "research.check_plan.v1"
CHECK_EVIDENCE_BINDING_SCHEMA_VERSION = "research.check_evidence_binding.v1"
CHECK_RESULT_SCHEMA_VERSION = "research.check_result.v2"
CHECK_REPLAY_SCHEMA_VERSION = "research.check_replay.v1"

CHECK_MODE_PREVIEW = "preview"
CHECK_MODE_EVIDENCE = "evidence"
CHECK_MODES = frozenset({CHECK_MODE_PREVIEW, CHECK_MODE_EVIDENCE})

GAP_POLICY_REJECT = "reject"
GAP_POLICY_RESET_REWARM = "reset_rewarm"
GAP_POLICY_CONTINUE_DEGRADED = "continue_degraded"
GAP_POLICIES = frozenset(
    {GAP_POLICY_REJECT, GAP_POLICY_RESET_REWARM, GAP_POLICY_CONTINUE_DEGRADED}
)

ASSERTION_OPERATORS = frozenset({"eq", "ne", "gt", "gte", "lt", "lte"})
_NON_SEMANTIC_RESULT_FIELDS = frozenset(
    {
        "check_id",
        "created_at",
        "completed_at",
        "duration_ms",
        "generated_at",
        "perf",
        "replay_id",
    }
)


def _required(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _mapping(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return dict(value)


def _semantic_result(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _semantic_result(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _NON_SEMANTIC_RESULT_FIELDS
        }
    if isinstance(value, (list, tuple)):
        return [_semantic_result(item) for item in value]
    return value


@dataclass(frozen=True)
class CheckDefinition:
    schema_version: str
    definition_id: str
    definition_version: str
    evaluator_id: str
    evaluator_version: str
    request_schema_version: str
    result_schema_version: str
    material_rules: Mapping[str, Any]
    definition_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != CHECK_DEFINITION_SCHEMA_VERSION:
            raise ValueError("unsupported Check definition schema")
        for field in (
            "definition_id",
            "definition_version",
            "evaluator_id",
            "evaluator_version",
            "request_schema_version",
            "result_schema_version",
        ):
            object.__setattr__(
                self,
                field,
                _required(getattr(self, field), field=f"check_definition.{field}"),
            )
        object.__setattr__(
            self,
            "material_rules",
            _mapping(self.material_rules, field="check_definition.material_rules"),
        )
        expected = stable_hash(self._material())
        if self.definition_hash and self.definition_hash != expected:
            raise ValueError("check_definition_hash_mismatch")
        object.__setattr__(self, "definition_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "definition_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "definition_hash": self.definition_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CheckDefinition:
        return cls(**dict(raw))


@dataclass(frozen=True)
class CheckRequest:
    schema_version: str
    mode: str
    definition_id: str
    definition_version: str
    definition_hash: str
    scope: Mapping[str, Any]
    parameters: Mapping[str, Any]
    dataset_id: str | None = None
    immutable_run_evidence: Mapping[str, Any] | None = None
    request_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != CHECK_REQUEST_SCHEMA_VERSION:
            raise ValueError("unsupported Check request schema")
        mode = str(self.mode or "").strip().lower()
        if mode not in CHECK_MODES:
            raise ValueError("check_request.mode must be preview or evidence")
        object.__setattr__(self, "mode", mode)
        for field in ("definition_id", "definition_version", "definition_hash"):
            object.__setattr__(
                self,
                field,
                _required(getattr(self, field), field=f"check_request.{field}"),
            )
        object.__setattr__(self, "scope", _mapping(self.scope, field="check_request.scope"))
        object.__setattr__(
            self,
            "parameters",
            _mapping(self.parameters, field="check_request.parameters"),
        )
        dataset_id = str(self.dataset_id or "").strip() or None
        object.__setattr__(self, "dataset_id", dataset_id)
        run_evidence = self.immutable_run_evidence
        if run_evidence is not None:
            run_evidence = _mapping(
                run_evidence, field="check_request.immutable_run_evidence"
            )
            object.__setattr__(self, "immutable_run_evidence", run_evidence)
        if mode == CHECK_MODE_EVIDENCE and bool(dataset_id) == bool(run_evidence):
            raise ValueError(
                "check_evidence_input_required: provide exactly one frozen Dataset or immutable run evidence"
            )
        expected = stable_hash(self._material())
        if self.request_hash and self.request_hash != expected:
            raise ValueError("check_request_hash_mismatch")
        object.__setattr__(self, "request_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "request_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "request_hash": self.request_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CheckRequest:
        return cls(**dict(raw))


@dataclass(frozen=True)
class ResolvedCheckPlan:
    schema_version: str
    request_hash: str
    market_data_requirements: tuple[Mapping[str, Any], ...]
    indicator_graph: tuple[Mapping[str, Any], ...]
    evaluation_range: Mapping[str, Any]
    materialization_range: Mapping[str, Any]
    warmup: Mapping[str, Any]
    outcome_tail: Mapping[str, Any]
    gap_policy: str
    execution: Mapping[str, Any] = field(default_factory=dict)
    missing_coverage: tuple[Mapping[str, Any], ...] = ()
    quality_evidence: tuple[Mapping[str, Any], ...] = ()
    plan_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != CHECK_PLAN_SCHEMA_VERSION:
            raise ValueError("unsupported Check plan schema")
        object.__setattr__(
            self, "request_hash", _required(self.request_hash, field="check_plan.request_hash")
        )
        aliases: set[str] = set()
        requirements: list[dict[str, Any]] = []
        for raw in self.market_data_requirements:
            requirement = _mapping(raw, field="check_plan.market_data_requirement")
            alias = _required(requirement.get("alias"), field="check_plan.requirement.alias")
            if alias in aliases:
                raise ValueError(f"check_plan_duplicate_alias: {alias}")
            aliases.add(alias)
            requirements.append(requirement)
        object.__setattr__(self, "market_data_requirements", tuple(requirements))
        object.__setattr__(
            self,
            "indicator_graph",
            tuple(
                _mapping(item, field="check_plan.indicator_graph")
                for item in self.indicator_graph
            ),
        )
        for field in ("evaluation_range", "materialization_range", "warmup", "outcome_tail"):
            object.__setattr__(
                self,
                field,
                _mapping(getattr(self, field), field=f"check_plan.{field}"),
            )
        object.__setattr__(
            self,
            "execution",
            _mapping(self.execution, field="check_plan.execution"),
        )
        gap_policy = str(self.gap_policy or "").strip().lower()
        if gap_policy not in GAP_POLICIES:
            raise ValueError("check_plan.gap_policy is invalid")
        object.__setattr__(self, "gap_policy", gap_policy)
        object.__setattr__(
            self,
            "missing_coverage",
            tuple(
                _mapping(item, field="check_plan.missing_coverage")
                for item in self.missing_coverage
            ),
        )
        object.__setattr__(
            self,
            "quality_evidence",
            tuple(
                _mapping(item, field="check_plan.quality_evidence")
                for item in self.quality_evidence
            ),
        )
        expected = stable_hash(self._material())
        if self.plan_hash and self.plan_hash != expected:
            raise ValueError("check_plan_hash_mismatch")
        object.__setattr__(self, "plan_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "plan_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "plan_hash": self.plan_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ResolvedCheckPlan:
        values = dict(raw)
        for field in (
            "market_data_requirements",
            "indicator_graph",
            "missing_coverage",
            "quality_evidence",
        ):
            values[field] = tuple(values.get(field) or ())
        return cls(**values)


@dataclass(frozen=True)
class CheckEvidenceBinding:
    schema_version: str
    definition_hash: str
    request_hash: str
    plan_hash: str
    code_revision: str
    evidence_kind: str
    input_binding: Mapping[str, Any]
    indicator_graph_hash: str
    indicator_output_hash: str
    fact_input_hash: str
    gap_transition_hash: str
    quality_hash: str
    gaps_hash: str
    input_hash: str = ""
    evidence_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != CHECK_EVIDENCE_BINDING_SCHEMA_VERSION:
            raise ValueError("unsupported Check evidence-binding schema")
        for field in (
            "definition_hash",
            "request_hash",
            "plan_hash",
            "code_revision",
            "indicator_graph_hash",
            "indicator_output_hash",
            "fact_input_hash",
            "gap_transition_hash",
            "quality_hash",
            "gaps_hash",
        ):
            object.__setattr__(
                self,
                field,
                _required(getattr(self, field), field=f"check_evidence.{field}"),
            )
        evidence_kind = str(self.evidence_kind or "").strip().lower()
        if evidence_kind not in {"frozen_market_data", "immutable_run_evidence"}:
            raise ValueError("check_evidence.evidence_kind is invalid")
        object.__setattr__(self, "evidence_kind", evidence_kind)
        input_binding = _mapping(
            self.input_binding, field="check_evidence.input_binding"
        )
        if evidence_kind == "frozen_market_data":
            input_binding = normalize_frozen_market_data_read_binding(input_binding)
        object.__setattr__(self, "input_binding", input_binding)
        expected_input_hash = stable_hash(input_binding)
        if self.input_hash and self.input_hash != expected_input_hash:
            raise ValueError("check_evidence_input_hash_mismatch")
        object.__setattr__(self, "input_hash", expected_input_hash)
        expected_evidence_hash = stable_hash(self._material())
        if self.evidence_hash and self.evidence_hash != expected_evidence_hash:
            raise ValueError("check_evidence_hash_mismatch")
        object.__setattr__(self, "evidence_hash", expected_evidence_hash)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "evidence_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "evidence_hash": self.evidence_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CheckEvidenceBinding:
        return cls(**dict(raw))


@dataclass(frozen=True)
class CheckResult:
    schema_version: str
    definition_hash: str
    request_hash: str
    plan_hash: str
    evidence_hash: str
    evaluator_id: str
    evaluator_version: str
    result: Mapping[str, Any]
    result_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != CHECK_RESULT_SCHEMA_VERSION:
            raise ValueError("unsupported Check result schema")
        for field in (
            "definition_hash",
            "request_hash",
            "plan_hash",
            "evidence_hash",
            "evaluator_id",
            "evaluator_version",
        ):
            object.__setattr__(
                self,
                field,
                _required(getattr(self, field), field=f"check_result.{field}"),
            )
        semantic = _semantic_result(_mapping(self.result, field="check_result.result"))
        object.__setattr__(self, "result", semantic)
        expected = stable_hash(self._material())
        if self.result_hash and self.result_hash != expected:
            raise ValueError("check_result_hash_mismatch")
        object.__setattr__(self, "result_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "result_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "result_hash": self.result_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CheckResult:
        return cls(**dict(raw))


class CheckEvaluator(Protocol):
    evaluator_id: str
    version: str

    def declare_requirements(
        self,
        *,
        definition: CheckDefinition,
        request: CheckRequest,
    ) -> Mapping[str, Any]:
        ...

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        ...


class CheckRegistry:
    """Exact-version registry tying Check definitions to their owning evaluator."""

    def __init__(self) -> None:
        self._definitions: dict[tuple[str, str], CheckDefinition] = {}
        self._evaluators: dict[tuple[str, str], CheckEvaluator] = {}

    def register_evaluator(self, evaluator: CheckEvaluator) -> None:
        identity = (
            _required(getattr(evaluator, "evaluator_id", None), field="evaluator.id"),
            _required(getattr(evaluator, "version", None), field="evaluator.version"),
        )
        if identity in self._evaluators:
            raise ValueError(
                f"check_evaluator_duplicate: id={identity[0]} version={identity[1]}"
            )
        self._evaluators[identity] = evaluator

    def register_definition(self, definition: CheckDefinition) -> None:
        identity = (definition.definition_id, definition.definition_version)
        if identity in self._definitions:
            raise ValueError(
                f"check_definition_duplicate: id={identity[0]} version={identity[1]}"
            )
        self._definitions[identity] = definition

    def resolve_definition(self, definition_id: str, version: str) -> CheckDefinition:
        identity = (str(definition_id), str(version))
        if identity not in self._definitions:
            raise ValueError(
                f"check_definition_unavailable: id={identity[0]} version={identity[1]}"
            )
        return self._definitions[identity]

    def resolve_evaluator(self, definition: CheckDefinition) -> CheckEvaluator:
        identity = (definition.evaluator_id, definition.evaluator_version)
        if identity not in self._evaluators:
            raise ValueError(
                f"check_evaluator_unavailable: id={identity[0]} version={identity[1]}"
            )
        return self._evaluators[identity]

    def resolve(self, definition_id: str, version: str) -> tuple[CheckDefinition, CheckEvaluator]:
        definition = self.resolve_definition(definition_id, version)
        return definition, self.resolve_evaluator(definition)


@dataclass(frozen=True)
class ScalarAssertionSpec:
    metric_path: str
    operator: str
    threshold: float | int | str | bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "metric_path",
            _required(self.metric_path, field="assertion.metric_path"),
        )
        operator = str(self.operator or "").strip().lower()
        if operator not in ASSERTION_OPERATORS:
            raise ValueError("assertion.operator is unsupported")
        object.__setattr__(self, "operator", operator)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _metric_value(payload: Mapping[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return False, None
        current = current[part]
    return True, current


def evaluate_scalar_assertions(
    result: Mapping[str, Any], assertions: Sequence[ScalarAssertionSpec]
) -> dict[str, Any]:
    """Evaluate fixed scalar assertions without granting promotion authority."""

    if not assertions:
        return {"assertions": [], "verdict": None}
    evaluations: list[dict[str, Any]] = []
    for assertion in assertions:
        resolved, actual = _metric_value(result, assertion.metric_path)
        status = "indeterminate"
        if resolved and actual is not None:
            try:
                status = (
                    "passed"
                    if {
                        "eq": actual == assertion.threshold,
                        "ne": actual != assertion.threshold,
                        "gt": actual > assertion.threshold,
                        "gte": actual >= assertion.threshold,
                        "lt": actual < assertion.threshold,
                        "lte": actual <= assertion.threshold,
                    }[assertion.operator]
                    else "failed"
                )
            except TypeError:
                status = "indeterminate"
        evaluations.append(
            {
                **assertion.to_dict(),
                "actual": actual,
                "status": status,
            }
        )
    statuses = {row["status"] for row in evaluations}
    if "indeterminate" in statuses:
        verdict = "indeterminate"
    elif "failed" in statuses:
        verdict = "failed"
    else:
        verdict = "passed"
    return {"assertions": evaluations, "verdict": verdict}


def verify_check_replay(
    original: CheckResult,
    replayed: CheckResult,
) -> dict[str, Any]:
    fields = (
        "definition_hash",
        "request_hash",
        "plan_hash",
        "evidence_hash",
        "evaluator_id",
        "evaluator_version",
        "result_hash",
    )
    mismatches = [
        {
            "field": field,
            "original": getattr(original, field),
            "replayed": getattr(replayed, field),
        }
        for field in fields
        if getattr(original, field) != getattr(replayed, field)
    ]
    return {
        "schema_version": CHECK_REPLAY_SCHEMA_VERSION,
        "matches": not mismatches,
        "original_result_hash": original.result_hash,
        "replayed_result_hash": replayed.result_hash,
        "mismatches": mismatches,
        "provider_call_performed": False,
    }


__all__ = [
    "ASSERTION_OPERATORS",
    "CHECK_DEFINITION_SCHEMA_VERSION",
    "CHECK_EVIDENCE_BINDING_SCHEMA_VERSION",
    "CHECK_MODE_EVIDENCE",
    "CHECK_MODE_PREVIEW",
    "CHECK_MODES",
    "CHECK_PLAN_SCHEMA_VERSION",
    "CHECK_REPLAY_SCHEMA_VERSION",
    "CHECK_REQUEST_SCHEMA_VERSION",
    "CHECK_RESULT_SCHEMA_VERSION",
    "GAP_POLICIES",
    "GAP_POLICY_CONTINUE_DEGRADED",
    "GAP_POLICY_REJECT",
    "GAP_POLICY_RESET_REWARM",
    "CheckDefinition",
    "CheckEvidenceBinding",
    "CheckEvaluator",
    "CheckRegistry",
    "CheckRequest",
    "CheckResult",
    "ResolvedCheckPlan",
    "ScalarAssertionSpec",
    "evaluate_scalar_assertions",
    "verify_check_replay",
]

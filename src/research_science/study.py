"""Capability-native contracts for deterministic offline research.

These contracts describe what a study needs without importing one strategy
family, market-fact combination, provider, or execution surface.  They are
pure immutable data contracts; callers supply frozen facts and registered
implementation bundles.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from .contracts import DatasetRole, ScientificProtocol

RESEARCH_BRIEF_SCHEMA_VERSION = "research.brief.v1"
STUDY_DEFINITION_SCHEMA_VERSION = "research.study_definition.v1"
RESEARCH_RUN_SCHEMA_VERSION = "research.run.v1"


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _required(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _string_set(values: Iterable[Any], *, field: str) -> tuple[str, ...]:
    return tuple(sorted({_required(value, field=field) for value in values}))


def _version_pairs(value: Mapping[str, Any], *, field: str) -> tuple[tuple[str, str], ...]:
    pairs = tuple(
        sorted(
            (
                _required(key, field=f"{field}.name"),
                _required(version, field=f"{field}.{key}"),
            )
            for key, version in dict(value or {}).items()
        )
    )
    if not pairs:
        raise ValueError(f"{field} must not be empty")
    return pairs


@dataclass(frozen=True)
class ResearchBrief:
    schema_version: str
    brief_id: str
    objective: str
    economic_claim: str
    economic_claim_intent: str
    requested_by: str
    brief_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != RESEARCH_BRIEF_SCHEMA_VERSION:
            raise ValueError("unsupported research brief schema")
        for name in ("brief_id", "objective", "economic_claim", "requested_by"):
            object.__setattr__(
                self,
                name,
                _required(getattr(self, name), field=f"brief.{name}"),
            )
        intent = str(self.economic_claim_intent or "").strip().lower()
        if intent not in {"exploration", "economic", "selection", "promotion"}:
            raise ValueError("brief.economic_claim_intent is invalid")
        object.__setattr__(self, "economic_claim_intent", intent)
        expected = stable_hash(self._material())
        if self.brief_hash and self.brief_hash != expected:
            raise ValueError("research_brief_hash_mismatch")
        object.__setattr__(self, "brief_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "brief_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "brief_hash": self.brief_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ResearchBrief:
        return cls(**dict(raw))


@dataclass(frozen=True)
class FactRequirement:
    """One named frozen fact dependency declared by a study."""

    fact_key: str
    fact_type: str
    role: str
    required: bool = True
    timeframe_seconds: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fact_key",
            _required(self.fact_key, field="fact_requirement.fact_key"),
        )
        object.__setattr__(
            self,
            "fact_type",
            _required(self.fact_type, field="fact_requirement.fact_type"),
        )
        role = str(self.role or "").strip().lower()
        if role not in {"primary", "context", "evidence"}:
            raise ValueError("fact_requirement.role is invalid")
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "required", bool(self.required))
        if self.timeframe_seconds is not None:
            timeframe = int(self.timeframe_seconds)
            if timeframe <= 0:
                raise ValueError("fact_requirement.timeframe_seconds must be positive")
            object.__setattr__(self, "timeframe_seconds", timeframe)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> FactRequirement:
        return cls(**dict(raw))


@dataclass(frozen=True)
class AvailabilityTransform:
    """Pinned derivation that establishes a fact's research decision clock."""

    transform_id: str
    transform_version: str
    output_fact_key: str
    input_fact_keys: tuple[str, ...]
    parameters: Mapping[str, Any]
    transform_hash: str = ""

    def __post_init__(self) -> None:
        for name in ("transform_id", "transform_version", "output_fact_key"):
            object.__setattr__(
                self,
                name,
                _required(getattr(self, name), field=f"availability.{name}"),
            )
        inputs = _string_set(
            self.input_fact_keys,
            field="availability.input_fact_key",
        )
        if not inputs:
            raise ValueError("availability transform requires input facts")
        object.__setattr__(self, "input_fact_keys", inputs)
        object.__setattr__(self, "parameters", dict(self.parameters or {}))
        expected = stable_hash(self._material())
        if self.transform_hash and self.transform_hash != expected:
            raise ValueError("availability_transform_hash_mismatch")
        object.__setattr__(self, "transform_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "transform_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "transform_hash": self.transform_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> AvailabilityTransform:
        values = dict(raw)
        values["input_fact_keys"] = tuple(raw.get("input_fact_keys") or ())
        return cls(**values)


@dataclass(frozen=True)
class TemporalJoinSpec:
    """One deterministic contextual fact join against a decision clock."""

    left_fact_key: str
    right_fact_key: str
    output_key: str
    join_type: str = "as_of"
    decision_clock: str = "decision_time"
    require_known_at_lte_decision: bool = True
    require_sample_time_lte_decision: bool = True
    missing_policy: str = "reject_frame"
    join_hash: str = ""

    def __post_init__(self) -> None:
        for name in ("left_fact_key", "right_fact_key", "output_key"):
            object.__setattr__(
                self,
                name,
                _required(getattr(self, name), field=f"temporal_join.{name}"),
            )
        if str(self.join_type or "").strip().lower() != "as_of":
            raise ValueError("temporal_join.join_type is unsupported")
        object.__setattr__(self, "join_type", "as_of")
        if str(self.decision_clock or "").strip().lower() != "decision_time":
            raise ValueError("temporal_join.decision_clock is unsupported")
        object.__setattr__(self, "decision_clock", "decision_time")
        if self.require_known_at_lte_decision is not True:
            raise ValueError("temporal join must enforce known_at causality")
        if self.require_sample_time_lte_decision is not True:
            raise ValueError("temporal join must enforce sample-time causality")
        missing = str(self.missing_policy or "").strip().lower()
        if missing not in {"reject_frame", "exclude_frame", "null"}:
            raise ValueError("temporal_join.missing_policy is invalid")
        object.__setattr__(self, "missing_policy", missing)
        expected = stable_hash(self._material())
        if self.join_hash and self.join_hash != expected:
            raise ValueError("temporal_join_hash_mismatch")
        object.__setattr__(self, "join_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "join_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "join_hash": self.join_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> TemporalJoinSpec:
        return cls(**dict(raw))


@dataclass(frozen=True)
class StudyDefinition:
    schema_version: str
    study_id: str
    brief: ResearchBrief
    instrument_ids: tuple[str, ...]
    fact_requirements: tuple[FactRequirement, ...]
    availability_transforms: tuple[AvailabilityTransform, ...]
    temporal_joins: tuple[TemporalJoinSpec, ...]
    feature_bundle_id: str
    feature_bundle_version: str
    search_space_bundle_id: str
    search_space_bundle_version: str
    evaluator_bundle_id: str
    evaluator_bundle_version: str
    benchmark_ids: tuple[str, ...]
    provider_fetch_allowed: bool = False
    external_trading_allowed: bool = False
    promotion_eligible: bool = False
    definition_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != STUDY_DEFINITION_SCHEMA_VERSION:
            raise ValueError("unsupported study definition schema")
        if not isinstance(self.brief, ResearchBrief):
            object.__setattr__(
                self,
                "brief",
                ResearchBrief.from_dict(self.brief),
            )
        object.__setattr__(
            self,
            "study_id",
            _required(self.study_id, field="study.study_id"),
        )
        instruments = _string_set(
            self.instrument_ids,
            field="study.instrument_id",
        )
        if not instruments:
            raise ValueError("study requires at least one instrument")
        object.__setattr__(self, "instrument_ids", instruments)
        facts = tuple(self.fact_requirements)
        if not facts or len({row.fact_key for row in facts}) != len(facts):
            raise ValueError("study fact keys must be non-empty and unique")
        if sum(row.role == "primary" for row in facts) != 1:
            raise ValueError("study requires exactly one primary fact")
        object.__setattr__(self, "fact_requirements", facts)
        keys = {row.fact_key for row in facts}
        transforms = tuple(self.availability_transforms)
        if len({row.output_fact_key for row in transforms}) != len(transforms):
            raise ValueError("study availability outputs must be unique")
        for transform in transforms:
            if transform.output_fact_key not in keys or not set(
                transform.input_fact_keys
            ) <= keys:
                raise ValueError("study availability transform references unknown facts")
        object.__setattr__(self, "availability_transforms", transforms)
        joins = tuple(self.temporal_joins)
        if len({row.output_key for row in joins}) != len(joins):
            raise ValueError("study temporal join outputs must be unique")
        for join in joins:
            if join.left_fact_key not in keys or join.right_fact_key not in keys:
                raise ValueError("study temporal join references unknown facts")
        object.__setattr__(self, "temporal_joins", joins)
        for name in (
            "feature_bundle_id",
            "feature_bundle_version",
            "search_space_bundle_id",
            "search_space_bundle_version",
            "evaluator_bundle_id",
            "evaluator_bundle_version",
        ):
            object.__setattr__(
                self,
                name,
                _required(getattr(self, name), field=f"study.{name}"),
            )
        benchmarks = _string_set(
            self.benchmark_ids,
            field="study.benchmark_id",
        )
        if self.brief.economic_claim_intent in {"selection", "promotion"} and not benchmarks:
            raise ValueError("selection-oriented study requires benchmarks")
        object.__setattr__(self, "benchmark_ids", benchmarks)
        if self.provider_fetch_allowed or self.external_trading_allowed or self.promotion_eligible:
            raise ValueError(
                "offline study cannot fetch providers, trade externally, or promote"
            )
        expected = stable_hash(self._material())
        if self.definition_hash and self.definition_hash != expected:
            raise ValueError("study_definition_hash_mismatch")
        object.__setattr__(self, "definition_hash", expected)

    @property
    def primary_fact(self) -> FactRequirement:
        return next(row for row in self.fact_requirements if row.role == "primary")

    @property
    def bundle_versions(self) -> dict[str, str]:
        return {
            self.feature_bundle_id: self.feature_bundle_version,
            self.search_space_bundle_id: self.search_space_bundle_version,
            self.evaluator_bundle_id: self.evaluator_bundle_version,
        }

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "definition_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "definition_hash": self.definition_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> StudyDefinition:
        values = dict(raw)
        values["brief"] = ResearchBrief.from_dict(raw.get("brief") or {})
        values["instrument_ids"] = tuple(raw.get("instrument_ids") or ())
        values["fact_requirements"] = tuple(
            FactRequirement.from_dict(row)
            for row in raw.get("fact_requirements") or ()
        )
        values["availability_transforms"] = tuple(
            AvailabilityTransform.from_dict(row)
            for row in raw.get("availability_transforms") or ()
        )
        values["temporal_joins"] = tuple(
            TemporalJoinSpec.from_dict(row)
            for row in raw.get("temporal_joins") or ()
        )
        values["benchmark_ids"] = tuple(raw.get("benchmark_ids") or ())
        return cls(**values)


class FeatureBundle(Protocol):
    bundle_id: str
    version: str

    def build(self, frames: Sequence[Any]) -> Sequence[Any]:
        ...


class SearchSpaceBundle(Protocol):
    bundle_id: str
    version: str

    def variants(self) -> Sequence[Any]:
        ...


class EvaluatorBundle(Protocol):
    bundle_id: str
    version: str

    def evaluate(self, *, features: Sequence[Any], variant: Any) -> Mapping[str, Any]:
        ...


class AvailabilityResolver(Protocol):
    transform_id: str
    version: str

    def derive(
        self,
        *,
        transform: AvailabilityTransform,
        frozen_inputs: Mapping[str, Sequence[Any]],
    ) -> Any:
        ...


class ResearchBundleRegistry:
    """Exact-version registry for study-selected implementations."""

    def __init__(self) -> None:
        self._bundles: dict[tuple[str, str], Any] = {}
        self._availability: dict[tuple[str, str], AvailabilityResolver] = {}

    def register(self, bundle: Any) -> None:
        identity = (
            _required(getattr(bundle, "bundle_id", None), field="bundle.bundle_id"),
            _required(getattr(bundle, "version", None), field="bundle.version"),
        )
        if identity in self._bundles:
            raise ValueError(
                f"research_bundle_duplicate: id={identity[0]} version={identity[1]}"
            )
        self._bundles[identity] = bundle

    def register_availability(self, resolver: AvailabilityResolver) -> None:
        identity = (
            _required(
                getattr(resolver, "transform_id", None),
                field="availability_resolver.transform_id",
            ),
            _required(
                getattr(resolver, "version", None),
                field="availability_resolver.version",
            ),
        )
        if identity in self._availability:
            raise ValueError(
                "availability_resolver_duplicate: "
                f"id={identity[0]} version={identity[1]}"
            )
        self._availability[identity] = resolver

    def resolve(self, bundle_id: str, version: str) -> Any:
        identity = (str(bundle_id), str(version))
        if identity not in self._bundles:
            raise ValueError(
                "research_bundle_unavailable: "
                f"id={identity[0]} version={identity[1]}"
            )
        return self._bundles[identity]

    def resolve_study(self, study: StudyDefinition) -> dict[str, Any]:
        resolved = {
            "feature": self.resolve(
                study.feature_bundle_id,
                study.feature_bundle_version,
            ),
            "search_space": self.resolve(
                study.search_space_bundle_id,
                study.search_space_bundle_version,
            ),
            "evaluator": self.resolve(
                study.evaluator_bundle_id,
                study.evaluator_bundle_version,
            ),
        }
        for transform in study.availability_transforms:
            identity = (transform.transform_id, transform.transform_version)
            if identity not in self._availability:
                raise ValueError(
                    "availability_resolver_unavailable: "
                    f"id={identity[0]} version={identity[1]}"
                )
        return resolved


@dataclass(frozen=True)
class ResearchRun:
    schema_version: str
    run_id: str
    study_definition_hash: str
    protocol_hash: str
    code_revision: str
    dataset_binding_hashes: tuple[tuple[str, str], ...]
    bundle_versions: tuple[tuple[str, str], ...]
    availability_binding_hashes: tuple[str, ...]
    created_by: str
    run_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != RESEARCH_RUN_SCHEMA_VERSION:
            raise ValueError("unsupported research run schema")
        for name in (
            "run_id",
            "study_definition_hash",
            "protocol_hash",
            "code_revision",
            "created_by",
        ):
            object.__setattr__(
                self,
                name,
                _required(getattr(self, name), field=f"run.{name}"),
            )
        datasets = tuple(sorted((str(role), str(value)) for role, value in self.dataset_binding_hashes))
        if {role for role, _ in datasets} != {role.value for role in DatasetRole}:
            raise ValueError("research run requires train, validation, and holdout bindings")
        object.__setattr__(self, "dataset_binding_hashes", datasets)
        bundles = tuple(sorted((str(key), str(value)) for key, value in self.bundle_versions))
        if len(bundles) < 3 or any(not key or not value for key, value in bundles):
            raise ValueError("research run requires pinned implementation bundles")
        object.__setattr__(self, "bundle_versions", bundles)
        availability = _string_set(
            self.availability_binding_hashes,
            field="run.availability_binding_hash",
        )
        object.__setattr__(self, "availability_binding_hashes", availability)
        expected = stable_hash(self._material())
        if self.run_hash and self.run_hash != expected:
            raise ValueError("research_run_hash_mismatch")
        object.__setattr__(self, "run_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if key != "run_hash"
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "run_hash": self.run_hash}


def preflight_study(
    *,
    study: StudyDefinition,
    protocol: ScientificProtocol,
    dataset_fact_types: Mapping[str, Iterable[str]],
    registry: ResearchBundleRegistry,
) -> dict[str, Any]:
    """Validate a frozen study/protocol composition without fetching providers."""

    if protocol.economic_claim_intent != study.brief.economic_claim_intent:
        raise ValueError("study and protocol economic claim intents differ")
    if set(protocol.instrument_ids) != set(study.instrument_ids):
        raise ValueError("study and protocol instruments differ")
    if not set(study.benchmark_ids) <= set(protocol.benchmark_ids):
        raise ValueError("protocol does not authorize every study benchmark")
    required_types = {
        requirement.fact_type
        for requirement in study.fact_requirements
        if requirement.required
    }
    role_hashes: dict[str, str] = {}
    for role in DatasetRole:
        supplied = {
            str(value)
            for value in dataset_fact_types.get(role.value, ())
        }
        missing = sorted(required_types - supplied)
        if missing:
            raise ValueError(
                "study dataset facts missing: "
                f"role={role.value} fact_types={','.join(missing)}"
            )
        role_hashes[role.value] = stable_hash(
            {"role": role.value, "fact_types": sorted(supplied)}
        )
    registry.resolve_study(study)
    material = {
        "schema_version": "research.study_preflight.v1",
        "study_definition_hash": study.definition_hash,
        "protocol_hash": protocol.protocol_hash,
        "dataset_fact_set_hashes": role_hashes,
        "bundle_versions": study.bundle_versions,
        "provider_fetch_allowed": False,
        "external_trading_allowed": False,
        "promotion_eligible": False,
    }
    return {**material, "preflight_hash": stable_hash(material)}


__all__ = [
    "AvailabilityTransform",
    "AvailabilityResolver",
    "EvaluatorBundle",
    "FactRequirement",
    "FeatureBundle",
    "RESEARCH_BRIEF_SCHEMA_VERSION",
    "RESEARCH_RUN_SCHEMA_VERSION",
    "ResearchBrief",
    "ResearchBundleRegistry",
    "ResearchRun",
    "STUDY_DEFINITION_SCHEMA_VERSION",
    "SearchSpaceBundle",
    "StudyDefinition",
    "TemporalJoinSpec",
    "preflight_study",
    "stable_hash",
]

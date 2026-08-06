"""Deterministic scientific authority contracts.

These contracts control what a research result may claim.  They never fetch
data and never grant execution, deployment, or capital permissions.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from statistics import fmean
from typing import Any, Iterable, Mapping, Sequence


PROTOCOL_SCHEMA_VERSION = "scientific_protocol.v1"
CANDIDATE_SCHEMA_VERSION = "research_candidate.v1"
SCIENTIFIC_EVIDENCE_SCHEMA_VERSION = "scientific_evidence.v1"
BLINDNESS_SCHEMA_VERSION = "holdout_blindness.v1"


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(payload),
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


def _positive_int(value: Any, *, field: str, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    lower = 0 if allow_zero else 1
    if parsed < lower:
        raise ValueError(f"{field} must be >= {lower}")
    return parsed


def _finite(value: Any, *, field: str, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not math.isfinite(parsed) or (minimum is not None and parsed < minimum):
        raise ValueError(f"{field} must be finite and >= {minimum}")
    return parsed


def _utc_text(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = _required(value, field=field)
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


class DatasetRole(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    HOLDOUT = "holdout"


class BlindnessClass(str, Enum):
    """What the platform can honestly prove about holdout exposure."""

    NONE = "NONE"
    AUTHOR_DECLARED = "AUTHOR_DECLARED"
    PLATFORM_CONTROLLED_HISTORICAL = "PLATFORM_CONTROLLED_HISTORICAL"
    EXTERNALLY_ATTESTED = "EXTERNALLY_ATTESTED"
    FORWARD_UNSEEN = "FORWARD_UNSEEN"


_BLINDNESS_RANK = {
    BlindnessClass.NONE: 0,
    BlindnessClass.AUTHOR_DECLARED: 1,
    BlindnessClass.PLATFORM_CONTROLLED_HISTORICAL: 2,
    BlindnessClass.EXTERNALLY_ATTESTED: 3,
    BlindnessClass.FORWARD_UNSEEN: 4,
}


class ScientificQualityClass(str, Enum):
    S0 = "S0"
    S1 = "S1"
    S2 = "S2"
    S3 = "S3"
    S4 = "S4"


_EXECUTION_QUALITY_RANK = {
    "X0": 0,
    "X1": 1,
    "X2": 2,
    "X3": 3,
    "X4": 4,
    "X5": 5,
}


def _string_set(values: Iterable[Any], *, field: str) -> tuple[str, ...]:
    return tuple(sorted({_required(value, field=field) for value in values}))


def _version_pairs(value: Any, *, field: str) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    pairs = tuple(
        sorted(
            (
                _required(key, field=f"{field}.name"),
                _required(version, field=f"{field}.{key}"),
            )
            for key, version in value.items()
        )
    )
    if not pairs:
        raise ValueError(f"{field} must not be empty")
    return pairs


@dataclass(frozen=True)
class DatasetAssignment:
    dataset_id: str
    dataset_hash: str
    role: DatasetRole | str
    window_start: str
    window_end: str
    blind_alias: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_id", _required(self.dataset_id, field="dataset.dataset_id"))
        object.__setattr__(self, "dataset_hash", _required(self.dataset_hash, field="dataset.dataset_hash"))
        role = self.role if isinstance(self.role, DatasetRole) else DatasetRole(str(self.role).lower())
        object.__setattr__(self, "role", role)
        start = _utc_text(self.window_start, field="dataset.window_start")
        end = _utc_text(self.window_end, field="dataset.window_end")
        if end <= start:
            raise ValueError("dataset.window_end must be after window_start")
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        alias = str(self.blind_alias or "").strip() or None
        if role is DatasetRole.HOLDOUT and alias is None:
            raise ValueError("holdout dataset assignment requires blind_alias")
        object.__setattr__(self, "blind_alias", alias)

    def to_private_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "role": self.role.value,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "blind_alias": self.blind_alias,
        }

    def to_public_dict(self, *, blindness: BlindnessClass) -> dict[str, Any]:
        payload = self.to_private_dict()
        if self.role is DatasetRole.HOLDOUT and blindness is not BlindnessClass.NONE:
            payload["dataset_id"] = None
            payload["dataset_hash"] = None
            payload["window_start"] = None
            payload["window_end"] = None
            payload["sealed"] = True
        else:
            payload["sealed"] = False
        return payload

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "DatasetAssignment":
        return cls(
            dataset_id=str(raw.get("dataset_id") or ""),
            dataset_hash=str(raw.get("dataset_hash") or ""),
            role=str(raw.get("role") or ""),
            window_start=raw.get("window_start"),
            window_end=raw.get("window_end"),
            blind_alias=raw.get("blind_alias"),
        )


@dataclass(frozen=True)
class SearchBudget:
    max_attempts: int
    max_runtime_seconds: float
    max_compute_units: float
    max_validation_feedback_uses: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_attempts", _positive_int(self.max_attempts, field="budget.max_attempts"))
        object.__setattr__(self, "max_runtime_seconds", _finite(self.max_runtime_seconds, field="budget.max_runtime_seconds", minimum=0.0))
        object.__setattr__(self, "max_compute_units", _finite(self.max_compute_units, field="budget.max_compute_units", minimum=0.0))
        object.__setattr__(
            self,
            "max_validation_feedback_uses",
            _positive_int(
                self.max_validation_feedback_uses,
                field="budget.max_validation_feedback_uses",
                allow_zero=True,
            ),
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "SearchBudget":
        return cls(
            max_attempts=raw.get("max_attempts"),
            max_runtime_seconds=raw.get("max_runtime_seconds"),
            max_compute_units=raw.get("max_compute_units"),
            max_validation_feedback_uses=raw.get(
                "max_validation_feedback_uses", 0
            ),
        )


@dataclass(frozen=True)
class LeakagePolicy:
    max_feature_lookback_bars: int
    label_horizon_bars: int
    max_holding_period_bars: int
    order_expiry_bars: int
    purge_bars: int = 0
    embargo_bars: int = 0

    def __post_init__(self) -> None:
        inputs = {
            name: _positive_int(getattr(self, name), field=f"leakage.{name}", allow_zero=True)
            for name in (
                "max_feature_lookback_bars",
                "label_horizon_bars",
                "max_holding_period_bars",
                "order_expiry_bars",
            )
        }
        for name, value in inputs.items():
            object.__setattr__(self, name, value)
        derived = max(inputs.values())
        supplied_purge = _positive_int(self.purge_bars, field="leakage.purge_bars", allow_zero=True)
        supplied_embargo = _positive_int(self.embargo_bars, field="leakage.embargo_bars", allow_zero=True)
        if supplied_purge not in {0, derived}:
            raise ValueError("leakage.purge_bars must equal the derived contamination horizon")
        if supplied_embargo not in {0, derived}:
            raise ValueError("leakage.embargo_bars must equal the derived contamination horizon")
        object.__setattr__(self, "purge_bars", derived)
        object.__setattr__(self, "embargo_bars", derived)

    @property
    def contamination_horizon_bars(self) -> int:
        return self.purge_bars

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "LeakagePolicy":
        return cls(
            max_feature_lookback_bars=raw.get("max_feature_lookback_bars", 0),
            label_horizon_bars=raw.get("label_horizon_bars", 0),
            max_holding_period_bars=raw.get("max_holding_period_bars", 0),
            order_expiry_bars=raw.get("order_expiry_bars", 0),
            purge_bars=raw.get("purge_bars", 0),
            embargo_bars=raw.get("embargo_bars", 0),
        )


@dataclass(frozen=True)
class WalkForwardFold:
    fold_index: int
    train_start_bar: int
    train_end_bar: int
    validation_start_bar: int
    validation_end_bar: int
    embargo_end_bar: int

    def __post_init__(self) -> None:
        for name in asdict(self):
            object.__setattr__(
                self,
                name,
                _positive_int(getattr(self, name), field=f"fold.{name}", allow_zero=True),
            )
        if not (
            self.train_start_bar < self.train_end_bar
            <= self.validation_start_bar
            < self.validation_end_bar
            <= self.embargo_end_bar
        ):
            raise ValueError("walk-forward fold boundaries are invalid")

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "WalkForwardFold":
        return cls(**{name: raw.get(name) for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class WalkForwardPlan:
    train_bars: int
    validation_bars: int
    step_bars: int
    fold_count: int
    folds: tuple[WalkForwardFold, ...]

    def __post_init__(self) -> None:
        for name in ("train_bars", "validation_bars", "step_bars", "fold_count"):
            object.__setattr__(self, name, _positive_int(getattr(self, name), field=f"walk_forward.{name}"))
        folds = tuple(self.folds)
        if len(folds) != self.fold_count:
            raise ValueError("walk_forward fold_count does not match folds")
        if [fold.fold_index for fold in folds] != list(range(self.fold_count)):
            raise ValueError("walk_forward fold indexes must be contiguous from zero")
        object.__setattr__(self, "folds", folds)

    @classmethod
    def build(
        cls,
        *,
        train_bars: int,
        validation_bars: int,
        step_bars: int,
        fold_count: int,
        leakage: LeakagePolicy,
    ) -> "WalkForwardPlan":
        train = _positive_int(train_bars, field="walk_forward.train_bars")
        validation = _positive_int(validation_bars, field="walk_forward.validation_bars")
        step = _positive_int(step_bars, field="walk_forward.step_bars")
        count = _positive_int(fold_count, field="walk_forward.fold_count")
        folds = []
        for index in range(count):
            train_start = index * step
            train_end = train_start + train
            validation_start = train_end + leakage.purge_bars
            validation_end = validation_start + validation
            folds.append(
                WalkForwardFold(
                    fold_index=index,
                    train_start_bar=train_start,
                    train_end_bar=train_end,
                    validation_start_bar=validation_start,
                    validation_end_bar=validation_end,
                    embargo_end_bar=validation_end + leakage.embargo_bars,
                )
            )
        return cls(
            train_bars=train,
            validation_bars=validation,
            step_bars=step,
            fold_count=count,
            folds=tuple(folds),
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "WalkForwardPlan":
        return cls(
            train_bars=raw.get("train_bars"),
            validation_bars=raw.get("validation_bars"),
            step_bars=raw.get("step_bars"),
            fold_count=raw.get("fold_count"),
            folds=tuple(WalkForwardFold.from_dict(row) for row in raw.get("folds") or ()),
        )


@dataclass(frozen=True)
class ScientificProtocol:
    schema_version: str
    protocol_id: str
    family_name: str
    economic_claim_intent: str
    datasets: tuple[DatasetAssignment, ...]
    blindness: BlindnessClass | str
    budget: SearchBudget
    leakage: LeakagePolicy
    walk_forward: WalkForwardPlan
    instrument_ids: tuple[str, ...]
    allowed_mutation_dimensions: tuple[str, ...]
    benchmark_ids: tuple[str, ...]
    primary_metric: str
    primary_metric_direction: str
    minimum_effect_size: float
    secondary_metrics: tuple[str, ...]
    safety_metrics: tuple[str, ...]
    alpha: float
    minimum_sample_count: int
    minimum_trade_count: int
    minimum_calendar_days: int
    minimum_exposure: float
    minimum_execution_quality_class: str
    execution_stress_ids: tuple[str, ...]
    multiple_testing_method: str
    robustness_requirements: tuple[str, ...]
    statistical_method_versions: tuple[tuple[str, str], ...]
    policy_versions: tuple[tuple[str, str], ...]
    created_by: str
    authorized_by: str
    authorization_request_id: str
    protocol_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != PROTOCOL_SCHEMA_VERSION:
            raise ValueError(f"unsupported protocol schema: {self.schema_version}")
        for name in (
            "protocol_id",
            "family_name",
            "primary_metric",
            "created_by",
            "authorized_by",
            "authorization_request_id",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), field=f"protocol.{name}"))
        intent = str(self.economic_claim_intent or "").strip().lower()
        if intent not in {"exploration", "economic", "selection", "promotion"}:
            raise ValueError("protocol.economic_claim_intent is invalid")
        object.__setattr__(self, "economic_claim_intent", intent)
        blindness = self.blindness if isinstance(self.blindness, BlindnessClass) else BlindnessClass(str(self.blindness))
        object.__setattr__(self, "blindness", blindness)
        datasets = tuple(self.datasets)
        roles = [row.role for row in datasets]
        if not datasets or DatasetRole.TRAIN not in roles or DatasetRole.VALIDATION not in roles or DatasetRole.HOLDOUT not in roles:
            raise ValueError("protocol requires train, validation, and holdout assignments")
        if any(roles.count(role) != 1 for role in DatasetRole):
            raise ValueError("protocol requires exactly one train, validation, and holdout assignment")
        if len({row.dataset_id for row in datasets}) != len(datasets):
            raise ValueError("protocol dataset assignments must be unique")
        holdout = next(row for row in datasets if row.role is DatasetRole.HOLDOUT)
        if any(row.dataset_hash == holdout.dataset_hash for row in datasets if row is not holdout):
            raise ValueError("holdout dataset must be materially distinct from train/validation")
        object.__setattr__(self, "datasets", datasets)
        instruments = _string_set(self.instrument_ids, field="protocol.instrument_id")
        if not instruments:
            raise ValueError("protocol.instrument_ids must not be empty")
        object.__setattr__(self, "instrument_ids", instruments)
        mutations = _string_set(
            self.allowed_mutation_dimensions,
            field="protocol.allowed_mutation_dimension",
        )
        if not mutations:
            raise ValueError("protocol.allowed_mutation_dimensions must not be empty")
        object.__setattr__(self, "allowed_mutation_dimensions", mutations)
        benchmarks = _string_set(self.benchmark_ids, field="protocol.benchmark_id")
        if intent in {"selection", "promotion"} and not benchmarks:
            raise ValueError("selection-oriented protocol requires a benchmark")
        object.__setattr__(self, "benchmark_ids", benchmarks)
        direction = str(self.primary_metric_direction or "").strip().lower()
        if direction not in {"maximize", "minimize"}:
            raise ValueError(
                "protocol.primary_metric_direction must be maximize or minimize"
            )
        object.__setattr__(self, "primary_metric_direction", direction)
        object.__setattr__(
            self,
            "minimum_effect_size",
            _finite(
                self.minimum_effect_size,
                field="protocol.minimum_effect_size",
                minimum=0.0,
            ),
        )
        secondary = _string_set(self.secondary_metrics, field="protocol.secondary_metric")
        safety = _string_set(self.safety_metrics, field="protocol.safety_metric")
        if intent in {"selection", "promotion"} and (not secondary or not safety):
            raise ValueError(
                "selection-oriented protocol requires secondary and safety metrics"
            )
        object.__setattr__(self, "secondary_metrics", secondary)
        object.__setattr__(self, "safety_metrics", safety)
        alpha = _finite(self.alpha, field="protocol.alpha", minimum=0.0)
        if alpha <= 0.0 or alpha >= 1.0:
            raise ValueError("protocol.alpha must be between zero and one")
        object.__setattr__(self, "alpha", alpha)
        object.__setattr__(self, "minimum_sample_count", _positive_int(self.minimum_sample_count, field="protocol.minimum_sample_count"))
        object.__setattr__(self, "minimum_trade_count", _positive_int(self.minimum_trade_count, field="protocol.minimum_trade_count"))
        object.__setattr__(self, "minimum_calendar_days", _positive_int(self.minimum_calendar_days, field="protocol.minimum_calendar_days"))
        object.__setattr__(self, "minimum_exposure", _finite(self.minimum_exposure, field="protocol.minimum_exposure", minimum=0.0))
        execution_class = str(self.minimum_execution_quality_class or "").strip().upper()
        if execution_class not in _EXECUTION_QUALITY_RANK:
            raise ValueError("protocol.minimum_execution_quality_class must be X0 through X5")
        if intent in {"economic", "selection", "promotion"} and _EXECUTION_QUALITY_RANK[execution_class] < 2:
            raise ValueError("economic protocol requires at least X2 execution quality")
        object.__setattr__(self, "minimum_execution_quality_class", execution_class)
        stresses = _string_set(self.execution_stress_ids, field="protocol.execution_stress_id")
        if intent in {"economic", "selection", "promotion"} and not stresses:
            raise ValueError("economic protocol requires pinned execution stresses")
        object.__setattr__(self, "execution_stress_ids", stresses)
        method = str(self.multiple_testing_method or "").strip().lower()
        if method not in {"bonferroni", "holm"}:
            raise ValueError("protocol.multiple_testing_method must be bonferroni or holm")
        object.__setattr__(self, "multiple_testing_method", method)
        requirements = tuple(sorted({_required(row, field="protocol.robustness_requirement") for row in self.robustness_requirements}))
        object.__setattr__(self, "robustness_requirements", requirements)
        object.__setattr__(
            self,
            "statistical_method_versions",
            _version_pairs(
                dict(self.statistical_method_versions),
                field="protocol.statistical_method_versions",
            ),
        )
        object.__setattr__(
            self,
            "policy_versions",
            _version_pairs(dict(self.policy_versions), field="protocol.policy_versions"),
        )
        expected = _stable_hash(self._material())
        if self.protocol_hash and self.protocol_hash != expected:
            raise ValueError("scientific_protocol_hash_mismatch")
        object.__setattr__(self, "protocol_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "protocol_id": self.protocol_id,
            "family_name": self.family_name,
            "economic_claim_intent": self.economic_claim_intent,
            "datasets": [row.to_private_dict() for row in self.datasets],
            "blindness": self.blindness.value,
            "budget": asdict(self.budget),
            "leakage": asdict(self.leakage),
            "walk_forward": {
                **{key: value for key, value in asdict(self.walk_forward).items() if key != "folds"},
                "folds": [asdict(row) for row in self.walk_forward.folds],
            },
            "instrument_ids": list(self.instrument_ids),
            "allowed_mutation_dimensions": list(self.allowed_mutation_dimensions),
            "benchmark_ids": list(self.benchmark_ids),
            "primary_metric": self.primary_metric,
            "primary_metric_direction": self.primary_metric_direction,
            "minimum_effect_size": self.minimum_effect_size,
            "secondary_metrics": list(self.secondary_metrics),
            "safety_metrics": list(self.safety_metrics),
            "alpha": self.alpha,
            "minimum_sample_count": self.minimum_sample_count,
            "minimum_trade_count": self.minimum_trade_count,
            "minimum_calendar_days": self.minimum_calendar_days,
            "minimum_exposure": self.minimum_exposure,
            "minimum_execution_quality_class": self.minimum_execution_quality_class,
            "execution_stress_ids": list(self.execution_stress_ids),
            "multiple_testing_method": self.multiple_testing_method,
            "robustness_requirements": list(self.robustness_requirements),
            "statistical_method_versions": dict(self.statistical_method_versions),
            "policy_versions": dict(self.policy_versions),
            "created_by": self.created_by,
            "authorized_by": self.authorized_by,
            "authorization_request_id": self.authorization_request_id,
        }

    def to_private_dict(self) -> dict[str, Any]:
        return {**self._material(), "protocol_hash": self.protocol_hash}

    def to_public_dict(self) -> dict[str, Any]:
        payload = self._material()
        payload["datasets"] = [
            row.to_public_dict(blindness=self.blindness) for row in self.datasets
        ]
        payload["protocol_hash"] = self.protocol_hash
        payload["blindness_schema_version"] = BLINDNESS_SCHEMA_VERSION
        payload["blindness_claim"] = {
            BlindnessClass.NONE: "no_blindness_claim",
            BlindnessClass.AUTHOR_DECLARED: (
                "author_declared_non_exposure; platform cannot prove prior knowledge"
            ),
            BlindnessClass.PLATFORM_CONTROLLED_HISTORICAL: (
                "controlled_workflow_non_exposure_only; prior external knowledge is not provable"
            ),
            BlindnessClass.EXTERNALLY_ATTESTED: (
                "external_custodian_attested_non_exposure"
            ),
            BlindnessClass.FORWARD_UNSEEN: (
                "candidate_frozen_before_evaluation_data_existed"
            ),
        }[self.blindness]
        return payload

    def assignment(self, role: DatasetRole | str) -> DatasetAssignment:
        normalized = role if isinstance(role, DatasetRole) else DatasetRole(str(role).lower())
        matches = [row for row in self.datasets if row.role is normalized]
        if len(matches) != 1:
            raise ValueError(f"protocol role {normalized.value} is not uniquely assigned")
        return matches[0]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ScientificProtocol":
        leakage = LeakagePolicy.from_dict(raw.get("leakage") or {})
        raw_walk_forward = raw.get("walk_forward") or {}
        walk_forward = (
            WalkForwardPlan.from_dict(raw_walk_forward)
            if raw_walk_forward.get("folds")
            else WalkForwardPlan.build(
                train_bars=raw_walk_forward.get("train_bars"),
                validation_bars=raw_walk_forward.get("validation_bars"),
                step_bars=raw_walk_forward.get("step_bars"),
                fold_count=raw_walk_forward.get("fold_count"),
                leakage=leakage,
            )
        )
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            protocol_id=str(raw.get("protocol_id") or ""),
            family_name=str(raw.get("family_name") or ""),
            economic_claim_intent=str(raw.get("economic_claim_intent") or ""),
            datasets=tuple(DatasetAssignment.from_dict(row) for row in raw.get("datasets") or ()),
            blindness=str(raw.get("blindness") or ""),
            budget=SearchBudget.from_dict(raw.get("budget") or {}),
            leakage=leakage,
            walk_forward=walk_forward,
            instrument_ids=tuple(raw.get("instrument_ids") or ()),
            allowed_mutation_dimensions=tuple(raw.get("allowed_mutation_dimensions") or ()),
            benchmark_ids=tuple(raw.get("benchmark_ids") or ()),
            primary_metric=str(raw.get("primary_metric") or ""),
            primary_metric_direction=str(raw.get("primary_metric_direction") or ""),
            minimum_effect_size=raw.get("minimum_effect_size"),
            secondary_metrics=tuple(raw.get("secondary_metrics") or ()),
            safety_metrics=tuple(raw.get("safety_metrics") or ()),
            alpha=raw.get("alpha"),
            minimum_sample_count=raw.get("minimum_sample_count"),
            minimum_trade_count=raw.get("minimum_trade_count"),
            minimum_calendar_days=raw.get("minimum_calendar_days"),
            minimum_exposure=raw.get("minimum_exposure"),
            minimum_execution_quality_class=str(raw.get("minimum_execution_quality_class") or ""),
            execution_stress_ids=tuple(raw.get("execution_stress_ids") or ()),
            multiple_testing_method=str(raw.get("multiple_testing_method") or ""),
            robustness_requirements=tuple(raw.get("robustness_requirements") or ()),
            statistical_method_versions=tuple(
                (str(key), str(value))
                for key, value in dict(raw.get("statistical_method_versions") or {}).items()
            ),
            policy_versions=tuple(
                (str(key), str(value))
                for key, value in dict(raw.get("policy_versions") or {}).items()
            ),
            created_by=str(raw.get("created_by") or ""),
            authorized_by=str(raw.get("authorized_by") or ""),
            authorization_request_id=str(raw.get("authorization_request_id") or ""),
            protocol_hash=str(raw.get("protocol_hash") or ""),
        )


@dataclass(frozen=True)
class CandidateSnapshot:
    schema_version: str
    candidate_id: str
    family_id: str
    protocol_hash: str
    source_attempt_id: str
    strategy_artifact_hash: str
    parameter_artifact_hash: str
    execution_model_hash: str
    metric_contract_hash: str
    research_dataset_hashes: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    frozen_by: str
    candidate_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != CANDIDATE_SCHEMA_VERSION:
            raise ValueError(f"unsupported candidate schema: {self.schema_version}")
        for name in (
            "candidate_id",
            "family_id",
            "protocol_hash",
            "source_attempt_id",
            "strategy_artifact_hash",
            "parameter_artifact_hash",
            "execution_model_hash",
            "metric_contract_hash",
            "frozen_by",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), field=f"candidate.{name}"))
        hashes = tuple(sorted({_required(row, field="candidate.evidence_hash") for row in self.evidence_hashes}))
        if not hashes:
            raise ValueError("candidate requires evidence hashes")
        object.__setattr__(self, "evidence_hashes", hashes)
        datasets = tuple(
            sorted(
                {
                    _required(row, field="candidate.research_dataset_hash")
                    for row in self.research_dataset_hashes
                }
            )
        )
        if not datasets:
            raise ValueError("candidate requires train/validation dataset hashes")
        object.__setattr__(self, "research_dataset_hashes", datasets)
        expected = _stable_hash(self._material())
        if self.candidate_hash and self.candidate_hash != expected:
            raise ValueError("research_candidate_hash_mismatch")
        object.__setattr__(self, "candidate_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "candidate_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "candidate_hash": self.candidate_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CandidateSnapshot":
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            candidate_id=str(raw.get("candidate_id") or ""),
            family_id=str(raw.get("family_id") or ""),
            protocol_hash=str(raw.get("protocol_hash") or ""),
            source_attempt_id=str(raw.get("source_attempt_id") or ""),
            strategy_artifact_hash=str(raw.get("strategy_artifact_hash") or ""),
            parameter_artifact_hash=str(raw.get("parameter_artifact_hash") or ""),
            execution_model_hash=str(raw.get("execution_model_hash") or ""),
            metric_contract_hash=str(raw.get("metric_contract_hash") or ""),
            research_dataset_hashes=tuple(raw.get("research_dataset_hashes") or ()),
            evidence_hashes=tuple(raw.get("evidence_hashes") or ()),
            frozen_by=str(raw.get("frozen_by") or ""),
            candidate_hash=str(raw.get("candidate_hash") or ""),
        )


@dataclass(frozen=True)
class ScientificEvidence:
    schema_version: str
    reproducible: bool
    protocol_bound: bool
    attempts_registered: int
    attempts_accounted: int
    budget_compliant: bool
    benchmark_present: bool
    walk_forward_complete: bool
    leakage_controls_applied: bool
    candidate_frozen_before_holdout: bool
    holdout_used_once: bool
    blindness: BlindnessClass | str
    sample_count: int
    trade_count: int
    calendar_days: int
    exposure: float
    minimum_sample_count: int
    minimum_trade_count: int
    minimum_calendar_days: int
    minimum_exposure: float
    execution_quality_sufficient: bool
    safety_metrics_passed: bool
    effect_size: float | None
    minimum_effect_size: float
    effect_size_sufficient: bool
    raw_p_value: float | None
    adjusted_p_value: float | None
    alpha: float
    confidence_interval_low: float | None
    robustness_passed: tuple[str, ...]
    robustness_required: tuple[str, ...]
    cost_stress_passed: bool
    latency_stress_passed: bool
    failed_trials_retained: bool
    family_closed_before_holdout: bool

    def __post_init__(self) -> None:
        if self.schema_version != SCIENTIFIC_EVIDENCE_SCHEMA_VERSION:
            raise ValueError(f"unsupported scientific evidence schema: {self.schema_version}")
        blindness = self.blindness if isinstance(self.blindness, BlindnessClass) else BlindnessClass(str(self.blindness))
        object.__setattr__(self, "blindness", blindness)
        for name in (
            "attempts_registered",
            "attempts_accounted",
            "sample_count",
            "trade_count",
            "calendar_days",
            "minimum_sample_count",
            "minimum_trade_count",
            "minimum_calendar_days",
        ):
            object.__setattr__(self, name, _positive_int(getattr(self, name), field=f"scientific_evidence.{name}", allow_zero=True))
        for name in ("exposure", "minimum_exposure"):
            object.__setattr__(self, name, _finite(getattr(self, name), field=f"scientific_evidence.{name}", minimum=0.0))
        object.__setattr__(
            self,
            "minimum_effect_size",
            _finite(
                self.minimum_effect_size,
                field="scientific_evidence.minimum_effect_size",
                minimum=0.0,
            ),
        )
        if self.effect_size is not None:
            object.__setattr__(
                self,
                "effect_size",
                _finite(self.effect_size, field="scientific_evidence.effect_size"),
            )
        alpha = _finite(self.alpha, field="scientific_evidence.alpha", minimum=0.0)
        if alpha <= 0.0 or alpha >= 1.0:
            raise ValueError("scientific_evidence.alpha must be between zero and one")
        object.__setattr__(self, "alpha", alpha)
        for name in ("raw_p_value", "adjusted_p_value"):
            value = getattr(self, name)
            if value is not None:
                parsed = _finite(value, field=f"scientific_evidence.{name}", minimum=0.0)
                if parsed > 1.0:
                    raise ValueError(f"scientific_evidence.{name} must be <= 1")
                object.__setattr__(self, name, parsed)
        if self.confidence_interval_low is not None:
            object.__setattr__(self, "confidence_interval_low", _finite(self.confidence_interval_low, field="scientific_evidence.confidence_interval_low"))
        object.__setattr__(self, "robustness_passed", tuple(sorted(set(self.robustness_passed))))
        object.__setattr__(self, "robustness_required", tuple(sorted(set(self.robustness_required))))


@dataclass(frozen=True)
class ScientificQualityAssessment:
    scientific_quality_class: ScientificQualityClass
    qualified: bool
    blocking_reasons: tuple[str, ...]
    evidence_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "scientific_quality_assessment.v1",
            "scientific_quality_class": self.scientific_quality_class.value,
            "qualified": self.qualified,
            "blocking_reasons": list(self.blocking_reasons),
            "evidence_hash": self.evidence_hash,
        }


def classify_scientific_quality(evidence: ScientificEvidence) -> ScientificQualityAssessment:
    """Return the highest class whose complete requirements are satisfied."""

    blockers: list[str] = []
    actual = ScientificQualityClass.S0
    if not evidence.reproducible:
        blockers.append("reproducibility_not_certified")
    s1 = (
        evidence.reproducible
        and evidence.protocol_bound
        and evidence.attempts_registered > 0
        and evidence.attempts_registered == evidence.attempts_accounted
        and evidence.budget_compliant
        and evidence.benchmark_present
        and evidence.failed_trials_retained
    )
    if s1:
        actual = ScientificQualityClass.S1
    else:
        blockers.append("protocol_or_search_accounting_incomplete")
    s2 = (
        s1
        and evidence.walk_forward_complete
        and evidence.leakage_controls_applied
        and evidence.candidate_frozen_before_holdout
        and evidence.sample_count >= evidence.minimum_sample_count
        and evidence.trade_count >= evidence.minimum_trade_count
        and evidence.calendar_days >= evidence.minimum_calendar_days
        and evidence.exposure >= evidence.minimum_exposure
        and evidence.execution_quality_sufficient
        and evidence.safety_metrics_passed
        and evidence.effect_size_sufficient
    )
    if s2:
        actual = ScientificQualityClass.S2
    elif s1:
        blockers.append("leakage_controlled_validation_incomplete")
    s3 = (
        s2
        and evidence.holdout_used_once
        and _BLINDNESS_RANK[evidence.blindness]
        >= _BLINDNESS_RANK[BlindnessClass.PLATFORM_CONTROLLED_HISTORICAL]
        and evidence.family_closed_before_holdout
    )
    if s3:
        actual = ScientificQualityClass.S3
    elif s2:
        blockers.append("sealed_holdout_requirements_incomplete")
    robustness_complete = set(evidence.robustness_required) <= set(evidence.robustness_passed)
    significance_passed = (
        evidence.adjusted_p_value is not None
        and evidence.adjusted_p_value <= evidence.alpha
        and evidence.confidence_interval_low is not None
        and evidence.confidence_interval_low > 0.0
    )
    s4 = (
        s3
        and robustness_complete
        and significance_passed
        and evidence.cost_stress_passed
        and evidence.latency_stress_passed
    )
    if s4:
        actual = ScientificQualityClass.S4
    elif s3:
        blockers.append("robustness_or_adjusted_uncertainty_incomplete")
    material = {
        **asdict(evidence),
        "blindness": evidence.blindness.value,
        "scientific_quality_class": actual.value,
        "blocking_reasons": sorted(set(blockers)),
    }
    return ScientificQualityAssessment(
        scientific_quality_class=actual,
        qualified=evidence.reproducible,
        blocking_reasons=tuple(sorted(set(blockers))),
        evidence_hash=_stable_hash(material),
    )


def adjusted_p_values(values: Sequence[float], *, method: str) -> tuple[float, ...]:
    """Deterministic Bonferroni or Holm family-wise error adjustment."""

    p_values = tuple(_finite(value, field="p_value", minimum=0.0) for value in values)
    if any(value > 1.0 for value in p_values):
        raise ValueError("p_values must be <= 1")
    normalized = str(method or "").strip().lower()
    count = len(p_values)
    if normalized == "bonferroni":
        return tuple(min(1.0, value * count) for value in p_values)
    if normalized != "holm":
        raise ValueError("multiple-testing method must be bonferroni or holm")
    ordered = sorted(enumerate(p_values), key=lambda row: (row[1], row[0]))
    adjusted = [0.0] * count
    running = 0.0
    for rank, (index, value) in enumerate(ordered):
        running = max(running, min(1.0, value * (count - rank)))
        adjusted[index] = running
    return tuple(adjusted)


def deterministic_block_bootstrap_ci(
    values: Sequence[float],
    *,
    block_size: int,
    resamples: int,
    confidence: float,
    seed_material: str,
) -> tuple[float, float]:
    """Moving-block mean interval with a seed derived from immutable material."""

    rows = tuple(_finite(value, field="bootstrap.value") for value in values)
    if len(rows) < 2:
        raise ValueError("bootstrap requires at least two observations")
    block = _positive_int(block_size, field="bootstrap.block_size")
    draws = _positive_int(resamples, field="bootstrap.resamples")
    if block > len(rows):
        raise ValueError("bootstrap.block_size exceeds observation count")
    level = _finite(confidence, field="bootstrap.confidence", minimum=0.0)
    if level <= 0.0 or level >= 1.0:
        raise ValueError("bootstrap.confidence must be between zero and one")
    seed = int(hashlib.sha256(_required(seed_material, field="bootstrap.seed_material").encode("utf-8")).hexdigest(), 16)
    rng = random.Random(seed)
    means: list[float] = []
    starts = len(rows) - block + 1
    for _ in range(draws):
        sample: list[float] = []
        while len(sample) < len(rows):
            start = rng.randrange(starts)
            sample.extend(rows[start : start + block])
        means.append(fmean(sample[: len(rows)]))
    means.sort()
    tail = (1.0 - level) / 2.0
    lower_index = max(0, min(draws - 1, int(math.floor(tail * draws))))
    upper_index = max(0, min(draws - 1, int(math.ceil((1.0 - tail) * draws)) - 1))
    return means[lower_index], means[upper_index]

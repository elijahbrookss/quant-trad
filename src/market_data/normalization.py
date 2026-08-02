"""Immutable normalization specs and causal, prior-only numeric transforms."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

from .fact_registry import (
    NORMALIZED_FACT_PREFIX,
    NORMALIZED_FACT_VERSION,
    get_fact_contract,
)


NORMALIZATION_SPEC_VERSION = "market.normalization_spec.v1"
NORMALIZED_MATERIAL_VERSION = "market.normalized_feature_material.v1"
TEN_THOUSAND = Decimal("10000")


def _utc(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"market_normalization_invalid: {field} must be datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: Any, *, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"market_normalization_invalid: {field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"market_normalization_invalid: {field} must be numeric"
        ) from exc
    if not result.is_finite():
        raise ValueError(f"market_normalization_invalid: {field} must be finite")
    return Decimal(0) if result == 0 else result


def _decimal_string(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _time_string(value: datetime) -> str:
    return _utc(value, field="time").isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )




def _content_hash(value: Any, *, field: str) -> str:
    digest = str(value or "").strip().lower()
    if len(digest) != 64:
        raise ValueError(
            f"market_normalization_invalid: {field} must be a SHA-256 hex digest"
        )
    try:
        int(digest, 16)
    except ValueError as exc:
        raise ValueError(
            f"market_normalization_invalid: {field} must be a SHA-256 hex digest"
        ) from exc
    return digest


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


class NormalizationFormula(str, Enum):
    BASIS_POINTS = "basis_points"
    RATIO = "ratio"
    CAUSAL_PERCENTILE = "causal_percentile"
    CAUSAL_ZSCORE = "causal_zscore"
    TIME_OF_DAY_MEDIAN_RATIO = "time_of_day_median_ratio"
    VOLATILITY_ADJUSTED_RETURN = "volatility_adjusted_return"


class NormalizedStatus(str, Enum):
    VALID = "valid"
    INSUFFICIENT_HISTORY = "insufficient_history"
    INVALID_INPUT = "invalid_input"
    ZERO_DENOMINATOR = "zero_denominator"
    ZERO_VARIANCE = "zero_variance"


@dataclass(frozen=True)
class NormalizationSpec:
    feature_name: str
    semantic_version: str
    input_fact_type: str
    output_fact_type: str
    formula: NormalizationFormula | str
    units: str
    window_seconds: Optional[int]
    minimum_observations: int
    warmup_observations: int
    partition: str = "series"
    missing_behavior: str = "emit_null_with_reason"
    materialization_mode: str = "frozen_or_on_demand"
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        feature = str(self.feature_name or "").strip().lower()
        semantic = str(self.semantic_version or "").strip()
        input_type = str(self.input_fact_type or "").strip().lower()
        output_type = str(self.output_fact_type or "").strip().lower()
        units = str(self.units or "").strip()
        if not feature or not semantic or not input_type or not units:
            raise ValueError("market_normalization_spec_invalid: identities are required")
        if output_type != f"{NORMALIZED_FACT_PREFIX}{feature}":
            raise ValueError(
                "market_normalization_spec_invalid: output fact type must match feature name"
            )
        try:
            get_fact_contract(input_type)
        except ValueError as exc:
            raise ValueError(
                "market_normalization_spec_invalid: input fact contract is unsupported"
            ) from exc
        partition = str(self.partition or "").strip().lower()
        if partition not in {"series", "series_minute_of_day"}:
            raise ValueError(
                "market_normalization_spec_invalid: unsupported partition"
            )
        materialization_mode = str(self.materialization_mode or "").strip().lower()
        if materialization_mode not in {"frozen_or_on_demand", "continuous"}:
            raise ValueError(
                "market_normalization_spec_invalid: unsupported materialization mode"
            )
        try:
            raw_formula = (
                self.formula.value
                if isinstance(self.formula, NormalizationFormula)
                else self.formula
            )
            formula = NormalizationFormula(str(raw_formula))
        except ValueError as exc:
            raise ValueError(
                "market_normalization_spec_invalid: unsupported formula"
            ) from exc
        minimum = int(self.minimum_observations)
        warmup = int(self.warmup_observations)
        if minimum < 0 or warmup < minimum:
            raise ValueError(
                "market_normalization_spec_invalid: warmup must cover minimum observations"
            )
        window = int(self.window_seconds) if self.window_seconds is not None else None
        if window is not None and window <= 0:
            raise ValueError(
                "market_normalization_spec_invalid: window_seconds must be positive"
            )
        if formula in {
            NormalizationFormula.CAUSAL_PERCENTILE,
            NormalizationFormula.CAUSAL_ZSCORE,
            NormalizationFormula.TIME_OF_DAY_MEDIAN_RATIO,
            NormalizationFormula.VOLATILITY_ADJUSTED_RETURN,
        } and window is None:
            raise ValueError(
                "market_normalization_spec_invalid: rolling formula requires a window"
            )
        if str(self.missing_behavior) != "emit_null_with_reason":
            raise ValueError(
                "market_normalization_spec_invalid: v1 requires explicit null status"
            )
        object.__setattr__(self, "feature_name", feature)
        object.__setattr__(self, "semantic_version", semantic)
        object.__setattr__(self, "input_fact_type", input_type)
        object.__setattr__(self, "output_fact_type", output_type)
        object.__setattr__(self, "formula", formula)
        object.__setattr__(self, "units", units)
        object.__setattr__(self, "window_seconds", window)
        object.__setattr__(self, "minimum_observations", minimum)
        object.__setattr__(self, "warmup_observations", warmup)
        object.__setattr__(self, "parameters", dict(self.parameters or {}))

        object.__setattr__(self, "partition", partition)
        object.__setattr__(self, "materialization_mode", materialization_mode)

    @property
    def spec_hash(self) -> str:
        return _stable_hash(self.material())

    @property
    def spec_id(self) -> str:
        return f"nsp_{self.spec_hash[:31]}"

    def material(self) -> dict[str, Any]:
        return {
            "schema_version": NORMALIZATION_SPEC_VERSION,
            "feature_name": self.feature_name,
            "semantic_version": self.semantic_version,
            "input_fact_type": self.input_fact_type,
            "output_fact_type": self.output_fact_type,
            "formula": self.formula.value,
            "units": self.units,
            "window_seconds": self.window_seconds,
            "minimum_observations": self.minimum_observations,
            "warmup_observations": self.warmup_observations,
            "partition": self.partition,
            "missing_behavior": self.missing_behavior,
            "materialization_mode": self.materialization_mode,
            "parameters": dict(self.parameters),
        }


@dataclass(frozen=True)
class NormalizationInput:
    source_series_id: int
    effective_at: datetime
    known_at: datetime
    market_commit_seq: int
    material_hash: str
    value: Optional[Decimal]
    denominator: Optional[Decimal] = None
    partition_key: str = "series"
    valid: bool = True
    invalid_reason: Optional[str] = None
    quality_fingerprint: Optional[str] = None

    def __post_init__(self) -> None:
        if int(self.source_series_id) <= 0 or int(self.market_commit_seq) <= 0:
            raise ValueError("market_normalization_input_invalid: source identity")
        effective = _utc(self.effective_at, field="effective_at")
        known = _utc(self.known_at, field="known_at")
        if known < effective:
            raise ValueError("market_normalization_input_invalid: known_at precedes effective")
        digest = _content_hash(self.material_hash, field="material_hash")
        quality_fingerprint = (
            _content_hash(self.quality_fingerprint, field="quality_fingerprint")
            if self.quality_fingerprint is not None
            else _stable_hash(
                {"valid": bool(self.valid), "invalid_reason": self.invalid_reason}
            )
        )
        object.__setattr__(self, "source_series_id", int(self.source_series_id))
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "market_commit_seq", int(self.market_commit_seq))
        object.__setattr__(self, "material_hash", digest)
        object.__setattr__(self, "quality_fingerprint", quality_fingerprint)
        object.__setattr__(
            self, "value", _decimal(self.value, field="value") if self.value is not None else None
        )
        object.__setattr__(
            self,
            "denominator",
            _decimal(self.denominator, field="denominator")
            if self.denominator is not None
            else None,
        )


@dataclass(frozen=True)
class NormalizedFeatureFact:
    series_id: int
    spec_id: str
    spec_hash: str
    effective_at: datetime
    known_at: datetime
    value: Optional[Decimal]
    status: NormalizedStatus | str
    reason: Optional[str]
    input_start: datetime
    input_end: datetime
    input_count: int
    input_watermark: int
    source_series_ids: tuple[int, ...]
    source_material_hashes: tuple[str, ...]
    input_fingerprint: str

    def __post_init__(self) -> None:
        if int(self.series_id) <= 0 or not str(self.spec_id or "").strip():
            raise ValueError("market_normalized_fact_invalid: identity")
        raw_status = (
            self.status.value
            if isinstance(self.status, NormalizedStatus)
            else self.status
        )
        status = NormalizedStatus(str(raw_status))
        value = _decimal(self.value, field="value") if self.value is not None else None
        if (status is NormalizedStatus.VALID) != (value is not None):
            raise ValueError("market_normalized_fact_invalid: status/value disagreement")
        effective = _utc(self.effective_at, field="effective_at")
        known = _utc(self.known_at, field="known_at")
        start = _utc(self.input_start, field="input_start")
        end = _utc(self.input_end, field="input_end")
        if known < effective or end > effective or start > end:
            raise ValueError("market_normalized_fact_invalid: causal time order")
        spec_hash = _content_hash(self.spec_hash, field="spec_hash")
        fingerprint = _content_hash(self.input_fingerprint, field="input_fingerprint")
        source_ids = tuple(sorted(set(map(int, self.source_series_ids))))
        if not source_ids or any(value <= 0 for value in source_ids):
            raise ValueError("market_normalized_fact_invalid: source series")
        input_count = int(self.input_count)
        watermark = int(self.input_watermark)
        if input_count <= 0 or watermark <= 0:
            raise ValueError("market_normalized_fact_invalid: input evidence")
        hashes = tuple(
            _content_hash(value, field="source_material_hash")
            for value in self.source_material_hashes
        )
        if not hashes:
            raise ValueError("market_normalized_fact_invalid: source witness hashes")
        if input_count < len(hashes) or len(hashes) > 3:
            raise ValueError(
                "market_normalized_fact_invalid: bounded source witness disagreement"
            )
        object.__setattr__(self, "series_id", int(self.series_id))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "effective_at", effective)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "input_start", start)
        object.__setattr__(self, "input_end", end)
        object.__setattr__(self, "spec_hash", spec_hash)
        object.__setattr__(self, "input_fingerprint", fingerprint)
        object.__setattr__(self, "input_count", input_count)
        object.__setattr__(self, "input_watermark", watermark)
        object.__setattr__(self, "source_series_ids", source_ids)
        object.__setattr__(self, "source_material_hashes", hashes)

    @property
    def material_hash(self) -> str:
        return _stable_hash(
            {
                "schema_version": NORMALIZED_MATERIAL_VERSION,
                "series_id": self.series_id,
                "spec_id": self.spec_id,
                "spec_hash": self.spec_hash,
                "effective_at": _time_string(self.effective_at),
                "known_at": _time_string(self.known_at),
                "value": _decimal_string(self.value),
                "status": self.status.value,
                "reason": self.reason,
                "input_start": _time_string(self.input_start),
                "input_end": _time_string(self.input_end),
                "input_count": self.input_count,
                "input_watermark": self.input_watermark,
                "source_series_ids": self.source_series_ids,
                "source_material_hashes": self.source_material_hashes,
                "input_fingerprint": self.input_fingerprint,
            }
        )


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal(2)


def _mean_std(values: Sequence[Decimal]) -> tuple[Decimal, Decimal]:
    with localcontext() as context:
        context.prec = 38
        mean = sum(values, Decimal(0)) / Decimal(len(values))
        variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
        return mean, variance.sqrt()


def evaluate_normalization(
    spec: NormalizationSpec,
    inputs: Sequence[NormalizationInput],
    *,
    output_series_id: int,
) -> tuple[NormalizedFeatureFact, ...]:
    """Evaluate one spec from a prefix; future inputs never alter earlier rows."""

    ordered = sorted(
        inputs,
        key=lambda row: (row.effective_at, row.known_at, row.market_commit_seq, row.material_hash),
    )
    results: list[NormalizedFeatureFact] = []
    prior_returns: list[
        tuple[NormalizationInput, NormalizationInput, Decimal]
    ] = []
    previous_valid_by_partition: dict[str, NormalizationInput] = {}
    for index, current in enumerate(ordered):
        window_start = (
            current.effective_at - timedelta(seconds=int(spec.window_seconds))
            if spec.window_seconds is not None
            else current.effective_at
        )
        prior = [
            row
            for row in ordered[:index]
            if row.effective_at >= window_start
            and row.partition_key == current.partition_key
            and row.valid
            and row.value is not None
        ]
        status = NormalizedStatus.VALID
        reason: Optional[str] = None
        value: Optional[Decimal] = None
        used = list(prior)
        if not current.valid or current.value is None:
            status = NormalizedStatus.INVALID_INPUT
            reason = current.invalid_reason or "source_input_invalid"
            used = []
        elif spec.formula is NormalizationFormula.BASIS_POINTS:
            value = current.value * TEN_THOUSAND
            used = []
        elif spec.formula is NormalizationFormula.RATIO:
            used = []
            if current.denominator is None or current.denominator == 0:
                status = NormalizedStatus.ZERO_DENOMINATOR
                reason = "denominator_is_zero_or_missing"
            else:
                value = current.value / current.denominator
        elif spec.formula in {
            NormalizationFormula.CAUSAL_PERCENTILE,
            NormalizationFormula.CAUSAL_ZSCORE,
            NormalizationFormula.TIME_OF_DAY_MEDIAN_RATIO,
        }:
            required_observations = max(
                spec.minimum_observations,
                spec.warmup_observations,
            )
            has_full_window = not bool(
                spec.parameters.get("require_full_window", True)
            ) or any(
                row.effective_at <= window_start
                and row.partition_key == current.partition_key
                and row.valid
                and row.value is not None
                for row in ordered[:index]
            )
            if len(prior) < required_observations or not has_full_window:
                status = NormalizedStatus.INSUFFICIENT_HISTORY
                reason = (
                    f"requires_{required_observations}_prior_observations_and_full_window"
                )
            else:
                prior_values = [row.value for row in prior if row.value is not None]
                if spec.formula is NormalizationFormula.CAUSAL_PERCENTILE:
                    value = Decimal(
                        sum(1 for prior_value in prior_values if prior_value <= current.value)
                    ) / Decimal(len(prior_values))
                elif spec.formula is NormalizationFormula.CAUSAL_ZSCORE:
                    mean, deviation = _mean_std(prior_values)
                    if deviation == 0:
                        status = NormalizedStatus.ZERO_VARIANCE
                        reason = "prior_window_zero_variance"
                    else:
                        value = (current.value - mean) / deviation
                else:
                    baseline = _median(prior_values)
                    if baseline == 0:
                        status = NormalizedStatus.ZERO_DENOMINATOR
                        reason = "prior_time_of_day_median_zero"
                    else:
                        value = current.value / baseline
        elif spec.formula is NormalizationFormula.VOLATILITY_ADJUSTED_RETURN:
            used = []
            previous_valid = previous_valid_by_partition.get(current.partition_key)
            if previous_valid is None or previous_valid.value is None or previous_valid.value <= 0 or current.value <= 0:
                status = NormalizedStatus.INSUFFICIENT_HISTORY
                reason = "requires_previous_positive_price"
            else:
                with localcontext() as context:
                    context.prec = 38
                    current_return = (current.value / previous_valid.value).ln()
                eligible_returns = [
                    (previous_source, source, return_value)
                    for previous_source, source, return_value in prior_returns
                    if source.effective_at >= window_start
                    and source.partition_key == current.partition_key
                ]
                used = [
                    input_row
                    for previous_source, source, _ in eligible_returns
                    for input_row in (previous_source, source)
                ]
                required_observations = max(
                    spec.minimum_observations,
                    spec.warmup_observations,
                )
                has_full_window = not bool(
                    spec.parameters.get("require_full_window", True)
                ) or any(
                    source.effective_at <= window_start
                    and source.partition_key == current.partition_key
                    for _, source, _ in prior_returns
                )
                if len(eligible_returns) < required_observations or not has_full_window:
                    status = NormalizedStatus.INSUFFICIENT_HISTORY
                    reason = (
                        f"requires_{required_observations}_prior_returns_and_full_window"
                    )
                else:
                    _, deviation = _mean_std(
                        [value for _, _, value in eligible_returns]
                    )
                    if deviation == 0:
                        status = NormalizedStatus.ZERO_VARIANCE
                        reason = "prior_return_window_zero_variance"
                    else:
                        value = current_return / deviation
                prior_returns.append((previous_valid, current, current_return))
        else:
            raise AssertionError(spec.formula)

        if current.valid and current.value is not None:
            previous_valid_by_partition[current.partition_key] = current
        evidence_by_identity = {
            (row.source_series_id, row.material_hash): row
            for row in [*used, current]
        }
        evidence = sorted(
            evidence_by_identity.values(),
            key=lambda row: (
                row.effective_at, row.known_at, row.market_commit_seq, row.material_hash
            ),
        )
        all_hashes = tuple(row.material_hash for row in evidence)
        witness_hashes = tuple(
            dict.fromkeys(
                (evidence[0].material_hash, current.material_hash, evidence[-1].material_hash)
            )
        )
        known_at = max(row.known_at for row in evidence)
        input_start = min(row.effective_at for row in evidence)
        input_fingerprint = _stable_hash(
            {
                "schema_version": "market.normalization_input.v1",
                "spec_hash": spec.spec_hash,
                "source_series_ids": sorted({row.source_series_id for row in evidence}),
                "source_material_hashes": all_hashes,
                "source_quality_fingerprints": tuple(
                    row.quality_fingerprint for row in evidence
                ),
            }
        )
        results.append(
            NormalizedFeatureFact(
                series_id=output_series_id,
                spec_id=spec.spec_id,
                spec_hash=spec.spec_hash,
                effective_at=current.effective_at,
                known_at=known_at,
                value=value if status is NormalizedStatus.VALID else None,
                status=status,
                reason=reason,
                input_start=input_start,
                input_end=current.effective_at,
                input_count=len(evidence),
                input_watermark=max(row.market_commit_seq for row in evidence),
                source_series_ids=tuple(row.source_series_id for row in evidence),
                source_material_hashes=witness_hashes,
                input_fingerprint=input_fingerprint,
            )
        )
    return tuple(results)


__all__ = [
    "NORMALIZATION_SPEC_VERSION",
    "NORMALIZED_FACT_VERSION",
    "NormalizationFormula",
    "NormalizationInput",
    "NormalizationSpec",
    "NormalizedFeatureFact",
    "NormalizedStatus",
    "evaluate_normalization",
]

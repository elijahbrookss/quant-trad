"""Provider-neutral contracts for bounded exact-numeric fact acquisition."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Protocol

from market_data.contracts import InstrumentRole
from market_data.fact_registry import get_fact_contract


NUMERIC_FACT_MANIFEST_VERSION = "market.numeric_fact_sources.v1"


class NumericFactProviderError(RuntimeError):
    """Provider-neutral acquisition failure that must remain visible to operators."""


class NumericFactProviderContractError(NumericFactProviderError):
    """A provider returned a batch that disagrees with the bounded request."""


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"numeric_provider_invalid: {field_name} must be datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal | int | str, *, field_name: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(
            f"numeric_provider_invalid: {field_name} forbids binary floating point"
        )
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"numeric_provider_invalid: {field_name} must be an exact decimal"
        ) from exc
    if not parsed.is_finite():
        raise ValueError(f"numeric_provider_invalid: {field_name} must be finite")
    return Decimal(0) if parsed.is_zero() else parsed


@dataclass(frozen=True)
class NumericFactBinding:
    """One data-driven mapping from a source adapter to a canonical fact series."""

    manifest_id: str
    manifest_hash: str
    id: str
    enabled: bool
    adapter: str
    instrument_id: str
    instrument_role: str
    fact_type: str
    contract_version: str
    unit: str
    dimensions: Mapping[str, Any]
    endpoint_ref: str
    source: Mapping[str, Any]
    schedule: Mapping[str, Any]
    quality_policy: Mapping[str, Any]
    risk: Mapping[str, Any]
    config: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError(
                "numeric_fact_manifest_invalid: binding enabled must be bool"
            )
        values = {
            "manifest_id": self.manifest_id,
            "manifest_hash": self.manifest_hash,
            "id": self.id,
            "adapter": self.adapter,
            "instrument_id": self.instrument_id,
            "instrument_role": self.instrument_role,
            "fact_type": self.fact_type,
            "contract_version": self.contract_version,
            "unit": self.unit,
            "endpoint_ref": self.endpoint_ref,
        }
        normalized = {key: str(value or "").strip() for key, value in values.items()}
        if any(not value for value in normalized.values()):
            raise ValueError("numeric_fact_manifest_invalid: binding identity is incomplete")
        fact_type = normalized["fact_type"].lower()
        contract = get_fact_contract(fact_type)
        contract.validate(
            contract_version=normalized["contract_version"],
            timeframe_seconds=None,
        )
        unit, dimensions = contract.validate_numeric_value(
            value=Decimal(1),
            unit=normalized["unit"],
            dimensions=self.dimensions,
        )
        if not contract.uses_exact_numeric_storage:
            raise ValueError(
                f"numeric_fact_manifest_invalid: fact_type={fact_type} is not exact numeric"
            )
        if (
            len(normalized["manifest_hash"]) != 64
            or any(
                character not in "0123456789abcdefABCDEF"
                for character in normalized["manifest_hash"]
            )
        ):
            raise ValueError("numeric_fact_manifest_invalid: manifest hash")
        object.__setattr__(self, "manifest_id", normalized["manifest_id"])
        object.__setattr__(self, "manifest_hash", normalized["manifest_hash"].lower())
        object.__setattr__(self, "id", normalized["id"])
        object.__setattr__(self, "enabled", self.enabled)
        object.__setattr__(self, "adapter", normalized["adapter"].lower())
        object.__setattr__(self, "instrument_id", normalized["instrument_id"])
        try:
            instrument_role = InstrumentRole(
                normalized["instrument_role"].lower()
            ).value
        except ValueError as exc:
            raise ValueError(
                "numeric_fact_manifest_invalid: instrument_role must be "
                "primary, underlying, benchmark, or explicit"
            ) from exc
        object.__setattr__(self, "instrument_role", instrument_role)
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "contract_version", normalized["contract_version"])
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "dimensions", dimensions)
        object.__setattr__(self, "endpoint_ref", normalized["endpoint_ref"])
        object.__setattr__(self, "source", dict(self.source or {}))
        schedule = dict(self.schedule or {})
        if set(schedule) != {
            "expected_update_interval_seconds",
            "deviation_threshold_basis_points",
        }:
            raise ValueError(
                "numeric_fact_manifest_invalid: schedule fields must match the v1 schema"
            )
        for key in schedule:
            value = schedule[key]
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise ValueError(
                    f"numeric_fact_manifest_invalid: schedule {key} must be "
                    "a nonnegative integer or null"
                )
        quality_policy = dict(self.quality_policy or {})
        if set(quality_policy) != {"max_staleness_seconds", "stale_behavior"}:
            raise ValueError(
                "numeric_fact_manifest_invalid: quality_policy fields must match the v1 schema"
            )
        if (
            isinstance(quality_policy["max_staleness_seconds"], bool)
            or not isinstance(quality_policy["max_staleness_seconds"], int)
            or quality_policy["max_staleness_seconds"] <= 0
            or str(quality_policy["stale_behavior"]).strip().lower() != "gap"
        ):
            raise ValueError("numeric_fact_manifest_invalid: quality_policy")
        quality_policy["max_staleness_seconds"] = int(
            quality_policy["max_staleness_seconds"]
        )
        quality_policy["stale_behavior"] = "gap"
        risk = dict(self.risk or {})
        if set(risk) != {
            "official_catalog_url",
            "market_risk_tier",
            "deprecation_status",
            "verified_at",
        } or any(not str(value or "").strip() for value in risk.values()):
            raise ValueError(
                "numeric_fact_manifest_invalid: risk fields must match the v1 schema"
            )
        if not str(risk["official_catalog_url"]).startswith("https://"):
            raise ValueError(
                "numeric_fact_manifest_invalid: official catalog URL must use HTTPS"
            )
        object.__setattr__(self, "schedule", schedule)
        object.__setattr__(self, "quality_policy", quality_policy)
        object.__setattr__(
            self,
            "risk",
            {key: str(value).strip() for key, value in risk.items()},
        )
        object.__setattr__(self, "config", dict(self.config or {}))


@dataclass(frozen=True)
class NumericFactManifest:
    schema_version: str
    id: str
    enabled: bool
    manifest_hash: str
    bindings: tuple[NumericFactBinding, ...]
    path: str

    def binding(self, binding_id: str, *, require_enabled: bool = True) -> NumericFactBinding:
        matches = [item for item in self.bindings if item.id == str(binding_id).strip()]
        if len(matches) != 1:
            raise ValueError(
                f"numeric_fact_binding_unknown: manifest={self.id} binding={binding_id}"
            )
        binding = matches[0]
        if require_enabled and (not self.enabled or not binding.enabled):
            raise RuntimeError(
                f"numeric_fact_binding_disabled: manifest={self.id} binding={binding.id}"
            )
        return binding


def load_numeric_fact_manifest(path: str | Path) -> NumericFactManifest:
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"numeric_fact_manifest_invalid: path={manifest_path}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError("numeric_fact_manifest_invalid: root must be an object")
    allowed = {"schema_version", "id", "enabled", "bindings"}
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        raise ValueError(
            "numeric_fact_manifest_invalid: unexpected root fields "
            + ",".join(unexpected)
        )
    if payload.get("schema_version") != NUMERIC_FACT_MANIFEST_VERSION:
        raise ValueError("numeric_fact_manifest_invalid: unsupported schema_version")
    manifest_id = str(payload.get("id") or "").strip()
    if not manifest_id or not isinstance(payload.get("enabled"), bool):
        raise ValueError("numeric_fact_manifest_invalid: id/enabled")
    raw_bindings = payload.get("bindings")
    if not isinstance(raw_bindings, list) or not raw_bindings:
        raise ValueError("numeric_fact_manifest_invalid: bindings are required")
    manifest_hash = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    bindings: list[NumericFactBinding] = []
    for raw in raw_bindings:
        if not isinstance(raw, dict):
            raise ValueError("numeric_fact_manifest_invalid: binding must be an object")
        required = {
            "id",
            "enabled",
            "adapter",
            "instrument_id",
            "instrument_role",
            "fact_type",
            "contract_version",
            "unit",
            "dimensions",
            "endpoint_ref",
            "source",
            "schedule",
            "quality_policy",
            "risk",
            "config",
        }
        if set(raw) != required:
            raise ValueError(
                "numeric_fact_manifest_invalid: binding fields must match the v1 schema"
            )
        if not isinstance(raw["enabled"], bool):
            raise ValueError("numeric_fact_manifest_invalid: binding enabled must be bool")
        bindings.append(
            NumericFactBinding(
                manifest_id=manifest_id,
                manifest_hash=manifest_hash,
                **raw,
            )
        )
    ids = [item.id for item in bindings]
    if len(ids) != len(set(ids)):
        raise ValueError("numeric_fact_manifest_invalid: duplicate binding id")
    return NumericFactManifest(
        schema_version=NUMERIC_FACT_MANIFEST_VERSION,
        id=manifest_id,
        enabled=bool(payload["enabled"]),
        manifest_hash=manifest_hash,
        bindings=tuple(bindings),
        path=str(manifest_path),
    )


@dataclass(frozen=True)
class NumericAcquisitionBudget:
    max_requests: int
    max_logs: int
    max_blocks: int
    max_retries: int = 2

    def __post_init__(self) -> None:
        for field_name in ("max_requests", "max_logs", "max_blocks"):
            raw_value = getattr(self, field_name)
            if (
                isinstance(raw_value, bool)
                or not isinstance(raw_value, int)
                or raw_value <= 0
            ):
                raise ValueError(
                    f"numeric_acquisition_budget_invalid: {field_name} must be positive integer"
                )
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 0
        ):
            raise ValueError(
                "numeric_acquisition_budget_invalid: max_retries must be nonnegative integer"
            )


@dataclass(frozen=True)
class ProviderNumericObservation:
    value: Decimal | int | str
    raw_value: str
    effective_at: datetime
    effective_at_method: str
    source_published_at: datetime | None
    known_at: datetime
    known_at_method: str
    source_event_key: str
    source_event_group_key: str | None
    source_event_component_key: str | None
    provenance: Mapping[str, Any]
    source_event_material: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        value = _decimal(self.value, field_name="value")
        raw_value = str(self.raw_value or "").strip()
        event_key = str(self.source_event_key or "").strip()
        effective_method = str(self.effective_at_method or "").strip().lower()
        known_method = str(self.known_at_method or "").strip().lower()
        if not raw_value or not event_key or not effective_method or not known_method:
            raise ValueError("numeric_provider_invalid: observation identity")
        effective_at = _utc(self.effective_at, field_name="effective_at")
        known_at = _utc(self.known_at, field_name="known_at")
        published = (
            _utc(self.source_published_at, field_name="source_published_at")
            if self.source_published_at is not None
            else None
        )
        if known_at < effective_at or (published is not None and known_at < published):
            raise ValueError("numeric_provider_invalid: causal time order")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "raw_value", raw_value)
        object.__setattr__(self, "effective_at", effective_at)
        object.__setattr__(self, "effective_at_method", effective_method)
        object.__setattr__(self, "source_published_at", published)
        object.__setattr__(self, "known_at", known_at)
        object.__setattr__(self, "known_at_method", known_method)
        object.__setattr__(self, "source_event_key", event_key)
        object.__setattr__(
            self,
            "source_event_group_key",
            str(self.source_event_group_key or "").strip() or None,
        )
        object.__setattr__(
            self,
            "source_event_component_key",
            str(self.source_event_component_key or "").strip() or None,
        )
        object.__setattr__(self, "provenance", dict(self.provenance or {}))
        object.__setattr__(
            self,
            "source_event_material",
            dict(self.source_event_material or self.provenance or {}),
        )

    @property
    def source_event_material_hash(self) -> str:
        return hashlib.sha256(
            _canonical_json(self.source_event_material).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class ProviderNumericGap:
    classification: str
    start: datetime
    end: datetime
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        classification = str(self.classification or "").strip().lower()
        start = _utc(self.start, field_name="gap.start")
        end = _utc(self.end, field_name="gap.end")
        if not classification or end <= start:
            raise ValueError("numeric_provider_invalid: gap identity/range")
        object.__setattr__(self, "classification", classification)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "evidence", dict(self.evidence or {}))


@dataclass(frozen=True)
class ProviderNumericBatch:
    observations: tuple[ProviderNumericObservation, ...]
    gaps: tuple[ProviderNumericGap, ...]
    range_start: datetime
    range_end: datetime
    source_position_start: str
    source_position_end: str
    source_position_head: str | None
    status: str
    capabilities: Mapping[str, Any]
    request: Mapping[str, Any]
    budget_requests_used: int = 0
    budget_logs_used: int = 0
    budget_blocks_scanned: int = 0

    def __post_init__(self) -> None:
        observations = tuple(self.observations or ())
        gaps = tuple(self.gaps or ())
        range_start = _utc(self.range_start, field_name="batch.range_start")
        range_end = _utc(self.range_end, field_name="batch.range_end")
        status = str(self.status or "").strip().lower()
        source_position_start = str(self.source_position_start or "").strip()
        source_position_end = str(self.source_position_end or "").strip()
        source_position_head = str(self.source_position_head or "").strip() or None
        if range_end <= range_start:
            raise ValueError("numeric_provider_invalid: batch range")
        if status not in {"complete", "partial", "failed"}:
            raise ValueError("numeric_provider_invalid: batch status")
        if not source_position_start or not source_position_end:
            raise ValueError("numeric_provider_invalid: batch source positions")
        if status == "complete" and gaps:
            raise ValueError("numeric_provider_invalid: complete batch has gaps")
        if status in {"partial", "failed"} and not gaps:
            raise ValueError(
                "numeric_provider_invalid: incomplete batch requires gap evidence"
            )
        if status == "failed" and observations:
            raise ValueError("numeric_provider_invalid: failed batch has observations")
        event_keys = [item.source_event_key for item in observations]
        if len(event_keys) != len(set(event_keys)):
            raise ValueError("numeric_provider_invalid: duplicate source event")
        if any(
            not range_start <= item.effective_at < range_end
            for item in observations
        ):
            raise ValueError("numeric_provider_invalid: observation outside batch range")
        for field_name in (
            "budget_requests_used",
            "budget_logs_used",
            "budget_blocks_scanned",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    f"numeric_provider_invalid: {field_name} must be "
                    "nonnegative integer"
                )
            object.__setattr__(self, field_name, value)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "gaps", gaps)
        object.__setattr__(self, "range_start", range_start)
        object.__setattr__(self, "range_end", range_end)
        object.__setattr__(self, "source_position_start", source_position_start)
        object.__setattr__(self, "source_position_end", source_position_end)
        object.__setattr__(self, "source_position_head", source_position_head)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "capabilities", dict(self.capabilities or {}))
        object.__setattr__(self, "request", dict(self.request or {}))


class NumericFactProvider(Protocol):
    adapter_id: str

    def fetch_current(
        self,
        binding: NumericFactBinding,
        *,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        ...

    def fetch_history(
        self,
        binding: NumericFactBinding,
        *,
        start: datetime,
        end: datetime,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        ...


__all__ = [
    "NUMERIC_FACT_MANIFEST_VERSION",
    "NumericAcquisitionBudget",
    "NumericFactBinding",
    "NumericFactManifest",
    "NumericFactProvider",
    "NumericFactProviderContractError",
    "NumericFactProviderError",
    "ProviderNumericBatch",
    "ProviderNumericGap",
    "ProviderNumericObservation",
    "load_numeric_fact_manifest",
]

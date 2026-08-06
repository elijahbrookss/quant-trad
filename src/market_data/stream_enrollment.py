"""Declarative collector enrollment and fail-closed safety contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .structure import ProductContract


STREAM_ENROLLMENT_MANIFEST_VERSION = "market.stream_enrollment_manifest.v1"
COLLECTOR_SAFETY_POLICY_VERSION = "market.collector_safety_policy.v1"
QUALIFICATION_EVIDENCE_VERSION = "market.qualification_evidence.v1"
SAFETY_HALT_VERSION = "market.safety_halt.v1"


def _hash(value: Any) -> str:
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


@dataclass(frozen=True)
class CollectorSafetyPolicy:
    schema_version: str
    policy_id: str
    warning_free_bytes: int
    critical_free_bytes: int
    warning_spool_ratio: float
    critical_spool_ratio: float
    warning_projected_exhaustion_hours: int
    critical_projected_exhaustion_hours: int
    evaluation_interval_seconds: int = 30
    policy_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != COLLECTOR_SAFETY_POLICY_VERSION:
            raise ValueError("unsupported collector safety policy schema")
        object.__setattr__(self, "policy_id", _required(self.policy_id, field="policy_id"))
        warning_free = int(self.warning_free_bytes)
        critical_free = int(self.critical_free_bytes)
        if critical_free <= 0 or warning_free <= critical_free:
            raise ValueError("safety free-byte thresholds must be positive and ordered")
        warning_spool = float(self.warning_spool_ratio)
        critical_spool = float(self.critical_spool_ratio)
        if not 0 < warning_spool < critical_spool < 1:
            raise ValueError("safety spool thresholds must be ordered within (0, 1)")
        warning_hours = int(self.warning_projected_exhaustion_hours)
        critical_hours = int(self.critical_projected_exhaustion_hours)
        if critical_hours <= 0 or warning_hours <= critical_hours:
            raise ValueError("safety exhaustion thresholds must be positive and ordered")
        interval = int(self.evaluation_interval_seconds)
        if interval < 5:
            raise ValueError("safety evaluation interval must be at least five seconds")
        object.__setattr__(self, "warning_free_bytes", warning_free)
        object.__setattr__(self, "critical_free_bytes", critical_free)
        object.__setattr__(self, "warning_spool_ratio", warning_spool)
        object.__setattr__(self, "critical_spool_ratio", critical_spool)
        object.__setattr__(self, "warning_projected_exhaustion_hours", warning_hours)
        object.__setattr__(self, "critical_projected_exhaustion_hours", critical_hours)
        object.__setattr__(self, "evaluation_interval_seconds", interval)
        expected = _hash(self._material())
        if self.policy_hash and self.policy_hash != expected:
            raise ValueError("collector_safety_policy_hash_mismatch")
        object.__setattr__(self, "policy_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "policy_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "policy_hash": self.policy_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CollectorSafetyPolicy":
        return cls(**dict(raw))


@dataclass(frozen=True)
class StreamEnrollment:
    enrollment_id: str
    fleet_id: str
    instrument_id: str
    product_type: str
    provider: str
    venue: str
    channels: tuple[str, ...]
    auth_mode: str
    adapter_version: str
    contract_version: str
    max_spool_bytes: int
    max_segment_bytes: int
    continuous: bool
    product_contract: ProductContract

    def __post_init__(self) -> None:
        for name in (
            "enrollment_id",
            "fleet_id",
            "instrument_id",
            "product_type",
            "provider",
            "venue",
            "auth_mode",
            "adapter_version",
            "contract_version",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), field=name))
        channels = tuple(sorted({_required(value, field="channel") for value in self.channels}))
        if not channels:
            raise ValueError("stream enrollment requires channels")
        object.__setattr__(self, "channels", channels)
        spool = int(self.max_spool_bytes)
        segment = int(self.max_segment_bytes)
        if spool <= 0 or segment <= 0 or segment > spool:
            raise ValueError("stream enrollment spool limits are invalid")
        object.__setattr__(self, "max_spool_bytes", spool)
        object.__setattr__(self, "max_segment_bytes", segment)
        if not isinstance(self.product_contract, ProductContract):
            raise TypeError("stream enrollment requires ProductContract")
        if self.product_contract.provider_product_id not in self.enrollment_id:
            raise ValueError("enrollment identity must include provider product id")

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["product_contract"]["provider_size_unit"] = (
            self.product_contract.provider_size_unit.value
        )
        result["product_contract"]["contract_size"] = (
            str(self.product_contract.contract_size)
            if self.product_contract.contract_size is not None
            else None
        )
        return result


@dataclass(frozen=True)
class StreamEnrollmentManifest:
    schema_version: str
    fleet_id: str
    safety_policy: CollectorSafetyPolicy
    enrollments: tuple[StreamEnrollment, ...]
    manifest_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != STREAM_ENROLLMENT_MANIFEST_VERSION:
            raise ValueError("unsupported stream enrollment manifest schema")
        fleet_id = _required(self.fleet_id, field="fleet_id")
        object.__setattr__(self, "fleet_id", fleet_id)
        rows = tuple(self.enrollments)
        if not rows or len({row.enrollment_id for row in rows}) != len(rows):
            raise ValueError("stream enrollments must be non-empty and unique")
        if any(row.fleet_id != fleet_id for row in rows):
            raise ValueError("stream enrollment fleet identity mismatch")
        object.__setattr__(self, "enrollments", rows)
        expected = _hash(self._material())
        if self.manifest_hash and self.manifest_hash != expected:
            raise ValueError("stream_enrollment_manifest_hash_mismatch")
        object.__setattr__(self, "manifest_hash", expected)

    def _material(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "fleet_id": self.fleet_id,
            "safety_policy": self.safety_policy.to_dict(),
            "enrollments": [row.to_dict() for row in self.enrollments],
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "manifest_hash": self.manifest_hash}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "StreamEnrollmentManifest":
        fleet_id = _required(raw.get("fleet_id"), field="fleet_id")
        enrollments: list[StreamEnrollment] = []
        for row in raw.get("enrollments") or ():
            values = dict(row)
            declared_fleet_id = str(values.pop("fleet_id", fleet_id))
            if declared_fleet_id != fleet_id:
                raise ValueError("stream enrollment fleet identity mismatch")
            contract_raw = dict(values.pop("product_contract"))
            contract_raw["contract_size"] = (
                Decimal(str(contract_raw["contract_size"]))
                if contract_raw.get("contract_size") is not None
                else None
            )
            enrollments.append(
                StreamEnrollment(
                    fleet_id=fleet_id,
                    channels=tuple(values.pop("channels", ())),
                    product_contract=ProductContract(**contract_raw),
                    **values,
                )
            )
        return cls(
            schema_version=str(raw.get("schema_version") or ""),
            fleet_id=fleet_id,
            safety_policy=CollectorSafetyPolicy.from_dict(raw.get("safety_policy") or {}),
            enrollments=tuple(enrollments),
            manifest_hash=str(raw.get("manifest_hash") or ""),
        )


def load_stream_enrollment_manifest(path: Path | str) -> StreamEnrollmentManifest:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("stream enrollment manifest root must be an object")
    return StreamEnrollmentManifest.from_dict(raw)


@dataclass(frozen=True)
class QualificationEvidence:
    schema_version: str
    definition_id: str
    fleet_id: str
    evaluated_at: datetime
    policy_hash: str
    adapter_supported: bool
    product_contract_registered: bool
    storage_writable: bool
    filesystem_free_bytes: int
    spool_bytes: int
    max_spool_bytes: int
    active_halt_scopes: tuple[str, ...]
    reasons: tuple[str, ...]
    evidence_hash: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != QUALIFICATION_EVIDENCE_VERSION:
            raise ValueError("unsupported qualification evidence schema")
        for name in ("definition_id", "fleet_id", "policy_hash"):
            object.__setattr__(self, name, _required(getattr(self, name), field=name))
        evaluated = self.evaluated_at
        if evaluated.tzinfo is None:
            evaluated = evaluated.replace(tzinfo=UTC)
        object.__setattr__(self, "evaluated_at", evaluated.astimezone(UTC))
        object.__setattr__(self, "active_halt_scopes", tuple(sorted(set(self.active_halt_scopes))))
        object.__setattr__(self, "reasons", tuple(sorted(set(self.reasons))))
        expected = _hash(self._material())
        if self.evidence_hash and self.evidence_hash != expected:
            raise ValueError("qualification_evidence_hash_mismatch")
        object.__setattr__(self, "evidence_hash", expected)

    @property
    def qualified(self) -> bool:
        return not self.reasons

    def _material(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if key != "evidence_hash"}

    def to_dict(self) -> dict[str, Any]:
        return {**self._material(), "qualified": self.qualified, "evidence_hash": self.evidence_hash}


@dataclass(frozen=True)
class SafetyHalt:
    schema_version: str
    halt_id: str
    scope_type: str
    scope_id: str
    severity: str
    reason: str
    policy_hash: str
    evidence: Mapping[str, Any]
    occurred_at: datetime

    def __post_init__(self) -> None:
        if self.schema_version != SAFETY_HALT_VERSION:
            raise ValueError("unsupported safety halt schema")
        for name in ("halt_id", "scope_id", "reason", "policy_hash"):
            object.__setattr__(self, name, _required(getattr(self, name), field=name))
        scope = str(self.scope_type or "").lower()
        if scope not in {"global", "fleet", "stream"}:
            raise ValueError("safety halt scope is invalid")
        severity = str(self.severity or "").lower()
        if severity not in {"warning", "critical", "operator"}:
            raise ValueError("safety halt severity is invalid")
        object.__setattr__(self, "scope_type", scope)
        object.__setattr__(self, "severity", severity)


__all__ = [
    "COLLECTOR_SAFETY_POLICY_VERSION",
    "QUALIFICATION_EVIDENCE_VERSION",
    "SAFETY_HALT_VERSION",
    "STREAM_ENROLLMENT_MANIFEST_VERSION",
    "CollectorSafetyPolicy",
    "QualificationEvidence",
    "SafetyHalt",
    "StreamEnrollment",
    "StreamEnrollmentManifest",
    "load_stream_enrollment_manifest",
]

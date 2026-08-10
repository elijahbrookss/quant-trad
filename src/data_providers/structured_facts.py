"""Data-driven bindings for typed, atomic structured provider observations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from market_data.contracts import InstrumentRole
from market_data.fact_registry import get_fact_contract, get_fact_payload_schema


STRUCTURED_FACT_MANIFEST_VERSION = "market.structured_fact_sources.v2"


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


@dataclass(frozen=True)
class StructuredFactBinding:
    """One reviewed provider mapping to a registered canonical payload schema."""

    manifest_id: str
    manifest_hash: str
    id: str
    enabled: bool
    adapter: str
    instrument_id: str
    instrument_role: str
    fact_type: str
    payload_schema_id: str
    dimensions: Mapping[str, Any]
    endpoint_ref: str
    source: Mapping[str, Any]
    schedule: Mapping[str, Any]
    quality_policy: Mapping[str, Any]
    risk: Mapping[str, Any]
    config: Mapping[str, Any]
    canonical_instrument: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise ValueError(
                "structured_fact_manifest_invalid: binding enabled must be bool"
            )
        string_fields = (
            "manifest_id",
            "manifest_hash",
            "id",
            "adapter",
            "instrument_id",
            "instrument_role",
            "fact_type",
            "payload_schema_id",
            "endpoint_ref",
        )
        normalized = {
            field_name: str(getattr(self, field_name) or "").strip()
            for field_name in string_fields
        }
        if any(not value for value in normalized.values()):
            raise ValueError(
                "structured_fact_manifest_invalid: binding identity is incomplete"
            )
        if len(normalized["manifest_hash"]) != 64 or any(
            character not in "0123456789abcdefABCDEF"
            for character in normalized["manifest_hash"]
        ):
            raise ValueError("structured_fact_manifest_invalid: manifest hash")
        fact_type = normalized["fact_type"].lower()
        payload_schema_id = normalized["payload_schema_id"].lower()
        schema = get_fact_payload_schema(payload_schema_id)
        if schema.fact_type != fact_type:
            raise ValueError(
                "structured_fact_manifest_invalid: payload schema and fact type disagree"
            )
        contract = get_fact_contract(fact_type)
        contract.validate(
            contract_version=payload_schema_id,
            timeframe_seconds=None,
        )
        dimensions = contract.normalize_dimensions(self.dimensions)
        if contract.uses_exact_numeric_storage:
            raise ValueError(
                "structured_fact_manifest_invalid: exact numeric facts use the numeric manifest"
            )
        try:
            instrument_role = InstrumentRole(
                normalized["instrument_role"].lower()
            ).value
        except ValueError as exc:
            raise ValueError(
                "structured_fact_manifest_invalid: instrument_role must be "
                "primary, underlying, benchmark, or explicit"
            ) from exc
        source = dict(self.source or {})
        if set(source) != {"provider", "venue", "source_kind", "adapter_version"}:
            raise ValueError(
                "structured_fact_manifest_invalid: source fields must match the v1 schema"
            )
        if any(not str(value or "").strip() for value in source.values()):
            raise ValueError("structured_fact_manifest_invalid: source identity")
        schedule = dict(self.schedule or {})
        if set(schedule) != {
            "expected_update_interval_seconds",
            "poll_interval_seconds",
        }:
            raise ValueError(
                "structured_fact_manifest_invalid: schedule fields must match the v1 schema"
            )
        for field_name, minimum in (
            ("expected_update_interval_seconds", 1),
            ("poll_interval_seconds", 10),
        ):
            value = schedule[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(
                    "structured_fact_manifest_invalid: "
                    f"schedule {field_name} must be an integer >= {minimum}"
                )
        quality = dict(self.quality_policy or {})
        if set(quality) != {"max_staleness_seconds", "stale_behavior"}:
            raise ValueError(
                "structured_fact_manifest_invalid: quality_policy fields must match the v1 schema"
            )
        if (
            isinstance(quality["max_staleness_seconds"], bool)
            or not isinstance(quality["max_staleness_seconds"], int)
            or quality["max_staleness_seconds"] <= 0
            or str(quality["stale_behavior"] or "").strip().lower() != "gap"
        ):
            raise ValueError("structured_fact_manifest_invalid: quality_policy")
        risk = dict(self.risk or {})
        if set(risk) != {
            "official_catalog_url",
            "market_risk_tier",
            "deprecation_status",
            "verified_at",
        } or any(not str(value or "").strip() for value in risk.values()):
            raise ValueError(
                "structured_fact_manifest_invalid: risk fields must match the v1 schema"
            )
        if not str(risk["official_catalog_url"]).startswith("https://data.chain.link/"):
            raise ValueError(
                "structured_fact_manifest_invalid: official catalog URL is required"
            )
        object.__setattr__(self, "manifest_id", normalized["manifest_id"])
        object.__setattr__(self, "manifest_hash", normalized["manifest_hash"].lower())
        object.__setattr__(self, "id", normalized["id"])
        object.__setattr__(self, "adapter", normalized["adapter"].lower())
        object.__setattr__(self, "instrument_id", normalized["instrument_id"])
        object.__setattr__(self, "instrument_role", instrument_role)
        object.__setattr__(self, "fact_type", fact_type)
        object.__setattr__(self, "payload_schema_id", payload_schema_id)
        object.__setattr__(self, "dimensions", dimensions)
        object.__setattr__(self, "endpoint_ref", normalized["endpoint_ref"])
        object.__setattr__(self, "source", {key: str(value).strip() for key, value in source.items()})
        object.__setattr__(self, "schedule", schedule)
        object.__setattr__(
            self,
            "quality_policy",
            {
                "max_staleness_seconds": int(quality["max_staleness_seconds"]),
                "stale_behavior": "gap",
            },
        )
        object.__setattr__(self, "risk", {key: str(value).strip() for key, value in risk.items()})
        object.__setattr__(self, "config", dict(self.config or {}))
        object.__setattr__(
            self, "canonical_instrument", dict(self.canonical_instrument or {})
        )


@dataclass(frozen=True)
class StructuredFactManifest:
    schema_version: str
    id: str
    enabled: bool
    manifest_hash: str
    bindings: tuple[StructuredFactBinding, ...]
    path: str

    def binding(
        self, binding_id: str, *, require_enabled: bool = True
    ) -> StructuredFactBinding:
        matches = [item for item in self.bindings if item.id == str(binding_id).strip()]
        if len(matches) != 1:
            raise ValueError(
                f"structured_fact_binding_unknown: manifest={self.id} binding={binding_id}"
            )
        binding = matches[0]
        if require_enabled and (not self.enabled or not binding.enabled):
            raise RuntimeError(
                f"structured_fact_binding_disabled: manifest={self.id} binding={binding.id}"
            )
        return binding


def load_structured_fact_manifest(path: str | Path) -> StructuredFactManifest:
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"structured_fact_manifest_invalid: path={manifest_path}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError("structured_fact_manifest_invalid: root must be an object")
    if set(payload) != {"schema_version", "id", "enabled", "bindings"}:
        raise ValueError(
            "structured_fact_manifest_invalid: root fields must match the v1 schema"
        )
    if payload.get("schema_version") != STRUCTURED_FACT_MANIFEST_VERSION:
        raise ValueError(
            "structured_fact_manifest_invalid: unsupported schema_version"
        )
    manifest_id = str(payload.get("id") or "").strip()
    if not manifest_id or not isinstance(payload.get("enabled"), bool):
        raise ValueError("structured_fact_manifest_invalid: id/enabled")
    raw_bindings = payload.get("bindings")
    if not isinstance(raw_bindings, list) or not raw_bindings:
        raise ValueError("structured_fact_manifest_invalid: bindings are required")
    manifest_hash = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    required = {
        "id",
        "enabled",
        "adapter",
        "instrument_id",
        "instrument_role",
        "canonical_instrument",
        "fact_type",
        "payload_schema_id",
        "dimensions",
        "endpoint_ref",
        "source",
        "schedule",
        "quality_policy",
        "risk",
        "config",
    }
    bindings: list[StructuredFactBinding] = []
    for raw in raw_bindings:
        if not isinstance(raw, dict) or set(raw) != required:
            raise ValueError(
                "structured_fact_manifest_invalid: binding fields must match the v2 schema"
            )
        binding = StructuredFactBinding(
            manifest_id=manifest_id,
            manifest_hash=manifest_hash,
            **raw,
        )
        if (
            not binding.canonical_instrument
            or binding.canonical_instrument.get("id") != binding.instrument_id
        ):
            raise ValueError(
                "structured_fact_manifest_invalid: reviewed canonical instrument "
                "metadata is required"
            )
        bindings.append(binding)
    ids = [binding.id for binding in bindings]
    if len(ids) != len(set(ids)):
        raise ValueError("structured_fact_manifest_invalid: duplicate binding id")
    return StructuredFactManifest(
        schema_version=STRUCTURED_FACT_MANIFEST_VERSION,
        id=manifest_id,
        enabled=bool(payload["enabled"]),
        manifest_hash=manifest_hash,
        bindings=tuple(bindings),
        path=str(manifest_path),
    )


__all__ = [
    "STRUCTURED_FACT_MANIFEST_VERSION",
    "StructuredFactBinding",
    "StructuredFactManifest",
    "load_structured_fact_manifest",
]

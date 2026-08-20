"""System-derived collector qualification and storage safety evaluation."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from market_data.archive import spool_backlog_bytes
from market_data.stream_enrollment import (
    QUALIFICATION_EVIDENCE_VERSION,
    CollectorSafetyPolicy,
    QualificationEvidence,
)

from ..storage.repos.market_structure import PostgresMarketStructureRepository


@dataclass(frozen=True)
class SafetyEvaluation:
    severity: str
    reasons: tuple[str, ...]
    evidence: Mapping[str, Any]


def evaluate_collector_safety(
    *,
    definition: Mapping[str, Any],
    repository: PostgresMarketStructureRepository,
    adapter_supported: bool,
    storage_root: Path,
) -> tuple[QualificationEvidence, SafetyEvaluation]:
    config = dict(definition.get("config") or {})
    fleet_id = str(config.get("fleet_id") or "").strip()
    if not fleet_id:
        raise ValueError("collector_qualification_invalid: fleet_id is absent")
    policy = CollectorSafetyPolicy.from_dict(config.get("safety_policy") or {})
    definition_id = str(definition["id"])
    spool_root = storage_root / "spool"
    storage_root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(storage_root)
    spool_bytes = spool_backlog_bytes(spool_root, definition_id=definition_id)
    max_spool_bytes = int(definition["max_spool_bytes"])
    spool_ratio = spool_bytes / max_spool_bytes
    growth = repository.stream_storage_growth(definition_id=definition_id)
    bytes_per_hour = float(growth["bytes_per_hour"])
    projected_exhaustion_hours = (
        usage.free / bytes_per_hour if bytes_per_hour > 0 else None
    )
    writable = os.access(storage_root, os.W_OK)
    halts = repository.active_safety_halts(
        fleet_id=fleet_id,
        definition_id=definition_id,
    )
    product_registered = True
    try:
        repository.get_product_contract(
            str(config.get("product_definition_version_id") or "")
        )
    except (TypeError, ValueError):
        product_registered = False

    reasons: list[str] = []
    if not adapter_supported:
        reasons.append("adapter_unsupported")
    if not product_registered:
        reasons.append("product_contract_unregistered")
    if not writable:
        reasons.append("storage_not_writable")
    if usage.free <= policy.critical_free_bytes:
        reasons.append("filesystem_free_bytes_critical")
    if spool_ratio >= policy.critical_spool_ratio:
        reasons.append("spool_utilization_critical")
    if (
        projected_exhaustion_hours is not None
        and projected_exhaustion_hours
        <= policy.critical_projected_exhaustion_hours
    ):
        reasons.append("projected_storage_exhaustion_critical")
    if halts:
        reasons.append("safety_halt_active")

    warning_reasons: list[str] = []
    if usage.free <= policy.warning_free_bytes:
        warning_reasons.append("filesystem_free_bytes_warning")
    if spool_ratio >= policy.warning_spool_ratio:
        warning_reasons.append("spool_utilization_warning")
    if (
        projected_exhaustion_hours is not None
        and projected_exhaustion_hours
        <= policy.warning_projected_exhaustion_hours
    ):
        warning_reasons.append("projected_storage_exhaustion_warning")
    severity = "critical" if reasons else "warning" if warning_reasons else "healthy"
    evidence = {
        "schema_version": "market.collector_safety_evaluation.v1",
        "definition_id": definition_id,
        "fleet_id": fleet_id,
        "filesystem_total_bytes": usage.total,
        "filesystem_free_bytes": usage.free,
        "spool_bytes": spool_bytes,
        "max_spool_bytes": max_spool_bytes,
        "spool_ratio": spool_ratio,
        "archive_growth_bytes_per_hour": bytes_per_hour,
        "archive_growth_window_seconds": float(growth["window_seconds"]),
        "projected_exhaustion_hours": projected_exhaustion_hours,
        "warning_reasons": sorted(warning_reasons),
        "critical_reasons": sorted(reasons),
        "active_halt_scopes": [
            f"{row['scope_type']}:{row['scope_id']}" for row in halts
        ],
        "policy_hash": policy.policy_hash,
    }
    qualification = QualificationEvidence(
        schema_version=QUALIFICATION_EVIDENCE_VERSION,
        definition_id=definition_id,
        fleet_id=fleet_id,
        evaluated_at=datetime.now(UTC),
        policy_hash=policy.policy_hash,
        adapter_supported=adapter_supported,
        product_contract_registered=product_registered,
        storage_writable=writable,
        filesystem_free_bytes=usage.free,
        spool_bytes=spool_bytes,
        max_spool_bytes=max_spool_bytes,
        active_halt_scopes=tuple(evidence["active_halt_scopes"]),
        reasons=tuple(reasons),
    )
    return qualification, SafetyEvaluation(
        severity=severity,
        reasons=tuple(sorted(reasons or warning_reasons)),
        evidence=evidence,
    )


__all__ = ["SafetyEvaluation", "evaluate_collector_safety"]

"""PostgreSQL authority for immutable specs and normalized feature revisions."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text

from market_data.contracts import TypedFeatureRecord
from market_data.normalization import (
    NormalizationSpec,
    NormalizedFeatureFact,
    NormalizedStatus,
)

from ._shared import db


logger = logging.getLogger(__name__)
_WARNED_LEGACY_SPEC_IDS: set[str] = set()


@dataclass(frozen=True)
class NormalizationIngestionOutcome:
    requested_count: int
    inserted_count: int
    noop_count: int
    max_commit_seq: int
    material_hashes: tuple[str, ...]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _stable_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(dict(value)).encode("utf-8")).hexdigest()


def _spec_from_row(row: Mapping[str, Any]) -> NormalizationSpec:
    spec = NormalizationSpec(
        feature_name=str(row["feature_name"]),
        semantic_version=str(row["semantic_version"]),
        input_fact_type=str(row["input_fact_type"]),
        output_fact_type=str(row["output_fact_type"]),
        formula=str(row["formula"]),
        units=str(row["units"]),
        window_seconds=(
            int(row["window_seconds"])
            if row.get("window_seconds") is not None
            else None
        ),
        minimum_observations=int(row["minimum_observations"]),
        warmup_observations=int(row["warmup_observations"]),
        partition=str(row["partition"]),
        missing_behavior=str(row["missing_behavior"]),
        materialization_mode=str(row["materialization_mode"]),
        parameters=dict(row.get("parameters") or {}),
    )
    if spec.spec_id != str(row["id"]) or spec.spec_hash != str(row["spec_hash"]):
        raise RuntimeError("market_normalization_spec_storage_corrupt: hash mismatch")
    return spec


def _unreferenced_legacy_spec_from_row(row: Mapping[str, Any]) -> Optional[NormalizationSpec]:
    """Recognize the retired 40-hex identity without weakening hash checks."""

    spec = NormalizationSpec(
        feature_name=str(row["feature_name"]),
        semantic_version=str(row["semantic_version"]),
        input_fact_type=str(row["input_fact_type"]),
        output_fact_type=str(row["output_fact_type"]),
        formula=str(row["formula"]),
        units=str(row["units"]),
        window_seconds=(
            int(row["window_seconds"])
            if row.get("window_seconds") is not None
            else None
        ),
        minimum_observations=int(row["minimum_observations"]),
        warmup_observations=int(row["warmup_observations"]),
        partition=str(row["partition"]),
        missing_behavior=str(row["missing_behavior"]),
        materialization_mode=str(row["materialization_mode"]),
        parameters=dict(row.get("parameters") or {}),
    )
    stored_id = str(row["id"])
    stored_hash = str(row["spec_hash"])
    legacy_id = f"nsp_{spec.spec_hash[:40]}"
    if spec.spec_hash != stored_hash or stored_id != legacy_id:
        return None
    if int(row.get("materialized_ref_count") or 0) > 0 or int(row.get("dataset_ref_count") or 0) > 0:
        raise RuntimeError(
            "market_normalization_legacy_identity_referenced: "
            f"spec_id={stored_id}"
        )
    return spec


def _fact_from_row(row: Mapping[str, Any]) -> NormalizedFeatureFact:
    fact = NormalizedFeatureFact(
        series_id=int(row["series_id"]),
        spec_id=str(row["spec_id"]),
        spec_hash=str(row["spec_hash"]),
        effective_at=row["effective_at"],
        known_at=row["known_at"],
        value=Decimal(row["value"]) if row.get("value") is not None else None,
        status=NormalizedStatus(str(row["status"])),
        reason=row.get("reason"),
        input_start=row["input_start"],
        input_end=row["input_end"],
        input_count=int(row["input_count"]),
        input_watermark=int(row["input_watermark"]),
        source_series_ids=tuple(int(value) for value in row["source_series_ids"]),
        source_material_hashes=tuple(str(value) for value in row["source_material_hashes"]),
        input_fingerprint=str(row["input_fingerprint"]),
    )
    if fact.material_hash != str(row["material_hash"]):
        raise RuntimeError("market_normalized_feature_storage_corrupt: hash mismatch")
    return fact


class PostgresNormalizationRepository:
    """Append-only owner for Phase 4 specs and normalized facts."""

    def register_spec(
        self,
        spec: NormalizationSpec,
        *,
        created_by: Optional[str] = None,
        approved_by: Optional[str] = None,
    ) -> NormalizationSpec:
        with db.session() as session:
            existing = session.execute(
                text(
                    """
                    SELECT * FROM market.normalization_specs
                    WHERE feature_name = :feature_name
                      AND semantic_version = :semantic_version
                    ORDER BY created_at, id
                    """
                ),
                {
                    "feature_name": spec.feature_name,
                    "semantic_version": spec.semantic_version,
                },
            ).mappings().all()
            if existing:
                stored = _spec_from_row(existing[0])
                if stored.spec_hash != spec.spec_hash:
                    raise RuntimeError(
                        "market_normalization_spec_conflict: semantic version already has different material"
                    )
                return stored
            session.execute(
                text(
                    """
                    INSERT INTO market.normalization_specs (
                        id, spec_hash, feature_name, semantic_version,
                        input_fact_type, output_fact_type, formula, units,
                        window_seconds, minimum_observations, warmup_observations,
                        partition, missing_behavior, materialization_mode,
                        parameters, created_by, approved_by, approved_at
                    ) VALUES (
                        :id, :spec_hash, :feature_name, :semantic_version,
                        :input_fact_type, :output_fact_type, :formula, :units,
                        :window_seconds, :minimum_observations, :warmup_observations,
                        :partition, :missing_behavior, :materialization_mode,
                        CAST(:parameters AS jsonb), :created_by, :approved_by,
                        CASE WHEN :approved_by IS NULL THEN NULL ELSE now() END
                    )
                    """
                ),
                {
                    **spec.material(),
                    "id": spec.spec_id,
                    "spec_hash": spec.spec_hash,
                    "parameters": _json(spec.parameters),
                    "created_by": str(created_by).strip() if created_by else None,
                    "approved_by": str(approved_by).strip() if approved_by else None,
                },
            )
        return spec

    def get_spec(self, spec_id: str) -> NormalizationSpec:
        with db.session() as session:
            row = session.execute(
                text("SELECT * FROM market.normalization_specs WHERE id = :id"),
                {"id": str(spec_id)},
            ).mappings().first()
        if row is None:
            raise ValueError(f"market_normalization_spec_unknown: spec_id={spec_id}")
        return _spec_from_row(row)

    def list_specs(self) -> tuple[NormalizationSpec, ...]:
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    SELECT specs.*,
                           (
                               SELECT COUNT(*)
                               FROM market.normalized_feature_versions AS values
                               WHERE values.spec_id = specs.id
                           ) AS materialized_ref_count,
                           (
                               SELECT COUNT(*)
                               FROM market.dataset_normalization_refs AS refs
                               WHERE refs.spec_id = specs.id
                           ) AS dataset_ref_count
                    FROM market.normalization_specs AS specs
                    ORDER BY feature_name, semantic_version, id
                    """
                )
            ).mappings().all()
        specs: list[NormalizationSpec] = []
        for row in rows:
            try:
                specs.append(_spec_from_row(row))
                continue
            except RuntimeError:
                legacy = _unreferenced_legacy_spec_from_row(row)
                if legacy is None:
                    raise
            stored_id = str(row["id"])
            if stored_id not in _WARNED_LEGACY_SPEC_IDS:
                logger.warning(
                    "market_normalization_legacy_spec_quarantined | spec_id=%s | spec_hash=%s | references=0",
                    stored_id,
                    row["spec_hash"],
                )
                _WARNED_LEGACY_SPEC_IDS.add(stored_id)
        return tuple(specs)

    def ingest(
        self,
        facts: Iterable[NormalizedFeatureFact],
    ) -> NormalizationIngestionOutcome:
        normalized = sorted(
            tuple(facts),
            key=lambda fact: (fact.effective_at, fact.known_at, fact.material_hash),
        )
        if not normalized:
            return NormalizationIngestionOutcome(0, 0, 0, 0, ())
        series_ids = {fact.series_id for fact in normalized}
        spec_ids = {fact.spec_id for fact in normalized}
        if len(series_ids) != 1 or len(spec_ids) != 1:
            raise ValueError("market_normalized_ingest_invalid: one series and spec required")
        inserted = 0
        noop = 0
        max_commit_seq = 0
        material_hashes: list[str] = []
        with db.session() as session:
            spec_row = session.execute(
                text("SELECT * FROM market.normalization_specs WHERE id = :id"),
                {"id": normalized[0].spec_id},
            ).mappings().first()
            if spec_row is None:
                raise ValueError("market_normalized_ingest_invalid: unknown spec")
            spec = _spec_from_row(spec_row)
            series = session.execute(
                text(
                    """
                    SELECT fact_type, contract_version
                    FROM market.series WHERE id = :series_id
                    """
                ),
                {"series_id": normalized[0].series_id},
            ).mappings().first()
            expected_version = f"market.normalized_feature.v1/{spec.spec_id}"
            if (
                series is None
                or str(series["fact_type"]) != spec.output_fact_type
                or str(series["contract_version"]) != expected_version
            ):
                raise ValueError(
                    "market_normalized_ingest_invalid: output series is not bound to spec"
                )
            for fact in normalized:
                if fact.spec_hash != spec.spec_hash:
                    raise ValueError("market_normalized_ingest_invalid: spec hash disagreement")
                existing = session.execute(
                    text(
                        """
                        SELECT revision, input_fingerprint, material_hash,
                               market_commit_seq, input_watermark, known_at
                        FROM market.normalized_feature_versions
                        WHERE series_id = :series_id
                          AND spec_id = :spec_id
                          AND effective_at = :effective_at
                        ORDER BY revision DESC
                        LIMIT 1
                        """
                    ),
                    {
                        "series_id": fact.series_id,
                        "spec_id": fact.spec_id,
                        "effective_at": fact.effective_at,
                    },
                ).mappings().first()
                if existing is not None and str(existing["material_hash"]) == fact.material_hash:
                    noop += 1
                    max_commit_seq = max(max_commit_seq, int(existing["market_commit_seq"]))
                    material_hashes.append(fact.material_hash)
                    continue
                if existing is not None and (
                    fact.input_watermark < int(existing["input_watermark"])
                    or fact.known_at < _utc(existing["known_at"])
                ):
                    raise RuntimeError(
                        "market_normalized_ingest_stale: older causal evidence "
                        "cannot supersede the latest normalized revision"
                    )
                if (
                    existing is not None
                    and fact.input_watermark == int(existing["input_watermark"])
                    and str(existing["input_fingerprint"]) != fact.input_fingerprint
                ):
                    raise RuntimeError(
                        "market_normalized_ingest_conflict: one evidence watermark "
                        "cannot identify contradictory inputs"
                    )
                if (
                    existing is not None
                    and str(existing["input_fingerprint"]) == fact.input_fingerprint
                ):
                    raise RuntimeError(
                        "market_normalized_ingest_conflict: identical inputs produced different material"
                    )
                revision = int(existing["revision"]) + 1 if existing is not None else 1
                version_id = "nfv_" + _stable_hash(
                    {
                        "series_id": fact.series_id,
                        "spec_id": fact.spec_id,
                        "effective_at": fact.effective_at.isoformat(),
                        "revision": revision,
                        "material_hash": fact.material_hash,
                    }
                )
                provenance_hash = _stable_hash(
                    {
                        "schema_version": "market.normalized_provenance.v1",
                        "spec_hash": fact.spec_hash,
                        "input_fingerprint": fact.input_fingerprint,
                        "source_series_ids": fact.source_series_ids,
                        "source_material_hashes": fact.source_material_hashes,
                    }
                )
                quality = {
                    "classification": fact.status.value,
                    "valid": fact.status is NormalizedStatus.VALID,
                    "reason": fact.reason,
                    "input_count": fact.input_count,
                    "input_watermark": fact.input_watermark,
                }
                commit_seq = session.execute(
                    text(
                        """
                        INSERT INTO market.normalized_feature_versions (
                            id, series_id, spec_id, revision, effective_at,
                            known_at, value, status, reason, input_start,
                            input_end, input_count, input_watermark,
                            source_series_ids, source_material_hashes,
                            input_fingerprint, material_hash, provenance_hash, quality
                        ) VALUES (
                            :id, :series_id, :spec_id, :revision, :effective_at,
                            :known_at, :value, :status, :reason, :input_start,
                            :input_end, :input_count, :input_watermark,
                            CAST(:source_series_ids AS jsonb),
                            CAST(:source_material_hashes AS jsonb),
                            :input_fingerprint, :material_hash, :provenance_hash,
                            CAST(:quality AS jsonb)
                        )
                        RETURNING market_commit_seq
                        """
                    ),
                    {
                        "id": version_id,
                        "series_id": fact.series_id,
                        "spec_id": fact.spec_id,
                        "revision": revision,
                        "effective_at": _utc(fact.effective_at),
                        "known_at": _utc(fact.known_at),
                        "value": fact.value,
                        "status": fact.status.value,
                        "reason": fact.reason,
                        "input_start": _utc(fact.input_start),
                        "input_end": _utc(fact.input_end),
                        "input_count": fact.input_count,
                        "input_watermark": fact.input_watermark,
                        "source_series_ids": _json(fact.source_series_ids),
                        "source_material_hashes": _json(fact.source_material_hashes),
                        "input_fingerprint": fact.input_fingerprint,
                        "material_hash": fact.material_hash,
                        "provenance_hash": provenance_hash,
                        "quality": _json(quality),
                    },
                ).scalar_one()
                inserted += 1
                max_commit_seq = max(max_commit_seq, int(commit_seq))
                material_hashes.append(fact.material_hash)
        return NormalizationIngestionOutcome(
            requested_count=len(normalized),
            inserted_count=inserted,
            noop_count=noop,
            max_commit_seq=max_commit_seq,
            material_hashes=tuple(material_hashes),
        )

    def read_records(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        known_at_lte: Optional[datetime] = None,
        as_of_commit_seq: Optional[int] = None,
    ) -> tuple[TypedFeatureRecord, ...]:
        if end <= start:
            raise ValueError("market_normalized_read_invalid: end must follow start")
        decision_time = known_at_lte or datetime.max.replace(tzinfo=UTC)
        with db.session() as session:
            rows = session.execute(
                text(
                    """
                    WITH visible AS (
                        SELECT values.*, specs.spec_hash,
                               ROW_NUMBER() OVER (
                                   PARTITION BY values.series_id, values.spec_id,
                                                values.effective_at
                                   ORDER BY values.revision DESC,
                                            values.market_commit_seq DESC
                               ) AS selected_revision
                        FROM market.normalized_feature_versions AS values
                        JOIN market.normalization_specs AS specs
                          ON specs.id = values.spec_id
                        WHERE values.series_id = :series_id
                          AND values.effective_at >= :start
                          AND values.effective_at < :end
                          AND values.known_at <= :known_at
                          AND (
                              :as_of_commit_seq IS NULL
                              OR values.market_commit_seq <= :as_of_commit_seq
                          )
                    )
                    SELECT * FROM visible
                    WHERE selected_revision = 1
                    ORDER BY effective_at, spec_id
                    """
                ),
                {
                    "series_id": int(series_id),
                    "start": _utc(start),
                    "end": _utc(end),
                    "known_at": _utc(decision_time),
                    "as_of_commit_seq": (
                        int(as_of_commit_seq)
                        if as_of_commit_seq is not None
                        else None
                    ),
                },
            ).mappings().all()
        return tuple(
            TypedFeatureRecord(
                version_id=str(row["id"]),
                series_id=int(row["series_id"]),
                revision=int(row["revision"]),
                market_commit_seq=int(row["market_commit_seq"]),
                provenance_hash=str(row["provenance_hash"]),
                quality=dict(row["quality"] or {}),
                fact=_fact_from_row(row),
            )
            for row in rows
        )


normalization_repository = PostgresNormalizationRepository()


__all__ = [
    "NormalizationIngestionOutcome",
    "PostgresNormalizationRepository",
    "normalization_repository",
]

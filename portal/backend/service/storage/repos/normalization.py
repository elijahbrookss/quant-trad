"""PostgreSQL authority for immutable specs and normalized feature revisions."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Optional

from sqlalchemy import text

from market_data.canonical_adapters import (
    DERIVED_MARKET_STATE_SOURCE,
    canonicalize_normalized_feature,
    decode_normalized_feature_record,
)
from market_data.contracts import TypedFeatureRecord
from market_data.fact_registry import (
    FactPayloadSchema,
    build_normalized_fact_payload_schema,
    register_fact_payload_schema,
)
from market_data.normalization import (
    NormalizationSpec,
    NormalizedFeatureFact,
)

from ._shared import db
from .market_data import market_data_repo
from .fact_storage import canonical_fact_storage_repository
from .market_lifecycle import market_storage_lifecycle_repository


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


def _ensure_payload_schema(session, spec: NormalizationSpec) -> FactPayloadSchema:
    schema = register_fact_payload_schema(
        build_normalized_fact_payload_schema(
            spec_id=spec.spec_id,
            fact_type=spec.output_fact_type,
            units=spec.units,
        )
    )
    session.execute(
        text(
            """
            INSERT INTO market.fact_schemas (
                schema_id, fact_type, contract_hash, contract,
                observation_time_field, material_hash_version,
                row_hash_version, query_fields, dataset_eligible
            ) VALUES (
                :schema_id, :fact_type, :contract_hash,
                CAST(:contract AS jsonb), :observation_time_field,
                :material_hash_version, :row_hash_version,
                CAST(:query_fields AS jsonb), :dataset_eligible
            )
            ON CONFLICT (schema_id) DO NOTHING
            """
        ),
        {
            "schema_id": schema.schema_id,
            "fact_type": schema.fact_type,
            "contract_hash": schema.contract_hash,
            "contract": _json(schema.contract),
            "observation_time_field": schema.observation_time_field,
            "material_hash_version": schema.material_hash_version,
            "row_hash_version": schema.row_hash_version,
            "query_fields": _json(schema.query_fields),
            "dataset_eligible": schema.dataset_eligible,
        },
    )
    stored = session.execute(
        text(
            "SELECT fact_type, contract_hash FROM market.fact_schemas "
            "WHERE schema_id = :schema_id"
        ),
        {"schema_id": schema.schema_id},
    ).mappings().one()
    if (
        str(stored["fact_type"]) != schema.fact_type
        or str(stored["contract_hash"]) != schema.contract_hash
    ):
        raise RuntimeError(
            "market_normalization_schema_conflict: stored payload contract differs "
            f"schema_id={schema.schema_id}"
        )
    return schema


class PostgresNormalizationRepository:
    """Append-only owner for normalization specs and normalized facts."""

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
                _ensure_payload_schema(session, stored)
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
            _ensure_payload_schema(session, spec)
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
        with market_storage_lifecycle_repository.dataset_snapshot_session(database=db) as session:
            return self._list_specs_with_session(session)

    def _list_specs_with_session(self, session) -> tuple[NormalizationSpec, ...]:
        rows = session.execute(text("""
                    SELECT specs.*,
                           (
                               SELECT COUNT(*)
                               FROM market.dataset_normalization_refs AS refs
                               WHERE refs.spec_id = specs.id
                           ) AS dataset_ref_count
                    FROM market.normalization_specs AS specs
                    ORDER BY feature_name, semantic_version, id
        """)).mappings().all()
        specs: list[NormalizationSpec] = []
        cold_references = None
        for row in rows:
            try:
                specs.append(_spec_from_row(row))
                continue
            except RuntimeError:
                legacy = _unreferenced_legacy_spec_from_row(row)
                if legacy is None:
                    raise
            stored_id = str(row["id"])
            # The retired identity guard counted exact opaque provenance
            # references, not similarly named external-event keys. Preserve
            # that predicate before quarantining a spec, across both tiers.
            hot_reference = session.execute(text("""
                SELECT 1 FROM market.fact_hot_payloads
                WHERE provenance->'_qt_normalization_evidence'->>'spec_id'=:id LIMIT 1
            """), {"id": stored_id}).scalar_one_or_none()
            if hot_reference is not None:
                raise RuntimeError(f"market_normalization_legacy_identity_referenced: spec_id={stored_id}")
            if cold_references is None:
                cold_references = set()
                requested_ids = {item["id"] for item in rows}
                with canonical_fact_storage_repository.stream_rows_by_ids(session, text("""
                    SELECT versions.id FROM market.fact_versions AS versions
                    WHERE NOT EXISTS (SELECT 1 FROM market.fact_hot_payloads AS hot
                                      WHERE hot.storage_day=versions.storage_day AND hot.id=versions.id)
                    ORDER BY versions.id
                """)) as cold_rows:
                    announced = False
                    for cold_row in cold_rows:
                        if not announced:
                            logger.warning("market_normalization_legacy_reference_scan | reason=verify_unreferenced_legacy_specs")
                            announced = True
                        evidence = cold_row["provenance"].get("_qt_normalization_evidence")
                        if isinstance(evidence, Mapping) and evidence.get("spec_id") in requested_ids:
                            cold_references.add(evidence["spec_id"])
            if stored_id in cold_references:
                raise RuntimeError(f"market_normalization_legacy_identity_referenced: spec_id={stored_id}")
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
            key=lambda fact: (
                fact.effective_at,
                fact.known_at,
                fact.material_hash,
            ),
        )
        if not normalized:
            return NormalizationIngestionOutcome(0, 0, 0, 0, ())
        series_ids = {fact.series_id for fact in normalized}
        spec_ids = {fact.spec_id for fact in normalized}
        if len(series_ids) != 1 or len(spec_ids) != 1:
            raise ValueError(
                "market_normalized_ingest_invalid: one series and spec required"
            )
        source_id = market_data_repo.register_source(
            DERIVED_MARKET_STATE_SOURCE,
            lineage={
                "schema_version": "market.derived_source_lineage.v1",
                "authority": "QT deterministic market-state transforms",
            },
        )
        with db.session() as session:
            spec_row = session.execute(
                text("SELECT * FROM market.normalization_specs WHERE id = :id"),
                {"id": normalized[0].spec_id},
            ).mappings().first()
            if spec_row is None:
                raise ValueError("market_normalized_ingest_invalid: unknown spec")
            spec = _spec_from_row(spec_row)
            schema = _ensure_payload_schema(session, spec)
            series = session.execute(
                text(
                    """
                    SELECT fact_type, contract_version
                    FROM market.series WHERE id = :series_id
                    """
                ),
                {"series_id": normalized[0].series_id},
            ).mappings().first()
            if (
                series is None
                or str(series["fact_type"]) != spec.output_fact_type
                or str(series["contract_version"]) != schema.schema_id
            ):
                raise ValueError(
                    "market_normalized_ingest_invalid: output series is not bound to spec"
                )

            canonical = []
            for fact in normalized:
                if fact.spec_hash != spec.spec_hash:
                    raise ValueError(
                        "market_normalized_ingest_invalid: spec hash disagreement"
                    )
                for material_hash in fact.source_material_hashes:
                    if not canonical_fact_storage_repository.material_witness_exists(
                        session, series_ids=fact.source_series_ids, material_hash=material_hash,
                    ):
                        raise ValueError(
                            "market_normalized_ingest_invalid: canonical source "
                            f"witness is missing material_hash={material_hash}"
                        )
                canonical_fact = canonicalize_normalized_feature(
                    fact,
                    spec=spec,
                    source=DERIVED_MARKET_STATE_SOURCE,
                )
                existing = session.execute(
                    text(
                        """
                        SELECT id
                        FROM market.fact_versions
                        WHERE series_id = :series_id
                          AND observation_key = :observation_key
                        ORDER BY revision DESC
                        LIMIT 1
                        """
                    ),
                    {
                        "series_id": fact.series_id,
                        "observation_key": canonical_fact.observation_key,
                    },
                ).scalar_one_or_none()
                existing = (canonical_fact_storage_repository.read_rows_by_ids(session, [existing])[existing]
                            if existing is not None else None)
                if (
                    existing is not None
                    and str(existing["row_hash"]) != canonical_fact.row_hash
                ):
                    existing_payload = dict(existing["payload"] or {})
                    existing_watermark = int(
                        existing_payload["input_watermark"]
                    )
                    existing_fingerprint = str(
                        existing_payload["input_fingerprint"]
                    )
                    if (
                        fact.input_watermark < existing_watermark
                        or fact.known_at < _utc(existing["known_at"])
                    ):
                        raise RuntimeError(
                            "market_normalized_ingest_stale: older causal evidence "
                            "cannot supersede the latest normalized revision"
                        )
                    if (
                        fact.input_watermark == existing_watermark
                        and existing_fingerprint != fact.input_fingerprint
                    ):
                        raise RuntimeError(
                            "market_normalized_ingest_conflict: one evidence "
                            "watermark cannot identify contradictory inputs"
                        )
                    if existing_fingerprint == fact.input_fingerprint:
                        raise RuntimeError(
                            "market_normalized_ingest_conflict: identical inputs "
                            "produced different material"
                        )
                canonical.append(canonical_fact)

            outcome = market_data_repo.ingest_facts_in_session(
                session,
                series_id=normalized[0].series_id,
                source_id=source_id,
                facts=canonical,
                request={
                    "operation": "market_normalized_feature_canonicalization",
                    "spec_id": spec.spec_id,
                    "spec_hash": spec.spec_hash,
                },
                source_revision=DERIVED_MARKET_STATE_SOURCE.adapter_version,
                allow_corrections=True,
            )
        return NormalizationIngestionOutcome(
            requested_count=len(normalized),
            inserted_count=(
                outcome.inserted_count + outcome.corrected_count
            ),
            noop_count=outcome.noop_count,
            max_commit_seq=outcome.max_commit_seq,
            material_hashes=tuple(fact.material_hash for fact in normalized),
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
        return tuple(
            decode_normalized_feature_record(record)
            for record in market_data_repo.read_facts(
                series_id=int(series_id),
                start=_utc(start),
                end=_utc(end),
                known_at_lte=_utc(decision_time),
                as_of_commit_seq=as_of_commit_seq,
            )
        )


normalization_repository = PostgresNormalizationRepository()


__all__ = [
    "NormalizationIngestionOutcome",
    "PostgresNormalizationRepository",
    "normalization_repository",
]

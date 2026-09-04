"""One exact canonical row codec shared by PostgreSQL and cold archives."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any

from .canonical import CanonicalFact, CanonicalFactRecord
from .contracts import SourceIdentity


LEGACY_MATERIAL_EVIDENCE_KEYS = {
    "market.bbo": "_qt_bbo_evidence",
    "market.depth_observation": "_qt_depth_evidence",
    "market.trade_flow_feature": "_qt_trade_flow_feature_evidence",
    "market.futures_spot_relationship": "_qt_basis_evidence",
    "market.market_response": "_qt_response_evidence",
}


def legacy_material_alias(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Derive one typed-compatibility lookup witness from canonical provenance."""
    key = LEGACY_MATERIAL_EVIDENCE_KEYS.get(str(row["fact_type"]))
    if key is None:
        return None
    evidence = row["provenance"].get(key)
    if evidence is None:
        return None
    if not isinstance(evidence, Mapping):
        raise RuntimeError(f"canonical_legacy_material_invalid: fact_version_id={row['id']} evidence_key={key}")
    material = evidence.get("legacy_material_hash")
    if material is None:
        return None
    if not isinstance(material, str) or re.fullmatch(r"[0-9a-f]{64}", material) is None:
        raise RuntimeError(f"canonical_legacy_material_invalid: fact_version_id={row['id']} evidence_key={key}")
    return {"fact_version_id": row["id"], "series_id": row["series_id"],
            "evidence_key": key, "material_hash": material}


def verify_archived_envelope(envelope: Mapping[str, Any], archived: Mapping[str, Any]) -> None:
    """Match retained identity/causal/source fields, independent of payload placement.

    The archive codec must already have validated all document hashes. This
    comparison then binds those documents to PostgreSQL without loading hot JSON.
    """
    for name, value in envelope.items():
        if name in {"payload", "provenance", "quality", "storage_day"}:
            continue
        if name not in archived or archived[name] != value:
            raise RuntimeError(
                f"canonical_archive_envelope_mismatch: fact_version_id={envelope.get('id')} field={name}"
            )


def source_from_storage_row(row: Mapping[str, Any]) -> SourceIdentity:
    source = SourceIdentity(
        provider=str(row["source_provider"]),
        venue=str(row["source_venue"]),
        source_kind=str(row["source_kind"]),
        adapter_version=str(row["source_adapter_version"]),
    )
    if source.identity_key != str(row["source_identity_key"]):
        raise RuntimeError(
            "market_data_corrupt: canonical source identity mismatch "
            f"source_id={row.get('source_id')}"
        )
    return source


def record_from_storage_row(row: Mapping[str, Any]) -> CanonicalFactRecord:
    fact = CanonicalFact(
        fact_type=str(row["fact_type"]),
        payload_schema_id=str(row["payload_schema_id"]),
        observation_key=str(row["observation_key"]),
        observation_time=row["observation_time"],
        observation_time_method=str(row["observation_time_method"]),
        source_published_at=row.get("source_published_at"),
        received_at=row.get("received_at"),
        accepted_at=row["accepted_at"],
        known_at=row["known_at"],
        known_at_method=str(row["known_at_method"]),
        source=source_from_storage_row(row),
        transformation_id=str(row["transformation_id"]),
        external_event_key=row.get("external_event_key"),
        external_event_group_key=row.get("external_event_group_key"),
        external_event_component_key=row.get("external_event_component_key"),
        state=str(row["state"]),
        payload=dict(row.get("payload") or {}),
        provenance_schema_id=str(row["provenance_schema_id"]),
        provenance=dict(row.get("provenance") or {}),
        quality_schema_id=str(row["quality_schema_id"]),
        quality=dict(row.get("quality") or {}),
    )
    expected = {
        "payload_contract_hash": fact.payload_contract_hash,
        "payload_hash": fact.payload_hash,
        "material_hash": fact.material_hash,
        "provenance_hash": fact.provenance_hash,
        "quality_hash": fact.quality_hash,
        "row_hash": fact.row_hash,
    }
    for field_name, expected_hash in expected.items():
        if str(row.get(field_name) or "") != expected_hash:
            raise RuntimeError(
                "market_data_corrupt: canonical Fact hash mismatch "
                f"field={field_name} fact_version_id={row.get('id')}"
            )
    return CanonicalFactRecord(
        series_id=int(row["series_id"]),
        source_id=int(row["source_id"]),
        revision=int(row["revision"]),
        market_commit_seq=int(row["market_commit_seq"]),
        ingestion_run_id=row.get("ingestion_run_id"),
        fact_version_id=str(row["id"]),
        row_hash=str(row["row_hash"]),
        fact=fact,
    )


def record_to_storage_row(
    record: CanonicalFactRecord, *, series_dimensions: Mapping[str, Any]
) -> dict[str, Any]:
    """Preserve all revision, source, causal, and JSON evidence without rehashing history."""
    row = record.fact.to_dict()
    source = row.pop("source")
    # Typed Parquet timestamps preserve PostgreSQL's exact microsecond clock.
    for name in ("observation_time", "source_published_at", "received_at", "accepted_at", "known_at"):
        row[name] = getattr(record.fact, name)
    row.update(
        id=record.fact_version_id,
        series_id=record.series_id,
        source_id=record.source_id,
        revision=record.revision,
        market_commit_seq=record.market_commit_seq,
        ingestion_run_id=record.ingestion_run_id,
        row_hash=record.row_hash,
        source_identity_key=source["identity_key"],
        source_provider=source["provider"],
        source_venue=source["venue"],
        source_kind=source["source_kind"],
        source_adapter_version=source["adapter_version"],
        series_dimensions=dict(series_dimensions),
    )
    # The record wrapper allows historical identities; hashes must still agree.
    record_from_storage_row(row)
    return row

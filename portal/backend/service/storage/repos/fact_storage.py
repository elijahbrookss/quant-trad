"""Verified cold-payload hydration behind the canonical PostgreSQL envelope.

Selection remains SQL over immutable revision metadata. Placement cannot change
which revisions are visible; archives only supply the selected JSON documents.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlalchemy import text

from core.storage_mounts import configured_archive_root
from market_data.archive import FilesystemRawArchiveObjectStore, RawArchiveObjectStore
from market_data.canonical_storage import verify_archived_envelope
from market_data.fact_archive import FactArchiveLimits, FactArchiveManifest, read_canonical_fact_archive
from market_data.fact_registry import (
    NORMALIZED_FACT_VERSION, build_normalized_fact_payload_schema,
    get_fact_payload_schema, register_fact_payload_schema,
)


CANONICAL_ENVELOPE_COLUMNS = """
    versions.*,
    sources.identity_key AS source_identity_key,
    sources.provider AS source_provider, sources.venue AS source_venue,
    sources.source_kind, sources.adapter_version AS source_adapter_version,
    series.dimensions AS series_dimensions
"""
CANONICAL_ENVELOPE_FROM = """
    FROM market.fact_versions AS versions
    JOIN market.sources AS sources ON sources.id = versions.source_id
    JOIN market.series AS series ON series.id = versions.series_id
"""
CANONICAL_ROW_COLUMNS = CANONICAL_ENVELOPE_COLUMNS + ", hot.payload, hot.provenance, hot.quality"
CANONICAL_ROW_FROM = CANONICAL_ENVELOPE_FROM + """
    LEFT JOIN market.fact_hot_payloads AS hot
      ON hot.storage_day = versions.storage_day AND hot.id = versions.id
"""
_DOCUMENTS = frozenset(("payload", "provenance", "quality"))


def ensure_payload_contracts(session, contracts: Sequence[tuple[str, str]]) -> None:
    """Reload spec-bound schemas read-only, including other series in a page.

    Only repository-defined schema builders are admitted. Persisted arbitrary
    JSON contracts are not executable schema definitions, and process-local
    registration is not evidence that this database still has the matching spec.
    """
    requested = {}
    for schema_id, contract_hash in contracts:
        if requested.setdefault(schema_id, contract_hash) != contract_hash:
            raise RuntimeError(f"canonical_payload_contract_conflict: schema_id={schema_id}")
    dynamic = {}
    prefix = NORMALIZED_FACT_VERSION + "/"
    for schema_id, contract_hash in requested.items():
        if schema_id.startswith(prefix):
            dynamic[schema_id[len(prefix):]] = (schema_id, contract_hash)
        elif get_fact_payload_schema(schema_id).contract_hash != contract_hash:
            raise RuntimeError(f"canonical_payload_contract_mismatch: schema_id={schema_id}")
    if not dynamic:
        return

    # Reuse the normalization owner's hash-checked decoder. The local import
    # avoids the repository composition cycle (normalization uses fact writes).
    from .normalization import _spec_from_row

    spec_ids = sorted(dynamic)
    for offset in range(0, len(spec_ids), 1000):
        batch = spec_ids[offset:offset + 1000]
        rows = session.execute(text("""
            SELECT specs.*, schemas.schema_id AS stored_schema_id,
                   schemas.contract_hash AS stored_contract_hash,
                   schemas.contract AS stored_contract
            FROM market.normalization_specs AS specs
            JOIN market.fact_schemas AS schemas ON schemas.schema_id = :prefix || specs.id
            WHERE specs.id = ANY(:spec_ids)
        """), {"prefix": prefix, "spec_ids": batch}).mappings().all()
        found = set()
        validated = []
        for row in rows:
            spec = _spec_from_row(row)
            if spec.spec_id not in batch or spec.spec_id in found:
                raise RuntimeError(f"canonical_payload_spec_coverage_invalid: spec_id={spec.spec_id}")
            found.add(spec.spec_id)
            schema = build_normalized_fact_payload_schema(
                spec_id=spec.spec_id, fact_type=spec.output_fact_type, units=spec.units,
            )
            expected_id, expected_hash = dynamic[spec.spec_id]
            if (schema.schema_id != expected_id or schema.contract_hash != expected_hash
                    or row["stored_schema_id"] != expected_id
                    or row["stored_contract_hash"] != expected_hash
                    or row["stored_contract"] != schema.contract):
                raise RuntimeError(f"canonical_payload_contract_mismatch: schema_id={expected_id}")
            validated.append(schema)
        if found != set(batch):
            raise RuntimeError(f"canonical_payload_spec_missing: spec_ids={sorted(set(batch) - found)}")
        for schema in validated:
            register_fact_payload_schema(schema)


def _read_only_store() -> RawArchiveObjectStore:
    return FilesystemRawArchiveObjectStore(
        configured_archive_root().expanduser().resolve() / "objects", writable=False,
    )


def _catalog_manifest(row: Mapping[str, Any]) -> FactArchiveManifest:
    manifest = FactArchiveManifest.from_dict(row["descriptor"], expected_hash=str(row["manifest_hash"]))
    bound_fields = {
        "id": manifest.manifest_id, "object_key": manifest.object_key,
        "object_sha256": manifest.object_sha256, "row_count": manifest.row_count,
        "byte_count": manifest.byte_count, "first_commit_seq": manifest.first_cursor[0],
        "first_id": manifest.first_cursor[1], "last_commit_seq": manifest.last_cursor[0],
        "last_id": manifest.last_cursor[1],
    }
    for name, expected in bound_fields.items():
        if row.get(name) != expected:
            raise RuntimeError(
                f"canonical_archive_catalog_mismatch: manifest_id={manifest.manifest_id} field={name}"
            )
    return manifest


class PostgresCanonicalFactStorageRepository:
    def __init__(
        self, *, object_store_factory: Callable[[], RawArchiveObjectStore] = _read_only_store,
        limits: FactArchiveLimits = FactArchiveLimits(),
    ):
        self.object_store_factory = object_store_factory
        self.limits = limits

    def read_rows_by_ids(self, session, fact_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Hydrate an already selected immutable identity set in bounded SQL batches."""
        identities = sorted(set(fact_ids))
        result = {}
        for offset in range(0, len(identities), 1000):
            batch = identities[offset:offset + 1000]
            rows = session.execute(text(f"""
                SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
                WHERE versions.id = ANY(:fact_ids)
            """), {"fact_ids": batch}).mappings().all()
            found = {row["id"] for row in rows}
            if found != set(batch) or len(rows) != len(found):
                raise RuntimeError("canonical_selected_identity_coverage_invalid")
            result.update((row["id"], row) for row in self.hydrate_rows(session, rows))
        return result

    def hydrate_rows(self, session, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Preserve SQL order and validate every cold page before supplying payloads.

        Ordinary READ COMMITTED sessions are safe across reclamation because the
        SELECT locks hot partitions through the transaction. Repeatable snapshots
        must hold the lifecycle shared fence before establishing their snapshot.
        The caller owns causal filtering; hydration never selects or drops rows.
        """
        result = [dict(row) for row in rows]
        ensure_payload_contracts(session, [
            (row["payload_schema_id"], row["payload_contract_hash"]) for row in result
        ])
        cold = {}
        for row in result:
            missing = [name for name in _DOCUMENTS if row.get(name) is None]
            if not missing:
                continue
            if len(missing) != len(_DOCUMENTS):
                raise RuntimeError(f"canonical_hot_payload_incomplete: fact_version_id={row.get('id')}")
            if "storage_day" not in row or not row.get("id") or not row.get("market_commit_seq"):
                raise RuntimeError("canonical_cold_identity_missing: placement, ID and watermark are required")
            identity = str(row["id"])
            if identity in cold:
                raise RuntimeError(f"canonical_cold_identity_duplicate: fact_version_id={identity}")
            cold[identity] = row
        if not cold:
            return result

        # Bound the SQL parameter set independently of the overall requested
        # history. The catalog range index locates a page without scanning files.
        matched: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        ids = sorted(cold)
        for offset in range(0, len(ids), 1000):
            matches = session.execute(text("""
                SELECT requested.id AS requested_id, manifests.*
                FROM market.fact_versions AS requested
                JOIN market.fact_retention_partitions AS partition
                  ON partition.storage_day = requested.storage_day
                 AND partition.state IN ('verified', 'reclaimed')
                JOIN market.fact_archive_manifests AS manifests
                  ON manifests.storage_day = requested.storage_day
                 AND (manifests.first_commit_seq, manifests.first_id)
                     <= (requested.market_commit_seq, requested.id)
                 AND (manifests.last_commit_seq, manifests.last_id)
                     >= (requested.market_commit_seq, requested.id)
                WHERE requested.id = ANY(:fact_ids)
                ORDER BY requested.id, manifests.page_ordinal
            """), {"fact_ids": ids[offset:offset + 1000]}).mappings().all()
            for item in matches:
                identity = str(item["requested_id"])
                if identity not in cold:
                    raise RuntimeError(f"canonical_archive_catalog_unrequested: fact_version_id={identity}")
                matched[identity].append(item)

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        manifests = {}
        for identity, row in cold.items():
            candidates = matched.get(identity, ())
            if len(candidates) != 1:
                raise RuntimeError(
                    f"canonical_archive_coverage_invalid: fact_version_id={identity} pages={len(candidates)}"
                )
            candidate = candidates[0]
            if candidate["storage_day"] != row["storage_day"]:
                raise RuntimeError(f"canonical_archive_placement_mismatch: fact_version_id={identity}")
            manifest = _catalog_manifest(candidate)
            cursor = (row["market_commit_seq"], identity)
            if not manifest.first_cursor <= cursor <= manifest.last_cursor:
                raise RuntimeError(f"canonical_archive_cursor_mismatch: fact_version_id={identity}")
            previous = manifests.setdefault(manifest.manifest_id, manifest)
            if previous != manifest:
                raise RuntimeError(f"canonical_archive_catalog_conflict: manifest_id={manifest.manifest_id}")
            grouped[manifest.manifest_id].append(row)

        store = self.object_store_factory()
        for manifest_id, wanted in grouped.items():
            manifest = manifests[manifest_id]
            ensure_payload_contracts(session, [
                contract for bounds in manifest.series for contract in bounds.payload_contracts
            ])
            archive_rows = read_canonical_fact_archive(
                store.local_path(manifest.object_key), expected=manifest, limits=self.limits,
            )
            indexed = {str(row["id"]): row for row in archive_rows}
            for envelope in wanted:
                identity = str(envelope["id"])
                archived = indexed.get(identity)
                if archived is None:
                    raise RuntimeError(f"canonical_archive_revision_missing: fact_version_id={identity}")
                # Check every selected envelope/source field against the cold
                # copy, not just ID or row_hash. storage_day is not market truth.
                verify_archived_envelope(envelope, archived)
                envelope.update({name: archived[name] for name in _DOCUMENTS})
        return result


canonical_fact_storage_repository = PostgresCanonicalFactStorageRepository()

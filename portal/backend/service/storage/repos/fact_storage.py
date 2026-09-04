"""Verified cold-payload hydration behind the canonical PostgreSQL envelope.

Selection remains SQL over immutable revision metadata. Placement cannot change
which revisions are visible; archives only supply the selected JSON documents.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
import logging
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
logger = logging.getLogger(__name__)


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

    @contextmanager
    def stream_rows_by_ids(self, session, statement, params=None, *, batch_size=128):
        """Hydrate an ordered SQL identity stream without retaining all payloads.

        The caller's statement selects one ID column and owns filtering/order.
        The context closes the server cursor even after an early match/error.
        Multi-page causal selections require a snapshot fenced before its first
        read, just like other canonical repository reads.
        """
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("canonical_stream_batch_invalid: expected 1..1000")
        selected = session.execute(statement, params or {}, execution_options={
            "stream_results": True, "yield_per": batch_size,
        })
        def hydrate():
            for batch in selected.scalars().partitions(batch_size):
                identities = list(batch)
                if len(identities) != len(set(identities)):
                    raise RuntimeError("canonical_selected_identity_duplicate")
                rows = self.read_rows_by_ids(session, identities)
                yield from (rows[identity] for identity in identities)
        rows = hydrate()
        try:
            yield rows
        finally:
            rows.close()
            selected.close()

    def material_witness_exists(self, session, *, series_ids, material_hash,
                                evidence_key=None, include_canonical=True):
        """Evaluate the existing canonical/legacy material predicate across tiers.

        Archive aliases are lookup hints, never proof. Verify selected payloads
        before accepting a hint. Generic legacy provenance remains supported:
        if no indexed witness exists, stream cold rows for unindexed keys rather
        than turning absent index entries into false 'missing source' claims.
        """
        ids = sorted({int(value) for value in series_ids})
        if not ids:
            return False
        material_hash = str(material_hash)
        params = {"series_ids": ids, "material_hash": material_hash}
        key_predicate = ""
        alias_predicate = ""
        if evidence_key is not None:
            params["evidence_key"] = str(evidence_key)
            key_predicate = "AND evidence.key=:evidence_key"
            alias_predicate = "AND aliases.evidence_key=:evidence_key"
        direct = """
            SELECT id FROM market.fact_versions
            WHERE series_id=ANY(:series_ids) AND material_hash=:material_hash
            UNION ALL
        """ if include_canonical else ""
        selected = session.execute(text(f"""
            SELECT DISTINCT id FROM (
                {direct}
                SELECT versions.id FROM market.fact_versions AS versions
                JOIN market.fact_hot_payloads AS hot ON hot.storage_day=versions.storage_day AND hot.id=versions.id
                WHERE versions.series_id=ANY(:series_ids) AND EXISTS (
                    SELECT 1 FROM jsonb_each(hot.provenance) AS evidence
                    WHERE jsonb_typeof(evidence.value)='object'
                      AND evidence.value->>'legacy_material_hash'=:material_hash {key_predicate})
                UNION ALL
                SELECT versions.id FROM market.fact_archive_material_aliases AS aliases
                JOIN market.fact_versions AS versions ON versions.id=aliases.fact_version_id
                JOIN market.fact_archive_manifests AS manifest ON manifest.id=aliases.manifest_id
                  AND manifest.storage_day=versions.storage_day
                JOIN market.fact_retention_partitions AS partition ON partition.storage_day=versions.storage_day
                  AND partition.state IN ('verified','reclaimed')
                WHERE versions.series_id=ANY(:series_ids) AND aliases.series_id=versions.series_id
                  AND aliases.material_hash=:material_hash {alias_predicate}
                  AND NOT EXISTS (SELECT 1 FROM market.fact_hot_payloads AS hot
                                  WHERE hot.storage_day=versions.storage_day AND hot.id=versions.id)
            ) AS candidates ORDER BY id LIMIT 1
        """), params).scalar_one_or_none()

        def matches(row):
            if int(row["series_id"]) not in ids:
                raise RuntimeError(f"canonical_material_source_scope_mismatch: fact_version_id={row['id']}")
            if include_canonical and row["material_hash"] == material_hash:
                return True
            for key, value in row["provenance"].items():
                if (evidence_key is not None and key != evidence_key) or not isinstance(value, Mapping):
                    continue
                legacy_hash = value.get("legacy_material_hash")
                # PostgreSQL ->> also accepts a numeric JSON legacy witness.
                # A digits-only SHA can have been retained as an integer; do
                # not change that historical admission predicate after cooling.
                if type(legacy_hash) in (str, int) and str(legacy_hash) == material_hash:
                    return True
            return False

        if selected is not None:
            row = self.read_rows_by_ids(session, [selected])[selected]
            if not matches(row):
                raise RuntimeError(f"canonical_material_alias_mismatch: fact_version_id={selected} material_hash={material_hash}")
            return True
        # Old source admission accepted any object-valued legacy witness, not
        # only the standard per-family key indexed by archive admission. Keep
        # that meaning without a new ingestion policy or an unbounded list.
        with self.stream_rows_by_ids(session, text("""
            SELECT versions.id FROM market.fact_versions AS versions
            WHERE versions.series_id=ANY(:series_ids)
              AND NOT EXISTS (SELECT 1 FROM market.fact_hot_payloads AS hot
                              WHERE hot.storage_day=versions.storage_day AND hot.id=versions.id)
            ORDER BY versions.id
        """), {"series_ids": ids}) as rows:
            announced = False
            for row in rows:
                if not announced:
                    logger.warning("canonical_material_unindexed_cold_search | series_ids=%s material_hash=%s evidence_key=%s reason=no_indexed_witness",
                                   ids, material_hash, evidence_key)
                    announced = True
                if matches(row):
                    return True
        return False

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

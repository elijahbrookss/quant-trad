"""Current placement proof for immutable canonical archive source edges."""
from collections import defaultdict

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from .fact_storage import _catalog_manifest, CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM


# These registered source families retain their evidence in the canonical row
# and permanent source/acquisition metadata, not separate raw Parquet objects.
# Structured reserve reports include the provider response bundle in provenance.
SELF_CONTAINED_FACT_TYPES = frozenset({
    "candle.ohlcv", "derivatives.funding_rate", "derivatives.open_interest",
    "market.reference_price", "market.reserve_balance", "asset.reserve_state",
})


def read_canonical_dependency_rows(session, identities, *, reader, max_logical_bytes, check_budget=None):
    """Size the bounded identity set before transferring hot JSON/decoding cold.

    Conservatively charge whole cold pages because hydration decodes those
    pages. An oversized source closure fails before loading its documents.
    """
    if not identities:
        return {}
    logical_bytes = 0
    cold_pages = {}
    for offset in range(0, len(identities), 1000):
        if check_budget is not None:
            check_budget()
        batch = identities[offset:offset + 1000]
        logical_bytes += session.execute(text(f"""
            SELECT coalesce(sum(6::bigint * octet_length(to_jsonb(candidates)::text)),0)
            FROM (SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
                  WHERE versions.id=ANY(:ids) AND hot.id IS NOT NULL) AS candidates
        """), {"ids": batch}).scalar_one()
        pages = session.execute(text("""
            SELECT DISTINCT manifests.* FROM market.fact_versions AS source
            LEFT JOIN market.fact_hot_payloads AS hot ON hot.storage_day=source.storage_day AND hot.id=source.id
            JOIN market.fact_archive_manifests AS manifests ON manifests.storage_day=source.storage_day
              AND (manifests.first_commit_seq,manifests.first_id)<=(source.market_commit_seq,source.id)
              AND (manifests.last_commit_seq,manifests.last_id)>=(source.market_commit_seq,source.id)
            WHERE source.id=ANY(:ids) AND hot.id IS NULL LIMIT :limit
        """), {"ids": batch, "limit": len(batch) + 1}).mappings().all()
        if len(pages) > len(batch):
            raise RuntimeError("canonical_source_dependency_archive_coverage_invalid")
        for page in pages:
            manifest = _catalog_manifest(page)
            if manifest.manifest_id not in cold_pages:
                cold_pages[manifest.manifest_id] = manifest
                logical_bytes += manifest.logical_byte_count
        if logical_bytes > max_logical_bytes:
            raise RuntimeError("canonical_source_dependency_byte_budget_exceeded: reduce archive page size or raise the explicit logical-byte budget")
    return reader.read_rows_by_ids(session, identities)


def verify_canonical_source_placements(session, dependencies, *, objects=None, check_budget=None):
    """Recheck exact source headers and cold bytes while lifecycle-fenced.

    The returned hash is ephemeral: compare it across the destructive handoff,
    never store it in immutable page receipts. Moving a verified source from hot
    to cold is legitimate and must not permanently invalidate its dependents.
    """
    expected = {item["fact_version_id"]: item["row_hash"] for item in dependencies}
    if len(expected) != len(dependencies):
        raise RuntimeError("canonical_source_dependency_duplicate")
    placements = []
    identities = sorted(expected)
    for offset in range(0, len(identities), 1000):
        if check_budget is not None:
            check_budget()
        batch = identities[offset:offset + 1000]
        headers = session.execute(text("""
            SELECT versions.id, versions.row_hash, versions.storage_day,
                   hot.id IS NOT NULL AS hot_present
            FROM market.fact_versions AS versions
            LEFT JOIN market.fact_hot_payloads AS hot
              ON hot.storage_day=versions.storage_day AND hot.id=versions.id
            WHERE versions.id=ANY(:ids) ORDER BY versions.id
        """), {"ids": batch}).mappings().all()
        if {item["id"] for item in headers} != set(batch):
            raise RuntimeError("canonical_source_dependency_missing")
        cold = [item["id"] for item in headers if not item["hot_present"]]
        pages = defaultdict(list)
        if cold:
            matches = session.execute(text("""
                SELECT source.id AS source_id, manifests.*
                FROM market.fact_versions AS source
                JOIN market.fact_retention_partitions AS partition
                  ON partition.storage_day=source.storage_day AND partition.state IN ('verified','reclaimed')
                JOIN market.fact_archive_manifests AS manifests ON manifests.storage_day=source.storage_day
                  AND (manifests.first_commit_seq,manifests.first_id)<=(source.market_commit_seq,source.id)
                  AND (manifests.last_commit_seq,manifests.last_id)>=(source.market_commit_seq,source.id)
                WHERE source.id=ANY(:ids) ORDER BY source.id,manifests.page_ordinal LIMIT :limit
            """), {"ids": cold, "limit": len(cold) + 1}).mappings().all()
            if len(matches) != len(cold):
                raise RuntimeError("canonical_source_dependency_archive_coverage_invalid")
            for item in matches:
                pages[item["source_id"]].append(item)
        for header in headers:
            if check_budget is not None:
                check_budget()
            identity = header["id"]
            if header["row_hash"] != expected[identity]:
                raise RuntimeError(f"canonical_source_dependency_hash_mismatch: fact_version_id={identity}")
            placement = {"id": identity, "storage_day": header["storage_day"].isoformat(), "tier": "hot"}
            if not header["hot_present"]:
                if len(pages[identity]) != 1:
                    raise RuntimeError(f"canonical_source_dependency_archive_coverage_invalid: fact_version_id={identity}")
                manifest = _catalog_manifest(pages[identity][0])
                if objects is not None:
                    objects.verify(manifest.object_key, manifest.object_sha256, expected_bytes=manifest.byte_count)
                placement.update(tier="cold", manifest_id=manifest.manifest_id, manifest_hash=manifest.manifest_hash)
            placements.append(placement)
    return archive_evidence_hash({"placements": placements})

"""Current placement proof for immutable canonical archive source edges."""
from collections import defaultdict
import json

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
# Reduce repeated causal-window round trips without changing source
# admission, result, byte, statement-time, or page budgets.
_CAUSAL_WINDOW_REQUEST_BATCH_ROWS = 512


class CanonicalSourceByteBudgetExceeded(RuntimeError):
    """A source-size preflight refusal, before loading the source documents."""

    def __init__(self, *, logical_bytes: int, max_logical_bytes: int):
        self.logical_bytes = logical_bytes
        self.max_logical_bytes = max_logical_bytes
        super().__init__(
            "canonical_source_dependency_byte_budget_exceeded: "
            f"logical_bytes={logical_bytes} max_logical_bytes={max_logical_bytes}; "
            "reduce archive page size or raise the explicit logical-byte budget"
        )


def resolve_causal_window_revisions(session, *, requests, reader, max_rows, max_logical_bytes, check_budget=None):
    """Keep complete bounded canonical windows, including corrections/invalidations.

    Instrument and family are mandatory. Optional immutable series/source IDs
    narrow a declared scope; an omitted ID means conservative evidence, never
    a lookup through today's mutable stream configuration.
    """
    indexed = {item["root_id"]: item for item in requests}
    if len(indexed) != len(requests) or len(requests) > max_rows:
        raise RuntimeError("canonical_window_request_budget_or_identity_invalid")
    for item in requests:
        if (not item["instrument_id"] or not item["fact_type"] or item["range_start"] > item["range_end"]
                or type(item["root_commit"]) is not int or item["root_commit"] <= 0
                or type(item["include_end"]) is not bool
                or any(value is not None and (type(value) is not int or value <= 0)
                       for value in (item["series_id"], item["source_id"]))):
            raise RuntimeError(f"canonical_window_request_invalid: fact_version_id={item['root_id']}")
    selections = defaultdict(list)
    count = 0
    for offset in range(0, len(requests), _CAUSAL_WINDOW_REQUEST_BATCH_ROWS):
        if check_budget is not None:
            check_budget()
        batch = [{**item, **{name: item[name].isoformat() for name in ("known_at", "range_start", "range_end")}}
                 for item in requests[offset:offset + _CAUSAL_WINDOW_REQUEST_BATCH_ROWS]]
        # Resolve optional series expansion once, then share an exact routed
        # observation range across roots with the same immutable scope. The
        # code-owned reader derives physical storage days from conservative
        # observation bounds, retains the sealed legacy relation, and refuses
        # a directory horizon above 4,096 days. Adding one microsecond is exact
        # for PostgreSQL timestamptz and preserves the resolver's inclusive end,
        # including a zero-width inclusive request, without treating event or
        # known-at dates as physical placement.
        #
        # The metadata sentinel bounds optional series expansion before any
        # routed header read. Directory lookup and header selection remain in
        # this one statement, so both use the same READ COMMITTED snapshot.
        routed = session.execute(text("""
            WITH requested AS (
                SELECT * FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS item(
                    root_id text,instrument_id text,fact_type text,series_id bigint,source_id bigint,
                    root_commit bigint,known_at timestamptz,range_start timestamptz,range_end timestamptz,
                    include_end boolean)
            ), resolved AS MATERIALIZED (
                SELECT requested.*,series.id AS resolved_series_id
                FROM requested JOIN market.series AS series
                  ON series.instrument_id=requested.instrument_id
                 AND series.fact_type=requested.fact_type
                 AND (requested.series_id IS NULL OR series.id=requested.series_id)
                ORDER BY requested.root_id,series.id LIMIT :routing_limit
            ), routing AS MATERIALIZED (
                SELECT count(*)::bigint AS resolved_count FROM resolved
            ), scopes AS MATERIALIZED (
                SELECT resolved_series_id,fact_type,source_id,range_start,range_end,include_end,
                       max(root_commit) AS max_root_commit,max(known_at) AS max_known_at
                FROM resolved CROSS JOIN routing
                WHERE routing.resolved_count<=:max_routing
                  AND (range_start<range_end OR include_end)
                GROUP BY resolved_series_id,fact_type,source_id,range_start,range_end,include_end
            ), routed_sources AS MATERIALIZED (
                SELECT scopes.resolved_series_id,scopes.fact_type,
                       scopes.source_id AS scope_source_id,scopes.range_start,scopes.range_end,
                       scopes.include_end,source.id,source.source_id,source.market_commit_seq,
                       source.known_at,source.observation_time,source.observation_key,source.revision
                FROM scopes CROSS JOIN LATERAL market.read_fact_headers_in_range(
                    scopes.resolved_series_id,scopes.range_start,
                    scopes.range_end + CASE WHEN scopes.include_end THEN interval '1 microsecond'
                                             ELSE interval '0 microseconds' END
                ) AS source
                WHERE source.fact_type=scopes.fact_type
                  AND (scopes.source_id IS NULL OR source.source_id=scopes.source_id)
                  AND source.market_commit_seq<=scopes.max_root_commit
                  AND source.known_at<=scopes.max_known_at
            ), candidates AS MATERIALIZED (
                SELECT resolved.root_id,source.id,source.resolved_series_id,
                       source.observation_key,source.revision
                FROM resolved JOIN routed_sources AS source
                  ON source.resolved_series_id=resolved.resolved_series_id
                 AND source.fact_type=resolved.fact_type
                 AND source.scope_source_id IS NOT DISTINCT FROM resolved.source_id
                 AND source.range_start=resolved.range_start AND source.range_end=resolved.range_end
                 AND source.include_end=resolved.include_end
                 AND source.market_commit_seq<=resolved.root_commit
                 AND source.known_at<=resolved.known_at
                ORDER BY resolved.root_id,source.resolved_series_id,
                         source.observation_key,source.revision LIMIT :limit
            )
            SELECT candidates.root_id,candidates.id,false AS metadata,routing.resolved_count,
                   candidates.resolved_series_id,candidates.observation_key,candidates.revision
            FROM candidates CROSS JOIN routing
            UNION ALL
            SELECT NULL::text,NULL::text,true AS metadata,routing.resolved_count,
                   NULL::bigint,NULL::text,NULL::bigint FROM routing
            ORDER BY metadata,root_id,resolved_series_id,observation_key,revision
        """), {"requests": json.dumps(batch), "limit": max_rows - count + 1,
                 "routing_limit": max_rows + 1, "max_routing": max_rows}).all()
        metadata = [row for row in routed if row[2] is True]
        if len(metadata) != 1:
            raise RuntimeError("canonical_window_routing_metadata_invalid")
        if metadata[0][3] > max_rows:
            raise RuntimeError("canonical_window_routing_budget_exceeded: reduce archive page size")
        found = [(row[0], row[1]) for row in routed if not row[2]]
        count += len(found)
        if count > max_rows:
            raise RuntimeError("canonical_window_source_budget_exceeded: reduce archive page size")
        for root_id, identity in found:
            if root_id not in indexed:
                raise RuntimeError("canonical_window_source_scope_mismatch")
            selections[root_id].append(identity)
    identities = sorted({identity for selected in selections.values() for identity in selected})
    sources = read_canonical_dependency_rows(session, identities, reader=reader,
        max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    if set(sources) != set(identities):
        raise RuntimeError("canonical_window_source_missing")
    for root_id, selected in selections.items():
        request = indexed[root_id]
        for identity in selected:
            if check_budget is not None:
                check_budget()
            row = sources[identity]
            if (row["fact_type"] != request["fact_type"]
                    or (request["series_id"] is not None and row["series_id"] != request["series_id"])
                    or (request["source_id"] is not None and row["source_id"] != request["source_id"])
                    or row["market_commit_seq"] > request["root_commit"] or row["known_at"] > request["known_at"]
                    or row["observation_time"] < request["range_start"] or row["observation_time"] > request["range_end"]
                    or (not request["include_end"] and row["observation_time"] == request["range_end"])):
                raise RuntimeError(f"canonical_window_source_scope_mismatch: fact_version_id={root_id} source_id={identity}")
    return sources, dict(selections)


def collect_source_history_archive_refs(session, *, rows, object_store):
    """Bind raw leaves of an already-resolved canonical source closure."""
    from market_data.archive import RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from .fact_book_prefix import resolve_book_prefixes_for_read
    from .fact_flow_admission import collect_trade_history_archive_refs
    references = collect_trade_history_archive_refs(session,
        rows=[row for row in rows if row["fact_type"] in {"market.trade", "market.trade_flow"}], object_store=object_store)
    objects = ArchiveVerificationBatch(object_store,
        limits=ArchiveVerificationLimits(max_objects=10_000, max_bytes=4 * 1024**3))
    books = resolve_book_prefixes_for_read(session,
        rows=[row for row in rows if row["fact_type"] in {"market.l2_book", "market.bbo", "market.depth_observation"}],
        object_store=object_store, byte_verifier=objects, limits=RawArchiveReadLimits(),
        max_mapping_rows=50_000, max_objects=objects.limits.max_objects)
    for identity, reference in books.items():
        if references.setdefault(identity, reference) != reference:
            raise RuntimeError(f"canonical_history_dependency_conflict: target_id={identity}")
    if len(references) > objects.limits.max_objects:
        raise RuntimeError("canonical_history_object_budget_exceeded: reduce Dataset window")
    return references


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
            raise CanonicalSourceByteBudgetExceeded(
                logical_bytes=logical_bytes, max_logical_bytes=max_logical_bytes)
    return reader.read_rows_by_ids(session, identities)


def _checkpoint_source_groups(session, identities, max_objects, check_budget):
    """Locate exact identities without decoding payloads; one cold page per group."""
    groups, manifests = defaultdict(list), {}
    for offset in range(0, len(identities), 1000):
        if check_budget is not None:
            check_budget()
        batch = identities[offset:offset + 1000]
        locations = session.execute(text(
            "SELECT id,storage_day FROM market.fact_identities WHERE id=ANY(:ids)"
        ), {"ids": batch}).mappings().all()
        if len(locations) != len(batch) or {row["id"] for row in locations} != set(batch):
            raise RuntimeError("canonical_checkpoint_source_identity_coverage_invalid")
        rows = session.execute(text("""
            SELECT source.id AS requested_id, hot.id AS hot_id, manifests.*
            FROM market.fact_versions AS source
            LEFT JOIN market.fact_hot_payloads AS hot
              ON hot.storage_day=source.storage_day AND hot.id=source.id
            LEFT JOIN market.fact_archive_manifests AS manifests
              ON hot.id IS NULL AND manifests.storage_day=source.storage_day
             AND (manifests.first_commit_seq,manifests.first_id)<=(source.market_commit_seq,source.id)
             AND (manifests.last_commit_seq,manifests.last_id)>=(source.market_commit_seq,source.id)
            WHERE source.id=ANY(:ids) AND source.storage_day=ANY(:days)
            ORDER BY source.id,manifests.page_ordinal LIMIT :limit
        """), {"ids": batch, "days": sorted({row["storage_day"] for row in locations}),
               "limit": len(batch) + 1}).mappings().all()
        if len(rows) != len(batch) or {row["requested_id"] for row in rows} != set(batch):
            raise RuntimeError("canonical_checkpoint_source_archive_coverage_invalid")
        for row in rows:
            if row["hot_id"] is not None:
                groups[None].append(row["requested_id"])
                continue
            if row["id"] is None:
                raise RuntimeError("canonical_checkpoint_source_archive_coverage_invalid")
            manifest = _catalog_manifest(row)
            prior = manifests.setdefault(manifest.manifest_id, manifest)
            if len(manifests) > max_objects:
                raise RuntimeError("canonical_checkpoint_source_object_budget_exceeded")
            if prior != manifest:
                raise RuntimeError("canonical_checkpoint_source_archive_catalog_conflict")
            groups[manifest.manifest_id].append(row["requested_id"])
    return groups, manifests


def read_checkpoint_dependency_rows(session, identities, *, reader, byte_verifier,
                                    max_logical_bytes, max_objects, check_budget=None):
    """Bound retained selected rows plus one cold decode, not all backing pages.

    Only checkpoint admission uses this path. The caller retains its exact
    causal selection and lifecycle fence. Every backing object joins the same
    cumulative verification count/byte budget as raw/checkpoint dependencies.
    No page, identity or integrity check is skipped to fit the working set.
    """
    identities = sorted(set(identities))
    if not identities:
        return {}
    groups, manifests = _checkpoint_source_groups(session, identities, max_objects, check_budget)
    result, retained_bytes = {}, 0
    for manifest_id, batch in groups.items():
        if check_budget is not None:
            check_budget()
        remaining = max_logical_bytes - retained_bytes
        if manifest_id is not None:
            manifest = manifests[manifest_id]
            if manifest.logical_byte_count > remaining:
                raise CanonicalSourceByteBudgetExceeded(
                    logical_bytes=retained_bytes + manifest.logical_byte_count,
                    max_logical_bytes=max_logical_bytes)
            byte_verifier.verify(manifest.object_key, manifest.object_sha256,
                                 expected_bytes=manifest.byte_count)
        # Recheck current placement and its whole-page charge immediately before
        # hydration. The common reader verifies all cold bytes and envelopes.
        selected = read_canonical_dependency_rows(session, batch, reader=reader,
            max_logical_bytes=remaining, check_budget=check_budget)
        if set(selected) != set(batch):
            raise RuntimeError("canonical_checkpoint_source_identity_coverage_invalid")
        for identity, row in selected.items():
            if check_budget is not None:
                check_budget()
            # Match the existing conservative hot-document multiplier. Only
            # selected rows survive a group; full decoded pages stay in reader.
            retained_bytes += 6 * len(json.dumps(row, default=str).encode("utf-8"))
            if retained_bytes > max_logical_bytes:
                raise CanonicalSourceByteBudgetExceeded(
                    logical_bytes=retained_bytes, max_logical_bytes=max_logical_bytes)
            result[identity] = row
    return result


def verify_canonical_source_placements(session, dependencies, *, objects=None, check_budget=None, handoff=None, manifest_id=None):
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
        # Permanent identities route the lookup; actual headers, hashes and
        # current hot/cold placement remain independently checked below.
        locations = session.execute(text(
            "SELECT id,storage_day FROM market.fact_identities WHERE id=ANY(:ids)"
        ), {"ids": batch}).mappings().all()
        if len(locations) != len(batch) or {row["id"] for row in locations} != set(batch):
            raise RuntimeError("canonical_source_dependency_missing")
        location_days = {row["id"]: row["storage_day"] for row in locations}
        days = sorted(set(location_days.values()))
        if check_budget is not None:
            check_budget()
        headers = session.execute(text("""
            SELECT versions.id, versions.row_hash, versions.storage_day,
                   hot.id IS NOT NULL AS hot_present
            """ + (", versions.market_commit_seq,versions.ctid::text AS _handoff_tid,versions.tableoid AS _handoff_oid "
                   if handoff is not None else "") + """
            FROM market.fact_versions AS versions
            LEFT JOIN market.fact_hot_payloads AS hot
              ON hot.storage_day=versions.storage_day AND hot.id=versions.id
             AND hot.storage_day=ANY(:days)
            WHERE versions.id=ANY(:ids) AND versions.storage_day=ANY(:days)
            ORDER BY versions.id
        """), {"ids": batch, "days": days}).mappings().all()
        if ({item["id"] for item in headers} != set(batch)
                or any(item["storage_day"] != location_days[item["id"]] for item in headers)):
            raise RuntimeError("canonical_source_dependency_missing")
        cold = [item["id"] for item in headers if not item["hot_present"]]
        pages = defaultdict(list)
        matches = []
        if cold:
            matches = session.execute(text("""
                SELECT source.id AS source_id, manifests.*
                """ + (", manifests.descriptor::text AS _handoff_descriptor " if handoff is not None else "") + """
                FROM market.fact_versions AS source
                JOIN market.fact_retention_partitions AS partition
                  ON partition.storage_day=source.storage_day AND partition.state IN ('verified','reclaimed')
                JOIN market.fact_archive_manifests AS manifests ON manifests.storage_day=source.storage_day
                  AND (manifests.first_commit_seq,manifests.first_id)<=(source.market_commit_seq,source.id)
                  AND (manifests.last_commit_seq,manifests.last_id)>=(source.market_commit_seq,source.id)
                WHERE source.id=ANY(:ids) AND source.storage_day=ANY(:days)
                ORDER BY source.id,manifests.page_ordinal LIMIT :limit
            """), {"ids": cold, "days": sorted({row["storage_day"] for row in headers if not row["hot_present"]}),
                    "limit": len(cold) + 1}).mappings().all()
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
        if handoff is not None:
            handoff.capture_sources(session, manifest_id, headers, matches)
    return archive_evidence_hash({"placements": placements})

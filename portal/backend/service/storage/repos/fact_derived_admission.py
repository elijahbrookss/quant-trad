"""Exact causal source closure for admitted derived canonical facts.

Material hashes identify input content, not a current delivery revision. Keep
every matching revision available at the derived fact's clocks. Hot provenance
and cold aliases only locate candidates; the canonical reader proves them.
"""
from collections import defaultdict
import json
import re

from sqlalchemy import text

from market_data.canonical_adapters import canonicalize_basis_feature, decode_bbo_feature_record
from market_data.canonical_storage import LEGACY_MATERIAL_EVIDENCE_KEYS, legacy_material_alias, record_from_storage_row
from market_data.fact_archive import FactArchiveLimits
from market_data.market_state import derive_basis_features

from .fact_dependencies import read_canonical_dependency_rows
from .fact_storage import PostgresCanonicalFactStorageRepository


DERIVED_FACT_TYPES = frozenset({"market.futures_spot_relationship", "market.derivative_state", "market.trade_flow", "market.trade_flow_feature"})


def resolve_material_source_revisions(session, *, requests, reader, max_rows, max_logical_bytes, check_budget=None):
    """Resolve bounded (root, role) witnesses without selecting latest aliases.

    Each request includes its fact family and causal clocks. All matched source
    rows are rechecked after hydration, including aliases, scope and clocks.
    The match budget counts repeated edges as well as unique payloads.
    """
    if len(requests) > max_rows:
        raise RuntimeError("canonical_material_source_budget_exceeded: reduce archive page size")
    indexed = {}
    for request in requests:
        identity = (request["root_id"], request["role"])
        if (identity in indexed or type(request["series_id"]) is not int or request["series_id"] <= 0
                or type(request["commit_seq"]) is not int or request["commit_seq"] <= 0
                or not isinstance(request["material_hash"], str)
                or re.fullmatch(r"[0-9a-f]{64}", request["material_hash"]) is None):
            raise RuntimeError(f"canonical_material_source_identity_invalid: fact_version_id={request['root_id']} role={request['role']}")
        indexed[identity] = request
    selections = defaultdict(list)
    count = 0
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        batch = [{**request, "known_at": request["known_at"].isoformat(),
                  "evidence_key": LEGACY_MATERIAL_EVIDENCE_KEYS.get(request["fact_type"])}
                 for request in requests[offset:offset + 128]]
        found = session.execute(text("""
            WITH requested AS (
                SELECT * FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS item(
                    root_id text,role text,series_id bigint,fact_type text,material_hash text,
                    commit_seq bigint,known_at timestamptz,evidence_key text)
            ), candidates AS (
                SELECT requested.root_id,requested.role,source.id
                FROM requested JOIN market.fact_versions AS source
                  ON source.series_id=requested.series_id AND source.fact_type=requested.fact_type
                  AND source.material_hash=requested.material_hash
                  AND source.market_commit_seq<=requested.commit_seq AND source.known_at<=requested.known_at
                UNION
                SELECT requested.root_id,requested.role,source.id
                FROM requested JOIN market.fact_versions AS source
                  ON source.series_id=requested.series_id AND source.fact_type=requested.fact_type
                  AND source.market_commit_seq<=requested.commit_seq AND source.known_at<=requested.known_at
                JOIN market.fact_hot_payloads AS hot ON hot.storage_day=source.storage_day AND hot.id=source.id
                WHERE CASE WHEN requested.evidence_key IS NOT NULL THEN
                    hot.provenance @> jsonb_build_object(requested.evidence_key,
                        jsonb_build_object('legacy_material_hash',requested.material_hash))
                    ELSE false END
                UNION
                SELECT requested.root_id,requested.role,source.id
                FROM requested JOIN market.fact_archive_material_aliases AS aliases
                  ON aliases.series_id=requested.series_id AND aliases.evidence_key=requested.evidence_key
                  AND aliases.material_hash=requested.material_hash
                JOIN market.fact_versions AS source ON source.id=aliases.fact_version_id
                  AND source.series_id=requested.series_id AND source.fact_type=requested.fact_type
                  AND source.market_commit_seq<=requested.commit_seq AND source.known_at<=requested.known_at
                JOIN market.fact_archive_manifests AS manifest ON manifest.id=aliases.manifest_id
                  AND manifest.storage_day=source.storage_day
                JOIN market.fact_retention_partitions AS partition ON partition.storage_day=source.storage_day
                  AND partition.state IN ('verified','reclaimed')
                WHERE NOT EXISTS (SELECT 1 FROM market.fact_hot_payloads AS hot
                    WHERE hot.storage_day=source.storage_day AND hot.id=source.id)
            )
            SELECT root_id,role,id FROM candidates ORDER BY root_id,role,id LIMIT :limit
        """), {"requests": json.dumps(batch), "limit": max_rows - count + 1}).all()
        count += len(found)
        if count > max_rows:
            raise RuntimeError("canonical_material_source_budget_exceeded: reduce archive page size")
        for root, role, identity in found:
            selections[(root, role)].append(identity)
    identities = sorted({identity for ids in selections.values() for identity in ids})
    sources = read_canonical_dependency_rows(session, identities, reader=reader,
        max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    if set(sources) != set(identities):
        raise RuntimeError("canonical_material_source_missing: selected canonical revisions are unavailable")
    for row in sources.values():
        if check_budget is not None:
            check_budget()
        record_from_storage_row(row)
    for identity, request in indexed.items():
        selected = [sources[source_id] for source_id in selections[identity]]
        if not selected:
            raise RuntimeError(f"canonical_material_source_missing: fact_version_id={identity[0]} role={identity[1]}")
        for row in selected:
            alias = legacy_material_alias(row)
            if (row["series_id"] != request["series_id"] or row["fact_type"] != request["fact_type"]
                    or row["market_commit_seq"] > request["commit_seq"] or row["known_at"] > request["known_at"]
                    or request["material_hash"] not in {row["material_hash"], alias["material_hash"] if alias else None}):
                raise RuntimeError(f"canonical_material_source_mismatch: fact_version_id={identity[0]} role={identity[1]} source_id={row['id']}")
        if len({row["observation_key"] for row in selected}) != 1:
            raise RuntimeError(f"canonical_material_source_ambiguous: fact_version_id={identity[0]} role={identity[1]}")
    return sources, dict(selections)


def resolve_basis_source_revisions(session, *, rows, object_store, max_rows, max_logical_bytes,
                                     max_file_bytes=128 * 1024**2, check_budget=None):
    """Verify basis against its declared BBO pair using the derivation owner.

    Mapping metadata remains immutable in PostgreSQL. Its scope/effective range
    must agree, but retention does not retroactively change the existing basis
    known-at contract to include mapping registration time.
    """
    roots = [row for row in rows if row["fact_type"] == "market.futures_spot_relationship"]
    requests = []
    for row in roots:
        record_from_storage_row(row)
        evidence = row["provenance"].get("_qt_basis_evidence")
        if not isinstance(evidence, dict):
            raise RuntimeError(f"canonical_basis_source_evidence_missing: fact_version_id={row['id']}")
        for role in ("futures", "spot"):
            requests.append({"root_id": row["id"], "role": role, "series_id": evidence.get(f"{role}_series_id"),
                "material_hash": evidence.get(f"{role}_bbo_material_hash"), "fact_type": "market.bbo",
                "commit_seq": row["market_commit_seq"], "known_at": row["known_at"]})
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    sources, selections = resolve_material_source_revisions(session, requests=requests, reader=reader,
        max_rows=max_rows, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    decoded = {identity: decode_bbo_feature_record(record_from_storage_row(row)).fact for identity, row in sources.items()}
    mappings = {}
    for offset in range(0, len(roots), 128):
        if check_budget is not None:
            check_budget()
        batch = [{"root_id": row["id"], "series_id": row["series_id"], "mapping_id": row["payload"]["mapping_id"],
                  "futures_series_id": row["provenance"]["_qt_basis_evidence"]["futures_series_id"],
                  "spot_series_id": row["provenance"]["_qt_basis_evidence"]["spot_series_id"]}
                 for row in roots[offset:offset + 128]]
        found = session.execute(text("""
            SELECT requested.root_id,mapping.*,root.instrument_id AS root_instrument,
                   futures.instrument_id AS futures_instrument,spot.instrument_id AS spot_instrument
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,series_id bigint,mapping_id text,futures_series_id bigint,spot_series_id bigint)
            LEFT JOIN market.instrument_role_mapping_versions AS mapping ON mapping.id=requested.mapping_id
            LEFT JOIN market.series AS root ON root.id=requested.series_id
            LEFT JOIN market.series AS futures ON futures.id=requested.futures_series_id
            LEFT JOIN market.series AS spot ON spot.id=requested.spot_series_id
        """), {"requests": json.dumps(batch)}).mappings().all()
        mappings.update({item["root_id"]: item for item in found})
    for row in roots:
        if check_budget is not None:
            check_budget()
        mapping = mappings.get(row["id"])
        if (mapping is None or mapping["id"] is None or mapping["role"] != "spot_reference"
                or mapping["primary_instrument_id"] != mapping["root_instrument"]
                or mapping["primary_instrument_id"] != mapping["futures_instrument"]
                or mapping["related_instrument_id"] != mapping["spot_instrument"]
                or mapping["effective_from"] > row["observation_time"]
                or (mapping["effective_to"] is not None and mapping["effective_to"] <= row["observation_time"])):
            raise RuntimeError(f"canonical_basis_mapping_missing_or_mismatched: fact_version_id={row['id']} mapping_id={row['payload']['mapping_id']}")
        # Typed decoding checks every legacy material witness. Repeated delivery
        # revisions then share the same declared content; no Cartesian re-derive
        # is needed, and every causal revision still gets a permanent edge.
        pair = [decoded[selections[(row["id"], role)][0]] for role in ("futures", "spot")]
        expected = derive_basis_features([pair[0]], [pair[1]], mapping_id=mapping["id"],
            computed_at=row["known_at"], series_id=row["series_id"])
        alias = legacy_material_alias(row)
        if (len(expected) != 1 or canonicalize_basis_feature(expected[0]).payload != row["payload"]
                or expected[0].effective_at != row["observation_time"]
                or alias is None or alias["material_hash"] != expected[0].material_hash):
            raise RuntimeError(f"canonical_basis_source_derivation_mismatch: fact_version_id={row['id']}")
    return [sources[identity] for identity in sorted(sources)]


def resolve_derived_source_revisions(session, *, rows, max_rows, **kwargs):
    """Compose the admitted family owners under one bounded source-edge set."""
    from .fact_derivative_admission import resolve_derivative_source_revisions
    from .fact_flow_admission import resolve_trade_flow_source_revisions
    from .fact_flow_feature_admission import resolve_flow_feature_source_revisions
    resolvers = {"market.futures_spot_relationship": resolve_basis_source_revisions,
                 "market.derivative_state": resolve_derivative_source_revisions,
                 "market.trade_flow": resolve_trade_flow_source_revisions,
                 "market.trade_flow_feature": resolve_flow_feature_source_revisions}
    grouped = defaultdict(list)
    for row in rows:
        if row["fact_type"] in resolvers:
            grouped[row["fact_type"]].append(row)
    sources = {}
    for fact_type, group in grouped.items():
        found = resolvers[fact_type](session, rows=group, max_rows=max_rows, **kwargs)
        for row in found:
            if row["id"] in sources and sources[row["id"]] != row:
                raise RuntimeError(f"canonical_derived_source_conflict: fact_version_id={row['id']}")
            sources[row["id"]] = row
        if len(sources) > max_rows:
            raise RuntimeError("canonical_archive_source_dependency_budget_exceeded: reduce archive page size")
    return [sources[identity] for identity in sorted(sources)]

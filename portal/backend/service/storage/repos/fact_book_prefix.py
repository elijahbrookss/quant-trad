"""Shared, bounded raw-prefix proofs for canonical book archival.

Each transaction certifies only the next dense interval and holds its chosen
immutable objects permanently. Later transactions extend that chain rather than
decoding the full connection history again. This is source-lineage evidence,
not a checkpoint/reconstruction proof or permission to reclaim hot data.
"""
from __future__ import annotations

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db import MarketFactBookPrefixChunkRecord, MarketFactBookPrefixDependencyRecord
from .fact_lineage import BOOK_SCOPE_FIELDS, _witness, canonical_book_prefixes, resolve_canonical_raw_archive_refs
from .market_lifecycle import MarketStorageLifecycleBusyError

BOOK_PREFIX_VERIFIER_VERSION = "market.book_prefix_verification.v1"
_SCOPE_WHERE = " AND ".join(f"{name}=:{name}" for name in (*BOOK_SCOPE_FIELDS, "verifier_version"))
_REFERENCE_FIELDS = ("object_key", "object_uri", "object_sha256", "content_fingerprint")


def _scope(prefix):
    return {name: prefix[name] for name in BOOK_SCOPE_FIELDS} | {"verifier_version": BOOK_PREFIX_VERIFIER_VERSION}


def _validate_chunk(chunk, scope):
    descriptor = chunk["descriptor"]
    fields = (*scope, "first_receive_ordinal", "last_receive_ordinal", "previous_chunk_id")
    if (not isinstance(descriptor, dict) or set(descriptor) != {*fields, "dependencies"}
            or any(chunk[name] != value for name, value in scope.items())
            or any(descriptor.get(name) != chunk[name] for name in fields)
            or archive_evidence_hash(descriptor) != chunk["evidence_hash"]
            or chunk["id"] != "fbp_" + chunk["evidence_hash"]
            or type(chunk["first_receive_ordinal"]) is not int
            or type(chunk["last_receive_ordinal"]) is not int
            or not 1 <= chunk["first_receive_ordinal"] <= chunk["last_receive_ordinal"]
            or not isinstance(descriptor["dependencies"], list) or not descriptor["dependencies"]):
        raise RuntimeError(f"canonical_book_prefix_certificate_invalid: chunk_id={chunk['id']}")
    return descriptor


def _dependencies(session, chunk, *, max_objects):
    """Rebind the receipt to its permanent holds and immutable raw descriptors."""
    found = session.execute(text("""
        SELECT holds.target_id, holds.object_key AS held_key, holds.object_sha256 AS held_sha256,
               manifests.object_key, manifests.object_uri, manifests.object_sha256,
               manifests.content_fingerprint, manifests.byte_count,
               EXISTS (SELECT 1 FROM market.storage_lifecycle_events AS events
                   WHERE events.action='archive_expire' AND events.target_kind='raw_manifest'
                     AND events.target_id=manifests.id AND events.event_type IN ('planned','completed')) AS unavailable
        FROM market.fact_book_prefix_dependencies AS holds
        JOIN market.raw_archive_manifests AS manifests ON manifests.id=holds.target_id
        WHERE holds.chunk_id=:id ORDER BY holds.target_id LIMIT :limit
    """), {"id": chunk["id"], "limit": max_objects + 1}).mappings().all()
    if len(found) > max_objects:
        raise RuntimeError("canonical_book_prefix_object_budget_exceeded")
    expected = [{"target_id": row["target_id"], **{name: row[name] for name in _REFERENCE_FIELDS}} for row in found]
    if expected != chunk["descriptor"]["dependencies"] or any(
            row["held_key"] != row["object_key"] or row["held_sha256"] != row["object_sha256"] or row["unavailable"]
            for row in found):
        raise RuntimeError(f"canonical_book_prefix_dependencies_invalid: chunk_id={chunk['id']}")
    return found


def prepare_next_book_prefix(session, *, rows, object_store, byte_verifier, limits,
                             max_mapping_rows, max_objects, check_budget=None, bound_manifest_ids=None):
    """Commit at most one bounded interval through the caller's transaction.

    The caller holds the lifecycle shared fence and retains the hot page. A
    per-scope transaction lock serializes extensions from different partitions.
    Nothing commits if decoding, cancellation, or catalog validation fails.
    """
    for prefix in sorted(canonical_book_prefixes(rows), key=lambda item: tuple(item[name] for name in BOOK_SCOPE_FIELDS)):
        if check_budget is not None:
            check_budget()
        scope = _scope(prefix)
        acquired = session.execute(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
            {"name": "quant-trad:book-prefix:" + archive_evidence_hash(scope)}).scalar_one()
        if not acquired:
            raise MarketStorageLifecycleBusyError("canonical_book_prefix_busy: retry committed progress")
        previous = session.execute(text(
            f"SELECT * FROM market.fact_book_prefix_chunks WHERE {_SCOPE_WHERE} "
            "ORDER BY last_receive_ordinal DESC LIMIT 1"
        ), scope).mappings().one_or_none()
        if previous is not None:
            _validate_chunk(previous, scope)
        first = previous["last_receive_ordinal"] + 1 if previous is not None else 1
        if first > prefix["receive_ordinal"]:
            continue
        # Leave room for multiple acknowledged placements of the same raw
        # positions. A pathological alias fan-out still fails the hard budget.
        last = min(prefix["receive_ordinal"], first + max(1, max_mapping_rows // 4) - 1)
        references = resolve_canonical_raw_archive_refs(
            session, rows=[], object_store=object_store, byte_verifier=byte_verifier,
            limits=limits, max_mapping_rows=max_mapping_rows, check_budget=check_budget,
            bound_manifest_ids=bound_manifest_ids,
            book_prefix_ranges=[{**prefix, "first_receive_ordinal": first, "receive_ordinal": last}],
        )
        if len(references) > max_objects:
            raise RuntimeError("canonical_book_prefix_object_budget_exceeded")
        descriptor = {**scope, "first_receive_ordinal": first, "last_receive_ordinal": last,
            "previous_chunk_id": previous["id"] if previous is not None else None,
            "dependencies": [{"target_id": identity, **reference} for identity, reference in sorted(references.items())]}
        evidence_hash = archive_evidence_hash(descriptor)
        chunk = {key: value for key, value in descriptor.items() if key != "dependencies"} | {
            "id": "fbp_" + evidence_hash, "descriptor": descriptor, "evidence_hash": evidence_hash,
        }
        session.add(MarketFactBookPrefixChunkRecord(**chunk))
        session.flush()
        for identity, reference in sorted(references.items()):
            session.add(MarketFactBookPrefixDependencyRecord(chunk_id=chunk["id"], target_id=identity,
                object_key=reference["object_key"], object_sha256=reference["object_sha256"]))
        session.flush()
        _dependencies(session, chunk, max_objects=max_objects)
        byte_verifier.assert_unchanged()
        if check_budget is not None:
            check_budget()
        return {"status": "book_prefix_verified", "chunk_id": chunk["id"],
            "definition_id": prefix["definition_id"], "session_id": prefix["session_id"],
            "connection_epoch": prefix["connection_epoch"], "first_receive_ordinal": first,
            "last_receive_ordinal": last, "required_receive_ordinal": prefix["receive_ordinal"]}
    return None


def resolve_verified_book_prefixes(session, *, rows, byte_verifier, max_objects,
                                    bound_manifest_ids=None, check_budget=None, max_chunks=5000):
    """Validate contiguous receipts and CURRENT bytes, without re-decoding history.

    Earlier admitted placements remain bound even after new aliases appear.
    Chunks can conservatively hold frames past a smaller root in another page;
    this changes no Fact or known-at selection and grants no future data access.
    """
    references, root_bindings = {}, {}
    inspected = 0
    for prefix in canonical_book_prefixes(rows):
        scope = _scope(prefix)
        roots = []
        for row in rows:
            if row["fact_type"] in {"market.l2_book", "market.bbo", "market.depth_observation"}:
                _, evidence = _witness(row)
                if all(evidence[name] == prefix[name] for name in BOOK_SCOPE_FIELDS):
                    roots.append((evidence["receive_ordinal"], row["id"]))
        roots.sort(reverse=True)
        chunks = session.execute(text(
            f"SELECT * FROM market.fact_book_prefix_chunks WHERE {_SCOPE_WHERE} "
            "AND first_receive_ordinal<=:until ORDER BY first_receive_ordinal LIMIT :limit"
        ), {**scope, "until": prefix["receive_ordinal"], "limit": max_chunks - inspected + 1}).mappings().all()
        inspected += len(chunks)
        if inspected > max_chunks:
            raise RuntimeError("canonical_book_prefix_chunk_budget_exceeded")
        first, previous_id = 1, None
        for chunk in chunks:
            if check_budget is not None:
                check_budget()
            _validate_chunk(chunk, scope)
            if chunk["first_receive_ordinal"] != first or chunk["previous_chunk_id"] != previous_id:
                raise RuntimeError(f"canonical_book_prefix_chain_incomplete: chunk_id={chunk['id']}")
            dependencies = _dependencies(session, chunk, max_objects=max_objects)
            for item in dependencies:
                identity = item["target_id"]
                if bound_manifest_ids is not None and identity not in bound_manifest_ids:
                    raise RuntimeError(f"canonical_book_prefix_bound_dependency_missing: target_id={identity}")
                reference = {name: item[name] for name in _REFERENCE_FIELDS}
                if references.setdefault(identity, reference) != reference:
                    raise RuntimeError(f"canonical_book_prefix_dependency_conflict: target_id={identity}")
                if len(references) > max_objects:
                    raise RuntimeError("canonical_book_prefix_object_budget_exceeded")
                byte_verifier.verify(item["object_key"], item["object_sha256"], expected_bytes=item["byte_count"])
            # A newer placement must not substitute a different frame for an
            # already certified position. Root witnesses bind to THEIR chunk,
            # not the union of objects from later intervals in this connection.
            while roots and roots[-1][0] <= chunk["last_receive_ordinal"]:
                _, identity = roots.pop()
                root_bindings[identity] = {item["target_id"] for item in dependencies}
            first, previous_id = chunk["last_receive_ordinal"] + 1, chunk["id"]
        if first <= prefix["receive_ordinal"]:
            raise RuntimeError(f"canonical_book_prefix_not_ready: fact_version_id={prefix['root_fact_version_id']} next_ordinal={first}")
    byte_verifier.assert_unchanged()
    return references, root_bindings

"""Narrow raw-reference admission shared by all canonical writer lanes.

This is a catalog/lifetime check, not a substitute for archive byte verification.
Expiry owns UPDATE locks on the same immutable manifest rows through unlink and
its completion event; canonical writers hold KEY SHARE through their commit.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
import json

from sqlalchemy import text

from market_data.canonical_storage import LEGACY_MATERIAL_EVIDENCE_KEYS


def _book_position(position, *, observation_key):
    if (not isinstance(position, Mapping)
            or not isinstance(position.get("definition_id"), str)
            or not isinstance(position.get("session_id"), str)
            or not position.get("definition_id") or not position.get("session_id")
            or type(position.get("connection_epoch")) is not int
            or type(position.get("receive_ordinal")) is not int
            or not 0 <= position["connection_epoch"] <= 9223372036854775807
            or not 1 <= position["receive_ordinal"] <= 9223372036854775807):
        raise ValueError(f"canonical_raw_reference_invalid: observation_key={observation_key}")
    return {name: position[name] for name in ("definition_id", "session_id", "connection_epoch", "receive_ordinal")}


def _unprotected_book_prefixes(session, prefixes):
    """A hot book row or committed prefix holds every object in its session.

    SELECT retains relation locks through this writer's commit, so reclamation
    cannot drop that row between this proof and publication of its successor.
    A cold-only session's immutable prefix is also a lifetime anchor, sharing
    the lifecycle session-hold predicate. It needs no hot payload lock and must
    not rescan an ever-growing prefix for each new update. A late import with
    neither holder must lock/check its complete prefix before creating a hold.
    """
    if not prefixes:
        return {}
    protected = set(session.execute(text("""
        SELECT requested.request_key FROM jsonb_to_recordset(CAST(:prefixes AS jsonb))
            AS requested(request_key text,definition_id text,session_id text)
        WHERE EXISTS (SELECT 1 FROM market.fact_hot_payloads AS hot
            WHERE hot.provenance @> jsonb_build_object('_qt_l2_evidence',
                jsonb_build_object('definition_id',requested.definition_id,'session_id',requested.session_id))
               OR hot.provenance @> jsonb_build_object('_qt_bbo_evidence',jsonb_build_object('source_position',
                jsonb_build_object('definition_id',requested.definition_id,'session_id',requested.session_id)))
               OR hot.provenance @> jsonb_build_object('_qt_depth_evidence',jsonb_build_object('source_position',
                jsonb_build_object('definition_id',requested.definition_id,'session_id',requested.session_id))))
           OR EXISTS (SELECT 1 FROM market.fact_book_prefix_chunks AS prefixes
                WHERE prefixes.definition_id=requested.definition_id AND prefixes.session_id=requested.session_id)
    """), {"prefixes": json.dumps(list(prefixes.values()))}).scalars())
    return {key: value for key, value in prefixes.items() if key not in protected}


def lock_canonical_raw_references(session, facts, *, max_mapping_rows=50_000):
    if type(max_mapping_rows) is not int or max_mapping_rows <= 0:
        raise ValueError("canonical_raw_reference_budget_invalid")
    raw_ids, positions, coverages, prefixes = set(), {}, {}, {}
    for fact in facts:
        evidence = fact.provenance
        book_position = None
        if fact.fact_type in {"market.trade", "market.l2_book"}:
            key = "_qt_trade_evidence" if fact.fact_type == "market.trade" else "_qt_l2_evidence"
            reference = evidence.get(key)
            identity = reference.get("raw_record_id") if isinstance(reference, Mapping) else None
            if identity is not None:
                if not isinstance(identity, str) or not identity:
                    raise ValueError(f"canonical_raw_reference_invalid: observation_key={fact.observation_key}")
                raw_ids.add(identity)
                if fact.fact_type == "market.l2_book":
                    book_position = _book_position(reference, observation_key=fact.observation_key)
        elif fact.fact_type in {"market.bbo", "market.depth_observation"}:
            reference = evidence.get(LEGACY_MATERIAL_EVIDENCE_KEYS[fact.fact_type])
            position = reference.get("source_position") if isinstance(reference, Mapping) else None
            if position is not None:
                item = _book_position(position, observation_key=fact.observation_key)
                identity = "position:" + json.dumps(item, sort_keys=True, separators=(",", ":"))
                positions[identity] = {"request_key": identity, **item}
                book_position = item
        elif fact.fact_type == "market.trade_flow":
            reference = evidence.get("_qt_trade_flow_evidence")
            if isinstance(reference, Mapping) and reference.get("coverage_interval_id"):
                identity = str(reference["coverage_interval_id"])
                revision = reference.get("coverage_revision")
                if type(revision) is not int or revision <= 0:
                    raise ValueError(f"canonical_raw_reference_invalid: observation_key={fact.observation_key}")
                key = f"coverage:{identity}:{revision}"
                coverages[key] = {"request_key": key, "interval_id": identity, "revision": revision}
        if book_position is not None:
            scope = {name: book_position[name] for name in ("definition_id", "session_id", "connection_epoch")}
            key = "prefix:" + json.dumps(scope, sort_keys=True, separators=(",", ":"))
            if key not in prefixes or book_position["receive_ordinal"] > prefixes[key]["receive_ordinal"]:
                prefixes[key] = {"request_key": key, **book_position}
    requested = {"record:" + identity for identity in raw_ids} | set(positions) | set(coverages)
    if not requested:
        return
    if len(requested) > max_mapping_rows:
        raise RuntimeError("canonical_raw_reference_budget_exceeded: reduce canonical batch size")
    # A snapshot established before waiting on a row lock must not hide an
    # expiry completion that committed while waiting. Normal canonical writes
    # use READ COMMITTED; reject a caller-supplied stale snapshot explicitly.
    isolation = session.execute(text("SHOW transaction_isolation")).scalar_one()
    if isolation != "read committed":
        raise RuntimeError("canonical_raw_reference_isolation_invalid: writes require READ COMMITTED")
    prefixes = _unprotected_book_prefixes(session, prefixes)
    if sum(prefix["receive_ordinal"] for prefix in prefixes.values()) > max_mapping_rows:
        raise RuntimeError("canonical_raw_reference_prefix_budget_exceeded: late book import requires a bounded complete prefix")
    requested.update(prefixes)
    queries = []
    if raw_ids:
        queries.append(("""
            SELECT 'record:' || mappings.raw_record_id AS request_key,
                   mappings.raw_record_id, mappings.manifest_id
            FROM market.raw_archive_record_mappings AS mappings
            WHERE mappings.raw_record_id=ANY(:ids)
        """, {"ids": sorted(raw_ids)}))
    if positions:
        queries.append(("""
            SELECT requested.request_key,mappings.raw_record_id,mappings.manifest_id
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                request_key text,definition_id text,session_id text,connection_epoch bigint,receive_ordinal bigint)
            JOIN market.raw_archive_manifests AS manifests
              ON manifests.definition_id=requested.definition_id AND manifests.session_id=requested.session_id
             AND manifests.connection_epoch=requested.connection_epoch
             AND manifests.first_receive_ordinal<=requested.receive_ordinal
             AND manifests.last_receive_ordinal>=requested.receive_ordinal
            JOIN market.raw_archive_record_mappings AS mappings ON mappings.manifest_id=manifests.id
             AND mappings.session_id=requested.session_id AND mappings.connection_epoch=requested.connection_epoch
             AND mappings.receive_ordinal=requested.receive_ordinal
        """, {"requests": json.dumps(list(positions.values()))}))
    if coverages:
        queries.append(("""
            SELECT requested.request_key,mappings.raw_record_id,mappings.manifest_id
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(request_key text,interval_id text,revision bigint)
            JOIN market.stream_coverage_interval_versions AS coverage
              ON coverage.interval_id=requested.interval_id AND coverage.revision=requested.revision
            JOIN market.raw_archive_manifests AS manifests
              ON manifests.definition_id=coverage.definition_id AND manifests.session_id=coverage.session_id
             AND manifests.connection_epoch=coverage.connection_epoch
            JOIN market.raw_archive_record_mappings AS mappings ON mappings.manifest_id=manifests.id
             AND mappings.session_id=coverage.session_id AND mappings.connection_epoch=coverage.connection_epoch
             AND mappings.receive_ordinal>=coverage.opening_receive_ordinal
             AND mappings.receive_ordinal<=coverage.last_receive_ordinal
        """, {"requests": json.dumps(list(coverages.values()))}))
    if prefixes:
        queries.append(("""
            SELECT requested.request_key,mappings.raw_record_id,mappings.manifest_id,mappings.receive_ordinal
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                request_key text,definition_id text,session_id text,connection_epoch bigint,receive_ordinal bigint)
            JOIN market.raw_archive_manifests AS manifests
              ON manifests.definition_id=requested.definition_id AND manifests.session_id=requested.session_id
             AND manifests.connection_epoch=requested.connection_epoch
            JOIN market.raw_archive_record_mappings AS mappings ON mappings.manifest_id=manifests.id
             AND mappings.session_id=requested.session_id AND mappings.connection_epoch=requested.connection_epoch
             AND mappings.receive_ordinal BETWEEN 1 AND requested.receive_ordinal
        """, {"requests": json.dumps(list(prefixes.values()))}))
    found = []
    for query, parameters in queries:
        found.extend(session.execute(text(query + " ORDER BY request_key,raw_record_id,manifest_id LIMIT :limit"),
                                     {**parameters, "limit": max_mapping_rows - len(found) + 1}).mappings())
        if len(found) > max_mapping_rows:
            raise RuntimeError("canonical_raw_reference_budget_exceeded: reduce canonical batch size")
    missing = requested - {row["request_key"] for row in found}
    if missing:
        raise RuntimeError(f"canonical_raw_reference_missing: reference={min(missing)}")
    prefix_positions = defaultdict(dict)
    for row in found:
        key = row["request_key"]
        if key in prefixes:
            ordinal = row["receive_ordinal"]
            if type(ordinal) is not int or not 1 <= ordinal <= prefixes[key]["receive_ordinal"]:
                raise RuntimeError(f"canonical_raw_reference_prefix_position_invalid: reference={key}")
            prior = prefix_positions[key].setdefault(ordinal, row["raw_record_id"])
            if prior != row["raw_record_id"]:
                raise RuntimeError(f"canonical_raw_reference_prefix_ambiguous: reference={key} ordinal={ordinal}")
    for key, prefix in prefixes.items():
        if len(prefix_positions[key]) != prefix["receive_ordinal"]:
            raise RuntimeError(f"canonical_raw_reference_prefix_incomplete: reference={key}")
    manifests = sorted({row["manifest_id"] for row in found})
    locked = session.execute(text("""
        SELECT id FROM market.raw_archive_manifests WHERE id=ANY(:ids) ORDER BY id FOR KEY SHARE
    """), {"ids": manifests}).scalars().all()
    if locked != manifests:
        raise RuntimeError("canonical_raw_reference_catalog_changed: retry admission")
    # This separate statement observes expiry committed during the lock wait.
    expiration_events = session.execute(text("""
        SELECT target_id,event_type FROM market.storage_lifecycle_events
        WHERE action='archive_expire' AND target_kind='raw_manifest'
          AND event_type IN ('planned','completed') AND target_id=ANY(:ids)
    """), {"ids": manifests}).mappings().all()
    expired = {row["target_id"] for row in expiration_events if row["event_type"] == "completed"}
    # A crash can release the lock after unlink but before its completion
    # event. A durable execution intent is therefore unavailable too, even if
    # a later failure/skipped event exists: neither proves the bytes survived.
    # Resume that expiration before trying to publish a new reference to it.
    unavailable = {row["target_id"] for row in expiration_events}
    copies = defaultdict(set)
    for row in found:
        copies[(row["request_key"], row["raw_record_id"])].add(row["manifest_id"])
    for (request, raw_id), placements in copies.items():
        if not placements - unavailable:
            if not placements - expired:
                raise RuntimeError(f"canonical_raw_reference_expired: reference={request} raw_record_id={raw_id}")
            raise RuntimeError(
                f"canonical_raw_reference_expiration_pending: reference={request} raw_record_id={raw_id}; "
                "resume the recorded archive expiration before retrying admission"
            )


__all__ = ["lock_canonical_raw_references"]

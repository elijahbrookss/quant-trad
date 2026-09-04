"""Bounded immutable book dependencies for canonical archive admission.

No latest-state aliases or mutable stream configuration are used. Canonical
source revisions and raw prefixes are separate obligations: one cannot stand
in for the other. Operational validity/product metadata remains in PostgreSQL.
"""
from __future__ import annotations

from collections import defaultdict
import json

from sqlalchemy import text

from market_data.book_archive import restore_book_checkpoint_parquet
from market_data.canonical_storage import record_from_storage_row
from market_data.fact_archive import FactArchiveLimits
from market_data.order_book import BOOK_RECONSTRUCTION_VERSION, L2ProductContract

from .fact_lineage import _witness, BOOK_SCOPE_FIELDS
from .fact_storage import PostgresCanonicalFactStorageRepository
from .fact_dependencies import read_canonical_dependency_rows


BOOK_FACT_TYPES = frozenset({"market.l2_book", "market.bbo", "market.depth_observation"})


def resolve_book_source_revisions(session, *, rows, object_store, max_rows, max_logical_bytes,
                                  max_file_bytes=128 * 1024**2, check_budget=None):
    """Return every causally eligible exact L2 revision for declared book states.

    A derived record identifies an L2 position and state, not a delivery revision
    ID. Preserve all matching historical revisions, bounded by its own commit
    and known-at clocks, rather than silently selecting a current revision.
    """
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    requests = []
    for row in rows:
        if row["fact_type"] not in BOOK_FACT_TYPES - {"market.l2_book"}:
            continue
        if check_budget is not None:
            check_budget()
        _, position = _witness(row)
        evidence = row["provenance"]["_qt_bbo_evidence" if row["fact_type"] == "market.bbo" else "_qt_depth_evidence"]
        series_id = evidence.get("source_l2_series_id")
        event_ordinal = position.get("event_ordinal")
        if type(series_id) is not int or series_id <= 0 or type(event_ordinal) is not int or event_ordinal < 0:
            raise RuntimeError(f"canonical_book_source_identity_invalid: fact_version_id={row['id']}")
        key = ":".join(str(position[name]) for name in
                       ("definition_id", "session_id", "connection_epoch", "receive_ordinal", "event_ordinal"))
        requests.append({"root_id": row["id"], "series_id": series_id, "observation_key": key,
                         "commit_seq": row["market_commit_seq"], "known_at": row["known_at"].isoformat()})
    selections = defaultdict(list)
    count = 0
    for offset in range(0, len(requests), 128):
        if check_budget is not None:
            check_budget()
        found = session.execute(text("""
            SELECT requested.root_id, source.id
            FROM jsonb_to_recordset(CAST(:requests AS jsonb)) AS requested(
                root_id text,series_id bigint,observation_key text,commit_seq bigint,known_at timestamptz)
            JOIN market.fact_versions AS source ON source.series_id=requested.series_id
              AND source.fact_type='market.l2_book' AND source.observation_key=requested.observation_key
              AND source.market_commit_seq<=requested.commit_seq AND source.known_at<=requested.known_at
            ORDER BY requested.root_id,source.market_commit_seq,source.id LIMIT :limit
        """), {"requests": json.dumps(requests[offset:offset + 128]), "limit": max_rows - count + 1}).all()
        count += len(found)
        if count > max_rows:
            raise RuntimeError("canonical_book_source_budget_exceeded: reduce archive page size")
        for root, identity in found:
            selections[root].append(identity)
    sources = read_canonical_dependency_rows(session, sorted({identity for ids in selections.values() for identity in ids}),
        reader=reader, max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    for source in sources.values():
        record_from_storage_row(source)
    for row in rows:
        if row["fact_type"] not in BOOK_FACT_TYPES - {"market.l2_book"}:
            continue
        _, position = _witness(row)
        matched = []
        for identity in selections[row["id"]]:
            source = sources[identity]
            _, witness = _witness(source)
            if any(witness.get(name) != position.get(name) for name in
                   (*BOOK_SCOPE_FIELDS, "receive_ordinal", "event_ordinal", "provider_sequence_num")):
                raise RuntimeError(f"canonical_book_source_scope_mismatch: fact_version_id={row['id']} source_id={identity}")
            if (source["payload"]["after_state_hash"] == row["payload"]["source_state_hash"]
                    and source["payload"]["validity_interval_id"] == row["payload"]["validity_interval_id"]):
                if (row["fact_type"] == "market.bbo" and source["payload"]["product_definition_version_id"]
                        != row["payload"]["product_definition_version_id"]):
                    raise RuntimeError(f"canonical_book_source_product_mismatch: fact_version_id={row['id']}")
                matched.append(identity)
        if not matched:
            raise RuntimeError(f"canonical_book_source_missing: fact_version_id={row['id']}")
    # Conservatively retain other causal revisions at the same declared
    # position too. They are evidence, not alternative current-state inputs.
    return [sources[identity] for identity in sorted(sources)]


def verify_book_metadata_and_checkpoints(session, *, rows, object_store, byte_verifier,
                                         max_objects, max_rows=50_000, max_logical_bytes=64 * 1024**2,
                                         max_file_bytes=128 * 1024**2,
                                         bound_checkpoint_ids=None, check_budget=None):
    """Validate immutable L2 product/validity scope and existing checkpoints.

    Checkpoints are optional acceleration artifacts; a session with no saved
    checkpoint is valid. Existing admitted checkpoints must remain readable.
    Reverification uses its committed checkpoint set, not future publications.
    Full raw prefixes are verified separately before this function is called.
    """
    scopes = {}
    members = defaultdict(list)
    for row in rows:
        if row["fact_type"] != "market.l2_book":
            continue
        _, position = _witness(row)
        payload = row["payload"]
        if payload.get("reconstruction_version") != BOOK_RECONSTRUCTION_VERSION:
            raise RuntimeError(f"canonical_book_reconstruction_unsupported: fact_version_id={row['id']}")
        key = (row["series_id"], payload["validity_interval_id"], payload["product_definition_version_id"])
        members[key].append((row, position))
        prior = scopes.get(key)
        if prior and any(prior[1].get(name) != position.get(name) for name in BOOK_SCOPE_FIELDS):
            raise RuntimeError(f"canonical_book_validity_scope_conflict: fact_version_id={row['id']}")
        if prior is None or (position["receive_ordinal"], position["event_ordinal"]) > (
                prior[1]["receive_ordinal"], prior[1]["event_ordinal"]):
            scopes[key] = (row, position)
    if len(scopes) > max_objects:
        raise RuntimeError("canonical_book_metadata_budget_exceeded: reduce archive page size")
    dependencies = {}
    checkpoint_contexts = []
    source_selections = {}
    source_count = 0
    wanted = None if bound_checkpoint_ids is None else set(bound_checkpoint_ids)
    for (series_id, interval_id, product_id), (row, position) in scopes.items():
        if check_budget is not None:
            check_budget()
        opening = session.execute(text("""
            SELECT * FROM market.book_validity_interval_versions WHERE interval_id=:id AND revision=1
        """), {"id": interval_id}).mappings().one_or_none()
        if (opening is None or opening["series_id"] != series_id or opening["status"] != "open_valid"
                or opening["reconstruction_version"] != BOOK_RECONSTRUCTION_VERSION
                or opening["opening_session_id"] != position["session_id"]
                or opening["opening_connection_epoch"] != position["connection_epoch"]
                or (opening["opening_receive_ordinal"], opening["opening_event_ordinal"])
                   > (position["receive_ordinal"], position["event_ordinal"])):
            raise RuntimeError(f"canonical_book_validity_missing_or_mismatched: fact_version_id={row['id']} interval_id={interval_id}")
        product = session.execute(text("SELECT * FROM market.product_definition_versions WHERE id=:id"),
                                  {"id": product_id}).mappings().one_or_none()
        if product is None or product["provider_product_id"] != position["provider_product_id"]:
            raise RuntimeError(f"canonical_book_product_missing_or_mismatched: fact_version_id={row['id']} product_id={product_id}")
        contract = L2ProductContract(provider_product_id=product["provider_product_id"],
            product_definition_version_id=product_id, provider_size_unit=product["provider_size_unit"],
            price_increment=product["price_increment"], quantity_increment=product["base_increment"])
        for member, member_position in members[(series_id, interval_id, product_id)]:
            if ((opening["opening_receive_ordinal"], opening["opening_event_ordinal"])
                    > (member_position["receive_ordinal"], member_position["event_ordinal"])):
                raise RuntimeError(f"canonical_book_validity_position_mismatch: fact_version_id={member['id']}")
            for entry in member["payload"]["entries"]:
                if entry["provider_size_unit"] != product["provider_size_unit"]:
                    raise RuntimeError(f"canonical_book_product_unit_mismatch: fact_version_id={member['id']}")
        bound = "" if wanted is None else "AND checkpoints.id=ANY(:bound_ids)"
        checkpoints = session.execute(text(f"""
            SELECT checkpoints.* FROM market.book_checkpoint_manifests AS checkpoints
            WHERE checkpoints.series_id=:series AND checkpoints.validity_interval_id=:interval
              AND checkpoints.product_definition_version_id=:product
              AND checkpoints.session_id=:session AND checkpoints.connection_epoch=:epoch
              AND (checkpoints.receive_ordinal,checkpoints.event_ordinal)<=(:ordinal,:event)
              {bound} AND NOT EXISTS (
                SELECT 1 FROM market.storage_lifecycle_events AS expired
                WHERE expired.action='archive_expire' AND expired.event_type='completed'
                  AND expired.target_kind='book_checkpoint' AND expired.target_id=checkpoints.id)
            ORDER BY checkpoints.id LIMIT :limit
        """), {"series": series_id, "interval": interval_id, "product": product_id,
               "session": position["session_id"], "epoch": position["connection_epoch"],
               "ordinal": position["receive_ordinal"], "event": position["event_ordinal"],
               "bound_ids": sorted(wanted or ()), "limit": max_objects - len(checkpoint_contexts) + 1}).mappings().all()
        if len(checkpoint_contexts) + len(checkpoints) > max_objects:
            raise RuntimeError("canonical_book_checkpoint_budget_exceeded: reduce archive page size")
        for checkpoint in checkpoints:
            if check_budget is not None:
                check_budget()
            ids = checkpoint["source_manifest_ids"]
            if (not isinstance(ids, list) or not ids or len(ids) > max_objects
                    or len(ids) != len(set(ids))):
                raise RuntimeError(f"canonical_book_checkpoint_sources_invalid: checkpoint_id={checkpoint['id']}")
            # Check exact delivery mappings, not min/max manifest ranges. An
            # expired original may have a verified compacted replacement in the
            # already-admitted raw prefix; original metadata itself is immutable.
            count = session.execute(text("""
                SELECT count(DISTINCT manifests.id)
                FROM market.raw_archive_manifests AS manifests
                JOIN market.raw_archive_record_mappings AS mappings ON mappings.manifest_id=manifests.id
                WHERE manifests.id=ANY(:ids) AND manifests.definition_id=:definition
                  AND manifests.session_id=:session AND manifests.connection_epoch=:epoch
                  AND mappings.session_id=:session AND mappings.connection_epoch=:epoch
                  AND mappings.receive_ordinal=:ordinal
                  AND EXISTS (SELECT 1 FROM market.raw_archive_ranges AS ranges
                              WHERE ranges.manifest_id=manifests.id AND ranges.provider_product_id=:product)
            """), {"ids": ids, "definition": position["definition_id"], "session": checkpoint["session_id"],
                   "epoch": checkpoint["connection_epoch"], "ordinal": checkpoint["receive_ordinal"],
                   "product": position["provider_product_id"]}).scalar_one()
            if count != len(ids):
                raise RuntimeError(f"canonical_book_checkpoint_source_mapping_missing: checkpoint_id={checkpoint['id']}")
            source_key = ":".join(str(value) for value in (position["definition_id"], checkpoint["session_id"],
                checkpoint["connection_epoch"], checkpoint["receive_ordinal"], checkpoint["event_ordinal"]))
            source_ids = session.execute(text("""
                SELECT id FROM market.fact_versions
                WHERE series_id=:series AND fact_type='market.l2_book' AND observation_key=:key
                  AND market_commit_seq<=:seq AND known_at<=:known_at
                ORDER BY market_commit_seq,id LIMIT :limit
            """), {"series": series_id, "key": source_key, "seq": row["market_commit_seq"],
                   "known_at": checkpoint["known_at"], "limit": max_rows - source_count + 1}).scalars().all()
            source_count += len(source_ids)
            if source_count > max_rows:
                raise RuntimeError("canonical_book_checkpoint_source_budget_exceeded: reduce archive page size")
            source_selections[checkpoint["id"]] = source_ids
            checkpoint_contexts.append((checkpoint, opening, position, contract))
    sources = {row["id"]: row for row in rows}
    requested = {identity for ids in source_selections.values() for identity in ids}
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: object_store,
        limits=FactArchiveLimits(max_rows=max(10_000, max_rows), max_logical_bytes=max_logical_bytes,
                                max_file_bytes=max_file_bytes))
    additional = read_canonical_dependency_rows(session, sorted(requested - sources.keys()), reader=reader,
        max_logical_bytes=max_logical_bytes, check_budget=check_budget)
    for source in additional.values():
        record_from_storage_row(source)
    sources.update(additional)
    for checkpoint, opening, position, contract in checkpoint_contexts:
        matched = False
        for identity in source_selections[checkpoint["id"]]:
            source = sources[identity]
            _, witness = _witness(source)
            if (any(witness.get(name) != position.get(name) for name in BOOK_SCOPE_FIELDS)
                    or any(witness.get(name) != checkpoint[name] for name in
                           ("receive_ordinal", "event_ordinal", "provider_sequence_num"))):
                raise RuntimeError(f"canonical_book_checkpoint_source_scope_mismatch: checkpoint_id={checkpoint['id']}")
            if (source["payload"]["after_state_hash"] == checkpoint["state_hash"]
                    and source["payload"]["validity_interval_id"] == checkpoint["validity_interval_id"]
                    and source["payload"]["product_definition_version_id"] == checkpoint["product_definition_version_id"]):
                matched = True
        if not matched:
            raise RuntimeError(f"canonical_book_checkpoint_source_state_missing: checkpoint_id={checkpoint['id']}")
        if check_budget is not None:
            check_budget()
        byte_verifier.verify(checkpoint["object_key"], checkpoint["object_sha256"], expected_bytes=checkpoint["byte_count"])
        restore_book_checkpoint_parquet(object_store.local_path(checkpoint["object_key"]), expected=checkpoint,
            opening=opening, definition_id=position["definition_id"], contract=contract, check_budget=check_budget)
        dependencies[checkpoint["id"]] = {"target_kind": "book_checkpoint", "target_id": checkpoint["id"],
            "object_key": checkpoint["object_key"], "object_sha256": checkpoint["object_sha256"]}
    if wanted is not None and set(dependencies) != wanted:
        raise RuntimeError("canonical_book_checkpoint_bound_coverage_missing")
    return ([dependencies[identity] for identity in sorted(dependencies)],
            [sources[identity] for identity in sorted(requested)])

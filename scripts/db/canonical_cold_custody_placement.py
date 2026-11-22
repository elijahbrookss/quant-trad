"""Inspect or move the two fixed archival custody catalogs before activation."""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
from time import monotonic

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES, COLD_HISTORY_LAYOUT_VERSION
from scripts.db.archive_reference_v2_placement import _definition
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.fact_header_v2_capture import _bounded_step


def _valid_oid(value):
    return type(value) is int and 0 < value <= 2**32 - 1


def _target(conn, expected_database, target_id):
    identity = conn.scalar(text("""
        SELECT control.system_identifier::text || '/' || db.oid::text
        FROM pg_control_system() AS control CROSS JOIN pg_database AS db
        WHERE db.datname=current_database()
    """))
    if identity != expected_database:
        raise RuntimeError("canonical_custody_wrong_database")
    target = conn.execute(text("""
        SELECT target.id AS target_id,target.root,target.filesystem_uuid,registration.binding,
               space.oid::bigint AS tablespace_oid,space.spcname AS tablespace_name,
               pg_tablespace_location(space.oid) AS location
        FROM public.portal_storage_header_tablespaces AS registration
        JOIN public.portal_storage_targets AS target ON target.id=registration.target_id
        JOIN pg_tablespace AS space ON space.oid=registration.tablespace_oid
        WHERE registration.database_identity=:database AND target.id=:target_id
          AND target.medium='hdd' AND target.state='active' AND target.roles ? 'history'
    """), {"database": identity, "target_id": target_id}).mappings().one_or_none()
    if target is None:
        raise RuntimeError("canonical_custody_registered_history_target_missing")
    result = dict(target)
    binding = result.pop("binding")
    expected = {"database_identity": identity, "target_id": target_id,
                "tablespace_oid": result["tablespace_oid"], "tablespace_name": result["tablespace_name"],
                "tablespace_location": result["location"], "target_root": result["root"],
                "filesystem_uuid": result["filesystem_uuid"]}
    if any(binding.get(key) != value for key, value in expected.items() if key in binding):
        raise RuntimeError("canonical_custody_registered_history_binding_changed")
    return result


def inspect_custody_members(conn):
    """Read exact physical members, including on an unregistered old-layout donor."""
    rows = conn.execute(text("""
        WITH owners AS (
            SELECT c.oid,c.relname,c.reltoastrelid
            FROM pg_class AS c JOIN pg_namespace AS ns ON ns.oid=c.relnamespace
            WHERE ns.nspname='market' AND c.relname=ANY(CAST(:names AS text[]))
              AND c.relkind='r' AND c.relpersistence='p'
              AND NOT EXISTS (SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
        ), heaps AS (
            SELECT oid,relname AS relation,'heap' AS member_kind FROM owners
            UNION ALL SELECT reltoastrelid,relname,'toast' FROM owners WHERE reltoastrelid<>0
        ), members AS (
            SELECT oid,relation,member_kind FROM heaps
            UNION ALL
            SELECT i.indexrelid,h.relation,
                   CASE WHEN h.member_kind='heap' THEN 'index' ELSE 'toast_index' END
            FROM heaps AS h JOIN pg_index AS i ON i.indrelid=h.oid
        )
        SELECT m.relation,c.oid::bigint AS oid,m.member_kind,ns.nspname AS schema,c.relname AS name,
               c.relkind,c.relpersistence,c.relfilenode::bigint AS filenode,
               COALESCE(NULLIF(c.reltablespace,0),db.dattablespace)::bigint AS tablespace_oid,
               (SELECT sum(pg_relation_size(c.oid,fork))::bigint
                FROM unnest(ARRAY['main','fsm','vm','init']) AS fork) AS bytes,
               (idx.indexrelid IS NULL OR (idx.indisready AND idx.indisvalid AND idx.indislive)) AS valid
        FROM members AS m JOIN pg_class AS c ON c.oid=m.oid
        JOIN pg_namespace AS ns ON ns.oid=c.relnamespace
        LEFT JOIN pg_index AS idx ON idx.indexrelid=c.oid
        CROSS JOIN pg_database AS db WHERE db.datname=current_database()
        ORDER BY m.relation,m.member_kind,c.oid LIMIT 65
    """), {"names": list(COLD_CUSTODY_TABLE_NAMES)}).mappings().all()
    kinds = {"heap": "r", "toast": "t", "index": "i", "toast_index": "i"}
    if len(rows) > 64 or any(
        row["relkind"] != kinds[row["member_kind"]] or row["relpersistence"] != "p"
        or not row["valid"] or not row["filenode"] or type(row["bytes"]) is not int for row in rows
    ) or {row["relation"] for row in rows if row["member_kind"] == "heap"} != set(COLD_CUSTODY_TABLE_NAMES):
        raise RuntimeError("canonical_custody_relation_inventory_invalid")
    return [dict(row) for row in rows]


def _relation_oids(members):
    return {row["relation"]: row["oid"] for row in members if row["member_kind"] == "heap"}


def _require_scope(members, target, expected_relation_oids, expected_tablespace_oid):
    if (not isinstance(expected_relation_oids, Mapping)
            or set(expected_relation_oids) != set(COLD_CUSTODY_TABLE_NAMES)
            or any(not _valid_oid(oid) for oid in expected_relation_oids.values())
            or not _valid_oid(expected_tablespace_oid)):
        raise ValueError("canonical_custody_invalid_expected_scope")
    if _relation_oids(members) != expected_relation_oids:
        raise RuntimeError("canonical_custody_relation_oid_changed")
    if target["tablespace_oid"] != expected_tablespace_oid:
        raise RuntimeError("canonical_custody_tablespace_oid_changed")


def _require_placement(members, target):
    if any(row["tablespace_oid"] != target["tablespace_oid"] for row in members):
        raise RuntimeError("canonical_custody_tablespace_placement_incomplete")


def inspect_custody_placement(conn, *, expected_database, target_id):
    """Read the exact current catalog binding and one copy's allocated bytes."""
    target = _target(conn, expected_database, target_id)
    members = inspect_custody_members(conn)
    return {"database_identity": expected_database, "target": target, "relation_oids": _relation_oids(members),
            "members": members, "placement_hash": custody_placement_hash(members),
            "copy_bytes": sum(row["bytes"] for row in members
                                                   if row["tablespace_oid"] != target["tablespace_oid"])}


def custody_placement_hash(members):
    """Bind relation OIDs and file placement while allowing fork allocations to grow."""
    return archive_evidence_hash({"members": [
        {key: value for key, value in row.items() if key != "bytes"} for row in members
    ]})


def inspect_custody_move_source(conn, *, expected_database, target_id, expected_tablespace_oid,
                                expected_relation_oids, source_placement_hash):
    inspected = inspect_custody_placement(conn, expected_database=expected_database, target_id=target_id)
    _require_scope(inspected["members"], inspected["target"], expected_relation_oids, expected_tablespace_oid)
    if inspected["placement_hash"] != source_placement_hash:
        raise RuntimeError("canonical_custody_source_placement_changed")
    return inspected


def assert_custody_placement(conn, *, expected_database, target_id, expected_tablespace_oid, expected_relation_oids):
    """Bind every existing custody member to the approved registered tablespace."""
    inspected = inspect_custody_placement(conn, expected_database=expected_database, target_id=target_id)
    _require_scope(inspected["members"], inspected["target"], expected_relation_oids, expected_tablespace_oid)
    _require_placement(inspected["members"], inspected["target"])
    return inspected


def _definitions(conn):
    # Compare actual native definitions (including FK/trigger details), and
    # bind constraint OIDs separately from their deparsed descriptions.
    logical = {name: _definition(conn, f"market.{name}") for name in COLD_CUSTODY_TABLE_NAMES}
    constraints = conn.execute(text("""
        SELECT c.oid::bigint,c.conrelid::bigint,c.conname
        FROM pg_constraint AS c
        WHERE c.conrelid=ANY(CAST(:relations AS regclass[])) ORDER BY c.oid LIMIT 129
    """), {"relations": [f"market.{name}" for name in COLD_CUSTODY_TABLE_NAMES]}).all()
    if len(constraints) > 128:
        raise RuntimeError("canonical_custody_constraint_inventory_exceeded")
    return logical, constraints


def _logical_members(members):
    return [{key: value for key, value in row.items() if key not in {"bytes", "filenode", "tablespace_oid"}}
            for row in members]


def _placed_members(members):
    return [{key: value for key, value in row.items() if key not in {"bytes", "filenode"}} for row in members]


def assert_original_custody_members(conn, original):
    """Require the donor's relation identity and effective tablespace for every member."""
    observed = inspect_custody_members(conn)
    if _placed_members(observed) != _placed_members(original):
        raise RuntimeError("canonical_custody_original_placement_changed")
    return observed


def move_custody_tablespace(conn, *, expected_database, target_id, expected_tablespace_oid,
                          expected_relation_oids, max_copy_bytes, timeout_seconds, source_placement_hash=None):
    """Move only the reviewed custody tables in a caller-owned transaction.

    PostgreSQL performs the byte-preserving relation moves. This step never
    commits, scans table rows, or qualifies device capacity and recovery.
    """
    if type(max_copy_bytes) is not int or not 0 <= max_copy_bytes <= 2**63 - 1:
        raise ValueError("canonical_custody_invalid_copy_budget")
    if not conn.in_transaction() or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT":
        raise RuntimeError("canonical_custody_move_requires_transaction")
    with _bounded_step(conn, timeout_seconds):
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))")):
            raise RuntimeError("canonical_custody_storage_busy")
        # Keep target state and registration stable through the rewrite.
        conn.exec_driver_sql("LOCK TABLE public.portal_storage_targets, public.portal_storage_header_tablespaces "
                             "IN SHARE MODE NOWAIT")
        target = _target(conn, expected_database, target_id)
        root, location = PurePosixPath(target["root"]), PurePosixPath(target["location"])
        if (target["tablespace_oid"] in (1663, 1664) or not root.is_absolute() or root == PurePosixPath("/")
                or not location.is_absolute() or ".." in location.parts or not location.is_relative_to(root)):
            raise RuntimeError("canonical_custody_move_requires_native_history_tablespace")
        for name in COLD_CUSTODY_TABLE_NAMES:
            conn.exec_driver_sql(f"LOCK TABLE ONLY market.{name} IN ACCESS EXCLUSIVE MODE NOWAIT")
        before = inspect_custody_members(conn)
        _require_scope(before, target, expected_relation_oids, expected_tablespace_oid)
        if source_placement_hash is not None and custody_placement_hash(before) != source_placement_hash:
            raise RuntimeError("canonical_custody_source_placement_changed")
        on_destination = {row["relation"] for row in before
                          if row["member_kind"] == "heap" and row["tablespace_oid"] == expected_tablespace_oid}
        if any(row["relation"] in on_destination and row["member_kind"] in {"toast", "toast_index"}
               and row["tablespace_oid"] != expected_tablespace_oid for row in before):
            raise RuntimeError("canonical_custody_mixed_toast_placement_requires_review")
        moving = [row for row in before if row["tablespace_oid"] != target["tablespace_oid"]]
        copy_bytes = sum(row["bytes"] for row in moving)
        if copy_bytes > max_copy_bytes:
            raise RuntimeError(f"canonical_custody_copy_budget_exceeded: bytes={copy_bytes} limit={max_copy_bytes}")
        properties = _definitions(conn)
        quote = conn.dialect.identifier_preparer.quote_identifier
        destination = quote(target["tablespace_name"])
        for row in moving:
            relation = quote(row["schema"]) + "." + quote(row["name"])
            if row["member_kind"] == "heap":
                # SET TABLESPACE also moves this heap's TOAST and its index.
                conn.exec_driver_sql(f"ALTER TABLE {relation} SET TABLESPACE {destination}")
            elif row["member_kind"] == "index":
                conn.exec_driver_sql(f"ALTER INDEX {relation} SET TABLESPACE {destination}")
        current_target = _target(conn, expected_database, target_id)
        if current_target != target:
            raise RuntimeError("canonical_custody_registered_history_binding_changed")
        after = inspect_custody_members(conn)
        _require_scope(after, target, expected_relation_oids, expected_tablespace_oid)
        _require_placement(after, target)
        if _logical_members(after) != _logical_members(before) or _definitions(conn) != properties:
            raise RuntimeError("canonical_custody_logical_definition_changed")
        if [row["bytes"] for row in before] != [row["bytes"] for row in after]:
            raise RuntimeError("canonical_custody_physical_bytes_changed")
        return {"database_identity": expected_database, "target": target, "relation_oids": _relation_oids(after),
                "copy_bytes": copy_bytes, "reused": not moving, "before": before, "after": after}


def restore_custody_tablespace(conn, *, fence, expected_database, original, forward_receipt,
                             expected_forward_hash, max_copy_bytes, timeout_seconds):
    """Restore the donor's proven native placement on a fenced latest cold clone."""
    if (not isinstance(fence, DatabaseFence) or fence.connection is not conn or not conn.in_transaction()
            or fence.database.get("allow_connections") is not False
            or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT"):
        raise ValueError("canonical_custody_restore_requires_live_closed_database_fence")
    if type(max_copy_bytes) is not int or not 0 <= max_copy_bytes <= 2**63 - 1:
        raise ValueError("canonical_custody_invalid_copy_budget")
    if (not isinstance(forward_receipt, dict) or set(forward_receipt) != {
            "database_identity", "target", "relation_oids", "copy_bytes", "reused", "before", "after", "receipt_hash",
            } or forward_receipt["receipt_hash"] != expected_forward_hash
            or archive_evidence_hash({key: value for key, value in forward_receipt.items() if key != "receipt_hash"})
            != expected_forward_hash or forward_receipt["database_identity"] != expected_database
            or not isinstance(forward_receipt["target"], dict)
            or any(not isinstance(forward_receipt[key], list) or not 1 <= len(forward_receipt[key]) <= 64
                   or any(not isinstance(member, dict) for member in forward_receipt[key]) for key in ("before", "after"))):
        raise ValueError("canonical_custody_restore_forward_receipt_invalid")
    prior = forward_receipt["before"]
    if (_placed_members(prior) != _placed_members(original)
            or _logical_members(prior) != _logical_members(forward_receipt["after"])
            or forward_receipt["relation_oids"] != _relation_oids(original)):
        raise RuntimeError("canonical_custody_restore_donor_placement_changed")

    with _bounded_step(conn, timeout_seconds) as limit:
        limit(fence.deadline - monotonic())
        fence.assert_quiet(conn)
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))")):
            raise RuntimeError("canonical_custody_storage_busy")
        conn.exec_driver_sql("LOCK TABLE market.fact_storage_state, public.portal_storage_targets, "
                             "public.portal_storage_header_tablespaces IN SHARE MODE NOWAIT")
        if conn.exec_driver_sql("SELECT layout_version,state FROM market.fact_storage_state LIMIT 2").all() != [
                (COLD_HISTORY_LAYOUT_VERSION, "ready")]:
            raise RuntimeError("canonical_custody_restore_authority_changed")
        for name in COLD_CUSTODY_TABLE_NAMES:
            conn.exec_driver_sql(f"LOCK TABLE ONLY market.{name} IN ACCESS EXCLUSIVE MODE NOWAIT")
        target_id = forward_receipt["target"].get("target_id")
        target = _target(conn, expected_database, target_id)
        if target != forward_receipt["target"]:
            raise RuntimeError("canonical_custody_restore_registered_target_changed")
        before = inspect_custody_members(conn)
        if _placed_members(before) != _placed_members(forward_receipt["after"]):
            raise RuntimeError("canonical_custody_restore_forward_placement_changed")
        _require_placement(before, target)

        history = target["tablespace_oid"]
        destinations = {1663: "pg_default", history: target["tablespace_name"]}
        prior_spaces = {row["oid"]: row["tablespace_oid"] for row in original}
        heap_spaces = {row["relation"]: row["tablespace_oid"] for row in original if row["member_kind"] == "heap"}
        if any(space not in destinations for space in prior_spaces.values()):
            raise RuntimeError("canonical_custody_restore_original_tablespace_requires_review")
        if any(row["member_kind"] in {"toast", "toast_index"}
               and row["tablespace_oid"] != heap_spaces[row["relation"]] for row in original):
            raise RuntimeError("canonical_custody_restore_mixed_toast_placement_requires_review")
        moving = [row for row in before if prior_spaces[row["oid"]] != history]
        copy_bytes = sum(row["bytes"] for row in moving)
        if copy_bytes > max_copy_bytes:
            raise RuntimeError(f"canonical_custody_restore_copy_budget_exceeded: bytes={copy_bytes} limit={max_copy_bytes}")
        properties = _definitions(conn)
        quote = conn.dialect.identifier_preparer.quote_identifier
        for row in moving:
            kind = row["member_kind"]
            if kind in {"heap", "index"}:
                relation = quote(row["schema"]) + "." + quote(row["name"])
                destination = quote(destinations[prior_spaces[row["oid"]]])
                conn.exec_driver_sql(f"ALTER {'TABLE' if kind == 'heap' else 'INDEX'} {relation} SET TABLESPACE {destination}")
        after = assert_original_custody_members(conn, original)
        if (_definitions(conn) != properties or _target(conn, expected_database, target_id) != target
                or [row["bytes"] for row in before] != [row["bytes"] for row in after]):
            raise RuntimeError("canonical_custody_restore_result_changed")
        fence.assert_quiet(conn)
    result = {"schema": "qt.canonical_cold_custody_restore.v1", "database_identity": expected_database,
              "target": target, "before": before, "after": after, "copy_bytes": copy_bytes,
              "forward_receipt_hash": expected_forward_hash}
    return result | {"receipt_hash": archive_evidence_hash(result)}

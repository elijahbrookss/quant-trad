"""Move the one deployed raw mapping primary key within a fenced transition."""

from __future__ import annotations

from pathlib import PurePosixPath
from time import monotonic

from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db.canonical_cold_custody_placement import _target, _valid_oid
from scripts.db.canonical_cold_database_fence import DatabaseFence
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_raw_index_placement.v1"
TABLE = "market.raw_archive_record_mappings"
INDEX = "market.pk_market_raw_archive_record_mapping"
RECENT_TABLESPACE_OID = 1663
_MEMBER = """
    SELECT heap.oid::bigint AS table_oid, idx.oid::bigint AS index_oid,
           con.oid::bigint AS constraint_oid, idx.relfilenode::bigint AS relfilenode,
           db.oid::bigint AS database_oid, control.catalog_version_no AS catalog_version,
           COALESCE(NULLIF(idx.reltablespace,0),db.dattablespace)::bigint AS tablespace_oid,
           COALESCE(NULLIF(heap.reltablespace,0),db.dattablespace)::bigint AS table_tablespace_oid,
           pg_relation_filepath(idx.oid) AS relation_path,
           (SELECT jsonb_object_agg(fork, pg_relation_size(idx.oid,fork))
            FROM unnest(ARRAY['main','fsm','vm','init']) AS fork) AS forks,
           pg_get_indexdef(idx.oid) AS definition, pg_get_constraintdef(con.oid) AS constraint_definition
    FROM pg_catalog.pg_class heap
    JOIN pg_catalog.pg_index i ON i.indrelid=heap.oid
    JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
    JOIN pg_catalog.pg_am am ON am.oid=idx.relam
    JOIN pg_catalog.pg_attribute raw_id ON raw_id.attrelid=heap.oid AND raw_id.attnum=i.indkey[0]
    JOIN pg_catalog.pg_attribute manifest ON manifest.attrelid=heap.oid AND manifest.attnum=i.indkey[1]
    JOIN pg_catalog.pg_opclass raw_ops ON raw_ops.oid=i.indclass[0]
    JOIN pg_catalog.pg_opclass manifest_ops ON manifest_ops.oid=i.indclass[1]
    JOIN pg_catalog.pg_constraint con ON con.conindid=idx.oid AND con.conrelid=heap.oid AND con.contype='p'
    CROSS JOIN pg_catalog.pg_control_system() control
    CROSS JOIN pg_catalog.pg_database db
    WHERE db.datname=current_database()
      AND current_setting('server_version_num')::integer / 10000=15
      AND heap.oid=to_regclass(:heap) AND idx.oid=to_regclass(:index)
      AND heap.relkind='r' AND idx.relkind='i' AND heap.relpersistence='p' AND idx.relpersistence='p'
      AND idx.relfilenode<>0 AND idx.reloptions IS NULL
      AND NOT EXISTS (SELECT FROM pg_catalog.pg_inherits
                      WHERE inhrelid IN (heap.oid,idx.oid) OR inhparent IN (heap.oid,idx.oid))
      AND am.amname='btree' AND i.indisprimary AND i.indisunique AND i.indimmediate
      AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisexclusion
      AND i.indnkeyatts=2 AND i.indnatts=2 AND i.indpred IS NULL AND i.indexprs IS NULL
      AND raw_id.attname='raw_record_id' AND manifest.attname='manifest_id'
      AND format_type(raw_id.atttypid,raw_id.atttypmod)='character varying(80)'
      AND format_type(manifest.atttypid,manifest.atttypmod)='character varying(128)'
      AND raw_id.attnotnull AND manifest.attnotnull AND NOT raw_id.attisdropped AND NOT manifest.attisdropped
      AND i.indoption[0]=0 AND i.indoption[1]=0
      AND raw_ops.opcdefault AND manifest_ops.opcdefault
      AND i.indcollation[0]=raw_id.attcollation AND i.indcollation[1]=manifest.attcollation
      AND con.conname=idx.relname AND con.convalidated AND NOT con.condeferrable
      AND con.conkey=ARRAY[raw_id.attnum,manifest.attnum]
"""
_PHYSICAL_FIELDS = {"tablespace_oid", "relation_path", "relfilenode", "forks"}


def _logical(member):
    return {key: value for key, value in member.items() if key not in _PHYSICAL_FIELDS}


def _fork_bytes(member):
    forks = member.get("forks")
    if (not isinstance(forks, dict) or forks.keys() != {"main", "fsm", "vm", "init"}
            or any(type(size) is not int or size < 0 for size in forks.values())):
        raise ValueError("canonical_raw_index_native_forks_invalid")
    return sum(forks.values())


def inspect_raw_mapping_index_member(conn):
    """Read the original native definition in a donor without a target registration."""
    row = conn.execute(text(_MEMBER), {"heap": TABLE, "index": INDEX}).mappings().one_or_none()
    if row is None:
        raise RuntimeError("canonical_raw_index_native_definition_changed")
    member = dict(row)
    _fork_bytes(member)
    return member


def _member(conn, history_oid):
    member = inspect_raw_mapping_index_member(conn)
    filenode = member["relfilenode"]
    db_oid = member["database_oid"]
    if member["tablespace_oid"] == RECENT_TABLESPACE_OID:
        expected_path = f"base/{db_oid}/{filenode}"
    elif member["tablespace_oid"] == history_oid:
        expected_path = f"pg_tblspc/{history_oid}/PG_15_{member['catalog_version']}/{db_oid}/{filenode}"
    else:
        raise RuntimeError("canonical_raw_index_unrecognized_placement")
    if member["relation_path"] != expected_path or member["table_tablespace_oid"] != history_oid:
        raise RuntimeError("canonical_raw_index_physical_path_changed")
    return member


def inspect_raw_mapping_index(conn, *, expected_database, target_id):
    """Read the exact native index, together with the registered HDD binding."""
    target = _target(conn, expected_database, target_id)
    root, location = PurePosixPath(target["root"]), PurePosixPath(target["location"])
    if (target["tablespace_oid"] in (RECENT_TABLESPACE_OID, 1664) or not root.is_absolute() or root == PurePosixPath("/")
            or ".." in root.parts or not location.is_absolute() or ".." in location.parts
            or not location.is_relative_to(root)):
        raise RuntimeError("canonical_raw_index_requires_native_history_tablespace")
    return {"database_identity": expected_database, "target": target,
            "member": _member(conn, target["tablespace_oid"])}


def assert_raw_mapping_index_placement(conn, *, expected_database, target_id, expected_tablespace_oid,
                                       expected_table_oid, expected_index_oid, on_history):
    """Check placement independently for a committed or rolled-back transition."""
    if (type(on_history) is not bool
            or not all(map(_valid_oid, (expected_tablespace_oid, expected_table_oid, expected_index_oid)))):
        raise ValueError("canonical_raw_index_invalid_scope")
    observed = inspect_raw_mapping_index(conn, expected_database=expected_database, target_id=target_id)
    member = observed["member"]
    if ((observed["target"]["tablespace_oid"], member["table_oid"], member["index_oid"])
            != (expected_tablespace_oid, expected_table_oid, expected_index_oid)):
        raise RuntimeError("canonical_raw_index_expected_oid_changed")
    if member["tablespace_oid"] != (expected_tablespace_oid if on_history else RECENT_TABLESPACE_OID):
        raise RuntimeError("canonical_raw_index_placement_changed")
    return observed


def _move(conn, *, fence, scope, max_copy_bytes, timeout_seconds, to_history, forward=None):
    if (not isinstance(fence, DatabaseFence) or fence.connection is not conn or not conn.in_transaction()
            or fence.database.get("allow_connections") is not False
            or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT"):
        raise ValueError("canonical_raw_index_requires_live_closed_database_fence")
    if type(max_copy_bytes) is not int or not 0 <= max_copy_bytes <= 2**63 - 1:
        raise ValueError("canonical_raw_index_invalid_copy_budget")
    with _bounded_step(conn, timeout_seconds) as limit:
        limit(fence.deadline - monotonic())
        fence.assert_quiet(conn)
        for lock in ("qt.storage.management.v1", _LIFECYCLE_LOCK_NAME):
            if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"), {"name": lock}):
                raise RuntimeError("canonical_raw_index_transition_lock_busy")
        conn.exec_driver_sql("LOCK TABLE market.fact_storage_state, public.portal_storage_targets, "
                             "public.portal_storage_header_tablespaces IN SHARE MODE NOWAIT")
        layout = FACT_STORAGE_LAYOUT_VERSION if to_history else COLD_HISTORY_LAYOUT_VERSION
        if conn.execute(text("""
            SELECT layout_version,state,completed_at IS NOT NULL FROM market.fact_storage_state LIMIT 2
        """)).all() != [(layout, "ready", True)]:
            raise RuntimeError("canonical_raw_index_authority_changed")
        conn.exec_driver_sql(f"LOCK TABLE ONLY {TABLE} IN ACCESS EXCLUSIVE MODE NOWAIT")
        before = assert_raw_mapping_index_placement(conn, **scope, on_history=not to_history)
        member = before["member"]
        target = before["target"]
        if forward is not None and (forward["database_identity"] != before["database_identity"]
                or forward["target"] != target or _logical(forward["after"]) != _logical(member)):
            raise RuntimeError("canonical_raw_index_forward_binding_changed")
        copy_bytes = _fork_bytes(member)
        if copy_bytes > max_copy_bytes:
            raise RuntimeError(f"canonical_raw_index_copy_budget_exceeded: bytes={copy_bytes} limit={max_copy_bytes}")
        destination = target["tablespace_name"] if to_history else "pg_default"
        quote = conn.dialect.identifier_preparer.quote_identifier
        conn.exec_driver_sql(f"ALTER INDEX {INDEX} SET TABLESPACE {quote(destination)}")
        after = assert_raw_mapping_index_placement(conn, **scope, on_history=to_history)
        if (after["target"] != target or _logical(after["member"]) != _logical(member)
                or after["member"]["forks"] != member["forks"]):
            raise RuntimeError("canonical_raw_index_move_result_changed")
        fence.assert_quiet(conn)
    result = {"version": VERSION, "database_identity": before["database_identity"], "target": target,
              "before": member, "after": after["member"], "copy_bytes": copy_bytes}
    if forward is not None:
        result["forward_receipt_hash"] = forward["receipt_hash"]
    return result | {"receipt_hash": archive_evidence_hash(result)}


def move_raw_mapping_index(conn, *, fence, expected_database, target_id, expected_tablespace_oid,
                           expected_table_oid, expected_index_oid, max_copy_bytes, timeout_seconds):
    """Move the original SSD B-tree in the caller's fenced authority transaction.

    The caller qualifies the physical devices, copy and WAL capacity, and
    the deployed recovery boundary before dispatch. This step never commits.
    """
    return _move(conn, fence=fence, max_copy_bytes=max_copy_bytes, timeout_seconds=timeout_seconds,
                 to_history=True, scope={"expected_database": expected_database, "target_id": target_id,
                 "expected_tablespace_oid": expected_tablespace_oid, "expected_table_oid": expected_table_oid,
                 "expected_index_oid": expected_index_oid})


def restore_raw_mapping_index(conn, *, fence, expected_database, target_id, expected_tablespace_oid,
                              expected_table_oid, expected_index_oid, max_copy_bytes, timeout_seconds,
                              forward_receipt, expected_forward_hash):
    """Restore the SSD B-tree after a verified forward move, with cold authority closed."""
    if (not isinstance(forward_receipt, dict) or forward_receipt.keys() != {
            "version", "database_identity", "target", "before", "after", "copy_bytes", "receipt_hash"}
            or forward_receipt["version"] != VERSION or forward_receipt["receipt_hash"] != expected_forward_hash
            or archive_evidence_hash({key: value for key, value in forward_receipt.items() if key != "receipt_hash"})
            != expected_forward_hash
            or not isinstance(forward_receipt["before"], dict) or not isinstance(forward_receipt["after"], dict)
            or forward_receipt["before"].get("tablespace_oid") != RECENT_TABLESPACE_OID
            or forward_receipt["after"].get("tablespace_oid") != expected_tablespace_oid
            or _logical(forward_receipt["before"]) != _logical(forward_receipt["after"])
            or forward_receipt["before"].get("forks") != forward_receipt["after"].get("forks")
            or _fork_bytes(forward_receipt["before"]) != forward_receipt["copy_bytes"]):
        raise ValueError("canonical_raw_index_forward_receipt_changed")
    return _move(conn, fence=fence, max_copy_bytes=max_copy_bytes, timeout_seconds=timeout_seconds,
                 to_history=False, forward=forward_receipt,
                 scope={"expected_database": expected_database, "target_id": target_id,
                        "expected_tablespace_oid": expected_tablespace_oid, "expected_table_oid": expected_table_oid,
                        "expected_index_oid": expected_index_oid})

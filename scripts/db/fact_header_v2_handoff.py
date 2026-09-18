"""Fixed preserving database handoff and lost-commit reconciliation.

Internal operator boundary, not deployment orchestration. The caller must stop
and drain publishers before calling and keep them stopped until the matching
runtime and archive root are activated. No service or configuration is changed.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from time import monotonic

from sqlalchemy import text

from portal.backend.db.fact_storage_schema import assert_fact_storage_contract
from portal.backend.service.storage.header_movement import _MoveWatch
from portal.backend.service.storage.header_resource_claims import _limits
from portal.backend.service.storage.header_resources import observe_header_resources
from scripts.db import archive_reference_v2_placement as reference_move
from scripts.db import archive_root_v2_copy as archives
from scripts.db import fact_header_v2_copy as headers, fact_header_v2_placement as physical
from scripts.db import fact_header_v2_references as references, raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import LOCK, SCHEMA, migration_step

logger = logging.getLogger(__name__)
RETAINED = "qt_fact_header_retained_v1"
RECEIPT_VERSION = "qt.fact_header_preserving_handoff.v1"


def _oid(conn, relation):
    return conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": relation})


def _switch_verified_tables(conn, verified, *, prevalidated, raw_mapping, evidence):
    """Shared DDL only; callers own admission, fencing, verification and commit."""

    from scripts.db import fact_header_v2_copy as copy
    from scripts.db.fact_header_v2_capture import SCHEMA
    from scripts.db import fact_header_v2_references as references
    from portal.backend.db.fact_storage_schema import assert_fact_storage_contract, install_fact_storage_functions

    if raw_mapping:
        from scripts.db import raw_mapping_v2_copy as raw
    retained = "qt_fact_header_retained_v1"
    count = target_count = verified["verified_header_rows"]
    # Verification allowed ordinary reads. Only the transactional rename
    # phase takes an exclusive fence; a busy reader refuses this handoff.
    switching = (copy.SOURCE, "market.fact_hot_payloads",
                 "market.fact_archive_material_aliases", "market.fact_archive_canonical_dependencies",
                 *(SCHEMA+"."+name for name in copy.TABLE_NAMES))
    if raw_mapping:
        switching += (raw.SOURCE, raw.TARGET)
    conn.exec_driver_sql("LOCK TABLE "+",".join(switching)+" IN ACCESS EXCLUSIVE MODE NOWAIT")
    incoming = conn.execute(text("""
        SELECT conrelid::regclass::text AS relation,conname,pg_get_constraintdef(oid) AS definition
        FROM pg_constraint WHERE contype='f' AND confrelid='market.fact_versions'::regclass
          AND conparentid=0 ORDER BY conrelid,conname
    """)).mappings().all()
    owners = {"market.fact_hot_payloads","market.fact_archive_material_aliases",
              "market.fact_archive_canonical_dependencies"}
    if len(incoming)!=3 or {row["relation"] for row in incoming}!=owners:
        raise RuntimeError("fact_header_handoff_unexpected_references")
    conn.exec_driver_sql("LOCK TABLE "+",".join(sorted(owners))+" IN ACCESS EXCLUSIVE MODE NOWAIT")
    children = conn.execute(text("""
        SELECT n.nspname,c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE i.inhparent=to_regclass(:parent) ORDER BY c.relname
    """),{"parent":SCHEMA+".fact_versions"}).all()
    if any(schema!=SCHEMA for schema,name in children):
        raise RuntimeError("fact_header_handoff_unexpected_partition")
    conn.exec_driver_sql(f"CREATE SCHEMA {retained}")
    conn.exec_driver_sql(f"REVOKE ALL ON SCHEMA {retained} FROM PUBLIC")
    conn.exec_driver_sql(f"""
        CREATE TABLE {retained}.fact_storage_state AS
        SELECT * FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'
    """)
    conn.exec_driver_sql("DROP VIEW market.fact_rows")
    conn.exec_driver_sql("DROP TRIGGER trg_assert_fact_hot_payload_valid ON market.fact_hot_payloads")
    for row in incoming:
        name=conn.dialect.identifier_preparer.quote(row["conname"])
        conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} DROP CONSTRAINT {name}')
    conn.exec_driver_sql(f"ALTER TABLE market.fact_versions SET SCHEMA {retained}")
    conn.exec_driver_sql(f"""
        CREATE TRIGGER trg_retained_source_closed BEFORE INSERT ON {retained}.fact_versions
        FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
    """)
    conn.exec_driver_sql(f"ALTER TABLE {retained}.fact_versions ENABLE ALWAYS TRIGGER trg_retained_source_closed")
    for name in (*copy.TABLE_NAMES, *(name for schema,name in children)):
        quoted=conn.dialect.identifier_preparer.quote(name)
        conn.exec_driver_sql(f"ALTER TABLE {SCHEMA}.{quoted} SET SCHEMA market")
    for row in incoming:
        name=conn.dialect.identifier_preparer.quote(row["conname"])
        if prevalidated:
            conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} RENAME CONSTRAINT {references.STAGED} TO {name}')
        else:
            definition=row["definition"].replace("market.fact_versions","market.fact_identities")
            conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} ADD CONSTRAINT {name} {definition}')
    if raw_mapping:
        conn.exec_driver_sql(f"ALTER TABLE {raw.SOURCE} SET SCHEMA {retained}")
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_retained_raw_source_closed
            BEFORE INSERT ON {retained}.{raw.NAME}
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
        conn.exec_driver_sql(f"ALTER TABLE {retained}.{raw.NAME} ENABLE ALWAYS TRIGGER trg_retained_raw_source_closed")
        conn.exec_driver_sql(f"ALTER TABLE {raw.TARGET} SET SCHEMA market")
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_reject_mutation_raw_archive_record_mappings
            BEFORE UPDATE OR DELETE ON {raw.SOURCE}
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
    # The source admission already verifies this unchanged canonical function.
    conn.exec_driver_sql("""
        CREATE TRIGGER trg_assert_fact_version_valid BEFORE INSERT ON market.fact_versions
        FOR EACH ROW EXECUTE FUNCTION market.assert_fact_version_valid()
    """)
    install_fact_storage_functions(conn)
    for name in ("fact_versions","fact_identities","fact_header_partitions"):
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_reject_mutation_{name} BEFORE UPDATE OR DELETE ON market.{name}
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
    conn.exec_driver_sql("DELETE FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'")
    conn.execute(text("""
        INSERT INTO market.fact_storage_state(layout_version,state,completed_at,evidence)
        VALUES('market.fact_storage_tiers.v2','ready',clock_timestamp(),CAST(:evidence AS jsonb))
    """), {"evidence": json.dumps(evidence)})

    assert_fact_storage_contract(conn)
    return {"source_rows_retained":count,"active_header_rows":target_count,
            "database_handoff_staged":True}


def commit_handoff(engine, *, policy, resource_limits, source_root, destination_root,
                   max_objects, max_bytes, page_rows=128, cancelled=None):
    """Commit one fully verified fixed handoff; sources remain retained.

    Rechecks every copied header, identity, lookup and archive object under
    fences, requires already validated references on HDD, and supervises the
    original attempt deadline and resource budget through commit. On any
    uncertain response, use inspect_handoff; never blindly replay the switch.
    This does not prove publisher drain or authorize collection to resume.
    """
    limits = _limits(resource_limits)
    if cancelled is not None and not callable(cancelled):
        raise ValueError("fact_header_handoff_cancellation_callback_invalid")
    started = monotonic()
    deadline = started + limits["movement_timeout_seconds"]
    watch = None
    with engine.connect() as conn:
        try:
            with conn.begin():
                previous = conn.scalar(text(
                    "SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous:
                    deadline = min(deadline, started+previous/1000)
                with migration_step(conn, limits["movement_timeout_seconds"]):
                    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                                            "hashtextextended('qt.storage.management.v1',0))")):
                        raise RuntimeError("fact_header_handoff_storage_busy")
                    state = headers._inspect_progress(conn)
                    saved = state["placement"]
                    if saved is None:
                        raise RuntimeError("fact_header_handoff_fixed_placement_required")
                    plan = physical._restore(saved["plan"])
                    targets = (plan.recent, plan.history)
                    reference_move._fixed_inputs(policy, limits, targets)
                    seconds = conn.scalar(text(f"""
                        SELECT EXTRACT(EPOCH FROM prepared_at+interval '24 hours'-clock_timestamp())
                        FROM {SCHEMA}.capture WHERE id=1
                    """))
                    deadline = min(deadline, monotonic()+float(seconds))
                    resources = observe_header_resources(conn, targets,
                        pg_controldata=plan.pg_controldata,
                        timeout_seconds=min(30, limits["movement_timeout_seconds"]))
                    budget, floors = reference_move._budget(conn,
                        observed={"bytes": 0, "_binding": saved}, policy=policy,
                        limits=limits, targets=targets, resources=resources)
                    # This outer watch outlives verification savepoints and stays
                    # attached to the same connection through commit/rollback.
                    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                        capacity=resources.capacity, floors=floors, deadline=deadline,
                        cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                    watch.start()
                    with archives.verified_archive_inventory(conn, source_root=source_root,
                            destination_root=destination_root, max_objects=max_objects,
                            max_bytes=max_bytes, page_rows=page_rows, policy=policy,
                            resource_limits=limits, cancelled=cancelled) as inventory:
                        with headers.verified_copy(conn, page_rows=page_rows,
                                timeout_seconds=limits["movement_timeout_seconds"]) as verified:
                            with raw.verified_copy(conn, page_rows=page_rows,
                                    timeout_seconds=limits["movement_timeout_seconds"]) as lookup:
                                if not references.inspect_references(conn)["references_complete"]:
                                    raise RuntimeError("fact_header_handoff_references_incomplete")
                                for relation in reference_move.RELATIONS:
                                    if reference_move.inspect_reference_catalog(
                                            conn, relation=relation)["placement"] != "history":
                                        raise RuntimeError("fact_header_handoff_reference_not_on_hdd")
                                active = {name: _oid(conn, SCHEMA+"."+name)
                                          for name in (*headers.TABLE_NAMES, raw.NAME)}
                                retained = {"fact_versions": _oid(conn, headers.SOURCE),
                                            raw.NAME: _oid(conn, raw.SOURCE)}
                                source_path, source_identity = archives._root(
                                    source_root, saved["recent_device"])
                                destination_path, destination_identity = archives._root(
                                    destination_root, saved["history_device"])
                                receipt = {
                                    "schema_version": RECEIPT_VERSION,
                                    "binding": saved, "policy_fingerprint": policy.fingerprint,
                                    "active_relation_oids": active, "retained_relation_oids": retained,
                                    "source_root": str(source_path), "destination_root": str(destination_path),
                                    "source_root_identity": list(source_identity),
                                    "destination_root_identity": list(destination_identity),
                                    "archive_inventory_sha256": inventory["inventory_sha256"],
                                    "verified_archive_objects": inventory["verified_catalog_objects"],
                                    "verified_archive_bytes": inventory["verified_catalog_bytes"],
                                    "verified_header_rows": verified["verified_header_rows"],
                                    "verified_lookup_rows": lookup["verified_lookup_rows"],
                                }
                                _switch_verified_tables(conn, verified, prevalidated=True,
                                    raw_mapping=True, evidence={"source_retained": True, "handoff": receipt})
                                watch.check()
                # Successful savepoint contexts have ended, but watcher and
                # transaction locks still protect the actual COMMIT.
            watch.check()
            logger.info("fact_header_preserving_handoff_committed | rows=%s duration_seconds=%s",
                        receipt["verified_header_rows"], monotonic()-started)
            return {"database_handoff_committed": True, "source_preserved": True,
                    "collection_resume_authorized": False, "root_activation_required": True,
                    "receipt": receipt, "resource_budget": budget}
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            if watch is not None:
                watch.stop(conn)


def inspect_handoff(conn, *, policy, source_root, destination_root):
    """Read the durable outcome after uncertainty; never switch or resume.

    Caller owns a bounded read transaction. The original migration deadline does
    not prevent inspecting an already committed result. This checks database
    identity, relation identities and contract, retained-source insert guards,
    fixed disk binding and the archive roots. It does not rehash all archives,
    certify current data integrity or authorize old-image rollback.
    """
    if not conn.in_transaction():
        raise ValueError("fact_header_handoff_inspection_transaction_required")
    # A missing certificate is not rollback evidence while the original
    # transaction may still commit. Share the existing migration ownership fence.
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                       {"name": LOCK}):
        raise RuntimeError("fact_header_handoff_outcome_pending")
    row = conn.execute(text("""
        SELECT state,evidence FROM market.fact_storage_state
        WHERE layout_version='market.fact_storage_tiers.v2'
    """)).mappings().one_or_none()
    if row is None:
        if (conn.scalar(text("SELECT state FROM market.fact_storage_state "
                             "WHERE layout_version='market.fact_storage_tiers.v1'")) != "ready"
                or conn.scalar(text("SELECT to_regnamespace(:name)"),
                               {"name": RETAINED}) is not None):
            raise RuntimeError("fact_header_handoff_outcome_unknown")
        return {"database_handoff_committed": False, "collection_resume_authorized": False}
    evidence = row["evidence"] or {}
    receipt = evidence.get("handoff")
    if (row["state"] != "ready" or evidence.get("source_retained") is not True
            or not isinstance(receipt, dict) or receipt.get("schema_version") != RECEIPT_VERSION
            or receipt.get("policy_fingerprint") != policy.fingerprint
            or receipt.get("source_root") != str(Path(source_root))
            or receipt.get("destination_root") != str(Path(destination_root))):
        raise RuntimeError("fact_header_handoff_receipt_mismatch")
    saved = receipt["binding"]
    pid = physical.verify(conn, saved)
    for role, path in (("source", source_root), ("destination", destination_root)):
        device = saved["recent_device" if role == "source" else "history_device"]
        if list(archives._root(path, device)[1]) != receipt[role+"_root_identity"]:
            raise RuntimeError("fact_header_handoff_archive_root_changed")
    expected_names = set((*headers.TABLE_NAMES, raw.NAME))
    if (set(receipt["active_relation_oids"]) != expected_names
            or set(receipt["retained_relation_oids"]) != {"fact_versions", raw.NAME}):
        raise RuntimeError("fact_header_handoff_receipt_inventory_invalid")
    for schema, key in (("market", "active_relation_oids"), (RETAINED, "retained_relation_oids")):
        for name, expected in receipt[key].items():
            if _oid(conn, schema+"."+name) != expected:
                raise RuntimeError("fact_header_handoff_relation_identity_changed")
    for name, trigger in (("fact_versions", "trg_retained_source_closed"),
                          (raw.NAME, "trg_retained_raw_source_closed")):
        closed = conn.scalar(text("""
            SELECT count(*) FROM pg_trigger
            WHERE tgrelid=to_regclass(:relation) AND tgname=:trigger
              AND tgenabled='A' AND tgtype=7 AND NOT tgisinternal
              AND tgfoid='market.reject_immutable_mutation()'::regprocedure
        """), {"relation": RETAINED+"."+name, "trigger": trigger})
        if closed != 1:
            raise RuntimeError("fact_header_handoff_retained_source_not_closed")
    for relation in ("market.fact_identities", raw.SOURCE, *reference_move.RELATIONS):
        physical.verify_group(conn, relation, history=True, saved=saved, pid=pid)
    assert_fact_storage_contract(conn)
    return {"database_handoff_committed": True, "source_preserved": True,
            "collection_resume_authorized": False, "root_activation_required": True,
            "receipt": receipt}

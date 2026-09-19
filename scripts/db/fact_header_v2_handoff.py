"""Fixed preserving database handoff, initial policy and outcome reconciliation.

Internal operator boundary, not deployment orchestration. The caller must stop
and drain publishers before calling and keep them stopped until the matching
runtime and archive root are activated. No host service or runtime configuration
is changed by these internal database/policy operations.
"""
from __future__ import annotations

from contextlib import contextmanager

import hashlib
import json
import logging
from pathlib import Path
from time import monotonic

from sqlalchemy import select, text
from sqlalchemy.orm import Session

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



@contextmanager
def _staging_transaction(engine, *, placement, policy, limits, deadline, cancelled):
    """Own a fixed staging transaction and its resource watch through commit."""
    deadline = min(deadline, monotonic()+limits["movement_timeout_seconds"])
    watch = None
    with engine.connect() as conn:
        try:
            with conn.begin():
                previous = conn.scalar(text(
                    "SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous:
                    deadline = min(deadline, monotonic()+previous/1000)
                with migration_step(conn, limits["movement_timeout_seconds"]):
                    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                                            "hashtextextended('qt.storage.management.v1',0))")):
                        raise RuntimeError("fact_header_staging_storage_busy")
                    saved, _ = physical.observe(conn, placement)
                    targets = (placement.recent, placement.history)
                    if conn.scalar(text("SELECT to_regclass(:name)"),
                                   {"name": SCHEMA+".capture"}) is not None:
                        seconds = conn.scalar(text(f"""SELECT EXTRACT(EPOCH FROM
                            prepared_at+interval '24 hours'-clock_timestamp())
                            FROM {SCHEMA}.capture WHERE id=1"""))
                        deadline = min(deadline, monotonic()+float(seconds))
                    resources = observe_header_resources(conn, targets,
                        pg_controldata=placement.pg_controldata,
                        timeout_seconds=min(30, limits["movement_timeout_seconds"]))
                    # Page/index allocations use the explicitly supplied maintenance
                    # allowances on BOTH drives; WAL/temp/growth retain their own
                    # allowances. No whole-copy size or free-space estimate is invented.
                    _, floors = reference_move._budget(conn,
                        observed={"bytes": 0, "_binding": saved}, policy=policy,
                        limits=limits, targets=targets, resources=resources)
                    watch = _MoveWatch(driver=conn.connection.driver_connection,
                        targets=targets, capacity=resources.capacity, floors=floors,
                        deadline=deadline, cancelled=cancelled,
                        grace=limits["cancellation_grace_seconds"])
                    watch.start()
                    yield conn, saved
                    watch.check()
            watch.check()
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            if watch is not None:
                watch.stop(conn)


def stage_handoff(engine, *, placement, policy, resource_limits, source_root,
                  destination_root, max_page_bytes, page_rows=128,
                  max_duration_seconds=3600, cancelled=None):
    """Run one bounded fixed-layout staging pass; never switch or resume clients.

    Existing copy cursors/queues/constraints are the only durable progress.
    Retry rechecks those identities and reuses verified archive objects; archive
    scans restart from the beginning. This pass is not final concurrent-inventory
    readiness. The original capture clock, including time between retries,
    remains the one-day limit. Callers must supply measured per-step allowances.
    """
    limits = _limits(resource_limits)
    if (not isinstance(placement, physical.CopyPlacement)
            or type(page_rows) is not int or not 1 <= page_rows <= 256
            or type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 86400
            or type(max_page_bytes) is not int or max_page_bytes <= 0
            or (cancelled is not None and not callable(cancelled))):
        raise ValueError("fact_header_staging_inputs_invalid")
    reference_move._fixed_inputs(policy, limits, (placement.recent, placement.history))
    if (policy.archives != policy.history or policy.backups != policy.history
            or not policy.movement_enabled or not policy.backup_enabled):
        raise ValueError("fact_header_staging_fixed_automatic_policy_required")
    deadline = monotonic()+max_duration_seconds

    def step_limits():
        if cancelled is not None and cancelled():
            raise RuntimeError("storage_move_cancelled")
        seconds = min(limits["movement_timeout_seconds"], int(deadline-monotonic()))
        if seconds < 1:
            raise RuntimeError("fact_header_staging_time_budget_exceeded")
        return {**limits, "movement_timeout_seconds": seconds}

    def transaction():
        return _staging_transaction(engine, placement=placement, policy=policy,
            limits=step_limits(), deadline=deadline, cancelled=cancelled)

    def catch_up():
        # Separate transactions retain each successful page across interruption.
        for copier in (headers, raw):
            while True:
                with transaction() as (conn, _):
                    report = copier.copy_page(conn, page_rows=page_rows,
                                              timeout_seconds=step_limits()["movement_timeout_seconds"])
                if report["caught_up_at_observation"]:
                    break

    logger.info("fact_header_staging_started | original_attempt_clock_preserved=true")
    with transaction() as (conn, saved):
        cutoff = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date - :days"),
                             {"days": policy.recent_days})
        if placement.history_before > cutoff:
            raise RuntimeError("fact_header_staging_recent_window_on_history")
        source, _ = archives._root(source_root, saved["recent_device"])
        destination, _ = archives._root(destination_root, saved["history_device"])
        if (not source.is_relative_to(Path(placement.recent.root).resolve(strict=True))
                or not destination.is_relative_to(Path(placement.history.root).resolve(strict=True))):
            raise RuntimeError("fact_header_staging_archive_outside_fixed_target")
        headers.prepare_copy(conn, placement=placement,
                             timeout_seconds=step_limits()["movement_timeout_seconds"])
        raw.prepare_copy(conn, timeout_seconds=step_limits()["movement_timeout_seconds"])
    catch_up()
    with transaction() as (conn, _):
        headers.enable_identity_capture(conn, page_rows=page_rows,
                                         timeout_seconds=step_limits()["movement_timeout_seconds"])
        slots = references.inspect_references(conn)["references"]
    for slot in slots:
        if slot["relation"] == references.PARENT:
            continue
        with transaction() as (conn, _):
            references.prepare_reference(conn, relation=slot["relation"],
                timeout_seconds=step_limits()["movement_timeout_seconds"])
        with transaction() as (conn, _):
            references.validate_reference(conn, relation=slot["relation"],
                timeout_seconds=step_limits()["movement_timeout_seconds"])
    with transaction() as (conn, _):
        references.adopt_payload_references(conn,
            timeout_seconds=step_limits()["movement_timeout_seconds"])
    for relation in reference_move.RELATIONS:
        reference_move.move_reference_catalog(engine, relation=relation,
            policy=policy, resource_limits=step_limits(), cancelled=cancelled)
    for family in archives.FAMILIES:
        cursor = ""
        while True:
            report = archives.copy_archive_page(engine, family=family,
                source_root=source_root, destination_root=destination_root,
                after_id=cursor, page_rows=page_rows, max_page_bytes=max_page_bytes,
                policy=policy, resource_limits=step_limits(), cancelled=cancelled)
            if report["page_objects"] < page_rows:
                break
            if report["next_after_id"] <= cursor:
                raise RuntimeError("fact_header_staging_archive_cursor_did_not_advance")
            cursor = report["next_after_id"]
    catch_up()
    step_limits()
    logger.info("fact_header_staging_pass_completed | final_fenced_verification_required=true")
    return {"staging_pass_complete": True, "source_authoritative": True,
            "migration_ready": False, "final_fenced_verification_required": True,
            "collection_resume_authorized": False, "database_handoff_committed": False}

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


POLICY_OPERATION = "qt.storage_preserving_handoff_policy.v1"


def _policy_plan_id(receipt):
    return "handoff-" + hashlib.sha256(json.dumps(receipt, sort_keys=True,
        separators=(",", ":")).encode()).hexdigest()[:32]


def inspect_handoff_policy(conn, *, policy, source_root, destination_root):
    """Reconcile initial policy outcome, including after the copy clock expired.

    Caller owns a bounded transaction. Never replay activation on an uncertain
    reply: first distinguish absent, committed and subsequently changed policy.
    This does not certify runtime configuration or authorize service restart.
    """
    outcome = inspect_handoff(conn, policy=policy, source_root=source_root,
                              destination_root=destination_root)
    if not outcome["database_handoff_committed"]:
        return {"policy_activated": False, "collection_resume_authorized": False}
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                            "hashtextextended('qt.storage.management.v1',0))")):
        raise RuntimeError("fact_header_policy_outcome_pending")
    plan_id = _policy_plan_id(outcome["receipt"])
    plan = conn.execute(text("""SELECT state,policy_hash,policy,base_revision,progress
        FROM public.portal_storage_plans WHERE id=:id"""), {"id": plan_id}).mappings().one_or_none()
    if plan is None:
        return {"policy_activated": False, "collection_resume_authorized": False}
    progress = plan["progress"]
    if (plan["state"] != "completed" or plan["policy_hash"] != policy.fingerprint
            or plan["policy"] != policy.to_dict()
            or progress != {"operation": POLICY_OPERATION, "policy_revision": plan["base_revision"]+1,
                            "database_handoff_plan": plan_id, "runtime_activation_required": True}):
        raise RuntimeError("fact_header_policy_receipt_mismatch")
    current = conn.execute(text("""SELECT revision,policy,applied_plan_id
        FROM public.portal_storage_policy WHERE id=1""")).mappings().one_or_none()
    current_matches = (current is not None and current["revision"] == progress["policy_revision"]
                       and current["policy"] == policy.to_dict() and current["applied_plan_id"] == plan_id)
    return {"policy_activated": True, "policy_current": current_matches,
            "policy_revision": progress["policy_revision"], "plan_id": plan_id,
            "runtime_activation_required": True, "collection_resume_authorized": False}


def activate_handoff_policy(engine, *, policy, resource_limits, source_root, destination_root,
                            cancelled=None):
    """Apply the first fixed policy/registry atomically after a committed handoff.

    Internal operator boundary: clients must remain paused. Reuses the existing
    target, prepared-tablespace, policy and plan records; creates no new authority,
    tablespace, directory, worker or host configuration. Original attempt and
    resource limits supervise the transaction through commit. Outcome inspection
    is available after expiry; activation does not extend the one-day clock.
    """
    from portal.backend.db.storage_target_models import StorageTargetRecord, StoragePolicyRecord, StoragePlanRecord
    from portal.backend.service.storage.header_catalog import read_header_catalog
    from portal.backend.service.storage.header_filesystem import verify_header_filesystem
    from portal.backend.service.storage.header_destinations import register_header_tablespaces
    from portal.backend.service.storage_management import _target

    limits = _limits(resource_limits)
    if cancelled is not None and not callable(cancelled):
        raise ValueError("fact_header_policy_cancellation_callback_invalid")
    if not policy.movement_enabled or not policy.backup_enabled:
        raise ValueError("fact_header_policy_automatic_history_and_recovery_required")
    started = monotonic()
    deadline = started + limits["movement_timeout_seconds"]
    watch = None
    with engine.connect() as conn:
        try:
            with conn.begin():
                previous = conn.scalar(text("SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous:
                    deadline = min(deadline, started + previous/1000)
                with migration_step(conn, limits["movement_timeout_seconds"]):
                    observed = inspect_handoff(conn, policy=policy, source_root=source_root,
                                               destination_root=destination_root)
                    if not observed["database_handoff_committed"]:
                        raise RuntimeError("fact_header_policy_database_handoff_required")
                    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                                            "hashtextextended('qt.storage.management.v1',0))")):
                        raise RuntimeError("fact_header_policy_storage_busy")
                    receipt = observed["receipt"]
                    saved = receipt["binding"]
                    placement = physical._restore(saved["plan"])
                    targets = (placement.recent, placement.history)
                    reference_move._fixed_inputs(policy, limits, targets)
                    if policy.archives != policy.history or policy.backups != policy.history:
                        raise ValueError("fact_header_policy_fixed_history_archive_recovery_required")
                    cutoff = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date - :days"),
                                         {"days": policy.recent_days})
                    if placement.history_before > cutoff:
                        raise RuntimeError("fact_header_policy_recent_window_already_on_history")
                    if not Path(destination_root).resolve(strict=True).is_relative_to(
                            Path(placement.history.root).resolve(strict=True)):
                        raise RuntimeError("fact_header_policy_archive_outside_history_target")
                    seconds = conn.scalar(text(f"""SELECT EXTRACT(EPOCH FROM
                        prepared_at+interval '24 hours'-clock_timestamp()) FROM {SCHEMA}.capture WHERE id=1"""))
                    deadline = min(deadline, monotonic()+float(seconds))
                    resources = observe_header_resources(conn, targets,
                        pg_controldata=placement.pg_controldata,
                        timeout_seconds=min(30, limits["movement_timeout_seconds"]))
                    budget, floors = reference_move._budget(conn,
                        observed={"bytes": 0, "_binding": saved}, policy=policy,
                        limits=limits, targets=targets, resources=resources)
                    watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                        capacity=resources.capacity, floors=floors, deadline=deadline,
                        cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                    watch.start()
                    # Read-only, bounded catalog observation uses its established
                    # independent transaction. Cooperating changes remain fenced
                    # by our storage lock; publishers must be paused by the caller.
                    def probe_budget():
                        watch.check()
                        remaining = min(30, int(deadline-monotonic()))
                        if remaining < 1:
                            raise RuntimeError("fact_header_policy_time_budget_exceeded")
                        return remaining
                    catalog = read_header_catalog(engine, max_partitions=4096,
                        timeout_seconds=probe_budget(),
                        destination_tablespace_oids=(placement.history_tablespace_oid,))
                    verified = verify_header_filesystem(catalog, targets,
                        pg_controldata=placement.pg_controldata,
                        timeout_seconds=probe_budget(),
                        destination_assignments={placement.history.target_id: placement.history_tablespace_oid})
                    if any(group.storage_day >= cutoff and any(
                            relation.target_id != placement.recent.target_id for relation in group.relations)
                            for group in verified.snapshot.partitions):
                        raise RuntimeError("fact_header_policy_recent_files_not_on_ssd")
                    watch.check()
                    plan_id = _policy_plan_id(receipt)
                    with Session(bind=conn, join_transaction_mode="create_savepoint") as session, session.begin():
                        records = list(session.scalars(select(StorageTargetRecord).limit(3)))
                        if len(records)>2 or any(row.id not in {x.target_id for x in targets} for row in records):
                            raise RuntimeError("fact_header_policy_target_inventory_changed")
                        by_id = {row.id: row for row in records}
                        for target in targets:
                            row = by_id.get(target.target_id)
                            if row is None:
                                session.add(StorageTargetRecord(id=target.target_id, label=target.label,
                                    filesystem_uuid=target.filesystem_uuid, root=target.root, medium=target.medium,
                                    roles=list(target.roles), state=target.state))
                            elif (_target(row) != target or row.reserved_bytes or row.auxiliary_reserved_bytes):
                                raise RuntimeError("fact_header_policy_target_identity_or_claim_changed")
                        if session.scalar(select(StoragePlanRecord.id).where(
                                StoragePlanRecord.state.in_(("queued", "running", "blocked"))).limit(1)):
                            raise RuntimeError("fact_header_policy_change_in_progress")
                        session.flush()
                        register_header_tablespaces(session, verified=verified)
                        config = session.get(StoragePolicyRecord, 1)
                        old_plan = session.get(StoragePlanRecord, plan_id)
                        if old_plan is not None:
                            result = inspect_handoff_policy(conn, policy=policy,
                                source_root=source_root, destination_root=destination_root)
                            if not result.get("policy_current"):
                                raise RuntimeError("fact_header_policy_changed_after_activation")
                        else:
                            if config is None:
                                config = StoragePolicyRecord(id=1, revision=0)
                                session.add(config)
                            if config.revision != 0 or config.policy not in (None, policy.to_dict()):
                                raise RuntimeError("fact_header_policy_existing_configuration_requires_review")
                            now = session.scalar(text("SELECT clock_timestamp()"))
                            config.policy = policy.to_dict()
                            config.revision = 1
                            config.applied_plan_id = plan_id
                            config.updated_at = now
                            session.add(StoragePlanRecord(id=plan_id, request_id=plan_id, base_revision=0,
                                policy=policy.to_dict(), policy_hash=policy.fingerprint, state="completed",
                                impact={"database_handoff_verified": True},
                                progress={"operation": POLICY_OPERATION, "policy_revision": 1,
                                          "database_handoff_plan": plan_id, "runtime_activation_required": True},
                                created_at=now, updated_at=now))
                            session.flush()
                            result = inspect_handoff_policy(conn, policy=policy,
                                source_root=source_root, destination_root=destination_root)
                        watch.check()
            watch.check()
            logger.info("fact_header_initial_policy_activated | plan_id=%s policy_revision=%s",
                        result["plan_id"], result["policy_revision"])
            return {**result, "resource_budget": budget}
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            if watch is not None:
                watch.stop(conn)

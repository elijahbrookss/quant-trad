"""Move fixed archive catalogs and the retained rollback source to history.

Internal operator step, not runtime wiring or a generic mover. Each operation
owns one transaction through commit, reuses existing physical/resource guards
and preserves table identity and every logical definition. V1 stays active.
"""
from __future__ import annotations

from contextlib import nullcontext

import logging
import math
from time import monotonic

from sqlalchemy import text

from core.storage_move_budget import assess_header_move_resources
from core.storage_targets import StoragePolicy
from portal.backend.service.storage.header_inspection import _bound_resource_targets
from portal.backend.service.storage.header_movement import _MoveWatch
from portal.backend.service.storage.header_resource_claims import _limits
from portal.backend.service.storage.header_resources import observe_header_resources
from scripts.db import fact_header_v2_copy as headers, fact_header_v2_placement as physical
from scripts.db import fact_header_v2_references as references
from scripts.db.fact_header_v2_admission import _columns, _constraints, _secondary_indexes
from scripts.db.fact_header_v2_capture import SCHEMA, inspect_capture

RELATIONS = ("market.fact_archive_material_aliases", "market.fact_archive_canonical_dependencies")
RETAINED_LEGACY = "qt_fact_storage_cutover_v1.fact_versions"
logger = logging.getLogger(__name__)


def _known(relation):
    if relation not in (*RELATIONS, RETAINED_LEGACY):
        raise ValueError("archive_reference_move_known_catalog_required")
    return relation


def _definition(conn, relation):
    identity = conn.execute(text("""
        SELECT oid::bigint,relkind,relpersistence,relowner::bigint,relacl,
               relrowsecurity,relforcerowsecurity,relreplident,reloptions
        FROM pg_class WHERE oid=to_regclass(:relation)
    """), {"relation": relation}).one()
    if identity[1:3] != ("r", "p"):
        raise RuntimeError("archive_reference_move_logged_ordinary_catalog_required")
    indexes = conn.execute(text("""
        SELECT indexrelid::bigint,indisvalid,indisready
        FROM pg_index WHERE indrelid=to_regclass(:relation) ORDER BY indexrelid LIMIT 65
    """), {"relation": relation}).all()
    if not indexes or len(indexes)>64 or any(not row[1] or not row[2] for row in indexes):
        raise RuntimeError("archive_reference_move_index_inventory_invalid")
    triggers = conn.execute(text("""
        SELECT to_jsonb(t) FROM pg_trigger t WHERE tgrelid=to_regclass(:relation)
        ORDER BY oid LIMIT 65
    """), {"relation": relation}).scalars().all()
    if len(triggers)>64:
        raise RuntimeError("archive_reference_move_trigger_inventory_exceeded")
    return (tuple(identity), _columns(conn, relation), _constraints(conn, relation),
            _secondary_indexes(conn, relation, include_constraints=True),
            [tuple(row) for row in indexes], triggers)


def _assert_retained_source(conn):
    """Only the old fenced rollback table is admitted before header copying."""
    triggers = conn.execute(text("""
        SELECT t.tgname,t.tgtype,t.tgenabled,n.nspname,p.proname
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE t.tgrelid=to_regclass(:relation) AND NOT t.tgisinternal
    """), {"relation": RETAINED_LEGACY}).all()
    required = {
        ("trg_storage_cutover_reject_insert", 7, "A", "market", "reject_immutable_mutation"),
        ("trg_reject_mutation_fact_versions", 27, "A", "market", "reject_immutable_mutation"),
    }
    if not required <= {tuple(row) for row in triggers}:
        raise RuntimeError("retained_source_move_immutable_fence_required")
    if conn.scalar(text("""
        SELECT EXISTS(SELECT 1 FROM pg_inherits
                      WHERE inhrelid=to_regclass(:relation) OR inhparent=to_regclass(:relation))
            OR EXISTS(SELECT 1 FROM pg_constraint
                      WHERE contype='f' AND confrelid=to_regclass(:relation))
            OR EXISTS(SELECT 1 FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
                      WHERE d.classid='pg_rewrite'::regclass
                        AND d.refclassid='pg_class'::regclass
                        AND d.refobjid=to_regclass(:relation))
    """), {"relation": RETAINED_LEGACY}):
        raise RuntimeError("retained_source_move_unexpected_dependency")


def inspect_reference_catalog(conn, *, relation, forward_operation_sha256=None):
    """Read this fixed owner's catalog placement; never authorize a move."""
    _known(relation)
    if forward_operation_sha256 is not None:
        from scripts.db import archive_root_v2_copy as archives
        from scripts.db import fact_header_forward_adoption as adoption
        if relation not in RELATIONS:
            raise ValueError("archive_reference_forward_catalog_required")
        with archives._operation_step(conn, 30,
                forward_operation_sha256=forward_operation_sha256) as (saved, _):
            adoption._reference_states(conn, adoption._state(conn, forward_operation_sha256))
            return _reference_placement(conn, relation, saved, physical.verify(conn, saved))
    state = headers._inspect_progress(conn)
    if state["placement"] is None:
        raise RuntimeError("archive_reference_move_physical_placement_required")
    if relation == RETAINED_LEGACY:
        _assert_retained_source(conn)
    else:
        references._inventory(conn)
    return _reference_placement(conn, relation, state["placement"], state["_placement_pid"])


def _reference_placement(conn, relation, saved, pid):
    definition = _definition(conn, relation)
    spaces = conn.execute(text("""
        WITH heap AS (SELECT oid,reltoastrelid FROM pg_class WHERE oid=to_regclass(:relation)),
        heaps AS (SELECT oid FROM heap UNION ALL SELECT reltoastrelid FROM heap WHERE reltoastrelid<>0),
        members AS (SELECT oid FROM heaps UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid IN(SELECT oid FROM heaps))
        SELECT DISTINCT COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::bigint
        FROM members JOIN pg_class c ON c.oid=members.oid
        CROSS JOIN pg_database d WHERE d.datname=current_database()
    """), {"relation": relation}).scalars().all()
    if spaces == [1663]:
        history = False
    elif spaces == [saved["plan"]["history_tablespace_oid"]]:
        history = True
    else:
        raise RuntimeError("archive_reference_move_mixed_or_unknown_placement")
    physical.verify_group(conn, relation, history=history, saved=saved, pid=pid)
    return {"relation": relation, "relation_oid": definition[0][0],
            "placement": "history" if history else "recent",
            "bytes": conn.scalar(text("SELECT pg_total_relation_size(to_regclass(:relation))"),
                                 {"relation": relation}),
            "migration_ready": False, "_definition": definition, "_binding": saved}


def _fixed_inputs(policy, limits, targets):
    ids = {target.target_id for target in targets}
    if set(limits["temporary_bytes"]) != ids:
        raise ValueError("archive_reference_move_fixed_resource_inventory_required")
    if (not isinstance(policy, StoragePolicy) or policy.recent != (targets[0].target_id,)
            or policy.history != (targets[1].target_id,)):
        raise ValueError("archive_reference_move_fixed_policy_required")
    policy.validate_targets(targets)


def _budget(conn, *, observed, policy, limits, targets, resources):
    ids = {target.target_id for target in targets}
    # The global storage transaction lock prevents new cooperating reservations.
    # Retain every already durable claim; the current synchronous copy allowance
    # exists only for this transaction and needs no new job or migration ledger.
    rows = conn.execute(text("""
        SELECT id,filesystem_uuid,root,medium,state,reserved_bytes,auxiliary_reserved_bytes
        FROM public.portal_storage_targets ORDER BY id LIMIT 33
    """)).mappings().all()
    if len(rows)>2 or any(row["id"] not in ids for row in rows):
        raise RuntimeError("archive_reference_move_registered_inventory_changed")
    by_id = {target.target_id: target for target in targets}
    if any(row["filesystem_uuid"] != by_id[row["id"]].filesystem_uuid
           or row["root"] != by_id[row["id"]].root or row["medium"] != by_id[row["id"]].medium
           or row["state"] != "active" for row in rows):
        raise RuntimeError("archive_reference_move_registered_identity_changed")
    current_policy = conn.scalar(text("SELECT policy FROM public.portal_storage_policy WHERE id=1"))
    if current_policy is not None and StoragePolicy.from_dict(current_policy).fingerprint != policy.fingerprint:
        raise RuntimeError("archive_reference_move_saved_policy_changed")
    copy_bytes = observed["bytes"]
    own = max(1, copy_bytes)
    reservations = {key: 0 for key in ids}
    auxiliary = dict(reservations)
    for row in rows:
        reservations[row["id"]] = row["reserved_bytes"]
        auxiliary[row["id"]] = row["auxiliary_reserved_bytes"]
    reservations[targets[1].target_id] += own
    wal, temporary = _bound_resource_targets(resources, targets, resources.capacity)
    if wal != targets[0].target_id or temporary != (targets[0].target_id,):
        raise RuntimeError("archive_reference_move_fixed_wal_and_temp_required")
    saved = observed["_binding"]
    for role, target in zip(("recent", "history"), targets):
        if resources.capacity[target.target_id].device_id != saved[role+"_device"]:
            raise RuntimeError("archive_reference_move_resource_device_changed")
    if (resources.database_identity != saved["database_identity"]
            or resources.backend_pid != conn.scalar(text("SELECT pg_backend_pid()"))):
        raise RuntimeError("archive_reference_move_resource_database_changed")
    budget = assess_header_move_resources(
        targets=targets, capacity=resources.capacity, policy=policy,
        observed_at=resources.observed_at, now=conn.scalar(text("SELECT clock_timestamp()")),
        copy_target_id=targets[1].target_id, copy_bytes=copy_bytes, own_reserved_bytes=own,
        reserved_bytes=reservations, auxiliary_reserved_bytes=auxiliary,
        own_auxiliary_reserved_bytes={key: 0 for key in ids},
        wal_target_id=wal, wal_bytes=limits["wal_bytes"],
        temporary_bytes=limits["temporary_bytes"], growth_bytes_per_second=limits["growth_bytes_per_second"],
        maintenance_bytes=limits["maintenance_bytes"], timeout_seconds=limits["movement_timeout_seconds"],
        cancellation_grace_seconds=limits["cancellation_grace_seconds"])
    if not budget["capacity_sufficient_for_declared_limits"]:
        raise RuntimeError("archive_reference_move_capacity_blocked")
    floors = {}
    for row in budget["filesystems"]:
        allowance = sum(row[key] for key in ("copy_bytes", "additional_wal_bytes",
                        "temporary_bytes", "ingestion_and_other_growth_bytes", "maintenance_bytes"))
        floors[row["target_id"]] = max(
            row["policy_reserve_bytes"] + row["other_reserved_copy_bytes"] + row["other_reserved_auxiliary_bytes"],
            row["available_bytes"] - allowance)
    return budget, floors


def move_reference_catalog(engine, *, relation, policy, resource_limits, cancelled=None,
                           expected_started_at=None, placement=None, deadline=None,
                           forward_operation_sha256=None, connection=None):
    """Move one known catalog+indexes atomically; preserve source semantics.

    An online caller may additionally bind the exact capture/placement and an
    earlier absolute deadline; these can only narrow the existing admission.
    Caller supplies measured resource allowances using the existing limits
    contract. Net filesystem consumption is supervised through commit/rollback;
    this is not per-process IO attribution or an instantaneous disk quota.
    A lost commit response is reconciled by inspecting actual placement and,
    while the original attempt is still valid, retrying: a verified already-HDD catalog is acknowledged without rewriting.
    """
    from scripts.db import archive_root_v2_copy as archives
    from scripts.db import fact_header_forward_adoption as adoption
    _known(relation)
    if forward_operation_sha256 is not None and relation not in RELATIONS:
        raise ValueError("archive_reference_forward_catalog_required")
    if connection is not None and (connection.engine is not engine or connection.closed
            or connection.invalidated or connection.in_transaction()):
        raise ValueError("archive_reference_move_connection_not_available")
    if ((expected_started_at is None) != (placement is None)
            or (placement is not None and (not isinstance(placement, physical.CopyPlacement)
                or not isinstance(expected_started_at, str) or not expected_started_at))
            or (deadline is not None and (type(deadline) not in (int, float)
                or not math.isfinite(deadline) or deadline <= monotonic()))):
        raise ValueError("archive_reference_move_attempt_binding_invalid")
    limits = _limits(resource_limits, migration=True)
    if cancelled is not None and not callable(cancelled):
        raise ValueError("archive_reference_move_cancellation_callback_invalid")
    if cancelled is not None and cancelled():
        raise RuntimeError("storage_move_cancelled")
    started = monotonic()
    deadline = min(deadline, started+limits["movement_timeout_seconds"]) if deadline is not None else started+limits["movement_timeout_seconds"]
    with (nullcontext(connection) if connection is not None else engine.connect()) as conn:
        watch = None
        try:
            with conn.begin():
                previous_ms = conn.scalar(text(
                    "SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'"))
                if previous_ms:
                    deadline = min(deadline, started+previous_ms/1000)
                with archives._operation_step(conn, limits["movement_timeout_seconds"], deadline=deadline,
                        forward_operation_sha256=forward_operation_sha256) as (_, owner_deadline):
                    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                                            "hashtextextended('qt.storage.management.v1',0))")):
                        raise RuntimeError("archive_reference_move_storage_busy")
                    # Collection continues on v1; this guards definitions, not
                    # source INSERTs. Only the selected catalog is fenced.
                    conn.exec_driver_sql(f"LOCK TABLE {headers.SOURCE} IN ACCESS SHARE MODE NOWAIT")
                    conn.exec_driver_sql(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
                    observed = inspect_reference_catalog(conn, relation=relation,
                        forward_operation_sha256=forward_operation_sha256)
                    saved = observed["_binding"]
                    plan = physical._restore(saved["plan"])
                    started_at = (adoption._state(conn, forward_operation_sha256)["started_at"].isoformat()
                                  if forward_operation_sha256 is not None else inspect_capture(conn)["started_at"])
                    if (placement is not None and (plan != placement or started_at != expected_started_at)):
                        raise RuntimeError("archive_reference_move_attempt_binding_changed")
                    deadline = min(deadline, owner_deadline)
                    targets = (plan.recent, plan.history)
                    _fixed_inputs(policy, limits, targets)
                    if observed["placement"] == "history":
                        receipt = {"relation": relation, "relation_oid": observed["relation_oid"],
                                   "placement": "history", "reused": True, "migration_ready": False}
                    else:
                        resources = observe_header_resources(conn, targets,
                            pg_controldata=plan.pg_controldata, timeout_seconds=min(30,limits["movement_timeout_seconds"]))
                        budget, floors = _budget(conn, observed=observed, policy=policy, limits=limits,
                                                 targets=targets, resources=resources)
                        watch = _MoveWatch(driver=conn.connection.driver_connection, targets=targets,
                            capacity=resources.capacity, floors=floors, deadline=deadline,
                            cancelled=cancelled, grace=limits["cancellation_grace_seconds"])
                        watch.start()
                        quote = conn.dialect.identifier_preparer.quote
                        destination = quote(saved["history_name"])
                        indexes = conn.execute(text("""
                            SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                            WHERE i.indrelid=to_regclass(:relation) ORDER BY c.oid LIMIT 65
                        """), {"relation": relation}).scalars().all()
                        conn.exec_driver_sql(f"ALTER TABLE {relation} SET TABLESPACE {destination}")
                        watch.check()
                        for name in indexes:
                            conn.exec_driver_sql(f"ALTER INDEX {quote(relation.split('.', 1)[0])}.{quote(name)} SET TABLESPACE {destination}")
                            watch.check()
                        after = inspect_reference_catalog(conn, relation=relation,
                            forward_operation_sha256=forward_operation_sha256)
                        if after["placement"] != "history" or after["_definition"] != observed["_definition"]:
                            raise RuntimeError("archive_reference_move_logical_definition_changed")
                        watch.check()
                        receipt = {"relation": relation, "relation_oid": observed["relation_oid"],
                                   "placement": "history", "reused": False, "copy_bytes": observed["bytes"],
                                   "resource_budget": budget, "migration_ready": False}
                # Watch remains pinned to this connection through commit.
            if watch is not None:
                watch.check()
            if monotonic() >= deadline:
                raise RuntimeError("archive_reference_move_time_budget_exceeded")
            logger.info("archive_reference_catalog_on_history | relation=%s reused=%s duration_seconds=%s",
                        relation, receipt["reused"], monotonic()-started)
            return {**receipt, "committed": True}
        except Exception as exc:
            if watch is not None and watch.failure is not None:
                raise RuntimeError(watch.failure) from exc
            raise
        finally:
            if watch is not None:
                watch.stop(conn)

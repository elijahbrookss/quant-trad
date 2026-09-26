"""Preserve exact per-page proof for a newly empty fixed migration shadow.

No historical target is trusted retroactively. Install before the first page.
Target insert guards establish target-subset-of-source; immutable rows and
monotone routing preserve verified pages. Existing capture/cursors prove source
coverage at a final fence. This does not verify archives or authorize a switch.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
import logging
from time import monotonic

from sqlalchemy import text

from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from scripts.db import fact_header_v2_placement as physical
from scripts.db.fact_header_v2_admission import assert_v1_source_admission
from scripts.db.fact_header_v2_capture import SCHEMA, migration_step

logger = logging.getLogger(__name__)

STATE = SCHEMA + ".online_copy_proof"
ROW_TRIGGER = "trg_qt_online_row_guard"
TRUNCATE_TRIGGER = "trg_qt_online_no_truncate"
_ACTIVE = "qt.online_copy_proof.switch_context"
ROLES = {
    "fact_versions": "header",
    "fact_identities": "identity",
    "fact_header_series_days": "routing",
    "fact_header_partitions": "partition",
    raw.NAME: "raw",
}
_TRUNCATE = """
BEGIN
    RAISE EXCEPTION 'online_copy_proof_truncate_refused';
END;
"""
_ROUTING = """
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'online_copy_proof_routing_delete_refused';
    END IF;
    IF TG_OP = 'UPDATE' AND (
        (NEW.series_id,NEW.storage_day) IS DISTINCT FROM (OLD.series_id,OLD.storage_day)
        OR NEW.min_observation_time > OLD.min_observation_time
        OR NEW.max_observation_time < OLD.max_observation_time
    ) THEN
        RAISE EXCEPTION 'online_copy_proof_routing_shrink_refused';
    END IF;
    RETURN NEW;
END;
"""
_PARTITION = """
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'online_copy_proof_partition_mutation_refused';
    END IF;
    RETURN NEW;
END;
"""


def _bodies(binding):
    bodies = {"truncate": _TRUNCATE, "routing": _ROUTING, "partition": _PARTITION}
    for role, source in (("header", headers.SOURCE), ("identity", headers.SOURCE),
                         ("raw", raw.SOURCE)):
        oid = binding["source_oids"][source]
        if role == "identity":
            projection = "jsonb_build_object(" + ",".join(
                "'" + key + "', original." + key for key in headers.IDENTITY_COLUMNS) + ")"
        else:
            projection = "to_jsonb(original)"
        predicate = ("original.raw_record_id=NEW.raw_record_id AND original.manifest_id=NEW.manifest_id"
                     if role == "raw" else "original.id=NEW.id")
        bodies[role] = f"""
DECLARE expected jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'online_copy_proof_row_mutation_refused';
    END IF;
    IF '{source}'::regclass::oid <> {oid}::oid THEN
        RAISE EXCEPTION 'online_copy_proof_source_changed';
    END IF;
    SELECT {projection} INTO expected FROM {source} original WHERE {predicate};
    IF expected IS NULL OR expected IS DISTINCT FROM to_jsonb(NEW) THEN
        RAISE EXCEPTION 'online_copy_proof_insert_mismatch: {role}';
    END IF;
    RETURN NEW;
END;
"""
    return bodies


def _binding(conn):
    return {
        "source_oids": {name: conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                        {"name": name}) for name in (headers.SOURCE, raw.SOURCE)},
        "targets": {name: headers._shape(conn, name) for name in ROLES},
        "target_properties": {name: raw._relation(conn, SCHEMA+"."+name) for name in ROLES},
        "state": headers._shape(conn, "online_copy_proof"),
        "header_progress_oid": conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                          {"name": headers.STATE}),
        "raw_progress_oid": conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                       {"name": raw.STATE}),
    }


def _install_truncate(conn, relation):
    conn.exec_driver_sql(f"CREATE TRIGGER {TRUNCATE_TRIGGER} BEFORE TRUNCATE ON {relation} "
                         f"FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.online_guard_truncate()")
    conn.exec_driver_sql(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {TRUNCATE_TRIGGER}")


def protect_new_partition(conn, day):
    """Called only in the transaction creating the private dated partition."""
    if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is None:
        return
    if not isinstance(day, date):
        raise ValueError("online_copy_proof_partition_date_required")
    relation = SCHEMA + ".fact_versions_" + day.strftime("%Y%m%d")
    # Row guards are cloned by PostgreSQL from the partition parent.
    _install_truncate(conn, relation)
    identity = raw._relation(conn, relation)
    saved = conn.scalar(text(f"SELECT leaf_bindings FROM {STATE} WHERE id=1"))
    if relation in saved:
        raise RuntimeError("online_copy_proof_partition_identity_already_recorded")
    conn.execute(text(f"UPDATE {STATE} SET leaf_bindings=leaf_bindings || CAST(:leaf AS jsonb) WHERE id=1"),
                 {"leaf": json.dumps({relation: identity})})


def prepare(conn, *, timeout_seconds=30):
    """Protect a demonstrably empty shadow; retry only verifies existing proof."""
    with migration_step(conn, timeout_seconds):
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is not None:
            inspect_protection(conn)
            return {"reused": True, "migration_ready": False}
        relations = [SCHEMA + "." + name for name in ROLES]
        conn.exec_driver_sql("LOCK TABLE " + ",".join(relations) +
                             " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        header = headers._inspect_progress(conn)
        lookup = raw._inspect(conn)
        assert_v1_source_admission(conn, identity_capture=header["identity_capture"])
        if header["verified_rows"] or lookup["verified_rows"] or header["identity_capture"]:
            raise RuntimeError("online_copy_proof_requires_new_empty_shadow")
        for relation in relations:
            if conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {relation})")):
                raise RuntimeError("online_copy_proof_requires_new_empty_shadow")
            if conn.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_trigger "
                                "WHERE tgrelid=to_regclass(:name) AND NOT tgisinternal)"),
                           {"name": relation}):
                raise RuntimeError("online_copy_proof_existing_target_trigger")
        if conn.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_inherits "
                            "WHERE inhparent=to_regclass(:name))"),
                       {"name": SCHEMA + ".fact_versions"}):
            raise RuntimeError("online_copy_proof_requires_no_existing_partitions")
        conn.exec_driver_sql(f"CREATE TABLE {STATE}(id integer PRIMARY KEY CHECK(id=1), binding jsonb NOT NULL, "
                             "leaf_bindings jsonb NOT NULL DEFAULT '{}'::jsonb)")
        binding = _binding(conn)
        for role, body in _bodies(binding).items():
            conn.exec_driver_sql(f"CREATE FUNCTION {SCHEMA}.online_guard_{role}() RETURNS trigger "
                "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$" + body + "$qt$")
            conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {SCHEMA}.online_guard_{role}() FROM PUBLIC")
        for name, role in ROLES.items():
            relation = SCHEMA + "." + name
            conn.exec_driver_sql(f"CREATE TRIGGER {ROW_TRIGGER} BEFORE INSERT OR UPDATE OR DELETE ON {relation} "
                                f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.online_guard_{role}()")
            conn.exec_driver_sql(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {ROW_TRIGGER}")
            _install_truncate(conn, relation)
        conn.execute(text(f"INSERT INTO {STATE}(id,binding) VALUES(1,CAST(:binding AS jsonb))"),
                     {"binding": json.dumps(binding)})
        inspect_protection(conn)
        logger.info("fact_header_online_proof_prepared | source_oid=%s",
                    binding["source_oids"][headers.SOURCE])
        return {"reused": False, "migration_ready": False}


def _inspect_triggers(conn, relation, role):
    rows = conn.execute(text("""
        SELECT t.tgname,t.tgtype,t.tgenabled,t.tgfoid::bigint,t.tgnargs,
               t.tgqual IS NULL,t.tgdeferrable,t.tginitdeferred,
               t.tgoldtable,t.tgnewtable
        FROM pg_trigger t WHERE t.tgrelid=to_regclass(:name) AND NOT t.tgisinternal
        ORDER BY t.tgname
    """), {"name": relation}).all()
    expected = []
    for name, kind, function in ((ROW_TRIGGER, 31, role), (TRUNCATE_TRIGGER, 34, "truncate")):
        oid = conn.scalar(text("SELECT to_regprocedure(:name)::oid::bigint"),
                          {"name": SCHEMA + ".online_guard_" + function + "()"})
        expected.append((name, kind, "A", oid, 0, True, False, False, None, None))
    if conn.scalar(text("SELECT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=to_regclass(:name))"),
                   {"name": relation}):
        raise RuntimeError("online_copy_proof_target_rule_changed: " + relation)
    if [tuple(row) for row in rows] != sorted(expected):
        raise RuntimeError("online_copy_proof_target_guard_changed: " + relation)


def inspect_protection(conn):
    """Read exact function/trigger/binding identities; never recreate drift."""
    saved = conn.execute(text(f"SELECT id,binding FROM {STATE}")).mappings().one()
    binding = saved["binding"]
    if saved["id"] != 1 or binding != _binding(conn):
        raise RuntimeError("online_copy_proof_binding_changed")
    for role, body in _bodies(binding).items():
        row = conn.execute(text("""
            SELECT p.prosrc,p.prosecdef,p.proconfig,p.provolatile,p.proretset,
                   p.pronargs,p.pronargdefaults,p.prorettype='trigger'::regtype,
                   l.lanname,p.proowner=n.nspowner
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            JOIN pg_language l ON l.oid=p.prolang
            WHERE p.oid=to_regprocedure(:name)
        """), {"name": SCHEMA + ".online_guard_" + role + "()"}).one_or_none()
        if (row is None or row[0].strip() != body.strip()
                or tuple(row[1:]) != (True, ["search_path=pg_catalog"], "v", False,
                                      0, 0, True, "plpgsql", True)):
            raise RuntimeError("online_copy_proof_function_changed: " + role)
    for name, role in ROLES.items():
        _inspect_triggers(conn, SCHEMA + "." + name, role)
    return binding


@contextmanager
def verified_copy(conn, *, timeout_seconds=30):
    """Close the committed tail and inspect proof, without rereading all rows.

    Keeps all fences through the caller's commit. No archive or runtime readiness
    is implied. It is not legal to bootstrap this proof from a historical scan
    or install it after copying. The ordinary full verifier remains unchanged.
    """
    started = monotonic()
    with migration_step(conn, timeout_seconds):
        relations = (headers.SOURCE, raw.SOURCE, "market.fact_hot_payloads",
                     "market.fact_archive_material_aliases", "market.fact_archive_canonical_dependencies",
                     *(SCHEMA + "." + name for name in ROLES),
                     headers.STATE, raw.STATE, headers.QUEUE, raw.QUEUE, SCHEMA + ".capture", STATE)
        conn.exec_driver_sql("LOCK TABLE " + ",".join(relations) +
                             " IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        inspect_protection(conn)
        header = headers._inspect_progress(conn)
        lookup = raw._inspect(conn)
        if (not header["baseline_complete"] or not header["identity_capture"]
                or not header["identity_history_ready"] or not lookup["baseline_complete"]
                or not lookup["history_ready"]):
            raise RuntimeError("online_copy_proof_copy_incomplete")
        for queue in (headers.QUEUE, raw.QUEUE):
            if conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {queue})")):
                raise RuntimeError("online_copy_proof_pending_capture")
        assert_v1_source_admission(conn, identity_capture=True)
        days = conn.execute(text(f"SELECT storage_day FROM {SCHEMA}.fact_header_partitions "
                                 "ORDER BY storage_day LIMIT 4097")).scalars().all()
        children = conn.execute(text("""
            SELECT n.nspname,c.relname,c.oid::bigint FROM pg_inherits i
            JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE i.inhparent=to_regclass(:parent) ORDER BY c.relname LIMIT 4097
        """), {"parent": SCHEMA + ".fact_versions"}).all()
        expected = {(SCHEMA, "fact_versions_" + day.strftime("%Y%m%d")) for day in days}
        if (len(days) > 4096 or len(children) > 4096
                or {(row[0], row[1]) for row in children} != expected):
            raise RuntimeError("online_copy_proof_partition_catalog_changed")
        saved_leaves = conn.scalar(text(f"SELECT leaf_bindings FROM {STATE} WHERE id=1"))
        if saved_leaves != {schema+"."+name: raw._relation(conn, schema+"."+name)
                            for schema, name, _ in children}:
            raise RuntimeError("online_copy_proof_partition_identity_changed")
        for day in days:
            headers._partition(conn, day, placement=header["placement"], pid=header["_placement_pid"])
            _inspect_triggers(conn, SCHEMA + ".fact_versions_" + day.strftime("%Y%m%d"), "header")
        physical.verify_group(conn, STATE, history=False, saved=header["placement"],
                              pid=header["_placement_pid"])
        report = {"verified_header_rows": header["verified_rows"],
                  "verified_identity_rows": header["verified_rows"],
                  "verified_lookup_rows": lookup["verified_rows"],
                  "verification_method": "protected_exact_copy_pages",
                  "verification_seconds": monotonic()-started,
                  "migration_ready": False, "source_authoritative": True,
                  "physical_placement_configured": True}
        logger.info("fact_header_online_proof_verified | headers=%s lookup=%s duration_seconds=%s",
                    report["verified_header_rows"], report["verified_lookup_rows"],
                    report["verification_seconds"])
        metadata = conn.info
        if metadata.get(_ACTIVE):
            raise RuntimeError("online_copy_proof_nested_switch_context")
        metadata[_ACTIVE] = report
        try:
            yield report
        finally:
            metadata.pop(_ACTIVE, None)


def release_for_switch(conn):
    """Remove temporary guards only inside the live verified transaction.

    The caller must switch in this same transaction. Failure rolls back guard
    removal with the switch. This is not a general permission to mutate a shadow.
    """
    if not conn.info.get(_ACTIVE):
        raise RuntimeError("online_copy_proof_live_switch_context_required")
    inspect_protection(conn)
    children = conn.execute(text("""
        SELECT n.nspname,c.relname FROM pg_inherits i
        JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE i.inhparent=to_regclass(:parent) ORDER BY c.relname
    """), {"parent": SCHEMA + ".fact_versions"}).all()
    quote = conn.dialect.identifier_preparer.quote_identifier
    # Dropping the parent's row trigger drops its PostgreSQL-created clones.
    for name in ROLES:
        relation = SCHEMA + "." + name
        conn.exec_driver_sql(f"DROP TRIGGER {ROW_TRIGGER} ON {relation}")
        conn.exec_driver_sql(f"DROP TRIGGER {TRUNCATE_TRIGGER} ON {relation}")
    for schema, name in children:
        conn.exec_driver_sql(f"DROP TRIGGER {TRUNCATE_TRIGGER} ON {quote(schema)}.{quote(name)}")
    for role in ("header", "identity", "raw", "routing", "partition", "truncate"):
        conn.exec_driver_sql(f"DROP FUNCTION {SCHEMA}.online_guard_{role}()")
    conn.exec_driver_sql(f"DROP TABLE {STATE}")
    logger.info("fact_header_online_proof_guards_released | switch_transaction_required=true")


def current_lookup_proof(conn):
    """Only the live enclosing header proof can supply an exact lookup result."""
    report = conn.info.get(_ACTIVE)
    if report is None:
        return None
    return {"verified_lookup_rows": report["verified_lookup_rows"],
            "verification_seconds": report["verification_seconds"],
            "verification_method": report["verification_method"],
            "migration_ready": False, "source_authoritative": True}

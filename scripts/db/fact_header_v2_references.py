"""Prevalidate the installed v1 payload/archive references against shadow IDs.

Internal one-time migration steps, not a cutover command. Original source FKs
stay in force. No table rename, old-FK removal, ready certificate or runtime
wiring occurs here. The caller commits each preparation/validation separately.
"""
from contextlib import contextmanager
import logging

from sqlalchemy import text

from scripts.db import fact_header_v2_copy as copy
from scripts.db.fact_header_v2_admission import assert_v1_source_admission, _incoming_references
from scripts.db.fact_header_v2_capture import SCHEMA, SOURCE

PARENT = "market.fact_hot_payloads"
STAGED = "qt_header_v2_identity_reference"
TARGET = SCHEMA + ".fact_identities"
logger = logging.getLogger(__name__)


@contextmanager
def _step(conn, timeout_seconds):
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError("fact_header_reference_timeout_out_of_bounds")
    if not conn.in_transaction():
        raise ValueError("fact_header_reference_caller_transaction_required")
    with conn.begin_nested():
        settings = {row["name"]:dict(row) for row in conn.execute(text("""
            SELECT name,current_setting(name) AS original,setting::bigint AS milliseconds
            FROM pg_settings WHERE name IN ('statement_timeout','lock_timeout')
        """)).mappings()}
        for name,limit in (("statement_timeout",timeout_seconds*1000),("lock_timeout",1000)):
            previous=settings[name]["milliseconds"]
            conn.execute(text("SELECT set_config(:name,:value,true)"),
                         {"name":name,"value":str(min(previous,limit) if previous else limit)})
        copy._lock(conn)
        yield
        # On error the savepoint restores settings and partial DDL together.
        for name,item in settings.items():
            conn.execute(text("SELECT set_config(:name,:value,true)"),
                         {"name":name,"value":item["original"]})


def _qualified(conn, relation):
    # Only callers holding an admitted slot reach here; quote the catalog name
    # as one identifier, never interpolate a user-provided SQL expression.
    if not relation.startswith("market."):
        raise RuntimeError("fact_header_reference_relation_outside_market")
    return "market."+conn.dialect.identifier_preparer.quote(relation[len("market."):])


def _inventory(conn):
    state=copy._inspect_progress(conn)
    if not state["identity_capture"]:
        raise RuntimeError("fact_header_reference_identity_capture_required")
    assert_v1_source_admission(conn,identity_capture=True)
    rows=_incoming_references(conn)
    result={row["relation"]:{"relation":row["relation"],"column":row["columns"][0],
                             "original_name":row["conname"]} for row in rows}
    # The private header FK is part of the trusted shadow model. Any other
    # incoming reference must be one of these known staged source slots.
    incoming=conn.execute(text("""
        SELECT n.nspname||'.'||c.relname AS relation,con.conname
        FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE con.contype='f' AND con.confrelid=to_regclass(:target)
          AND NOT (con.conrelid=to_regclass(:headers) OR
              (n.nspname=:private_schema AND con.conparentid IN (
                  SELECT oid FROM pg_constraint WHERE conrelid=to_regclass(:headers)
                    AND contype='f' AND confrelid=to_regclass(:target))))
          LIMIT 8193
    """),{"target":TARGET,"headers":SCHEMA+".fact_versions","private_schema":SCHEMA}).mappings().all()
    if len(incoming)>8192 or any(row["relation"] not in result or row["conname"]!=STAGED
                                  for row in incoming):
        raise RuntimeError("fact_header_reference_unexpected_shadow_dependency")
    return result


def _existing(conn, slot):
    row=conn.execute(text("""
        SELECT con.oid::bigint AS oid,con.contype,con.conparentid::bigint AS parent_oid,
               con.convalidated,con.condeferrable,con.condeferred,
               con.confrelid=to_regclass(:target) AS target_matches,
               con.confmatchtype,con.confupdtype,con.confdeltype,
               ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k.num
                     ORDER BY k.pos) AS columns,
               ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=con.confrelid AND a.attnum=k.num
                     ORDER BY k.pos) AS references
        FROM pg_constraint con WHERE con.conrelid=to_regclass(:relation) AND con.conname=:name
    """),{"target":TARGET,"relation":slot["relation"],"name":STAGED}).mappings().one_or_none()
    if row is None:
        return None
    if (row["contype"]!="f" or not row["target_matches"]
            or row["columns"]!=[slot["column"]] or row["references"]!=["id"]
            or row["condeferrable"] or row["condeferred"]
            or (row["confmatchtype"],row["confupdtype"],row["confdeltype"])!=("s","a","r")):
        raise RuntimeError("fact_header_reference_definition_changed: "+slot["relation"])
    triggers=conn.execute(text("""
        SELECT p.proname,t.tgenabled,t.tgisinternal,t.tgdeferrable,t.tginitdeferred
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE t.tgconstraint=:oid AND n.nspname='pg_catalog' ORDER BY p.proname
    """),{"oid":row["oid"]}).mappings().all()
    expected={"RI_FKey_check_ins","RI_FKey_check_upd"}
    if not row["parent_oid"]:
        expected|={"RI_FKey_restrict_del","RI_FKey_noaction_upd"}
    if ({t["proname"] for t in triggers}!=expected or len(triggers)!=len(expected)
            or any(t["tgenabled"] not in ("O","A") or not t["tgisinternal"]
                   or t["tgdeferrable"] or t["tginitdeferred"] for t in triggers)):
        raise RuntimeError("fact_header_reference_enforcement_changed: "+slot["relation"])
    return dict(row)


def _states(conn, slots):
    states={name:_existing(conn,slot) for name,slot in slots.items()}
    parent=states[PARENT]
    for name,state in states.items():
        if state is None:
            continue
        expected_parent=(parent["oid"] if parent and name.startswith(PARENT+"_") else 0)
        if state["parent_oid"]!=expected_parent:
            raise RuntimeError("fact_header_reference_parent_changed: "+name)
    return states


def _slot(slots, relation):
    if not isinstance(relation,str) or relation not in slots or relation==PARENT:
        raise ValueError("fact_header_reference_known_ordinary_relation_required")
    return slots[relation]


def prepare_reference(conn, *, relation, timeout_seconds=10):
    """Add one unvalidated FK under a brief writer fence, then caller commits."""
    with _step(conn,timeout_seconds):
        conn.exec_driver_sql(f"LOCK TABLE {SOURCE} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        slots=_inventory(conn)
        slot=_slot(slots,relation)
        existing=_states(conn,slots)[relation]
        if existing is None:
            column=conn.dialect.identifier_preparer.quote(slot["column"])
            conn.exec_driver_sql(f"ALTER TABLE {_qualified(conn,relation)} ADD CONSTRAINT {STAGED} "
                                f"FOREIGN KEY({column}) REFERENCES {TARGET}(id) ON DELETE RESTRICT NOT VALID")
            existing=_existing(conn,slot)
            reused=False
        else:
            reused=True
        logger.info("fact_header_reference_prepared | relation=%s reused=%s",relation,reused)
        return {"relation":relation,"constraint_oid":existing["oid"],
                "validated":existing["convalidated"],"reused":reused,"migration_ready":False}


def validate_reference(conn, *, relation, timeout_seconds=60):
    """Validate one prepared ordinary FK without taking the source writer fence."""
    with _step(conn,timeout_seconds):
        slots=_inventory(conn)
        slot=_slot(slots,relation)
        before=_states(conn,slots)[relation]
        if before is None:
            raise RuntimeError("fact_header_reference_not_prepared: "+relation)
        conn.exec_driver_sql(f"ALTER TABLE {_qualified(conn,relation)} VALIDATE CONSTRAINT {STAGED}")
        after=_existing(conn,slot)
        if after["oid"]!=before["oid"] or not after["convalidated"]:
            raise RuntimeError("fact_header_reference_validation_changed")
        logger.info("fact_header_reference_validated | relation=%s",relation)
        return {"relation":relation,"constraint_oid":after["oid"],
                "validated":True,"reused":before["convalidated"],"migration_ready":False}


def inspect_references(conn, *, timeout_seconds=10):
    """Report current per-relation progress, never permission for final switch."""
    with _step(conn,timeout_seconds):
        slots=_inventory(conn)
        states=_states(conn,slots)
        return {"references":[{"relation":name,"prepared":state is not None,
                               "validated":bool(state and state["convalidated"]),
                               "constraint_oid":state["oid"] if state else None}
                              for name,state in sorted(states.items())],
                "references_complete":all(state and state["convalidated"] for state in states.values()),
                "migration_ready":False}


def adopt_payload_references(conn, *, timeout_seconds=10):
    """Attach prevalidated leaf FKs; retain all original source references."""
    with _step(conn,timeout_seconds):
        conn.exec_driver_sql(f"LOCK TABLE {SOURCE},ONLY {PARENT} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        slots=_inventory(conn)
        before=_states(conn,slots)
        if any(state is None or not state["convalidated"] for name,state in before.items() if name!=PARENT):
            raise RuntimeError("fact_header_reference_prevalidation_incomplete")
        reused=before[PARENT] is not None
        if not reused:
            conn.exec_driver_sql(f"ALTER TABLE {PARENT} ADD CONSTRAINT {STAGED} "
                                f"FOREIGN KEY(id) REFERENCES {TARGET}(id) ON DELETE RESTRICT")
        after=_states(conn,slots)
        if (any(state is None or not state["convalidated"] for state in after.values())
                or any(after[name]["oid"]!=state["oid"] for name,state in before.items() if name!=PARENT)):
            raise RuntimeError("fact_header_reference_adoption_did_not_reuse_validation")
        logger.info("fact_header_payload_references_adopted | reused=%s",reused)
        return {"references_complete":True,"reused":reused,"migration_ready":False}

"""Preserving, bounded copy of the installed immutable raw archive lookup.

Uses the already prepared fixed SSD/HDD header migration. No generic mover,
source deletion, switch, runtime wiring or readiness certificate. The source
remains authoritative; a later admitted cutover must close its writer boundary.
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import MetaData, text, tuple_
from sqlalchemy.dialects.postgresql import insert

from portal.backend.db import Base
from scripts.db import fact_header_v2_copy as headers, fact_header_v2_placement as physical
from scripts.db.fact_header_v2_admission import (
    _columns, _constraints, _secondary_indexes, V1_IMMUTABLE_GUARD_BODY,
)
from scripts.db.fact_header_v2_capture import SCHEMA
from scripts.db.fact_header_v2_references import _step

NAME = "raw_archive_record_mappings"
SOURCE = "market." + NAME
TARGET = SCHEMA + "." + NAME
QUEUE_NAME = "pending_raw_mapping_keys"
QUEUE = SCHEMA + "." + QUEUE_NAME
STATE_NAME = "raw_mapping_copy_progress"
STATE = SCHEMA + "." + STATE_NAME
KEYS = ("raw_record_id", "manifest_id")
COLUMNS = tuple(column.name for column in Base.metadata.tables[SOURCE].columns)
logger = logging.getLogger(__name__)


def _table():
    metadata = MetaData()
    for key, table in Base.metadata.tables.items():
        if key != SOURCE:
            table.to_metadata(metadata)
    return Base.metadata.tables[SOURCE].to_metadata(
        metadata, schema=SCHEMA,
        referred_schema_fn=lambda table,schema,constraint,referred_schema: referred_schema)


def _relation(conn, relation):
    row = conn.execute(text("""
        SELECT oid::bigint,relkind,relpersistence,relowner::bigint,relacl,
               relrowsecurity,relforcerowsecurity,relreplident,reloptions
        FROM pg_class WHERE oid=to_regclass(:relation)
    """), {"relation":relation}).one()
    return list(row)


def _triggers(conn):
    return [list(row) for row in conn.execute(text("""
        SELECT t.oid::bigint,t.tgname,t.tgtype,t.tgenabled,t.tgisinternal,
               t.tgdeferrable,t.tginitdeferred,t.tgnargs,t.tgargs::text,
               pg_get_expr(t.tgqual,t.tgrelid),t.tgoldtable,t.tgnewtable,
               t.tgconstraint::bigint,p.oid::bigint,p.pronamespace::bigint,
               p.proowner::bigint,p.prosrc,p.prosecdef,p.proconfig,
               p.prorettype::regtype::text,p.pronargs,l.lanname,p.provolatile,
               p.proretset,p.pronargdefaults
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_language l ON l.oid=p.prolang
        WHERE t.tgrelid=to_regclass(:source) ORDER BY t.tgname LIMIT 32
    """), {"source":SOURCE})]


def _admit_source(conn):
    """Only the existing simple immutable table is qualified for this copy."""
    source, target = _relation(conn,SOURCE), _relation(conn,TARGET)
    if (source[1:3] != ["r","p"] or source[3] != target[3]
            or source[4:] != [None,False,False,"d",None]
            or target[4:] != [None,False,False,"d",None]):
        raise RuntimeError("raw_mapping_copy_source_ownership_or_policy_changed")
    if (_columns(conn,SOURCE) != _columns(conn,TARGET)
            or _constraints(conn,SOURCE) != _constraints(conn,TARGET)
            or _secondary_indexes(conn,SOURCE,include_constraints=True)
                != _secondary_indexes(conn,TARGET,include_constraints=True)):
        raise RuntimeError("raw_mapping_copy_source_definition_changed")
    rows = conn.execute(text("""
        SELECT t.tgname,t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,
               t.tgnargs,t.tgqual,t.tgoldtable,t.tgnewtable,
               p.prosrc,p.prosecdef,p.proconfig,p.proowner::bigint,
               p.prorettype::regtype::text,p.pronargs,l.lanname,p.provolatile,
               p.proretset,p.pronargdefaults,
               n.nspname,p.proname
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_language l ON l.oid=p.prolang
        WHERE t.tgrelid=to_regclass(:source) AND NOT t.tgisinternal
    """), {"source":SOURCE}).all()
    if len(rows)!=1:
        raise RuntimeError("raw_mapping_copy_source_trigger_set_changed")
    row=rows[0]
    if (row[:2]!=("trg_reject_mutation_raw_archive_record_mappings",27)
            or row[2] not in ("O","A") or tuple(row[3:9])!=(False,False,0,None,None,None)
            or row[9].strip()!=V1_IMMUTABLE_GUARD_BODY.strip()
            or tuple(row[10:])!=(False,None,source[3],"trigger",0,"plpgsql","v",False,0,
                                "market","reject_immutable_mutation")):
        raise RuntimeError("raw_mapping_copy_source_guard_changed")
    _dependencies(conn)


def _dependencies(conn):
    if conn.scalar(text("""
        SELECT EXISTS(SELECT 1 FROM pg_inherits
                       WHERE inhrelid=to_regclass(:source) OR inhparent=to_regclass(:source))
            OR EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=to_regclass(:source))
            OR EXISTS(SELECT 1 FROM pg_constraint WHERE contype='f' AND confrelid=to_regclass(:source))
            OR EXISTS(SELECT 1 FROM pg_depend d
                WHERE d.refclassid='pg_class'::regclass AND d.refobjid=to_regclass(:source)
                  AND d.classid IN ('pg_rewrite'::regclass,'pg_proc'::regclass))
            OR EXISTS(SELECT 1 FROM pg_depend d JOIN pg_class c ON c.oid=to_regclass(:source)
                JOIN pg_type t ON t.oid=c.reltype WHERE d.classid='pg_proc'::regclass
                  AND d.refclassid='pg_type'::regclass AND d.refobjid IN(c.reltype,t.typarray))
            OR EXISTS(SELECT 1 FROM pg_publication_rel WHERE prrelid=to_regclass(:source))
            OR EXISTS(SELECT 1 FROM pg_publication WHERE puballtables)
            OR EXISTS(SELECT 1 FROM pg_publication_namespace WHERE pnnspid='market'::regnamespace)
            OR EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgrelid=to_regclass(:source)
                AND t.tgisinternal AND (t.tgenabled NOT IN ('O','A') OR NOT EXISTS(
                    SELECT 1 FROM pg_constraint con WHERE con.oid=t.tgconstraint
                    AND con.conrelid=t.tgrelid AND con.contype='f' AND con.convalidated)))
    """), {"source":SOURCE}):
        raise RuntimeError("raw_mapping_copy_source_dependency_changed")


def _binding(conn):
    return json.loads(json.dumps({
        "source":headers._shape(conn,NAME,schema="market"),
        "source_properties":_relation(conn,SOURCE),
        "target":headers._shape(conn,NAME),
        "target_properties":_relation(conn,TARGET),
        "queue":headers._shape(conn,QUEUE_NAME),
        "state":headers._shape(conn,STATE_NAME),
        "triggers":_triggers(conn),
    }))


def _inspect(conn):
    header_state=headers._inspect_progress(conn)
    if header_state["placement"] is None:
        raise RuntimeError("raw_mapping_copy_physical_placement_required")
    state=dict(conn.execute(text(f"SELECT * FROM {STATE} WHERE id=1")).mappings().one())
    if (state["binding"]!=_binding(conn) or state["placement"]!=header_state["placement"]
            or state["header_state_oid"]!=conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                                                     {"name":headers.STATE})):
        raise RuntimeError("raw_mapping_copy_bound_layout_changed")
    _dependencies(conn)
    pid=header_state["_placement_pid"]
    for relation,history in ((SOURCE,False),(TARGET,True),(QUEUE,False),(STATE,False)):
        physical.verify_group(conn,relation,history=history,saved=state["placement"],pid=pid)
    return state


def prepare_copy(conn, *, timeout_seconds=30):
    """Stage capture and the HDD target atomically at a nonwaiting writer fence."""
    with _step(conn,timeout_seconds), physical.tablespace(conn,""):
        header_state=headers._inspect_progress(conn)
        placement=header_state["placement"]
        if placement is None:
            raise RuntimeError("raw_mapping_copy_physical_placement_required")
        if conn.scalar(text("SELECT to_regclass(:name)"),{"name":STATE}) is not None:
            return _report(conn,_inspect(conn),verified=0,reused=True)
        conn.exec_driver_sql(f"LOCK TABLE {SOURCE} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        for relation in (TARGET,QUEUE):
            if conn.scalar(text("SELECT to_regclass(:name)"),{"name":relation}) is not None:
                raise RuntimeError("raw_mapping_copy_unregistered_shadow")
        with physical.tablespace(conn,placement["history_name"]):
            _table().create(conn)
        _admit_source(conn)
        conn.exec_driver_sql(f"""
            CREATE TABLE {QUEUE}(raw_record_id varchar(80),manifest_id varchar(128),
                                  PRIMARY KEY(raw_record_id,manifest_id))
        """)
        conn.exec_driver_sql(f"""
            CREATE TABLE {STATE}(
                id integer PRIMARY KEY CHECK(id=1),header_state_oid oid NOT NULL,
                high_raw varchar(80),high_manifest varchar(128),
                after_raw varchar(80),after_manifest varchar(128),
                baseline_complete boolean NOT NULL,
                verified_rows bigint NOT NULL DEFAULT 0 CHECK(verified_rows>=0),
                binding jsonb NOT NULL,placement jsonb NOT NULL,
                CHECK((high_raw IS NULL)=(high_manifest IS NULL)),
                CHECK((after_raw IS NULL)=(after_manifest IS NULL)))
        """)
        source_oid=conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),{"name":SOURCE})
        conn.exec_driver_sql(f"""
            CREATE FUNCTION {SCHEMA}.capture_raw_mapping_insert() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$
            BEGIN
                IF TG_RELID <> {source_oid}::oid THEN
                    RAISE EXCEPTION 'raw_mapping_copy_source_changed';
                END IF;
                INSERT INTO {QUEUE}(raw_record_id,manifest_id)
                    VALUES(NEW.raw_record_id,NEW.manifest_id)
                    ON CONFLICT(raw_record_id,manifest_id) DO NOTHING;
                RETURN NULL;
            END;
            $qt$
        """)
        conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {SCHEMA}.capture_raw_mapping_insert() FROM PUBLIC")
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_qt_raw_mapping_v2_capture AFTER INSERT ON {SOURCE}
            FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.capture_raw_mapping_insert()
        """)
        # Reuse the migration's admitted statement-level immutable guard.
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_qt_raw_mapping_v2_reject_change
            BEFORE UPDATE OR DELETE OR TRUNCATE ON {SOURCE}
            FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_fact_source_change()
        """)
        for trigger in ("trg_qt_raw_mapping_v2_capture","trg_qt_raw_mapping_v2_reject_change"):
            conn.exec_driver_sql(f"ALTER TABLE {SOURCE} ENABLE ALWAYS TRIGGER {trigger}")
        high=conn.execute(text(f"""
            SELECT raw_record_id,manifest_id FROM {SOURCE}
            ORDER BY raw_record_id DESC,manifest_id DESC LIMIT 1
        """)).one_or_none()
        conn.execute(text(f"""
            INSERT INTO {STATE}(id,header_state_oid,high_raw,high_manifest,
                                baseline_complete,binding,placement)
            VALUES(1,to_regclass(:headers),:raw,:manifest,:empty,
                   CAST(:binding AS jsonb),CAST(:placement AS jsonb))
        """), {"headers":headers.STATE,"raw":high[0] if high else None,
               "manifest":high[1] if high else None,"empty":high is None,
               "binding":json.dumps(_binding(conn)),"placement":json.dumps(placement)})
        logger.info("raw_mapping_v2_copy_prepared | source=%s",SOURCE)
        return _report(conn,_inspect(conn),verified=0,reused=False)


def copy_page(conn, *, page_rows=128, timeout_seconds=30):
    """Verify every copied field before retiring exact keys and advancing."""
    if type(page_rows) is not int or not 1<=page_rows<=4096:
        raise ValueError("raw_mapping_copy_page_rows_out_of_bounds")
    with _step(conn,timeout_seconds):
        state=_inspect(conn)
        queued=[]
        if not state["baseline_complete"]:
            predicate="(raw_record_id,manifest_id)<=(:high_raw,:high_manifest)"
            if state["after_raw"] is not None:
                predicate+=" AND (raw_record_id,manifest_id)>(:after_raw,:after_manifest)"
            rows=[dict(row) for row in conn.execute(text(f"""
                SELECT {",".join(COLUMNS)} FROM {SOURCE} WHERE {predicate}
                ORDER BY raw_record_id,manifest_id LIMIT :limit
            """),{**state,"limit":page_rows}).mappings()]
        else:
            queued=conn.execute(text(f"""
                SELECT raw_record_id,manifest_id FROM {QUEUE}
                ORDER BY raw_record_id,manifest_id LIMIT :limit FOR UPDATE
            """),{"limit":page_rows}).all()
            original=Base.metadata.tables[SOURCE]
            keys=tuple_(original.c.raw_record_id,original.c.manifest_id)
            rows=[dict(row) for row in conn.execute(original.select().where(keys.in_(queued))).mappings()] if queued else []
            if {tuple(row[key] for key in KEYS) for row in rows}!=set(queued):
                raise RuntimeError("raw_mapping_copy_captured_source_missing")
        if rows:
            target=_table()
            copied_keys=[tuple(row[key] for key in KEYS) for row in rows]
            conn.execute(insert(target).on_conflict_do_nothing(),rows)
            actual={tuple(row[key] for key in KEYS):dict(row) for row in conn.execute(
                target.select().where(tuple_(target.c.raw_record_id,target.c.manifest_id).in_(copied_keys))).mappings()}
            if any(actual.get(tuple(row[key] for key in KEYS))!=row for row in rows):
                raise RuntimeError("raw_mapping_copy_full_row_mismatch")
            # Independent IN lists would incorrectly remove cross-product keys.
            params={f"{name}{index}":value for index,key in enumerate(copied_keys)
                    for name,value in zip(("raw","manifest"),key)}
            values=",".join(f"(:raw{index},:manifest{index})" for index in range(len(copied_keys)))
            conn.execute(text(f"DELETE FROM {QUEUE} WHERE (raw_record_id,manifest_id) IN ({values})"),params)
        if not state["baseline_complete"]:
            if rows:
                conn.execute(text(f"UPDATE {STATE} SET after_raw=:raw,after_manifest=:manifest WHERE id=1"),
                             {"raw":rows[-1]["raw_record_id"],"manifest":rows[-1]["manifest_id"]})
            else:
                conn.exec_driver_sql(f"UPDATE {STATE} SET baseline_complete=true WHERE id=1")
        conn.execute(text(f"UPDATE {STATE} SET verified_rows=verified_rows+:count WHERE id=1"),
                     {"count":len(rows)})
        state=_inspect(conn)
        logger.info("raw_mapping_v2_copy_page_verified | rows=%s baseline_complete=%s",
                    len(rows),state["baseline_complete"])
        return _report(conn,state,verified=len(rows),reused=True)


def _report(conn,state,*,verified,reused):
    pending=conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {QUEUE})"))
    return {"schema_version":"qt.raw_mapping_shadow_copy.v1",
            "verified_page_rows":verified,"baseline_complete":state["baseline_complete"],
            "capture_pending":pending,
            "caught_up_at_observation":state["baseline_complete"] and not pending,
            "source_authoritative":True,"migration_ready":False,"reused":reused}

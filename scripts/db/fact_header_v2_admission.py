"""Admit the known tiered-v1 source before the preserving header upgrade.

Internal catalog check, not cutover authorization. Compare the source against a
trusted prepared shadow built from the reviewed v2 models. Its caller must
first verify shadow/capture identity and definition, and repeat this admission
under the eventual final writer fence. No SQL writes or repairs are performed.
The frozen v1 function bodies below come from installed source 487d18c6.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from portal.backend.db.fact_storage_schema import _view_signature
from scripts.db.fact_header_v2_capture import SCHEMA, SOURCE, inspect_capture

logger = logging.getLogger(__name__)
MAX_CATALOG_ROWS = 8192

V1_FACT_GUARD_BODY = """
                DECLARE
                    series_fact_type text;
                    series_contract_version text;
                BEGIN
                    SELECT fact_type, contract_version
                    INTO series_fact_type, series_contract_version
                    FROM market.series
                    WHERE id = NEW.series_id;
                    IF series_fact_type IS NULL THEN
                        RAISE EXCEPTION
                            'canonical_fact_invalid: unknown series_id=%',
                            NEW.series_id;
                    END IF;
                    IF NEW.fact_type <> series_fact_type
                       OR NEW.payload_schema_id <> series_contract_version THEN
                        RAISE EXCEPTION
                            'canonical_fact_invalid: series/schema mismatch series_id=% series_fact_type=% series_contract_version=% fact_type=% payload_schema_id=%',
                            NEW.series_id,
                            series_fact_type,
                            series_contract_version,
                            NEW.fact_type,
                            NEW.payload_schema_id;
                    END IF;
                    RETURN NEW;
                END;
                """

V1_IMMUTABLE_GUARD_BODY = """
                BEGIN
                    RAISE EXCEPTION 'immutable market-data relation %.% rejects %',
                        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
                END;
                """

V1_HOT_PAYLOAD_VALIDATION_BODY = """
DECLARE
    revision_row market.fact_versions%ROWTYPE;
    partition_state text;
BEGIN
    SELECT * INTO revision_row FROM market.fact_versions WHERE id = NEW.id;
    IF NOT FOUND OR
       (NEW.storage_day, NEW.series_id, NEW.payload_schema_id, NEW.observation_time)
       IS DISTINCT FROM
       (revision_row.storage_day, revision_row.series_id, revision_row.payload_schema_id, revision_row.observation_time)
    THEN
        RAISE EXCEPTION 'fact_hot_payload_identity_mismatch: fact_version_id=%', NEW.id;
    END IF;
    SELECT state INTO partition_state FROM market.fact_retention_partitions
    WHERE storage_day = NEW.storage_day FOR SHARE;
    IF partition_state IS DISTINCT FROM 'open' THEN
        RAISE EXCEPTION 'fact_hot_partition_not_open: storage_day=% fact_version_id=%',
            NEW.storage_day, NEW.id;
    END IF;
    RETURN NEW;
END;
"""

V1_FACT_PAYLOAD_REQUIRED_BODY = """
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM market.fact_hot_payloads
        WHERE storage_day = NEW.storage_day AND id = NEW.id
    ) THEN
        RAISE EXCEPTION 'fact_hot_payload_missing: fact_version_id=% storage_day=%', NEW.id, NEW.storage_day;
    END IF;
    RETURN NULL;
END;
"""

V1_COLD_FACT_READ_BODY = """
BEGIN
    RAISE EXCEPTION 'canonical_fact_cold_read_required: fact_version_id=% field=% use the tier-aware repository',
        fact_id, field_name;
END;
"""

V1_FACT_ROWS_VIEW_SELECT = """
    SELECT versions.*,
           CASE WHEN hot.id IS NULL THEN market.cold_fact_read_required(versions.id, 'payload')
                ELSE hot.payload END AS payload,
           CASE WHEN hot.id IS NULL THEN market.cold_fact_read_required(versions.id, 'provenance')
                ELSE hot.provenance END AS provenance,
           CASE WHEN hot.id IS NULL THEN market.cold_fact_read_required(versions.id, 'quality')
                ELSE hot.quality END AS quality
    FROM market.fact_versions AS versions
    LEFT JOIN market.fact_hot_payloads AS hot
      ON hot.storage_day = versions.storage_day AND hot.id = versions.id
"""


def _rows(conn, sql, params=None):
    rows=[dict(row) for row in conn.execute(text(sql),params or {}).mappings()]
    if len(rows)>MAX_CATALOG_ROWS:
        raise RuntimeError("fact_header_source_catalog_limit")
    return rows


def _columns(conn, relation):
    return _rows(conn,"""
        SELECT a.attname,format_type(a.atttypid,a.atttypmod) AS type,
               a.attnotnull,a.attcollation::bigint,a.attidentity,a.attgenerated,
               pg_get_expr(d.adbin,d.adrelid) AS default_expression
        FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE a.attrelid=to_regclass(:relation) AND a.attnum>0 AND NOT a.attisdropped
        ORDER BY a.attnum LIMIT 8193
    """,{"relation":relation})


def _constraints(conn, relation):
    return _rows(conn,"""
        SELECT conname,contype,convalidated,condeferrable,condeferred,
               pg_get_constraintdef(oid) AS definition,
               ARRAY(SELECT a.attname::text FROM unnest(conkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=conrelid AND a.attnum=k.num ORDER BY k.pos) AS columns
        FROM pg_constraint WHERE conrelid=to_regclass(:relation)
        ORDER BY conname LIMIT 8193
    """,{"relation":relation})


def _secondary_indexes(conn, relation):
    return _rows(conn,"""
        SELECT c.relname,i.indisvalid,i.indisready,i.indisunique,i.indnkeyatts,i.indnatts,
               i.indoption::text,i.indclass::text,i.indcollation::text,am.amname,
               pg_get_expr(i.indpred,i.indrelid) AS predicate,
               pg_get_expr(i.indexprs,i.indrelid) AS expressions,
               ARRAY(SELECT pg_get_indexdef(i.indexrelid,p,true)
                     FROM generate_series(1,i.indnkeyatts) p ORDER BY p) AS keys
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=to_regclass(:relation)
          AND NOT EXISTS(SELECT 1 FROM pg_constraint con WHERE con.conindid=i.indexrelid)
        ORDER BY c.relname LIMIT 8193
    """,{"relation":relation})


def _assert_guard_functions(conn, owner):
    expected={
        "assert_fact_version_valid()":(V1_FACT_GUARD_BODY,0,"trigger"),
        "reject_immutable_mutation()":(V1_IMMUTABLE_GUARD_BODY,0,"trigger"),
        "assert_fact_hot_payload_valid()":(V1_HOT_PAYLOAD_VALIDATION_BODY,0,"trigger"),
        "require_fact_hot_payload()":(V1_FACT_PAYLOAD_REQUIRED_BODY,0,"trigger"),
        "cold_fact_read_required(text,text)":(V1_COLD_FACT_READ_BODY,2,"jsonb"),
    }
    for signature,(body,args,returns) in expected.items():
        row=conn.execute(text("""
            SELECT p.prosrc,p.provolatile,p.prosecdef,p.proretset,p.pronargs,p.pronargdefaults,
                   p.prorettype::regtype::text AS returns,p.proconfig,l.lanname,p.proowner::bigint AS owner
            FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
            WHERE p.oid=to_regprocedure(:name)
        """),{"name":"market."+signature}).mappings().one_or_none()
        if row is None or (
            row["prosrc"].strip()!=body.strip() or row["provolatile"]!="v" or row["prosecdef"]
            or row["proretset"] or row["pronargs"]!=args or row["pronargdefaults"]!=0
            or row["returns"]!=returns or row["proconfig"] is not None
            or row["lanname"]!="plpgsql" or row["owner"]!=owner):
            raise RuntimeError("fact_header_source_function_changed: "+signature)


def _assert_source_triggers(conn):
    rows=_rows(conn,"""
        SELECT t.tgname,t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,
               t.tgnargs,t.tgqual IS NULL AS unfiltered,
               t.tgoldtable,t.tgnewtable,n.nspname||'.'||p.proname||'()' AS function
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE t.tgrelid='market.fact_versions'::regclass AND NOT t.tgisinternal
        ORDER BY t.tgname LIMIT 8193
    """)
    expected={
        "trg_assert_fact_version_valid":(7,False,"market.assert_fact_version_valid()","OA"),
        "trg_reject_mutation_fact_versions":(27,False,"market.reject_immutable_mutation()","OA"),
        "trg_require_fact_hot_payload":(5,True,"market.require_fact_hot_payload()","A"),
        "trg_qt_header_v2_capture":(5,False,SCHEMA+".capture_fact_insert()","A"),
        "trg_qt_header_v2_reject_change":(58,False,SCHEMA+".reject_fact_source_change()","A"),
    }
    if {row["tgname"] for row in rows}!=set(expected):
        raise RuntimeError("fact_header_source_trigger_set_changed")
    for row in rows:
        kind,deferred,function,enabled=expected[row["tgname"]]
        if (row["tgtype"]!=kind or row["tgenabled"] not in enabled
                or row["tgdeferrable"]!=deferred or row["tginitdeferred"]!=deferred
                or row["function"]!=function or row["tgnargs"]!=0 or not row["unfiltered"]
                or row["tgoldtable"] is not None or row["tgnewtable"] is not None):
            raise RuntimeError("fact_header_source_trigger_changed: "+row["tgname"])



def _assert_payload_guards(conn, children):
    parent=conn.execute(text("""
        SELECT oid::bigint AS oid,relkind,relpersistence,pg_get_partkeydef(oid) AS key
        FROM pg_class WHERE oid='market.fact_hot_payloads'::regclass
    """)).mappings().one()
    if (parent["relkind"],parent["relpersistence"],parent["key"])!=("p","p","RANGE (storage_day)"):
        raise RuntimeError("fact_header_source_payload_parent_changed")
    primary=[row for row in _constraints(conn,"market.fact_hot_payloads") if row["contype"]=="p"]
    if (len(primary)!=1 or primary[0]["columns"]!=["storage_day","id"]
            or not primary[0]["convalidated"] or primary[0]["condeferrable"]):
        raise RuntimeError("fact_header_source_payload_primary_key_changed")
    relation_ids=[parent["oid"],*(row["oid"] for row in children)]
    for name,kind,function,enabled in (
        ("trg_assert_fact_hot_payload_valid",7,"assert_fact_hot_payload_valid","A"),
        ("trg_reject_mutation_fact_hot_payloads",27,"reject_immutable_mutation","OA"),
    ):
        triggers=_rows(conn,"""
            SELECT t.oid::bigint AS oid,t.tgrelid::bigint AS relation,
                   t.tgparentid::bigint AS parent,t.tgtype,t.tgenabled,t.tgdeferrable,
                   t.tginitdeferred,t.tgnargs,t.tgqual IS NULL AS unfiltered,
                   t.tgoldtable,t.tgnewtable,t.tgfoid=to_regprocedure(:function) AS matches
            FROM pg_trigger t WHERE t.tgrelid=ANY(CAST(:relations AS oid[]))
              AND t.tgname=:name AND NOT t.tgisinternal ORDER BY t.tgrelid LIMIT 8193
        """,{"relations":relation_ids,"name":name,"function":"market."+function+"()"})
        roots=[row for row in triggers if row["relation"]==parent["oid"]]
        if len(roots)!=1 or len(triggers)!=len(relation_ids):
            raise RuntimeError("fact_header_source_payload_trigger_missing")
        for row in triggers:
            if (row["parent"]!=(0 if row["relation"]==parent["oid"] else roots[0]["oid"])
                    or row["tgtype"]!=kind or row["tgenabled"] not in enabled
                    or row["tgdeferrable"] or row["tginitdeferred"] or row["tgnargs"]!=0
                    or not row["unfiltered"] or row["tgoldtable"] is not None
                    or row["tgnewtable"] is not None or not row["matches"]):
                raise RuntimeError("fact_header_source_payload_trigger_changed")


def _incoming_references(conn):
    children=_rows(conn,"""
        SELECT c.oid::bigint AS oid,n.nspname||'.'||c.relname AS relation,c.relkind
        FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE i.inhparent='market.fact_hot_payloads'::regclass ORDER BY c.oid LIMIT 8193
    """)
    if any(row["relkind"]!="r" or not row["relation"].startswith("market.fact_hot_payloads_")
           for row in children):
        raise RuntimeError("fact_header_source_payload_partition_changed")
    _assert_payload_guards(conn,children)
    parent_owners={
        "market.fact_hot_payloads":"id",
        "market.fact_archive_material_aliases":"fact_version_id",
        "market.fact_archive_canonical_dependencies":"fact_version_id",
    }
    child_owners={row["relation"]:"id" for row in children}
    rows=_rows(conn,"""
        SELECT con.oid::bigint AS oid,con.conparentid::bigint AS parent_oid,
               con.conname,n.nspname||'.'||c.relname AS relation,con.convalidated,
               con.condeferrable,con.condeferred,con.confmatchtype,con.confupdtype,con.confdeltype,
               ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k.num ORDER BY k.pos) AS columns,
               ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY k(num,pos)
                     JOIN pg_attribute a ON a.attrelid=con.confrelid AND a.attnum=k.num ORDER BY k.pos) AS references
        FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE con.contype='f' AND con.confrelid='market.fact_versions'::regclass
        ORDER BY c.oid,con.oid LIMIT 8193
    """)
    roots=[row for row in rows if not row["parent_oid"]]
    if len(roots)!=len(parent_owners) or {row["relation"] for row in roots}!=set(parent_owners):
        raise RuntimeError("fact_header_source_incoming_reference_set_changed")
    root=next(row["oid"] for row in roots if row["relation"]=="market.fact_hot_payloads")
    seen=[]
    for row in rows:
        expected=({**parent_owners,**child_owners}).get(row["relation"])
        if (expected is None or row["columns"]!=[expected] or row["references"]!=["id"]
                or not row["convalidated"] or row["condeferrable"] or row["condeferred"]
                or row["confmatchtype"]!="s" or row["confupdtype"]!="a" or row["confdeltype"]!="r"
                or row["parent_oid"]!=(root if row["relation"] in child_owners else 0)):
            raise RuntimeError("fact_header_source_incoming_reference_changed: "+row["relation"])
        seen.append(row["relation"])
    if len(seen)!=len(set(seen)) or set(seen)!=set(parent_owners)|set(child_owners):
        raise RuntimeError("fact_header_source_incoming_reference_coverage")
    return rows


def assert_v1_source_admission(conn):
    """Check the known source contract; never authorize placement or switching."""
    context=inspect_capture(conn)
    if conn.scalar(text("""
        SELECT to_regclass('market.fact_identities') IS NOT NULL
            OR to_regclass('market.fact_header_partitions') IS NOT NULL
            OR to_regclass('market.fact_header_series_days') IS NOT NULL
            OR EXISTS(SELECT 1 FROM market.fact_storage_state
                      WHERE layout_version='market.fact_storage_tiers.v2')
    """)):
        raise RuntimeError("fact_header_source_active_v2_layout_present")
    shadow=SCHEMA+".fact_versions"
    source=conn.execute(text("""
        SELECT c.relowner::bigint AS owner,c.relacl,c.relrowsecurity,c.relforcerowsecurity,
               c.relreplident,c.reltype::bigint AS row_type,t.typarray::bigint AS array_type,
               c.relowner=(SELECT relowner FROM pg_class WHERE oid=to_regclass(:shadow)) AS same_owner,
               (SELECT relacl FROM pg_class WHERE oid=to_regclass(:shadow)) AS shadow_acl
        FROM pg_class c JOIN pg_type t ON t.oid=c.reltype WHERE c.oid='market.fact_versions'::regclass
    """),{"shadow":shadow}).mappings().one()
    if (not source["same_owner"] or source["relacl"] is not None or source["shadow_acl"] is not None
            or source["relrowsecurity"]
            or source["relforcerowsecurity"] or source["relreplident"]!="d"):
        raise RuntimeError("fact_header_source_ownership_or_policy_changed")
    if conn.scalar(text("""
        SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent='market.fact_versions'::regclass
                       OR inhrelid='market.fact_versions'::regclass)
            OR EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class='market.fact_versions'::regclass)
    """)):
        raise RuntimeError("fact_header_source_inheritance_or_rules_changed")
    source_columns=_columns(conn,SOURCE)
    if source_columns!=_columns(conn,shadow):
        raise RuntimeError("fact_header_source_columns_or_defaults_changed")
    original,expected=_constraints(conn,SOURCE),_constraints(conn,shadow)
    for kind in ("c","f"):
        left=[row for row in original if row["contype"]==kind]
        right=[row for row in expected if row["contype"]==kind and row["conname"]!="fk_market_fact_identity_day"]
        if left!=right or any(not row["convalidated"] or row["condeferrable"] for row in left):
            raise RuntimeError("fact_header_source_constraints_changed")
    unique=[row for row in original if row["contype"] in ("p","u")]
    if (len(unique)!=2 or {(row["contype"],tuple(row["columns"])) for row in unique}
            !={("p",("id",)),("u",("series_id","observation_key","revision"))}
            or any(not row["convalidated"] or row["condeferrable"] or row["condeferred"] for row in unique)
            or any(row["contype"] not in ("c","f","p","u","t") for row in original)):
        raise RuntimeError("fact_header_source_global_identity_constraints_changed")
    invalid_unique=conn.scalar(text("""
        SELECT EXISTS(SELECT 1 FROM pg_constraint con JOIN pg_index i ON i.indexrelid=con.conindid
            WHERE con.conrelid='market.fact_versions'::regclass AND con.contype IN('p','u')
              AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indisunique OR NOT i.indimmediate))
    """))
    if invalid_unique or _secondary_indexes(conn,SOURCE)!=_secondary_indexes(conn,shadow):
        raise RuntimeError("fact_header_source_indexes_changed")
    _assert_guard_functions(conn,source["owner"])
    _assert_source_triggers(conn)
    references=_incoming_references(conn)
    views=_rows(conn,"""
        SELECT DISTINCT n.nspname||'.'||c.relname AS relation,c.relkind
        FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
        JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE d.classid='pg_rewrite'::regclass AND d.refclassid='pg_class'::regclass
          AND d.refobjid IN('market.fact_versions'::regclass,to_regclass('market.fact_rows'))
          AND c.oid<>d.refobjid ORDER BY relation LIMIT 8193
    """)
    if views!=[{"relation":"market.fact_rows","relkind":"v"}]:
        raise RuntimeError("fact_header_source_dependent_views_changed")
    view=conn.execute(text("""
        SELECT pg_get_viewdef(oid,true) AS definition,relowner::bigint AS owner,relacl,reloptions,
               reltype::bigint AS row_type,(SELECT typarray::bigint FROM pg_type WHERE oid=reltype) AS array_type
        FROM pg_class WHERE oid='market.fact_rows'::regclass
    """)).mappings().one()
    expected_view=V1_FACT_ROWS_VIEW_SELECT.replace(
        "versions.*",", ".join("versions."+row["attname"] for row in source_columns))
    if (view["owner"]!=source["owner"] or view["relacl"] is not None or view["reloptions"] is not None
            or _view_signature(view["definition"])!=_view_signature(expected_view)):
        raise RuntimeError("fact_header_source_view_contract_changed")
    dependent_functions=_rows(conn,"""
        SELECT DISTINCT p.oid::regprocedure::text AS function
        FROM pg_depend d JOIN pg_proc p ON d.classid='pg_proc'::regclass AND p.oid=d.objid
        WHERE (d.refclassid='pg_class'::regclass
               AND d.refobjid IN('market.fact_versions'::regclass,'market.fact_rows'::regclass))
           OR (d.refclassid='pg_type'::regclass
               AND d.refobjid IN(:row_type,:array_type,:view_type,:view_array))
        ORDER BY function LIMIT 8193
    """,{"row_type":source["row_type"],"array_type":source["array_type"],
         "view_type":view["row_type"],"view_array":view["array_type"]})
    if dependent_functions:
        raise RuntimeError("fact_header_source_dependent_functions_changed")
    if conn.scalar(text("""
        SELECT EXISTS(SELECT 1 FROM pg_depend WHERE classid='pg_class'::regclass
            AND objid='market.fact_commit_seq'::regclass AND refclassid='pg_class'::regclass
            AND deptype IN('a','i'))
    """)):
        raise RuntimeError("fact_header_source_commit_sequence_must_be_standalone")
    logger.info("fact_header_v1_source_admitted | source_oid=%s incoming_references=%s",
                context["source_oid"],len(references))
    return {"schema_version":"qt.fact_header_source_admission.v1",**context,
            "source_schema_admitted":True,"migration_ready":False,
            "incoming_reference_count":len(references)}

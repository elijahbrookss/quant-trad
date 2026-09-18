"""Transactional insert capture for the one-time tiered-v1 header upgrade.

Internal migration primitive only: no CLI, source copy, cutover or readiness
certificate. The eventual orchestrator must admit the full source contract.
All DDL belongs to the caller's transaction; original records are never changed.
"""
from __future__ import annotations

import logging

from sqlalchemy import inspect, text

SCHEMA = "qt_fact_header_cutover_v2"
SOURCE = "market.fact_versions"
QUEUE = SCHEMA + ".pending_fact_ids"
STATE = SCHEMA + ".capture"
LOCK = "quant-trad:fact-header-cutover:v2"
logger = logging.getLogger(__name__)

_REJECT_BODY = """
BEGIN
    RAISE EXCEPTION USING MESSAGE = 'fact_header_capture_source_is_immutable: operation=' || TG_OP;
END;
"""


def _capture_body(source_oid):
    return f"""
BEGIN
    IF TG_RELID <> {source_oid}::oid THEN
        RAISE EXCEPTION 'fact_header_capture_source_changed';
    END IF;
    INSERT INTO {QUEUE}(id) VALUES (NEW.id) ON CONFLICT (id) DO NOTHING;
    RETURN NULL;
END;
"""


def _context(conn):
    row = conn.execute(text("""
        SELECT c.oid::bigint AS source_oid, c.relkind, c.relpersistence,
               d.oid::bigint AS database_oid,
               (pg_control_system()).system_identifier::text AS cluster_id,
               current_setting('server_version_num')::int AS server_version
        FROM pg_class c CROSS JOIN pg_database d
        WHERE c.oid=to_regclass('market.fact_versions') AND d.datname=current_database()
    """)).mappings().one_or_none()
    if row is None or row["relkind"] != "r" or row["relpersistence"] != "p":
        raise RuntimeError("fact_header_capture_requires_logged_v1_source")
    if row["server_version"] // 10000 != 15:
        raise RuntimeError("fact_header_capture_postgresql_version_not_qualified")
    inspector = inspect(conn)
    columns = {item["name"]:item for item in inspector.get_columns("fact_versions", schema="market")}
    identity = columns.get("id")
    if (identity is None or identity["nullable"] or str(identity["type"]) not in ("TEXT", "VARCHAR(64)")
            or inspector.get_pk_constraint("fact_versions",schema="market")["constrained_columns"] != ["id"]):
        raise RuntimeError("fact_header_capture_source_id_incompatible")
    ready = conn.execute(text("""
        SELECT state FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'
    """)).scalar_one_or_none()
    if ready != "ready":
        raise RuntimeError("fact_header_capture_v1_certificate_required")
    return {name:row[name] for name in ("source_oid","database_oid","cluster_id")}


def inspect_capture(conn):
    """Verify this capture stage; this cannot admit or activate a migration."""
    context = _context(conn)
    inspector = inspect(conn)
    expected_columns = {
        "pending_fact_ids":{"id":"TEXT"},
        "capture":{"id":"INTEGER","source_oid":"OID","database_oid":"OID",
                   "cluster_id":"TEXT","queue_oid":"OID","prepared_at":"TIMESTAMP WITH TIME ZONE"},
    }
    for name,expected in expected_columns.items():
        if conn.scalar(text("SELECT to_regclass(:name)"), {"name":SCHEMA+"."+name}) is None:
            raise RuntimeError("fact_header_capture_layout_incomplete")
        columns = inspector.get_columns(name,schema=SCHEMA)
        observed = {item["name"]:str(item["type"].compile(dialect=conn.dialect)) for item in columns}
        if observed != expected or any(item["nullable"] for item in columns):
            raise RuntimeError("fact_header_capture_layout_changed")
        if inspector.get_pk_constraint(name,schema=SCHEMA)["constrained_columns"] != ["id"]:
            raise RuntimeError("fact_header_capture_primary_key_changed")
        kind = conn.execute(text("""
            SELECT relkind,relpersistence FROM pg_class WHERE oid=to_regclass(:name)
        """), {"name":SCHEMA+"."+name}).one()
        if tuple(kind) != ("r","p"):
            raise RuntimeError("fact_header_capture_durable_tables_required")
    saved = conn.execute(text(f"""
        SELECT id,source_oid::bigint,database_oid::bigint,cluster_id,queue_oid::bigint FROM {STATE}
    """)).mappings().one()
    queue_oid = conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),{"name":QUEUE})
    if (saved["id"] != 1 or saved["queue_oid"] != queue_oid
            or any(saved[name] != value for name,value in context.items())):
        raise RuntimeError("fact_header_capture_identity_changed")
    expected_functions = {
        "capture_fact_insert": (_capture_body(context["source_oid"]),True),
        "reject_fact_source_change": (_REJECT_BODY,False),
    }
    for name,(body,definer) in expected_functions.items():
        row = conn.execute(text("""
            SELECT p.prosrc,p.prosecdef,p.proconfig,p.prorettype='trigger'::regtype AS returns_trigger,
                   l.lanname,p.pronargs,p.proowner=n.nspowner AS schema_owned
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            JOIN pg_language l ON l.oid=p.prolang
            WHERE p.oid=to_regprocedure(:signature)
        """), {"signature":SCHEMA+"."+name+"()"}).mappings().one_or_none()
        if (row is None or row["prosrc"].strip()!=body.strip() or row["prosecdef"]!=definer
                or row["proconfig"]!=["search_path=pg_catalog"] or not row["returns_trigger"]
                or row["lanname"]!="plpgsql" or row["pronargs"]!=0 or not row["schema_owned"]):
            raise RuntimeError("fact_header_capture_function_changed")
    expected_triggers = {
        "trg_qt_header_v2_capture": ("capture_fact_insert",5),
        "trg_qt_header_v2_reject_change": ("reject_fact_source_change",58),
    }
    for name,(function,kind) in expected_triggers.items():
        row = conn.execute(text("""
            SELECT t.tgtype,t.tgenabled,t.tgfoid=to_regprocedure(:function) AS function_matches,
                   t.tgnargs,t.tgqual IS NULL AS unfiltered
            FROM pg_trigger t WHERE t.tgrelid=:source AND t.tgname=:name AND NOT t.tgisinternal
        """), {"function":SCHEMA+"."+function+"()","source":context["source_oid"],"name":name}).mappings().one_or_none()
        if (row is None or row["tgtype"]!=kind or row["tgenabled"]!="A"
                or not row["function_matches"] or row["tgnargs"]!=0 or not row["unfiltered"]):
            raise RuntimeError("fact_header_capture_trigger_changed")
    return {"schema_version":"qt.fact_header_capture.v1",**context,
            "capture_active":True,"migration_ready":False}


def install_capture(conn):
    """Stage insert capture atomically; retry only reuses an intact capture."""
    if not conn.in_transaction():
        raise ValueError("fact_header_capture_caller_transaction_required")
    with conn.begin_nested():
        return _install_capture(conn)


def _install_capture(conn):
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),{"name":LOCK}):
        raise RuntimeError("fact_header_capture_migration_busy")
    # Require a short writer-free boundary; a busy source causes a retry.
    # New writers cannot pass until trigger and queue commit together; reads continue.
    conn.exec_driver_sql("LOCK TABLE market.fact_versions IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    context = _context(conn)
    if conn.scalar(text("SELECT to_regnamespace(:name)"),{"name":SCHEMA}) is not None:
        return {**inspect_capture(conn),"reused":True}
    conn.exec_driver_sql(f"CREATE SCHEMA {SCHEMA}")
    conn.exec_driver_sql(f"REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC")
    conn.exec_driver_sql(f"CREATE TABLE {QUEUE}(id text PRIMARY KEY)")
    conn.exec_driver_sql(f"""
        CREATE TABLE {STATE}(
            id integer PRIMARY KEY CHECK(id=1),source_oid oid NOT NULL,
            database_oid oid NOT NULL,cluster_id text NOT NULL,queue_oid oid NOT NULL,
            prepared_at timestamptz NOT NULL DEFAULT clock_timestamp())
    """)
    conn.execute(text(f"""
        INSERT INTO {STATE}(id,source_oid,database_oid,cluster_id,queue_oid)
        VALUES(1,:source_oid,:database_oid,:cluster_id,to_regclass(:queue))
    """),{**context,"queue":QUEUE})
    for name,body,security in (
        ("capture_fact_insert",_capture_body(context["source_oid"]),"DEFINER"),
        ("reject_fact_source_change",_REJECT_BODY,"INVOKER"),
    ):
        sql = (f"CREATE FUNCTION {SCHEMA}.{name}() RETURNS trigger LANGUAGE plpgsql "
               f"SECURITY {security} SET search_path=pg_catalog AS $qt$" + body + "$qt$")
        conn.exec_driver_sql(sql)
        conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {SCHEMA}.{name}() FROM PUBLIC")
    conn.exec_driver_sql(f"""
        CREATE TRIGGER trg_qt_header_v2_capture AFTER INSERT ON {SOURCE}
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.capture_fact_insert()
    """)
    conn.exec_driver_sql(f"""
        CREATE TRIGGER trg_qt_header_v2_reject_change BEFORE UPDATE OR DELETE OR TRUNCATE ON {SOURCE}
        FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_fact_source_change()
    """)
    for name in ("trg_qt_header_v2_capture","trg_qt_header_v2_reject_change"):
        conn.exec_driver_sql(f"ALTER TABLE {SOURCE} ENABLE ALWAYS TRIGGER {name}")
    result = inspect_capture(conn)
    logger.info("fact_header_v2_capture_staged | source_oid=%s database_oid=%s",
                context["source_oid"],context["database_oid"])
    return {**result,"reused":False}

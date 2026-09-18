"""Transactional insert capture for the one-time tiered-v1 header upgrade.

Internal migration primitive only: no CLI, source copy, cutover or readiness
certificate. The eventual orchestrator must admit the full source contract.
All DDL belongs to the caller's transaction; original records are never changed.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import logging
from time import monotonic

from sqlalchemy import event, inspect, text

SCHEMA = "qt_fact_header_cutover_v2"
SOURCE = "market.fact_versions"
QUEUE = SCHEMA + ".pending_fact_ids"
STATE = SCHEMA + ".capture"
LOCK = "quant-trad:fact-header-cutover:v2"
logger = logging.getLogger(__name__)


@contextmanager
def migration_step(conn, timeout_seconds=30):
    """Bound one fixed migration step and reuse capture's original 24-hour clock.

    The caller owns commit. This is not a cutover or a duration qualification.
    A rejected/expired step leaves the original source and capture intact.
    """
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise ValueError("fact_header_migration_timeout_out_of_bounds")
    if not conn.in_transaction():
        raise ValueError("fact_header_copy_caller_transaction_required")
    if conn.get_isolation_level() != "READ COMMITTED":
        raise ValueError("fact_header_copy_read_committed_required")
    deadline = monotonic() + timeout_seconds
    # Keep this metadata reference through disconnect cleanup; accessing conn.info
    # after invalidation could otherwise attempt to reconnect inside rollback.
    metadata = conn.info
    with conn.begin_nested():
        settings = {row["name"]: dict(row) for row in conn.execute(text("""
            SELECT name,current_setting(name) AS original,setting::bigint AS milliseconds
            FROM pg_settings WHERE name IN ('statement_timeout','lock_timeout')
        """)).mappings()}
        previous = settings["statement_timeout"]["milliseconds"]
        if previous:
            deadline = min(deadline, monotonic() + previous / 1000)

        def remaining():
            milliseconds = int((deadline - monotonic()) * 1000)
            if milliseconds <= 0:
                raise RuntimeError("fact_header_migration_step_timeout")
            return milliseconds

        guard_key = "qt.fact_header_migration_deadlines.v2"
        guard = metadata.get(guard_key)
        owns_guard = guard is None
        if owns_guard:
            guard = {"remaining": [], "configuring": False}
            metadata[guard_key] = guard

        def bound_statement(connection, cursor, statement, parameters, context, executemany):
            if guard["configuring"] or statement.lstrip().upper().startswith("ROLLBACK TO SAVEPOINT "):
                return  # An expired outer step must still let a nested step unwind.
            milliseconds = min(check() for check in guard["remaining"])
            guard["configuring"] = True
            try:
                # One listener for this connection: nested steps add deadlines,
                # not listeners that recursively trigger each other's SQL.
                # Keep the normal connection path for disconnect invalidation.
                connection.exec_driver_sql("""
                    SELECT set_config('statement_timeout',
                        LEAST(%s, CASE WHEN setting::bigint=0 THEN %s
                                       ELSE setting::bigint END)::text, true)
                    FROM pg_settings WHERE name='statement_timeout'
                """, (milliseconds, milliseconds)).close()
            finally:
                guard["configuring"] = False

        guard["remaining"].append(remaining)
        if owns_guard:
            event.listen(conn, "before_cursor_execute", bound_statement)
        try:
            original_lock = settings["lock_timeout"]["milliseconds"]
            conn.execute(text("SELECT set_config('lock_timeout',:value,true)"),
                         {"value": str(min(original_lock, 1000) if original_lock else 1000)})
            if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                               {"name": LOCK}):
                raise RuntimeError("fact_header_copy_migration_busy")
            if conn.scalar(text("SELECT to_regclass(:name)"), {"name": STATE}) is not None:
                seconds = conn.scalar(text(f"""
                    SELECT EXTRACT(EPOCH FROM
                        prepared_at + interval '24 hours' - clock_timestamp())::double precision
                    FROM {STATE} WHERE id=1
                """))
                if seconds is None or seconds > 86400:
                    raise RuntimeError("fact_header_migration_start_time_invalid")
                if seconds <= 0:
                    raise RuntimeError("fact_header_migration_attempt_expired")
                deadline = min(deadline, monotonic() + seconds)
            yield
            remaining()
        finally:
            # Remove before savepoint rollback or returning this connection;
            # an expired guard must never prevent recovery or leak to other work.
            guard["remaining"].remove(remaining)
            if owns_guard:
                event.remove(conn, "before_cursor_execute", bound_statement)
                del metadata[guard_key]
        for name, item in settings.items():
            conn.execute(text("SELECT set_config(:name,:value,true)"),
                         {"name": name, "value": item["original"]})


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
        SELECT id,source_oid::bigint,database_oid::bigint,cluster_id,queue_oid::bigint,prepared_at FROM {STATE}
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
            "capture_active":True,"migration_ready":False,
            "started_at":saved["prepared_at"].isoformat(),
            "deadline_at":(saved["prepared_at"]+timedelta(hours=24)).isoformat()}


def install_capture(conn, *, timeout_seconds=30):
    """Stage insert capture atomically; retry only reuses an intact capture."""
    with migration_step(conn, timeout_seconds):
        return _install_capture(conn)


def _install_capture(conn):
    started_at = conn.scalar(text("SELECT clock_timestamp()"))
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
        INSERT INTO {STATE}(id,source_oid,database_oid,cluster_id,queue_oid,prepared_at)
        VALUES(1,:source_oid,:database_oid,:cluster_id,to_regclass(:queue),:started_at)
    """),{**context,"queue":QUEUE,"started_at":started_at})
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


def _identity_body(source_oid, target_oid):
    return f"""
BEGIN
    IF TG_RELID <> {source_oid}::oid
       OR '{SCHEMA}.fact_identities'::regclass::oid <> {target_oid}::oid THEN
        RAISE EXCEPTION 'fact_header_identity_capture_relation_changed';
    END IF;
    INSERT INTO {SCHEMA}.fact_identities(id,storage_day,series_id,observation_key,revision)
        VALUES(NEW.id,NEW.storage_day,NEW.series_id,NEW.observation_key,NEW.revision)
        ON CONFLICT(id) DO NOTHING;
    IF NOT EXISTS(
        SELECT 1 FROM {SCHEMA}.fact_identities i WHERE i.id=NEW.id
          AND (i.storage_day,i.series_id,i.observation_key,i.revision)
              IS NOT DISTINCT FROM
              (NEW.storage_day,NEW.series_id,NEW.observation_key,NEW.revision)
    ) THEN
        RAISE EXCEPTION 'fact_header_identity_capture_content_mismatch';
    END IF;
    RETURN NULL;
END;
"""


def inspect_identity_capture(conn):
    """Check the fixed prepared shadow mirror, not completeness or cutover."""
    context=inspect_capture(conn)
    target_oid=conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                           {"name":SCHEMA+".fact_identities"})
    if target_oid is None:
        raise RuntimeError("fact_header_identity_capture_target_missing")
    row=conn.execute(text("""
        SELECT p.prosrc,p.prosecdef,p.proconfig,p.provolatile,p.proretset,
               p.pronargs,p.pronargdefaults,p.prorettype='trigger'::regtype AS returns_trigger,
               l.lanname,p.proowner=n.nspowner AS owned,
               t.tgtype,t.tgenabled,t.tgnargs,t.tgqual IS NULL AS unfiltered,
               t.tgdeferrable,t.tginitdeferred,t.tgoldtable,t.tgnewtable
        FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
        WHERE t.tgrelid=:source AND t.tgname='trg_qt_header_v2_capture_identity'
          AND NOT t.tgisinternal AND p.oid=to_regprocedure(:function)
    """),{"source":context["source_oid"],
          "function":SCHEMA+".capture_fact_identity()"}).mappings().one_or_none()
    if (row is None or row["prosrc"].strip()!=_identity_body(context["source_oid"],target_oid).strip()
            or not row["prosecdef"] or row["proconfig"]!=["search_path=pg_catalog"]
            or row["provolatile"]!="v" or row["proretset"] or row["pronargs"]!=0
            or row["pronargdefaults"]!=0 or not row["returns_trigger"]
            or row["lanname"]!="plpgsql" or not row["owned"] or row["tgtype"]!=5
            or row["tgenabled"]!="A" or row["tgnargs"]!=0 or not row["unfiltered"]
            or row["tgdeferrable"] or row["tginitdeferred"]
            or row["tgoldtable"] is not None or row["tgnewtable"] is not None):
        raise RuntimeError("fact_header_identity_capture_changed")
    return {"identity_capture_active":True,"target_oid":target_oid,"migration_ready":False}


def install_identity_capture(conn, *, timeout_seconds=30):
    """Install the existing identity mirror within the original attempt budget."""
    with migration_step(conn, timeout_seconds):
        return _install_identity_capture(conn)


def _install_identity_capture(conn):
    """Internal post-baseline step, after verifying the trusted shadow copy.

    The caller owns activation's writer fence and savepoint. This primitive
    never upgrades an existing mirror silently or certifies identity coverage.
    """
    if not conn.in_transaction():
        raise ValueError("fact_header_identity_capture_caller_transaction_required")
    if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                       {"name":LOCK}):
        raise RuntimeError("fact_header_capture_migration_busy")
    conn.exec_driver_sql("LOCK TABLE market.fact_versions IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    context=inspect_capture(conn)
    target_oid=conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                           {"name":SCHEMA+".fact_identities"})
    if target_oid is None:
        raise RuntimeError("fact_header_identity_capture_target_missing")
    body=_identity_body(context["source_oid"],target_oid)
    conn.exec_driver_sql(f"CREATE FUNCTION {SCHEMA}.capture_fact_identity() RETURNS trigger "
                        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$"
                        +body+"$qt$")
    conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {SCHEMA}.capture_fact_identity() FROM PUBLIC")
    conn.exec_driver_sql(f"""
        CREATE TRIGGER trg_qt_header_v2_capture_identity AFTER INSERT ON {SOURCE}
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.capture_fact_identity()
    """)
    conn.exec_driver_sql(f"ALTER TABLE {SOURCE} ENABLE ALWAYS TRIGGER trg_qt_header_v2_capture_identity")
    return inspect_identity_capture(conn)

"""Conservative per-series observation ranges for dated-header selection.

Bounds expand in the inserting transaction. They are an index of canonical
headers, not an independent timeline, identity, or retention authority.
"""
from __future__ import annotations

from sqlalchemy import inspect, text

SERIES_DAY_TABLE = "fact_header_series_days"
SERIES_DAY_TABLES = (SERIES_DAY_TABLE,)

EXTEND_SERIES_DAY_BODY = """
BEGIN
    INSERT INTO market.fact_header_series_days AS bounds
        (series_id, storage_day, min_observation_time, max_observation_time)
    VALUES (NEW.series_id, NEW.storage_day, NEW.observation_time, NEW.observation_time)
    ON CONFLICT (series_id, storage_day) DO UPDATE
    SET min_observation_time = LEAST(bounds.min_observation_time, EXCLUDED.min_observation_time),
        max_observation_time = GREATEST(bounds.max_observation_time, EXCLUDED.max_observation_time)
    WHERE bounds.min_observation_time > EXCLUDED.min_observation_time
       OR bounds.max_observation_time < EXCLUDED.max_observation_time;
    RETURN NULL;
END;
"""

GUARD_SERIES_DAY_BODY = """
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION 'canonical_series_day_removal_forbidden: operation=%', TG_OP;
    END IF;
    IF (NEW.series_id, NEW.storage_day) IS DISTINCT FROM (OLD.series_id, OLD.storage_day)
       OR NEW.min_observation_time > OLD.min_observation_time
       OR NEW.max_observation_time < OLD.max_observation_time THEN
        RAISE EXCEPTION 'canonical_series_day_narrowing_forbidden: series_id=% storage_day=%',
            OLD.series_id, OLD.storage_day;
    END IF;
    RETURN NEW;
END;
"""

READ_SERIES_DAY_BODY = """
DECLARE
    selected_days date[];
BEGIN
    IF requested_series IS NULL OR requested_series <= 0
       OR range_start IS NULL OR range_end IS NULL OR range_end <= range_start THEN
        RAISE EXCEPTION 'canonical_series_day_request_invalid';
    END IF;
    SELECT array_agg(candidate.storage_day ORDER BY candidate.storage_day) INTO selected_days
    FROM (
        SELECT storage_day FROM market.fact_header_series_days
        WHERE series_id=requested_series AND min_observation_time < range_end
          AND max_observation_time >= range_start
        ORDER BY storage_day LIMIT 4097
    ) AS candidate;
    IF cardinality(selected_days) > 4096 THEN
        RAISE EXCEPTION 'canonical_series_day_horizon_exceeded';
    END IF;
    IF selected_days IS NULL THEN
        RETURN;
    END IF;
    RETURN QUERY EXECUTE format(
        'SELECT * FROM market.fact_versions WHERE storage_day = ANY(%L::date[]) '
        'AND series_id=$1 AND observation_time >= $2 AND observation_time < $3',
        selected_days
    ) USING requested_series,range_start,range_end;
END;
"""
READ_SERIES_DAY_SIGNATURE = "market.read_fact_headers_in_range(bigint,timestamptz,timestamptz)"


_FUNCTIONS = (
    ("extend_fact_header_series_day", EXTEND_SERIES_DAY_BODY),
    ("guard_fact_header_series_day", GUARD_SERIES_DAY_BODY),
)
_TRIGGERS = (
    ("fact_versions", "trg_extend_fact_header_series_day", "extend_fact_header_series_day",
     "AFTER INSERT", "ROW", 5),
    (SERIES_DAY_TABLE, "trg_guard_fact_header_series_day", "guard_fact_header_series_day",
     "BEFORE UPDATE OR DELETE", "ROW", 27),
    (SERIES_DAY_TABLE, "trg_guard_fact_header_series_day_truncate", "guard_fact_header_series_day",
     "BEFORE TRUNCATE", "STATEMENT", 34),
)


def install_fact_series_day_functions(conn) -> None:
    """Clean-schema or explicit cutover installation; never a runtime backfill."""
    for name, body in _FUNCTIONS:
        conn.execute(text(
            f"CREATE OR REPLACE FUNCTION market.{name}() RETURNS trigger "
            "LANGUAGE plpgsql AS $qt$" + body + "$qt$"
        ))
    # STABLE fixes all internal SELECTs to the calling statement's snapshot.
    # Dynamic planning receives literal, typed storage dates rather than an
    # unknown array InitPlan. Only server-produced dates enter quoted SQL;
    # caller arguments remain bound through USING.
    conn.execute(text(
        "CREATE OR REPLACE FUNCTION market.read_fact_headers_in_range("
        "requested_series bigint,range_start timestamptz,range_end timestamptz) "
        "RETURNS SETOF market.fact_versions LANGUAGE plpgsql STABLE SECURITY INVOKER "
        "SET search_path=pg_catalog AS $qt$" + READ_SERIES_DAY_BODY + "$qt$"
    ))
    for table, trigger, function, timing, scope, _ in _TRIGGERS:
        conn.execute(text(
            f"CREATE TRIGGER {trigger} {timing} ON market.{table} "
            f"FOR EACH {scope} EXECUTE FUNCTION market.{function}()"
        ))
        conn.execute(text(f"ALTER TABLE market.{table} ENABLE ALWAYS TRIGGER {trigger}"))


def assert_fact_series_day_contract(conn) -> None:
    if conn.execute(text(
        "SELECT to_regclass('market.fact_header_series_days')"
    )).scalar_one_or_none() is None:
        raise RuntimeError("canonical_series_day_directory_missing: explicit cutover required")
    relation = conn.execute(text(
        "SELECT relkind,relpersistence FROM pg_class "
        "WHERE oid='market.fact_header_series_days'::regclass"
    )).one()
    if tuple(relation) != ("r", "p"):
        raise RuntimeError("canonical_series_day_relation_incompatible: durable ordinary table required")
    inspector = inspect(conn)
    columns = {
        column["name"]: (str(column["type"]), column["nullable"])
        for column in inspector.get_columns(SERIES_DAY_TABLE, schema="market")
    }
    if columns != {
        "series_id": ("BIGINT", False), "storage_day": ("DATE", False),
        "min_observation_time": ("TIMESTAMP", False),
        "max_observation_time": ("TIMESTAMP", False),
    }:
        raise RuntimeError("canonical_series_day_columns_incompatible: explicit repair required")
    for column in inspector.get_columns(SERIES_DAY_TABLE, schema="market"):
        if column["name"].endswith("observation_time") and not column["type"].timezone:
            raise RuntimeError("canonical_series_day_timezone_incompatible: explicit repair required")
    pk = inspector.get_pk_constraint(SERIES_DAY_TABLE, schema="market")
    if tuple(pk.get("constrained_columns") or ()) != ("series_id", "storage_day"):
        raise RuntimeError("canonical_series_day_primary_key_incompatible: explicit repair required")
    check = conn.execute(text("""
        SELECT convalidated AND NOT condeferrable
               AND pg_get_constraintdef(oid) =
                   'CHECK ((min_observation_time <= max_observation_time))'
        FROM pg_constraint WHERE conrelid='market.fact_header_series_days'::regclass
          AND conname='ck_market_fact_series_day_bounds'
    """)).scalar_one_or_none()
    if check is not True:
        raise RuntimeError("canonical_series_day_bounds_constraint_invalid: explicit repair required")
    reader = conn.execute(text("""
        SELECT p.prosrc,p.provolatile,p.prosecdef,p.proretset,
               p.prorettype='market.fact_versions'::regtype,
               p.proconfig,p.pronargdefaults,l.lanname
        FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
        WHERE p.oid=to_regprocedure(:signature)
    """), {"signature": READ_SERIES_DAY_SIGNATURE}).one_or_none()
    if reader is None or tuple(reader) != (
        READ_SERIES_DAY_BODY,"s",False,True,True,["search_path=pg_catalog"],0,"plpgsql",
    ):
        raise RuntimeError("canonical_series_day_reader_incompatible: explicit repair required")
    for name, body in _FUNCTIONS:
        actual = conn.execute(text(
            "SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='market' AND p.proname=:name"
        ), {"name": name}).scalars().all()
        if actual != [body]:
            raise RuntimeError(f"canonical_series_day_function_incompatible: function={name}")
    for table, trigger, function, _, _, kind in _TRIGGERS:
        actual = conn.execute(text(
            "SELECT t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,n.nspname,p.proname "
            "FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
            "JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE t.tgrelid=to_regclass(:relation) AND t.tgname=:trigger"
        ), {"relation": "market." + table, "trigger": trigger}).one_or_none()
        if actual is None or tuple(actual) != (kind, "A", False, False, "market", function):
            raise RuntimeError(f"canonical_series_day_trigger_incompatible: trigger={trigger}")

    incompatible_children = conn.execute(text("""
        SELECT child.relname
        FROM pg_inherits i JOIN pg_class child ON child.oid=i.inhrelid
        JOIN pg_trigger parent ON parent.tgrelid=i.inhparent
          AND parent.tgname='trg_extend_fact_header_series_day'
        LEFT JOIN pg_trigger clone ON clone.tgrelid=i.inhrelid
          AND clone.tgname=parent.tgname
        WHERE i.inhparent='market.fact_versions'::regclass
          AND (clone.oid IS NULL OR clone.tgparentid<>parent.oid
               OR clone.tgtype<>5 OR clone.tgenabled<>'A'
               OR clone.tgfoid<>parent.tgfoid OR clone.tgdeferrable OR clone.tginitdeferred)
        ORDER BY child.relname LIMIT 1
    """)).scalar_one_or_none()
    if incompatible_children is not None:
        raise RuntimeError(
            f"canonical_series_day_child_capture_incompatible: relation={incompatible_children}"
        )

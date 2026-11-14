"""Explicit transition of deployed archive references to one canonical identity."""

from sqlalchemy import text

from .cold_history_models import COLD_HISTORY_LAYOUT_VERSION


ARCHIVE_FACT_REFERENCE_TABLES = (
    "fact_archive_canonical_dependencies",
    "fact_archive_material_aliases",
    "fact_archive_prepared_canonical_dependencies",
    "fact_archive_prepared_material_aliases",
)
_FUNCTION = "market.require_hot_or_cold_fact"
_TRIGGER = "trg_require_hot_or_cold_fact"
_BODY = f"""
DECLARE
    hot_day date;
    hot_series bigint;
    hot_key text;
    hot_revision integer;
    has_hot_header boolean;
BEGIN
    SELECT storage_day,series_id,observation_key,revision
      INTO hot_day,hot_series,hot_key,hot_revision FROM market.fact_identities WHERE id=NEW.fact_version_id;
    IF FOUND THEN
        -- Use the registered date to prune partitions, including the sealed
        -- legacy range during the fenced transition.
        EXECUTE 'SELECT EXISTS (
            SELECT FROM market.fact_versions
            WHERE id=$1 AND storage_day=$2 AND (series_id,observation_key,revision)=($3,$4,$5)
        )' INTO has_hot_header USING NEW.fact_version_id,hot_day,hot_series,hot_key,hot_revision;
        IF has_hot_header THEN
            RETURN NULL;
        END IF;
    END IF;
    IF EXISTS (
        SELECT FROM market.fact_storage_state
        WHERE layout_version='{COLD_HISTORY_LAYOUT_VERSION}' AND state='ready' AND completed_at IS NOT NULL
    ) AND EXISTS (
        SELECT FROM market.cold_fact_locators AS locator
        JOIN market.cold_fact_pages AS page ON page.id=locator.page_id
        WHERE locator.id=NEW.fact_version_id
          AND (page.staged_storage_day IS NULL OR EXISTS (
              SELECT FROM market.cold_fact_retired_days AS retired
              WHERE retired.storage_day=page.staged_storage_day
          ))
    ) THEN
        RETURN NULL;
    END IF;
    RAISE foreign_key_violation USING MESSAGE=format(
        'canonical_archive_fact_reference_missing: relation=%s fact_version_id=%s',
        TG_TABLE_NAME, NEW.fact_version_id
    );
END;
"""


def _fact_reference_foreign_keys(conn, table):
    return conn.execute(text("""
        SELECT c.conname, c.convalidated, c.condeferrable, c.condeferred, c.conparentid::bigint,
               c.confdeltype, c.confupdtype, c.confmatchtype,
               c.conkey=ARRAY[source.attnum],
               c.confrelid='market.fact_identities'::regclass AND c.confkey=ARRAY[target.attnum]
        FROM pg_constraint AS c
        JOIN pg_attribute AS source ON source.attrelid=c.conrelid AND source.attname='fact_version_id'
        LEFT JOIN pg_attribute AS target ON target.attrelid=c.confrelid AND target.attname='id'
        WHERE c.conrelid=to_regclass(:relation) AND c.contype='f'
          AND source.attnum=ANY(c.conkey)
    """), {"relation": f"market.{table}"}).all()


def assert_cold_dependency_bindings(conn) -> None:
    function = conn.execute(text("""
        SELECT p.prosrc,p.provolatile,p.prosecdef,p.proretset,p.proisstrict,
               p.prorettype='trigger'::regtype,p.proconfig,p.pronargs,p.prokind,l.lanname
        FROM pg_proc AS p JOIN pg_language AS l ON l.oid=p.prolang
        WHERE p.oid=to_regprocedure(:signature)
    """), {"signature": _FUNCTION + "()"}).one_or_none()
    if function is None or tuple(function) != (
        _BODY, "v", False, False, False, True, ["search_path=pg_catalog"], 0, "f", "plpgsql",
    ):
        raise RuntimeError("cold_fact_dependency_function_invalid")
    for table in ARCHIVE_FACT_REFERENCE_TABLES:
        trigger = conn.execute(text("""
            SELECT t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,
                   t.tgisinternal,t.tgqual IS NULL,t.tgnargs,t.tgfoid=to_regprocedure(:signature),
                   t.tgattr::text,t.tgoldtable IS NULL,t.tgnewtable IS NULL,
                   c.contype,c.conrelid=t.tgrelid
            FROM pg_trigger AS t JOIN pg_constraint AS c ON c.oid=t.tgconstraint
            WHERE t.tgrelid=to_regclass(:relation) AND t.tgname=:trigger
        """), {"signature": _FUNCTION + "()", "relation": f"market.{table}", "trigger": _TRIGGER}).one_or_none()
        if trigger is None or tuple(trigger) != (
            21, "A", False, False, False, True, 0, True, "", True, True, "t", True,
        ) or _fact_reference_foreign_keys(conn, table):
            raise RuntimeError(f"cold_fact_dependency_binding_invalid: relation=market.{table}")


def prepare_cold_dependency_bindings(conn) -> None:
    """Replace only the four validated identity FKs in a fenced transaction.

    The caller proves existing reference coverage through the source-to-target
    audit and keeps the writer fence through activation. This operation neither
    removes an existing identity nor publishes a staged page.
    """
    if not conn.in_transaction() or conn.get_execution_options().get("isolation_level") == "AUTOCOMMIT":
        raise RuntimeError("cold_fact_dependency_transition_requires_transaction")
    for table in ARCHIVE_FACT_REFERENCE_TABLES:
        conn.exec_driver_sql(f"LOCK TABLE market.{table} IN ACCESS EXCLUSIVE MODE NOWAIT")
    constraints = {}
    for table in ARCHIVE_FACT_REFERENCE_TABLES:
        rows = _fact_reference_foreign_keys(conn, table)
        if len(rows) != 1 or tuple(rows[0][1:]) != (True, False, False, 0, "r", "a", "s", True, True):
            raise RuntimeError(f"cold_fact_dependency_source_binding_invalid: relation=market.{table}")
        constraints[table] = conn.dialect.identifier_preparer.quote_identifier(rows[0][0])
    conn.execute(text(
        f"CREATE FUNCTION {_FUNCTION}() RETURNS trigger LANGUAGE plpgsql "
        f"SECURITY INVOKER SET search_path=pg_catalog AS $qt${_BODY}$qt$"
    ))
    for table, constraint in constraints.items():
        conn.exec_driver_sql(
            f"CREATE CONSTRAINT TRIGGER {_TRIGGER} AFTER INSERT OR UPDATE ON market.{table} "
            f"NOT DEFERRABLE FOR EACH ROW EXECUTE FUNCTION {_FUNCTION}()"
        )
        conn.exec_driver_sql(f"ALTER TABLE market.{table} ENABLE ALWAYS TRIGGER {_TRIGGER}")
        conn.exec_driver_sql(f"ALTER TABLE market.{table} DROP CONSTRAINT {constraint}")
    assert_cold_dependency_bindings(conn)

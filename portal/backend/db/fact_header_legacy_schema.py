"""Fixed sealed legacy range, installed only by an explicit operator cutover.

The binding is empty on a clean daily layout. Reading it never adopts a table,
changes a boundary or repairs missing history. Physical movement remains owned
by the separate complete storage inventory and operator admission.
"""
from __future__ import annotations

from sqlalchemy import inspect, text

LEGACY_TABLE = "fact_header_legacy"
LEGACY_RELATION = "market.fact_versions_legacy"
LEGACY_END_SIGNATURE = "market.fact_header_legacy_end_day()"

LEGACY_END_BODY = """
DECLARE
    bound_day date;
    bound_oid bigint;
BEGIN
    SELECT end_day,relation_oid INTO bound_day,bound_oid
    FROM market.fact_header_legacy WHERE id=1;
    IF NOT FOUND THEN
        IF to_regclass('market.fact_versions_legacy') IS NOT NULL THEN
            RAISE EXCEPTION 'fact_header_legacy_unregistered';
        END IF;
        RETURN NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid
        WHERE c.oid=to_regclass('market.fact_versions_legacy')
          AND c.oid::bigint=bound_oid AND c.relkind='r' AND c.relpersistence='p'
          AND i.inhparent='market.fact_versions'::regclass
          AND pg_get_expr(c.relpartbound,c.oid)=format(
              'FOR VALUES FROM (MINVALUE) TO (%L)',bound_day::text)
    ) THEN
        RAISE EXCEPTION 'fact_header_legacy_binding_changed: relation_oid=% end_day=%',
            bound_oid,bound_day;
    END IF;
    RETURN bound_day;
END;
"""

REJECT_LEGACY_MUTATION_BODY = """
BEGIN
    RAISE EXCEPTION 'fact_header_legacy_sealed: relation=%.% operation=%',
        TG_TABLE_SCHEMA,TG_TABLE_NAME,TG_OP;
END;
"""


def install_fact_header_legacy_functions(conn) -> None:
    """Install code-owned functions/registry guards, not a legacy binding."""
    conn.execute(text(
        "CREATE OR REPLACE FUNCTION market.fact_header_legacy_end_day() "
        "RETURNS date LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog "
        "AS $qt$" + LEGACY_END_BODY + "$qt$"
    ))
    conn.execute(text(
        "CREATE OR REPLACE FUNCTION market.reject_fact_header_legacy_mutation() "
        "RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog "
        "AS $qt$" + REJECT_LEGACY_MUTATION_BODY + "$qt$"
    ))
    for suffix, events, scope in (("", "UPDATE OR DELETE", "ROW"),
                                   ("_truncate", "TRUNCATE", "STATEMENT")):
        name = "trg_guard_fact_header_legacy" + suffix
        conn.execute(text(
            f"CREATE TRIGGER {name} BEFORE {events} ON market.fact_header_legacy "
            f"FOR EACH {scope} EXECUTE FUNCTION market.reject_fact_header_legacy_mutation()"
        ))
        conn.execute(text(f"ALTER TABLE market.fact_header_legacy ENABLE ALWAYS TRIGGER {name}"))


def _assert_trigger(conn, relation, name, kind):
    actual = conn.execute(text("""
        SELECT t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,
               t.tgfoid=to_regprocedure('market.reject_fact_header_legacy_mutation()'),
               t.tgqual IS NULL,t.tgnargs
        FROM pg_trigger t WHERE t.tgrelid=to_regclass(:relation) AND t.tgname=:name
    """), {"relation": relation, "name": name}).one_or_none()
    if actual is None or tuple(actual) != (kind, "A", False, False, True, True, 0):
        raise RuntimeError(f"fact_header_legacy_seal_invalid: relation={relation} trigger={name}")


def assert_fact_header_legacy_contract(conn):
    """Validate the single optional retained child; return its exclusive end day."""
    relation = conn.execute(text("""
        SELECT relkind,relpersistence,relrowsecurity,relforcerowsecurity
        FROM pg_class WHERE oid=to_regclass('market.fact_header_legacy')
    """)).one_or_none()
    if relation is None or tuple(relation) != ("r", "p", False, False):
        raise RuntimeError("fact_header_legacy_catalog_missing_or_incompatible: explicit cutover required")
    inspector = inspect(conn)
    columns = {c["name"]: (str(c["type"]), c["nullable"])
               for c in inspector.get_columns(LEGACY_TABLE, schema="market")}
    if columns != {"id": ("INTEGER", False), "end_day": ("DATE", False),
                   "relation_oid": ("BIGINT", False)}:
        raise RuntimeError("fact_header_legacy_columns_incompatible")
    if inspector.get_pk_constraint(LEGACY_TABLE, schema="market")["constrained_columns"] != ["id"]:
        raise RuntimeError("fact_header_legacy_primary_key_incompatible")
    checks = dict(conn.execute(text("""
        SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid='market.fact_header_legacy'::regclass AND contype='c'
          AND convalidated AND NOT condeferrable
    """)).all())
    if (checks.get("ck_market_fact_header_legacy_singleton") != "CHECK ((id = 1))"
            or checks.get("ck_market_fact_header_legacy_oid") !=
            "CHECK (((relation_oid > 0) AND (relation_oid <= '4294967295'::bigint)))"):
        raise RuntimeError("fact_header_legacy_constraints_incompatible")
    for signature, body, volatility, result in (
        (LEGACY_END_SIGNATURE, LEGACY_END_BODY, "s", "date"),
        ("market.reject_fact_header_legacy_mutation()", REJECT_LEGACY_MUTATION_BODY, "v", "trigger"),
    ):
        actual = conn.execute(text("""
            SELECT p.prosrc,p.provolatile,p.prosecdef,p.proretset,
                   p.prorettype=to_regtype(:result),p.proconfig,p.pronargs,l.lanname
            FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
            WHERE p.oid=to_regprocedure(:signature)
        """), {"signature": signature, "result": result}).one_or_none()
        if actual is None or tuple(actual) != (
                body, volatility, False, False, True, ["search_path=pg_catalog"], 0, "plpgsql"):
            raise RuntimeError(f"fact_header_legacy_function_incompatible: function={signature}")
    _assert_trigger(conn, "market.fact_header_legacy", "trg_guard_fact_header_legacy", 27)
    _assert_trigger(conn, "market.fact_header_legacy", "trg_guard_fact_header_legacy_truncate", 34)
    end_day = conn.scalar(text("SELECT market.fact_header_legacy_end_day()"))
    if end_day is None:
        return None
    _assert_trigger(conn, LEGACY_RELATION, "trg_seal_fact_versions_legacy", 31)
    _assert_trigger(conn, LEGACY_RELATION, "trg_seal_fact_versions_legacy_truncate", 34)
    # ATTACH must retain valid native primary/revision keys and every parent
    # search index. An unlinked or invalid leaf cannot be admitted as history.
    missing_index = conn.scalar(text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_index parent
            WHERE parent.indrelid='market.fact_versions'::regclass AND (
                NOT parent.indisvalid OR NOT parent.indisready OR NOT parent.indislive
                OR NOT EXISTS (
                    SELECT 1 FROM pg_inherits link JOIN pg_index child ON child.indexrelid=link.inhrelid
                    WHERE link.inhparent=parent.indexrelid
                      AND child.indrelid='market.fact_versions_legacy'::regclass
                      AND child.indisvalid AND child.indisready AND child.indislive)))
    """))
    if missing_index:
        raise RuntimeError("fact_header_legacy_index_binding_invalid")
    # Native FKs cloned from the canonical parent may not be disabled or lost.
    missing_reference = conn.scalar(text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint parent
            WHERE parent.conrelid='market.fact_versions'::regclass AND parent.contype='f'
              AND NOT EXISTS (
                SELECT 1 FROM pg_constraint child
                WHERE child.conrelid='market.fact_versions_legacy'::regclass
                  AND child.conparentid=parent.oid AND child.convalidated
                  AND child.confrelid=parent.confrelid AND child.conkey=parent.conkey
                  AND child.confkey=parent.confkey
                  AND child.condeferrable=parent.condeferrable
                  AND child.condeferred=parent.condeferred
                  AND child.confdeltype=parent.confdeltype AND child.confupdtype=parent.confupdtype
                  AND EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgconstraint=child.oid)
                  AND NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgconstraint=child.oid
                                  AND (t.tgenabled NOT IN ('O','A') OR t.tgqual IS NOT NULL))))
    """))
    if missing_reference:
        raise RuntimeError("fact_header_legacy_reference_binding_invalid")
    return end_day

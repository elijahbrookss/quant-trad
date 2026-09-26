"""Dated canonical headers with global immutable identity.

Clean bootstrap and explicit operator cutovers install this layout. Runtime may
provision an empty daily partition, but never adopt or backfill old relations.
"""
from __future__ import annotations

from datetime import date, timedelta
import logging

from sqlalchemy import inspect, text

logger = logging.getLogger(__name__)
IDENTITY_TABLES = ("fact_identities", "fact_header_partitions")
IDENTITY_CUTOVER = "docs/engineering/fact-header-layout-v2.md"

REGISTER_IDENTITY_BODY = """
BEGIN
    INSERT INTO market.fact_identities
        (id, storage_day, series_id, observation_key, revision)
    VALUES (NEW.id, NEW.storage_day, NEW.series_id, NEW.observation_key, NEW.revision)
    ON CONFLICT (id) DO NOTHING;
    IF NOT EXISTS (
        SELECT 1 FROM market.fact_identities AS identity
        WHERE (identity.id, identity.storage_day, identity.series_id,
               identity.observation_key, identity.revision)
              IS NOT DISTINCT FROM
              (NEW.id, NEW.storage_day, NEW.series_id, NEW.observation_key, NEW.revision)
    ) THEN
        RAISE EXCEPTION 'canonical_fact_identity_mismatch: fact_version_id=% storage_day=%',
            NEW.id, NEW.storage_day;
    END IF;
    RETURN NEW;
END;
"""

REQUIRE_HEADER_BODY = """
DECLARE
    header_exists boolean;
BEGIN
    -- Replan with the concrete day; a generic parent plan locks old partitions.
    EXECUTE 'SELECT EXISTS (
        SELECT 1 FROM market.fact_versions AS header
        WHERE header.id = $1 AND header.storage_day = $2
          AND (header.series_id, header.observation_key, header.revision)
              IS NOT DISTINCT FROM ($3, $4, $5)
    )' INTO header_exists
       USING NEW.id, NEW.storage_day, NEW.series_id, NEW.observation_key, NEW.revision;
    IF NOT header_exists THEN
        RAISE EXCEPTION 'canonical_fact_identity_header_missing: fact_version_id=% storage_day=%',
            NEW.id, NEW.storage_day;
    END IF;
    RETURN NULL;
END;
"""


REJECT_IDENTITY_MUTATION_BODY = """
BEGIN
    RAISE EXCEPTION 'immutable canonical identity relation %.% rejects %',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
"""


def install_fact_identity_functions(conn) -> None:
    for name, body in (
        ("register_fact_identity", REGISTER_IDENTITY_BODY),
        ("require_fact_identity_header", REQUIRE_HEADER_BODY),
        ("reject_fact_identity_mutation", REJECT_IDENTITY_MUTATION_BODY),
    ):
        conn.execute(text(
            f"CREATE OR REPLACE FUNCTION market.{name}() RETURNS trigger "
            "LANGUAGE plpgsql AS $qt$" + body + "$qt$"
        ))
    conn.execute(text(
        "CREATE TRIGGER trg_register_fact_identity BEFORE INSERT ON market.fact_versions "
        "FOR EACH ROW EXECUTE FUNCTION market.register_fact_identity()"
    ))
    conn.execute(text(
        "CREATE CONSTRAINT TRIGGER trg_require_fact_identity_header "
        "AFTER INSERT ON market.fact_identities DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION market.require_fact_identity_header()"
    ))
    conn.execute(text("ALTER TABLE market.fact_versions ENABLE ALWAYS TRIGGER trg_register_fact_identity"))
    conn.execute(text("ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_require_fact_identity_header"))


    for table in IDENTITY_TABLES:
        conn.execute(text(
            f"CREATE TRIGGER trg_guard_{table} BEFORE UPDATE OR DELETE ON market.{table} "
            "FOR EACH ROW EXECUTE FUNCTION market.reject_fact_identity_mutation()"
        ))
        conn.execute(text(f"ALTER TABLE market.{table} ENABLE ALWAYS TRIGGER trg_guard_{table}"))


def fact_header_partition_name(storage_day: date) -> str:
    if type(storage_day) is not date:
        raise ValueError("fact_header_partition_invalid: UTC date required")
    return "fact_versions_" + storage_day.strftime("%Y%m%d")


def _partition_bound(storage_day: date) -> str:
    until = storage_day + timedelta(days=1)
    return f"FOR VALUES FROM ('{storage_day.isoformat()}') TO ('{until.isoformat()}')"


def _assert_header_partition(conn, storage_day: date) -> None:
    relation = "market." + fact_header_partition_name(storage_day)
    row = conn.execute(text(
        "SELECT c.relkind, pg_get_expr(c.relpartbound, c.oid), i.inhparent::regclass::text "
        "FROM pg_class c LEFT JOIN pg_inherits i ON i.inhrelid=c.oid "
        "WHERE c.oid=to_regclass(:relation)"
    ), {"relation": relation}).one_or_none()
    if row is None or tuple(row) != ("r", _partition_bound(storage_day), "market.fact_versions"):
        raise RuntimeError(f"fact_header_partition_missing_or_incompatible: relation={relation}; inspect before retrying")


def ensure_fact_header_partition(conn, storage_day: date) -> str:
    """Create one new empty partition; never replace a recorded missing partition."""
    name = fact_header_partition_name(storage_day)
    relation = "market." + name
    exists = conn.execute(text(
        "SELECT 1 FROM market.fact_header_partitions WHERE storage_day=:day"
    ), {"day": storage_day}).scalar_one_or_none()
    if exists:
        _assert_header_partition(conn, storage_day)
        return relation
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:relation, 0))"),
                 {"relation": relation})
    exists = conn.execute(text(
        "SELECT 1 FROM market.fact_header_partitions WHERE storage_day=:day"
    ), {"day": storage_day}).scalar_one_or_none()
    if exists:
        _assert_header_partition(conn, storage_day)
        return relation
    if conn.execute(text("SELECT to_regclass(:relation)"),
                    {"relation": relation}).scalar_one_or_none() is not None:
        raise RuntimeError(f"fact_header_partition_unregistered: relation={relation}; inspect before retrying")
    conn.exec_driver_sql(f'CREATE TABLE market."{name}" (LIKE market.fact_versions INCLUDING ALL)')
    until = storage_day + timedelta(days=1)
    conn.exec_driver_sql(
        f'ALTER TABLE market."{name}" ADD CONSTRAINT "{name}_day" '
        f"CHECK (storage_day >= DATE '{storage_day.isoformat()}' AND storage_day < DATE '{until.isoformat()}')"
    )
    conn.exec_driver_sql(
        f'ALTER TABLE market.fact_versions ATTACH PARTITION market."{name}" {_partition_bound(storage_day)}'
    )
    conn.execute(text(
        "INSERT INTO market.fact_header_partitions(storage_day) VALUES (:day)"
    ), {"day": storage_day})
    _assert_header_partition(conn, storage_day)
    logger.warning("market_fact_header_partition_created | storage_day=%s relation=%s", storage_day, relation)
    return relation


def assert_fact_identity_contract(conn) -> None:
    for name in IDENTITY_TABLES:
        if conn.execute(text("SELECT to_regclass(:relation)"),
                        {"relation": "market." + name}).scalar_one_or_none() is None:
            raise RuntimeError(f"Canonical identity layout is missing market.{name}. See {IDENTITY_CUTOVER}; explicit cutover required")
    kind, key = conn.execute(text(
        "SELECT relkind,pg_get_partkeydef(oid) FROM pg_class WHERE oid='market.fact_versions'::regclass"
    )).one()
    if kind != "p" or key != "RANGE (storage_day)":
        raise RuntimeError(f"Canonical headers require dated partitions. See {IDENTITY_CUTOVER}; explicit cutover required")
    inspector = inspect(conn)
    for table, columns in (("fact_versions", ("id", "storage_day")), ("fact_identities", ("id",))):
        if tuple(inspector.get_pk_constraint(table, schema="market").get("constrained_columns") or ()) != columns:
            raise RuntimeError(f"Canonical identity primary key differs: market.{table}. See {IDENTITY_CUTOVER}; explicit cutover required")
    unique = {tuple(item["column_names"]) for item in
              inspector.get_unique_constraints("fact_identities", schema="market")}
    if not {("id", "storage_day"), ("series_id", "observation_key", "revision")} <= unique:
        raise RuntimeError("canonical_identity_unique_constraint_missing: explicit repair required")
    binding = conn.execute(text("""
        SELECT convalidated AND NOT condeferrable AND confdeltype='r'
               AND pg_get_constraintdef(oid) =
                   'FOREIGN KEY (id, storage_day) REFERENCES market.fact_identities(id, storage_day) ON DELETE RESTRICT'
        FROM pg_constraint
        WHERE conrelid='market.fact_versions'::regclass AND conname='fk_market_fact_identity_day'
    """)).scalar_one_or_none()
    if binding is not True:
        raise RuntimeError("canonical_identity_binding_invalid: explicit repair required")
    for name, body in (
        ("register_fact_identity", REGISTER_IDENTITY_BODY),
        ("require_fact_identity_header", REQUIRE_HEADER_BODY),
        ("reject_fact_identity_mutation", REJECT_IDENTITY_MUTATION_BODY),
    ):
        actual = conn.execute(text(
            "SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='market' AND p.proname=:name"
        ), {"name": name}).scalars().all()
        if actual != [body]:
            raise RuntimeError(f"Canonical identity function differs: market.{name}. See {IDENTITY_CUTOVER}; explicit cutover required")
    for table, trigger, function, kind, deferred in (
        ("fact_versions", "trg_register_fact_identity", "register_fact_identity", 7, False),
        ("fact_identities", "trg_require_fact_identity_header", "require_fact_identity_header", 5, True),
        *((table, "trg_guard_" + table, "reject_fact_identity_mutation", 27, False)
          for table in IDENTITY_TABLES),
    ):
        actual = conn.execute(text(
            "SELECT t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,n.nspname,p.proname "
            "FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
            "JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE t.tgrelid=to_regclass(:relation) AND t.tgname=:trigger"
        ), {"relation": "market." + table, "trigger": trigger}).one_or_none()
        if actual is None or tuple(actual) != (kind, "A", deferred, deferred, "market", function):
            raise RuntimeError(f"Canonical identity enforcement differs: {table}.{trigger}. See {IDENTITY_CUTOVER}; explicit cutover required")
    days = conn.execute(text(
        "SELECT storage_day FROM market.fact_header_partitions ORDER BY storage_day LIMIT 4097"
    )).scalars().all()
    if len(days) > 4096:
        raise RuntimeError("fact_header_partition_inventory_exceeded: review the storage horizon")
    for day in days:
        _assert_header_partition(conn, day)
    registered = {fact_header_partition_name(day) for day in days}
    attached = set(conn.execute(text(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid "
        "WHERE i.inhparent='market.fact_versions'::regclass"
    )).scalars())
    if registered != attached:
        raise RuntimeError("fact_header_partition_catalog_mismatch: explicit cutover or repair required")


def assert_fact_identity_references(conn, *, allow_missing_canonical_dependencies=False) -> None:
    """Require payload/archive references to bind to the global identity table."""
    for table, column in (
        ("fact_hot_payloads", "id"),
        ("fact_archive_material_aliases", "fact_version_id"),
        ("fact_archive_canonical_dependencies", "fact_version_id"),
    ):
        relation = "market." + table
        if allow_missing_canonical_dependencies and table == "fact_archive_canonical_dependencies":
            if conn.execute(text("SELECT to_regclass(:relation)"),
                            {"relation": relation}).scalar_one_or_none() is None:
                continue
        expected = f"FOREIGN KEY ({column}) REFERENCES market.fact_identities(id) ON DELETE RESTRICT"
        count = conn.execute(text(
            "SELECT count(*) FROM pg_constraint "
            "WHERE conrelid=to_regclass(:relation) AND contype='f' "
            "AND convalidated AND NOT condeferrable AND pg_get_constraintdef(oid)=:definition"
        ), {"relation": relation, "definition": expected}).scalar_one()
        if count != 1:
            raise RuntimeError(
                f"canonical_identity_reference_invalid: relation={relation} column={column}; "
                f"see {IDENTITY_CUTOVER}"
            )

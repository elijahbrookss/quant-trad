"""Disposable source fixture matching the installed 487d18c6 header layout.

The rest of the application schema and records come from the clean fixture.
This is test setup only, not a downgrade or operator migration command.
"""
from contextlib import nullcontext
from datetime import date, timedelta
import logging
import os
from pathlib import Path

from sqlalchemy import text
from portal.backend.db.fact_storage_schema import fact_partition_name

logger = logging.getLogger(__name__)

DDL = Path(__file__).parents[1] / "fixtures" / "fact_header_tiered_v1.sql"


def restore_tiered_v1_fixture(storage):
    if os.getenv("QT_DB_TEST_ISOLATED") != "1":
        raise RuntimeError("tiered_v1_fixture_requires_owned_disposable_database")
    engine = storage.database._engine
    with engine.begin() as conn:
        database = conn.scalar(text("SELECT current_database()"))
        if not database.startswith("qt_migration_"):
            raise RuntimeError("tiered_v1_fixture_requires_fresh_migration_database")
        conn.exec_driver_sql("CREATE TEMP TABLE preserved_headers AS SELECT * FROM market.fact_versions")
        incoming = conn.execute(text("""
            SELECT conrelid::regclass::text AS relation,conname,
                   pg_get_constraintdef(oid) AS definition
            FROM pg_constraint
            WHERE contype='f' AND confrelid='market.fact_identities'::regclass
              AND conparentid=0 AND conrelid<>'market.fact_versions'::regclass
            ORDER BY conrelid,conname
        """)).mappings().all()
        for row in incoming:
            quoted = conn.dialect.identifier_preparer.quote(row["conname"])
            conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} DROP CONSTRAINT {quoted}')
        conn.exec_driver_sql("DROP FUNCTION market.read_fact_headers_in_range(bigint,timestamptz,timestamptz)")
        conn.exec_driver_sql("DROP VIEW market.fact_rows")
        conn.exec_driver_sql("DROP TABLE market.fact_versions")
        for name in ("fact_header_series_days", "fact_header_partitions", "fact_identities"):
            conn.exec_driver_sql("DROP TABLE market."+name)
        conn.execute(text(DDL.read_text()))
        columns = conn.execute(text("""
            SELECT attname FROM pg_attribute
            WHERE attrelid='market.fact_versions'::regclass AND attnum>0 AND NOT attisdropped
            ORDER BY attnum
        """)).scalars().all()
        names = ",".join(conn.dialect.identifier_preparer.quote(name) for name in columns)
        conn.exec_driver_sql(f"INSERT INTO market.fact_versions ({names}) SELECT {names} FROM preserved_headers")
        for row in incoming:
            quoted = conn.dialect.identifier_preparer.quote(row["conname"])
            definition = row["definition"].replace("market.fact_identities", "market.fact_versions")
            conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} ADD CONSTRAINT {quoted} {definition}')
        # This method is byte-for-byte AST-equivalent in the pinned v1 source.
        storage.database._ensure_canonical_fact_insert_trigger(conn)
        conn.exec_driver_sql("""
            CREATE TRIGGER trg_reject_mutation_fact_versions
            BEFORE UPDATE OR DELETE ON market.fact_versions
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
        conn.exec_driver_sql("""
            CREATE CONSTRAINT TRIGGER trg_require_fact_hot_payload AFTER INSERT ON market.fact_versions
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION market.require_fact_hot_payload()
        """)
        conn.exec_driver_sql("ALTER TABLE market.fact_versions ENABLE ALWAYS TRIGGER trg_require_fact_hot_payload")
        conn.execute(text("""
            UPDATE market.fact_storage_state SET layout_version='market.fact_storage_tiers.v1'
            WHERE layout_version='market.fact_storage_tiers.v2'
        """))
        conn.exec_driver_sql("DROP TABLE preserved_headers")


def stage_shadow_handoff_fixture(conn, storage, *, prevalidated=False, raw_mapping=False):
    """Rehearse the fixed dependency switch; never an operator entry point.

    Only tiny owned fixtures qualify. Exact fenced verification is shared with
    the fixed migration primitives; production capacity, full duration, commit
    supervision and post-resume recovery are NOT qualified here.
    Every change remains in the caller's transaction.
    """
    from scripts.db import fact_header_v2_copy as copy
    from scripts.db import fact_header_v2_references as references

    if os.getenv("QT_DB_TEST_ISOLATED") != "1" or not conn.in_transaction():
        raise RuntimeError("shadow_handoff_fixture_requires_owned_transaction")
    if not conn.scalar(text("SELECT current_database()")).startswith("qt_migration_"):
        raise RuntimeError("shadow_handoff_fixture_requires_fresh_migration_database")
    # This fixture remains tiny-only; verification is an internal operator
    # primitive, not permission to relabel this test handoff as a migration.
    if conn.scalar(text("SELECT count(*) FROM (SELECT 1 FROM market.fact_versions LIMIT 1025) tiny"))>1024:
        raise RuntimeError("shadow_handoff_fixture_requires_tiny_verified_copy")
    if raw_mapping:
        from scripts.db import raw_mapping_v2_copy as raw
        if conn.scalar(text(f"SELECT count(*) FROM (SELECT 1 FROM {raw.SOURCE} LIMIT 1025) tiny"))>1024:
            raise RuntimeError("shadow_handoff_fixture_requires_tiny_raw_mapping_copy")
    with copy.verified_copy(conn, page_rows=2, timeout_seconds=60) as verified:
        if verified["verified_header_rows"]>1024:
            raise RuntimeError("shadow_handoff_fixture_requires_tiny_verified_copy")
        if prevalidated and not references.inspect_references(conn)["references_complete"]:
            raise RuntimeError("shadow_handoff_fixture_references_incomplete")
        with (raw.verified_copy(conn, page_rows=2, timeout_seconds=60) if raw_mapping else nullcontext()) as lookup:
            if lookup is not None and lookup["verified_lookup_rows"]>1024:
                raise RuntimeError("shadow_handoff_fixture_requires_tiny_raw_mapping_copy")
            return _switch_tiny_verified_fixture(conn, storage, verified,
                                                 prevalidated=prevalidated, raw_mapping=raw_mapping)


def _switch_tiny_verified_fixture(conn, storage, verified, *, prevalidated, raw_mapping):
    from scripts.db.fact_header_v2_handoff import _switch_verified_tables

    result = _switch_verified_tables(conn, verified, prevalidated=prevalidated,
        raw_mapping=raw_mapping, evidence={"source_retained": True, "disposable_rehearsal": True})
    return {**result, "scope": "disposable_fixture_only"}


# Frozen deployed f673cb62 partition writer; only the Python name is changed.
# V1 has no header-partition step. Do not call the v2 provisioning entrypoint
# against an intentionally restored v1 source.
def ensure_v1_payload_partition(conn, storage_day: date) -> str:
    """Provision an empty, deterministic daily table once; never adopt unknown data."""
    name = fact_partition_name(storage_day)
    relation = "market." + name
    state = conn.execute(text(
        "SELECT state FROM market.fact_retention_partitions WHERE storage_day = :day"
    ), {"day": storage_day}).scalar_one_or_none()
    if state is not None:
        if state != "open":
            raise RuntimeError(f"fact_hot_partition_not_open: storage_day={storage_day} state={state}")
        return relation
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 0))"), {"name": relation})
    state = conn.execute(text(
        "SELECT state FROM market.fact_retention_partitions WHERE storage_day = :day"
    ), {"day": storage_day}).scalar_one_or_none()
    if state is not None:
        if state != "open":
            raise RuntimeError(f"fact_hot_partition_not_open: storage_day={storage_day} state={state}")
        return relation
    if conn.execute(text("SELECT to_regclass(:name)"), {"name": relation}).scalar_one_or_none() is not None:
        raise RuntimeError(f"fact_hot_partition_unregistered: relation={relation} manual inspection required")
    until = storage_day + timedelta(days=1)
    # All identifiers/date literals are generated from a validated datetime.date.
    # ATTACH uses a less restrictive parent lock than CREATE TABLE ... PARTITION OF.
    conn.exec_driver_sql(f'CREATE TABLE market."{name}" (LIKE market.fact_hot_payloads INCLUDING ALL)')
    conn.exec_driver_sql(
        f'ALTER TABLE market."{name}" ADD CONSTRAINT "{name}_day" '
        f"CHECK (storage_day >= DATE '{storage_day.isoformat()}' AND storage_day < DATE '{until.isoformat()}')"
    )
    conn.exec_driver_sql(
        f'ALTER TABLE market.fact_hot_payloads ATTACH PARTITION market."{name}" '
        f"FOR VALUES FROM ('{storage_day.isoformat()}') TO ('{until.isoformat()}')"
    )
    conn.execute(text(
        "INSERT INTO market.fact_retention_partitions (storage_day, state) VALUES (:day, 'open')"
    ), {"day": storage_day})
    logger.warning("market_fact_partition_created | storage_day=%s relation=%s", storage_day, relation)
    return relation

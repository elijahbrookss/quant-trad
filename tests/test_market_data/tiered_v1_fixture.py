"""Disposable source fixture matching the installed 487d18c6 header layout.

The rest of the application schema and records come from the clean fixture.
This is test setup only, not a downgrade or operator migration command.
"""
import os
from pathlib import Path

from sqlalchemy import text

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

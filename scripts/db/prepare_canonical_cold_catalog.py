"""Explicit creation of the cold catalog and registered-HDD lookup tables."""

from __future__ import annotations

import argparse
import os

from sqlalchemy import create_engine, text
from sqlalchemy.schema import CreateIndex, CreateTable

from portal.backend.db.cold_history_models import (
    Base, COLD_IMMUTABILITY_TRIGGER, COLD_LOCATOR_TABLE_NAMES, COLD_TABLE_NAMES,
    assert_cold_history_immutability, assert_cold_history_placement,
)
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION


def prepare_catalog(conn, *, expected_database: str, target_id: str) -> None:
    conn.execute(text("SET LOCAL lock_timeout = '500ms'"))
    conn.execute(text("SET LOCAL statement_timeout = '20s'"))
    conn.execute(text("SELECT pg_advisory_xact_lock(9021001)"))
    identity = conn.execute(text("""
        SELECT control.system_identifier::text || '/' || db.oid::text
        FROM pg_control_system() AS control CROSS JOIN pg_database AS db
        WHERE db.datname=current_database()
    """)).scalar_one()
    if identity != expected_database:
        raise RuntimeError("cold_fact_catalog_database_identity_mismatch")
    state = conn.execute(text("""
        SELECT layout_version,state FROM market.fact_storage_state LIMIT 2
    """)).all()
    if state != [(FACT_STORAGE_LAYOUT_VERSION, "ready")]:
        raise RuntimeError("cold_fact_catalog_requires_source_layout")
    target = conn.execute(text("""
        SELECT p.spcname
        FROM public.portal_storage_header_tablespaces AS registered
        JOIN public.portal_storage_targets AS targets ON targets.id=registered.target_id
        JOIN pg_tablespace AS p ON p.oid=registered.tablespace_oid
        WHERE registered.database_identity=:identity AND registered.target_id=:target_id
          AND targets.medium='hdd' AND targets.state='active'
          AND length(pg_tablespace_location(p.oid)) > 0
    """), {"identity": identity, "target_id": target_id}).scalar_one_or_none()
    if target is None:
        raise RuntimeError("cold_fact_catalog_registered_history_target_missing")
    tablespace = conn.dialect.identifier_preparer.quote_identifier(target)
    tables = [table for table in Base.metadata.sorted_tables
              if table.schema == "market" and table.name in COLD_TABLE_NAMES]
    for table in tables:
        exists = conn.execute(text("SELECT to_regclass(:relation) IS NOT NULL"),
                              {"relation": table.fullname}).scalar_one()
        if exists:
            raise RuntimeError(f"cold_fact_catalog_relation_exists: relation={table.fullname}")
    for table in tables:
        suffix = f" TABLESPACE {tablespace}" if table.name in COLD_LOCATOR_TABLE_NAMES else ""
        conn.execute(text(str(CreateTable(table).compile(dialect=conn.dialect)).rstrip() + suffix))
        for index in sorted(table.indexes, key=lambda item: item.name):
            conn.execute(text(str(CreateIndex(index).compile(dialect=conn.dialect)).rstrip() + suffix))
        if suffix:
            # PostgreSQL creates constraint-backed indexes independently of
            # the heap. These indexes are empty at this explicit DDL boundary.
            index_names = conn.execute(text("""
                SELECT index.relname FROM pg_index AS binding
                JOIN pg_class AS index ON index.oid=binding.indexrelid
                WHERE binding.indrelid=to_regclass(:relation)
                  AND binding.indisunique
            """), {"relation": table.fullname}).scalars()
            for name in index_names:
                quoted = conn.dialect.identifier_preparer.quote_identifier(name)
                conn.execute(text(f"ALTER INDEX market.{quoted} SET TABLESPACE {tablespace}"))
        quoted_table = conn.dialect.identifier_preparer.quote_identifier(table.name)
        conn.execute(text(f"""
            CREATE TRIGGER {COLD_IMMUTABILITY_TRIGGER}
            BEFORE UPDATE OR DELETE OR TRUNCATE ON market.{quoted_table}
            FOR EACH STATEMENT EXECUTE FUNCTION market.reject_immutable_mutation()
        """))
        conn.execute(text(f"""
            ALTER TABLE market.{quoted_table} ENABLE ALWAYS TRIGGER {COLD_IMMUTABILITY_TRIGGER}
        """))
    assert_cold_history_placement(conn)
    assert_cold_history_immutability(conn)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-database", required=True, help="PostgreSQL system identifier/database OID")
    parser.add_argument("--target-id", required=True, help="registered HDD storage target ID")
    args = parser.parse_args()
    engine = create_engine(os.environ["PG_DSN"])
    try:
        with engine.begin() as conn:
            prepare_catalog(conn, expected_database=args.expected_database, target_id=args.target_id)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

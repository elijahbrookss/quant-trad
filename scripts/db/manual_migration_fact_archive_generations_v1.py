"""Explicit metadata-only archive generation cutover.

Run with backend and canonical retention workers stopped:

    python scripts/db/manual_migration_fact_archive_generations_v1.py

Historical archive rows are not copied or rewritten. Existing pages continue to
use their legacy catalogs until a newly prepared generation is activated.
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text

from portal.backend.db import db
from portal.backend.db.fact_storage_schema import (
    FACT_ARCHIVE_GENERATION_TABLES,
    assert_fact_storage_contract,
    install_fact_archive_generation_functions,
)
from portal.backend.db.models import Base


_LOCK = "quant-trad:fact-archive-generations-v1"
_PLACEMENT_SOURCES = {
    "fact_archive_prepared_material_aliases": "fact_archive_material_aliases",
    "fact_archive_prepared_canonical_dependencies": "fact_archive_canonical_dependencies",
}


def _relation_tablespace(conn, relation: str) -> dict:
    """Require one effective tablespace for heap, TOAST, and every index."""
    rows = conn.execute(text("""
        WITH heap AS (
            SELECT oid,reltoastrelid FROM pg_class WHERE oid=to_regclass(:relation)
        ), heaps AS (
            SELECT oid FROM heap UNION ALL
            SELECT reltoastrelid FROM heap WHERE reltoastrelid<>0
        ), members AS (
            SELECT oid FROM heaps UNION ALL
            SELECT indexrelid FROM pg_index WHERE indrelid IN(SELECT oid FROM heaps)
        )
        SELECT DISTINCT COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::bigint AS oid,
               t.spcname AS name
        FROM members JOIN pg_class c ON c.oid=members.oid
        JOIN pg_database d ON d.datname=current_database()
        JOIN pg_tablespace t
          ON t.oid=COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)
        ORDER BY oid
    """), {"relation": relation}).mappings().all()
    if len(rows) != 1:
        raise RuntimeError(
            "fact_archive_generation_catalog_placement_mixed: "
            f"relation={relation} tablespaces={[dict(row) for row in rows]}"
        )
    return dict(rows[0])


def _create_generation_table(conn, name: str) -> dict | None:
    source = _PLACEMENT_SOURCES.get(name)
    if source is None:
        Base.metadata.tables["market." + name].create(conn, checkfirst=False)
        return None
    expected = _relation_tablespace(conn, "market." + source)
    previous = conn.execute(text("SHOW default_tablespace")).scalar_one()
    try:
        conn.execute(text(
            "SELECT set_config('default_tablespace',:tablespace,true)"
        ), {"tablespace": expected["name"]})
        Base.metadata.tables["market." + name].create(conn, checkfirst=False)
    finally:
        conn.execute(text(
            "SELECT set_config('default_tablespace',:tablespace,true)"
        ), {"tablespace": previous})
    actual = _relation_tablespace(conn, "market." + name)
    if actual != expected:
        raise RuntimeError(
            "fact_archive_generation_catalog_placement_changed: "
            f"source={source} target={name} expected={expected} actual={actual}"
        )
    return expected


@contextmanager
def _migration_connection(database):
    engine = database._engine
    owned = engine is None
    if owned:
        dsn = database.dsn or database._resolve_dsn()
        engine = create_engine(dsn, **database._engine_options())
    try:
        with engine.begin() as conn:
            yield conn
    finally:
        if owned:
            engine.dispose()


def migrate(database=db) -> dict:
    tables = tuple(FACT_ARCHIVE_GENERATION_TABLES)
    with _migration_connection(database) as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"), {"name": _LOCK})
        existing = {name for name in tables
                    if conn.execute(text("SELECT to_regclass(:name)"),
                                    {"name": "market." + name}).scalar_one_or_none() is not None}
        if existing and existing != set(tables):
            raise RuntimeError(
                "fact_archive_generation_partial_layout: "
                f"present={sorted(existing)} missing={sorted(set(tables)-existing)}"
            )
        placements = {}
        if not existing:
            for name in tables:
                placement = _create_generation_table(conn, name)
                if placement is not None:
                    placements[name] = placement
            install_fact_archive_generation_functions(conn)
            conn.execute(text(
                "CREATE TRIGGER trg_reject_mutation_fact_archive_verification_generations "
                "BEFORE UPDATE OR DELETE ON market.fact_archive_verification_generations "
                "FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()"
            ))
            conn.execute(text(
                "ALTER TABLE market.fact_archive_verification_generations ENABLE ALWAYS TRIGGER "
                "trg_reject_mutation_fact_archive_verification_generations"
            ))
        inspector = inspect(conn)
        observed = set(inspector.get_table_names(schema="market"))
        missing = set(tables) - observed
        if missing:
            raise RuntimeError(f"fact_archive_generation_migration_incomplete: missing={sorted(missing)}")
        triggers = conn.execute(text("""
            SELECT c.relname,t.tgname FROM pg_trigger t
            JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='market' AND c.relname=ANY(:tables) AND NOT t.tgisinternal
            ORDER BY c.relname,t.tgname
        """), {"tables": list(tables)}).all()
        required = {
            ("fact_archive_preparations", "trg_guard_fact_archive_preparation"),
            ("fact_archive_active_generations", "trg_guard_fact_archive_active_generation"),
            *((name, "trg_guard_" + name) for name in (
                "fact_archive_preparation_dependencies", "fact_archive_catalog_batches",
                "fact_archive_prepared_material_aliases",
                "fact_archive_prepared_canonical_dependencies",
            )),
        }
        required.add((
            "fact_archive_verification_generations",
            "trg_reject_mutation_fact_archive_verification_generations",
        ))
        if not required.issubset(set(triggers)):
            raise RuntimeError("fact_archive_generation_mutation_guards_missing")
        for target, source in _PLACEMENT_SOURCES.items():
            source_placement = _relation_tablespace(conn, "market." + source)
            target_placement = _relation_tablespace(conn, "market." + target)
            if target_placement != source_placement:
                raise RuntimeError(
                    "fact_archive_generation_catalog_placement_changed: "
                    f"source={source} target={target} expected={source_placement} "
                    f"actual={target_placement}"
                )
            placements[target] = target_placement
        assert_fact_storage_contract(conn)
    return {
        "status": "ready", "tables": list(tables),
        "historical_rows_rewritten": False,
        "catalog_tablespaces": placements,
    }


if __name__ == "__main__":
    print(migrate())

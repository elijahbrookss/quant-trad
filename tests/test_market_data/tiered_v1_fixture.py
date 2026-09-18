"""Disposable source fixture matching the installed 487d18c6 header layout.

The rest of the application schema and records come from the clean fixture.
This is test setup only, not a downgrade or operator migration command.
"""
from contextlib import nullcontext
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
    from scripts.db import fact_header_v2_copy as copy
    from scripts.db.fact_header_v2_capture import SCHEMA
    from scripts.db import fact_header_v2_references as references
    from portal.backend.db.fact_storage_schema import assert_fact_storage_contract, install_fact_storage_functions

    if raw_mapping:
        from scripts.db import raw_mapping_v2_copy as raw
    retained = "qt_fact_header_retained_v1"
    count = target_count = verified["verified_header_rows"]
    # Verification allowed ordinary reads. Only the short, transactional rename
    # phase takes an exclusive fence; a busy reader refuses this tiny rehearsal.
    switching = (copy.SOURCE, "market.fact_hot_payloads",
                 "market.fact_archive_material_aliases", "market.fact_archive_canonical_dependencies",
                 *(SCHEMA+"."+name for name in copy.TABLE_NAMES))
    if raw_mapping:
        switching += (raw.SOURCE, raw.TARGET)
    conn.exec_driver_sql("LOCK TABLE "+",".join(switching)+" IN ACCESS EXCLUSIVE MODE NOWAIT")
    incoming = conn.execute(text("""
        SELECT conrelid::regclass::text AS relation,conname,pg_get_constraintdef(oid) AS definition
        FROM pg_constraint WHERE contype='f' AND confrelid='market.fact_versions'::regclass
          AND conparentid=0 ORDER BY conrelid,conname
    """)).mappings().all()
    owners = {"market.fact_hot_payloads","market.fact_archive_material_aliases",
              "market.fact_archive_canonical_dependencies"}
    if len(incoming)!=3 or {row["relation"] for row in incoming}!=owners:
        raise RuntimeError("shadow_handoff_fixture_unexpected_references")
    conn.exec_driver_sql("LOCK TABLE "+",".join(sorted(owners))+" IN ACCESS EXCLUSIVE MODE NOWAIT")
    children = conn.execute(text("""
        SELECT n.nspname,c.relname FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE i.inhparent=to_regclass(:parent) ORDER BY c.relname
    """),{"parent":SCHEMA+".fact_versions"}).all()
    if any(schema!=SCHEMA for schema,name in children):
        raise RuntimeError("shadow_handoff_fixture_unexpected_partition")
    conn.exec_driver_sql(f"CREATE SCHEMA {retained}")
    conn.exec_driver_sql(f"REVOKE ALL ON SCHEMA {retained} FROM PUBLIC")
    conn.exec_driver_sql(f"""
        CREATE TABLE {retained}.fact_storage_state AS
        SELECT * FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'
    """)
    conn.exec_driver_sql("DROP VIEW market.fact_rows")
    conn.exec_driver_sql("DROP TRIGGER trg_assert_fact_hot_payload_valid ON market.fact_hot_payloads")
    for row in incoming:
        name=conn.dialect.identifier_preparer.quote(row["conname"])
        conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} DROP CONSTRAINT {name}')
    conn.exec_driver_sql(f"ALTER TABLE market.fact_versions SET SCHEMA {retained}")
    conn.exec_driver_sql(f"""
        CREATE TRIGGER trg_fixture_retained_source_closed BEFORE INSERT ON {retained}.fact_versions
        FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
    """)
    conn.exec_driver_sql(f"ALTER TABLE {retained}.fact_versions ENABLE ALWAYS TRIGGER trg_fixture_retained_source_closed")
    for name in (*copy.TABLE_NAMES, *(name for schema,name in children)):
        quoted=conn.dialect.identifier_preparer.quote(name)
        conn.exec_driver_sql(f"ALTER TABLE {SCHEMA}.{quoted} SET SCHEMA market")
    for row in incoming:
        name=conn.dialect.identifier_preparer.quote(row["conname"])
        if prevalidated:
            conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} RENAME CONSTRAINT {references.STAGED} TO {name}')
        else:
            definition=row["definition"].replace("market.fact_versions","market.fact_identities")
            conn.exec_driver_sql(f'ALTER TABLE {row["relation"]} ADD CONSTRAINT {name} {definition}')
    if raw_mapping:
        conn.exec_driver_sql(f"ALTER TABLE {raw.SOURCE} SET SCHEMA {retained}")
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_fixture_retained_raw_source_closed
            BEFORE INSERT ON {retained}.{raw.NAME}
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
        conn.exec_driver_sql(f"ALTER TABLE {retained}.{raw.NAME} ENABLE ALWAYS TRIGGER trg_fixture_retained_raw_source_closed")
        conn.exec_driver_sql(f"ALTER TABLE {raw.TARGET} SET SCHEMA market")
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_reject_mutation_raw_archive_record_mappings
            BEFORE UPDATE OR DELETE ON {raw.SOURCE}
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
    storage.database._ensure_canonical_fact_insert_trigger(conn)
    install_fact_storage_functions(conn)
    for name in ("fact_versions","fact_identities","fact_header_partitions"):
        conn.exec_driver_sql(f"""
            CREATE TRIGGER trg_reject_mutation_{name} BEFORE UPDATE OR DELETE ON market.{name}
            FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()
        """)
    conn.exec_driver_sql("DELETE FROM market.fact_storage_state WHERE layout_version='market.fact_storage_tiers.v1'")
    conn.exec_driver_sql("""
        INSERT INTO market.fact_storage_state(layout_version,state,completed_at,evidence)
        VALUES('market.fact_storage_tiers.v2','ready',clock_timestamp(),
               '{"source_retained":true,"disposable_rehearsal":true}'::jsonb)
    """)
    assert_fact_storage_contract(conn)
    return {"source_rows_retained":count,"active_header_rows":target_count,
            "scope":"disposable_fixture_only"}

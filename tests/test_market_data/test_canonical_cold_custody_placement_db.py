import json

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from portal.backend.db.cold_history_models import COLD_CUSTODY_TABLE_NAMES
from portal.backend.db.storage_target_models import StorageHeaderTablespaceRecord, StorageTargetRecord
from scripts.db.canonical_cold_custody_placement import (
    assert_custody_placement,
    inspect_custody_placement,
    move_custody_tablespace,
)
from tests.test_market_data.test_canonical_cold_migration_db import local_catalog_server, prepared_database  # noqa: F401

pytestmark = pytest.mark.db


def test_native_custody_move_preserves_forks_and_fk_scope(prepared_database):
    engine, tablespace, root = prepared_database
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA market")
        StorageTargetRecord.__table__.create(conn)
        StorageHeaderTablespaceRecord.__table__.create(conn)
        database, tablespace_oid = conn.execute(text("""
            SELECT system_identifier::text || '/' || db.oid::text, s.oid::bigint
            FROM pg_control_system() CROSS JOIN pg_database AS db CROSS JOIN pg_tablespace AS s
            WHERE db.datname=current_database() AND s.spcname=:name
        """), {"name": tablespace}).one()
        conn.execute(StorageTargetRecord.__table__.insert(), {
            "id": "fixture-hdd", "label": "Fixture history", "filesystem_uuid": "fixture-device",
            "root": str(root), "medium": "hdd", "roles": ["history"],
        })
        conn.execute(StorageHeaderTablespaceRecord.__table__.insert(), {
            "database_identity": database, "target_id": "fixture-hdd", "tablespace_oid": tablespace_oid,
            "binding": {"tablespace_oid": tablespace_oid, "tablespace_location": str(root)},
        })
        conn.exec_driver_sql("CREATE TABLE market.fixture_parent (id text PRIMARY KEY)")
        conn.exec_driver_sql("INSERT INTO market.fixture_parent VALUES ('parent')")
        for name in COLD_CUSTODY_TABLE_NAMES:
            conn.exec_driver_sql(f"""
                CREATE TABLE market.{name} (
                    owner text REFERENCES market.fixture_parent(id) ON DELETE RESTRICT,
                    object_hash text NOT NULL, PRIMARY KEY(owner,object_hash), document text NOT NULL
                );
                CREATE INDEX {name}_fixture_hash ON market.{name}(object_hash);
                ALTER TABLE market.{name} ALTER COLUMN document SET STORAGE EXTERNAL;
                INSERT INTO market.{name}
                SELECT 'parent','key',string_agg(md5(i::text),'') FROM generate_series(1,256) i;
            """)
        expected_rows = {name: conn.exec_driver_sql(f"SELECT * FROM market.{name}").all()
                         for name in COLD_CUSTODY_TABLE_NAMES}
        constraints = conn.exec_driver_sql("""
            SELECT c.oid,c.conname,pg_get_constraintdef(c.oid) FROM pg_constraint AS c
            WHERE c.conrelid='market.fact_archive_dependencies'::regclass
               OR c.conrelid='market.fact_archive_preparation_dependencies'::regclass ORDER BY c.oid
        """).all()
    target = {"expected_database": database, "target_id": "fixture-hdd"}
    with engine.connect() as conn:
        prior = inspect_custody_placement(conn, **target)
        assert prior["copy_bytes"] > 0
        assert {row["member_kind"] for row in prior["members"]} == {"heap", "index", "toast", "toast_index"}
        assert any(row["bytes"] > 0 for row in prior["members"] if row["member_kind"] == "toast")
        assert json.loads(json.dumps(prior)) == prior
    scope = {**target, "expected_tablespace_oid": tablespace_oid,
             "expected_relation_oids": prior["relation_oids"]}
    move = {**scope, "max_copy_bytes": prior["copy_bytes"], "timeout_seconds": 20}
    with engine.connect() as conn:
        with pytest.raises(RuntimeError, match="requires_transaction"):
            move_custody_tablespace(conn, **move)
    for changed, refusal in (
        ({"expected_database": "wrong"}, "wrong_database"),
        ({"expected_tablespace_oid": tablespace_oid + 1}, "tablespace_oid_changed"),
        ({"expected_relation_oids": {name: oid + 1 for name, oid in prior["relation_oids"].items()}},
         "relation_oid_changed"),
        ({"max_copy_bytes": prior["copy_bytes"] - 1}, "copy_budget_exceeded"),
    ):
        with pytest.raises(RuntimeError, match=refusal):
            with engine.begin() as conn:
                move_custody_tablespace(conn, **(move | changed))
    with engine.begin() as reader:
        reader.exec_driver_sql("LOCK TABLE market.fact_archive_preparation_dependencies IN ACCESS SHARE MODE")
        with pytest.raises(DBAPIError, match="could not obtain lock") as busy:
            with engine.begin() as conn:
                move_custody_tablespace(conn, **move)
        assert busy.value.orig.pgcode == "55P03"
    with pytest.raises(RuntimeError, match="mixed_toast_placement_requires_review"):
        with engine.begin() as conn:
            # Test-only catalog fault. Roll back before PostgreSQL can reuse
            # this inconsistent mapping or another connection can observe it.
            conn.execute(text("UPDATE pg_class SET reltablespace=:space WHERE oid=CAST(:heap AS oid)"),
                         {"space": tablespace_oid, "heap": prior["relation_oids"][COLD_CUSTODY_TABLE_NAMES[0]]})
            move_custody_tablespace(conn, **move)
    with engine.connect() as conn:
        with pytest.raises(RuntimeError, match="placement_incomplete"):
            assert_custody_placement(conn, **scope)
        conn.rollback()
        with pytest.raises(RuntimeError, match="interrupt after native move"):
            with conn.begin():
                receipt = move_custody_tablespace(conn, **move)
                assert not receipt["reused"]
                assert_custody_placement(conn, **scope)
                raise RuntimeError("interrupt after native move")
        assert inspect_custody_placement(conn, **target) == prior
    with engine.begin() as conn:
        receipt = move_custody_tablespace(conn, **move)
        assert receipt["copy_bytes"] == prior["copy_bytes"]
        assert all(row["tablespace_oid"] == tablespace_oid for row in receipt["after"])
        assert {row["oid"] for row in receipt["before"]} == {row["oid"] for row in receipt["after"]}
    with engine.begin() as conn:
        assert_custody_placement(conn, **scope)
        assert move_custody_tablespace(conn, **(move | {"max_copy_bytes": 0}))["reused"]
        assert conn.exec_driver_sql("""
            SELECT c.oid,c.conname,pg_get_constraintdef(c.oid) FROM pg_constraint AS c
            WHERE c.conrelid='market.fact_archive_dependencies'::regclass
               OR c.conrelid='market.fact_archive_preparation_dependencies'::regclass ORDER BY c.oid
        """).all() == constraints
        for name in COLD_CUSTODY_TABLE_NAMES:
            assert conn.exec_driver_sql(f"SELECT * FROM market.{name}").all() == expected_rows[name]
    with pytest.raises(DBAPIError) as rejected:
        with engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM market.fixture_parent WHERE id='parent'")
    assert rejected.value.orig.pgcode == "23503"

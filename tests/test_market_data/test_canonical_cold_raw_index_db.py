import os
from pathlib import PurePosixPath
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.db.storage_target_models import StorageHeaderTablespaceRecord, StorageTargetRecord
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_raw_index import (
    INDEX, RECENT_TABLESPACE_OID, TABLE, assert_raw_mapping_index_placement, inspect_raw_mapping_index,
    inspect_raw_mapping_index_member, move_raw_mapping_index, restore_raw_mapping_index,
)
from tests.test_market_data.migration_test_support import fresh_migration_database


pytestmark = pytest.mark.db


@pytest.fixture
def raw_index_database():
    directory = os.environ.get("QT_TEST_RAW_INDEX_TABLESPACE_DIRECTORY")
    if not directory:
        pytest.skip("requires an empty tablespace directory in the disposable TimescaleDB server")
    if os.environ.get("QT_DB_TEST_ISOLATED") != "1" or os.environ.get("RUN_DB_TESTS") != "1":
        pytest.fail("fixture requires the isolated database test lane")
    root = PurePosixPath(directory)
    if not root.is_relative_to("/tmp") or root == PurePosixPath("/tmp") or ".." in root.parts:
        pytest.fail("fixture requires a dedicated /tmp directory in the disposable server")
    parent = make_url(os.environ["PG_DSN"])
    if parent.host not in {"localhost", "127.0.0.1", "::1"} or parent.query:
        pytest.fail("fixture requires the isolated loopback TimescaleDB server")
    tablespace = f"qt_raw_{uuid4().hex[:16]}"
    admin = create_engine(parent.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin.connect() as conn:
            conn.execute(text(f"CREATE TABLESPACE {tablespace} LOCATION :directory"), {"directory": directory})
        try:
            with fresh_migration_database("raw_index") as dsn:
                engine = create_engine(dsn, poolclass=NullPool)
                control = create_engine(engine.url.set(database="postgres"), poolclass=NullPool)
                try:
                    with engine.begin() as conn:
                        conn.exec_driver_sql(f"""
                            CREATE SCHEMA market;
                            CREATE TABLE market.fact_storage_state (
                                layout_version text NOT NULL, state text NOT NULL, completed_at timestamptz
                            );
                            CREATE TABLE {TABLE} (
                                raw_record_id varchar(80) NOT NULL, manifest_id varchar(128) NOT NULL,
                                object_row_index bigint NOT NULL,
                                CONSTRAINT pk_market_raw_archive_record_mapping PRIMARY KEY(raw_record_id,manifest_id)
                                    USING INDEX TABLESPACE pg_default,
                                UNIQUE (manifest_id,object_row_index) USING INDEX TABLESPACE {tablespace}
                            ) TABLESPACE {tablespace};
                            INSERT INTO {TABLE}
                                SELECT 'raw-' || i,'manifest',i FROM generate_series(1,500) AS i;
                        """)
                        conn.execute(text("INSERT INTO market.fact_storage_state VALUES (:layout,'ready',now())"),
                                     {"layout": FACT_STORAGE_LAYOUT_VERSION})
                        StorageTargetRecord.__table__.create(conn)
                        StorageHeaderTablespaceRecord.__table__.create(conn)
                        identity = dict(conn.execute(text("""
                            SELECT system_identifier::text AS system_identifier, db.oid::bigint AS database_oid,
                                   db.datname AS database
                            FROM pg_control_system() CROSS JOIN pg_database db WHERE datname=current_database()
                        """)).mappings().one())
                        database = f"{identity['system_identifier']}/{identity['database_oid']}"
                        tablespace_oid = conn.scalar(text("SELECT oid::bigint FROM pg_tablespace WHERE spcname=:name"),
                                                     {"name": tablespace})
                        conn.execute(StorageTargetRecord.__table__.insert(), {
                            "id": "raw-index-history", "label": "Disposable native tablespace", "root": directory,
                            "filesystem_uuid": "native-fixture", "medium": "hdd", "roles": ["history"],
                        })
                        conn.execute(StorageHeaderTablespaceRecord.__table__.insert(), {
                            "database_identity": database, "target_id": "raw-index-history",
                            "tablespace_oid": tablespace_oid, "binding": {"tablespace_location": directory},
                        })
                    yield engine, control, identity, database, tablespace_oid
                finally:
                    engine.dispose()
                    control.dispose()
        finally:
            with admin.connect() as conn:
                conn.exec_driver_sql(f"DROP TABLESPACE {tablespace}")
    finally:
        admin.dispose()


def test_fenced_raw_index_move_and_inverse_preserve_native_membership(raw_index_database):
    engine, control, identity, database, tablespace_oid = raw_index_database
    inputs = {"expected_database": database, "target_id": "raw-index-history"}
    with engine.connect() as conn, control.connect() as admin:
        with conn.begin():
            before = inspect_raw_mapping_index(conn, **inputs)["member"]
            assert inspect_raw_mapping_index_member(conn) == before
            other = conn.scalar(text("""
                SELECT indexrelid::bigint FROM pg_index WHERE indrelid=CAST(:table AS regclass) AND NOT indisprimary
            """), {"table": TABLE})
            assert before["tablespace_oid"] == RECENT_TABLESPACE_OID
            assert before["table_tablespace_oid"] == tablespace_oid
        scope = inputs | {"expected_index_oid": before["index_oid"], "expected_table_oid": before["table_oid"],
                          "expected_tablespace_oid": tablespace_oid}
        args = scope | {"max_copy_bytes": 1_000_000, "timeout_seconds": 15}
        with conn.begin(), pytest.raises(ValueError, match="closed_database_fence"):
            move_raw_mapping_index(conn, fence=None, **args)
        with pytest.raises(RuntimeError, match="native_definition_changed"):
            with conn.begin():
                conn.exec_driver_sql(f"ALTER INDEX {INDEX} SET (fillfactor=80)")
                inspect_raw_mapping_index(conn, **inputs)

        fence = close_database_fence(conn, admin, expected_database=identity, timeout_seconds=90,
                                     record=lambda *_: None)
        moves = args | {"fence": fence}
        for change, error in (({"expected_index_oid": other}, "expected_oid_changed"),
                              ({"expected_tablespace_oid": RECENT_TABLESPACE_OID}, "expected_oid_changed"),
                              ({"max_copy_bytes": 0}, "copy_budget_exceeded")):
            with conn.begin(), pytest.raises(RuntimeError, match=error):
                move_raw_mapping_index(conn, **(moves | change))
        with pytest.raises(RuntimeError, match="rollback after forward write"):
            with conn.begin():
                result = move_raw_mapping_index(conn, **moves)
                assert result["before"]["forks"] == result["after"]["forks"]
                assert_raw_mapping_index_placement(conn, **scope, on_history=True)
                raise RuntimeError("rollback after forward write")
        with conn.begin():
            assert assert_raw_mapping_index_placement(conn, **scope, on_history=False)["member"] == before
        with conn.begin():
            result = move_raw_mapping_index(conn, **moves)
            assert result["after"]["constraint_oid"] == before["constraint_oid"]
            assert result["copy_bytes"] == sum(before["forks"].values())
            unstamped = {key: value for key, value in result.items() if key != "receipt_hash"}
            assert archive_evidence_hash(unstamped) == result["receipt_hash"]
        with conn.begin(), pytest.raises(RuntimeError, match="authority_changed"):
            restore_raw_mapping_index(conn, **moves, forward_receipt=result,
                                      expected_forward_hash=result["receipt_hash"])
        with conn.begin():
            conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                         {"layout": COLD_HISTORY_LAYOUT_VERSION})

        with conn.begin(), pytest.raises(RuntimeError, match="authority_changed"):
            move_raw_mapping_index(conn, **moves)
        with conn.begin(), pytest.raises(ValueError, match="forward_receipt_changed"):
            restore_raw_mapping_index(conn, **moves, forward_receipt=result, expected_forward_hash="0" * 64)
        with pytest.raises(RuntimeError, match="rollback after inverse write"):
            with conn.begin():
                restore_raw_mapping_index(conn, **moves, forward_receipt=result,
                                          expected_forward_hash=result["receipt_hash"])
                raise RuntimeError("rollback after inverse write")
        with conn.begin():
            assert_raw_mapping_index_placement(conn, **scope, on_history=True)
            # Model history appended between the forward and rollback attempts.
            conn.exec_driver_sql(f"INSERT INTO {TABLE} SELECT 'raw-' || i,'manifest',i FROM generate_series(501,2000) i")
            inverse = restore_raw_mapping_index(conn, **moves, forward_receipt=result,
                                                expected_forward_hash=result["receipt_hash"])
            assert inverse["copy_bytes"] > result["copy_bytes"]
            assert inverse["after"]["forks"] == inverse["before"]["forks"]
            assert inverse["after"]["relation_path"].startswith("base/")
            assert inverse["after"]["index_oid"] == before["index_oid"]
            assert inverse["after"]["constraint_oid"] == before["constraint_oid"]
            assert inverse["after"]["definition"] == before["definition"]
            assert inverse["forward_receipt_hash"] == result["receipt_hash"]
            conn.execute(text("UPDATE market.fact_storage_state SET layout_version=:layout"),
                         {"layout": FACT_STORAGE_LAYOUT_VERSION})
        reopen_database_fence(fence, admin, expected_outcome="original", read_outcome=lambda _: "original",
                              record=lambda *_: None)

    with engine.begin() as conn:
        assert_raw_mapping_index_placement(conn, **scope, on_history=False)
        assert conn.exec_driver_sql(f"""
            SELECT object_row_index FROM {TABLE} WHERE raw_record_id='raw-1862'
        """).scalar_one() == 1862
        unrelated = conn.scalar(text("SELECT reltablespace FROM pg_class WHERE oid=:oid"), {"oid": other})
        assert unrelated == tablespace_oid
        with pytest.raises(DBAPIError) as duplicate, conn.begin_nested():
            conn.exec_driver_sql(f"INSERT INTO {TABLE} VALUES ('raw-1862','manifest',3000)")
        assert duplicate.value.orig.pgcode == "23505"

"""Prove transactional capture, not full v1 schema admission or cutover."""
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DBAPIError

from scripts.db.fact_header_v2_capture import (
    SCHEMA, SOURCE, QUEUE, LOCK, install_capture, inspect_capture,
)
from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def capture_source():
    with fresh_migration_database("header_capture",install_extensions=False) as dsn:
        engine = create_engine(dsn,future=True)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql("CREATE SCHEMA market")
                conn.exec_driver_sql("""
                    CREATE TABLE market.fact_versions(
                        id text PRIMARY KEY,market_commit_seq bigint NOT NULL,body text NOT NULL);
                    INSERT INTO market.fact_versions VALUES('existing',1,'preserved');
                    CREATE TABLE market.fact_storage_state(layout_version text PRIMARY KEY,state text);
                    INSERT INTO market.fact_storage_state VALUES('market.fact_storage_tiers.v1','ready')
                """)
            yield engine
        finally:
            engine.dispose()


def insert(conn, identity, sequence=2):
    conn.execute(text(f"INSERT INTO {SOURCE} VALUES(:id,:seq,:body)"),
                 {"id":identity,"seq":sequence,"body":"content-"+identity})


def ids(engine, relation):
    with engine.begin() as conn:
        return conn.execute(text(f"SELECT id FROM {relation} ORDER BY id")).scalars().all()


def install(engine):
    with engine.begin() as conn:
        return install_capture(conn)


def test_capture_preserves_source_captures_late_ids_and_reuses_queue(capture_source):
    first = install(capture_source)
    assert first["capture_active"] and not first["migration_ready"] and not first["reused"]
    assert ids(capture_source,SOURCE) == ["existing"]
    assert ids(capture_source,QUEUE) == []
    with capture_source.begin() as conn:
        insert(conn,"higher",100)
    # A later commit can fall behind a backfill cursor. Capture must not filter
    # by sequence, ID sorting or a timestamp sampled before that commit.
    with capture_source.begin() as conn:
        insert(conn,"earlier",2)
    assert ids(capture_source,QUEUE) == ["earlier","higher"]
    assert install(capture_source) == {**first,"reused":True}
    assert ids(capture_source,QUEUE) == ["earlier","higher"]


def test_capture_install_failure_rolls_back_ddl_even_if_caller_catches(capture_source):
    def fail(conn,cursor,statement,parameters,context,executemany):
        if statement.lstrip().startswith("CREATE TRIGGER trg_qt_header_v2_capture"):
            raise RuntimeError("injected after capture trigger")
    event.listen(capture_source,"after_cursor_execute",fail)
    try:
        with capture_source.begin() as conn:
            insert(conn,"outer")
            with pytest.raises(RuntimeError,match="injected after"):
                install_capture(conn)
            assert conn.scalar(text("SELECT to_regnamespace(:name)"),{"name":SCHEMA}) is None
    finally:
        event.remove(capture_source,"after_cursor_execute",fail)
    assert ids(capture_source,SOURCE) == ["existing","outer"]
    assert install(capture_source)["capture_active"]


def test_source_and_capture_roll_back_together(capture_source):
    install(capture_source)
    with pytest.raises(RuntimeError,match="rollback"):
        with capture_source.begin() as conn:
            insert(conn,"aborted")
            assert conn.scalar(text(f"SELECT count(*) FROM {QUEUE}")) == 1
            raise RuntimeError("rollback")
    assert ids(capture_source,QUEUE) == []
    assert ids(capture_source,SOURCE) == ["existing"]


def test_backend_loss_then_retry_preserves_capture(capture_source):
    install(capture_source)
    with pytest.raises(DBAPIError):
        with capture_source.begin() as conn:
            insert(conn,"crash")
            pid = conn.scalar(text("SELECT pg_backend_pid()"))
            with capture_source.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),{"pid":pid})
            conn.exec_driver_sql("SELECT 1")
    assert ids(capture_source,QUEUE) == []
    assert ids(capture_source,SOURCE) == ["existing"]
    with capture_source.begin() as conn:
        assert inspect_capture(conn)["capture_active"]
        insert(conn,"crash")
    assert ids(capture_source,QUEUE) == ["crash"]
    assert ids(capture_source,SOURCE) == ["crash","existing"]


@pytest.mark.parametrize("statement",[
    "UPDATE market.fact_versions SET body='changed'",
    "DELETE FROM market.fact_versions",
    "TRUNCATE market.fact_versions",
])
def test_capture_protects_copy_source_from_mutation(capture_source,statement):
    install(capture_source)
    with pytest.raises(DBAPIError,match="capture_source_is_immutable"):
        with capture_source.begin() as conn:
            conn.exec_driver_sql(statement)
    assert ids(capture_source,SOURCE) == ["existing"]


@pytest.mark.parametrize("statement,expected",[
    ("ALTER TABLE market.fact_versions DISABLE TRIGGER trg_qt_header_v2_capture","trigger_changed"),
    ("ALTER FUNCTION qt_fact_header_cutover_v2.capture_fact_insert() SET search_path=public","function_changed"),
    ("DROP TABLE qt_fact_header_cutover_v2.pending_fact_ids; "
     "CREATE TABLE qt_fact_header_cutover_v2.pending_fact_ids(id text PRIMARY KEY)","identity_changed"),
])
def test_resume_rejects_broken_capture_instead_of_resetting_it(capture_source,statement,expected):
    install(capture_source)
    with capture_source.begin() as conn:
        conn.exec_driver_sql(statement)
    with pytest.raises(RuntimeError,match=expected):
        install(capture_source)


def test_existing_writer_precedes_capture_and_new_writes_are_captured(capture_source):
    with capture_source.connect() as writer:
        with writer.begin():
            insert(writer,"inflight")
            with pytest.raises(DBAPIError):
                install(capture_source)
        assert install(capture_source)["capture_active"]
    assert ids(capture_source,SOURCE) == ["existing","inflight"]
    assert ids(capture_source,QUEUE) == []
    with capture_source.begin() as conn:
        insert(conn,"after")
    assert ids(capture_source,QUEUE) == ["after"]


def test_capture_serializes_migration_actors_and_requires_transaction(capture_source):
    with capture_source.connect() as conn:
        with pytest.raises(ValueError,match="caller_transaction_required"):
            install_capture(conn)
    with capture_source.begin() as owner:
        owner.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),{"name":LOCK})
        with pytest.raises(RuntimeError,match="migration_busy"):
            install(capture_source)
    assert install(capture_source)["capture_active"]


def test_writer_needs_no_private_queue_permission(capture_source):
    install(capture_source)
    role = "qt_capture_writer_"+uuid4().hex[:16]
    with capture_source.begin() as conn:
        conn.exec_driver_sql(f"CREATE ROLE {role} NOLOGIN")
        conn.exec_driver_sql(f"GRANT USAGE ON SCHEMA market TO {role}")
        conn.exec_driver_sql(f"GRANT INSERT ON {SOURCE} TO {role}")
    try:
        with capture_source.begin() as conn:
            conn.exec_driver_sql(f"SET LOCAL ROLE {role}")
            insert(conn,"restricted-writer")
        with pytest.raises(DBAPIError,match="permission denied"):
            with capture_source.begin() as conn:
                conn.exec_driver_sql(f"SET LOCAL ROLE {role}")
                conn.exec_driver_sql(f"SELECT * FROM {QUEUE}")
        assert ids(capture_source,QUEUE) == ["restricted-writer"]
    finally:
        with capture_source.begin() as conn:
            conn.exec_driver_sql(f"DROP OWNED BY {role}")
            conn.exec_driver_sql(f"DROP ROLE {role}")

"""Final copy verification never treats counts as proof or leaves a failed fence."""
import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_copy as copy
from scripts.db.fact_header_v2_capture import SCHEMA, QUEUE
from tests.test_market_data.test_fact_header_copy_db import (
    source, _finish, _insert, _headers, _frozen_records,
)
from tests.test_market_data.test_fact_storage_tiers_db import storage
from tests.test_market_data.tiered_v1_fixture import stage_shadow_handoff_fixture

pytestmark = pytest.mark.db


def _prepare(source):
    engine = source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
    _finish(engine)
    with engine.begin() as conn:
        copy.enable_identity_capture(conn)
    return engine


def test_exact_verification_rejects_damaged_headers_ids_and_routing(source):
    engine = _prepare(source)
    with engine.begin() as conn:
        source_before = _headers(conn, copy.SOURCE)
        with copy.verified_copy(conn, page_rows=2) as report:
            assert report["verified_header_rows"] == report["verified_identity_rows"] == 7
            assert not report["migration_ready"]
            assert report["source_authoritative"]
        # Even when row counts still match, altered content must refuse handoff.
        changes = [
            (f"UPDATE {SCHEMA}.fact_versions SET row_hash=repeat('e',64)", "headers"),
            (f"DELETE FROM {SCHEMA}.fact_versions WHERE storage_day=(SELECT min(storage_day) FROM {SCHEMA}.fact_versions)", "headers"),
            (f"UPDATE {SCHEMA}.fact_identities SET observation_key=observation_key||'-wrong'", "identities"),
            (f"INSERT INTO {SCHEMA}.fact_identities VALUES(repeat('f',64),current_date,1,'orphan',1)", "identities"),
            (f"DELETE FROM {SCHEMA}.fact_header_series_days", "routing_incomplete"),
            (f"UPDATE {SCHEMA}.fact_header_series_days SET min_observation_time=max_observation_time", "routing_incomplete"),
            (f"DELETE FROM {SCHEMA}.fact_header_partitions", "partition_catalog_mismatch"),
        ]
        for sql, expected in changes:
            with conn.begin_nested() as damage:
                conn.exec_driver_sql(sql)
                with pytest.raises(RuntimeError, match=expected):
                    with copy.verified_copy(conn, page_rows=2):
                        pytest.fail("damaged copy admitted")
                assert _headers(conn, copy.SOURCE) == source_before
                damage.rollback()
        # Extra target records also fail even though every original is present.
        with conn.begin_nested() as temporary:
            extra = _insert(conn, source, "target-only")
            temporary.rollback()
        with conn.begin_nested() as damage:
            copy._copy_rows(conn, [extra])
            with pytest.raises(RuntimeError, match="headers"):
                with copy.verified_copy(conn, page_rows=2):
                    pytest.fail("extra target record admitted")
            damage.rollback()
        with copy.verified_copy(conn, page_rows=1) as report:
            assert report["verified_header_rows"] == 7
        assert _frozen_records(conn) == source.frozen_before
    assert source.archive_path.read_bytes() == source.archive_bytes


def test_final_verification_refuses_pending_rows_and_busy_source(source):
    engine = _prepare(source)
    with engine.begin() as writer:
        _insert(writer, source, "writer-before-fence")
        with engine.begin() as verifier:
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                with copy.verified_copy(verifier):
                    pytest.fail("active writer admitted")
            assert verifier.scalar(text("SELECT 1")) == 1
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="copy_incomplete"):
            with copy.verified_copy(conn):
                pytest.fail("pending capture admitted")
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {QUEUE})"))
    _finish(engine)
    with engine.begin() as conn:
        with copy.verified_copy(conn, page_rows=2) as report:
            assert report["verified_header_rows"] == 8
            # Verification blocks writers, not ordinary recent/history reads.
            with engine.begin() as reader:
                reader.exec_driver_sql("SET LOCAL lock_timeout='250ms'")
                for relation in (copy.SOURCE, SCHEMA+".fact_versions", SCHEMA+".fact_identities"):
                    assert reader.scalar(text(f"SELECT count(*) FROM {relation}")) == 8
                # An already active reader can continue, while the actual switch
                # refuses rather than waiting indefinitely or changing tables.
                with pytest.raises(DBAPIError, match="could not obtain lock"):
                    stage_shadow_handoff_fixture(conn, source)
                assert conn.scalar(text("SELECT to_regclass('market.fact_identities')")) is None
                assert conn.scalar(text("SELECT count(*) FROM market.fact_versions")) == 8
            # Source and private target remain fenced through caller handoff.
            for relation in (copy.SOURCE, SCHEMA+".fact_versions"):
                with pytest.raises(DBAPIError, match="lock timeout"), engine.begin() as contender:
                    contender.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
                    contender.exec_driver_sql(f"LOCK TABLE {relation} IN ROW EXCLUSIVE MODE")
        # Successful savepoint completion does not release the transaction fence.
        with pytest.raises(DBAPIError, match="lock timeout"), engine.begin() as contender:
            contender.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
            _insert(contender, source, "blocked-until-commit")
    with engine.begin() as writer:
        _insert(writer, source, "after-verified-transaction")


def test_failed_verification_releases_its_fence_without_rolling_back_outer_work(source):
    engine = _prepare(source)
    interrupted = [False]

    def fail_page(conn, cursor, statement, parameters, context, executemany):
        if not interrupted[0] and statement.startswith("SELECT "+",".join(copy.HEADER_COLUMNS)):
            interrupted[0] = True
            raise RuntimeError("injected_verification_interruption")

    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TEMP TABLE unrelated_work(value integer)")
        conn.exec_driver_sql("INSERT INTO unrelated_work VALUES(42)")
        event.listen(conn, "after_cursor_execute", fail_page)
        try:
            with pytest.raises(RuntimeError, match="injected_verification_interruption"):
                with copy.verified_copy(conn, page_rows=2):
                    pytest.fail("interrupted verification admitted")
        finally:
            event.remove(conn, "after_cursor_execute", fail_page)
        assert interrupted[0]
        assert conn.scalar(text("SELECT value FROM unrelated_work")) == 42
        # No verification lock survives its failed savepoint. Prove real intake
        # commits while this outer transaction remains open.
        with engine.begin() as writer:
            writer.exec_driver_sql("SET LOCAL lock_timeout='1s'")
            _insert(writer, source, "after-failed-verification")
        assert _frozen_records(conn) == source.frozen_before
    _finish(engine)
    with engine.begin() as conn:
        with copy.verified_copy(conn, page_rows=1) as report:
            assert report["verified_header_rows"] == 8

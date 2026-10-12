"""PG15 capability proof for bounded payload-reference handoff; no operator."""
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from tests.test_market_data.migration_test_support import fresh_migration_database

pytestmark = pytest.mark.db


@pytest.fixture
def reference_source():
    with fresh_migration_database("header_fk_handoff", install_extensions=False) as dsn:
        engine = create_engine(dsn, future=True)
        try:
            with engine.begin() as conn:
                assert conn.scalar(text("SHOW server_version_num")).startswith("15")
                conn.exec_driver_sql("""
                    CREATE TABLE old_headers(id varchar(64) PRIMARY KEY);
                    CREATE TABLE new_identities(id varchar(64) PRIMARY KEY);
                    INSERT INTO old_headers VALUES('one'),('two'),('three'),('four'),('old-only');
                    INSERT INTO new_identities SELECT id FROM old_headers WHERE id!='old-only';
                    CREATE TABLE payloads(day integer NOT NULL,id varchar(64) NOT NULL,
                        PRIMARY KEY(day,id),
                        CONSTRAINT original_reference FOREIGN KEY(id)
                            REFERENCES old_headers(id) ON DELETE RESTRICT)
                        PARTITION BY RANGE(day);
                    CREATE TABLE payloads_1 PARTITION OF payloads FOR VALUES FROM(1) TO(2);
                    CREATE TABLE payloads_2 PARTITION OF payloads FOR VALUES FROM(2) TO(3);
                    INSERT INTO payloads VALUES(1,'one'),(2,'two');
                """)
            yield engine
        finally:
            engine.dispose()


def references(conn):
    return {row["conrelid"]:dict(row) for row in conn.execute(text("""
        SELECT oid::bigint,conrelid::regclass::text,conparentid::bigint,convalidated
        FROM pg_constraint WHERE contype='f' AND confrelid='new_identities'::regclass
    """)).mappings()}


def stage_leaves(engine):
    for day in (1,2):
        with engine.begin() as conn:
            conn.exec_driver_sql(f"""
                ALTER TABLE payloads_{day} ADD CONSTRAINT staged_identity_reference
                    FOREIGN KEY(id) REFERENCES new_identities(id) ON DELETE RESTRICT NOT VALID
            """)


def test_prevalidated_payload_references_are_adopted_without_replacement(reference_source):
    engine=reference_source
    # PG15 does not support a NOT VALID reference directly on the parent.
    with engine.begin() as conn:
        with pytest.raises(DBAPIError,match="cannot add NOT VALID foreign key"):
            with conn.begin_nested():
                conn.exec_driver_sql("""
                    ALTER TABLE payloads ADD CONSTRAINT unsupported_reference
                    FOREIGN KEY(id) REFERENCES new_identities(id) NOT VALID
                """)
        assert conn.scalar(text("SELECT count(*) FROM payloads"))==2
    stage_leaves(engine)
    # Validate while a collector transaction holds an INSERT lock, then prove
    # another collector can INSERT before the validation transaction commits.
    with engine.begin() as writer:
        writer.exec_driver_sql("INSERT INTO payloads VALUES(1,'three')")
        with engine.begin() as validator:
            validator.exec_driver_sql("SET LOCAL lock_timeout='1s'")
            for day in (1,2):
                validator.exec_driver_sql(
                    f"ALTER TABLE payloads_{day} VALIDATE CONSTRAINT staged_identity_reference")
            with engine.begin() as second_writer:
                second_writer.exec_driver_sql("SET LOCAL lock_timeout='1s'")
                second_writer.exec_driver_sql("INSERT INTO payloads VALUES(2,'four')")
            before=references(validator)
            assert set(before)=={"payloads_1","payloads_2"}
            assert all(row["convalidated"] and not row["conparentid"] for row in before.values())
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            ALTER TABLE payloads ADD CONSTRAINT final_identity_reference
                FOREIGN KEY(id) REFERENCES new_identities(id) ON DELETE RESTRICT
        """)
        after=references(conn)
        parent=after.pop("payloads")
        assert parent["convalidated"]
        assert set(after)==set(before)
        # Reusing validated child constraint OIDs is PostgreSQL's attachment path,
        # not a newly scheduled validation. This is not a duration benchmark.
        for name,row in after.items():
            assert row["oid"]==before[name]["oid"]
            assert row["convalidated"] and row["conparentid"]==parent["oid"]
        conn.exec_driver_sql("ALTER TABLE payloads DROP CONSTRAINT original_reference")
        assert conn.scalar(text("SELECT count(*) FROM payloads"))==4
        with pytest.raises(DBAPIError,match="foreign key constraint"):
            with conn.begin_nested():
                conn.exec_driver_sql("INSERT INTO payloads VALUES(1,'old-only')")
        conn.exec_driver_sql("INSERT INTO new_identities VALUES('new-only')")
        conn.exec_driver_sql("INSERT INTO payloads VALUES(1,'new-only')")
        # Future normal partitions inherit the final parent reference.
        conn.exec_driver_sql("CREATE TABLE payloads_3 PARTITION OF payloads FOR VALUES FROM(3) TO(4)")
        with pytest.raises(DBAPIError,match="foreign key constraint"):
            with conn.begin_nested():
                conn.exec_driver_sql("INSERT INTO payloads VALUES(3,'old-only')")


def test_unvalidated_leaf_cannot_be_adopted_as_verified(reference_source):
    engine=reference_source
    with engine.begin() as conn:
        conn.exec_driver_sql("INSERT INTO payloads VALUES(1,'old-only')")
    stage_leaves(engine)
    with engine.begin() as conn:
        before=references(conn)
        with pytest.raises(DBAPIError,match="foreign key constraint"):
            with conn.begin_nested():
                conn.exec_driver_sql("""
                    ALTER TABLE payloads ADD CONSTRAINT final_identity_reference
                        FOREIGN KEY(id) REFERENCES new_identities(id) ON DELETE RESTRICT
                """)
        assert references(conn)==before
        assert conn.scalar(text("SELECT count(*) FROM payloads"))==3
        assert conn.scalar(text("""
            SELECT convalidated FROM pg_constraint
            WHERE conrelid='payloads'::regclass AND conname='original_reference'
        """))

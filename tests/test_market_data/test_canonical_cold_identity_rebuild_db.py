from datetime import date
import json
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from portal.backend.db.fact_identity_schema import (
    REGISTER_IDENTITY_BODY, REQUIRE_HEADER_BODY, REJECT_IDENTITY_MUTATION_BODY,
)
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.db import fact_header_v2_online_proof as online
from scripts.db.canonical_cold_identity_rebuild import (
    PREVIOUS, SOURCE, STAGED, finalize_identity_suffix, prepare_identity_suffix, swap_identity_suffix,
)


pytestmark = pytest.mark.db
DAY = date(2026, 10, 31)
RETAINED = f"{online.SCHEMA}.fact_versions"


@pytest.fixture
def identity_database():
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        pytest.skip("requires disposable cold catalog PostgreSQL")
    url = make_url(dsn)
    if url.host != "127.0.0.1" or url.database != "qt_cold_catalog_test":
        pytest.skip("requires dedicated loopback fixture database")
    engine = create_engine(url)
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            scope = conn.begin_nested()
            try:
                conn.exec_driver_sql("""
                    CREATE SCHEMA market;
                    CREATE SCHEMA qt_fact_header_cutover_v2;
                    CREATE TABLE market.fact_identities (
                        id varchar(64) PRIMARY KEY, storage_day date NOT NULL, series_id bigint NOT NULL,
                        observation_key varchar(512) NOT NULL, revision int NOT NULL,
                        CONSTRAINT ck_market_fact_identity_key CHECK (observation_key <> ''),
                        CONSTRAINT ck_market_fact_identity_revision CHECK (revision > 0),
                        CONSTRAINT uq_market_fact_identity_day UNIQUE (id, storage_day),
                        CONSTRAINT uq_market_fact_identity_revision UNIQUE (series_id, observation_key, revision)
                    );
                    CREATE TABLE market.fact_versions (
                        id varchar(64), storage_day date, series_id bigint, observation_key varchar(512),
                        revision int,
                        CONSTRAINT fk_market_fact_identity_day FOREIGN KEY (id,storage_day)
                            REFERENCES market.fact_identities(id,storage_day) ON DELETE RESTRICT
                    ) PARTITION BY RANGE (storage_day);
                    CREATE TABLE market.fact_versions_prefix PARTITION OF market.fact_versions
                        FOR VALUES FROM (MINVALUE) TO ('2026-10-31');
                    CREATE TABLE market.fact_versions_suffix PARTITION OF market.fact_versions
                        FOR VALUES FROM ('2026-10-31') TO ('2026-11-02');
                    CREATE TABLE market.fact_hot_payloads (
                        id varchar(64), storage_day date,
                        CONSTRAINT fact_hot_payloads_id_fkey FOREIGN KEY (id)
                            REFERENCES market.fact_identities(id) ON DELETE RESTRICT
                    ) PARTITION BY RANGE (storage_day);
                    CREATE TABLE market.fact_payload_suffix PARTITION OF market.fact_hot_payloads
                        FOR VALUES FROM ('2026-10-31') TO ('2026-11-02');
                    CREATE TABLE qt_fact_header_cutover_v2.fact_versions
                        (LIKE market.fact_versions) PARTITION BY RANGE (storage_day);
                    ALTER TABLE qt_fact_header_cutover_v2.fact_versions
                        ADD CONSTRAINT fk_market_fact_identity_day FOREIGN KEY (id,storage_day)
                        REFERENCES market.fact_identities(id,storage_day) ON DELETE RESTRICT;
                    CREATE FUNCTION market.reject_immutable_mutation() RETURNS trigger LANGUAGE plpgsql
                    AS $guard$ BEGIN RAISE EXCEPTION 'refused'; END; $guard$;
                    CREATE FUNCTION qt_fact_header_cutover_v2.online_guard_truncate() RETURNS trigger LANGUAGE plpgsql
                    AS $guard$ BEGIN RAISE EXCEPTION 'truncate refused'; END; $guard$;
                    CREATE TRIGGER trg_retained_source_closed BEFORE INSERT ON qt_fact_header_cutover_v2.fact_versions
                        FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation();
                    ALTER TABLE qt_fact_header_cutover_v2.fact_versions ENABLE ALWAYS TRIGGER trg_retained_source_closed;
                """)
                for name, body in (
                    ("register_fact_identity", REGISTER_IDENTITY_BODY),
                    ("require_fact_identity_header", REQUIRE_HEADER_BODY),
                    ("reject_fact_identity_mutation", REJECT_IDENTITY_MUTATION_BODY),
                ):
                    conn.execute(text(f"CREATE FUNCTION market.{name}() RETURNS trigger LANGUAGE plpgsql AS $b$" + body + "$b$"))
                conn.exec_driver_sql("""
                    CREATE TRIGGER trg_register_fact_identity BEFORE INSERT ON market.fact_versions
                      FOR EACH ROW EXECUTE FUNCTION market.register_fact_identity();
                    ALTER TABLE market.fact_versions ENABLE ALWAYS TRIGGER trg_register_fact_identity;
                    CREATE CONSTRAINT TRIGGER trg_require_fact_identity_header AFTER INSERT ON market.fact_identities
                      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION market.require_fact_identity_header();
                    ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_require_fact_identity_header;
                    CREATE TRIGGER trg_guard_fact_identities BEFORE UPDATE OR DELETE ON market.fact_identities
                      FOR EACH ROW EXECUTE FUNCTION market.reject_fact_identity_mutation();
                    ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_guard_fact_identities;
                    CREATE TRIGGER trg_reject_mutation_fact_identities BEFORE UPDATE OR DELETE ON market.fact_identities
                      FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation();
                    CREATE TRIGGER trg_qt_online_no_truncate BEFORE TRUNCATE ON market.fact_identities
                      FOR EACH STATEMENT EXECUTE FUNCTION qt_fact_header_cutover_v2.online_guard_truncate();
                    ALTER TABLE market.fact_identities ENABLE ALWAYS TRIGGER trg_qt_online_no_truncate;
                    INSERT INTO market.fact_versions VALUES ('prefix','2026-10-30',1,'p',1),
                                                           ('suffix','2026-10-31',1,'s',1);
                    INSERT INTO market.fact_hot_payloads VALUES ('suffix','2026-10-31');
                    SET CONSTRAINTS ALL IMMEDIATE;
                    SET CONSTRAINTS ALL DEFERRED;
                """)
                identity = conn.scalar(text("""
                    SELECT c.system_identifier::text || '/' || d.oid::text
                    FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()
                """))
                yield conn, identity
            finally:
                scope.rollback()
    finally:
        engine.dispose()


def _fence(conn):
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),
                 {"name": _LIFECYCLE_LOCK_NAME})


def _install_forward_retained_proof(conn):
    relation_oids = conn.execute(text(f"""
        SELECT 'market.fact_versions_prefix'::regclass::oid, '{RETAINED}'::regclass::oid,
               'market.fact_versions'::regclass::oid
    """)).one()
    original_oid, parent_oid, current_oid = relation_oids
    assert original_oid != current_oid
    binding = {
        "source_oids": {online.headers.SOURCE: original_oid, online.raw.SOURCE: current_oid},
        "targets": {"fact_versions": online.headers._shape(conn, "fact_versions")},
    }
    assert binding["targets"]["fact_versions"][0] == parent_oid
    bodies = online._bodies(binding)
    for role in ("header", "truncate"):
        name = f"{online.SCHEMA}.online_guard_{role}()"
        conn.execute(text(f"CREATE OR REPLACE FUNCTION {name} RETURNS trigger "
                          "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $qt$"
                          + bodies[role] + "$qt$"))
        conn.exec_driver_sql(f"REVOKE ALL ON FUNCTION {name} FROM PUBLIC")
    conn.exec_driver_sql(f"""
        DROP TRIGGER trg_retained_source_closed ON {RETAINED};
        CREATE TRIGGER {online.ROW_TRIGGER} BEFORE INSERT OR UPDATE OR DELETE ON {RETAINED}
          FOR EACH ROW EXECUTE FUNCTION {online.SCHEMA}.online_guard_header();
        ALTER TABLE {RETAINED} ENABLE ALWAYS TRIGGER {online.ROW_TRIGGER};
        CREATE TABLE {online.STATE}(id integer PRIMARY KEY CHECK(id=1), binding jsonb NOT NULL,
                                    leaf_bindings jsonb NOT NULL DEFAULT '{{}}'::jsonb)
    """)
    online._install_truncate(conn, RETAINED)
    conn.execute(text(f"INSERT INTO {online.STATE}(id,binding) VALUES (1,CAST(:binding AS jsonb))"),
                 {"binding": json.dumps(binding)})
    return original_oid


def test_exact_suffix_rebinds_foreign_keys_and_rollback_restores_oid(identity_database):
    conn, identity = identity_database
    parameters = {"expected_database": identity, "cutoff": DAY}
    stage = prepare_identity_suffix(conn, **parameters)
    assert stage["retained_rows"] == 1
    with pytest.raises(RuntimeError, match="cold_identity_writer_fence_not_held"):
        with conn.begin_nested():
            swap_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("""
        INSERT INTO market.fact_versions VALUES ('late','2026-11-01',1,'l',1);
        INSERT INTO market.fact_hot_payloads VALUES ('late','2026-11-01');
        SET CONSTRAINTS ALL IMMEDIATE;
        SET CONSTRAINTS ALL DEFERRED;
    """)
    _fence(conn)
    with pytest.raises(RuntimeError, match="cold_identity_source_header_prefix_still_present"):
        with conn.begin_nested():
            swap_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("DROP TABLE market.fact_versions_prefix")
    switch = conn.begin_nested()
    receipt = swap_identity_suffix(conn, **parameters)
    assert (receipt["old_oid"], receipt["new_oid"], receipt["retained_rows"]) == (
        stage["old_oid"], stage["new_oid"], 2,
    )
    assert conn.execute(text(f"SELECT id FROM {SOURCE} ORDER BY id")).scalars().all() == ["late", "suffix"]
    assert conn.execute(text(f"SELECT id FROM {PREVIOUS} ORDER BY id")).scalars().all() == ["late", "prefix", "suffix"]
    assert conn.scalar(text("SELECT to_regclass('market.ix_cold_hot_identity_day_id') IS NOT NULL"))
    assert set(receipt["rebound_foreign_keys"]) == {
        "market.fact_versions", "market.fact_hot_payloads", "qt_fact_header_cutover_v2.fact_versions",
    }
    with pytest.raises(DBAPIError, match="immutable canonical identity"):
        with conn.begin_nested():
            conn.exec_driver_sql(f"DELETE FROM {SOURCE} WHERE id='suffix'")
    with pytest.raises(DBAPIError, match="truncate refused"):
        with conn.begin_nested():
            conn.exec_driver_sql(f"TRUNCATE {PREVIOUS}")
    switch.rollback()
    assert conn.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == stage["old_oid"]
    assert conn.scalar(text(f"SELECT '{STAGED}'::regclass::oid")) == stage["new_oid"]
    assert conn.scalar(text(f"SELECT to_regclass('{PREVIOUS}') IS NULL"))


def test_forward_retained_guard_remains_bound_after_original_header_drop(identity_database):
    conn, identity = identity_database
    original_oid = _install_forward_retained_proof(conn)
    parameters = {"expected_database": identity, "cutoff": DAY}
    stage = prepare_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("DROP TABLE market.fact_versions_prefix")
    assert not conn.scalar(text("SELECT EXISTS(SELECT FROM pg_class WHERE oid=:oid)"), {"oid": original_oid})
    _fence(conn)
    result = swap_identity_suffix(conn, expected_retired_legacy_oid=original_oid, **parameters)
    assert result["new_oid"] == stage["new_oid"]
    assert RETAINED in result["rebound_foreign_keys"]
    with pytest.raises(DBAPIError, match="online_copy_proof_truncate_refused"):
        with conn.begin_nested():
            conn.exec_driver_sql(f"TRUNCATE {RETAINED}")


@pytest.mark.parametrize(("statement", "reason"), [
    (f"ALTER TABLE {RETAINED} DISABLE TRIGGER {online.ROW_TRIGGER}", "target_guard_changed"),
    (f"DROP TRIGGER {online.TRUNCATE_TRIGGER} ON {RETAINED}", "target_guard_changed"),
    (f"ALTER FUNCTION {online.SCHEMA}.online_guard_header() SECURITY INVOKER", "function_changed: header"),
    (f"CREATE OR REPLACE FUNCTION {online.SCHEMA}.online_guard_header() RETURNS trigger LANGUAGE plpgsql "
     "SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN RETURN NEW; END; $$", "function_changed: header"),
    (f"GRANT EXECUTE ON FUNCTION {online.SCHEMA}.online_guard_header() TO PUBLIC", "function_acl_changed: header"),
    (f"DROP TABLE {online.STATE}", "parent_proof_missing"),
    (f"UPDATE {online.STATE} SET binding = jsonb_set(binding, '{{targets,fact_versions,0}}', '1')",
     "parent_proof_mismatch"),
])
def test_forward_retained_guard_rejects_drift(identity_database, statement, reason):
    conn, identity = identity_database
    original_oid = _install_forward_retained_proof(conn)
    parameters = {"expected_database": identity, "cutoff": DAY}
    stage = prepare_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("DROP TABLE market.fact_versions_prefix")
    _fence(conn)
    conn.exec_driver_sql(statement)
    with pytest.raises(RuntimeError, match=reason):
        with conn.begin_nested():
            swap_identity_suffix(conn, expected_retired_legacy_oid=original_oid, **parameters)
    assert conn.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == stage["old_oid"]


def test_forward_retained_guard_requires_audited_original_oid_to_agree(identity_database):
    conn, identity = identity_database
    original_oid = _install_forward_retained_proof(conn)
    parameters = {"expected_database": identity, "cutoff": DAY}
    stage = prepare_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("DROP TABLE market.fact_versions_prefix")
    _fence(conn)
    with pytest.raises(RuntimeError, match="parent_proof_mismatch"):
        with conn.begin_nested():
            swap_identity_suffix(conn, expected_retired_legacy_oid=original_oid + 1, **parameters)
    assert conn.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == stage["old_oid"]


def test_exact_retirement_is_oid_bound_restrict_only_and_idempotent(identity_database):
    conn, identity = identity_database
    parameters = {"expected_database": identity, "cutoff": DAY}
    prepare_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("DROP TABLE market.fact_versions_prefix")
    _fence(conn)
    swap = swap_identity_suffix(conn, **parameters)
    parameters.update(old_oid=swap["old_oid"], new_oid=swap["new_oid"],
                      verified_recovery_digest="e" * 64)
    with pytest.raises(RuntimeError, match="retirement_receipt_mismatch"):
        with conn.begin_nested():
            finalize_identity_suffix(conn, **(parameters | {"old_oid": 3}))
    conn.exec_driver_sql(f"CREATE VIEW {PREVIOUS}_reader AS SELECT id FROM {PREVIOUS}")
    with pytest.raises(RuntimeError, match="incoming_dependency_unreviewed"):
        with conn.begin_nested():
            finalize_identity_suffix(conn, **parameters)
    conn.exec_driver_sql(f"DROP VIEW {PREVIOUS}_reader")
    with conn.begin_nested():
        assert not finalize_identity_suffix(conn, **parameters)["already_finalized"]
    assert conn.scalar(text(f"SELECT to_regclass('{PREVIOUS}') IS NULL"))
    assert finalize_identity_suffix(conn, **parameters)["already_finalized"]
    with pytest.raises(RuntimeError, match="retirement_completion_conflict"):
        with conn.begin_nested():
            finalize_identity_suffix(conn, **(parameters | {"verified_recovery_digest": "f" * 64}))


@pytest.mark.parametrize("obstacle", ["bad_identity", "extra_fk", "retained_child", "external_view"])
def test_registry_rebuild_refuses_unreviewed_data_or_dependency(identity_database, obstacle):
    conn, identity = identity_database
    parameters = {"expected_database": identity, "cutoff": DAY}
    stage = prepare_identity_suffix(conn, **parameters)
    conn.exec_driver_sql("DROP TABLE market.fact_versions_prefix")
    _fence(conn)
    statements = {
        "bad_identity": (f"UPDATE {STAGED} SET observation_key='other'", "retained_set_mismatch"),
        "extra_fk": (
            f"CREATE TABLE market.extra_dependency (id varchar(64) REFERENCES {SOURCE}(id))",
            "incoming_foreign_keys_unreviewed",
        ),
        "retained_child": ("""CREATE TABLE qt_fact_header_cutover_v2.child
            PARTITION OF qt_fact_header_cutover_v2.fact_versions
            FOR VALUES FROM ('2026-10-31') TO ('2026-11-01')""", "retained_cutover_parent_not_empty"),
        "external_view": (f"CREATE VIEW market.unreviewed_dependency AS SELECT id FROM {SOURCE}",
                          "incoming_dependency_unreviewed"),
    }
    statement, reason = statements[obstacle]
    conn.exec_driver_sql(statement)
    with pytest.raises(RuntimeError, match=reason):
        with conn.begin_nested():
            swap_identity_suffix(conn, **parameters)
    assert conn.scalar(text(f"SELECT '{SOURCE}'::regclass::oid")) == stage["old_oid"]

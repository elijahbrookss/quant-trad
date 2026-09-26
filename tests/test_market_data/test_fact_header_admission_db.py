"""Known-v1 admission uses real source schema; unexpected dependencies refuse."""
import pytest
from sqlalchemy import text

from scripts.db.fact_header_v2_admission import assert_v1_source_admission
from scripts.db import fact_header_v2_copy as copy
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_copy_db import source, _headers
from tests.test_market_data.test_fact_storage_tiers_db import storage

pytestmark = pytest.mark.db


def test_known_source_is_admitted_and_changed_contracts_are_refused(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
        report=assert_v1_source_admission(conn)
        assert report["source_schema_admitted"] and not report["migration_ready"]
    changes=[
        ("default", "ALTER TABLE market.fact_versions ALTER COLUMN market_commit_seq SET DEFAULT 1",
         "columns_or_defaults"),
        ("check", "ALTER TABLE market.fact_versions DROP CONSTRAINT ck_market_fact_row_hash",
         "constraints_changed"),
        ("index", "DROP INDEX market.ix_market_fact_series_known",
         "indexes_changed"),
        ("guard", """CREATE OR REPLACE FUNCTION market.assert_fact_version_valid()
            RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$""",
         "function_changed"),
        ("disabled", "ALTER TABLE market.fact_versions DISABLE TRIGGER trg_assert_fact_version_valid",
         "trigger_changed"),
        ("unexpected_trigger", """CREATE TRIGGER unexpected_source_trigger BEFORE INSERT
            ON market.fact_versions FOR EACH ROW EXECUTE FUNCTION market.assert_fact_version_valid()""",
         "trigger_set_changed"),
        ("external_reference", "CREATE TABLE public.unhandled_reference(id varchar(64) REFERENCES market.fact_versions(id))",
         "incoming_reference_set_changed"),
        ("missing_reference", """ALTER TABLE market.fact_archive_material_aliases
            DROP CONSTRAINT fact_archive_material_aliases_fact_version_id_fkey""",
         "incoming_reference_set_changed"),
        ("direct_view", "CREATE VIEW public.unhandled_view AS SELECT id FROM market.fact_versions",
         "dependent_views_changed"),
        ("indirect_view", "CREATE VIEW public.unhandled_projection AS SELECT id FROM market.fact_rows",
         "dependent_views_changed"),
        ("rowtype_function", """CREATE FUNCTION public.unhandled_header_result() RETURNS SETOF market.fact_versions
            LANGUAGE SQL AS 'SELECT * FROM market.fact_versions'""",
         "dependent_functions_changed"),
        ("projection_function", """CREATE FUNCTION public.unhandled_projection_result()
            RETURNS SETOF market.fact_rows LANGUAGE SQL AS 'SELECT * FROM market.fact_rows'""",
         "dependent_functions_changed"),
        ("payload_guard", "ALTER TABLE market.fact_hot_payloads DISABLE TRIGGER trg_assert_fact_hot_payload_valid",
         "payload_trigger_changed"),
        ("payload_child_guard", "ALTER TABLE market.fact_hot_payloads_"+source.open_day.strftime("%Y%m%d")+
         " DISABLE TRIGGER trg_assert_fact_hot_payload_valid", "payload_trigger_changed"),
        ("active_v2_object", "CREATE TABLE market.fact_header_series_days(unexpected integer)",
         "active_v2_layout_present"),
        ("active_v2_certificate", """INSERT INTO market.fact_storage_state(layout_version,state,completed_at)
            VALUES('market.fact_storage_tiers.v2','ready',clock_timestamp())""",
         "active_v2_layout_present"),
        ("rls", "ALTER TABLE market.fact_versions ENABLE ROW LEVEL SECURITY",
         "ownership_or_policy_changed"),
        ("acl", "GRANT SELECT ON market.fact_versions TO PUBLIC",
         "ownership_or_policy_changed"),
        ("owned_sequence", "ALTER SEQUENCE market.fact_commit_seq OWNED BY market.fact_versions.market_commit_seq",
         "commit_sequence_must_be_standalone"),
    ]
    for label,sql,message in changes:
        with engine.connect() as conn:
            transaction=conn.begin()
            try:
                conn.exec_driver_sql(sql)
                with pytest.raises(RuntimeError,match=message):
                    assert_v1_source_admission(conn)
                assert _headers(conn,copy.SOURCE)==source.source_before,label
            finally:
                transaction.rollback()
    with engine.begin() as conn:
        assert assert_v1_source_admission(conn)["source_schema_admitted"]


def test_refused_preparation_cannot_commit_capture_or_shadow(source):
    engine=source.database._engine
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE VIEW public.unhandled_projection AS SELECT id FROM market.fact_rows")
        with pytest.raises(RuntimeError,match="dependent_views_changed"):
            copy.prepare_copy(conn)
        assert conn.scalar(text("SELECT to_regnamespace(:schema)"),{"schema":SCHEMA}) is None
        assert not conn.scalar(text("""
            SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='market.fact_versions'::regclass
                           AND tgname='trg_qt_header_v2_capture')
        """))
        assert _headers(conn,copy.SOURCE)==source.source_before


def test_dependency_added_after_preparation_blocks_preparation_retry(source):
    engine=source.database._engine
    with engine.begin() as conn:
        copy.prepare_copy(conn)
        conn.exec_driver_sql("CREATE VIEW public.unhandled_projection AS SELECT id FROM market.fact_rows")
    with engine.begin() as conn:
        with pytest.raises(RuntimeError,match="dependent_views_changed"):
            copy.prepare_copy(conn)
        assert conn.scalar(text(f"SELECT count(*) FROM {SCHEMA}.fact_versions"))==0
        assert _headers(conn,copy.SOURCE)==source.source_before

"""Exact page proof survives live append/correction capture, never target drift."""
import os

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_online_proof as proof
from scripts.db import fact_header_v2_copy as headers, raw_mapping_v2_copy as raw
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_header_online_db import _prepare, _advance
from tests.test_market_data.test_fact_header_copy_placement_db import placed, source
from tests.test_market_data.test_fact_header_copy_db import _insert, _frozen_records
from tests.test_market_data.test_fact_storage_tiers_db import storage
from tests.test_market_data.tiered_v1_fixture import stage_shadow_handoff_fixture


pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned SSD/HDD storage-demo topology"),
]


def _protected(placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    with engine.begin() as conn:
        assert not proof.prepare(conn)["reused"]
        assert proof.prepare(conn)["reused"]
    _advance(engine, options)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    return engine, options


def test_protected_page_proof_has_exact_counts_without_final_row_scan(
        placed, tmp_path, monkeypatch):
    engine, options = _protected(placed, tmp_path, monkeypatch)
    with engine.begin() as conn:
        _insert(conn, placed, "proof-live-identity-mirror")
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="pending_capture"):
            with proof.verified_copy(conn):
                pytest.fail("uncopied committed tail admitted")
    _advance(engine, options)
    def no_full_scan(*args, **kwargs):
        pytest.fail("online proof called the full-history verifier")
    monkeypatch.setattr(headers, "_verified_pages", no_full_scan)
    with engine.begin() as conn:
        counts = [conn.scalar(text(f"SELECT count(*) FROM {name}"))
                  for name in (headers.SOURCE, raw.SOURCE)]
        frozen = _frozen_records(conn)
        with proof.verified_copy(conn) as report:
            assert report["verified_header_rows"] == counts[0]
            assert report["verified_identity_rows"] == counts[0]
            assert report["verified_lookup_rows"] == counts[1]
            assert not report["migration_ready"]
            print("PROTECTED_SQL_FENCE_SECONDS="+str(report["verification_seconds"]))
            # Fence prevents a late commit from crossing final reconciliation.
            with pytest.raises(DBAPIError), engine.begin() as writer:
                writer.exec_driver_sql("LOCK TABLE market.fact_versions IN ROW EXCLUSIVE MODE NOWAIT")
        assert _frozen_records(conn) == frozen == placed.frozen_before


def test_proof_rejects_mutation_wrong_insert_and_direct_leaf_truncate(
        placed, tmp_path, monkeypatch):
    engine, _ = _protected(placed, tmp_path, monkeypatch)
    with engine.connect() as conn:
        day = conn.scalar(text(f"SELECT min(storage_day) FROM {SCHEMA}.fact_versions"))
    leaf = SCHEMA + ".fact_versions_" + day.strftime("%Y%m%d")
    for sql, message in [
        (f"UPDATE {SCHEMA}.fact_versions SET row_hash=repeat('e',64)", "row_mutation"),
        (f"DELETE FROM {SCHEMA}.fact_versions", "row_mutation"),
        (f"UPDATE {SCHEMA}.fact_identities SET observation_key='wrong'", "row_mutation"),
        (f"UPDATE {raw.TARGET} SET raw_frame_sha256=repeat('e',64)", "row_mutation"),
        (f"DELETE FROM {SCHEMA}.fact_header_series_days", "routing_delete"),
        (f"UPDATE {SCHEMA}.fact_header_series_days SET min_observation_time=max_observation_time+interval '1 day'", "routing_shrink"),
        (f"DELETE FROM {SCHEMA}.fact_header_partitions", "partition_mutation"),
        (f"TRUNCATE {leaf}", "truncate_refused"),
        (f"TRUNCATE {raw.TARGET}", "truncate_refused"),
        (f"INSERT INTO {SCHEMA}.fact_identities VALUES(repeat('f',64),current_date,1,'orphan',1)", "insert_mismatch"),
    ]:
        with pytest.raises(DBAPIError, match=message), engine.begin() as conn:
            conn.exec_driver_sql(sql)
    with engine.begin() as conn:
        with proof.verified_copy(conn):
            pass
        with conn.begin_nested() as temporary:
            conn.exec_driver_sql(f"ALTER TABLE {leaf} DISABLE TRIGGER {proof.ROW_TRIGGER}")
            with pytest.raises(RuntimeError, match="target_guard_changed"):
                with proof.verified_copy(conn):
                    pytest.fail("disabled leaf protection admitted")
            temporary.rollback()
        with conn.begin_nested() as temporary:
            conn.exec_driver_sql(f"ALTER TABLE {SCHEMA}.fact_versions ENABLE ROW LEVEL SECURITY")
            with pytest.raises(RuntimeError, match="binding_changed"):
                with proof.verified_copy(conn):
                    pytest.fail("target visibility policy drift admitted")
            temporary.rollback()
        with conn.begin_nested() as temporary:
            conn.execute(text(f"UPDATE {proof.STATE} SET leaf_bindings='{{}}'::jsonb WHERE id=1"))
            with pytest.raises(RuntimeError, match="partition_identity_changed"):
                with proof.verified_copy(conn):
                    pytest.fail("lost leaf identity binding admitted")
            temporary.rollback()
        with proof.verified_copy(conn):
            pass


def test_proof_never_certifies_preexisting_copy_and_switch_removal_is_transactional(
        placed, tmp_path, monkeypatch):
    engine, options = _prepare(placed, tmp_path, monkeypatch)
    with engine.begin() as conn:
        with conn.begin_nested() as trial:
            headers.copy_page(conn, page_rows=1)
            with pytest.raises(RuntimeError, match="requires_new_empty_shadow"):
                proof.prepare(conn)
            trial.rollback()
        proof.prepare(conn)
    _advance(engine, options)
    with engine.begin() as conn:
        headers.enable_identity_capture(conn)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="live_switch_context_required"):
            proof.release_for_switch(conn)
        with pytest.raises(RuntimeError, match="injected abort"), conn.begin_nested():
            with proof.verified_copy(conn):
                proof.release_for_switch(conn)
                raise RuntimeError("injected abort")
        with proof.verified_copy(conn):
            pass
        # Existing tiny fixture performs its own full verification as an
        # independent cross-check before the actual rename. This is not a
        # claim that the production short-switch operator is integrated.
        with proof.verified_copy(conn):
            proof.release_for_switch(conn)
            stage_shadow_handoff_fixture(conn, placed, raw_mapping=True)
        assert _frozen_records(conn) == placed.frozen_before
    # New runtime insertion must not retain guards bound to the old source.
    from dataclasses import replace
    from datetime import timedelta
    fact = replace(placed.original_facts[0], observation_key="after-online-proof-switch",
                   observation_time=placed.original_facts[0].observation_time+timedelta(days=3))
    assert placed.repo.ingest_facts(series_id=placed.series_id,
                                   source_id=placed.source_id, facts=[fact]).inserted_count == 1


@pytest.mark.parametrize("scenario", [
    "test_online_pass_interrupted_page_rolls_back_without_losing_earlier_pages",
    "test_online_empty_observation_does_not_lose_late_commit_or_starve_raw",
])
def test_existing_online_concurrency_with_protected_shadow(placed, tmp_path, monkeypatch, scenario):
    from tests.test_market_data import test_fact_header_online_db as cases
    original = cases._prepare
    def prepare_protected(*args):
        engine, options = original(*args)
        with engine.begin() as conn:
            proof.prepare(conn)
        return engine, options
    monkeypatch.setattr(cases, "_prepare", prepare_protected)
    getattr(cases, scenario)(placed, tmp_path, monkeypatch)


def test_existing_switch_uses_live_proof_and_removes_guards_atomically(placed, tmp_path, monkeypatch):
    engine, _ = _protected(placed, tmp_path, monkeypatch)
    def no_full_scan(*args, **kwargs):
        pytest.fail("protected switch fell back to a full-history scan")
    monkeypatch.setattr(headers, "_verified_pages", no_full_scan)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="rollback after switch"), conn.begin_nested():
            stage_shadow_handoff_fixture(conn, placed, raw_mapping=True)
            raise RuntimeError("rollback after switch")
        assert conn.scalar(text("SELECT to_regclass(:name)"), {"name": proof.STATE}) is not None
        with headers.verified_copy(conn) as report:
            assert report["verification_method"] == "protected_exact_copy_pages"
            with raw.verified_copy(conn) as lookup:
                assert lookup["verified_lookup_rows"] == 2
        stage_shadow_handoff_fixture(conn, placed, raw_mapping=True)
        assert conn.scalar(text("SELECT to_regclass(:name)"), {"name": proof.STATE}) is None
        assert _frozen_records(conn) == placed.frozen_before

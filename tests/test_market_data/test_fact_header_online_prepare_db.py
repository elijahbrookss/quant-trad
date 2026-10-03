"""Atomic preparation on the owned QT SSD/HDD fixture; no host cutover."""
from datetime import timedelta
from pathlib import Path
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_online as online
from scripts.db import fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from scripts.db import fact_header_v2_online_proof as protection
from scripts.db import archive_root_v2_online as archives
from tests.test_market_data.test_archive_online_copy_db import _prepare
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.test_fact_header_copy_placement_db import _assert_disk
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import BASE, storage

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                reason="requires owned SSD/HDD storage-demo topology")]


def _setup(storage, tmp_path, monkeypatch):
    engine, options, source, _ = _prepare(
        storage, tmp_path, monkeypatch, prepare_captures=False)
    options.pop("page_rows")
    options.pop("max_page_bytes")
    return engine, dict(placement=storage.copy_plan, attempt_seconds=180, **options), source


def _saved(conn):
    return conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c"))


def test_online_preparation_retry_preserves_original_clock_and_live_publication(
        storage, tmp_path, monkeypatch):
    engine, options, source = _setup(storage, tmp_path, monkeypatch)
    with engine.connect() as conn:
        frozen = _frozen_records(conn)
    result = online.prepare_attempt(engine, **options)
    assert not result["migration_ready"] and not result["final_switch_authorized"]
    with engine.begin() as conn:
        original = _saved(conn)
        assert original["attempt_seconds"] == 180
        protection.inspect_protection(conn)
        assert conn.scalar(text(f"SELECT verified_rows FROM {headers.STATE}")) == 0
        assert conn.scalar(text(f"SELECT verified_rows FROM {raw.STATE}")) == 0
    _raw_book_fixture(storage, source, monkeypatch,
                      definition_id="online-atomic-live",
                      provider_product_id="BTC-USD-ATOMIC",
                      event_start=BASE+timedelta(hours=2))
    retry = online.prepare_attempt(engine, **(options | {"attempt_seconds": 96*3600}))
    assert retry["started_at"] == result["started_at"]
    with engine.begin() as conn:
        assert _saved(conn) == original
        assert _frozen_records(conn) == frozen
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {archives.QUEUE})"))
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {raw.QUEUE})"))
    # An early cancellation cannot change the admitted original attempt.
    with pytest.raises(RuntimeError, match="storage_move_cancelled"):
        online.prepare_attempt(engine, **options, cancelled=lambda: True)
    with engine.connect() as conn:
        assert _saved(conn) == original


def test_online_preparation_killed_transaction_leaves_no_partial_capture(
        storage, tmp_path, monkeypatch):
    engine, options, source = _setup(storage, tmp_path, monkeypatch)
    killed = []
    def terminate(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CREATE TABLE "+archives.STATE+"("):
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            killed.append(True)
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", terminate)
    try:
        with pytest.raises(DBAPIError):
            online.prepare_attempt(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", terminate)
    assert killed
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"),
                           {"name": capture.SCHEMA}) is None
    # Normal source publication still works after the complete DDL rollback.
    _raw_book_fixture(storage, source, monkeypatch,
                      definition_id="online-atomic-after-kill",
                      provider_product_id="BTC-USD-ATOMIC-KILL",
                      event_start=BASE+timedelta(hours=3))
    online.prepare_attempt(engine, **options)


def test_online_preparation_lost_commit_reply_reuses_exact_captures(
        storage, tmp_path, monkeypatch):
    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    prepared = set()
    def observe(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CREATE TABLE "+archives.STATE+"("):
            prepared.add(id(conn))
    commit = Connection._commit_impl
    def lose_reply(conn):
        commit(conn)
        if id(conn) in prepared:
            raise RuntimeError("atomic preparation reply lost after actual commit")
    event.listen(engine, "after_cursor_execute", observe)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(Connection, "_commit_impl", lose_reply)
            with pytest.raises(RuntimeError, match="reply lost"):
                online.prepare_attempt(engine, **options)
    finally:
        event.remove(engine, "after_cursor_execute", observe)
    with engine.connect() as conn:
        original = _saved(conn)
        oid = conn.scalar(text("SELECT to_regclass(:name)::oid"),
                          {"name": archives.STATE})
    online.prepare_attempt(engine, **options)
    with engine.connect() as conn:
        assert _saved(conn) == original
        assert conn.scalar(text("SELECT to_regclass(:name)::oid"),
                           {"name": archives.STATE}) == oid


def test_online_preparation_refuses_busy_source_without_partial_capture(
        storage, tmp_path, monkeypatch):
    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    with engine.begin() as writer:
        writer.exec_driver_sql("LOCK TABLE market.fact_versions IN ROW EXCLUSIVE MODE")
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            online.prepare_attempt(engine, **options)
        # The source transaction was neither canceled nor replaced by admission.
        assert writer.scalar(text("SELECT 1")) == 1
    with engine.connect() as conn:
        assert conn.scalar(text("SELECT to_regnamespace(:name)"),
                           {"name": capture.SCHEMA}) is None
    online.prepare_attempt(engine, **options)


@pytest.mark.parametrize("header_started", [False, True])
def test_explicit_online_steps_preserve_commits_intake_and_original_attempt(
        storage, tmp_path, monkeypatch, header_started):
    from scripts.db import archive_reference_v2_placement as catalogs
    from scripts.db import fact_header_v2_references as references
    from tests.test_market_data.test_fact_header_copy_db import _insert

    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    if header_started:
        # Exact former progress schema: no new columns or implicit upgrade.
        with engine.begin() as conn:
            headers.prepare_copy(conn, placement=storage.copy_plan, attempt_seconds=180)
    prepared = online.prepare_attempt(engine, **options)
    args = {k:options[k] for k in ("placement", "policy", "resource_limits")}
    steps = dict(**args, expected_started_at=prepared["started_at"])
    with engine.begin() as conn:
        original = _saved(conn)
        frozen = _frozen_records(conn)
        has_retained = conn.scalar(text("SELECT to_regclass(:name)"),
                                    {"name": catalogs.RETAINED_LEGACY}) is not None
    if has_retained:
        catalogs.move_reference_catalog(engine, relation=catalogs.RETAINED_LEGACY,
            policy=options["policy"], resource_limits=options["resource_limits"])
    with pytest.raises(RuntimeError, match="preparation_order_required"):
        online.preparation_step(engine, step="raw_history", **steps)
    with pytest.raises(RuntimeError, match="attempt_binding_changed"):
        online.preparation_step(engine, step="identity_history",
            **(steps | {"expected_started_at": "different attempt"}))
    if header_started:
        # A prior version may have committed header pages before being resumed.
        with engine.begin() as conn:
            headers.copy_page(conn, page_rows=1)
    moved = []
    identity_pages = 0
    for _ in range(40):
        result = online.copy_pass(engine, max_pages=2, page_rows=2, **args)
        identity_pages += result["committed_pages"].get("identities", 0)
        if result["outcome"] == "identity_relocation_required":
            phase = "identity_history"
        elif result["outcome"] == "identity_order_required":
            phase = "identity_order"
        elif result["outcome"] == "raw_relocation_required":
            phase = "raw_history"
        elif result["outcome"] == "both_tails_observed_empty":
            break
        else:
            continue
        report = online.preparation_step(engine, step=phase, **steps)
        assert report["committed"] and not report["final_switch_authorized"]
        assert online.preparation_step(engine, step=phase, **steps)["reused"]
        moved.append(phase)
        if phase == "identity_history" and not header_started:
            with engine.begin() as conn:
                progress = headers._inspect_progress(conn)
                assert progress["verified_rows"] == 0 and progress["identity_baseline_complete"]
                assert conn.scalar(text(f"SELECT count(*) FROM {headers.SCHEMA}.fact_versions")) == 0
                _assert_disk(conn, headers.SCHEMA+".fact_identities", Path("/qt-history"))
        if phase == "raw_history" and not header_started:
            with engine.begin() as conn:
                assert headers._inspect_progress(conn)["verified_rows"] == 0
                assert raw._inspect(conn)["history_ready"]
                assert conn.scalar(text(f"SELECT count(*) FROM {headers.SCHEMA}.fact_versions")) == 0
    else:
        pytest.fail("tiny explicit phase fixture did not converge")
    assert (identity_pages == 0) is header_started
    assert moved == (["identity_history", "raw_history"] if header_started
                     else ["raw_history", "identity_history", "identity_order"])
    online.preparation_step(engine, step="identity_capture", **steps)
    with engine.begin() as conn:
        slots = [row["relation"] for row in references.inspect_references(conn)["references"]
                 if row["relation"] != references.PARENT]
    assert len(slots) >= 2
    for relation in slots:
        online.preparation_step(engine, step="reference_prepare", relation=relation, **steps)

    inserted = []
    def intake(conn, cursor, statement, parameters, context, executemany):
        if " VALIDATE CONSTRAINT " in statement and not inserted:
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL statement_timeout='2s'")
                inserted.append(_insert(writer, storage, "explicit-reference-intake"))
    event.listen(engine, "after_cursor_execute", intake)
    try:
        online.preparation_step(engine, step="reference_validate", relation=slots[0], **steps)
    finally:
        event.remove(engine, "after_cursor_execute", intake)
    assert inserted

    def terminate(conn, cursor, statement, parameters, context, executemany):
        if " VALIDATE CONSTRAINT " in statement:
            with engine.begin() as killer:
                assert killer.scalar(text("SELECT pg_terminate_backend(:pid,5000)"),
                    {"pid": conn.connection.driver_connection.get_backend_pid()})
            conn.exec_driver_sql("SELECT 1")
    event.listen(engine, "after_cursor_execute", terminate)
    try:
        with pytest.raises(DBAPIError):
            online.preparation_step(engine, step="reference_validate", relation=slots[1], **steps)
    finally:
        event.remove(engine, "after_cursor_execute", terminate)
    with engine.begin() as conn:
        states = {row["relation"]:row for row in references.inspect_references(conn)["references"]}
        assert states[slots[0]]["validated"] and not states[slots[1]]["validated"]
        assert _saved(conn) == original and _frozen_records(conn) == frozen
    for relation in slots[1:]:
        online.preparation_step(engine, step="reference_validate", relation=relation, **steps)
    assert online.preparation_step(engine, step="reference_adopt", **steps)["references_complete"]
    with engine.begin() as conn:
        assert _saved(conn) == original and _frozen_records(conn) == frozen
        assert headers._inspect_progress(conn)["identity_history_ready"]
        assert raw._inspect(conn)["history_ready"]


def test_identity_prepass_preserves_prior_pages_tail_and_clock_after_interruption(
        storage, tmp_path, monkeypatch):
    from tests.test_market_data.test_fact_header_copy_db import _insert
    engine, options, _ = _setup(storage, tmp_path, monkeypatch)
    online.prepare_attempt(engine, **options)
    with engine.begin() as conn:
        original = _saved(conn)
        frozen = _frozen_records(conn)
        with pytest.raises(RuntimeError, match="identity_history_baseline_incomplete"):
            headers.place_identity_on_history(conn)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="identity_staging_order_required"):
            headers.copy_page(conn)
    with engine.begin() as conn:
        assert headers.copy_identity_page(conn, page_rows=1)["verified_page_rows"] == 1
        saved = headers._inspect_progress(conn)
    with engine.begin() as writer:
        added = _insert(writer, storage, "identity-prepass-concurrent")
    def broken(conn, rows, **kw):
        original_copy(conn, rows, **kw)
        raise RuntimeError("injected identity page interruption")
    original_copy = headers._copy_identities
    with monkeypatch.context() as patch:
        patch.setattr(headers, "_copy_identities", broken)
        with pytest.raises(RuntimeError, match="injected identity"):
            with engine.begin() as conn:
                headers.copy_identity_page(conn, page_rows=1)
    online.prepare_attempt(engine, **(options | {"attempt_seconds": 96*3600}))
    with engine.begin() as conn:
        current = headers._inspect_progress(conn)
        assert {k:v for k,v in current.items() if k != "_placement_pid"} == {
            k:v for k,v in saved.items() if k != "_placement_pid"}
        assert conn.scalar(text(f"SELECT count(*) FROM {headers.SCHEMA}.fact_identities")) == 1
        assert conn.scalar(text(f"SELECT count(*) FROM {headers.SCHEMA}.fact_versions")) == 0
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {headers.QUEUE} WHERE id=:id)"), {"id": added["id"]})
        _assert_disk(conn, headers.SCHEMA+".fact_identities", Path("/qt-source/pgdata"))
        assert _saved(conn) == original
    for _ in range(30):
        with engine.begin() as conn:
            report = headers.copy_identity_page(conn, page_rows=2)
        if report["identity_baseline_complete"]:
            break
    else:
        pytest.fail("identity baseline did not converge")
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {headers.QUEUE} WHERE id=:id)"), {"id": added["id"]})
        headers.place_identity_on_history(conn)
        _assert_disk(conn, headers.SCHEMA+".fact_identities", Path("/qt-history"))
        assert headers._inspect_progress(conn)["verified_rows"] == 0
        original_node = conn.scalar(text(f"SELECT pg_relation_filenode('{headers.SCHEMA}.fact_identities')"))
        original_temp = conn.scalar(text("SHOW temp_file_limit"))
    concurrent = []
    def live_source_while_private_locked(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CLUSTER ") and not concurrent:
            with engine.begin() as writer:
                writer.exec_driver_sql("SET LOCAL lock_timeout='1s'")
                concurrent.append(_insert(writer, storage, "identity-reorder-concurrent"))
    def fail_after_rewrite(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CLUSTER "):
            raise RuntimeError("injected after native identity rewrite")
    event.listen(engine, "before_cursor_execute", live_source_while_private_locked)
    event.listen(engine, "after_cursor_execute", fail_after_rewrite)
    try:
        with engine.begin() as conn:
            with pytest.raises(RuntimeError, match="injected after native identity rewrite"):
                headers.prepare_identity_order(conn, temporary_bytes=32*1024**2, rewrite_bytes=64*1024**2)
            assert conn.scalar(text(f"SELECT pg_relation_filenode('{headers.SCHEMA}.fact_identities')")) == original_node
            assert conn.scalar(text("SHOW temp_file_limit")) == original_temp
            assert not headers._inspect_progress(conn).get("header_id_order", False)
            protection.inspect_protection(conn)
    finally:
        event.remove(engine, "before_cursor_execute", live_source_while_private_locked)
        event.remove(engine, "after_cursor_execute", fail_after_rewrite)
    assert concurrent
    with engine.begin() as conn:
        assert conn.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {headers.QUEUE} WHERE id=:id)"),
                           {"id": concurrent[0]["id"]})
        headers.prepare_identity_order(conn, temporary_bytes=32*1024**2, rewrite_bytes=64*1024**2)
        assert headers._inspect_progress(conn)["header_id_order"]
        assert headers.prepare_identity_order(conn, temporary_bytes=32*1024**2, rewrite_bytes=64*1024**2)["reused"]
        assert _saved(conn) == original
        protection.inspect_protection(conn)
    last_id = None
    for _ in range(30):
        with engine.begin() as conn:
            before = headers._inspect_progress(conn)
            report = headers.copy_page(conn, page_rows=2)
            after = headers._inspect_progress(conn)
            if not before["baseline_complete"] and report["verified_page_rows"]:
                assert last_id is None or after["after_id"] > last_id
                last_id = after["after_id"]
        if report["caught_up_at_observation"]:
            break
    else:
        pytest.fail("header baseline and captured tail did not converge")
    for _ in range(30):
        with engine.begin() as conn:
            report = raw.copy_page(conn, page_rows=2)
        if report["caught_up_at_observation"]:
            break
    else:
        pytest.fail("raw baseline did not converge")
    with engine.begin() as conn:
        raw.place_on_history(conn)
        headers.enable_identity_capture(conn, page_rows=2)
        with headers.verified_copy(conn, page_rows=2) as proof:
            assert proof["verified_header_rows"] == proof["verified_identity_rows"]
        assert _saved(conn) == original and _frozen_records(conn) == frozen

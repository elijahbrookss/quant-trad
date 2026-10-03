"""Native forward archive ownership, preserving cancellation and atomic switch.

Small disposable records; these establish no production throughput or pause bound.
"""
from datetime import timedelta
import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection

from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db import Database
from scripts.db import archive_root_v2_online as online, archive_root_v2_copy as archives
from scripts.db import fact_header_forward_adoption as adoption, fact_header_forward_keys as keys
from scripts.db import fact_header_v2_capture as capture, fact_header_v2_cancel as cancellation
from scripts.db import fact_header_v2_handoff as handoff, fact_header_v2_references as references
from scripts.db import archive_reference_v2_placement as catalogs, fact_header_v2_copy as headers
from scripts.db import raw_mapping_v2_copy as raw
from tests.test_market_data.test_fact_storage_tiers_db import storage as native_storage, _placement
from tests.test_market_data.test_archive_online_copy_db import _prepare, _drain, _synthetic_descriptor
from tests.test_market_data.test_archive_root_copy_db import _hashes
from tests.test_market_data.test_fact_header_forward_adoption_db import _old, CANCEL, OPERATION
from tests.test_market_data.test_fact_header_copy_db import _frozen_records

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
    reason="requires owned two-filesystem storage-demo topology")]


@pytest.fixture
def storage(native_storage, monkeypatch):
    native_storage.today -= timedelta(days=3)
    _placement(monkeypatch, native_storage.today)
    return native_storage


def _original_archives(conn):
    return {name: conn.execute(text("SELECT to_jsonb(t) FROM " + name + " t ORDER BY to_jsonb(t)::text")).scalars().all()
            for name in (online.STATE, online.PROGRESS, online.QUEUE)}


def test_forward_archives_preserve_canceled_work_and_fence_final_switch(storage, tmp_path, monkeypatch):
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch, prepare_captures=False)
    with engine.begin() as conn:
        headers.prepare_copy(conn, placement=storage.copy_plan, stage_identity_first=True)
        raw.prepare_copy(conn)
        online.prepare(conn, source_root=options["source_root"], destination_root=options["destination_root"])
    for _ in range(64):
        with engine.begin() as conn:
            if raw._inspect(conn)["baseline_complete"]:
                raw.place_on_history(conn)
                break
            raw.copy_page(conn, page_rows=128)
    else:
        pytest.fail("raw fixture baseline did not converge")
    for _ in range(64):
        with engine.begin() as conn:
            if headers._inspect_progress(conn)["identity_baseline_complete"]:
                headers.place_identity_on_history(conn)
                break
            headers.copy_identity_page(conn, page_rows=128)
    else:
        pytest.fail("identity fixture baseline did not converge")
    roots = {name: options[name] for name in ("source_root", "destination_root")}
    with engine.begin() as conn:
        original = cancellation._capture_binding(conn)
        cancellation.cancel_attempt(conn, expected_capture=original, intent_sha256=CANCEL,
            expected_started_at=capture.inspect_capture(conn)["started_at"], **roots)
    with engine.begin() as conn:
        _synthetic_descriptor(conn, source, "!uncaptured-" + uuid4().hex)
    keys.prepare_keys(engine, expected_capture=original, intent_sha256=CANCEL)
    with engine.begin() as conn:
        old, old_archives, frozen = _old(conn), _original_archives(conn), _frozen_records(conn)
        adoption.prepare_adoption(conn, expected_capture=original, cancellation_intent_sha256=CANCEL,
            operation_sha256=OPERATION, attempt_seconds=600)
        assert not online.prepare(conn, **roots, forward_operation_sha256=OPERATION)["reused"]
        assert online.prepare(conn, **roots, forward_operation_sha256=OPERATION)["reused"]
        started = adoption._state(conn)["started_at"]
        expires = adoption._state(conn)["expires_at"]
    owner = online._capture(OPERATION)
    forward = dict(options, forward_operation_sha256=OPERATION)
    for family in archives.FAMILIES:
        _drain(engine, forward, family)
    family = "raw_archive_manifests"
    reproved = archives.copy_archive_page(engine, family=family, **forward)
    assert reproved["reused_objects"] > 0 and not reproved["migration_ready"]
    # A lower ID allocated before the empty observation commits afterward.
    with engine.connect() as writer:
        transaction = writer.begin()
        key, content = _synthetic_descriptor(writer, source, "!" + uuid4().hex)
        assert online.copy_page(engine, family=family, **forward)["captured_tail_empty_at_observation"]
        transaction.commit()
    original_put = FilesystemRawArchiveObjectStore.put_verified
    def interrupt(self, **kwargs):
        original_put(self, **kwargs)
        raise RuntimeError("interrupt forward archive after file publication")
    with monkeypatch.context() as failing:
        failing.setattr(FilesystemRawArchiveObjectStore, "put_verified", interrupt)
        with pytest.raises(RuntimeError, match="after file publication"):
            online.copy_page(engine, family=family, **forward)
    assert (options["destination_root"] / key).read_bytes() == content
    with engine.begin() as conn:
        assert conn.scalar(text("SELECT count(*) FROM " + owner.queue)) == 1
        assert _old(conn) == old and _original_archives(conn) == old_archives
    # A lost COMMIT response reuses committed progress, not the old attempt.
    commit = Connection._commit_impl
    def lost_reply(conn):
        commit(conn)
        raise RuntimeError("lost forward archive commit reply")
    with monkeypatch.context() as lost:
        lost.setattr(Connection, "_commit_impl", lost_reply)
        with pytest.raises(RuntimeError, match="lost forward archive commit reply"):
            online.copy_page(engine, family=family, **forward)
    assert online.copy_page(engine, family=family, **forward)["selected_catalog_rows"] == 0
    with pytest.raises(RuntimeError, match="intent_changed"):
        online.copy_page(engine, family=family, **dict(forward, forward_operation_sha256="c" * 64))
    with pytest.raises(RuntimeError, match="cancelled"):
        online.copy_page(engine, family=family, **options)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="live_inventory_context_required"):
            online.retire_capture(conn, **roots, forward_operation_sha256=OPERATION)
    # Cancellation is a distinct preserving transaction, including queued rows.
    with engine.begin() as writer:
        _synthetic_descriptor(writer, source, "!cancel-" + uuid4().hex)
    before_files = _hashes(options["destination_root"])
    with pytest.raises(RuntimeError, match="rollback forward cancellation"), engine.begin() as conn:
        assert adoption.retire_adoption(conn, operation_sha256=OPERATION)["retired"]
        assert adoption.retire_adoption(conn, operation_sha256=OPERATION)["reused"]
        assert conn.scalar(text("SELECT count(*) FROM " + owner.queue)) == 1
        assert _old(conn) == old and _original_archives(conn) == old_archives
        raise RuntimeError("rollback forward cancellation")
    assert _hashes(options["destination_root"]) == before_files
    _drain(engine, forward, family)
    for _ in range(64):
        with engine.begin() as conn:
            result = adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=128)
        if result["retained_targets_verified"]:
            break
    else:
        pytest.fail("forward adoption fixture did not converge")
    with engine.begin() as conn:
        inventory = adoption.inspect_references(conn, operation_sha256=OPERATION)["references"]
    for relation in inventory:
        if relation == references.PARENT:
            continue
        with engine.begin() as conn:
            adoption.prepare_reference(conn, operation_sha256=OPERATION, relation=relation)
        with engine.begin() as conn:
            adoption.validate_reference(conn, operation_sha256=OPERATION, relation=relation)
    with engine.begin() as conn:
        adoption.adopt_payload_references(conn, operation_sha256=OPERATION)
        today = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date"))
        switch = dict(operation_sha256=OPERATION, end_day=today, evidence={"fixture":"forward archives"})
        assert adoption._state(conn)["started_at"] == started
        assert adoption._state(conn)["expires_at"] == expires
    with pytest.raises(RuntimeError, match="live_archive_inventory_required"), engine.begin() as conn:
        handoff.stage_forward_tables(conn, **switch)
    verification = {k: v for k, v in forward.items() if k != "max_page_bytes"}
    with pytest.raises(RuntimeError, match="rollback archive switch"), engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000, max_bytes=64*1024**2, **verification):
            with pytest.raises(RuntimeError, match="live_inventory_context_required"):
                online.retire_capture(conn, **roots)
            handoff.stage_forward_tables(conn, **switch)
            raise RuntimeError("rollback archive switch")
    from time import monotonic
    from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK
    # Use the real owning SQL session throughout relocation, switch and lost
    # reply reconciliation; a pooled second session must not borrow its lock.
    with engine.connect() as owning:
        with owning.begin():
            assert owning.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:name,0))"),
                                 {"name": CONTROLLER_LOCK})
            pid = owning.scalar(text("SELECT pg_backend_pid()"))
        try:
            with pytest.raises(RuntimeError, match="controller_active"), engine.begin() as other:
                adoption.inspect_references(other, operation_sha256=OPERATION)
            for relation in catalogs.RELATIONS:
                move = dict(relation=relation, policy=options["policy"], resource_limits=options["resource_limits"],
                            expected_started_at=started.isoformat(), placement=storage.copy_plan,
                            forward_operation_sha256=OPERATION, connection=owning)
                assert not catalogs.move_reference_catalog(engine, **move)["reused"]
                assert catalogs.move_reference_catalog(engine, **move)["reused"]
            with owning.begin():
                assert owning.scalar(text("SELECT to_regclass(:name)"), {"name":owner.closed}) is None
                assert _old(owning) == old and _original_archives(owning) == old_archives
            finish = {k: v for k, v in options.items() if k != "max_page_bytes"}
            finish.update(max_objects=1000, max_bytes=64*1024**2,
                          forward_operation_sha256=OPERATION, forward_end_day=today,
                          connection=owning, deadline=monotonic()+30, activate_policy=True)
            checks = []
            def publisher_check(conn, *, deadline):
                checks.append(conn.scalar(text("SELECT pg_backend_pid()")))
                assert conn is owning and checks[-1] == pid
                if len(checks) == 2:
                    raise RuntimeError("interrupt after forward certificate publication")
            with pytest.raises(RuntimeError, match="after forward certificate"):
                handoff.commit_handoff(engine, publisher_check=publisher_check, **finish)
            with owning.begin():
                assert not handoff.inspect_handoff(owning, policy=options["policy"], **roots)["database_handoff_committed"]
                assert owning.scalar(text("SELECT to_regclass(:name)"), {"name":owner.closed}) is None
            def lost_final_reply(conn):
                commit(conn)
                raise RuntimeError("lost final forward commit reply")
            with monkeypatch.context() as lost:
                lost.setattr(Connection, "_commit_impl", lost_final_reply)
                with pytest.raises(RuntimeError, match="lost final forward commit reply"):
                    handoff.commit_handoff(engine, publisher_check=publisher_check, **finish)
            assert checks == [pid]*4
            with owning.begin():
                observed = handoff.inspect_handoff(owning, policy=options["policy"], **roots)
                assert observed["database_handoff_committed"]
                assert observed["receipt"]["forward_header"]["operation_sha256"] == OPERATION
                assert handoff.inspect_handoff_policy(owning, policy=options["policy"], **roots)["policy_current"]
                with owning.begin_nested() as changed:
                    owning.exec_driver_sql("ALTER TABLE market.fact_versions_legacy DISABLE TRIGGER trg_seal_fact_versions_legacy")
                    with pytest.raises(RuntimeError):
                        handoff.inspect_handoff(owning, policy=options["policy"], **roots)
                    changed.rollback()
                with owning.begin_nested() as changed:
                    owning.execute(text("UPDATE " + adoption.STATE + " SET progress='{}'::jsonb WHERE id=1"))
                    with pytest.raises(RuntimeError, match="certificate_changed"):
                        handoff.inspect_handoff(owning, policy=options["policy"], **roots)
                    changed.rollback()
        finally:
            # Do not return a session-level owner lock to the pool.
            owning.invalidate()
    with engine.begin() as conn:
        assert _old(conn) == old and _original_archives(conn) == old_archives
        assert _frozen_records(conn) == frozen
        assert conn.scalar(text("SELECT count(*) FROM " + owner.queue)) == 0
        assert conn.scalar(text("SELECT receipt FROM " + owner.closed))["source_retained"]
    assert _hashes(options["source_root"]) == _hashes(options["destination_root"])
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
    finally:
        restarted._reset_engine()

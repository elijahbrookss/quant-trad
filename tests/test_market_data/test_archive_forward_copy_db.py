"""Native forward archive ownership, preserving cancellation and atomic switch.

Small disposable records; these establish no production throughput or pause bound.
"""
from dataclasses import replace
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


def _prepare_canceled(storage, tmp_path, monkeypatch):
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
    return engine, options, source, original


def _prepare_forward(storage, tmp_path, monkeypatch, *, key_preparation=None, successor_operation=None, recent_lookup=False):
    engine, options, source, original = _prepare_canceled(storage, tmp_path, monkeypatch)
    roots = {name: options[name] for name in ("source_root", "destination_root")}
    if key_preparation is None:
        keys.prepare_keys(engine, expected_capture=original, intent_sha256=CANCEL)
    else:
        key_preparation(engine, options, original)
    with engine.begin() as conn:
        old, old_archives, frozen = _old(conn), _original_archives(conn), _frozen_records(conn)
        adoption.prepare_adoption(conn, expected_capture=original, cancellation_intent_sha256=CANCEL,
            operation_sha256=OPERATION, attempt_seconds=600)
        assert not online.prepare(conn, **roots, forward_operation_sha256=OPERATION)["reused"]
        assert online.prepare(conn, **roots, forward_operation_sha256=OPERATION)["reused"]
        started = adoption._state(conn)["started_at"]
        expires = adoption._state(conn)["expires_at"]
    if successor_operation is not None:
        # Keep the first owner's partial proof, queued catalog rows and files.
        # The second owner must rediscover writes from the uncaptured interval.
        with engine.begin() as conn:
            adoption.adoption_page(conn, operation_sha256=OPERATION, page_rows=1)
            _synthetic_descriptor(conn, source, "!before-retirement-" + uuid4().hex)
            adoption.retire_adoption(conn, operation_sha256=OPERATION)
            terminal = adoption.inspect_retirement(conn, operation_sha256=OPERATION)
            previous_owner = online._capture(OPERATION, conn=conn)
            storage.retired_forward_records = _retired_forward_records(conn, previous_owner)
            _synthetic_descriptor(conn, source, "!successor-gap-" + uuid4().hex)
        placement_args = {}
        if recent_lookup:
            from scripts.db import fact_header_forward_placement as lookup
            storage.copy_plan = replace(storage.copy_plan, recent_lookup_indexes=True)
            placed = lookup.move_lookup_indexes(engine, operation_sha256="d"*64,
                predecessor_operation_sha256=OPERATION, predecessor_terminal_sha256=adoption._digest(terminal),
                placement=storage.copy_plan, policy=options["policy"], resource_limits=options["resource_limits"])
            assert placed["completion"] is not None
            placement_args["lookup_operation_sha256"] = "d"*64
        with engine.begin() as conn:
            adoption.prepare_adoption(conn, expected_capture=original, cancellation_intent_sha256=CANCEL,
                operation_sha256=successor_operation, attempt_seconds=600, **placement_args,
                predecessor_operation_sha256=OPERATION, predecessor_terminal_sha256=adoption._digest(terminal))
            online.prepare(conn, **roots, forward_operation_sha256=successor_operation)
            state = adoption._state(conn, successor_operation)
            started, expires = state["started_at"], state["expires_at"]
    return engine, options, source, original, old, old_archives, frozen, started, expires


def _retired_forward_records(conn, owner):
    relations = (adoption.STATE, owner.state, owner.progress, owner.queue, owner.closed)
    return {name: conn.execute(text("SELECT to_jsonb(t) FROM " + name + " t ORDER BY to_jsonb(t)::text")).scalars().all()
            for name in relations}


@pytest.mark.parametrize("successor", [False, True, "recent_lookup", "retain_raw"])
def test_forward_archives_preserve_canceled_work_and_fence_final_switch(storage, tmp_path, monkeypatch, successor):
    operation = "c" * 64 if successor else OPERATION
    engine, options, source, original, old, old_archives, frozen, started, expires = _prepare_forward(
        storage, tmp_path, monkeypatch, successor_operation=operation if successor else None, recent_lookup=successor in ("recent_lookup", "retain_raw"))
    roots = {name: options[name] for name in ("source_root", "destination_root")}
    if successor == "retain_raw":
        for _ in range(64):
            with engine.begin() as conn:
                state = adoption._state(conn, operation)
                if all(state["progress"]["identity_" + side]["complete"] for side in ("source", "target")):
                    original_raw_oid = handoff._oid(conn, raw.SOURCE)
                    adoption._retain_raw_source(conn, state)
                    break
                adoption.adoption_page(conn, operation_sha256=operation, page_rows=128)
        else:
            pytest.fail("identity fixture did not finish")
    with engine.begin() as conn:
        owner = online._capture(operation, conn=conn)
    forward = dict(options, forward_operation_sha256=operation)
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
        online.copy_page(engine, family=family, **dict(forward, forward_operation_sha256="d" * 64))
    with pytest.raises(RuntimeError, match="cancelled"):
        online.copy_page(engine, family=family, **options)
    with engine.begin() as conn:
        with pytest.raises(RuntimeError, match="live_inventory_context_required"):
            online.retire_capture(conn, **roots, forward_operation_sha256=operation)
    # Cancellation is a distinct preserving transaction, including queued rows.
    with engine.begin() as writer:
        _synthetic_descriptor(writer, source, "!cancel-" + uuid4().hex)
    before_files = _hashes(options["destination_root"])
    with pytest.raises(RuntimeError, match="rollback forward cancellation"), engine.begin() as conn:
        assert adoption.retire_adoption(conn, operation_sha256=operation)["retired"]
        assert adoption.retire_adoption(conn, operation_sha256=operation)["reused"]
        assert conn.scalar(text("SELECT count(*) FROM " + owner.queue)) == 1
        assert _old(conn) == old and _original_archives(conn) == old_archives
        raise RuntimeError("rollback forward cancellation")
    assert _hashes(options["destination_root"]) == before_files
    _drain(engine, forward, family)
    for _ in range(64):
        with engine.begin() as conn:
            result = adoption.adoption_page(conn, operation_sha256=operation, page_rows=128)
        if result["retained_targets_verified"]:
            break
    else:
        pytest.fail("forward adoption fixture did not converge")
    with engine.begin() as conn:
        inventory = adoption.inspect_references(conn, operation_sha256=operation)["references"]
    for relation in inventory:
        if relation == references.PARENT:
            continue
        with engine.begin() as conn:
            adoption.prepare_reference(conn, operation_sha256=operation, relation=relation)
        with engine.begin() as conn:
            adoption.validate_reference(conn, operation_sha256=operation, relation=relation)
    with engine.begin() as conn:
        adoption.adopt_payload_references(conn, operation_sha256=operation)
        today = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date"))
        switch = dict(operation_sha256=operation, end_day=today, evidence={"fixture":"forward archives"})
        assert adoption._state(conn, operation)["started_at"] == started
        assert adoption._state(conn, operation)["expires_at"] == expires
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
                adoption.inspect_references(other, operation_sha256=operation)
            for relation in catalogs.RELATIONS:
                move = dict(relation=relation, policy=options["policy"], resource_limits=options["resource_limits"],
                            expected_started_at=started.isoformat(), placement=storage.copy_plan,
                            forward_operation_sha256=operation, connection=owning)
                assert not catalogs.move_reference_catalog(engine, **move)["reused"]
                assert catalogs.move_reference_catalog(engine, **move)["reused"]
            with owning.begin():
                assert owning.scalar(text("SELECT to_regclass(:name)"), {"name":owner.closed}) is None
                assert _old(owning) == old and _original_archives(owning) == old_archives
            finish = {k: v for k, v in options.items() if k != "max_page_bytes"}
            finish.update(max_objects=1000, max_bytes=64*1024**2,
                          forward_operation_sha256=operation, forward_end_day=today,
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
                if successor == "retain_raw":
                    assert handoff._oid(owning, raw.SOURCE) == original_raw_oid
                    assert observed["receipt"]["forward_header"]["raw_history_placement_pending"]
                assert observed["receipt"]["forward_header"]["operation_sha256"] == operation
                assert handoff.inspect_handoff_policy(owning, policy=options["policy"], **roots)["policy_current"]
                with owning.begin_nested() as changed:
                    owning.exec_driver_sql("ALTER TABLE market.fact_versions_legacy DISABLE TRIGGER trg_seal_fact_versions_legacy")
                    with pytest.raises(RuntimeError):
                        handoff.inspect_handoff(owning, policy=options["policy"], **roots)
                    changed.rollback()
                with owning.begin_nested() as changed:
                    owning.execute(text("UPDATE " + adoption.state_relation(owning, operation) + " SET progress='{}'::jsonb WHERE id=1"))
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

    if successor:
        with engine.begin() as conn:
            previous_owner = online._capture(OPERATION, conn=conn)
            assert _retired_forward_records(conn, previous_owner) == storage.retired_forward_records


@pytest.mark.parametrize("successor", [False, True, "recent_lookup"])
def test_forward_controller_owns_pages_final_session_and_lost_commit(storage, tmp_path, monkeypatch, successor):
    from time import monotonic
    from sqlalchemy import event
    from scripts.automation.storage_online_controller import OnlineController
    from scripts.automation.storage_online_operation import prepare_background

    operation = "c" * 64 if successor else OPERATION
    engine, options, source, original, old, old_archives, frozen, started, expires = _prepare_forward(
        storage, tmp_path, monkeypatch, successor_operation=operation if successor else None, recent_lookup=successor == "recent_lookup")
    with engine.begin() as conn:
        today = conn.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date"))
    settings = dict(placement=storage.copy_plan, expected_started_at=started.isoformat(),
        max_objects=1000, max_bytes=64*1024**2, forward_operation_sha256=operation,
        forward_end_day=today, **options)
    # Background reentry retains its immutable intent and original clock.
    with OnlineController(engine, **settings) as first:
        with pytest.raises(RuntimeError, match="controller_busy"):
            with OnlineController(engine, **settings):
                pass
        report = first.command(dict(controller_id=first.controller_id, sequence=1, operation="sql_copy"))
        assert report["result"]["phase"] in {"forward_adoption", "catch_up"}
        first_deadline = first.proof.deadline
        with first.rollback_source_fence(deadline=monotonic()+15) as check:
            assert check()["database_resume_fence_held"]
            with engine.begin() as writer:
                _synthetic_descriptor(writer, source, "!forward-rollback-" + uuid4().hex)
            assert check()["database_handoff_committed"] is False
        assert first.state == "aborted"
    engine.dispose()
    with OnlineController(engine, **settings) as worker:
        assert worker.proof.deadline <= first_deadline + 0.1
        assert worker.proof.hashed_bytes == 0
        calls = []
        def exchange(operation, **args):
            calls.append((operation, args.get("step")))
            assert len(calls) < 256, "small forward controller fixture did not converge"
            return worker.command(dict(controller_id=worker.controller_id,
                sequence=worker._sequence+1, operation=operation, **args))
        prepared = prepare_background(exchange, preparation_seconds=30, forward=True)
        assert not prepared["final_switch_authorized"]
        assert not any(step in {"identity_history", "identity_order", "identity_capture", "raw_history"}
                       for _, step in calls)
        with worker._owner.begin():
            assert _old(worker._owner) == old and _original_archives(worker._owner) == old_archives
            assert adoption._state(worker._owner, operation)["expires_at"] == expires
            assert adoption._state(worker._owner, operation)["started_at"] == started
        # An uncaught foreign source session prevents COMMIT; its idle state
        # cannot be treated as publisher retirement.
        with engine.connect() as peer:
            peer.exec_driver_sql("SELECT 1")
            with pytest.raises(RuntimeError, match="sql_publishers_not_drained"):
                worker.commit_database(deadline=monotonic()+30)
            assert not worker.reconcile_database()["database_handoff_committed"]
    engine.dispose()
    with OnlineController(engine, **settings) as worker:
        for _ in range(32):
            reply = worker.command(dict(controller_id=worker.controller_id,
                sequence=worker._sequence+1, operation="reprove"))
            if set(reply["reproved_families_at_observation"]) == set(archives.FAMILIES):
                break
        else:
            pytest.fail("forward controller reproof did not converge")
        worker.admit_builtin_database_jobs()
        deadline = monotonic()+30
        # Final delta cannot restart the finite adoption scan. The immutable
        # mirrors already keep committed source writes in the adopted targets.
        assert worker.final_delta(deadline=deadline)["sql"]["committed_pages"] == 0
        with worker.final_database_session(deadline=deadline):
            assert worker._final_connection is worker._owner
            assert worker._final_pid == worker._pid
            from scripts.automation import storage_online_final as host_final
            session = worker.final_session_observation(deadline=deadline)
            assert session["capture"]["operation_sha256"] == operation
            assert session["capture"]["started_at"] == started.isoformat()
            assert session["capture"]["expires_at"] == expires.isoformat()
            assert "prepared_at" not in session["capture"]
            from scripts.automation.storage_online_forward_worker import capture_binding
            assert session["capture"] == capture_binding(worker._capture)
            forward_binding = session["forward"]
            assert forward_binding["operation_sha256"] == operation
            assert forward_binding["started_at"] == started.isoformat()
            assert forward_binding["expires_at"] == expires.isoformat()
            assert forward_binding["end_day"] == today.isoformat()
            assert host_final._valid_forward_session(forward_binding)
            binding = dict(controller_id=worker.controller_id, capture=session["capture"], forward=forward_binding)
            reply = dict(controller_id=worker.controller_id, operation="final_session_check", state="background",
                bound_final_deadline=deadline, last_sequence=2, final_switch_authorized=False,
                collection_resume_authorized=False, result=session)
            assert host_final._final_session_reply(reply, operation="final_session_check", binding=binding,
                deadline=deadline, sequence=1)[0] == session
            commit = Connection._commit_impl
            switching = set()
            def observe(conn, cursor, statement, parameters, context, executemany):
                if "RENAME TO fact_versions_legacy" in statement:
                    switching.add(id(conn))
            def lost_reply(conn):
                commit(conn)
                if id(conn) in switching:
                    raise RuntimeError("forward controller COMMIT reply lost")
            event.listen(engine, "after_cursor_execute", observe)
            try:
                with monkeypatch.context() as lost:
                    lost.setattr(Connection, "_commit_impl", lost_reply)
                    with pytest.raises(RuntimeError, match="COMMIT reply lost"):
                        worker.commit_database(deadline=deadline, activate_policy=True)
            finally:
                event.remove(engine, "after_cursor_execute", observe)
            assert switching and worker.state == "commit_unknown"
            observed = worker.inspect_outcome(deadline=min(deadline, monotonic()+4))
            assert observed["database_handoff_committed"] and observed["initial_policy_activated"]
            assert worker.reconcile_database()["database_handoff_committed"]
            assert worker.state == "committed"
    with engine.begin() as conn:
        assert _old(conn) == old and _original_archives(conn) == old_archives
        assert _frozen_records(conn) == frozen
    restarted = Database(storage.dsn)
    try:
        assert restarted.ensure_schema(), str(restarted.last_error)
    finally:
        restarted._reset_engine()

    if successor:
        with engine.begin() as conn:
            previous_owner = online._capture(OPERATION, conn=conn)
            assert _retired_forward_records(conn, previous_owner) == storage.retired_forward_records


def _phase_budget_regression(monkeypatch, options, *, key_seconds, initial_seconds=None, initial_intent=None):
    """Exercise real capacity admission with a longer immutable migration limit."""
    from copy import deepcopy
    from math import ceil
    from scripts.automation import storage_online_forward_worker as worker

    limits = options["resource_limits"]
    limits["movement_timeout_seconds"] = 216000
    limits["growth_bytes_per_second"] = {key:1024 for key in limits["growth_bytes_per_second"]}
    original = deepcopy(limits)
    budget = catalogs._budget
    observations = []
    def checked(conn, **kwargs):
        initial = worker._initial(conn, initial_intent) if initial_seconds is not None else None
        key_state = keys._read_state(conn)
        phase = initial or (key_state if key_state and not key_state["complete"] else None)
        bound = initial_seconds if initial is not None else key_seconds
        if phase is not None:
            remaining = (phase["expires_at"]-conn.scalar(text("SELECT clock_timestamp()"))).total_seconds()
            # Admission is sampled just before this observer; allow one rounded
            # second for the metadata read, never a new phase on reentry.
            bound = min(bound, ceil(remaining)+1)
        selected = kwargs["limits"]
        assert 0 < selected["movement_timeout_seconds"] <= bound
        assert {**selected, "movement_timeout_seconds":216000} == original
        assert limits == original
        result, floors = budget(conn, **kwargs)
        assert result["movement_timeout_seconds"] == selected["movement_timeout_seconds"]
        for row in result["filesystems"]:
            assert row["ingestion_and_other_growth_bytes"] == 1024*result["growth_window_seconds"]
        observations.append((initial is not None, result["movement_timeout_seconds"]))
        return result, floors
    monkeypatch.setattr(catalogs, "_budget", checked)
    return observations


def test_forward_key_watch_cancels_same_session_and_preserves_committed_index(storage, tmp_path, monkeypatch):
    from sqlalchemy import event
    from time import monotonic

    def prepare(engine, options, original):
        budgets = _phase_budget_regression(monkeypatch, options, key_seconds=600)
        def run(**extra):
            return keys.prepare_keys_supervised(engine, expected_capture=original, intent_sha256=CANCEL,
                placement=storage.copy_plan, policy=options["policy"],
                resource_limits=options["resource_limits"], max_duration_seconds=600, **extra)
        stopped = {"cancel":False, "pid":None}
        def interrupt(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY"):
                stopped["pid"] = conn.connection.driver_connection.get_backend_pid()
                stopped["cancel"] = True
                # Native SQL on the exact build connection. The real watchdog
                # must deliver cancellation here, rather than a pooled peer.
                cursor.execute("SELECT pg_sleep(4)")
        event.listen(engine, "before_cursor_execute", interrupt)
        try:
            with pytest.raises(RuntimeError, match="storage_move_cancelled") as error:
                run(cancelled=lambda: stopped["cancel"])
        finally:
            event.remove(engine, "before_cursor_execute", interrupt)
        assert stopped["pid"]
        assert error.value.__cause__.pgcode == "57014"
        with engine.begin() as conn:
            partial = keys._read_state(conn)
            assert partial and not partial["complete"]
            assert not any(keys.inspect_keys(conn).values())
            assert cancellation._capture_binding(conn) == original
            # Both session locks have been released after the watcher stopped.
            assert conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                "hashtextextended('qt.storage.management.v1',0))"))
            assert conn.scalar(text("SELECT pg_try_advisory_xact_lock("
                "hashtextextended('qt.storage.online.controller.v1',0))"))
        first = next(iter(keys.KEYS))
        reconnected = []
        lost_session = [False]
        def opened(driver, record):
            if lost_session[0]:
                reconnected.append(driver)
        def lost(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY "+first):
                lost_session[0] = True
                conn.invalidate()
                raise RuntimeError("supervised index COMMIT reply lost")
        event.listen(engine, "connect", opened)
        event.listen(engine, "after_cursor_execute", lost)
        try:
            with pytest.raises(RuntimeError, match="preparation_session_lost"):
                run()
        finally:
            event.remove(engine, "after_cursor_execute", lost)
            event.remove(engine, "connect", opened)
        # Settings restoration and advisory unlock must not reconnect after
        # losing the build session. The valid committed index remains reusable.
        assert lost_session[0] and not reconnected
        with engine.begin() as conn:
            oid = keys.inspect_keys(conn)[first]
            assert oid
        assert run()["keys_prepared"]
        with engine.begin() as conn:
            complete = keys._read_state(conn)
            assert complete["complete"] and complete["index_oids"][first] == oid
            assert complete["started_at"] == partial["started_at"]
            assert complete["expires_at"] == partial["expires_at"]
        assert run()["reused"]
        assert len(budgets) == 4 and all(not initial for initial, _ in budgets)
    _prepare_forward(storage, tmp_path, monkeypatch, key_preparation=prepare)


@pytest.mark.parametrize("outcome,successor", [("recover", False), ("expire", False),
    ("recover", True), ("expire", True), ("retain_raw", True)],
    ids=["False-recover", "False-expire", "True-recover", "True-expire", "retain_raw"])
def test_forward_worker_initialization_owns_clock_and_atomic_commit(storage, tmp_path, monkeypatch, outcome, successor):
    from datetime import datetime, timezone
    import time
    from scripts.automation import storage_online_forward_worker as worker
    from scripts.automation.storage_host_boundary import digest
    from tests.test_storage_online_forward_worker import _request, _successor_request

    engine, options, source, original = _prepare_canceled(storage, tmp_path, monkeypatch)
    kwargs = dict(targets=(storage.copy_plan.recent, storage.copy_plan.history),
        policy=options["policy"], limits=options["resource_limits"],
        source=options["source_root"], destination=options["destination_root"],
        key_seconds=3600, initial_seconds=30 if outcome == "expire" else 600)
    request = _request(original)
    request["forward"]["cancellation_intent_sha256"] = CANCEL
    request["forward"]["operation_sha256"] = digest({k:v for k,v in request["forward"].items() if k != "operation_sha256"})
    previous_records = None
    if successor:
        from scripts.db import fact_header_forward_placement as lookup
        worker.prepare_forward(engine, request, **{**kwargs, "initial_seconds":600})
        previous_operation = request["forward"]["operation_sha256"]
        with engine.begin() as conn:
            adoption.retire_adoption(conn, operation_sha256=previous_operation)
            terminal = adoption.inspect_retirement(conn, operation_sha256=previous_operation)
            previous_owner = online._capture(previous_operation, conn=conn)
            previous_records = _retired_forward_records(conn, previous_owner)
            previous_initializer = worker._initial(conn)
        storage.copy_plan = replace(storage.copy_plan, recent_lookup_indexes=True)
        from scripts.automation import storage_online_keys as key_worker
        from scripts.automation import storage_online_worker as source_worker
        with engine.begin() as conn:
            database_identity = conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
        lookup_request = {**request, "database_identity":database_identity}
        package = dict(schema_version="qt.storage_online_lookup_package.v1", plan_sha256="1"*64,
            image="sha256:"+"2"*64, source_revision="3"*40, source_tree_hash="4"*64, operation_sha256="d"*64,
            predecessor_operation_sha256=previous_operation, predecessor_terminal_sha256=adoption._digest(terminal),
            predecessor_terminal_file="storage-online-forward-terminal.json", predecessor_terminal_file_sha256="5"*64)
        payload = dict(action="inspect_lookups", request=lookup_request, package=package, wall_deadline=time.time()+300)
        with monkeypatch.context() as inventory:
            inventory.setattr(source_worker, "request_configuration", lambda request,path:
                (options["policy"], {**options["resource_limits"],"movement_timeout_seconds":216000}, (storage.copy_plan.recent, storage.copy_plan.history)))
            assert key_worker.worker_lookups(engine, payload) is None
            result = key_worker.worker_lookups(engine, {**payload,"action":"place_lookups","wall_deadline":time.time()+3600})
            assert result["complete"] and result["operation_sha256"] == "d"*64 and result["duration_seconds"] == 3600
            assert key_worker.worker_lookups(engine, payload) == result
        request = _successor_request(original)
        request["forward"].update(cancellation_intent_sha256=CANCEL,
            predecessor_operation_sha256=previous_operation, predecessor_terminal_sha256=adoption._digest(terminal),
            lookup_operation_sha256="d"*64)
        request["forward"]["operation_sha256"] = digest({k:v for k,v in request["forward"].items() if k != "operation_sha256"})
    operation = request["forward"]["operation_sha256"]
    intent = request["forward"]
    budgets = _phase_budget_regression(monkeypatch, options, key_seconds=3600,
        initial_seconds=kwargs["initial_seconds"], initial_intent=intent)
    def never_rebuild(*args, **kwargs):
        pytest.fail("initialization retry/successor attempted key phase again")
    if successor:
        monkeypatch.setattr(keys, "prepare_keys_supervised", never_rebuild)
    from scripts.automation import storage_online_forward as host_forward
    queries = []
    def database_query(database_id, sql, *, read_only_seconds):
        assert read_only_seconds == 5
        assert database_id == "disposable-forward-host"
        queries.append(sql)
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
            return conn.scalar(text(sql))
    monkeypatch.setattr(host_forward.host, "database_query", database_query)
    def observed():
        phase, capture = host_forward.observe_adoption("disposable-forward-host", request=request)
        return host_forward.admit_adoption_observation(request, initialization=phase,
            capture=capture, now=time.time())
    with engine.begin() as conn:
        old, archives_before = _old(conn), _original_archives(conn)
    prepare = online.prepare
    def interrupted(conn, **args):
        prepare(conn, **args)
        raise RuntimeError("interrupt after forward archive initialization")
    with monkeypatch.context() as failing:
        failing.setattr(online, "prepare", interrupted)
        with pytest.raises(RuntimeError, match="interrupt after forward"):
            worker.prepare_forward(engine, request, **kwargs)
    with engine.begin() as conn:
        initial = worker._initial(conn, intent)
        key_state = keys._read_state(conn)
        assert not initial["complete"] and key_state["complete"]
        assert [is_initial for is_initial, _ in budgets] == ([True] if successor else [False, True])
        assert adoption._state(conn, operation) is None
        assert conn.scalar(text("SELECT to_regclass(:name)"), {"name":online._capture(operation, conn=conn).state}) is None
        assert _old(conn) == old and _original_archives(conn) == archives_before
        if successor:
            assert worker._initial(conn) == previous_initializer
            assert _retired_forward_records(conn, previous_owner) == previous_records
        # Source remains writable after rolled-back guards and archive capture.
        _synthetic_descriptor(conn, source, "!worker-interrupted-"+uuid4().hex)
    with pytest.raises(RuntimeError, match="active_adoption_required"):
        observed()
    monkeypatch.setattr(keys, "prepare_keys_supervised", never_rebuild)
    if outcome == "expire":
        time.sleep(max(0, (initial["expires_at"]-datetime.now(timezone.utc)).total_seconds())+0.1)
        with pytest.raises(RuntimeError, match="initialization_expired"):
            worker.prepare_forward(engine, request, **kwargs)
        with engine.begin() as conn:
            assert worker._initial(conn, intent) == initial and keys._read_state(conn) == key_state
            assert adoption._state(conn, operation) is None
            assert _old(conn) == old
        return
    commit = Connection._commit_impl
    lost = [False]
    def lost_commit(conn):
        # Lose only the initialization COMMIT reply, after both SQL owners and
        # the complete receipt became durable in the same transaction.
        state = worker._initial(conn, intent)
        complete = state is not None and state["complete"]
        commit(conn)
        if complete and not lost[0]:
            lost[0] = True
            raise RuntimeError("lost forward initialization COMMIT")
    with monkeypatch.context() as failing:
        failing.setattr(Connection, "_commit_impl", lost_commit)
        with pytest.raises(RuntimeError, match="lost forward initialization COMMIT"):
            worker.prepare_forward(engine, request, **kwargs)
    assert lost[0]
    assert [is_initial for is_initial, _ in budgets] == ([True, True] if successor else [False, True, True])
    with engine.begin() as conn:
        complete = worker._initial(conn, intent)
        adopted = adoption._state(conn, operation)
        assert complete == {**initial, "complete":True}
        assert keys._read_state(conn) == key_state
        assert _old(conn) == old
    placement, started = worker.prepare_forward(engine, request, **kwargs)
    assert placement == storage.copy_plan and started == adopted["started_at"].isoformat()
    with engine.begin() as conn:
        assert worker._initial(conn, intent) == complete and adoption._state(conn, operation) == adopted
        assert keys._read_state(conn) == key_state
        assert _old(conn) == old
    owner, deadline = observed()
    assert owner["operation_sha256"] == operation
    assert owner["started_at"] == adopted["started_at"].isoformat()
    assert deadline == adopted["expires_at"].timestamp()
    assert queries and all(query.startswith("SELECT ") for query in queries)
    if successor:
        with pytest.raises(RuntimeError, match="active_adoption_required"):
            host_forward.observe_adoption("disposable-forward-host")
        with engine.begin() as conn:
            assert worker._initial(conn) == previous_initializer
            assert _retired_forward_records(conn, previous_owner) == previous_records
    changed = dict(request, source_inode=request["source_inode"]+1)
    with pytest.raises(RuntimeError, match="initialization_binding_changed"):
        worker.prepare_forward(engine, changed, **kwargs)
    if successor and outcome in ("recover", "retain_raw"):
        request = _qualify_reschedule(engine, storage, source, request, kwargs, retain_raw=outcome == "retain_raw")
    # The confined terminal selects this same explicit owner, even with the old
    # retired adoption/initializer still present. Its lost-reply reconciliation
    # remains read-only and cannot retire the predecessor a second time.
    from scripts.automation import storage_online_terminal as terminal_worker
    payload = dict(action="inspect", request=request, expected_capture=None, intent_sha256="f"*64,
        package={"schema_version":"qt.storage_online_terminal.v1"})
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        terminal_observed = terminal_worker._forward_sql(conn, payload, timeout_seconds=30)
        assert not terminal_observed["retired"] and terminal_observed["capture"]["operation_sha256"] == operation
    payload.update(action="apply", expected_capture=terminal_observed["capture"])
    # This native database fixture has writable storage mounts. The confined
    # terminal must reject that namespace; the Linux host rehearsal exercises
    # apply through its actual read-only container. Keep this guard intact.
    with pytest.raises(RuntimeError, match="terminal_readonly_namespace_required"):
        with engine.begin() as conn:
            terminal_worker._forward_sql(conn, payload, timeout_seconds=30)
    with engine.begin() as conn:
        adoption.retire_adoption(conn, operation_sha256=operation)
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        terminal_result = terminal_worker._forward_sql(conn, {**payload,"action":"reconcile"}, timeout_seconds=30)
        assert terminal_result["retired"] and terminal_result["capture"] == terminal_observed["capture"]
        if successor:
            assert _retired_forward_records(conn, previous_owner) == previous_records
            assert worker._initial(conn) == previous_initializer
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        retired=adoption.inspect_retirement(conn, operation_sha256=operation)
        assert retired["rows_preserved"] and retired["operation_sha256"]==operation
    with pytest.raises(RuntimeError, match="adoption_retired"):
        worker.prepare_forward(engine, request, **kwargs)
    with pytest.raises(RuntimeError, match="active_adoption_required"):
        observed()


def _qualify_reschedule(engine, storage, source, request, kwargs, *, retain_raw=False):
    """Use actual SQL guards/progress and resume; never replace them with mocks."""
    from copy import deepcopy
    from datetime import datetime, timezone
    from scripts.automation import storage_online_forward_worker as worker
    from tests.test_storage_online_forward_worker import _rescheduled
    from tests.test_market_data.test_fact_header_copy_db import _insert
    operation = request["forward"]["operation_sha256"]
    tomorrow = (datetime.now(timezone.utc).date()+timedelta(days=1)).isoformat()
    proposed = _rescheduled(request, end_day=tomorrow)
    with engine.begin() as conn:
        # The archive fixture has no hot rows, but has an empty open partition.
        # Select its registered day; rescheduling performs no partition DDL.
        storage.open_day = conn.scalar(text("SELECT storage_day FROM market.fact_retention_partitions "
            "WHERE state='open' ORDER BY storage_day DESC LIMIT 1"))
        assert storage.open_day is not None
        adoption.adoption_page(conn, operation_sha256=operation, page_rows=1)
        expected = worker.inspect_reschedule(conn, request=request)
        old_state = adoption._state(conn, operation)
        frozen = _frozen_records(conn)
    assert old_state["progress"]["identity_target"]["verified"] > 0
    with engine.connect() as owner:
        owner.execute(text("SELECT pg_advisory_lock(hashtextextended(:name,0))"), {"name":adoption.CONTROLLER_LOCK})
        try:
            with pytest.raises(RuntimeError, match="controller_active"):
                with engine.begin() as conn:
                    worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
        finally:
            owner.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), {"name":adoption.CONTROLLER_LOCK})
    outside = _rescheduled(request, end_day=(datetime.now(timezone.utc).date()+timedelta(days=5)).isoformat())
    with pytest.raises(RuntimeError, match="outside_original_deadline"):
        with engine.begin() as conn:
            worker.reschedule_initialization(conn, request=outside, expected=expected, final_seconds=1)
    with pytest.raises(RuntimeError, match="rollback reschedule fixture"):
        with engine.begin() as conn:
            worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
            raise RuntimeError("rollback reschedule fixture")
    with engine.begin() as conn:
        assert worker.inspect_reschedule(conn, request=request) == expected
    with engine.begin() as conn:
        _insert(conn, storage, "published-before-reschedule")
        _synthetic_descriptor(conn, source, "!reschedule-live-"+uuid4().hex)
    with engine.begin() as conn:
        # Ordinary publication changes data, not the preserved proof journal.
        assert worker.inspect_reschedule(conn, request=request) == expected
        worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
    # Discard the commit response. Fresh read-only reconciliation must recover
    # the new binding without replaying any mutation or restarting the scan.
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        actual = worker.inspect_reschedule(conn, request=proposed)
        wanted = deepcopy(expected)
        wanted["initialization"]["binding"] = worker.initialization_binding(proposed)
        assert actual == wanted and adoption._state(conn, operation) == old_state
        assert _frozen_records(conn) == frozen
    with pytest.raises(RuntimeError, match="preimage_changed"):
        with engine.begin() as conn:
            worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
    with pytest.raises(RuntimeError, match="initialization_binding_changed"):
        worker.prepare_forward(engine, request, **kwargs)
    worker.prepare_forward(engine, proposed, **kwargs)
    with engine.begin() as conn:
        assert adoption._state(conn, operation) == old_state
        row = _insert(conn, storage, "published-after-reschedule")
        assert conn.scalar(text("SELECT count(*) FROM "+adoption.IDENTITY+" WHERE id=:id"), {"id":row["id"]}) == 1
        assert _frozen_records(conn) == frozen
    return _qualify_extension(engine, storage, proposed, kwargs, retain_raw=retain_raw)


def _qualify_extension(engine, storage, previous, kwargs, *, retain_raw=False):
    """Extend only the explicit bound; retain real cursors, guards and inputs."""
    from copy import deepcopy
    from scripts.automation import storage_online_forward_worker as worker
    from tests.test_storage_online_forward_worker import _extended
    operation = previous["forward"]["operation_sha256"]
    if retain_raw:
        for _ in range(64):
            with engine.begin() as conn:
                state = adoption._state(conn, operation)
                if all(state["progress"]["identity_" + side]["complete"] for side in ("source", "target")):
                    break
                adoption.adoption_page(conn, operation_sha256=operation, page_rows=1)
        else:
            pytest.fail("identity fixture did not finish")
    with engine.begin() as conn:
        old = adoption._state(conn, operation)
        frozen = _frozen_records(conn)
        archive_before = online._forward_amendment_state(conn, operation)
        archive_owner = online._capture(operation, conn=conn)
        queued_before = conn.execute(text(f"SELECT * FROM {archive_owner.queue} ORDER BY family,id")).all()
    boundary = (old["expires_at"].date()+timedelta(days=1)).isoformat()
    proposed = _extended(previous, end_day=boundary, attempt_seconds=old["attempt_seconds"]+86400)
    if retain_raw:
        proposed["forward_reschedule"]["raw_mapping_mode"] = "retain_source"
    with engine.begin() as conn:
        expected = worker.inspect_reschedule(conn, request=proposed)
    for mutation, message in (
        (lambda r:r["forward_reschedule"].update(previous_request_sha256="f"*64), "predecessor_changed"),
        (lambda r:r["forward_reschedule"].update(end_day=previous["forward_reschedule"]["end_day"]), "predecessor_changed")):
        bad = deepcopy(proposed)
        mutation(bad)
        with pytest.raises(RuntimeError, match=message):
            with engine.begin() as conn:
                worker.reschedule_initialization(conn, request=bad, expected=expected, final_seconds=1)
    with engine.connect() as owner:
        owner.execute(text("SELECT pg_advisory_lock(hashtextextended(:name,0))"), {"name":adoption.CONTROLLER_LOCK})
        try:
            with pytest.raises(RuntimeError, match="controller_active"):
                with engine.begin() as conn:
                    worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
        finally:
            owner.execute(text("SELECT pg_advisory_unlock(hashtextextended(:name,0))"), {"name":adoption.CONTROLLER_LOCK})
    with pytest.raises(RuntimeError, match="adoption_expired"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE "+adoption.state_relation(conn, operation)+
                " SET started_at=started_at-interval '5 days',expires_at=expires_at-interval '5 days' WHERE id=1"))
            worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
    with pytest.raises(RuntimeError, match="rollback extension fixture"):
        with engine.begin() as conn:
            worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
            raise RuntimeError("rollback extension fixture")
    with engine.begin() as conn:
        assert worker.inspect_reschedule(conn, request=proposed) == expected
        worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
    # Ignore the response and reconcile independently, without replaying SQL.
    with engine.begin() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        actual = worker.inspect_reschedule(conn, request=proposed)
        wanted = deepcopy(expected)
        wanted["initialization"]["binding"] = worker.initialization_binding(proposed)
        wanted["capture"].update(attempt_seconds=old["attempt_seconds"]+86400,
            expires_at=(old["expires_at"]+timedelta(days=1)).isoformat())
        if retain_raw:
            wanted["raw_mapping_mode"] = "retain_source"
        assert actual == wanted
        expected_state = {**old, "attempt_seconds":old["attempt_seconds"]+86400,
            "expires_at":old["expires_at"]+timedelta(days=1)}
        if retain_raw:
            expected_state["binding"] = {**old["binding"], "raw_mapping_mode": "retain_source"}
            assert not all(old["progress"]["raw_" + side]["complete"] for side in ("source", "target"))
        assert adoption._state(conn, operation) == expected_state
        archive_after = deepcopy(archive_before)
        archive_after["capture"]["binding"]["expires_at"] = (old["expires_at"]+timedelta(days=1)).isoformat()
        assert online._forward_amendment_state(conn, operation) == archive_after
        assert conn.execute(text(f"SELECT * FROM {archive_owner.queue} ORDER BY family,id")).all() == queued_before
        assert _frozen_records(conn) == frozen
    with pytest.raises(RuntimeError, match="preimage_changed"):
        with engine.begin() as conn:
            worker.reschedule_initialization(conn, request=proposed, expected=expected, final_seconds=1)
    with pytest.raises(RuntimeError, match="reschedule_initializer_required"):
        worker.prepare_forward(engine, previous, **kwargs)
    worker.prepare_forward(engine, proposed, **kwargs)
    return proposed


def test_key_only_worker_keeps_original_clock_and_never_initializes_adoption(storage, tmp_path, monkeypatch):
    from sqlalchemy import event
    import time
    from scripts.automation import storage_online_keys as key_worker
    from scripts.automation import storage_online_worker as source_worker
    from scripts.automation import storage_online_forward_worker as worker
    from scripts.automation.storage_host_boundary import digest
    from tests.test_storage_online_forward_worker import _request

    engine, options, source, original = _prepare_canceled(storage, tmp_path, monkeypatch)
    budgets = _phase_budget_regression(monkeypatch, options, key_seconds=3600)
    targets = (storage.copy_plan.recent, storage.copy_plan.history)
    # Inventory transport is an adapter here; actual SQL, placement, capacity,
    # watchdog and concurrent index creation use the disposable native owners.
    monkeypatch.setattr(source_worker, "request_configuration",
        lambda request,path:(options["policy"], options["resource_limits"], targets))
    with engine.begin() as conn:
        identity = conn.scalar(text("SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()"))
        before, archives_before = _old(conn), _original_archives(conn)
    request = _request(original)
    del request["forward"]
    request["database_identity"] = identity
    payload = dict(action="inspect_keys", request=request, expected_capture=original,
        intent_sha256=CANCEL, wall_deadline=time.time()+300)
    assert key_worker.worker_keys(engine,payload) is None
    payload.update(action="prepare_keys",wall_deadline=time.time()+3600)
    def lose(conn,cursor,statement,parameters,context,executemany):
        if statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY "+next(iter(keys.KEYS))):
            raise RuntimeError("key-only index acknowledgement lost")
    event.listen(engine,"after_cursor_execute",lose)
    try:
        with pytest.raises(RuntimeError,match="acknowledgement lost"):
            key_worker.worker_keys(engine,payload)
    finally:
        event.remove(engine,"after_cursor_execute",lose)
    with engine.begin() as conn:
        partial = keys._read_state(conn)
        first_oid = keys.inspect_keys(conn)[next(iter(keys.KEYS))]
        assert first_oid and not partial["complete"]
    observed = key_worker.worker_keys(engine,{**payload,"action":"inspect_keys","wall_deadline":time.time()+300})
    assert not observed["complete"]
    result = key_worker.worker_keys(engine,payload)
    assert result["complete"] and result["index_oids"][next(iter(keys.KEYS))] == first_oid
    assert result["started_at"] == partial["started_at"].isoformat()
    assert result["expires_at"] == partial["expires_at"].isoformat()
    assert key_worker.worker_keys(engine,{**payload,"action":"inspect_keys","wall_deadline":time.time()+300}) == result
    with engine.begin() as conn:
        assert keys._read_state(conn)["duration_seconds"] == 3600
        assert _old(conn) == before and _original_archives(conn) == archives_before
        assert worker._initial(conn) is None and adoption._state(conn) is None
    assert budgets and all(not initial for initial,_ in budgets)
    # A later separately bound adoption reuses completed keys, without another
    # CREATE INDEX or renewal of their SQL start/expiry.
    later = _request(original)
    later["forward"]["cancellation_intent_sha256"] = CANCEL
    later["forward"]["operation_sha256"] = digest({k:v for k,v in later["forward"].items() if k != "operation_sha256"})
    def never_build(conn,cursor,statement,parameters,context,executemany):
        if statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY"):
            pytest.fail("completed keys rebuilt during separately owned adoption")
    event.listen(engine,"before_cursor_execute",never_build)
    try:
        worker.prepare_forward(engine,later,targets=targets,policy=options["policy"],limits=options["resource_limits"],
            source=options["source_root"],destination=options["destination_root"],key_seconds=3600,initial_seconds=600)
    finally:
        event.remove(engine,"before_cursor_execute",never_build)
    with engine.begin() as conn:
        complete = keys._read_state(conn)
        assert complete["started_at"] == partial["started_at"] and complete["expires_at"] == partial["expires_at"]
        assert worker._initial(conn)["complete"]

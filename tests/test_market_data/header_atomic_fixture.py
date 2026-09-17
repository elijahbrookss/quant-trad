"""Real atomic move/crash probes, restricted to the private namespace cluster."""
from pathlib import Path
from time import monotonic
from uuid import uuid4

from sqlalchemy import create_engine, event, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from core.storage_targets import StoragePolicy
from portal.backend.db.storage_target_models import (
    StorageHeaderMoveRecord, StoragePlanRecord, StoragePolicyRecord, StorageTargetRecord,
)
from portal.backend.service.storage.header_catalog import read_header_catalog
from portal.backend.service.storage.header_filesystem import verify_header_filesystem
from portal.backend.service.storage.header_destinations import register_header_tablespaces, review_header_moves
from portal.backend.service.storage.header_journal import reserve_header_batch
from portal.backend.service.storage.header_movement import stage_header_move
from portal.backend.service.storage_management import StorageConflict
from tests.test_market_data.postgres_commit_proxy import drop_commit_ack


def prove_atomic_moves(engine, root, targets, destination_oid, storage_day, partition_name, binary):
    if (root.resolve().parent != Path("/tmp") or not root.name.startswith("qt-header-ns-")
            or engine.url.host is not None or engine.url.query.get("host") != str(root / "socket")):
        raise RuntimeError("atomic_fixture_requires_private_socket_cluster")
    quote = engine.dialect.identifier_preparer.quote_identifier

    def move_indexes(conn, tablespace):
        names = conn.execute(text("""
            SELECT n.nspname,c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE i.indrelid=CAST(:name AS regclass)
        """), {"name": "market." + partition_name}).all()
        for schema, name in names:
            conn.exec_driver_sql("ALTER INDEX " + quote(schema) + "." + quote(name)
                                 + " SET TABLESPACE " + quote(tablespace))

    # The preceding namespace primitive tests finish on history. Prepare a new
    # source state privately; no application helper resets or rewrites source data.
    with engine.begin() as conn:
        conn.exec_driver_sql("ALTER TABLE ONLY market." + quote(partition_name) + " SET TABLESPACE pg_default")
        move_indexes(conn, "pg_default")
        conn.execute(text("CREATE TABLE public.atomic_move_probe(marker text PRIMARY KEY)"))

    def prepare():
        with Session(engine) as session:
            config = session.get(StoragePolicyRecord, 1)
            policy = StoragePolicy.from_dict(config.policy)
            revision = config.revision
        observed = read_header_catalog(engine, destination_tablespace_oids=(destination_oid,))
        verified = verify_header_filesystem(observed, targets, pg_controldata=binary,
                                            destination_assignments={"history": destination_oid})
        plan_id = "atomic-" + uuid4().hex
        with Session(engine) as session, session.begin():
            session.add(StoragePlanRecord(id=plan_id, request_id=plan_id, base_revision=revision,
                policy=policy.to_dict(), policy_hash=policy.fingerprint, state="queued", impact={}, progress={}))
        with Session(engine) as session, session.begin():
            register_header_tablespaces(session, verified=verified)
            reservations = {target.id: target.reserved_bytes
                            for target in session.scalars(select(StorageTargetRecord))}
            review = review_header_moves(verified=verified, policy=policy, targets=targets,
                                         reserved_bytes=reservations)
            receipt = reserve_header_batch(session, plan_id=plan_id,
                                            review_hash=review["plan_hash"], verified=verified)
        if len(receipt["moves"]) != 1:
            raise RuntimeError("atomic_fixture_requires_one_move")
        return receipt["moves"][0]["id"], review["plan_hash"], verified

    move_id, review_hash, baseline = prepare()
    with engine.connect() as conn:
        original_rows = conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions ORDER BY id")).all()
    original_group = baseline.snapshot.partitions[0]
    original_nodes = [(item.oid, item.relfilenode, item.target_id) for item in original_group.relations]
    expected_reserved = sum(item.byte_count for item in original_group.relations)

    def call(session, identifier=move_id, reviewed=review_hash, timeout_seconds=60):
        return stage_header_move(session, move_id=identifier, review_hash=reviewed,
                                 pg_controldata=binary, timeout_seconds=timeout_seconds)

    def state(identifier=move_id):
        with Session(engine) as session:
            move = session.get(StorageHeaderMoveRecord, identifier)
            return move.state, move.completion_evidence, session.get(StorageTargetRecord, "history").reserved_bytes

    def assert_original():
        inventory = read_header_catalog(engine)
        verified = verify_header_filesystem(inventory, targets, pg_controldata=binary)
        group = verified.snapshot.partitions[0]
        if [(item.oid, item.relfilenode, item.target_id) for item in group.relations] != original_nodes:
            raise RuntimeError("atomic_fixture_source_not_rolled_back")
        if state() != ("reserved", None, expected_reserved):
            raise RuntimeError("atomic_fixture_journal_not_rolled_back")
        with engine.connect() as conn:
            if conn.execute(text("SELECT id,md5(payload) FROM market.fact_versions ORDER BY id")).all() != original_rows:
                raise RuntimeError("atomic_fixture_rows_changed")

    report = {}
    class InjectedFailure(RuntimeError):
        pass

    for phase in ("table", "index"):
        fired = [False]
        prefix = "ALTER TABLE" if phase == "table" else "ALTER INDEX"
        def inject(conn, cursor, statement, parameters, context, executemany):
            if not fired[0] and statement.startswith(prefix):
                fired[0] = True
                raise InjectedFailure("injected_after_" + phase)
        event.listen(engine, "after_cursor_execute", inject)
        try:
            with Session(engine) as session, session.begin():
                session.execute(text("INSERT INTO public.atomic_move_probe VALUES (:marker)"),
                                {"marker": "savepoint-" + phase})
                try:
                    call(session)
                except InjectedFailure:
                    pass
                else:
                    raise RuntimeError("atomic_fixture_failure_not_injected")
        finally:
            event.remove(engine, "after_cursor_execute", inject)
        assert_original()
        with engine.connect() as conn:
            marker = conn.scalar(text("SELECT count(*) FROM public.atomic_move_probe WHERE marker=:marker"),
                                 {"marker": "savepoint-" + phase})
        report["atomic_savepoint_" + phase] = fired[0] and marker == 1

    # Block only the final capacity update, after the real files have moved.
    # A statement timeout must unwind the savepoint, including the prior DDL.
    blocker = engine.connect()
    blocked = [False]
    def block_bookkeeping(conn, cursor, statement, parameters, context, executemany):
        if not blocked[0] and statement.startswith("UPDATE portal_storage_targets "):
            blocker.execute(text("SET LOCAL statement_timeout='3s'"))
            blocker.execute(text("SELECT id FROM portal_storage_targets WHERE id='history' FOR UPDATE"))
            blocked[0] = True
    event.listen(engine, "before_cursor_execute", block_bookkeeping)
    started = monotonic()
    try:
        with Session(engine) as session, session.begin():
            original_timeout = session.scalar(text("SHOW statement_timeout"))
            session.execute(text("INSERT INTO public.atomic_move_probe VALUES ('bookkeeping-timeout')"))
            try:
                call(session, timeout_seconds=10)
            except DBAPIError as exc:
                sqlstate = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
                if sqlstate != "57014" or not blocked[0]:
                    raise
            else:
                raise RuntimeError("atomic_fixture_blocked_bookkeeping_did_not_timeout")
            if session.scalar(text("SHOW statement_timeout")) != original_timeout:
                raise RuntimeError("atomic_fixture_timeout_not_restored")
    finally:
        event.remove(engine, "before_cursor_execute", block_bookkeeping)
        blocker.close()
    assert_original()
    with engine.connect() as conn:
        marker = conn.scalar(text("SELECT count(*) FROM public.atomic_move_probe WHERE marker='bookkeeping-timeout'"))
    report["atomic_bookkeeping_timeout"] = blocked[0] and marker == 1 and monotonic() - started < 25

    with Session(engine) as session:
        transaction = session.begin()
        result = call(session)
        report["atomic_completion_is_provisional"] = (
            result["commit_required"] and not result["reused"]
            and session.get(StorageHeaderMoveRecord, move_id).state == "completed"
            and session.get(StorageTargetRecord, "history").reserved_bytes == 0)
        transaction.rollback()
    assert_original()
    report["atomic_outer_rollback"] = True

    for phase in ("table", "index"):
        fired = [False]
        prefix = "ALTER TABLE" if phase == "table" else "ALTER INDEX"
        def terminate(conn, cursor, statement, parameters, context, executemany):
            if not fired[0] and statement.startswith(prefix):
                fired[0] = True
                pid = conn.scalar(text("SELECT pg_backend_pid()"))
                with engine.begin() as killer:
                    if not killer.scalar(text("SELECT pg_terminate_backend(:pid, 5000)"), {"pid": pid}):
                        raise RuntimeError("atomic_fixture_backend_not_terminated")
                conn.exec_driver_sql("SELECT 1")  # Must see the dead server connection.
        event.listen(engine, "after_cursor_execute", terminate)
        try:
            try:
                with Session(engine) as session, session.begin():
                    call(session)
            except DBAPIError:
                pass
            else:
                raise RuntimeError("atomic_fixture_backend_loss_not_observed")
        finally:
            event.remove(engine, "after_cursor_execute", terminate)
        assert_original()
        report["atomic_backend_loss_" + phase] = fired[0]

    try:
        with Session(engine) as session, session.begin():
            pid = session.scalar(text("SELECT pg_backend_pid()"))
            call(session)
            with engine.begin() as killer:
                if not killer.scalar(text("SELECT pg_terminate_backend(:pid, 5000)"), {"pid": pid}):
                    raise RuntimeError("atomic_fixture_completion_backend_not_terminated")
    except DBAPIError:
        pass
    else:
        raise RuntimeError("atomic_fixture_pending_commit_loss_not_observed")
    assert_original()
    report["atomic_backend_loss_before_commit"] = True

    # Lose the actual wire response AFTER PostgreSQL emits CommandComplete(COMMIT).
    with drop_commit_ack(root, root / "socket") as proxy:
        query = {**engine.url.query, "host": str(proxy.directory),
                 "sslmode": "disable", "gssencmode": "disable"}
        proxied = create_engine(engine.url.set(query=query))
        try:
            try:
                with Session(proxied) as session, session.begin():
                    call(session)
                    proxy.armed.set()  # Only this outer COMMIT may lose its response.
            except DBAPIError:
                pass
            else:
                raise RuntimeError("atomic_fixture_commit_response_was_not_lost")
            if not proxy.committed.is_set():
                raise RuntimeError("atomic_fixture_server_commit_not_witnessed")
        finally:
            proxied.dispose()
    completed_state, completion, reserved = state()
    report["atomic_lost_commit_is_committed"] = completed_state == "completed" and reserved == 0
    changed = []
    def record_ddl(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith(("ALTER TABLE", "ALTER INDEX")):
            changed.append(statement)
    event.listen(engine, "after_cursor_execute", record_ddl)
    try:
        with Session(engine) as session, session.begin():
            retry = call(session)
    finally:
        event.remove(engine, "after_cursor_execute", record_ddl)
    report["atomic_retry_no_copy_or_release"] = retry["reused"] and not changed and state()[2] == 0
    with engine.connect() as conn:
        report["atomic_rows_preserved"] = conn.execute(
            text("SELECT id,md5(payload) FROM market.fact_versions ORDER BY id")).all() == original_rows

    # A split group: only its index needs copying. Preserve the HDD heap exactly.
    original_heap = next(item for item in completion["physical"]["members"]
                         if item["oid"] == completion["physical"]["heap_oid"])
    with engine.begin() as conn:
        move_indexes(conn, "pg_default")
    next_id, next_review, _ = prepare()
    changed.clear()
    event.listen(engine, "after_cursor_execute", record_ddl)
    try:
        with Session(engine) as session, session.begin():
            call(session, next_id, next_review)
    finally:
        event.remove(engine, "after_cursor_execute", record_ddl)
    _, next_completion, next_reserved = state(next_id)
    final_heap = next(item for item in next_completion["physical"]["members"] if item["oid"] == original_heap["oid"])
    report["atomic_retains_destination_heap"] = (
        original_heap == final_heap and len(changed) == 1
        and changed[0].startswith("ALTER INDEX") and next_reserved == 0)
    # The old receipt cannot silently certify an intervening physical rewrite.
    try:
        with Session(engine) as session, session.begin():
            call(session)
        report["atomic_changed_completion_refused"] = False
    except StorageConflict as exc:
        report["atomic_changed_completion_refused"] = str(exc) == "storage_move_completed_files_changed"
    return report

"""Real QT archive publication plus ordered-commit and interrupted-page probes."""
from dataclasses import replace
from datetime import timedelta
import hashlib
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db import Base
from scripts.db import archive_root_v2_online as online, archive_root_v2_copy as archives
from scripts.db import fact_header_v2_copy as headers
from scripts.db.fact_header_v2_capture import SCHEMA
from tests.test_market_data.test_fact_storage_tiers_db import storage, _placement, BASE
from tests.test_market_data.test_fact_book_retention_db import _cold_book_handoff
from tests.test_market_data.test_fact_header_copy_placement_db import _configure_placement
from tests.test_market_data.test_archive_reference_placement_db import _options
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_archive_root_copy_db import _hashes
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                reason="requires owned SSD/HDD storage-demo topology")]


def _prepare(storage, tmp_path, monkeypatch, *, source_directory=None, destination_directory=None,
             recent_root="/qt-source/pgdata"):
    assert os.getuid() == 70 and os.getenv("QT_DB_TEST_ISOLATED") == "1"
    source = Path(source_directory) if source_directory else Path(recent_root) / ("archive-online-"+uuid4().hex)
    source.mkdir(exist_ok=source_directory is not None)
    book = _cold_book_handoff(storage, source, monkeypatch, split_sources=False)
    storage.open_day = storage.today
    _placement(monkeypatch, storage.open_day)
    restore_tiered_v1_fixture(storage)
    _configure_placement(storage, tmp_path, monkeypatch, recent_root=recent_root)
    storage.copy_plan = replace(storage.copy_plan, history_before=storage.today-timedelta(days=30))
    destination = (Path(destination_directory) if destination_directory else
                   Path("/qt-history") / ("archive-online-"+uuid4().hex))
    destination.mkdir(parents=True, exist_ok=destination_directory is not None)
    options = dict(source_root=source/"objects", destination_root=destination,
                   page_rows=2, max_page_bytes=32*1024**2, **_options(storage))
    options["policy"] = replace(options["policy"], movement_enabled=True, backup_enabled=True)
    engine = storage.database._engine
    with engine.begin() as conn:
        headers.prepare_copy(conn, placement=storage.copy_plan)
        with pytest.raises(RuntimeError, match="raw_shadow_preparation_required"):
            online.prepare(conn, source_root=options["source_root"], destination_root=destination)
        online.raw.prepare_copy(conn)
        assert not online.prepare(conn, source_root=options["source_root"],
                                  destination_root=destination)["reused"]
        assert online.prepare(conn, source_root=options["source_root"],
                              destination_root=destination)["reused"]
    return engine, options, source, book


def _drain(engine, options, family):
    for _ in range(128):
        report = online.copy_page(engine, family=family, **options)
        assert not report["migration_ready"] and not report["final_switch_authorized"]
        if report["baseline_complete"] and report["captured_tail_empty_at_observation"]:
            return report
    pytest.fail("bounded archive fixture did not converge")


def _synthetic_descriptor(conn, source, identity):
    """Ordering diagnostic only; real publisher/reader coverage is separate."""
    original = dict(conn.execute(text("SELECT * FROM market.raw_archive_manifests LIMIT 1")).mappings().one())
    content = ("synthetic immutable ordering probe "+identity).encode()
    key = "online-probe/"+identity.replace("!", "low-")
    path = source/"objects"/key
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(content)
    original.update(id=identity, object_key=key, object_uri=path.as_uri(),
                    object_sha256=hashlib.sha256(content).hexdigest(),
                    content_fingerprint=hashlib.sha256((identity+"content").encode()).hexdigest(),
                    byte_count=len(content))
    conn.execute(Base.metadata.tables["market.raw_archive_manifests"].insert().values(**original))
    return key, content


def test_online_archive_real_publication_converges_without_losing_frozen_objects(storage, tmp_path, monkeypatch):
    engine, options, source, book = _prepare(storage, tmp_path, monkeypatch)
    original_hashes = _hashes(options["source_root"])
    # The fixture intentionally serves v1 while this image contains the v2
    # reader. Preserve exact frozen source rows here; the existing archive
    # regression separately exercises actual application reads after switching.
    with engine.connect() as conn:
        frozen = _frozen_records(conn)
    original_put = FilesystemRawArchiveObjectStore.put_verified
    published = [False]
    def publish_during_copy(self, **kwargs):
        result = original_put(self, **kwargs)
        if not published[0]:
            published[0] = True
            _raw_book_fixture(storage, source, monkeypatch, definition_id="online-archive-live",
                              provider_product_id="BTC-USD-ONLINE",
                              event_start=BASE+timedelta(hours=1))
        return result
    with monkeypatch.context() as concurrent:
        concurrent.setattr(FilesystemRawArchiveObjectStore, "put_verified", publish_during_copy)
        online.copy_page(engine, family="raw_archive_manifests", **options)
    assert published[0]
    for family in archives.FAMILIES:
        _drain(engine, options, family)
    assert _hashes(options["source_root"]) | original_hashes == _hashes(options["source_root"])
    with engine.connect() as conn:
        assert _frozen_records(conn) == frozen
    # Independent full verifier remains required until filesystem proof exists.
    verification = {k:v for k,v in options.items() if k != "max_page_bytes"}
    with engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000, max_bytes=64*1024**2,
                                                 **verification) as report:
            assert report["verified_catalog_objects"] > 0
            assert not report["root_activation_authorized"]


def test_online_archive_captures_lower_id_after_empty_observation_and_preserves_failed_page(storage, tmp_path, monkeypatch):
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    family = "raw_archive_manifests"
    _drain(engine, options, family)
    with engine.connect() as writer:
        transaction = writer.begin()
        key, content = _synthetic_descriptor(writer, source, "!"+uuid4().hex)
        observed = online.copy_page(engine, family=family, **options)
        assert observed["captured_tail_empty_at_observation"]
        transaction.commit()
    before_start = None
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE} WHERE family=:family"),
                           {"family": family}) == 1
        before_start = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
    original_put = FilesystemRawArchiveObjectStore.put_verified
    published = []
    def fail_after_publish(self, **kwargs):
        result = original_put(self, **kwargs)
        published.append((result.object_key,self.local_path(result.object_key).stat().st_ino))
        raise RuntimeError("injected archive controller interruption")
    with monkeypatch.context() as failing:
        failing.setattr(FilesystemRawArchiveObjectStore, "put_verified", fail_after_publish)
        with pytest.raises(RuntimeError, match="controller interruption"):
            online.copy_page(engine, family=family, **options)
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE} WHERE family=:family"),
                           {"family": family}) == 1
    assert published and (options["destination_root"]/key).read_bytes() == content
    resumed = online.copy_page(engine, family=family, **options)
    assert resumed["reused_objects"] == 1 and resumed["captured_tail_empty_at_observation"]
    assert (options["destination_root"]/key).stat().st_ino == published[0][1]
    with engine.connect() as conn:
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == before_start
    # Losing the commit response does not duplicate progress or require deleting files.
    with engine.begin() as writer:
        second_key, _ = _synthetic_descriptor(writer, source, "!"+uuid4().hex)
    original_commit = Connection._commit_impl
    lost = [False]
    def lose_reply(conn):
        original_commit(conn)
        if not lost[0]:
            lost[0] = True
            raise RuntimeError("injected archive commit reply lost")
    with monkeypatch.context() as uncertain:
        uncertain.setattr(Connection, "_commit_impl", lose_reply)
        with pytest.raises(RuntimeError, match="reply lost"):
            online.copy_page(engine, family=family, **options)
    assert lost[0] and (options["destination_root"]/second_key).is_file()
    retry = online.copy_page(engine, family=family, **options)
    assert retry["selected_catalog_rows"] == 0 and retry["captured_tail_empty_at_observation"]

    # Existing expiry evidence may precede this queued object's copy. Only this
    # owned synthetic object is removed; real retained fixture objects stay put.
    from portal.backend.service.storage.repos.market_lifecycle import (
        PostgresMarketStorageLifecycleRepository, _LIFECYCLE_LOCK_NAME,
    )
    identity = "!expired-"+uuid4().hex
    with engine.begin() as writer:
        expired_key, _ = _synthetic_descriptor(writer, source, identity)
    with engine.begin() as expiry:
        expiry.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name,0))"),
                       {"name": _LIFECYCLE_LOCK_NAME})
        PostgresMarketStorageLifecycleRepository()._append_event_with_session(
            expiry, operation_id="online-expiry-"+uuid4().hex, action="archive_expire",
            event_type="completed", target_kind="raw_manifest", target_id=identity,
            evidence={"scope": "owned synthetic expiry/catch-up probe"})
        (source/"objects"/expired_key).unlink()
    expired = online.copy_page(engine, family=family, **options)
    assert expired["selected_catalog_rows"] == expired["expired_catalog_rows"] == 1
    assert expired["page_objects"] == 0 and expired["captured_tail_empty_at_observation"]
    assert not (options["destination_root"]/expired_key).exists()


def test_online_archive_binding_guard_and_original_deadline_refuse_drift(storage, tmp_path, monkeypatch):
    engine, options, _, _ = _prepare(storage, tmp_path, monkeypatch)
    for sql in ["UPDATE market.raw_archive_manifests SET byte_count=byte_count",
                "DELETE FROM market.book_checkpoint_manifests",
                "TRUNCATE market.fact_archive_manifests CASCADE"]:
        with pytest.raises(DBAPIError), engine.begin() as conn:
            conn.exec_driver_sql(sql)
    other = options["destination_root"].parent/("other-"+uuid4().hex)
    other.mkdir()
    with pytest.raises(RuntimeError, match="binding_changed"):
        online.copy_page(engine, family="raw_archive_manifests",
                         **(options | {"destination_root": other}))
    with engine.begin() as conn:
        conn.exec_driver_sql(f"ALTER TABLE market.raw_archive_manifests DISABLE TRIGGER {online.TRIGGER}")
    with pytest.raises(RuntimeError, match="binding_changed"):
        online.copy_page(engine, family="raw_archive_manifests", **options)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"ALTER TABLE market.raw_archive_manifests ENABLE ALWAYS TRIGGER {online.TRIGGER}")
        conn.exec_driver_sql(f"UPDATE {SCHEMA}.capture SET prepared_at=clock_timestamp()-interval '25 hours'")
    with pytest.raises(RuntimeError, match="expired"):
        online.copy_page(engine, family="raw_archive_manifests", **options)


@pytest.mark.parametrize("scenario", [
    "test_online_archive_real_publication_converges_without_losing_frozen_objects",
    "test_online_archive_captures_lower_id_after_empty_observation_and_preserves_failed_page",
])
def test_live_file_leases_preserve_real_publication_proof_without_final_hashing(
        storage, tmp_path, monkeypatch, scenario):
    from contextlib import ExitStack
    from time import monotonic
    from scripts.db.archive_file_v2_proof import ArchiveFileProof
    original = _prepare
    held = []
    with ExitStack() as stack:
        def prepare_leased(*args):
            engine, options, source, book = original(*args)
            proof = stack.enter_context(ArchiveFileProof(options["destination_root"],
                max_files=128, max_bytes=64*1024**2, deadline=monotonic()+120))
            held.append(proof)
            # A catalog fence cannot see or authorize losing an upload that
            # has not published its descriptor. Keep that source file intact.
            unpublished = options["source_root"]/"unpublished-upload.partial"
            unpublished.write_bytes(b"not yet catalogued")
            held.append(unpublished)
            options["file_proof"] = proof
            return engine, options, source, book
        def no_final_hash(*args, **kwargs):
            pytest.fail("leased inventory reread all destination contents")
        monkeypatch.setitem(globals(), "_prepare", prepare_leased)
        monkeypatch.setattr(archives, "_sha256_file", no_final_hash)
        globals()[scenario](storage, tmp_path, monkeypatch)
        assert held[0].hashed_bytes > 0
        assert held[1].read_bytes() == b"not yet catalogued"
        assert not (held[0].root/held[1].name).exists()


def test_file_proof_reentry_requires_background_reverification_and_detects_replacement(
        storage, tmp_path, monkeypatch):
    from time import monotonic
    from scripts.db.archive_file_v2_proof import ArchiveFileProof
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    def fresh():
        return ArchiveFileProof(options["destination_root"], max_files=128,
                                max_bytes=64*1024**2, deadline=monotonic()+120)
    verification = {k:v for k,v in options.items() if k != "max_page_bytes"}
    with fresh() as first:
        for family in archives.FAMILIES:
            _drain(engine, options | {"file_proof": first}, family)
    # Copy progress survives a controller lifetime; lease proof does not.
    with fresh() as resumed:
        with pytest.raises(RuntimeError, match="object_not_verified"), engine.begin() as conn:
            with archives.verified_archive_inventory(conn, max_objects=1000,
                    max_bytes=64*1024**2, file_proof=resumed, **verification):
                pytest.fail("saved copy cursor substituted for live proof")
        # Existing cursor copier re-verifies bounded pages without altering the
        # original online cursors, queues, capture start or copying source anew.
        for family in archives.FAMILIES:
            after = ""
            for _ in range(128):
                report = archives.copy_archive_page(engine, family=family,
                    after_id=after, file_proof=resumed, **options)
                if not report["page_objects"]:
                    break
                assert not report["copied_objects"]
                after = report["next_after_id"]
            else:
                pytest.fail("reverification page budget exceeded")
        with pytest.raises(RuntimeError, match="path_changed"), engine.begin() as conn:
            with archives.verified_archive_inventory(conn, max_objects=1000,
                    max_bytes=64*1024**2, file_proof=resumed, **verification):
                # A changed pathname must not inherit the old inode's proof,
                # even if replacement bytes are exactly equal.
                path = next(p for p in options["destination_root"].rglob("*") if p.is_file())
                data = path.read_bytes()
                path.unlink()
                path.write_bytes(data)
                with pytest.raises(RuntimeError, match="path_changed"):
                    resumed.verify_all()
                # The final context exit must refuse this change as well.


def test_archive_capture_retirement_requires_live_fence_and_rolls_back(storage, tmp_path, monkeypatch):
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    roots = {k: options[k] for k in ("source_root", "destination_root")}
    verify = {k: v for k, v in options.items() if k != "max_page_bytes"}
    original_hashes = _hashes(options["source_root"])
    with engine.connect() as conn:
        original_start = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
    with pytest.raises(RuntimeError, match="live_inventory_context"), engine.begin() as conn:
        online.retire_capture(conn, **roots)
    # Copying files via another path does not retire unfinished online baselines.
    for family in archives.FAMILIES:
        after = ""
        for _ in range(128):
            report = archives.copy_archive_page(engine, family=family, after_id=after, **options)
            if not report["page_objects"]:
                break
            after = report["next_after_id"]
        else:
            pytest.fail("fixture baseline exceeded bound")
    with engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000,
                max_bytes=64*1024**2, **verify):
            with pytest.raises(RuntimeError, match="capture_not_closed"):
                online.retire_capture(conn, **roots)
    for family in archives.FAMILIES:
        _drain(engine, options, family)
    with engine.begin() as writer:
        _synthetic_descriptor(writer, source, "!retire-tail-"+uuid4().hex)
    # The final verifier sees the complete catalog, but an undrained queue must
    # still prevent capture retirement even when its file was separately copied.
    archives.copy_archive_page(engine, family="raw_archive_manifests", **options)
    with engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000,
                max_bytes=64*1024**2, **verify):
            with pytest.raises(RuntimeError, match="capture_not_closed"):
                online.retire_capture(conn, **roots)
    _drain(engine, options, "raw_archive_manifests")
    with pytest.raises(RuntimeError, match="abort after retirement"), engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000,
                max_bytes=64*1024**2, **verify):
            receipt = online.retire_capture(conn, **roots)
            assert not receipt["root_activation_authorized"]
            assert conn.scalar(text("SELECT to_regclass(:name)"), {"name": online.CLOSED})
            raise RuntimeError("abort after retirement")
    with engine.begin() as writer:
        # Rollback restored the capture function and every trigger.
        online._inspect(writer, **roots)
        _synthetic_descriptor(writer, source, "!after-abort-"+uuid4().hex)
    _drain(engine, options, "raw_archive_manifests")
    before_close = _hashes(options["source_root"])
    with engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000,
                max_bytes=64*1024**2, **verify) as old_report:
            with engine.begin() as other:
                with pytest.raises(RuntimeError, match="live_inventory_context"):
                    online.retire_capture(other, **roots)
            receipt = online.retire_capture(conn, **roots)
        with pytest.raises(RuntimeError, match="live_inventory_context"):
            online.retire_capture(conn, **roots)
    with engine.begin() as conn:
        saved = conn.scalar(text(f"SELECT receipt FROM {online.CLOSED}"))
        assert saved == receipt and saved["inventory"] == old_report
        assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == original_start
        assert conn.scalar(text(f"SELECT count(*) FROM {online.PROGRESS}")) == len(archives.FAMILIES)
        assert conn.scalar(text(f"SELECT count(*) FROM {online.QUEUE}")) == 0
        assert conn.scalar(text("SELECT count(*) FROM pg_trigger WHERE tgname IN (:capture,:guard)"),
                           {"capture": online.TRIGGER, "guard": online.GUARD}) == 0
        with pytest.raises(RuntimeError, match="capture_closed"):
            online.prepare(conn, **roots)
    with pytest.raises(RuntimeError, match="capture_closed"):
        online.copy_page(engine, family="raw_archive_manifests", **options)
    assert _hashes(options["source_root"]) == before_close
    assert all(before_close[key] == value for key, value in original_hashes.items())


def test_expired_capture_cannot_be_retired_or_restarted(storage, tmp_path, monkeypatch):
    engine, options, _, _ = _prepare(storage, tmp_path, monkeypatch)
    roots = {k: options[k] for k in ("source_root", "destination_root")}
    verify = {k: v for k, v in options.items() if k != "max_page_bytes"}
    for family in archives.FAMILIES:
        _drain(engine, options, family)
    with engine.begin() as conn:
        with archives.verified_archive_inventory(conn, max_objects=1000,
                max_bytes=64*1024**2, **verify):
            with conn.begin_nested() as rewind:
                conn.exec_driver_sql(f"UPDATE {SCHEMA}.capture SET prepared_at="
                                     "clock_timestamp()-interval '25 hours'")
                with pytest.raises(RuntimeError, match="expired"):
                    online.retire_capture(conn, **roots)
                rewind.rollback()
            assert conn.scalar(text("SELECT to_regclass(:name)"), {"name": online.CLOSED}) is None
    with engine.begin() as conn:
        online._inspect(conn, **roots)


def test_live_archive_proof_spans_real_handoff_commit_and_killed_switch(
        storage, tmp_path, monkeypatch):
    from time import monotonic
    from sqlalchemy import event
    from scripts.db import fact_header_v2_handoff as handoff
    from scripts.db.archive_file_v2_proof import ArchiveFileProof
    engine, options, source, _ = _prepare(storage, tmp_path, monkeypatch)
    roots = {k: options[k] for k in ("source_root", "destination_root")}
    with engine.connect() as conn:
        frozen = _frozen_records(conn)
        started = conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture"))
    from scripts.db import fact_header_v2_online_proof as sql_proof
    with engine.begin() as conn:
        sql_proof.prepare(conn)
    # Existing admitted staging fixture; no host hold or production action.
    handoff.stage_handoff(engine, placement=storage.copy_plan,
        max_duration_seconds=120, **options)
    with ArchiveFileProof(options["destination_root"], max_files=128,
                          max_bytes=64*1024**2, deadline=monotonic()+120) as proof:
        for family in archives.FAMILIES:
            _drain(engine, options | {"file_proof": proof}, family)
        hashed = proof.hashed_bytes
        def no_final_hash(*args, **kwargs):
            pytest.fail("leased final handoff reread file contents")
        monkeypatch.setattr(archives, "_sha256_file", no_final_hash)
        monkeypatch.setattr(headers, "_verified_pages", no_final_hash)
        finish = {k: v for k, v in options.items() if k != "max_page_bytes"}
        finish.update(max_objects=1000, max_bytes=64*1024**2, file_proof=proof)
        killed = [False]
        def kill(conn, cursor, statement, parameters, context, executemany):
            if not killed[0] and statement.startswith("ALTER TABLE market.fact_versions SET SCHEMA"):
                killed[0] = True
                with engine.begin() as killer:
                    killer.execute(text("SELECT pg_terminate_backend(:pid,5000)"),
                        {"pid": conn.connection.driver_connection.get_backend_pid()})
                conn.exec_driver_sql("SELECT 1")
        event.listen(engine, "after_cursor_execute", kill)
        try:
            with pytest.raises(DBAPIError):
                handoff.commit_handoff(engine, **finish)
        finally:
            event.remove(engine, "after_cursor_execute", kill)
        assert killed[0]
        with engine.begin() as conn:
            online._inspect(conn, **roots)
            assert conn.scalar(text("SELECT to_regclass(:name)"), {"name": online.CLOSED}) is None
            assert conn.scalar(text(f"SELECT prepared_at FROM {SCHEMA}.capture")) == started
        # The same owning process retains leases across failed SQL and through
        # the actual retry COMMIT, including a simulated lost commit response.
        original_commit = Connection._commit_impl
        observed = []
        def lost_reply(conn):
            proof.verify_all()
            original_commit(conn)
            proof.verify_all()
            observed.append(True)
            raise RuntimeError("archive handoff commit reply lost")
        with monkeypatch.context() as lost:
            lost.setattr(Connection, "_commit_impl", lost_reply)
            with pytest.raises(RuntimeError, match="commit reply lost"):
                handoff.commit_handoff(engine, **finish)
        assert observed == [True] and proof.hashed_bytes == hashed
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='10s'")
            inspected = handoff.inspect_handoff(conn, policy=options["policy"], **roots)
            assert inspected["database_handoff_committed"]
            assert conn.scalar(text(f"SELECT receipt FROM {online.CLOSED}"))["source_retained"]
            assert _frozen_records(conn) == frozen
        proof.verify_all()

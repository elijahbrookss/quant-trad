"""Owned host-driven positive entrypoint fixture, no production inputs."""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import timedelta
import json
import os
from pathlib import Path
import time

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_online_proof as protection
from scripts.db import fact_header_v2_online as online
from scripts.db import archive_reference_v2_placement as catalogs
from scripts.db import fact_header_v2_references as references
from tests.test_market_data.test_archive_online_copy_db import _prepare
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data import test_fact_storage_tiers_db as tiers
from tests.test_market_data.test_fact_storage_tiers_db import BASE
from tests.test_market_data.migration_test_support import _isolated_parent_dsn

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_ONLINE_ENTRYPOINT_FIXTURE") != "1",
    reason="requires owned host-controlled online entrypoint topology")]


@pytest.fixture
def storage(monkeypatch):
    if os.getenv("QT_ONLINE_HOST_FIXTURE") == "1":
        # The host owns this entire fresh cluster and binds POSTGRES_DB to this
        # database. Do not create a different database behind host admission.
        @contextmanager
        def fresh_host_database(_label):
            dsn = _isolated_parent_dsn()
            name = make_url(dsn).database
            assert name.startswith("qt_migration_online_") and len(name) == len("qt_migration_online_")+16
            assert all(c in "0123456789abcdef" for c in name[len("qt_migration_online_"):])
            engine = create_engine(dsn)
            try:
                with engine.begin() as conn:
                    assert conn.scalar(text("SELECT to_regnamespace('market')")) is None
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
                yield dsn
            finally:
                engine.dispose()
        monkeypatch.setattr(tiers, "fresh_migration_database", fresh_host_database)
    yield from tiers.storage.__wrapped__(monkeypatch)



def _stage_online_fixture(engine, placement, options, started):
    """Finite owned fixture using explicit production primitives, no host hold."""
    args = dict(placement=placement, policy=options["policy"],
                resource_limits=options["resource_limits"])
    steps = dict(**args, expected_started_at=started, max_duration_seconds=30)
    with engine.connect() as conn:
        retained = conn.scalar(text("SELECT to_regclass(:name)"),
                                {"name": catalogs.RETAINED_LEGACY}) is not None
    if retained:
        catalogs.move_reference_catalog(engine, relation=catalogs.RETAINED_LEGACY,
            policy=options["policy"], resource_limits=options["resource_limits"])
    for _ in range(64):
        result = online.copy_pass(engine, max_pages=2, page_rows=2,
                                  max_duration_seconds=30, **args)
        if result["outcome"] == "identity_relocation_required":
            online.preparation_step(engine, step="identity_history", **steps)
        elif result["outcome"] == "raw_relocation_required":
            online.preparation_step(engine, step="raw_history", **steps)
        elif result["outcome"] == "both_tails_observed_empty":
            break
    else:
        pytest.fail("finite online setup did not converge")
    online.preparation_step(engine, step="identity_capture", **steps)
    with engine.begin() as conn:
        slots = references.inspect_references(conn)["references"]
    for slot in slots:
        if slot["relation"] == references.PARENT:
            continue
        for step in ("reference_prepare", "reference_validate"):
            online.preparation_step(engine, step=step, relation=slot["relation"], **steps)
    online.preparation_step(engine, step="reference_adopt", **steps)
    for relation in catalogs.RELATIONS:
        catalogs.move_reference_catalog(engine, relation=relation,
            policy=options["policy"], resource_limits=options["resource_limits"])
    # Archive baseline copying belongs to the live controller, not this setup.

@dataclass(frozen=True)
class _UnpreparedTargets:
    """Fixture configuration only; deliberately has no tablespace OID/authority."""
    recent: object
    history: object
    history_before: object


def _declare_targets(storage, control, monkeypatch, *, recent_root):
    from core.storage_targets import StorageTarget
    assert os.getenv("QT_DB_TEST_ISOLATED") == "1" and os.getuid() == 70
    recent, history = Path(recent_root), Path("/qt-history")
    assert recent.stat().st_dev != history.stat().st_dev
    real = os.getenv("QT_ONLINE_CANONICAL_FIXTURE") == "1"
    udev = Path("/run/qt-host-udev/data") if real else control/"udev-placement"
    if not real:
        udev.mkdir()
    def identity(root, fallback):
        if not real:
            return fallback
        dev = root.stat().st_dev
        lines = (udev/f"b{os.major(dev)}:{os.minor(dev)}").read_text().splitlines()
        values = [line.split("=", 1)[1] for line in lines if line.startswith("E:ID_FS_UUID=")]
        assert len(values) == 1 and values[0]
        return values[0]
    targets = (StorageTarget("ssd", "Recent", identity(recent, "uuid-copy-ssd"), str(recent), "ssd"),
               StorageTarget("hdd", "History", identity(history, "uuid-copy-hdd"), str(history), "hdd"))
    if not real:
        for target in targets:
            dev = Path(target.root).stat().st_dev
            (udev/f"b{os.major(dev)}:{os.minor(dev)}").write_text("E:ID_FS_UUID="+target.filesystem_uuid+"\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    storage.copy_plan = _UnpreparedTargets(*targets, storage.today)
    storage.copy_udev = udev
    return storage


def test_prepared_worker_serves_and_catches_live_publication(storage, tmp_path, monkeypatch):
    control = Path("/qt-control")
    deadline = time.monotonic()+180
    def wait(name):
        while not (control/name).exists():
            if time.monotonic() >= deadline:
                raise AssertionError("online entrypoint fixture host deadline: "+name)
            time.sleep(0.1)
    initial_capture = os.getenv("QT_ONLINE_INITIAL_CAPTURE") == "1"
    atomic = os.getenv("QT_ONLINE_ATOMIC_PREPARE") == "1"
    worker_phases = os.getenv("QT_ONLINE_WORKER_PHASES") == "1"
    assert not worker_phases or atomic
    full_operation = os.getenv("QT_ONLINE_FULL_OPERATION") == "1"
    if full_operation:
        assert initial_capture and worker_phases
        from tests.test_market_data import test_archive_online_copy_db as archive_fixture
        monkeypatch.setattr(archive_fixture, "_configure_placement", _declare_targets)
    runtime_recovery = None
    if os.getenv("QT_ONLINE_RUNTIME_FIXTURE")=="1":
        from tests.test_market_data.online_runtime_recovery_fixture import configure_source
        runtime_recovery = configure_source(storage, monkeypatch)
    engine, options, source, book = _prepare(storage, control, monkeypatch,
        source_directory="/app/logs/market-structure",
        destination_directory="/qt-history/archives/objects",
        recent_root="/var/lib/postgresql/data", prepare_captures=not atomic)
    if os.getenv("QT_ONLINE_RUNTIME_FIXTURE")=="1":
        # A prospective full startup observation, declared before capture/worker
        # binding. The original 180s capture clock is never extended.
        options["resource_limits"]["movement_timeout_seconds"] = 120
        # Only these freshly created empty fixture destination directories.
        # Original private source archives keep their ownership and permissions.
        destination=Path("/qt-history/archives/objects")
        assert not any(destination.iterdir())
        if os.getenv("QT_ONLINE_CANONICAL_FIXTURE") == "1":
            # The empty shared root is provisioned before source startup with
            # the normal operator/application owner and the database group.
            parent = destination.parent.stat()
            assert (parent.st_uid, parent.st_gid, parent.st_mode & 0o7777) == (1000, 70, 0o2770)
        else:
            destination.parent.chmod(0o2770)
        destination.chmod(0o2770)
    if atomic and not initial_capture:
        online.prepare_attempt(engine, placement=storage.copy_plan, attempt_seconds=180,
            **{k:v for k,v in options.items() if k not in {"page_rows", "max_page_bytes"}})
    with engine.begin() as conn:
        if initial_capture:
            assert conn.scalar(text("SELECT to_regclass('qt_fact_header_cutover_v2.capture')")) is None
            started = None
        else:
            protection.prepare(conn)
            started = capture.inspect_capture(conn)["started_at"]
        identity = conn.scalar(text("SELECT system_identifier::text||'/'||"
            "(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system()"))
        frozen = _frozen_records(conn)
        assert conn.scalar(text("SHOW archive_mode")) == "off"
    retained_before = None
    if worker_phases and not full_operation:
        # Create an actual immutable retained rollback source in this owned
        # database. The fixture never moves it; the retained worker must do so.
        from tests.test_market_data.test_archive_reference_placement_db import _seed_retained_source, _rows
        with engine.begin() as conn:
            retained = conn.scalar(text("SELECT to_regclass(:name)"),
                {"name": catalogs.RETAINED_LEGACY}) is not None
        if not retained:
            retained_before = _seed_retained_source(engine)
        else:
            with engine.begin() as conn:
                retained_before = _rows(conn, catalogs.RETAINED_LEGACY)
    elif not worker_phases and atomic:
        _stage_online_fixture(engine, storage.copy_plan, options, started)
    elif not worker_phases:
        handoff.stage_handoff(engine, placement=storage.copy_plan, max_duration_seconds=120, **options)
    info=(source/"objects").stat()
    request=dict(schema_version="qt.storage_online_worker.v1",
        source_revision=os.environ["QT_IMAGE_SOURCE_REVISION"],
        source_tree_hash=os.environ["QT_IMAGE_SOURCE_TREE_HASH"],
        database_identity=identity, source_device=info.st_dev, source_inode=info.st_ino,
        expected_started_at=started, policy=asdict(options["policy"]),
        resource_limits=options["resource_limits"], max_page_bytes=options["max_page_bytes"],
        max_objects=128,max_bytes=64*1024**2,page_rows=2,command_seconds=30)
    if initial_capture:
        request["capture_preparation"] = dict(history_before=storage.copy_plan.history_before.isoformat(),
            attempt_seconds=180, requested_at=None, deadline=None)
    if os.getenv("QT_ONLINE_RUNTIME_FIXTURE")=="1":
        request["archive_shared_group_id"]=70
    # Generated disposable credentials only, private fixture control, never receipts.
    (control/"connection.json").write_text(json.dumps({"dsn":storage.dsn}))
    (control/"request.json").write_text(json.dumps(request))
    (control/"inventory.json").write_text(json.dumps({"schema_version":"qt.storage_inventory.v1",
        "targets":[asdict(storage.copy_plan.recent),asdict(storage.copy_plan.history)]}))
    if os.getenv("QT_SIGNAL_REAL_PUBLICATION") == "1":
        from tests.test_market_data.online_signal_publication_fixture import prepare_definition
        prepare_definition(storage, book, source/"objects")
    (control/"ready.json").write_text(json.dumps({"udev":str(storage.copy_udev)}))
    if full_operation:
        # Seed then retire BEFORE the operator replaces the source DB. No capture,
        # placement, migration, or publication is performed by this fixture later.
        assert runtime_recovery is not None
        for label, hour in (("live", 4), ("final", 5)):
            _raw_book_fixture(storage, source, monkeypatch,
                definition_id="host-entrypoint-"+label,
                provider_product_id="BTC-USD-HOST-"+label.upper(),
                event_start=BASE+timedelta(hours=hour), replay_features=True)
        from tests.test_market_data.online_runtime_recovery_fixture import finish_source
        finish_source(storage, runtime_recovery, control, options["policy"])
        with engine.connect() as conn:
            assert conn.scalar(text("SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'qt%cutover%'")) == 0
            assert _frozen_records(conn) == frozen
        return
    wait("publish")
    if initial_capture:
        with engine.begin() as conn:
            started = capture.inspect_capture(conn)["started_at"]
        assert started is not None
    _raw_book_fixture(storage, source, monkeypatch,
        definition_id="host-entrypoint-live",provider_product_id="BTC-USD-HOST-ENTRY",
        event_start=BASE+timedelta(hours=4), replay_features=runtime_recovery is not None)
    (control/"published").write_text("published")
    if worker_phases:
        wait("phases-finished")
        with engine.begin() as conn:
            assert references.inspect_references(conn)["references_complete"]
            for relation in (*catalogs.RELATIONS, catalogs.RETAINED_LEGACY):
                assert catalogs.inspect_reference_catalog(conn, relation=relation)["placement"] == "history"
            assert _rows(conn, catalogs.RETAINED_LEGACY) == retained_before
        (control/"catalogs-verified").write_text("verified")
    if os.getenv("QT_ONLINE_FINAL_DELTA") == "1":
        wait("final-publish")
        _raw_book_fixture(storage, source, monkeypatch,
            definition_id="host-entrypoint-final", provider_product_id="BTC-USD-HOST-FINAL",
            event_start=BASE+timedelta(hours=5), replay_features=runtime_recovery is not None)
        (control/"final-published").write_text("published")
    wait("finished")
    if runtime_recovery is not None:
        from tests.test_market_data.online_runtime_recovery_fixture import finish_source
        finish_source(storage, runtime_recovery, control, options["policy"])
    with engine.begin() as conn:
        assert _frozen_records(conn)==frozen
        assert capture.inspect_capture(conn)["started_at"]==started
        assert conn.scalar(text("SHOW archive_mode"))=="off"
    assert (source/"objects").stat().st_ino==info.st_ino

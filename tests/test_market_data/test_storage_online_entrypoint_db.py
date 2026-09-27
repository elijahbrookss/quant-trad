"""Owned host-driven positive entrypoint fixture, no production inputs."""
from contextlib import contextmanager
from dataclasses import asdict
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

def test_prepared_worker_serves_and_catches_live_publication(storage, tmp_path, monkeypatch):
    control = Path("/qt-control")
    deadline = time.monotonic()+180
    def wait(name):
        while not (control/name).exists():
            if time.monotonic() >= deadline:
                raise AssertionError("online entrypoint fixture host deadline: "+name)
            time.sleep(0.1)
    atomic = os.getenv("QT_ONLINE_ATOMIC_PREPARE") == "1"
    worker_phases = os.getenv("QT_ONLINE_WORKER_PHASES") == "1"
    assert not worker_phases or atomic
    engine, options, source, _ = _prepare(storage, control, monkeypatch,
        source_directory="/app/logs/market-structure",
        destination_directory="/qt-history/archives/objects",
        recent_root="/var/lib/postgresql/data", prepare_captures=not atomic)
    if atomic:
        online.prepare_attempt(engine, placement=storage.copy_plan, attempt_seconds=180,
            **{k:v for k,v in options.items() if k not in {"page_rows", "max_page_bytes"}})
    with engine.begin() as conn:
        protection.prepare(conn)
        started = capture.inspect_capture(conn)["started_at"]
        identity = conn.scalar(text("SELECT system_identifier::text||'/'||"
            "(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system()"))
        frozen = _frozen_records(conn)
        assert conn.scalar(text("SHOW archive_mode")) == "off"
    if worker_phases:
        # The retained-table move remains an explicit external phase. Only the
        # controller's qualified private/reference phases go through its pipe.
        with engine.begin() as conn:
            retained = conn.scalar(text("SELECT to_regclass(:name)"),
                {"name": catalogs.RETAINED_LEGACY}) is not None
        if retained:
            catalogs.move_reference_catalog(engine, relation=catalogs.RETAINED_LEGACY,
                policy=options["policy"], resource_limits=options["resource_limits"])
    elif atomic:
        _stage_online_fixture(engine, storage.copy_plan, options, started)
    else:
        handoff.stage_handoff(engine, placement=storage.copy_plan, max_duration_seconds=120, **options)
    info=(source/"objects").stat()
    request=dict(schema_version="qt.storage_online_worker.v1",
        source_revision=os.environ["QT_IMAGE_SOURCE_REVISION"],
        source_tree_hash=os.environ["QT_IMAGE_SOURCE_TREE_HASH"],
        database_identity=identity, source_device=info.st_dev, source_inode=info.st_ino,
        expected_started_at=started, policy=asdict(options["policy"]),
        resource_limits=options["resource_limits"], max_page_bytes=options["max_page_bytes"],
        max_objects=128,max_bytes=64*1024**2,page_rows=2,command_seconds=30)
    # Generated disposable credentials only, private fixture control, never receipts.
    (control/"connection.json").write_text(json.dumps({"dsn":storage.dsn}))
    (control/"request.json").write_text(json.dumps(request))
    (control/"inventory.json").write_text(json.dumps({"schema_version":"qt.storage_inventory.v1",
        "targets":[asdict(storage.copy_plan.recent),asdict(storage.copy_plan.history)]}))
    (control/"ready.json").write_text(json.dumps({"udev":str(storage.copy_udev)}))
    wait("publish")
    _raw_book_fixture(storage, source, monkeypatch,
        definition_id="host-entrypoint-live",provider_product_id="BTC-USD-HOST-ENTRY",
        event_start=BASE+timedelta(hours=4))
    (control/"published").write_text("published")
    if worker_phases:
        wait("inspect-references")
        # This private diagnostic channel supplies only fixture catalog names;
        # it is not a production discovery or release-authority interface.
        with engine.begin() as conn:
            relations = [r["relation"] for r in references.inspect_references(conn)["references"]
                         if r["relation"] != references.PARENT]
        assert len(relations) <= 128
        (control/"references.json").write_text(json.dumps(relations))
        wait("phases-finished")
        with engine.begin() as conn:
            assert references.inspect_references(conn)["references_complete"]
        for relation in catalogs.RELATIONS:
            catalogs.move_reference_catalog(engine, relation=relation,
                policy=options["policy"], resource_limits=options["resource_limits"])
        (control/"catalogs-moved").write_text("moved")
    if os.getenv("QT_ONLINE_FINAL_DELTA") == "1":
        wait("final-publish")
        _raw_book_fixture(storage, source, monkeypatch,
            definition_id="host-entrypoint-final", provider_product_id="BTC-USD-HOST-FINAL",
            event_start=BASE+timedelta(hours=5))
        (control/"final-published").write_text("published")
    wait("finished")
    with engine.begin() as conn:
        assert _frozen_records(conn)==frozen
        assert capture.inspect_capture(conn)["started_at"]==started
        assert conn.scalar(text("SHOW archive_mode"))=="off"
    assert (source/"objects").stat().st_ino==info.st_ino

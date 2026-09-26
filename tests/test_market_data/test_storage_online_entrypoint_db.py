"""Owned host-driven positive entrypoint fixture, no production inputs."""
from dataclasses import asdict
from datetime import timedelta
import json
import os
from pathlib import Path
import time

import pytest
from sqlalchemy import text

from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_online_proof as protection
from tests.test_market_data.test_archive_online_copy_db import _prepare
from tests.test_market_data.test_fact_header_copy_db import _frozen_records
from tests.test_market_data.test_fact_raw_lineage_db import _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import BASE, storage

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_ONLINE_ENTRYPOINT_FIXTURE") != "1",
    reason="requires owned host-controlled online entrypoint topology")]


def test_prepared_worker_serves_and_catches_live_publication(storage, tmp_path, monkeypatch):
    control = Path("/qt-control")
    deadline = time.monotonic()+180
    def wait(name):
        while not (control/name).exists():
            if time.monotonic() >= deadline:
                raise AssertionError("online entrypoint fixture host deadline: "+name)
            time.sleep(0.1)
    engine, options, source, _ = _prepare(storage, control, monkeypatch,
        source_directory="/app/logs/market-structure",
        destination_directory="/qt-history/archives/objects",
        recent_root="/var/lib/postgresql/data")
    with engine.begin() as conn:
        protection.prepare(conn)
        started = capture.inspect_capture(conn)["started_at"]
        identity = conn.scalar(text("SELECT system_identifier::text||'/'||"
            "(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system()"))
        frozen = _frozen_records(conn)
        assert conn.scalar(text("SHOW archive_mode")) == "off"
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
    wait("finished")
    with engine.begin() as conn:
        assert _frozen_records(conn)==frozen
        assert capture.inspect_capture(conn)["started_at"]==started
        assert conn.scalar(text("SHOW archive_mode"))=="off"
    assert (source/"objects").stat().st_ino==info.st_ino

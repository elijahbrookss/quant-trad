"""Packaged raw placement and encrypted restore on disposable native storage.

The heartbeat is an explicit test fixture, not a live collector claim. The CLI,
PostgreSQL/filesystem verifier, native move, backup tools and readers are real.
This does not measure production throughput or authorize host maintenance.
"""
from datetime import timedelta
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import text

from core.storage_targets import StoragePolicy
from scripts.db import raw_mapping_v2_placement as raw_move
from scripts.db import fact_header_forward_adoption as adoption
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_references as references
from scripts.db import archive_reference_v2_placement as catalogs
from scripts.db import archive_root_v2_copy as archives
from scripts.db.fact_header_v2_deadline import CONTROLLER_LOCK
from tests.test_market_data.test_archive_forward_copy_db import _prepare_forward, _drain
from tests.test_market_data.test_archive_reference_placement_db import _options, _rows
from tests.test_market_data.test_fact_storage_tiers_db import _placement
from tests.test_market_data.test_incremental_application_db import (
    storage, test_encrypted_incremental_restores_qt_cold_current_frozen_and_book_replay as _exercise_restore,
)

pytestmark = [pytest.mark.db, pytest.mark.skipif(os.getenv("QT_INCREMENTAL_APPLICATION_TEST") != "1",
    reason="requires a fresh owned incremental-recovery Compose topology")]


def prepare_retained_raw(storage, tmp_path, monkeypatch):
    """Use actual archives, keeping fault-injection descriptors out of replay."""
    today = storage.today
    storage.today -= timedelta(days=3)
    _placement(monkeypatch, storage.today)
    root = Path("/qt-history")/("incremental-raw-"+uuid4().hex)
    operation = "c"*64
    engine, options, source, _, _, _, _, started, _ = _prepare_forward(storage, tmp_path, monkeypatch,
        successor_operation=operation, recent_lookup=True,
        destination_directory=root/"objects", synthetic_probes=False)
    for _ in range(64):
        with engine.begin() as conn:
            state = adoption._state(conn, operation)
            if all(state["progress"]["identity_"+side]["complete"] for side in ("source", "target")):
                adoption._retain_raw_source(conn, state)
                break
            adoption.adoption_page(conn, operation_sha256=operation, page_rows=128)
    else:
        pytest.fail("retained raw recovery identity preparation did not converge")
    forward = dict(options, forward_operation_sha256=operation)
    for family in archives.FAMILIES:
        _drain(engine, forward, family)
    for _ in range(64):
        with engine.begin() as conn:
            result = adoption.adoption_page(conn, operation_sha256=operation, page_rows=128)
        if result["retained_targets_verified"]:
            break
    else:
        pytest.fail("retained raw recovery adoption did not converge")
    with engine.begin() as conn:
        inventory = adoption.inspect_references(conn, operation_sha256=operation)["references"]
    for relation in inventory:
        if relation != references.PARENT:
            with engine.begin() as conn:
                adoption.prepare_reference(conn, operation_sha256=operation, relation=relation)
            with engine.begin() as conn:
                adoption.validate_reference(conn, operation_sha256=operation, relation=relation)
    with engine.begin() as conn:
        adoption.adopt_payload_references(conn, operation_sha256=operation)
    with engine.connect() as owner:
        try:
            with owner.begin():
                assert owner.scalar(text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"),
                    {"key": CONTROLLER_LOCK})
            for relation in catalogs.RELATIONS:
                catalogs.move_reference_catalog(engine, relation=relation, policy=options["policy"],
                    resource_limits=options["resource_limits"], expected_started_at=started.isoformat(),
                    placement=storage.copy_plan, forward_operation_sha256=operation, connection=owner)
            finish = {key: value for key, value in options.items() if key != "max_page_bytes"}
            handoff.commit_handoff(engine, **finish, max_objects=1000, max_bytes=64*1024**2,
                forward_operation_sha256=operation, forward_end_day=today, connection=owner,
                deadline=monotonic()+30, activate_policy=True)
        finally:
            owner.invalidate()
    # Preserve the old archives but make their original location unavailable.
    # All following current/frozen/replay reads must use the HDD copy.
    source.rename(source.with_name(source.name+"-retained"))
    storage.today = storage.open_day = today
    return root


def execute_packaged_move(storage, source_root, tmp_path):
    """Actual packaged CLI; fixture liveness does not simulate a running loop."""
    engine = storage.database._engine
    worker_id = "storage-maintenance:"+socket.gethostname()+":raw-recovery-fixture"
    with engine.begin() as conn:
        receipt = raw_move._handoff(raw_move._state(conn))
        policy = StoragePolicy.from_dict(conn.scalar(text("SELECT policy FROM public.portal_storage_policy WHERE id=1")))
        before = raw_move._files(conn)
        conn.execute(text("INSERT INTO market.collector_worker_state "
            "(worker_id,worker_role,worker_version,state,started_at,heartbeat_at,expires_at,context) "
            "VALUES(:worker,'market_storage_maintenance','fixture','idle',clock_timestamp(),clock_timestamp(),"
            "clock_timestamp()+interval '1 hour',CAST(:context AS jsonb))"),
            {"worker": worker_id, "context": json.dumps({"storage_lifecycle":{"state":"running"}})})
    request = {"schema_version": raw_move.OPERATION, "source_revision": os.environ["QT_IMAGE_SOURCE_REVISION"],
        "request_id": "restore-raw-"+uuid4().hex, "handoff_sha256": raw_move._digest(receipt),
        "policy": policy.to_dict(), "resource_limits": _options(storage)["resource_limits"]}
    request["resource_limits"]["movement_timeout_seconds"] = 180
    env = {**os.environ, "PG_DSN":storage.dsn, "QT_DISABLE_DOTENV":"1",
        "QT_STORAGE_MAINTENANCE_OWNER":"dedicated", "QT_ARCHIVE_SHARED_GROUP_ID":"70",
        "MARKET_STRUCTURE_STORAGE_ROOT":str(source_root), "MARKET_STRUCTURE_WORKING_ROOT":str(source_root),
        "QT_MARKET_DATA_EXPECTED_UUID":storage.copy_plan.history.filesystem_uuid,
        "QT_MARKET_DATA_WORKING_EXPECTED_UUID":storage.copy_plan.history.filesystem_uuid,
        "QT_STORAGE_UDEV_ROOT":str(storage.copy_udev)}
    # Production maintenance carries the image identity, not research settings.
    env.pop("SOURCE_REVISION", None)
    env.pop("SOURCE_TREE_HASH", None)
    command = [sys.executable,"-m","cli.main","--no-audit-log","storage","place-retained-raw","--request-file","-"]
    def invoke(*action):
        result = subprocess.run([*command, *action], input=json.dumps(request), env=env,
            text=True, capture_output=True, timeout=210)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)
    inspected = invoke()
    assert inspected["state"] == "not_started" and not inspected["execution_admitted"]
    with engine.begin() as conn:
        assert raw_move._plan(conn, inspected["plan_id"]) is None and raw_move._files(conn) == before
    moved = invoke("--execute")
    assert not moved["reused"] and not moved["recovery_verified"]
    assert moved["maintenance_worker_id"] == worker_id
    assert invoke()["state"] == "completed"
    assert invoke("--execute")["reused"]
    assert invoke("--cancel")["reused"]  # A completed move is never reversed.
    with engine.begin() as conn:
        expected = {"rows": _rows(conn, raw_move.raw.SOURCE),
            "retained": _rows(conn, handoff.RETAINED+"."+raw_move.raw.NAME),
            "evidence": raw_move._state(conn), "plan":raw_move._plan(conn,moved["plan_id"]),
            "files":raw_move._files(conn)}
        assert expected["evidence"]["handoff"] == receipt
    return expected


def assert_restored_raw(conn, expected):
    # A physical restore preserves relation identities and placement metadata;
    # its new host directory inode is deliberately not the old machine's one.
    assert _rows(conn, raw_move.raw.SOURCE) == expected["rows"]
    assert _rows(conn, handoff.RETAINED+"."+raw_move.raw.NAME) == expected["retained"]
    assert raw_move._state(conn) == expected["evidence"]
    assert raw_move._plan(conn, expected["plan"]["id"]) == expected["plan"]
    assert raw_move._files(conn) == expected["files"]


def test_packaged_raw_move_is_in_encrypted_restore_with_current_frozen_and_raw_reads(storage, tmp_path, monkeypatch):
    _exercise_restore(storage, tmp_path, monkeypatch, retained_raw=True)

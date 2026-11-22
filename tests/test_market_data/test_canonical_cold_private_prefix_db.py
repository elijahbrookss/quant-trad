"""Exercise the private prefix entry point across native legacy and dated facts."""

from contextlib import nullcontext
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_IMMUTABILITY_TRIGGER
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from scripts.db import canonical_cold_private_prefix as prefix, canonical_cold_private_seal as seal
from scripts.db.canonical_cold_private_placement import SCHEMA as PLACEMENT_SCHEMA
from scripts.db.canonical_cold_receipts import validate_audit_receipts
from tests.test_market_data.test_canonical_cold_activate_db import _attach_legacy_child, _configure_fixture_catalog
from tests.test_market_data.test_fact_storage_tiers_db import _ingest, _placement, storage


@pytest.mark.db
@pytest.mark.parametrize("storage", ["benchmark"], indirect=True)
def test_multiday_prefix_entrypoint_resumes_and_refuses_stale_export(storage, tmp_path, monkeypatch):
    engine = storage.database._engine
    legacy_day = storage.today - timedelta(days=32)
    dated_day = legacy_day + timedelta(days=1)
    root, candidate, work, original = (tmp_path / name for name in ("clone", "candidate", "work", "original"))
    for path in (root, candidate, work, original):
        path.mkdir(mode=0o700)
    old_store = FilesystemRawArchiveObjectStore(original / "objects")
    _placement(monkeypatch, legacy_day)
    _ingest(storage)
    _ingest(storage, replace(storage.fact, observation_key="legacy-sql-tail"))
    _placement(monkeypatch, dated_day)
    _ingest(storage, replace(storage.fact, observation_key="dated-open"))
    owner = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=old_store,
        temporary_directory=original / "staging", limits=FactArchiveLimits(max_rows=1, row_group_size=1))
    owner.seal_partition(legacy_day)
    assert owner.stage_next_page(legacy_day)["status"] == "page_acknowledged"
    shutil.copytree(old_store.root, candidate / "objects")
    with engine.begin() as conn:
        database = conn.exec_driver_sql("""
            SELECT system_identifier::text || '/' || (SELECT oid FROM pg_database WHERE datname=current_database())
            FROM pg_control_system()
        """).scalar_one()
        commit = storage.repo._current_commit_seq_with_session(conn)
        _attach_legacy_child(conn, legacy_day, dated_day)
        _configure_fixture_catalog(conn, database, candidate)

    device = root.stat().st_dev
    device_id = f"{os.major(device)}:{os.minor(device)}"
    clone = seal.PrivateClone(engine.url, {"database_identity": database, "accepted_commit_seq": commit,
        "device_id": device_id, "archive_objects_inode": old_store.root.stat().st_ino},
        root / "control/socket/.s.PGSQL.5432")
    proof, placement, limits = (tmp_path / name for name in ("clone.json", "placement.json", "limits.json"))

    def document(path, data):
        path.write_text(json.dumps(data, sort_keys=True))
        return sha256(path.read_bytes()).hexdigest()

    proof_sha = document(proof, clone.proof)
    body = {"schema": PLACEMENT_SCHEMA, "clone_proof_sha256": proof_sha,
            "database_identity": database, "accepted_commit_seq": commit, "device_id": device_id,
            "archive_root": str(candidate), "history_target_id": "activation-fixture"}
    placement_sha = document(placement, body | {"verification_sha256": archive_evidence_hash(body)})
    load_caps = prefix.SharedHddLoadLimits(1, 1024**2, 1024**2, 1024**2, 2 * 1024**2, 0, 1024)
    limits_sha = document(limits, vars(load_caps))

    # The native app DB fixture runs over loopback without a recovered
    # postmaster. Exercise the real entrypoint and protected journal with a
    # synthetic mount namespace and those two private postmaster checks replaced.
    monkeypatch.setattr(prefix, "load_private_clone", lambda *_: clone)
    monkeypatch.setattr(prefix, "assert_private_placement", lambda *_a, **_kw: None)
    monkeypatch.setattr(seal, "assert_promoted_clone", lambda *_: None)
    monkeypatch.setattr(prefix.socket, "if_nameindex", lambda: [(1, "lo")])
    monkeypatch.setattr(prefix, "supervise_private_page", lambda *_a, **_kw: nullcontext())
    monkeypatch.delenv("QT_MARKET_DATA_EXPECTED_UUID", raising=False)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(candidate))

    args = SimpleNamespace(phase="plan", max_batches=1, max_days=64, max_seconds=60, pages_per_batch=1,
        max_export_bytes=1024**2, min_free_bytes=1, batch_work_bytes=2 * FactArchiveLimits().max_file_bytes,
        proof=proof, proof_sha256=proof_sha, placement=placement, placement_sha256=placement_sha,
        work_root=work, through_day=dated_day, prefix_sha256=None, seal_day=None, source_archive_object_root=None,
        header_oid=None, payload_oid=None, count_statement_timeout_ms=1000,
        load_limits=limits, load_limits_sha256=limits_sha, pg_controldata=Path("/bin/true"))
    original_statvfs = os.statvfs

    def statvfs(path):
        actual = original_statvfs(path)
        readonly = path in {proof, placement, limits, old_store.root} or (path == candidate and args.phase != "stage")
        return SimpleNamespace(f_flag=os.ST_RDONLY if readonly else 0, f_bavail=actual.f_bavail,
                               f_frsize=actual.f_frsize)

    monkeypatch.setattr(os, "statvfs", statvfs)
    mount_backing = {"work": "/rehearsal/work"}
    original_open = Path.open

    def open_path(path, *open_args, **kwargs):
        if path == Path("/proc/self/mountinfo"):
            lines = [f"1 0 {device_id} /rehearsal/clone {root} rw - ext4 /dev/fixture rw\n",
                     f"2 0 {device_id} /rehearsal/candidate {candidate} ro - ext4 /dev/fixture rw\n",
                     f"3 0 {device_id} {mount_backing['work']} {work} rw - ext4 /dev/fixture rw\n"]
            return BytesIO("".join(lines).encode())
        return original_open(path, *open_args, **kwargs)

    monkeypatch.setattr(Path, "open", open_path)
    planned = prefix.run(args)
    assert planned["prefix"]["legacy_end_day"] == dated_day.isoformat()
    first, second = planned["prefix"]["days"]
    assert first["child"] == "fact_versions_legacy"
    assert second["day"] == dated_day.isoformat() and planned["retention"][second["day"]]["state"] == "open"
    args.prefix_sha256 = planned["prefix_sha256"]
    args.phase = "stage"
    mount_backing["work"] = "/rehearsal/clone/pgdata"
    with pytest.raises(ValueError, match="archive_backing_overlaps_clone"):
        prefix.run(args)
    assert not (work / "migration.sqlite").exists()
    mount_backing["work"] = "/rehearsal/work"

    assert prefix.run(args)["next_day"] == legacy_day.isoformat()
    assert prefix.run(args)["next_day"] == dated_day.isoformat()
    assert prefix.run(args)["deferred_reason"] == "private_prefix_day_requires_seal"
    args.phase = "load"
    with pytest.raises(RuntimeError, match="required_phase_incomplete: stage"):
        prefix.run(args)

    args.phase, args.seal_day = "seal", dated_day
    args.header_oid, args.payload_oid = second["child_oid"], planned["retention"][second["day"]]["payload_oid"]
    args.source_archive_object_root = old_store.root
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(original))
    sealed = prefix.run(args)["seal"]
    assert sealed["state"] == "sealed" and sealed["expected_rows"] == 1
    assert prefix.run(args)["seal"] == sealed
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(candidate))

    for phase in ("stage", "load", "audit", "export"):
        args.phase = phase
        for attempt in range(8):
            visited = prefix.run(args)
            if visited["complete"]:
                break
        else:
            pytest.fail(f"private prefix phase did not finish: {phase}")
    audit_file = work / "audit-receipts.json"
    audit = validate_audit_receipts(json.loads(audit_file.read_bytes()))
    assert (audit["verified_days"], audit["verified_pages"], audit["verified_rows"]) == (2, 3, 3)
    assert visited["artifacts"]["audit"]["sha256"] == sha256(audit_file.read_bytes()).hexdigest()
    assert json.loads((work / "seal-receipts.json").read_bytes()) == [sealed]

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE market.fact_retention_partitions SET expected_rows=2 WHERE storage_day=:day
        """), {"day": dated_day})
    with pytest.raises(RuntimeError, match="activation_retention_changed"):
        prefix.run(args)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE market.fact_retention_partitions SET expected_rows=1 WHERE storage_day=:day
        """), {"day": dated_day})
        conn.exec_driver_sql(f"ALTER TABLE market.cold_fact_pages DISABLE TRIGGER {COLD_IMMUTABILITY_TRIGGER}")
        conn.execute(text("""
            UPDATE market.cold_fact_pages SET manifest_hash=repeat('f',64)
            WHERE id=(SELECT min(id) FROM market.cold_fact_pages)
        """))
        conn.exec_driver_sql(f"ALTER TABLE market.cold_fact_pages ENABLE ALWAYS TRIGGER {COLD_IMMUTABILITY_TRIGGER}")
    with pytest.raises(RuntimeError, match="export_loaded_inventory_changed"):
        prefix.run(args)

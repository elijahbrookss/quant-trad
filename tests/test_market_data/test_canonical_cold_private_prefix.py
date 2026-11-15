from contextlib import nullcontext
from datetime import date
from hashlib import sha256
import json
import os
from types import SimpleNamespace

import pytest

from scripts.db import canonical_cold_private_prefix as prefix
from scripts.db.canonical_cold_migration import MigrationCheckpoint


def test_forward_and_inverse_batches_resume_from_persisted_receipts(tmp_path, monkeypatch):
    checkpoint = MigrationCheckpoint(tmp_path / "migration.sqlite")
    scope = {"fixed": "scope"}
    journal = prefix.PrefixJournal(checkpoint, scope)
    plan = {"cold_before_day": "2026-10-15", "days": [{"day": "2026-10-13"}, {"day": "2026-10-14"}]}
    for row in plan["days"]:
        journal.record("load", {"committed": True}, day=row["day"], done=True)
    native = []

    def audit(_engine, _checkpoint, *, day=None, after_ordinal=-1, inventory=False, after_manifest="", **_):
        result = {"prefix_hash": "a" * 64}
        if day is not None:
            native.append((day.isoformat(), after_ordinal))
            return result | {"day_batch": {"verified_pages": 1, "last_verified_ordinal": after_ordinal + 1,
                "day_scan_end_reached": day.day == 14 or after_ordinal == 0}}
        if inventory:
            native.append(("inverse", after_manifest))
            return result | {"inverse_inventory_batch": {"verified_pages": 1,
                "last_verified_manifest": "second" if after_manifest else "first",
                "inventory_scan_end_reached": bool(after_manifest)}}
        native.append(("plan",))
        return result

    monkeypatch.setattr(prefix, "audit_candidate", audit)
    monkeypatch.setattr(prefix, "_free", lambda *_args, **_kwargs: None)
    args = SimpleNamespace(phase="audit", prefix_sha256="a" * 64, max_batches=2, work_root=tmp_path,
                           max_days=30, pages_per_batch=1, batch_work_bytes=1024)

    def batch():
        return prefix._run_batches(args, engine=None, source=None, target=None, clone=None, placement=None,
            plan=plan, retirement={}, journal=prefix.PrefixJournal(checkpoint, scope), store=None, limits=None,
            floor=1, remaining=lambda: 10)

    assert batch() == {"complete": False, "batches": 2, "next_day": "2026-10-13"}
    assert batch() == {"complete": False, "batches": 2, "next": "inverse_inventory"}
    assert batch() == {"complete": True, "batches": 2}
    assert native == [("plan",), ("2026-10-13", -1), ("2026-10-13", 0), ("2026-10-14", -1),
                      ("inverse", ""), ("inverse", "first")]
    assert len(list(journal.items("audit_plan", "audit_day", "audit_inverse"))) == 6
    with pytest.raises(RuntimeError, match="journal_scope_changed"):
        prefix.PrefixJournal(checkpoint, {"fixed": "changed"})


def test_seal_receipt_survives_lost_acknowledgement_and_exports_exact_bytes(tmp_path, monkeypatch):
    archive = tmp_path / "original"
    archive.mkdir()
    journal = prefix.PrefixJournal(MigrationCheckpoint(tmp_path / "migration.sqlite"), {"fixed": "scope"})
    day = "2026-10-14"
    plan = {"days": [{"day": day, "child": "fact_versions_20261014", "child_oid": 901}]}
    marker = {"state": "open", "sealed_at": None, "expected_rows": None, "payload_oid": 902, "source_bytes": 0}
    clone = SimpleNamespace(url=SimpleNamespace(database="qt_bench_test"),
                            proof={"database_identity": "42/43", "accepted_commit_seq": 99})
    args = SimpleNamespace(seal_day=date.fromisoformat(day), header_oid=901, payload_oid=902,
        source_archive_object_root=archive, count_statement_timeout_ms=1000, proof_sha256="a" * 64, work_root=tmp_path)
    monkeypatch.setattr(prefix, "_matches_inode", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(prefix, "os", SimpleNamespace(ST_RDONLY=1, statvfs=lambda _: SimpleNamespace(f_flag=1)))
    monkeypatch.setattr(prefix, "read_saved_history_policy", lambda _conn: (None, {"policy_revision": 7}))
    calls = []

    def seal(*_args, **_kwargs):
        calls.append("seal")
        marker.update(state="sealed", sealed_at="2026-11-15T08:00:00+00:00", expected_rows=4, source_bytes=128)
        raise OSError("lost acknowledgement")

    monkeypatch.setattr(prefix, "seal_one", seal)
    scope = {"clone": clone, "engine": SimpleNamespace(begin=nullcontext), "journal": journal,
             "plan": plan, "retirement": {day: marker}, "remaining": lambda: 10}
    with pytest.raises(OSError, match="lost acknowledgement"):
        prefix._seal(args, **scope)
    receipt = prefix._seal(args, **scope)
    assert calls == ["seal"]
    assert receipt == {"schema": "qt.private_rehearsal_cohort_seal.v1", "proof_sha256": "a" * 64,
        "database": "qt_bench_test", "database_identity": "42/43", "accepted_commit_seq": 99,
        "storage_day": day, "header_oid": 901, "payload_oid": 902, "previous_state": "open", "state": "sealed",
        "sealed_at": marker["sealed_at"], "expected_rows": 4, "source_payload_bytes": 128,
        "policy": {"policy_revision": 7}}

    monkeypatch.setattr(prefix, "os", os)
    path = tmp_path / "seals.json"
    export = prefix._export(path, journal.items("seal"), max_bytes=8192, remaining=lambda: 10)
    # A crash after publication can leave an extra hard link under the staged name.
    os.link(path, path.with_name(f".{path.name}.pending"))
    assert prefix._export(path, journal.items("seal"), max_bytes=8192, remaining=lambda: 10) == export
    assert path.stat().st_nlink == 1
    assert export["sha256"] == sha256(path.read_bytes()).hexdigest()
    assert json.loads(path.read_bytes()) == [receipt]

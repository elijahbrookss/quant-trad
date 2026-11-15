"""Use one held native transaction to reverse the whole fixture cutover."""

from contextlib import nullcontext
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from scripts.db.canonical_cold_activate import _EXECUTION_SCOPE
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_paused_donor import PausedRollbackDonor
from scripts.db.canonical_cold_private_activate import SCHEMA, _certificate_metadata
from scripts.db import canonical_cold_private_rollback as rehearsal
from scripts.db.canonical_cold_private_rollback import RollbackLimits, rollback_under_fence, run_private_rollback
from tests.test_market_data.test_canonical_cold_activate_db import _fence_connections
from tests.test_market_data.test_canonical_cold_finalize_rollback_db import (
    DEFINITION, _later_intake, _later_retirement, initially_dated_rollback_clone, legacy_rollback_clone, storage,  # noqa: F401
)


pytestmark = pytest.mark.db


@pytest.mark.parametrize("legacy_rollback_clone", ["verified-native-raw-index"], indirect=True)
def test_private_rollback_chain_preserves_post_switch_native_rows(legacy_rollback_clone, monkeypatch):
    _exercise_chain(legacy_rollback_clone, monkeypatch)


def test_private_rollback_chain_rebuilds_initial_and_recurring_children(initially_dated_rollback_clone, monkeypatch):
    _exercise_chain(initially_dated_rollback_clone, monkeypatch)


def _accept_new_data(scope, monkeypatch):
    store = scope.candidate_store
    later = _later_intake(scope, store.root.parent, monkeypatch)
    with monkeypatch.context() as temporary:
        day, manifest, _, _ = _later_retirement(scope, store.root.parent, temporary)
    series = {key: value if type(value) is int else value.isoformat() for key, value in scope.selection.items()}
    cases = [
        {"name": "old and new IDs", "mode": "exact", "max_rows": 4,
         "ids": [scope.expected_frozen[0].fact_version_id, scope.expected_latest[0].fact_version_id,
                 later.candles[0].fact_version_id, later.trade[0].fact_version_id]},
        {"name": "original frozen", "mode": "frozen_revisions", **series, "max_rows": 4, "known_at_lte": None,
         "dataset_id": scope.frozen.dataset_id, "dataset_hash": scope.frozen.dataset_hash},
        {"name": "original as-of", "mode": "as_of", **series, "max_rows": 4, "known_at_lte": None,
         "as_of_commit_seq": scope.frozen.max_commit_seq},
    ]
    return later, {day.isoformat(): manifest}, cases


_LIMITS = RollbackLimits(2 << 20, 2 << 20, 100, 2 << 20, 8, 2 << 20, 128, 64, 2 << 20, 16, 2 << 20,
                         2 << 20, 2 << 20)


def _exercise_chain(scope, monkeypatch):
    later, recurring, cases = _accept_new_data(scope, monkeypatch)
    latest = scope.storage.repo.current_commit_seq()
    with _fence_connections(scope.storage) as (conn, control), scope.donor.snapshot() as donor:
        fence = close_database_fence(conn, control, expected_database=scope.target_identity, timeout_seconds=90,
                                     record=lambda *_: None)
        arguments = {"fence": fence, "donor_identity": scope.donor_identity, "initial_commit": scope.source_commit,
                     "latest_commit": latest, "execution_hash": scope.certificate["execution_hash"], "audit": scope.audit,
                     "receipts": scope.forward_receipts, "seals": {}, "recurring": recurring,
                     "stream_ids": [DEFINITION], "cases": cases, "store": scope.candidate_store,
                     "donor_store": scope.donor_store, "limits": _LIMITS}
        with pytest.raises(RuntimeError, match="recurring_inventory_changed"), conn.begin():
            rollback_under_fence(conn, donor, **(arguments | {"recurring": {}}))
        with conn.begin():
            result = rollback_under_fence(conn, donor, **arguments)
            assert result["post_cutover_pages"]["row_count"] == 1
            assert result["streams"][0]["definition_id"] == DEFINITION
            layout = result["layout"]
            assert layout["source_commit"] == latest
            assert bool(layout["restored_dated"]) != bool(layout["restored_legacy"])
            assert layout["restored_recurring"].keys() == recurring.keys()
            if scope.certificate.get("raw_mapping_index"):
                assert layout["restored_raw_mapping_index"]["copy_bytes"] > 0
                assert layout["restored_custody_placement"]["copy_bytes"] > 0

        def outcome(check):
            observed = check.execute(text("SELECT layout_version,evidence FROM market.fact_storage_state")).one()
            assert observed[0] == FACT_STORAGE_LAYOUT_VERSION
            assert archive_evidence_hash(observed[1]) == layout["receipt_sha256"]
            return "original"

        reopen_database_fence(fence, control, expected_outcome="original", read_outcome=outcome, record=lambda *_: None)
    assert scope.storage.repo.current_commit_seq() == latest
    assert scope.storage.repo.read_facts(**later.selection) == later.candles
    assert scope.storage.repo.read_facts(**later.window) == later.trade


@pytest.mark.parametrize("storage", ["benchmark"], indirect=True)
def test_private_rollback_entry_reopens_old_layout_after_durable_commit(initially_dated_rollback_clone, monkeypatch):
    scope = initially_dated_rollback_clone
    later, recurring, cases = _accept_new_data(scope, monkeypatch)
    latest = scope.storage.repo.current_commit_seq()
    forward = scope.activation_outcome["execution"]
    execution = {key: forward[key] for key in _EXECUTION_SCOPE} | {
        "schema": SCHEMA, "source_commit_seq": scope.source_commit, "cold_before_day": scope.end_day.isoformat(),
        "clone_proof_sha256": "a" * 64, "placement_sha256": "b" * 64, "witness_sha256": "c" * 64,
        "custody_target_id": forward["raw_mapping_index"]["target_id"],
        "custody_tablespace_oid": forward["raw_mapping_index"]["tablespace_oid"],
    }
    execution_hash = archive_evidence_hash(execution)
    with scope.storage.database._engine.begin() as conn:
        # The forward fixture has already exercised the native activation.
        # Attach its rehearsal metadata in this disposable database.
        conn.execute(text("""
            UPDATE market.fact_storage_state SET evidence=(evidence-'evidence'-'approved_package_hash')||CAST(:private AS jsonb)
        """), {"private": json.dumps(_certificate_metadata(execution) | {"execution_hash": execution_hash})})
        epoch = conn.scalar(text("SELECT extract(epoch FROM pg_postmaster_start_time())::text"))
    identity = scope.target_identity
    clone = SimpleNamespace(url=scope.storage.database._engine.url, proof={
        "accepted_commit_seq": scope.source_commit, "postmaster_epoch": epoch,
        "database_identity": f"{identity['system_identifier']}/{identity['database_oid']}",
    })
    placement = {"archive_root": str(scope.candidate_store.root.parent), "clone_proof_sha256": "a" * 64}
    monkeypatch.setattr(rehearsal.socket, "if_nameindex", lambda: [(1, "lo")])
    monkeypatch.setattr(rehearsal, "assert_private_placement", lambda *a, **kw: None)

    def readonly_donor(self, conn, observed):
        assert conn is self.connection and observed == self.identity
        assert conn.scalar(text("SHOW transaction_read_only")) == "on"

    # Real paused-donor mount and retained socket checks have a separate
    # native hot standby fixture. The SQL donor here is read-only.
    monkeypatch.setattr(PausedRollbackDonor, "assert_snapshot", readonly_donor)
    phases = {}
    with _fence_connections(scope.storage) as (conn, control), scope.donor.snapshot() as source:
        donor = PausedRollbackDonor(source, scope.donor_identity, scope.source_commit, scope.donor_store.root, "a" * 64,
                                    source.get_transaction(), None, None, {}, {}, {}, float("inf"))
        result = run_private_rollback(
            conn, control, clone=clone, placement=placement, donor=donor, execution=execution,
            execution_hash=execution_hash, receipts=scope.audit_receipts, seals={}, store=scope.candidate_store,
            donor_store=scope.donor_store, limits=_LIMITS, timeout_seconds=90, record=lambda phase, data: phases.update({phase: data}),
            supervise_capacity=lambda *_: nullcontext({"fixture": True}),
            settings={"latest_commit": latest, "recurring": recurring, "stream_ids": [DEFINITION], "witness_cases": cases},
        )
    assert phases["private_rollback_precommit"] == phases["private_rollback_verified"]
    assert phases["database_reopened"]["outcome"] == "original"
    assert phases["private_rollback_verified"]["xid"] == result["transaction"]["xid"]
    assert scope.storage.repo.read_facts(**later.selection) == later.candles
    assert scope.storage.repo.read_facts(**later.window) == later.trade
    next_candle = replace(later.candle, open_time=later.candle.close_time,
                          close_time=later.candle.close_time + (later.candle.close_time - later.candle.open_time))
    write = scope.storage.repo.ingest_candles(series_id=later.selection["series_id"], source_id=later.product,
                                             facts=[next_candle], ingestion_run_id="private-rollback-first-restart")
    assert write.inserted_count == 1 and write.max_commit_seq == result["layout"]["next_commit"]

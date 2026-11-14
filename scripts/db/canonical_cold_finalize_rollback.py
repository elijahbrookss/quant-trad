"""Close physical rollback on a fenced disposable copy of the latest candidate."""

from __future__ import annotations

from datetime import UTC, date
import json
from time import monotonic

from sqlalchemy import text

from market_data.archive_namespace import archive_namespace
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_header_legacy_schema import assert_fact_header_legacy_contract
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract, fact_partition_name
from portal.backend.service.storage.recovery_copies import _snapshot_layout
from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository
from scripts.db.canonical_cold_restore_references import _database_identity, _require_disposable_fence, restore_original_reference_bindings
from scripts.db.canonical_cold_reverse_allocator import capture_allocator
from scripts.db.canonical_cold_reverse_projection import verify_candidate_only_pages
from scripts.db.canonical_cold_rollback_cohort import _require_snapshot
from scripts.db.canonical_cold_witness import check_witnesses
from scripts.db.fact_header_v2_capture import _bounded_step


VERSION = "qt.canonical_cold_finalize_rollback_fixture.v1"


def _receipt_digest(receipt):
    return archive_evidence_hash({key: value for key, value in receipt.items() if key != "receipt_sha256"})


def finalize_rollback_layout(conn, donor, *, fence, allocator, original, original_hash, legacy, payloads,
                             direct_cold, witness, witness_hash, store, dated=(), recurring=(), timeout_seconds=120):
    """Restore the original read authority after exact data and retention reconstruction.

    The clone writer and archive fences stay held through certificate
    publication and read acceptance. The caller owns restart and all
    host-level decisions.
    """
    _require_disposable_fence(conn, fence)
    if conn is donor or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 120:
        raise ValueError("canonical_cold_finalize_rollback_invalid_boundary")
    donor_identity = original["database"]
    _require_snapshot(donor, donor_identity)
    assert_fact_storage_contract(donor)
    donor_legacy_end = assert_fact_header_legacy_contract(donor)
    original_certificate = _snapshot_layout(donor)
    if original_certificate["layout_version"] != FACT_STORAGE_LAYOUT_VERSION or archive_evidence_hash(witness) != witness_hash:
        raise RuntimeError("canonical_cold_finalize_rollback_original_evidence_changed")

    with archive_namespace(store.root, exclusive=True) as check_archive, _bounded_step(conn, timeout_seconds) as limit:
        limit(max(0, fence.deadline - monotonic()))
        fence.assert_quiet(conn)
        identity = _database_identity(conn)
        cold = conn.execute(text("""
            SELECT layout_version,evidence FROM market.fact_storage_state WHERE state='ready' AND completed_at IS NOT NULL
        """)).one()
        if (identity != allocator.database or cold.layout_version != COLD_HISTORY_LAYOUT_VERSION
                or direct_cold.get("receipt_sha256") != _receipt_digest({key: value for key, value in direct_cold.items()
                                                                       if key != "inserted"})
                or direct_cold["target_database"] != identity or direct_cold["source_commit"] != allocator.visible_commit
                or direct_cold["execution_hash"] != cold.evidence["execution_hash"]):
            raise RuntimeError("canonical_cold_finalize_rollback_candidate_evidence_changed")

        retirements = conn.execute(text("""
            SELECT days.storage_day,days.handoff_kind,retention.state,days.row_count,days.manifest_set_hash,
                   to_jsonb(retention) AS marker
            FROM market.cold_fact_retired_days days
            LEFT JOIN market.fact_retention_partitions retention USING(storage_day)
            ORDER BY days.storage_day
        """)).all()
        end = assert_fact_header_legacy_contract(conn)
        if end != donor_legacy_end or (end is None) != (legacy is None):
            raise RuntimeError("canonical_cold_finalize_rollback_legacy_scope_changed")
        if legacy is not None and (
            legacy.get("receipt_sha256") != _receipt_digest(legacy) or legacy["target_database"] != identity
            or legacy["donor_identity"] != donor_identity or legacy["audit_set_hash"] != cold.evidence["audit_set_hash"]
            or end != date.fromisoformat(legacy["end_day"])
            or conn.scalar(text("SELECT to_regclass('market.fact_versions_legacy')::oid::bigint")) != legacy["legacy_oid"]
        ):
            raise RuntimeError("canonical_cold_finalize_rollback_legacy_receipt_changed")
        legacy_days = [row for row in retirements if row.handoff_kind == "initial" and end is not None and row.storage_day < end]
        expected_payloads = {row.storage_day.isoformat(): row for row in legacy_days if row.state is not None}
        observed_payloads = {receipt["storage_day"]: receipt for receipt in payloads}
        if (len(payloads) != len(observed_payloads) or observed_payloads.keys() != expected_payloads.keys()
                or (legacy is not None and legacy["rows"] != sum(row.row_count for row in legacy_days))):
            raise RuntimeError("canonical_cold_finalize_rollback_payload_scope_changed")
        for day, row in expected_payloads.items():
            receipt = observed_payloads[day]
            oid = conn.scalar(text("SELECT to_regclass(:name)::oid::bigint"),
                              {"name": "market." + fact_partition_name(row.storage_day)})
            if (receipt.get("receipt_sha256") != _receipt_digest(receipt) or receipt["target_database"] != identity
                    or receipt["legacy_header_receipt_sha256"] != legacy["receipt_sha256"]
                    or receipt["retention_sha256"] != archive_evidence_hash(row.marker)
                    or (receipt["retention_state"], receipt["row_count"], receipt["payload_oid"]) != (row.state, row.row_count, oid)
                    or ((oid is None) != (row.state == "reclaimed"))):
                raise RuntimeError(f"canonical_cold_finalize_rollback_payload_changed: {day}")

        expected_dated = {row.storage_day.isoformat(): row for row in retirements
                          if row.handoff_kind == "initial" and (end is None or row.storage_day >= end) and row.state is not None}
        observed_dated = {receipt["storage_day"]: receipt for receipt in dated}
        if len(dated) != len(observed_dated) or observed_dated.keys() != expected_dated.keys():
            raise RuntimeError("canonical_cold_finalize_rollback_dated_scope_changed")
        for day, row in expected_dated.items():
            receipt = observed_dated[day]
            observed = conn.execute(text("""
                SELECT to_regclass(:header)::oid::bigint,to_regclass(:payload)::oid::bigint,
                       (SELECT created_at FROM market.fact_header_partitions WHERE storage_day=:day)
            """), {"header": "market." + fact_header_partition_name(row.storage_day),
                    "payload": "market." + fact_partition_name(row.storage_day), "day": row.storage_day}).one()
            if (receipt.get("receipt_sha256") != _receipt_digest(receipt) or receipt["target_database"] != identity
                    or receipt["donor_identity"] != donor_identity or receipt["audit_set_hash"] != cold.evidence["audit_set_hash"]
                    or receipt["retention_sha256"] != archive_evidence_hash(row.marker)
                    or (receipt["retention_state"], receipt["row_count"]) != (row.state, row.row_count)
                    or observed[0] != receipt["header_oid"] or observed[1] != receipt["payload_oid"]
                    or observed[2] is None or observed[2].astimezone(UTC).isoformat() != receipt["registered_at"]
                    or ((observed[1] is None) != (row.state == "reclaimed"))):
                raise RuntimeError(f"canonical_cold_finalize_rollback_dated_changed: {day}")

        expected_recurring = {row.storage_day.isoformat(): row for row in retirements if row.handoff_kind == "recurring"}
        observed_recurring = {receipt["storage_day"]: receipt for receipt in recurring}
        if len(recurring) != len(observed_recurring) or observed_recurring.keys() != expected_recurring.keys():
            raise RuntimeError("canonical_cold_finalize_rollback_recurring_scope_changed")
        for day, row in expected_recurring.items():
            receipt = observed_recurring[day]
            header_oid, payload_oid = conn.execute(text("SELECT to_regclass(:header)::oid::bigint,to_regclass(:payload)::oid::bigint"), {
                "header": "market." + fact_header_partition_name(row.storage_day),
                "payload": "market." + fact_partition_name(row.storage_day),
            }).one()
            if (receipt.get("receipt_sha256") != _receipt_digest({key: value for key, value in receipt.items() if key != "already_restored"})
                    or receipt["target_database"] != identity or receipt["manifest_set_hash"] != row.manifest_set_hash
                    or receipt["row_count"] != row.row_count or row.state != "reclaimed"
                    or receipt["retention_sha256"] != archive_evidence_hash(row.marker)
                    or header_oid != receipt["header_oid"] or payload_oid is not None):
                raise RuntimeError(f"canonical_cold_finalize_rollback_recurring_changed: {day}")

        # Capturing the live allocator before and after the authority change
        # proves physical reversal did not consume or rewind the latest values.
        if capture_allocator(conn, fence=fence, layout=COLD_HISTORY_LAYOUT_VERSION) != allocator:
            raise RuntimeError("canonical_cold_finalize_rollback_candidate_allocator_changed")
        verify_candidate_only_pages(conn, reader=ColdHistoryReader(store), receipt=direct_cold,
                                    initial=cold.evidence["source_commit_seq"], check=lambda: (fence.check_clock(), check_archive()))
        references = restore_original_reference_bindings(conn, fence=fence, original=original, donor=donor,
                                                         expected_hash=original_hash, timeout_seconds=timeout_seconds)
        evidence = {
            "schema": VERSION, "target_database": identity, "donor_certificate": original_certificate,
            "previous_activation": cold.evidence, "restored_legacy": legacy, "restored_payloads": observed_payloads,
            "restored_dated": observed_dated, "restored_recurring": observed_recurring,
            "candidate_only": direct_cold["receipt_sha256"], "reference_restore": references,
            "legacy_relation_mapping": {"original_oid": legacy["original_oid"], "restored_oid": legacy["legacy_oid"]}
                if legacy is not None else None,
            "source_commit": allocator.visible_commit, "next_commit": allocator.next_value, "witness_hash": witness_hash,
        }
        written = conn.execute(text("""
            UPDATE market.fact_storage_state SET layout_version=:layout,completed_at=clock_timestamp(),
                evidence=CAST(:evidence AS jsonb) WHERE layout_version=:prior
        """), {"layout": FACT_STORAGE_LAYOUT_VERSION, "prior": COLD_HISTORY_LAYOUT_VERSION,
                "evidence": json.dumps(evidence)}).rowcount
        if written != 1:
            raise RuntimeError("canonical_cold_finalize_rollback_layout_changed")
        assert_fact_storage_contract(conn)
        if (check_witnesses(conn, witness, store=store, timeout_seconds=timeout_seconds) != witness_hash
                or PostgresMarketDataRepository._current_commit_seq_with_session(conn) != allocator.visible_commit):
            raise RuntimeError("canonical_cold_finalize_rollback_content_changed")
        restored = capture_allocator(conn, fence=fence, layout=FACT_STORAGE_LAYOUT_VERSION)
        if (restored.last_value, restored.is_called, restored.sequence_oid) != (
            allocator.last_value, allocator.is_called, allocator.sequence_oid,
        ):
            raise RuntimeError("canonical_cold_finalize_rollback_allocator_changed")
        check_archive()
        fence.assert_quiet(conn)
    return evidence | {"receipt_sha256": archive_evidence_hash(evidence)}

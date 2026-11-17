from copy import deepcopy

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.db.canonical_cold_audit import AUDIT_VERSION, GATES
from scripts.db.canonical_cold_receipts import manifest_inventory_hash, validate_audit_receipts


DAY = "2026-11-01"
EMPTY_DAY = "2026-11-02"
CLIENT = {"system_identifier": "7431", "database": "target", "database_oid": 101, "header_oid": 103}
SOURCE = CLIENT | {"database": "source", "database_oid": 102}
PREFIX = {"first_day": DAY, "cold_before_day": "2026-11-03", "legacy_end_day": None,
          "days": [{"day": DAY, "child": "fact_versions_20261101", "child_oid": 104},
                   {"day": EMPTY_DAY, "child": None, "child_oid": None}]}
A, B, C = ["cfa_" + letter * 64 for letter in "abc"]
DIGEST = "d" * 64
EMPTY = [0, ""]
CURSOR_1 = [5, "mfv_a"]
CURSOR_2 = [7, "mfv_b"]
CURSOR_3 = [9, "mfv_c"]


def _seal(receipt):
    receipt.pop("receipt_hash", None)
    receipt["receipt_hash"] = archive_evidence_hash(receipt)
    return receipt


def _receipt(**scope):
    return _seal({"schema": AUDIT_VERSION, "database": CLIENT.copy(), "source_database": SOURCE.copy(),
                  "prefix": deepcopy(PREFIX), "prefix_hash": archive_evidence_hash(PREFIX),
                  "checkpoint_binding_hash": DIGEST, "observed_at": "2026-11-12T09:00:00+00:00",
                  "remaining_acceptance": GATES.copy(), "activation_ready": False, **scope})


def _day(*, ordinal, previous, last, identity, end, count=1, low=None, reused=False):
    page = {"ordinal": ordinal, "after": previous.copy(), "first_cursor": (low or last).copy(),
            "last_cursor": last.copy(), "manifest_id": identity, "manifest_hash": DIGEST, "row_count": count,
            "reused": reused}
    witnesses = ([{"ordinal": ordinal, "kind": "prepared_generation", "authority_hash": DIGEST,
                   "current_verification_hash": None}] if reused else [])
    return {"storage_day": DAY, "expected_day_rows": 3, "expected_day_pages": 2,
            "expected_day_last_cursor": CURSOR_3.copy(), "start_after_ordinal": ordinal - 1,
            "start_after_cursor": previous.copy(), "last_verified_ordinal": ordinal, "last_cursor": last.copy(),
            "source_retention_state": "verified", "source_sealed_at": "2026-11-02T03:00:00+00:00",
            "day_scan_end_reached": end, "verified_pages": 1, "verified_rows": count,
            "manifest_hashes": [DIGEST], "page_receipts": [page], "source_archive_authority_receipts": witnesses}


def _empty_day():
    return {"storage_day": EMPTY_DAY, "expected_day_rows": 0, "expected_day_pages": 0,
            "expected_day_last_cursor": EMPTY.copy(), "start_after_ordinal": -1,
            "start_after_cursor": EMPTY.copy(), "last_verified_ordinal": -1, "last_cursor": EMPTY.copy(),
            "source_retention_state": None, "source_sealed_at": None,
            "day_scan_end_reached": True, "verified_pages": 0, "verified_rows": 0,
            "manifest_hashes": [], "page_receipts": [], "source_archive_authority_receipts": [],
            "source_day_empty": True}


def _inverse(identity, *, previous, rows, end):
    return {"start_after_manifest": previous, "last_verified_manifest": identity, "verified_pages": 1,
            "inventory_scan_end_reached": end, "manifest_ids": [identity],
            "page_receipts": [{"manifest_id": identity, "manifest_hash": DIGEST, "storage_day": DAY, "row_count": rows}]}


@pytest.fixture
def receipts():
    return [_receipt(),
            _receipt(day_batch=_day(ordinal=0, previous=EMPTY, last=CURSOR_1, identity=A, end=False, reused=True)),
            _receipt(day_batch=_day(ordinal=1, previous=CURSOR_1, last=CURSOR_3, identity=B, end=True, count=2, low=CURSOR_2)),
            _receipt(day_batch=_empty_day()),
            _receipt(inverse_inventory_batch=_inverse(A, previous="", rows=1, end=False)),
            _receipt(inverse_inventory_batch=_inverse(B, previous=A, rows=2, end=True))]


def test_complete_plan_is_exact_and_order_independent(receipts):
    actual = validate_audit_receipts(receipts, target_identity=CLIENT, source_identity=SOURCE,
                                     checkpoint_binding_hash=DIGEST, expected_prefix_hash=archive_evidence_hash(PREFIX))
    assert actual == validate_audit_receipts(reversed(receipts))
    assert (actual["verified_days"], actual["verified_pages"], actual["verified_rows"]) == (2, 2, 3)
    assert actual["retired_prefix_max_commit_seq"] == 9
    assert actual["days"][DAY]["source_retention_state"] == "verified"
    assert actual["days"][EMPTY_DAY]["last_cursor"] == EMPTY
    assert "activation_ready" not in actual
    assert actual["audit_set_hash"] == archive_evidence_hash({key: value for key, value in actual.items() if key != "audit_set_hash"})
    for digest_key, rows in (("inventory_hash", [(A, DAY, DIGEST, 1), (B, DAY, DIGEST, 2)]),
                             ("loaded_inventory_hash", [(A, DIGEST, 1), (B, DIGEST, 2)])):
        assert actual[digest_key] == manifest_inventory_hash(iter(rows)) == archive_evidence_hash({"manifests": rows})


@pytest.mark.parametrize("index,error", [(1, "day_cursor_gap"), (2, "bad_day_end"), (3, "forward_day_scope"),
                                         (4, "inverse_cursor_gap"), (5, "bad_inverse_end")])
def test_missing_forward_or_reverse_batch_fails(receipts, index, error):
    receipts.pop(index)
    with pytest.raises(ValueError, match=error):
        validate_audit_receipts(receipts)


@pytest.mark.parametrize("path,value,error", [
    (("day_batch", "page_receipts", 0, "after"), [4, "previous"], "page_cursor_gap"),
    (("day_batch", "page_receipts", 0, "manifest_id"), A, "duplicate_manifest"),
    (("day_batch", "page_receipts", 0, "row_count"), 3, "day_batch_summary"),
    (("day_batch", "source_sealed_at"), "2026-11-02T04:00:00+00:00", "day_retention_state_changed"),
    (("day_batch", "source_retention_state"), "reclaimed", "day_retention_state_changed"),
    (("source_database", "database_oid"), 900, "audit_scope_mismatch"),
])
def test_rehashed_forward_drift_is_rejected(receipts, path, value, error):
    item = receipts[2]
    found = item
    for field in path[:-1]:
        found = found[field]
    found[path[-1]] = value
    with pytest.raises(ValueError, match=error):
        validate_audit_receipts(receipts[:2] + [_seal(item)] + receipts[3:])


def test_rejects_duplicate_alias_coverage_and_bad_inverse_membership(receipts):
    with pytest.raises(ValueError, match="duplicate_receipt"):
        validate_audit_receipts(receipts + [receipts[0]])
    item = receipts[5]
    item["inverse_inventory_batch"]["page_receipts"][0]["manifest_id"] = C
    with pytest.raises(ValueError, match="inverse_inventory_manifest_mismatch"):
        validate_audit_receipts(receipts[:-1] + [_seal(item)])


def test_rejects_tampered_receipt_and_external_mismatch(receipts):
    with pytest.raises(ValueError, match="external_binding_mismatch"):
        validate_audit_receipts(receipts, checkpoint_binding_hash="b" * 64)
    receipts[0]["activation_ready"] = True
    with pytest.raises(ValueError, match="receipt_hash_mismatch"):
        validate_audit_receipts(receipts)

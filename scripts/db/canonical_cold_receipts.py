"""Validate all forward and inverse audit batches for one candidate inventory.

This consolidates read-only evidence. An operator must bind its digest to a
separate writer fence, independent restore and exact transition procedure.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
from hashlib import sha256

from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from scripts.db.canonical_cold_audit import AUDIT_VERSION, GATES
from scripts.db.canonical_cold_migration import INITIAL


RECEIPT_SET_VERSION = "qt.canonical_cold_audit_receipt_set.v1"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_MANIFEST = re.compile(r"cfa_[0-9a-f]{64}")
_IDENTITY_FIELDS = {"system_identifier", "database", "database_oid", "header_oid"}


def _require(condition, error, detail=""):
    if not condition:
        raise ValueError(f"canonical_cold_receipts_{error}" + (f": {detail}" if detail else ""))


def manifest_inventory_hash(rows):
    """Hash ordered manifest rows without buffering the mounted catalog.

    Each row follows ``(id, day, hash, count)`` or ``(id, hash, count)``.
    This hashes the same JSON encoding as archive_evidence_hash for this scope.
    """
    digest = sha256(b'{"manifests":[')
    first = True
    for row in rows:
        if not first:
            digest.update(b",")
        digest.update(json.dumps(list(row), separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii"))
        first = False
    digest.update(b"]}")
    return digest.hexdigest()


def _matches(pattern, value):
    return type(value) is str and pattern.fullmatch(value) is not None


def _integer(value, *, minimum=0):
    return type(value) is int and value >= minimum


def _cursor(value, *, allow_initial=True):
    _require(isinstance(value, (list, tuple)) and len(value) == 2, "bad_fact_cursor")
    observed = tuple(value)
    _require((allow_initial and observed == INITIAL and type(observed[0]) is int)
             or (_integer(observed[0], minimum=1) and isinstance(observed[1], str) and observed[1]),
             "bad_fact_cursor")
    return observed


def _identity(value):
    return (type(value) is dict and value.keys() == _IDENTITY_FIELDS
            and all(type(value[key]) is str and value[key] for key in ("system_identifier", "database"))
            and all(_integer(value[key], minimum=1) for key in ("database_oid", "header_oid")))


def _date(value):
    day = date.fromisoformat(value)
    _require(day.isoformat() == value, "noncanonical_day")
    return day


def _prefix_days(plan):
    first, cutoff = _date(plan["first_day"]), _date(plan["cold_before_day"])
    end = _date(plan["legacy_end_day"]) if plan["legacy_end_day"] is not None else None
    days = plan["days"]
    _require(type(days) is list and cutoff >= first and (end is None or end <= cutoff)
             and len(days) == (cutoff - first).days, "invalid_prefix_dates")
    expected_days = {}
    children = {}
    for offset, entry in enumerate(days):
        day = first + timedelta(days=offset)
        _require(entry["day"] == day.isoformat(), "noncontiguous_prefix", day.isoformat())
        child, oid = entry["child"], entry["child_oid"]
        expected = "fact_versions_legacy" if end is not None and day < end else fact_header_partition_name(day)
        if expected == "fact_versions_legacy" or child is not None:
            _require(child == expected and _integer(oid, minimum=1), "bad_prefix_child", day.isoformat())
            _require(children.setdefault(child, oid) == oid, "changed_prefix_child", child)
        else:
            _require(oid is None, "bad_prefix_child", day.isoformat())
        expected_days[entry["day"]] = entry
    _require(len(set(children.values())) == len(children), "duplicate_prefix_child_oid")
    return expected_days


def _validate_common(receipt):
    _require(type(receipt) is dict and receipt.get("schema") == AUDIT_VERSION, "unsupported_receipt_schema")
    body = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    _require(archive_evidence_hash(body) == receipt.get("receipt_hash"), "receipt_hash_mismatch")
    _require(archive_evidence_hash(receipt["prefix"]) == receipt["prefix_hash"], "prefix_hash_mismatch")
    _require(_identity(receipt["database"]) and _identity(receipt["source_database"]), "bad_database_identity")
    _require(_matches(_DIGEST, receipt["checkpoint_binding_hash"]), "bad_checkpoint_hash")
    _require(datetime.fromisoformat(receipt["observed_at"]).utcoffset() is not None, "bad_observation_time")
    _require(receipt["activation_ready"] is False and receipt["remaining_acceptance"] == GATES,
             "bad_receipt_authority")
    return {key: receipt[key] for key in ("database", "source_database", "prefix", "prefix_hash", "checkpoint_binding_hash")}


def _validate_authority(batch, reused):
    observed = []
    for authority in batch["source_archive_authority_receipts"]:
        _require(_integer(authority["ordinal"]) and authority["kind"] in {"legacy_catalog", "prepared_generation"}
                 and _matches(_DIGEST, authority["authority_hash"]), "bad_source_archive_authority")
        observed.append(authority["ordinal"])
        verification = authority["current_verification_hash"]
        _require(verification is None or _matches(_DIGEST, verification), "bad_archive_verification_hash")
    _require(observed == reused, "missing_or_extra_archive_authority")


def _forward_day(day, batches, planned_child, manifests):
    _require(all(_integer(batch["start_after_ordinal"], minimum=-1) for batch in batches),
             "bad_day_resume_ordinal", day)
    batches.sort(key=lambda batch: batch["start_after_ordinal"])
    first = batches[0]
    expected_rows, expected_pages = first["expected_day_rows"], first["expected_day_pages"]
    expected_tail = _cursor(first["expected_day_last_cursor"])
    state, sealed_at = first["source_retention_state"], first["source_sealed_at"]
    _require(state in {None, "open", "sealed", "verified", "reclaimed"}
             and ((sealed_at is None) == (state in {None, "open"}))
             and (sealed_at is None or datetime.fromisoformat(sealed_at).utcoffset() is not None)
             and (expected_rows == 0 or state in {"sealed", "verified", "reclaimed"}),
             "bad_source_retention_state", day)
    _require(_integer(expected_rows) and _integer(expected_pages) and expected_pages <= expected_rows
             and ((expected_pages == 0) == (expected_tail == INITIAL))
             and (expected_pages == 0 or planned_child is not None), "bad_day_expected_counts", day)
    ordinal, accumulated, previous = -1, 0, INITIAL
    for index, batch in enumerate(batches):
        _require((batch["expected_day_rows"], batch["expected_day_pages"], _cursor(batch["expected_day_last_cursor"]))
                 == (expected_rows, expected_pages, expected_tail), "day_scope_changed", day)
        _require((batch["source_retention_state"], batch["source_sealed_at"]) == (state, sealed_at),
                 "day_retention_state_changed", day)
        _require(batch["start_after_ordinal"] == ordinal and _cursor(batch["start_after_cursor"]) == previous,
                 "day_cursor_gap_or_overlap", day)
        if batch.get("source_day_empty", False):
            _require(expected_rows == 0 and len(batches) == 1, "bad_empty_day_proof", day)
        pages = batch["page_receipts"]
        _require(type(pages) is list and (pages or expected_pages == 0), "empty_nonterminal_batch", day)
        reused, hashes, rows = [], [], 0
        for page in pages:
            ordinal += 1
            _require(_integer(page["ordinal"]) and page["ordinal"] == ordinal
                     and _cursor(page["after"]) == previous, "page_cursor_gap_or_overlap", day)
            first_cursor = _cursor(page["first_cursor"], allow_initial=False)
            last_cursor = _cursor(page["last_cursor"], allow_initial=False)
            count = page["row_count"]
            _require(previous < first_cursor <= last_cursor and _integer(count, minimum=1)
                     and ((first_cursor == last_cursor) == (count == 1)), "bad_page_cursor_or_count", day)
            identity, digest = page["manifest_id"], page["manifest_hash"]
            _require(_matches(_MANIFEST, identity) and _matches(_DIGEST, digest) and identity not in manifests,
                     "bad_or_duplicate_manifest", day)
            manifests[identity] = (day, digest, count)
            _require(type(page["reused"]) is bool, "bad_page_reuse_marker", day)
            if page["reused"]:
                reused.append(ordinal)
            hashes.append(digest)
            rows += count
            previous = last_cursor
        _validate_authority(batch, reused)
        _require(batch["day_scan_end_reached"] is (index + 1 == len(batches)), "bad_day_end_marker", day)
        _require(batch["verified_pages"] == len(pages) and batch["verified_rows"] == rows
                 and batch["manifest_hashes"] == hashes and batch["last_verified_ordinal"] == ordinal
                 and _cursor(batch["last_cursor"]) == previous, "day_batch_summary_mismatch", day)
        accumulated += rows
    _require((ordinal + 1, accumulated, previous) == (expected_pages, expected_rows, expected_tail),
             "day_total_or_tail_mismatch", day)
    return {"expected_rows": expected_rows, "expected_pages": expected_pages,
            "verified_rows": accumulated, "verified_pages": ordinal + 1,
            "source_retention_state": state, "source_sealed_at": sealed_at,
            "last_cursor": list(previous)}


def _inverse_inventory(batches, manifests):
    _require(batches, "inverse_inventory_missing")
    _require(all(type(batch["start_after_manifest"]) is str for batch in batches), "bad_inverse_resume_manifest")
    batches.sort(key=lambda batch: batch["start_after_manifest"])
    previous, checked = "", 0
    for index, batch in enumerate(batches):
        _require(batch["start_after_manifest"] == previous, "inverse_cursor_gap_or_overlap")
        pages = batch["page_receipts"]
        _require(type(pages) is list and (pages or (not manifests and len(batches) == 1)), "empty_inverse_batch")
        page_ids = []
        for page in pages:
            identity = page["manifest_id"]
            _require(_matches(_MANIFEST, identity) and identity > previous and
                     manifests.get(identity) == (page["storage_day"], page["manifest_hash"], page["row_count"]),
                     "inverse_inventory_manifest_mismatch", str(identity))
            page_ids.append(identity)
            previous = identity
        _require(batch["verified_pages"] == len(pages) and batch["manifest_ids"] == page_ids
                 and batch["last_verified_manifest"] == previous, "inverse_batch_summary_mismatch")
        _require(batch["inventory_scan_end_reached"] is (index + 1 == len(batches)), "bad_inverse_end_marker")
        checked += len(pages)
    _require(checked == len(manifests), "incomplete_inverse_inventory")


def validate_audit_receipts(receipts, *, target_identity=None, source_identity=None,
                            checkpoint_binding_hash=None, expected_prefix_hash=None):
    """Validate exact scope and traversal, returning an order-independent set digest."""
    common = None
    hashes = set()
    forward = defaultdict(list)
    inverse = []
    plan_seen = False
    try:
        for receipt in receipts:
            current = _validate_common(receipt)
            if common is None:
                common = current
            _require(common == current, "audit_scope_mismatch")
            digest = receipt["receipt_hash"]
            _require(digest not in hashes, "duplicate_receipt", digest)
            hashes.add(digest)
            if "day_batch" in receipt and "inverse_inventory_batch" in receipt:
                raise ValueError("canonical_cold_receipts_multiple_scopes_in_one_receipt")
            if "day_batch" in receipt:
                batch = receipt["day_batch"]
                forward[batch["storage_day"]].append(batch)
            elif "inverse_inventory_batch" in receipt:
                inverse.append(receipt["inverse_inventory_batch"])
            else:
                _require(not plan_seen, "duplicate_plan_receipt")
                plan_seen = True
        _require(common is not None, "no_audit_receipts")
        for field, expected in (("database", target_identity), ("source_database", source_identity),
                                ("checkpoint_binding_hash", checkpoint_binding_hash), ("prefix_hash", expected_prefix_hash)):
            _require(expected is None or common[field] == expected, "external_binding_mismatch", field)
        planned_days = _prefix_days(common["prefix"])
        _require(forward.keys() == planned_days.keys(), "forward_day_scope_mismatch",
                 f"missing={sorted(planned_days.keys() - forward.keys())} extra={sorted(forward.keys() - planned_days.keys())}")
        manifests = {}
        days = {day: _forward_day(day, forward[day], entry["child"], manifests) for day, entry in planned_days.items()}
        _inverse_inventory(inverse, manifests)
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("canonical_cold_receipts_invalid_structure") from exc
    inventory = sorted(manifests.items())
    authorities = {}
    for day, batches in forward.items():
        for batch in batches:
            by_ordinal = {item["ordinal"]: item for item in batch["source_archive_authority_receipts"]}
            for page in batch["page_receipts"]:
                if page["reused"]:
                    authorities[page["manifest_id"]] = (day, by_ordinal[page["ordinal"]]["authority_hash"])
    summary = {"schema": RECEIPT_SET_VERSION, **common, "verified_days": len(days),
               "verified_pages": len(manifests), "verified_rows": sum(item["verified_rows"] for item in days.values()),
               "retired_prefix_max_commit_seq": max((item["last_cursor"][0] for item in days.values()), default=0),
               "days": days,
               "inventory_hash": manifest_inventory_hash(
                   (identity, day, digest, count) for identity, (day, digest, count) in inventory),
               "loaded_inventory_hash": manifest_inventory_hash(
                   (identity, digest, count) for identity, (_, digest, count) in inventory),
               "source_authority_inventory_hash": manifest_inventory_hash(
                   (identity, day, digest) for identity, (day, digest) in sorted(authorities.items())),
               "receipt_hashes": sorted(hashes), "remaining_acceptance": list(GATES)}
    return summary | {"audit_set_hash": archive_evidence_hash(summary)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipts", type=Path, help="JSON array containing the bounded audit receipts")
    args = parser.parse_args()
    with args.receipts.open(encoding="utf-8") as source:
        receipts = json.load(source)
    _require(type(receipts) is list, "expected_receipt_array")
    print(json.dumps(validate_audit_receipts(receipts), sort_keys=True))


if __name__ == "__main__":
    main()

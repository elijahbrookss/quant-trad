"""Read-only audit of an inactive cold catalog and its retiring source prefix.

For each selected day compare the live source, mounted all-revision pages and
loaded exact locators. Use the inverse inventory pass to detect extra cold
pages. These bounded receipts still need a final owner fence and the separate
recovery, lineage, frozen dataset, and runtime acceptance checks.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, timedelta
from hashlib import sha256
from itertools import islice
import json
from math import isfinite
import os
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine, text

from core.storage_mounts import configured_archive_root
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdHistoryReader
from market_data.fact_archive import FactArchiveLimits, FactArchiveManifest, archive_evidence_hash
from portal.backend.db.cold_history_models import assert_cold_history_immutability, assert_cold_history_placement
from portal.backend.db.fact_identity_schema import _partition_bound, fact_header_partition_name
from portal.backend.service.storage.repos.cold_history import _manifest, cold_layout_active
from portal.backend.service.storage.repos.fact_storage import ensure_payload_contracts
from scripts.db.canonical_cold_load import TargetDatabase
from scripts.db.canonical_cold_migration import INITIAL, MigrationCheckpoint, PostgresMigrationSource


AUDIT_VERSION = "qt.canonical_cold_candidate_audit.v1"
GATES = ["all_batches_reconciled_under_writer_and_catalog_fence", "independent_paired_restore",
         "raw_and_l2_coverage_holds_and_replay", "frozen_and_as_of_runtime_parity",
         "retirement_dependencies_and_physical_space"]


def _ensure_target(conn, source, target):
    identity = source.identity(conn)
    if (identity["system_identifier"], identity["database"], identity["database_oid"]) != (
        target.system_identifier, target.name, target.oid,
    ):
        raise RuntimeError("canonical_cold_audit_wrong_database")
    if cold_layout_active(conn):
        raise RuntimeError("canonical_cold_audit_layout_already_active")
    assert_cold_history_placement(conn)
    assert_cold_history_immutability(conn)
    unique_keys = conn.execute(text("""SELECT pg_get_constraintdef(constraint_row.oid)
        FROM pg_constraint constraint_row JOIN pg_index index_row ON index_row.indexrelid=constraint_row.conindid
        WHERE constraint_row.conrelid='market.fact_identities'::regclass
          AND constraint_row.contype IN ('p','u') AND constraint_row.convalidated
          AND NOT constraint_row.condeferrable AND index_row.indisvalid AND index_row.indisready""")).scalars()
    if not {"PRIMARY KEY (id)", "UNIQUE (series_id, observation_key, revision)"} <= set(unique_keys):
        raise RuntimeError("canonical_cold_audit_source_global_identity_guards_missing")
    return identity


def _plan(conn, *, cutoff, max_days):
    legacy_end = conn.execute(text("SELECT market.fact_header_legacy_end_day()")).scalar_one()
    if legacy_end is not None and cutoff < legacy_end:
        raise RuntimeError(f"canonical_cold_audit_prefix_splits_legacy_relation: end={legacy_end}")
    registered = conn.execute(text("""SELECT storage_day FROM market.fact_header_partitions
        ORDER BY storage_day LIMIT :limit"""), {"limit": max_days + 1}).scalars().all()
    if len(registered) > max_days or (legacy_end is not None and any(day < legacy_end for day in registered)):
        raise RuntimeError("canonical_cold_audit_dated_registry_invalid_or_exceeds_budget")
    children = conn.execute(text("""SELECT c.oid::bigint AS oid, n.nspname AS schema, c.relname AS name,
                c.relkind AS kind, c.relpersistence AS persistence, pg_get_expr(c.relpartbound,0) AS bound
        FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE i.inhparent='market.fact_versions'::regclass LIMIT :limit"""),
        {"limit": max_days + 2}).mappings().all()
    expected = {fact_header_partition_name(day): _partition_bound(day) for day in registered}
    if legacy_end is not None:
        expected["fact_versions_legacy"] = f"FOR VALUES FROM (MINVALUE) TO ('{legacy_end.isoformat()}')"
    names = {child["name"] for child in children}
    if len(children) != len(names) or names != set(expected):
        raise RuntimeError("canonical_cold_audit_registered_and_attached_children_disagree")
    oid_by_name = {}
    for child in children:
        name = child["name"]
        if (child["schema"], child["kind"], child["persistence"], child["bound"]) != (
            "market", "r", "p", expected[name],
        ):
            raise RuntimeError(f"canonical_cold_audit_partition_binding_mismatch: child={name}")
        oid_by_name[name] = child["oid"]
    first = conn.execute(text("SELECT storage_day FROM market.fact_versions "
                              "WHERE storage_day < :cutoff ORDER BY storage_day LIMIT 1"),
                         {"cutoff": cutoff}).scalar_one_or_none()
    old_registered = [day for day in registered if day < cutoff]
    start = min([day for day in (first, *old_registered) if day is not None], default=cutoff)
    width = (cutoff - start).days
    if width > max_days:
        raise RuntimeError(f"canonical_cold_audit_day_span_budget_exceeded: days={width}")
    days = []
    for offset in range(width):
        day = start + timedelta(days=offset)
        child = "fact_versions_legacy" if legacy_end is not None and day < legacy_end else fact_header_partition_name(day)
        days.append({"day": day.isoformat(), "child": child if child in oid_by_name else None,
                     "child_oid": oid_by_name.get(child)})
    return {"cold_before_day": cutoff.isoformat(), "legacy_end_day": legacy_end.isoformat() if legacy_end else None,
            "first_day": start.isoformat(), "days": days}


def _source_rows(conn, *, day, after, through, limit):
    where = "v.storage_day=:day"
    params = {"day": day, "after_seq": after[0], "after_id": after[1], "limit": limit}
    if after != INITIAL:
        where += " AND (v.market_commit_seq,v.id)>(:after_seq,:after_id)"
    if through is not None:
        where += " AND (v.market_commit_seq,v.id)<=(:last_seq,:last_id)"
        params.update(last_seq=through[0], last_id=through[1])
    return conn.execute(text(f"""SELECT v.tableoid::bigint AS child_oid, v.id, v.series_id,
            v.observation_key,v.revision,v.market_commit_seq,v.row_hash,
            i.id AS identity_id, i.series_id AS identity_series_id,i.observation_key AS identity_key,
            i.revision AS identity_revision,i.storage_day AS identity_day
        FROM (SELECT tableoid,* FROM market.fact_versions v WHERE {where}
              ORDER BY v.market_commit_seq,v.id LIMIT :limit) v
        LEFT JOIN market.fact_identities i ON i.id=v.id
        ORDER BY v.market_commit_seq,v.id"""), params).mappings().all()


def _fact_key(row):
    return tuple(row[name] for name in ("id", "series_id", "observation_key", "revision", "market_commit_seq", "row_hash"))


def _locators(conn, manifest):
    return conn.execute(text("""SELECT id,page_id,ordinal,series_id,key_digest,revision,market_commit_seq,row_hash
        FROM market.cold_fact_locators WHERE page_id=:id ORDER BY ordinal LIMIT :limit"""),
        {"id": manifest.manifest_id, "limit": manifest.row_count + 1}).mappings().all()


def _assert_page_series(conn, manifest, rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["series_id"]].append(row)
    actual = conn.execute(text("""SELECT series_id,first_observation_at,last_observation_at,last_commit_seq,
            last_active_accepted_at,last_active_observation_at FROM market.cold_fact_page_series
        WHERE page_id=:id ORDER BY series_id LIMIT :limit"""),
        {"id": manifest.manifest_id, "limit": len(grouped) + 1}).all()
    expected = []
    for bounds in manifest.series:
        group = grouped[bounds.series_id]
        active = [row for row in group if row["state"] == "active"]
        expected.append((bounds.series_id, bounds.first_observation_at, bounds.last_observation_at,
                         max(row["market_commit_seq"] for row in group),
                         max((row["accepted_at"] for row in active), default=None),
                         max((row["observation_time"] for row in active), default=None)))
    if actual != expected:
        raise RuntimeError(f"canonical_cold_audit_series_metadata_mismatch: page={manifest.manifest_id}")


def _assert_retained_order(conn, *, rows, cutoff):
    # One index seek per represented series, comparing the last retiring and
    # first retained commit. A tied commit is an invalid split for this cutover.
    series_max = {}
    key_max = {}
    for row in rows:
        series, key, revision, commit = (row[name] for name in
            ("series_id", "observation_key", "revision", "market_commit_seq"))
        series_max[series] = max(series_max.get(series, 0), commit)
        key_max[(series, key)] = max(key_max.get((series, key), 0), revision)
    witness = conn.execute(text("""SELECT cold.series_id,cold.last_commit,hot.first_commit
        FROM jsonb_to_recordset(CAST(:series AS jsonb)) AS cold(series_id bigint,last_commit bigint)
        CROSS JOIN LATERAL (
            SELECT market_commit_seq AS first_commit FROM market.fact_versions
            WHERE series_id=cold.series_id AND storage_day>=:cutoff ORDER BY market_commit_seq LIMIT 1
        ) hot WHERE hot.first_commit<=cold.last_commit LIMIT 1"""), {
            "series": json.dumps([{"series_id": series, "last_commit": seq} for series, seq in series_max.items()]),
            "cutoff": cutoff,
        }).one_or_none()
    if witness is not None:
        raise RuntimeError(f"canonical_cold_audit_retained_series_commit_overlap: witness={tuple(witness)}")
    # Global identity has the exact full observation key. The restriction
    # targets an incompatible retained revision without relying on its digest.
    witness = conn.execute(text("""SELECT cold.series_id,hot.id,cold.last_revision,hot.revision
        FROM jsonb_to_recordset(CAST(:keys AS jsonb)) AS cold(series_id bigint,observation_key text,last_revision integer)
        CROSS JOIN LATERAL (
            SELECT id,revision FROM market.fact_identities
            WHERE series_id=cold.series_id AND observation_key=cold.observation_key
              AND revision<=cold.last_revision AND storage_day>=:cutoff LIMIT 1
        ) hot LIMIT 1"""), {"keys": json.dumps([
            {"series_id": series, "observation_key": key, "last_revision": revision}
            for (series, key), revision in key_max.items()
        ]), "cutoff": cutoff}).one_or_none()
    if witness is not None:
        raise RuntimeError(f"canonical_cold_audit_retained_hot_revision_overlap: witness={tuple(witness)}")


def _audit_page(conn, reader, *, page, child_oid, cutoff):
    manifest = page.manifest
    # New archive workers can acknowledge an equivalent or overlapping page
    # after staging. Reused pages remain bound to their current generation;
    # new pages without a captured authority must be reconciled explicitly.
    published = conn.execute(text("""SELECT * FROM market.fact_archive_manifests
        WHERE storage_day=:day AND (first_commit_seq,first_id)<=(:last_seq,:last_id)
          AND (last_commit_seq,last_id)>=(:first_seq,:first_id) ORDER BY page_ordinal LIMIT 2"""),
        {"day": page.source_day, "first_seq": manifest.first_cursor[0], "first_id": manifest.first_cursor[1],
         "last_seq": manifest.last_cursor[0], "last_id": manifest.last_cursor[1]}).mappings().all()
    if page.reused:
        if (len(published) != 1 or published[0]["id"] != manifest.manifest_id or page.source_authority is None
                or PostgresMigrationSource.source_authority(conn, published[0]) != page.source_authority):
            raise RuntimeError(f"canonical_cold_audit_source_archive_authority_changed: page={manifest.manifest_id}")
    elif published:
        raise RuntimeError(f"canonical_cold_audit_new_overlapping_source_archive_page: page={manifest.manifest_id}")
    loaded = conn.execute(text("SELECT * FROM market.cold_fact_pages WHERE id=:id"),
                          {"id": manifest.manifest_id}).mappings().one_or_none()
    if loaded is None or loaded["staged_storage_day"] is not None or _manifest(loaded) != manifest:
        raise RuntimeError(f"canonical_cold_audit_loaded_manifest_missing_or_changed: page={manifest.manifest_id}")
    rows = reader.read_page(manifest)
    source = _source_rows(conn, day=page.source_day, after=page.after, through=manifest.last_cursor,
                          limit=manifest.row_count + 1)
    locators = _locators(conn, manifest)
    if len(source) != manifest.row_count or len(locators) != manifest.row_count:
        raise RuntimeError(f"canonical_cold_audit_page_coverage_mismatch: page={manifest.manifest_id} "
                           f"expected={manifest.row_count} source={len(source)} loaded={len(locators)}")
    seen = set()
    for ordinal, (row, header, locator) in enumerate(zip(rows, source, locators, strict=True)):
        identity, series, key, revision, commit, row_hash = _fact_key(row)
        if header["id"] in seen:
            raise RuntimeError(f"canonical_cold_audit_duplicate_source_identity: id={header['id']}")
        seen.add(header["id"])
        if (_fact_key(header) != (identity, series, key, revision, commit, row_hash)
                or header["child_oid"] != child_oid):
            raise RuntimeError(f"canonical_cold_audit_source_identity_mismatch: id={identity} page={manifest.manifest_id}")
        if tuple(header[field] for field in ("identity_id", "identity_series_id", "identity_key",
                                             "identity_revision", "identity_day")) != (
            identity, series, key, revision, page.source_day,
        ):
            raise RuntimeError(f"canonical_cold_audit_source_registry_mismatch: id={identity}")
        expected_locator = (identity, manifest.manifest_id, ordinal, series, revision, commit,
                            sha256(key.encode("utf-8")).digest(), bytes.fromhex(row_hash))
        observed_locator = (locator["id"], locator["page_id"], locator["ordinal"], locator["series_id"],
                            locator["revision"], locator["market_commit_seq"], bytes(locator["key_digest"]),
                            bytes(locator["row_hash"]))
        if observed_locator != expected_locator:
            raise RuntimeError(f"canonical_cold_audit_loaded_locator_mismatch: id={identity} page={manifest.manifest_id}")
    _assert_page_series(conn, manifest, rows)
    _assert_retained_order(conn, rows=rows, cutoff=cutoff)


def _audit_day(conn, checkpoint, binding, store, *, source, day, cutoff, child_oid, after_ordinal, max_pages, deadline):
    with checkpoint.read_connection() as db:
        progress = db.execute("""SELECT completed,rows,next_ordinal,last_seq,last_id FROM cohorts WHERE day=?""",
                              (day.isoformat(),)).fetchone()
    if progress is None:
        if after_ordinal != -1:
            raise ValueError(f"canonical_cold_audit_resume_ordinal_invalid: day={day}")
        if _source_rows(conn, day=day, after=INITIAL, through=None, limit=1):
            raise RuntimeError(f"canonical_cold_audit_checkpoint_missing_for_nonempty_day: day={day}")
        return {"storage_day": day.isoformat(), "verified_pages": 0, "verified_rows": 0,
                "source_day_empty": True, "day_scan_end_reached": True}
    complete, total_rows, total_pages, final_seq, final_id = progress
    if not complete:
        raise RuntimeError(f"canonical_cold_audit_unfinished_checkpoint_day: day={day}")
    cohort = source.cohort(conn, day)
    checkpoint.check_cohort(cohort)
    if total_rows != cohort.expected_rows:
        raise RuntimeError(f"canonical_cold_audit_sealed_row_count_changed: day={day}")
    if not -1 <= after_ordinal < total_pages:
        raise ValueError(f"canonical_cold_audit_resume_ordinal_invalid: day={day}")
    after = INITIAL
    if after_ordinal >= 0:
        prior_pages = checkpoint.iter_completed_pages(day=day, after_ordinal=after_ordinal - 1)
        try:
            prior = next(prior_pages, None)
        finally:
            prior_pages.close()
        if prior is None or prior.ordinal != after_ordinal:
            raise RuntimeError(f"canonical_cold_audit_resume_page_missing: day={day} ordinal={after_ordinal}")
        after = prior.manifest.last_cursor
    reader = ColdHistoryReader(store, limits=FactArchiveLimits(**binding["limits"]),
                               ensure_contracts=lambda pairs: ensure_payload_contracts(conn, pairs))
    current, count, page_hashes, authority_receipts = after_ordinal, 0, [], []
    pages = checkpoint.iter_completed_pages(day=day, after_ordinal=after_ordinal)
    try:
        for page in islice(pages, max_pages):
            if monotonic() >= deadline:
                break
            if page.ordinal != current + 1 or page.after != after or page.manifest.first_cursor <= after:
                raise RuntimeError(f"canonical_cold_audit_checkpoint_gap_or_overlap: day={day} ordinal={page.ordinal}")
            _audit_page(conn, reader, page=page, child_oid=child_oid, cutoff=cutoff)
            count += page.manifest.row_count
            current = page.ordinal
            after = page.manifest.last_cursor
            page_hashes.append(page.manifest.manifest_hash)
            if page.source_authority is not None:
                authority_receipts.append({"ordinal": page.ordinal, "kind": page.source_authority["kind"],
                    "current_verification_hash": page.source_authority["verification_hash"],
                    "authority_hash": archive_evidence_hash(page.source_authority)})
    finally:
        pages.close()
    end_reached = current + 1 == total_pages
    if end_reached and (after != (final_seq, final_id) or _source_rows(
            conn, day=day, after=after, through=None, limit=1)):
        raise RuntimeError(f"canonical_cold_audit_source_tail_changed: day={day}")
    if end_reached and after_ordinal == -1 and count != total_rows:
        raise RuntimeError(f"canonical_cold_audit_checkpoint_row_count_changed: day={day}")
    if not end_reached and len(page_hashes) < max_pages and monotonic() < deadline:
        raise RuntimeError(f"canonical_cold_audit_checkpoint_page_missing: day={day} after_ordinal={current}")
    return {"storage_day": day.isoformat(), "verified_rows": count, "verified_pages": len(page_hashes),
            "start_after_ordinal": after_ordinal, "last_verified_ordinal": current, "last_cursor": list(after),
            "expected_day_rows": total_rows, "expected_day_pages": total_pages,
            "day_scan_end_reached": end_reached, "manifest_hashes": page_hashes,
            "source_archive_authority_receipts": authority_receipts}


def _audit_inventory(conn, checkpoint, *, allowed_days, after_manifest, max_pages, deadline):
    loaded = conn.execute(text("SELECT * FROM market.cold_fact_pages WHERE id>:after ORDER BY id LIMIT :limit"),
                          {"after": after_manifest, "limit": max_pages + 1}).mappings().all()
    pages = []
    with checkpoint.read_connection() as db:
        for row in loaded[:max_pages]:
            if monotonic() >= deadline:
                break
            manifest = _manifest(row)
            record = db.execute("""SELECT pages.day,pages.manifest_hash,pages.descriptor FROM pages JOIN cohorts USING(day)
                WHERE pages.manifest_id=? AND cohorts.completed=1""", (manifest.manifest_id,)).fetchone()
            if (
                row["staged_storage_day"] is not None or record is None
                or record[0] not in allowed_days
                or FactArchiveManifest.from_dict(json.loads(record[2]), expected_hash=record[1]) != manifest
            ):
                raise RuntimeError(f"canonical_cold_audit_unplanned_loaded_manifest: page={manifest.manifest_id}")
            pages.append(manifest.manifest_id)
    return {"start_after_manifest": after_manifest, "last_verified_manifest": pages[-1] if pages else after_manifest,
            "verified_pages": len(pages), "inventory_scan_end_reached": len(pages) == len(loaded),
            "manifest_ids": pages}


def audit_candidate(engine, checkpoint, *, target, object_store, cutoff, day=None, inventory=False,
                    after_ordinal=-1, after_manifest="", max_pages=1, max_days=512, max_seconds=120,
                    statement_timeout_ms=30_000):
    if (type(max_pages) is not int or max_pages < 1 or type(max_days) is not int or max_days < 1
            or not isfinite(max_seconds) or max_seconds <= 0 or (day is not None and inventory)
            or type(after_ordinal) is not int or after_ordinal < -1 or (day is None and after_ordinal != -1)
            or (not inventory and after_manifest)):
        raise ValueError("canonical_cold_audit_invalid_mode_or_budget")
    deadline = monotonic() + max_seconds
    binding = checkpoint.binding()
    if Path(binding["staging_root"]).resolve(strict=True) != object_store.root:
        raise RuntimeError("canonical_cold_audit_object_root_does_not_match_checkpoint")
    source = PostgresMigrationSource(engine, expected_system_identifier=target.system_identifier,
                                     statement_timeout_ms=statement_timeout_ms)
    with source.snapshot() as conn:
        identity = _ensure_target(conn, source, target)
        plan = _plan(conn, cutoff=cutoff, max_days=max_days)
        by_day = {record["day"]: record for record in plan["days"]}
        result = {"schema": AUDIT_VERSION, "database": identity, "prefix": plan,
                  "checkpoint_binding_hash": archive_evidence_hash(binding),
                  "observed_at": conn.execute(text("SELECT clock_timestamp()")).scalar_one().isoformat(),
                  "prefix_hash": archive_evidence_hash(plan), "activation_ready": False,
                  "remaining_acceptance": GATES}
        if day is not None:
            record = by_day.get(day.isoformat())
            if record is None:
                raise RuntimeError(f"canonical_cold_audit_day_outside_retiring_prefix: day={day}")
            result["day_batch"] = _audit_day(conn, checkpoint, binding, object_store, source=source,
                day=day, cutoff=cutoff, child_oid=record["child_oid"], after_ordinal=after_ordinal,
                max_pages=max_pages, deadline=deadline)
        if inventory:
            result["inverse_inventory_batch"] = _audit_inventory(conn, checkpoint, allowed_days=by_day,
                after_manifest=after_manifest, max_pages=max_pages, deadline=deadline)
    return result | {"receipt_hash": archive_evidence_hash(result)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-system-identifier", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--target-database-oid", required=True, type=int)
    parser.add_argument("--checkpoint", required=True, type=Path)
    parser.add_argument("--cold-before-day", required=True, type=date.fromisoformat)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--day", type=date.fromisoformat)
    scope.add_argument("--inverse-inventory", action="store_true")
    parser.add_argument("--after-ordinal", default=-1, type=int)
    parser.add_argument("--after-manifest", default="")
    parser.add_argument("--max-pages", default=1, type=int)
    parser.add_argument("--max-days", default=512, type=int)
    parser.add_argument("--max-seconds", default=120, type=int)
    args = parser.parse_args()
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN is required")
    target = TargetDatabase(args.target_system_identifier, args.target_database, args.target_database_oid)
    root = (configured_archive_root() / "objects").resolve(strict=True)
    store = FilesystemRawArchiveObjectStore(root, writable=False)
    engine = create_engine(dsn, future=True)
    try:
        result = audit_candidate(engine, MigrationCheckpoint(args.checkpoint), target=target,
            object_store=store, cutoff=args.cold_before_day, day=args.day, inventory=args.inverse_inventory,
            after_ordinal=args.after_ordinal, after_manifest=args.after_manifest, max_pages=args.max_pages,
            max_days=args.max_days, max_seconds=args.max_seconds)
        print(json.dumps(result, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

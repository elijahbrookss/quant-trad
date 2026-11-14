"""Stage one-time canonical migration pages with an exact, resumable source comparison.

The source connection is read-only. The staging checkpoint never publishes a
serving generation or authorizes source retirement. Use an isolated snapshot
for rehearsal and a separately fenced source for final handoff.
"""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, date
import json
from math import isfinite
import os
from pathlib import Path
import sqlite3
from time import monotonic

from sqlalchemy import create_engine, text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_storage import verify_archived_envelope
from market_data.fact_archive import (
    FactArchiveLimits, FactArchiveManifest, archive_evidence_hash,
    publish_canonical_fact_archive, read_canonical_fact_archive,
    verify_canonical_fact_archive_rows,
)
from portal.backend.service.storage.repos.fact_archival import FACT_ARCHIVE_VERIFIER_VERSION, _receipt
from portal.backend.service.storage.repos.fact_archive_generation import (
    PREPARED_CATALOG_TABLES, catalog_batch_hash, catalog_commitment, catalog_root,
)
from portal.backend.service.storage.repos.fact_storage import (
    CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM, _catalog_manifest, ensure_payload_contracts,
)


INITIAL = (0, "")
DOCS = ("payload", "provenance", "quality")
SCHEMA_VERSION = "qt.canonical_cold_migration.v2"


def _cursor(row):
    return (row["market_commit_seq"], row["id"])


@dataclass(frozen=True)
class Cohort:
    day: date
    sealed_at: str
    expected_rows: int
    reuse_prefix_pages: int
    state: str


@dataclass(frozen=True)
class SourcePage:
    manifest: FactArchiveManifest
    authority: dict


@dataclass(frozen=True)
class StagedPage:
    source_day: date
    ordinal: int
    after: tuple[int, str]
    manifest: FactArchiveManifest
    reused: bool
    source_authority: dict | None = None


class MigrationCheckpoint:
    """A staging-local journal. One cohort per invocation, bounded pages per batch."""

    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def connection(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            with db:
                yield db

    @contextmanager
    def read_connection(self):
        # The catalog loader must open an existing journal read-only. A typo
        # cannot silently create a new empty database.
        uri = self.path.resolve(strict=True).as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=5)) as db:
            yield db

    def initialize(self, binding):
        value = json.dumps({"schema": SCHEMA_VERSION, **binding}, sort_keys=True)
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS binding (id INTEGER PRIMARY KEY CHECK (id=1), value TEXT NOT NULL)")
            db.execute("INSERT OR IGNORE INTO binding VALUES (1, ?)", (value,))
            if db.execute("SELECT value FROM binding WHERE id=1").fetchone()[0] != value:
                raise RuntimeError("canonical_cold_migration_binding_changed: use a new or matching checkpoint")
            db.execute("""CREATE TABLE IF NOT EXISTS cohorts (
                day TEXT PRIMARY KEY, sealed_at TEXT NOT NULL, expected_rows INTEGER NOT NULL CHECK (expected_rows>=0),
                reuse_prefix_pages INTEGER NOT NULL CHECK (reuse_prefix_pages>=0),
                completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0,1)),
                rows INTEGER NOT NULL DEFAULT 0 CHECK (rows>=0),
                next_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (next_ordinal>=0),
                last_seq INTEGER NOT NULL DEFAULT 0, last_id TEXT NOT NULL DEFAULT ''
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS pages (
                day TEXT NOT NULL REFERENCES cohorts(day), ordinal INTEGER NOT NULL CHECK (ordinal>=0),
                after_seq INTEGER NOT NULL, after_id TEXT NOT NULL,
                manifest_hash TEXT NOT NULL, manifest_id TEXT NOT NULL UNIQUE, descriptor TEXT NOT NULL,
                source_authority TEXT,
                reused INTEGER NOT NULL CHECK (reused IN (0,1)), PRIMARY KEY (day, ordinal)
            )""")

    def start(self, cohort):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""INSERT OR IGNORE INTO cohorts (day, sealed_at, expected_rows, reuse_prefix_pages)
                VALUES (?, ?, ?, ?)""", (cohort.day.isoformat(), cohort.sealed_at,
                                            cohort.expected_rows, cohort.reuse_prefix_pages))
        return self.check_cohort(cohort)

    def check_cohort(self, cohort):
        with self.connection() as db:
            value = db.execute("SELECT sealed_at, expected_rows, reuse_prefix_pages FROM cohorts WHERE day=?",
                               (cohort.day.isoformat(),)).fetchone()
        if value is None or value[:2] != (cohort.sealed_at, cohort.expected_rows) or value[2] > cohort.reuse_prefix_pages:
            raise RuntimeError(f"canonical_cold_migration_source_changed: day={cohort.day}")
        return value[2]

    def progress(self, day):
        with self.read_connection() as db:
            row = db.execute("SELECT completed, rows, next_ordinal, last_seq, last_id FROM cohorts WHERE day=?",
                             (day.isoformat(),)).fetchone()
        if row is None:
            raise RuntimeError(f"canonical_cold_migration_unknown_cohort: day={day}")
        return row

    def binding(self):
        with self.read_connection() as db:
            value = db.execute("SELECT value FROM binding WHERE id=1").fetchone()
        if value is None:
            raise RuntimeError("canonical_cold_migration_missing_binding")
        binding = json.loads(value[0])
        if binding.get("schema") != SCHEMA_VERSION:
            raise RuntimeError("canonical_cold_migration_unsupported_binding")
        return binding

    def record(self, page):
        manifest = page.manifest
        if manifest.first_cursor <= page.after or manifest.row_count <= 0:
            raise RuntimeError(f"canonical_cold_migration_page_invalid: day={page.source_day} ordinal={page.ordinal}")
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE cohorts SET rows=rows+?, next_ordinal=next_ordinal+1,
                last_seq=?, last_id=? WHERE day=? AND next_ordinal=? AND last_seq=? AND last_id=?
                AND completed=0 AND rows+?<=expected_rows""", (
                    manifest.row_count, *manifest.last_cursor, page.source_day.isoformat(), page.ordinal,
                    *page.after, manifest.row_count,
                )).rowcount
            if changed != 1:
                raise RuntimeError(f"canonical_cold_migration_checkpoint_conflict: day={page.source_day}")
            db.execute("INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                page.source_day.isoformat(), page.ordinal, *page.after, manifest.manifest_hash,
                manifest.manifest_id, json.dumps(manifest.to_dict(), sort_keys=True),
                json.dumps(page.source_authority, sort_keys=True) if page.source_authority else None,
                page.reused,
            ))

    def complete(self, day, *, ordinal, after):
        with self.connection() as db:
            changed = db.execute("""UPDATE cohorts SET completed=1 WHERE day=? AND rows=expected_rows
                AND next_ordinal=? AND last_seq=? AND last_id=?""", (day.isoformat(), ordinal, *after)).rowcount
        if changed != 1:
            raise RuntimeError(f"canonical_cold_migration_incomplete_coverage: day={day}")

    def iter_completed_pages(self, *, day=None, after_ordinal=-1):
        """Yield manifest/file membership and exact cursor from completed cohorts only."""
        day_value = day.isoformat() if day is not None else None
        with self.read_connection() as db:
            records = db.execute("""SELECT pages.* FROM pages JOIN cohorts USING(day)
                WHERE cohorts.completed=1 AND (? IS NULL OR day=?) AND ordinal>? ORDER BY day, ordinal""",
                (day_value, day_value, after_ordinal))
            for day_value, ordinal, after_seq, after_id, manifest_hash, identity, payload, authority, reused in records:
                manifest = FactArchiveManifest.from_dict(json.loads(payload), expected_hash=manifest_hash)
                if manifest.manifest_id != identity:
                    raise RuntimeError(f"canonical_cold_migration_manifest_binding_changed: id={identity}")
                yield StagedPage(date.fromisoformat(day_value), ordinal, (after_seq, after_id), manifest,
                                 bool(reused), json.loads(authority) if authority else None)

    def verify_completed_cohort(self, *, day, destination_store, limits=FactArchiveLimits()):
        """Recheck current staged bytes and journal coverage before catalog preparation."""
        complete, expected_rows, expected_pages, seq, identity = self.progress(day)
        if not complete:
            raise RuntimeError(f"canonical_cold_migration_incomplete_coverage: day={day}")
        after = INITIAL
        rows = pages = 0
        for page in self.iter_completed_pages(day=day):
            if page.ordinal != pages or page.after != after or page.manifest.first_cursor <= after:
                raise RuntimeError(f"canonical_cold_migration_checkpoint_order_changed: day={day} page={pages}")
            read_canonical_fact_archive(destination_store.local_path(page.manifest.object_key),
                                        expected=page.manifest, limits=limits)
            rows += page.manifest.row_count
            pages += 1
            after = page.manifest.last_cursor
        if (rows, pages, *after) != (expected_rows, expected_pages, seq, identity):
            raise RuntimeError(f"canonical_cold_migration_checkpoint_coverage_changed: day={day}")
        return {"storage_day": day.isoformat(), "verified_rows": rows, "verified_pages": pages,
                "last_cursor": list(after)}


class PostgresMigrationSource:
    def __init__(self, engine, *, expected_system_identifier, statement_timeout_ms=30_000):
        if type(statement_timeout_ms) is not int or statement_timeout_ms < 1:
            raise ValueError("canonical_cold_migration_invalid_statement_timeout")
        self.engine = engine
        self.expected_system_identifier = str(expected_system_identifier)
        self.statement_timeout_ms = statement_timeout_ms

    @contextmanager
    def snapshot(self):
        with self.engine.begin() as conn:
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            conn.execute(text("SELECT set_config('statement_timeout', :value, true)"),
                         {"value": str(self.statement_timeout_ms)})
            conn.execute(text("SET LOCAL lock_timeout = '2s'"))
            conn.execute(text("SET LOCAL TIME ZONE 'UTC'"))
            yield conn

    def identity(self, conn):
        row = conn.execute(text("""SELECT control.system_identifier::text AS system_identifier,
            current_database() AS database,
            (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()) AS database_oid,
            'market.fact_versions'::regclass::oid::bigint AS header_oid
            FROM pg_catalog.pg_control_system() control""")).mappings().one()
        if row["system_identifier"] != self.expected_system_identifier:
            raise RuntimeError("canonical_cold_migration_wrong_source_system_identifier")
        return dict(row)

    @staticmethod
    def cohort(conn, day):
        row = conn.execute(text("""SELECT p.state,p.sealed_at,p.expected_rows,
            (SELECT coalesce(max(page_ordinal)+1,0) FROM market.fact_archive_manifests WHERE storage_day=:day) AS pages
            FROM market.fact_retention_partitions p WHERE p.storage_day=:day"""), {"day": day}).mappings().one_or_none()
        if row is None or row["state"] not in {"sealed", "verified", "reclaimed"}:
            raise RuntimeError(f"canonical_cold_migration_source_unsealed_or_missing: day={day}")
        return Cohort(day, row["sealed_at"].astimezone(UTC).isoformat(), row["expected_rows"], row["pages"], row["state"])

    @staticmethod
    def existing(conn, day, ordinal):
        row = conn.execute(text("""SELECT * FROM market.fact_archive_manifests
            WHERE storage_day=:day AND page_ordinal=:ordinal"""), {"day": day, "ordinal": ordinal}).mappings().one_or_none()
        if row is None:
            raise RuntimeError(f"canonical_cold_migration_archive_gap: day={day} ordinal={ordinal}")
        manifest = _catalog_manifest(row)
        return SourcePage(manifest, PostgresMigrationSource.source_authority(conn, row))

    @staticmethod
    def source_authority(conn, page):
        """Bind current published authority without creating a new proof receipt."""
        published = conn.execute(text("""SELECT g.preparation_id,g.generation,g.catalog_commitment,
                prep.manifest_id AS prepared_id,prep.storage_day AS prepared_day,
                prep.page_ordinal AS prepared_ordinal,prep.generation AS prepared_generation,
                prep.catalog_commitment AS prepared_commitment,prep.state AS prepared_state,
                prep.manifest_descriptor AS prepared_descriptor,prep.source_binding_hash,
                prep.alias_count,prep.canonical_dependency_count,prep.alias_root,prep.canonical_dependency_root
            FROM market.fact_archive_active_generations g
            LEFT JOIN market.fact_archive_preparations prep ON prep.id=g.preparation_id WHERE g.manifest_id=:id"""),
            {"id": page["id"]}).mappings().one_or_none()
        receipt = conn.execute(text("""SELECT manifest_id,verifier_version,manifest_hash,catalog_hash,verification_hash
            FROM market.fact_archive_verifications WHERE manifest_id=:id AND verifier_version=:version"""),
            {"id": page["id"], "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one_or_none()
        binding = conn.execute(text("""SELECT preparation_id,catalog_commitment
            FROM market.fact_archive_verification_generations
            WHERE manifest_id=:id AND verifier_version=:version"""),
            {"id": page["id"], "version": FACT_ARCHIVE_VERIFIER_VERSION}).mappings().one_or_none()
        authority = {"kind": "legacy_catalog", "manifest_id": page["id"], "manifest_hash": page["manifest_hash"]}
        if published is not None:
            if tuple(published[key] for key in ("prepared_id", "prepared_day", "prepared_ordinal", "prepared_generation",
                                                "prepared_commitment", "prepared_state", "prepared_descriptor")) != (
                page["id"], page["storage_day"], page["page_ordinal"], published["generation"],
                published["catalog_commitment"], "active", page["descriptor"],
            ):
                raise RuntimeError(f"canonical_cold_migration_active_preparation_mismatch: id={page['id']}")
            authority.update(kind="prepared_generation", preparation_id=published["preparation_id"],
                             generation=published["generation"], catalog_commitment=published["catalog_commitment"],
                             prepared_source_binding_hash=published["source_binding_hash"])
            PostgresMigrationSource.verify_prepared_catalog(conn, page, published, authority)
        if receipt is not None and dict(receipt) != _receipt(page, receipt["catalog_hash"]):
            raise RuntimeError(f"canonical_cold_migration_current_receipt_mismatch: id={page['id']}")
        expected_binding = ((published["preparation_id"], published["catalog_commitment"])
                            if published is not None and receipt is not None else None)
        seen_binding = (binding["preparation_id"], binding["catalog_commitment"]) if binding is not None else None
        if seen_binding != expected_binding:
            raise RuntimeError(f"canonical_cold_migration_current_generation_receipt_mismatch: id={page['id']}")
        authority.update(verifier_version=FACT_ARCHIVE_VERIFIER_VERSION,
                         verification_hash=receipt["verification_hash"] if receipt is not None else None)
        return authority

    @staticmethod
    def verify_prepared_catalog(conn, page, prepared, authority):
        # Reuse the current preparation's commitment algorithm. Limit SQL to
        # one selected preparation and the deployed per-page catalog budgets.
        preparation = prepared["preparation_id"]
        roots = {}
        commitments = {}
        specifications = {
            "aliases": ("fact_version_id,evidence_key,series_id,material_hash", "fact_version_id,evidence_key",
                        prepared["alias_count"], prepared["alias_root"], page["row_count"]),
            "canonical_dependencies": ("fact_version_id,row_hash", "fact_version_id",
                                       prepared["canonical_dependency_count"], prepared["canonical_dependency_root"], 50_000),
        }
        for kind, (columns, order, expected_count, expected_root, max_rows) in specifications.items():
            if not 0 <= expected_count <= max_rows:
                raise RuntimeError(f"canonical_cold_migration_prepared_catalog_budget: id={page['id']} catalog={kind}")
            rows = conn.execute(text(f"SELECT batch_ordinal,{columns} FROM market.{PREPARED_CATALOG_TABLES[kind]} "
                f"WHERE preparation_id=:id ORDER BY {order} LIMIT :limit"),
                {"id": preparation, "limit": expected_count + 1}).mappings().all()
            projected = [{name: value for name, value in row.items() if name != "batch_ordinal"} for row in rows]
            root = catalog_root(projected)
            if len(rows) != expected_count or root != expected_root:
                raise RuntimeError(f"canonical_cold_migration_prepared_catalog_root_mismatch: id={page['id']} catalog={kind}")
            receipts = conn.execute(text("""SELECT batch_ordinal,row_count,first_key,last_key,content_hash
                FROM market.fact_archive_catalog_batches WHERE preparation_id=:id AND catalog_kind=:kind
                ORDER BY batch_ordinal LIMIT :limit"""),
                {"id": preparation, "kind": kind, "limit": expected_count + 1}).mappings().all()
            if (sum(item["row_count"] for item in receipts) != expected_count
                    or any(item["batch_ordinal"] != ordinal for ordinal, item in enumerate(receipts))):
                raise RuntimeError(f"canonical_cold_migration_prepared_catalog_receipts_mismatch: id={page['id']} catalog={kind}")
            offset = 0
            keys = order.split(",")
            for ordinal, receipt in enumerate(receipts):
                end = offset + receipt["row_count"]
                batch = projected[offset:end]
                if (not batch or any(row["batch_ordinal"] != ordinal for row in rows[offset:end])
                        or [batch[0][key] for key in keys] != receipt["first_key"]
                        or [batch[-1][key] for key in keys] != receipt["last_key"]
                        or catalog_batch_hash(preparation_id=preparation, catalog_kind=kind, ordinal=ordinal,
                                              rows=batch) != receipt["content_hash"]):
                    raise RuntimeError(f"canonical_cold_migration_prepared_catalog_batch_mismatch: id={page['id']} catalog={kind}")
                offset = end
            roots[kind] = {"count": expected_count, "root": root}
            commitments[kind] = [item["content_hash"] for item in receipts]
        observed = catalog_commitment(manifest_id=page["id"], preparation_id=preparation,
            generation=prepared["generation"], alias_count=prepared["alias_count"],
            canonical_dependency_count=prepared["canonical_dependency_count"], alias_batches=commitments["aliases"],
            canonical_dependency_batches=commitments["canonical_dependencies"])
        if observed != authority["catalog_commitment"]:
            raise RuntimeError(f"canonical_cold_migration_prepared_catalog_commitment_mismatch: id={page['id']}")
        authority["prepared_catalogs"] = roots

    @staticmethod
    def rows(conn, day, after, *, through, limits):
        params = {"day": day, "seq": after[0], "id": after[1], "limit": limits.max_rows,
                  "max_bytes": limits.max_logical_bytes}
        where = "versions.storage_day=:day AND (versions.market_commit_seq,versions.id) > (:seq,:id)"
        if through is not None:
            where += " AND (versions.market_commit_seq,versions.id) <= (:end_seq,:end_id)"
            params.update(end_seq=through[0], end_id=through[1], limit=limits.max_rows+1)
        # Match the v1 archive admission bound. Cap ASCII JSON transfer before
        # reading payloads into the worker, including the escape/timestamp allowance.
        found = conn.execute(text(f"""WITH candidates AS MATERIALIZED (
                SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM} WHERE {where}
                ORDER BY versions.market_commit_seq,versions.id LIMIT :limit
            ), documents AS MATERIALIZED (
                SELECT id,market_commit_seq,to_jsonb(candidates)::text AS document FROM candidates
            ), budget AS (
                SELECT id, sum(octet_length(document)::bigint+35+11::bigint*
                    char_length(regexp_replace(document COLLATE "C", '[ -~]+', '', 'g')))
                    OVER (ORDER BY market_commit_seq,id) AS cumulative_bytes FROM documents
            ) SELECT candidates.* FROM candidates JOIN budget USING(id)
              WHERE budget.cumulative_bytes<=:max_bytes ORDER BY candidates.market_commit_seq,candidates.id"""),
            params).mappings().all()
        if not found and conn.execute(text(f"SELECT EXISTS (SELECT 1 FROM market.fact_versions versions WHERE {where})"),
                                      params).scalar_one():
            raise RuntimeError(f"canonical_cold_migration_source_row_budget_exceeded: day={day} after={after}")
        if through is not None and len(found) > limits.max_rows:
            raise RuntimeError(f"canonical_cold_migration_source_page_budget_exceeded: day={day} after={after}")
        ensure_payload_contracts(conn, [(row["payload_schema_id"], row["payload_contract_hash"]) for row in found])
        return [{name: value for name, value in row.items() if name != "storage_day"} for row in found]


def _compare_existing(source, archived, *, manifest, cohort, after, limits):
    if len(source) != len(archived) or not source or manifest.first_cursor <= after:
        raise RuntimeError(f"canonical_cold_migration_archive_coverage_mismatch: day={cohort.day} after={after}")
    full_source = True
    for expected, found in zip(source, archived, strict=True):
        verify_archived_envelope(expected, found)
        missing = {key for key in DOCS if expected[key] is None}
        if missing:
            if cohort.state != "reclaimed" or len(missing) != len(DOCS):
                raise RuntimeError(f"canonical_cold_migration_hot_documents_missing: fact_version_id={expected['id']}")
            full_source = False
        elif any(expected[key] != found[key] for key in DOCS):
            raise RuntimeError(f"canonical_cold_migration_hot_documents_changed: fact_version_id={expected['id']}")
    if full_source:
        verify_canonical_fact_archive_rows(source, expected=manifest, limits=limits)


def stage(source, checkpoint, *, day, source_store, destination_store, temporary_directory,
          limits=FactArchiveLimits(), max_pages=1, max_seconds=120):
    """Resume one sealed physical day. Page count and wall budget are per invocation."""
    if type(max_pages) is not int or max_pages < 1 or not isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError("canonical_cold_migration_invalid_budget")
    deadline = monotonic() + max_seconds
    with source.snapshot() as conn:
        binding = source.identity(conn)
        cohort = source.cohort(conn, day)
    checkpoint.initialize({"source": binding, "source_root": str(source_store.root),
                           "staging_root": str(destination_store.root), "limits": asdict(limits)})
    prefix = checkpoint.start(cohort)
    for page_index in range(max_pages + 1):
        completed, count, ordinal, seq, identity = checkpoint.progress(day)
        if completed or monotonic() >= deadline:
            break
        after = (seq, identity)
        with source.snapshot() as conn:
            current = source.cohort(conn, day)
            if source.identity(conn) != binding or checkpoint.check_cohort(current) != prefix:
                raise RuntimeError(f"canonical_cold_migration_source_changed: day={day}")
            if page_index == max_pages and count != current.expected_rows:
                break
            archived = source.existing(conn, day, ordinal) if ordinal < prefix else None
            rows = source.rows(conn, day, after, through=archived.manifest.last_cursor if archived else None,
                               limits=limits)
        if not rows:
            if archived is not None:
                raise RuntimeError(f"canonical_cold_migration_archive_source_missing: day={day} ordinal={ordinal}")
            checkpoint.complete(day, ordinal=ordinal, after=after)
            break
        if page_index == max_pages:
            raise RuntimeError(f"canonical_cold_migration_source_exceeds_count: day={day}")
        if monotonic() >= deadline:
            break
        if count + len(rows) > current.expected_rows:
            raise RuntimeError(f"canonical_cold_migration_source_exceeds_count: day={day}")
        if archived is None:
            if any(row[key] is None for row in rows for key in DOCS):
                raise RuntimeError(f"canonical_cold_migration_source_archived_after_checkpoint: day={day}")
            # Source SQL is ordered, but the codec independently verifies this
            # ordering and every canonical document/row hash before publication.
            manifest = publish_canonical_fact_archive(rows, object_store=destination_store,
                                                       temporary_directory=temporary_directory, limits=limits)
            verify_canonical_fact_archive_rows(rows, expected=manifest, limits=limits)
        else:
            manifest = archived.manifest
            source_path = source_store.local_path(manifest.object_key)
            decoded = read_canonical_fact_archive(source_path, expected=manifest, limits=limits)
            _compare_existing(rows, decoded, manifest=manifest, cohort=current, after=after, limits=limits)
            if source_store.root != destination_store.root:
                ack = destination_store.put_verified(object_key=manifest.object_key, source_path=source_path,
                                                      expected_sha256=manifest.object_sha256)
                if (ack.object_key, ack.sha256, ack.byte_count) != (manifest.object_key, manifest.object_sha256, manifest.byte_count):
                    raise RuntimeError(f"canonical_cold_migration_upload_mismatch: id={manifest.manifest_id}")
                read_canonical_fact_archive(destination_store.local_path(manifest.object_key), expected=manifest, limits=limits)
        checkpoint.record(StagedPage(day, ordinal, after, manifest, archived is not None,
                                     archived.authority if archived is not None else None))
    completed, rows, ordinal, seq, identity = checkpoint.progress(day)
    return {"schema": SCHEMA_VERSION, "storage_day": day.isoformat(), "rows": rows, "pages": ordinal,
            "last_cursor": [seq, identity], "cohort_complete": bool(completed), "expected_rows": cohort.expected_rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-system-identifier", required=True)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    parser.add_argument("--source-object-root", required=True, type=Path)
    parser.add_argument("--staging-object-root", required=True, type=Path)
    parser.add_argument("--temporary-directory", required=True, type=Path)
    parser.add_argument("--checkpoint", required=True, type=Path)
    parser.add_argument("--reuse-in-place", action="store_true",
                        help="Allow verified content-addressed staging in the existing object namespace")
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--max-seconds", type=int, default=120)
    parser.add_argument("--max-page-rows", type=int, default=FactArchiveLimits().max_rows)
    parser.add_argument("--max-logical-page-bytes", type=int, default=FactArchiveLimits().max_logical_bytes)
    parser.add_argument("--max-encoded-page-bytes", type=int, default=FactArchiveLimits().max_file_bytes)
    parser.add_argument("--row-group-size", type=int, default=FactArchiveLimits().row_group_size)
    args = parser.parse_args()
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        parser.error("PG_DSN is required")
    roots = [path.resolve(strict=True) for path in (args.source_object_root, args.staging_object_root, args.temporary_directory)]
    if (roots[0] == roots[1]) != args.reuse_in_place:
        parser.error("--reuse-in-place requires identical object roots; otherwise use a separate root")
    if roots[0] in roots[1].parents or roots[1] in roots[0].parents:
        parser.error("source and staging object roots cannot nest")
    if any(not path.is_dir() for path in roots) or roots[0] in roots[2].parents or roots[0] == roots[2]:
        parser.error("supply existing directories and keep temporary files outside the source root")
    if roots[0] in args.checkpoint.resolve().parents:
        parser.error("checkpoint must be outside the source root")
    limits = FactArchiveLimits(max_rows=args.max_page_rows, max_logical_bytes=args.max_logical_page_bytes,
                               max_file_bytes=args.max_encoded_page_bytes, row_group_size=args.row_group_size)
    engine = create_engine(dsn, future=True)
    try:
        result = stage(PostgresMigrationSource(engine, expected_system_identifier=args.source_system_identifier),
                       MigrationCheckpoint(args.checkpoint), day=args.day,
                       source_store=FilesystemRawArchiveObjectStore(roots[0], writable=False),
                       destination_store=FilesystemRawArchiveObjectStore(roots[1], writable=True),
                       temporary_directory=roots[2], limits=limits,
                       max_pages=args.max_pages, max_seconds=args.max_seconds)
        print(json.dumps(result, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

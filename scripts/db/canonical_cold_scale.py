"""Exercise native cold lookup publication on a disposable local PostgreSQL cluster."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import json
from math import ceil
import os
from pathlib import Path
from random import Random
import resource
import shutil
import statistics
import subprocess
import sys
from time import monotonic, perf_counter

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.canonical_storage import record_to_storage_row
from market_data.cold_history import ColdHistoryReader, publish_cold_page, select_fact_rows
from market_data.contracts import SourceIdentity
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_LOCATOR_TABLE_NAMES, COLD_TABLE_NAMES, assert_cold_history_placement
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION
from portal.backend.db.storage_target_models import StorageHeaderTablespaceRecord, StorageTargetRecord
from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
from scripts.db.canonical_cold_audit import _assert_page_series, _locators
from scripts.db.canonical_cold_benchmark import _db_url
from scripts.db.prepare_canonical_cold_catalog import prepare_catalog


VERSION = "qt.canonical_cold_catalog_scale_fixture.v1"
BASE = datetime(2024, 1, 1, tzinfo=UTC)
SERIES = 16
SOURCE = SourceIdentity("SYNTHETIC", "OFFLINE", "fixture", "cold-scale.v1")


def _row(index):
    key, version = divmod(index, 2)
    step, series = divmod(key, SERIES)
    observed = BASE + timedelta(minutes=step)
    accepted = observed + timedelta(minutes=1, seconds=version)
    rate = f"0.{key + 1:07d}{version + 1}"
    fact = CanonicalFact(fact_type="derivatives.funding_rate", payload_schema_id="derivatives.funding_rate.v2",
        observation_key=observed.isoformat(), observation_time=observed, observation_time_method="collector_schedule",
        accepted_at=accepted, known_at=accepted, known_at_method="platform_acceptance", received_at=accepted,
        source=SOURCE, transformation_id="cold-scale.v1", payload={"rate": rate, "raw_rate": rate,
        "funding_time": observed, "interval_seconds": 3600, "unit": "fraction"},
        provenance={"synthetic": True}, quality={"valid": not (version and key % 37 == 0)},
        state="invalidated" if version and key % 37 == 0 else "active")
    return record_to_storage_row(CanonicalFactRecord(series_id=series + 1, source_id=1, revision=version + 1,
        market_commit_seq=index + 1, ingestion_run_id="cold-scale-fixture", fact=fact), series_dimensions={"fixture": True})


def _init(engine, tablespace, directory, rows):
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE SCHEMA market;
            CREATE FUNCTION market.reject_immutable_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
              BEGIN RAISE EXCEPTION 'immutable scale fixture'; END;
            $$;
            CREATE TABLE market.series (id bigint PRIMARY KEY);
            CREATE TABLE market.fact_storage_state (layout_version text,state text);
        """)
        conn.execute(text("INSERT INTO market.series SELECT generate_series(1,:count)"), {"count": min(SERIES, rows // 2)})
        conn.execute(text("INSERT INTO market.fact_storage_state VALUES (:layout,'ready')"), {"layout": FACT_STORAGE_LAYOUT_VERSION})
        StorageTargetRecord.__table__.create(conn)
        StorageHeaderTablespaceRecord.__table__.create(conn)
        identity, postgres = conn.execute(text("""
            SELECT c.system_identifier::text||'/'||db.oid::text,current_setting('server_version')
            FROM pg_control_system() c CROSS JOIN pg_database db WHERE db.datname=current_database()
        """)).one()
        oid = conn.scalar(text("SELECT oid::bigint FROM pg_tablespace WHERE spcname=:name"), {"name": tablespace})
        conn.execute(StorageTargetRecord.__table__.insert(), {"id": "history-fixture", "label": "Disposable scale fixture",
            "filesystem_uuid": "fixture-only", "root": str(directory), "medium": "hdd", "roles": ["history"]})
        conn.execute(StorageHeaderTablespaceRecord.__table__.insert(), {"database_identity": identity, "target_id": "history-fixture",
            "tablespace_oid": oid, "binding": {}})
        prepare_catalog(conn, expected_database=identity, target_id="history-fixture")
    return identity, postgres


def _sizes(conn):
    return {table: {"heap": heap, "indexes": indexes, "total": total} for table, heap, indexes, total in conn.execute(text("""
        SELECT relname,pg_relation_size(c.oid),pg_indexes_size(c.oid),pg_total_relation_size(c.oid)
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='market' AND c.relname=ANY(:names) ORDER BY relname
    """), {"names": list(COLD_TABLE_NAMES)})}


def _stats(samples):
    order = sorted(samples)
    return {"count": len(order), "sum_s": sum(order), "p50_s": statistics.median(order), "p95_s": order[ceil(len(order) * .95) - 1]}


def _require_budget(deadline, directories):
    if monotonic() >= deadline:
        raise TimeoutError("cold scale fixture elapsed budget exhausted")
    if any(shutil.disk_usage(path).free < 5 * 1024**3 for path in directories):
        raise RuntimeError("cold scale fixture free-space floor reached")


def _audit(engine, reader, manifests, deadline, directories):
    times, accepted, root = [], 0, sha256()
    with engine.connect() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        for manifest in manifests:
            _require_budget(deadline, directories)
            begin = perf_counter()
            archived = reader.read_page(manifest)
            locators = _locators(conn, manifest)
            for ordinal, (row, locator) in enumerate(zip(archived, locators, strict=True)):
                expected = _row(accepted)
                actual_location = (locator["id"], locator["page_id"], locator["ordinal"], locator["series_id"],
                                   locator["revision"], locator["market_commit_seq"], bytes(locator["key_digest"]), bytes(locator["row_hash"]))
                if row != expected or actual_location != (row["id"], manifest.manifest_id, ordinal, row["series_id"],
                        row["revision"], row["market_commit_seq"], sha256(row["observation_key"].encode()).digest(), bytes.fromhex(row["row_hash"])):
                    raise RuntimeError("fixture source/archive/catalog exact mismatch")
                root.update(bytes.fromhex(row["row_hash"]))
                accepted += 1
            _assert_page_series(conn, manifest, archived)
            times.append(perf_counter() - begin)
            if len(times) % 10 == 0:
                print(f"audited {accepted} fixture rows in {len(times)} pages", file=sys.stderr, flush=True)
        if conn.scalar(text("SELECT count(*) FROM market.cold_fact_locators")) != accepted:
            raise RuntimeError("fixture inverse locator count mismatch")
    return {"rows": accepted, "sha256_ordered_row_hashes": root.hexdigest(), "page_timings": _stats(times)}


def _lookups(engine, repository, rows, page_rows, repeats, deadline, directories):
    random = Random(1729)
    times = {name: [] for name in ("exact_locator", "exact_with_parquet", "as_of_with_parquet")}
    exact, ranged = sha256(), sha256()
    for _ in range(repeats):
        _require_budget(deadline, directories)
        index = random.randrange(rows)
        expected = _row(index)
        with Session(engine) as s, s.begin():
            s.execute(text("SET TRANSACTION READ ONLY"))
            identity = expected["id"]
            start = perf_counter()
            claim = repository.placement_claims(s, [identity])[identity]
            times["exact_locator"].append(perf_counter() - start)
            if claim[0].ordinal != index % page_rows or claim[1] != expected["row_hash"]:
                raise RuntimeError("fixture native exact locator lookup differs")
            start = perf_counter()
            actual = repository.read_by_ids(s, [identity])[identity]
            times["exact_with_parquet"].append(perf_counter() - start)
            if actual != expected:
                raise RuntimeError("fixture native exact hydrated lookup differs")
            exact.update(bytes.fromhex(actual["row_hash"]))
            scope = dict(series_id=expected["series_id"], start=expected["observation_time"],
                end=expected["observation_time"] + timedelta(seconds=1), as_of_commit_seq=expected["market_commit_seq"],
                known_at_lte=expected["known_at"])
            start = perf_counter()
            pages = repository.candidate_pages(s, **{key: value for key, value in scope.items() if key != "known_at_lte"})
            result = select_fact_rows((), repository.reader.iter_range(pages, **scope), **scope)
            times["as_of_with_parquet"].append(perf_counter() - start)
            if result != ([expected] if expected["state"] == "active" else []):
                raise RuntimeError("fixture native as-of visibility differs")
            ranged.update(bytes.fromhex(archive_evidence_hash({"rows": result})))
    return {"metrics": {name: _stats(samples) for name, samples in times.items()},
            "exact_sha256": exact.hexdigest(), "as_of_sha256": ranged.hexdigest()}


def run(args):
    url = _db_url(os.environ.get("PG_DSN"))
    root = args.root.resolve(strict=True)
    directory = root / "history"
    parts = (root / "objects", root / "staging", directory)
    if (any(not path.is_dir() or list(path.iterdir()) for path in parts)
            or not root.name.startswith("qt-native-cold-scale-") or root.parent != Path("/tmp") or url.database != "qt_bench_native_scale"):
        raise ValueError("scale fixture requires its disposable private database and three empty scratch directories")
    deadline = monotonic() + args.max_seconds
    _require_budget(deadline, (root,))
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    engine = create_engine(url, connect_args={"options": "-c statement_timeout=120000 -c lock_timeout=2000"})
    created_db = created_ts = False
    tablespace = "qt_native_cold_scale_hdd"
    try:
        with admin.connect() as conn:
            if conn.exec_driver_sql("SHOW listen_addresses").scalar_one() != "":
                raise RuntimeError("scale fixture requires TCP disabled in its isolated cluster")
            conn.exec_driver_sql(f"CREATE DATABASE {url.database} TEMPLATE template0")
            created_db = True
            conn.execute(text(f"CREATE TABLESPACE {tablespace} LOCATION :root"), {"root": str(directory)})
            created_ts = True
        identity, postgres = _init(engine, tablespace, directory, args.rows)
        limits = FactArchiveLimits(max_rows=args.page_rows)
        store = FilesystemRawArchiveObjectStore(parts[0])
        reader = ColdHistoryReader(store, limits=limits)
        repo = PostgresColdHistory(reader)
        totals = {name: [] for name in ("generate", "encode_publish", "verify_admit_commit", "simulated_day_exposure")}
        manifests = []
        with engine.connect() as conn:
            start_wal = conn.scalar(text("SELECT pg_current_wal_insert_lsn()::text"))
        for offset in range(0, args.rows, args.page_rows):
            _require_budget(deadline, (root,))
            start = perf_counter()
            group = [_row(index) for index in range(offset, min(offset + args.page_rows, args.rows))]
            totals["generate"].append(perf_counter() - start)
            start = perf_counter()
            page = publish_cold_page(group, object_store=store, temporary_directory=parts[1], limits=limits)
            totals["encode_publish"].append(perf_counter() - start)
            staged_day = date(2024, 1, 1) + timedelta(days=offset // args.page_rows)
            start = perf_counter()
            with Session(engine) as session, session.begin():
                repo.admit(session, page, staged_storage_day=staged_day)
            totals["verify_admit_commit"].append(perf_counter() - start)
            start = perf_counter()
            with Session(engine) as s, s.begin():
                if repo.placement_claims(s, [group[0]["id"]], require_all=False):
                    raise RuntimeError("fixture staged page was visible without retirement receipt")
                s.execute(text("""INSERT INTO market.cold_fact_retired_days
                    (storage_day,manifest_set_hash,row_count,reclaimed_header_bytes,protected_dataset_ranges,handoff_kind)
                    VALUES (:day,:hash,:rows,0,0,'recurring')"""),
                    {"day": staged_day, "hash": page.manifest_hash, "rows": page.row_count})
            totals["simulated_day_exposure"].append(perf_counter() - start)
            manifests.append(page)
            if len(manifests) % 10 == 0:
                print(f"published {offset + len(group)} fixture rows in {len(manifests)} pages", file=sys.stderr, flush=True)
        with engine.connect() as conn:
            assert_cold_history_placement(conn)
            sizes = _sizes(conn)
            wal_bytes = conn.scalar(text("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(:start AS pg_lsn))::bigint"), {"start": start_wal})
        exact = _audit(engine, reader, manifests, deadline, (root,))
        if exact["rows"] != args.rows:
            raise RuntimeError("cold scale fixture source total differs")
        result = {"version": VERSION, "database_identity": identity, "postgres": postgres,
            "application_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "rows": args.rows, "pages": len(manifests), "page_rows": args.page_rows, "archive_file_bytes": sum(p.byte_count for p in manifests),
            "tables": sizes, "lookup_tables_total_bytes": sum(sizes[name]["total"] for name in COLD_LOCATOR_TABLE_NAMES),
            "publication_wal_bytes": wal_bytes, "phases": {name: _stats(samples) for name, samples in totals.items()},
            "audit": exact, "lookups": _lookups(engine, repo, args.rows, args.page_rows, args.repeats, deadline, (root,)),
            "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "physical_scope": {"history_and_objects_st_dev": [path.stat().st_dev for path in (directory, parts[0])],
                               "pgdata_st_dev": (root / "pgdata").stat().st_dev, "shared_local_fixture_disk": True}}
    finally:
        engine.dispose()
        try:
            with admin.connect() as conn:
                if created_db:
                    conn.exec_driver_sql(f"DROP DATABASE {url.database} WITH (FORCE)")
                if created_ts:
                    conn.exec_driver_sql(f"DROP TABLESPACE {tablespace}")
        finally:
            admin.dispose()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--page-rows", type=int, default=10_000)
    parser.add_argument("--max-seconds", type=int, default=4200)
    parser.add_argument("--repeats", type=int, default=24)
    args = parser.parse_args()
    if (not 32 <= args.rows <= 1_100_000 or args.rows % 2 or not 512 <= args.page_rows <= 10_000
            or args.page_rows % 2 or not 5 <= args.repeats <= 50 or not 1 <= args.max_seconds <= 4200):
        parser.error("fixture input is outside the bounded row, page, sample or duration range")
    print(json.dumps(run(args), sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

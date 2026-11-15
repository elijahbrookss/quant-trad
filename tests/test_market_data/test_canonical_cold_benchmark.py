from copy import deepcopy
from datetime import UTC, date, datetime
import os
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from scripts.db import canonical_cold_benchmark as bench
from tests.test_market_data.test_canonical_cold_migration_db import _prepare, local_catalog_server  # noqa: F401


def _plan():
    return {"restore_pair_hash": "0" * 64, "commit_seq": 2, "retire_before": "2026-08-10", "repeats": 5,
            "timeout_seconds": 5, "max_rows": 50, "max_archive_bytes": 100000,
            "cases": [{"name": "mixed", "mode": "all_revisions", "series_id": 1,
                       "start": "2026-08-09T00:00:00Z", "end": "2026-08-12T00:00:00Z",
                       "known_at_lte": "2026-08-12T00:00:00Z", "source_identity_keys": [],
                       "storage_witnesses": {"old-id": "2026-08-09", "hot-id": "2026-08-10"}}]}


def _report(plan, side, result):
    return {"version": bench.VERSION, "side": side, "plan": plan, "plan_hash": bench._digest(plan),
            "machine": {"host": "fixture"}, "runner_sha256": "1" * 64, "cache_policy": "fixture",
            "python": "3.12", "libraries": {}, "database": {"layout": bench.LAYOUTS[side],
            "postgres": "15", "timescaledb": None, "settings": {}, "cold_before_day": plan["retire_before"]},
            "cases": {"mixed": result | {"scope": {"commit_seq": 2}}}}


def test_exact_result_comparison_and_case_budget():
    plan = bench.validate_plan(_plan())
    fact = SimpleNamespace(to_dict=lambda: {"known_at": "2026-08-09T01:00:00Z", "payload": {"rate": "1.1"}})
    row = SimpleNamespace(series_id=1, source_id=1, market_commit_seq=2, revision=1, fact_version_id="old-id",
                          ingestion_run_id="run", row_hash="fixture", fact=fact)
    kwargs = {"expected_series": 1, "commit": 2, "limit": 2, "witnesses": ("old-id",)}
    report = bench.measure_case(lambda: [row], summarize=lambda rows: bench._record_result(rows, **kwargs),
                                repeats=5, seconds=5, max_rows=2, max_archive_bytes=50)
    source = _report(plan, "source", report)
    candidate = _report(plan, "candidate", report)
    summary = bench.compare(source, candidate)
    assert summary["cases"]["mixed"]["result"]["rows"] == 1
    assert len(report["repeated"]) == 5
    assert report["repeated_p95_ns"] >= report["repeated_p50_ns"] > 0
    changed = deepcopy(candidate)
    changed["cases"]["mixed"]["result"]["sha256"] = "a" * 64
    with pytest.raises(RuntimeError, match="read result differs"):
        bench.compare(source, changed)
    with pytest.raises(RuntimeError, match="repeated an identity"):
        bench._record_result([row, row], **kwargs)
    with pytest.raises(RuntimeError, match="storage witnesses"):
        bench._record_result([row], **(kwargs | {"witnesses": ("missing",)}))
    with pytest.raises(RuntimeError, match="escaped its scope"):
        bench._record_result([row], **(kwargs | {"commit": 1}))
    plan["cases"][0]["as_of_commit_seq"] = 3
    with pytest.raises(ValueError, match="as-of commit exceeds"):
        bench.validate_plan(plan)
    plan["cases"][0].pop("as_of_commit_seq")
    plan["cases"][0]["storage_witnesses"].pop("old-id")
    with pytest.raises(ValueError, match="retiring fact and a retained hot fact"):
        bench.validate_plan(plan)


def test_frozen_series_preserves_the_named_receiver_hash_and_serializes_native_clocks():
    from scripts.automation.canonical_cold_receiver_runtime import fingerprint_frozen_records

    plan = _plan()
    plan["commit_seq"] = 3
    case = {key: plan["cases"][0][key] for key in ("series_id", "start", "end", "storage_witnesses")}
    case.update(name="legacy", mode="frozen_series", dataset_id="mds_fixture", dataset_hash="a" * 64,
                series_commit_seq=3, expected_rows=1)
    plan["cases"].append(case)
    bench.validate_plan(plan)
    manifest = {"series_id": 1, "range_start": bench._timestamp(case["start"]),
                "range_end": bench._timestamp(case["end"]), "max_commit_seq": 3, "row_count": 1,
                "source_summary": {}}
    dataset = SimpleNamespace(dataset_id=case["dataset_id"], dataset_hash=case["dataset_hash"],
                              max_commit_seq=3, series=[manifest])
    clock = datetime(2026, 8, 9, 1, tzinfo=UTC)
    material = {"known_at": clock.isoformat()}
    row = SimpleNamespace(series_id=1, revision=2, market_commit_seq=3, ingestion_run_id="run",
                          source_identity_key="provider", canonical_material_hash="material",
                          fact=SimpleNamespace(to_dict=lambda: material))

    def read_dataset_series(*, dataset_id, series_id):
        assert (dataset_id, series_id) == (dataset.dataset_id, 1)
        return [row]

    repo = SimpleNamespace(get_dataset=lambda _: dataset, read_dataset_series=read_dataset_series)
    read, summarize, _ = bench._operation(case, plan, repo, None, None)
    expected = "819392c7e89fe519e49a50a1f7ee8abcf719383cde9c72a61d8fb9b6b07048c5"
    for value in (clock.isoformat(), clock):
        material["known_at"] = value
        records = read()
        assert summarize(records) == {"rows": 1, "sha256": expected}
        assert fingerprint_frozen_records(records, mode="frozen_series", series_id=1, commit_seq=3) == expected
    with pytest.raises(RuntimeError, match="different row count than its manifest"):
        summarize([])
    material["known_at"] = object()
    with pytest.raises(TypeError):
        summarize(read())
    manifest["source_summary"]["record_selection"] = "all_canonical_revisions.v1"
    with pytest.raises(RuntimeError, match="legacy frozen series manifest differs"):
        bench._operation(case, plan, repo, None, None)


def test_unsafe_connection_rejected():
    with pytest.raises(ValueError, match="explicit Unix socket"):
        bench._db_url("postgresql+psycopg2://dbreader@127.0.0.1:5432/qt_bench_fake")
    with pytest.raises(ValueError, match="explicit Unix socket"):
        bench._db_url("postgresql+psycopg2://dbreader@/production?host=/tmp&port=5432")


@pytest.mark.db
def test_private_restore_evidence_readonly_session_and_exact_placement(local_catalog_server):
    from portal.backend.db.session import Database
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.orm import sessionmaker

    url, _root, _account = local_catalog_server
    name = "qt_bench_evidence_fixture"
    url = url.update_query_dict({"port": "5432"})
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    target = url.set(database=name)
    setup = create_engine(target)
    reader = create_engine(bench._db_url(target.render_as_string(hide_password=False)),
                           connect_args={"options": "-c default_transaction_read_only=on"})
    try:
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {name} TEMPLATE template0")
        with setup.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA market")
            conn.exec_driver_sql("""
                CREATE TABLE market.fact_storage_state (layout_version text, state text,
                    completed_at timestamptz, evidence jsonb);
                CREATE TABLE market.fact_versions (id text,storage_day date);
                CREATE TABLE market.cold_fact_locators (id text,page_id text);
                CREATE TABLE market.cold_fact_pages (id text,staged_storage_day date);
                CREATE TABLE market.cold_fact_retired_days (storage_day date,handoff_kind text);
                INSERT INTO market.fact_versions VALUES ('old-id','2026-08-09'), ('hot-id','2026-08-10');
            """)
            conn.execute(text("INSERT INTO market.fact_storage_state VALUES (:layout,'ready',now(),'{}')"),
                         {"layout": bench.LAYOUTS["source"]})
        assert bench._database_evidence(reader)["name"] == name
        bench._verify_witnesses(reader, _plan(), "source")
        with pytest.raises(RuntimeError, match="fact placement differs"):
            bench._verify_witnesses(reader, _plan(), "candidate")
        with setup.begin() as conn:
            conn.exec_driver_sql("""
                DELETE FROM market.fact_versions WHERE id='old-id';
                INSERT INTO market.cold_fact_pages VALUES ('cold-page',NULL);
                INSERT INTO market.cold_fact_locators VALUES ('old-id','cold-page');
                INSERT INTO market.cold_fact_retired_days VALUES ('2026-08-09','initial');
            """)
        bench._verify_witnesses(reader, _plan(), "candidate")

        database = Database()
        database._engine = reader
        database._available = True
        database._session_factory = sessionmaker(bind=reader)
        with database.locked_snapshot_session(shared_lock_name="benchmark-fixture") as session:
            assert session.scalars(text("SELECT storage_day FROM market.fact_versions")).all() == [date(2026, 8, 10)]
            assert session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            assert session.scalar(text("SHOW transaction_read_only")) == "on"
            with pytest.raises(DBAPIError) as error:
                session.execute(text("DELETE FROM market.fact_versions"))
            assert error.value.orig.pgcode == "25006"
            session.rollback()
    finally:
        reader.dispose()
        setup.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        admin.dispose()


@pytest.mark.db
def test_runner_times_same_public_revision_read_across_hot_and_cold(local_catalog_server, monkeypatch, tmp_path):
    from market_data.archive import FilesystemRawArchiveObjectStore
    from market_data.cold_history import ColdHistoryReader
    from market_data.fact_archive import publish_canonical_fact_archive
    from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
    from tests.test_market_data.test_canonical_fact_archive import _row

    url, root, account = local_catalog_server
    name = "qt_bench_read_fixture"
    tablespace = "qt_bench_read_hdd"
    hdd = root / tablespace
    hdd.mkdir(mode=0o700)
    if os.geteuid() == 0:
        os.chown(hdd, account.pw_uid, account.pw_gid)
    url = url.update_query_dict({"port": "5432"})
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    target = url.set(database=name)
    setup = create_engine(target)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    old, hot = _row(), _row(2)
    archive = publish_canonical_fact_archive([old], object_store=store, temporary_directory=tmp_path)
    plan = _plan()
    plan["cases"][0]["storage_witnesses"] = {old["id"]: "2026-08-09", hot["id"]: "2026-08-10"}
    monkeypatch.setenv("PG_DSN", target.render_as_string(hide_password=False))
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(bench, "_code_revision", lambda module, expected: expected)
    try:
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {name} TEMPLATE template0")
            conn.execute(text(f"CREATE TABLESPACE {tablespace} LOCATION :root"), {"root": str(hdd)})
        identity = _prepare(setup, [old, hot], archive, tablespace, hdd)
        with setup.begin() as conn:
            conn.exec_driver_sql("""
                ALTER TABLE market.fact_storage_state ADD COLUMN evidence jsonb;
                CREATE FUNCTION market.read_fact_headers_in_range(sid bigint, lo timestamptz, hi timestamptz)
                RETURNS SETOF market.fact_versions LANGUAGE SQL STABLE AS $$
                    SELECT * FROM market.fact_versions WHERE series_id=sid AND observation_time>=lo AND observation_time<hi;
                $$;
            """)
            for table in ("fact_versions", "fact_hot_payloads"):
                conn.execute(text(f"UPDATE market.{table} SET storage_day='2026-08-10' WHERE id=:id"), {"id": hot["id"]})
        args = {"revision": "a" * 40, "runtime_digest": "sha256:" + "b" * 64, "archive_root": tmp_path,
                "system_identifier": identity.system_identifier, "database_oid": identity.oid}
        before = bench.run(SimpleNamespace(side="source", **args), bench.validate_plan(plan))
        with setup.begin() as conn:
            PostgresColdHistory(ColdHistoryReader(store)).admit(conn, archive)
            conn.execute(text("DELETE FROM market.fact_versions WHERE id=:id"), {"id": old["id"]})
            conn.execute(text("DELETE FROM market.fact_hot_payloads WHERE id=:id"), {"id": old["id"]})
            conn.execute(text("""
                INSERT INTO market.cold_fact_retired_days
                    (storage_day,manifest_set_hash,row_count,reclaimed_header_bytes,protected_dataset_ranges,handoff_kind)
                VALUES ('2026-08-09',:digest,1,0,0,'initial');
            """), {"digest": archive.manifest_hash})
            conn.execute(text("""UPDATE market.fact_storage_state SET layout_version=:layout,
                evidence='{"cold_before_day":"2026-08-10"}'"""), {"layout": bench.LAYOUTS["candidate"]})
        after = bench.run(SimpleNamespace(side="candidate", **args), plan)
        comparison = bench.compare(before, after)
        assert comparison["cases"]["mixed"]["result"]["rows"] == 2
        assert after["cases"]["mixed"]["first"]["archive_counters"]["archive_source_bytes"] > 0
    finally:
        setup.dispose()
        with admin.connect() as conn:
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
            conn.exec_driver_sql(f"DROP TABLESPACE IF EXISTS {tablespace}")
        admin.dispose()

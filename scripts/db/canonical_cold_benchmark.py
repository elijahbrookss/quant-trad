"""Compare QT reads on a paired offline restore. See canonical_cold_benchmark.md."""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
from functools import partial
from hashlib import sha256
from importlib import metadata
from math import ceil
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
from time import perf_counter_ns, process_time_ns


VERSION = "qt.canonical_cold_read_benchmark.v1"
LAYOUTS = {"source": "market.fact_storage_tiers.v2", "candidate": "market.canonical_cold_history.v1"}
MODES = {"latest_as_of", "all_revisions", "frozen_revisions", "frozen_series", "book_replay"}


def _json(value, *, default=None):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=default)


def _digest(value):
    return sha256(_json(value).encode()).hexdigest()


def _matches(value, pattern):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _bounded_int(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def _timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("benchmark timestamps require an explicit time zone")
    return parsed.astimezone(UTC)


def _identity(value):
    return _matches(value, r"[A-Za-z0-9._:/-]{1,160}")


def validate_plan(plan):
    if set(plan) != {"restore_pair_hash", "commit_seq", "retire_before", "repeats", "timeout_seconds",
                    "max_rows", "max_archive_bytes", "cases"}:
        raise ValueError("benchmark plan keys are incomplete or unknown")
    if (not _matches(plan["restore_pair_hash"], r"[0-9a-f]{64}")
            or not _bounded_int(plan["commit_seq"], 1, 2**63 - 1)
            or not _matches(plan["retire_before"], r"\d{4}-\d{2}-\d{2}")
            or not _bounded_int(plan["repeats"], 5, 100)
            or not _bounded_int(plan["timeout_seconds"], 1, 300)
            or not _bounded_int(plan["max_rows"], 1, 500_000)
            or not _bounded_int(plan["max_archive_bytes"], 1, 2**30)
            or not isinstance(plan["cases"], list) or not 1 <= len(plan["cases"]) <= 20):
        raise ValueError("benchmark plan is outside its bounded read limits")
    cutoff = date.fromisoformat(plan["retire_before"])
    names = set()
    placements = set()
    for case in plan["cases"]:
        if not _identity(case.get("name")) or case["name"] in names or case.get("mode") not in MODES:
            raise ValueError("benchmark case requires a unique name and a supported read mode")
        names.add(case["name"])
        base = {"name", "mode"}
        if case["mode"] == "book_replay":
            if (set(case) != base | {"definition_id", "session_id"}
                    or any(not _identity(case[key]) for key in ("definition_id", "session_id"))):
                raise ValueError("book replay requires an exact definition and session")
            continue
        base |= {"series_id", "start", "end", "storage_witnesses"}
        if case["mode"] == "frozen_series":
            base |= {"series_commit_seq", "expected_rows"}
            if (not _bounded_int(case.get("series_commit_seq"), 1, plan["commit_seq"])
                    or not _bounded_int(case.get("expected_rows"), 1, plan["max_rows"])):
                raise ValueError("frozen series requires a bounded series commit and expected row count")
        else:
            base |= {"known_at_lte", "source_identity_keys"}
        if case["mode"] in {"frozen_revisions", "frozen_series"}:
            base |= {"dataset_id", "dataset_hash"}
            if (not _identity(case.get("dataset_id"))
                    or not _matches(case.get("dataset_hash"), r"[0-9a-f]{64}")):
                raise ValueError("frozen reads require the exact dataset ID and hash")
        elif "as_of_commit_seq" in case:
            base.add("as_of_commit_seq")
            if not _bounded_int(case["as_of_commit_seq"], 1, plan["commit_seq"]):
                raise ValueError("benchmark as-of commit exceeds the restored boundary")
        if set(case) != base or not _bounded_int(case["series_id"], 1, 2**63 - 1):
            raise ValueError("benchmark fact read scope is incomplete")
        if not timedelta(0) < _timestamp(case["end"]) - _timestamp(case["start"]) <= timedelta(days=366):
            raise ValueError("benchmark fact windows must be positive and at most 366 days")
        if case["mode"] != "frozen_series":
            _timestamp(case["known_at_lte"])
            keys = case["source_identity_keys"]
            if not isinstance(keys, list) or len(keys) > 16 or any(not _identity(key) for key in keys):
                raise ValueError("benchmark source identity scope is invalid")
        witnesses = case["storage_witnesses"]
        if not isinstance(witnesses, dict) or not 1 <= len(witnesses) <= 4:
            raise ValueError("benchmark fact case requires one to four retained fact/day witnesses")
        for fact_id, value in witnesses.items():
            if not _identity(fact_id) or not _matches(value, r"\d{4}-\d{2}-\d{2}"):
                raise ValueError("benchmark witness must name an exact fact ID and accepted storage day")
            placements.add(date.fromisoformat(value) < cutoff)
    if placements != {True, False}:
        raise ValueError("benchmark plan must exercise both a retiring fact and a retained hot fact")
    return plan


def _stat_io():
    try:
        counters = (line.split(": ") for line in Path("/proc/self/io").read_text().splitlines())
        return {key: int(value) for key, value in counters}
    except OSError:
        return {}


def _record_result(records, *, expected_series, commit, limit, witnesses):
    if not records or len(records) > limit:
        raise RuntimeError("benchmark fact result is empty or exceeds the plan row limit")
    digest = sha256()
    size = 0
    seen = set()
    for row in records:
        if (row.series_id != expected_series or not 0 < row.market_commit_seq <= commit
                or not row.fact_version_id or row.fact_version_id in seen):
            raise RuntimeError("benchmark fact result escaped its scope or repeated an identity")
        seen.add(row.fact_version_id)
        encoded = (_json([row.fact_version_id, row.series_id, row.source_id, row.revision,
                          row.market_commit_seq, row.ingestion_run_id, row.row_hash, row.fact.to_dict()]) + "\n").encode()
        digest.update(encoded)
        size += len(encoded)
    if not seen.issuperset(witnesses):
        raise RuntimeError("benchmark fact read did not return all declared storage witnesses")
    return {"rows": len(records), "sha256": digest.hexdigest(), "encoded_result_bytes": size}


def _replay_result(result, *, limit):
    rows = result["raw_record_count"]
    digest = result["replay_fingerprint"]
    if (not _bounded_int(rows, 1, limit) or not _matches(digest, r"[0-9a-f]{64}")
            or result["reconciliation"]["equal"] is not True or result["checkpoint_delta_equal"] is False):
        raise RuntimeError("benchmark book replay did not verify")
    return {"rows": rows, "sha256": digest, "snapshots": result["snapshot_count"],
            "mutation_batches": result["mutation_batch_count"], "checkpoints": result["checkpoint_count"],
            "checkpoint_delta_equal": result["checkpoint_delta_equal"],
            "features": result["features"], "final_state_hash": result["final_state_hash"]}


def measure_case(operation, *, summarize, repeats, seconds, max_rows, max_archive_bytes):
    from core.execution_control import ExecutionControl, controlled_execution

    samples = []
    expected = None
    for _ in range(repeats + 1):
        control = ExecutionControl()
        control.limit(seconds=seconds, input_rows=max_rows, archive_read_bytes=max_archive_bytes)
        before_io = _stat_io()
        before_cpu = process_time_ns()
        with controlled_execution(control):
            before = perf_counter_ns()
            output = operation()
            read_ns = perf_counter_ns() - before
            result = summarize(output)
            elapsed = perf_counter_ns() - before
        del output
        cpu = process_time_ns() - before_cpu
        after_io = _stat_io()
        sample = {"read_ns": read_ns, "read_and_hash_ns": elapsed, "process_cpu_ns": cpu,
                  "process_read_bytes": after_io["read_bytes"] - before_io["read_bytes"]
                  if "read_bytes" in before_io and "read_bytes" in after_io else None,
                  "archive_counters": {key: value for key, value in control.snapshot()["consumed"].items()
                                       if key.startswith("archive_")}}
        samples.append(sample)
        if expected is None:
            expected = result
        elif result != expected:
            raise RuntimeError("benchmark case result changed between repeated reads")
    repeated = sorted(sample["read_ns"] for sample in samples[1:])
    return {"result": expected, "first": samples[0], "repeated": samples[1:],
            "repeated_p50_ns": statistics.median(repeated), "repeated_p95_ns": repeated[ceil(0.95 * repeats) - 1]}


def _db_url(dsn):
    from sqlalchemy.engine import make_url

    if not dsn or any(os.environ.get(key) for key in ("PGHOSTADDR", "PGSERVICE", "PGOPTIONS")):
        raise ValueError("use PG_DSN with a private Unix socket and no libpq service or host overrides")
    url = make_url(dsn)
    socket_dir = url.query.get("host")
    if (url.drivername not in {"postgresql", "postgresql+psycopg2"} or url.host or url.port
            or not url.username or not _matches(url.database, r"qt_bench_[a-z0-9_]{1,48}")
            or set(url.query) != {"host", "port"} or not _matches(url.query["port"], r"[0-9]{1,5}")
            or not 1 <= int(url.query["port"]) <= 65535 or not isinstance(socket_dir, str)):
        raise ValueError("PG_DSN must target a disposable qt_bench_* database via one explicit Unix socket")
    directory = Path(socket_dir)
    if (not directory.is_absolute() or directory != directory.resolve() or "," in socket_dir
            or not directory.is_dir() or directory.stat().st_mode & 0o022
            or not (directory / f".s.PGSQL.{url.query['port']}").is_socket()):
        raise ValueError("PG_DSN must point to a private local PostgreSQL socket directory")
    return url


def _database_evidence(engine):
    from sqlalchemy import text

    with engine.connect() as conn:
        if not conn.exec_driver_sql("""
            SELECT current_setting('transaction_read_only')='on' AND inet_server_addr() IS NULL
                   AND current_setting('listen_addresses')=''
        """).scalar_one():
            raise RuntimeError("benchmark requires a read-only session on a Unix-socket-only restore cluster")
        result = dict(conn.execute(text("""
            SELECT system_identifier::text, current_database() AS name,
                   (SELECT oid FROM pg_database WHERE datname=current_database()) AS oid,
                   current_setting('server_version') AS postgres,
                   (SELECT extversion FROM pg_extension WHERE extname='timescaledb') AS timescaledb,
                   pg_database_size(current_database()) AS database_bytes,
                   pg_postmaster_start_time()::text AS postmaster_start
            FROM pg_catalog.pg_control_system()
        """)).mappings().one())
        result.update(conn.execute(text("""
            SELECT layout_version AS layout, evidence->>'cold_before_day' AS cold_before_day
            FROM market.fact_storage_state
            WHERE state='ready' AND completed_at IS NOT NULL
        """)).mappings().one())
        result["settings"] = {key: value for key, value in conn.execute(text("""
            SELECT name,setting FROM pg_settings WHERE name IN
                ('shared_buffers','effective_cache_size','work_mem','random_page_cost',
                 'effective_io_concurrency','max_parallel_workers_per_gather','jit')
        """))}
    return result


def _verify_witnesses(engine, plan, side):
    from sqlalchemy import text

    cutoff = date.fromisoformat(plan["retire_before"])
    with engine.connect() as conn:
        for case in plan["cases"]:
            for fact_id, expected in case.get("storage_witnesses", {}).items():
                day = date.fromisoformat(expected)
                rows = conn.execute(text("""
                    SELECT storage_day FROM market.fact_versions WHERE id=:id AND storage_day=:day
                """), {"id": fact_id, "day": day}).scalars().all()
                cold = []
                if side == "candidate":
                    cold = conn.execute(text("""
                        SELECT COALESCE(page.staged_storage_day, retired.storage_day)
                        FROM market.cold_fact_locators AS loc
                        JOIN market.cold_fact_pages AS page ON page.id=loc.page_id
                        LEFT JOIN market.cold_fact_retired_days AS retired
                          ON retired.storage_day=:day AND retired.handoff_kind='initial'
                        WHERE loc.id=:id AND (page.staged_storage_day IS NULL OR EXISTS (
                            SELECT 1 FROM market.cold_fact_retired_days AS visible
                            WHERE visible.storage_day=page.staged_storage_day
                        ))
                    """), {"id": fact_id, "day": day}).scalars().all()
                expected_placement = ([], [day]) if side == "candidate" and day < cutoff else ([day], [])
                if (rows, cold) != expected_placement:
                    raise RuntimeError(f"benchmark fact placement differs for case {case['name']} witness {fact_id}")


def _code_revision(repo_module, expected):
    checkout = Path(repo_module.__file__).resolve().parents[5]
    revision = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    if revision != expected or subprocess.run(
        ["git", "-C", str(checkout), "diff", "--quiet", "HEAD", "--"], check=False,
    ).returncode:
        raise RuntimeError("benchmark imported application must be the exact clean checkout")
    return revision


def _frozen_series_operation(case, plan, repo):
    from core.execution_control import execution_checkpoint

    dataset = repo.get_dataset(case["dataset_id"])
    series_id = case["series_id"]
    commit = case["series_commit_seq"]
    series = next((row for row in dataset.series if row["series_id"] == series_id), None)
    if (dataset.dataset_id != case["dataset_id"] or dataset.dataset_hash != case["dataset_hash"]
            or not 0 < commit <= dataset.max_commit_seq <= plan["commit_seq"]
            or series is None or series["source_summary"].get("record_selection") is not None
            or (series["range_start"], series["range_end"], series["max_commit_seq"], series["row_count"]) != (
                _timestamp(case["start"]), _timestamp(case["end"]), commit, case["expected_rows"])):
        raise RuntimeError("benchmark legacy frozen series manifest differs from the declared case")

    def summarize(records):
        if len(records) != case["expected_rows"]:
            raise RuntimeError("benchmark frozen series returned a different row count than its manifest")
        digest = sha256()
        for row in records:
            execution_checkpoint()
            if row.series_id != series_id or not 0 < row.market_commit_seq <= commit:
                raise RuntimeError("benchmark frozen series returned a record outside its watermark or series")
            # Match the named receiver's frozen_series fingerprint on the standalone source overlay.
            material = [row.series_id, row.revision, row.market_commit_seq, row.ingestion_run_id,
                        row.source_identity_key, row.canonical_material_hash, row.fact.to_dict()]
            digest.update((_json(material, default=datetime.isoformat) + "\n").encode())
        return {"rows": len(records), "sha256": digest.hexdigest()}

    scope = {"dataset_id": dataset.dataset_id, "dataset_hash": dataset.dataset_hash,
             "series_id": series_id, "commit_seq": commit, "expected_rows": case["expected_rows"]}
    return partial(repo.read_dataset_series, dataset_id=dataset.dataset_id, series_id=series_id), summarize, scope


def _operation(case, plan, repo, book, archive_root):
    if case["mode"] == "frozen_series":
        return _frozen_series_operation(case, plan, repo)
    if case["mode"] == "book_replay":
        from market_data.archive import FilesystemRawArchiveObjectStore

        scope = {"definition_id": case["definition_id"], "session_id": case["session_id"]}
        raw = book.repository.list_session_manifests(**scope)
        checkpoints = book.repository.list_book_checkpoints(**scope)
        store = FilesystemRawArchiveObjectStore(archive_root / "objects", writable=False)
        archive_bytes = 0
        for row in raw + checkpoints:
            size = store.local_path(str(row["object_key"])).stat().st_size
            if size <= 0 or size != int(row["byte_count"]):
                raise RuntimeError("benchmark replay object size differs from the manifest")
            archive_bytes += size
        if (not raw or sum(int(row["record_count"]) for row in raw) > plan["max_rows"]
                or archive_bytes > plan["max_archive_bytes"]):
            raise RuntimeError("benchmark replay session is missing or exceeds the plan limits")
        def summarize(result):
            return _replay_result(result, limit=plan["max_rows"]) | {"raw_and_checkpoint_file_bytes": archive_bytes}

        return partial(book.replay_book_session, **scope, storage_root=archive_root), summarize, scope

    scope = {key: _timestamp(case[key]) for key in ("start", "end", "known_at_lte")}
    scope.update(series_id=case["series_id"], source_identity_keys=case["source_identity_keys"])
    commit = case.get("as_of_commit_seq", plan["commit_seq"])
    if case["mode"] == "frozen_revisions":
        dataset = repo.get_dataset(case["dataset_id"])
        if (dataset.dataset_id != case["dataset_id"] or dataset.dataset_hash != case["dataset_hash"]
                or not 0 < dataset.max_commit_seq <= commit):
            raise RuntimeError("benchmark frozen dataset identity or commit boundary differs")
        series = next((row for row in dataset.series if row["series_id"] == case["series_id"]), None)
        if series is None or scope["start"] < series["range_start"] or scope["end"] > series["range_end"]:
            raise RuntimeError("benchmark frozen case exceeds the dataset series range")
        commit = int(series["max_commit_seq"])
        if not 0 < commit <= dataset.max_commit_seq:
            raise RuntimeError("benchmark frozen series watermark exceeds its dataset")
        scope["dataset_id"] = dataset.dataset_id
        read = repo.read_dataset_fact_revisions
    else:
        scope["as_of_commit_seq"] = commit
        read = repo.read_facts if case["mode"] == "latest_as_of" else repo.read_fact_revisions
    summarize = partial(_record_result, expected_series=case["series_id"], commit=commit, limit=plan["max_rows"],
                        witnesses=case["storage_witnesses"])
    return partial(read, **scope), summarize, {"commit_seq": commit}


def run(args, plan):
    os.environ["QT_DISABLE_DOTENV"] = "1"
    import resource

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    url = _db_url(os.environ.get("PG_DSN"))
    if (not _matches(args.revision, r"[0-9a-f]{40}") or not _matches(args.runtime_digest, r"sha256:[0-9a-f]{64}")):
        raise ValueError("provide a full Git SHA and runtime image or environment digest")

    from core.settings import get_settings
    from core.storage_mounts import configured_archive_root, require_configured_archive_mount
    from portal.backend.db.session import db
    from portal.backend.service.market import market_structure_service as book_module
    from portal.backend.service.storage.repos import market_data

    revision = _code_revision(market_data, args.revision)
    if get_settings().storage.history_cache_bytes:
        raise RuntimeError("benchmark requires the optional writable history cache to be disabled")
    archive_root = configured_archive_root()
    if (not archive_root.is_absolute() or archive_root != archive_root.resolve()
            or archive_root != args.archive_root.resolve() or not (archive_root / "objects").is_dir()):
        raise RuntimeError("benchmark requires the explicit restored archive root and existing objects directory")
    mount = require_configured_archive_mount(archive_root, require_writable=False)
    timeout = plan["timeout_seconds"] * 1000
    engine = create_engine(url, connect_args={"application_name": "qt_offline_cold_benchmark", "connect_timeout": 5,
        "options": f"-c default_transaction_read_only=on -c statement_timeout={timeout} -c lock_timeout={timeout}"})
    object_store = book_module.FilesystemRawArchiveObjectStore
    try:
        before = _database_evidence(engine)
        if (before["system_identifier"], before["oid"], before["layout"]) != (
            args.system_identifier, args.database_oid, LAYOUTS[args.side],
        ) or (args.side == "candidate" and before["cold_before_day"] != plan["retire_before"]):
            raise RuntimeError("benchmark database identity or storage authority differs from the declared restore")
        if db._engine is not None:
            raise RuntimeError("benchmark requires a standalone process without initialized application sessions")
        # Keep the application session and lifecycle shared lease. Installing the
        # engine here skips the normal schema-bootstrap mutation path.
        db._engine = engine
        db._session_factory = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
        db._available = True
        repo = market_data.PostgresMarketDataRepository()
        # Replay's object-store default also serves collectors. Use its existing
        # read-only mode so the identical service can run on a read-only mount.
        book_module.FilesystemRawArchiveObjectStore = partial(object_store, writable=False)
        book = book_module.MarketStructureService()
        if repo.current_commit_seq() != plan["commit_seq"]:
            raise RuntimeError("benchmark source boundary differs from the restored commit watermark")
        _verify_witnesses(engine, plan, args.side)
        cases = {}
        for case in plan["cases"]:
            operation, summarize, evidence = _operation(case, plan, repo, book, archive_root)
            cases[case["name"]] = measure_case(operation, summarize=summarize, repeats=plan["repeats"],
                seconds=plan["timeout_seconds"], max_rows=plan["max_rows"],
                max_archive_bytes=plan["max_archive_bytes"]) | {"scope": evidence}
        if _database_evidence(engine) != before or repo.current_commit_seq() != plan["commit_seq"]:
            raise RuntimeError("benchmark database evidence changed during the read workload")
    finally:
        book_module.FilesystemRawArchiveObjectStore = object_store
        if db._engine is engine:
            db._engine = None
            db._session_factory = None
            db._available = False
        engine.dispose()
    return {"version": VERSION, "side": args.side, "plan": plan, "plan_hash": _digest(plan), "cases": cases,
            "application_revision": revision, "runtime_digest_declared": args.runtime_digest, "database": before,
            "machine": platform.uname()._asdict() | {"logical_cpu_count": os.cpu_count()},
            "python": platform.python_version(), "libraries": {key: metadata.version(key)
                for key in ("sqlalchemy", "psycopg2-binary", "pyarrow")},
            "archive": {"root": str(archive_root), "device": archive_root.stat().st_dev,
                        "uuid": mount.filesystem_uuid if mount else None},
            "peak_process_rss_kib_linux": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "cache_policy": "qt_read_cache_disabled_os_and_pg_caches_uncontrolled",
            "runner_sha256": sha256(Path(__file__).read_bytes()).hexdigest()}


def compare(source, candidate):
    for side, report in (("source", source), ("candidate", candidate)):
        validate_plan(report["plan"])
        if (report["version"] != VERSION or report["side"] != side or report["plan_hash"] != _digest(report["plan"])
                or report["database"]["layout"] != LAYOUTS[side]
                or (side == "candidate" and report["database"]["cold_before_day"] != report["plan"]["retire_before"])
                or set(report["cases"]) != {case["name"] for case in report["plan"]["cases"]}):
            raise RuntimeError("benchmark receipt has the wrong side, scope or layout")
    for key in ("plan_hash", "machine", "runner_sha256", "cache_policy", "python", "libraries"):
        if source[key] != candidate[key]:
            raise RuntimeError(f"benchmark receipts differ in {key}")
    for key in ("postgres", "timescaledb", "settings"):
        if source["database"][key] != candidate["database"][key]:
            raise RuntimeError(f"benchmark database runtime differs in {key}")
    overview = {}
    for name, before in source["cases"].items():
        after = candidate["cases"][name]
        if before["scope"] != after["scope"] or before["result"] != after["result"]:
            raise RuntimeError(f"benchmark read result differs for {name}")
        overview[name] = {"result": before["result"], "source_p50_ns": before["repeated_p50_ns"],
                          "candidate_p50_ns": after["repeated_p50_ns"], "source_p95_ns": before["repeated_p95_ns"],
                          "candidate_p95_ns": after["repeated_p95_ns"]}
    return {"version": VERSION, "plan_hash": source["plan_hash"], "cases": overview,
            "source_report_hash": _digest(source), "candidate_report_hash": _digest(candidate)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    trial = commands.add_parser("run")
    trial.add_argument("--side", choices=LAYOUTS, required=True)
    trial.add_argument("--plan", type=Path, required=True)
    trial.add_argument("--revision", required=True)
    trial.add_argument("--runtime-digest", required=True)
    trial.add_argument("--system-identifier", required=True)
    trial.add_argument("--database-oid", type=int, required=True)
    trial.add_argument("--archive-root", type=Path, required=True)
    comparison = commands.add_parser("compare")
    comparison.add_argument("--source", type=Path, required=True)
    comparison.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "run":
        if platform.system() != "Linux":
            parser.error("restored trials require Linux with a private PostgreSQL Unix socket")
        report = run(args, validate_plan(json.loads(args.plan.read_text())))
    else:
        report = compare(json.loads(args.source.read_text()), json.loads(args.candidate.read_text()))
    print(json.dumps(report, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

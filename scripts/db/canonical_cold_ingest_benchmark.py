"""Run native QT ingestion alongside pinned reads on a disposable restored clone."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from functools import partial
from hashlib import sha256
from importlib import metadata
from math import ceil
import json
import os
from pathlib import Path
import platform
import statistics
from threading import Barrier, Event
from time import perf_counter_ns
from types import SimpleNamespace

if __package__:
    from . import canonical_cold_benchmark as reads
else:
    import canonical_cold_benchmark as reads


VERSION = "qt.canonical_cold_ingest_benchmark.v1"
FACT_TYPE = "derivatives.funding_rate"
CONTRACT = "derivatives.funding_rate.v2"


def validate_plan(plan):
    if set(plan) != {"reads", "batches", "batch_rows", "interval_ms", "observation_start"}:
        raise ValueError("mixed workload plan is incomplete or has unknown fields")
    history = reads.validate_plan(plan["reads"])
    if (any(case["mode"] == "book_replay" for case in history["cases"])
            or not reads._bounded_int(plan["batches"], 8, 80)
            or not reads._bounded_int(plan["batch_rows"], 1, 128)
            or not reads._bounded_int(plan["interval_ms"], 0, 1000)):
        raise ValueError("mixed workload requires bounded batches and fact or frozen read cases")
    reads._timestamp(plan["observation_start"])
    return plan


def _fact(source, observation, acceptance, position, *, corrected):
    from market_data.canonical import CanonicalFact

    rate = f"0.{position + 1:05d}{1 if corrected else 0}"
    return CanonicalFact(fact_type=FACT_TYPE, payload_schema_id=CONTRACT,
        observation_key=f"qt-offline-position-{position:06d}", observation_time=observation,
        observation_time_method="collector_schedule", source=source, transformation_id=VERSION,
        received_at=acceptance, accepted_at=acceptance, known_at=acceptance, known_at_method="platform_acceptance",
        payload={"rate": rate, "raw_rate": rate, "funding_time": observation - timedelta(hours=1),
                 "interval_seconds": 3600, "unit": "fraction"},
        provenance={"synthetic": True, "position": position}, quality={"valid": True})


def _time(operation, *, seconds, **limits):
    from core.execution_control import ExecutionControl, controlled_execution

    control = ExecutionControl()
    control.limit(seconds=seconds, **limits)
    with controlled_execution(control):
        before = perf_counter_ns()
        value = operation()
        end = perf_counter_ns()
    return value, {"start_ns": before, "end_ns": end, "latency_ns": end - before,
                   "archive_counters": {key: amount for key, amount in control.snapshot()["consumed"].items()
                                        if key.startswith("archive_")}}


def _write(repo, *, source, scope, plan, gate, stop):
    horizon = reads._timestamp(plan["observation_start"])
    count = plan["batch_rows"]
    timeout = plan["reads"]["timeout_seconds"]
    output, expected = [], []
    sequence = 0
    fresh_positions, previous = (), []
    gate.wait(timeout=timeout)
    for step in range(plan["batches"]):
        if stop.is_set():
            raise RuntimeError("mixed workload reader failed")
        accepted = datetime.now(UTC)
        operation = ("insert", "retry", "correct", "retry")[step % 4]
        if operation == "insert":
            fresh_positions = range(sequence, sequence + count)
            sequence += count
        if operation == "retry":
            facts, counts = previous, (0, 0, count)
        else:
            facts = [_fact(source, horizon + timedelta(seconds=position), accepted, position,
                           corrected=operation == "correct") for position in fresh_positions]
            counts = (count, 0, 0) if operation == "insert" else (0, count, 0)
        if facts[-1].observation_time > accepted:
            raise RuntimeError("synthetic observation clock exceeds actual acceptance")
        run_id = f"{scope['id']}-{step:03d}"
        request = {"schema": VERSION, "run": scope["id"], "step": step, "kind": operation}
        outcome, sample = _time(partial(repo.ingest_facts, series_id=scope["series"], source_id=scope["source"],
                                        facts=facts, ingestion_run_id=run_id, request=request), seconds=timeout)
        if (outcome.ingestion_run_id != run_id or outcome.requested_count != count
                or (outcome.inserted_count, outcome.corrected_count, outcome.noop_count) != counts):
            raise RuntimeError(f"mixed workload writer outcome differs: step={step} operation={operation}")
        if operation != "retry":
            expected.extend((fact, run_id, 1 if operation == "insert" else 2) for fact in facts)
        previous = facts
        output.append(sample | {"operation": operation, "run_id": run_id, "counts": counts,
                                "commit_seq": outcome.max_commit_seq, "dispatch_utc": accepted.isoformat()})
        if stop.wait(plan["interval_ms"] / 1000):
            raise RuntimeError("mixed workload reader failed")
    return output, expected


def _history(operations, settings, gate, stop):
    results, samples = {}, []
    gate.wait(timeout=settings["timeout_seconds"])
    for _ in range(settings["repeats"]):
        for name, operation, summarize, scope in operations:
            if stop.is_set():
                raise RuntimeError("mixed workload writer failed")
            value, sample = _time(operation, seconds=settings["timeout_seconds"],
                                  input_rows=settings["max_rows"], archive_read_bytes=settings["max_archive_bytes"])
            result = summarize(value) | {"scope": scope}
            if results.setdefault(name, result) != result:
                raise RuntimeError(f"mixed workload changed pinned history: case={name}")
            samples.append(sample | {"case": name})
    return samples, results


def _percentiles(samples):
    values = sorted(item["latency_ns"] for item in samples)
    return {"count": len(values), "p50_ns": statistics.median(values), "p95_ns": values[ceil(0.95 * len(values)) - 1]}


def _lanes(writes, samples):
    def overlaps(left, right):
        return left["start_ns"] < right["end_ns"] and right["start_ns"] < left["end_ns"]

    mixed_writes = [item for item in writes if any(overlaps(item, other) for other in samples)]
    mixed_reads = [item for item in samples if any(overlaps(item, other) for other in writes)]
    if not mixed_writes or not mixed_reads:
        raise RuntimeError("mixed workload did not observe overlapping public read and write calls")
    report = {}
    for lane, rows, mixed, key in (("ingest", writes, mixed_writes, "operation"),
                                   ("history", samples, mixed_reads, "case")):
        report[lane] = {}
        for name in sorted({row[key] for row in rows}):
            concurrent = [row for row in mixed if row[key] == name]
            report[lane][name] = {"all": _percentiles([row for row in rows if row[key] == name]),
                                  "overlapping": _percentiles(concurrent) if concurrent else None}
    return report


def exercise(repo, *, source, scope, plan, operations, writing):
    gate, stop = Barrier(2), Event()

    def fail_peer(operation):
        try:
            return operation()
        except BaseException:
            stop.set()
            gate.abort()
            raise

    def writer():
        with writing():
            return _write(repo, source=source, scope=scope, plan=plan, gate=gate, stop=stop)

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="qt-offline-mixed") as workers:
        writer_task = workers.submit(fail_peer, writer)
        reader_task = workers.submit(fail_peer, partial(_history, operations, plan["reads"], gate, stop))
        writes, expected = writer_task.result()
        samples, history = reader_task.result()
    return writes, expected, samples, history, _lanes(writes, samples)


def _audit(repo, reader, *, expected, writes, scope, plan, today, side):
    from market_data.canonical import build_fact_version_id
    from sqlalchemy import text

    prior_write = plan["reads"]["commit_seq"]
    for write in writes:
        commit = write["commit_seq"]
        retry = write["operation"] == "retry"
        if (retry and commit != prior_write) or (not retry and commit <= prior_write):
            raise RuntimeError("mixed workload retry advanced commit or a genuine write did not")
        prior_write = commit
    start = reads._timestamp(plan["observation_start"])
    end = start + timedelta(seconds=len(expected) + 1)
    rows = repo.read_fact_revisions(series_id=scope["series"], start=start, end=end)
    actual = {(row.fact.observation_key, row.revision): row for row in rows}
    if len(actual) != len(expected) or len(rows) != len(expected):
        raise RuntimeError("mixed workload exact native revision count differs")
    normalized = []
    prior = plan["reads"]["commit_seq"]
    for fact, run, revision in expected:
        row = actual[(fact.observation_key, revision)]
        if (row.fact != fact or row.row_hash != fact.row_hash or row.ingestion_run_id != run
                or row.fact_version_id != build_fact_version_id(series_id=scope["series"],
                    observation_key=fact.observation_key, revision=revision, row_hash=fact.row_hash)
                or row.source_id != scope["source"] or row.market_commit_seq <= prior):
            raise RuntimeError("mixed workload stored revision or actual acceptance clock differs")
        prior = row.market_commit_seq
        normalized.append({"key": fact.observation_key, "revision": revision, "payload": dict(fact.payload),
                           "observation_time": fact.observation_time.isoformat(), "provenance": dict(fact.provenance)})
    newest = repo.read_facts(series_id=scope["series"], start=start, end=end)
    latest = {key: row for (key, _), row in sorted(actual.items())}
    if ({row.fact.observation_key: row for row in newest} != latest or repo.current_commit_seq() != prior
            or writes[-1]["commit_seq"] != prior):
        raise RuntimeError("mixed workload latest read/watermark differs or another writer advanced it")
    with reader.connect() as conn:
        hot = conn.execute(text("""
            SELECT h.id,h.storage_day,p.id,i.id FROM market.fact_versions AS h
            LEFT JOIN market.fact_hot_payloads AS p ON p.storage_day=h.storage_day AND p.id=h.id
            LEFT JOIN market.fact_identities AS i ON i.storage_day=h.storage_day AND i.id=h.id
            WHERE h.series_id=:series
        """), scope).all()
        if set(hot) != {(row.fact_version_id, today, row.fact_version_id, row.fact_version_id) for row in rows}:
            raise RuntimeError("mixed workload synthetic revision did not remain hot with its payload and identity")
        if side == "candidate" and conn.scalar(text("SELECT EXISTS (SELECT FROM market.cold_fact_locators WHERE series_id=:series)"), scope):
            raise RuntimeError("mixed workload candidate routed synthetic live facts into cold storage")
        runs = conn.execute(text("""
            SELECT id,requested_count,inserted_count,corrected_count,noop_count,status FROM market.ingestion_runs
            WHERE id=ANY(:ids) ORDER BY id
        """), {"ids": [write["run_id"] for write in writes]}).all()
        if len(runs) != len(writes) or any(
            tuple(run) != (write["run_id"], plan["batch_rows"], *write["counts"], "completed")
            for run, write in zip(runs, writes)
        ):
            raise RuntimeError("mixed workload persisted ingestion-run outcomes differ")
    return {"rows": len(rows), "latest": len(newest), "normalized_material_sha256": reads._digest(normalized),
            "exact_run_sha256": reads._record_result(rows, expected_series=scope["series"], commit=prior,
                                                     limit=len(rows), witnesses=())["sha256"], "final_commit": prior}


@contextmanager
def _bind(module, *, reader, writer):
    lane = ContextVar("mixed_benchmark_writer", default=False)

    @contextmanager
    def writing():
        token = lane.set(True)
        try:
            yield
        finally:
            lane.reset(token)

    original = module.db
    module.db = SimpleNamespace(session=lambda: (writer if lane.get() else reader).session(),
                                locked_snapshot_session=reader.locked_snapshot_session)
    try:
        yield writing
    finally:
        module.db = original


def _register(repo, writer, scope_id):
    from market_data.contracts import SourceIdentity
    from portal.backend.db import InstrumentRecord

    with writer.session() as session:
        session.add(InstrumentRecord(id=scope_id, datasource="QT_SYNTHETIC", exchange="OFFLINE",
                                     symbol=scope_id, instrument_type="spot", extra_metadata={"benchmark": VERSION}))
    source = SourceIdentity(provider="QT_SYNTHETIC", venue="OFFLINE", source_kind="benchmark",
                            adapter_version=scope_id)
    source_id = repo.register_source(source, lineage={"benchmark": VERSION, "run": scope_id})
    series_id = repo.register_series(instrument_id=scope_id, fact_type=FACT_TYPE, timeframe_seconds=None,
                                     contract_version=CONTRACT)
    return source, {"id": scope_id, "source": source_id, "series": series_id}


def run(args, plan):
    os.environ["QT_DISABLE_DOTENV"] = "1"
    from core.settings import get_settings
    from core.storage_mounts import configured_archive_root, require_configured_archive_mount
    from portal.backend.db.session import Database
    from portal.backend.service.storage.repos import market_data
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    url = reads._db_url(os.environ.get("PG_DSN"))
    plan = validate_plan(plan)
    if (args.disposable_database != url.database or not reads._matches(args.revision, r"[0-9a-f]{40}")
            or not reads._matches(args.runtime_digest, r"sha256:[0-9a-f]{64}")):
        raise ValueError("provide the exact disposable database name, full application Git SHA and runtime digest")
    revision = reads._code_revision(market_data, args.revision)
    history = plan["reads"]
    root = configured_archive_root()
    if (get_settings().storage.history_cache_bytes or not root.is_absolute()
            or root != root.resolve() or root != args.archive_root.resolve() or not (root / "objects").is_dir()):
        raise RuntimeError("mixed workload requires the explicit restored archive root and disabled writable QT cache")
    require_configured_archive_mount(root, require_writable=False)
    timeout = history["timeout_seconds"] * 1000

    def database(read_only):
        owner = Database()
        owner._engine = create_engine(url, connect_args={"application_name": "qt_offline_mixed_benchmark", "connect_timeout": 5,
            "options": f"-c default_transaction_read_only={read_only} -c statement_timeout={timeout} -c lock_timeout={timeout}"})
        owner._session_factory = sessionmaker(bind=owner._engine, expire_on_commit=False, autoflush=False)
        owner._available = True
        return owner

    reader = database("on")
    writer = None
    try:
        evidence = reads._database_evidence(reader._engine)
        if ((evidence["system_identifier"], evidence["oid"], evidence["layout"])
                != (args.system_identifier, args.database_oid, reads.LAYOUTS[args.side])
                or args.side == "candidate" and evidence["cold_before_day"] != history["retire_before"]):
            raise RuntimeError("mixed workload database identity or authority differs")
        reads._verify_witnesses(reader._engine, history, args.side)
        writer = database("off")
        scope_id = "qt-bench-" + reads._digest(plan)[:20]
        with _bind(market_data, reader=reader, writer=writer) as writing:
            repo = market_data.PostgresMarketDataRepository()
            if repo.current_commit_seq() != history["commit_seq"]:
                raise RuntimeError("mixed workload requires a fresh restored commit boundary")
            with reader.session() as session:
                today = session.scalar(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date"))
                if session.scalar(text("SELECT state FROM market.fact_retention_partitions WHERE storage_day=:day"),
                                  {"day": today}) != "open":
                    raise RuntimeError("mixed workload requires today's open hot storage partition on the restore")
            final_observation = reads._timestamp(plan["observation_start"]) + timedelta(
                seconds=ceil(plan["batches"] / 4) * plan["batch_rows"] - 1)
            if final_observation > datetime.now(UTC):
                raise RuntimeError("mixed workload requires all synthetic observation times to precede dispatch")
            operations = [(case["name"], *reads._operation(case, history, repo, None, root)) for case in history["cases"]]
            with writing():
                source, scope = _register(repo, writer, scope_id)
            writes, expected, samples, results, summary = exercise(repo, source=source, scope=scope, plan=plan,
                                                                   operations=operations, writing=writing)
            acceptance = _audit(repo, reader._engine, expected=expected, writes=writes, scope=scope, plan=plan,
                                today=today, side=args.side)
    finally:
        if writer is not None:
            writer._engine.dispose()
        reader._engine.dispose()
    return {"version": VERSION, "side": args.side, "plan": plan, "plan_sha256": reads._digest(plan),
            "application_revision": revision, "runtime_digest_declared": args.runtime_digest,
            "runner_sha256": sha256(Path(__file__).read_bytes()).hexdigest(), "database": evidence, "history_results": results,
            "ingestion": acceptance, "summary": summary, "write_samples": writes, "history_samples": samples,
            "machine": platform.uname()._asdict() | {"logical_cpu_count": os.cpu_count()},
            "python": platform.python_version(), "libraries": {name: metadata.version(name)
                for name in ("sqlalchemy", "psycopg2-binary", "pyarrow")},
            "archive_root": str(root), "storage_day": today.isoformat()}


def _validate_receipt(trial, side):
    plan = validate_plan(trial["plan"])
    history = plan["reads"]
    writes, samples = trial["write_samples"], trial["history_samples"]
    case_names = [case["name"] for case in history["cases"]]
    kinds = [("insert", "retry", "correct", "retry")[index % 4] for index in range(plan["batches"])]
    if (trial["version"] != VERSION or trial["side"] != side or trial["database"]["layout"] != reads.LAYOUTS[side]
            or (side == "candidate" and trial["database"]["cold_before_day"] != history["retire_before"])
            or trial["plan_sha256"] != reads._digest(plan) or [item["operation"] for item in writes] != kinds
            or [item["case"] for item in samples] != case_names * history["repeats"]
            or set(trial["history_results"]) != set(case_names)
            or trial["summary"] != _lanes(writes, samples)):
        raise RuntimeError("mixed workload receipt is incomplete or has the wrong authority")


def compare(source, candidate):
    for side, trial in (("source", source), ("candidate", candidate)):
        _validate_receipt(trial, side)
    for key in ("machine", "plan_sha256", "history_results", "runner_sha256", "python", "libraries"):
        if source[key] != candidate[key]:
            raise RuntimeError(f"mixed workload receipt differs in {key}")
    for key in ("postgres", "timescaledb", "settings"):
        if source["database"][key] != candidate["database"][key]:
            raise RuntimeError(f"mixed workload database configuration differs in {key}")
    for key in ("rows", "latest", "normalized_material_sha256"):
        if source["ingestion"][key] != candidate["ingestion"][key]:
            raise RuntimeError(f"mixed workload normalized ingestion differs in {key}")
    return {"version": VERSION, "plan_sha256": source["plan_sha256"], "history_results": source["history_results"],
            "source": source["summary"], "candidate": candidate["summary"],
            "source_sha256": reads._digest(source), "candidate_sha256": reads._digest(candidate)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    trial = commands.add_parser("run")
    trial.add_argument("--side", choices=reads.LAYOUTS, required=True)
    trial.add_argument("--plan", type=Path, required=True)
    trial.add_argument("--disposable-database", required=True, help="exact clone name authorized to receive synthetic writes")
    trial.add_argument("--revision", required=True)
    trial.add_argument("--runtime-digest", required=True)
    trial.add_argument("--system-identifier", required=True)
    trial.add_argument("--database-oid", type=int, required=True)
    trial.add_argument("--archive-root", type=Path, required=True)
    pair = commands.add_parser("compare")
    pair.add_argument("--source", type=Path, required=True)
    pair.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    report = (run(args, json.loads(args.plan.read_text())) if args.command == "run"
              else compare(json.loads(args.source.read_text()), json.loads(args.candidate.read_text())))
    print(json.dumps(report, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

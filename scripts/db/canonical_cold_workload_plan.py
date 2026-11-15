"""Assemble H05 and B1 benchmark inputs from a verified private source clone."""

import argparse
from datetime import date, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path

if __package__:
    from . import canonical_cold_benchmark as reads
    from . import canonical_cold_ingest_benchmark as ingest
else:
    import canonical_cold_benchmark as reads
    import canonical_cold_ingest_benchmark as ingest


VERSION = "qt.canonical_cold_saved_workloads.v1"
INSTRUMENT = "0a019a46-c679-4c30-b90c-41518c68e31f"
H05 = "mds_b7c35a1ac454e899031ec4a06dda41f3"
B1 = "mds_230a9cff937d9280043e893357842af3"
B1_HASH = "230a9cff937d9280043e893357842af30a1be3434d3610dc511f91b924396927"
B1_AUTHORED_SHA = "49c6fc3eff2c1820e94ab4cc8686d2845c4c0302c8c0de5353f1b949b5cec1ad"
B1_REQUEST_HASH = "3ec2fe646a6b6241b5bd8c0032e31023761d737092bf4a7e8cd51c57306382ed"
B1_DEFINITION_HASH = "9999f4647f36b70ab1bb4ec101abc49de95581d549f38d747bdb7b6fcc8b5072"
B1_PLAN_HASH = "b3f4466cd1da8f18e83d33bda0972dc7ae7e9e9231d7821b56a43693edc86e3d"
B1_RESULT_HASH = "0793df23b58a215c5a8672b2589ce1435c827e547b2455fc531602ea45161e0d"
B1_PRODUCING_REVISION = "dc0f17cd0d91d2f3ea9e2a5c1b91524d08f62c76"
SOURCE_ARCHIVE_RELATIONS = (
    "fact_archive_active_generations", "fact_archive_prepared_material_aliases", "fact_archive_preparations",
)


def _document(path, *, limit):
    with Path(path).open("rb") as stream:
        raw = stream.read(limit + 1)
    if not 0 < len(raw) <= limit:
        raise ValueError("saved workload input exceeds its byte limit")
    return raw, json.loads(raw)


def validate_b1_payload(authored, immutable_request, definition, resolved):
    from research_science.check import CheckDefinition, CheckRequest, ResolvedCheckPlan

    if sha256(authored).hexdigest() != B1_AUTHORED_SHA:
        raise ValueError("B1 authored request bytes differ from saved request")
    payload = json.loads(authored)
    definition = CheckDefinition.from_dict(definition)
    request = CheckRequest.from_dict(immutable_request)
    plan = ResolvedCheckPlan.from_dict(resolved)
    if ((definition.definition_hash, request.definition_hash, request.request_hash, request.dataset_id,
         plan.plan_hash, plan.request_hash) != (B1_DEFINITION_HASH, B1_DEFINITION_HASH, B1_REQUEST_HASH, B1,
                                                B1_PLAN_HASH, B1_REQUEST_HASH)):
        raise ValueError("B1 retained definition, request or plan differs")
    return payload


def _days(scope):
    cutoff = date.fromisoformat(scope["retire_before"])
    days = scope["accepted_days"]
    if not isinstance(days, list) or not 1 <= len(days) <= 12:
        raise ValueError("provide one to twelve exact accepted storage days")
    days = sorted(date.fromisoformat(day) for day in days)
    if len(set(days)) != len(days) or not days[0] < cutoff <= days[-1]:
        raise ValueError("accepted storage days must be distinct and cross the retirement boundary")
    return cutoff, days


def _catalog(conn, *, dataset_id, series_id, frame, start, end, watermark):
    from sqlalchemy import text

    rows = conn.execute(text("""
        SELECT d.dataset_hash, d.max_commit_seq AS dataset_commit, d.created_at,
               ds.range_start, ds.range_end, ds.max_commit_seq, ds.row_count,
               ds.material_hash, ds.provenance_hash, ds.quality_hash,
               ds.source_summary, ds.payload_schemas
        FROM market.datasets AS d JOIN market.dataset_series AS ds ON ds.dataset_id=d.id
        JOIN market.series AS s ON s.id=ds.series_id
        WHERE d.id=:dataset_id AND ds.series_id=:series_id
          AND s.instrument_id=:instrument AND s.fact_type='candle.ohlcv' AND s.timeframe_seconds=:frame
          AND s.contract_version='candle.ohlcv.v1'
        LIMIT 2
    """), {"dataset_id": dataset_id, "series_id": series_id, "instrument": INSTRUMENT, "frame": frame}).mappings().all()
    if len(rows) != 1:
        raise ValueError(f"saved dataset membership missing or ambiguous: {dataset_id}/{series_id}")
    row = dict(rows[0])
    if (not reads._matches(row["dataset_hash"], r"[a-f0-9]{64}")
            or not row["dataset_hash"].startswith(dataset_id.removeprefix("mds_"))
            or not 0 < row["max_commit_seq"] <= row["dataset_commit"] <= watermark
            or (row["range_start"], row["range_end"]) != (reads._timestamp(start), reads._timestamp(end))
            or not 0 < row["row_count"] <= 44_641):
        raise ValueError(f"saved dataset range, commit or contract differs: {dataset_id}/{series_id}")
    return row


def _case(conn, *, name, series, start, end, clock, commit, days, source_keys=(), frozen=None):
    from sqlalchemy import text

    witnesses = {}
    for day in days:
        key = conn.execute(text("""
            SELECT f.id FROM market.fact_versions AS f JOIN market.sources AS s ON s.id=f.source_id
            WHERE f.storage_day=:day AND f.series_id=:series AND f.observation_time>=:start
              AND f.observation_time<:end AND f.known_at<=:clock AND f.market_commit_seq<=:commit
              AND (cardinality(CAST(:sources AS text[]))=0 OR s.identity_key=ANY(CAST(:sources AS text[])))
            ORDER BY f.observation_time, f.market_commit_seq, f.id LIMIT 1
        """), {"day": day, "series": series, "start": start, "end": end, "clock": clock,
                "commit": commit, "sources": list(source_keys)}).scalar_one_or_none()
        if key is not None:
            witnesses[key] = day.isoformat()
    if not witnesses:
        raise ValueError(f"no exact accepted-day witness for {name}")
    case = {"name": name, "series_id": series, "start": start.isoformat(), "end": end.isoformat(),
            "known_at_lte": clock.isoformat(), "source_identity_keys": list(source_keys),
            "storage_witnesses": dict(list(witnesses.items())[:4])}
    if frozen is not None:
        return case | {"mode": "frozen_revisions", "dataset_id": frozen[0], "dataset_hash": frozen[1]}
    return case | {"mode": "all_revisions", "as_of_commit_seq": commit}


def assemble(conn, *, scope, proof_sha256, watermark, b1_request):
    """Read closed native memberships and one witness per bounded physical day."""
    from sqlalchemy import text

    if set(scope) != {"retire_before", "accepted_days", "hot"}:
        raise ValueError("saved workload scope requires retire_before, accepted_days and hot")
    cutoff, days = _days(scope)
    hot = scope["hot"]
    if (not isinstance(hot, dict) or set(hot) != {"series_id", "start", "end"}
            or not reads._bounded_int(hot["series_id"], 1, 2**63 - 1)):
        raise ValueError("retained hot witness requires an exact series and observation window")
    hot_start, hot_end = (reads._timestamp(hot[key]) for key in ("start", "end"))
    if not timedelta(0) < hot_end - hot_start <= timedelta(days=1):
        raise ValueError("retained hot witness window must be positive and at most one day")
    missing = [name for name in SOURCE_ARCHIVE_RELATIONS if conn.execute(text(
        "SELECT to_regclass(:name) IS NULL"), {"name": f"market.{name}"}).scalar_one()]
    if missing:
        raise RuntimeError("restored source lacks deployed archive generation relations: " + ", ".join(missing))

    end = "2022-02-01T00:00:00Z"
    h05 = _catalog(conn, dataset_id=H05, series_id=713, frame=60, start="2021-12-31T23:59:00Z", end=end, watermark=watermark)
    b1 = _catalog(conn, dataset_id=B1, series_id=2361, frame=900, start="2021-12-30T23:45:00Z", end=end, watermark=watermark)
    if (b1["dataset_hash"], b1["max_commit_seq"], b1["row_count"]) != (B1_HASH, 207402044, 3073):
        raise ValueError("B1 saved dataset hash, commit or selected candle count differs")

    def historical(name, dataset, series, catalog):
        summary = catalog["source_summary"]
        keys = tuple(sorted(summary.get("counts", ())))
        if not keys or len(keys) > 16 or not all(reads._matches(key, r"[a-f0-9]{64}") for key in keys):
            raise ValueError(f"saved dataset source binding incomplete: {name}")
        frozen = ((dataset, catalog["dataset_hash"])
                  if summary.get("record_selection") == "all_canonical_revisions.v1" else None)
        return _case(conn, name=name, series=series, start=catalog["range_start"], end=catalog["range_end"],
                     clock=catalog["created_at"], commit=catalog["max_commit_seq"], days=[day for day in days if day < cutoff],
                     source_keys=keys, frozen=frozen)

    h05_case = historical("h05_dense_history", H05, 713, h05)
    b1_case = historical("b1_canonical_witness", B1, 2361, b1)
    clock = conn.exec_driver_sql("SELECT transaction_timestamp()").scalar_one()
    hot_case = _case(conn, name="retained_hot", series=hot["series_id"], start=hot_start, end=hot_end,
                     clock=clock, commit=watermark, days=[day for day in days if day >= cutoff])
    boundary = {"restore_pair_hash": proof_sha256, "commit_seq": watermark, "retire_before": cutoff.isoformat(),
                "repeats": 5, "timeout_seconds": 120, "max_rows": 500_000, "max_archive_bytes": 512 * 1024**2}
    read_plan = reads.validate_plan(boundary | {"cases": [h05_case, b1_case, hot_case]})
    b1_replay = {"status": "historical_runtime_required", "producing_revision": B1_PRODUCING_REVISION,
        "dataset_id": B1, "dataset_hash": B1_HASH, "definition_hash": B1_DEFINITION_HASH, "request_hash": B1_REQUEST_HASH,
        "plan_hash": B1_PLAN_HASH, "result_hash": B1_RESULT_HASH, "authored_request": b1_request,
        "read_boundary": boundary | {"cases": [b1_case, hot_case]}}
    ingest_plan = ingest.validate_plan({"reads": boundary | {"cases": [b1_case, hot_case]}, "batches": 24,
        "batch_rows": 32, "interval_ms": 100, "observation_start": hot_start.isoformat()})
    return {"schema": VERSION, "reads": read_plan, "b1_historical_replay": b1_replay,
            "ingest": ingest_plan, "catalog": {H05: h05, B1: b1}}


def main():
    from sqlalchemy import create_engine
    from portal.backend.service.storage.repos.market_data import PostgresMarketDataRepository

    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("scope", "proof", "b1-authored", "b1-immutable-request", "b1-definition", "b1-plan"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    parser.add_argument("--proof-sha256", required=True)
    args = parser.parse_args()
    _, scope = _document(args.scope, limit=65536)
    proof_bytes, proof = _document(args.proof, limit=65536)
    authored, _ = _document(args.b1_authored, limit=65536)
    _, immutable_request = _document(args.b1_immutable_request, limit=65536)
    _, definition = _document(args.b1_definition, limit=65536)
    _, retained_plan = _document(args.b1_plan, limit=65536)
    b1_request = validate_b1_payload(authored, immutable_request, definition, retained_plan)
    if sha256(proof_bytes).hexdigest() != args.proof_sha256 or proof.get("schema") != "qt.benchmark_clone_boundary.v1":
        raise ValueError("verified private benchmark proof digest differs")
    url = reads._db_url(os.environ.get("PG_DSN"))
    endpoint = Path(url.query["host"]) / f".s.PGSQL.{url.query['port']}"
    if endpoint.stat().st_ino != proof["socket_inode"]:
        raise ValueError("verified private benchmark socket inode differs")
    engine = create_engine(url, connect_args={"connect_timeout": 5, "application_name": "qt_benchmark_workload_plan",
        "options": "-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=2000"})
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            identity, started, readonly = conn.exec_driver_sql("""
                SELECT system_identifier::text || '/' || (SELECT oid FROM pg_database WHERE datname=current_database()),
                       extract(epoch FROM pg_postmaster_start_time())::text,
                       current_setting('transaction_read_only')='on' AND inet_server_addr() IS NULL
                          AND current_setting('listen_addresses')='' AND NOT pg_is_in_recovery()
                FROM pg_control_system()
            """).one()
            watermark = proof["accepted_commit_seq"]
            if (identity, started, readonly) != (proof["database_identity"], proof["postmaster_epoch"], True):
                raise RuntimeError("verified private benchmark database or postmaster differs")
            if PostgresMarketDataRepository._current_commit_seq_with_session(conn) != watermark:
                raise RuntimeError("verified private benchmark accepted watermark differs")
            result = assemble(conn, scope=scope, proof_sha256=args.proof_sha256, watermark=watermark, b1_request=b1_request)
    finally:
        engine.dispose()
    print(json.dumps(result, default=lambda value: value.isoformat(), sort_keys=True))


if __name__ == "__main__":
    main()

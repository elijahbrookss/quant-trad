"""Compare native frozen research Check executions on paired offline QT restores."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from time import perf_counter_ns

if __package__:
    from . import canonical_cold_benchmark as reads
else:
    import canonical_cold_benchmark as reads


VERSION = "qt.canonical_cold_research_replay.v1"


def validate_plan(plan):
    if (not isinstance(plan, dict) or set(plan) != {"read_boundary", "checks"}
            or not isinstance(plan["checks"], list) or not 1 <= len(plan["checks"]) <= 8):
        raise ValueError("frozen research rehearsal requires a bounded read boundary and Check inventory")
    reads.validate_plan(plan["read_boundary"])
    if any(case["mode"] == "book_replay" for case in plan["read_boundary"]["cases"]):
        raise ValueError("frozen research witnesses must use canonical fact or frozen series reads")
    names = set()
    for check in plan["checks"]:
        if (not isinstance(check, dict) or set(check) != {"name", "dataset_hash", "witness", "request"}
                or not reads._identity(check["name"]) or check["name"] in names
                or not reads._matches(check["dataset_hash"], r"[0-9a-f]{64}")
                or not isinstance(check["request"], dict)):
            raise ValueError("frozen research case requires a unique name, exact dataset hash and request")
        request = check["request"]
        if (request.get("mode") != "evidence" or not reads._identity(request.get("dataset_id"))
                or request.get("freeze") not in (None, False) or request.get("acquire_missing") not in (None, False)
                or request.get("preparation") or request.get("immutable_run_evidence")):
            raise ValueError("frozen research rehearsal only accepts an existing Dataset and evidence request")
        witness = next((case for case in plan["read_boundary"]["cases"] if case["name"] == check["witness"]), None)
        if witness is None or (witness.get("dataset_id") is not None and
                (witness["dataset_id"], witness["dataset_hash"]) != (request["dataset_id"], check["dataset_hash"])):
            raise ValueError("frozen research case needs a matching canonical fact witness")
        names.add(check["name"])
    return plan


def _semantics(built, *, revision, case, boundary):
    from research_science.check import CheckDefinition, CheckEvidenceBinding, CheckRequest, CheckResult, ResolvedCheckPlan

    definition = CheckDefinition.from_dict(built["definition"])
    request = CheckRequest.from_dict(built["request"])
    plan = ResolvedCheckPlan.from_dict(built["plan"])
    evidence = CheckEvidenceBinding.from_dict(built["evidence"])
    result = CheckResult.from_dict(built["result"])
    witness = next(item for item in boundary["cases"] if item["name"] == case["witness"])
    series = [row for row in evidence.input_binding["series"] if row["series_id"] == witness["series_id"]]
    expected_hashes = definition.definition_hash, request.request_hash, plan.plan_hash
    if (tuple(getattr(evidence, f"{name}_hash") for name in ("definition", "request", "plan")) != expected_hashes
            or tuple(getattr(result, f"{name}_hash") for name in ("definition", "request", "plan")) != expected_hashes
            or result.evidence_hash != evidence.evidence_hash or evidence.code_revision != revision
            or result.evaluator_id != definition.evaluator_id or result.evaluator_version != definition.evaluator_version
            or evidence.evidence_kind != "frozen_market_data" or evidence.input_binding["provider_access"] != "disabled"
            or request.dataset_id != case["request"]["dataset_id"]
            or evidence.input_binding["dataset_id"] != request.dataset_id
            or evidence.input_binding["dataset_hash"] != case["dataset_hash"]
            or not any(reads._timestamp(row["range_start"]) <= reads._timestamp(witness["start"])
                       < reads._timestamp(witness["end"]) <= reads._timestamp(row["range_end"])
                       and (witness.get("dataset_id") or witness.get("as_of_commit_seq") == row["max_commit_seq"])
                       for row in series)
            or not 0 < evidence.input_binding["max_commit_seq"] <= boundary["commit_seq"]
            or result.result.get("status") != "completed"):
        raise RuntimeError(f"frozen research case did not produce a complete bound provider-free result: {case['name']}")
    return {"definition": definition.to_dict(), "request": request.to_dict(), "plan": plan.to_dict(),
            "evidence": {key: value for key, value in evidence.to_dict().items()
                         if key not in {"code_revision", "evidence_hash"}},
            "result": {key: value for key, value in result.to_dict().items() if key not in {"evidence_hash", "result_hash"}}}


def execute_cases(plan, *, revision, build):
    from core.execution_control import ExecutionControl, controlled_execution

    boundary = plan["read_boundary"]
    cases = {}
    for case in plan["checks"]:
        control = ExecutionControl()
        control.limit(seconds=boundary["timeout_seconds"], input_rows=boundary["max_rows"],
                      archive_read_bytes=boundary["max_archive_bytes"])
        start = perf_counter_ns()
        with controlled_execution(control):
            built = build(case["request"])
        elapsed = perf_counter_ns() - start
        semantic = _semantics(built, revision=revision, case=case, boundary=boundary)
        cases[case["name"]] = {"elapsed_ns": elapsed, "native_sha256": reads._digest(built),
                               "semantic_sha256": reads._digest(semantic), "native": built,
                               "execution_metrics": control.snapshot()}
    return cases


def run(args, plan):
    os.environ["QT_DISABLE_DOTENV"] = "1"
    from core.settings import get_settings
    from core.storage_mounts import configured_archive_root, require_configured_archive_mount
    from portal.backend.db.session import db
    from portal.backend.service.market import runtime_market_data
    from portal.backend.service.provenance import evidence_source_revision
    from portal.backend.service.research.service import build_research_check_evidence
    from portal.backend.service.storage.repos import market_data
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    if (not reads._matches(args.revision, r"[0-9a-f]{40}")
            or not reads._matches(args.runtime_digest, r"sha256:[0-9a-f]{64}")):
        raise ValueError("frozen research rehearsal requires the application Git SHA and runtime digest")
    url = reads._db_url(os.environ.get("PG_DSN"))
    plan = validate_plan(plan)
    revision = evidence_source_revision()
    if revision != args.revision or db._engine is not None or get_settings().storage.history_cache_bytes:
        raise RuntimeError("frozen research requires the exact clean application and an uninitialized read-only pool")
    archive_root = configured_archive_root()
    if not archive_root.is_absolute() or archive_root != archive_root.resolve() or archive_root != args.archive_root.resolve():
        raise RuntimeError("frozen research requires the explicit restored archive mount")
    require_configured_archive_mount(archive_root, require_writable=False)
    if not (archive_root / "objects").is_dir():
        raise RuntimeError("frozen research requires the restored archive objects directory")
    boundary = plan["read_boundary"]
    timeout = boundary["timeout_seconds"] * 1000
    engine = create_engine(url, connect_args={"application_name": "qt_offline_frozen_research", "connect_timeout": 5,
        "options": f"-c default_transaction_read_only=on -c statement_timeout={timeout} -c lock_timeout={timeout}"})
    provider_service = runtime_market_data.MarketDataCollectorService

    class ProviderBlocked:
        def __init__(self, **_kwargs):
            pass

        def __getattr__(self, name):
            raise RuntimeError(f"frozen research rehearsal attempted provider operation: {name}")

    try:
        identity = reads._database_evidence(engine)
        if ((identity["system_identifier"], identity["oid"], identity["layout"])
                != (args.system_identifier, args.database_oid, reads.LAYOUTS[args.side])
                or args.side == "candidate" and identity["cold_before_day"] != boundary["retire_before"]):
            raise RuntimeError("frozen research restore identity or storage authority differs")
        reads._verify_witnesses(engine, boundary, args.side)
        db._engine = engine
        db._session_factory = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
        db._available = True
        runtime_market_data.MarketDataCollectorService = ProviderBlocked
        repo = market_data.market_data_repo
        if repo.current_commit_seq() != boundary["commit_seq"]:
            raise RuntimeError("frozen research restored commit boundary differs")
        witnesses = {}
        from core.execution_control import ExecutionControl, controlled_execution

        for case in boundary["cases"]:
            operation, summarize, scope = reads._operation(case, boundary, repo, None, archive_root)
            control = ExecutionControl()
            control.limit(seconds=boundary["timeout_seconds"], input_rows=boundary["max_rows"],
                          archive_read_bytes=boundary["max_archive_bytes"])
            with controlled_execution(control):
                witnesses[case["name"]] = summarize(operation()) | {"scope": scope}
        cases = execute_cases(plan, revision=revision, build=build_research_check_evidence)
        if reads._database_evidence(engine) != identity or repo.current_commit_seq() != boundary["commit_seq"]:
            raise RuntimeError("frozen research restore changed during execution")
    finally:
        runtime_market_data.MarketDataCollectorService = provider_service
        if db._engine is engine:
            db._engine, db._session_factory, db._available = None, None, False
        engine.dispose()
    return {"version": VERSION, "side": args.side, "plan": plan, "plan_sha256": reads._digest(plan), "database": identity,
            "application_revision": revision, "runtime_digest_declared": args.runtime_digest, "checks": cases,
            "input_witnesses": witnesses}


def compare(source, candidate):
    for side, trial in (("source", source), ("candidate", candidate)):
        plan = validate_plan(trial["plan"])
        if (trial["version"] != VERSION or trial["side"] != side or trial["plan_sha256"] != reads._digest(plan)
                or trial["database"]["layout"] != reads.LAYOUTS[side]
                or (side == "candidate" and trial["database"]["cold_before_day"] != plan["read_boundary"]["retire_before"])
                or set(trial["checks"]) != {case["name"] for case in plan["checks"]}
                or set(trial["input_witnesses"]) != {case["name"] for case in plan["read_boundary"]["cases"]}):
            raise RuntimeError("frozen research trial scope or storage authority differs")
        for case in plan["checks"]:
            evidence = trial["checks"][case["name"]]
            native = evidence["native"]
            if (evidence["native_sha256"] != reads._digest(native) or evidence["semantic_sha256"] != reads._digest(
                    _semantics(native, revision=trial["application_revision"], case=case, boundary=plan["read_boundary"]))):
                raise RuntimeError(f"frozen research receipt hashes differ: {case['name']}")
    if source["plan_sha256"] != candidate["plan_sha256"] or source["input_witnesses"] != candidate["input_witnesses"]:
        raise RuntimeError("frozen research paired input boundary or exact selected revisions differ")
    results = {}
    for name, left in source["checks"].items():
        right = candidate["checks"][name]
        if left["semantic_sha256"] != right["semantic_sha256"]:
            raise RuntimeError(f"frozen research paired ordered Check output or input evidence differs: {name}")
        results[name] = {"semantic_sha256": left["semantic_sha256"], "source_native_sha256": left["native_sha256"],
                         "candidate_native_sha256": right["native_sha256"]}
    return {"version": VERSION, "plan_sha256": source["plan_sha256"], "checks": results,
            "source_sha256": reads._digest(source), "candidate_sha256": reads._digest(candidate)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    trial = commands.add_parser("run")
    trial.add_argument("--side", choices=reads.LAYOUTS, required=True)
    trial.add_argument("--plan", type=Path, required=True)
    trial.add_argument("--revision", required=True)
    trial.add_argument("--runtime-digest", required=True)
    trial.add_argument("--system-identifier", required=True)
    trial.add_argument("--database-oid", type=int, required=True)
    trial.add_argument("--archive-root", type=Path, required=True)
    pair = commands.add_parser("compare")
    pair.add_argument("--source", type=Path, required=True)
    pair.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    result = (run(args, json.loads(args.plan.read_text())) if args.command == "run"
              else compare(json.loads(args.source.read_text()), json.loads(args.candidate.read_text())))
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

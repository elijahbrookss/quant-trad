"""Reconcile selected held archive objects on independently restored QT copies."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date
from itertools import zip_longest
import json
import os
from pathlib import Path
from time import monotonic

# A direct path invocation can import the operator tooling beside this file even
# when PYTHONPATH deliberately selects the deployed checkout's application.
if __package__:
    from .canonical_cold_benchmark import LAYOUTS, _code_revision, _db_url, _identity, _matches
else:
    from canonical_cold_benchmark import LAYOUTS, _code_revision, _db_url, _identity, _matches


VERSION = "qt.canonical_cold_custody_audit.v1"
HOLD_QUERIES = {
    "explicit": """SELECT * FROM market.archive_retention_pin_versions
        WHERE target_kind=:kind AND target_id=:id ORDER BY pin_id COLLATE "C",revision""",
    "dataset": """SELECT * FROM market.dataset_archive_refs WHERE :kind='raw_manifest'
        AND raw_archive_manifest_id=:id ORDER BY dataset_id COLLATE "C"
    """,
    "published": """SELECT holds.*, active.preparation_id, active.generation, active.catalog_commitment
        FROM market.fact_archive_dependencies AS holds LEFT JOIN market.fact_archive_active_generations AS active
          ON active.manifest_id=holds.manifest_id WHERE holds.target_kind=:kind AND holds.target_id=:id
        ORDER BY holds.manifest_id COLLATE "C"
    """,
    "prepared": """SELECT holds.*, preparation.manifest_id, preparation.state, preparation.generation,
        preparation.catalog_commitment FROM market.fact_archive_preparation_dependencies AS holds
        JOIN market.fact_archive_preparations AS preparation ON preparation.id=holds.preparation_id
        WHERE holds.target_kind=:kind AND holds.target_id=:id ORDER BY holds.preparation_id COLLATE "C"
    """,
    "prefix": """SELECT * FROM market.fact_book_prefix_dependencies WHERE :kind='raw_manifest'
        AND target_id=:id ORDER BY chunk_id COLLATE "C"
    """,
    "compaction_sources": """SELECT * FROM market.raw_archive_compaction_sources WHERE :kind='raw_manifest'
        AND replacement_manifest_id=:id ORDER BY source_ordinal""",
    "compaction_replacements": """SELECT * FROM market.raw_archive_compaction_sources WHERE :kind='raw_manifest'
        AND source_manifest_id=:id ORDER BY replacement_manifest_id COLLATE "C"
    """,
}


def _validate_plan(plan):
    if (set(plan) != {"restore_pair_hash", "commit_seq", "retire_before", "targets"}
            or not _matches(plan["restore_pair_hash"], r"[0-9a-f]{64}")
            or type(plan["commit_seq"]) is not int or not 0 < plan["commit_seq"] <= 2**63 - 1
            or not _matches(plan["retire_before"], r"\d{4}-\d{2}-\d{2}")
            or not isinstance(plan["targets"], list) or not 1 <= len(plan["targets"]) <= 32):
        raise ValueError("custody audit requires a paired restore boundary and bounded target inventory")
    date.fromisoformat(plan["retire_before"])
    identities = set()
    witnessed_holds = set()
    for target in plan["targets"]:
        if (not isinstance(target, dict) or set(target) != {"kind", "id", "required_holds"} or not _identity(target["id"])
                or target["kind"] not in {"raw_manifest", "book_checkpoint"}
                or not isinstance(target["required_holds"], list)
                or not set(target["required_holds"]) <= {"explicit", "dataset", "published", "prepared", "prefix"}):
            raise ValueError("custody audit target or required hold witness is invalid")
        identity = target["kind"], target["id"]
        if identity in identities:
            raise ValueError("custody audit repeats a target")
        identities.add(identity)
        witnessed_holds.update(target["required_holds"])
    if not any(kind == "raw_manifest" for kind, _ in identities) or not witnessed_holds:
        raise ValueError("custody audit plan needs raw ordinals and at least one exact retention hold")
    return plan


def _fold(rows, *, name, limit, check, target=None):
    from market_data.fact_archive import archive_evidence_hash

    digest, count = archive_evidence_hash({"schema": VERSION, "name": name}), 0
    for row in rows:
        check()
        count += 1
        if count > limit:
            raise RuntimeError(f"custody audit row budget exceeded: {name}")
        if target is not None and (row.get("object_key", target["object_key"]) != target["object_key"]
                or row.get("object_sha256", target["object_sha256"]) != target["object_sha256"]
                or row.get("content_fingerprint", target["content_fingerprint"]) != target["content_fingerprint"]):
            raise RuntimeError(f"custody audit hold points to different object evidence: {name}")
        digest = archive_evidence_hash({"prior": digest, "row": dict(row)})
    return {"count": count, "ordered_sha256": digest}


def _raw_mappings(session, *, manifest, path, limits, check, observe=None):
    from market_data.archive import iter_raw_archive_parquet, read_raw_archive_content_fingerprint
    from sqlalchemy import text

    identity = manifest["id"]
    count = manifest["record_count"]
    signature = (manifest["definition_id"], manifest["session_id"], manifest["connection_epoch"])
    result = session.execute(text("""
        SELECT raw_record_id, spool_segment_id, session_id, connection_epoch, receive_ordinal,
               object_row_index, object_row_group, raw_frame_sha256, mapped_at, known_at
        FROM market.raw_archive_record_mappings WHERE manifest_id=:id
        ORDER BY object_row_index LIMIT :limit
    """).execution_options(yield_per=128), {"id": identity, "limit": count + 1})
    records = iter_raw_archive_parquet(path, limits=limits)
    seen = 0

    def exact_rows():
        nonlocal seen
        for i, (row, record) in enumerate(zip_longest(result.mappings(), records)):
            check()
            if row is None or record is None:
                raise RuntimeError(f"custody audit raw mapping count mismatch: {identity}")
            if ((record.definition_id, record.session_id, record.connection_epoch) != signature
                    or row["object_row_index"] != i or row["object_row_group"] != 0
                    or any(row[name] != getattr(record, name) for name in (
                        "raw_record_id", "spool_segment_id", "session_id", "connection_epoch",
                        "receive_ordinal", "raw_frame_sha256",
                    )) or (i == 0 and record.receive_ordinal != manifest["first_receive_ordinal"])
                    or (i == count - 1 and record.receive_ordinal != manifest["last_receive_ordinal"])):
                raise RuntimeError(f"custody audit raw ordinal mismatch: {identity} row={i}")
            seen += 1
            if observe is not None:
                observe(record)
            yield row

    try:
        receipt = _fold(exact_rows(), name="raw_ordinals", limit=count, check=check)
    finally:
        result.close()
        records.close()
    if seen != count or read_raw_archive_content_fingerprint(path, limits=limits, check_budget=check) != manifest["content_fingerprint"]:
        raise RuntimeError(f"custody audit raw fingerprint or count mismatch: {identity}")
    return receipt


def _target(session, *, target, repository, objects, limits, max_hold_rows, check):
    from market_data.fact_archive import archive_evidence_hash
    from sqlalchemy import text

    params = {"kind": target["kind"], "id": target["id"], "limit": max_hold_rows + 1}
    table = {"raw_manifest": "raw_archive_manifests", "book_checkpoint": "book_checkpoint_manifests"}[target["kind"]]
    manifest = dict(session.execute(text(f"SELECT * FROM market.{table} WHERE id=:id"), params).mappings().one())
    if target["kind"] == "raw_manifest" and not 0 < manifest["record_count"] <= limits.max_rows:
        raise RuntimeError(f"custody audit raw row budget exceeded: {target['id']}")
    holds = {}
    for name, statement in HOLD_QUERIES.items():
        with session.execute(text(statement + " LIMIT :limit").execution_options(yield_per=128), params) as result:
            holds[name] = _fold(result.mappings(), name=name, limit=max_hold_rows, check=check, target=manifest)
    if any(not holds[name]["count"] for name in target["required_holds"]):
        raise RuntimeError(f"custody audit required hold witness missing: {target['id']}")
    status = dict(repository.archive_retention_status(target_kind=target["kind"], target_id=target["id"]))
    hot_backlog = status.pop("canonical_backlog_present")
    if (status["expired"] or (target["required_holds"] and not status["pinned"])
            or ("explicit" in target["required_holds"] and not status["explicit_active_pins"])
            or ("dataset" in target["required_holds"] and not status["dataset_pin_count"])
            or (set(target["required_holds"]) & {"published", "prefix"} and not status["canonical_dependency_count"])):
        raise RuntimeError(f"custody audit required object expired or lacks expected protection: {target['id']}")
    evidence = objects.verify(manifest["object_key"], manifest["object_sha256"], expected_bytes=manifest["byte_count"])
    ordinal = None
    if target["kind"] == "raw_manifest":
        ordinal = _raw_mappings(session, manifest=manifest, path=objects.object_store.local_path(evidence.object_key),
                                limits=limits, check=check)
    return {"kind": target["kind"], "id": target["id"], "manifest_sha256": archive_evidence_hash(manifest),
            "verified_file_sha256": evidence.object_sha256, "verified_file_bytes": evidence.byte_count,
            "raw_ordinals": ordinal, "holds": holds, "effective_status_sha256": archive_evidence_hash(status),
            "hot_backlog_observed": hot_backlog}


@contextmanager
def _owner_session(owner, snapshot):
    # The public custody API normally opens its own transaction. Reuse this
    # audit's leased snapshot for every catalog, owner and physical-byte check.
    from types import SimpleNamespace

    previous = owner.db
    owner.db = SimpleNamespace(session=lambda: _borrow_snapshot(snapshot))
    try:
        yield owner.PostgresMarketStructureRepository()
    finally:
        owner.db = previous


@contextmanager
def _borrow_snapshot(snapshot):
    yield snapshot


def run(args, plan):
    os.environ["QT_DISABLE_DOTENV"] = "1"
    from market_data.archive import FilesystemRawArchiveObjectStore, RawArchiveReadLimits
    from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
    from market_data.fact_archive import archive_evidence_hash
    from portal.backend.db.session import Database
    from portal.backend.service.storage.repos import market_data, market_structure
    from portal.backend.service.storage.repos.market_lifecycle import market_storage_lifecycle_repository
    from sqlalchemy import create_engine, text

    url = _db_url(os.environ.get("PG_DSN"))
    plan = _validate_plan(plan)
    revision = _code_revision(market_data, args.revision)
    from core.storage_mounts import configured_archive_root, require_configured_archive_mount

    root = configured_archive_root()
    if not root.is_absolute() or root != root.resolve() or root != args.archive_root.resolve():
        raise RuntimeError("custody audit requires the explicit restored archive root")
    require_configured_archive_mount(root, require_writable=False)
    store = FilesystemRawArchiveObjectStore(root / "objects", writable=False)
    stop = monotonic() + args.max_seconds

    def check():
        if monotonic() >= stop:
            raise TimeoutError("custody audit elapsed budget exceeded")

    raw_limits = RawArchiveReadLimits(max_rows=args.max_raw_rows, max_file_bytes=args.max_archive_bytes)
    objects = ArchiveVerificationBatch(store, limits=ArchiveVerificationLimits(
        max_objects=len(plan["targets"]), max_bytes=args.max_archive_bytes), check_budget=check)
    database = Database()
    timeout = args.max_seconds * 1000
    engine = create_engine(url, connect_args={"application_name": "qt_offline_custody_audit", "connect_timeout": 5,
        "options": f"-c default_transaction_read_only=on -c statement_timeout={timeout} -c lock_timeout={timeout}"})
    database._engine = engine
    database._available = True
    try:
        with market_storage_lifecycle_repository.dataset_snapshot_session(database=database) as session:
            identity = dict(session.execute(text("""
                SELECT system_identifier::text,current_database() AS name,
                       (SELECT oid FROM pg_database WHERE datname=current_database()) AS oid,
                       current_setting('transaction_read_only')='on' AND inet_server_addr() IS NULL
                           AND current_setting('listen_addresses')='' AS isolated
                FROM pg_catalog.pg_control_system()
            """)).mappings().one())
            layout, cutoff = session.execute(text("""
                SELECT layout_version, evidence->>'cold_before_day' FROM market.fact_storage_state
                WHERE state='ready' AND completed_at IS NOT NULL
            """)).one()
            if (not identity["isolated"] or (identity["system_identifier"], identity["oid"], layout)
                    != (args.system_identifier, args.database_oid, LAYOUTS[args.side])
                    or (args.side == "candidate" and cutoff != plan["retire_before"])
                    or market_data.PostgresMarketDataRepository._current_commit_seq_with_session(session) != plan["commit_seq"]):
                raise RuntimeError("custody audit restore identity, layout, or commit boundary differs")
            with _owner_session(market_structure, session) as owner, objects.distinct_references():
                outcomes = [_target(session, target=target, repository=owner, objects=objects,
                    limits=raw_limits, max_hold_rows=args.max_hold_rows, check=check) for target in plan["targets"]]
    finally:
        engine.dispose()
    return {"version": VERSION, "side": args.side, "database": identity | {"layout": layout, "cutoff": cutoff},
            "archive_root": str(root), "application_revision": revision, "plan": plan,
            "budgets": {name: getattr(args, name) for name in ("max_raw_rows", "max_hold_rows", "max_archive_bytes", "max_seconds")},
            "plan_sha256": archive_evidence_hash(plan), "outcomes": outcomes}


def _complete_outcome(outcome, target):
    fields = {"kind", "id", "manifest_sha256", "verified_file_sha256", "verified_file_bytes", "raw_ordinals",
              "holds", "effective_status_sha256", "hot_backlog_observed"}
    if (set(outcome) != fields or type(outcome["verified_file_bytes"]) is not int or outcome["verified_file_bytes"] <= 0
            or type(outcome["hot_backlog_observed"]) is not bool
            or any(not _matches(outcome[key], r"[0-9a-f]{64}") for key in (
                "manifest_sha256", "verified_file_sha256", "effective_status_sha256"))
            or set(outcome["holds"]) != set(HOLD_QUERIES)):
        return False
    counts = dict(outcome["holds"])
    if target["kind"] == "raw_manifest":
        counts["raw_ordinals"] = outcome["raw_ordinals"]
    elif outcome["raw_ordinals"] is not None:
        return False
    required = set(target["required_holds"]) | ({"raw_ordinals"} if target["kind"] == "raw_manifest" else set())
    return all(isinstance(summary, dict) and set(summary) == {"count", "ordered_sha256"}
               and type(summary["count"]) is int and summary["count"] >= int(name in required)
               and _matches(summary["ordered_sha256"], r"[0-9a-f]{64}") for name, summary in counts.items())


def compare(source, candidate):
    from market_data.fact_archive import archive_evidence_hash

    for side, report in (("source", source), ("candidate", candidate)):
        plan = _validate_plan(report["plan"])
        expected = [(row["kind"], row["id"]) for row in plan["targets"]]
        actual = [(row["kind"], row["id"]) for row in report["outcomes"]]
        if (report["version"] != VERSION or report["side"] != side or report["database"]["layout"] != LAYOUTS[side]
                or (side == "candidate" and report["database"]["cutoff"] != plan["retire_before"])
                or report["plan_sha256"] != archive_evidence_hash(plan) or expected != actual
                or not all(_complete_outcome(outcome, target) for outcome, target
                           in zip(report["outcomes"], plan["targets"], strict=True))):
            raise RuntimeError("custody audit incomplete or mismatched receipt")
    if source["plan_sha256"] != candidate["plan_sha256"] or source["budgets"] != candidate["budgets"]:
        raise RuntimeError("custody audit receipts have different restore boundaries or target inventories")
    outcomes = []
    for left, right in zip(source["outcomes"], candidate["outcomes"], strict=True):
        evidence = {key: value for key, value in left.items() if key != "hot_backlog_observed"}
        if evidence != {key: value for key, value in right.items() if key != "hot_backlog_observed"}:
            raise RuntimeError(f"custody audit ordered archive evidence differs: {left['kind']}/{left['id']}")
        outcomes.append(evidence | {"source_hot_backlog": left["hot_backlog_observed"],
                                    "candidate_hot_backlog": right["hot_backlog_observed"]})
    return {"version": VERSION, "plan_sha256": source["plan_sha256"], "outcomes": outcomes,
            "source_sha256": archive_evidence_hash(source), "candidate_sha256": archive_evidence_hash(candidate)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    trial = subcommands.add_parser("run")
    trial.add_argument("--side", choices=LAYOUTS, required=True)
    trial.add_argument("--plan", type=Path, required=True)
    trial.add_argument("--revision", required=True)
    trial.add_argument("--system-identifier", required=True)
    trial.add_argument("--database-oid", type=int, required=True)
    trial.add_argument("--archive-root", type=Path, required=True)
    trial.add_argument("--max-seconds", type=int, default=60, choices=range(1, 601), metavar="1..600")
    trial.add_argument("--max-raw-rows", type=int, default=50000, choices=range(1, 500001), metavar="1..500000")
    trial.add_argument("--max-hold-rows", type=int, default=10000, choices=range(1, 100001), metavar="1..100000")
    trial.add_argument("--max-archive-bytes", type=int, default=268435456)
    pair = subcommands.add_parser("compare")
    pair.add_argument("--source", type=Path, required=True)
    pair.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "run":
        if (not _matches(args.revision, r"[0-9a-f]{40}") or not 0 < args.max_archive_bytes <= 2**30):
            parser.error("provide a full Git SHA and a total physical archive budget of at most 1 GiB")
        result = run(args, json.loads(args.plan.read_text()))
    else:
        result = compare(json.loads(args.source.read_text()), json.loads(args.candidate.read_text()))
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

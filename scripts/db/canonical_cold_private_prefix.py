"""Stage, load and audit a reviewed inclusive prefix on a private QT clone.

Run plan first, then pin its prefix_sha256 for every writing or auditing pass.
Passes share the same private HDD work directory and native SQLite checkpoint.
Stage uses the seeded candidate archive in place. Its mount is writable for
stage and read-only for load and audit. Each command has a finite batch limit.
"""

import argparse
from dataclasses import asdict
from datetime import date, datetime, timedelta
from functools import partial
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import socket
import stat
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_mounts import configured_archive_root
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import FactArchiveLimits, archive_evidence_hash
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from portal.backend.service.storage.history_policy import _read as read_saved_history_policy
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.db.canonical_cold_audit import _ensure_target, _plan, audit_candidate
from scripts.db.canonical_cold_load import TargetDatabase, _load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from scripts.db.canonical_cold_private_activate import _pinned_bytes
from scripts.db.canonical_cold_private_load import SharedHddLoadLimits, shared_hdd_floor, supervise_private_page
from scripts.db.canonical_cold_private_placement import _clone_root, _mount_backing, assert_private_placement, load_private_placement
from scripts.db.canonical_cold_private_seal import _matches_inode, load_private_clone, seal_one
from scripts.db.canonical_cold_receipts import validate_audit_receipts


SCHEMA = "qt.private_canonical_cold_prefix.v1"
_SEALED = {"sealed", "verified", "reclaimed"}


def _json(item):
    return json.dumps(item, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


class PrefixJournal:
    def __init__(self, checkpoint, scope):
        self.checkpoint = checkpoint
        with checkpoint.connection() as db:
            db.execute("CREATE TABLE IF NOT EXISTS private_prefix_scope (id INTEGER PRIMARY KEY, scope TEXT NOT NULL)")
            db.execute("INSERT OR IGNORE INTO private_prefix_scope VALUES (1, ?)", (_json(scope),))
            if db.execute("SELECT scope FROM private_prefix_scope WHERE id=1").fetchone()[0] != _json(scope):
                raise RuntimeError("private_prefix_journal_scope_changed")
            db.execute("""CREATE TABLE IF NOT EXISTS private_prefix_events (
                seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, day TEXT NOT NULL, done INTEGER NOT NULL,
                digest TEXT NOT NULL, item TEXT NOT NULL)
            """)
            db.execute("CREATE INDEX IF NOT EXISTS private_prefix_event_cursor ON private_prefix_events(kind,day,seq)")

    @staticmethod
    def _item(digest, encoded):
        if sha256(encoded.encode()).hexdigest() != digest:
            raise RuntimeError("private_prefix_journal_evidence_changed")
        return json.loads(encoded)

    def latest(self, kind, day=""):
        with self.checkpoint.read_connection() as db:
            row = db.execute("""SELECT done,digest,item FROM private_prefix_events WHERE kind=? AND day=?
                ORDER BY seq DESC LIMIT 1""", (kind, day)).fetchone()
        return (False, None) if row is None else (bool(row[0]), self._item(*row[1:]))

    def record(self, kind, item, *, done, day=""):
        encoded = _json(item)
        with self.checkpoint.connection() as db:
            db.execute("INSERT INTO private_prefix_events(kind,day,done,digest,item) VALUES(?,?,?,?,?)",
                       (kind, day, int(done), sha256(encoded.encode()).hexdigest(), encoded))

    def items(self, *kinds):
        with self.checkpoint.read_connection() as db:
            for row in db.execute(f"SELECT digest,item FROM private_prefix_events WHERE kind IN ({','.join('?' * len(kinds))}) ORDER BY seq", kinds):
                yield self._item(*row)


def _work_root(path, *, clone, archive):
    _, (device, inode) = _private_root(path)
    expected = clone.proof["device_id"]
    if f"{os.major(device)}:{os.minor(device)}" != expected or os.statvfs(path).f_flag & os.ST_RDONLY:
        raise ValueError("private_prefix_work_requires_writable_shared_hdd")
    root = _clone_root(clone)
    for other in (root, archive):
        if path.is_relative_to(other) or other.is_relative_to(path):
            raise ValueError("private_prefix_work_overlaps_clone_or_archive")
        _mount_backing(other, path, expected)
    checkpoint = path / "migration.sqlite"
    for name in (checkpoint.name, checkpoint.name + "-journal", checkpoint.name + "-wal", checkpoint.name + "-shm"):
        candidate = path / name
        if os.path.lexists(candidate):
            info = candidate.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_dev != device:
                raise ValueError("private_prefix_unsafe_checkpoint_file")
    return checkpoint, inode


def _native_plan(source, target, *, clone, placement, cutoff, max_days):
    with source.snapshot() as conn:
        assert_private_placement(conn, clone=clone, placement=placement, require_writable=True)
        source_identity = _ensure_target(conn, source, target)
        prefix = _plan(conn, cutoff=cutoff, max_days=max_days)
        if not prefix["days"]:
            raise RuntimeError("private_prefix_empty_scope")
        first = date.fromisoformat(prefix["first_day"])
        rows = conn.execute(text("""SELECT storage_day,state,sealed_at,expected_rows,source_bytes,
                    to_regclass('market.fact_hot_payloads_' || to_char(storage_day,'YYYYMMDD'))::oid::bigint AS payload_oid
            FROM market.fact_retention_partitions WHERE storage_day>=:first AND storage_day<:cutoff
            ORDER BY storage_day LIMIT :limit"""), {"first": first, "cutoff": cutoff, "limit": max_days + 1}).mappings().all()
        retirement = {row["storage_day"].isoformat(): {key: value.isoformat() if hasattr(value, "isoformat") else value
                      for key, value in row.items() if key != "storage_day"} for row in rows}
        reservations = conn.scalar(text("""SELECT reserved_bytes + auxiliary_reserved_bytes
            FROM public.portal_storage_targets WHERE id=:id"""), {"id": placement["history_target_id"]})
    return source_identity, prefix, retirement, reservations


def _free(path, *, minimum, allowance=0):
    stats = os.statvfs(path)
    available = stats.f_bavail * stats.f_frsize
    shared_hdd_floor(available, minimum=minimum, allowance=allowance)
    return available


def _seal(args, *, clone, engine, journal, plan, retirement, remaining):
    if (args.seal_day is None or args.header_oid is None or args.payload_oid is None
            or args.source_archive_object_root is None or args.count_statement_timeout_ms is None):
        raise ValueError("private_prefix_seal_requires_reviewed_day_children_and_readonly_source_archive")
    day = args.seal_day.isoformat()
    binding = next((row for row in plan["days"] if row["day"] == day), None)
    marker = retirement.get(day)
    if (binding is None or binding["child"] != fact_header_partition_name(args.seal_day)
            or binding["child_oid"] != args.header_oid or marker is None or marker["payload_oid"] != args.payload_oid):
        raise RuntimeError("private_prefix_seal_scope_changed")
    if args.payload_oid <= 0 or not 1 <= args.count_statement_timeout_ms <= 60_000:
        raise ValueError("private_prefix_seal_invalid_oid_or_timeout")
    original_objects = args.source_archive_object_root
    if (original_objects.resolve(strict=True) != original_objects
            or not os.statvfs(original_objects).f_flag & os.ST_RDONLY
            or not _matches_inode(original_objects, clone.proof, "archive_objects_inode", expected_type=stat.S_ISDIR)):
        raise ValueError("private_prefix_seal_original_archive_changed")
    intent = {"proof_sha256": args.proof_sha256, "database": clone.url.database, "storage_day": day,
              "database_identity": clone.proof["database_identity"], "accepted_commit_seq": clone.proof["accepted_commit_seq"],
              "header_oid": args.header_oid, "payload_oid": args.payload_oid, "previous_state": "open"}
    _, previous_intent = journal.latest("seal_intent", day)
    _, previous = journal.latest("seal", day)
    with engine.begin() as conn:
        _, policy = read_saved_history_policy(conn)
    recorded_intent = {"scope": intent, "policy": policy, "source_objects": str(original_objects)}
    if previous_intent is not None and previous_intent != recorded_intent:
        raise RuntimeError("private_prefix_seal_reviewed_intent_changed")
    if previous_intent is None:
        if marker["state"] != "open" or marker["sealed_at"] is not None or marker["expected_rows"] is not None:
            raise RuntimeError("private_prefix_seal_original_open_state_not_recorded")
        journal.record("seal_intent", recorded_intent, day=day, done=True)
        _sync(args.work_root)
    if marker["state"] == "open":
        if previous is not None:
            raise RuntimeError("private_prefix_seal_persisted_state_reverted")
        receipt = seal_one(clone, proof_sha256=args.proof_sha256, day=args.seal_day, expected_state="open",
            header_oid=args.header_oid, payload_oid=args.payload_oid, archive_object_root=args.source_archive_object_root,
            max_seconds=min(3600, remaining()), count_statement_timeout_ms=args.count_statement_timeout_ms)
    elif marker["state"] == "sealed" and marker["sealed_at"] is not None and marker["expected_rows"] is not None:
        # A disconnect can lose the result after the exact private marker committed.
        # Keep the same public receipt shape and recover its fields from native SQL.
        native = {**intent, "state": "sealed", "sealed_at": marker["sealed_at"], "expected_rows": marker["expected_rows"],
                  "source_payload_bytes": marker["source_bytes"], "schema": "qt.private_rehearsal_cohort_seal.v1"}
        if previous is not None:
            if (any(previous[key] != value for key, value in native.items() if key != "sealed_at")
                    or datetime.fromisoformat(previous["sealed_at"]) != datetime.fromisoformat(native["sealed_at"])):
                raise RuntimeError("private_prefix_seal_marker_changed")
            return previous
        receipt = native | {"policy": policy}
    else:
        raise RuntimeError("private_prefix_seal_native_state_changed")
    journal.record("seal", receipt, day=day, done=True)
    _sync(args.work_root)
    return receipt


def _export(path, items, *, max_bytes, remaining):
    digest = sha256()
    count = total = 0
    temporary = path.with_name(f".{path.name}.pending")
    if os.path.lexists(temporary):
        info = temporary.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_dev != path.parent.stat().st_dev:
            raise ValueError("private_prefix_export_pending_path_unsafe")
        if info.st_nlink != 1 and (not path.is_file() or path.is_symlink() or not temporary.samefile(path)):
            raise ValueError("private_prefix_export_pending_path_unsafe")
        temporary.unlink()
        _sync(path.parent)
    with temporary.open("xb") as stream:
        try:
            def write(data):
                nonlocal total
                remaining()
                total += len(data)
                if total > max_bytes:
                    raise ValueError("private_prefix_export_budget_exceeded")
                stream.write(data)
                digest.update(data)

            write(b"[")
            for item in items:
                write((b",\n" if count else b"") + _json(item).encode())
                count += 1
            write(b"]\n")
            stream.flush()
            os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                    raise ValueError("private_prefix_export_existing_path_unsafe")
                if info.st_size != total:
                    raise RuntimeError("private_prefix_export_existing_bytes_changed")
                observed = sha256()
                with path.open("rb") as existing:
                    while data := existing.read(1024 * 1024):
                        remaining()
                        observed.update(data)
                if path.stat().st_size != total or observed.digest() != digest.digest():
                    raise RuntimeError("private_prefix_export_existing_bytes_changed")
            _sync(path.parent)
        finally:
            temporary.unlink()
            _sync(path.parent)
    return {"path": str(path), "records": count, "bytes": total, "sha256": digest.hexdigest()}


def _stage_scratch(archive, prefix_sha256, work_inode):
    scratch = archive / f".stage-{prefix_sha256[:16]}-{work_inode}"
    scratch.mkdir(mode=0o700, exist_ok=True)
    _, (device, _inode) = _private_root(scratch)
    if device != archive.stat().st_dev:
        raise ValueError("private_prefix_stage_scratch_device_changed")
    leftovers = []
    for entry in scratch.iterdir():
        info = entry.lstat()
        if (not re.fullmatch(r"canonical-facts\.[a-z0-9_]{8}\.parquet", entry.name)
                or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                or info.st_dev != device or leftovers):
            raise ValueError("private_prefix_stage_scratch_unexpected_file")
        leftovers.append(entry)
    for entry in leftovers:
        entry.unlink()
        _sync(scratch)
    return scratch


def _run_batches(args, *, engine, source, target, clone, placement, plan, retirement, journal, store, limits, floor, remaining):
    checkpoint = journal.checkpoint
    cutoff = date.fromisoformat(plan["cold_before_day"])
    phase = args.phase
    if phase in {"load", "audit", "export"}:
        required = {"load": "stage", "audit": "load", "export": "load"}[phase]
        if any(not journal.latest(required, row["day"])[0] for row in plan["days"]):
            raise RuntimeError(f"private_prefix_required_phase_incomplete: {required}")
    if phase == "export":
        if not journal.latest("audit_inverse")[0] or any(not journal.latest("audit_day", row["day"])[0] for row in plan["days"]):
            raise RuntimeError("private_prefix_audit_incomplete")
        kinds = ("audit_plan", "audit_day", "audit_inverse")
        audit_summary = validate_audit_receipts(journal.items(*kinds), expected_prefix_hash=args.prefix_sha256)
        remaining()
        _free(args.work_root, minimum=floor, allowance=2 * args.max_export_bytes)
        artifacts = {"audit": _export(args.work_root / "audit-receipts.json", journal.items(*kinds),
                max_bytes=args.max_export_bytes, remaining=remaining),
            "seals": _export(args.work_root / "seal-receipts.json", journal.items("seal"),
                max_bytes=args.max_export_bytes, remaining=remaining)}
        return {"complete": True, "artifacts": artifacts, "audit_set_hash": audit_summary["audit_set_hash"],
                "loaded_inventory_hash": audit_summary["loaded_inventory_hash"],
                "verified_rows": audit_summary["verified_rows"], "verified_pages": audit_summary["verified_pages"]}

    kind = "audit_day" if phase == "audit" else phase
    audit_scope = {"target": target, "object_store": store, "cutoff": cutoff, "max_days": args.max_days,
                   "max_pages": args.pages_per_batch}

    def audit(**mode):
        seconds = remaining()
        return audit_candidate(engine, checkpoint, **audit_scope, **mode, max_seconds=seconds,
                               statement_timeout_ms=min(30_000, seconds * 1000))

    batch = 0
    if phase == "audit" and not journal.latest("audit_plan")[0]:
        item = audit()
        if item["prefix_hash"] != args.prefix_sha256:
            raise RuntimeError("private_prefix_audit_plan_changed")
        journal.record("audit_plan", item, done=True)
        batch += 1

    if phase == "load":
        payload = _pinned_bytes(args.load_limits, args.load_limits_sha256, 8192)
        load_limits = SharedHddLoadLimits(**json.loads(payload))
        if load_limits.min_free_bytes < args.min_free_bytes:
            raise ValueError("private_prefix_load_floor_below_reviewed_floor")
        if not args.pg_controldata.is_absolute() or not args.pg_controldata.is_file():
            raise ValueError("private_prefix_requires_native_pg_controldata")
        supervise = partial(supervise_private_page, clone=clone, placement=placement, limits=load_limits,
                            pg_controldata=args.pg_controldata)

    for item in plan["days"]:
        day_string = item["day"]
        day = date.fromisoformat(day_string)
        done, previous = journal.latest(kind, day_string)
        while not done:
            if batch >= args.max_batches:
                return {"complete": False, "batches": batch, "next_day": day_string}
            state = retirement.get(day_string)
            _free(args.work_root, minimum=floor, allowance=args.batch_work_bytes)
            if phase == "stage":
                if state is None:
                    with source.snapshot() as conn:
                        has_header = conn.scalar(text("SELECT EXISTS (SELECT 1 FROM market.fact_versions WHERE storage_day=:day)"),
                                                 {"day": day})
                    if has_header:
                        raise RuntimeError(f"private_prefix_unregistered_nonempty_day: {day_string}")
                    result, done = {"storage_day": day_string, "empty": True}, True
                elif state["state"] not in _SEALED:
                    return {"complete": False, "batches": batch, "next_day": day_string,
                            "deferred_reason": "private_prefix_day_requires_seal", "day": item | state}
                else:
                    scratch = _stage_scratch(store.root.parent, args.prefix_sha256, args.work_root.stat().st_ino)
                    seconds = remaining()
                    source.statement_timeout_ms = min(30_000, seconds * 1000)
                    result = stage(source, checkpoint, day=day, source_store=store, destination_store=store,
                                   temporary_directory=scratch, limits=limits, max_pages=1, max_seconds=seconds)
                    done = result["cohort_complete"]
                    if not done and result["pages"] == (previous["pages"] if previous else 0):
                        return {"complete": False, "batches": batch, "next_day": day_string,
                                "deferred_reason": "stage_no_progress"}
            elif phase == "load":
                staged = journal.latest("stage", day_string)[1]
                if staged.get("empty"):
                    result, done = staged, True
                else:
                    seconds = remaining()
                    result = _load_checkpoint(engine, checkpoint, target=target, object_store=store, day=day,
                        after_ordinal=-1 if previous is None else previous["last_processed_ordinal"],
                        max_pages=args.pages_per_batch, max_seconds=seconds, statement_timeout_ms=min(30_000, seconds * 1000),
                        history_target_id=placement["history_target_id"], supervise=supervise)
                    done = result["checkpoint_end_reached"]
                    if "deferred_reason" in result and result["pages_processed"] == 0:
                        return {"complete": False, "batches": batch, "next_day": day_string, **result}
            else:
                after = -1 if previous is None else previous["day_batch"]["last_verified_ordinal"]
                result = audit(day=day, after_ordinal=after)
                if result["prefix_hash"] != args.prefix_sha256:
                    raise RuntimeError("private_prefix_audit_plan_changed")
                step = result["day_batch"]
                done = step["day_scan_end_reached"]
                if not done and step["verified_pages"] == 0:
                    return {"complete": False, "batches": batch, "next_day": day_string, "deferred_reason": "audit_no_progress"}
            journal.record(kind, result, day=day_string, done=done)
            batch += 1
            _free(args.work_root, minimum=floor)
            if "deferred_reason" in result:
                return {"complete": False, "batches": batch, "next_day": day_string, **result}
            previous = result

    if phase == "audit":
        done, previous = journal.latest("audit_inverse")
        while not done:
            if batch >= args.max_batches:
                return {"complete": False, "batches": batch, "next": "inverse_inventory"}
            _free(args.work_root, minimum=floor, allowance=args.batch_work_bytes)
            cursor = "" if previous is None else previous["inverse_inventory_batch"]["last_verified_manifest"]
            previous = audit(inventory=True, after_manifest=cursor)
            if previous["prefix_hash"] != args.prefix_sha256:
                raise RuntimeError("private_prefix_audit_plan_changed")
            result = previous["inverse_inventory_batch"]
            done = result["inventory_scan_end_reached"]
            if not done and result["verified_pages"] == 0:
                return {"complete": False, "batches": batch, "next": "inverse_inventory", "deferred_reason": "audit_no_progress"}
            journal.record("audit_inverse", previous, done=done)
            batch += 1
            _free(args.work_root, minimum=floor)
    return {"complete": True, "batches": batch}


def run(args):
    if ({name for _, name in socket.if_nameindex()} != {"lo"}
            or not 1 <= args.max_batches <= 4096 or not 1 <= args.max_days <= 4096
            or not 1 <= args.pages_per_batch <= 256 or not 2 <= args.max_seconds <= 14_400
            or not 1 <= args.max_export_bytes <= 512 * 1024**2 or args.min_free_bytes <= 0 or args.batch_work_bytes <= 0):
        raise ValueError("private_prefix_requires_isolated_and_bounded_worker")
    codec_limits = FactArchiveLimits()
    if args.phase == "stage" and args.batch_work_bytes < 2 * codec_limits.max_file_bytes:
        raise ValueError("private_prefix_stage_work_budget_below_two_encoded_pages")
    if args.phase == "load" and any(getattr(args, field) is None for field in ("load_limits", "load_limits_sha256", "pg_controldata")):
        raise ValueError("private_prefix_load_requires_limits_and_native_control_tool")
    deadline = monotonic() + args.max_seconds

    def remaining():
        value = int(deadline - monotonic())
        if value < 1:
            raise TimeoutError("private_prefix_run_deadline")
        return value

    _pinned_bytes(args.proof, args.proof_sha256, 65536)
    clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
    placement = load_private_placement(args.placement, args.placement_sha256, clone, clone_proof_sha256=args.proof_sha256)
    archive = Path(placement["archive_root"])
    application_archive = archive
    if args.phase == "seal":
        if args.source_archive_object_root is None:
            raise ValueError("private_prefix_seal_requires_readonly_original_archive")
        application_archive = args.source_archive_object_root.parent
    if (configured_archive_root() != application_archive
            or bool(os.statvfs(archive).f_flag & os.ST_RDONLY) == (args.phase == "stage")):
        raise ValueError("private_prefix_archive_mount_mode_changed")
    store = None if args.phase == "seal" else FilesystemRawArchiveObjectStore(archive / "objects", writable=args.phase == "stage")
    system, db_oid = clone.proof["database_identity"].split("/")
    target = TargetDatabase(system, clone.url.database, int(db_oid))
    engine = create_engine(clone.url, poolclass=NullPool, hide_parameters=True, isolation_level="READ COMMITTED",
        connect_args={"connect_timeout": 5, "passfile": "/dev/null", "application_name": "qt_private_cold_prefix"})
    source = PostgresMigrationSource(engine, expected_system_identifier=system,
                                     statement_timeout_ms=min(30_000, remaining() * 1000))
    try:
        identity, plan, retirement, reservations = _native_plan(source, target, clone=clone, placement=placement,
            cutoff=args.through_day + timedelta(days=1), max_days=args.max_days)
        prefix_sha256 = archive_evidence_hash(plan)
        if args.phase == "plan":
            return {"schema": SCHEMA, "prefix_sha256": prefix_sha256, "database": identity, "prefix": plan,
                    "retention": retirement, "accepted_commit_seq": clone.proof["accepted_commit_seq"]}
        if args.prefix_sha256 != prefix_sha256:
            raise ValueError("private_prefix_reviewed_native_plan_changed")
        checkpoint_path, inode = _work_root(args.work_root, clone=clone, archive=archive)
        floor = args.min_free_bytes + reservations
        _free(args.work_root, minimum=floor, allowance=args.batch_work_bytes)
        scopes = {"schema": SCHEMA, "prefix_sha256": prefix_sha256, "database": identity,
                  "clone_proof_sha256": args.proof_sha256, "placement_sha256": args.placement_sha256,
                  "candidate_archive": str(archive / "objects"), "work_inode": inode}
        with archive_namespace(args.work_root, exclusive=True):
            if _work_root(args.work_root, clone=clone, archive=archive) != (checkpoint_path, inode):
                raise RuntimeError("private_prefix_work_mount_changed")
            checkpoint = MigrationCheckpoint(checkpoint_path)
            journal = PrefixJournal(checkpoint, scopes)
            checkpoint.initialize({"source": identity, "source_root": scopes["candidate_archive"],
                                   "staging_root": scopes["candidate_archive"],
                                   "limits": asdict(codec_limits)})
            _sync(args.work_root)
            if args.phase == "seal":
                result = {"complete": True, "seal": _seal(args, clone=clone, engine=engine, journal=journal,
                                                          plan=plan, retirement=retirement, remaining=remaining)}
            else:
                result = _run_batches(args, engine=engine, source=source, target=target, clone=clone, placement=placement,
                    plan=plan, retirement=retirement, journal=journal, store=store, limits=codec_limits, floor=floor, remaining=remaining)
            remaining()
        return scopes | {"phase": args.phase, **result}
    finally:
        engine.dispose()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("plan", "seal", "stage", "load", "audit", "export"))
    for name in ("proof", "placement", "work-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("proof-sha256", "placement-sha256", "min-free-bytes", "batch-work-bytes"):
        parser.add_argument(f"--{name}", type=int if name.endswith("bytes") else str, required=True)
    parser.add_argument("--through-day", type=date.fromisoformat, required=True)
    parser.add_argument("--prefix-sha256")
    for name, default in (("max-seconds", 120), ("max-days", 512), ("max-batches", 1),
                          ("pages-per-batch", 1), ("max-export-bytes", 64 * 1024**2)):
        parser.add_argument(f"--{name}", type=int, default=default)
    for name in ("source-archive-object-root", "load-limits", "pg-controldata"):
        parser.add_argument(f"--{name}", type=Path)
    for name in ("header-oid", "payload-oid", "count-statement-timeout-ms"):
        parser.add_argument(f"--{name}", type=int)
    parser.add_argument("--load-limits-sha256")
    parser.add_argument("--seal-day", type=date.fromisoformat)
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        result = run(args)
    except SQLAlchemyError as error:
        parser.exit(1, f"private_prefix_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as error:
        parser.exit(1, f"private_prefix_refused: {error}\n")
    print(_json(result))
    return 75 if result.get("deferred_reason") else 0


if __name__ == "__main__":
    raise SystemExit(main())

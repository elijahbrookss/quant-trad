"""Bind native collector evidence to the one reviewed cutover package."""

from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
from time import sleep

from sqlalchemy import text

from core.storage_mounts import require_configured_working_mount
from market_data.archive import RawArchiveReadLimits
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.fact_archive import archive_evidence_hash
from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_package import _hash_file, _pin, _unique_fields
from scripts.db.canonical_cold_collector_drain import InactiveLevel2, CollectorDrainLimits, capture_collector_tail, verify_collector_drain
from scripts.db.canonical_cold_collector_restart import CollectorRestartPending, verify_collector_restart
from scripts.db.canonical_cold_recovery_pending_spool import PendingSpoolScope


PLAN_VERSION = "qt.canonical_cold_collector_plan.v2"
RECEIPT_VERSION = "qt.canonical_cold_collector_execution.v1"


def _root_identity(path):
    if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError("canonical_cold_collector_unqualified_directory")
    info = path.stat()
    return info.st_dev, info.st_ino


def bind_approved_collector_checks(conn, *, package_path, package, store, receipt_root):
    """Load the exact approved roster and preserve the dynamic shutdown proof."""
    binding = package["artifacts"]["provider_boundary"]
    plan_hash, content = _hash_file(Path(package_path).parent / binding["filename"], json_limit=65_536)
    if plan_hash != binding["sha256"]:
        raise RuntimeError("canonical_cold_collector_plan_digest_changed")
    plan = json.loads(content, object_pairs_hook=_unique_fields)
    if (not isinstance(plan, dict) or set(plan) != {
            "schema", "stream_ids", "poll_ids", "spool_root", "limits", "rehearsal_sha256", "inactive_level2",
        } or plan["schema"] != PLAN_VERSION or not _pin(plan["rehearsal_sha256"])
            or not all(isinstance(plan[name], list) for name in ("stream_ids", "poll_ids"))
            or not isinstance(plan["spool_root"], str) or not isinstance(plan["limits"], dict)
            or set(plan["limits"]) != {"archive", "raw", "max_events", "max_raw_rows", "max_spool_entries"}):
        raise ValueError("canonical_cold_collector_plan_invalid")
    inactive = plan["inactive_level2"]
    inactive_level2 = None
    if inactive is not None:
        if (not isinstance(inactive, dict) or set(inactive) != {
            "scope", "failed_event_id", "failed_event_sha256", "published_event_id", "published_event_sha256",
        } or not isinstance(inactive["scope"], dict)):
            raise ValueError("canonical_cold_collector_plan_invalid_inactive_scope")
        inactive_level2 = InactiveLevel2(**{**inactive, "scope": PendingSpoolScope(**inactive["scope"])})
        if inactive_level2.scope.definition_id not in plan["stream_ids"]:
            raise ValueError("canonical_cold_collector_inactive_definition_not_in_roster")
    pending_scope_sha256 = None if inactive_level2 is None else archive_evidence_hash(asdict(inactive_level2.scope))
    target = package["scope"]["target_database"]
    database_identity = f"{target['system_identifier']}/{target['database_oid']}"
    budgets = plan["limits"]
    limits = CollectorDrainLimits(
        archive=ArchiveVerificationLimits(**budgets["archive"]), raw=RawArchiveReadLimits(**budgets["raw"]),
        max_events=budgets["max_events"], max_raw_rows=budgets["max_raw_rows"], max_spool_entries=budgets["max_spool_entries"],
    )
    spool = Path(plan["spool_root"])
    require_configured_working_mount(spool)
    spool_identity = _root_identity(spool)
    receipts = Path(receipt_root)
    receipts_identity = _root_identity(receipts)
    info = receipts.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("canonical_cold_collector_receipts_require_private_directory")
    check = None
    drained = None

    def assert_roots():
        if check is None:
            raise RuntimeError("canonical_cold_collector_execution_tail_unobserved")
        check()
        require_configured_working_mount(spool)
        if _root_identity(spool) != spool_identity or _root_identity(receipts) != receipts_identity:
            raise RuntimeError("canonical_cold_collector_execution_directory_changed")

    def seal(phase, evidence):
        assert_roots()
        record = {"schema": RECEIPT_VERSION, "phase": phase, "plan_sha256": plan_hash, "evidence": evidence}
        digest = archive_evidence_hash(record)
        artifact = {**record, "sha256": digest}
        path = receipts / f"collector-{phase}-{digest}.json"
        try:
            host.save_receipt(path, artifact, initial=True)
        except FileExistsError:
            if host.load_receipt(path, max_bytes=128 * 1024**2) != artifact:
                raise RuntimeError("canonical_cold_collector_execution_receipt_changed")
        assert_roots()
        return artifact

    def capture(check_deadline):
        nonlocal check
        check = check_deadline
        assert_roots()
        with conn.begin():
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            evidence = capture_collector_tail(
                conn, stream_ids=tuple(plan["stream_ids"]), poll_ids=tuple(plan["poll_ids"]), check=check,
                inactive_level2=inactive_level2,
            )
        return seal("pre_stop", evidence)

    def verify(target, source_commit, before):
        nonlocal drained
        assert_roots()
        if (target is not conn or before.get("plan_sha256") != plan_hash or before.get("phase") != "pre_stop"
                or archive_evidence_hash({key: item for key, item in before.items() if key != "sha256"}) != before.get("sha256")):
            raise RuntimeError("canonical_cold_collector_execution_tail_changed")
        evidence = verify_collector_drain(
            target, before=before["evidence"], source_commit_seq=source_commit, spool_root=spool,
            store=store, limits=limits, check=check, inactive_level2=inactive_level2, database_identity=database_identity,
        )
        record = seal("drained", {**evidence, "execution_pre_stop_sha256": before["sha256"]})
        drained = evidence, record["sha256"]
        return record["sha256"]

    def verify_restarted(services):
        if drained is None:
            raise RuntimeError("canonical_cold_collector_execution_drain_unobserved")
        collector = services["market-data-collector"]
        if collector["running"] is not True or not collector["started_at"]:
            raise RuntimeError("canonical_cold_collector_execution_new_runtime_unconfirmed")
        started = datetime.fromisoformat(collector["started_at"])
        last_pending = None
        while True:
            try:
                assert_roots()
                with conn.begin():
                    conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    milliseconds = max(1, min(15_000, int(check() * 1_000)))
                    conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(milliseconds)})
                    evidence = verify_collector_restart(
                        conn, drained=drained[0], started_at=started, store=store, limits=limits, check=check, spool_root=spool,
                    )
                return seal("restarted", {**evidence, "execution_drained_sha256": drained[1]})["sha256"]
            except CollectorRestartPending as pending:
                last_pending = pending
            except TimeoutError as expired:
                if last_pending is not None:
                    expired.add_note(str(last_pending))
                raise
            sleep(.25)

    return capture, verify, verify_restarted, pending_scope_sha256

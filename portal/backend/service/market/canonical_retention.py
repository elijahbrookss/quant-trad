"""Policy-bounded orchestration over the canonical archive/reclaim owners.

The cursor is only an in-process scheduling hint. Committed partition, page,
and verification records are the durable resume state and deletion authority
always comes from the existing fenced reclaimer's fresh evidence checks.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import logging
from pathlib import Path
from threading import Lock
from time import monotonic

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.fact_archive import FactArchiveLimits
from ..storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from ..storage.repos.fact_reclamation import FactReclamationLimits, PostgresCanonicalFactReclamationRepository
from ..storage.repos.fact_retention import (
    archive_admission_blockers, canonical_fact_retention_repository, require_hot_window_elapsed,
)
from ..storage.repos.market_lifecycle import MarketStorageLifecycleBusyError

logger = logging.getLogger(__name__)


class CanonicalRetentionBudgetExceeded(RuntimeError):
    stop_reason = "time_budget"


class CanonicalRetentionStopRequested(CanonicalRetentionBudgetExceeded):
    stop_reason = "stop_requested"


class CanonicalFactRetentionExecutor:
    def __init__(self, *, repository=canonical_fact_retention_repository):
        self.repository = repository
        self._cursor = None
        self._run_lock = Lock()

    def _require_archive(self, *, policy, storage_root, action):
        filesystem = self.repository._filesystem(storage_root)
        blockers = archive_admission_blockers(policy=policy, filesystem=filesystem,
                                              publication=action in {"seal_partition", "stage_page"})
        if blockers:
            raise RuntimeError(f"canonical_retention_archive_admission_failed: action={action} blockers={blockers}")

    def _execute_step(self, *, item, policy, storage_root, check_budget, remaining_seconds):
        day = date.fromisoformat(item["storage_day"])
        action = item["action"]
        self._require_archive(policy=policy, storage_root=storage_root, action=action)

        def guard(session, partition):
            check_budget()
            require_hot_window_elapsed(session, partition, policy=policy)
            self._require_archive(policy=policy, storage_root=storage_root, action=action)

        archive = PostgresCanonicalFactArchiveRepository(
            database=self.repository.database,
            object_store=FilesystemRawArchiveObjectStore(Path(storage_root).expanduser().resolve() / "objects"),
            temporary_directory=Path(storage_root).expanduser().resolve() / "canonical-staging",
            limits=FactArchiveLimits(max_rows=policy.max_page_rows,
                row_group_size=min(512, policy.max_page_rows),
                max_logical_bytes=policy.max_page_logical_bytes,
                max_file_bytes=2 * policy.max_page_logical_bytes),
            max_dependency_bytes=policy.max_verification_bytes,
            max_dependency_objects=policy.max_verification_objects,
            statement_timeout_ms=min(policy.execution_statement_timeout_ms, max(1, int(remaining_seconds * 1000))),
            partition_guard=guard, check_budget=check_budget,
        )
        verification = ArchiveVerificationLimits(max_bytes=policy.max_verification_bytes,
            max_objects=policy.max_verification_objects, max_pages=policy.max_verification_pages)
        if action == "seal_partition":
            return {**archive.seal_partition(day), "status": "partition_sealed"}
        if action == "stage_page":
            return archive.stage_next_page(day)
        if action == "verify_page":
            return archive.verify_next_page(day)
        if action == "verify_partition":
            return archive.verify_partition(day, limits=verification)
        if action == "reclaim_partition":
            reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True,
                limits=FactReclamationLimits(statement_timeout_ms=min(1000, policy.execution_statement_timeout_ms)))
            return reclaimer.reclaim_partition(day, eligible_before=date.fromisoformat(item["eligible_before"]),
                                                execute=True, verification_limits=verification)
        raise ValueError(f"canonical_retention_action_invalid: action={action}")

    def run(self, *, policy: CanonicalFactRetentionPolicy, storage_root: Path,
            execute: bool = False, after_storage_day: date | None = None, cancelled=None):
        if type(execute) is not bool or (after_storage_day is not None and type(after_storage_day) is not date):
            raise ValueError("canonical_retention_request_invalid")
        if not execute or not policy.execution_enabled:
            return {"schema_version": "market.canonical_retention_run.v1",
                    "status": "dry_run" if not execute else "disabled", "outcomes": [], "failure_count": 0,
                    "plan": self.repository.plan(policy=policy, storage_root=storage_root,
                                                 after_storage_day=after_storage_day)}
        if not self._run_lock.acquire(blocking=False):
            raise MarketStorageLifecycleBusyError("canonical_retention_worker_busy: retry later")
        try:
            return self._run(policy=policy, storage_root=storage_root, after_storage_day=after_storage_day, cancelled=cancelled)
        finally:
            self._run_lock.release()

    def _run(self, *, policy, storage_root, after_storage_day, cancelled):
        started = monotonic()
        deadline = started + policy.max_run_seconds
        cursor = after_storage_day if after_storage_day is not None else self._cursor
        outcomes, initial_plan = [], None
        stop_reason = "step_budget"
        scans = 0

        def check_budget():
            if cancelled is not None and cancelled():
                raise CanonicalRetentionStopRequested("canonical_retention_stop_requested: resume committed progress next run")
            if monotonic() >= deadline:
                raise CanonicalRetentionBudgetExceeded("canonical_retention_run_budget_exceeded: resume committed progress next run")

        for _ in range(policy.max_steps_per_run):
            try:
                check_budget()
                plan_policy = replace(policy, max_plan_seconds=min(policy.max_plan_seconds,
                                                                   max(1, int(deadline - monotonic()))))
                plan = self.repository.plan(policy=plan_policy, storage_root=storage_root, after_storage_day=cursor)
                scans += 1
                initial_plan = initial_plan or plan
                check_budget()
            except CanonicalRetentionBudgetExceeded as exc:
                stop_reason = exc.stop_reason
                break
            candidates = [item for item in plan["actions"] if item["eligible"]]
            if not candidates:
                next_day = plan["next_after_storage_day"]
                cursor = date.fromisoformat(next_day) if next_day else None
                if cursor is None:
                    stop_reason = "scan_complete"
                    if plan["actions"]:
                        waiting = {"active_or_future_storage_day", "hot_window_not_elapsed"}
                        stop_reason = ("no_eligible_work" if all(set(row["blockers"]) <= waiting
                                                               for row in plan["actions"]) else "blocked")
                    break
                continue
            item = candidates[0]
            day = date.fromisoformat(item["storage_day"])
            # Revisit this day's durable progress on success/budget suspension;
            # skip a failed/busy day this cycle so it cannot starve later days.
            cursor = day - timedelta(days=1)
            try:
                result = self._execute_step(item=item, policy=policy, storage_root=storage_root,
                                           check_budget=check_budget, remaining_seconds=deadline - monotonic())
                outcomes.append({**result, "action": item["action"], "storage_day": day.isoformat()})
                if result["status"] in {"partition_reclaimed", "already_reclaimed"}:
                    cursor = day
                logger.info("canonical_retention_step_finished | storage_day=%s action=%s status=%s",
                            day, item["action"], result["status"])
            except CanonicalRetentionBudgetExceeded as exc:
                outcomes.append({"action": item["action"], "storage_day": day.isoformat(),
                                 "status": "deferred", "reason": str(exc)})
                stop_reason = exc.stop_reason
                break
            except MarketStorageLifecycleBusyError as exc:
                outcomes.append({"action": item["action"], "storage_day": day.isoformat(),
                                 "status": "deferred", "reason": str(exc)})
                logger.info("canonical_retention_step_deferred | storage_day=%s action=%s reason=%s", day, item["action"], exc)
                cursor = day
            except Exception as exc:
                if getattr(getattr(exc, "orig", None), "pgcode", None) == "55P03":
                    outcomes.append({"action": item["action"], "storage_day": day.isoformat(),
                                     "status": "deferred", "reason": "canonical_retention_relation_busy"})
                    logger.info("canonical_retention_step_deferred | storage_day=%s action=%s reason=relation_busy", day, item["action"])
                else:
                    outcomes.append({"action": item["action"], "storage_day": day.isoformat(),
                                     "status": "failed", "error": str(exc)})
                    logger.exception("canonical_retention_step_failed | storage_day=%s action=%s", day, item["action"])
                cursor = day
        self._cursor = cursor
        failures = sum(item["status"] == "failed" for item in outcomes)
        result = {"schema_version": "market.canonical_retention_run.v1",
                  "status": "degraded" if failures else "bounded" if stop_reason.endswith("budget") else stop_reason,
                  "stop_reason": stop_reason, "plan": initial_plan, "outcomes": outcomes, "failure_count": failures,
                  "planning_pages": scans, "elapsed_seconds": monotonic() - started,
                  "next_after_storage_day": cursor.isoformat() if cursor else None,
                  "resume_authority": "committed_partition_pages_and_verifications"}
        logger.info("canonical_retention_run_finished | status=%s steps=%s scans=%s failures=%s next_after_storage_day=%s",
                    result["status"], len(outcomes), scans, failures, result["next_after_storage_day"])
        return result

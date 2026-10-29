"""Policy-bounded orchestration over the canonical archive/reclaim owners.

The cursor is only an in-process scheduling hint. Committed partition, page,
and verification records are the durable resume state and deletion authority
always comes from the existing fenced reclaimer's fresh evidence checks.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, timedelta
from math import isfinite
import logging
from pathlib import Path
from threading import Lock
from time import monotonic, perf_counter

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
from ..storage.history_policy import saved_canonical_policy, require_saved_history_policy

logger = logging.getLogger(__name__)

_PAGE_PREREQUISITE_STATUSES = frozenset({"book_prefix_verified", "trade_prefix_verified"})
_MAX_PAGE_PREREQUISITE_COMMITS = 64
_SELECTED_ACTIONS = frozenset({"stage_page", "reclaim_partition"})
_SELECTED_SUCCESS = {
    "stage_page": "page_acknowledged",
    "reclaim_partition": "partition_reclaimed",
}


@dataclass
class _RetryState:
    storage_day: date
    first_failure_at: float
    last_failure_at: float
    next_eligible_at: float
    attempt_count: int
    reason: str


class CanonicalRetentionBudgetExceeded(RuntimeError):
    stop_reason = "time_budget"


class CanonicalRetentionStopRequested(CanonicalRetentionBudgetExceeded):
    stop_reason = "stop_requested"


class CanonicalFactRetentionExecutor:
    def __init__(self, *, repository=canonical_fact_retention_repository, use_saved_history_policy=False):
        self.repository = repository
        self.use_saved_history_policy = use_saved_history_policy
        self._cursor = None
        self._retry_states = {}
        self._run_lock = Lock()
        self._archive = None
        self._archive_action = None
        self._archive_scope = None
        self._retained_checkpoint_evidence = None
        self._retained_checkpoint_scope = None

    def _dispose_retained_checkpoint_evidence(self, *, reason):
        cache, self._retained_checkpoint_evidence = self._retained_checkpoint_evidence, None
        scope, self._retained_checkpoint_scope = self._retained_checkpoint_scope, None
        if cache is None:
            return
        logger.info(
            "canonical_checkpoint_source_evidence_retention_disposed | reason=%s "
            "scope=%s hits=%s misses=%s evictions=%s retained_rows=%s "
            "retained_charge_bytes=%s",
            reason, scope, getattr(cache, "hit_count", 0),
            getattr(cache, "miss_count", 0), getattr(cache, "eviction_count", 0),
            getattr(cache, "row_count", 0), getattr(cache, "retained_bytes", 0),
        )
        cache.dispose()

    def _dispose_archive(self, *, preserve_checkpoint=False, reason="archive_disposed"):
        archive, self._archive = self._archive, None
        self._archive_action = None
        scope, self._archive_scope = self._archive_scope, None
        if archive is not None:
            if preserve_checkpoint and scope is not None:
                take = getattr(archive, "take_checkpoint_source_evidence_cache", None)
                cache = take() if take is not None else None
                if cache is not None and getattr(cache, "row_count", 0) > 0:
                    self._dispose_retained_checkpoint_evidence(reason="retention_replaced")
                    self._retained_checkpoint_evidence = cache
                    self._retained_checkpoint_scope = scope
                    logger.info(
                        "canonical_checkpoint_source_evidence_retained | scope=%s "
                        "hits=%s misses=%s evictions=%s retained_rows=%s "
                        "retained_charge_bytes=%s",
                        scope, getattr(cache, "hit_count", 0),
                        getattr(cache, "miss_count", 0), getattr(cache, "eviction_count", 0),
                        getattr(cache, "row_count", 0), getattr(cache, "retained_bytes", 0),
                    )
                elif cache is not None:
                    cache.dispose()
            dispose = getattr(archive, "dispose_dependency_evidence", None)
            if dispose is not None:
                dispose()
        if not preserve_checkpoint:
            self._dispose_retained_checkpoint_evidence(reason=reason)

    def _retry_schedule(self, now):
        return [{
            "storage_day": state.storage_day.isoformat(),
            "attempt_count": state.attempt_count,
            "reason": state.reason,
            "age_seconds": max(0.0, now - state.first_failure_at),
            "next_eligible_in_seconds": max(0.0, state.next_eligible_at - now),
        } for state in sorted(self._retry_states.values(), key=lambda item: item.storage_day)]

    def _schedule_retry(self, *, day, reason, policy, now, count_attempt=True):
        state = self._retry_states.get(day)
        if state is None:
            limit = max(1, policy.max_candidate_partitions)
            if len(self._retry_states) >= limit:
                newest = max(self._retry_states)
                if day >= newest:
                    logger.warning(
                        "canonical_retention_retry_queue_full | storage_day=%s limit=%s oldest_storage_day=%s",
                        day, limit, min(self._retry_states),
                    )
                    return
                del self._retry_states[newest]
            state = _RetryState(
                storage_day=day, first_failure_at=now, last_failure_at=now,
                next_eligible_at=now + policy.max_run_seconds,
                attempt_count=1 if count_attempt else 0, reason=reason,
            )
            self._retry_states[day] = state
        else:
            state.last_failure_at = now
            state.next_eligible_at = now + policy.max_run_seconds
            state.attempt_count += int(count_attempt)
            state.reason = reason
        logger.info(
            "canonical_retention_retry_scheduled | storage_day=%s attempt_count=%s "
            "retry_after_seconds=%s reason=%s",
            day, state.attempt_count, policy.max_run_seconds, reason,
        )

    def _clear_retry(self, day):
        state = self._retry_states.pop(day, None)
        if state is not None:
            logger.info(
                "canonical_retention_retry_cleared | storage_day=%s attempt_count=%s",
                day, state.attempt_count,
            )

    def _require_archive(self, *, policy, storage_root, action):
        filesystem = self.repository._filesystem(storage_root)
        blockers = archive_admission_blockers(policy=policy, filesystem=filesystem,
                                              publication=action in {"seal_partition", "stage_page"})
        if blockers:
            raise RuntimeError(f"canonical_retention_archive_admission_failed: action={action} blockers={blockers}")

    def _execute_step(self, *, item, policy, storage_root, check_budget, remaining_seconds,
                      storage_policy_witness=None, prerequisite_commits=None):
        day = date.fromisoformat(item["storage_day"])
        action = item["action"]
        reusable = action in {"stage_page", "verify_page"}
        resolved_storage_root = Path(storage_root).expanduser().resolve()
        scope = (str(resolved_storage_root), day.isoformat(), action) if reusable else None
        # Evidence belongs only to consecutive work for the same page action
        # and day. Clear it before any incompatible transition: a refused
        # transition must not leave an old page proof available to later work.
        if self._archive is not None and (
                not reusable or self._archive_action != action or self._archive_scope != scope):
            self._dispose_archive(reason="action_or_scope_changed")
        if (self._retained_checkpoint_evidence is not None
                and self._retained_checkpoint_scope != scope):
            self._dispose_retained_checkpoint_evidence(reason="action_or_scope_changed")
        self._require_archive(policy=policy, storage_root=storage_root, action=action)

        def guard(session, partition):
            check_budget()
            current_policy = policy
            if self.use_saved_history_policy:
                current_policy = require_saved_history_policy(session,
                    witness=storage_policy_witness, policy=policy, storage_root=storage_root)
            require_hot_window_elapsed(session, partition, policy=current_policy)
            self._require_archive(policy=current_policy, storage_root=storage_root, action=action)

        # Reuse only the current run's staging repository. Its one-page buffer
        # avoids repeating hot hydration for each committed prefix interval.
        # A separate bounded sparse checkpoint cache may be adopted by the new
        # repository after a normal scheduler yield.
        def current_remaining_seconds():
            value = remaining_seconds() if callable(remaining_seconds) else remaining_seconds
            if type(value) not in {int, float} or not isfinite(value) or value <= 0:
                raise CanonicalRetentionBudgetExceeded(
                    "canonical_retention_run_budget_exceeded: resume committed progress next run"
                )
            return value

        archive = self._archive if reusable else None
        timeout = min(policy.execution_statement_timeout_ms,
                      max(1, int(current_remaining_seconds() * 1000)))
        if archive is None:
            archive = PostgresCanonicalFactArchiveRepository(
                database=self.repository.database,
                object_store=FilesystemRawArchiveObjectStore(resolved_storage_root / "objects"),
                temporary_directory=resolved_storage_root / "canonical-staging",
                limits=FactArchiveLimits(max_rows=policy.max_page_rows,
                    row_group_size=min(512, policy.max_page_rows),
                    max_logical_bytes=policy.max_page_logical_bytes,
                    max_file_bytes=2 * policy.max_page_logical_bytes),
                max_dependency_bytes=policy.max_verification_bytes,
                max_dependency_objects=policy.max_verification_objects,
                statement_timeout_ms=timeout,
                partition_guard=guard, check_budget=check_budget,
            )
            if self._retained_checkpoint_evidence is not None:
                cache, self._retained_checkpoint_evidence = self._retained_checkpoint_evidence, None
                self._retained_checkpoint_scope = None
                adopt = getattr(archive, "adopt_checkpoint_source_evidence_cache", None)
                adopted = False
                if adopt is None:
                    cache.dispose()
                else:
                    try:
                        adopted = adopt(cache)
                    except BaseException:
                        cache.dispose()
                        raise
                if not adopted:
                    # Refusal owns no evidence. Dispose here even when the
                    # repository already did so; cache disposal is idempotent.
                    cache.dispose()
                    logger.info(
                        "canonical_checkpoint_source_evidence_retention_disposed | "
                        "reason=incompatible_repository_limits scope=%s",
                        scope,
                    )
        archive.partition_guard = guard
        archive.statement_timeout_ms = timeout
        if reusable:
            enable_reuse = getattr(archive, "enable_dependency_evidence_reuse", None)
            if enable_reuse is not None:
                enable_reuse()
        self._archive = archive if reusable else None
        self._archive_action = action if reusable else None
        self._archive_scope = scope if reusable else None
        verification = ArchiveVerificationLimits(max_bytes=policy.max_verification_bytes,
            max_objects=policy.max_verification_objects, max_pages=policy.max_verification_pages)
        if action == "seal_partition":
            return {**archive.seal_partition(day), "status": "partition_sealed"}
        if action == "stage_page":
            prerequisites = prerequisite_commits if prerequisite_commits is not None else []
            for _ in range(_MAX_PAGE_PREREQUISITE_COMMITS):
                check_budget()
                archive.statement_timeout_ms = min(
                    policy.execution_statement_timeout_ms,
                    max(1, int(current_remaining_seconds() * 1000)),
                )
                result = archive.stage_next_page(day)
                if result.get("status") not in _PAGE_PREREQUISITE_STATUSES:
                    return {
                        **result,
                        "prerequisite_commit_count": len(prerequisites),
                        "prerequisite_commits": prerequisites,
                    }
                prerequisites.append({key: value for key, value in result.items() if key != "storage_day"})
            return {
                "storage_day": day,
                "status": "page_prerequisite_budget",
                "reason": "canonical_archive_page_prerequisite_budget",
                "prerequisite_commit_count": len(prerequisites),
                "prerequisite_commits": prerequisites,
            }
        if action == "verify_page":
            return archive.verify_next_page(day)
        if action == "verify_partition":
            return archive.verify_partition(day, limits=verification)
        if action == "restart_verification":
            return archive.restart_partition_verification(day)
        if action == "reclaim_partition":
            reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True,
                limits=FactReclamationLimits(
                    statement_timeout_ms=min(1000, timeout),
                    pinned_ranges_statement_timeout_ms=min(
                        policy.pinned_ranges_statement_timeout_ms, timeout),
                    max_handoff_seconds=policy.max_handoff_seconds,
                ), remaining_run_seconds=current_remaining_seconds)
            return reclaimer.reclaim_partition(day, eligible_before=date.fromisoformat(item["eligible_before"]),
                                                execute=True, verification_limits=verification)
        raise ValueError(f"canonical_retention_action_invalid: action={action}")

    def run(self, *, policy: CanonicalFactRetentionPolicy, storage_root: Path,
            execute: bool = False, after_storage_day: date | None = None, cancelled=None,
            progress=None):
        if type(execute) is not bool or (after_storage_day is not None and type(after_storage_day) is not date):
            raise ValueError("canonical_retention_request_invalid")
        witness = None
        if self.use_saved_history_policy:
            policy, witness = saved_canonical_policy(self.repository.database,
                policy=policy, storage_root=storage_root)
        if not execute or not policy.execution_enabled:
            return {"schema_version": "market.canonical_retention_run.v1",
                    "status": "dry_run" if not execute else "disabled", "outcomes": [], "failure_count": 0,
                    "plan": self.repository.plan(policy=policy, storage_root=storage_root,
                                                 after_storage_day=after_storage_day)}
        if not self._run_lock.acquire(blocking=False):
            raise MarketStorageLifecycleBusyError("canonical_retention_worker_busy: retry later")
        result = None
        try:
            result = self._run(
                policy=policy, storage_root=storage_root,
                after_storage_day=after_storage_day, cancelled=cancelled,
                storage_policy_witness=witness, progress=progress,
            )
            return result
        finally:
            normal_page_yield = (
                result is not None
                and result.get("stop_reason") in {"time_budget", "step_budget"}
                and any(
                    item.get("action") in {"stage_page", "verify_page"}
                    for item in result.get("outcomes", ())
                )
            )
            # Durable catalog rows remain the only resume authority. A normal
            # scheduler yield may retain bounded immutable checkpoint rows; no
            # transaction, lock, plan, page buffer or raw evidence crosses it.
            self._dispose_archive(
                preserve_checkpoint=normal_page_yield,
                reason="run_exit",
            )
            self._run_lock.release()

    def run_selected(self, *, policy: CanonicalFactRetentionPolicy, storage_root: Path,
                     storage_day: date, expected_action: str, execute: bool = False,
                     cancelled=None, progress=None):
        """Execute exactly one freshly planned private canary action.

        This is deliberately narrower than ``run``: it never advances a scan,
        consults retry hints or performs another lifecycle family. The normal
        planner, saved policy, admission checks and action repositories remain
        authoritative. A changed day/action/eligibility refuses before mutation.
        """
        if (type(execute) is not bool or type(storage_day) is not date
                or type(expected_action) is not str or expected_action not in _SELECTED_ACTIONS):
            raise ValueError("canonical_retention_selected_request_invalid")
        witness = None
        if self.use_saved_history_policy:
            policy, witness = saved_canonical_policy(
                self.repository.database, policy=policy, storage_root=storage_root,
            )
        if not policy.execution_enabled:
            return {
                "schema_version": "market.canonical_retention_selected_run.v1",
                "status": "disabled",
                "storage_day": storage_day.isoformat(),
                "expected_action": expected_action,
                "outcomes": [],
                "failure_count": 0,
                "plan": self.repository.plan(
                    policy=policy, storage_root=storage_root,
                    after_storage_day=storage_day - timedelta(days=1),
                ),
            }
        if not execute:
            plan = self.repository.plan(
                policy=policy, storage_root=storage_root,
                after_storage_day=storage_day - timedelta(days=1),
            )
            item = self._selected_plan_item(
                plan=plan, storage_day=storage_day, expected_action=expected_action,
            )
            result = {
                "schema_version": "market.canonical_retention_selected_run.v1",
                "status": "dry_run",
                "storage_day": storage_day.isoformat(),
                "expected_action": expected_action,
                "outcomes": [],
                "failure_count": 0,
                "plan": plan,
                "selected_action": item,
            }
            if self.use_saved_history_policy:
                result["storage_policy"] = witness
            return result
        if not self._run_lock.acquire(blocking=False):
            raise MarketStorageLifecycleBusyError("canonical_retention_worker_busy: retry later")
        try:
            # Selected canaries are an explicit operator boundary, not a
            # routine scheduler yield. They always start with fresh evidence.
            self._dispose_archive(reason="selected_action")
            return self._run_selected(
                policy=policy, storage_root=storage_root, storage_day=storage_day,
                expected_action=expected_action, cancelled=cancelled,
                storage_policy_witness=witness, progress=progress,
            )
        finally:
            self._dispose_archive()
            self._run_lock.release()

    @staticmethod
    def _selected_plan_item(*, plan, storage_day, expected_action):
        matches = [
            item for item in plan["actions"]
            if date.fromisoformat(item["storage_day"]) == storage_day
        ]
        if len(matches) != 1:
            raise RuntimeError(
                "canonical_retention_selected_action_changed: "
                f"storage_day={storage_day} expected_action={expected_action} "
                f"matching_actions={len(matches)}"
            )
        item = matches[0]
        if item["action"] != expected_action or not item["eligible"]:
            raise RuntimeError(
                "canonical_retention_selected_action_changed: "
                f"storage_day={storage_day} expected_action={expected_action} "
                f"actual_action={item['action']} eligible={item['eligible']} "
                f"blockers={item.get('blockers', [])}"
            )
        return item

    def _run_selected(self, *, policy, storage_root, storage_day, expected_action,
                      cancelled, storage_policy_witness=None, progress=None):
        started = monotonic()
        deadline = started + policy.max_run_seconds

        def observe(**fields):
            if progress is None:
                return
            try:
                progress({"schema_version": "market.canonical_retention_progress.v1", **fields})
            except Exception:
                logger.warning("canonical_retention_progress_observer_failed", exc_info=True)

        def check_budget():
            if cancelled is not None and cancelled():
                raise CanonicalRetentionStopRequested(
                    "canonical_retention_stop_requested: no selected action started"
                )
            if monotonic() >= deadline:
                raise CanonicalRetentionBudgetExceeded(
                    "canonical_retention_run_budget_exceeded: no selected action retry"
                )

        def remaining_run_seconds():
            check_budget()
            return deadline - monotonic()

        observe(stage="planning", completed_steps=0,
                after_storage_day=(storage_day - timedelta(days=1)).isoformat())
        check_budget()
        planning_started = perf_counter()
        plan_policy = replace(
            policy,
            max_plan_seconds=min(policy.max_plan_seconds, max(1, int(deadline - monotonic()))),
        )
        plan = self.repository.plan(
            policy=plan_policy, storage_root=storage_root,
            after_storage_day=storage_day - timedelta(days=1),
        )
        planning_seconds = perf_counter() - planning_started
        check_budget()
        item = self._selected_plan_item(
            plan=plan, storage_day=storage_day, expected_action=expected_action,
        )
        observe(stage="executing", completed_steps=0, storage_day=storage_day.isoformat(),
                action=expected_action)
        failure_count = 0
        status = "completed"
        try:
            raw_outcome = self._execute_step(
                item=item, policy=policy, storage_root=storage_root,
                check_budget=check_budget, remaining_seconds=remaining_run_seconds,
                storage_policy_witness=storage_policy_witness, prerequisite_commits=[],
            )
            if raw_outcome.get("status") != _SELECTED_SUCCESS[expected_action]:
                status = "failed"
                failure_count = 1
                error = (
                    "canonical_retention_selected_outcome_changed: "
                    f"storage_day={storage_day} expected_action={expected_action} "
                    f"expected_status={_SELECTED_SUCCESS[expected_action]} "
                    f"actual_status={raw_outcome.get('status')}"
                )
                outcome = {
                    **raw_outcome, "action": expected_action,
                    "storage_day": storage_day.isoformat(), "status": "failed",
                    "actual_status": raw_outcome.get("status"), "error": error,
                }
            else:
                outcome = {**raw_outcome, "action": expected_action,
                           "storage_day": storage_day.isoformat()}
        except (CanonicalRetentionBudgetExceeded, MarketStorageLifecycleBusyError) as exc:
            status = "deferred"
            failure_count = 1
            outcome = {"action": expected_action, "storage_day": storage_day.isoformat(),
                       "status": "deferred", "reason": str(exc)}
            logger.warning(
                "canonical_retention_selected_step_deferred | storage_day=%s action=%s reason=%s",
                storage_day, expected_action, exc,
            )
        except Exception as exc:
            status = "failed"
            failure_count = 1
            outcome = {"action": expected_action, "storage_day": storage_day.isoformat(),
                       "status": "failed", "error": str(exc)}
            logger.exception(
                "canonical_retention_selected_step_failed | storage_day=%s action=%s",
                storage_day, expected_action,
            )
        finished = monotonic()
        result = {
            "schema_version": "market.canonical_retention_selected_run.v1",
            "status": status,
            "stop_reason": ("selected_action_complete" if status == "completed"
                            else "selected_action_stopped"),
            "storage_day": storage_day.isoformat(),
            "expected_action": expected_action,
            "plan": plan,
            "outcomes": [outcome],
            "failure_count": failure_count,
            "planning_pages": 1,
            "planning_seconds": planning_seconds,
            "elapsed_seconds": finished - started,
            "resume_authority": "committed_partition_pages_and_verifications",
            "prerequisite_commit_count": outcome.get("prerequisite_commit_count", 0),
        }
        if self.use_saved_history_policy:
            result["storage_policy"] = storage_policy_witness
        observe(stage="finished", completed_steps=1, status=status,
                storage_day=storage_day.isoformat(), action=expected_action)
        logger.info(
            "canonical_retention_selected_run_finished | storage_day=%s action=%s "
            "status=%s planning_seconds=%.6f elapsed_seconds=%.6f",
            storage_day, expected_action, outcome.get("status"), planning_seconds,
            result["elapsed_seconds"],
        )
        return result

    def _run(self, *, policy, storage_root, after_storage_day, cancelled, storage_policy_witness=None,
             progress=None):
        started = monotonic()
        deadline = started + policy.max_run_seconds
        cursor = after_storage_day if after_storage_day is not None else self._cursor
        explicit_cursor = after_storage_day is not None
        retry_day = None
        retry_attempted = False
        retry_followthrough = False
        if not explicit_cursor and self._retry_states:
            due = [state for state in self._retry_states.values()
                   if state.next_eligible_at <= started]
            if due:
                retry_day = min(due, key=lambda item: item.storage_day).storage_day
                cursor = retry_day - timedelta(days=1)
            elif cursor is None:
                # Skip cooling failures while still allowing later work. The
                # oldest due retry overrides this hint on a later natural run.
                cursor = min(self._retry_states)
        outcomes, initial_plan = [], None
        stop_reason = "step_budget"
        scans = 0
        planning_seconds = 0.0

        def observe(**fields):
            if progress is None:
                return
            try:
                progress({"schema_version": "market.canonical_retention_progress.v1", **fields})
            except Exception:
                logger.warning("canonical_retention_progress_observer_failed", exc_info=True)

        def check_budget():
            if cancelled is not None and cancelled():
                raise CanonicalRetentionStopRequested("canonical_retention_stop_requested: resume committed progress next run")
            if monotonic() >= deadline:
                raise CanonicalRetentionBudgetExceeded("canonical_retention_run_budget_exceeded: resume committed progress next run")

        def remaining_run_seconds():
            check_budget()
            return deadline - monotonic()

        for _ in range(policy.max_steps_per_run):
            planning_started = perf_counter()
            planning_outcome = "failed"
            try:
                observe(stage="planning", completed_steps=len(outcomes), planning_pages=scans,
                        after_storage_day=cursor.isoformat() if cursor else None)
                check_budget()
                plan_policy = replace(policy, max_plan_seconds=min(policy.max_plan_seconds,
                                                                   max(1, int(deadline - monotonic()))))
                plan = self.repository.plan(policy=plan_policy, storage_root=storage_root, after_storage_day=cursor)
                scans += 1
                initial_plan = initial_plan or plan
                check_budget()
                planning_outcome = "completed"
            except CanonicalRetentionBudgetExceeded as exc:
                stop_reason = exc.stop_reason
                break
            finally:
                planning_elapsed = perf_counter() - planning_started
                planning_seconds += planning_elapsed
                logger.info(
                    "canonical_retention_phase | phase=planning outcome=%s seconds=%.6f scan=%s "
                    "after_storage_day=%s",
                    planning_outcome, planning_elapsed, scans, cursor,
                )
            candidates = [item for item in plan["actions"] if item["eligible"]]
            item = None
            if retry_day is not None and not retry_attempted:
                retry_plan_item = next((candidate for candidate in plan["actions"]
                                        if date.fromisoformat(candidate["storage_day"]) == retry_day), None)
                item = retry_plan_item if retry_plan_item is not None and retry_plan_item["eligible"] else None
                if item is not None:
                    retry_attempted = True
                    logger.info(
                        "canonical_retention_retry_selected | storage_day=%s attempt_count=%s",
                        retry_day, self._retry_states[retry_day].attempt_count,
                    )
                elif retry_plan_item is None:
                    # Another committed owner may already have completed and
                    # removed the target. A fresh exact scan is authoritative.
                    retry_attempted = True
                    self._clear_retry(retry_day)
                else:
                    # A changed fresh plan cannot be bypassed. Delay another
                    # attempt and continue unrelated eligible work this run.
                    retry_attempted = True
                    self._schedule_retry(
                        day=retry_day, reason="fresh_plan_not_eligible", policy=policy,
                        now=monotonic(), count_attempt=False,
                    )
            if item is None and not explicit_cursor:
                candidates = [
                    candidate for candidate in candidates
                    if date.fromisoformat(candidate["storage_day"]) not in self._retry_states
                    or date.fromisoformat(candidate["storage_day"]) == retry_day
                ]
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
            item = item or candidates[0]
            day = date.fromisoformat(item["storage_day"])
            # Revisit this day's durable progress on success/budget suspension;
            # skip a failed/busy day this cycle so it cannot starve later days.
            cursor = day - timedelta(days=1)
            prerequisite_commits = []

            def retain_prerequisite_progress(outcome):
                if prerequisite_commits:
                    outcome["prerequisite_commit_count"] = len(prerequisite_commits)
                    outcome["prerequisite_commits"] = list(prerequisite_commits)
                return outcome

            try:
                observe(stage="executing", completed_steps=len(outcomes),
                        storage_day=day.isoformat(), action=item["action"])
                result = self._execute_step(item=item, policy=policy, storage_root=storage_root,
                                           check_budget=check_budget, remaining_seconds=remaining_run_seconds,
                                           storage_policy_witness=storage_policy_witness,
                                           prerequisite_commits=prerequisite_commits)
                outcomes.append({**result, "action": item["action"], "storage_day": day.isoformat()})
                observe(stage="step_finished", completed_steps=len(outcomes),
                        storage_day=day.isoformat(), action=item["action"],
                        status=result.get("status"))
                if result["status"] in {"partition_reclaimed", "already_reclaimed"}:
                    cursor = day
                    self._clear_retry(day)
                    if day == retry_day:
                        retry_followthrough = False
                elif day == retry_day and day in self._retry_states:
                    retry_followthrough = True
                else:
                    self._clear_retry(day)
                logger.info("canonical_retention_step_finished | storage_day=%s action=%s status=%s",
                            day, item["action"], result["status"])
            except CanonicalRetentionBudgetExceeded as exc:
                outcomes.append(retain_prerequisite_progress(
                    {"action": item["action"], "storage_day": day.isoformat(),
                     "status": "deferred", "reason": str(exc)}
                ))
                if day in self._retry_states:
                    self._schedule_retry(
                        day=day, reason=exc.stop_reason, policy=policy,
                        now=monotonic(), count_attempt=False,
                    )
                    if day == retry_day:
                        retry_followthrough = False
                stop_reason = exc.stop_reason
                break
            except MarketStorageLifecycleBusyError as exc:
                self._dispose_archive()
                outcomes.append(retain_prerequisite_progress(
                    {"action": item["action"], "storage_day": day.isoformat(),
                     "status": "deferred", "reason": str(exc)}
                ))
                self._schedule_retry(
                    day=day, reason="lifecycle_busy", policy=policy, now=monotonic(),
                )
                if day == retry_day:
                    retry_followthrough = False
                logger.info("canonical_retention_step_deferred | storage_day=%s action=%s reason=%s", day, item["action"], exc)
                cursor = day
            except Exception as exc:
                sqlstate = getattr(getattr(exc, "orig", None), "pgcode", None)
                if sqlstate == "57014":
                    # The archive owner has already rolled back. A statement
                    # cancelled at the run boundary must retain this day's
                    # resume cursor; a query timeout with budget left is still
                    # a real failure, as are non-cancellation errors.
                    try:
                        check_budget()
                    except CanonicalRetentionBudgetExceeded as budget:
                        outcomes.append(retain_prerequisite_progress(
                            {"action": item["action"], "storage_day": day.isoformat(),
                             "status": "deferred", "reason": str(budget)}
                        ))
                        if day in self._retry_states:
                            self._schedule_retry(
                                day=day, reason=budget.stop_reason, policy=policy,
                                now=monotonic(), count_attempt=False,
                            )
                            if day == retry_day:
                                retry_followthrough = False
                        stop_reason = budget.stop_reason
                        logger.info("canonical_retention_step_deferred | storage_day=%s action=%s reason=%s sqlstate=57014",
                                    day, item["action"], stop_reason)
                        break
                if sqlstate == "55P03":
                    self._dispose_archive(reason="relation_busy")
                    outcomes.append(retain_prerequisite_progress(
                        {"action": item["action"], "storage_day": day.isoformat(),
                         "status": "deferred", "reason": "canonical_retention_relation_busy"}
                    ))
                    self._schedule_retry(
                        day=day, reason="relation_busy", policy=policy, now=monotonic(),
                    )
                    if day == retry_day:
                        retry_followthrough = False
                    logger.info("canonical_retention_step_deferred | storage_day=%s action=%s reason=relation_busy", day, item["action"])
                else:
                    self._dispose_archive(reason="step_failed")
                    outcomes.append(retain_prerequisite_progress(
                        {"action": item["action"], "storage_day": day.isoformat(),
                         "status": "failed", "error": str(exc)}
                    ))
                    self._schedule_retry(
                        day=day, reason=f"{type(exc).__name__}:{sqlstate or 'none'}", policy=policy,
                        now=monotonic(),
                    )
                    if day == retry_day:
                        retry_followthrough = False
                    logger.exception("canonical_retention_step_failed | storage_day=%s action=%s", day, item["action"])
                cursor = day
        if retry_followthrough and retry_day in self._retry_states:
            self._schedule_retry(
                day=retry_day, reason="partition_followthrough", policy=policy,
                now=monotonic(), count_attempt=False,
            )
        self._cursor = cursor
        failures = sum(item["status"] == "failed" for item in outcomes)
        prerequisite_commits = sum(item.get("prerequisite_commit_count", 0) for item in outcomes)
        finished = monotonic()
        result = {"schema_version": "market.canonical_retention_run.v1",
                  "status": "degraded" if failures else "bounded" if stop_reason.endswith("budget") else stop_reason,
                  "stop_reason": stop_reason, "plan": initial_plan, "outcomes": outcomes, "failure_count": failures,
                  "planning_pages": scans, "planning_seconds": planning_seconds,
                  "elapsed_seconds": finished - started,
                  "next_after_storage_day": cursor.isoformat() if cursor else None,
                  "resume_authority": "committed_partition_pages_and_verifications",
                  "prerequisite_commit_count": prerequisite_commits,
                  "retry_schedule": self._retry_schedule(finished)}
        if self.use_saved_history_policy:
            result["storage_policy"] = storage_policy_witness
        observe(stage="finished", completed_steps=len(outcomes), status=result["status"],
                next_after_storage_day=result["next_after_storage_day"])
        logger.info("canonical_retention_run_finished | status=%s steps=%s scans=%s prerequisite_commits=%s failures=%s next_after_storage_day=%s",
                    result["status"], len(outcomes), scans, prerequisite_commits, failures,
                    result["next_after_storage_day"])
        return result

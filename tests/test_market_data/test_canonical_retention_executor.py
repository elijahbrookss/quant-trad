from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from core.market_storage_lifecycle import CanonicalFactRetentionPolicy
from portal.backend.service.market import canonical_retention as module
from portal.backend.service.market.canonical_retention import CanonicalFactRetentionExecutor

DAY = date(2026, 8, 1)


def _item(action="stage_page", day=DAY, eligible=True):
    return {"storage_day": day.isoformat(), "action": action, "eligible": eligible,
            "eligible_before": "2026-08-05", "blockers": [] if eligible else ["canonical_dependency_proof_required"]}


def _plan(actions=(), next_day=None):
    return {"actions": list(actions), "next_after_storage_day": next_day.isoformat() if next_day else None}


def _executor(plans):
    calls = []

    def plan(**kwargs):
        calls.append(kwargs)
        return plans[min(len(calls) - 1, len(plans) - 1)]

    return CanonicalFactRetentionExecutor(repository=SimpleNamespace(plan=plan)), calls


@pytest.mark.parametrize("execute,enabled,status", [(False, False, "dry_run"), (False, True, "dry_run"), (True, False, "disabled")])
def test_dry_run_and_separate_enablement_never_construct_a_writer(tmp_path, monkeypatch, execute, enabled, status):
    executor, calls = _executor([_plan([_item()])])
    monkeypatch.setattr(executor, "_execute_step", lambda **_: pytest.fail("no mutation"))
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=enabled),
                          storage_root=tmp_path / "missing", execute=execute)
    assert result["status"] == status and result["outcomes"] == []
    assert len(calls) == 1 and list(tmp_path.iterdir()) == []


def test_step_budget_and_next_phase_are_driven_by_replanned_progress(tmp_path, monkeypatch):
    executor, calls = _executor([_plan([_item("seal_partition")]), _plan([_item("stage_page")]),
                                 _plan([_item("verify_page")])])
    seen = []

    def step(**kwargs):
        seen.append(kwargs["item"]["action"])
        return {"status": "done"}

    monkeypatch.setattr(executor, "_execute_step", step)
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=2),
                          storage_root=tmp_path, execute=True)
    assert seen == ["seal_partition", "stage_page"]
    assert result["status"] == "bounded" and result["stop_reason"] == "step_budget"
    assert result["next_after_storage_day"] == (DAY - timedelta(days=1)).isoformat()
    executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1),
                 storage_root=tmp_path, execute=True)
    assert seen[-1] == "verify_page" and calls[-1]["after_storage_day"] == DAY - timedelta(days=1)


@pytest.mark.parametrize("action", ["stage_page", "reclaim_partition"])
def test_selected_action_executes_exactly_one_fresh_matching_plan_item(tmp_path, monkeypatch, action):
    executor, calls = _executor([_plan([_item(action), _item(day=DAY + timedelta(days=1))])])
    seen = []
    committed = module._SELECTED_SUCCESS[action]
    monkeypatch.setattr(
        executor, "_execute_step",
        lambda **kwargs: seen.append(kwargs["item"]) or {"status": committed},
    )
    result = executor.run_selected(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True),
        storage_root=tmp_path, storage_day=DAY, expected_action=action, execute=True,
    )
    assert calls == [{
        "policy": calls[0]["policy"], "storage_root": tmp_path,
        "after_storage_day": DAY - timedelta(days=1),
    }]
    assert seen == [_item(action)]
    assert result["schema_version"] == "market.canonical_retention_selected_run.v1"
    assert result["stop_reason"] == "selected_action_complete"
    assert result["outcomes"] == [{
        "status": committed, "action": action, "storage_day": DAY.isoformat(),
    }]


@pytest.mark.parametrize(
    "planned,expected,error",
    [
        ([], "stage_page", "matching_actions=0"),
        ([_item("verify_page")], "stage_page", "actual_action=verify_page"),
        ([_item("stage_page", eligible=False)], "stage_page", "eligible=False"),
        ([_item("stage_page"), _item("stage_page")], "stage_page", "matching_actions=2"),
    ],
)
def test_selected_action_refuses_changed_or_ineligible_selection_before_mutation(
        tmp_path, monkeypatch, planned, expected, error):
    executor, _ = _executor([_plan(planned)])
    monkeypatch.setattr(executor, "_execute_step", lambda **_: pytest.fail("no mutation"))
    with pytest.raises(RuntimeError, match=error):
        executor.run_selected(
            policy=CanonicalFactRetentionPolicy(execution_enabled=True),
            storage_root=tmp_path, storage_day=DAY, expected_action=expected, execute=True,
        )


def test_selected_action_never_uses_retry_schedule_or_runs_a_second_step(tmp_path, monkeypatch):
    executor, calls = _executor([_plan([_item("stage_page")]), _plan([_item("reclaim_partition")])])
    executor._retry_states[DAY] = module._RetryState(DAY, 0, 0, 0, 7, "old failure")
    monkeypatch.setattr(executor, "_execute_step", lambda **_: {"status": "page_acknowledged"})
    result = executor.run_selected(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=64),
        storage_root=tmp_path, storage_day=DAY, expected_action="stage_page", execute=True,
    )
    assert len(calls) == 1
    assert result["outcomes"][0]["status"] == "page_acknowledged"
    assert executor._retry_states[DAY].attempt_count == 7


def test_selected_action_records_one_failure_without_retrying(tmp_path, monkeypatch):
    executor, calls = _executor([_plan([_item("reclaim_partition")])])
    attempts = []

    def fail(**kwargs):
        attempts.append(kwargs["item"])
        raise RuntimeError("handoff proof refused")

    monkeypatch.setattr(executor, "_execute_step", fail)
    result = executor.run_selected(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=64),
        storage_root=tmp_path, storage_day=DAY,
        expected_action="reclaim_partition", execute=True,
    )
    assert len(calls) == 1 and attempts == [_item("reclaim_partition")]
    assert result["status"] == "failed" and result["failure_count"] == 1
    assert result["outcomes"] == [{
        "action": "reclaim_partition", "storage_day": DAY.isoformat(),
        "status": "failed", "error": "handoff proof refused",
    }]
    assert result["stop_reason"] == "selected_action_stopped"


@pytest.mark.parametrize(
    "action,actual",
    [("stage_page", "source_exhausted"),
     ("stage_page", "page_prerequisite_budget"),
     ("reclaim_partition", "already_reclaimed")],
)
def test_selected_action_requires_the_exact_intended_committed_outcome(
        tmp_path, monkeypatch, action, actual):
    executor, calls = _executor([_plan([_item(action)])])
    raw = {"status": actual}
    if actual == "page_prerequisite_budget":
        raw.update(
            reason="canonical_archive_page_prerequisite_budget",
            prerequisite_commit_count=2,
            prerequisite_commits=[{"status": "book_prefix_verified"},
                                  {"status": "trade_prefix_verified"}],
        )
    monkeypatch.setattr(executor, "_execute_step", lambda **_: raw)
    result = executor.run_selected(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True),
        storage_root=tmp_path, storage_day=DAY, expected_action=action, execute=True,
    )
    assert len(calls) == 1
    assert result["status"] == "failed" and result["failure_count"] == 1
    assert result["outcomes"][0]["status"] == "failed"
    assert result["outcomes"][0]["actual_status"] == actual
    assert f"actual_status={actual}" in result["outcomes"][0]["error"]
    if actual == "page_prerequisite_budget":
        assert result["outcomes"][0]["prerequisite_commit_count"] == 2
        assert len(result["outcomes"][0]["prerequisite_commits"]) == 2
        assert result["outcomes"][0]["reason"] == "canonical_archive_page_prerequisite_budget"


def test_selected_action_dry_run_validates_exact_fresh_selection(tmp_path):
    item = _item("stage_page")
    executor, calls = _executor([_plan([item])])
    result = executor.run_selected(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True),
        storage_root=tmp_path, storage_day=DAY,
        expected_action="stage_page", execute=False,
    )
    assert len(calls) == 1 and result["status"] == "dry_run"
    assert result["selected_action"] == item and result["outcomes"] == []

    changed, _ = _executor([_plan([_item("verify_page")])])
    with pytest.raises(RuntimeError, match="actual_action=verify_page"):
        changed.run_selected(
            policy=CanonicalFactRetentionPolicy(execution_enabled=True),
            storage_root=tmp_path, storage_day=DAY,
            expected_action="stage_page", execute=False,
        )


def test_selected_action_rejects_generic_or_unsupported_actions_before_planning(tmp_path):
    executor, calls = _executor([_plan([_item()])])
    with pytest.raises(ValueError, match="selected_request_invalid"):
        executor.run_selected(
            policy=CanonicalFactRetentionPolicy(execution_enabled=True),
            storage_root=tmp_path, storage_day=DAY, expected_action="verify_page", execute=True,
        )
    assert calls == []


def test_blocked_candidate_page_cannot_starve_a_later_eligible_day(tmp_path, monkeypatch):
    executor, calls = _executor([_plan([_item(eligible=False)], next_day=DAY),
                                 _plan([_item(day=DAY + timedelta(days=1))])])
    monkeypatch.setattr(executor, "_execute_step", lambda **_: {"status": "done"})
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=2),
                          storage_root=tmp_path, execute=True)
    assert calls[1]["after_storage_day"] == DAY
    assert result["planning_pages"] == 2 and len(result["outcomes"]) == 1


def test_step_failure_is_visible_and_does_not_starve_next_day(tmp_path, monkeypatch, caplog):
    executor, calls = _executor([_plan([_item()]), _plan([_item(day=DAY + timedelta(days=1))])])

    def step(**kwargs):
        if kwargs["item"]["storage_day"] == DAY.isoformat():
            raise RuntimeError("checksum mismatch")
        return {"status": "page_acknowledged"}

    monkeypatch.setattr(executor, "_execute_step", step)
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=2),
                          storage_root=tmp_path, execute=True)
    assert result["status"] == "degraded" and result["failure_count"] == 1
    assert result["outcomes"][0]["error"] == "checksum mismatch"
    assert calls[1]["after_storage_day"] == DAY
    assert "canonical_retention_step_failed" in caplog.text


def test_failed_oldest_retry_is_delayed_then_selected_without_starving_later_days(
        tmp_path, monkeypatch, caplog):
    executor, calls = _executor([
        _plan([_item()]),
        _plan([_item(day=DAY + timedelta(days=1))]),
        _plan([_item(day=DAY + timedelta(days=2))]),
        _plan([_item()]),
        _plan([_item(day=DAY + timedelta(days=1))]),
        _plan([_item("reclaim_partition")]),
    ])
    clock = [0.0]
    attempts = []
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])

    def step(**kwargs):
        day = date.fromisoformat(kwargs["item"]["storage_day"])
        attempts.append(day)
        if day == DAY and attempts.count(DAY) == 1:
            raise RuntimeError("dependency batch timeout")
        if day == DAY and attempts.count(DAY) == 3:
            return {"status": "partition_reclaimed", "reclaimed_bytes": 4096}
        return {"status": "page_acknowledged"}

    monkeypatch.setattr(executor, "_execute_step", step)
    policy = CanonicalFactRetentionPolicy(
        execution_enabled=True, max_steps_per_run=2, max_run_seconds=10,
    )
    with caplog.at_level("INFO"):
        failed = executor.run(policy=policy, storage_root=tmp_path, execute=True)
        clock[0] = 5.0
        cooling = executor.run(
            policy=replace(policy, max_steps_per_run=1), storage_root=tmp_path, execute=True,
        )
        clock[0] = 10.0
        retried = executor.run(
            policy=replace(policy, max_steps_per_run=1), storage_root=tmp_path, execute=True,
        )
        clock[0] = 15.0
        followthrough_cooling = executor.run(
            policy=replace(policy, max_steps_per_run=1), storage_root=tmp_path, execute=True,
        )
        clock[0] = 20.0
        completed = executor.run(
            policy=replace(policy, max_steps_per_run=1), storage_root=tmp_path, execute=True,
        )

    assert failed["retry_schedule"] == [{
        "storage_day": DAY.isoformat(), "attempt_count": 1,
        "reason": "RuntimeError:none",
        "age_seconds": 0.0, "next_eligible_in_seconds": 10.0,
    }]
    assert cooling["outcomes"][0]["storage_day"] == (DAY + timedelta(days=2)).isoformat()
    assert retried["outcomes"][0]["storage_day"] == DAY.isoformat()
    assert retried["retry_schedule"] == [{
        "storage_day": DAY.isoformat(), "attempt_count": 1,
        "reason": "partition_followthrough",
        "age_seconds": 10.0, "next_eligible_in_seconds": 10.0,
    }]
    assert followthrough_cooling["outcomes"][0]["storage_day"] == (DAY + timedelta(days=1)).isoformat()
    assert completed["outcomes"][0]["status"] == "partition_reclaimed"
    assert completed["retry_schedule"] == []
    assert attempts == [DAY, DAY + timedelta(days=1), DAY + timedelta(days=2), DAY,
                        DAY + timedelta(days=1), DAY]
    assert calls[-1]["after_storage_day"] == DAY - timedelta(days=1)
    assert caplog.text.count("canonical_retention_retry_selected") == 2
    assert "canonical_retention_retry_cleared" in caplog.text


def test_due_retry_keeps_partition_for_one_bounded_run_before_cooling(tmp_path, monkeypatch):
    executor, _ = _executor([
        _plan([_item()]),
        _plan([_item(day=DAY + timedelta(days=1))]),
        _plan([_item()]),
        _plan([_item()]),
    ])
    clock = [0.0]
    attempts = []
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])

    def step(**kwargs):
        day = date.fromisoformat(kwargs["item"]["storage_day"])
        attempts.append(day)
        if attempts == [DAY]:
            raise RuntimeError("dependency batch timeout")
        return {"status": "page_acknowledged"}

    monkeypatch.setattr(executor, "_execute_step", step)
    policy = CanonicalFactRetentionPolicy(
        execution_enabled=True, max_steps_per_run=2, max_run_seconds=10,
    )
    executor.run(policy=policy, storage_root=tmp_path, execute=True)
    clock[0] = 10.0
    retried = executor.run(policy=policy, storage_root=tmp_path, execute=True)

    assert [item["storage_day"] for item in retried["outcomes"]] == [DAY.isoformat()] * 2
    assert retried["retry_schedule"] == [{
        "storage_day": DAY.isoformat(), "attempt_count": 1,
        "reason": "partition_followthrough",
        "age_seconds": 10.0, "next_eligible_in_seconds": 10.0,
    }]
    assert attempts == [DAY, DAY + timedelta(days=1), DAY, DAY]


def test_worker_restart_discards_retry_hint_and_resumes_from_committed_oldest_progress(
        tmp_path, monkeypatch):
    failed_executor, _ = _executor([_plan([_item()])])
    monkeypatch.setattr(
        failed_executor, "_execute_step",
        lambda **_: (_ for _ in ()).throw(RuntimeError("dependency batch timeout")),
    )
    failed = failed_executor.run(
        policy=CanonicalFactRetentionPolicy(
            execution_enabled=True, max_steps_per_run=1, max_run_seconds=10,
        ),
        storage_root=tmp_path, execute=True,
    )
    assert failed["retry_schedule"][0]["storage_day"] == DAY.isoformat()

    restarted, calls = _executor([_plan([_item()])])
    monkeypatch.setattr(restarted, "_execute_step", lambda **_: {"status": "page_acknowledged"})
    result = restarted.run(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1),
        storage_root=tmp_path, execute=True,
    )
    assert calls[0]["after_storage_day"] is None
    assert result["outcomes"][0]["status"] == "page_acknowledged"
    assert result["retry_schedule"] == []


def test_time_budget_after_planning_prevents_starting_a_mutation(tmp_path, monkeypatch):
    executor, _ = _executor([_plan([_item()])])
    clock = iter([0, 0, 0, 2, 2])
    monkeypatch.setattr(module, "monotonic", lambda: next(clock))
    monkeypatch.setattr(executor, "_execute_step", lambda **_: pytest.fail("past run budget"))
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_run_seconds=1),
                          storage_root=tmp_path, execute=True)
    assert result["stop_reason"] == "time_budget" and result["outcomes"] == []


def test_new_executor_restarts_scan_not_committed_storage_progress(tmp_path, monkeypatch):
    executor, calls = _executor([_plan([_item("reclaim_partition")])])
    monkeypatch.setattr(executor, "_execute_step", lambda **_: {"status": "partition_reclaimed", "reclaimed_bytes": 4096})
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1),
                          storage_root=tmp_path, execute=True)
    assert calls[0]["after_storage_day"] is None
    assert result["outcomes"][0]["reclaimed_bytes"] == 4096
    assert result["next_after_storage_day"] == DAY.isoformat()


def test_fresh_mount_and_headroom_are_rechecked_even_with_an_eligible_plan(tmp_path):
    executor = CanonicalFactRetentionExecutor(repository=SimpleNamespace(_filesystem=lambda _: {"status": "unavailable"}))
    with pytest.raises(RuntimeError, match="archive_admission_failed"):
        executor._require_archive(policy=CanonicalFactRetentionPolicy(), storage_root=tmp_path, action="reclaim_partition")


def test_worker_stop_request_preserves_resume_cursor_and_starts_no_more_work(tmp_path, monkeypatch):
    executor, _ = _executor([_plan([_item()])])
    stopped = False

    def step(**kwargs):
        nonlocal stopped
        stopped = True
        kwargs["check_budget"]()

    monkeypatch.setattr(executor, "_execute_step", step)
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True), storage_root=tmp_path,
                          execute=True, cancelled=lambda: stopped)
    assert result["stop_reason"] == "stop_requested" and result["failure_count"] == 0
    assert result["outcomes"][0]["status"] == "deferred"
    assert result["next_after_storage_day"] == (DAY - timedelta(days=1)).isoformat()


def test_stage_page_commits_bounded_prerequisites_then_publishes_and_replans(tmp_path, monkeypatch):
    executor, plans = _executor([_plan([_item()]), _plan([_item("verify_page")])])
    executor.repository.database = object()
    monkeypatch.setattr(executor, "_require_archive", lambda **_: None)
    calls, verification_calls = [], []

    class Archive:
        def __init__(self, **kwargs):
            self.check_budget = kwargs["check_budget"]

        def stage_next_page(self, day):
            self.check_budget()
            calls.append(day)
            if len(calls) <= 2:
                return {"status": "book_prefix_verified", "storage_day": day.isoformat(),
                        "chunk_id": f"prefix-{len(calls)}"}
            if len(calls) == 3:
                return {"status": "page_acknowledged", "storage_day": day.isoformat(), "page_ordinal": 7}
            pytest.fail("the same-page loop must stop at publication")

        def verify_next_page(self, day):
            verification_calls.append(day)
            return {"status": "page_verified", "storage_day": day.isoformat(), "page_ordinal": 7}

    monkeypatch.setattr(module, "PostgresCanonicalFactArchiveRepository", Archive)
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=2),
                          storage_root=tmp_path, execute=True)
    assert len(plans) == 2 and calls == [DAY, DAY, DAY] and verification_calls == [DAY]
    assert result["prerequisite_commit_count"] == 2
    assert result["outcomes"] == [{
        "status": "page_acknowledged", "storage_day": DAY.isoformat(), "page_ordinal": 7,
        "prerequisite_commit_count": 2,
        "prerequisite_commits": [
            {"status": "book_prefix_verified", "chunk_id": "prefix-1"},
            {"status": "book_prefix_verified", "chunk_id": "prefix-2"},
        ],
        "action": "stage_page",
    }, {
        "status": "page_verified", "storage_day": DAY.isoformat(), "page_ordinal": 7,
        "action": "verify_page",
    }]


def test_stage_page_prerequisite_allowance_is_hard_bounded(tmp_path, monkeypatch):
    executor, plans = _executor([_plan([_item()])])
    executor.repository.database = object()
    monkeypatch.setattr(executor, "_require_archive", lambda **_: None)
    monkeypatch.setattr(module, "_MAX_PAGE_PREREQUISITE_COMMITS", 3)
    calls = []

    class Archive:
        def __init__(self, **kwargs):
            self.check_budget = kwargs["check_budget"]

        def stage_next_page(self, day):
            self.check_budget()
            calls.append(day)
            return {"status": "trade_prefix_verified", "storage_day": day.isoformat(),
                    "chunk_id": f"prefix-{len(calls)}"}

    monkeypatch.setattr(module, "PostgresCanonicalFactArchiveRepository", Archive)
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1),
                          storage_root=tmp_path, execute=True)
    assert len(plans) == 1 and calls == [DAY, DAY, DAY]
    assert result["prerequisite_commit_count"] == 3
    assert result["outcomes"][0]["status"] == "page_prerequisite_budget"
    assert result["outcomes"][0]["reason"] == "canonical_archive_page_prerequisite_budget"
    assert len(result["outcomes"][0]["prerequisite_commits"]) == 3


@pytest.mark.parametrize(
    "cause,expected_status,expected_stop_reason,expected_calls",
    [
        ("cancel", "deferred", "stop_requested", 1),
        ("deadline", "deferred", "time_budget", 1),
        ("refusal", "failed", "step_budget", 2),
    ],
)
def test_stage_page_retains_committed_prerequisites_when_later_inner_call_stops(
    tmp_path, monkeypatch, cause, expected_status, expected_stop_reason, expected_calls,
):
    executor, _ = _executor([_plan([_item()])])
    executor.repository.database = object()
    monkeypatch.setattr(executor, "_require_archive", lambda **_: None)
    clock, stopped, calls = [0.0], [False], []
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])

    class Archive:
        def __init__(self, **kwargs):
            self.check_budget = kwargs["check_budget"]

        def stage_next_page(self, day):
            calls.append(day)
            if len(calls) == 1:
                if cause == "cancel":
                    stopped[0] = True
                elif cause == "deadline":
                    clock[0] = 2.0
                return {"status": "book_prefix_verified", "storage_day": day.isoformat(),
                        "chunk_id": "committed-prefix"}
            raise RuntimeError("current admission refusal")

    monkeypatch.setattr(module, "PostgresCanonicalFactArchiveRepository", Archive)
    result = executor.run(
        policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1, max_run_seconds=1),
        storage_root=tmp_path, execute=True, cancelled=lambda: stopped[0],
    )
    outcome = result["outcomes"][0]
    assert len(calls) == expected_calls
    assert outcome["status"] == expected_status and result["stop_reason"] == expected_stop_reason
    assert outcome["prerequisite_commit_count"] == 1
    assert outcome["prerequisite_commits"] == [
        {"status": "book_prefix_verified", "chunk_id": "committed-prefix"}
    ]
    assert result["prerequisite_commit_count"] == 1
    if cause == "refusal":
        assert outcome["error"] == "current admission refusal"


def test_staging_repository_is_reused_only_within_one_bounded_run(tmp_path, monkeypatch):
    executor, _ = _executor([_plan([_item()])])
    executor.repository.database = object()
    monkeypatch.setattr(executor, "_require_archive", lambda **_: None)
    monkeypatch.setattr(module, "_MAX_PAGE_PREREQUISITE_COMMITS", 2)
    instances, calls = [], []

    class Archive:
        def __init__(self, **kwargs):
            instances.append(self)
            self.check_budget = kwargs["check_budget"]

        def stage_next_page(self, day):
            self.check_budget()
            calls.append(self)
            return {"status": "book_prefix_verified", "storage_day": day.isoformat()}

    monkeypatch.setattr(module, "PostgresCanonicalFactArchiveRepository", Archive)
    policy = CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1)
    for _ in range(2):
        result = executor.run(policy=policy, storage_root=tmp_path, execute=True)
        assert result["stop_reason"] == "step_budget" and len(result["outcomes"]) == 1
        assert result["outcomes"][0]["status"] == "page_prerequisite_budget"
        assert result["outcomes"][0]["prerequisite_commit_count"] == 2
        assert executor._archive is None
    assert len(instances) == 2
    assert calls == [instances[0], instances[0], instances[1], instances[1]]


@pytest.mark.parametrize("cause", ["time_budget", "stop_requested"])
def test_sql_cancellation_at_run_boundary_defers_and_revisits_same_day(
    tmp_path, monkeypatch, caplog, cause,
):
    from sqlalchemy.exc import DBAPIError

    executor, calls = _executor([_plan([_item()])])
    clock = [0.0]
    stopped = [False]
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    error = RuntimeError("canceling statement due to statement timeout")
    error.pgcode = "57014"

    def step(**kwargs):
        if cause == "time_budget":
            clock[0] = 1.0
        else:
            stopped[0] = True
        raise DBAPIError("SELECT fixture", {}, error)

    monkeypatch.setattr(executor, "_execute_step", step)
    policy = CanonicalFactRetentionPolicy(execution_enabled=True, max_run_seconds=1)
    with caplog.at_level("INFO"):
        result = executor.run(policy=policy, storage_root=tmp_path, execute=True,
                              cancelled=lambda: stopped[0])
    assert result["stop_reason"] == cause and result["failure_count"] == 0
    assert result["outcomes"][0]["status"] == "deferred"
    assert result["next_after_storage_day"] == (DAY - timedelta(days=1)).isoformat()
    assert "canonical_retention_step_deferred" in caplog.text
    assert "sqlstate=57014" in caplog.text
    assert "canonical_retention_step_failed" not in caplog.text
    assert executor._archive is None

    clock[0] = 2.0
    stopped[0] = False
    monkeypatch.setattr(executor, "_execute_step", lambda **_: {"status": "page_acknowledged"})
    result = executor.run(policy=replace(policy, max_steps_per_run=1),
                          storage_root=tmp_path, execute=True)
    assert calls[-1]["after_storage_day"] == DAY - timedelta(days=1)
    assert result["outcomes"][0]["status"] == "page_acknowledged"


@pytest.mark.parametrize("sqlstate,expired", [("57014", False), ("XX000", True), (None, True)])
def test_sql_or_data_failure_is_not_hidden_by_run_budget(tmp_path, monkeypatch, caplog, sqlstate, expired):
    from sqlalchemy.exc import DBAPIError

    executor, _ = _executor([_plan([_item()])])
    clock = [0.0]
    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    original = RuntimeError("actual query or data failure")
    original.pgcode = sqlstate
    error = DBAPIError("SELECT fixture", {}, original) if sqlstate else original

    def step(**kwargs):
        clock[0] = 2.0 if expired else 0.5
        raise error

    monkeypatch.setattr(executor, "_execute_step", step)
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True,
                          max_run_seconds=1, max_steps_per_run=1), storage_root=tmp_path, execute=True)
    assert result["status"] == "degraded" and result["failure_count"] == 1
    assert result["outcomes"][0]["status"] == "failed"
    assert "actual query or data failure" in result["outcomes"][0]["error"]
    assert result["next_after_storage_day"] == DAY.isoformat()
    assert "canonical_retention_step_failed" in caplog.text


@pytest.mark.parametrize("seconds", [1, 10, 60])
def test_final_handoff_policy_reaches_reclaimer_without_expanding_statement_bound(tmp_path, monkeypatch, seconds):
    executor, _ = _executor([])
    executor.repository.database = object()
    seen = []
    monkeypatch.setattr(executor, "_require_archive", lambda **_: None)
    monkeypatch.setattr(module, "PostgresCanonicalFactArchiveRepository",
                        lambda **kwargs: SimpleNamespace(**kwargs))

    class Reclaimer:
        def __init__(self, **kwargs):
            seen.append(kwargs)

        def reclaim_partition(self, day, **kwargs):
            assert day == DAY and kwargs["execute"] is True
            return {"status": "partition_reclaimed"}

    monkeypatch.setattr(module, "PostgresCanonicalFactReclamationRepository", Reclaimer)
    policy = CanonicalFactRetentionPolicy(max_handoff_seconds=seconds,
                                          pinned_ranges_statement_timeout_ms=4321)
    executor._execute_step(item=_item("reclaim_partition"), policy=policy,
        storage_root=tmp_path, check_budget=lambda: None, remaining_seconds=600)
    assert seen[0]["enabled"] is True
    assert seen[0]["limits"].max_handoff_seconds == seconds
    assert seen[0]["limits"].statement_timeout_ms == 1000
    assert seen[0]["limits"].pinned_ranges_statement_timeout_ms == 4321


@pytest.mark.parametrize("execution,pinned,remaining,expected", [
    (5000, 4321, 10, 4321),
    (3000, 5000, 10, 3000),
    (5000, 5000, 2, 2000),
])
def test_pinned_range_timeout_composes_separate_query_execution_and_run_bounds(
    tmp_path, monkeypatch, execution, pinned, remaining, expected,
):
    executor, _ = _executor([])
    executor.repository.database = object()
    actual_reclaimer = module.PostgresCanonicalFactReclamationRepository
    seen = {}
    monkeypatch.setattr(executor, "_require_archive", lambda **_: None)

    class Archive(SimpleNamespace):
        def _check_budget(self):
            self.check_budget()

    monkeypatch.setattr(module, "PostgresCanonicalFactArchiveRepository",
                        lambda **kwargs: Archive(**kwargs))

    class Reclaimer:
        def __init__(self, **kwargs):
            self.inner = actual_reclaimer(**kwargs)
            seen["limits"] = kwargs["limits"]

        def reclaim_partition(self, *_args, **_kwargs):
            seen["effective"] = self.inner._pinned_ranges_timeout_ms()
            return {"status": "partition_reclaimed"}

    monkeypatch.setattr(module, "PostgresCanonicalFactReclamationRepository", Reclaimer)
    policy = CanonicalFactRetentionPolicy(
        execution_statement_timeout_ms=execution,
        pinned_ranges_statement_timeout_ms=pinned,
    )
    executor._execute_step(item=_item("reclaim_partition"), policy=policy,
        storage_root=tmp_path, check_budget=lambda: None, remaining_seconds=remaining)
    assert seen["limits"].statement_timeout_ms == 1000
    assert seen["limits"].pinned_ranges_statement_timeout_ms == expected
    assert seen["effective"] == expected


def test_progress_observer_reports_existing_boundaries_without_owning_execution(tmp_path, monkeypatch):
    executor, _ = _executor([_plan([_item("reclaim_partition")])])
    monkeypatch.setattr(executor, "_execute_step",
                        lambda **_: {"status": "partition_reclaimed", "reclaimed_bytes": 7})
    reports = []
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1),
                          storage_root=tmp_path, execute=True, progress=reports.append)
    assert result["outcomes"][0]["reclaimed_bytes"] == 7
    assert [item["stage"] for item in reports] == ["planning", "executing", "step_finished", "finished"]
    assert reports[1]["storage_day"] == DAY.isoformat()
    assert reports[2]["status"] == "partition_reclaimed"
    assert reports[-1]["completed_steps"] == 1


def test_progress_observer_failure_cannot_change_retention_semantics(tmp_path, monkeypatch):
    executor, _ = _executor([_plan([_item()])])
    monkeypatch.setattr(executor, "_execute_step", lambda **_: {"status": "page_acknowledged"})
    result = executor.run(policy=CanonicalFactRetentionPolicy(execution_enabled=True, max_steps_per_run=1),
                          storage_root=tmp_path, execute=True,
                          progress=lambda _: (_ for _ in ()).throw(RuntimeError("observer failed")))
    assert result["failure_count"] == 0
    assert result["outcomes"][0]["status"] == "page_acknowledged"

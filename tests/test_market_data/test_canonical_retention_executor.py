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

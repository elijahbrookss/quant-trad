from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("check_release_ci", Path(__file__).resolve().parents[1] / "scripts/automation/check_release_ci.py")
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
SHA = "a" * 40


def response():
    run = dict(id=10, head_sha=SHA, event="push", head_branch="develop", status="completed", conclusion="success", run_attempt=2, html_url="https://github.com/example/actions/runs/10")
    jobs = [dict(name=name, status="completed", conclusion="success") for name in ci.REQUIRED_JOBS]
    return run, dict(total_count=len(jobs), jobs=jobs)


def test_requires_exact_latest_push_run_and_complete_attempt(monkeypatch):
    run, jobs = response()
    calls = []
    def api(path):
        calls.append(path)
        return {"workflow_runs": [run]} if "workflows/" in path else jobs
    monkeypatch.setattr(ci, "api", api)
    assert ci.qualify(SHA)["attempt"] == 2
    assert "head_sha=" + SHA in calls[0]
    assert "attempts/2/jobs" in calls[1]


@pytest.mark.parametrize("field,value", [("head_sha", "b" * 40), ("event", "pull_request"), ("head_branch", "feature"), ("status", "in_progress"), ("conclusion", "failure")])
def test_rejects_wrong_or_incomplete_run(monkeypatch, field, value):
    run, _ = response()
    run[field] = value
    monkeypatch.setattr(ci, "api", lambda _: {"workflow_runs": [run]})
    with pytest.raises(ValueError):
        ci.qualify(SHA)


@pytest.mark.parametrize("conclusion", ["skipped", "neutral", "cancelled", "failure", None])
def test_rejects_non_successful_required_job(monkeypatch, conclusion):
    run, jobs = response()
    jobs["jobs"][0]["conclusion"] = conclusion
    monkeypatch.setattr(ci, "api", lambda path: {"workflow_runs": [run]} if "workflows/" in path else jobs)
    with pytest.raises(ValueError, match="required CI job"):
        ci.qualify(SHA)


def test_newer_pending_run_cannot_use_older_success(monkeypatch):
    run, _ = response()
    newer = {**run, "id": 11, "status": "in_progress", "conclusion": None}
    monkeypatch.setattr(ci, "api", lambda _: {"workflow_runs": [run, newer]})
    with pytest.raises(ValueError):
        ci.qualify(SHA)


def test_missing_run_or_job_fails_closed(monkeypatch):
    monkeypatch.setattr(ci, "api", lambda _: {"workflow_runs": []})
    with pytest.raises(ValueError):
        ci.qualify(SHA)
    run, jobs = response()
    jobs["jobs"].pop()
    monkeypatch.setattr(ci, "api", lambda path: {"workflow_runs": [run]} if "workflows/" in path else jobs)
    with pytest.raises(ValueError):
        ci.qualify(SHA)

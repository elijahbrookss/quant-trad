from __future__ import annotations

from copy import deepcopy
import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("check_release_ci", ROOT / "scripts/automation/check_release_ci.py")
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
SHA = "a" * 40


def response():
    # Derive real expanded labels from the workflow, not the admission constant.
    workflow = yaml.safe_load((ROOT / ".github/workflows/test.yaml").read_text())
    labels = []
    for name, job in workflow["jobs"].items():
        matrix = job.get("strategy", {}).get("matrix", {}).get("runtime")
        labels.extend(f"{name} ({runtime})" for runtime in matrix) if matrix else labels.append(name)
    run = dict(id=10, head_sha=SHA, event="push", head_branch="develop", path=".github/workflows/test.yaml",
               status="completed", conclusion="success", run_attempt=2,
               repository=dict(full_name=ci.REPOSITORY), head_repository=dict(full_name=ci.REPOSITORY),
               html_url=f"https://github.com/{ci.REPOSITORY}/actions/runs/10")
    jobs = [dict(id=index+1, name=name, head_sha=SHA, run_id=10, run_attempt=2,
                 status="completed", conclusion="success", started_at="2026-10-10T01:00:00Z",
                 completed_at="2026-10-10T02:00:00Z",
                 steps=[dict(name="Attest and pack disposable qualification evidence", conclusion="success")])
            for index, name in enumerate(labels)]
    artifacts = [dict(id=index+1, name="committed-recovery-"+runtime, expired=False,
                      digest="sha256:"+"b"*64, created_at="2026-10-10T01:59:00Z",
                      archive_download_url=f"https://api.github.com/repos/{ci.REPOSITORY}/actions/artifacts/{index+1}/zip",
                      workflow_run=dict(id=10, head_sha=SHA))
                 for index, runtime in enumerate(workflow["jobs"]["committed-recovery"]["strategy"]["matrix"]["runtime"])]
    return run, dict(total_count=len(jobs), jobs=jobs), dict(total_count=len(artifacts), artifacts=artifacts)


def mock_api(monkeypatch, run, jobs, artifacts, calls=None):
    def api(path):
        if calls is not None:
            calls.append(path)
        if "workflows/" in path:
            return {"workflow_runs": [run]}
        return artifacts if "/artifacts?" in path else jobs
    monkeypatch.setattr(ci, "api", api)


def test_requires_exact_latest_push_run_and_complete_attempt(monkeypatch):
    run, jobs, artifacts = response()
    calls = []
    mock_api(monkeypatch, run, jobs, artifacts, calls)
    receipt = ci.qualify(SHA)
    assert receipt["attempt"] == 2
    assert receipt["event"] == "release_ci_qualified"
    assert len(receipt["jobs"]) == 7
    assert {job["name"] for job in jobs["jobs"]} == ci.REQUIRED_JOBS
    assert len(receipt["artifacts"]) == 2
    assert "head_sha=" + SHA in calls[0]
    assert "attempts/2/jobs" in calls[1]
    run["head_branch"] = "feature/handoff"
    receipt = ci.qualify(SHA, handoff_branch="feature/handoff")
    assert receipt["event"] == "handoff_ci_qualified"
    assert "branch=feature%2Fhandoff" in calls[-3]
    with pytest.raises(ValueError):
        ci.qualify(SHA)  # Work-branch evidence never admits deployment.
    with pytest.raises(ValueError):
        ci.qualify(SHA, handoff_branch="develop")


@pytest.mark.parametrize("field,value", [("head_sha", "b" * 40), ("event", "pull_request"), ("head_branch", "feature"), ("status", "in_progress"), ("conclusion", "failure")])
def test_rejects_wrong_or_incomplete_run(monkeypatch, field, value):
    run, jobs, artifacts = response()
    run[field] = value
    mock_api(monkeypatch, run, jobs, artifacts)
    with pytest.raises(ValueError):
        ci.qualify(SHA)


@pytest.mark.parametrize("conclusion", ["skipped", "neutral", "cancelled", "failure", None])
def test_rejects_non_successful_required_job(monkeypatch, conclusion):
    run, jobs, artifacts = response()
    jobs["jobs"][0]["conclusion"] = conclusion
    mock_api(monkeypatch, run, jobs, artifacts)
    with pytest.raises(ValueError, match="required CI job"):
        ci.qualify(SHA)


def test_newer_pending_run_cannot_use_older_success(monkeypatch):
    run, _, _ = response()
    newer = {**run, "id": 11, "status": "in_progress", "conclusion": None}
    monkeypatch.setattr(ci, "api", lambda _: {"workflow_runs": [run, newer]})
    with pytest.raises(ValueError):
        ci.qualify(SHA)


def test_missing_run_or_job_fails_closed(monkeypatch):
    monkeypatch.setattr(ci, "api", lambda _: {"workflow_runs": []})
    with pytest.raises(ValueError):
        ci.qualify(SHA)
    run, original_jobs, original_artifacts = response()
    for omitted in (set(ci.RECOVERY_JOBS), {next(iter(ci.RECOVERY_JOBS))}, {"pr-suite"}):
        jobs = deepcopy(original_jobs)
        jobs["jobs"] = [job for job in jobs["jobs"] if job["name"] not in omitted]
        jobs["total_count"] = len(jobs["jobs"])
        mock_api(monkeypatch, run, jobs, original_artifacts)
        with pytest.raises(ValueError, match="required CI job"):
            ci.qualify(SHA)
    for field, value in (("run_attempt", 1), ("head_sha", "c"*40), ("run_id", 9)):
        jobs = deepcopy(original_jobs)
        jobs["jobs"][0][field] = value
        mock_api(monkeypatch, run, jobs, original_artifacts)
        with pytest.raises(ValueError, match="required CI job"):
            ci.qualify(SHA)
    for field, value in (("expired", True), ("digest", ""), ("created_at", "2026-10-09T01:59:00Z"),
                         ("workflow_run", {"id": 10, "head_sha": "c"*40}),
                         ("digest", "sha256:malformed"), ("workflow_run", {"id": 9, "head_sha": SHA})):
        artifacts = deepcopy(original_artifacts)
        artifacts["artifacts"][0][field] = value
        mock_api(monkeypatch, run, original_jobs, artifacts)
        with pytest.raises(ValueError, match="required CI artifact"):
            ci.qualify(SHA)
    for missing in ("digest", "workflow_run"):
        artifacts = deepcopy(original_artifacts)
        artifacts["artifacts"][0].pop(missing)
        mock_api(monkeypatch, run, original_jobs, artifacts)
        with pytest.raises(ValueError, match="required CI artifact"):
            ci.qualify(SHA)
    artifacts = deepcopy(original_artifacts)
    artifacts["artifacts"] = []
    artifacts["total_count"] = 0
    mock_api(monkeypatch, run, original_jobs, artifacts)
    with pytest.raises(ValueError, match="required CI artifact"):
        ci.qualify(SHA)
    for field, value in (("event", "pull_request"), ("head_sha", "d"*40),
                         ("head_repository", {"full_name": "fork-owner/quant-trad"}),
                         ("repository", {"full_name": "fork-owner/quant-trad"})):
        candidate = {**run, "head_branch": "feature/handoff", field: value}
        mock_api(monkeypatch, candidate, original_jobs, original_artifacts)
        with pytest.raises(ValueError):
            ci.qualify(SHA, handoff_branch="feature/handoff")
    jobs = deepcopy(original_jobs)
    jobs["jobs"][0]["name"] = "wrong-job"
    mock_api(monkeypatch, run, jobs, original_artifacts)
    with pytest.raises(ValueError, match="required CI job"):
        ci.qualify(SHA)
    jobs = deepcopy(original_jobs)
    for job in jobs["jobs"]:
        job["steps"] = []
    mock_api(monkeypatch, run, jobs, original_artifacts)
    with pytest.raises(ValueError, match="source attestation"):
        ci.qualify(SHA)


@pytest.mark.parametrize("conclusion", ["success", "failure"])
def test_promotion_rehearsal_fixture_matches_strict_admission(tmp_path, monkeypatch, conclusion):
    import json
    import os
    import subprocess
    import sys
    rehearsal_spec = importlib.util.spec_from_file_location(
        "test_server_promotion_fixture", ROOT / "scripts/ci/test_server_promotion.py")
    rehearsal = importlib.util.module_from_spec(rehearsal_spec)
    rehearsal_spec.loader.exec_module(rehearsal)
    fake_gh = tmp_path / "gh"
    fake_gh.write_text(rehearsal.GITHUB_API_FIXTURE)
    calls = []
    def fixture_api(path):
        calls.append(path)
        result = subprocess.run([sys.executable, str(fake_gh), "api", path], check=True,
            capture_output=True, text=True, timeout=10,
            env={**os.environ, "QT_FIXTURE_CI": conclusion})
        return json.loads(result.stdout)
    monkeypatch.setattr(ci, "api", fixture_api)
    if conclusion == "failure":
        with pytest.raises(ValueError, match="not successful or has wrong identity"):
            ci.qualify(SHA)
    else:
        receipt = ci.qualify(SHA)
        assert receipt["revision"] == SHA
        assert receipt["event"] == "release_ci_qualified"
        assert {job["name"] for job in receipt["jobs"]} == ci.REQUIRED_JOBS
        assert len(receipt["artifacts"]) == 2
        assert any("attempts/1/jobs" in path for path in calls)

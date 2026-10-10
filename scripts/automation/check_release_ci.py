#!/usr/bin/env python3
"""Exact-source CI evidence; deployment remains develop-push-only."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import re
import subprocess
from urllib.parse import quote

REPOSITORY = "elijahbrookss/quant-trad"
FIRST_RELEASE_RUNTIME = "24357ff387776822f676ae2a8b1cce7f209c313e"
RECOVERY_JOBS = {
    "committed-recovery (current)": "committed-recovery-current",
    f"committed-recovery ({FIRST_RELEASE_RUNTIME})": f"committed-recovery-{FIRST_RELEASE_RUNTIME}",
}
REQUIRED_JOBS = {"pr-suite", "frontend", "deployment-contract", "clean-database-bootstrap", "deployment-rehearsal", *RECOVERY_JOBS}
ATTESTATION_STEP = "Attest and pack disposable qualification evidence"


def api(path: str) -> dict:
    result = subprocess.run(["gh", "api", f"repos/{REPOSITORY}/{path}"],
                            check=True, capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout)


def _time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def qualify(revision: str, *, handoff_branch: str | None = None) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("a full lowercase commit SHA is required")
    branch = "develop" if handoff_branch is None else handoff_branch
    if handoff_branch is not None and not re.fullmatch(r"(?:feature|hotfix)/[^\s?&#]+", branch):
        raise ValueError("handoff requires an explicit feature/ or hotfix/ branch")
    runs = api(f"actions/workflows/test.yaml/runs?head_sha={revision}&event=push&branch={quote(branch, safe='')}&per_page=100")["workflow_runs"]
    if not runs:
        raise ValueError(f"no {branch} push CI run exists for this exact commit")
    run = max(runs, key=lambda value: value["id"])
    if (run["head_sha"] != revision or run["event"] != "push"
            or run["head_branch"] != branch or run.get("path") != ".github/workflows/test.yaml"
            or run["status"] != "completed" or run["conclusion"] != "success"):
        raise ValueError("latest exact-commit push CI run is not successful or has wrong identity")
    jobs_response = api(f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100")
    if jobs_response["total_count"] > 100 or jobs_response["total_count"] != len(jobs_response["jobs"]):
        raise ValueError("CI job response is incomplete or exceeds the bounded admission query")
    selected = {}
    for name in sorted(REQUIRED_JOBS):
        matches = [job for job in jobs_response["jobs"] if job["name"] == name]
        if (len(matches) != 1 or matches[0]["status"] != "completed" or matches[0]["conclusion"] != "success"
                or matches[0].get("head_sha") != revision or matches[0].get("run_id") != run["id"]
                or matches[0].get("run_attempt") != run["run_attempt"]):
            raise ValueError(f"required CI job did not succeed in this exact attempt: {name}")
        selected[name] = matches[0]
    artifacts_response = api(f"actions/runs/{run['id']}/artifacts?per_page=100")
    if artifacts_response["total_count"] > 100 or artifacts_response["total_count"] != len(artifacts_response["artifacts"]):
        raise ValueError("CI artifact response is incomplete or exceeds the bounded admission query")
    artifacts = []
    for job_name, artifact_name in RECOVERY_JOBS.items():
        job = selected[job_name]
        attestation = [step for step in job.get("steps", []) if step.get("name") == ATTESTATION_STEP]
        if len(attestation) != 1 or attestation[0].get("conclusion") != "success":
            raise ValueError(f"required CI source attestation did not succeed: {job_name}")
        matches = [item for item in artifacts_response["artifacts"] if item["name"] == artifact_name
                   and _time(job["started_at"]) <= _time(item["created_at"]) <= _time(job["completed_at"])]
        if (len(matches) != 1 or matches[0].get("expired") is not False
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", matches[0].get("digest", ""))
                or matches[0].get("workflow_run", {}).get("id") != run["id"]
                or matches[0].get("workflow_run", {}).get("head_sha") != revision):
            raise ValueError(f"required CI artifact is absent, expired, or has wrong identity: {artifact_name}")
        item = matches[0]
        artifacts.append({key: item[key] for key in ("id", "name", "digest", "created_at", "archive_download_url")})
    return {"event": "release_ci_qualified" if handoff_branch is None else "handoff_ci_qualified",
            "revision": revision, "branch": branch, "workflow": run["path"], "run_id": run["id"],
            "run_url": run["html_url"], "attempt": run["run_attempt"],
            "jobs": [{"name": name, "id": job["id"]} for name, job in sorted(selected.items())],
            "artifacts": artifacts, "attestation_step": ATTESTATION_STEP,
            "scope": "CI broad database and explicitly selected recovery proofs",
            "local_failure_review": "not assessed; preserve and reconcile known local failures"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("revision")
    parser.add_argument("--handoff-branch", help="work-branch handoff evidence only; never deployment admission")
    args = parser.parse_args()
    try:
        print(json.dumps(qualify(args.revision, handoff_branch=args.handoff_branch)))
    except (ValueError, KeyError, IndexError, TypeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f"CI admission failed: {exc}") from exc

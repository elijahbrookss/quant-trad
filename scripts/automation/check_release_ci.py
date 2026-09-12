#!/usr/bin/env python3
"""Admit only a successful exact-commit develop push run, including every gate."""
from __future__ import annotations

import json
import re
import subprocess
import sys

REPOSITORY = "elijahbrookss/quant-trad"
REQUIRED_JOBS = {"pr-suite", "frontend", "deployment-contract", "clean-database-bootstrap", "deployment-rehearsal"}


def api(path: str) -> dict:
    result = subprocess.run(["gh", "api", f"repos/{REPOSITORY}/{path}"],
                            check=True, capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout)


def qualify(revision: str) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("a full lowercase commit SHA is required")
    runs = api(f"actions/workflows/test.yaml/runs?head_sha={revision}&event=push&branch=develop&per_page=100")["workflow_runs"]
    if not runs:
        raise ValueError("no develop push CI run exists for this exact commit")
    run = max(runs, key=lambda value: value["id"])
    if (run["head_sha"] != revision or run["event"] != "push"
            or run["head_branch"] != "develop" or run["status"] != "completed"
            or run["conclusion"] != "success"):
        raise ValueError("latest exact-commit develop CI run is not successful")
    jobs = api(f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100")
    if jobs["total_count"] > 100:
        raise ValueError("CI job response exceeds the bounded admission query")
    for name in sorted(REQUIRED_JOBS):
        matches = [job for job in jobs["jobs"] if job["name"] == name]
        if len(matches) != 1 or matches[0]["status"] != "completed" or matches[0]["conclusion"] != "success":
            raise ValueError(f"required CI job did not succeed: {name}")
    return {"event": "release_ci_qualified", "revision": revision, "run_url": run["html_url"], "attempt": run["run_attempt"]}


if __name__ == "__main__":
    try:
        print(json.dumps(qualify(sys.argv[1])))
    except (ValueError, KeyError, IndexError, subprocess.SubprocessError) as exc:
        raise SystemExit(f"release CI admission failed: {exc}") from exc

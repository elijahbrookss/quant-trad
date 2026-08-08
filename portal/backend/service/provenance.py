"""Version and provenance constants for backend-produced artifacts."""

from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path


RUNTIME_CONTRACT_VERSION = "runtime_contract.v1"
RUNTIME_STORAGE_SCHEMA_VERSION = "portal_runtime_storage.v2"

REPORT_CONTRACT_VERSION = "run_report.v2"
REPORT_SCHEMA_VERSION = "run_report.v2"
REPORT_DATASET_SCHEMA_VERSION = "run_research_dataset.v1"
REPORT_MATERIALIZATION_SCHEMA_VERSION = "run_report_materialization_status.v1"
REPORT_INPUT_FINGERPRINT_SCHEMA_VERSION = "report_input_fingerprint.v1"
REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION = "portal_report_materialization_storage.v2"


@lru_cache(maxsize=1)
def source_revision() -> str:
    """Return the commit hash that produced this backend process."""

    value = str(os.getenv("SOURCE_REVISION") or "").strip()
    if value:
        return value

    repo_root = Path(__file__).resolve().parents[3]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            "SOURCE_REVISION is required for run provenance when the local git "
            "checkout is unavailable."
        ) from exc
    revision = result.stdout.strip()
    if not revision:
        raise RuntimeError("SOURCE_REVISION resolved to an empty commit hash")
    return revision


def evidence_source_revision() -> str:
    """Return an immutable revision, rejecting dirty local evidence execution."""

    configured = str(os.getenv("SOURCE_REVISION") or "").strip()
    if configured:
        return configured

    revision = source_revision()
    repo_root = Path(__file__).resolve().parents[3]
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            "check_evidence_source_revision_unavailable: cannot verify a clean checkout"
        ) from exc
    if result.stdout.strip():
        raise RuntimeError(
            "check_evidence_dirty_source_forbidden: commit all producing code and "
            "configuration before durable evidence execution"
        )
    return revision


__all__ = [
    "REPORT_CONTRACT_VERSION",
    "REPORT_DATASET_SCHEMA_VERSION",
    "REPORT_INPUT_FINGERPRINT_SCHEMA_VERSION",
    "REPORT_MATERIALIZATION_SCHEMA_VERSION",
    "REPORT_MATERIALIZATION_STORAGE_SCHEMA_VERSION",
    "REPORT_SCHEMA_VERSION",
    "evidence_source_revision",
    "RUNTIME_CONTRACT_VERSION",
    "RUNTIME_STORAGE_SCHEMA_VERSION",
    "source_revision",
]

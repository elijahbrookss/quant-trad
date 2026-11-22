"""Read the minimum ordinary release after a completed canonical cutover."""

from pathlib import Path
import re

from scripts.automation.storage_host_boundary import load_receipt


def compatible_release_floor(path: Path) -> str:
    value = load_receipt(path)
    terminal = value.get("runtime_evidence", {})
    if (value.get("schema") != "qt.canonical_cold_host_hold.v1" or value.get("phase") != "runtime_complete"
            or terminal.get("outcome") not in ("committed", "original") or terminal.get("acceptance", {}).get("ready") is not True
            or terminal.get("outcome") != value.get("database_evidence", {}).get("database_reopened", {}).get("outcome")):
        raise RuntimeError("canonical_cold_completion_receipt_invalid")
    if terminal["outcome"] == "original":
        return ""
    revision = value.get("candidate_revision")
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("canonical_cold_completion_revision_invalid")
    return revision

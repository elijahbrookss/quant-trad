"""Provider-neutral range evidence; producers own the meaning of completeness."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, UTC
from typing import Any
import json

from .frozen import semantic_hash

SCHEMA = "market.producer_range_evidence.v1"
_FIELDS = ("schema_version", "series_id", "source_id", "source_identity_key", "start", "end",
           "classification", "coverage_status", "known_at", "detected_as_of_commit_seq", "evidence")


def utc(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("producer_range_evidence_invalid: timezone is required")
    return parsed.astimezone(UTC)


def iso(value: Any) -> str:
    return utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def producer_range_evidence(*, series_id, source_id, source_identity_key, start, end,
                            status, known_at, commit_seq, witness):
    if status not in {"complete", "interrupted"} or utc(end) <= utc(start):
        raise ValueError("producer_range_evidence_invalid: status or range")
    if int(series_id) <= 0 or int(source_id) <= 0 or not source_identity_key or not witness:
        raise ValueError("producer_range_evidence_invalid: missing identity or witness")
    row = {"schema_version": SCHEMA, "series_id": int(series_id), "source_id": int(source_id),
           "source_identity_key": str(source_identity_key), "start": iso(start), "end": iso(end),
           "classification": "source_quiet" if status == "complete" else "source_interrupted",
           "coverage_status": status, "known_at": iso(known_at),
           "detected_as_of_commit_seq": int(commit_seq), "evidence": json.loads(json.dumps(dict(witness), default=iso))}
    row["range_evidence_hash"] = semantic_hash(row)
    return row


def is_complete_range_evidence(row: Mapping[str, Any]) -> bool:
    """Legacy quality remains a gap. New proof must be intact, never guessed."""
    if row.get("schema_version") != SCHEMA:
        if "coverage_status" in row or "range_evidence_hash" in row:
            raise ValueError("producer_range_evidence_invalid: unsupported schema")
        return False
    material = {key: row.get(key) for key in _FIELDS}
    if semantic_hash(material) != row.get("range_evidence_hash"):
        raise ValueError("producer_range_evidence_invalid: hash disagreement")
    if (row.get("coverage_status") not in {"complete", "interrupted"}
            or utc(row["end"]) <= utc(row["start"]) or not row.get("source_identity_key")
            or not isinstance(row.get("evidence"), Mapping)
            or not row["evidence"] or int(row.get("series_id") or 0) <= 0
            or int(row.get("source_id") or 0) <= 0
            or int(row.get("detected_as_of_commit_seq", -1)) < 0
            or row.get("classification") != ("source_quiet" if row["coverage_status"] == "complete" else "source_interrupted")):
        raise ValueError("producer_range_evidence_invalid: malformed evidence")
    utc(row["known_at"])
    return row["coverage_status"] == "complete"

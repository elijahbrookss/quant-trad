from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Dict, Optional


FORENSICS_RETRIEVAL_SCHEMA_VERSION = 1


def _cursor_contract(*, seq: int, row_id: int) -> Dict[str, int]:
    return {
        "after_seq": int(seq),
        "after_row_id": int(row_id),
    }


def _truth_document(document: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "document_id": document.get("document_id"),
        "cursor": dict(document.get("cursor") or {}),
        "truth": dict(document.get("truth") or {}),
    }


def forensic_event_page_contract(
    *,
    bot_id: str,
    run_id: str,
    after_seq: int,
    after_row_id: int,
    limit: int,
    filters: Mapping[str, Any],
    documents: Iterable[Mapping[str, Any]],
    next_after_seq: int,
    next_after_row_id: int,
    has_more: bool,
) -> Dict[str, Any]:
    return {
        "schema_version": FORENSICS_RETRIEVAL_SCHEMA_VERSION,
        "contract": "botlens_forensic_event_page",
        "bot_id": str(bot_id),
        "run_id": str(run_id),
        "order": "asc",
        "page_size": int(limit),
        "filters": dict(filters or {}),
        "cursor": _cursor_contract(seq=after_seq, row_id=after_row_id),
        "next_cursor": _cursor_contract(seq=next_after_seq, row_id=next_after_row_id),
        "has_more": bool(has_more),
        "documents": [_truth_document(document) for document in documents],
    }


def signal_forensic_contract(
    *,
    bot_id: str,
    run_id: str,
    signal_id: str,
    signal: Mapping[str, Any],
    root_event_id: Optional[str],
    correlation_id: Optional[str],
    documents: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_version": FORENSICS_RETRIEVAL_SCHEMA_VERSION,
        "contract": "botlens_signal_forensics",
        "bot_id": str(bot_id),
        "run_id": str(run_id),
        "signal_id": str(signal_id),
        "signal": dict(signal or {}),
        "causal_chain": {
            "root_event_id": root_event_id,
            "correlation_id": correlation_id,
            "documents": [_truth_document(document) for document in documents],
        },
    }


__all__ = [
    "FORENSICS_RETRIEVAL_SCHEMA_VERSION",
    "forensic_event_page_contract",
    "signal_forensic_contract",
]

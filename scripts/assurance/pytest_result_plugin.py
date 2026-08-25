"""Deterministic, stdout-only pytest result transport for assurance runs.

The executor loads this plugin through ``PYTEST_PLUGINS`` so the catalog's
shell-free pytest argv remains exact. Each line is flushed immediately. A
consumer can therefore retain typed collection and outcome evidence even when
the process is interrupted before the terminal session event.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Any


SCHEMA_VERSION = "qt.pytest_events.v1"
LINE_PREFIX = "QT_ASSURANCE_PYTEST_EVENT="
_OUTCOME_ORDER = {
    "passed": 0,
    "skipped": 1,
    "xfailed": 2,
    "xpassed": 3,
    "failed": 4,
}


def _emit(event: dict[str, Any]) -> None:
    envelope = {"schema_version": SCHEMA_VERSION, **event}
    payload = json.dumps(
        envelope,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    stream = sys.__stdout__ or sys.stdout
    stream.write(f"{LINE_PREFIX}{payload}\n")
    stream.flush()


def _report_outcome(report: Any) -> str | None:
    was_xfail = bool(getattr(report, "wasxfail", False))
    if report.failed:
        return "xpassed" if was_xfail else "failed"
    if report.skipped:
        return "xfailed" if was_xfail else "skipped"
    if report.passed and report.when == "call":
        return "xpassed" if was_xfail else "passed"
    return None


@dataclass
class _Transport:
    collected_node_ids: list[str] = field(default_factory=list)
    outcomes: dict[str, str] = field(default_factory=dict)
    collection_errors: list[str] = field(default_factory=list)

    def record(self, node_id: str, outcome: str) -> None:
        previous = self.outcomes.get(node_id)
        if previous is None or _OUTCOME_ORDER[outcome] > _OUTCOME_ORDER[previous]:
            self.outcomes[node_id] = outcome


_TRANSPORT = _Transport()


def pytest_sessionstart(session: Any) -> None:  # pragma: no cover - called by pytest
    del session
    _TRANSPORT.collected_node_ids.clear()
    _TRANSPORT.outcomes.clear()
    _TRANSPORT.collection_errors.clear()
    _emit({"event": "session_start"})


def pytest_collection_finish(session: Any) -> None:  # pragma: no cover - called by pytest
    _TRANSPORT.collected_node_ids = sorted(item.nodeid for item in session.items)
    _emit(
        {
            "event": "collection",
            "node_ids": _TRANSPORT.collected_node_ids,
        }
    )


def pytest_collectreport(report: Any) -> None:  # pragma: no cover - called by pytest
    if report.failed:
        node_id = str(report.nodeid)
        if node_id not in _TRANSPORT.collection_errors:
            _TRANSPORT.collection_errors.append(node_id)
            _TRANSPORT.collection_errors.sort()
        _emit({"event": "collection_error", "node_id": node_id})


def pytest_runtest_logreport(report: Any) -> None:  # pragma: no cover - called by pytest
    outcome = _report_outcome(report)
    if outcome is None:
        return
    node_id = str(report.nodeid)
    _TRANSPORT.record(node_id, outcome)
    _emit(
        {
            "event": "test_outcome",
            "node_id": node_id,
            "outcome": _TRANSPORT.outcomes[node_id],
        }
    )


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:  # pragma: no cover
    del session
    ordered_results = [
        {"node_id": node_id, "outcome": _TRANSPORT.outcomes.get(node_id, "failed")}
        for node_id in _TRANSPORT.collected_node_ids
    ]
    counts = {name: 0 for name in _OUTCOME_ORDER}
    for result in ordered_results:
        counts[result["outcome"]] += 1
    counts["failed"] += len(_TRANSPORT.collection_errors)
    _emit(
        {
            "collection_errors": _TRANSPORT.collection_errors,
            "counts": counts,
            "event": "session_result",
            "exit_code": int(exitstatus),
            "node_ids": _TRANSPORT.collected_node_ids,
            "results": ordered_results,
        }
    )

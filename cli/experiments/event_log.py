from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from cli.audit import utc_now

from .contracts import EVENT_SCHEMA, json_safe


class ExperimentEventLog:
    def __init__(self, path: str | Path, *, experiment_id: str) -> None:
        self.path = Path(path)
        self.experiment_id = experiment_id
        self._seq = self._read_last_seq()

    def _read_last_seq(self) -> int:
        if not self.path.exists():
            return 0
        last = 0
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            last = max(last, int(payload.get("seq") or 0))
        return last

    def append(
        self,
        *,
        event_type: str,
        operation: str,
        status: str,
        step_id: str | None = None,
        target: dict[str, Any] | None = None,
        artifact_refs: list[dict[str, Any]] | None = None,
        error: dict[str, Any] | None = None,
        request_id: str | None = None,
        actor_type: str = "agent",
        actor_id: str = "codex",
        interface: str = "cli",
    ) -> dict[str, Any]:
        self._seq += 1
        payload = {
            "schema_version": EVENT_SCHEMA,
            "seq": self._seq,
            "timestamp": utc_now().isoformat(),
            "request_id": request_id or str(uuid.uuid4()),
            "experiment_id": self.experiment_id,
            "step_id": step_id,
            "actor_type": actor_type,
            "actor_id": actor_id,
            "interface": interface,
            "event_type": event_type,
            "operation": operation,
            "target": target or {},
            "status": status,
            "artifact_refs": artifact_refs or [],
            "error": error,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(json_safe(payload), sort_keys=True, default=str) + "\n")
        return payload


def read_events(
    path: str | Path,
    *,
    tail: int | None = None,
    event_type: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    event_path = Path(path)
    if not event_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in event_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if event_type and payload.get("event_type") != event_type:
            continue
        if status and payload.get("status") != status:
            continue
        rows.append(payload)
    return rows[-int(tail) :] if tail else rows


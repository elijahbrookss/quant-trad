from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from cli.audit import utc_now

from .contracts import TERMINAL_EXPERIMENT_STATUSES, json_safe


def _destination_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _read_notifications(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": "experiment_notifications.v1", "deliveries": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {"schema_version": "experiment_notifications.v1", "deliveries": []}


def _write_notifications(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def notify_terminal_state(*, notifications_path: Path, state: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    status = str(state.get("status") or "")
    if status not in TERMINAL_EXPERIMENT_STATUSES:
        return {"status": "skipped", "reason": "experiment_not_terminal"}
    if not bool(policy.get("enabled", False)):
        return {"status": "skipped", "reason": "notifications_disabled"}
    if status not in set(policy.get("on_states") or []):
        return {"status": "skipped", "reason": "terminal_state_not_subscribed"}

    payload = _read_notifications(notifications_path)
    existing_keys = {str(item.get("delivery_key")) for item in payload.get("deliveries") or [] if isinstance(item, dict)}
    deliveries = list(payload.get("deliveries") or [])
    sinks = [str(item) for item in policy.get("sinks") or ["file"]]
    message = f"Experiment {state.get('experiment_id')} finished with status {status}"
    for sink in sinks:
        destination = "stderr" if sink == "console" else str(notifications_path)
        delivery_key = f"{state.get('experiment_id')}:{status}:{sink}:{_destination_hash(destination)}"
        if delivery_key in existing_keys:
            continue
        if sink == "console":
            print(message, file=sys.stderr, flush=True)
        deliveries.append(
            {
                "delivery_key": delivery_key,
                "sink": sink,
                "destination": destination,
                "status": "sent",
                "sent_at": utc_now().isoformat(),
                "message": message,
            }
        )
    payload["deliveries"] = deliveries
    _write_notifications(notifications_path, payload)
    return {"status": "sent", "deliveries": deliveries}


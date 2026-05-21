from __future__ import annotations

from pathlib import Path
from typing import Any

from .state_store import ExperimentStateStore, find_experiment_dir


def doctor_experiment(root: str | Path, ref: str) -> dict[str, Any]:
    path = find_experiment_dir(root, ref)
    store = ExperimentStateStore(root, path=path)
    checks: list[dict[str, Any]] = []

    def check(name: str, ok: bool, *, detail: Any = None) -> None:
        checks.append({"name": name, "status": "ok" if ok else "failed", "detail": detail})

    check("plan_exists", store.plan_path.exists(), detail=str(store.plan_path))
    check("state_exists", store.state_path.exists(), detail=str(store.state_path))
    check("events_exists", store.events_path.exists(), detail=str(store.events_path))
    state = store.load_state() if store.state_path.exists() else {}
    for step in state.get("steps") or []:
        for ref_item in step.get("artifact_refs") or []:
            artifact_path = ref_item.get("path") if isinstance(ref_item, dict) else None
            if artifact_path:
                check("artifact_ref_exists", Path(artifact_path).exists(), detail=artifact_path)
    terminal = str(state.get("status") or "") in {"COMPLETED", "FAILED", "PARTIALLY_COMPLETED", "CANCELLED"}
    if terminal:
        notify_status = state.get("notification_status")
        check("terminal_notification_recorded", bool(notify_status), detail=notify_status)
    failed = [item for item in checks if item.get("status") != "ok"]
    return {
        "schema_version": "experiment_doctor.v1",
        "experiment_id": state.get("experiment_id") or path.name,
        "status": "failed" if failed else "ok",
        "checks": checks,
    }


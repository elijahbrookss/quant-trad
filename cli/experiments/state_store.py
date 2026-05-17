from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from cli.audit import date_partition, safe_path_part, timestamp_slug, utc_now

from .contracts import STATE_SCHEMA, build_step_plan, json_safe


def experiment_id_for_name(name: str) -> str:
    return f"{safe_path_part(name)}-{timestamp_slug()}-{uuid.uuid4().hex[:8]}"


def experiment_suite_dir(root: str | Path, experiment_id: str) -> Path:
    return Path(root).expanduser() / "experiments" / date_partition() / safe_path_part(experiment_id)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def find_experiment_dir(root: str | Path, ref: str) -> Path:
    raw_ref = str(ref or "").strip()
    if not raw_ref:
        raise ValueError("experiment reference is required")
    candidate = Path(raw_ref).expanduser()
    if candidate.exists():
        if candidate.is_file():
            if candidate.name == "state.json":
                return candidate.parent
            if candidate.name == "plan.json":
                return candidate.parent
            return candidate.parent
        return candidate
    root_path = Path(root).expanduser() / "experiments"
    safe_ref = safe_path_part(raw_ref)
    matches = list(root_path.glob(f"**/{safe_ref}/state.json")) if root_path.exists() else []
    if matches:
        return matches[0].parent
    if root_path.exists():
        for path in root_path.glob("**/state.json"):
            state = _read_json(path)
            if raw_ref == str(state.get("experiment_id") or ""):
                return path.parent
    raise ValueError(f"experiment suite state not found for {raw_ref!r}")


class ExperimentStateStore:
    def __init__(self, root: str | Path, *, experiment_id: str | None = None, path: str | Path | None = None) -> None:
        if path is not None:
            self.path = Path(path).expanduser()
            self.experiment_id = self.path.name
        else:
            if not experiment_id:
                raise ValueError("experiment_id is required when path is not provided")
            self.path = experiment_suite_dir(root, experiment_id)
            self.experiment_id = experiment_id

    @property
    def plan_path(self) -> Path:
        return self.path / "plan.json"

    @property
    def state_path(self) -> Path:
        return self.path / "state.json"

    @property
    def events_path(self) -> Path:
        return self.path / "events.ndjson"

    @property
    def runs_dir(self) -> Path:
        return self.path / "runs"

    @property
    def artifacts_dir(self) -> Path:
        return self.path / "artifacts"

    @property
    def notifications_path(self) -> Path:
        return self.path / "notifications.json"

    def write_plan(self, plan: dict[str, Any]) -> None:
        _write_json(self.plan_path, plan)

    def load_plan(self) -> dict[str, Any]:
        return _read_json(self.plan_path)

    def write_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = utc_now().isoformat()
        _write_json(self.state_path, state)

    def load_state(self) -> dict[str, Any]:
        return _read_json(self.state_path)

    def create_state(self, plan: dict[str, Any]) -> dict[str, Any]:
        now = utc_now().isoformat()
        state = {
            "schema_version": STATE_SCHEMA,
            "experiment_id": self.experiment_id,
            "plan_hash": plan.get("plan_hash"),
            "status": "CREATED",
            "created_at": now,
            "updated_at": now,
            "current_step_id": None,
            "steps": build_step_plan(plan),
            "run_refs": [],
            "comparison_refs": [],
            "pass_gate_result_ref": None,
            "notification_status": None,
            "terminal_error": None,
            "paths": {
                "experiment_dir": str(self.path),
                "plan": str(self.plan_path),
                "state": str(self.state_path),
                "events": str(self.events_path),
            },
        }
        self.write_plan(plan)
        self.write_state(state)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "reports").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "comparisons").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "summaries").mkdir(parents=True, exist_ok=True)
        return state

    def run_record_path(self, window_id: str, variant_id: str) -> Path:
        return self.runs_dir / f"{safe_path_part(window_id)}__{safe_path_part(variant_id)}.json"

    def write_run_record(self, window_id: str, variant_id: str, payload: dict[str, Any]) -> Path:
        path = self.run_record_path(window_id, variant_id)
        _write_json(path, payload)
        return path

    def load_run_record(self, window_id: str, variant_id: str) -> dict[str, Any]:
        return _read_json(self.run_record_path(window_id, variant_id))

    def load_run_records(self) -> dict[tuple[str, str], dict[str, Any]]:
        records: dict[tuple[str, str], dict[str, Any]] = {}
        if not self.runs_dir.exists():
            return records
        for path in self.runs_dir.glob("*.json"):
            payload = _read_json(path)
            records[(str(payload.get("window_id")), str(payload.get("variant_id")))] = payload
        return records


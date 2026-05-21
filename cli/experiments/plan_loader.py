from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from .contracts import build_step_plan, normalize_plan


def _load_yaml(raw: str, *, source: str) -> Any:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - requirements include PyYAML.
        raise ValueError("PyYAML is required to read experiment plan YAML files") from exc
    try:
        return yaml.safe_load(raw)
    except Exception as exc:  # noqa: BLE001 - normalize parser errors into CLI ValueError.
        raise ValueError(f"invalid experiment plan YAML in {source}: {exc}") from exc


def load_plan(path: str | Path) -> dict[str, Any]:
    source = str(path)
    raw = sys.stdin.read() if source == "-" else Path(path).expanduser().read_text(encoding="utf-8")
    if not raw.strip():
        raise ValueError("experiment plan is empty")
    if source.endswith(".json"):
        payload = json.loads(raw)
    else:
        payload = _load_yaml(raw, source=source)
    if not isinstance(payload, dict):
        raise ValueError("experiment plan must be an object")
    return normalize_plan(payload)


def plan_preview(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "experiment_plan_preview.v1",
        "plan": plan,
        "steps": build_step_plan(plan),
        "step_count": len(build_step_plan(plan)),
    }


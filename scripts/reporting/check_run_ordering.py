#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portal.backend.service.reports import report_data  # noqa: E402
from portal.backend.service.reports.run_research_dataset import _runtime_ordering_health  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Check runtime event ordering health for a run.")
    parser.add_argument("--run-id", required=True, help="Run ID to inspect.")
    args = parser.parse_args()

    events = report_data.list_run_events(str(args.run_id))
    health: dict[str, Any] = _runtime_ordering_health(events)
    print(json.dumps({"run_id": str(args.run_id), **health}, indent=2, sort_keys=True))
    return 0 if health.get("status") in {"ready", "backfilled"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

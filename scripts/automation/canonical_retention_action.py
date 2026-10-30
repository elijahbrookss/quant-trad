#!/usr/bin/env python3
"""Run one exact private canonical-retention canary action."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys


os.environ.setdefault("QT_DISABLE_DOTENV", "1")
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.settings import get_settings  # noqa: E402
from core.storage_mounts import (  # noqa: E402
    require_configured_archive_mount,
    require_configured_working_mount,
)
from market_data.archive import FilesystemRawArchiveObjectStore  # noqa: E402
from portal.backend.db import db  # noqa: E402
from portal.backend.service.async_jobs import wait_for_database_ready  # noqa: E402
from portal.backend.service.market.market_structure_service import DEFAULT_STORAGE_ROOT  # noqa: E402
from portal.backend.service.storage.maintenance_runtime import storage_maintenance_runners  # noqa: E402
from portal.backend.service.storage.repos.query_diagnostic import write_receipt  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run exactly one freshly planned canonical storage action. This "
            "private operator refuses a changed action and never runs history "
            "or recovery maintenance."
        )
    )
    parser.add_argument("--storage-day", required=True, help="Exact UTC storage day (YYYY-MM-DD).")
    parser.add_argument(
        "--expected-action", required=True, choices=("stage_page", "reclaim_partition"),
    )
    parser.add_argument("--receipt", required=True, help="Private JSON receipt path.")
    parser.add_argument("--execute", action="store_true", help="Execute the one selected action.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        storage_day = date.fromisoformat(args.storage_day)
    except ValueError as exc:
        raise SystemExit("--storage-day must be YYYY-MM-DD") from exc
    settings = get_settings()
    if os.geteuid() != 70:
        raise PermissionError("canonical_retention_action_database_user_required")
    if settings.storage.maintenance_owner != "dedicated":
        raise ValueError("canonical_retention_action_dedicated_owner_required")
    if settings.storage.archive_shared_group_id is None:
        raise ValueError("canonical_retention_action_shared_archive_required")
    require_configured_archive_mount()
    require_configured_working_mount()
    FilesystemRawArchiveObjectStore(DEFAULT_STORAGE_ROOT / "objects")
    runners = storage_maintenance_runners(
        db, storage_root=DEFAULT_STORAGE_ROOT,
        limits_path=settings.storage.maintenance_limits_path,
        require_incremental=True,
    )
    if not wait_for_database_ready(
            timeout_seconds=settings.workers.collectors.db_wait_timeout_seconds,
            poll_interval_seconds=0.5):
        raise RuntimeError("canonical_retention_action_database_timeout")
    result = runners["service"].run_canonical_action(
        policy=settings.market_data_lifecycle,
        storage_day=storage_day,
        expected_action=args.expected_action,
        storage_root=DEFAULT_STORAGE_ROOT,
        execute=args.execute,
    )
    write_receipt(Path(args.receipt), result)
    print(json.dumps(result, sort_keys=True, default=str))
    return 0 if result["status"] in {"completed", "dry_run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

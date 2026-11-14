#!/usr/bin/env python3
"""Run one exact private canonical-retention canary action."""

from __future__ import annotations

import argparse
import hashlib
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
from portal.backend.service.storage.repos.fact_archival import (  # noqa: E402
    SelectedVerificationPage, validate_selected_verification_batch,
)
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
        "--expected-action", required=True, choices=("stage_page", "verify_page", "verify_partition", "reclaim_partition"),
    )
    parser.add_argument("--verification-pages", type=int,
        help="Exact reviewed page count and cap for one whole-day action.")
    parser.add_argument("--verification-bytes", type=int, help="Maximum distinct archive bytes for the selected day.")
    parser.add_argument("--verification-objects", type=int, help="Maximum distinct archive objects for the selected day.")
    parser.add_argument("--manifest-set-hash", help="Current verified manifest-set hash for a reviewed reclaim_partition.")
    parser.add_argument("--page-ordinal", type=int)
    parser.add_argument("--manifest-id")
    parser.add_argument("--manifest-hash")
    parser.add_argument("--generation", type=int, help="0 for a legacy page with no active prepared generation.")
    parser.add_argument("--preparation-id")
    parser.add_argument("--catalog-commitment")
    parser.add_argument("--verification-batch", type=Path, help="Reviewed JSON array of contiguous verify_page pins.")
    parser.add_argument("--verification-batch-sha256", help="Expected SHA-256 of the exact reviewed batch file.")
    parser.add_argument("--receipt", required=True, help="Private JSON receipt path.")
    parser.add_argument("--execute", action="store_true", help="Execute the one selected action.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.verification_pages is not None and (
        args.expected_action not in {"verify_partition", "reclaim_partition"}
        or not 1 <= args.verification_pages <= 100_000
    ):
        parser.error("--verification-pages requires a whole-day action and a count of 1..100000")
    if (args.verification_bytes is not None or args.verification_objects is not None) and args.verification_pages is None:
        parser.error("Byte and object allowances require the exact --verification-pages count.")
    if args.verification_bytes is not None and not 1 <= args.verification_bytes <= 1024**4:
        parser.error("--verification-bytes must be between 1 and 1 TiB.")
    if args.verification_objects is not None and not 1 <= args.verification_objects <= 100_000:
        parser.error("--verification-objects must be between 1 and 100000.")
    if ((args.expected_action == "reclaim_partition" and args.verification_pages is not None)
            != (args.manifest_set_hash is not None)):
        parser.error("A reviewed reclaim_partition requires both --verification-pages and --manifest-set-hash.")
    if args.manifest_set_hash is not None and (len(args.manifest_set_hash) != 64
            or any(char not in "0123456789abcdef" for char in args.manifest_set_hash)):
        parser.error("--manifest-set-hash must be a lowercase SHA-256.")
    page_fields = (args.page_ordinal, args.manifest_id, args.manifest_hash, args.generation,
                   args.preparation_id, args.catalog_commitment)
    page = batch = None
    if (args.verification_batch is None) != (args.verification_batch_sha256 is None):
        parser.error("A verification batch requires its exact SHA-256.")
    if args.expected_action == "verify_page":
        try:
            if args.verification_batch is None:
                page = SelectedVerificationPage(*page_fields)
            elif any(value is not None for value in page_fields):
                parser.error("A verification batch cannot be combined with single-page pins.")
            else:
                with args.verification_batch.open("rb") as handle:
                    data = handle.read(65_537)
                if len(data) > 65_536 or hashlib.sha256(data).hexdigest() != args.verification_batch_sha256:
                    raise ValueError("canonical_archive_selected_batch_hash_or_size_invalid")
                payload = json.loads(data)
                if not isinstance(payload, list) or not 2 <= len(payload) <= 128:
                    raise ValueError("canonical_archive_selected_batch_invalid")
                batch = validate_selected_verification_batch(tuple(SelectedVerificationPage(**row) for row in payload))
        except (OSError, TypeError, ValueError) as exc:
            parser.error(str(exc))
    elif args.verification_batch is not None or any(value is not None for value in page_fields):
        parser.error("Exact page pins require verify_page.")
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
    verification = {}
    if args.verification_pages is not None:
        verification["verification_pages"] = args.verification_pages
    if args.verification_bytes is not None:
        verification["verification_bytes"] = args.verification_bytes
    if args.verification_objects is not None:
        verification["verification_objects"] = args.verification_objects
    if args.manifest_set_hash is not None:
        verification["manifest_set_hash"] = args.manifest_set_hash
    if page is not None:
        verification["verification_page"] = page
    if batch is not None:
        verification["verification_batch"] = batch
    result = runners["service"].run_canonical_action(
        policy=settings.market_data_lifecycle,
        storage_day=storage_day,
        expected_action=args.expected_action,
        storage_root=DEFAULT_STORAGE_ROOT,
        execute=args.execute,
        **verification,
    )
    if batch is not None:
        result["verification_batch_sha256"] = args.verification_batch_sha256
    write_receipt(Path(args.receipt), result)
    print(json.dumps(result, sort_keys=True, default=str))
    return 0 if result["status"] in {"completed", "dry_run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("QT_LOGGING_LOKI_URL", "")
os.environ.setdefault("QT_LOGGING_DEBUG", "false")
os.environ.setdefault("QT_LOGGING_LEVEL", "WARNING")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portal.backend.service.reports.contract import (  # noqa: E402
    get_report_diagnostics,
    get_report_readiness,
    get_run_report_summary,
    get_run_research_dataset,
)
from portal.backend.service.reports.export_bundle import (  # noqa: E402
    build_export_archive,
    build_export_manifest,
)


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _export_report(args: argparse.Namespace) -> int:
    archive, filename = build_export_archive(
        str(args.run_id),
        include_json=not args.no_json,
        include_csv=not args.no_csv,
        include_candles=bool(args.include_candles),
    )
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    out_path.write_bytes(archive)
    _print_json(
        {
            "run_id": str(args.run_id),
            "path": str(out_path),
            "filename": filename,
            "size_bytes": len(archive),
        }
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect or export canonical reporting data for a run.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_run_arg(command: str, help_text: str) -> argparse.ArgumentParser:
        subparser = subparsers.add_parser(command, help=help_text)
        subparser.add_argument("--run-id", required=True, help="Run ID to inspect.")
        return subparser

    add_run_arg("readiness", "Print report readiness.")
    add_run_arg("dataset", "Print the canonical RunResearchDataset payload.")
    add_run_arg("summary", "Print the compact report summary.")
    add_run_arg("diagnostics", "Print report diagnostics.")
    manifest = add_run_arg("manifest", "Print the report export manifest.")
    manifest.add_argument("--include-candles", action="store_true", help="Include candle files in manifest sizing.")

    export = add_run_arg("export", "Write a report export zip.")
    export.add_argument("--out-dir", default="logs/reports", help="Directory for the generated zip.")
    export.add_argument("--no-json", action="store_true", help="Exclude JSON payloads from the export.")
    export.add_argument("--no-csv", action="store_true", help="Exclude CSV payloads from the export.")
    export.add_argument("--include-candles", action="store_true", help="Include candle datasets in the export.")

    args = parser.parse_args()
    run_id = str(args.run_id)

    if args.command == "readiness":
        _print_json(get_report_readiness(run_id))
        return 0
    if args.command == "dataset":
        _print_json(get_run_research_dataset(run_id))
        return 0
    if args.command == "summary":
        _print_json(get_run_report_summary(run_id))
        return 0
    if args.command == "diagnostics":
        _print_json(get_report_diagnostics(run_id))
        return 0
    if args.command == "manifest":
        _print_json(build_export_manifest(run_id, include_candles=bool(args.include_candles)))
        return 0
    if args.command == "export":
        return _export_report(args)

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

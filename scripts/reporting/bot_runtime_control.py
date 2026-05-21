#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

os.environ.setdefault("QT_LOGGING_LOKI_URL", "")
os.environ.setdefault("QT_LOGGING_DEBUG", "false")
os.environ.setdefault("QT_LOGGING_LEVEL", "WARNING")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portal.backend.service.reports import report_data  # noqa: E402


TERMINAL_STATUSES = {
    "completed",
    "failed",
    "crashed",
    "canceled",
    "cancelled",
    "startup_failed",
    "degraded_terminal",
    "stopped",
}


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str), flush=True)


def _api_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]:
    body = json.dumps(payload or {}).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed status={exc.code} body={detail}") from exc


def _run_status(run_id: str) -> dict[str, Any]:
    run = report_data.get_run(run_id)
    if not run:
        return {"run_id": run_id, "found": False, "status": "not_found"}
    return {
        "run_id": run_id,
        "found": True,
        "status": run.get("status"),
        "started_at": run.get("started_at"),
        "ended_at": run.get("ended_at"),
        "bot_id": run.get("bot_id"),
        "run_type": run.get("run_type"),
        "timeframe": run.get("timeframe"),
        "symbols": run.get("symbols") or [],
    }


def _cmd_active(args: argparse.Namespace) -> int:
    url = f"{args.api_url.rstrip('/')}/api/bots/{args.bot_id}/active-run"
    _print_json(_api_json("GET", url, timeout=args.timeout))
    return 0


def _cmd_start(args: argparse.Namespace) -> int:
    url = f"{args.api_url.rstrip('/')}/api/bots/{args.bot_id}/start"
    body = {"request_id": args.request_id} if args.request_id else {}
    payload = _api_json("POST", url, payload=body, timeout=args.timeout)
    keys = ("status", "bot_id", "run_id", "active_run_id", "request_id", "message")
    _print_json({key: payload.get(key) for key in keys})
    return 0


def _cmd_stop(args: argparse.Namespace) -> int:
    url = f"{args.api_url.rstrip('/')}/api/bots/{args.bot_id}/stop"
    body: dict[str, Any] = {
        "preserve_container": bool(args.preserve_container),
    }
    if args.run_id:
        body["run_id"] = str(args.run_id)
    if args.request_id:
        body["request_id"] = str(args.request_id)
    payload = _api_json("POST", url, payload=body, timeout=args.timeout)
    keys = ("status", "bot_id", "run_id", "request_id", "message")
    _print_json({key: payload.get(key) for key in keys})
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    status = _run_status(str(args.run_id))
    _print_json(status)
    return 0 if status.get("found") else 1


def _cmd_wait(args: argparse.Namespace) -> int:
    deadline = time.monotonic() + float(args.timeout)
    last_status: dict[str, Any] = {}
    while True:
        last_status = _run_status(str(args.run_id))
        if args.print_each:
            _print_json(last_status)
        status = str(last_status.get("status") or "").strip().lower()
        if status in TERMINAL_STATUSES:
            if not args.print_each:
                _print_json(last_status)
            return 0 if status == "completed" or args.allow_non_completed else 1
        if time.monotonic() >= deadline:
            _print_json({**last_status, "wait_status": "timeout", "timeout_seconds": args.timeout})
            return 124
        time.sleep(float(args.interval))


def main() -> int:
    parser = argparse.ArgumentParser(description="Small bot runtime control helpers for local audit workflows.")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000", help="Backend API URL.")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout or wait timeout depending on command.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    active = subparsers.add_parser("active", help="Print active-run state for a bot.")
    active.add_argument("--bot-id", required=True)
    active.set_defaults(func=_cmd_active)

    start = subparsers.add_parser("start", help="Start a bot and print the accepted run id.")
    start.add_argument("--bot-id", required=True)
    start.add_argument("--request-id")
    start.set_defaults(func=_cmd_start)

    stop = subparsers.add_parser("stop", help="Stop a bot run through the backend API.")
    stop.add_argument("--bot-id", required=True)
    stop.add_argument("--run-id")
    stop.add_argument("--request-id")
    stop.add_argument("--preserve-container", action="store_true")
    stop.set_defaults(func=_cmd_stop)

    status = subparsers.add_parser("status", help="Print persisted DB status for a run.")
    status.add_argument("--run-id", required=True)
    status.set_defaults(func=_cmd_status)

    wait = subparsers.add_parser("wait", help="Wait for a run to reach a terminal DB status.")
    wait.add_argument("--run-id", required=True)
    wait.add_argument("--interval", type=float, default=30.0)
    wait.add_argument("--print-each", action="store_true")
    wait.add_argument("--allow-non-completed", action="store_true")
    wait.set_defaults(func=_cmd_wait)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

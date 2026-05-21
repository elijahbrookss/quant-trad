#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

os.environ.setdefault("QT_LOGGING_LOKI_URL", "")
os.environ.setdefault("QT_LOGGING_DEBUG", "false")
os.environ.setdefault("QT_LOGGING_LEVEL", "WARNING")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portal.backend.service.bots.botlens_event_replay import (  # noqa: E402
    load_domain_projection_batches,
    rebuild_run_projection_snapshot,
    rebuild_symbol_projection_snapshot,
)
from portal.backend.service.bots.botlens_state import (  # noqa: E402
    serialize_run_projection_snapshot,
    serialize_symbol_projection_snapshot,
)
from portal.backend.service.reports import report_data  # noqa: E402


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _projection_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    projection = _mapping(payload.get("projection"))
    concerns = _mapping(projection.get("concerns"))
    if payload.get("kind") == "symbol_projection_snapshot":
        return {
            "projection_seq": projection.get("seq"),
            "symbol_key": projection.get("symbol_key"),
            "candle_count": len(_mapping(concerns.get("candles")).get("items") or []),
            "overlay_count": len(_mapping(concerns.get("overlays")).get("items") or []),
            "signal_count": len(_mapping(concerns.get("signals")).get("items") or []),
            "decision_count": len(_mapping(concerns.get("decisions")).get("items") or []),
            "trade_count": len(_mapping(concerns.get("trades")).get("items") or []),
            "diagnostic_count": len(_mapping(concerns.get("diagnostics")).get("items") or []),
            "readiness": _mapping(concerns.get("readiness")),
        }
    return {
        "projection_seq": projection.get("seq"),
        "bot_id": projection.get("bot_id"),
        "run_id": projection.get("run_id"),
        "lifecycle": _mapping(concerns.get("lifecycle")),
        "health": _mapping(concerns.get("health")),
        "fault_count": len(_mapping(concerns.get("faults")).get("items") or []),
        "symbol_count": len(_mapping(concerns.get("symbol_catalog")).get("entries") or {}),
        "open_trade_count": len(_mapping(concerns.get("open_trades")).get("entries") or {}),
        "readiness": _mapping(concerns.get("readiness")),
    }


def _batch_summary(batches: Sequence[Any]) -> dict[str, Any]:
    seqs = [int(getattr(batch, "seq", 0) or 0) for batch in batches]
    monotonic = seqs == sorted(seqs)
    return {
        "batch_count": len(batches),
        "event_count": sum(len(getattr(batch, "events", ()) or ()) for batch in batches),
        "first_seq": seqs[0] if seqs else None,
        "last_seq": seqs[-1] if seqs else None,
        "monotonic_seq": monotonic,
    }


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay BotLens projection state from canonical runtime events.")
    parser.add_argument("--run-id", required=True, help="Run ID to inspect.")
    parser.add_argument("--bot-id", help="Override bot ID when the run row is unavailable or incomplete.")
    parser.add_argument("--symbol-key", help="Replay one symbol projection instead of the run projection.")
    parser.add_argument("--max-seq", type=int, help="Only replay events up to this run sequence.")
    args = parser.parse_args()

    run_id = str(args.run_id)
    run = report_data.get_run(run_id) or {}
    bot_id = str(args.bot_id or run.get("bot_id") or "").strip()
    if not bot_id:
        _print_json({"run_id": run_id, "status": "run_not_found", "reason": "bot_id_unavailable"})
        return 1

    try:
        batches = load_domain_projection_batches(
            bot_id=bot_id,
            run_id=run_id,
            series_key=args.symbol_key,
            max_seq=args.max_seq,
        )
        summary = _batch_summary(batches)
        if args.symbol_key:
            snapshot = rebuild_symbol_projection_snapshot(
                bot_id=bot_id,
                run_id=run_id,
                symbol_key=str(args.symbol_key),
                max_seq=args.max_seq,
            )
            payload = serialize_symbol_projection_snapshot(snapshot) if snapshot else None
        else:
            snapshot = rebuild_run_projection_snapshot(bot_id=bot_id, run_id=run_id, max_seq=args.max_seq)
            payload = serialize_run_projection_snapshot(snapshot) if snapshot else None
    except Exception as exc:  # noqa: BLE001 - this is an audit helper that reports replay failure context.
        _print_json({"run_id": run_id, "bot_id": bot_id, "status": "projection_error", "error": str(exc)})
        return 1

    status = "ready" if payload and summary["batch_count"] and summary["monotonic_seq"] else "unavailable"
    if payload and summary["batch_count"] and not summary["monotonic_seq"]:
        status = "inconsistent"

    output = {
        "run_id": run_id,
        "bot_id": bot_id,
        "symbol_key": args.symbol_key,
        "status": status,
        **summary,
        "projection": _projection_summary(payload or {}),
    }
    _print_json(output)
    return 0 if status == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())

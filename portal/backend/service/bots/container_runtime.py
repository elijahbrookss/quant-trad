from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing as mp
import os
import queue
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Mapping

from portal.backend.service.bots.bot_runtime import BotRuntime
from portal.backend.service.storage.storage import load_bots, record_bot_run_snapshot, update_bot_runtime_status

logger = logging.getLogger(__name__)
_TERMINAL_STATUSES = {"completed", "stopped", "error", "failed", "crashed"}


def _configure_logging() -> None:
    logging.basicConfig(level=getattr(logging, os.getenv("PORTAL_LOG_LEVEL", "INFO").upper(), logging.INFO))


def _emit_telemetry(url: str, payload: Mapping[str, Any]) -> bool:
    if not url:
        return False
    try:
        import websockets  # type: ignore
    except Exception:
        logger.warning("bot_telemetry_library_missing | package=websockets")
        return False

    async def _send() -> None:
        async with websockets.connect(url, open_timeout=2, close_timeout=1) as ws:
            await ws.send(json.dumps(payload))

    try:
        asyncio.run(_send())
    except Exception as exc:  # noqa: BLE001
        logger.warning("bot_telemetry_send_failed | error=%s", exc)
        return False
    return True


def _series_worker(
    *,
    run_id: str,
    bot_id: str,
    series_key: str,
    bot_config: Mapping[str, Any],
    event_queue: "mp.Queue[Dict[str, Any]]",
) -> None:
    logger.info(
        "bot_series_process_started | run_id=%s | bot_id=%s | cache_owner=series_process | cache_scope_id=%s",
        run_id,
        bot_id,
        series_key,
    )
    child_config = dict(bot_config)
    child_config["strategy_ids"] = [series_key]
    child_config["strategy_id"] = series_key

    def _state_callback(patch: Dict[str, Any]) -> None:
        event_queue.put({"kind": "runtime_patch", "series_key": series_key, "patch": dict(patch)})

    runtime = BotRuntime(
        bot_id=bot_id,
        config=child_config,
        state_callback=_state_callback,
    )
    runtime.reset_if_finished()
    runtime.start()

    while True:
        snapshot = runtime.snapshot()
        status = str(snapshot.get("status") or "").lower()
        event_queue.put(
            {
                "kind": "snapshot",
                "series_key": series_key,
                "status": status,
                "snapshot": snapshot,
                "at": datetime.utcnow().isoformat() + "Z",
            }
        )
        if status in _TERMINAL_STATUSES:
            if status in {"error", "failed", "crashed"}:
                raise RuntimeError(f"series runtime failed | series_key={series_key} | status={status}")
            break
        time.sleep(0.2)


def main() -> int:
    _configure_logging()
    bot_id = str(os.getenv("BOT_ID") or "").strip()
    if not bot_id:
        raise RuntimeError("BOT_ID is required")

    snapshot_interval_ms = int(os.getenv("SNAPSHOT_INTERVAL_MS") or "0")
    if snapshot_interval_ms <= 0:
        raise RuntimeError("SNAPSHOT_INTERVAL_MS must be > 0")

    telemetry_url = str(os.getenv("BACKEND_TELEMETRY_WS_URL") or "").strip()

    bot = next((b for b in load_bots() if b.get("id") == bot_id), None)
    if bot is None:
        raise RuntimeError(f"Bot not found: {bot_id}")

    run_id = str(uuid.uuid4())
    update_bot_runtime_status(bot_id=bot_id, run_id=run_id, status="running")
    strategy_id = str(bot.get("strategy_id") or "").strip()
    if not strategy_id:
        raise RuntimeError(f"Bot {bot_id} has no strategy_id configured")
    series_keys: List[str] = [strategy_id]
    if not series_keys:
        raise RuntimeError("Bot has no series to execute")

    child_queues: Dict[str, "mp.Queue[Dict[str, Any]]"] = {}
    children: Dict[str, mp.Process] = {}
    latest_snapshots: Dict[str, Dict[str, Any]] = {}

    for series_key in series_keys:
        event_queue: "mp.Queue[Dict[str, Any]]" = mp.Queue()
        child_queues[series_key] = event_queue
        proc = mp.Process(
            target=_series_worker,
            kwargs={
                "run_id": run_id,
                "bot_id": bot_id,
                "series_key": series_key,
                "bot_config": bot,
                "event_queue": event_queue,
            },
            daemon=False,
        )
        proc.start()
        children[series_key] = proc

    seq = 0
    telemetry_degraded = False
    try:
        while children:
            loop_started = time.monotonic()
            for series_key, event_queue in child_queues.items():
                while True:
                    try:
                        event = event_queue.get_nowait()
                    except queue.Empty:
                        break
                    latest_snapshots[series_key] = dict(event)

            for series_key, proc in list(children.items()):
                if proc.exitcode is None:
                    continue
                if proc.exitcode != 0:
                    raise RuntimeError(
                        f"series child failed | run_id={run_id} | bot_id={bot_id} | series_key={series_key} | exitcode={proc.exitcode}"
                    )
                del children[series_key]

            seq += 1
            now_iso = datetime.utcnow().isoformat() + "Z"
            for series_key in series_keys:
                snapshot = dict(latest_snapshots.get(series_key) or {})
                snapshot.setdefault("kind", "snapshot")
                snapshot.setdefault("series_key", series_key)
                snapshot.setdefault("status", "running" if series_key in children else "stopped")
                snapshot.setdefault("at", now_iso)
                record_bot_run_snapshot(
                    {
                        "run_id": run_id,
                        "bot_id": bot_id,
                        "series_key": series_key,
                        "snapshot_seq": seq,
                        "snapshot_payload": snapshot,
                    }
                )
                telemetry_payload = {
                    "run_id": run_id,
                    "bot_id": bot_id,
                    "series_key": series_key,
                    "snapshot_seq": seq,
                    "snapshot": snapshot,
                }
                sent = _emit_telemetry(telemetry_url, telemetry_payload)
                if not sent:
                    telemetry_degraded = True

            status = "running" if children else "stopped"
            update_bot_runtime_status(
                bot_id=bot_id,
                run_id=run_id,
                status=status,
                telemetry_degraded=telemetry_degraded,
            )

            elapsed = time.monotonic() - loop_started
            sleep_for = max((snapshot_interval_ms / 1000.0) - elapsed, 0.05)
            time.sleep(sleep_for)
    except Exception:
        update_bot_runtime_status(bot_id=bot_id, run_id=run_id, status="failed", telemetry_degraded=telemetry_degraded)
        raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

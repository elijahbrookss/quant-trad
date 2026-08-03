"""Long-running bounded market-data collection worker."""

from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
from typing import Any

from core.settings import get_settings

from portal.backend.service.async_jobs import wait_for_database_ready
from portal.backend.service.market.collector_service import market_data_collector


logger = logging.getLogger(__name__)
_STOP = False
_SETTINGS = get_settings()
_WORKER_SETTINGS = _SETTINGS.workers.collectors
_LEASE_SECONDS = 90.0
_WORKER_STATE_TTL_SECONDS = 30.0
_WORKER_HEARTBEAT_SECONDS = 10.0
_WORKER_VERSION = "market_data_collector.v2"


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    logger.info("market_data_collector_shutdown_signal | signum=%s", signum)


def _worker_id() -> str:
    return f"market-data:{socket.gethostname()}:{os.getpid()}"


class _WorkerHeartbeat:
    """Keep idle and in-flight collector process liveness observable."""

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._state = "starting"
        self._active_definition_id: str | None = None
        self._active_attempt_id: str | None = None
        self._last_error: str | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="market-data-collector-heartbeat",
            daemon=True,
        )

    def start(self) -> None:
        market_data_collector.register_worker(
            worker_id=self.worker_id,
            worker_role="scheduled_market_fact_collector",
            worker_version=_WORKER_VERSION,
            ttl_seconds=_WORKER_STATE_TTL_SECONDS,
            state="starting",
            capabilities={
                "fact_types": [
                    "derivatives.open_interest",
                    "derivatives.funding_rate",
                ],
                "concurrency": 1,
            },
            context={"hostname": socket.gethostname(), "pid": os.getpid()},
        )
        self.set_state("idle", publish=True)
        self._thread.start()

    def set_state(
        self,
        state: str,
        *,
        active_definition_id: str | None = None,
        active_attempt_id: str | None = None,
        last_error: str | None = None,
        publish: bool = False,
    ) -> None:
        with self._lock:
            self._state = state
            self._active_definition_id = active_definition_id
            self._active_attempt_id = active_attempt_id
            self._last_error = last_error
        if publish:
            self._publish()

    def _snapshot(self) -> tuple[str, str | None, str | None, str | None]:
        with self._lock:
            return (
                self._state,
                self._active_definition_id,
                self._active_attempt_id,
                self._last_error,
            )

    def _publish(self) -> None:
        state, definition_id, attempt_id, last_error = self._snapshot()
        market_data_collector.heartbeat_worker(
            worker_id=self.worker_id,
            ttl_seconds=_WORKER_STATE_TTL_SECONDS,
            state=state,
            active_definition_id=definition_id,
            active_attempt_id=attempt_id,
            last_error=last_error,
        )

    def _run(self) -> None:
        while not self._stop.wait(_WORKER_HEARTBEAT_SECONDS):
            try:
                self._publish()
            except Exception as exc:
                logger.warning(
                    "market_data_collector_worker_heartbeat_failed | worker_id=%s error=%s",
                    self.worker_id,
                    exc,
                )

    def stop(self) -> None:
        self.set_state("stopping", publish=True)
        self._stop.set()
        self._thread.join(timeout=2.0)
        market_data_collector.stop_worker(worker_id=self.worker_id)


def main() -> int:
    logging.basicConfig(
        level=_SETTINGS.logging.level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    worker_id = _worker_id()
    if not wait_for_database_ready(
        timeout_seconds=_WORKER_SETTINGS.db_wait_timeout_seconds,
        poll_interval_seconds=0.5,
    ):
        logger.error(
            "market_data_collector_db_timeout | worker_id=%s timeout_seconds=%s",
            worker_id,
            _WORKER_SETTINGS.db_wait_timeout_seconds,
        )
        return 2

    heartbeat = _WorkerHeartbeat(worker_id)
    try:
        heartbeat.start()
    except Exception as exc:
        logger.error(
            "market_data_collector_worker_registration_failed | worker_id=%s error=%s",
            worker_id,
            exc,
        )
        return 3

    idle = float(_WORKER_SETTINGS.idle_sleep_seconds)
    idle_max = max(idle, float(_WORKER_SETTINGS.idle_sleep_max_seconds))
    current_idle = idle
    logger.info(
        "market_data_collector_ready | worker_id=%s concurrency=1",
        worker_id,
    )
    while not _STOP:
        heartbeat.set_state("idle")
        try:
            claim = market_data_collector.claim_due(
                owner_id=worker_id,
                lease_seconds=_LEASE_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - durable worker loop
            heartbeat.set_state("degraded", last_error=str(exc), publish=True)
            logger.warning(
                "market_data_collector_claim_retry | worker_id=%s error=%s",
                worker_id,
                exc,
            )
            time.sleep(current_idle)
            current_idle = min(idle_max, current_idle * 2)
            continue
        if claim is None:
            time.sleep(current_idle)
            current_idle = min(idle_max, current_idle * 2)
            continue

        current_idle = idle
        heartbeat.set_state(
            "collecting",
            active_definition_id=claim.definition_id,
            active_attempt_id=claim.attempt_id,
            publish=True,
        )
        started = time.monotonic()
        try:
            result = market_data_collector.collect(
                claim,
                lease_seconds=_LEASE_SECONDS,
            )
            heartbeat.set_state("idle", publish=True)
            logger.info(
                "market_data_collection_succeeded | definition_id=%s attempt_id=%s "
                "series_id=%s scheduled_for=%s commit_seq=%s duration_ms=%.3f",
                claim.definition_id,
                claim.attempt_id,
                claim.series_id,
                claim.scheduled_for.isoformat(),
                result["outcome"]["max_commit_seq"],
                (time.monotonic() - started) * 1000.0,
            )
        except Exception as exc:  # noqa: BLE001 - failure is persisted by service
            heartbeat.set_state("degraded", last_error=str(exc), publish=True)
            logger.warning(
                "market_data_collection_failed | definition_id=%s attempt_id=%s "
                "scheduled_for=%s duration_ms=%.3f error=%s",
                claim.definition_id,
                claim.attempt_id,
                claim.scheduled_for.isoformat(),
                (time.monotonic() - started) * 1000.0,
                exc,
            )

    try:
        heartbeat.stop()
    except Exception as exc:
        logger.warning(
            "market_data_collector_worker_stop_failed | worker_id=%s error=%s",
            worker_id,
            exc,
        )
    logger.info("market_data_collector_stopped | worker_id=%s", worker_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

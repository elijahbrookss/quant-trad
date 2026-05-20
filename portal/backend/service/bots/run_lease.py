"""Runtime-side per-run lease renewal."""

from __future__ import annotations

import logging
import socket
import threading
from typing import Any, Dict, Mapping

from core.settings import get_settings

from ..storage.storage import release_bot_run_lease, renew_bot_run_lease

logger = logging.getLogger(__name__)
_BOT_RUNTIME_SETTINGS = get_settings().bot_runtime


def default_run_lease_runner_id() -> str:
    return socket.gethostname() or "unknown"


class RunLeaseRenewer:
    """Renew a bot run lease while a runtime process is alive."""

    def __init__(
        self,
        *,
        bot_id: str,
        run_id: str,
        runner_id: str,
        lease_token: str,
        ttl_seconds: float | None = None,
        interval_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.bot_id = str(bot_id or "").strip()
        self.run_id = str(run_id or "").strip()
        self.runner_id = str(runner_id or "").strip()
        self.lease_token = str(lease_token or "").strip()
        self.ttl_seconds = float(ttl_seconds or _BOT_RUNTIME_SETTINGS.run_lease_ttl_seconds)
        self.interval_seconds = float(interval_seconds or _BOT_RUNTIME_SETTINGS.run_lease_renew_interval_seconds)
        self.metadata = dict(metadata or {})
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._latest: Dict[str, Any] = {}
        self._failure: Exception | None = None

    @property
    def available(self) -> bool:
        return bool(self.bot_id and self.run_id and self.runner_id and self.lease_token)

    def start(self) -> None:
        if not self.available:
            raise RuntimeError(
                "bot run lease cannot start without bot_id, run_id, runner_id, and lease_token "
                f"bot_id={self.bot_id or '<missing>'} run_id={self.run_id or '<missing>'} runner_id={self.runner_id or '<missing>'}"
            )
        self._renew_once()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"BotRunLeaseRenewer-{self.run_id[:8]}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "bot_run_lease_renewer_started | bot_id=%s | run_id=%s | runner_id=%s | ttl_seconds=%s | interval_seconds=%s",
            self.bot_id,
            self.run_id,
            self.runner_id,
            self.ttl_seconds,
            self.interval_seconds,
        )

    def stop(self, *, release: bool = False, status: str = "released", metadata: Mapping[str, Any] | None = None) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(float(self.interval_seconds), 1.0))
            self._thread = None
        if release and self.available:
            release_bot_run_lease(
                bot_id=self.bot_id,
                run_id=self.run_id,
                runner_id=self.runner_id,
                lease_token=self.lease_token,
                status=status,
                metadata=metadata or self.metadata,
            )
        logger.info(
            "bot_run_lease_renewer_stopped | bot_id=%s | run_id=%s | runner_id=%s | release=%s",
            self.bot_id,
            self.run_id,
            self.runner_id,
            bool(release),
        )

    def assert_healthy(self) -> None:
        with self._lock:
            failure = self._failure
        if failure is not None:
            raise RuntimeError(
                f"bot_run_lease_lost: bot_id={self.bot_id} run_id={self.run_id} runner_id={self.runner_id} error={failure}"
            ) from failure

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            latest = dict(self._latest)
            failure = self._failure
        return {
            "runner_id": self.runner_id,
            "status": latest.get("status"),
            "generation": latest.get("generation"),
            "expires_at": latest.get("expires_at"),
            "renewed_at": latest.get("renewed_at"),
            "ttl_seconds": self.ttl_seconds,
            "renew_interval_seconds": self.interval_seconds,
            "healthy": failure is None,
            "failure": str(failure) if failure is not None else None,
        }

    def _loop(self) -> None:
        while not self._stop_event.wait(max(float(self.interval_seconds), 0.1)):
            try:
                self._renew_once()
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._failure = exc
                logger.exception(
                    "bot_run_lease_renew_failed | bot_id=%s | run_id=%s | runner_id=%s",
                    self.bot_id,
                    self.run_id,
                    self.runner_id,
                )
                self._stop_event.set()
                return

    def _renew_once(self) -> None:
        lease = renew_bot_run_lease(
            bot_id=self.bot_id,
            run_id=self.run_id,
            runner_id=self.runner_id,
            lease_token=self.lease_token,
            ttl_seconds=self.ttl_seconds,
            metadata=self.metadata,
        )
        with self._lock:
            self._latest = dict(lease)
            self._failure = None


__all__ = ["RunLeaseRenewer", "default_run_lease_runner_id"]

"""Utilities for broadcasting bot updates over server-sent events."""

from __future__ import annotations

import logging
import uuid
from queue import Empty, Full, Queue
from threading import Lock
from typing import Callable, Dict, Mapping, Tuple

logger = logging.getLogger(__name__)


class BotStreamManager:
    """Manage SSE subscribers for bot-level updates.

    This keeps subscription bookkeeping isolated from the rest of the bot
    service so broadcast logic stays focused and testable.
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, Queue] = {}
        self._lock = Lock()

    def broadcast(self, event: str, payload: Mapping[str, object]) -> None:
        """Fan out *payload* to all subscribers, tagging it with *event*."""

        message = dict(payload or {})
        message.setdefault("type", event)
        with self._lock:
            channels = list(self._subscribers.values())
        if not channels:
            return
        for channel in channels:
            if not self._offer(channel, message):
                logger.warning("[BotStream] dropping subscriber after repeated enqueue failure")

    def subscribe_all(
        self, snapshot_fn: Callable[[], object]
    ) -> Tuple[Callable[[], None], Queue, Dict[str, object]]:
        """Register a new subscriber and return (release, queue, initial).

        The snapshot callable is invoked immediately to provide the initial
        payload, matching the previous API shape.
        """

        channel: Queue = Queue(maxsize=256)
        token = str(uuid.uuid4())
        with self._lock:
            self._subscribers[token] = channel
        logger.debug("[BotStream] subscriber added", extra={"token": token})

        def _release() -> None:
            with self._lock:
                existing = self._subscribers.pop(token, None)
            if existing:
                self._drain_queue(existing)
            logger.debug("[BotStream] subscriber released", extra={"token": token})

        initial = {"type": "snapshot", "bots": snapshot_fn()}
        return _release, channel, initial

    def _offer(self, channel: Queue, message: Mapping[str, object]) -> bool:
        """Attempt to enqueue *message*, draining once on failure."""

        try:
            channel.put_nowait(message)
            return True
        except Full:
            self._drain_queue(channel)
        try:
            channel.put_nowait(message)
            return True
        except Full:
            return False

    @staticmethod
    def _drain_queue(channel: Queue) -> None:
        """Empty a queue non-blockingly."""

        try:
            while True:
                channel.get_nowait()
        except Empty:
            return

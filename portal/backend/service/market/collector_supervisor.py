"""Generic supervision for worker-owned continuous collector adapters."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping, Protocol, Sequence

from ..storage.repos.market_structure import (
    PostgresMarketStructureRepository,
    market_structure_repository,
)
from .continuous_stream_collector import (
    ContinuousMarketStructureCollector,
    continuous_market_structure_collector,
)


logger = logging.getLogger(__name__)


class ContinuousCollectorAdapter(Protocol):
    """One provider/domain implementation behind the generic supervisor."""

    adapter_id: str

    def supports(self, definition: Mapping[str, Any]) -> bool:
        ...

    async def run(
        self,
        *,
        definition_id: str,
        owner_id: str,
        stop_requested,
        bounded_validation: bool,
    ) -> Mapping[str, Any]:
        ...


class MarketStructureTradeAdapter:
    adapter_id = "coinbase.market_structure_trades.v1"

    def __init__(
        self,
        collector: ContinuousMarketStructureCollector = (
            continuous_market_structure_collector
        ),
    ) -> None:
        self.collector = collector

    def supports(self, definition: Mapping[str, Any]) -> bool:
        return (
            str(definition.get("provider") or "").upper() == "COINBASE"
            and tuple(definition.get("channels") or ())
            == ("market_trades", "heartbeats")
        )

    async def run(
        self,
        *,
        definition_id: str,
        owner_id: str,
        stop_requested,
        bounded_validation: bool,
    ) -> Mapping[str, Any]:
        return await self.collector.run(
            definition_id=definition_id,
            owner_id=owner_id,
            stop_requested=stop_requested,
            bounded_validation=bounded_validation,
        )


class CollectorAdapterRegistry:
    """Explicit registry; unsupported definitions fail loud."""

    def __init__(self, adapters: Sequence[ContinuousCollectorAdapter] = ()) -> None:
        self._adapters: dict[str, ContinuousCollectorAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    def register(self, adapter: ContinuousCollectorAdapter) -> None:
        adapter_id = str(adapter.adapter_id or "").strip()
        if not adapter_id:
            raise ValueError("continuous_collector_adapter_invalid: id is required")
        if adapter_id in self._adapters:
            raise ValueError(
                f"continuous_collector_adapter_duplicate: adapter_id={adapter_id}"
            )
        self._adapters[adapter_id] = adapter

    def resolve(self, definition: Mapping[str, Any]) -> ContinuousCollectorAdapter:
        matches = [
            adapter
            for adapter in self._adapters.values()
            if adapter.supports(definition)
        ]
        if len(matches) != 1:
            raise ValueError(
                "continuous_collector_adapter_resolution_failed: "
                f"definition_id={definition.get('id')} matches={len(matches)}"
            )
        return matches[0]

    def catalog(self) -> list[str]:
        return sorted(self._adapters)


@dataclass
class _TaskState:
    definition_id: str
    adapter_id: str
    mode: str
    stop_at: datetime | None
    stop_event: threading.Event
    task: asyncio.Task
    started_at: datetime
    restart_count: int = 0
    last_error: str | None = None


class ContinuousCollectorSupervisor:
    """Discover enabled definitions and own one task per stream."""

    def __init__(
        self,
        *,
        owner_id: str,
        repository: PostgresMarketStructureRepository = market_structure_repository,
        registry: CollectorAdapterRegistry | None = None,
        poll_seconds: float = 2.0,
    ) -> None:
        self.owner_id = str(owner_id)
        self.repository = repository
        self.registry = registry or CollectorAdapterRegistry(
            (MarketStructureTradeAdapter(),)
        )
        self.poll_seconds = max(0.25, float(poll_seconds))
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._thread_main,
            name="continuous-collector-supervisor",
            daemon=True,
        )
        self._snapshot_lock = threading.Lock()
        self._snapshot: dict[str, Any] = {
            "state": "starting",
            "adapter_catalog": self.registry.catalog(),
            "tasks": {},
            "errors": {},
        }

    def start(self) -> None:
        self._thread.start()

    def stop(self, *, timeout_seconds: float = 30.0) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, timeout_seconds))
        if self._thread.is_alive():
            raise RuntimeError(
                "continuous_collector_supervisor_stop_timeout: "
                f"owner_id={self.owner_id}"
            )

    def snapshot(self) -> dict[str, Any]:
        with self._snapshot_lock:
            return {
                "state": self._snapshot["state"],
                "adapter_catalog": list(self._snapshot["adapter_catalog"]),
                "tasks": {
                    key: dict(value)
                    for key, value in dict(self._snapshot["tasks"]).items()
                },
                "errors": dict(self._snapshot.get("errors") or {}),
            }

    def _publish_snapshot(
        self,
        *,
        state: str,
        tasks: Mapping[str, _TaskState],
        errors: Mapping[str, str] | None = None,
    ) -> None:
        payload = {
            definition_id: {
                "adapter_id": item.adapter_id,
                "mode": item.mode,
                "started_at": item.started_at.isoformat(),
                "stop_at": item.stop_at.isoformat() if item.stop_at else None,
                "restart_count": item.restart_count,
                "last_error": item.last_error,
                "done": item.task.done(),
            }
            for definition_id, item in sorted(tasks.items())
        }
        with self._snapshot_lock:
            self._snapshot = {
                "state": state,
                "adapter_catalog": self.registry.catalog(),
                "tasks": payload,
                "errors": dict(sorted((errors or {}).items())),
            }

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except Exception:
            with self._snapshot_lock:
                self._snapshot["state"] = "failed"
            logger.exception(
                "continuous_collector_supervisor_failed | owner_id=%s",
                self.owner_id,
            )

    async def _run(self) -> None:
        tasks: dict[str, _TaskState] = {}
        restart_after: dict[str, float] = {}
        restart_count: dict[str, int] = {}
        task_errors: dict[str, str] = {}
        self._publish_snapshot(state="running", tasks=tasks, errors=task_errors)
        while not self._stop.is_set():
            definitions = self.repository.list_stream_definitions()
            definitions_by_id = {
                str(item["id"]): item for item in definitions
            }
            now = datetime.now(UTC)
            desired: dict[str, tuple[Mapping[str, Any], str, datetime | None]] = {}
            completed_this_pass: set[str] = set()
            for definition_id, definition in definitions_by_id.items():
                if not bool(definition.get("enabled")):
                    continue
                config = dict(definition.get("config") or {})
                runtime = config.get("collector_runtime")
                if not isinstance(runtime, Mapping):
                    continue
                mode = str(runtime.get("mode") or "").lower()
                if mode not in {"validation", "production"}:
                    continue
                raw_stop = runtime.get("stop_at")
                stop_at = (
                    datetime.fromisoformat(str(raw_stop).replace("Z", "+00:00"))
                    if raw_stop
                    else None
                )
                if stop_at is not None and stop_at.tzinfo is None:
                    stop_at = stop_at.replace(tzinfo=UTC)
                if stop_at is not None and now >= stop_at:
                    continue
                desired[definition_id] = (definition, mode, stop_at)

            for definition_id, state in list(tasks.items()):
                desired_item = desired.get(definition_id)
                changed = bool(
                    desired_item is not None
                    and (
                        desired_item[1] != state.mode
                        or desired_item[2] != state.stop_at
                    )
                )
                if desired_item is None or changed or self._stop.is_set():
                    state.stop_event.set()
                if not state.task.done():
                    continue
                completed_this_pass.add(definition_id)
                try:
                    result = state.task.result()
                    logger.info(
                        "continuous_collector_task_stopped | definition_id=%s "
                        "adapter_id=%s mode=%s result=%s",
                        definition_id,
                        state.adapter_id,
                        state.mode,
                        dict(result),
                    )
                    if state.mode == "validation":
                        self.repository.configure_continuous_runtime(
                            definition_id=definition_id,
                            enabled=False,
                            mode="stopped",
                            requested_by=f"supervisor:{self.owner_id}",
                            policy={},
                        )
                except Exception as exc:
                    state.last_error = f"{type(exc).__name__}: {exc}"
                    task_errors[definition_id] = state.last_error
                    logger.error(
                        "continuous_collector_task_failed | definition_id=%s "
                        "adapter_id=%s mode=%s restart_count=%s error=%s",
                        definition_id,
                        state.adapter_id,
                        state.mode,
                        state.restart_count,
                        exc,
                    )
                    if state.mode == "validation":
                        self.repository.configure_continuous_runtime(
                            definition_id=definition_id,
                            enabled=False,
                            mode="stopped",
                            requested_by=f"supervisor:{self.owner_id}",
                            policy={},
                        )
                    else:
                        count = restart_count.get(definition_id, 0) + 1
                        restart_count[definition_id] = count
                        restart_after[definition_id] = (
                            time.monotonic() + min(60.0, float(2 ** min(count, 6)))
                        )
                del tasks[definition_id]

            for definition_id, (definition, mode, stop_at) in desired.items():
                if definition_id in tasks or definition_id in completed_this_pass:
                    continue
                if time.monotonic() < restart_after.get(definition_id, 0.0):
                    continue
                try:
                    adapter = self.registry.resolve(definition)
                except Exception as exc:
                    task_errors[definition_id] = f"{type(exc).__name__}: {exc}"
                    restart_after[definition_id] = time.monotonic() + 30.0
                    logger.error(
                        "continuous_collector_definition_rejected | "
                        "definition_id=%s mode=%s error=%s",
                        definition_id,
                        mode,
                        exc,
                    )
                    continue
                task_errors.pop(definition_id, None)
                stop_event = threading.Event()
                task = asyncio.create_task(
                    adapter.run(
                        definition_id=definition_id,
                        owner_id=f"{self.owner_id}:{definition_id}",
                        stop_requested=lambda event=stop_event, deadline=stop_at: (
                            self._stop.is_set()
                            or event.is_set()
                            or (
                                deadline is not None
                                and datetime.now(UTC) >= deadline
                            )
                        ),
                        bounded_validation=(mode == "validation"),
                    )
                )
                tasks[definition_id] = _TaskState(
                    definition_id=definition_id,
                    adapter_id=adapter.adapter_id,
                    mode=mode,
                    stop_at=stop_at,
                    stop_event=stop_event,
                    task=task,
                    started_at=datetime.now(UTC),
                    restart_count=restart_count.get(definition_id, 0),
                )
                logger.info(
                    "continuous_collector_task_started | definition_id=%s "
                    "adapter_id=%s mode=%s stop_at=%s",
                    definition_id,
                    adapter.adapter_id,
                    mode,
                    stop_at.isoformat() if stop_at else None,
                )
            self._publish_snapshot(
                state="running", tasks=tasks, errors=task_errors
            )
            await asyncio.sleep(self.poll_seconds)

        for state in tasks.values():
            state.stop_event.set()
        if tasks:
            done, pending = await asyncio.wait(
                [state.task for state in tasks.values()],
                timeout=30.0,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    task.result()
        self._publish_snapshot(state="stopped", tasks={}, errors=task_errors)


__all__ = [
    "CollectorAdapterRegistry",
    "ContinuousCollectorAdapter",
    "ContinuousCollectorSupervisor",
    "MarketStructureTradeAdapter",
]

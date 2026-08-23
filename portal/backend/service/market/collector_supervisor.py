"""Generic supervision for worker-owned continuous collector adapters."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping, Protocol, Sequence

from market_data.collector_operations import CollectorAction, CollectorKind

from ..storage.repos.market_structure import (
    PostgresMarketStructureRepository,
    market_structure_repository,
)
from ..storage.repos.collector_operations import (
    PostgresCollectorOperationsRepository,
    collector_operations_repository,
)
from .continuous_stream_collector import DEFAULT_STORAGE_ROOT
from .continuous_stream_collector import (
    CoinbaseContinuousTransportAdapter,
    CoinbaseLevel2BookProjectionAdapter,
    CoinbaseMarketTradeProjectionAdapter,
)
from .continuous_stream_runtime import (
    ContinuousStreamRuntime,
    continuous_stream_runtime,
)
from .collector_safety import evaluate_collector_safety


logger = logging.getLogger(__name__)


class ContinuousCollectorAdapter(Protocol):
    """One provider/domain implementation behind the generic supervisor."""

    adapter_id: str

    def supports(self, definition: Mapping[str, Any]) -> bool:
        ...

    def registration_errors(self, definition: Mapping[str, Any]) -> list[str]:
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


class CoinbaseMarketTradeCollectorAdapter:
    """Compose Coinbase transport with the market-trade projection."""

    adapter_id = "coinbase.market_structure_trades.v1"

    def __init__(
        self,
        collector: ContinuousStreamRuntime = continuous_stream_runtime,
        projection=None,
        transport=None,
    ) -> None:
        self.collector = collector
        self.projection = projection or CoinbaseMarketTradeProjectionAdapter()
        self.transport = transport or CoinbaseContinuousTransportAdapter()

    def supports(self, definition: Mapping[str, Any]) -> bool:
        return self.transport.supports(definition) and tuple(
            definition.get("channels") or ()
        ) == self.projection.channels

    def registration_errors(self, definition: Mapping[str, Any]) -> list[str]:
        config = dict(definition.get("config") or {})
        reasons = []
        for key in (
            "product_definition_version_id",
            "aggregate_series_ids",
            "flow_feature_series_ids",
        ):
            if not config.get(key):
                reasons.append(f"config_missing:{key}")
        for key in ("aggregate_series_ids", "flow_feature_series_ids"):
            values = dict(config.get(key) or {})
            if not {"1", "60"}.issubset(values):
                reasons.append(f"config_incomplete:{key}")
        return reasons

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
            projection=self.projection,
            transport=self.transport,
        )


class CoinbaseLevel2CollectorAdapter(CoinbaseMarketTradeCollectorAdapter):
    """Compose Coinbase transport with the stateful Level 2 projection."""

    adapter_id = "coinbase.market_structure_level2.v1"

    def __init__(
        self,
        collector: ContinuousStreamRuntime = continuous_stream_runtime,
    ) -> None:
        super().__init__(
            collector,
            projection=CoinbaseLevel2BookProjectionAdapter(),
            transport=CoinbaseContinuousTransportAdapter(),
        )

    def supports(self, definition: Mapping[str, Any]) -> bool:
        return self.transport.supports(definition) and tuple(
            definition.get("channels") or ()
        ) == self.projection.channels

    def registration_errors(self, definition: Mapping[str, Any]) -> list[str]:
        config = dict(definition.get("config") or {})
        reasons = []
        for key in (
            "product_definition_version_id",
            "bbo_series_id",
            "depth_series_id",
            "response_feature_series_id",
            "trade_series_id",
            "flow_feature_series_ids",
        ):
            if not config.get(key):
                reasons.append(f"config_missing:{key}")
        flow_series = dict(config.get("flow_feature_series_ids") or {})
        if not {"1", "60"}.issubset(flow_series):
            reasons.append("config_incomplete:flow_feature_series_ids")
        return reasons


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
    control_generation: int
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
        operations_repository: PostgresCollectorOperationsRepository = (
            collector_operations_repository
        ),
        registry: CollectorAdapterRegistry | None = None,
        poll_seconds: float = 2.0,
    ) -> None:
        self.owner_id = str(owner_id)
        self.repository = repository
        self.operations_repository = operations_repository
        self.registry = registry or CollectorAdapterRegistry(
            (
                CoinbaseMarketTradeCollectorAdapter(),
                CoinbaseLevel2CollectorAdapter(),
            )
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
                "control_generation": item.control_generation,
                "started_at": item.started_at.isoformat(),
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
            desired: dict[str, tuple[Mapping[str, Any], int]] = {}
            completed_this_pass: set[str] = set()
            for definition_id, definition in definitions_by_id.items():
                if not bool(definition.get("enabled")) or str(
                    definition.get("desired_state") or ""
                ) != "running":
                    continue
                adapter_supported = True
                try:
                    self.registry.resolve(definition)
                except ValueError:
                    adapter_supported = False
                qualification, safety = evaluate_collector_safety(
                    definition=definition,
                    repository=self.repository,
                    adapter_supported=adapter_supported,
                    storage_root=DEFAULT_STORAGE_ROOT,
                )
                if safety.severity == "warning":
                    window = int(now.timestamp()) // 300
                    reason_key = ",".join(safety.reasons)
                    request_digest = hashlib.sha256(
                        f"{definition_id}:{window}:{reason_key}".encode("utf-8")
                    ).hexdigest()
                    self.repository.record_safety_event(
                        request_id=f"auto-warning:{request_digest}",
                        scope_type="stream",
                        scope_id=definition_id,
                        event_type="warning",
                        severity="warning",
                        actor_id=f"supervisor:{self.owner_id}",
                        reason=reason_key,
                        policy_hash=qualification.policy_hash,
                        evidence=safety.evidence,
                    )
                if not qualification.qualified:
                    if "safety_halt_active" not in qualification.reasons:
                        reason_key = ",".join(qualification.reasons)
                        request_digest = hashlib.sha256(
                            (
                                f"{definition_id}:{qualification.policy_hash}:"
                                f"{reason_key}"
                            ).encode("utf-8")
                        ).hexdigest()
                        self.repository.record_safety_event(
                            request_id=f"auto-halt:{request_digest}",
                            scope_type="stream",
                            scope_id=definition_id,
                            event_type="halted",
                            severity="critical",
                            actor_id=f"supervisor:{self.owner_id}",
                            reason=reason_key,
                            policy_hash=qualification.policy_hash,
                            evidence=safety.evidence,
                        )
                        self.operations_repository.apply_lifecycle_action(
                            request_id=f"auto-pause:{request_digest}",
                            collector_id=definition_id,
                            collector_kind=CollectorKind.CONTINUOUS_STREAM,
                            action=CollectorAction.PAUSE,
                            requested_at=now,
                            actor_id=f"supervisor:{self.owner_id}",
                            context={
                                "reason": "collector_safety_not_qualified",
                                "policy_hash": qualification.policy_hash,
                                "evidence": safety.evidence,
                            },
                        )
                    task_errors[definition_id] = (
                        "collector_safety_not_qualified: "
                        + ",".join(qualification.reasons)
                    )
                    continue
                desired[definition_id] = (
                    definition,
                    int(definition.get("control_generation") or 0),
                )

            for definition_id, state in list(tasks.items()):
                desired_item = desired.get(definition_id)
                changed = bool(
                    desired_item is not None
                    and desired_item[1] != state.control_generation
                )
                expected_stop = bool(
                    desired_item is None or changed or self._stop.is_set()
                )
                if expected_stop:
                    state.stop_event.set()
                if not state.task.done():
                    continue
                completed_this_pass.add(definition_id)
                try:
                    result = state.task.result()
                    if expected_stop:
                        logger.info(
                            "continuous_collector_task_stopped | definition_id=%s "
                            "adapter_id=%s control_generation=%s result=%s",
                            definition_id,
                            state.adapter_id,
                            state.control_generation,
                            dict(result),
                        )
                    else:
                        state.last_error = "continuous_collector_stopped_unexpectedly"
                        task_errors[definition_id] = state.last_error
                        count = restart_count.get(definition_id, 0) + 1
                        restart_count[definition_id] = count
                        restart_after[definition_id] = (
                            time.monotonic()
                            + min(60.0, float(2 ** min(count, 6)))
                        )
                        logger.error(
                            "continuous_collector_task_stopped_unexpectedly | "
                            "definition_id=%s adapter_id=%s restart_count=%s result=%s",
                            definition_id,
                            state.adapter_id,
                            count,
                            dict(result),
                        )
                except Exception as exc:
                    state.last_error = f"{type(exc).__name__}: {exc}"
                    task_errors[definition_id] = state.last_error
                    if expected_stop:
                        logger.warning(
                            "continuous_collector_task_stop_failed | definition_id=%s "
                            "adapter_id=%s control_generation=%s error=%s",
                            definition_id,
                            state.adapter_id,
                            state.control_generation,
                            exc,
                        )
                    else:
                        count = restart_count.get(definition_id, 0) + 1
                        restart_count[definition_id] = count
                        restart_after[definition_id] = (
                            time.monotonic() + min(60.0, float(2 ** min(count, 6)))
                        )
                        logger.error(
                            "continuous_collector_task_failed | definition_id=%s "
                            "adapter_id=%s control_generation=%s restart_count=%s error=%s",
                            definition_id,
                            state.adapter_id,
                            state.control_generation,
                            count,
                            exc,
                        )
                del tasks[definition_id]

            for definition_id, (definition, control_generation) in desired.items():
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
                        "definition_id=%s control_generation=%s error=%s",
                        definition_id,
                        control_generation,
                        exc,
                    )
                    continue
                task_errors.pop(definition_id, None)
                stop_event = threading.Event()
                task = asyncio.create_task(
                    adapter.run(
                        definition_id=definition_id,
                        owner_id=f"{self.owner_id}:{definition_id}",
                        stop_requested=lambda event=stop_event: (
                            self._stop.is_set() or event.is_set()
                        ),
                        bounded_validation=False,
                    )
                )
                tasks[definition_id] = _TaskState(
                    definition_id=definition_id,
                    adapter_id=adapter.adapter_id,
                    control_generation=control_generation,
                    stop_event=stop_event,
                    task=task,
                    started_at=datetime.now(UTC),
                    restart_count=restart_count.get(definition_id, 0),
                )
                logger.info(
                    "continuous_collector_task_started | definition_id=%s "
                    "adapter_id=%s control_generation=%s",
                    definition_id,
                    adapter.adapter_id,
                    control_generation,
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


MarketStructureTradeAdapter = CoinbaseMarketTradeCollectorAdapter
MarketStructureL2Adapter = CoinbaseLevel2CollectorAdapter


__all__ = [
    "CoinbaseLevel2CollectorAdapter",
    "CoinbaseMarketTradeCollectorAdapter",
    "CollectorAdapterRegistry",
    "ContinuousCollectorAdapter",
    "ContinuousCollectorSupervisor",
    "MarketStructureTradeAdapter",
    "MarketStructureL2Adapter",
]

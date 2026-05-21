from __future__ import annotations

import queue

import pytest


def test_drain_parent_event_queue_preserves_shared_fanin_order() -> None:
    pytest.importorskip("sqlalchemy")

    from portal.backend.service.bots.container_runtime import _drain_parent_event_queue

    parent_queue: queue.Queue[dict] = queue.Queue()
    for payload in [
        {"worker_id": "worker-1", "index": 0},
        {"worker_id": "worker-2", "index": 0},
        {"worker_id": "worker-1", "index": 1},
        {"worker_id": "worker-3", "index": 0},
        {"worker_id": "worker-2", "index": 1},
        {"worker_id": "worker-1", "index": 2},
    ]:
        parent_queue.put(payload)

    handled: list[str] = []

    def _handle(worker_id: str, _event: dict) -> None:
        handled.append(worker_id)

    drained = _drain_parent_event_queue(
        event_queue=parent_queue,
        handle_event=_handle,
    )

    assert handled == [
        "worker-1",
        "worker-2",
        "worker-1",
        "worker-3",
        "worker-2",
        "worker-1",
    ]
    assert drained == {
        "worker-1": 3,
        "worker-2": 2,
        "worker-3": 1,
    }


def test_parent_event_queue_maxsize_preserves_total_worker_capacity() -> None:
    pytest.importorskip("sqlalchemy")

    from portal.backend.service.bots.container_runtime import _parent_event_queue_maxsize

    single_worker_capacity = _parent_event_queue_maxsize(worker_count=1)

    assert single_worker_capacity >= 8
    assert _parent_event_queue_maxsize(worker_count=3) == single_worker_capacity * 3

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from portal.backend.workers.market_data_collector_health import live_worker_for_host


class _Repository:
    def __init__(self, rows):
        self.rows = rows

    def list_worker_states(self, *, limit):
        assert limit == 1000
        return list(self.rows)


def test_collector_health_uses_database_heartbeat_for_current_container() -> None:
    now = datetime.now(UTC)
    row = live_worker_for_host(
        repository=_Repository(
            [
                {
                    "worker_id": "market-data:other-host:1",
                    "state": "idle",
                    "alive": True,
                    "heartbeat_at": now,
                },
                {
                    "worker_id": "market-data:collector-host:42",
                    "state": "degraded",
                    "alive": True,
                    "heartbeat_at": now,
                },
            ]
        ),
        hostname="collector-host",
    )

    assert row["worker_id"] == "market-data:collector-host:42"
    assert row["state"] == "degraded"


def test_collector_health_rejects_stale_or_missing_heartbeat() -> None:
    with pytest.raises(RuntimeError, match="no live worker heartbeat"):
        live_worker_for_host(
            repository=_Repository(
                [
                    {
                        "worker_id": "market-data:collector-host:42",
                        "state": "stopped",
                        "alive": False,
                        "heartbeat_at": datetime.now(UTC),
                    }
                ]
            ),
            hostname="collector-host",
        )


def test_collector_health_rejects_failed_continuous_supervisor() -> None:
    with pytest.raises(RuntimeError, match="continuous supervisor failed"):
        live_worker_for_host(
            repository=_Repository(
                [
                    {
                        "worker_id": "market-data:collector-host:42",
                        "state": "idle",
                        "alive": True,
                        "heartbeat_at": datetime.now(UTC),
                        "context": {
                            "continuous_collectors": {"state": "failed"}
                        },
                    }
                ]
            ),
            hostname="collector-host",
        )

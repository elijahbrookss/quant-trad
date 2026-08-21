"""Container health probe for the durable market-data worker heartbeat."""

from __future__ import annotations

import json
import socket
import sys
from collections.abc import Mapping
from typing import Any

from portal.backend.service.storage.repos.market_collection import (
    PostgresMarketCollectionRepository,
    market_collection_repo,
)


def live_worker_for_host(
    *,
    repository: PostgresMarketCollectionRepository = market_collection_repo,
    hostname: str | None = None,
) -> dict[str, Any]:
    """Return this container's live worker row or fail the health probe."""

    normalized_host = str(hostname or socket.gethostname()).strip()
    if not normalized_host:
        raise RuntimeError("market_data_collector_health_invalid: hostname is empty")
    worker_prefix = f"market-data:{normalized_host}:"
    matches = [
        dict(row)
        for row in repository.list_worker_states(limit=1000)
        if isinstance(row, Mapping)
        and str(row.get("worker_id") or "").startswith(worker_prefix)
        and bool(row.get("alive"))
    ]
    if not matches:
        raise RuntimeError(
            "market_data_collector_health_failed: no live worker heartbeat "
            f"for hostname={normalized_host}"
        )
    latest = max(matches, key=lambda row: row.get("heartbeat_at"))
    context = dict(latest.get("context") or {})
    continuous = dict(context.get("continuous_collectors") or {})
    if str(continuous.get("state") or "").lower() == "failed":
        raise RuntimeError(
            "market_data_collector_health_failed: continuous supervisor failed "
            f"for hostname={normalized_host}"
        )
    return latest


def main() -> int:
    try:
        row = live_worker_for_host()
    except Exception as exc:  # noqa: BLE001 - health probes fail closed
        print(str(exc), file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": "healthy",
                "worker_id": str(row["worker_id"]),
                "state": str(row["state"]),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

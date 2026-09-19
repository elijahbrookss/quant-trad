"""Container health probe for the durable market-data worker heartbeat."""

from __future__ import annotations

import json
import os
import socket
import sys
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portal.backend.service.storage.repos.market_collection import PostgresMarketCollectionRepository


def _read_live_heartbeat(worker_prefix: str) -> list[dict[str, Any]]:
    # A health check must not initialize/provision the application schema on
    # every invocation. The worker owns startup validation and its heartbeat.
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool

    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("market_data_collector_health_failed: PG_DSN is not configured")
    engine = create_engine(dsn, poolclass=NullPool, hide_parameters=True,
        connect_args={"connect_timeout": 2,
            "options": "-c default_transaction_read_only=on -c statement_timeout=1500 -c lock_timeout=1000"})
    try:
        with engine.connect() as connection:
            rows = connection.execute(text("""
                SELECT worker_id, state, heartbeat_at, context, true AS alive
                FROM market.collector_worker_state
                WHERE expires_at > now() AND state NOT IN ('stopping', 'stopped')
                  AND left(worker_id, length(:prefix)) = :prefix
                ORDER BY heartbeat_at DESC, worker_id LIMIT 1
            """), {"prefix": worker_prefix}).mappings().all()
            return [dict(row) for row in rows]
    finally:
        engine.dispose()


def live_worker_for_host(
    *,
    repository: PostgresMarketCollectionRepository | None = None,
    hostname: str | None = None,
) -> dict[str, Any]:
    """Return this container's live worker row or fail the health probe."""

    normalized_host = str(hostname or socket.gethostname()).strip()
    if not normalized_host:
        raise RuntimeError("market_data_collector_health_invalid: hostname is empty")
    worker_prefix = f"market-data:{normalized_host}:"
    rows = repository.list_worker_states(limit=1000) if repository is not None else _read_live_heartbeat(worker_prefix)
    matches = [
        dict(row)
        for row in rows
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

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


def test_default_health_reads_only_current_host_heartbeat(monkeypatch):
    from portal.backend.workers import market_data_collector_health as health
    expected = {"worker_id": "market-data:fixture:1", "state": "idle", "alive": True,
                "heartbeat_at": datetime.now(UTC)}
    observed = []
    monkeypatch.setattr(health, "_read_live_heartbeat", lambda prefix: observed.append(prefix) or [expected])
    assert health.live_worker_for_host(hostname="fixture") == expected
    assert observed == ["market-data:fixture:"]


def test_health_database_read_is_bounded_readonly_and_disposes_engine(monkeypatch):
    import sqlalchemy
    from portal.backend.workers import market_data_collector_health as health
    from sqlalchemy.pool import NullPool
    observed = {}

    class Rows:
        def mappings(self): return self
        def all(self): return []

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, statement, params):
            assert "FROM market.collector_worker_state" in str(statement)
            assert "LIMIT 1" in str(statement)
            assert params == {"prefix": "market-data:fixture:"}
            return Rows()

    class Engine:
        def connect(self): return Connection()
        def dispose(self): observed["disposed"] = True

    def create(dsn, **options):
        assert dsn == "postgresql+psycopg2://synthetic/fixture"
        observed.update(options)
        return Engine()

    monkeypatch.setenv("PG_DSN", "postgresql+psycopg2://synthetic/fixture")
    monkeypatch.setattr(sqlalchemy, "create_engine", create)
    assert health._read_live_heartbeat("market-data:fixture:") == []
    assert observed["poolclass"] is NullPool and observed["disposed"]
    assert observed["connect_args"]["connect_timeout"] == 2
    assert "default_transaction_read_only=on" in observed["connect_args"]["options"]
    assert "statement_timeout=1500" in observed["connect_args"]["options"]

"""Database session management helpers for the portal backend."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateTable

from .models import Base


logger = logging.getLogger(__name__)
_SCHEMA_LOCK_KEY = 9021001


class Database:
    """Lightweight wrapper around SQLAlchemy engine/session handling."""

    def __init__(self) -> None:
        self._engine = None
        self._session_factory: Optional[sessionmaker] = None
        self._available = False
        self._error: Optional[Exception] = None
        self.dsn = self._resolve_dsn()

    @staticmethod
    def _resolve_dsn() -> str:
        """Return the configured PostgreSQL DSN."""

        value = os.getenv("PG_DSN")
        if value:
            return value
        raise RuntimeError("PG_DSN is required. No SQLite fallback is supported.")

    @staticmethod
    def _env_flag(name: str, default: bool) -> bool:
        raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
        return raw in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw in (None, ""):
            return int(default)
        try:
            return int(raw)
        except (TypeError, ValueError):
            return int(default)

    def _engine_options(self) -> Dict[str, object]:
        """Build SQLAlchemy engine options with liveness guards enabled by default."""

        pool_recycle = max(-1, self._env_int("PG_POOL_RECYCLE_SECONDS", 900))
        pool_timeout = max(1, self._env_int("PG_POOL_TIMEOUT_SECONDS", 30))
        connect_timeout = max(1, self._env_int("PG_CONNECT_TIMEOUT_SECONDS", 5))

        connect_args: Dict[str, object] = {
            "connect_timeout": connect_timeout,
            "application_name": str(os.getenv("PG_APPLICATION_NAME", "quant_trad_portal")).strip()
            or "quant_trad_portal",
        }

        # TCP keepalive improves resilience to dead sockets/network middleboxes.
        if self._env_flag("PG_TCP_KEEPALIVE_ENABLED", True):
            connect_args["keepalives"] = 1
            connect_args["keepalives_idle"] = max(1, self._env_int("PG_TCP_KEEPALIVE_IDLE_SECONDS", 30))
            connect_args["keepalives_interval"] = max(
                1, self._env_int("PG_TCP_KEEPALIVE_INTERVAL_SECONDS", 10)
            )
            connect_args["keepalives_count"] = max(1, self._env_int("PG_TCP_KEEPALIVE_COUNT", 3))

        return {
            "future": True,
            "pool_pre_ping": self._env_flag("PG_POOL_PRE_PING", True),
            "pool_recycle": pool_recycle,
            "pool_timeout": pool_timeout,
            "connect_args": connect_args,
        }

    def ensure_schema(self) -> bool:
        """Initialise the database engine and create tables if required."""

        if self._engine is not None and self._available:
            return True
        try:
            if self._engine is None:
                self._engine = create_engine(self.dsn, **self._engine_options())
            if self._session_factory is None:
                self._session_factory = sessionmaker(
                    bind=self._engine,
                    expire_on_commit=False,
                    autoflush=False,
                    future=True,
                )
            self._create_tables_if_not_exists()
            self._assert_schema_contract()
            self._available = True
            logger.info("portal_db_ready | dsn=%s", self.dsn)
        except SQLAlchemyError as exc:
            self._error = exc
            self._available = False
            self._reset_engine()
            logger.warning("portal_db_unavailable | dsn=%s | error=%s", self.dsn, exc)
        except Exception as exc:  # noqa: BLE001 - defensive catch
            self._error = exc
            self._available = False
            self._reset_engine()
            logger.exception("portal_db_initialise_failed | dsn=%s", self.dsn)
        return self._available

    @contextmanager
    def session(self) -> Iterator[Session]:
        """Yield a SQLAlchemy session, committing on success."""

        if not self.ensure_schema():
            raise RuntimeError("Portal database is not available")
        assert self._session_factory is not None  # for mypy/static hints
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:  # noqa: BLE001 - commit/rollback guard
            session.rollback()
            raise
        finally:
            session.close()

    def _create_tables_if_not_exists(self) -> None:
        """Create ORM tables using PostgreSQL IF NOT EXISTS semantics."""

        if not self._engine:
            return
        with self._engine.begin() as conn:
            # Serialize schema DDL across backend + workers.
            conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": _SCHEMA_LOCK_KEY})
            try:
                for table in Base.metadata.sorted_tables:
                    conn.execute(CreateTable(table, if_not_exists=True))
            finally:
                conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _SCHEMA_LOCK_KEY})

    def _reset_engine(self) -> None:
        """Dispose engine/session so the next readiness check can retry init cleanly."""

        if self._engine is not None:
            self._engine.dispose()
        self._engine = None
        self._session_factory = None

    def reset_for_fork(self) -> None:
        """Reset inherited engine/session state when running in a forked process."""

        self.reset_connection_state()

    def reset_connection_state(self) -> None:
        """Dispose engine/session so future operations reopen fresh DB connections."""

        self._reset_engine()
        self._available = False
        self._error = None

    def _assert_schema_contract(self) -> None:
        """Assert tables/columns match ORM contract; create missing tables once."""
        if not self._engine:
            return
        inspector = inspect(self._engine)
        table_names = set(inspector.get_table_names())

        def require_table(name: str) -> None:
            if name not in table_names:
                Base.metadata.tables[name].create(self._engine, checkfirst=True)
                logger.warning("portal_db_table_created | table=%s", name)

        def assert_columns(name: str) -> None:
            expected = {column.name for column in Base.metadata.tables[name].columns}
            existing = {col["name"] for col in inspector.get_columns(name)}
            missing = sorted(set(expected) - existing)
            if missing:
                logger.error(
                    "portal_db_column_mismatch | table=%s | missing=%s",
                    name,
                    ",".join(missing),
                )
                raise RuntimeError(
                    f"Table '{name}' is missing columns: {', '.join(missing)}. "
                    "Drop the table or rebuild the database to ensure a clean schema."
                )

        require_table("portal_bot_runs")
        require_table("portal_bot_run_steps")
        require_table("portal_bot_trades")
        require_table("portal_bots")
        require_table("portal_async_jobs")
        require_table("portal_bot_run_events")
        require_table("portal_bot_run_view_state")
        assert_columns("portal_bot_run_steps")
        assert_columns("portal_bot_trades")
        assert_columns("portal_bots")
        assert_columns("portal_async_jobs")
        assert_columns("portal_bot_run_events")
        assert_columns("portal_bot_run_view_state")

    @property
    def available(self) -> bool:
        """Return whether the database is reachable."""

        return self.ensure_schema()

    @property
    def last_error(self) -> Optional[Exception]:
        """Return the last connection error, if any."""

        return self._error


db = Database()

__all__ = ["db", "Database"]

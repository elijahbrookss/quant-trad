"""Database session management helpers for the portal backend."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from core.settings import get_settings
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateSchema, CreateTable

from .models import Base, REQUIRED_BOT_RUN_EVENT_INDEXES


logger = logging.getLogger(__name__)
_SCHEMA_LOCK_KEY = 9021001
_DB_SETTINGS = get_settings().database


class Database:
    """Lightweight wrapper around SQLAlchemy engine/session handling."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._engine = None
        self._session_factory: Optional[sessionmaker] = None
        self._available = False
        self._error: Optional[Exception] = None
        self.dsn = str(dsn).strip() if dsn else None

    @staticmethod
    def _resolve_dsn() -> str:
        """Return the configured PostgreSQL DSN."""
        value = _DB_SETTINGS.dsn
        if value:
            return value
        raise RuntimeError("PG_DSN is required. No SQLite fallback is supported.")

    def _engine_options(self) -> Dict[str, object]:
        """Build SQLAlchemy engine options with liveness guards enabled by default."""

        connect_args: Dict[str, object] = {
            "connect_timeout": _DB_SETTINGS.connect_timeout_seconds,
            "application_name": _DB_SETTINGS.application_name or "quant_trad_portal",
        }

        # TCP keepalive improves resilience to dead sockets/network middleboxes.
        if _DB_SETTINGS.tcp_keepalive_enabled:
            connect_args["keepalives"] = 1
            connect_args["keepalives_idle"] = _DB_SETTINGS.tcp_keepalive_idle_seconds
            connect_args["keepalives_interval"] = _DB_SETTINGS.tcp_keepalive_interval_seconds
            connect_args["keepalives_count"] = _DB_SETTINGS.tcp_keepalive_count

        return {
            "future": True,
            "pool_pre_ping": _DB_SETTINGS.pool_pre_ping,
            "pool_recycle": _DB_SETTINGS.pool_recycle_seconds,
            "pool_timeout": _DB_SETTINGS.pool_timeout_seconds,
            "connect_args": connect_args,
        }

    def ensure_schema(self) -> bool:
        """Initialise the database engine and create tables if required."""

        if self._engine is not None and self._available:
            return True
        try:
            if not self.dsn:
                self.dsn = self._resolve_dsn()
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
                for schema_name in sorted(
                    {
                        str(table.schema)
                        for table in Base.metadata.sorted_tables
                        if str(table.schema or "").strip()
                    }
                ):
                    conn.execute(CreateSchema(schema_name, if_not_exists=True))
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
        table_names_by_schema: Dict[Optional[str], set[str]] = {}

        def _schema_table_names(schema: Optional[str]) -> set[str]:
            key = str(schema).strip() if schema is not None else None
            if key not in table_names_by_schema:
                table_names_by_schema[key] = set(inspector.get_table_names(schema=key))
            return table_names_by_schema[key]

        def require_table(name: str, *, schema: Optional[str] = None) -> None:
            metadata_key = f"{schema}.{name}" if schema else name
            if name not in _schema_table_names(schema):
                Base.metadata.tables[metadata_key].create(self._engine, checkfirst=True)
                logger.warning("portal_db_table_created | schema=%s | table=%s", schema or "public", name)
                _schema_table_names(schema).add(name)

        def assert_columns(name: str, *, schema: Optional[str] = None) -> None:
            metadata_key = f"{schema}.{name}" if schema else name
            expected = {column.name for column in Base.metadata.tables[metadata_key].columns}
            existing = {col["name"] for col in inspector.get_columns(name, schema=schema)}
            missing = sorted(set(expected) - existing)
            if missing:
                logger.error(
                    "portal_db_column_mismatch | schema=%s | table=%s | missing=%s",
                    schema or "public",
                    name,
                    ",".join(missing),
                )
                raise RuntimeError(
                    f"Table '{schema + '.' if schema else ''}{name}' is missing columns: {', '.join(missing)}. "
                    "Drop the table or rebuild the database to ensure a clean schema."
                )

        def warn_missing_indexes(name: str, required: set[str] | frozenset[str], *, schema: Optional[str] = None) -> None:
            existing = {str(index.get("name") or "") for index in inspector.get_indexes(name, schema=schema)}
            missing = sorted(set(required) - existing)
            if missing:
                logger.warning(
                    "portal_db_required_indexes_missing | schema=%s | table=%s | missing=%s | migration=%s",
                    schema or "public",
                    name,
                    ",".join(missing),
                    "scripts/db/manual_migration_botlens_runtime_event_storage_efficiency_v2.sql",
                )

        require_table("portal_bot_runs")
        require_table("portal_bot_run_steps")
        require_table("portal_bot_run_lifecycle")
        require_table("portal_bot_run_lifecycle_events")
        require_table("portal_bot_trades")
        require_table("portal_bots")
        require_table("portal_strategies")
        require_table("portal_strategy_rules")
        require_table("portal_strategy_indicators")
        require_table("portal_strategy_instruments")
        require_table("portal_strategy_variants")
        require_table("portal_async_jobs")
        require_table("portal_bot_run_events")
        require_table("botlens_backend_events_v1", schema="observability_events")
        require_table("botlens_backend_metric_samples_v1", schema="observability_metrics")
        assert_columns("portal_bot_run_steps")
        assert_columns("portal_bot_run_lifecycle")
        assert_columns("portal_bot_run_lifecycle_events")
        assert_columns("portal_bot_trades")
        assert_columns("portal_bots")
        assert_columns("portal_strategies")
        assert_columns("portal_strategy_rules")
        assert_columns("portal_strategy_indicators")
        assert_columns("portal_strategy_instruments")
        assert_columns("portal_strategy_variants")
        assert_columns("portal_async_jobs")
        assert_columns("portal_bot_run_events")
        assert_columns("botlens_backend_events_v1", schema="observability_events")
        assert_columns("botlens_backend_metric_samples_v1", schema="observability_metrics")
        warn_missing_indexes("portal_bot_run_events", REQUIRED_BOT_RUN_EVENT_INDEXES)

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

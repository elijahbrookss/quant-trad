"""Database session management helpers for the portal backend."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from core.settings import get_settings
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable

from .models import (
    Base,
    REQUIRED_BOT_RUN_INDEXES,
    REQUIRED_BOT_RUN_EVENT_INDEXES,
    REQUIRED_BOT_RUN_LEASE_INDEXES,
    REQUIRED_BOT_RUN_LIFECYCLE_INDEXES,
    REQUIRED_PROVIDER_CREDENTIAL_INDEXES,
    REQUIRED_REPORT_MATERIALIZATION_INDEXES,
    REQUIRED_RESEARCH_ITEM_INDEXES,
    REQUIRED_RESEARCH_LINK_INDEXES,
)


logger = logging.getLogger(__name__)
_SCHEMA_LOCK_KEY = 9021001
_DB_SETTINGS = get_settings().database
_HARD_CUTOVER_TABLE_RENAMES = {
    (None, "portal_report_materializations"): "portal_report_materializations_v1",
    (None, "portal_bot_run_step_rollups"): "portal_bot_run_step_rollups_v1",
    ("observability_events", "botlens_backend_events"): "botlens_backend_events_v1",
    ("observability_metrics", "botlens_backend_metric_rollups"): "botlens_backend_metric_rollups_v1",
}


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
            self._bootstrap_schema_contract()
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

    def _bootstrap_schema_contract(self) -> None:
        """Create missing schema objects and assert existing objects match the ORM contract."""

        if not self._engine:
            return
        with self._engine.begin() as conn:
            # Serialize schema DDL across backend + workers.
            conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": _SCHEMA_LOCK_KEY})
            try:
                self._create_missing_schemas(conn)
                self._create_missing_tables(conn)
                self._assert_columns(conn)
                self._create_missing_indexes(conn)
                self._assert_required_indexes(conn)
                logger.info("portal_db_schema_contract_ready")
            finally:
                conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _SCHEMA_LOCK_KEY})

    def _create_missing_schemas(self, conn) -> None:
        """Create non-public schemas declared by metadata when absent."""

        inspector = inspect(conn)
        existing_schemas = {str(name) for name in inspector.get_schema_names()}
        for schema_name in sorted(
            {
                str(table.schema)
                for table in Base.metadata.sorted_tables
                if str(table.schema or "").strip()
            }
        ):
            if schema_name in existing_schemas:
                continue
            conn.execute(CreateSchema(schema_name))
            logger.info("portal_db_schema_created | schema=%s", schema_name)
            existing_schemas.add(schema_name)

    def _create_missing_tables(self, conn) -> None:
        """Create metadata tables that are missing from the configured database."""

        inspector = inspect(conn)
        table_names_by_schema: Dict[Optional[str], set[str]] = {}

        def schema_table_names(schema: Optional[str]) -> set[str]:
            key = str(schema).strip() if schema is not None else None
            if key not in table_names_by_schema:
                table_names_by_schema[key] = set(inspector.get_table_names(schema=key))
            return table_names_by_schema[key]

        for table in Base.metadata.sorted_tables:
            schema_name = str(table.schema or "").strip() or None
            retired_name = _HARD_CUTOVER_TABLE_RENAMES.get((schema_name, table.name))
            if retired_name:
                active_ref = f"{schema_name or 'public'}.{table.name}"
                retired_ref = f"{schema_name or 'public'}.{retired_name}"
                active_exists, retired_exists = conn.execute(
                    text("SELECT to_regclass(:active_ref), to_regclass(:retired_ref)"),
                    {"active_ref": active_ref, "retired_ref": retired_ref},
                ).one()
                if active_exists is not None and retired_exists is not None:
                    raise RuntimeError(
                        f"Both active table '{active_ref}' and retired table '{retired_ref}' exist. "
                        "Drop the retired table or rerun scripts/db/manual_migration_versioning_hard_cutover.sql cleanly."
                    )
                if active_exists is None and retired_exists is not None:
                    raise RuntimeError(
                        f"Table '{active_ref}' is missing but retired table '{retired_ref}' exists. "
                        "Run scripts/db/manual_migration_versioning_hard_cutover.sql before starting this code."
                    )

            if table.name in schema_table_names(schema_name):
                continue
            conn.execute(CreateTable(table))
            logger.warning("portal_db_table_created | schema=%s | table=%s", schema_name or "public", table.name)
            schema_table_names(schema_name).add(table.name)

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

    def _assert_columns(self, conn) -> None:
        """Fail loud when an existing table does not match the current column contract."""

        inspector = inspect(conn)
        for table in Base.metadata.sorted_tables:
            schema_name = str(table.schema or "").strip() or None
            expected = {column.name for column in table.columns}
            existing = {col["name"] for col in inspector.get_columns(table.name, schema=schema_name)}
            missing = sorted(expected - existing)
            if not missing:
                continue
            logger.error(
                "portal_db_column_mismatch | schema=%s | table=%s | missing=%s",
                schema_name or "public",
                table.name,
                ",".join(missing),
            )
            raise RuntimeError(
                f"Table '{schema_name + '.' if schema_name else ''}{table.name}' is missing columns: {', '.join(missing)}. "
                "Drop the table or rebuild the database to ensure a clean schema."
            )

    def _create_missing_indexes(self, conn) -> None:
        """Create metadata indexes that are missing from otherwise valid tables."""

        inspector = inspect(conn)
        for table in Base.metadata.sorted_tables:
            schema_name = str(table.schema or "").strip() or None
            existing = {str(index.get("name") or "") for index in inspector.get_indexes(table.name, schema=schema_name)}
            for index in sorted(table.indexes, key=lambda item: str(item.name or "")):
                index_name = str(index.name or "").strip()
                if not index_name or index_name in existing:
                    continue
                conn.execute(CreateIndex(index))
                logger.info(
                    "portal_db_index_created | schema=%s | table=%s | index=%s",
                    schema_name or "public",
                    table.name,
                    index_name,
                )
                existing.add(index_name)

    def _assert_required_indexes(self, conn) -> None:
        """Fail loud when a required operational index is still absent after bootstrap."""

        inspector = inspect(conn)

        def assert_required_indexes(
            name: str,
            required: set[str] | frozenset[str],
            *,
            schema: Optional[str] = None,
        ) -> None:
            existing = {str(index.get("name") or "") for index in inspector.get_indexes(name, schema=schema)}
            missing = sorted(set(required) - existing)
            if not missing:
                return
            logger.error(
                "portal_db_required_indexes_missing | schema=%s | table=%s | missing=%s",
                schema or "public",
                name,
                ",".join(missing),
            )
            raise RuntimeError(
                f"Table '{schema + '.' if schema else ''}{name}' is missing required indexes: {', '.join(missing)}. "
                "The bootstrap creates indexes declared in portal/backend/db/models.py; update the model contract or rebuild the database."
            )

        assert_required_indexes("portal_bot_run_events", REQUIRED_BOT_RUN_EVENT_INDEXES)
        assert_required_indexes("portal_bot_runs", REQUIRED_BOT_RUN_INDEXES)
        assert_required_indexes("portal_report_materializations", REQUIRED_REPORT_MATERIALIZATION_INDEXES)
        assert_required_indexes("portal_bot_run_lifecycle", REQUIRED_BOT_RUN_LIFECYCLE_INDEXES)
        assert_required_indexes("portal_bot_run_leases", REQUIRED_BOT_RUN_LEASE_INDEXES)
        assert_required_indexes("portal_research_items", REQUIRED_RESEARCH_ITEM_INDEXES)
        assert_required_indexes("portal_research_links", REQUIRED_RESEARCH_LINK_INDEXES)
        assert_required_indexes("portal_provider_credential_refs", REQUIRED_PROVIDER_CREDENTIAL_INDEXES)

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

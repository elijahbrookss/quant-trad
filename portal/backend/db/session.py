"""Database session management helpers for the portal backend."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


logger = logging.getLogger(__name__)


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
        """Return the configured DSN or fall back to a local SQLite file."""

        value = os.getenv("PG_DSN")
        if value:
            return value
        raise RuntimeError("PG_DSN is required. No SQLite fallback is supported.")

    def ensure_schema(self) -> bool:
        """Initialise the database engine and create tables if required."""

        if self._engine is not None:
            return self._available
        try:
            self._engine = create_engine(self.dsn, future=True)
            self._session_factory = sessionmaker(
                bind=self._engine,
                expire_on_commit=False,
                autoflush=False,
                future=True,
            )
            Base.metadata.create_all(self._engine)
            self._apply_schema_migrations()
            self._available = True
            logger.info("portal_db_ready | dsn=%s", self.dsn)
        except SQLAlchemyError as exc:
            self._error = exc
            self._available = False
            logger.warning("portal_db_unavailable | dsn=%s | error=%s", self.dsn, exc)
        except Exception as exc:  # noqa: BLE001 - defensive catch
            self._error = exc
            self._available = False
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

    def _apply_schema_migrations(self) -> None:
        """Perform lightweight in-place migrations for existing installations."""
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
        require_table("portal_bot_trades")
        require_table("portal_bots")
        assert_columns("portal_bot_trades")
        assert_columns("portal_bots")

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

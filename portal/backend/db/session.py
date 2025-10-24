"""Database session management helpers for the portal backend."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from sqlalchemy import create_engine
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

        for env_key in ("PORTAL_DB_DSN", "PG_DSN"):
            value = os.getenv(env_key)
            if value:
                return value
        data_dir = Path(__file__).resolve().parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{data_dir / 'portal.db'}"

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

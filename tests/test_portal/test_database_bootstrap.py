from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import Any, Iterable

import pytest
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable

from portal.backend.db import session as db_session
from portal.backend.db.models import (
    Base,
    REQUIRED_BOT_RUN_EVENT_INDEXES,
    REQUIRED_BOT_RUN_INDEXES,
    REQUIRED_PROVIDER_CREDENTIAL_INDEXES,
    REQUIRED_REPORT_MATERIALIZATION_INDEXES,
)


def _table_key(table) -> tuple[str | None, str]:
    schema = str(table.schema or "").strip() or None
    return schema, str(table.name)


class _Result:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._row = row

    def one(self) -> tuple[Any, ...]:
        return self._row


class _Inspector:
    def __init__(
        self,
        *,
        schemas: Iterable[str] = ("public",),
        tables: Iterable[tuple[str | None, str]] = (),
        indexes: dict[tuple[str | None, str], set[str]] | None = None,
        missing_columns: dict[tuple[str | None, str], set[str]] | None = None,
    ) -> None:
        self.schemas = {str(schema) for schema in schemas}
        self.tables: dict[str | None, set[str]] = {}
        for schema, table in tables:
            self.tables.setdefault(schema, set()).add(table)
        self.indexes = indexes or {}
        self.missing_columns = missing_columns or {}

    def get_schema_names(self) -> list[str]:
        return sorted(self.schemas)

    def get_table_names(self, schema: str | None = None) -> list[str]:
        return sorted(self.tables.get(schema, set()))

    def get_columns(self, name: str, schema: str | None = None) -> list[dict[str, str]]:
        metadata_key = f"{schema}.{name}" if schema else name
        table = Base.metadata.tables[metadata_key]
        missing = self.missing_columns.get((schema, name), set())
        return [{"name": column.name} for column in table.columns if column.name not in missing]

    def get_indexes(self, name: str, schema: str | None = None) -> list[dict[str, str]]:
        return [{"name": index_name} for index_name in sorted(self.indexes.get((schema, name), set()))]


class _Connection:
    def __init__(self, inspector: _Inspector) -> None:
        self.inspector = inspector
        self.executed: list[Any] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        self.executed.append(statement)
        if isinstance(statement, CreateSchema):
            self.inspector.schemas.add(str(statement.element))
            return _Result(())
        if isinstance(statement, CreateTable):
            table = statement.element
            schema, name = _table_key(table)
            self.inspector.tables.setdefault(schema, set()).add(name)
            return _Result(())
        if isinstance(statement, CreateIndex):
            index = statement.element
            schema, name = _table_key(index.table)
            self.inspector.indexes.setdefault((schema, name), set()).add(str(index.name))
            return _Result(())
        if "to_regclass" in str(statement):
            query_params = params or {}
            table_ref = str(query_params.get("table_ref") or "")
            if table_ref:
                return _Result((self._regclass(table_ref),))
            active_ref = str(query_params.get("active_ref") or "")
            retired_ref = str(query_params.get("retired_ref") or "")
            return _Result((self._regclass(active_ref), self._regclass(retired_ref)))
        return _Result(())

    def _regclass(self, ref: str) -> str | None:
        schema, _, name = ref.rpartition(".")
        key = (None if schema == "public" else schema, name)
        return ref if name in self.inspector.tables.get(key[0], set()) else None


class _Engine:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    @contextmanager
    def begin(self):
        yield self.connection


def _database_with_fake_engine(monkeypatch, inspector: _Inspector) -> tuple[db_session.Database, _Connection]:
    connection = _Connection(inspector)
    monkeypatch.setattr(db_session, "inspect", lambda _conn: inspector)
    database = db_session.Database("postgresql+psycopg2://test:test@localhost/test")
    database._engine = _Engine(connection)
    return database, connection


def test_redact_dsn_for_log_removes_userinfo_and_sensitive_query_values() -> None:
    redacted = db_session._redact_dsn_for_log(
        "postgresql+psycopg2://quanttrad:super-secret@tsdb:5432/quanttrad"
        "?sslmode=require&token=raw-token&sslkey=/private/key.pem"
    )

    assert "quanttrad:super-secret" not in redacted
    assert "super-secret" not in redacted
    assert "raw-token" not in redacted
    assert "/private/key.pem" not in redacted
    assert redacted == (
        "postgresql+psycopg2://redacted:***@tsdb:5432/quanttrad"
        "?sslkey=redacted&sslmode=require&token=redacted"
    )


def test_database_ready_log_redacts_dsn(monkeypatch, caplog) -> None:
    database = db_session.Database(
        "postgresql+psycopg2://quanttrad:super-secret@tsdb:5432/quanttrad?token=raw-token"
    )
    database._engine = object()
    database._session_factory = object()
    monkeypatch.setattr(database, "_bootstrap_schema_contract", lambda: None)

    caplog.set_level(logging.INFO, logger=db_session.logger.name)

    assert database.ensure_schema() is True

    messages = "\n".join(record.getMessage() for record in caplog.records)
    assert "portal_db_ready" in messages
    assert "super-secret" not in messages
    assert "raw-token" not in messages
    assert "postgresql+psycopg2://redacted:***@tsdb:5432/quanttrad?token=redacted" in messages


def test_bootstrap_fresh_database_creates_current_tables_and_indexes(monkeypatch) -> None:
    inspector = _Inspector()
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    database._bootstrap_schema_contract()

    created_indexes = {statement.element.name for statement in connection.executed if isinstance(statement, CreateIndex)}
    assert REQUIRED_BOT_RUN_EVENT_INDEXES <= created_indexes
    assert REQUIRED_BOT_RUN_INDEXES <= created_indexes
    assert REQUIRED_PROVIDER_CREDENTIAL_INDEXES <= created_indexes
    assert REQUIRED_REPORT_MATERIALIZATION_INDEXES <= created_indexes
    assert "observability_events" in inspector.schemas
    assert "observability_metrics" in inspector.schemas

    for statement in connection.executed:
        if isinstance(statement, (CreateSchema, CreateTable)):
            assert getattr(statement, "if_not_exists", False) is False


def test_bootstrap_fails_loud_when_retired_lifecycle_tables_exist(monkeypatch) -> None:
    inspector = _Inspector(
        tables=[(None, "portal_bot_run_lifecycle")],
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(RuntimeError, match="Retired table 'public.portal_bot_run_lifecycle'"):
        database._bootstrap_schema_contract()

    created_tables = [statement for statement in connection.executed if isinstance(statement, CreateTable)]
    assert created_tables == []


def test_bootstrap_existing_valid_tables_repairs_missing_model_indexes(monkeypatch) -> None:
    inspector = _Inspector(
        schemas={"public", "observability_events", "observability_metrics"},
        tables=[_table_key(table) for table in Base.metadata.sorted_tables],
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    database._bootstrap_schema_contract()

    created_tables = [statement for statement in connection.executed if isinstance(statement, CreateTable)]
    created_indexes = {statement.element.name for statement in connection.executed if isinstance(statement, CreateIndex)}
    assert created_tables == []
    assert REQUIRED_BOT_RUN_EVENT_INDEXES <= created_indexes


def test_bootstrap_fails_loud_on_existing_column_drift_before_index_repair(monkeypatch) -> None:
    inspector = _Inspector(
        schemas={"public", "observability_events", "observability_metrics"},
        tables=[_table_key(table) for table in Base.metadata.sorted_tables],
        missing_columns={(None, "portal_bot_runs"): {"storage_schema_version"}},
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(RuntimeError, match="portal_bot_runs.*storage_schema_version"):
        database._bootstrap_schema_contract()

    created_indexes = [statement for statement in connection.executed if isinstance(statement, CreateIndex)]
    assert created_indexes == []

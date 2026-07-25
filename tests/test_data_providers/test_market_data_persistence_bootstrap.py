from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy.schema import CreateIndex, CreateTable

from data_providers.config.runtime import PersistenceConfig
from portal.backend.service.providers import persistence as provider_persistence


def _config() -> PersistenceConfig:
    return PersistenceConfig(
        dsn="postgresql+psycopg2://test:test@localhost/test",
        candles_raw_table="market_candles_raw",
        derivatives_state_table="derivatives_market_state",
        closures_table="portal_candle_closures",
    )


class _Result:
    def __init__(self, row: tuple[Any, ...] = ()) -> None:
        self._row = row

    def one(self) -> tuple[Any, ...]:
        return self._row


class _Inspector:
    def __init__(self) -> None:
        self.tables: set[str] = set()
        self.columns: dict[str, set[str]] = {}
        self.indexes: dict[str, set[str]] = {}

    def get_table_names(self) -> list[str]:
        return sorted(self.tables)

    def get_columns(self, name: str) -> list[dict[str, str]]:
        return [{"name": column} for column in sorted(self.columns.get(name, set()))]

    def get_indexes(self, name: str) -> list[dict[str, str]]:
        return [{"name": index_name} for index_name in sorted(self.indexes.get(name, set()))]


class _Connection:
    def __init__(self, inspector: _Inspector) -> None:
        self.inspector = inspector
        self.executed: list[Any] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        self.executed.append(statement)
        if isinstance(statement, CreateTable):
            table = statement.element
            self.inspector.tables.add(str(table.name))
            self.inspector.columns[str(table.name)] = {column.name for column in table.columns}
            return _Result()
        if isinstance(statement, CreateIndex):
            index = statement.element
            self.inspector.indexes.setdefault(str(index.table.name), set()).add(str(index.name))
            return _Result()
        return _Result()


class _Engine:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    @contextmanager
    def begin(self):
        yield self.connection


def _service(monkeypatch: pytest.MonkeyPatch, inspector: _Inspector):
    connection = _Connection(inspector)
    monkeypatch.setattr(provider_persistence, "inspect", lambda _conn: inspector)
    service = provider_persistence.DataPersistenceService(_config(), engine=_Engine(connection))
    return service, connection


def test_market_data_bootstrap_creates_tables_and_indexes_without_if_not_exists(monkeypatch) -> None:
    inspector = _Inspector()
    service, connection = _service(monkeypatch, inspector)

    service._bootstrap_schema_contract(connection)

    assert {"market_candles_raw", "derivatives_market_state", "portal_candle_closures"} <= inspector.tables
    created_indexes = {statement.element.name for statement in connection.executed if isinstance(statement, CreateIndex)}
    assert {
        "idx_candles_raw_instrument_tf_time",
        "idx_derivatives_state_instrument_time",
        "idx_derivatives_state_time",
        "idx_candle_closures_lookup",
    } <= created_indexes
    for statement in connection.executed:
        if isinstance(statement, CreateTable):
            assert getattr(statement, "if_not_exists", False) is False


def test_market_data_bootstrap_fails_on_column_drift_before_index_repair(monkeypatch) -> None:
    inspector = _Inspector()
    service, connection = _service(monkeypatch, inspector)
    for table in service._tables:
        inspector.tables.add(str(table.name))
        inspector.columns[str(table.name)] = {column.name for column in table.columns}
    inspector.columns["portal_candle_closures"].remove("metadata")

    with pytest.raises(RuntimeError, match="portal_candle_closures.*metadata"):
        service._bootstrap_schema_contract(connection)

    created_indexes = [statement for statement in connection.executed if isinstance(statement, CreateIndex)]
    assert created_indexes == []

from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import Any, Iterable

import pytest
from sqlalchemy import CheckConstraint
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable

from portal.backend.db import session as db_session
from portal.backend.db.models import (
    Base,
    REQUIRED_ASYNC_JOB_CONSTRAINTS,
    REQUIRED_ASYNC_JOB_INDEXES,
    REQUIRED_BOT_RUN_EVENT_INDEXES,
    REQUIRED_BOT_RUN_INDEXES,
    REQUIRED_PROVIDER_CREDENTIAL_INDEXES,
    REQUIRED_REPORT_MATERIALIZATION_INDEXES,
)


_CLEAN_BOOTSTRAP_TABLES = frozenset(
    {
        ("market", "fact_schemas"),
        ("market", "fact_versions"),
        ("market", "fact_acquisition_coverage"),
        ("market", "book_operational_rollups"),
    }
)


def _table_key(table) -> tuple[str | None, str]:
    schema = str(table.schema or "").strip() or None
    return schema, str(table.name)


class _Result:
    def __init__(self, row: Any) -> None:
        self._row = row

    def one(self) -> tuple[Any, ...]:
        return self._row

    def scalar_one_or_none(self) -> Any | None:
        return self._row[0] if self._row else None

    def scalar_one(self) -> Any:
        if not self._row:
            raise AssertionError("fake scalar result is empty")
        return self._row[0]

    def mappings(self) -> _Result:
        return self

    def first(self) -> Any | None:
        return self._row if self._row else None


class _Inspector:
    def __init__(
        self,
        *,
        schemas: Iterable[str] = ("public",),
        tables: Iterable[tuple[str | None, str]] = (),
        indexes: dict[tuple[str | None, str], set[str]] | None = None,
        missing_columns: dict[tuple[str | None, str], set[str]] | None = None,
        missing_constraints: dict[tuple[str | None, str], set[str]] | None = None,
        constraint_overrides: dict[str, dict[str, Any]] | None = None,
        index_overrides: dict[str, dict[str, Any]] | None = None,
        stale_book_fact: dict[str, Any] | None = None,
        stale_book_checkpoint: dict[str, Any] | None = None,
        missing_book_rollup_series_id: int | None = None,
        missing_book_operational_indexes: set[str] | None = None,
        book_operational_index_overrides: (
            dict[str, dict[str, Any]] | None
        ) = None,
        book_checkpoint_rollup_trigger_ready: bool = True,
    ) -> None:
        self.schemas = {str(schema) for schema in schemas}
        self.tables: dict[str | None, set[str]] = {}
        for schema, table in tables:
            self.tables.setdefault(schema, set()).add(table)
        self.indexes = {
            key: set(value)
            for key, value in (indexes or {}).items()
        }
        # Current canonical relations are created only for a genuinely clean
        # database. Existing databases must already carry their indexes.
        for schema, table_name in _CLEAN_BOOTSTRAP_TABLES:
            if table_name not in self.tables.get(schema, set()):
                continue
            table = Base.metadata.tables.get(f"{schema}.{table_name}")
            if table is None:
                continue
            self.indexes.setdefault((schema, table_name), set()).update(
                str(index.name)
                for index in table.indexes
                if index.name
            )
        self.missing_columns = missing_columns or {}
        self.missing_constraints = missing_constraints or {}
        self.constraint_overrides = constraint_overrides or {}
        self.index_overrides = index_overrides or {}
        self.stale_book_fact = stale_book_fact
        self.stale_book_checkpoint = stale_book_checkpoint
        self.missing_book_rollup_series_id = missing_book_rollup_series_id
        self.missing_book_operational_indexes = (
            missing_book_operational_indexes or set()
        )
        self.book_operational_index_overrides = (
            book_operational_index_overrides or {}
        )
        self.book_checkpoint_rollup_trigger_ready = (
            book_checkpoint_rollup_trigger_ready
        )

    def get_schema_names(self) -> list[str]:
        return sorted(self.schemas)

    def get_table_names(self, schema: str | None = None) -> list[str]:
        return sorted(self.tables.get(schema, set()))

    def get_columns(self, name: str, schema: str | None = None) -> list[dict[str, str]]:
        metadata_key = f"{schema}.{name}" if schema else name
        table = Base.metadata.tables[metadata_key]
        missing = self.missing_columns.get((schema, name), set())
        return [{"name": column.name} for column in table.columns if column.name not in missing]

    def get_indexes(
        self,
        name: str,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        metadata_key = f"{schema}.{name}" if schema else name
        table = Base.metadata.tables[metadata_key]
        model_indexes = {
            str(index.name): index
            for index in table.indexes
            if index.name
        }
        payloads = []
        for index_name in sorted(self.indexes.get((schema, name), set())):
            index = model_indexes[index_name]
            where = index.dialect_options["postgresql"].get("where")
            payload = {
                "name": index_name,
                "unique": bool(index.unique),
                "column_names": [
                    str(column.name)
                    for column in index.columns
                ],
                "dialect_options": {
                    **(
                        {"postgresql_where": str(where)}
                        if where is not None
                        else {}
                    )
                },
            }
            payload.update(self.index_overrides.get(index_name, {}))
            payloads.append(payload)
        return payloads

    def get_pk_constraint(
        self,
        name: str,
        schema: str | None = None,
    ) -> dict[str, Any]:
        metadata_key = f"{schema}.{name}" if schema else name
        table = Base.metadata.tables[metadata_key]
        return {
            "name": str(table.primary_key.name or ""),
            "constrained_columns": [
                str(column.name)
                for column in table.primary_key.columns
            ],
        }

    def get_check_constraints(
        self,
        name: str,
        schema: str | None = None,
) -> list[dict[str, str]]:
        metadata_key = f"{schema}.{name}" if schema else name
        table = Base.metadata.tables[metadata_key]
        missing = self.missing_constraints.get((schema, name), set())
        payloads = []
        for constraint in table.constraints:
            if (
                not isinstance(constraint, CheckConstraint)
                or not constraint.name
                or str(constraint.name) in missing
            ):
                continue
            name = str(constraint.name)
            payload = {
                "name": name,
                "sqltext": str(constraint.sqltext),
            }
            payload.update(self.constraint_overrides.get(name, {}))
            payloads.append(payload)
        return payloads


class _Connection:
    def __init__(self, inspector: _Inspector) -> None:
        self.inspector = inspector
        self.executed: list[Any] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        self.executed.append(statement)
        statement_text = str(statement)
        if "FROM pg_extension" in statement_text:
            return _Result(("2.14.2",))
        if "timescaledb_information.hypertables" in statement_text:
            return _Result((True,))
        if "format_type(attribute.atttypid" in statement_text:
            return _Result(("numeric",))
        if "FROM pg_attribute AS attribute" in statement_text:
            return _Result(("", "nextval('market.fact_commit_seq'::regclass)"))
        if "book_checkpoint_rollup_trigger_ready" in statement_text:
            return _Result(
                (self.inspector.book_checkpoint_rollup_trigger_ready,)
            )
        if "function_body_hash" in statement_text:
            return _Result(())
        if "FROM pg_trigger" in statement_text:
            return _Result((True,))
        if "operational_index_name" in statement_text:
            query_params = params or {}
            index_name = str(query_params["index_name"])
            if index_name in self.inspector.missing_book_operational_indexes:
                return _Result(())
            expected_columns = {
                "ix_market_fact_series_commit": (
                    "series_id",
                    "market_commit_seq",
                ),
                "ix_market_book_checkpoint_series_acknowledged": (
                    "series_id",
                    "acknowledged_at",
                    "id",
                ),
            }[index_name]
            expected_definition = {
                "ix_market_fact_series_commit": (
                    "CREATE INDEX ix_market_fact_series_commit ON "
                    "market.fact_versions USING btree "
                    "(series_id, market_commit_seq)"
                ),
                "ix_market_book_checkpoint_series_acknowledged": (
                    "CREATE INDEX "
                    "ix_market_book_checkpoint_series_acknowledged ON "
                    "market.book_checkpoint_manifests USING btree "
                    "(series_id, acknowledged_at, id)"
                ),
            }[index_name]
            row = {
                "operational_index_name": index_name,
                "indisvalid": True,
                "indisready": True,
                "indisunique": False,
                "indnkeyatts": len(expected_columns),
                "indnatts": len(expected_columns),
                "has_plain_columns": True,
                "is_unfiltered": True,
                "access_method": "btree",
                "key_columns": expected_columns,
                "definition": expected_definition,
            }
            row.update(
                self.inspector.book_operational_index_overrides.get(
                    index_name,
                    {},
                )
            )
            return _Result(row)
        if "missing_required_book_rollup_series_id" in statement_text:
            missing_series_id = self.inspector.missing_book_rollup_series_id
            return _Result(
                (missing_series_id,) if missing_series_id is not None else ()
            )
        if "latest_fact_commit_seq" in statement_text:
            return _Result(self.inspector.stale_book_fact or ())
        if "latest_checkpoint_id" in statement_text:
            return _Result(self.inspector.stale_book_checkpoint or ())
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


def _database_with_fake_engine(
    monkeypatch,
    inspector: _Inspector,
    *,
    canonical_ready: bool = True,
) -> tuple[db_session.Database, _Connection]:
    if canonical_ready:
        inspector.schemas.add("market")
        inspector.tables.setdefault("market", set()).update(
            {"fact_schemas", "fact_versions", "book_operational_rollups"}
        )
    connection = _Connection(inspector)
    monkeypatch.setattr(db_session, "inspect", lambda _conn: inspector)
    database = db_session.Database("postgresql+psycopg2://test:test@localhost/test")
    database._engine = _Engine(connection)
    # Canonical registry equality is covered separately; these metadata tests
    # isolate clean/current table provisioning and drift behavior.
    monkeypatch.setattr(database, "_assert_canonical_fact_migration", lambda _conn: None)
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


def test_bootstrap_fresh_database_creates_complete_current_schema(
    monkeypatch,
) -> None:
    inspector = _Inspector()
    database, connection = _database_with_fake_engine(
        monkeypatch,
        inspector,
        canonical_ready=False,
    )

    database._bootstrap_schema_contract()

    created_tables = {
        _table_key(statement.element)
        for statement in connection.executed
        if isinstance(statement, CreateTable)
    }
    created_indexes = {
        _table_key(statement.element.table)
        for statement in connection.executed
        if isinstance(statement, CreateIndex)
    }
    assert _CLEAN_BOOTSTRAP_TABLES <= created_tables
    assert {
        ("market", "fact_versions"),
        ("market", "fact_acquisition_coverage"),
    } <= created_indexes

    registry_table_position = next(
        position
        for position, statement in enumerate(connection.executed)
        if isinstance(statement, CreateTable)
        and _table_key(statement.element) == ("market", "fact_schemas")
    )
    validation_function_position = next(
        position
        for position, statement in enumerate(connection.executed)
        if "CREATE OR REPLACE FUNCTION market.validate_fact_payload"
        in str(statement)
    )
    fact_table_position = next(
        position
        for position, statement in enumerate(connection.executed)
        if isinstance(statement, CreateTable)
        and _table_key(statement.element) == ("market", "fact_versions")
    )
    assert registry_table_position < validation_function_position < fact_table_position
    executed_sql = "\n".join(str(statement) for statement in connection.executed)
    assert "record_book_checkpoint_operational_rollup_v1" in executed_sql
    assert "ENABLE ALWAYS TRIGGER" in executed_sql
    assert "trg_record_book_checkpoint_operational_rollup_v1" in executed_sql


def test_bootstrap_migrated_acquisition_schema_passes_and_creates_other_objects(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    database._bootstrap_schema_contract()

    created_indexes = {statement.element.name for statement in connection.executed if isinstance(statement, CreateIndex)}
    assert REQUIRED_BOT_RUN_EVENT_INDEXES <= created_indexes
    assert REQUIRED_BOT_RUN_INDEXES <= created_indexes
    assert REQUIRED_PROVIDER_CREDENTIAL_INDEXES <= created_indexes
    assert REQUIRED_REPORT_MATERIALIZATION_INDEXES <= created_indexes
    assert REQUIRED_ASYNC_JOB_INDEXES <= created_indexes
    async_constraints = {
        str(constraint.name)
        for constraint in Base.metadata.tables["portal_async_jobs"].constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert REQUIRED_ASYNC_JOB_CONSTRAINTS <= async_constraints
    assert "observability_events" in inspector.schemas
    assert "observability_metrics" in inspector.schemas

    created_explicit_tables = {
        _table_key(statement.element)
        for statement in connection.executed
        if isinstance(statement, CreateTable)
        and _table_key(statement.element) in _CLEAN_BOOTSTRAP_TABLES
    }
    created_explicit_indexes = {
        _table_key(statement.element.table)
        for statement in connection.executed
        if isinstance(statement, CreateIndex)
        and _table_key(statement.element.table) in _CLEAN_BOOTSTRAP_TABLES
    }
    assert created_explicit_tables == set()
    assert created_explicit_indexes == set()

    for statement in connection.executed:
        if isinstance(statement, (CreateSchema, CreateTable)):
            assert getattr(statement, "if_not_exists", False) is False


def test_book_operational_rollup_requires_explicit_existing_store_migration(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
    )
    database, _connection = _database_with_fake_engine(monkeypatch, inspector)
    inspector.tables["market"].remove("book_operational_rollups")

    with pytest.raises(
        RuntimeError,
        match="manual_migration_book_operational_rollups_v1.sql",
    ):
        database._assert_book_operational_rollup_migration(_connection)


def test_book_operational_rollup_column_drift_names_explicit_migration(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
        missing_columns={
            ("market", "book_operational_rollups"): {"mutation_count"}
        },
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match="mutation_count.*manual_migration_book_operational_rollups_v1.sql",
    ):
        database._assert_book_operational_rollup_migration(connection)


def test_book_operational_rollup_requires_every_l2_series_to_be_seeded(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
        missing_book_rollup_series_id=41,
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match="no seeded row.*series_id=41",
    ):
        database._assert_book_operational_rollup_migration(connection)


def test_book_operational_rollup_requires_durable_checkpoint_trigger(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
        book_checkpoint_rollup_trigger_ready=False,
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match=(
            "always-on durable checkpoint rollup trigger.*"
            "Observed contract: trigger_row=missing"
        ),
    ):
        database._assert_book_operational_rollup_migration(connection)

    trigger_query = next(
        str(statement)
        for statement in connection.executed
        if "book_checkpoint_rollup_trigger_ready" in str(statement)
    )
    for exact_contract_guard in (
        "trigger.tgconstraint = 0",
        "trigger.tgnargs = 0",
        "trigger.tgqual IS NULL",
        "trigger.tgoldtable IS NULL",
        "trigger.tgnewtable IS NULL",
        "pg_get_triggerdef(trigger.oid, false)",
        "procedure.prorettype = 'trigger'::regtype",
        "procedure.proparallel = 'u'",
        "NOT procedure.prosecdef",
        "procedure.proconfig IS NULL",
        "btrim(",
        "procedure.prosrc",
        "'[[:space:]]+'",
        "has_function_privilege",
    ):
        assert exact_contract_guard in trigger_query


def test_book_operational_rollup_rejects_wrong_bounded_status_index(
    monkeypatch,
) -> None:
    index_name = "ix_market_book_checkpoint_series_acknowledged"
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
        book_operational_index_overrides={
            index_name: {
                "indnkeyatts": 2,
                "indnatts": 2,
                "key_columns": ("series_id", "acknowledged_at"),
                "definition": "CREATE INDEX wrong_checkpoint_index",
            }
        },
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match=(
            "bounded status indexes.*"
            "ix_market_book_checkpoint_series_acknowledged"
        ),
    ):
        database._assert_book_operational_rollup_migration(connection)


def test_book_operational_rollup_stale_checkpoint_requires_reseed(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
        stale_book_checkpoint={
            "series_id": 17,
            "checkpoint_high_water_id": "checkpoint-old",
            "latest_checkpoint_id": "checkpoint-new",
        },
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match="stale checkpoint counters.*series_id=17",
    ):
        database._assert_book_operational_rollup_migration(connection)


def test_book_operational_rollup_stale_fact_requires_reseed(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=_CLEAN_BOOTSTRAP_TABLES,
        stale_book_fact={
            "series_id": 23,
            "fact_high_water_commit_seq": 100,
            "latest_fact_commit_seq": 101,
        },
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match="stale canonical Fact counters.*series_id=23",
    ):
        database._assert_book_operational_rollup_migration(connection)


def test_bootstrap_fails_loud_when_retired_lifecycle_tables_exist(monkeypatch) -> None:
    inspector = _Inspector(
        tables=[(None, "portal_bot_run_lifecycle")],
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(RuntimeError, match="Retired table 'public.portal_bot_run_lifecycle'"):
        database._bootstrap_schema_contract()

    created_tables = [statement for statement in connection.executed if isinstance(statement, CreateTable)]
    assert created_tables == []


def test_bootstrap_fails_loud_when_legacy_fact_table_exists(monkeypatch) -> None:
    inspector = _Inspector(
        schemas={"public", "market"},
        tables=[("market", "candle_versions")],
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(RuntimeError, match="Legacy market-data tables remain active"):
        database._bootstrap_schema_contract()

    created_tables = [
        statement
        for statement in connection.executed
        if isinstance(statement, CreateTable)
    ]
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


@pytest.mark.parametrize(
    ("table", "column", "migration"),
    [
        (
            "gap_evidence",
            "source_id",
            "scripts/db/manual_migration_gap_source_identity_v1.sql",
        ),
        (
            "dataset_series",
            "quality_evidence",
            "scripts/db/manual_migration_dataset_quality_evidence_v1.sql",
        ),
    ],
)
def test_bootstrap_names_preserving_market_data_column_migration(
    monkeypatch,
    table: str,
    column: str,
    migration: str,
) -> None:
    inspector = _Inspector(
        schemas={"public", "market", "observability_events", "observability_metrics"},
        tables=[_table_key(candidate) for candidate in Base.metadata.sorted_tables],
        missing_columns={("market", table): {column}},
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(RuntimeError) as exc_info:
        database._bootstrap_schema_contract()

    message = str(exc_info.value)
    assert migration in message
    assert "writers stopped" in message
    assert not any(
        isinstance(statement, CreateIndex) for statement in connection.executed
    )


def test_bootstrap_fails_loud_when_async_fencing_constraint_is_missing(
    monkeypatch,
) -> None:
    inspector = _Inspector(
        schemas={"public", "observability_events", "observability_metrics"},
        tables=[_table_key(table) for table in Base.metadata.sorted_tables],
        missing_constraints={
            (None, "portal_async_jobs"): {
                "ck_portal_async_jobs_claim_state",
            }
        },
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match="portal_async_jobs.*ck_portal_async_jobs_claim_state",
    ):
        database._bootstrap_schema_contract()

    created_indexes = [
        statement
        for statement in connection.executed
        if isinstance(statement, CreateIndex)
    ]
    assert created_indexes == []


@pytest.mark.parametrize(
    "definition",
    [
        (
            "status = 'running' AND ("
            "lock_owner IS NOT NULL "
            "AND locked_at IS NOT NULL "
            "AND heartbeat_at IS NOT NULL "
            "AND claim_token_hash IS NOT NULL "
            "OR status <> 'running'"
            ") AND lock_owner IS NULL "
            "AND locked_at IS NULL "
            "AND heartbeat_at IS NULL "
            "AND claim_token_hash IS NULL"
        ),
        (
            "status = 'RUNNING' AND lock_owner IS NOT NULL "
            "AND locked_at IS NOT NULL "
            "AND heartbeat_at IS NOT NULL "
            "AND claim_token_hash IS NOT NULL "
            "OR status <> 'RUNNING' AND lock_owner IS NULL "
            "AND locked_at IS NULL "
            "AND heartbeat_at IS NULL "
            "AND claim_token_hash IS NULL"
        ),
    ],
)
def test_bootstrap_fails_loud_on_same_named_constraint_definition_drift(
    monkeypatch,
    definition: str,
) -> None:
    inspector = _Inspector(
        schemas={"public", "observability_events", "observability_metrics"},
        tables=[_table_key(table) for table in Base.metadata.sorted_tables],
        constraint_overrides={
            "ck_portal_async_jobs_claim_state": {
                "sqltext": definition,
            }
        },
    )
    database, connection = _database_with_fake_engine(monkeypatch, inspector)

    with pytest.raises(
        RuntimeError,
        match="mismatched constraint definitions.*claim_state",
    ):
        database._bootstrap_schema_contract()

    assert not any(
        isinstance(statement, CreateIndex)
        for statement in connection.executed
    )


def test_bootstrap_fails_loud_on_same_named_async_index_definition_drift(
    monkeypatch,
) -> None:
    async_index_names = {
        str(index.name)
        for index in Base.metadata.tables["portal_async_jobs"].indexes
        if index.name
    }
    inspector = _Inspector(
        schemas={"public", "observability_events", "observability_metrics"},
        tables=[_table_key(table) for table in Base.metadata.sorted_tables],
        indexes={(None, "portal_async_jobs"): async_index_names},
        index_overrides={
            "uq_portal_async_jobs_inflight_request": {
                "unique": False,
            }
        },
    )
    database, _connection = _database_with_fake_engine(
        monkeypatch,
        inspector,
    )

    with pytest.raises(
        RuntimeError,
        match="mismatched index definitions.*inflight_request",
    ):
        database._bootstrap_schema_contract()


@pytest.mark.parametrize(
    "predicate",
    [
        (
            "status NOT IN ('queued', 'running', 'retry') "
            "AND request_fingerprint IS NOT NULL"
        ),
        (
            "status IN ('queued', 'running', 'retry') "
            "AND request_fingerprint IS NOT NULL "
            "AND partition_key IS NOT NULL"
        ),
        (
            "status IN ('queued', 'RUNNING', 'retry') "
            "AND request_fingerprint IS NOT NULL"
        ),
    ],
)
def test_bootstrap_rejects_async_index_predicate_semantic_drift(
    monkeypatch,
    predicate: str,
) -> None:
    async_index_names = {
        str(index.name)
        for index in Base.metadata.tables["portal_async_jobs"].indexes
        if index.name
    }
    inspector = _Inspector(
        schemas={"public", "observability_events", "observability_metrics"},
        tables=[_table_key(table) for table in Base.metadata.sorted_tables],
        indexes={(None, "portal_async_jobs"): async_index_names},
        index_overrides={
            "uq_portal_async_jobs_inflight_request": {
                "dialect_options": {
                    "postgresql_where": predicate,
                },
            }
        },
    )
    database, _connection = _database_with_fake_engine(
        monkeypatch,
        inspector,
    )

    with pytest.raises(
        RuntimeError,
        match="mismatched index definitions.*inflight_request",
    ):
        database._bootstrap_schema_contract()

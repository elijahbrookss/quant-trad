"""Database session management helpers for the portal backend."""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from core.settings import get_settings
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable

from .models import (
    Base,
    REQUIRED_ASYNC_JOB_CONSTRAINTS,
    REQUIRED_ASYNC_JOB_INDEXES,
    REQUIRED_BOT_RUN_INDEXES,
    REQUIRED_BOT_RUN_EVENT_INDEXES,
    REQUIRED_BOT_RUN_LEASE_INDEXES,
    REQUIRED_PROVIDER_CREDENTIAL_INDEXES,
    REQUIRED_REPORT_MATERIALIZATION_INDEXES,
    REQUIRED_RESEARCH_ITEM_INDEXES,
    REQUIRED_RESEARCH_LINK_INDEXES,
)


logger = logging.getLogger(__name__)
_SCHEMA_LOCK_KEY = 9021001
_DB_SETTINGS = get_settings().database
_SENSITIVE_DSN_QUERY_TOKENS = (
    "api_key",
    "credential",
    "key",
    "pass",
    "password",
    "pwd",
    "secret",
    "token",
)
_HARD_CUTOVER_TABLE_RENAMES = {
    (None, "portal_report_materializations"): "portal_report_materializations_v1",
    (None, "portal_bot_run_step_rollups"): "portal_bot_run_step_rollups_v1",
    ("observability_events", "botlens_backend_events"): "botlens_backend_events_v1",
    ("observability_metrics", "botlens_backend_metric_rollups"): "botlens_backend_metric_rollups_v1",
}

_RETIRED_TABLES = (
    (None, "portal_bot_run_lifecycle"),
    (None, "portal_bot_run_lifecycle_events"),
)

_LEGACY_MARKET_DATA_TABLES = (
    "public.market_candles_raw",
    "public.derivatives_market_state",
    "public.portal_candle_closures",
)
_CANONICAL_MARKET_DATA_TABLE = "market.candle_versions"

_ASYNC_JOB_RUNNING_CLAIM_DEFINITION = (
    "status='running'andlock_ownerisnotnullandlocked_atisnotnull"
    "andheartbeat_atisnotnullandclaim_token_hashisnotnull"
)
_ASYNC_JOB_RELEASED_CLAIM_DEFINITION = (
    "status<>'running'andlock_ownerisnullandlocked_atisnull"
    "andheartbeat_atisnullandclaim_token_hashisnull"
)
_ASYNC_JOB_CONSTRAINT_DEFINITIONS = {
    "ck_portal_async_jobs_claim_generation_nonnegative": {
        "claim_generation>=0",
    },
    "ck_portal_async_jobs_claim_state": {
        (
            f"{_ASYNC_JOB_RUNNING_CLAIM_DEFINITION}or"
            f"{_ASYNC_JOB_RELEASED_CLAIM_DEFINITION}"
        ),
        (
            f"({_ASYNC_JOB_RUNNING_CLAIM_DEFINITION})or"
            f"({_ASYNC_JOB_RELEASED_CLAIM_DEFINITION})"
        ),
    },
}

_ASYNC_JOB_INDEX_DEFINITIONS = {
    "ix_portal_async_jobs_claimable": {
        "unique": False,
        "columns": ("status", "job_type", "available_at", "created_at"),
    },
    "ix_portal_async_jobs_running_heartbeat": {
        "unique": False,
        "columns": ("status", "job_type", "heartbeat_at"),
    },
    "uq_portal_async_jobs_inflight_request": {
        "unique": True,
        "columns": (
            "job_type",
            "partition_key",
            "request_fingerprint",
        ),
        "predicates": {
            (
                "statusin('queued','running','retry')"
                "andrequest_fingerprintisnotnull"
            ),
            (
                "(((status)=any((array['queued','running','retry'])[]))"
                "and(request_fingerprintisnotnull))"
            ),
        },
    },
}


def _normalize_postgres_definition(value: object) -> str:
    parts = re.split(r"('(?:''|[^'])*')", str(value or ""))
    for index in range(0, len(parts), 2):
        normalized = parts[index].lower()
        normalized = normalized.replace("::character varying", "")
        normalized = normalized.replace("::text", "")
        parts[index] = re.sub(r"\s+", "", normalized)
    return "".join(parts)


def _redact_dsn_for_log(dsn: Optional[str]) -> str:
    """Return a DSN safe for lifecycle logs while preserving routing context."""

    raw = str(dsn or "").strip()
    if not raw:
        return "<unset>"
    try:
        url = make_url(raw)
    except Exception:  # noqa: BLE001 - malformed DSNs may still contain secrets
        return "<redacted-dsn>"

    query = dict(url.query)
    redacted_query: dict[str, object] = {}
    for key, value in query.items():
        key_lower = str(key).lower()
        if any(token in key_lower for token in _SENSITIVE_DSN_QUERY_TOKENS):
            if isinstance(value, tuple):
                redacted_query[str(key)] = tuple("redacted" for _ in value)
            else:
                redacted_query[str(key)] = "redacted"
        else:
            redacted_query[str(key)] = value

    if url.username is not None or url.password is not None:
        url = url.set(username="redacted", password="redacted")
    if redacted_query != query:
        url = url.set(query=redacted_query)
    return url.render_as_string(hide_password=True)


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
            logger.info("portal_db_ready | dsn=%s", _redact_dsn_for_log(self.dsn))
        except SQLAlchemyError as exc:
            self._error = exc
            self._available = False
            self._reset_engine()
            logger.warning("portal_db_unavailable | dsn=%s | error=%s", _redact_dsn_for_log(self.dsn), exc)
        except Exception as exc:  # noqa: BLE001 - defensive catch
            self._error = exc
            self._available = False
            self._reset_engine()
            logger.exception("portal_db_initialise_failed | dsn=%s", _redact_dsn_for_log(self.dsn))
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
            # Transaction-scoped locking serializes schema DDL across backend +
            # workers and releases automatically on commit or rollback.  An
            # explicit unlock inside an aborted PostgreSQL transaction masks the
            # statement that actually violated the schema contract.
            conn.execute(
                text("SELECT pg_advisory_xact_lock(:key)"),
                {"key": _SCHEMA_LOCK_KEY},
            )
            self._assert_market_data_cutover_state(conn)
            self._create_missing_schemas(conn)
            self._ensure_market_data_commit_sequence(conn)
            self._assert_market_commit_clock(conn, existing_only=True)
            self._assert_retired_tables_absent(conn)
            self._create_missing_tables(conn)
            self._ensure_market_data_hypertables(conn)
            self._assert_market_commit_clock(conn, existing_only=False)
            self._assert_columns(conn)
            self._assert_required_constraints(conn)
            self._create_missing_indexes(conn)
            self._assert_required_indexes(conn)
            self._ensure_market_data_immutability(conn)
            logger.info("portal_db_schema_contract_ready")

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

    def _assert_retired_tables_absent(self, conn) -> None:
        """Fail loud until explicit lifecycle hard-cutover cleanup is applied."""

        for schema, name in _RETIRED_TABLES:
            table_ref = f"{schema or 'public'}.{name}"
            (existing,) = conn.execute(
                text("SELECT to_regclass(:table_ref)"),
                {"table_ref": table_ref},
            ).one()
            if existing is None:
                continue
            logger.error("portal_db_retired_table_present | table=%s", table_ref)
            raise RuntimeError(
                f"Retired table '{table_ref}' is still present. "
                "Run scripts/db/manual_migration_canonical_lifecycle_ledger_v1.sql "
                "after verifying canonical lifecycle coverage."
            )

    def _assert_market_data_cutover_state(self, conn) -> None:
        """Reject legacy active tables instead of creating a dual storage path."""

        legacy_present = []
        for table_ref in _LEGACY_MARKET_DATA_TABLES:
            (existing,) = conn.execute(
                text("SELECT to_regclass(:table_ref)"),
                {"table_ref": table_ref},
            ).one()
            if existing is not None:
                legacy_present.append(table_ref)
        (canonical_present,) = conn.execute(
            text("SELECT to_regclass(:table_ref)"),
            {"table_ref": _CANONICAL_MARKET_DATA_TABLE},
        ).one()
        if not legacy_present:
            return

        logger.error(
            "portal_db_legacy_market_data_present | tables=%s canonical_present=%s",
            ",".join(legacy_present),
            canonical_present is not None,
        )
        raise RuntimeError(
            "Legacy market-data tables remain active: "
            f"{', '.join(legacy_present)}. Stop backend and paper writers, then run "
            "scripts/db/manual_migration_market_data_v2_hard_cutover.sql. "
            "The canonical service will not start with dual candle ownership."
        )

    def _ensure_market_data_commit_sequence(self, conn) -> None:
        """Create the one causal commit clock shared by every typed fact table."""

        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS market.fact_commit_seq"))

    def _assert_market_commit_clock(self, conn, *, existing_only: bool) -> None:
        """Reject per-table identity clocks before heterogeneous facts can start."""

        for table_name in (
            "candle_versions",
            "open_interest_versions",
            "funding_rate_versions",
            "market_trade_versions",
            "trade_flow_aggregate_versions",
            "l2_snapshot_versions",
            "l2_mutation_batches",
            "bbo_feature_versions",
            "depth_feature_versions",
            "trade_flow_feature_versions",
            "futures_spot_relationship_versions",
            "derivative_state_versions",
            "market_response_feature_versions",
            "normalized_feature_versions",
        ):
            table_ref = f"market.{table_name}"
            existing = conn.execute(
                text("SELECT to_regclass(:table_ref)"), {"table_ref": table_ref}
            ).scalar_one()
            if existing is None:
                if existing_only:
                    continue
                raise RuntimeError(f"Canonical table {table_ref} is missing")
            identity, expression = conn.execute(
                text(
                    """
                    SELECT attribute.attidentity,
                           pg_get_expr(default_value.adbin, default_value.adrelid) AS default_expression
                    FROM pg_attribute AS attribute
                    LEFT JOIN pg_attrdef AS default_value
                      ON default_value.adrelid = attribute.attrelid
                     AND default_value.adnum = attribute.attnum
                    WHERE attribute.attrelid = CAST(:table_ref AS regclass)
                      AND attribute.attname = 'market_commit_seq'
                      AND NOT attribute.attisdropped
                    """
                ),
                {"table_ref": table_ref},
            ).one()
            expression = str(expression or "")
            identity = str(identity or "")
            if identity or "market.fact_commit_seq" not in expression:
                raise RuntimeError(
                    f"Table '{table_ref}' does not use the shared market fact commit clock. "
                    "Stop backend and collector processes, then run "
                    "scripts/db/manual_migration_market_fact_commit_clock_v1.sql."
                )

    def _ensure_market_data_hypertables(self, conn) -> None:
        """Require TimescaleDB and make typed time-series fact tables hypertables."""

        extension_version = conn.execute(
            text("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        ).scalar_one_or_none()
        if not extension_version:
            raise RuntimeError(
                "TimescaleDB is required for canonical market-data storage. "
                "Install the extension before starting the backend."
            )
        for table_name, time_column in (
            ("candle_versions", "candle_open_time"),
            ("open_interest_versions", "sample_time"),
            ("funding_rate_versions", "sample_time"),
            ("market_trade_versions", "provider_event_time"),
            ("trade_flow_aggregate_versions", "bucket_start"),
            ("l2_snapshot_versions", "effective_at"),
            ("l2_mutation_batches", "effective_at"),
            ("bbo_feature_versions", "bucket_start"),
            ("depth_feature_versions", "bucket_start"),
            ("trade_flow_feature_versions", "bucket_start"),
            ("futures_spot_relationship_versions", "effective_at"),
            ("derivative_state_versions", "effective_at"),
            ("normalized_feature_versions", "effective_at"),
            ("market_response_feature_versions", "bucket_start"),
        ):
            conn.execute(
                text(
                    "SELECT create_hypertable("
                    f"'market.{table_name}', "
                    f"by_range('{time_column}'), "
                    "if_not_exists => TRUE, "
                    "migrate_data => TRUE"
                    ")"
                )
            )
            is_hypertable = conn.execute(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM timescaledb_information.hypertables "
                    "WHERE hypertable_schema = 'market' "
                    "AND hypertable_name = :table_name"
                    ")"
                ),
                {"table_name": table_name},
            ).scalar_one()
            if not bool(is_hypertable):
                raise RuntimeError(
                    f"Canonical table market.{table_name} was not created as a hypertable."
                )

    def _ensure_market_data_immutability(self, conn) -> None:
        """Protect append-only facts and frozen datasets from in-place mutation."""

        conn.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION market.reject_immutable_mutation()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    RAISE EXCEPTION 'immutable market-data relation %.% rejects %',
                        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
                END;
                $$
                """
            )
        )
        for table_name in (
            "sources",
            "series",
            "candle_versions",
            "open_interest_versions",
            "funding_rate_versions",
            "gap_evidence",
            "datasets",
            "dataset_series",
            "product_definition_versions",
            "instrument_role_mapping_versions",
            "stream_session_events",
            "raw_archive_manifests",
            "raw_archive_ranges",
            "raw_archive_record_mappings",
            "raw_archive_compaction_sources",
            "archive_retention_pin_versions",
            "stream_coverage_interval_versions",
            "stream_quality_events",
            "market_trade_identities",
            "market_trade_versions",
            "trade_flow_aggregate_versions",
            "l2_snapshot_versions",
            "l2_snapshot_levels",
            "l2_mutation_batches",
            "l2_mutations",
            "bbo_feature_versions",
            "depth_feature_versions",
            "trade_flow_feature_versions",
            "futures_spot_relationship_versions",
            "normalization_specs",
            "normalized_feature_versions",
            "dataset_normalization_refs",
            "derivative_state_versions",
            "market_response_feature_versions",
            "book_validity_interval_versions",
            "book_checkpoint_manifests",
            "book_quality_event_links",
            "dataset_archive_refs",
        ):
            trigger_name = f"trg_reject_mutation_{table_name}"
            conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_trigger
                            WHERE tgname = '{trigger_name}'
                              AND tgrelid = 'market.{table_name}'::regclass
                              AND NOT tgisinternal
                        ) THEN
                            CREATE TRIGGER {trigger_name}
                            BEFORE UPDATE OR DELETE ON market.{table_name}
                            FOR EACH ROW
                            EXECUTE FUNCTION market.reject_immutable_mutation();
                        END IF;
                    END;
                    $$
                    """
                )
            )

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

    def _assert_required_constraints(self, conn) -> None:
        """Fail loud when an existing queue omits ownership constraints."""

        inspector = inspect(conn)
        constraints = {
            str(constraint.get("name") or ""): constraint
            for constraint in inspector.get_check_constraints(
                "portal_async_jobs",
                schema=None,
            )
        }
        missing = sorted(REQUIRED_ASYNC_JOB_CONSTRAINTS - constraints.keys())
        if missing:
            logger.error(
                "portal_db_required_constraints_missing | "
                "schema=public table=portal_async_jobs missing=%s",
                ",".join(missing),
            )
            raise RuntimeError(
                "Table 'portal_async_jobs' is missing required constraints: "
                f"{', '.join(missing)}. Run "
                "scripts/db/manual_migration_async_job_fencing_v1.sql "
                "with backend and worker processes stopped."
            )

        mismatched = []
        for name, expected in _ASYNC_JOB_CONSTRAINT_DEFINITIONS.items():
            actual = constraints[name].get("sqltext")
            if _normalize_postgres_definition(actual) not in expected:
                mismatched.append(name)
        if mismatched:
            logger.error(
                "portal_db_required_constraints_mismatched | "
                "schema=public table=portal_async_jobs mismatched=%s",
                ",".join(mismatched),
            )
            raise RuntimeError(
                "Table 'portal_async_jobs' has mismatched constraint "
                f"definitions: {', '.join(mismatched)}. Run "
                "scripts/db/manual_migration_async_job_fencing_v1.sql "
                "with backend and worker processes stopped."
            )

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
        assert_required_indexes("portal_bot_run_leases", REQUIRED_BOT_RUN_LEASE_INDEXES)
        assert_required_indexes("portal_research_items", REQUIRED_RESEARCH_ITEM_INDEXES)
        assert_required_indexes("portal_research_links", REQUIRED_RESEARCH_LINK_INDEXES)
        assert_required_indexes("portal_provider_credential_refs", REQUIRED_PROVIDER_CREDENTIAL_INDEXES)
        assert_required_indexes("portal_async_jobs", REQUIRED_ASYNC_JOB_INDEXES)
        self._assert_async_job_index_definitions(inspector)

    def _assert_async_job_index_definitions(self, inspector) -> None:
        """Reject same-named async indexes with unsafe definitions."""

        indexes = {
            str(index.get("name") or ""): index
            for index in inspector.get_indexes(
                "portal_async_jobs",
                schema=None,
            )
        }
        mismatched = []
        for name, expected in _ASYNC_JOB_INDEX_DEFINITIONS.items():
            index = indexes.get(name)
            if index is None:
                continue
            if bool(index.get("unique")) != bool(expected["unique"]):
                mismatched.append(name)
                continue
            if tuple(index.get("column_names") or ()) != tuple(
                expected["columns"]
            ):
                mismatched.append(name)
                continue
            expected_predicates = expected.get("predicates")
            if expected_predicates is None:
                continue
            options = index.get("dialect_options") or {}
            predicate = str(
                options.get("postgresql_where")
                or index.get("filter_definition")
                or ""
            )
            normalized = _normalize_postgres_definition(predicate)
            if normalized not in expected_predicates:
                mismatched.append(name)
        if mismatched:
            logger.error(
                "portal_db_required_indexes_mismatched | "
                "schema=public table=portal_async_jobs mismatched=%s",
                ",".join(mismatched),
            )
            raise RuntimeError(
                "Table 'portal_async_jobs' has mismatched index "
                f"definitions: {', '.join(mismatched)}. Run "
                "scripts/db/manual_migration_async_job_fencing_v1.sql "
                "with backend and worker processes stopped."
            )

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

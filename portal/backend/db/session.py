"""Database session management helpers for the portal backend."""

from __future__ import annotations

import json
import logging
import re
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from core.settings import get_settings
from market_data.fact_registry import (
    build_normalized_fact_payload_schema,
    register_fact_payload_schema,
    supported_static_fact_payload_schemas,
)
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
    REQUIRED_RESEARCH_AUTHORITY_INDEXES,
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
    "market.candle_versions",
    "market.open_interest_versions",
    "market.funding_rate_versions",
    "market.numeric_fact_versions",
    "market.market_trade_versions",
    "market.trade_flow_aggregate_versions",
    "market.l2_snapshot_versions",
    "market.l2_snapshot_levels",
    "market.l2_mutation_batches",
    "market.l2_mutations",
    "market.bbo_feature_versions",
    "market.depth_feature_versions",
    "market.trade_flow_feature_versions",
    "market.futures_spot_relationship_versions",
    "market.derivative_state_versions",
    "market.market_response_feature_versions",
    "market.normalized_feature_versions",
)
_CANONICAL_MARKET_DATA_TABLE = "market.fact_versions"
_CLEAN_BOOTSTRAP_ONLY_TABLES = frozenset(
    {
        ("market", "fact_schemas"),
        ("market", "fact_versions"),
        ("market", "fact_acquisition_coverage"),
        ("market", "book_operational_rollups"),
    }
)
_NUMERIC_COVERAGE_REQUIRED_INDEXES = frozenset(
    {"ix_market_fact_acquisition_coverage_lookup"}
)
_CANONICAL_FACT_REQUIRED_INDEXES = frozenset(
    {
        "ix_market_fact_series_time_revision",
        "ix_market_fact_series_commit",
        "ix_market_fact_series_material",
        "ix_market_fact_series_source",
        "ix_market_fact_series_known",
        "ix_market_fact_schema_time",
        "ix_market_fact_source_time",
        "ix_market_fact_external_group",
        "ix_market_fact_payload_gin",
        "ix_market_fact_provenance_gin",
        "ix_market_fact_exact_value",
        "ix_market_fact_exact_rate",
        "ix_market_fact_funding_time",
    }
)
_CANONICAL_FACT_LOOKUP_INDEX_COLUMNS = {
    "ix_market_fact_series_material": ("series_id", "material_hash"),
    "ix_market_fact_series_source": ("series_id", "source_id"),
}
_BOOK_OPERATIONAL_STATUS_INDEXES = (
    (
        "fact_versions",
        "ix_market_fact_series_commit",
        ("series_id", "market_commit_seq"),
        "CREATE INDEX ix_market_fact_series_commit ON market.fact_versions "
        "USING btree (series_id, market_commit_seq)",
    ),
    (
        "book_checkpoint_manifests",
        "ix_market_book_checkpoint_series_acknowledged",
        ("series_id", "acknowledged_at", "id"),
        "CREATE INDEX ix_market_book_checkpoint_series_acknowledged ON "
        "market.book_checkpoint_manifests USING btree "
        "(series_id, acknowledged_at, id)",
    ),
)
_BOOK_CHECKPOINT_ROLLUP_TRIGGER_DEFINITION = (
    "CREATE TRIGGER trg_record_book_checkpoint_operational_rollup_v1 "
    "AFTER INSERT ON market.book_checkpoint_manifests FOR EACH ROW "
    "EXECUTE FUNCTION market.record_book_checkpoint_operational_rollup_v1()"
)
_BOOK_CHECKPOINT_ROLLUP_FUNCTION_BODY = """
BEGIN
    UPDATE market.book_operational_rollups
    SET checkpoint_count = checkpoint_count + 1,
        checkpoint_high_water_acknowledged_at = CASE
            WHEN checkpoint_count = 0
              OR (NEW.acknowledged_at, NEW.id) >
                 (checkpoint_high_water_acknowledged_at,
                  checkpoint_high_water_id)
            THEN NEW.acknowledged_at
            ELSE checkpoint_high_water_acknowledged_at
        END,
        checkpoint_high_water_id = CASE
            WHEN checkpoint_count = 0
              OR (NEW.acknowledged_at, NEW.id) >
                 (checkpoint_high_water_acknowledged_at,
                  checkpoint_high_water_id)
            THEN NEW.id
            ELSE checkpoint_high_water_id
        END,
        updated_at = now()
    WHERE series_id = NEW.series_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'market_book_operational_rollup_missing: checkpoint insert has no seeded counters series_id=%',
            NEW.series_id;
    END IF;
    RETURN NEW;
END;
""".strip()
_BOOK_CHECKPOINT_ROLLUP_FUNCTION_BODY_NORMALIZED = " ".join(
    _BOOK_CHECKPOINT_ROLLUP_FUNCTION_BODY.split()
)
_COLUMN_MIGRATION_GUIDANCE = {
    ("market", "collection_definitions", "desired_state"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "collection_definitions", "control_generation"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "collection_definitions", "control_requested_at"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "collection_definitions", "control_requested_by"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "collection_definitions", "control_request_id"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "stream_definitions", "desired_state"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "stream_definitions", "control_generation"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "stream_definitions", "control_requested_at"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "stream_definitions", "control_requested_by"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "stream_definitions", "control_request_id"):
        "scripts/db/manual_migration_collector_operations_v1.sql",
    ("market", "gap_evidence", "source_id"):
        "scripts/db/manual_migration_gap_source_identity_v1.sql",
    ("market", "dataset_series", "quality_evidence"):
        "scripts/db/manual_migration_dataset_quality_evidence_v1.sql",
    ("market", "dataset_series", "payload_schemas"):
        "scripts/db/manual_migration_canonical_fact_store_v1.sql",
}

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
            clean_database = self._is_clean_database(conn)
            if clean_database:
                logger.warning("portal_db_clean_schema_initializing")
            self._assert_market_data_cutover_state(conn)
            self._create_missing_schemas(conn)
            self._ensure_market_data_commit_sequence(conn)
            self._assert_market_commit_clock(conn, existing_only=True)
            self._assert_retired_tables_absent(conn)
            self._create_missing_tables(
                conn,
                include_clean_bootstrap=clean_database,
            )
            if clean_database:
                self._seed_static_fact_registry(conn)
                self._create_missing_indexes(
                    conn,
                    include_clean_bootstrap=True,
                )
                self._ensure_canonical_fact_insert_trigger(conn)
                self._ensure_book_checkpoint_rollup_trigger(conn)
                self._ensure_market_data_immutability(conn)
            self._assert_fact_acquisition_migration(conn)
            self._assert_canonical_fact_migration(conn)
            self._assert_book_operational_rollup_migration(conn)
            self._ensure_market_data_hypertables(conn)
            self._assert_market_commit_clock(conn, existing_only=False)
            self._assert_columns(conn)
            self._assert_required_constraints(conn)
            self._create_missing_indexes(conn)
            self._assert_required_indexes(conn)
            self._ensure_market_data_immutability(conn)
            if clean_database:
                logger.info("portal_db_clean_schema_ready")
            logger.info("portal_db_schema_contract_ready")

    def _is_clean_database(self, conn) -> bool:
        """Return whether no Quant-Trad model relation exists yet."""

        inspector = inspect(conn)
        existing_schemas = {str(name) for name in inspector.get_schema_names()}
        tables_by_schema: Dict[Optional[str], set[str]] = {}
        for table in Base.metadata.sorted_tables:
            schema_name = str(table.schema or "").strip() or None
            lookup_schema = schema_name or "public"
            if lookup_schema not in existing_schemas:
                continue
            if schema_name not in tables_by_schema:
                tables_by_schema[schema_name] = set(
                    inspector.get_table_names(schema=schema_name)
                )
            if table.name in tables_by_schema[schema_name]:
                return False
        return True

    def _ensure_canonical_fact_functions(self, conn) -> None:
        """Install functions required to create the current canonical Fact schema."""

        conn.execute(
            text(
                r"""
                CREATE OR REPLACE FUNCTION market.validate_fact_fields(
                    field_contracts jsonb,
                    candidate jsonb
                )
                RETURNS boolean
                LANGUAGE plpgsql
                IMMUTABLE
                AS $$
                DECLARE
                    field_contract jsonb;
                    field_name text;
                    field_kind text;
                    field_value jsonb;
                    field_text text;
                    required boolean;
                    nullable boolean;
                    minimum_text text;
                    minimum_inclusive boolean;
                    numeric_value numeric;
                    items_contract jsonb;
                    array_item jsonb;
                BEGIN
                    IF jsonb_typeof(field_contracts) <> 'array'
                       OR jsonb_typeof(candidate) <> 'object' THEN
                        RETURN false;
                    END IF;
                    IF EXISTS (
                        SELECT 1
                        FROM jsonb_object_keys(candidate) AS candidate_key
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM jsonb_array_elements(field_contracts) AS declared
                            WHERE declared->>'name' = candidate_key
                        )
                    ) THEN
                        RETURN false;
                    END IF;

                    FOR field_contract IN
                        SELECT value FROM jsonb_array_elements(field_contracts)
                    LOOP
                        field_name := field_contract->>'name';
                        field_kind := field_contract->>'kind';
                        required := COALESCE(
                            (field_contract->>'required')::boolean,
                            true
                        );
                        nullable := COALESCE(
                            (field_contract->>'nullable')::boolean,
                            false
                        );

                        IF NOT (candidate ? field_name) THEN
                            IF required THEN
                                RETURN false;
                            END IF;
                            CONTINUE;
                        END IF;

                        field_value := candidate->field_name;
                        IF jsonb_typeof(field_value) = 'null' THEN
                            IF NOT nullable THEN
                                RETURN false;
                            END IF;
                            CONTINUE;
                        END IF;
                        field_text := candidate->>field_name;
                        numeric_value := NULL;

                        IF field_kind IN ('decimal', 'float64') THEN
                            IF jsonb_typeof(field_value) <> 'string'
                               OR field_text !~ '^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$' THEN
                                RETURN false;
                            END IF;
                            numeric_value := field_text::numeric;
                        ELSIF field_kind = 'integer' THEN
                            IF jsonb_typeof(field_value) <> 'number'
                               OR field_text !~ '^-?(0|[1-9][0-9]*)$' THEN
                                RETURN false;
                            END IF;
                            numeric_value := field_text::numeric;
                        ELSIF field_kind = 'string' THEN
                            IF jsonb_typeof(field_value) <> 'string'
                               OR field_text = '' THEN
                                RETURN false;
                            END IF;
                            IF jsonb_array_length(
                                COALESCE(field_contract->'enum', '[]'::jsonb)
                            ) > 0
                               AND NOT (field_contract->'enum' ? field_text) THEN
                                RETURN false;
                            END IF;
                        ELSIF field_kind = 'timestamp' THEN
                            IF jsonb_typeof(field_value) <> 'string' THEN
                                RETURN false;
                            END IF;
                            PERFORM field_text::timestamptz;
                        ELSIF field_kind = 'boolean' THEN
                            IF jsonb_typeof(field_value) <> 'boolean' THEN
                                RETURN false;
                            END IF;
                        ELSIF field_kind = 'object' THEN
                            IF jsonb_typeof(field_value) <> 'object' THEN
                                RETURN false;
                            END IF;
                        ELSIF field_kind = 'array' THEN
                            IF jsonb_typeof(field_value) <> 'array' THEN
                                RETURN false;
                            END IF;
                            items_contract := field_contract->'items';
                            IF items_contract IS NOT NULL
                               AND jsonb_typeof(items_contract) <> 'null' THEN
                                IF items_contract->>'kind' <> 'object'
                                   OR jsonb_typeof(items_contract->'fields') <> 'array' THEN
                                    RETURN false;
                                END IF;
                                FOR array_item IN
                                    SELECT value FROM jsonb_array_elements(field_value)
                                LOOP
                                    IF NOT market.validate_fact_fields(
                                        items_contract->'fields',
                                        array_item
                                    ) THEN
                                        RETURN false;
                                    END IF;
                                END LOOP;
                            END IF;
                        ELSE
                            RETURN false;
                        END IF;

                        minimum_text := field_contract->>'minimum';
                        minimum_inclusive := COALESCE(
                            (field_contract->>'minimum_inclusive')::boolean,
                            true
                        );
                        IF minimum_text IS NOT NULL
                           AND numeric_value IS NOT NULL THEN
                            IF numeric_value < minimum_text::numeric
                               OR (
                                   NOT minimum_inclusive
                                   AND numeric_value = minimum_text::numeric
                               ) THEN
                                RETURN false;
                            END IF;
                        END IF;
                    END LOOP;
                    RETURN true;
                EXCEPTION
                    WHEN invalid_text_representation
                       OR datetime_field_overflow
                       OR numeric_value_out_of_range THEN
                        RETURN false;
                END;
                $$
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION market.validate_fact_payload(
                    requested_schema_id text,
                    candidate jsonb
                )
                RETURNS boolean
                LANGUAGE plpgsql
                STABLE
                AS $$
                DECLARE
                    schema_contract jsonb;
                BEGIN
                    SELECT contract
                    INTO schema_contract
                    FROM market.fact_schemas
                    WHERE schema_id = requested_schema_id;
                    IF schema_contract IS NULL THEN
                        RETURN false;
                    END IF;
                    RETURN market.validate_fact_fields(
                        schema_contract->'fields',
                        candidate
                    );
                END;
                $$
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION market.canonical_fact_utc_timestamp(
                    value text
                )
                RETURNS timestamptz
                LANGUAGE sql
                IMMUTABLE
                STRICT
                PARALLEL SAFE
                SET search_path = pg_catalog
                AS $$
                    SELECT (
                        substring(value FROM 1 FOR 26)::timestamp without time zone
                        AT TIME ZONE 'UTC'
                    )
                $$
                """
            )
        )

    def _seed_static_fact_registry(self, conn) -> None:
        """Seed code-owned immutable Fact contracts into a clean database."""

        statement = text(
            """
            INSERT INTO market.fact_schemas (
                schema_id,
                fact_type,
                contract_hash,
                contract,
                observation_time_field,
                material_hash_version,
                row_hash_version,
                query_fields,
                dataset_eligible
            ) VALUES (
                :schema_id,
                :fact_type,
                :contract_hash,
                CAST(:contract AS jsonb),
                :observation_time_field,
                :material_hash_version,
                :row_hash_version,
                CAST(:query_fields AS jsonb),
                :dataset_eligible
            )
            ON CONFLICT (schema_id) DO NOTHING
            """
        )
        for schema in supported_static_fact_payload_schemas():
            conn.execute(
                statement,
                {
                    "schema_id": schema.schema_id,
                    "fact_type": schema.fact_type,
                    "contract_hash": schema.contract_hash,
                    "contract": json.dumps(
                        schema.contract,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ),
                    "observation_time_field": schema.observation_time_field,
                    "material_hash_version": schema.material_hash_version,
                    "row_hash_version": schema.row_hash_version,
                    "query_fields": json.dumps(
                        list(schema.query_fields),
                        separators=(",", ":"),
                    ),
                    "dataset_eligible": schema.dataset_eligible,
                },
            )

    def _ensure_canonical_fact_insert_trigger(self, conn) -> None:
        """Install the current schema/series insert guard for a clean Fact store."""

        conn.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION market.assert_fact_version_valid()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    series_fact_type text;
                    series_contract_version text;
                BEGIN
                    SELECT fact_type, contract_version
                    INTO series_fact_type, series_contract_version
                    FROM market.series
                    WHERE id = NEW.series_id;
                    IF series_fact_type IS NULL THEN
                        RAISE EXCEPTION
                            'canonical_fact_invalid: unknown series_id=%',
                            NEW.series_id;
                    END IF;
                    IF NEW.fact_type <> series_fact_type
                       OR NEW.payload_schema_id <> series_contract_version THEN
                        RAISE EXCEPTION
                            'canonical_fact_invalid: series/schema mismatch series_id=% series_fact_type=% series_contract_version=% fact_type=% payload_schema_id=%',
                            NEW.series_id,
                            series_fact_type,
                            series_contract_version,
                            NEW.fact_type,
                            NEW.payload_schema_id;
                    END IF;
                    IF NOT market.validate_fact_payload(
                        NEW.payload_schema_id,
                        NEW.payload
                    ) THEN
                        RAISE EXCEPTION
                            'canonical_fact_invalid: payload does not satisfy schema_id=% observation_key=%',
                            NEW.payload_schema_id,
                            NEW.observation_key;
                    END IF;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TRIGGER trg_assert_fact_version_valid
                BEFORE INSERT ON market.fact_versions
                FOR EACH ROW
                EXECUTE FUNCTION market.assert_fact_version_valid()
                """
            )
        )

    def _ensure_book_checkpoint_rollup_trigger(self, conn) -> None:
        """Install the durable checkpoint counter for a clean market store."""

        conn.execute(
            text(
                f"""
                CREATE OR REPLACE FUNCTION
                    market.record_book_checkpoint_operational_rollup_v1()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                {_BOOK_CHECKPOINT_ROLLUP_FUNCTION_BODY}
                $$
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TRIGGER
                    trg_record_book_checkpoint_operational_rollup_v1
                AFTER INSERT ON market.book_checkpoint_manifests
                FOR EACH ROW
                EXECUTE FUNCTION
                    market.record_book_checkpoint_operational_rollup_v1()
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE market.book_checkpoint_manifests
                ENABLE ALWAYS TRIGGER
                    trg_record_book_checkpoint_operational_rollup_v1
                """
            )
        )

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
            "scripts/db/manual_migration_canonical_fact_hard_cutover_v1.sql. "
            "The canonical service will not start with dual Fact ownership."
        )

    def _ensure_market_data_commit_sequence(self, conn) -> None:
        """Create the one causal commit clock shared by every typed fact table."""

        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS market.fact_commit_seq"))

    def _assert_market_commit_clock(self, conn, *, existing_only: bool) -> None:
        """Reject per-table identity clocks before heterogeneous facts can start."""

        for table_name in (
            "fact_versions",
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
        """Require TimescaleDB for retained market projections and evidence."""

        extension_version = conn.execute(
            text("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        ).scalar_one_or_none()
        if not extension_version:
            raise RuntimeError(
                "TimescaleDB is required for canonical market-data storage. "
                "Install the extension before starting the backend."
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
            "fact_schemas",
            "fact_versions",
            "fact_acquisition_coverage",
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
            "storage_lifecycle_events",
            "stream_coverage_interval_versions",
            "stream_quality_events",
            "market_trade_identities",
            "normalization_specs",
            "dataset_normalization_refs",
            "book_validity_interval_versions",
            "book_checkpoint_manifests",
            "book_quality_event_links",
            "dataset_archive_refs",
            "collector_operation_events",
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

    def _create_missing_tables(
        self,
        conn,
        *,
        include_clean_bootstrap: bool = False,
    ) -> None:
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
            if (
                (schema_name, table.name) in _CLEAN_BOOTSTRAP_ONLY_TABLES
                and not include_clean_bootstrap
            ):
                continue
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
            if (
                include_clean_bootstrap
                and (schema_name, table.name) == ("market", "fact_versions")
            ):
                # fact_schemas is dependency-sorted ahead of fact_versions.
                # Install the validation functions only after that registry
                # relation exists and immediately before its first consumer.
                self._ensure_canonical_fact_functions(conn)
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

    def _assert_book_operational_rollup_migration(self, conn) -> None:
        """Require the explicitly seeded Level 2 status rollup on existing stores."""

        relation = conn.execute(
            text("SELECT to_regclass(:table_ref)"),
            {"table_ref": "market.book_operational_rollups"},
        ).scalar_one_or_none()
        if relation is None:
            raise RuntimeError(
                "Table 'market.book_operational_rollups' is missing. Run "
                "scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )
        expected = {
            "series_id",
            "snapshot_count",
            "batch_count",
            "mutation_count",
            "checkpoint_count",
            "checkpoint_high_water_acknowledged_at",
            "checkpoint_high_water_id",
            "fact_high_water_commit_seq",
            "updated_at",
        }
        inspector = inspect(conn)
        existing = {
            str(column["name"])
            for column in inspector.get_columns(
                "book_operational_rollups",
                schema="market",
            )
        }
        missing = sorted(expected - existing)
        if missing:
            raise RuntimeError(
                "Table 'market.book_operational_rollups' is missing columns: "
                f"{', '.join(missing)}. Run "
                "scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )
        checkpoint_trigger_ready = bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_trigger AS trigger
                        JOIN pg_proc AS procedure
                          ON procedure.oid = trigger.tgfoid
                        JOIN pg_namespace AS procedure_namespace
                          ON procedure_namespace.oid = procedure.pronamespace
                        JOIN pg_language AS language
                          ON language.oid = procedure.prolang
                        WHERE trigger.tgname =
                            'trg_record_book_checkpoint_operational_rollup_v1'
                          AND trigger.tgrelid =
                            'market.book_checkpoint_manifests'::regclass
                          AND NOT trigger.tgisinternal
                          AND trigger.tgenabled = 'A'
                          AND trigger.tgtype = 5
                          AND trigger.tgconstraint = 0
                          AND trigger.tgnargs = 0
                          AND trigger.tgqual IS NULL
                          AND trigger.tgoldtable IS NULL
                          AND trigger.tgnewtable IS NULL
                          AND pg_get_triggerdef(trigger.oid, false) =
                              :expected_trigger_definition
                          AND procedure_namespace.nspname = 'market'
                          AND procedure.proname =
                            'record_book_checkpoint_operational_rollup_v1'
                          AND procedure.prokind = 'f'
                          AND procedure.prorettype = 'trigger'::regtype
                          AND procedure.pronargs = 0
                          AND NOT procedure.proretset
                          AND language.lanname = 'plpgsql'
                          AND procedure.provolatile = 'v'
                          AND procedure.proparallel = 'u'
                          AND NOT procedure.proisstrict
                          AND NOT procedure.prosecdef
                          AND NOT procedure.proleakproof
                          AND procedure.proconfig IS NULL
                          AND btrim(
                              regexp_replace(
                                  procedure.prosrc,
                                  '[[:space:]]+',
                                  ' ',
                                  'g'
                              )
                          ) = :expected_function_body
                          AND has_function_privilege(
                              current_user,
                              procedure.oid,
                              'EXECUTE'
                          )
                    ) AS book_checkpoint_rollup_trigger_ready
                    """
                ),
                {
                    "expected_trigger_definition": (
                        _BOOK_CHECKPOINT_ROLLUP_TRIGGER_DEFINITION
                    ),
                    "expected_function_body": (
                        _BOOK_CHECKPOINT_ROLLUP_FUNCTION_BODY_NORMALIZED
                    ),
                },
            ).scalar_one()
        )
        if not checkpoint_trigger_ready:
            trigger_contract = conn.execute(
                text(
                    """
                    SELECT
                        trigger.tgenabled AS trigger_enabled,
                        trigger.tgtype AS trigger_type,
                        trigger.tgconstraint AS trigger_constraint_oid,
                        trigger.tgnargs AS trigger_arg_count,
                        trigger.tgqual AS trigger_when,
                        trigger.tgoldtable AS old_transition_table,
                        trigger.tgnewtable AS new_transition_table,
                        pg_get_triggerdef(trigger.oid, false)
                            AS trigger_definition,
                        procedure_namespace.nspname AS function_schema,
                        procedure.proname AS function_name,
                        procedure.prokind AS function_kind,
                        procedure.prorettype::regtype::text
                            AS function_return_type,
                        procedure.pronargs AS function_arg_count,
                        procedure.proretset AS function_returns_set,
                        language.lanname AS function_language,
                        procedure.provolatile AS function_volatility,
                        procedure.proparallel AS function_parallelism,
                        procedure.proisstrict AS function_strict,
                        procedure.prosecdef AS function_security_definer,
                        procedure.proleakproof AS function_leakproof,
                        procedure.proconfig AS function_config,
                        md5(
                            btrim(
                                regexp_replace(
                                    procedure.prosrc,
                                    '[[:space:]]+',
                                    ' ',
                                    'g'
                                )
                            )
                        ) AS function_body_hash,
                        has_function_privilege(
                            current_user,
                            procedure.oid,
                            'EXECUTE'
                        ) AS caller_can_execute
                    FROM pg_trigger AS trigger
                    JOIN pg_proc AS procedure
                      ON procedure.oid = trigger.tgfoid
                    JOIN pg_namespace AS procedure_namespace
                      ON procedure_namespace.oid = procedure.pronamespace
                    JOIN pg_language AS language
                      ON language.oid = procedure.prolang
                    WHERE trigger.tgname =
                        'trg_record_book_checkpoint_operational_rollup_v1'
                      AND trigger.tgrelid =
                        'market.book_checkpoint_manifests'::regclass
                      AND NOT trigger.tgisinternal
                    """
                )
            ).mappings().first()
            observed_contract = (
                "trigger_row=missing"
                if trigger_contract is None
                else ", ".join(
                    f"{key}={value!r}"
                    for key, value in trigger_contract.items()
                )
            )
            raise RuntimeError(
                "Table 'market.book_checkpoint_manifests' is missing the "
                "always-on durable checkpoint rollup trigger. "
                f"Observed contract: {observed_contract}. Run "
                "scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )
        self._assert_book_operational_status_indexes(conn)
        missing_series_id = conn.execute(
            text(
                """
                WITH required_series AS (
                    SELECT series.id AS series_id
                    FROM market.series AS series
                    WHERE series.contract_version = 'market.l2_book.v1'
                    UNION
                    SELECT definitions.series_id
                    FROM market.stream_definitions AS definitions
                    WHERE definitions.contract_version = 'market.l2_book.v1'
                )
                SELECT required_series.series_id
                    AS missing_required_book_rollup_series_id
                FROM required_series
                LEFT JOIN market.book_operational_rollups AS rollups
                  ON rollups.series_id = required_series.series_id
                WHERE rollups.series_id IS NULL
                ORDER BY required_series.series_id
                LIMIT 1
                """
            )
        ).scalar_one_or_none()
        if missing_series_id is not None:
            raise RuntimeError(
                "Table 'market.book_operational_rollups' has no seeded row for "
                f"Level 2 series_id={int(missing_series_id)}. Run "
                "scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )
        stale_fact = conn.execute(
            text(
                """
                SELECT rollups.series_id,
                       rollups.fact_high_water_commit_seq,
                       COALESCE(latest.market_commit_seq, 0)
                           AS latest_fact_commit_seq
                FROM market.book_operational_rollups AS rollups
                JOIN market.series AS series
                  ON series.id = rollups.series_id
                LEFT JOIN LATERAL (
                    SELECT facts.market_commit_seq
                    FROM market.fact_versions AS facts
                    WHERE facts.series_id = rollups.series_id
                      AND facts.payload_schema_id = 'market.l2_book.v1'
                    ORDER BY facts.market_commit_seq DESC
                    LIMIT 1
                ) AS latest ON TRUE
                WHERE series.contract_version = 'market.l2_book.v1'
                  AND COALESCE(latest.market_commit_seq, 0)
                      <> rollups.fact_high_water_commit_seq
                ORDER BY rollups.series_id
                LIMIT 1
                """
            )
        ).mappings().first()
        if stale_fact is not None:
            raise RuntimeError(
                "Table 'market.book_operational_rollups' has stale canonical "
                f"Fact counters for Level 2 series_id={int(stale_fact['series_id'])} "
                "stored_fact_commit_seq="
                f"{int(stale_fact['fact_high_water_commit_seq'])} "
                "latest_fact_commit_seq="
                f"{int(stale_fact['latest_fact_commit_seq'])}. Run "
                "scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )
        stale_checkpoint = conn.execute(
            text(
                """
                SELECT rollups.series_id,
                       rollups.checkpoint_high_water_id,
                       latest.id AS latest_checkpoint_id
                FROM market.book_operational_rollups AS rollups
                JOIN market.series AS series
                  ON series.id = rollups.series_id
                LEFT JOIN LATERAL (
                    SELECT checkpoints.id, checkpoints.acknowledged_at
                    FROM market.book_checkpoint_manifests AS checkpoints
                    WHERE checkpoints.series_id = rollups.series_id
                    ORDER BY checkpoints.acknowledged_at DESC,
                             checkpoints.id DESC
                    LIMIT 1
                ) AS latest ON TRUE
                WHERE series.contract_version = 'market.l2_book.v1'
                  AND (
                      latest.id IS DISTINCT FROM
                          rollups.checkpoint_high_water_id
                      OR latest.acknowledged_at IS DISTINCT FROM
                          rollups.checkpoint_high_water_acknowledged_at
                  )
                ORDER BY rollups.series_id
                LIMIT 1
                """
            )
        ).mappings().first()
        if stale_checkpoint is not None:
            raise RuntimeError(
                "Table 'market.book_operational_rollups' has stale checkpoint "
                f"counters for Level 2 series_id={int(stale_checkpoint['series_id'])} "
                f"stored_checkpoint_id={stale_checkpoint['checkpoint_high_water_id']} "
                f"latest_checkpoint_id={stale_checkpoint['latest_checkpoint_id']}. "
                "Run scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )

    def _assert_book_operational_status_indexes(self, conn) -> None:
        """Require exact bounded-read indexes before probing Level 2 high waters."""

        mismatches: list[str] = []
        for (
            table_name,
            index_name,
            expected_columns,
            expected_definition,
        ) in (
            _BOOK_OPERATIONAL_STATUS_INDEXES
        ):
            row = conn.execute(
                text(
                    """
                    SELECT
                        index_class.relname AS operational_index_name,
                        index_state.indisvalid,
                        index_state.indisready,
                        index_state.indisunique,
                        index_state.indnkeyatts,
                        index_state.indnatts,
                        index_state.indexprs IS NULL AS has_plain_columns,
                        index_state.indpred IS NULL AS is_unfiltered,
                        access_method.amname AS access_method,
                        ARRAY(
                            SELECT COALESCE(
                                attribute.attname::text,
                                '<expression>'
                            )
                            FROM unnest(
                                index_state.indkey::smallint[]
                            ) WITH ORDINALITY
                                AS index_key(attnum, position)
                            LEFT JOIN pg_attribute AS attribute
                              ON attribute.attrelid = index_state.indrelid
                             AND attribute.attnum = index_key.attnum
                            WHERE index_key.position
                                  <= index_state.indnkeyatts
                            ORDER BY index_key.position
                        ) AS key_columns,
                        pg_get_indexdef(index_state.indexrelid) AS definition
                    FROM pg_index AS index_state
                    JOIN pg_class AS index_class
                      ON index_class.oid = index_state.indexrelid
                    JOIN pg_class AS table_class
                      ON table_class.oid = index_state.indrelid
                    JOIN pg_namespace AS table_namespace
                      ON table_namespace.oid = table_class.relnamespace
                    JOIN pg_namespace AS index_namespace
                      ON index_namespace.oid = index_class.relnamespace
                    JOIN pg_am AS access_method
                      ON access_method.oid = index_class.relam
                    WHERE table_namespace.nspname = 'market'
                      AND table_class.relname = :table_name
                      AND index_namespace.nspname = 'market'
                      AND index_class.relname = :index_name
                    """
                ),
                {"table_name": table_name, "index_name": index_name},
            ).mappings().first()
            actual_columns = tuple(row["key_columns"] or ()) if row else ()
            if (
                row is None
                or not bool(row["indisvalid"])
                or not bool(row["indisready"])
                or bool(row["indisunique"])
                or str(row["access_method"]) != "btree"
                or not bool(row["has_plain_columns"])
                or not bool(row["is_unfiltered"])
                or int(row["indnkeyatts"]) != len(expected_columns)
                or int(row["indnatts"]) != len(expected_columns)
                or actual_columns != expected_columns
                or str(row["definition"]) != expected_definition
            ):
                definition = str(row["definition"]) if row else "<missing>"
                mismatches.append(
                    f"{index_name} expected={expected_columns!r} "
                    f"actual={definition}"
                )
        if mismatches:
            raise RuntimeError(
                "Level 2 bounded status indexes are missing or invalid: "
                f"{'; '.join(mismatches)}. Run "
                "scripts/db/manual_migration_book_operational_rollups_v1.sql "
                "with writers stopped before starting this code."
            )

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
            migrations = sorted(
                {
                    migration
                    for column in missing
                    if (
                        migration := _COLUMN_MIGRATION_GUIDANCE.get(
                            (schema_name, table.name, column)
                        )
                    )
                }
            )
            if migrations:
                raise RuntimeError(
                    f"Table '{schema_name + '.' if schema_name else ''}{table.name}' "
                    f"is missing columns: {', '.join(missing)}. Run "
                    f"{', then '.join(migrations)} with writers stopped before "
                    "starting this code."
                )
            raise RuntimeError(
                f"Table '{schema_name + '.' if schema_name else ''}{table.name}' is missing columns: {', '.join(missing)}. "
                "Drop the table or rebuild the database to ensure a clean schema."
            )

    def _create_missing_indexes(
        self,
        conn,
        *,
        include_clean_bootstrap: bool = False,
    ) -> None:
        """Create metadata indexes that are missing from otherwise valid tables."""

        inspector = inspect(conn)
        for table in Base.metadata.sorted_tables:
            schema_name = str(table.schema or "").strip() or None
            if (
                (schema_name, table.name) in _CLEAN_BOOTSTRAP_ONLY_TABLES
                and not include_clean_bootstrap
            ):
                continue
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

    def _assert_fact_acquisition_migration(self, conn) -> None:
        """Validate migration-owned acquisition coverage without startup DDL."""

        if conn.execute(
            text("SELECT to_regclass(:table_ref)"),
            {"table_ref": "market.fact_acquisition_coverage"},
        ).scalar_one_or_none() is None:
            raise RuntimeError(
                "Migration-owned acquisition coverage table "
                "'market.fact_acquisition_coverage' is missing. Stop "
                "backend, collector, worker, and paper processes, then run "
                "scripts/db/manual_migration_numeric_fact_store_v1.sql."
            )
        inspector = inspect(conn)
        series_columns = {
            str(column["name"])
            for column in inspector.get_columns("series", schema="market")
        }
        if "dimensions" not in series_columns:
            raise RuntimeError(
                "Table 'market.series' is missing migration-owned column dimensions. "
                "Run scripts/db/manual_migration_numeric_fact_store_v1.sql."
            )
        coverage_indexes = {
            str(item.get("name") or "")
            for item in inspector.get_indexes(
                "fact_acquisition_coverage", schema="market"
            )
        }
        missing_coverage_indexes = sorted(
            _NUMERIC_COVERAGE_REQUIRED_INDEXES - coverage_indexes
        )
        if missing_coverage_indexes:
            raise RuntimeError(
                "Table 'market.fact_acquisition_coverage' is missing required "
                f"indexes: {', '.join(missing_coverage_indexes)}. Run "
                "scripts/db/manual_migration_numeric_fact_store_v1.sql."
            )
        trigger_name = "trg_reject_mutation_fact_acquisition_coverage"
        exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_trigger
                    WHERE tgname = :trigger_name
                      AND tgrelid =
                          'market.fact_acquisition_coverage'::regclass
                      AND NOT tgisinternal
                )
                """
            ),
            {"trigger_name": trigger_name},
        ).scalar_one()
        if not bool(exists):
            raise RuntimeError(
                "Table 'market.fact_acquisition_coverage' is missing immutable "
                f"trigger '{trigger_name}'. Run "
                "scripts/db/manual_migration_numeric_fact_store_v1.sql."
            )

    def _assert_canonical_fact_migration(self, conn) -> None:
        """Require the explicit generalized Fact schema and code registry."""

        for table_name in ("fact_schemas", "fact_versions"):
            if conn.execute(
                text("SELECT to_regclass(:table_ref)"),
                {"table_ref": f"market.{table_name}"},
            ).scalar_one_or_none() is None:
                raise RuntimeError(
                    "Canonical Fact schema is missing table "
                    f"market.{table_name}. Stop backend, collectors, workers, "
                    "and paper runtimes, then run "
                    "scripts/db/manual_migration_canonical_fact_store_v1.sql."
                )

        inspector = inspect(conn)
        primary_key = inspector.get_pk_constraint("fact_versions", schema="market")
        if tuple(primary_key.get("constrained_columns") or ()) != ("id",):
            raise RuntimeError(
                "Table 'market.fact_versions' must use canonical primary key (id). "
                "Run scripts/db/manual_migration_canonical_fact_store_v1.sql."
            )
        indexes = {
            str(item.get("name") or "")
            for item in inspector.get_indexes("fact_versions", schema="market")
        }
        missing_indexes = sorted(_CANONICAL_FACT_REQUIRED_INDEXES - indexes)
        if missing_indexes:
            migration = (
                "scripts/db/manual_migration_canonical_fact_lookup_indexes_v1.sql"
                if set(missing_indexes)
                <= set(_CANONICAL_FACT_LOOKUP_INDEX_COLUMNS)
                else "scripts/db/manual_migration_canonical_fact_store_v1.sql"
            )
            raise RuntimeError(
                "Table 'market.fact_versions' is missing required indexes: "
                f"{', '.join(missing_indexes)}. Run "
                f"{migration}."
            )
        lookup_index_rows = {
            str(row["index_name"]): row
            for row in conn.execute(
                text(
                    """
                    SELECT
                        index_class.relname AS index_name,
                        index_state.indisvalid,
                        index_state.indisready,
                        index_state.indisunique,
                        index_state.indnkeyatts,
                        index_state.indnatts,
                        index_state.indexprs IS NULL AS has_plain_columns,
                        index_state.indpred IS NULL AS is_unfiltered,
                        access_method.amname AS access_method,
                        ARRAY(
                            SELECT COALESCE(
                                attribute.attname::text,
                                '<expression>'
                            )
                            FROM unnest(
                                index_state.indkey::smallint[]
                            ) WITH ORDINALITY
                                AS index_key(attnum, position)
                            LEFT JOIN pg_attribute AS attribute
                              ON attribute.attrelid = index_state.indrelid
                             AND attribute.attnum = index_key.attnum
                            WHERE index_key.position
                                  <= index_state.indnkeyatts
                            ORDER BY index_key.position
                        ) AS key_columns,
                        pg_get_indexdef(index_state.indexrelid) AS definition
                    FROM pg_index AS index_state
                    JOIN pg_class AS index_class
                      ON index_class.oid = index_state.indexrelid
                    JOIN pg_class AS table_class
                      ON table_class.oid = index_state.indrelid
                    JOIN pg_namespace AS table_namespace
                      ON table_namespace.oid = table_class.relnamespace
                    JOIN pg_namespace AS index_namespace
                      ON index_namespace.oid = index_class.relnamespace
                    JOIN pg_am AS access_method
                      ON access_method.oid = index_class.relam
                    WHERE table_namespace.nspname = 'market'
                      AND table_class.relname = 'fact_versions'
                      AND index_namespace.nspname = 'market'
                      AND index_class.relname::text
                          = ANY(CAST(:index_names AS text[]))
                    """
                ),
                {"index_names": list(_CANONICAL_FACT_LOOKUP_INDEX_COLUMNS)},
            ).mappings()
        }
        mismatched_lookup_indexes: list[str] = []
        for index_name, expected_columns in sorted(
            _CANONICAL_FACT_LOOKUP_INDEX_COLUMNS.items()
        ):
            row = lookup_index_rows.get(index_name)
            actual_columns = tuple(row["key_columns"] or ()) if row else ()
            if (
                row is None
                or not bool(row["indisvalid"])
                or not bool(row["indisready"])
                or bool(row["indisunique"])
                or str(row["access_method"]) != "btree"
                or not bool(row["has_plain_columns"])
                or not bool(row["is_unfiltered"])
                or int(row["indnkeyatts"]) != len(expected_columns)
                or int(row["indnatts"]) != len(expected_columns)
                or actual_columns != expected_columns
            ):
                definition = str(row["definition"]) if row else "<missing>"
                mismatched_lookup_indexes.append(
                    f"{index_name} expected={expected_columns!r} "
                    f"actual={definition}"
                )
        if mismatched_lookup_indexes:
            raise RuntimeError(
                "Table 'market.fact_versions' has invalid canonical Fact lookup "
                "index definitions: "
                f"{'; '.join(mismatched_lookup_indexes)}. Run "
                "scripts/db/manual_migration_canonical_fact_lookup_indexes_v1.sql."
            )
        dataset_columns = {
            str(column["name"])
            for column in inspector.get_columns("dataset_series", schema="market")
        }
        if "payload_schemas" not in dataset_columns:
            raise RuntimeError(
                "Table 'market.dataset_series' is missing payload_schemas. Run "
                "scripts/db/manual_migration_canonical_fact_store_v1.sql."
            )

        stored_registry = {
            str(row["schema_id"]): (
                str(row["fact_type"]),
                str(row["contract_hash"]),
            )
            for row in conn.execute(
                text(
                    "SELECT schema_id, fact_type, contract_hash "
                    "FROM market.fact_schemas"
                )
            ).mappings()
        }
        expected_registry = {
            schema.schema_id: (schema.fact_type, schema.contract_hash)
            for schema in supported_static_fact_payload_schemas()
        }
        normalized_specs = conn.execute(
            text(
                "SELECT id, output_fact_type, units "
                "FROM market.normalization_specs "
                "WHERE id ~ '^nsp_[0-9a-f]{31}$' "
                "ORDER BY id"
            )
        ).mappings()
        for spec in normalized_specs:
            schema = register_fact_payload_schema(
                build_normalized_fact_payload_schema(
                    spec_id=str(spec["id"]),
                    fact_type=str(spec["output_fact_type"]),
                    units=str(spec["units"]),
                )
            )
            expected_registry[schema.schema_id] = (
                schema.fact_type,
                schema.contract_hash,
            )
        if stored_registry != expected_registry:
            raise RuntimeError(
                "Canonical Fact schema registry differs from code. Stop all writers "
                "and apply the canonical Fact store/data migration."
            )
        for table_name, trigger_name in (
            ("fact_schemas", "trg_reject_mutation_fact_schemas"),
            ("fact_versions", "trg_reject_mutation_fact_versions"),
            ("fact_versions", "trg_assert_fact_version_valid"),
        ):
            exists = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_trigger
                        WHERE tgname = :trigger_name
                          AND tgrelid = CAST(:table_ref AS regclass)
                          AND NOT tgisinternal
                    )
                    """
                ),
                {
                    "trigger_name": trigger_name,
                    "table_ref": f"market.{table_name}",
                },
            ).scalar_one()
            if not bool(exists):
                raise RuntimeError(
                    f"Table 'market.{table_name}' is missing trigger "
                    f"'{trigger_name}'. Run "
                    "scripts/db/manual_migration_canonical_fact_store_v1.sql."
                )

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
        for table_name, required_indexes in sorted(
            REQUIRED_RESEARCH_AUTHORITY_INDEXES.items()
        ):
            assert_required_indexes(table_name, required_indexes)
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

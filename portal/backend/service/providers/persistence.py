import hashlib
import json
import re
from typing import Any, List, Mapping, Tuple

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateIndex, CreateTable

from core.logger import logger
from indicators.config import DataContext
from data_providers.config.runtime import PersistenceConfig
from data_providers.utils.ohlcv import interval_to_timedelta


_SCHEMA_LOCK_KEY = 9021002
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(value: str, *, setting: str) -> str:
    name = str(value or "").strip()
    if not _TABLE_NAME_RE.match(name):
        raise ValueError(f"{setting} must be a simple PostgreSQL table identifier.")
    return name


def _market_data_tables(config: PersistenceConfig) -> tuple[Table, Table, Table]:
    metadata = MetaData()
    candles_raw = Table(
        _validate_table_name(config.candles_raw_table, setting="candles_raw_table"),
        metadata,
        Column("instrument_id", Text, nullable=False),
        Column("timeframe_seconds", Integer, nullable=False),
        Column("candle_time", TIMESTAMP(timezone=True), nullable=False),
        Column("close_time", TIMESTAMP(timezone=True), nullable=False),
        Column("open", Float, nullable=False),
        Column("high", Float, nullable=False),
        Column("low", Float, nullable=False),
        Column("close", Float, nullable=False),
        Column("volume", Float, nullable=True),
        Column("trade_count", BigInteger, nullable=True),
        Column("is_closed", Boolean, nullable=False, server_default=text("TRUE")),
        Column("source_time", TIMESTAMP(timezone=True), nullable=True),
        Column("inserted_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
        PrimaryKeyConstraint("instrument_id", "timeframe_seconds", "candle_time"),
        CheckConstraint("timeframe_seconds > 0"),
        CheckConstraint("close_time > candle_time"),
        CheckConstraint("high >= low"),
        CheckConstraint("low <= open AND open <= high"),
        CheckConstraint("low <= close AND close <= high"),
        CheckConstraint("volume IS NULL OR volume >= 0"),
        CheckConstraint("trade_count IS NULL OR trade_count >= 0"),
    )
    derivatives_state = Table(
        _validate_table_name(config.derivatives_state_table, setting="derivatives_state_table"),
        metadata,
        Column("instrument_id", Text, nullable=False),
        Column("observed_at", TIMESTAMP(timezone=True), nullable=False),
        Column("source_time", TIMESTAMP(timezone=True), nullable=True),
        Column("open_interest", Float, nullable=True),
        Column("open_interest_value", Float, nullable=True),
        Column("funding_rate", Float, nullable=True),
        Column("funding_time", TIMESTAMP(timezone=True), nullable=True),
        Column("mark_price", Float, nullable=True),
        Column("index_price", Float, nullable=True),
        Column("premium_rate", Float, nullable=True),
        Column("premium_index", Float, nullable=True),
        Column("next_funding_time", TIMESTAMP(timezone=True), nullable=True),
        Column("inserted_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")),
        PrimaryKeyConstraint("instrument_id", "observed_at"),
        CheckConstraint("open_interest IS NULL OR open_interest >= 0"),
        CheckConstraint("open_interest_value IS NULL OR open_interest_value >= 0"),
    )
    closures = Table(
        _validate_table_name(config.closures_table, setting="closures_table"),
        metadata,
        Column("instrument_id", Text, nullable=False),
        Column("timeframe_seconds", Integer, nullable=False),
        Column("start_ts", TIMESTAMP(timezone=True), nullable=False),
        Column("end_ts", TIMESTAMP(timezone=True), nullable=False),
        Column("metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
        Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()")),
        PrimaryKeyConstraint("instrument_id", "timeframe_seconds", "start_ts", "end_ts"),
        CheckConstraint("timeframe_seconds > 0"),
        CheckConstraint("end_ts > start_ts"),
    )
    Index(
        "idx_candles_raw_instrument_tf_time",
        candles_raw.c.instrument_id,
        candles_raw.c.timeframe_seconds,
        candles_raw.c.candle_time.desc(),
    )
    Index(
        "idx_derivatives_state_instrument_time",
        derivatives_state.c.instrument_id,
        derivatives_state.c.observed_at.desc(),
    )
    Index("idx_derivatives_state_time", derivatives_state.c.observed_at.desc())
    Index("idx_candle_closures_lookup", closures.c.instrument_id, closures.c.timeframe_seconds, closures.c.start_ts)
    return candles_raw, derivatives_state, closures


class DataPersistenceService:
    """Handle storage, schema management, and closure bookkeeping for OHLCV data."""

    def __init__(self, config: PersistenceConfig, *, engine=None):
        self._config = config
        self._tables = _market_data_tables(config)
        self._engine = engine or (create_engine(config.dsn) if config.dsn else None)
        self._schema_logged = False

        if not self._engine:
            logger.warning("Database engine unavailable; persistence features disabled.")

    @property
    def config(self) -> PersistenceConfig:
        return self._config

    @property
    def engine_available(self) -> bool:
        return self._engine is not None

    @staticmethod
    def _advisory_lock_key(
        *,
        datasource: str,
        instrument_id: str,
        timeframe_seconds: int,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> int:
        payload = "|".join(
            [
                str(datasource or "").upper(),
                str(instrument_id or ""),
                str(int(timeframe_seconds)),
                pd.to_datetime(start, utc=True).isoformat(),
                pd.to_datetime(end, utc=True).isoformat(),
            ]
        )
        digest = hashlib.blake2b(payload.encode("utf-8"), digest_size=8).digest()
        value = int.from_bytes(digest, byteorder="big", signed=False)
        if value >= 2**63:
            value -= 2**64
        return value

    def acquire_ingest_lock(
        self,
        ctx: DataContext,
        datasource: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> Any | None:
        """Acquire a series/window advisory lock before provider ingestion."""

        if not self._engine or pd.to_datetime(end, utc=True) <= pd.to_datetime(start, utc=True):
            return None

        self.ensure_schema()

        conn = None
        try:
            instrument_id, timeframe_seconds = self._resolve_context(ctx)
            lock_key = self._advisory_lock_key(
                datasource=datasource,
                instrument_id=instrument_id,
                timeframe_seconds=timeframe_seconds,
                start=pd.to_datetime(start, utc=True),
                end=pd.to_datetime(end, utc=True),
            )
            conn = self._engine.connect()
            conn.execute(text("SELECT pg_advisory_lock(:lock_key)"), {"lock_key": lock_key})
            logger.debug(
                "candle_ingest_lock_acquired | datasource=%s instrument_id=%s timeframe_seconds=%s start=%s end=%s",
                datasource,
                instrument_id,
                timeframe_seconds,
                pd.to_datetime(start, utc=True).isoformat(),
                pd.to_datetime(end, utc=True).isoformat(),
            )
            return conn, lock_key
        except SQLAlchemyError as exc:
            logger.warning("candle_ingest_lock_acquire_failed | error=%s", exc)
            if conn is not None:
                conn.close()
            return None

    def release_ingest_lock(self, handle: Any | None) -> None:
        """Release a lock returned by acquire_ingest_lock."""

        if not handle:
            return
        conn, lock_key = handle
        try:
            conn.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": lock_key})
        except SQLAlchemyError as exc:
            logger.warning("candle_ingest_lock_release_failed | error=%s", exc)
        finally:
            conn.close()

    def ensure_schema(self):
        """Create or validate candle, derivatives, and closure storage schema."""

        if not self._engine:
            logger.warning(
                "Database engine unavailable; skipping ensure_schema call for '%s'.",
                self._config.candles_raw_table,
            )
            return

        try:
            with self._engine.begin() as conn:
                conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": _SCHEMA_LOCK_KEY})
                try:
                    self._bootstrap_schema_contract(conn)
                finally:
                    conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _SCHEMA_LOCK_KEY})
            if not self._schema_logged:
                logger.info(
                    "market_data_schema_contract_ready | raw=%s derivatives=%s closures=%s",
                    self._config.candles_raw_table,
                    self._config.derivatives_state_table,
                    self._config.closures_table,
                )
                self._schema_logged = True
        except SQLAlchemyError as e:
            logger.exception(
                "Failed to ensure schema for raw=%s derivatives=%s closures=%s: %s",
                self._config.candles_raw_table,
                self._config.derivatives_state_table,
                self._config.closures_table,
                e,
            )
            raise

    def _bootstrap_schema_contract(self, conn) -> None:
        inspector = inspect(conn)
        existing_tables = set(inspector.get_table_names())
        for table in self._tables:
            if table.name in existing_tables:
                continue
            conn.execute(CreateTable(table))
            logger.warning("market_data_table_created | table=%s", table.name)
            existing_tables.add(table.name)

        inspector = inspect(conn)
        for table in self._tables:
            self._assert_table_columns(inspector, table)

        inspector = inspect(conn)
        for table in self._tables:
            existing_indexes = {str(index.get("name") or "") for index in inspector.get_indexes(table.name)}
            for index in sorted(table.indexes, key=lambda item: str(item.name or "")):
                index_name = str(index.name or "")
                if index_name in existing_indexes:
                    continue
                conn.execute(CreateIndex(index))
                logger.info("market_data_index_created | table=%s index=%s", table.name, index_name)
                existing_indexes.add(index_name)

        inspector = inspect(conn)
        for table in self._tables:
            existing_indexes = {str(index.get("name") or "") for index in inspector.get_indexes(table.name)}
            required_indexes = {str(index.name or "") for index in table.indexes}
            missing = sorted(required_indexes - existing_indexes)
            if missing:
                raise RuntimeError(
                    f"Market data table '{table.name}' is missing required indexes: {', '.join(missing)}."
                )

    @staticmethod
    def _assert_table_columns(inspector, table: Table) -> None:
        expected = {column.name for column in table.columns}
        existing = {column["name"] for column in inspector.get_columns(table.name)}
        missing = sorted(expected - existing)
        if not missing:
            return
        raise RuntimeError(
            f"Market data table '{table.name}' is missing columns: {', '.join(missing)}. "
            "Rebuild the database or run an explicit out-of-band migration."
        )

    def fetch_ohlcv(self, ctx: DataContext, datasource: str) -> pd.DataFrame:
        """Load OHLCV rows for the requested context."""

        if not self._engine:
            return pd.DataFrame()

        self.ensure_schema()

        try:
            instrument_id, timeframe_seconds = self._resolve_context(ctx)
            query = text(
                f"""
                SELECT candle_time AS timestamp, open, high, low, close, volume, trade_count
                FROM {self._config.candles_raw_table}
                WHERE instrument_id = :instrument_id
                  AND timeframe_seconds = :timeframe_seconds
                  AND candle_time BETWEEN :start AND :end
                ORDER BY candle_time
                """
            )
            return pd.read_sql(
                query,
                self._engine,
                params={
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "start": ctx.start,
                    "end": ctx.end,
                },
            )
        except SQLAlchemyError as exc:
            logger.exception("Database error during OHLCV query: %s", exc)
            raise

    def load_closure_ranges(
        self,
        ctx: DataContext,
        datasource: str,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        """Retrieve cached closure windows for the requested series."""

        if not self._engine:
            return []

        self.ensure_schema()

        instrument_id, timeframe_seconds = self._resolve_context(ctx)
        query = text(
            f"""
            SELECT start_ts, end_ts
            FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :request_start OR start_ts >= :request_end)
            ORDER BY start_ts
            """
        )

        try:
            with self._engine.begin() as conn:
                rows = conn.execute(
                    query,
                    {
                        "instrument_id": instrument_id,
                        "timeframe_seconds": timeframe_seconds,
                        "request_start": requested_start,
                        "request_end": requested_end,
                    },
                ).fetchall()
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to load closure ranges for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            raise

        closures: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        for row in rows:
            closures.append((pd.to_datetime(row[0], utc=True), pd.to_datetime(row[1], utc=True)))

        return closures

    def load_closure_evidence_ranges(
        self,
        ctx: DataContext,
        datasource: str,
        requested_start: pd.Timestamp,
        requested_end: pd.Timestamp,
    ) -> List[Mapping[str, Any]]:
        """Retrieve cached closure windows with provider-agnostic evidence metadata."""

        if not self._engine:
            return []

        self.ensure_schema()

        instrument_id, timeframe_seconds = self._resolve_context(ctx)
        query = text(
            f"""
            SELECT start_ts, end_ts, metadata
            FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :request_start OR start_ts >= :request_end)
            ORDER BY start_ts
            """
        )

        try:
            with self._engine.begin() as conn:
                rows = conn.execute(
                    query,
                    {
                        "instrument_id": instrument_id,
                        "timeframe_seconds": timeframe_seconds,
                        "request_start": requested_start,
                        "request_end": requested_end,
                    },
                ).fetchall()
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to load closure evidence ranges for %s [%s]: %s",
                ctx.symbol,
                ctx.interval,
                exc,
            )
            raise

        evidence: List[Mapping[str, Any]] = []
        for row in rows:
            metadata = row[2] if len(row) > 2 else {}
            evidence.append(
                {
                    "start": pd.to_datetime(row[0], utc=True),
                    "end": pd.to_datetime(row[1], utc=True),
                    "metadata": metadata if isinstance(metadata, Mapping) else {},
                }
            )
        return evidence

    def record_closure_range(
        self,
        ctx: DataContext,
        datasource: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        metadata: Mapping[str, Any] | None = None,
    ):
        """Persist a window indicating upstream returned no data."""

        if not self._engine or end <= start:
            return

        self.ensure_schema()

        instrument_id, timeframe_seconds = self._resolve_context(ctx)
        start_ts = pd.to_datetime(start, utc=True)
        end_ts = pd.to_datetime(end, utc=True)

        overlap_query = text(
            f"""
            SELECT start_ts, end_ts, metadata FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        delete_query = text(
            f"""
            DELETE FROM {self._config.closures_table}
            WHERE instrument_id = :instrument_id
              AND timeframe_seconds = :timeframe_seconds
              AND NOT (end_ts <= :start_ts OR start_ts >= :end_ts)
            """
        )

        insert_query = text(
            f"""
            INSERT INTO {self._config.closures_table}
                (instrument_id, timeframe_seconds, start_ts, end_ts, metadata)
            VALUES (:instrument_id, :timeframe_seconds, :start_ts, :end_ts, CAST(:metadata AS jsonb))
            ON CONFLICT (instrument_id, timeframe_seconds, start_ts, end_ts) DO NOTHING
            """
        )

        evidence_metadata = dict(metadata or {})
        params = {
            "instrument_id": instrument_id,
            "timeframe_seconds": timeframe_seconds,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "metadata": json.dumps(evidence_metadata, sort_keys=True),
        }

        try:
            with self._engine.begin() as conn:
                rows = conn.execute(overlap_query, params).fetchall()
                if rows:
                    start_ts = min(start_ts, *(pd.to_datetime(row[0], utc=True) for row in rows))
                    end_ts = max(end_ts, *(pd.to_datetime(row[1], utc=True) for row in rows))
                    merged_sources = [
                        row[2]
                        for row in rows
                        if len(row) > 2 and isinstance(row[2], Mapping) and row[2]
                    ]
                    if merged_sources:
                        evidence_metadata = {
                            **evidence_metadata,
                            "merged_closure_evidence": merged_sources[:8],
                        }
                    conn.execute(
                        delete_query,
                        {
                            **params,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                        },
                    )

                conn.execute(
                    insert_query,
                    {
                        **params,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "metadata": json.dumps(evidence_metadata, sort_keys=True),
                    },
                )

                logger.info(
                    "Recorded scheduled closure for %s [%s]: %s -> %s",
                    instrument_id,
                    timeframe_seconds,
                    start_ts.isoformat(),
                    end_ts.isoformat(),
                )
        except SQLAlchemyError as exc:
            logger.exception(
                "Failed to record closure for %s [%s]: %s",
                instrument_id,
                timeframe_seconds,
                exc,
            )
            raise

    def write_dataframe(self, df: pd.DataFrame, ctx: DataContext) -> int:
        """Write a prepared OHLCV dataframe into the persistence layer."""

        if df.empty:
            return 0

        if not self._engine:
            logger.warning(
                "Database engine unavailable; skipping ingestion for %s [%s].",
                ctx.symbol,
                ctx.interval,
            )
            return 0

        self.ensure_schema()

        try:
            instrument_id, timeframe_seconds = self._resolve_context(ctx)
            if "timestamp" not in df.columns:
                raise ValueError("Input dataframe missing 'timestamp' column for candle_time mapping.")
            candle_time = pd.to_datetime(df["timestamp"], utc=True)
            close_time = candle_time + interval_to_timedelta(ctx.interval)
            prepared = pd.DataFrame(
                {
                    "instrument_id": instrument_id,
                    "timeframe_seconds": timeframe_seconds,
                    "candle_time": candle_time,
                    "close_time": close_time,
                    "open": df.get("open"),
                    "high": df.get("high"),
                    "low": df.get("low"),
                    "close": df.get("close"),
                    "volume": df.get("volume"),
                    "trade_count": df.get("trade_count"),
                    "is_closed": True,
                    "source_time": df.get("source_time"),
                }
            )
            with self._engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            f"CREATE TEMP TABLE tmp (LIKE {self._config.candles_raw_table} INCLUDING DEFAULTS) ON COMMIT DROP;"
                        )
                    )
                    try:
                        prepared.to_sql("tmp", conn, if_exists="append", index=False, method="multi")
                    except Exception as exc:
                        logger.exception("Failed to write to temp table 'tmp': %s", exc)
                        raise

                    conn.execute(
                        text(
                            f"INSERT INTO {self._config.candles_raw_table} "
                            f"SELECT * FROM tmp ON CONFLICT DO NOTHING;"
                        )
                    )

            logger.info(
                "Ingested %d rows for %s [%s].",
                len(prepared),
                instrument_id,
                timeframe_seconds,
            )
            logger.debug(
                "candle_ingest_range | instrument_id=%s timeframe_seconds=%s time_min=%s time_max=%s",
                instrument_id,
                timeframe_seconds,
                candle_time.min().isoformat(),
                candle_time.max().isoformat(),
            )
            return len(prepared)

        except SQLAlchemyError as exc:
            logger.exception("DB error during ingest for %s: %s", ctx.symbol, exc)
            raise

    def _resolve_context(self, ctx: DataContext) -> Tuple[str, int]:
        if not ctx.instrument_id:
            raise ValueError("instrument_id is required for candle persistence operations.")
        timeframe = interval_to_timedelta(ctx.interval)
        timeframe_seconds = int(timeframe.total_seconds())
        if timeframe_seconds <= 0:
            raise ValueError(f"Invalid timeframe interval: {ctx.interval}")
        return ctx.instrument_id, timeframe_seconds


__all__ = ["DataPersistenceService"]

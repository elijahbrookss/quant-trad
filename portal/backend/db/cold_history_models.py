"""PostgreSQL catalog for immutable canonical history and HDD lookup relations."""

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, Date, DateTime, ForeignKey, Index, Integer,
    LargeBinary, String, Text, UniqueConstraint, text,
)
from sqlalchemy.dialects.postgresql import JSONB

from .models import Base

COLD_LOCATOR_TABLE_NAMES = (
    "cold_fact_locators", "cold_fact_materials", "cold_fact_normalization_specs",
    "cold_book_session_facts",
)
COLD_CUSTODY_TABLE_NAMES = (
    "fact_archive_dependencies", "fact_archive_preparation_dependencies",
)
COLD_TABLE_NAMES = ("cold_fact_pages", "cold_fact_page_series", "cold_fact_retired_days",
                    "cold_fact_pruned_days", *COLD_LOCATOR_TABLE_NAMES)
COLD_HISTORY_LAYOUT_VERSION = "market.canonical_cold_history.v1"
COLD_IMMUTABILITY_TRIGGER = "trg_cold_fact_immutable"


def assert_cold_history_immutability(conn):
    guarded = set(conn.execute(text("""
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_trigger t ON t.tgrelid=c.oid
        WHERE n.nspname='market' AND c.relname=ANY(CAST(:names AS text[]))
          AND t.tgname=:trigger AND t.tgenabled='A' AND t.tgtype=58 AND t.tgqual IS NULL
          AND t.tgfoid=to_regprocedure('market.reject_immutable_mutation()')
    """), {"names": list(COLD_TABLE_NAMES), "trigger": COLD_IMMUTABILITY_TRIGGER}).scalars())
    if guarded != set(COLD_TABLE_NAMES):
        raise RuntimeError("cold_fact_catalog_immutability_required")


def _assert_hdd_placement(conn, names, error):
    """Require durable history placement for the heap, TOAST, and every index."""
    actual = conn.execute(text("""
        WITH tables AS (
            SELECT c.oid, c.relname, c.reltoastrelid
            FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace
            WHERE n.nspname='market' AND c.relname=ANY(CAST(:names AS text[])) AND c.relkind='r'
        ), members AS (
            SELECT oid, relname FROM tables
            UNION ALL SELECT reltoastrelid, relname FROM tables WHERE reltoastrelid<>0
            UNION ALL SELECT x.indexrelid, tables.relname FROM tables
                JOIN pg_index x ON x.indrelid IN (tables.oid, tables.reltoastrelid)
        )
        SELECT members.relname, c.relname AS member,
               COALESCE(NULLIF(c.reltablespace,0), db.dattablespace) AS tablespace
        FROM members JOIN pg_class AS c USING(oid)
        CROSS JOIN pg_database AS db WHERE db.datname=current_database()
    """), {"names": list(names)}).mappings().all()
    registered = set(conn.execute(text("""
        SELECT t.tablespace_oid
        FROM public.portal_storage_header_tablespaces t
        JOIN public.portal_storage_targets s ON s.id=t.target_id
        WHERE s.medium='hdd' AND s.state='active'
          AND t.database_identity=(
              SELECT control.system_identifier::text || '/' || db.oid::text
              FROM pg_control_system() AS control CROSS JOIN pg_database AS db
              WHERE db.datname=current_database()
          )
    """)).scalars())
    if (
        {row["relname"] for row in actual} != set(names)
        or not registered or any(row["tablespace"] not in registered for row in actual)
    ):
        raise RuntimeError(error)


def assert_cold_history_placement(conn):
    _assert_hdd_placement(conn, COLD_LOCATOR_TABLE_NAMES, "cold_fact_locator_history_tablespace_required")


def assert_cold_custody_catalog_placement(conn):
    _assert_hdd_placement(conn, COLD_CUSTODY_TABLE_NAMES, "cold_fact_custody_history_tablespace_required")


class ColdFactPageRecord(Base):
    __tablename__ = "cold_fact_pages"
    __table_args__ = (
        CheckConstraint("row_count > 0 AND byte_count > 0", name="ck_cold_page_size"),
        CheckConstraint("first_commit_seq > 0 AND last_commit_seq >= first_commit_seq", name="ck_cold_page_commit"),
        CheckConstraint(
            "length(object_sha256) = 64 AND length(manifest_hash) = 64",
            name="ck_cold_page_hash_length",
        ),
        Index("ix_cold_fact_pages_staged_day", "staged_storage_day",
              postgresql_where=text("staged_storage_day IS NOT NULL")),
        {"schema": "market"},
    )

    id = Column(String(128), primary_key=True)
    object_key = Column(Text, unique=True, nullable=False)
    object_sha256 = Column(String(64), nullable=False)
    manifest_hash = Column(String(64), nullable=False)
    row_count = Column(Integer, nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    first_commit_seq = Column(BigInteger, nullable=False)
    last_commit_seq = Column(BigInteger, nullable=False)
    descriptor = Column(JSONB, nullable=False)
    staged_storage_day = Column(Date, nullable=True)


class ColdFactRetiredDayRecord(Base):
    """Publish staged pages when their verified source is retired."""

    __tablename__ = "cold_fact_retired_days"
    __table_args__ = (
        CheckConstraint("handoff_kind IN ('initial', 'recurring')", name="ck_cold_retired_day_handoff_kind"),
        {"schema": "market"},
    )
    storage_day = Column(Date, primary_key=True)
    manifest_set_hash = Column(String(64), nullable=False)
    row_count = Column(BigInteger, nullable=False)
    reclaimed_header_bytes = Column(BigInteger, nullable=False)
    protected_dataset_ranges = Column(BigInteger, nullable=False)
    handoff_kind = Column(String(16), nullable=False, server_default=text("'recurring'"))


class ColdFactPrunedDayRecord(Base):
    """Record that the retired day's hot registry entries have been released."""

    __tablename__ = "cold_fact_pruned_days"
    __table_args__ = {"schema": "market"}
    storage_day = Column(ForeignKey("market.cold_fact_retired_days.storage_day", ondelete="RESTRICT"),
                         primary_key=True)


class ColdFactPageSeriesRecord(Base):
    __tablename__ = "cold_fact_page_series"
    __table_args__ = (
        CheckConstraint(
            "first_observation_at <= last_observation_at",
            name="ck_cold_page_series_observation",
        ),
        CheckConstraint(
            "(last_active_accepted_at IS NULL) = (last_active_observation_at IS NULL)",
            name="ck_cold_page_series_active_bounds",
        ),
        Index(
            "ix_cold_page_series_observation", "series_id",
            "first_observation_at", "last_observation_at",
        ),
        Index("ix_cold_page_series_acceptance", "series_id", "last_active_accepted_at"),
        {"schema": "market"},
    )

    page_id = Column(
        ForeignKey("market.cold_fact_pages.id", ondelete="RESTRICT"), primary_key=True,
    )
    series_id = Column(
        ForeignKey("market.series.id", ondelete="RESTRICT"), primary_key=True,
    )
    first_observation_at = Column(DateTime(timezone=True), nullable=False)
    last_observation_at = Column(DateTime(timezone=True), nullable=False)
    last_commit_seq = Column(BigInteger, nullable=False)
    last_active_accepted_at = Column(DateTime(timezone=True))
    last_active_observation_at = Column(DateTime(timezone=True))


class ColdFactLocatorRecord(Base):
    """Rebuildable exact lookup, installed with every file/index on the history device."""

    __tablename__ = "cold_fact_locators"
    __table_args__ = (
        CheckConstraint("ordinal >= 0 AND revision > 0", name="ck_cold_locator_position"),
        CheckConstraint("octet_length(key_digest) = 32", name="ck_cold_locator_key_digest"),
        CheckConstraint("octet_length(row_hash) = 32", name="ck_cold_locator_row_hash"),
        UniqueConstraint("page_id", "ordinal", name="uq_cold_locator_position"),
        Index("ix_cold_locator_key", "series_id", "key_digest", "revision"),
        Index("ix_cold_locator_commit", "series_id", "market_commit_seq"),
        CheckConstraint("market_commit_seq > 0", name="ck_cold_locator_commit"),
        {"schema": "market"},
    )

    id = Column(String(64), primary_key=True)
    page_id = Column(ForeignKey("market.cold_fact_pages.id", ondelete="RESTRICT"), nullable=False)
    ordinal = Column(Integer, nullable=False)
    series_id = Column(BigInteger, nullable=False)
    key_digest = Column(LargeBinary, nullable=False)
    revision = Column(Integer, nullable=False)
    row_hash = Column(LargeBinary, nullable=False)
    market_commit_seq = Column(BigInteger, nullable=False)


class ColdBookSessionFactRecord(Base):
    """Exact replay membership in a retained L2 definition/session scope."""

    __tablename__ = "cold_book_session_facts"
    __table_args__ = (
        CheckConstraint("octet_length(session_digest) = 32", name="ck_cold_book_session_digest"),
        {"schema": "market"},
    )

    session_digest = Column(LargeBinary, primary_key=True)
    market_commit_seq = Column(BigInteger, primary_key=True)
    fact_id = Column(ForeignKey("market.cold_fact_locators.id", ondelete="RESTRICT"), primary_key=True)


class ColdFactMaterialRecord(Base):
    """Rebuildable canonical and legacy material lookup on the HDD tablespace."""

    __tablename__ = "cold_fact_materials"
    __table_args__ = (
        CheckConstraint(
            "octet_length(hash_digest) = 32 AND octet_length(evidence_key_digest) = 32",
            name="ck_cold_material_digests",
        ),
        {"schema": "market"},
    )

    series_id = Column(BigInteger, primary_key=True)
    hash_digest = Column(LargeBinary, primary_key=True)
    # Canonical and empty-key legacy claims have distinct scopes.
    canonical = Column(Boolean, primary_key=True)
    evidence_key_digest = Column(LargeBinary, primary_key=True)
    fact_id = Column(ForeignKey("market.cold_fact_locators.id", ondelete="RESTRICT"), primary_key=True)


class ColdNormalizationSpecReferenceRecord(Base):
    """Exact lookup for retained normalization evidence in any cold Fact."""

    __tablename__ = "cold_fact_normalization_specs"
    __table_args__ = (
        CheckConstraint("octet_length(spec_digest) = 32", name="ck_cold_normalization_spec_digest"),
        {"schema": "market"},
    )

    spec_digest = Column(LargeBinary, primary_key=True)
    fact_id = Column(ForeignKey("market.cold_fact_locators.id", ondelete="RESTRICT"), primary_key=True)

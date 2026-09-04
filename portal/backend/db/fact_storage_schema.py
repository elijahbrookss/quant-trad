"""Code-owned clean storage layout, partition admission, and startup checks.

This module never migrates an existing fact table. Historical data moves only
through the explicit operator cutover under scripts/db/.
"""

from __future__ import annotations

from datetime import date, timedelta
import logging
import re

from sqlalchemy import inspect, text

from .market_storage_models import MarketFactHotPayloadRecord
from .market_data_models import MarketFactVersionRecord

logger = logging.getLogger(__name__)

FACT_STORAGE_LAYOUT_VERSION = "market.fact_storage_tiers.v1"
FACT_STORAGE_TABLES = (
    "fact_hot_payloads", "fact_retention_partitions", "fact_archive_manifests",
    "fact_archive_series", "fact_archive_dependencies", "fact_archive_material_aliases",
    "fact_archive_verifications", "fact_storage_state",
)
FACT_STORAGE_IMMUTABLE_TABLES = (
    "fact_hot_payloads", "fact_archive_manifests", "fact_archive_series", "fact_archive_dependencies",
    "fact_archive_material_aliases",
    "fact_archive_verifications",
)
FACT_PAYLOAD_INDEXES = frozenset(index.name for index in MarketFactHotPayloadRecord.__table__.indexes)
FACT_STORAGE_CUTOVER = "scripts/db/manual_migration_fact_storage_tiers_v1.py"

HOT_PAYLOAD_VALIDATION_BODY = """
DECLARE
    revision_row market.fact_versions%ROWTYPE;
    partition_state text;
BEGIN
    SELECT * INTO revision_row FROM market.fact_versions WHERE id = NEW.id;
    IF NOT FOUND OR
       (NEW.storage_day, NEW.series_id, NEW.payload_schema_id, NEW.observation_time)
       IS DISTINCT FROM
       (revision_row.storage_day, revision_row.series_id, revision_row.payload_schema_id, revision_row.observation_time)
    THEN
        RAISE EXCEPTION 'fact_hot_payload_identity_mismatch: fact_version_id=%', NEW.id;
    END IF;
    SELECT state INTO partition_state FROM market.fact_retention_partitions
    WHERE storage_day = NEW.storage_day FOR SHARE;
    IF partition_state IS DISTINCT FROM 'open' THEN
        RAISE EXCEPTION 'fact_hot_partition_not_open: storage_day=% fact_version_id=%',
            NEW.storage_day, NEW.id;
    END IF;
    RETURN NEW;
END;
"""

FACT_PAYLOAD_REQUIRED_BODY = """
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM market.fact_hot_payloads
        WHERE storage_day = NEW.storage_day AND id = NEW.id
    ) THEN
        RAISE EXCEPTION 'fact_hot_payload_missing: fact_version_id=% storage_day=%', NEW.id, NEW.storage_day;
    END IF;
    RETURN NULL;
END;
"""

COLD_FACT_READ_BODY = """
BEGIN
    RAISE EXCEPTION 'canonical_fact_cold_read_required: fact_version_id=% field=% use the tier-aware repository',
        fact_id, field_name;
END;
"""

FACT_ROWS_VIEW_SELECT = """
    SELECT versions.*,
           CASE WHEN hot.id IS NULL THEN market.cold_fact_read_required(versions.id, 'payload')
                ELSE hot.payload END AS payload,
           CASE WHEN hot.id IS NULL THEN market.cold_fact_read_required(versions.id, 'provenance')
                ELSE hot.provenance END AS provenance,
           CASE WHEN hot.id IS NULL THEN market.cold_fact_read_required(versions.id, 'quality')
                ELSE hot.quality END AS quality
    FROM market.fact_versions AS versions
    LEFT JOIN market.fact_hot_payloads AS hot
      ON hot.storage_day = versions.storage_day AND hot.id = versions.id
"""


def _view_signature(sql: str) -> str:
    # PostgreSQL expands stars, adds harmless parentheses/literal text casts,
    # and omits optional AS. Do not erase joins, predicates, or expressions.
    sql = re.sub(r"(?<=')::text\b", "", sql)
    # varchar revision IDs are rendered with text coercions for equality and
    # the text-argument error function. These are the only column casts erased.
    sql = re.sub(r"\b((?:versions|hot)\.id)::text\b", r"\1", sql)
    sql = re.sub(r"\bAS\b", "", sql, flags=re.IGNORECASE)
    return re.sub(r"[\s();]", "", sql).lower()


def install_fact_storage_functions(conn) -> None:
    """Install clean-layout enforcement. Called by clean bootstrap or explicit cutover only."""
    conn.execute(text(
        "CREATE OR REPLACE FUNCTION market.assert_fact_hot_payload_valid() RETURNS trigger "
        "LANGUAGE plpgsql AS $qt$" + HOT_PAYLOAD_VALIDATION_BODY + "$qt$"
    ))
    conn.execute(text(
        "CREATE OR REPLACE FUNCTION market.require_fact_hot_payload() RETURNS trigger "
        "LANGUAGE plpgsql AS $qt$" + FACT_PAYLOAD_REQUIRED_BODY + "$qt$"
    ))
    conn.execute(text(
        "CREATE OR REPLACE FUNCTION market.cold_fact_read_required(fact_id text, field_name text) RETURNS jsonb "
        "LANGUAGE plpgsql AS $qt$" + COLD_FACT_READ_BODY + "$qt$"
    ))
    conn.execute(text(
        "CREATE TRIGGER trg_assert_fact_hot_payload_valid BEFORE INSERT ON market.fact_hot_payloads "
        "FOR EACH ROW EXECUTE FUNCTION market.assert_fact_hot_payload_valid()"
    ))
    conn.execute(text(
        "CREATE CONSTRAINT TRIGGER trg_require_fact_hot_payload AFTER INSERT ON market.fact_versions "
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION market.require_fact_hot_payload()"
    ))
    conn.execute(text(
        "ALTER TABLE market.fact_hot_payloads ENABLE ALWAYS TRIGGER trg_assert_fact_hot_payload_valid"
    ))
    conn.execute(text(
        "ALTER TABLE market.fact_versions ENABLE ALWAYS TRIGGER trg_require_fact_hot_payload"
    ))
    # This SQL projection preserves existing hot query fields. A consumer not
    # yet using the tier-aware repository must fail, never silently lose cold rows.
    conn.execute(text("CREATE VIEW market.fact_rows AS " + FACT_ROWS_VIEW_SELECT))


def fact_partition_name(storage_day: date) -> str:
    if type(storage_day) is not date:
        raise ValueError("fact_storage_partition_invalid: UTC date required")
    return "fact_hot_payloads_" + storage_day.strftime("%Y%m%d")


def ensure_fact_payload_partition(conn, storage_day: date) -> str:
    """Provision an empty, deterministic daily table once; never adopt unknown data."""
    name = fact_partition_name(storage_day)
    relation = "market." + name
    state = conn.execute(text(
        "SELECT state FROM market.fact_retention_partitions WHERE storage_day = :day"
    ), {"day": storage_day}).scalar_one_or_none()
    if state is not None:
        if state != "open":
            raise RuntimeError(f"fact_hot_partition_not_open: storage_day={storage_day} state={state}")
        return relation
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 0))"), {"name": relation})
    state = conn.execute(text(
        "SELECT state FROM market.fact_retention_partitions WHERE storage_day = :day"
    ), {"day": storage_day}).scalar_one_or_none()
    if state is not None:
        if state != "open":
            raise RuntimeError(f"fact_hot_partition_not_open: storage_day={storage_day} state={state}")
        return relation
    if conn.execute(text("SELECT to_regclass(:name)"), {"name": relation}).scalar_one_or_none() is not None:
        raise RuntimeError(f"fact_hot_partition_unregistered: relation={relation} manual inspection required")
    until = storage_day + timedelta(days=1)
    # All identifiers/date literals are generated from a validated datetime.date.
    # ATTACH uses a less restrictive parent lock than CREATE TABLE ... PARTITION OF.
    conn.exec_driver_sql(f'CREATE TABLE market."{name}" (LIKE market.fact_hot_payloads INCLUDING ALL)')
    conn.exec_driver_sql(
        f'ALTER TABLE market."{name}" ADD CONSTRAINT "{name}_day" '
        f"CHECK (storage_day >= DATE '{storage_day.isoformat()}' AND storage_day < DATE '{until.isoformat()}')"
    )
    conn.exec_driver_sql(
        f'ALTER TABLE market.fact_hot_payloads ATTACH PARTITION market."{name}" '
        f"FOR VALUES FROM ('{storage_day.isoformat()}') TO ('{until.isoformat()}')"
    )
    conn.execute(text(
        "INSERT INTO market.fact_retention_partitions (storage_day, state) VALUES (:day, 'open')"
    ), {"day": storage_day})
    logger.warning("market_fact_partition_created | storage_day=%s relation=%s", storage_day, relation)
    return relation


def current_fact_storage_day(session) -> date:
    """Choose one database-owned placement day before a bounded ingestion batch."""
    day = session.execute(text("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")).scalar_one()
    ensure_fact_payload_partition(session.connection(), day)
    return day


def assert_fact_storage_contract(conn) -> None:
    """Refuse an old, partial, or incompatible layout without altering it."""
    inspector = inspect(conn)
    columns = {item["name"] for item in inspector.get_columns("fact_versions", schema="market")}
    if "storage_day" not in columns or columns & {"payload", "provenance", "quality"}:
        raise RuntimeError(
            "Canonical Fact storage layout requires an explicit cutover. Stop all writers and run "
            + FACT_STORAGE_CUTOVER
        )
    header_indexes = {item["name"] for item in inspector.get_indexes("fact_versions", schema="market")}
    if "ix_market_fact_storage_page" not in header_indexes:
        raise RuntimeError(f"Canonical storage page index is missing. Run {FACT_STORAGE_CUTOVER}")
    for name in FACT_STORAGE_TABLES:
        if conn.execute(text("SELECT to_regclass(:relation)"), {"relation": "market." + name}).scalar_one_or_none() is None:
            raise RuntimeError(f"Canonical Fact storage is missing market.{name}. Run {FACT_STORAGE_CUTOVER}")
    ready = conn.execute(text(
        "SELECT state FROM market.fact_storage_state WHERE layout_version = :version"
    ), {"version": FACT_STORAGE_LAYOUT_VERSION}).scalar_one_or_none()
    if ready != "ready":
        raise RuntimeError(f"Canonical Fact storage cutover is not ready: state={ready!r}. Run {FACT_STORAGE_CUTOVER}")
    kind, key = conn.execute(text(
        "SELECT relkind, pg_get_partkeydef(oid) FROM pg_class WHERE oid = 'market.fact_hot_payloads'::regclass"
    )).one()
    if kind != "p" or key != "RANGE (storage_day)":
        raise RuntimeError(f"Canonical hot payloads must use daily range partitions. Run {FACT_STORAGE_CUTOVER}")
    pk = inspector.get_pk_constraint("fact_hot_payloads", schema="market")
    if tuple(pk.get("constrained_columns") or ()) != ("storage_day", "id"):
        raise RuntimeError("Canonical hot payload primary key differs from (storage_day, id)")
    indexes = {item["name"] for item in inspector.get_indexes("fact_hot_payloads", schema="market")}
    if FACT_PAYLOAD_INDEXES - indexes:
        raise RuntimeError(f"Canonical hot payload indexes missing: {sorted(FACT_PAYLOAD_INDEXES - indexes)}")
    if conn.execute(text("SELECT to_regclass('market.fact_rows')")).scalar_one_or_none() is None:
        raise RuntimeError(f"Canonical hot-row projection is missing. Run {FACT_STORAGE_CUTOVER}")
    actual_view = conn.execute(text("SELECT pg_get_viewdef('market.fact_rows'::regclass, true)")).scalar_one()
    expected_view = FACT_ROWS_VIEW_SELECT.replace(
        "versions.*", ", ".join("versions." + column.name for column in MarketFactVersionRecord.__table__.columns)
    )
    if _view_signature(actual_view) != _view_signature(expected_view):
        raise RuntimeError(f"Canonical hot-row projection differs. Run {FACT_STORAGE_CUTOVER}")
    for name, body in (
        ("assert_fact_hot_payload_valid", HOT_PAYLOAD_VALIDATION_BODY),
        ("require_fact_hot_payload", FACT_PAYLOAD_REQUIRED_BODY),
        ("cold_fact_read_required", COLD_FACT_READ_BODY),
    ):
        stored = conn.execute(text(
            "SELECT p.prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'market' AND p.proname = :name"
        ), {"name": name}).scalars().all()
        if stored != [body]:
            raise RuntimeError(f"Canonical Fact storage function differs: market.{name}. Run {FACT_STORAGE_CUTOVER}")

    for table, trigger, function, kind, deferred in (
        ("fact_hot_payloads", "trg_assert_fact_hot_payload_valid", "assert_fact_hot_payload_valid", 7, False),
        ("fact_versions", "trg_require_fact_hot_payload", "require_fact_hot_payload", 5, True),
    ):
        actual = conn.execute(text(
            "SELECT t.tgtype, t.tgenabled, t.tgdeferrable, t.tginitdeferred, n.nspname, p.proname "
            "FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE t.tgrelid = to_regclass(:relation) AND t.tgname = :trigger"
        ), {"relation": "market." + table, "trigger": trigger}).one_or_none()
        if actual is None or tuple(actual) != (kind, "A", deferred, deferred, "market", function):
            raise RuntimeError(f"Canonical Fact storage enforcement differs: {table}.{trigger}. Run {FACT_STORAGE_CUTOVER}")

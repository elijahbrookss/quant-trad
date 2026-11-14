"""Exercise the exact cold candidate parity gate against native disposable SQL."""
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import JSON, bindparam, text

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import publish_canonical_fact_archive
from portal.backend.db.cold_history_models import COLD_IMMUTABILITY_TRIGGER
from portal.backend.db.fact_header_legacy_schema import install_fact_header_legacy_functions
from portal.backend.db.fact_identity_schema import fact_header_partition_name
from scripts.db.canonical_cold_audit import audit_candidate
from scripts.db.canonical_cold_load import load_checkpoint
from scripts.db.canonical_cold_migration import MigrationCheckpoint, PostgresMigrationSource, stage
from tests.test_market_data.test_canonical_cold_migration import DAY, LIMITS
from tests.test_market_data.test_canonical_cold_migration_db import _prepare, prepared_database  # noqa: F401
from tests.test_market_data.test_canonical_fact_archive import _row


pytestmark = pytest.mark.db
DAILY = DAY + timedelta(days=1)
CUTOFF = DAILY + timedelta(days=1)


@pytest.fixture
def candidate(prepared_database, tmp_path):  # noqa: F811
    engine, tablespace, history_root = prepared_database
    legacy = [_row(), _row(2)]
    later = [_row(3, state="invalidated"), _row(series_id=2, commit=4)]
    original, target_store = [FilesystemRawArchiveObjectStore(tmp_path / name) for name in ("source", "stage")]
    archived = publish_canonical_fact_archive(legacy, object_store=original,
                                              temporary_directory=tmp_path, limits=LIMITS)
    target = _prepare(engine, legacy, archived, tablespace, history_root)
    daily_name = fact_header_partition_name(DAILY)
    retained_name = fact_header_partition_name(CUTOFF)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE market.fact_versions RENAME TO fact_versions_legacy"))
        conn.execute(text("CREATE TABLE market.fact_versions (LIKE market.fact_versions_legacy) PARTITION BY RANGE (storage_day)"))
        conn.execute(text(f"ALTER TABLE market.fact_versions ATTACH PARTITION market.fact_versions_legacy "
                          f"FOR VALUES FROM (MINVALUE) TO ('{DAILY.isoformat()}')"))
        for name, day in ((daily_name, DAILY), (retained_name, CUTOFF)):
            high = day + timedelta(days=1)
            conn.execute(text(f"CREATE TABLE market.{name} PARTITION OF market.fact_versions "
                              f"FOR VALUES FROM ('{day.isoformat()}') TO ('{high.isoformat()}')"))
        conn.execute(text("CREATE TABLE market.fact_header_legacy (id integer PRIMARY KEY, end_day date, relation_oid bigint)"))
        conn.execute(text("INSERT INTO market.fact_header_legacy VALUES (1,:day,'market.fact_versions_legacy'::regclass::oid::bigint)"),
                     {"day": DAILY})
        conn.execute(text("CREATE TABLE market.fact_header_partitions (storage_day date PRIMARY KEY)"))
        conn.execute(text("INSERT INTO market.fact_header_partitions VALUES (:daily),(:retained)"), {"daily": DAILY, "retained": CUTOFF})
        install_fact_header_legacy_functions(conn)
        conn.execute(text("""INSERT INTO market.fact_retention_partitions (storage_day,state,sealed_at,expected_rows)
            SELECT :day,'sealed',sealed_at+INTERVAL '1 day',2 FROM market.fact_retention_partitions WHERE storage_day=:previous"""),
            {"day": DAILY, "previous": DAY})
        fields = [row[0] for row in conn.execute(text("""SELECT attname FROM pg_attribute
            WHERE attrelid='market.fact_versions'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum"""))]
        columns = ", ".join(fields)
        params = ", ".join(":" + field for field in fields)
        conn.execute(text(f"INSERT INTO market.fact_versions ({columns}) VALUES ({params})"),
                     [row | {"storage_day": DAILY} for row in later])
        conn.execute(text("INSERT INTO market.fact_hot_payloads VALUES (:id,:day,:payload,:provenance,:quality)")
            .bindparams(*(bindparam(name, type_=JSON) for name in ("payload", "provenance", "quality"))),
            [row | {"day": DAILY} for row in later])
        # Store the exact identity registry shape, with global duplicate guards.
        conn.execute(text("""CREATE TABLE market.fact_identities (id text PRIMARY KEY,storage_day date,
            series_id bigint,observation_key text,revision integer,UNIQUE(series_id,observation_key,revision))"""))
        conn.execute(text("INSERT INTO market.fact_identities SELECT id,storage_day,series_id,observation_key,revision FROM market.fact_versions"))
    checkpoint = MigrationCheckpoint(tmp_path / "audit-checkpoint.db")
    real_source = PostgresMigrationSource(engine, expected_system_identifier=target.system_identifier)
    kwargs = {"source_store": original, "destination_store": target_store,
              "temporary_directory": tmp_path, "limits": LIMITS}
    for day in (DAY, DAILY):
        assert stage(real_source, checkpoint, day=day, **kwargs)["cohort_complete"]
        assert load_checkpoint(engine, checkpoint, target=target, object_store=target_store, day=day)["checkpoint_end_reached"]
    return engine, checkpoint, {"target": target, "object_store": target_store, "cutoff": CUTOFF}


def test_exact_source_prefix_and_inverse_inventory(candidate):
    engine, checkpoint, kwargs = candidate
    plan = audit_candidate(engine, checkpoint, **kwargs)
    assert plan["prefix"]["first_day"] == DAY.isoformat()
    assert plan["prefix"]["legacy_end_day"] == DAILY.isoformat()
    assert [item["child"] for item in plan["prefix"]["days"]] == ["fact_versions_legacy", fact_header_partition_name(DAILY)]
    assert not plan["activation_ready"]
    for day in (DAY, DAILY):
        day_receipt = audit_candidate(engine, checkpoint, day=day, **kwargs)
        assert day_receipt["prefix_hash"] == plan["prefix_hash"]
        assert day_receipt["day_batch"]["verified_rows"] == 2
        assert day_receipt["day_batch"]["day_scan_end_reached"]
    inventory = audit_candidate(engine, checkpoint, inventory=True, **kwargs)["inverse_inventory_batch"]
    assert not inventory["inventory_scan_end_reached"]
    final = audit_candidate(engine, checkpoint, inventory=True, after_manifest=inventory["last_verified_manifest"],
                            **kwargs)["inverse_inventory_batch"]
    assert final["inventory_scan_end_reached"] and final["verified_pages"] == 1
    with pytest.raises(RuntimeError, match="prefix_splits_legacy_relation"):
        audit_candidate(engine, checkpoint, **(kwargs | {"cutoff": DAY}))
    with pytest.raises(RuntimeError, match="wrong_database"):
        audit_candidate(engine, checkpoint, **(kwargs | {"target": replace(kwargs["target"], oid=99)}))


def test_source_hash_and_head_mismatches_block_audit(candidate):
    engine, checkpoint, kwargs = candidate
    with engine.begin() as conn:
        conn.execute(text("UPDATE market.fact_versions SET row_hash=:hash WHERE storage_day=:day AND revision=3"),
                     {"day": DAILY, "hash": "b" * 64})
    with pytest.raises(RuntimeError, match="source_identity_mismatch"):
        audit_candidate(engine, checkpoint, day=DAILY, **kwargs)
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO market.fact_versions (id,storage_day,series_id,observation_key,revision,market_commit_seq,row_hash)
            VALUES ('future-row',:retained,1,'unrelated',1,1,:hash)"""), {"retained": CUTOFF, "hash": "a" * 64})
    with pytest.raises(RuntimeError, match="retained_series_commit_overlap"):
        audit_candidate(engine, checkpoint, day=DAY, **kwargs)


def test_drift_in_loaded_locator_and_out_of_order_revision_are_detected(candidate):
    engine, checkpoint, kwargs = candidate
    first = _row()
    second = _row(2)
    with engine.begin() as conn:
        # Deliberately fault the test-only physical copy below the production
        # always-enabled immutable table guard.
        conn.execute(text("ALTER TABLE market.cold_fact_locators DISABLE TRIGGER USER"))
        conn.execute(text("UPDATE market.cold_fact_locators SET row_hash=:hash WHERE id=:id"),
                     {"id": first["id"], "hash": bytes.fromhex("b" * 64)})
        conn.execute(text(f"ALTER TABLE market.cold_fact_locators ENABLE ALWAYS TRIGGER {COLD_IMMUTABILITY_TRIGGER}"))
    with pytest.raises(RuntimeError, match="loaded_locator_mismatch"):
        audit_candidate(engine, checkpoint, day=DAY, **kwargs)
    with engine.begin() as conn:
        conn.execute(text("""INSERT INTO market.fact_versions (id,storage_day,series_id,observation_key,revision,market_commit_seq,row_hash)
            VALUES ('future-row',:retained,1,:key,1,20,:hash)"""),
            {"retained": CUTOFF, "key": second["observation_key"], "hash": "a" * 64})
        # Let the retained revision conflict with a retiring revision of the
        # same observation key while preserving global uniqueness.
        conn.execute(text("UPDATE market.fact_identities SET revision=4 WHERE storage_day=:day AND revision=1"),
                     {"day": DAY})
        conn.execute(text("INSERT INTO market.fact_identities VALUES ('future-row',:retained,1,:key,1)"),
                     {"retained": CUTOFF, "key": second["observation_key"]})
    with pytest.raises(RuntimeError, match="retained_hot_revision_overlap"):
        audit_candidate(engine, checkpoint, day=DAILY, **kwargs)


def test_missing_day_checkpoint_and_unplanned_loaded_page_are_detected(candidate):
    engine, checkpoint, kwargs = candidate
    # A journal may be copied with one approved day omitted. Exercise the
    # inverse catalog check and then demand the absent nonempty source day.
    with checkpoint.connection() as db:
        db.execute("DELETE FROM pages WHERE day=?", (DAILY.isoformat(),))
        db.execute("DELETE FROM cohorts WHERE day=?", (DAILY.isoformat(),))
    with pytest.raises(RuntimeError, match="checkpoint_missing_for_nonempty_day"):
        audit_candidate(engine, checkpoint, day=DAILY, **kwargs)
    with pytest.raises(RuntimeError, match="unplanned_loaded_manifest"):
        audit_candidate(engine, checkpoint, inventory=True, max_pages=10, **kwargs)

from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.fact_archive import FactArchiveLimits, publish_canonical_fact_archive, read_canonical_fact_archive
from scripts.db.canonical_cold_acceptance import (
    Disposition, ParityCase, acquisition_disposition, coinbase_gap_request_budget, verify_parity,
)
from scripts.db.canonical_cold_migration import (
    Cohort, DOCS, MigrationCheckpoint, PostgresMigrationSource, SourcePage, stage,
)
from tests.test_market_data.test_canonical_fact_archive import _row


DAY = date(2026, 8, 9)
LIMITS = FactArchiveLimits(max_rows=2, row_group_size=1)


class DrySource:
    def __init__(self, rows, manifest, state="sealed"):
        self.source_rows = rows
        self.manifest = manifest
        self.source_cohort = Cohort(DAY, "2026-08-10T00:00:00+00:00", len(rows), int(bool(manifest)), state)

    def snapshot(self):
        return nullcontext()

    def identity(self, conn):
        return {"system_identifier": "fixture", "database": "test", "database_oid": 42, "header_oid": 99}

    def cohort(self, conn, day):
        assert day == DAY
        return self.source_cohort

    def existing(self, conn, day, ordinal):
        assert ordinal == 0
        return SourcePage(self.manifest, {"kind": "legacy_catalog"})

    def rows(self, conn, day, after, *, through, limits):
        rows = []
        for row in self.source_rows:
            cursor = (row["market_commit_seq"], row["id"])
            if cursor <= after or (through is not None and cursor > through):
                continue
            stored = dict(row)
            if self.source_cohort.state == "reclaimed":
                stored.update(dict.fromkeys(DOCS))
            rows.append(stored)
        return rows[:limits.max_rows]


@pytest.fixture
def roots(tmp_path):
    original, destination = [FilesystemRawArchiveObjectStore(tmp_path / name) for name in ("source", "stage")]
    raw_rows = [_row(), _row(2), _row(3, state="invalidated"), _row(series_id=2, commit=4)]
    archived = publish_canonical_fact_archive(raw_rows[:2], object_store=original,
                                              temporary_directory=tmp_path, limits=LIMITS)
    checkpoint = MigrationCheckpoint(tmp_path / "progress.db")
    return raw_rows, archived, checkpoint, {"day": DAY, "source_store": original,
        "destination_store": destination, "temporary_directory": tmp_path, "limits": LIMITS}


def test_resume_reuses_archive_and_exports_remaining_exact_revisions(roots):
    rows, manifest, checkpoint, kwargs = roots
    source = DrySource(rows, manifest)
    first = stage(source, checkpoint, **kwargs)
    assert first["rows"] == 2 and first["pages"] == 1 and not first["cohort_complete"]
    assert list(checkpoint.iter_completed_pages()) == []

    # A new process can resume from the durable cursor and invisible page.
    resumed = MigrationCheckpoint(checkpoint.path)
    assert stage(source, resumed, **kwargs)["cohort_complete"]
    pages = list(resumed.iter_completed_pages(day=DAY))
    assert [page.reused for page in pages] == [True, False]
    assert pages[0].manifest == manifest
    assert pages[1].after == manifest.last_cursor
    destination = kwargs["destination_store"]
    assert [row for page in pages for row in read_canonical_fact_archive(
        destination.local_path(page.manifest.object_key), expected=page.manifest, limits=LIMITS)] == rows
    assert resumed.verify_completed_cohort(day=DAY, destination_store=destination, limits=LIMITS)["verified_rows"] == 4
    assert stage(source, resumed, **kwargs)["cohort_complete"]


def test_reclaimed_archive_is_bound_to_retained_header_and_requires_complete_coverage(roots):
    rows, manifest, checkpoint, kwargs = roots
    source = DrySource(rows[:2], manifest, state="reclaimed")
    assert stage(source, checkpoint, **kwargs)["cohort_complete"]
    assert next(checkpoint.iter_completed_pages()).manifest.manifest_hash == manifest.manifest_hash


def test_mismatch_holds_cursor_and_resume_checks_source_cohort(roots):
    rows, manifest, checkpoint, kwargs = roots
    rows[0] = rows[0] | {"provenance": {"wrong": True}}
    source = DrySource(rows, manifest)
    with pytest.raises(RuntimeError, match="hot_documents_changed"):
        stage(source, checkpoint, **kwargs)
    assert checkpoint.progress(DAY) == (0, 0, 0, 0, "")
    source.source_cohort = replace(source.source_cohort, sealed_at="elsewhere")
    with pytest.raises(RuntimeError, match="source_changed"):
        stage(source, checkpoint, **kwargs)


def test_unaccounted_header_does_not_complete(roots):
    rows, manifest, checkpoint, kwargs = roots
    source = DrySource(rows[:2], manifest)
    source.source_cohort = replace(source.source_cohort, expected_rows=3)
    with pytest.raises(RuntimeError, match="incomplete_coverage"):
        stage(source, checkpoint, max_pages=2, **kwargs)
    assert list(checkpoint.iter_completed_pages()) == []


def test_rehearsal_rechecks_current_target_bytes(roots):
    rows, manifest, checkpoint, kwargs = roots
    stage(DrySource(rows[:2], manifest), checkpoint, **kwargs)
    destination = kwargs["destination_store"]
    destination.local_path(manifest.object_key).unlink()
    with pytest.raises(FileNotFoundError):
        checkpoint.verify_completed_cohort(day=DAY, destination_store=destination, limits=LIMITS)


def test_source_transaction_is_explicitly_read_only():
    class Engine:
        def begin(self):
            return nullcontext(self)

        def execute(self, statement, params=None):
            statements.append(str(statement))

    statements = []
    with PostgresMigrationSource(Engine(), expected_system_identifier="fixture").snapshot():
        assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"


def test_reacquisition_never_replaces_accepted_or_pinned_evidence():
    eligible = dict(kind="candle.ohlcv", provider="COINBASE", source_kind="historical_api", timeframe_seconds=60,
                    gap_proven=True, accepted_evidence=False, pinned=False, irreproducible_lineage=False)
    assert acquisition_disposition(**eligible) == Disposition.REACQUIRE_GAP
    for fence in ("accepted_evidence", "pinned", "irreproducible_lineage"):
        assert acquisition_disposition(**(eligible | {fence: True})) == Disposition.COPY_EXACT
    assert acquisition_disposition(kind="market.l2_book", provider="COINBASE") == Disposition.COPY_EXACT
    assert acquisition_disposition(**(eligible | {"kind": "cold_lookup_index", "verified_rebuild_inputs": True})) == Disposition.REBUILD_PROJECTION
    assert acquisition_disposition(kind="typed_candle_projection") == Disposition.COPY_EXACT


def test_reacquisition_budget_uses_half_open_provider_pages():
    start = datetime(2021, 1, 1, tzinfo=UTC)
    end = start + timedelta(minutes=300)
    result = coinbase_gap_request_budget(windows=[("BTC-USD", start, end), ("ETH-USD", start, end)],
                                         timeframe_seconds=60, measured_requests_per_second=2)
    assert result == {"expected_periods": 600, "minimum_requests": 4, "request_seconds_at_measured_rate": 2}
    with pytest.raises(ValueError, match="unaligned_window"):
        coinbase_gap_request_budget(windows=[("BTC-USD", start, end + timedelta(seconds=1))], timeframe_seconds=60)


def test_parity_detects_missing_raw_readback_and_timing_difference():
    fact = _row()
    raw = {"session": "book", "sequence": 9, "ordinal": 3, "sha256": "a" * 64}
    facts = ParityCase("as_of", "before-correction", "capture-v1", "capture-v1", [fact], [fact])
    ordinals = ParityCase("raw_ordinals", "exact-session", "capture-v1", "capture-v1", [raw], [raw])
    receipts = verify_parity([facts, ordinals], required_domains={"as_of", "raw_ordinals"})
    assert [item["rows"] for item in receipts] == [1, 1]
    with pytest.raises(RuntimeError, match="missing_domain_witnesses"):
        verify_parity([facts], required_domains={"as_of", "raw_ordinals"})
    changed = [dict(fact, known_at=fact["known_at"] + timedelta(microseconds=1))]
    with pytest.raises(RuntimeError, match="content_mismatch"):
        verify_parity([replace(facts, target=changed)], required_domains={"as_of"})
    with pytest.raises(RuntimeError, match="coverage_mismatch"):
        verify_parity([replace(ordinals, target=[])], required_domains={"raw_ordinals"})

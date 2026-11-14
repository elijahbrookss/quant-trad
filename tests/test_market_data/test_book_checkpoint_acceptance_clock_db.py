"""A raw-backed checkpoint can precede platform acceptance of its exact source Fact."""

from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive_verification import ArchiveVerificationBatch, ArchiveVerificationLimits
from market_data.book_archive import publish_book_checkpoint
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_book_admission import verify_book_metadata_and_checkpoints
from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM
from tests.test_market_data.test_fact_raw_lineage_db import _publish_book_result, _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage  # noqa: F401


pytestmark = pytest.mark.db


def test_raw_checkpoint_before_canonical_source_acceptance_stages_and_verifies(storage, tmp_path, monkeypatch):
    fixture = _raw_book_fixture(storage, tmp_path, monkeypatch)
    original = fixture.results[0]
    checkpoint = original.checkpoints[0]
    encoded, acknowledgement = publish_book_checkpoint(checkpoint, object_store=fixture.store,
                                                      temporary_directory=tmp_path / "checkpoint-stage")
    assert fixture.structures.commit_book_checkpoint(
        fixture.claim, checkpoint=checkpoint, encoded=encoded, acknowledgement=acknowledgement,
        source_manifest_ids=[fixture.manifests[0]],
    )
    accepted_at = checkpoint.known_at + timedelta(minutes=15)
    snapshot = replace(original.snapshot, event=replace(original.snapshot.event,
                                                       accepted_at=accepted_at, known_at=accepted_at))
    fixture.results[0] = replace(original, snapshot=snapshot)
    _publish_book_result(fixture, 0)
    archive = PostgresCanonicalFactArchiveRepository(database=storage.database, object_store=fixture.store,
                                                     temporary_directory=tmp_path / "facts-stage")

    with storage.database.session() as session:
        row = dict(session.execute(text(f"""
            SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM}
            WHERE versions.storage_day=:day AND versions.series_id=:series
        """), {"day": fixture.day, "series": fixture.series_id}).mappings().one())
        assert row["known_at"] == accepted_at
        assert row["payload"]["after_state_hash"] == checkpoint.state_hash

    def verify(root, *, bound=None):
        with storage.database.session() as session:
            return verify_book_metadata_and_checkpoints(
                session, rows=[root], object_store=fixture.store,
                byte_verifier=ArchiveVerificationBatch(fixture.store, limits=ArchiveVerificationLimits()),
                max_objects=8, bound_checkpoint_ids=bound,
            )

    # A checkpoint and a matching Fact are separately available. Neither may
    # advance the root's causal evidence when its own ceiling is earlier.
    assert verify(row | {"known_at": checkpoint.known_at - timedelta(microseconds=1)}) == ([], [])
    with pytest.raises(RuntimeError, match="canonical_book_checkpoint_bound_coverage_missing"):
        verify(row | {"known_at": checkpoint.known_at - timedelta(microseconds=1)}, bound=[checkpoint.checkpoint_id])
    with pytest.raises(RuntimeError, match="canonical_book_checkpoint_source_state_missing"):
        verify(row | {"known_at": checkpoint.known_at})
    with pytest.raises(RuntimeError, match="canonical_book_checkpoint_source_state_missing"):
        verify(row | {"market_commit_seq": row["market_commit_seq"] - 1})

    archive.seal_partition(fixture.day)
    for _ in range(4):
        staged = archive.stage_next_page(fixture.day)
        if staged["status"] == "page_acknowledged":
            break
    else:
        pytest.fail("the raw-backed page did not stage within its fixture budget")
    assert archive.verify_next_page(fixture.day)["status"] == "page_verified"
    with storage.database.session() as session:
        assert session.execute(text("""
            SELECT target_kind,target_id FROM market.fact_archive_dependencies WHERE manifest_id=:id
            ORDER BY target_kind,target_id
        """), {"id": staged["manifest_id"]}).all() == [
            ("book_checkpoint", checkpoint.checkpoint_id), ("raw_manifest", fixture.manifests[0]),
        ]

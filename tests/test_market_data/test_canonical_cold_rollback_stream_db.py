"""Rollback stream evidence checks exact scoped SQL and native archive files."""

from datetime import timedelta

import pytest
from sqlalchemy import text

from market_data.archive_verification import ArchiveVerificationLimits
from market_data.book_archive import publish_book_checkpoint
from market_data.contracts import DatasetSeriesRequest
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_rollback_stream import capture_rollback_stream, verify_rollback_stream
from tests.test_market_data.test_canonical_cold_activate_db import _fence_connections
from tests.test_market_data.test_canonical_recovery_spool_db import (
    DEFINITION, _define_trade_stream, _native_recover, _seal_trade,
)
from tests.test_market_data.test_fact_raw_lineage_db import _publish_book_result, _raw_book_fixture
from tests.test_market_data.test_fact_storage_tiers_db import storage  # noqa: F401


pytestmark = pytest.mark.db


def test_rollback_stream_detects_changed_sql_and_raw_checkpoint_bytes(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    repo, claim, series = _define_trade_stream(storage, monkeypatch)
    _, record = _seal_trade(tmp_path, claim, trade_id="rollback-operational-trade")
    _native_recover(repo, tmp_path)
    storage.repo.freeze_dataset([DatasetSeriesRequest(
        series, record.received_at - timedelta(seconds=10), record.received_at + timedelta(seconds=10),
    )])

    book = _raw_book_fixture(storage, tmp_path, monkeypatch, definition_id="rollback-operation-book")
    for index in range(len(book.results)):
        _publish_book_result(book, index)
    checkpoint = book.results[0].checkpoints[0]
    encoded, acknowledgement = publish_book_checkpoint(
        checkpoint, object_store=book.store, temporary_directory=tmp_path / "checkpoints",
    )
    book.structures.commit_book_checkpoint(book.claim, checkpoint=checkpoint, encoded=encoded,
                                          acknowledgement=acknowledgement, source_manifest_ids=[book.manifests[0]])
    book.structures.release(book.claim)
    with storage.database._engine.connect() as conn:
        identity = dict(conn.execute(text("""
            SELECT control.system_identifier::text AS system_identifier,
                   current_database() AS database,d.oid::bigint AS database_oid
            FROM pg_control_system() control CROSS JOIN pg_database d WHERE d.datname=current_database()
        """)).mappings().one())
    with _fence_connections(storage) as (conn, control):
        fence = close_database_fence(conn, control, expected_database=identity, timeout_seconds=60,
                                      record=lambda *_: None)
        options = dict(fence=fence, store=book.store, max_rows=200,
                       object_limits=ArchiveVerificationLimits(max_objects=20, max_bytes=8*1024**2))
        try:
            with conn.begin():
                trade = capture_rollback_stream(conn, definition_id=DEFINITION, **options)
                captured_book = capture_rollback_stream(conn, definition_id=book.claim.definition_id, **options)
                assert trade["archive"]["objects"] == 1
                assert trade["relations"]["market.dataset_archive_refs"]["rows"] == 1
                assert trade["relations"]["market.raw_archive_record_mappings"]["rows"] == 1
                assert captured_book["archive"]["objects"] == len(book.manifests) + 1
                assert captured_book["relations"]["market.book_checkpoint_manifests"]["rows"] == 1
                assert captured_book["relations"]["market.book_reconstruction_state"]["rows"] == 1
                with pytest.raises(RuntimeError, match="row_budget_exceeded"):
                    capture_rollback_stream(conn, definition_id=DEFINITION, **(options | {"max_rows": 1}))
            with conn.begin():
                assert verify_rollback_stream(conn, evidence=trade, expected_hash=trade["receipt_sha256"],
                                               **options) == trade
                assert verify_rollback_stream(conn, evidence=captured_book,
                                               expected_hash=captured_book["receipt_sha256"], **options) == captured_book
                with conn.begin_nested() as changed:
                    conn.execute(text("""
                        UPDATE market.book_reconstruction_state SET receive_ordinal=receive_ordinal+1
                        WHERE series_id=:series
                    """), {"series": book.series_id})
                    with pytest.raises(RuntimeError, match="book_reconstruction_state"):
                        verify_rollback_stream(conn, evidence=captured_book,
                                               expected_hash=captured_book["receipt_sha256"], **options)
                    changed.rollback()
                path = book.store.local_path(acknowledgement.object_key)
                original = path.read_bytes()
                try:
                    path.write_bytes(b"x" * len(original))
                    with pytest.raises(RuntimeError, match="archive_verification_checksum_mismatch"):
                        verify_rollback_stream(conn, evidence=captured_book,
                                               expected_hash=captured_book["receipt_sha256"], **options)
                finally:
                    path.write_bytes(original)
                assert verify_rollback_stream(conn, evidence=captured_book,
                                               expected_hash=captured_book["receipt_sha256"], **options) == captured_book
        finally:
            reopen_database_fence(fence, control, expected_outcome="original",
                                    read_outcome=lambda _: "original", record=lambda *_: None)

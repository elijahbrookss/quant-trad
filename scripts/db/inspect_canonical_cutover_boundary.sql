\set ON_ERROR_STOP on
\pset pager off

-- Current serving schema only. Capture immediately before and after the
-- approved writer fence to tie its recovery receipt to the ingest boundary.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '8s';
SET LOCAL lock_timeout = '500ms';
SET LOCAL timezone = 'UTC';

SELECT transaction_timestamp() AS captured_at,
       current_setting('server_version_num') AS postgres_version,
       (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb') AS timescaledb_version,
       pg_current_wal_lsn() AS observed_wal_lsn,
       pg_current_wal_flush_lsn() AS flushed_wal_lsn,
       (SELECT COALESCE(MAX(latest.market_commit_seq), 0)
        FROM market.series AS series
        CROSS JOIN LATERAL (
            SELECT fact.market_commit_seq
            FROM market.fact_versions AS fact
            WHERE fact.series_id = series.id
            ORDER BY fact.market_commit_seq DESC
            LIMIT 1
        ) AS latest) AS visible_market_commit_seq;

SELECT policy.revision, policy.policy->'recent_days' AS recent_days,
       policy.policy->'reserve_percent' AS reserve_percent,
       policy.policy->'movement_enabled' AS movement_enabled,
       policy.policy->'backup_enabled' AS backup_enabled,
       archiver.archived_count, archiver.last_archived_time,
       archiver.failed_count, archiver.last_failed_time
FROM portal_storage_policy AS policy CROSS JOIN pg_stat_archiver AS archiver
WHERE policy.id = 1;

-- Definitions remain visible after a controlled stop. Capture all enabled
-- definitions plus definitions with a surviving lease.
SELECT definition.id AS definition_id, definition.series_id,
       definition.provider, definition.provider_product_id, definition.channels,
       definition.desired_state, definition.control_generation,
       definition.max_spool_bytes, definition.max_segment_bytes,
       lease.lease_generation, lease.heartbeat_at, lease.expires_at,
       event.id AS latest_event_id, event.session_id, event.connection_epoch,
       event.event_ordinal, event.event_type, event.occurred_at, event.evidence_hash,
       manifest.id AS latest_manifest_id, manifest.session_id AS manifest_session,
       manifest.connection_epoch AS manifest_epoch,
       manifest.last_receive_ordinal AS manifest_last_ordinal,
       manifest.last_received_at AS manifest_last_received_at,
       manifest.object_sha256 AS manifest_object_sha256
FROM market.stream_definitions AS definition
LEFT JOIN market.stream_lease_state AS lease ON lease.definition_id = definition.id
LEFT JOIN LATERAL (
    SELECT id, session_id, connection_epoch, event_ordinal, event_type, occurred_at, evidence_hash
    FROM market.stream_session_events
    WHERE definition_id = definition.id
    ORDER BY occurred_at DESC, id DESC
    LIMIT 1
) AS event ON TRUE
LEFT JOIN LATERAL (
    SELECT id, session_id, connection_epoch, last_receive_ordinal, last_received_at, object_sha256
    FROM market.raw_archive_manifests
    WHERE definition_id = definition.id
    ORDER BY first_received_at DESC, id DESC
    LIMIT 1
) AS manifest ON TRUE
WHERE definition.enabled OR lease.definition_id IS NOT NULL
ORDER BY definition.id;

-- A coverage interval and a book validity interval are independent pieces of
-- evidence. Both keep their actual acceptance history across the cutover.
SELECT definition.id AS definition_id, coverage.interval_id, coverage.revision,
       coverage.session_id, coverage.connection_epoch, coverage.status,
       coverage.ordering_assurance, coverage.archive_status,
       coverage.last_provider_sequence_num, coverage.last_raw_record_id,
       coverage.last_receive_ordinal, coverage.canonicalization_watermark_ordinal,
       coverage.archive_complete_through_ordinal, coverage.last_effective_at,
       coverage.closing_session_event_id, coverage.closing_effective_at,
       coverage.known_at
FROM market.stream_definitions AS definition
LEFT JOIN LATERAL (
    SELECT interval_id, revision, session_id, connection_epoch, status, ordering_assurance,
           archive_status, last_provider_sequence_num, last_raw_record_id, last_receive_ordinal,
           canonicalization_watermark_ordinal, archive_complete_through_ordinal,
           last_effective_at, closing_session_event_id, closing_effective_at, known_at
    FROM market.stream_coverage_interval_versions
    WHERE definition_id = definition.id
      AND provider_product_id = definition.provider_product_id
      AND channel = 'market_trades'
    ORDER BY opening_effective_at DESC, revision DESC, id DESC
    LIMIT 1
) AS coverage ON TRUE
WHERE definition.enabled AND definition.channels ? 'market_trades'
ORDER BY definition.id;

SELECT definition.id AS definition_id, book.interval_id, book.revision,
       book.status, book.opening_snapshot_id,
       book.opening_session_id, book.opening_connection_epoch,
       book.last_session_id, book.last_connection_epoch,
       book.last_sequence_num, book.last_receive_ordinal, book.last_event_ordinal,
       book.last_valid_effective_at, book.last_state_hash,
       book.closing_sequence_num, book.closing_effective_at, book.reason, book.known_at
FROM market.stream_definitions AS definition
LEFT JOIN LATERAL (
    SELECT interval_id, revision, status, opening_snapshot_id, opening_session_id,
           opening_connection_epoch, last_session_id, last_connection_epoch,
           last_sequence_num, last_receive_ordinal, last_event_ordinal,
           last_valid_effective_at, last_state_hash,
           closing_sequence_num, closing_effective_at, reason, known_at
    FROM market.book_validity_interval_versions
    WHERE series_id = definition.series_id
    ORDER BY opening_effective_at DESC, revision DESC, id DESC
    LIMIT 1
) AS book ON TRUE
WHERE definition.enabled AND definition.channels ? 'level2'
ORDER BY definition.id;

-- The current projection can advance well past the immutable interval's
-- opening position. Capture it with its checkpoint and current state hash.
SELECT definition.id AS definition_id, state.session_id, state.connection_epoch,
       state.lifecycle, state.validity_interval_id, state.checkpoint_id,
       state.provider_sequence_num, state.receive_ordinal, state.event_ordinal,
       state.state_hash, state.updated_at,
       checkpoint.receive_ordinal AS checkpoint_receive_ordinal,
       checkpoint.object_sha256 AS checkpoint_object_sha256
FROM market.stream_definitions AS definition
LEFT JOIN market.book_reconstruction_state AS state ON state.series_id = definition.series_id
LEFT JOIN market.book_checkpoint_manifests AS checkpoint ON checkpoint.id = state.checkpoint_id
WHERE definition.enabled AND definition.channels ? 'level2'
ORDER BY definition.id;

SELECT definition.id AS definition_id, source.provider, source.source_kind,
       series.fact_type, definition.series_id, definition.desired_state,
       definition.control_generation, definition.poll_interval_seconds,
       definition.next_scheduled_at, definition.last_attempt_at,
       definition.last_success_at, definition.lease_generation,
       definition.lease_expires_at, definition.consecutive_failures
FROM market.collection_definitions AS definition
JOIN market.series AS series ON series.id = definition.series_id
JOIN market.sources AS source ON source.id = definition.source_id
WHERE definition.enabled OR definition.lease_owner IS NOT NULL
ORDER BY source.provider, series.fact_type, definition.id;

COMMIT;

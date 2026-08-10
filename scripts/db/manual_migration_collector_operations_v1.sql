\set ON_ERROR_STOP on

-- Establish the provider-neutral collector control and immutable operation ledger.
--
-- Run with the backend and all market-data collector workers stopped:
--
--   make db-file file=scripts/db/manual_migration_collector_operations_v1.sql

BEGIN;
SELECT pg_advisory_xact_lock(9021064);

DO $$
BEGIN
    IF to_regclass('market.collection_definitions') IS NULL
       OR to_regclass('market.stream_definitions') IS NULL
       OR to_regclass('market.sources') IS NULL
       OR to_regclass('market.stream_lease_state') IS NULL THEN
        RAISE EXCEPTION
            'collector operations migration requires canonical collector relations';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM market.collection_definitions
        WHERE lease_owner IS NOT NULL
           OR lease_token_hash IS NOT NULL
           OR lease_expires_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'collector operations migration requires scheduled collector leases to be released';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM market.stream_lease_state
        WHERE expires_at > now()
    ) THEN
        RAISE EXCEPTION
            'collector operations migration requires continuous collector leases to expire';
    END IF;
END $$;

LOCK TABLE market.collection_definitions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE market.stream_definitions IN ACCESS EXCLUSIVE MODE;

ALTER TABLE market.collection_definitions
    ADD COLUMN IF NOT EXISTS desired_state varchar(16),
    ADD COLUMN IF NOT EXISTS control_generation bigint,
    ADD COLUMN IF NOT EXISTS control_requested_at timestamptz,
    ADD COLUMN IF NOT EXISTS control_requested_by varchar(128),
    ADD COLUMN IF NOT EXISTS control_request_id varchar(128);

ALTER TABLE market.stream_definitions
    ADD COLUMN IF NOT EXISTS desired_state varchar(16),
    ADD COLUMN IF NOT EXISTS control_generation bigint,
    ADD COLUMN IF NOT EXISTS control_requested_at timestamptz,
    ADD COLUMN IF NOT EXISTS control_requested_by varchar(128),
    ADD COLUMN IF NOT EXISTS control_request_id varchar(128);

-- Preserve only definitions that the checked-in runtime can actually execute.
-- Historical test definitions remain visible but are stopped and later project as
-- INVALID rather than becoming claimable when the worker restarts.
UPDATE market.collection_definitions AS definitions
SET desired_state = CASE
        WHEN definitions.enabled
         AND definitions.config->>'schema_version' = 'market_collection_definition.v1'
         AND sources.adapter_version IN (
             'coinbase_advanced_trade.open_interest.public_poll.v1',
             'coinbase_advanced_trade.funding_rate.public_poll.v1',
             'chainlink_mvr_bundle.v1'
         )
        THEN 'running'
        ELSE 'stopped'
    END,
    control_generation = COALESCE(definitions.control_generation, 0),
    control_requested_at = COALESCE(definitions.control_requested_at, now()),
    control_requested_by = COALESCE(
        definitions.control_requested_by,
        'migration:collector_operations_v1'
    ),
    control_request_id = COALESCE(
        definitions.control_request_id,
        'migration:collector_operations_v1:' || definitions.id
    )
FROM market.sources AS sources
WHERE sources.id = definitions.source_id
  AND (
      definitions.desired_state IS NULL
      OR definitions.control_generation IS NULL
      OR definitions.control_requested_at IS NULL
      OR definitions.control_requested_by IS NULL
      OR definitions.control_request_id IS NULL
  );

UPDATE market.stream_definitions AS definitions
SET desired_state = CASE
        WHEN definitions.enabled
         AND upper(definitions.provider) = 'COINBASE'
         AND sources.adapter_version = 'coinbase_advanced_trade.market_trades.v1'
         AND jsonb_typeof(definitions.channels) = 'array'
         AND definitions.channels @> '["market_trades", "heartbeats"]'::jsonb
         AND jsonb_array_length(definitions.channels) = 2
         AND jsonb_typeof(definitions.config->'aggregate_series_ids') = 'object'
         AND definitions.config#>>'{aggregate_series_ids,1}' IS NOT NULL
         AND definitions.config#>>'{aggregate_series_ids,60}' IS NOT NULL
         AND jsonb_typeof(definitions.config->'flow_feature_series_ids') = 'object'
         AND definitions.config#>>'{flow_feature_series_ids,1}' IS NOT NULL
         AND definitions.config#>>'{flow_feature_series_ids,60}' IS NOT NULL
         AND definitions.config->>'product_definition_version_id' IS NOT NULL
        THEN 'running'
        ELSE 'stopped'
    END,
    control_generation = COALESCE(definitions.control_generation, 0),
    control_requested_at = COALESCE(definitions.control_requested_at, now()),
    control_requested_by = COALESCE(
        definitions.control_requested_by,
        'migration:collector_operations_v1'
    ),
    control_request_id = COALESCE(
        definitions.control_request_id,
        'migration:collector_operations_v1:' || definitions.id
    ),
    config = CASE
        WHEN definitions.config ? 'collector_runtime'
        THEN (definitions.config - 'collector_runtime') || jsonb_build_object(
            'runtime_policy',
            COALESCE(definitions.config#>'{collector_runtime,policy}', '{}'::jsonb)
        )
        ELSE definitions.config
    END
FROM market.sources AS sources
WHERE sources.id = definitions.source_id
  AND (
      definitions.desired_state IS NULL
      OR definitions.control_generation IS NULL
      OR definitions.control_requested_at IS NULL
      OR definitions.control_requested_by IS NULL
      OR definitions.control_request_id IS NULL
      OR definitions.config ? 'collector_runtime'
  );

ALTER TABLE market.collection_definitions
    ALTER COLUMN desired_state SET DEFAULT 'stopped',
    ALTER COLUMN desired_state SET NOT NULL,
    ALTER COLUMN control_generation SET DEFAULT 0,
    ALTER COLUMN control_generation SET NOT NULL;

ALTER TABLE market.stream_definitions
    ALTER COLUMN desired_state SET DEFAULT 'stopped',
    ALTER COLUMN desired_state SET NOT NULL,
    ALTER COLUMN control_generation SET DEFAULT 0,
    ALTER COLUMN control_generation SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'market.collection_definitions'::regclass
          AND conname = 'ck_market_collection_desired_state'
    ) THEN
        ALTER TABLE market.collection_definitions
            ADD CONSTRAINT ck_market_collection_desired_state
            CHECK (desired_state IN ('running', 'stopped', 'paused'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'market.collection_definitions'::regclass
          AND conname = 'ck_market_collection_control_generation_nonnegative'
    ) THEN
        ALTER TABLE market.collection_definitions
            ADD CONSTRAINT ck_market_collection_control_generation_nonnegative
            CHECK (control_generation >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'market.stream_definitions'::regclass
          AND conname = 'ck_market_stream_desired_state'
    ) THEN
        ALTER TABLE market.stream_definitions
            ADD CONSTRAINT ck_market_stream_desired_state
            CHECK (desired_state IN ('running', 'stopped', 'paused'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'market.stream_definitions'::regclass
          AND conname = 'ck_market_stream_control_generation_nonnegative'
    ) THEN
        ALTER TABLE market.stream_definitions
            ADD CONSTRAINT ck_market_stream_control_generation_nonnegative
            CHECK (control_generation >= 0);
    END IF;
END $$;

DROP INDEX IF EXISTS market.ix_market_collection_claimable;
CREATE INDEX ix_market_collection_claimable
    ON market.collection_definitions (
        desired_state,
        enabled,
        next_scheduled_at,
        available_at
    );

DROP INDEX IF EXISTS market.ix_market_stream_definition_enabled;
CREATE INDEX ix_market_stream_definition_enabled
    ON market.stream_definitions (desired_state, enabled, provider, venue);

CREATE TABLE IF NOT EXISTS market.collector_operation_events (
    id varchar(64) PRIMARY KEY,
    request_id varchar(128) NOT NULL,
    collector_id varchar(64) NOT NULL,
    collector_kind varchar(32) NOT NULL,
    action varchar(32) NOT NULL,
    status varchar(16) NOT NULL,
    requested_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    actor_id varchar(128) NOT NULL,
    context jsonb NOT NULL DEFAULT '{}'::jsonb,
    prior_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    resulting_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    error text,
    CONSTRAINT uq_market_collector_operation_request UNIQUE (request_id),
    CONSTRAINT ck_market_collector_operation_kind CHECK (
        collector_kind IN ('scheduled_fact', 'continuous_stream')
    ),
    CONSTRAINT ck_market_collector_operation_action CHECK (
        action IN ('start', 'stop', 'restart', 'pause', 'resume', 'recover')
    ),
    CONSTRAINT ck_market_collector_operation_status CHECK (
        status IN ('succeeded', 'failed')
    ),
    CONSTRAINT ck_market_collector_operation_context_object CHECK (
        jsonb_typeof(context) = 'object'
    ),
    CONSTRAINT ck_market_collector_operation_prior_object CHECK (
        jsonb_typeof(prior_state) = 'object'
    ),
    CONSTRAINT ck_market_collector_operation_result_object CHECK (
        jsonb_typeof(resulting_state) = 'object'
    ),
    CONSTRAINT ck_market_collector_operation_evidence_object CHECK (
        jsonb_typeof(evidence) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS ix_market_collector_operation_collector_time
    ON market.collector_operation_events (collector_id, requested_at);
CREATE INDEX IF NOT EXISTS ix_market_collector_operation_recorded
    ON market.collector_operation_events (recorded_at);

CREATE OR REPLACE FUNCTION market.reject_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable market-data relation %.% rejects %',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS trg_reject_mutation_collector_operation_events
    ON market.collector_operation_events;
CREATE TRIGGER trg_reject_mutation_collector_operation_events
BEFORE UPDATE OR DELETE ON market.collector_operation_events
FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation();

COMMIT;

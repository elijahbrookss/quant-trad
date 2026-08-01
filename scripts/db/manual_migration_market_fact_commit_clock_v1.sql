\set ON_ERROR_STOP on

-- Shared market-fact commit clock v1.
--
-- Run manually with backend, collector, worker, and paper-runtime processes
-- stopped. This preserves accepted candle revisions, archives old frozen dataset
-- manifests for manual inspection, and establishes one causal sequence shared by
-- candles and every later typed market fact.

BEGIN;
SELECT pg_advisory_xact_lock(9021003);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
    ) THEN
        RAISE EXCEPTION
            'market fact clock migration requires exclusive database access; stop backend, collector, worker, and paper processes';
    END IF;
    IF to_regclass('market.candle_versions') IS NULL THEN
        RAISE EXCEPTION
            'market fact clock migration requires market.candle_versions';
    END IF;
END $$;

LOCK TABLE market.candle_versions IN ACCESS EXCLUSIVE MODE;
CREATE SEQUENCE IF NOT EXISTS market.fact_commit_seq;

ALTER TABLE market.candle_versions
    ALTER COLUMN market_commit_seq DROP IDENTITY IF EXISTS,
    ALTER COLUMN market_commit_seq SET DEFAULT nextval('market.fact_commit_seq'::regclass);

DO $$
DECLARE
    maximum_commit bigint;
BEGIN
    SELECT COALESCE(MAX(market_commit_seq), 0)
    INTO maximum_commit
    FROM market.candle_versions;

    IF to_regclass('market.open_interest_versions') IS NOT NULL THEN
        EXECUTE
            'ALTER TABLE market.open_interest_versions '
            'ALTER COLUMN market_commit_seq DROP IDENTITY IF EXISTS, '
            'ALTER COLUMN market_commit_seq SET DEFAULT '
            'nextval(''market.fact_commit_seq''::regclass)';
        SELECT GREATEST(
            maximum_commit,
            COALESCE((
                SELECT MAX(market_commit_seq)
                FROM market.open_interest_versions
            ), 0)
        ) INTO maximum_commit;
    END IF;

    IF maximum_commit > 0 THEN
        PERFORM setval('market.fact_commit_seq', maximum_commit, true);
    ELSE
        PERFORM setval('market.fact_commit_seq', 1, false);
    END IF;
END $$;

-- Dataset provenance hashing is now typed-fact aware (v2), so old manifests are
-- not silently represented as valid multi-fact freezes. Preserve them as an
-- unsupported snapshot and require fresh preparation for canonical backtests.
CREATE SCHEMA IF NOT EXISTS legacy_market_v1;

DO $$
BEGIN
    IF to_regclass('legacy_market_v1.market_dataset_series_pre_multifact') IS NULL THEN
        CREATE TABLE legacy_market_v1.market_dataset_series_pre_multifact
        AS TABLE market.dataset_series WITH DATA;
    END IF;
    IF to_regclass('legacy_market_v1.market_datasets_pre_multifact') IS NULL THEN
        CREATE TABLE legacy_market_v1.market_datasets_pre_multifact
        AS TABLE market.datasets WITH DATA;
    END IF;
END $$;

TRUNCATE TABLE market.dataset_series, market.datasets;

DO $$
DECLARE
    expression text;
    identity_kind text;
BEGIN
    SELECT pg_get_expr(default_value.adbin, default_value.adrelid),
           attribute.attidentity
    INTO expression, identity_kind
    FROM pg_attribute AS attribute
    LEFT JOIN pg_attrdef AS default_value
      ON default_value.adrelid = attribute.attrelid
     AND default_value.adnum = attribute.attnum
    WHERE attribute.attrelid = 'market.candle_versions'::regclass
      AND attribute.attname = 'market_commit_seq';

    IF COALESCE(identity_kind, '') <> ''
       OR position('market.fact_commit_seq' in COALESCE(expression, '')) = 0 THEN
        RAISE EXCEPTION
            'market fact clock migration verification failed for candle_versions';
    END IF;
END $$;

COMMIT;

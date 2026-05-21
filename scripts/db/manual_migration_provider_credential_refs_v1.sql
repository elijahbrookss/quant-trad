-- Provider credential references v1.
--
-- Run manually against PG_DSN:
--   docker exec -i quant-trad-tsdb-1 psql -U quanttrad -d quanttrad < scripts/db/manual_migration_provider_credential_refs_v1.sql

CREATE TABLE IF NOT EXISTS portal_provider_credential_refs (
    credential_ref TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    venue_id TEXT NOT NULL DEFAULT '',
    environment TEXT NOT NULL DEFAULT 'paper',
    display_name TEXT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    secrets_encrypted TEXT NOT NULL,
    secret_version INTEGER NOT NULL DEFAULT 1,
    required_secret_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    validation JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_validated_at TIMESTAMPTZ NULL,
    last_used_at TIMESTAMPTZ NULL,
    revoked_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS ix_provider_credential_refs_provider_venue
    ON portal_provider_credential_refs (provider_id, venue_id, environment)
    WHERE revoked_at IS NULL;

DO $$
BEGIN
    IF to_regclass('portal_provider_credentials') IS NOT NULL THEN
        INSERT INTO portal_provider_credential_refs (
            credential_ref,
            provider_id,
            venue_id,
            environment,
            display_name,
            status,
            secrets_encrypted,
            secret_version,
            required_secret_keys,
            validation,
            created_at,
            updated_at
        )
        SELECT
            lower(replace(provider_id, '_', '-')) || '-' ||
                lower(replace(CASE WHEN venue_id = '' THEN 'default' ELSE venue_id END, '_', '-')) ||
                '-paper' AS credential_ref,
            provider_id,
            venue_id,
            'paper' AS environment,
            provider_id || CASE WHEN venue_id = '' THEN '' ELSE ' ' || venue_id END || ' paper' AS display_name,
            'active' AS status,
            secrets_encrypted,
            1 AS secret_version,
            CASE
                WHEN provider_id = 'COINBASE' AND venue_id = 'COINBASE_DIRECT'
                    THEN '["COINBASE_API_KEY","COINBASE_API_SECRET"]'::jsonb
                WHEN provider_id = 'ALPACA' AND venue_id = 'ALPACA'
                    THEN '["ALPACA_API_KEY","ALPACA_SECRET_KEY"]'::jsonb
                ELSE '[]'::jsonb
            END AS required_secret_keys,
            '{"status":"legacy_imported","message":"Imported from portal_provider_credentials by manual migration."}'::jsonb AS validation,
            created_at,
            updated_at
        FROM portal_provider_credentials
        ON CONFLICT (credential_ref) DO NOTHING;
    END IF;
END $$;

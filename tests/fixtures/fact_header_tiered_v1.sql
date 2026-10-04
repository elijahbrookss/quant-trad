-- Header definition from installed source commit 487d18c6.
-- Compiled from its ORM model; disposable v1 migration fixture only.
CREATE TABLE market.fact_versions (
	id VARCHAR(64) NOT NULL, 
	storage_day DATE DEFAULT (clock_timestamp() AT TIME ZONE 'UTC')::date NOT NULL, 
	series_id BIGINT NOT NULL, 
	observation_key VARCHAR(512) NOT NULL, 
	revision INTEGER NOT NULL, 
	market_commit_seq BIGINT DEFAULT nextval('market.fact_commit_seq'::regclass) NOT NULL, 
	source_id BIGINT NOT NULL, 
	ingestion_run_id VARCHAR(64), 
	fact_type VARCHAR(64) NOT NULL, 
	payload_schema_id VARCHAR(128) NOT NULL, 
	payload_contract_hash VARCHAR(64) NOT NULL, 
	observation_time TIMESTAMP WITH TIME ZONE NOT NULL, 
	observation_time_method VARCHAR(64) NOT NULL, 
	source_published_at TIMESTAMP WITH TIME ZONE, 
	received_at TIMESTAMP WITH TIME ZONE, 
	accepted_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	known_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	known_at_method VARCHAR(64) NOT NULL, 
	transformation_id VARCHAR(128) NOT NULL, 
	external_event_key VARCHAR(512), 
	external_event_group_key VARCHAR(512), 
	external_event_component_key VARCHAR(256), 
	state VARCHAR(16) NOT NULL, 
	payload_hash VARCHAR(64) NOT NULL, 
	material_hash VARCHAR(64) NOT NULL, 
	provenance_schema_id VARCHAR(64) NOT NULL, 
	provenance_hash VARCHAR(64) NOT NULL, 
	quality_schema_id VARCHAR(64) NOT NULL, 
	quality_hash VARCHAR(64) NOT NULL, 
	row_hash VARCHAR(64) NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_market_fact_observation_revision UNIQUE (series_id, observation_key, revision), 
	CONSTRAINT fk_market_fact_payload_contract FOREIGN KEY(payload_schema_id, fact_type, payload_contract_hash) REFERENCES market.fact_schemas (schema_id, fact_type, contract_hash) ON DELETE RESTRICT, 
	CONSTRAINT ck_market_fact_revision_positive CHECK (revision > 0), 
	CONSTRAINT ck_market_fact_type CHECK (fact_type <> ''), 
	CONSTRAINT ck_market_fact_observation_key CHECK (observation_key <> ''), 
	CONSTRAINT ck_market_fact_observation_method CHECK (observation_time_method <> ''), 
	CONSTRAINT ck_market_fact_known_method CHECK (known_at_method <> ''), 
	CONSTRAINT ck_market_fact_transformation CHECK (transformation_id <> ''), 
	CONSTRAINT ck_market_fact_state CHECK (state IN ('active', 'invalidated')), 
	CONSTRAINT ck_market_fact_acceptance_after_receipt CHECK (received_at IS NULL OR accepted_at >= received_at), 
	CONSTRAINT ck_market_fact_receipt_known_after_acceptance CHECK (known_at_method NOT IN ('platform_acceptance', 'platform_receipt', 'stream_receipt') OR known_at >= accepted_at), 
	CONSTRAINT ck_market_fact_payload_hash CHECK (payload_hash ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT ck_market_fact_material_hash CHECK (material_hash ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT ck_market_fact_provenance_hash CHECK (provenance_hash ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT ck_market_fact_quality_hash CHECK (quality_hash ~ '^[0-9a-f]{64}$'), 
	CONSTRAINT ck_market_fact_row_hash CHECK (row_hash ~ '^[0-9a-f]{64}$'), 
	FOREIGN KEY(series_id) REFERENCES market.series (id) ON DELETE RESTRICT, 
	FOREIGN KEY(source_id) REFERENCES market.sources (id) ON DELETE RESTRICT, 
	FOREIGN KEY(ingestion_run_id) REFERENCES market.ingestion_runs (id) ON DELETE RESTRICT
);
CREATE INDEX ix_market_fact_external_group ON market.fact_versions (series_id, external_event_group_key);
CREATE INDEX ix_market_fact_schema_time ON market.fact_versions (payload_schema_id, observation_time);
CREATE INDEX ix_market_fact_series_accepted ON market.fact_versions (series_id, accepted_at, market_commit_seq);
CREATE INDEX ix_market_fact_series_commit ON market.fact_versions (series_id, market_commit_seq);
CREATE INDEX ix_market_fact_series_known ON market.fact_versions (series_id, known_at, observation_time);
CREATE INDEX ix_market_fact_series_material ON market.fact_versions (series_id, material_hash);
CREATE INDEX ix_market_fact_series_source ON market.fact_versions (series_id, source_id);
CREATE INDEX ix_market_fact_series_time_revision ON market.fact_versions (series_id, observation_time DESC, observation_key, revision DESC);
CREATE INDEX ix_market_fact_source_time ON market.fact_versions (source_id, observation_time);
CREATE INDEX ix_market_fact_storage_family ON market.fact_versions (storage_day, fact_type);
CREATE INDEX ix_market_fact_storage_page ON market.fact_versions (storage_day, market_commit_seq, id);

CREATE OR REPLACE FUNCTION market.assert_fact_hot_payload_valid() RETURNS trigger LANGUAGE plpgsql AS $qt$
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
$qt$;

CREATE OR REPLACE FUNCTION market.require_fact_hot_payload() RETURNS trigger LANGUAGE plpgsql AS $qt$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM market.fact_hot_payloads
        WHERE storage_day = NEW.storage_day AND id = NEW.id
    ) THEN
        RAISE EXCEPTION 'fact_hot_payload_missing: fact_version_id=% storage_day=%', NEW.id, NEW.storage_day;
    END IF;
    RETURN NULL;
END;
$qt$;

CREATE VIEW market.fact_rows AS 
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
;

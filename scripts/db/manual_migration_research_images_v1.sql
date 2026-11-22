-- Optional explicit provisioning before deploying research image support.
-- Uses the existing QT database. No historical evidence is rewritten.
BEGIN;
CREATE TABLE IF NOT EXISTS portal_research_images (
    id varchar(64) PRIMARY KEY,
    item_id varchar(64) NOT NULL REFERENCES portal_research_items(id) ON DELETE RESTRICT,
    manifest jsonb NOT NULL,
    content bytea NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT ck_research_image_bytes CHECK (octet_length(content) BETWEEN 1 AND 4194304)
);
CREATE INDEX IF NOT EXISTS ix_portal_research_images_item_created
    ON portal_research_images (item_id, created_at);
COMMIT;

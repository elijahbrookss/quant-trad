-- Research memory storage: observations, research checks, hypotheses, studies, and links.

CREATE TABLE IF NOT EXISTS portal_research_items (
    id VARCHAR(64) PRIMARY KEY,
    kind VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    title VARCHAR(255) NOT NULL,
    body VARCHAR(8192),
    instrument_id VARCHAR(64),
    symbol VARCHAR(64),
    timeframe VARCHAR(32),
    datasource VARCHAR(64),
    exchange VARCHAR(64),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_revision VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portal_research_links (
    id VARCHAR(96) PRIMARY KEY,
    source_item_id VARCHAR(64) NOT NULL REFERENCES portal_research_items(id) ON DELETE CASCADE,
    target_type VARCHAR(64) NOT NULL,
    target_id VARCHAR(255) NOT NULL,
    relation VARCHAR(64) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_research_link_identity UNIQUE (source_item_id, target_type, target_id, relation)
);

CREATE INDEX IF NOT EXISTS ix_portal_research_items_kind_status_updated
    ON portal_research_items (kind, status, updated_at);

CREATE INDEX IF NOT EXISTS ix_portal_research_items_symbol_timeframe
    ON portal_research_items (symbol, timeframe);

CREATE INDEX IF NOT EXISTS ix_portal_research_links_source_relation
    ON portal_research_links (source_item_id, relation);

CREATE INDEX IF NOT EXISTS ix_portal_research_links_target
    ON portal_research_links (target_type, target_id);

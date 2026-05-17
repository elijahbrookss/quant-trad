-- Manual schema transition for ADR 0018.
--
-- Strategy variants now persist decision output filters only. ATM selection
-- belongs to strategy/bot/experiment config, and strategy variants no longer
-- persist param overrides.

ALTER TABLE portal_strategy_variants
    ADD COLUMN IF NOT EXISTS output_filters JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE portal_strategy_variants
    DROP COLUMN IF EXISTS param_overrides,
    DROP COLUMN IF EXISTS atm_template_id;

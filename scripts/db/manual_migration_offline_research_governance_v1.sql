BEGIN;

-- The normal schema contract creates the Phase 4-6 tables on a clean
-- database.  This additive migration is for a database that admitted the
-- initial governance-case table before creation request identity became
-- mandatory during the bounded campaign.
ALTER TABLE IF EXISTS public.portal_research_governance_cases
    ADD COLUMN IF NOT EXISTS creation_request_id VARCHAR(128);

UPDATE public.portal_research_governance_cases
SET creation_request_id = 'migration:v1:' || md5(id)
WHERE creation_request_id IS NULL;

ALTER TABLE IF EXISTS public.portal_research_governance_cases
    ALTER COLUMN creation_request_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_research_governance_case_creation_request'
          AND conrelid = 'public.portal_research_governance_cases'::regclass
    ) THEN
        ALTER TABLE public.portal_research_governance_cases
            ADD CONSTRAINT uq_research_governance_case_creation_request
            UNIQUE (creation_request_id);
    END IF;
END
$$;

COMMIT;

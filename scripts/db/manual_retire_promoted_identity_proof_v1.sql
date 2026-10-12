-- Explicit operator repair after a committed forward handoff. No row changes.
-- Pass the exact committed operation: psql -v operation_sha256=<64 hex> -f ...
-- Preserve the old proof, functions, truncate protection and permanent guards.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '100ms';
SET LOCAL idle_in_transaction_session_timeout = '5s';
SELECT set_config('qt.repair_operation', :'operation_sha256', true);
LOCK TABLE ONLY market.fact_identities IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $repair$
DECLARE
    operation text := current_setting('qt.repair_operation');
    proof jsonb;
    terminal jsonb;
    expected_body text;
    source_oid oid;
    adoption_schema text;
BEGIN
    IF operation !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'identity_proof_repair_operation_invalid';
    END IF;
    adoption_schema := 'qt_fwd_' || left(operation, 56);
    IF to_regclass(format('%I.adoption', adoption_schema)) IS NULL THEN
        adoption_schema := 'qt_fact_header_forward_v2';
    END IF;
    EXECUTE format('SELECT terminal FROM %I.adoption WHERE id=1 AND operation_sha256=$1',
                   adoption_schema) INTO STRICT terminal USING operation;
    SELECT binding INTO STRICT proof FROM qt_fact_header_cutover_v2.online_copy_proof WHERE id=1;
    source_oid := (proof->'source_oids'->>'market.fact_versions')::oid;
    IF terminal->>'kind' IS DISTINCT FROM 'switched' OR terminal->>'operation_sha256' IS DISTINCT FROM operation
       OR (terminal->>'legacy_oid')::oid IS DISTINCT FROM source_oid
       OR 'market.fact_versions_legacy'::regclass::oid <> source_oid
       OR 'market.fact_versions'::regclass::oid = source_oid
       OR 'market.fact_identities'::regclass::oid IS DISTINCT FROM
          (proof->'targets'->'fact_identities'->>0)::oid
       OR market.fact_header_legacy_end_day() IS DISTINCT FROM (terminal->>'end_day')::date
       OR NOT EXISTS (SELECT 1 FROM market.fact_storage_state
                      WHERE layout_version='market.fact_storage_tiers.v2' AND state='ready') THEN
        RAISE EXCEPTION 'identity_proof_repair_committed_binding_changed';
    END IF;
    expected_body := format($body$
DECLARE expected jsonb;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'online_copy_proof_row_mutation_refused';
    END IF;
    IF 'market.fact_versions'::regclass::oid <> %s::oid THEN
        RAISE EXCEPTION 'online_copy_proof_source_changed';
    END IF;
    SELECT jsonb_build_object('id', original.id,'storage_day', original.storage_day,'series_id', original.series_id,'observation_key', original.observation_key,'revision', original.revision) INTO expected FROM market.fact_versions original WHERE original.id=NEW.id;
    IF expected IS NULL OR expected IS DISTINCT FROM to_jsonb(NEW) THEN
        RAISE EXCEPTION 'online_copy_proof_insert_mismatch: identity';
    END IF;
    RETURN NEW;
END;
$body$, source_oid);
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
        WHERE t.tgrelid='market.fact_identities'::regclass
          AND t.tgname='trg_qt_online_row_guard' AND NOT t.tgisinternal
          AND t.tgtype=31 AND t.tgenabled='A' AND t.tgnargs=0 AND t.tgqual IS NULL
          AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgparentid=0
          AND p.oid='qt_fact_header_cutover_v2.online_guard_identity()'::regprocedure
          AND p.prosrc=expected_body AND p.prosecdef
          AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
    ) THEN
        RAISE EXCEPTION 'identity_proof_repair_guard_changed';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgrelid='market.fact_identities'::regclass
          AND tgname='trg_guard_fact_identities' AND tgenabled='A' AND tgtype=27
          AND tgfoid='market.reject_fact_identity_mutation()'::regprocedure
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgrelid='market.fact_identities'::regclass
          AND tgname='trg_require_fact_identity_header' AND tgenabled='A' AND tgtype=5
          AND tgdeferrable AND tginitdeferred
          AND tgfoid='market.require_fact_identity_header()'::regprocedure
    ) THEN
        RAISE EXCEPTION 'identity_proof_repair_permanent_guard_missing';
    END IF;
    DROP TRIGGER trg_qt_online_row_guard ON market.fact_identities;
END;
$repair$;
COMMIT;

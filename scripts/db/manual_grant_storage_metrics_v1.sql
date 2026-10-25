-- PRIVILEGE TRANSITION: separate explicit operator approval required.
-- Run as an authorized role administrator after index/function installation.
-- psql -v api_role=<existing-app-role> -f this-file
-- qt_storage_metrics_grafana LOGIN INHERIT must be created/credentialed separately after approval.
-- No credentials, login creation or database CONNECT privilege is supplied here.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout='250ms';
SET LOCAL statement_timeout='5s';
DO $check$
BEGIN
 -- One-time transition: do not adopt pre-existing named principals or silently
 -- expand their authority. An uncertain prior run requires catalog reconciliation.
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname IN
     ('qt_storage_metrics_owner','qt_storage_metrics_reader')) THEN
  RAISE EXCEPTION 'storage_metrics_roles_already_exist_review_required';
 END IF;
 CREATE ROLE qt_storage_metrics_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
 CREATE ROLE qt_storage_metrics_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid=m.member
    WHERE r.rolname='qt_storage_metrics_grafana'
       OR m.roleid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname='qt_storage_metrics_grafana')) THEN
  RAISE EXCEPTION 'storage_metrics_unexpected_role_membership';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_shdepend d JOIN pg_catalog.pg_roles r ON r.oid=d.refobjid
    WHERE d.refclassid='pg_catalog.pg_authid'::regclass AND r.rolname='qt_storage_metrics_grafana'
      AND d.deptype IN ('o','a')
      AND NOT (d.classid='pg_catalog.pg_database'::regclass AND d.deptype='a')) THEN
  RAISE EXCEPTION 'storage_metrics_login_existing_object_authority';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='qt_storage_metrics_grafana' AND rolcanlogin
     AND rolinherit AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls) THEN
  RAISE EXCEPTION 'storage_metrics_dedicated_login_missing_or_unsafe';
 END IF;
 IF pg_catalog.has_database_privilege('qt_storage_metrics_grafana',current_database(),'CREATE')
    OR pg_catalog.has_database_privilege('qt_storage_metrics_owner',current_database(),'CREATE') THEN
  RAISE EXCEPTION 'storage_metrics_database_create_privilege_requires_separate_review';
 END IF;
 IF pg_catalog.has_database_privilege('qt_storage_metrics_grafana',current_database(),'TEMP')
    OR pg_catalog.has_database_privilege('qt_storage_metrics_owner',current_database(),'TEMP') THEN
  RAISE EXCEPTION 'storage_metrics_ambient_temporary_privilege_requires_separate_review';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname IN ('public','market','observability_metrics','observability_events')
      AND CASE WHEN c.relkind IN ('r','p','v','m','f') THEN
          (pg_catalog.has_table_privilege('qt_storage_metrics_grafana',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
           OR pg_catalog.has_any_column_privilege('qt_storage_metrics_grafana',c.oid,'SELECT,INSERT,UPDATE,REFERENCES'))
          ELSE false END) THEN
  RAISE EXCEPTION 'storage_metrics_login_ambient_source_authority';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname IN ('public','market','observability_metrics','observability_events')
      AND CASE WHEN c.relkind='S' THEN
          pg_catalog.has_sequence_privilege('qt_storage_metrics_grafana',c.oid,'USAGE,SELECT,UPDATE')
          ELSE false END) THEN
  RAISE EXCEPTION 'storage_metrics_login_ambient_sequence_authority';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_namespace n
    WHERE n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast_temp_%'
      AND pg_catalog.has_schema_privilege('qt_storage_metrics_grafana',n.oid,'CREATE')) THEN
  RAISE EXCEPTION 'storage_metrics_login_schema_write_authority';
 END IF;
END
$check$;
GRANT USAGE ON SCHEMA public,market,observability_metrics TO qt_storage_metrics_owner;
GRANT SELECT(sampled_at,database_size_bytes,relation_count) ON observability_metrics.database_capacity_samples TO qt_storage_metrics_owner;
GRANT SELECT(sampled_at,schema_name,relation_name,total_bytes) ON observability_metrics.database_relation_capacity_samples TO qt_storage_metrics_owner;
GRANT SELECT(id,acknowledged_at,byte_count,manifest_hash) ON market.fact_archive_manifests TO qt_storage_metrics_owner;
GRANT SELECT(manifest_id,manifest_hash,verified_at,verifier_version) ON market.fact_archive_verifications TO qt_storage_metrics_owner;
GRANT SELECT(storage_day,state,source_bytes,reclaimed_at,reclaimed_bytes) ON market.fact_retention_partitions TO qt_storage_metrics_owner;
GRANT SELECT(id,revision,policy) ON public.portal_storage_policy TO qt_storage_metrics_owner;
-- CREATE is needed only for the ownership transfer and is removed in this transaction.
GRANT CREATE ON SCHEMA observability_metrics TO qt_storage_metrics_owner;
ALTER FUNCTION observability_metrics.storage_metrics_admit_v1() OWNER TO qt_storage_metrics_owner;
ALTER FUNCTION observability_metrics.storage_metrics_admit_v1() SECURITY DEFINER;
ALTER FUNCTION observability_metrics.storage_metrics_admit_v1() SET search_path TO pg_catalog,pg_temp;
REVOKE ALL ON FUNCTION observability_metrics.storage_metrics_admit_v1() FROM PUBLIC;
ALTER FUNCTION observability_metrics.storage_series_v1(text,timestamptz,timestamptz,integer) OWNER TO qt_storage_metrics_owner;
ALTER FUNCTION observability_metrics.storage_series_v1(text,timestamptz,timestamptz,integer) SECURITY DEFINER;
ALTER FUNCTION observability_metrics.storage_series_v1(text,timestamptz,timestamptz,integer) SET search_path TO pg_catalog,pg_temp;
REVOKE ALL ON FUNCTION observability_metrics.storage_series_v1(text,timestamptz,timestamptz,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION observability_metrics.storage_series_v1(text,timestamptz,timestamptz,integer) TO qt_storage_metrics_reader, :"api_role";
ALTER FUNCTION observability_metrics.storage_growth_v1(text,timestamptz,timestamptz,integer) OWNER TO qt_storage_metrics_owner;
ALTER FUNCTION observability_metrics.storage_growth_v1(text,timestamptz,timestamptz,integer) SECURITY DEFINER;
ALTER FUNCTION observability_metrics.storage_growth_v1(text,timestamptz,timestamptz,integer) SET search_path TO pg_catalog,pg_temp;
REVOKE ALL ON FUNCTION observability_metrics.storage_growth_v1(text,timestamptz,timestamptz,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION observability_metrics.storage_growth_v1(text,timestamptz,timestamptz,integer) TO qt_storage_metrics_reader, :"api_role";
ALTER FUNCTION observability_metrics.storage_metric_status_v1() OWNER TO qt_storage_metrics_owner;
ALTER FUNCTION observability_metrics.storage_metric_status_v1() SECURITY DEFINER;
ALTER FUNCTION observability_metrics.storage_metric_status_v1() SET search_path TO pg_catalog,pg_temp;
REVOKE ALL ON FUNCTION observability_metrics.storage_metric_status_v1() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION observability_metrics.storage_metric_status_v1() TO qt_storage_metrics_reader, :"api_role";
REVOKE CREATE ON SCHEMA observability_metrics FROM qt_storage_metrics_owner;
DO $owner_check$
BEGIN
 IF EXISTS(SELECT 1 FROM pg_catalog.pg_namespace n
    WHERE n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast_temp_%'
      AND pg_catalog.has_schema_privilege('qt_storage_metrics_owner',n.oid,'CREATE')) THEN
  RAISE EXCEPTION 'storage_metrics_owner_schema_write_authority';
 END IF;
END
$owner_check$;
GRANT USAGE ON SCHEMA observability_metrics TO qt_storage_metrics_reader, :"api_role";
GRANT qt_storage_metrics_reader TO qt_storage_metrics_grafana;
ALTER ROLE qt_storage_metrics_grafana CONNECTION LIMIT 2;
ALTER ROLE qt_storage_metrics_grafana SET statement_timeout='5s';
ALTER ROLE qt_storage_metrics_grafana SET lock_timeout='250ms';
ALTER ROLE qt_storage_metrics_grafana SET default_transaction_read_only=on;
COMMIT;
-- These are pre-dispatch defaults, not immutable ceilings against a malicious SQL client.
-- Keep datasource SQL fixed; do not grant raw source access, CREATE, TEMP or other memberships.
-- PostgreSQL commonly grants database TEMP through PUBLIC. This script refuses
-- that ambient authority and effective database CREATE; it never revokes PUBLIC privileges. Any database-wide
-- PUBLIC change and compensating grants require separate explicit approval.

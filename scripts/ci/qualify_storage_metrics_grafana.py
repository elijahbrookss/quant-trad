"""Qualify real Grafana PostgreSQL pool sessions on GitHub-owned services.

Requires fresh disposable PostgreSQL 15 and Grafana services supplied by CI.
No Docker lifecycle, production access, credentials in output, or runtime grants.
The script creates one random database and synthetic principals, installs the
checked-in SQL, and removes only its owned resources in finally.

Example hosted invocation (values supplied privately by the workflow):
  PG_DSN=<loopback test admin DSN> QT_TEST_GRAFANA_PASSWORD=<synthetic password>
  python scripts/ci/qualify_storage_metrics_grafana.py --confirm-disposable \
      --grafana-url http://127.0.0.1:3000 --grafana-postgres-host postgres:5432

PG_DSN is the existing test connection input. The Grafana address names that
same disposable PostgreSQL service from Grafana's network namespace.
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
import secrets
import sys
from threading import Barrier
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
LOGIN = "qt_storage_metrics_grafana"
API_ROLE = "qt_storage_metrics_ci_api"
OWNER = "qt_storage_metrics_owner"
READER = "qt_storage_metrics_reader"
ROLES = (LOGIN, API_ROLE, OWNER, READER)
SESSION_SQL = """SELECT current_user AS login,
 current_setting('default_transaction_read_only') AS default_read_only,
 current_setting('transaction_read_only') AS read_only,
 current_setting('statement_timeout') AS statement_timeout,
 current_setting('lock_timeout') AS lock_timeout,
 pg_backend_pid() AS pid, has_database_privilege(current_user,current_database(),'TEMP') AS temporary_allowed,
 has_any_column_privilege(current_user,'observability_metrics.database_capacity_samples','SELECT') AS raw_select_allowed,
 has_table_privilege(current_user,'public.storage_metrics_ci_write_probe','INSERT') AS raw_insert_allowed,
 has_schema_privilege(current_user,'public','CREATE') AS schema_create_allowed
 FROM (SELECT pg_sleep(0.4)) AS owned_pause"""


def require(condition: bool, label: str) -> None:
    if not condition:
        raise AssertionError(label)


def guarded_inputs(args):
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and args.confirm_disposable, "github_owned_disposable_services_required")
    from psycopg2.extensions import parse_dsn
    dsn = os.environ.get("PG_DSN", "")
    parsed = parse_dsn(dsn)
    require(parsed.get("host") in {"127.0.0.1", "::1", "localhost"}
            and parsed.get("hostaddr", "127.0.0.1") in {"127.0.0.1", "::1"},
            "loopback_postgres_admin_required")
    url = urllib.parse.urlsplit(args.grafana_url)
    require(url.scheme == "http" and url.hostname in {"127.0.0.1", "::1", "localhost"}
            and not url.username and not url.password and url.path in {"", "/"}
            and not url.query and not url.fragment, "loopback_disposable_grafana_required")
    require(re.fullmatch(r"[a-zA-Z0-9_.-]+:[0-9]{1,5}", args.grafana_postgres_host) is not None,
            "grafana_postgres_service_address_required")
    password = os.environ.get("QT_TEST_GRAFANA_PASSWORD")
    require(bool(password), "synthetic_grafana_admin_password_required")
    return parsed, password


class Grafana:
    def __init__(self, url, password):
        self.url = url.rstrip("/")
        self.auth = "Basic " + base64.b64encode(("admin:" + password).encode()).decode()

    def request(self, path, payload=None, *, method=None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(self.url + path, data=data,
            method=method, headers={"Authorization": self.auth, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                raw = response.read(2_097_153)
                status = response.status
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read(2_097_153)
        require(len(raw) <= 2_097_152, "grafana_response_bound")
        try:
            return status, json.loads(raw) if raw else {}
        except ValueError:
            raise AssertionError("grafana_non_json_response") from None

    def query(self, uid, query):
        now = int(time.time() * 1000)
        return self.request("/api/ds/query", {"from": str(now - 3_600_000), "to": str(now),
            "queries": [{"refId": "A", "datasource": {"uid": uid, "type": "postgres"},
                         "rawSql": query, "format": "table", "rawQuery": True,
                         "intervalMs": 300000, "maxDataPoints": 720}]})


def rows(outcome):
    status, payload = outcome
    require(status == 200, "grafana_query_http_success")
    result = payload.get("results", {}).get("A", {})
    require(not result.get("error"), "grafana_query_success")
    output = []
    for frame in result.get("frames", []):
        fields = [field["name"] for field in frame["schema"]["fields"]]
        output.extend(dict(zip(fields, row)) for row in zip(*frame["data"]["values"]))
    return output


def rejected(outcome, label):
    status, payload = outcome
    result = payload.get("results", {}).get("A", {})
    # Grafana can redact PostgreSQL causes to a generic query error. Require
    # the actual query's failure envelope, never an arbitrary HTTP/auth failure.
    # Pool privilege facts and timeout elapsed bounds are asserted separately;
    # direct authenticated DB qualification checks the precise SQLSTATE causes.
    require(status in {200, 400, 500} and bool(result.get("error")),
            label + "_query_must_fail")
    print("storage_metrics_grafana stage=" + label + " http_status=" + str(status)
          + " result=query_failed", flush=True)


def install_sql(connection, filename, *, api_role=None, individual=False):
    source = (ROOT / "scripts/db" / filename).read_text()
    source = "\n".join(line for line in source.splitlines() if not line.startswith("\\"))
    if api_role:
        from psycopg2 import sql
        source = source.replace(':"api_role"', sql.Identifier(api_role).as_string(connection))
    with connection.cursor() as cursor:
        # Index CONCURRENTLY commands must be separate autocommit statements.
        statements = [source]
        if individual:
            # The reviewed index script contains semicolons in line comments.
            # Strip only comment lines before splitting its simple DDL statements.
            ddl = "\n".join(line for line in source.splitlines()
                            if not line.lstrip().startswith("--"))
            statements = ddl.split(";")
        for statement in statements:
            if statement.strip():
                cursor.execute(statement)


def prepare(connection):
    from psycopg2 import sql
    with connection.cursor() as cursor:
        cursor.execute("""
          CREATE SCHEMA market;
          CREATE SCHEMA observability_metrics;
          REVOKE CREATE ON SCHEMA public FROM PUBLIC;
          CREATE TABLE observability_metrics.database_capacity_samples
            (sampled_at timestamp PRIMARY KEY,database_size_bytes bigint,relation_count integer);
          CREATE TABLE observability_metrics.database_relation_capacity_samples
            (sampled_at timestamp,schema_name text,relation_name text,total_bytes bigint);
          CREATE TABLE market.fact_archive_manifests
            (id text PRIMARY KEY,acknowledged_at timestamptz,byte_count bigint,manifest_hash text);
          CREATE TABLE market.fact_archive_verifications
            (manifest_id text,manifest_hash text,verified_at timestamptz,verifier_version text);
          CREATE TABLE market.fact_retention_partitions
            (storage_day date PRIMARY KEY,state text,source_bytes bigint,reclaimed_at timestamptz,reclaimed_bytes bigint);
          CREATE TABLE public.portal_storage_policy (id integer PRIMARY KEY,revision integer,policy jsonb);
          CREATE TABLE public.storage_metrics_ci_write_probe (value integer);
          INSERT INTO public.portal_storage_policy VALUES(1,1,'{"recent_days":30}');
          INSERT INTO market.fact_retention_partitions
            VALUES(current_date-40,'verified',4096,NULL,NULL);
        """)
        # TEMP is revoked only in this newly created, task-owned database.
        cursor.execute(sql.SQL("REVOKE TEMP ON DATABASE {} FROM PUBLIC").format(
            sql.Identifier(connection.info.dbname)))
        base = datetime.now(UTC).replace(second=0, microsecond=0)
        base -= timedelta(minutes=base.minute % 5 + 30)
        for i in range(6):
            at = base + timedelta(minutes=i * 5)
            cursor.execute("INSERT INTO observability_metrics.database_capacity_samples VALUES(%s,%s,1)",
                           (at.replace(tzinfo=None), 10000 + i * 100))
            cursor.execute("INSERT INTO observability_metrics.database_relation_capacity_samples VALUES(%s,'market','synthetic',%s)",
                           (at.replace(tzinfo=None), 4000 + i * 10))
        cursor.execute("INSERT INTO market.fact_archive_manifests VALUES('synthetic',%s,2048,'synthetic-hash')",
                       (base + timedelta(minutes=5),))
        cursor.execute("INSERT INTO market.fact_archive_verifications VALUES('synthetic','synthetic-hash',%s,'market.canonical_archive_verification.v12')",
                       (base + timedelta(minutes=10),))
    install_sql(connection, "manual_add_storage_metrics_indexes_v1.sql", individual=True)
    install_sql(connection, "manual_migration_storage_metrics_v1.sql")
    install_sql(connection, "manual_grant_storage_metrics_v1.sql", api_role=API_ROLE)
    return base, base + timedelta(minutes=30)


def prove(grafana, uid, connection, start, end):
    require(grafana.request("/api/health")[0] == 200, "grafana_health")
    gate = Barrier(2)
    def session():
        gate.wait(timeout=5)
        result, = rows(grafana.query(uid, SESSION_SQL))
        return result
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(session) for _ in range(2)]
        sessions = [future.result(timeout=15) for future in futures]
    require(len({item["pid"] for item in sessions}) == 2, "two_real_pool_sessions_required")
    for item in sessions:
        require(item["login"] == LOGIN and item["default_read_only"] == item["read_only"] == "on"
                and item["statement_timeout"] == "5s" and item["lock_timeout"] == "250ms"
                and item["temporary_allowed"] is False, "pooled_role_defaults")
        require(item["raw_select_allowed"] is False and item["raw_insert_allowed"] is False
                and item["schema_create_allowed"] is False, "pooled_source_privileges_denied")
    print("storage_metrics_grafana stage=pooled_role_defaults sessions=2 result=pass", flush=True)
    bounds = "'" + start.isoformat() + "'::timestamptz,'" + end.isoformat() + "'::timestamptz,300"
    query = "SELECT * FROM observability_metrics.storage_series_v1('database_allocated_bytes'," + bounds + ")"
    values = rows(grafana.query(uid, query))
    require(any(row.get("value") == 10000 for row in values), "metric_synthetic_value")
    for metric in ("market_allocated_bytes", "archive_compressed_bytes",
                   "verified_first_compressed_bytes", "reclaimed_physical_hot_bytes"):
        require(bool(rows(grafana.query(uid,
            "SELECT * FROM observability_metrics.storage_series_v1('" + metric + "'," + bounds + ")"))),
            "metric_rows")
    require(bool(rows(grafana.query(uid, "SELECT * FROM observability_metrics.storage_metric_status_v1()"))),
            "status_rows")
    require(any(row.get("value") is not None for row in rows(grafana.query(uid,
        "SELECT * FROM observability_metrics.storage_growth_v1('database_allocated_bytes'," + bounds + ")"))),
        "growth_rows")
    for label, forbidden in (
        ("raw_select_denied", "SELECT * FROM observability_metrics.database_capacity_samples"),
        ("raw_insert_denied", "INSERT INTO public.storage_metrics_ci_write_probe VALUES(1)"),
        ("temporary_table_denied", "CREATE TEMP TABLE storage_metrics_ci_temp(value integer)"),
    ):
        rejected(grafana.query(uid, forbidden), label)
        # A working allowed query after each denial excludes a datasource outage.
        require(bool(rows(grafana.query(uid, query))), label + "_pool_remains_usable")
    began = time.monotonic()
    rejected(grafana.query(uid, "SELECT pg_sleep(7)"), "statement_timeout")
    elapsed = time.monotonic() - began
    require(4 <= elapsed <= 10, "statement_timeout_elapsed_bound")
    print("storage_metrics_grafana stage=statement_timeout elapsed_seconds=" + str(round(elapsed, 2))
          + " result=pass", flush=True)
    connection.autocommit = False
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout='1s'")
            cursor.execute("LOCK observability_metrics.database_capacity_samples IN ACCESS EXCLUSIVE MODE")
        began = time.monotonic()
        rejected(grafana.query(uid, query), "lock_timeout")
        elapsed = time.monotonic() - began
        require(0.15 <= elapsed <= 3, "lock_timeout_elapsed_bound")
        print("storage_metrics_grafana stage=lock_timeout elapsed_seconds=" + str(round(elapsed, 2))
              + " result=pass", flush=True)
    finally:
        connection.rollback()
        connection.autocommit = True
    require(bool(rows(grafana.query(uid, query))), "pool_recovers_after_cancel")
    print("storage_metrics_grafana stage=metrics_and_denials result=pass", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-disposable", action="store_true")
    parser.add_argument("--grafana-url", required=True)
    parser.add_argument("--grafana-postgres-host", default="postgres:5432")
    args = parser.parse_args()
    admin_options, grafana_password = guarded_inputs(args)
    import psycopg2
    from psycopg2 import sql
    import yaml
    database = "qt_storage_metrics_grafana_ci_" + secrets.token_hex(6)
    uid = "qt-storage-ci-" + secrets.token_hex(6)
    login_password = secrets.token_urlsafe(32)
    admin = connection = None
    database_created = datasource_created = False
    owned_roles = []
    grafana = Grafana(args.grafana_url, grafana_password)
    cleanup_errors = []
    try:
        admin = psycopg2.connect(**{**admin_options, "connect_timeout": 5})
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute("SELECT current_setting('server_version_num')::integer")
            require(150000 <= cursor.fetchone()[0] < 160000, "postgres_15_required")
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=ANY(%s)", (list(ROLES),))
            require(not cursor.fetchall(), "fresh_cluster_principals_required")
            cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(database)))
            database_created = True
            cursor.execute(sql.SQL("CREATE ROLE {} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %s").format(sql.Identifier(LOGIN)),
                           (login_password,))
            owned_roles.append(LOGIN)
            cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(API_ROLE)))
            owned_roles.append(API_ROLE)
        connection = psycopg2.connect(**{**admin_options, "dbname": database, "connect_timeout": 5})
        connection.autocommit = True
        start, end = prepare(connection)
        owned_roles.extend((OWNER, READER))
        template = yaml.safe_load((ROOT / "docker/grafana/storage-metrics/enabled.yml").read_text())
        datasource, = template["datasources"]
        datasource.update(uid=uid, name="Owned storage metrics CI " + uid,
                          url=args.grafana_postgres_host)
        datasource["jsonData"]["database"] = database
        datasource["secureJsonData"] = {"password": login_password}
        datasource_created = True  # Own this exact random UID even if the create reply is lost.
        status, result = grafana.request("/api/datasources", datasource)
        require(status == 200 and result.get("datasource", {}).get("uid") == uid,
                "create_owned_datasource")
        datasource_created = True
        prove(grafana, uid, connection, start, end)
    finally:
        if datasource_created:
            try:
                require(grafana.request("/api/datasources/uid/" + uid, method="DELETE")[0] in {200, 404},
                        "delete_owned_datasource")
            except Exception:
                cleanup_errors.append("datasource")
        if connection is not None:
            connection.close()
        if admin is not None:
            try:
                with admin.cursor() as cursor:
                    if database_created:
                        cursor.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
                    for role in reversed(owned_roles):
                        cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
            except Exception:
                cleanup_errors.append("database_or_principals")
            finally:
                admin.close()
        require(not cleanup_errors, "owned_resource_cleanup_failed")
        print("storage_metrics_grafana stage=owned_cleanup result=pass", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as failure:
        # Driver and HTTP exceptions can contain credentials/query text. Never
        # print them. Assertion labels are fixed source strings, not responses.
        label = str(failure) if isinstance(failure, AssertionError) else type(failure).__name__
        print("storage_metrics_grafana result=fail reason=" + label, file=sys.stderr)
        raise SystemExit(1) from None

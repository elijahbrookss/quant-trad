"""Real resource observation probes on the namespace fixture's private cluster."""
from pathlib import Path

from sqlalchemy import text

from core.storage_mounts import StorageMountError
from portal.backend.service.storage.header_resources import observe_header_resources


def prove_resource_paths(engine, targets, destination_oid, binary):
    root = Path(engine.url.query.get("host", "")).parent
    if (root.parent != Path("/tmp") or not root.name.startswith("qt-header-ns-")
            or engine.url.host is not None):
        raise RuntimeError("resource_fixture_requires_private_socket_cluster")
    report = {}
    with engine.connect() as conn, conn.begin():
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        before = conn.scalar(text("SHOW statement_timeout"))
        result = observe_header_resources(conn, targets, pg_controldata=binary)
        report["resources_default_bound"] = (
            result.backend_pid == conn.scalar(text("SELECT pg_backend_pid()"))
            and all(item["target_id"] == "fixture" for item in result.bindings)
            and not result.execution_available)
        report["resources_preserve_transaction"] = conn.in_transaction() and conn.scalar(text("SHOW statement_timeout")) == before
        conn.execute(text("SET LOCAL temp_tablespaces='qt_fixture_history'"))
        result = observe_header_resources(conn, targets, pg_controldata=binary)
        spools = {item["tablespace_oid"]: item for item in result.bindings if item["role"] == "temporary_files"}
        report["resources_custom_and_fallback"] = (
            set(spools) == {1663, destination_oid}
            and spools[1663]["target_id"] == "fixture"
            and spools[destination_oid]["target_id"] == "history")
        conn.execute(text("CREATE TEMP TABLE resource_temp_probe(value integer) ON COMMIT DROP"))
        conn.execute(text("INSERT INTO resource_temp_probe VALUES (1)"))
        location = conn.scalar(text("SELECT pg_relation_filepath('resource_temp_probe'::regclass)"))
        report["resources_real_temp_allocation"] = location.startswith(f"pg_tblspc/{destination_oid}/")
        result = observe_header_resources(conn, targets, pg_controldata=binary)
        report["resources_existing_temp_directory"] = any(
            item["role"] == "temporary_relations" and item["tablespace_oid"] == destination_oid
            and item["exists"] for item in result.bindings)
        assert conn.scalar(text("SELECT value FROM resource_temp_probe")) == 1

    # The raw GUC remains stale after rename; observe must refuse, not guess.
    # Quote/comma normalization is then exercised by PostgreSQL parse_ident.
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.execute(text("SET LOCAL temp_tablespaces='qt_fixture_history'"))
            quoted_name = 'history, "quoted"'
            identifier = engine.dialect.identifier_preparer.quote_identifier(quoted_name)
            conn.exec_driver_sql("ALTER TABLESPACE qt_fixture_history RENAME TO " + identifier)
            try:
                with conn.begin_nested():
                    observe_header_resources(conn, targets, pg_controldata=binary)
            except StorageMountError as exc:
                report["resources_stale_temp_name_refused"] = "missing or inaccessible" in str(exc)
            else:
                report["resources_stale_temp_name_refused"] = False
            conn.execute(text("SELECT set_config('temp_tablespaces',:setting,true)"), {"setting": identifier})
            result = observe_header_resources(conn, targets, pg_controldata=binary)
            report["resources_quoted_temp_name"] = any(
                item["role"] == "temporary_files" and item["tablespace_oid"] == destination_oid
                for item in result.bindings)
        finally:
            transaction.rollback()
    return report

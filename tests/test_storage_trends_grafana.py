"""Source contracts; actual grants/session deadlines need disposable DB proof."""
import json
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
UID = "quanttrad-storage-metrics"


def dashboard():
    path = ROOT / "docker/grafana/storage-metrics/capacity-database-growth.json"
    data = json.loads(path.read_text())
    return data, {panel["id"]: panel for panel in data["panels"]}


def test_metrics_datasource_requires_explicit_enable_without_broad_credentials():
    disabled = yaml.safe_load((ROOT / "docker/grafana/provisioning/datasources/storage-metrics.yml").read_text())
    assert disabled == {"apiVersion": 1, "datasources": []}
    enabled = yaml.safe_load((ROOT / "docker/grafana/storage-metrics/enabled.yml").read_text())
    source, = enabled["datasources"]
    assert source["uid"] == UID and source["type"] == "postgres"
    assert source["user"] == "qt_storage_metrics_grafana"
    assert source["url"] == "${QT_STORAGE_METRICS_DB_URL}"
    assert source["secureJsonData"] == {"password": "$QT_STORAGE_METRICS_DB_PASSWORD"}
    assert source["editable"] is False
    pool = source["jsonData"]
    assert pool["maxOpenConns"] == pool["maxIdleConns"] == 2
    assert pool["maxIdleConnsAuto"] is False
    assert 0 < pool["connMaxLifetime"] <= 300
    assert pool["database"] == "${POSTGRES_DB}"
    assert "POSTGRES_PASSWORD" not in json.dumps(enabled)
    assert "POSTGRES_USER" not in json.dumps(enabled)
    legacy = yaml.safe_load((ROOT / "docker/grafana/provisioning/datasources/postgres.yml").read_text())
    assert legacy["datasources"][0]["uid"] == "quanttrad-postgres"
    assert legacy["datasources"][0]["user"] == "${POSTGRES_USER}"


def test_current_metadata_has_no_fake_history_or_zero_fill():
    _, panels = dashboard()
    for panel_id in (28, 29, 31):
        panel = panels[panel_id]
        assert panel["datasource"]["uid"] == UID
        assert "CURRENT" in panel["description"]
        sql = panel["targets"][0]["rawSql"]
        assert "observability_metrics.storage_metric_status_v1()" in sql
        assert "${__from}" not in sql and "${__to}" not in sql
        assert "state = 'ready' AND complete AND NOT truncated" in sql
        assert "WHERE false" not in sql
        if panel_id in (28, 29):
            assert "coalesce(reason, \'\')" in sql
        else:
            assert "coalesce" not in sql.lower()
    assert "eligible_backlog_physical_bytes" in panels[28]["targets"][0]["rawSql"]
    assert "verified_guarded_physical_bytes" in panels[29]["targets"][0]["rawSql"]
    assert "oldest_day" in panels[31]["targets"][0]["rawSql"]
    assert "state, reason, complete, truncated" in panels[31]["targets"][0]["rawSql"]


def test_completed_byte_lanes_are_distinct_not_attempts_or_net_physical_growth():
    _, panels = dashboard()
    panel = panels[30]
    sql = panel["targets"][0]["rawSql"]
    for metric in ("archive_compressed_bytes", "verified_first_compressed_bytes",
                   "reclaimed_physical_hot_bytes"):
        assert "storage_series_v1('" + metric + "'" in sql
    assert sql.count("storage_series_v1(") == 3
    assert sql.count("state = 'ready' AND complete AND NOT truncated") == 3
    assert panel["fieldConfig"]["defaults"]["unit"] == "bytes"
    assert panel["fieldConfig"]["defaults"]["custom"]["spanNulls"] is False
    assert panel["options"]["legend"]["calcs"] == []
    assert "COALESCE" not in sql and "ELSE 0" not in sql
    assert "SUM(" not in sql.upper()


def test_shared_queries_use_utc_boundaries_whitelist_and_visible_status():
    data, panels = dashboard()
    assert data["uid"] == "quanttrad-capacity-growth"
    assert data["timezone"] == "utc"
    for panel_id in (2, 3, 4, 6, 7, 30, 32):
        panel = panels[panel_id]
        sql = " ".join(target["rawSql"] for target in panel["targets"])
        assert panel["datasource"]["uid"] == UID
        assert "to_timestamp(${__from}/1000.0)" in sql
        assert "to_timestamp(${__to}/1000.0)" in sql
        assert "THEN 300" in sql and "THEN 1800" in sql and "ELSE 7200 END" in sql
        assert "${schema" not in sql and "${relation" not in sql
        for table in ("fact_retention_partitions", "fact_archive_manifests",
                      "fact_archive_verifications", "pg_catalog", "set_config"):
            assert table not in sql
    for panel_id in (2, 3, 4):
        assert panels[panel_id]["options"]["reduceOptions"]["calcs"] == ["last"]
    for panel_id in (6, 7):
        assert panels[panel_id]["fieldConfig"]["defaults"]["custom"]["spanNulls"] is False
    sql = panels[32]["targets"][0]["rawSql"]
    assert panels[32]["type"] == "table"
    assert "state, reason, complete, truncated" in sql
    assert "storage_metric_status_v1()" in sql
    assert sql.count("storage_series_v1(") == 5
    assert "storage_growth_v1(" in sql
    assert "database_growth_bytes_per_day" in sql
    assert "unconfigured/unavailable" in panels[1]["options"]["content"]


def test_existing_diagnostics_preserve_datasource_and_semantics():
    data, panels = dashboard()
    for panel_id in (8, 9, 10, 11, 12, 13, 14, 15):
        assert panels[panel_id]["datasource"]["uid"] == "quanttrad-postgres"
        assert panels[panel_id]["targets"]
    for panel_id in (25, 26):
        assert panels[panel_id]["datasource"]["uid"] == "quanttrad-loki"
    assert data["refresh"] == "15s"


def test_growth_uses_shared_versioned_formula_with_explicit_unit_conversion():
    _, panels = dashboard()
    sql = panels[3]["targets"][0]["rawSql"]
    assert "storage_growth_v1(\'database_allocated_bytes\'" in sql
    assert "value / 86400.0" in sql
    assert "LAG(" not in sql.upper()
    assert "ELSE 0" not in sql

def test_default_dashboard_keeps_working_legacy_until_explicit_activation():
    path = ROOT / "docker/grafana/provisioning/dashboards/capacity-database-growth.json"
    legacy = json.loads(path.read_text())
    enabled, _ = dashboard()
    panels = {panel["id"]: panel for panel in legacy["panels"]}
    assert legacy["uid"] == enabled["uid"] == "quanttrad-capacity-growth"
    for panel_id in (2, 3, 4, 6, 7):
        panel = panels[panel_id]
        assert panel["datasource"]["uid"] == "quanttrad-postgres"
        assert "observability_metrics.database_" in panel["targets"][0]["rawSql"]
    assert "quanttrad-storage-metrics" not in path.read_text()
    assert "Explicitly enabled" in enabled["description"]
    for panel_id in (28, 29, 30, 31):
        assert "bounded_storage_projection_unavailable" in panels[panel_id]["targets"][0]["rawSql"]

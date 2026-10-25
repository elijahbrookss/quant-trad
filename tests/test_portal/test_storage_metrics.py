from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from contextlib import contextmanager

import pytest
from sqlalchemy.exc import OperationalError

from portal.backend.service.storage.metrics import (
    METRICS, StorageMetrics, _series, grafana_link, metric_range,
)

NOW = datetime(2026,1,31,tzinfo=UTC)


@pytest.mark.parametrize("resolution,bucket",[("auto",7200),("2h",7200)])
def test_full_retention_range_is_capped(resolution,bucket):
    assert metric_range(NOW-timedelta(days=30),NOW,resolution)[2] == bucket


@pytest.mark.parametrize("start,end,resolution",[
    (NOW,NOW,"auto"),(NOW-timedelta(days=31),NOW,"auto"),
    (NOW-timedelta(days=30),NOW,"5m"),
    (NOW.replace(tzinfo=None),NOW,"auto"),
    (NOW-timedelta(hours=1),NOW,"arbitrary_sql"),
])
def test_invalid_ranges_never_reach_sql(start,end,resolution):
    with pytest.raises(ValueError): metric_range(start,end,resolution)


@pytest.mark.parametrize("url",["javascript:alert(1)","https://u:p@example.test/d/x",
    "https://example.test/d/x?token=secret","https://example.test/\nfoo"])
def test_grafana_link_rejects_credentials_script_and_unbounded_query(url):
    assert grafana_link(url)["dashboard_url"] is None


def test_static_grafana_link_keeps_operator_selected_origin():
    assert grafana_link("https://example.test/grafana/d/storage") == {
        "state":"available","dashboard_url":"https://example.test/grafana/d/storage","reason":None}


def row(time,value,state="ready",reason=None,complete=True,truncated=False):
    return dict(time=time,value=value,state=state,reason=reason,complete=complete,
                truncated=truncated,source_count=2)


def test_stock_gaps_preserve_negative_growth_without_zero_fill():
    rows=[row(NOW-timedelta(minutes=15),100),row(NOW-timedelta(minutes=10),None,"unavailable","no_sample"),
          row(NOW-timedelta(minutes=5),75)]
    result=_series("database_allocated_bytes",rows,start=NOW-timedelta(minutes=15),end=NOW,
                   bucket=300,now=NOW)
    assert result["state"]=="partial"
    assert result["points"][1]["value"] is None
    assert result["summary"]["net_change_bytes"]==-25
    assert result["summary"]["total_bytes"] is None
    assert result["coverage"]["complete"] is False


def test_flow_last_event_does_not_become_sampler_staleness():
    result=_series("archive_compressed_bytes",[row(NOW-timedelta(hours=3),123)],
                   start=NOW-timedelta(hours=4),end=NOW,bucket=300,now=NOW)
    assert result["state"]=="ready"
    assert result["summary"]["total_bytes"]==123
    assert result["summary"]["net_change_bytes"] is None


def test_truncated_source_never_gets_authoritative_total():
    result=_series("verified_first_compressed_bytes",[row(NOW,None,"partial","source_limit",False,True)],
                   start=NOW-timedelta(hours=1),end=NOW,bucket=300,now=NOW)
    assert result["coverage"]["truncated"] is True
    assert result["summary"]["total_bytes"] is None


def test_database_failure_returns_independent_unavailable_metrics():
    class Database:
        @contextmanager
        def session(self):
            raise OperationalError("synthetic",{},Exception("synthetic"))
            yield
    result=StorageMetrics(Database(),settings=lambda:SimpleNamespace(storage=SimpleNamespace()),
                          clock=lambda:NOW).snapshot(start=NOW-timedelta(hours=1),end=NOW)
    assert result["schema_version"]=="qt.storage_metrics.v1"
    assert [r["metric_id"] for r in result["series"]]==list(METRICS)
    assert all(r["state"]=="unavailable" and r["summary"]["total_bytes"] is None for r in result["series"])
    assert all(r["value"] is None for r in result["stats"])


def test_summary_uses_actual_sample_span_and_preserves_raw_gap_flag():
    first=row(NOW-timedelta(hours=1),100)
    first["observed_at"]=NOW-timedelta(minutes=54)
    last=row(NOW-timedelta(minutes=30),160,"partial","raw_sample_gap",False)
    last["observed_at"]=NOW-timedelta(minutes=21)
    last["max_gap_seconds"]=1200
    result=_series("database_allocated_bytes",[first,last],start=NOW-timedelta(hours=1),
                   end=NOW-timedelta(minutes=20),bucket=1800,now=NOW)
    assert result["summary"]["first_at"]==(NOW-timedelta(minutes=54)).isoformat()
    assert result["summary"]["last_at"]==(NOW-timedelta(minutes=21)).isoformat()
    assert result["summary"]["average_bytes_per_day"]==pytest.approx(60*86400/(33*60))
    assert result["state"]=="partial"
    assert result["points"][-1]["max_gap_seconds"]==1200


def test_manual_function_installation_matches_code_owned_clean_definitions():
    from pathlib import Path
    from portal.backend.db import storage_metrics_schema as schema
    sql=(Path(__file__).parents[2]/"scripts/db/manual_migration_storage_metrics_v1.sql").read_text()
    assert 'RETURNS TABLE("time" timestamptz' in schema.STORAGE_SERIES_SQL
    assert 'RETURNS TABLE("time" timestamptz' in schema.STORAGE_GROWTH_SQL
    for source in (schema.STORAGE_ADMISSION_SQL,schema.STORAGE_SERIES_SQL,schema.STORAGE_STATUS_SQL,schema.STORAGE_GROWTH_SQL):
        assert source in sql
    for signature in schema.SIGNATURES:
        assert f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC" in sql


def test_route_rejects_extra_and_duplicate_query_arguments(monkeypatch):
    from fastapi import HTTPException
    from starlette.requests import Request
    from portal.backend.controller import storage_management as controller
    for query in (b"from=x&to=y&source_id=private", b"from=x&from=y&to=z"):
        request=Request({"type":"http","query_string":query,"headers":[]})
        with pytest.raises(HTTPException) as raised:
            controller.storage_metric_history(request=request,from_time="x",to_time="y",resolution="auto")
        assert raised.value.status_code==422

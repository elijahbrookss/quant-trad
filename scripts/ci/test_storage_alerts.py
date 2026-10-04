"""Prove the production storage rules against disposable local Loki/Grafana.

No SMTP, host mounts, production credentials, or external endpoints. The unique
Compose project and its data volumes are removed even if the proof fails.
"""

from __future__ import annotations

import base64
import copy
import json
import math
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
RESOURCES = ("docker-engine-storage", "market-archive-storage")
RULES = (
    "qt-storage-warning", "qt-storage-critical", "qt-storage-emergency",
    "qt-nvme-storage-unavailable", "qt-archive-storage-unavailable",
)


def request(url: str, payload: dict | None = None, *, auth: bool = False) -> Any:
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(b"admin:storage-test-only").decode()
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(
            f"storage_alert_test_http_error: status={exc.code} detail={detail}"
        ) from exc
    return json.loads(raw) if raw else None


def wait_ready(url: str) -> None:
    # Fresh Grafana images migrate a large SQLite schema. Keep initialization
    # bounded without making slow local Docker disks look like alert failures.
    started = time.monotonic()
    deadline = started + 600
    next_progress = started + 30
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
        if time.monotonic() >= next_progress:
            print(f"storage_alert_test_initializing url={url} elapsed_seconds={int(time.monotonic() - started)}", flush=True)
            next_progress = time.monotonic() + 30
        time.sleep(1)
    raise RuntimeError(f"storage_alert_test_not_ready: url={url} error={last_error}")


def values(result: dict, ref: str) -> dict[str, float]:
    series = result.get("results", {}).get(ref, {})
    if series.get("error"):
        raise AssertionError(f"storage_alert_query_failed: ref={ref} error={series['error']}")
    observed = {}
    for frame in series.get("frames", []):
        for index, field in enumerate(frame["schema"]["fields"]):
            if field["type"] != "number":
                continue
            rows = frame["data"]["values"][index]
            if rows:
                key = field.get("labels", {}).get("resource_id", "unlabelled")
                if key in observed:
                    raise AssertionError(f"storage_alert_duplicate_series: ref={ref} resource={key}")
                observed[key] = float(rows[-1])
    return observed


def evaluate(grafana: str, rule: dict, timestamp: float, *, expression: bool = True) -> dict:
    queries = []
    for item in rule["data"]:
        if item["refId"] != "A" and not expression:
            continue
        query = copy.deepcopy(item["model"])
        query["refId"] = item["refId"]
        query["datasource"] = {"uid": item["datasourceUid"], "type": "loki" if item["refId"] == "A" else "__expr__"}
        query["intervalMs"] = 1000
        query["maxDataPoints"] = 1000
        queries.append(query)
    return request(
        grafana + "/api/ds/query",
        {"from": str(int((timestamp - 600) * 1000)), "to": str(int(timestamp * 1000)), "queries": queries},
        auth=True,
    )


def prove(grafana: str, loki: str) -> None:
    loaded = request(grafana + "/api/v1/provisioning/alert-rules", auth=True)
    rules = {rule["uid"]: rule for rule in loaded}
    assert set(rules) == set(RULES), rules.keys()
    for uid in RULES[3:]:
        assert rules[uid]["noDataState"] == "Alerting"
        assert rules[uid]["execErrState"] == "Alerting"
    cases = [(0, None), (69.99, None), (70, "warning"), (84.99, "warning"), (85, "critical"), (91.99, "critical"), (92, "emergency"), (100, "emergency")]
    base = int(time.time()) - 7200
    entries = []

    def add(timestamp: int, resource: str, kind: str, **fields: Any) -> None:
        payload = {"resource_id": resource, "sample_kind": kind, "physical_host_visible": True, **fields}
        entries.append([str(timestamp * 1_000_000_000 + len(entries)), json.dumps(payload)])

    for index, (percent, _) in enumerate(cases):
        timestamp = base + index * 180
        for resource in RESOURCES:
            add(timestamp, resource, "filesystem", used_percent=percent)
            add(timestamp, resource, "storage_health", available=1)
    failed_time = base + len(cases) * 180
    add(failed_time, RESOURCES[0], "storage_health", available=1)
    add(failed_time, RESOURCES[1], "storage_health", available=0)
    missing_time = failed_time + 180
    add(missing_time, RESOURCES[0], "storage_health", available=1)
    guest_time = missing_time + 180
    add(guest_time, RESOURCES[1], "storage_health", available=1, physical_host_visible=False)
    request(loki + "/loki/api/v1/push", {"streams": [{"stream": {"compose_service": "docker-stats"}, "values": entries}]})

    for index, (percent, severity) in enumerate(cases):
        timestamp = base + index * 180 + 1
        for uid in RULES[:3]:
            result = evaluate(grafana, rules[uid], timestamp)
            assert values(result, "A") == {resource: percent for resource in RESOURCES}, result
            expected = float(uid == f"qt-storage-{severity}")
            assert values(result, "C") == {resource: expected for resource in RESOURCES}, result
        print(f"storage_alert_threshold_proved percent={percent} severity={severity}", flush=True)
    for uid, resource, expected in zip(RULES[3:], RESOURCES, (0.0, 1.0), strict=True):
        result = evaluate(grafana, rules[uid], failed_time + 1)
        assert values(result, "C") == {resource: expected}, result
    missing = evaluate(grafana, rules[RULES[4]], missing_time + 1, expression=False)
    assert values(missing, "A") == {}, missing
    healthy = evaluate(grafana, rules[RULES[3]], missing_time + 1)
    assert values(healthy, "C") == {RESOURCES[0]: 0.0}, healthy
    guest = evaluate(grafana, rules[RULES[4]], guest_time + 1, expression=False)
    assert values(guest, "A") == {}, guest
    print("storage_alert_health_proved failure=per-disk missing=Alerting guest=not-physical", flush=True)


def prove_capacity_dashboard(grafana: str, loki: str) -> None:
    """Exercise the shipped native runway queries with physical and missing evidence."""
    dashboard = json.loads(
        (ROOT / "docker/grafana/provisioning/dashboards/capacity-database-growth.json").read_text()
    )
    panels = {panel["id"]: panel for panel in dashboard["panels"]}
    base = int(time.time()) - 60
    entries = []

    def add(at: int, resource: str, *, free: int, uuid: str = "disk-a",
            total: int = 1000, physical: bool = True, health: int | None = 1) -> None:
        common = {"resource_id": resource, "physical_host_visible": physical}
        for payload in (
            {**common, "sample_kind": "filesystem", "filesystem_uuid": uuid,
             "device_id": "8:1", "total_bytes": total, "used_bytes": total - free,
             "available_bytes": free},
            {**common, "sample_kind": "storage_health", "available": health},
        ):
            if payload["sample_kind"] == "storage_health" and health is None:
                continue
            entries.append([str(at * 1_000_000_000 + len(entries)), json.dumps(payload)])

    for resource, old, new in (
        ("growing", 250, 200), ("falling", 200, 250),
        ("flat", 200, 200), ("above-threshold", 150, 100),
    ):
        add(base - 86400, resource, free=old)
        add(base, resource, free=new)
    add(base - 86400, "replaced", free=250, uuid="old-disk")
    add(base, "replaced", free=200, uuid="new-disk")
    add(base - 86400, "resized", free=250)
    add(base, "resized", free=200, total=2000)
    add(base, "missing-history", free=200)
    add(base - 86400, "stale", free=250)
    add(base - 180, "stale", free=200)
    for resource, physical, uuid, health in (
        ("guest", False, "disk-a", 1), ("unknown-uuid", True, "", 1),
        ("unhealthy", True, "disk-a", 0), ("missing-health", True, "disk-a", None),
    ):
        add(base - 86400, resource, free=250, physical=physical, uuid=uuid)
        add(base, resource, free=200, physical=physical, uuid=uuid, health=health)
    entries.sort(key=lambda row: int(row[0]))
    request(loki + "/loki/api/v1/push", {"streams": [{
        "stream": {"compose_service": "docker-stats", "fixture": "capacity-runway"},
        "values": entries,
    }]})

    for panel_id, expected in (
        (25, {"growing": 50, "falling": -50, "flat": 0, "above-threshold": 50}),
        (26, {"growing": 1, "above-threshold": -1}),
    ):
        target = copy.deepcopy(panels[panel_id]["targets"][0])
        target.update(datasource=panels[panel_id]["datasource"], intervalMs=1000,
                      maxDataPoints=1)
        result = request(grafana + "/api/ds/query", {
            "from": str((base - 120) * 1000), "to": str((base + 1) * 1000),
            "queries": [target],
        }, auth=True)
        actual = values(result, "A")
        assert actual.keys() == expected.keys(), (panel_id, actual, expected)
        assert all(math.isclose(actual[key], value, rel_tol=1e-8, abs_tol=1e-8)
                   for key, value in expected.items()), (panel_id, actual, expected)
    print("storage_capacity_dashboard_proved growth=24h signed=True "
          "runway=85-percent missing-stale-replaced-resized-guest-unhealthy=unavailable",
          flush=True)


def main() -> int:
    # A caller's remote Docker context must never turn a test into a deployment.
    context = json.loads(subprocess.check_output(["docker", "context", "inspect"], text=True))[0]
    endpoint = os.environ.get("DOCKER_HOST") or context["Endpoints"]["docker"]["Host"]
    if not endpoint.startswith(("unix://", "npipe://")):
        raise RuntimeError(f"storage_alert_test_requires_local_docker: endpoint={endpoint}")
    project = "qt-storage-alert-test-" + uuid.uuid4().hex[:12]
    compose = ["docker", "compose", "--project-name", project, "--file", str(ROOT / "docker/test/storage-alerting.compose.yml")]
    print(f"storage_alert_test_start project={project}", flush=True)
    try:
        subprocess.run([*compose, "up", "--detach", "--quiet-pull"], check=True, timeout=600)
        def endpoint_for(service: str, port: int) -> str:
            address = subprocess.check_output([*compose, "port", service, str(port)], text=True).strip()
            if not address.startswith("127.0.0.1:"):
                raise RuntimeError(f"storage_alert_test_requires_loopback: address={address}")
            return "http://" + address
        grafana = endpoint_for("grafana", 3000)
        loki = endpoint_for("loki", 3100)
        wait_ready(grafana + "/api/health")
        wait_ready(loki + "/ready")
        prove(grafana, loki)
        prove_capacity_dashboard(grafana, loki)
    except BaseException:
        subprocess.run([*compose, "logs", "--tail", "80"], check=False, timeout=30)
        raise
    finally:
        subprocess.run([*compose, "down", "--volumes", "--remove-orphans"], check=True, timeout=90)
    print("storage_alert_test_passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

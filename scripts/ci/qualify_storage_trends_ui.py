"""Hosted Chromium qualification of the built Storage UI using synthetic data only.

Install playwright==1.56.0 and its Chromium browser in the GitHub job, not in
the application environment. No backend, Docker, credentials or product services
are started. Every API response is intercepted; external requests never leave
the browser. The owned loopback static server and browser are closed in finally.

python scripts/ci/qualify_storage_trends_ui.py --dist portal/frontend/dist \
  --artifacts "$RUNNER_TEMP/storage-trends-ui" --source-revision "$GITHUB_SHA" \
  --confirm-synthetic
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from functools import partial
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import re
import sys
import subprocess
from threading import Thread
from urllib.parse import parse_qs, urlsplit

PLAYWRIGHT_VERSION = "1.56.0"
SYNTHETIC_NOW = datetime(2026, 11, 1, 8, tzinfo=UTC)
GRAFANA = "https://grafana.example.invalid/d/quanttrad-capacity-growth/quanttrad-capacity-and-database-growth"
METRICS = (
    ("database_allocated_bytes", "sampled_database_allocation"),
    ("market_allocated_bytes", "sampled_market_schema_allocation"),
    ("archive_compressed_bytes", "acknowledged_compressed_manifest_bytes"),
    ("verified_first_compressed_bytes", "first_qualified_verification_compressed_bytes"),
    ("reclaimed_physical_hot_bytes", "reclaimed_physical_partition_bytes"),
)


def require(condition, label):
    if not condition:
        raise AssertionError(label)


def iso(value):
    return value.astimezone(UTC).isoformat()


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0), "fixture_requires_utc")
    return parsed


def storage_fixture(sequence):
    policy = {"schema_version": "qt.storage_policy.v1", "recent": ["synthetic-ssd"],
        "history": ["synthetic-hdd"], "archives": ["synthetic-hdd"], "backups": ["synthetic-hdd"],
        "recent_days": 30, "reserve_percent": 20, "backup_interval_hours": 24,
        "backup_copies": 2, "movement_enabled": False, "backup_enabled": False}
    targets = [{"target_id": "synthetic-" + medium, "label": "Synthetic " + medium.upper(),
        "medium": medium, "roles": ["recent", "history", "archives", "backups"],
        "state": "active", "status": "available",
        "capacity": {"total_bytes": 1024**4, "available_bytes": 768 * 1024**3}}
        for medium in ("ssd", "hdd")]
    return {"revision": sequence, "policy": policy, "targets": targets, "candidates": [],
        "active_plan": None, "health": "available", "movement": {"state": "ready"},
        "backup": {"state": "unknown"}, "retention": {
            "state": "partial", "sampled_at": iso(SYNTHETIC_NOW), "policy": {},
            "completeness": {"complete": False, "truncated": False, "unknowns": ["synthetic_gap"]},
            "lifecycle": {"current": {"name": "canonical_retention", "state": "running",
                "progress": {"action": "verify_page"}},
                "last_durable_advancement": {"stage": "archive_page_committed",
                    "happened_at": iso(SYNTHETIC_NOW - timedelta(minutes=5))}},
            "backlog": {"eligible_source_bytes": 8 * 1024**3, "eligible_partitions": 4,
                "oldest_eligible_age_days": 40, "verified_guarded_source_bytes": 2 * 1024**3,
                "verified_guarded_partitions": 1},
            "storage": {"physical_hot": {"source_bytes": 80 * 1024**3, "estimated_rows": 1000},
                "compressed_archive": {"bytes": 16 * 1024**3, "verified_bytes": 12 * 1024**3}},
            "recovery_floor": {"state": "above_floor", "available_bytes": 768 * 1024**3,
                "margin_bytes": 512 * 1024**3}}}


def metrics_fixture(query, mode):
    require(set(query) == {"from", "to", "resolution"}, "metrics_query_allowlist")
    require(all(len(values) == 1 for values in query.values()), "metrics_query_unique")
    require(query["resolution"] == ["auto"], "metrics_query_auto_resolution")
    start, end = timestamp(query["from"][0]), timestamp(query["to"][0])
    seconds = (end - start).total_seconds()
    require(0 < seconds <= 30 * 86400, "metrics_bounded_range")
    bucket = next(value for value in (300, 1800, 7200) if math.ceil(seconds / value) <= 720)
    count = math.ceil(seconds / bucket)
    series = []
    for index, (metric_id, basis) in enumerate(METRICS):
        stock = index < 2
        points = []
        for ordinal in range(count):
            at = start + timedelta(seconds=ordinal * bucket)
            observed = min(at + timedelta(seconds=60), end - timedelta(milliseconds=1))
            missing = mode == "unavailable" or (stock and ordinal == count // 2) or (not stock and ordinal % 4 != 0)
            value = None if missing else ((64 if index == 0 else 24) * 1024**3
                + ordinal * 1024**2 if stock else (index + 1) * 1024**2)
            gap = stock and ordinal == count // 2 + 1
            points.append({"time": iso(at), "value": value,
                "state": "unavailable" if missing else "partial" if gap else "ready",
                "reason": ("no_sample" if stock else "no_completion_record") if missing else "raw_sample_gap" if gap else None,
                "observed_at": None if missing else iso(observed),
                "max_gap_seconds": 700 if gap else 300 if stock else None})
        good = [point for point in points if point["value"] is not None]
        summary = {"first_at": good[0]["observed_at"] if good else None,
            "last_at": good[-1]["observed_at"] if good else None,
            "net_change_bytes": None, "average_bytes_per_day": None, "total_bytes": None}
        if len(good) > 1 and stock:
            summary["net_change_bytes"] = good[-1]["value"] - good[0]["value"]
            span = (timestamp(summary["last_at"]) - timestamp(summary["first_at"])).total_seconds()
            summary["average_bytes_per_day"] = summary["net_change_bytes"] * 86400 / span
        elif good and not stock:
            summary["total_bytes"] = sum(point["value"] for point in good)
        state = "unavailable" if not good else "stale" if mode == "stale" and stock else "partial" if stock else "ready"
        series.append({"metric_id": metric_id, "unit": "bytes", "basis": basis, "state": state,
            "reason": "no_observations" if not good else "capacity_sample_stale" if state == "stale" else "raw_sample_gap" if stock else None,
            "points": points, "coverage": {"complete": not stock and bool(good), "truncated": False,
                "source_count": len(good), "source_limit": 10000,
                "missing_buckets": count - len(good)}, "summary": summary})
    return {"schema_version": "qt.storage_metrics.v1",
        "generated_at": iso(SYNTHETIC_NOW - timedelta(seconds=901 if mode == "stale" else 0)),
        "range": {"from": iso(start), "to": iso(end), "bucket_seconds": bucket},
        "cadence_seconds": 300, "retention_days": 30, "series": series, "stats": [],
        "links": {"grafana": {"state": "available", "dashboard_url": GRAFANA, "reason": None}}}


class BuiltFrontend(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        if path.startswith("/api/"):
            self.send_error(404)
            return
        target = Path(self.translate_path(path))
        if path == "/" or (not target.is_file() and not Path(path).suffix):
            self.path = "/v2.html"
        super().do_GET()

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


class SyntheticAPI:
    def __init__(self, origin):
        self.origin = origin
        self.mode = "partial"
        self.status_reads = 0
        self.metrics = []
        self.mutations = 0
        self.unexpected = []
        self.page_errors = []
        self.last_payload = None

    def route(self, route):
        request = route.request
        parsed = urlsplit(request.url)
        if parsed.path.startswith("/api/"):
            if not request.url.startswith(self.origin + "/"):
                self.unexpected.append("external_api_origin")
                route.fulfill(status=403, json={"detail": "Owned loopback API only"})
            elif request.method != "GET":
                self.mutations += 1
                route.fulfill(status=405, json={"detail": "synthetic_read_only"})
            elif parsed.path == "/api/health":
                route.fulfill(json={"status": "ok"})
            elif parsed.path == "/api/research/browse":
                route.fulfill(json={"items": [], "total": 0, "next_cursor": None})
            elif parsed.path == "/api/storage":
                self.status_reads += 1
                route.fulfill(json=storage_fixture(self.status_reads))
            elif parsed.path == "/api/storage/metrics":
                self.metrics.append(parse_qs(parsed.query, keep_blank_values=True))
                if self.mode == "error":
                    route.fulfill(status=503, json={"detail": "Synthetic metrics failure"})
                else:
                    self.last_payload = metrics_fixture(self.metrics[-1], self.mode)
                    route.fulfill(json=self.last_payload)
            else:
                self.unexpected.append("api_route")
                route.fulfill(status=404, json={"detail": "Unexpected synthetic API route"})
        elif request.url.startswith(self.origin + "/"):
            route.continue_()
        elif parsed.hostname in {"fonts.googleapis.com", "fonts.gstatic.com"}:
            route.fulfill(content_type="text/css", body="")
        elif parsed.hostname == "grafana.example.invalid":
            route.fulfill(content_type="text/html", body="<title>Synthetic Grafana</title><p>Synthetic range target</p>")
        else:
            self.unexpected.append("external_route")
            route.abort()

    def clean(self):
        require(self.mutations == 0, "no_api_mutations")
        require(not self.unexpected, "no_unexpected_network")
        require(not self.page_errors, "no_uncaught_browser_errors")


def check_layout(page, *, painted=True):
    require(page.evaluate("document.documentElement.scrollWidth <= window.innerWidth + 1"), "page_horizontal_overflow")
    for selector in (".qt2-main", ".qt2-storage", ".qt2-storage-trends"):
        require(page.locator(selector).evaluate("(el) => el.scrollWidth <= el.clientWidth + 1"), "storage_horizontal_overflow")
    hosts = page.locator(".qt2-storage-trend-chart")
    require(hosts.count() == 4, "four_native_charts")
    for host in hosts.all():
        host.scroll_into_view_if_needed()
        box = host.bounding_box()
        require(box is not None and box["width"] >= 120 and box["height"] >= 90, "chart_visible_geometry")
        require(host.locator("canvas").count() > 0, "real_chart_canvas")
        if painted:
            page.wait_for_function("""(el) => [...el.querySelectorAll('canvas')].some(c => {
              if (!c.width || !c.height) return false;
              const pixels = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
              for (let i = 3; i < pixels.length; i += 4) if (pixels[i] !== 0) return true;
              return false;
            })""", arg=host.element_handle())


def assert_link(page, payload):
    link = page.get_by_role("link", name="Open in Grafana", exact=False)
    href = link.get_attribute("href")
    parsed = urlsplit(href)
    params = parse_qs(parsed.query)
    require(parsed.scheme == "https" and parsed.netloc == "grafana.example.invalid", "trusted_grafana_origin")
    require(parsed.path == urlsplit(GRAFANA).path, "fixed_grafana_dashboard")
    require(params == {"from": [str(int(timestamp(payload["range"]["from"]).timestamp() * 1000))],
        "to": [str(int(timestamp(payload["range"]["to"]).timestamp() * 1000))],
        "timezone": ["utc"]}, "grafana_exact_displayed_bounds")
    require(link.get_attribute("target") == "_blank", "grafana_new_tab")
    require(set(link.get_attribute("rel").split()) >= {"noopener", "noreferrer"}, "grafana_no_opener")
    return href


def run_viewport(browser, origin, artifact_dir, name, viewport, mobile, expect):
    result = {"status": "running", "checks": [], "screenshots": [], "api": {}}
    context = browser.new_context(viewport=viewport, is_mobile=mobile, has_touch=mobile,
        timezone_id="America/New_York", locale="en-US", reduced_motion="reduce", service_workers="block")
    api = SyntheticAPI(origin)
    context.route("**/*", api.route)
    page = context.new_page()
    page.on("pageerror", lambda error: api.page_errors.append(type(error).__name__))
    page.set_default_timeout(15000)
    stage = "navigation"
    try:
        page.clock.install(time=SYNTHETIC_NOW)
        page.goto(origin + "/studies", wait_until="domcontentloaded")
        expect(page.get_by_role("heading", name="Studies", exact=True)).to_be_visible()
        settings = page.locator('summary[aria-label="Settings"]')
        if mobile:
            settings.tap()
        else:
            settings.focus()
            settings.press("Enter")
        page.get_by_role("link", name="Storage", exact=True).click()
        expect(page).to_have_url(re.compile(r"/settings/storage$"))
        expect(page.get_by_role("heading", name="Storage trends", exact=True)).to_be_visible()
        expect(page.get_by_role("link", name="Open in Grafana", exact=False)).to_be_visible()
        expect(page.get_by_text("canonical retention", exact=True)).to_be_visible()
        expect(page.get_by_text(re.compile("Current .* Storage:"))).to_be_visible()
        result["checks"].append("settings_storage_navigation")

        stage = "charts_and_accessibility"
        check_layout(page)
        expect(page.get_by_text(re.compile("Market schema is a subset"))).to_be_visible()
        expect(page.get_by_text(re.compile("Separate byte bases and scales"))).to_be_visible()
        expect(page.get_by_text(re.compile("Coverage partial")).first).to_be_visible()
        require(page.locator("iframe").count() == 0, "no_grafana_iframe")
        table_summary = page.get_by_text("Accessible data table · UTC · bytes", exact=True)
        table_summary.focus()
        table_summary.press("Enter")
        region = page.get_by_role("region", name="Scrollable storage trends data")
        expect(region).to_be_visible()
        require(region.get_attribute("tabindex") == "0", "keyboard_data_region")
        expect(region.get_by_role("cell", name="Unavailable", exact=True).first).to_be_visible()
        table_summary.press("Enter")
        page.locator(".qt2-storage-trends").screenshot(path=str(artifact_dir / (name + "-trends.png")))
        result["screenshots"].append(name + "-trends.png")
        page.screenshot(path=str(artifact_dir / (name + "-storage.png")), full_page=True)
        result["screenshots"].append(name + "-storage.png")
        result["checks"].append("native_canvas_mobile_layout_keyboard_table")

        stage = "range_presets"
        for preset, seconds in (("6h", 21600), ("7d", 604800), ("30d", 2592000)):
            with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
                page.get_by_label("Range", exact=True).select_option(preset)
            expect(page.get_by_text(re.compile("Loading storage trends"))).to_have_count(0)
            payload = deepcopy(api.last_payload)
            require((timestamp(payload["range"]["to"]) - timestamp(payload["range"]["from"])).total_seconds() == seconds,
                "range_preset_duration")
            assert_link(page, payload)
            check_layout(page)
        result["checks"].append("bounded_presets_and_coarse_charts")

        stage = "custom_absolute_range"
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_label("Range", exact=True).select_option("custom")
        apply = page.get_by_role("button", name="Apply range", exact=True)
        expect(apply).to_be_enabled()
        page.get_by_label("From (UTC)", exact=True).fill("2026-01-01T00:00")
        page.get_by_label("To exclusive (UTC)", exact=True).fill("2026-03-01T00:00")
        calls = len(api.metrics)
        apply.click()
        expect(page.get_by_role("alert")).to_contain_text("30 days or less")
        require(len(api.metrics) == calls, "invalid_range_no_request")
        page.get_by_label("From (UTC)", exact=True).fill("2026-11-01T04:30")
        page.get_by_label("To exclusive (UTC)", exact=True).fill("2026-11-01T07:30")
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            apply.click()
        expect(page.get_by_text(re.compile("Loading storage trends"))).to_have_count(0)
        payload = deepcopy(api.last_payload)
        require((timestamp(payload["range"]["to"]) - timestamp(payload["range"]["from"])).total_seconds() == 10800,
            "dst_absolute_three_hours")
        assert_link(page, payload)
        expect(page.get_by_label(re.compile("Live"))).not_to_be_checked()
        check_layout(page)
        page.locator(".qt2-storage-trends").screenshot(path=str(artifact_dir / (name + "-custom.png")))
        result["screenshots"].append(name + "-custom.png")
        result["checks"].append("custom_utc_validation_dst_absolute_link")

        stage = "policy_draft_and_poll"
        page.locator(".qt2-storage-advanced > summary").click()
        reserve = page.get_by_label("Free space reserve (%)", exact=True)
        reserve.fill("25")
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_label("Range", exact=True).select_option("24h")
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_label(re.compile("Live")).check()
        expect(page.get_by_text(re.compile("Loading storage trends"))).to_have_count(0)
        previous_status, previous_metrics = api.status_reads, len(api.metrics)
        with page.expect_response(lambda response: urlsplit(response.url).path == "/api/storage"), \
                page.expect_response(lambda response: urlsplit(response.url).path == "/api/storage/metrics"):
            page.clock.fast_forward(61000)
        expect(page.get_by_text(re.compile("Loading storage trends"))).to_have_count(0)
        require(api.status_reads > previous_status and len(api.metrics) > previous_metrics, "independent_polls_exercised")
        expect(reserve).to_have_value("25")
        expect(page.get_by_text(re.compile("Current .* Storage:"))).to_be_visible()
        result["checks"].append("dirty_policy_survives_poll_and_range")

        stage = "grafana_open_freeze"
        payload = deepcopy(api.last_payload)
        href = assert_link(page, payload)
        with page.expect_popup() as popup_event:
            page.get_by_role("link", name="Open in Grafana", exact=False).click()
        popup = popup_event.value
        popup.wait_for_load_state("domcontentloaded")
        require(popup.url == href, "popup_exact_absolute_range")
        require(popup.evaluate("window.opener === null"), "popup_has_no_opener")
        popup.close()
        expect(page.get_by_label(re.compile("Live"))).not_to_be_checked()
        expect(page.get_by_text(re.compile("Loading storage trends"))).to_have_count(0)
        frozen_calls = len(api.metrics)
        page.clock.fast_forward(61000)
        require(len(api.metrics) == frozen_calls, "grafana_open_freezes_live_metrics")
        require(page.get_by_role("link", name="Open in Grafana", exact=False).get_attribute("href") == href,
            "grafana_link_not_recomputed_now")
        expect(reserve).to_have_value("25")
        result["checks"].append("grafana_absolute_popup_freezes_display")

        stage = "stale_partial_error"
        api.mode = "stale"
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_label(re.compile("Live")).check()
        expect(page.get_by_text(re.compile("Stale trends response"))).to_be_visible()
        stale_href = assert_link(page, api.last_payload)
        api.mode = "error"
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_label("Range", exact=True).select_option("6h")
        expect(page.get_by_role("alert")).to_contain_text("Last successful range remains displayed")
        require(page.get_by_role("link", name="Open in Grafana", exact=False).get_attribute("href") == stale_href,
            "error_keeps_displayed_link")
        check_layout(page)
        expect(reserve).to_have_value("25")
        page.screenshot(path=str(artifact_dir / (name + "-error.png")), full_page=True)
        result["screenshots"].append(name + "-error.png")
        api.mode = "partial"
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_role("button", name="Retry trends", exact=True).click()
        expect(page.get_by_role("alert")).to_have_count(0)
        expect(page.get_by_text(re.compile("Stale trends response"))).to_have_count(0)
        result["checks"].append("partial_stale_error_and_retry")

        stage = "unavailable_samples"
        api.mode = "unavailable"
        with page.expect_response(lambda response: "/api/storage/metrics?" in response.url):
            page.get_by_label("Range", exact=True).select_option("7d")
        expect(page.get_by_text("Recorded total Unavailable", exact=True)).to_have_count(3)
        table_summary.click()
        expect(region).to_be_visible()
        values = region.locator("tbody td").all_text_contents()
        require(bool(values) and all(value.strip() == "Unavailable" for value in values), "missing_samples_never_zero")
        check_layout(page, painted=False)
        expect(reserve).to_have_value("25")
        api.clean()
        result["checks"].extend(["missing_samples_not_zero", "no_mutations_or_external_services"])
        result["status"] = "passed"
    except Exception as error:
        result["status"] = "failed"
        result["failure"] = {"stage": stage, "error_type": type(error).__name__,
            "check": str(error) if isinstance(error, AssertionError) and re.fullmatch(r"[a-z_]+", str(error)) else None}
        try:
            page.screenshot(path=str(artifact_dir / (name + "-failure.png")), full_page=True, timeout=5000)
            result["screenshots"].append(name + "-failure.png")
        except Exception:
            pass
    finally:
        result["api"] = {"status_reads": api.status_reads, "metrics_reads": len(api.metrics),
            "mutations": api.mutations, "unexpected_routes": len(api.unexpected), "uncaught_errors": len(api.page_errors)}
        context.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", required=True, type=Path)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--confirm-synthetic", action="store_true")
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and args.confirm_synthetic, "github_hosted_synthetic_qualification_required")
    require(re.fullmatch(r"[0-9a-f]{40}", args.source_revision) is not None, "exact_source_revision_required")
    root = Path(__file__).resolve().parents[2]
    actual_revision = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    require(actual_revision == args.source_revision, "qualification_source_revision_mismatch")
    require(subprocess.run(["git", "-C", str(root), "diff", "--quiet", "HEAD", "--", "portal/frontend"],
        check=False).returncode == 0, "tracked_frontend_source_clean")
    dist, artifacts = args.dist.resolve(), args.artifacts.resolve()
    require((dist / "v2.html").is_file() and "/assets/" in (dist / "v2.html").read_text(), "built_v2_dist_required")
    require(not any(path.is_symlink() for path in dist.rglob("*")), "dist_symlinks_forbidden")
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
    require(artifacts != temporary and artifacts.is_relative_to(temporary), "owned_runner_temp_artifacts_required")
    artifacts.mkdir(parents=True, exist_ok=False)
    summary = {"schema_version": "qt.storage_trends_ui_qualification.v1", "status": "failed",
        "source_revision": args.source_revision, "synthetic_only": True,
        "network_scope": "owned_loopback_static_and_intercepted_responses", "viewports": {},
        "playwright_version": None, "browser_version": None, "dist_sha256": None,
        "failure": None, "cleanup": {"http_server_stopped": False, "browser_closed": False}}
    server = None
    thread = None
    browser = None
    try:
        summary["playwright_version"] = version("playwright")
        require(summary["playwright_version"] == PLAYWRIGHT_VERSION, "pinned_playwright_required")
        digest = hashlib.sha256()
        for path in sorted(dist.rglob("*")):
            if path.is_file():
                digest.update(str(path.relative_to(dist)).encode())
                digest.update(path.read_bytes())
        summary["dist_sha256"] = digest.hexdigest()
        from playwright.sync_api import sync_playwright, expect
        server = ThreadingHTTPServer(("127.0.0.1", 0), partial(BuiltFrontend, directory=str(dist)))
        server.daemon_threads = True
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
        thread.start()
        origin = "http://127.0.0.1:" + str(server.server_address[1])
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True,
                args=["--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1"])
            summary["browser_version"] = browser.version
            for name, viewport, mobile in (("desktop", {"width": 1440, "height": 1000}, False),
                    ("mobile", {"width": 390, "height": 844}, True)):
                summary["viewports"][name] = run_viewport(browser, origin, artifacts, name, viewport, mobile, expect)
            browser.close()
            browser = None
            summary["cleanup"]["browser_closed"] = True
        summary["status"] = "passed" if all(view["status"] == "passed" for view in summary["viewports"].values()) else "failed"
    except Exception as error:
        summary["failure"] = {"stage": "setup_or_cleanup", "error_type": type(error).__name__}
    finally:
        if browser is not None:
            try:
                browser.close()
                summary["cleanup"]["browser_closed"] = True
            except Exception:
                pass
        if server is not None:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            summary["cleanup"]["http_server_stopped"] = not thread.is_alive()
        if not all(summary["cleanup"].values()):
            summary["status"] = "failed"
            summary["failure"] = summary["failure"] or {"stage": "cleanup", "error_type": "OwnedResourceCleanupIncomplete"}
        (artifacts / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": summary["status"], "summary": "summary.json", "synthetic_only": True}))
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())

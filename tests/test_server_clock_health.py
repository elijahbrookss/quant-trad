from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

import pytest
import yaml

from scripts.reporting import server_clock_health as clock

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 16, 3, 0, tzinfo=timezone.utc)
TRACKING = """Ref time (UTC)  : Fri Oct 16 02:59:30 2026
System time     : 0.001 seconds fast of NTP time
Root delay      : 0.080 seconds
Root dispersion : 0.005 seconds
Leap status     : Normal
"""
ACTIVITY = "200 OK\n1 sources online\n4 sources offline\n"
SOURCES = "MS Name/IP address Stratum Poll Reach LastRx Last sample\n^* 192.0.2.1 2 6 377 30 +1us[+2us] +/- 40ms\n"


def assess(tracking=TRACKING, activity=ACTIVITY, sources=SOURCES):
    return clock.assess(tracking, activity, sources, NOW)


def test_healthy_bound_is_conditional_and_does_not_export_source_addresses():
    result = assess()
    assert result["healthy"] == result["available"] == 1
    assert result["uncertainty_seconds"] == pytest.approx(.046)
    assert result["reference_age_seconds"] == 30
    assert "192.0.2.1" not in json.dumps(result)


@pytest.mark.parametrize("tracking,activity,sources,reason", [
    (TRACKING, ACTIVITY.replace("1 sources", "0 sources"), SOURCES, "no_selected_online_source"),
    (TRACKING, ACTIVITY, SOURCES.replace("^*", "^?"), "no_selected_online_source"),
    (TRACKING.replace("02:59:30", "02:29:59"), ACTIVITY, SOURCES, "stale_reference"),
    (TRACKING.replace("02:59:30", "03:00:02"), ACTIVITY, SOURCES, "future_reference"),
    (TRACKING.replace("0.080", "0.200"), ACTIVITY, SOURCES, "excessive_uncertainty"),
    (TRACKING.replace("Normal", "Not synchronised"), ACTIVITY, SOURCES, "unsynchronised"),
    (TRACKING.replace("0.080", "nan"), ACTIVITY, SOURCES, "invalid_status"),
    (TRACKING.replace("0.080", "1e999"), ACTIVITY, SOURCES, "invalid_status"),
    (TRACKING.replace("0.005", "-0.005"), ACTIVITY, SOURCES, "invalid_status"),
    (TRACKING, ACTIVITY, SOURCES.replace("377", "000"), "invalid_status"),
    (TRACKING, ACTIVITY, SOURCES.replace("377", "999"), "invalid_status"),
    (TRACKING, "", SOURCES, "invalid_status"),
    ("", ACTIVITY, SOURCES, "invalid_status"),
    (TRACKING.replace("Ref time (UTC)", "Unknown field"), ACTIVITY, SOURCES, "invalid_status"),
    (TRACKING, ACTIVITY, SOURCES + SOURCES.splitlines()[-1], "invalid_status"),
])
def test_unhealthy_or_invalid_status_is_explicit(tracking, activity, sources, reason):
    result = assess(tracking, activity, sources)
    assert result["healthy"] == 0
    assert reason in result["reasons"]
    assert result["available"] == int(reason != "invalid_status")


def test_exact_operational_thresholds_and_offset_direction():
    tracking = TRACKING.replace("02:59:30", "02:30:00").replace("0.080", "0.188")
    assert assess(tracking)["healthy"] == 1  # .001 + .188/2 + .005 = .100
    assert assess(tracking.replace("fast", "slow"))["healthy"] == 1


@pytest.mark.parametrize("error,reason", [
    (FileNotFoundError(), "probe_failed"),
    (subprocess.CalledProcessError(1, "chronyc"), "probe_failed"),
    (subprocess.TimeoutExpired("chronyc", 3), "probe_timeout"),
])
def test_probe_failure_is_unhealthy_and_does_not_export_raw_error(monkeypatch, error, reason):
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(clock.subprocess, "run", fail)
    result = clock.sample()
    assert result["healthy"] == result["available"] == 0
    assert result["reasons"] == [reason]


def test_probe_only_executes_fixed_read_only_status_commands(monkeypatch):
    calls = []
    outputs = iter((TRACKING, ACTIVITY, SOURCES))
    def run(command, **kwargs):
        calls.append(command)
        assert kwargs["timeout"] == 3 and kwargs["check"]
        assert kwargs["env"]["LC_ALL"] == "C"
        return subprocess.CompletedProcess(command, 0, next(outputs))
    monkeypatch.setattr(clock.subprocess, "run", run)
    clock.sample()
    assert calls == list(clock.COMMANDS)
    assert calls == [("/usr/bin/chronyc", "tracking"), ("/usr/bin/chronyc", "activity"), ("/usr/bin/chronyc", "-n", "sources")]


def test_export_rotates_two_bounded_files_and_rejects_symlinks(tmp_path, monkeypatch):
    monkeypatch.setattr(clock, "MAX_FILE_BYTES", 900)
    record = assess()
    for _ in range(15):
        clock.append_sample(tmp_path, record)
    for file in tmp_path.iterdir():
        assert file.stat().st_size <= 900
        assert all(json.loads(line)["healthy"] == 1 for line in file.read_text().splitlines())
    assert {f.name for f in tmp_path.iterdir()} == {"clock-health.jsonl", "clock-health.jsonl.1"}
    (tmp_path / "clock-health.jsonl").unlink()
    target = tmp_path / "untouched"
    target.write_text("untouched")
    (tmp_path / "clock-health.jsonl").symlink_to(target)
    with pytest.raises(ValueError, match="regular_files"):
        clock.append_sample(tmp_path, record)
    assert target.read_text() == "untouched"


def test_export_refuses_missing_or_shared_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        clock.append_sample(tmp_path / "missing", assess())
    tmp_path.chmod(0o777)
    with pytest.raises(ValueError, match="writable_by_others"):
        clock.append_sample(tmp_path, assess())


def test_optional_ingress_contains_no_clock_socket_or_credentials():
    overlay = yaml.safe_load((ROOT / "docker/docker-compose.clock-health.yml").read_text())
    assert set(overlay["services"]) == {"alloy", "grafana"}
    for service in overlay["services"].values():
        assert set(service) == {"volumes"}
        for mount in service["volumes"]:
            assert mount["read_only"] is True
            assert mount["bind"]["create_host_path"] is False
    assert overlay["services"]["alloy"]["volumes"][0]["source"] == "/srv/quanttrad/deploy-state/clock-health"
    rule = yaml.safe_load((ROOT / "docker/grafana/provisioning/alerting/clock-safety.yml").read_text())["groups"][0]["rules"][0]
    assert rule["noDataState"] == rule["execErrState"] == "Alerting"
    assert rule["for"] == "2m"


def test_combined_uncertainty_overflow_is_invalid():
    tracking = TRACKING.replace("0.001", "1e308").replace("0.080", "1e308").replace("0.005", "1e308")
    assert assess(tracking)["reasons"] == ["invalid_status"]


def test_export_rejects_hardlinked_file_and_symlink_directory(tmp_path):
    target = tmp_path / "untouched"
    target.write_text("original")
    output = tmp_path / "clock-health.jsonl"
    output.hardlink_to(target)
    with pytest.raises(ValueError, match="regular_files"):
        clock.append_sample(tmp_path, assess())
    assert target.read_text() == "original"
    alias = tmp_path / "directory-link"
    alias.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="owned_directory"):
        clock.append_sample(alias, assess())


def test_export_rejects_shared_writable_output(tmp_path):
    output = tmp_path / "clock-health.jsonl"
    output.write_text("unchanged")
    output.chmod(0o666)
    with pytest.raises(ValueError, match="regular_files"):
        clock.append_sample(tmp_path, assess())
    assert output.read_text() == "unchanged"

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from scripts.reporting import server_filesystem_capacity as sampler


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def capacity_inputs(tmp_path):
    engine = tmp_path / "engine"
    archive = tmp_path / "archive"
    udev = tmp_path / "udev"
    for path in (engine, archive, udev):
        path.mkdir()
    device = engine.stat().st_dev
    device_id = f"{os.major(device)}:{os.minor(device)}"
    (udev / f"b{device_id}").write_text("E:ID_FS_UUID=archive-uuid\n")
    return {
        "engine_info": {"OperatingSystem": "Ubuntu", "KernelVersion": "6.8.0", "DockerRootDir": "/var/lib/docker"},
        "docker_root": engine, "archive_root": archive,
        "docker_host_root": "/var/lib/docker", "expected_archive_uuid": "archive-uuid",
        "udev_root": udev, "observed_at": "2026-09-03T00:00:00+00:00",
        # The observer is ro while the actual ext4 superblock is rw.
        "mountinfo": f"51 22 {device_id} / /host-docker ro,relatime - ext4 /dev/test rw\n",
    }


def test_capacity_samples_keep_separate_disk_identities_and_do_not_write(capacity_inputs):
    samples = sampler.collect_samples(**capacity_inputs)
    values = [s for s in samples if s["sample_kind"] == "filesystem"]
    assert {s["resource_id"] for s in values} == {"docker-engine-storage", "market-archive-storage"}
    assert {s["capacity_authority"] for s in values} == {"engine_storage_filesystem", "archive_filesystem"}
    for row in values:
        assert row["physical_host_visible"] is True
        assert 0 <= row["used_percent"] <= 100
        assert row["used_percent"] == pytest.approx(100 * row["used_bytes"] / (row["used_bytes"] + row["available_bytes"]))
        assert list(Path(row["path"]).iterdir()) == []
    assert all(s["available"] == 1 for s in samples if s["sample_kind"] == "storage_health")


def test_bad_archive_does_not_hide_nvme_capacity_or_fabricate_zero(capacity_inputs):
    capacity_inputs["expected_archive_uuid"] = "wrong-uuid"
    samples = sampler.collect_samples(**capacity_inputs)
    values = [s for s in samples if s["sample_kind"] == "filesystem"]
    assert [s["resource_id"] for s in values] == ["docker-engine-storage"]
    errors = [s for s in samples if s["sample_kind"] == "capacity_unavailable"]
    assert len(errors) == 1
    assert errors[0]["resource_id"] == "market-archive-storage"
    assert "identity_mismatch" in errors[0]["reason"]
    assert "used_percent" not in errors[0]
    health = {s["resource_id"]: s["available"] for s in samples if s["sample_kind"] == "storage_health"}
    assert health == {"docker-engine-storage": 1, "market-archive-storage": 0}


def test_missing_archive_remains_missing(capacity_inputs):
    capacity_inputs["archive_root"].rmdir()
    samples = sampler.collect_samples(**capacity_inputs)
    assert not capacity_inputs["archive_root"].exists()
    assert any("storage_mount_unavailable" in s.get("reason", "") for s in samples)


def test_wrong_docker_root_cannot_masquerade_as_database_disk(capacity_inputs):
    capacity_inputs["engine_info"]["DockerRootDir"] = "/new-docker-root"
    samples = sampler.collect_samples(**capacity_inputs)
    assert [s["resource_id"] for s in samples if s["sample_kind"] == "filesystem"] == ["market-archive-storage"]
    assert any("storage_engine_root_mismatch" in s.get("reason", "") for s in samples)


@pytest.mark.parametrize("system,kernel", [("Docker Desktop", "6.8.0"), ("Ubuntu", "6.1-microsoft-standard-WSL2")])
def test_guest_capacity_never_claims_physical_host_authority(capacity_inputs, system, kernel):
    capacity_inputs["engine_info"].update(OperatingSystem=system, KernelVersion=kernel)
    samples = sampler.collect_samples(**capacity_inputs)
    assert all(s["physical_host_visible"] is False for s in samples)
    assert all(s["capacity_authority"] == "virtual_guest_storage" for s in samples)


def test_unknown_host_identity_is_unavailable_not_native_linux(capacity_inputs):
    capacity_inputs["engine_info"] = {}
    samples = sampler.collect_samples(**capacity_inputs)
    assert not any(s["sample_kind"] == "filesystem" for s in samples)
    assert all(s["available"] == 0 for s in samples if s["sample_kind"] == "storage_health")


def test_read_only_superblock_is_not_confused_with_read_only_observer(capacity_inputs):
    assert sampler._superblock_read_only("8:1", "42 1 8:1 / /disk ro - ext4 /dev/sda1 rw") is False
    assert sampler._superblock_read_only("8:1", "42 1 8:1 / /disk ro - ext4 /dev/sda1 ro") is True
    capacity_inputs["mountinfo"] = capacity_inputs["mountinfo"].replace("/dev/test rw", "/dev/test ro")
    samples = sampler.collect_samples(**capacity_inputs)
    assert not any(s["sample_kind"] == "filesystem" for s in samples)
    assert all(s["available"] == 0 for s in samples if s["sample_kind"] == "storage_health")


@pytest.mark.parametrize("mountinfo", ["", "42 1 8:1 / /disk ro - ext4 /dev/sda1", "42 1 8:1 / /disk ro - ext4 /dev/sda1 ro,rw"])
def test_missing_or_ambiguous_superblock_evidence_fails(mountinfo):
    with pytest.raises(sampler.StorageMountError):
        sampler._superblock_read_only("8:1", mountinfo)


def test_docker_probe_timeout_emits_explicit_unavailability_for_both_disks(monkeypatch, capsys):
    def timeout(*args, **kwargs):
        assert kwargs["timeout"] == 8
        raise subprocess.TimeoutExpired("docker info", timeout=8)
    monkeypatch.setattr(sampler.subprocess, "run", timeout)
    assert sampler.main() == 1
    samples = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(samples) == 4
    assert not any(s["sample_kind"] == "filesystem" for s in samples)
    assert {s["resource_id"] for s in samples} == {"docker-engine-storage", "market-archive-storage"}


def test_server_sampler_reuses_runtime_image_without_secrets_or_write_access():
    services = yaml.safe_load((ROOT / "docker/docker-compose.server.yml").read_text())["services"]
    service = services["docker-stats"]
    assert service["image"] == services["backend"]["image"]
    assert "env_file" not in service
    assert "PG_DSN" not in service["environment"]
    assert service["environment"]["QT_FILESYSTEM_CAPACITY_PROBE"].endswith("server_filesystem_capacity.py")
    for volume in service["volumes"]:
        if isinstance(volume, dict):
            assert volume["read_only"] is True
            assert volume["bind"]["create_host_path"] is False
        else:
            assert volume.endswith(":ro")
    assert any("storage-safety.yml" in str(v) for v in services["grafana"]["volumes"])


def _storage_rules():
    return yaml.safe_load((ROOT / "docker/grafana/provisioning/alerting/storage-safety.yml").read_text())["groups"][0]["rules"]


@pytest.mark.parametrize("percent,expected", [(0, None), (69.99, None), (70, "warning"), (84.99, "warning"), (85, "critical"), (91.99, "critical"), (92, "emergency"), (100, "emergency")])
def test_storage_thresholds_start_at_exact_boundary_and_do_not_duplicate(percent, expected):
    firing = []
    for rule in _storage_rules()[:3]:
        expression = rule["data"][1]["model"]["expression"].replace("$A", "value").replace("&&", "and")
        if eval(expression, {"__builtins__": {}}, {"value": percent}):
            firing.append(rule["uid"].removeprefix("qt-storage-"))
    assert firing == ([] if expected is None else [expected])


def test_each_disk_has_its_own_missing_evidence_alert():
    rules = _storage_rules()
    for rule, resource in zip(rules[3:], ("docker-engine-storage", "market-archive-storage"), strict=True):
        assert rule["noDataState"] == "Alerting"
        assert rule["execErrState"] == "Alerting"
        query = rule["data"][0]["model"]["expr"]
        assert f'resource_id="{resource}"' in query
        assert 'physical_host_visible="true"' in query
        assert 'sample_kind="storage_health"' in query
        assert "min_over_time" in query
        assert rule["data"][1]["model"]["expression"] == "$A < 1"
    for rule in rules:
        assert rule["data"][0]["datasourceUid"] == "quanttrad-loki"
        assert rule["annotations"]["first_action"]
        assert rule["annotations"]["recovery"]

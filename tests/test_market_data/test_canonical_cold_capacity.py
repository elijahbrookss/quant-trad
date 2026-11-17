from copy import deepcopy

import pytest

from scripts.db.canonical_cold_capacity import compare_capacity


def _sample(resource, scope, authority, device, used, hour):
    return {
        "resource_id": resource, "capacity_scope": scope, "capacity_authority": authority,
        "sample_kind": "filesystem", "runtime_kind": "native_linux", "physical_host_visible": True,
        "device_id": device, "filesystem_uuid": resource, "path": f"/{resource}",
        "total_bytes": 1_050, "used_bytes": used, "available_bytes": 1_000 - used,
        "observed_at": f"2026-11-14T{hour:02}:00:00+00:00",
    }


@pytest.fixture
def samples():
    def pair(ssd, hdd, hour):
        return [
            _sample("docker-engine-storage", "docker_engine_storage", "engine_storage_filesystem", "8:1", ssd, hour),
            _sample("market-archive-storage", "market_archive_storage", "archive_filesystem", "8:2", hdd, hour),
        ]
    return pair(850, 300, 18), pair(620, 420, 19)


def test_reports_net_ssd_reclaim_hdd_growth_and_remaining_target(samples):
    result = compare_capacity(*samples)
    assert result["resources"]["docker-engine-storage"]["after_used_percent"] == 62
    assert result["resources"]["market-archive-storage"]["available_bytes_change"] == -120
    assert result["ssd_net_reclaimed_bytes"] == 230
    assert result["ssd_below_65_percent"]
    assert result["ssd_bytes_above_50_percent"] == 120


@pytest.mark.parametrize(("change", "error"), [
    ({"runtime_kind": "virtualized_docker_desktop"}, "native_device"),
    ({"filesystem_uuid": "another-drive"}, "identity_or_order"),
    ({"available_bytes": 370}, "filesystem_scope"),
    ({"observed_at": "2026-11-14T20:00:00+00:00"}, "aligned_independent"),
])
def test_refuses_unaligned_filesystems(samples, change, error):
    before, after = deepcopy(samples)
    after[0].update(change)
    with pytest.raises(ValueError, match=error):
        compare_capacity(before, after)

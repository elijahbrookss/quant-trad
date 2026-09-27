from pathlib import Path
import re

import yaml

from scripts.provenance import source_tree_hash

ROOT = Path(__file__).resolve().parents[1]


def test_fixed_overlay_preserves_spool_and_database_paths_without_fallback_mounts():
    overlay = yaml.safe_load((ROOT / "docker/docker-compose.storage-server.yml").read_text())
    services = overlay["services"]
    assert set(services) == {"tsdb", "backend", "initialize", "market-data-collector", "storage-maintenance"}
    assert services["storage-maintenance"]["pid"] == "service:tsdb"
    for name, service in services.items():
        assert not service.get("privileged") and not service.get("devices")
        for volume in service["volumes"]:
            if isinstance(volume, dict):
                assert volume["bind"]["create_host_path"] is False
        if name == "tsdb":
            assert "environment" not in service
            continue
        maintenance = name == "storage-maintenance"
        assert service["user"] == ("70:70" if maintenance else "1000:1000")
        assert ("postgres-data:/var/lib/postgresql/data" in service["volumes"]) == (maintenance or name == "backend")
        mounts = {x["target"]: x for x in service["volumes"] if isinstance(x, dict)}
        assert ("/run/quanttrad/recovery" in mounts) == maintenance
        assert ("/app/logs/market-structure" in mounts) != maintenance
        if not maintenance:
            assert "pid" not in service
        assert "HDD_ROOT:?" in mounts["/qt-history"]["source"]
        assert mounts["/run/quanttrad/storage-inventory.json"]["read_only"]
        environment = service["environment"]
        assert environment["MARKET_STRUCTURE_STORAGE_ROOT"] == "/qt-history/archives"
        assert environment["QT_DISABLE_DOTENV"] == "1"
        assert environment["QT_STORAGE_MAINTENANCE_OWNER"] == "dedicated"
        assert environment["QT_ARCHIVE_SHARED_GROUP_ID"] in service["group_add"]
        assert service["cap_drop"] == ["ALL"]
    worker = services["storage-maintenance"]
    assert worker["environment"]["QT_STORAGE_MAINTENANCE_LIMITS_PATH"] == "/run/quanttrad/storage-maintenance.json"
    assert worker["environment"]["QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED"] == "true"
    assert "QT_STORAGE_MAINTENANCE_LIMITS_PATH" not in services["market-data-collector"]["environment"]


def test_packaged_preserving_operator_is_part_of_runtime_attestation(tmp_path):
    dockerfile = (ROOT / "portal/backend/Dockerfile").read_text().split("FROM runtime AS storage-test")[0]
    copied = set(re.findall(r"scripts/(?:db|automation)/[a-z_0-9]+\.py", dockerfile))
    assert copied == set(source_tree_hash.OPERATOR_FILES)
    assert "COPY scripts/db/ /" not in dockerfile
    for name in source_tree_hash.OPERATOR_FILES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original operator")
    original = source_tree_hash.working_tree_hash(tmp_path)
    for name in source_tree_hash.OPERATOR_FILES:
        path = tmp_path / name
        path.write_text("changed operator")
        assert source_tree_hash.working_tree_hash(tmp_path) != original
        path.write_text("original operator")

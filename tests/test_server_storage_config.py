from pathlib import Path
import re

import yaml

from scripts.provenance import source_tree_hash

ROOT = Path(__file__).resolve().parents[1]


def test_fixed_overlay_preserves_spool_and_database_paths_without_fallback_mounts():
    overlay = yaml.safe_load((ROOT / "docker/docker-compose.storage-server.yml").read_text())
    services = overlay["services"]
    assert set(services) == {"tsdb", "backend", "initialize", "market-data-collector"}
    assert services["market-data-collector"]["pid"] == "service:tsdb"
    for name, service in services.items():
        assert not service.get("privileged")
        assert not service.get("devices")
        for volume in service["volumes"]:
            if isinstance(volume, dict):
                assert volume["bind"]["create_host_path"] is False
        if name == "tsdb":
            assert "environment" not in service  # never changes existing PGDATA
            continue
        assert service["user"] == "70:70"
        assert "postgres-data:/var/lib/postgresql/data" in service["volumes"]
        mounts = {x["target"]: x for x in service["volumes"] if isinstance(x, dict)}
        assert "WORKING_ROOT:?" in mounts["/app/logs/market-structure"]["source"]
        assert "HDD_ROOT:?" in mounts["/qt-history"]["source"]
        assert mounts["/run/quanttrad/storage-inventory.json"]["read_only"]
        environment = service["environment"]
        assert environment["MARKET_STRUCTURE_WORKING_ROOT"] == "/app/logs/market-structure"
        assert environment["MARKET_STRUCTURE_STORAGE_ROOT"] == "/qt-history/archives"
        assert environment["QT_DISABLE_DOTENV"] == "1"
    worker = services["market-data-collector"]
    assert worker["environment"]["QT_STORAGE_MAINTENANCE_LIMITS_PATH"] == "/run/quanttrad/storage-maintenance.json"
    assert worker["environment"]["QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED"] == "true"
    assert worker["environment"]["QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED"] == "true"
    assert "group_add" not in worker and "group_add" not in services["initialize"]


def test_packaged_preserving_operator_is_part_of_runtime_attestation(tmp_path):
    dockerfile = (ROOT / "portal/backend/Dockerfile").read_text().split("FROM runtime AS storage-test")[0]
    copied = set(re.findall(r"scripts/db/[a-z_0-9]+\.py", dockerfile))
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

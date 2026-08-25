from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DOCKERFILE = ROOT / "docker/assurance/frontend-node.Dockerfile"
DB_PROFILE = ROOT / "docker/assurance/python-db-isolated.profile.json"
PROOF_CATALOG = ROOT / "docs/assurance/guarantees/proof-catalog.json"

NODE_DIGEST = "2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0"
PYTHON_DIGEST = "a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134"
TIMESCALE_DIGEST = "0698d9bf8cfd81e042f653ff3be767682b376bf0e95b431b2922e2d8a72195d1"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_frontend_profile_uses_only_pinned_node_and_python_images() -> None:
    dockerfile = FRONTEND_DOCKERFILE.read_text(encoding="utf-8")

    assert f"node@sha256:{NODE_DIGEST}" in dockerfile
    assert f"python@sha256:{PYTHON_DIGEST}" in dockerfile
    assert "COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node" in dockerfile
    assert "COPY requirements.lock /opt/qt-assurance/requirements.lock" in dockerfile
    assert "python -m pip install --no-cache-dir --no-deps" in dockerfile
    assert "python -m pip check" in dockerfile
    assert "node --version" in dockerfile
    assert "apt-get" not in dockerfile
    assert "curl" not in dockerfile
    assert "wget" not in dockerfile


def test_database_profile_is_exact_isolated_and_cleanup_bound() -> None:
    profile = _json(DB_PROFILE)

    assert profile["schema_version"] == "qt.assurance_environment_profile.v1"
    assert profile["id"] == "python-db-isolated"
    assert profile["platform"] == {"os": "linux", "architecture": "amd64"}
    assert profile["runtime"] == {"python": ">=3.12,<3.13"}
    service = profile["service"]
    assert service["image"] == f"timescale/timescaledb@sha256:{TIMESCALE_DIGEST}"
    assert service["postgresql"] == ">=15,<16"
    assert service["timescaledb"] == "==2.14.2"
    assert service["required_extensions"] == ["pgcrypto", "timescaledb"]
    assert service["network_mode"] == "isolated_bridge"
    assert service["publish_host"] == "127.0.0.1"
    assert service["publish_port"] == "ephemeral"
    isolation = profile["isolation"]
    assert isolation["database_identity"] == "unique_per_attestation_session"
    assert isolation["credentials"] == "synthetic_session_only"
    assert isolation["shared_development_database_forbidden"] is True
    assert isolation["live_database_forbidden"] is True
    assert isolation["production_database_forbidden"] is True
    assert isolation["cleanup_evidence_required"] is True


def test_catalog_profile_bindings_match_the_executable_profiles() -> None:
    catalog = _json(PROOF_CATALOG)
    profiles = {profile["id"]: profile for profile in catalog["environment_profiles"]}

    assert profiles["frontend-node"]["python"] == ">=3.12,<3.13"
    assert profiles["frontend-node"]["node"] == ">=20,<21"
    assert profiles["frontend-node"]["execution_class"] == "isolated_container"
    assert (
        profiles["frontend-node"]["runtime_definition"]
        == "docker/assurance/frontend-node.Dockerfile"
    )
    assert profiles["python-db-isolated"]["python"] == ">=3.12,<3.13"
    assert profiles["python-db-isolated"]["execution_class"] == "isolated_database"
    assert (
        profiles["python-db-isolated"]["runtime_definition"]
        == "docker/assurance/python-db-isolated.profile.json"
    )
    assert profiles["manual-recovery"]["execution_class"] == "isolated_recovery"
    assert (
        profiles["manual-recovery"]["runtime_definition"]
        == "docs/assurance/guarantees/procedures/isolated-recovery-rehearsal.md"
    )
    db_proofs = [
        proof
        for proof in catalog["proofs"]
        if proof["environment_profile_id"] == "python-db-isolated"
    ]
    assert len(db_proofs) == 7
    assert all(proof["proof_kind"] == "database_integration" for proof in db_proofs)

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from scripts.assurance import build_runner


ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DOCKERFILE = ROOT / "docker/assurance/frontend-node.Dockerfile"
DB_PROFILE = ROOT / "docker/assurance/python-db-isolated.profile.json"
RUNNER_BUILD_PROFILE = ROOT / "docker/assurance/runner-build.profile.json"
WHEEL_MANIFEST = ROOT / "docker/assurance/python-wheel-manifest.lock.json"
REQUIREMENTS_LOCK = ROOT / "requirements.lock"
PROOF_CATALOG = ROOT / "docs/assurance/guarantees/proof-catalog.json"
SCHEMA_DIR = ROOT / "docs/assurance/guarantees/schemas"

NODE_DIGEST = "2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0"
PYTHON_DIGEST = "a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134"
TIMESCALE_DIGEST = "0698d9bf8cfd81e042f653ff3be767682b376bf0e95b431b2922e2d8a72195d1"
WHEEL_ENTRY_COUNT = 91
WHEEL_SELECTED_BYTES = 223452511
WHEEL_ENTRY_MANIFEST_SHA256 = (
    "dbf015ad7ff4211cdb4a6fa311a66abc0b540026b502ab9c3eca2b7d487b6b86"
)


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _normalized_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _assert_local_schema_refs_resolve(value: object, definitions: set[str]) -> None:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            assert ref.removeprefix("#/$defs/") in definitions
        for item in value.values():
            _assert_local_schema_refs_resolve(item, definitions)
    elif isinstance(value, list):
        for item in value:
            _assert_local_schema_refs_resolve(item, definitions)


def test_frontend_profile_uses_only_pinned_node_and_python_images() -> None:
    dockerfile = FRONTEND_DOCKERFILE.read_text(encoding="utf-8")

    assert f"node@sha256:{NODE_DIGEST}" in dockerfile
    assert f"python@sha256:{PYTHON_DIGEST}" in dockerfile
    assert "FROM ${NODE_IMAGE} AS node_runtime" in dockerfile
    assert "FROM ${PYTHON_IMAGE} AS runtime" in dockerfile
    assert "COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node" in dockerfile
    assert (
        "COPY .qt-assurance-wheelhouse/python-wheel-manifest.lock.json "
        "/opt/qt-assurance/python-wheel-manifest.lock.json"
    ) in dockerfile
    assert (
        "COPY .qt-assurance-wheelhouse/requirements.hashed.txt "
        "/opt/qt-assurance/requirements.hashed.txt"
    ) in dockerfile
    assert (
        "COPY .qt-assurance-wheelhouse/wheelhouse/ "
        "/opt/qt-assurance/wheelhouse/"
    ) in dockerfile
    assert '"--no-index"' in dockerfile
    assert '"--no-deps"' in dockerfile
    assert '"--only-binary=:all:"' in dockerfile
    assert '"--require-hashes"' in dockerfile
    assert '"--find-links=/opt/qt-assurance/wheelhouse"' in dockerfile
    assert "COPY requirements.lock" not in dockerfile
    assert "RUN python" not in dockerfile
    assert "apt-get" not in dockerfile
    assert "psql" not in dockerfile
    assert "curl" not in dockerfile
    assert "wget" not in dockerfile


def test_runner_build_profile_and_dockerfile_match_the_fail_closed_validator() -> None:
    profile_bytes = RUNNER_BUILD_PROFILE.read_bytes()
    profile = _json(RUNNER_BUILD_PROFILE)

    assert profile_bytes == _canonical_json_bytes(profile)
    assert profile["schema_version"] == "qt.assurance_runner_build_profile.v1"
    assert profile["external_order_submission_enabled"] is False
    assert profile["docker"] == {
        "network_mode": "none",
        "pull": False,
        "no_cache": True,
        "shell": False,
        "context_transport": "verified_tar_stdin",
    }
    assert profile["installation"]["intentionally_unavailable_executables"] == [
        "psql"
    ]
    assert [image["digest"] for image in profile["base_images"]] == [
        f"sha256:{NODE_DIGEST}",
        f"sha256:{PYTHON_DIGEST}",
    ]
    assert build_runner.validate_build_profile(profile_bytes) == profile
    build_runner.validate_bound_dockerfile(FRONTEND_DOCKERFILE.read_bytes(), profile)


def test_wheel_manifest_is_the_exact_hash_complete_requirements_closure() -> None:
    manifest_bytes = WHEEL_MANIFEST.read_bytes()
    manifest = _json(WHEEL_MANIFEST)
    requirements_bytes = REQUIREMENTS_LOCK.read_bytes()

    assert manifest_bytes == _canonical_json_bytes(manifest)
    assert manifest["target"] == {
        "implementation": "cp",
        "python_version": "3.12",
        "abi": "cp312",
        "platform": "linux/amd64",
        "glibc_max": "2.36",
    }
    assert manifest["aggregate"] == {
        "entry_count": WHEEL_ENTRY_COUNT,
        "selected_bytes": WHEEL_SELECTED_BYTES,
        "entry_manifest_sha256": WHEEL_ENTRY_MANIFEST_SHA256,
    }
    entries = manifest["entries"]
    assert len(entries) == WHEEL_ENTRY_COUNT
    assert sum(entry["size"] for entry in entries) == WHEEL_SELECTED_BYTES
    assert [entry["name"] for entry in entries] == sorted(
        entry["name"] for entry in entries
    )
    assert len({entry["sha256"] for entry in entries}) == WHEEL_ENTRY_COUNT
    assert len({entry["filename"].casefold() for entry in entries}) == WHEEL_ENTRY_COUNT

    pins = []
    for raw_line in requirements_bytes.decode("utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, version = line.split("==", 1)
        pins.append((_normalized_distribution_name(name), version))
    assert [(entry["name"], entry["version"]) for entry in entries] == pins

    aggregate_lines = sorted(
        f"{entry['name']}=={entry['version']}\t{entry['sha256']}\t"
        f"{entry['size']}\t{entry['filename']}\n"
        for entry in entries
    )
    assert (
        hashlib.sha256("".join(aggregate_lines).encode("utf-8")).hexdigest()
        == WHEEL_ENTRY_MANIFEST_SHA256
    )
    assert build_runner.validate_wheel_manifest(manifest_bytes, requirements_bytes) == manifest


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
    assert (
        profiles["frontend-node"]["runner_build_profile"]
        == "docker/assurance/runner-build.profile.json"
    )
    assert profiles["python-db-isolated"]["python"] == ">=3.12,<3.13"
    assert profiles["python-db-isolated"]["execution_class"] == "isolated_database"
    assert (
        profiles["python-db-isolated"]["runtime_definition"]
        == "docker/assurance/python-db-isolated.profile.json"
    )
    assert (
        profiles["python-db-isolated"]["runner_build_profile"]
        == "docker/assurance/runner-build.profile.json"
    )
    assert (
        profiles["python-nondb"]["runner_build_profile"]
        == "docker/assurance/runner-build.profile.json"
    )
    assert profiles["manual-recovery"]["execution_class"] == "isolated_recovery"
    assert (
        profiles["manual-recovery"]["runtime_definition"]
        == "docs/assurance/guarantees/procedures/isolated-recovery-rehearsal.md"
    )
    assert "runner_build_profile" not in profiles["manual-recovery"]
    db_proofs = [
        proof
        for proof in catalog["proofs"]
        if proof["environment_profile_id"] == "python-db-isolated"
    ]
    assert len(db_proofs) == 7
    assert all(proof["proof_kind"] == "database_integration" for proof in db_proofs)


def test_runner_build_schema_set_is_closed_and_version_bound() -> None:
    expected = {
        "python-wheel-manifest.v1.schema.json": (
            "qt.assurance_python_wheel_manifest.v1"
        ),
        "runner-build-profile.v1.schema.json": (
            "qt.assurance_runner_build_profile.v1"
        ),
        "runner-materialization.v1.schema.json": (
            "qt.assurance_runner_materialization.v1"
        ),
        "runner-build-record.v1.schema.json": (
            "qt.assurance_runner_build_record.v1"
        ),
    }
    for filename, schema_version in expected.items():
        schema = _json(SCHEMA_DIR / filename)
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["additionalProperties"] is False
        assert schema["properties"]["schema_version"]["const"] == schema_version
        _assert_local_schema_refs_resolve(schema, set(schema["$defs"]))

    build_record_schema = _json(SCHEMA_DIR / "runner-build-record.v1.schema.json")
    assert "argv_sha256" in build_record_schema["$defs"]["invocation"]["required"]

    proof_schema = _json(SCHEMA_DIR / "proof-catalog.v1.schema.json")
    profile_schema = proof_schema["$defs"]["environmentProfile"]
    assert profile_schema["properties"]["runner_build_profile"] == {
        "$ref": "#/$defs/repoPath",
        "type": "string",
    }

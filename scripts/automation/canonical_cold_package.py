"""Bind reviewed cutover artifacts and the measured after-drain SQL boundary."""

from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.fact_archive import archive_evidence_hash
from scripts.automation import storage_host_boundary as host
from scripts.db.canonical_cold_activate import (
    ACTIVATION_VERSION, _EXECUTION_SCOPE, _PHYSICAL_GATES, validate_execution_record,
)
from scripts.db.canonical_cold_witness import MAX_WITNESS_BYTES, load_approved_witnesses


VERSION = "qt.canonical_cold_approval_package.v4"
_ARTIFACT_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_MAX_ARTIFACT_BYTES = 128 * 1024**2
_MAX_RUNTIME_BYTES = 524_288
_PARSED_ARTIFACT_LIMITS = {"host_inventory": 65_536, "candidate_transaction_witness": MAX_WITNESS_BYTES}


def _hash_file(path, *, json_limit=0):
    digest = sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as file:
        before = os.fstat(file.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or before.st_mode & 0o077
                or not 0 < before.st_size <= (json_limit or _MAX_ARTIFACT_BYTES)):
            raise RuntimeError("canonical_cold_package_invalid_private_artifact")
        size = 0
        content = bytearray()
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            size += len(chunk)
            if size > before.st_size:
                raise RuntimeError("canonical_cold_package_artifact_changed")
            digest.update(chunk)
            if json_limit:
                content.extend(chunk)
        after = os.fstat(file.fileno())
        observed = path.lstat()
    def stamp(info):
        return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
    if size != before.st_size or stamp(before) != stamp(after) or stamp(before) != stamp(observed):
        raise RuntimeError("canonical_cold_package_artifact_changed")
    return digest.hexdigest(), content


def _pin(value):
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def _unique_fields(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("canonical_cold_package_duplicate_field")
    return result


def load_approved_package(path, *, approved_package_hash, inventory):
    """Verify the exact private review package plus each referenced artifact.

    The separately obtained approval digest is required. The artifact hashes
    bind the reviewed evidence. Each rehearsal and recovery check still owns
    evaluation of its evidence.
    """
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path or not _pin(approved_package_hash):
        raise ValueError("canonical_cold_package_approval_mismatch")
    digest, raw = _hash_file(path, json_limit=65_536)
    if digest != approved_package_hash:
        raise ValueError("canonical_cold_package_approval_mismatch")
    package = json.loads(raw, object_pairs_hook=_unique_fields)
    if (not isinstance(package, dict) or set(package) != {
            "schema", "approved_inventory_hash", "source_commit_floor", "scope", "artifacts", "max_outage_seconds",
            "runtime_model_hash",
        } or package["schema"] != VERSION or not _pin(package["approved_inventory_hash"])
            or type(package["source_commit_floor"]) is not int or package["source_commit_floor"] < 1
            or type(package["max_outage_seconds"]) is not int
            or not 1 <= package["max_outage_seconds"] <= MAX_MIGRATION_SECONDS
            or not _pin(package["runtime_model_hash"])
            or not isinstance(package["scope"], dict) or package["scope"].keys() != _EXECUTION_SCOPE
            or not isinstance(package["artifacts"], dict)
            or package["artifacts"].keys() != _PHYSICAL_GATES | {"command_sheet", "host_inventory", "runtime_model"}):
        raise ValueError("canonical_cold_package_invalid_structure")
    if not isinstance(inventory, dict) or host.digest(inventory) != package["approved_inventory_hash"]:
        raise ValueError("canonical_cold_package_host_inventory_changed")
    names = set()
    for name, binding in package["artifacts"].items():
        if (not isinstance(binding, dict) or binding.keys() != {"filename", "sha256"}
                or not isinstance(binding["filename"], str) or not _ARTIFACT_NAME.fullmatch(binding["filename"])
                or binding["filename"] in names or binding["filename"] == path.name or not _pin(binding["sha256"])):
            raise ValueError(f"canonical_cold_package_invalid_artifact_binding: {name}")
        names.add(binding["filename"])
        digest, content = _hash_file(path.parent / binding["filename"], json_limit=_PARSED_ARTIFACT_LIMITS.get(name, 0))
        if digest != binding["sha256"]:
            raise RuntimeError(f"canonical_cold_package_artifact_digest_changed: {name}")
        if name == "host_inventory" and json.loads(content, object_pairs_hook=_unique_fields) != inventory:
            raise RuntimeError("canonical_cold_package_saved_inventory_changed")
        if name == "candidate_transaction_witness":
            load_approved_witnesses(bytes(content), expected_sha256=digest)
    if _hash_file(path, json_limit=65_536)[0] != approved_package_hash:
        raise RuntimeError("canonical_cold_package_approval_changed")
    bind_execution(package, approved_package_hash=approved_package_hash, source_commit_seq=package["source_commit_floor"],
                   provider_boundary=package["artifacts"]["provider_boundary"]["sha256"],
                   independent_recovery=package["artifacts"]["independent_recovery"]["sha256"])
    return package


def read_approved_runtime_model(package_path, package):
    binding = package["artifacts"]["runtime_model"]
    digest, content = _hash_file(Path(package_path).parent / binding["filename"], json_limit=_MAX_RUNTIME_BYTES)
    if digest != binding["sha256"]:
        raise RuntimeError("canonical_cold_package_runtime_model_changed")
    model = json.loads(content, object_pairs_hook=_unique_fields)
    if host.digest(model) != package["runtime_model_hash"]:
        raise RuntimeError("canonical_cold_package_runtime_model_changed")
    return model


def read_approved_candidate_witness(package_path, package):
    binding = package["artifacts"]["candidate_transaction_witness"]
    digest, content = _hash_file(Path(package_path).parent / binding["filename"], json_limit=MAX_WITNESS_BYTES)
    if digest != binding["sha256"]:
        raise RuntimeError("canonical_cold_package_candidate_witness_changed")
    return bytes(content)


def bind_execution(package, *, approved_package_hash, source_commit_seq, provider_boundary, independent_recovery):
    """Fill only current boundary evidence within the approved retirement scope."""
    if (not _pin(approved_package_hash)
            or type(source_commit_seq) is not int or source_commit_seq < package["source_commit_floor"]
            or not _pin(provider_boundary) or not _pin(independent_recovery)):
        raise ValueError("canonical_cold_package_final_boundary_missing")
    execution = {
        **package["scope"], "schema": ACTIVATION_VERSION, "approved_package_hash": approved_package_hash,
        "source_commit_seq": source_commit_seq,
        "evidence": {gate: package["artifacts"][gate]["sha256"] for gate in _PHYSICAL_GATES} | {
            "provider_boundary": provider_boundary, "independent_recovery": independent_recovery,
        },
    }
    digest = archive_evidence_hash(execution)
    validate_execution_record(execution, expected_execution_hash=digest)
    return execution, digest

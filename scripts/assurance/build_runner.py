#!/usr/bin/env python3
"""Materialize and build the source-bound offline assurance runner.

This module creates environment evidence only.  It does not execute a proof,
attest a proof result, close a remediation, activate a guarantee, pull an
image, or enable external order submission.
"""

from __future__ import annotations

import argparse
import base64
import csv
import email.parser
import email.policy
import hashlib
import io
import json
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import tarfile
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Callable, Mapping, Sequence

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import InvalidWheelFilename, parse_wheel_filename
from packaging.version import InvalidVersion, Version

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.assurance import docker_lifecycle


WHEEL_MANIFEST_PATH = "docker/assurance/python-wheel-manifest.lock.json"
BUILD_PROFILE_PATH = "docker/assurance/runner-build.profile.json"
DOCKERFILE_PATH = "docker/assurance/frontend-node.Dockerfile"
REQUIREMENTS_LOCK_PATH = "requirements.lock"
MATERIALIZER_PATH = "scripts/assurance/build_runner.py"

WHEEL_MANIFEST_SCHEMA_VERSION = "qt.assurance_python_wheel_manifest.v1"
BUILD_PROFILE_SCHEMA_VERSION = "qt.assurance_runner_build_profile.v1"
MATERIALIZATION_SCHEMA_VERSION = "qt.assurance_runner_materialization.v1"
BUILD_RECORD_SCHEMA_VERSION = "qt.assurance_runner_build_record.v1"
ARCHIVED_BUILD_RECORD_SCHEMA_VERSION = "qt.assurance_runner_build_record_archive.v1"

TARGET_PYTHON = Version("3.12")
TARGET_PLATFORM = "linux/amd64"
TARGET_GLIBC = (2, 36)
WHEELHOUSE_PREFIX = ".qt-assurance-wheelhouse"
WHEELHOUSE_TAR_NAME = "wheelhouse.tar"
CONTEXT_TAR_NAME = "build-context.tar"
MATERIALIZATION_RECEIPT_NAME = "materialization-receipt.json"
DEFAULT_BUILD_RECORD_NAME = "runner-build-record.json"
DEFAULT_BUILD_LOG_NAME = "runner-build.log"
BUILD_TIMEOUT_SECONDS = 7200
MAX_CACHE_CANDIDATES = 100_000

SOURCE_LABEL = docker_lifecycle.BUILD_SOURCE_LABEL
SOURCE_TREE_LABEL = docker_lifecycle.BUILD_SOURCE_TREE_LABEL
BUILD_PROFILE_LABEL = docker_lifecycle.BUILD_PROFILE_LABEL
BUILD_DEFINITION_LABEL = docker_lifecycle.BUILD_DEFINITION_LABEL
WHEEL_MANIFEST_LABEL = docker_lifecycle.WHEEL_MANIFEST_LABEL
WHEEL_ARTIFACT_LABEL = docker_lifecycle.WHEEL_ARTIFACT_LABEL
BUILD_CONTEXT_LABEL = docker_lifecycle.BUILD_CONTEXT_LABEL
REQUIRED_IMAGE_LABELS = docker_lifecycle.RUNNER_BUILD_LABELS

HEX64_RE = re.compile(r"[0-9a-f]{64}\Z")
COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
SAFE_TAG_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+:-]{0,255}\Z")
IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
BUILD_ID_RE = re.compile(r"QT-RUNNER-BUILD-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{16}\Z")
NORMALIZED_NAME_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")
WHEEL_TAG_RE = re.compile(r"[A-Za-z0-9_.]+-[A-Za-z0-9_.]+-[A-Za-z0-9_.]+\Z")
WINDOWS_RESERVED_COMPONENTS = {
    "aux",
    "con",
    "nul",
    "prn",
    *(f"com{index}" for index in range(1, 10)),
    *(f"lpt{index}" for index in range(1, 10)),
}


class RunnerBuildError(RuntimeError):
    """The offline runner provenance boundary could not be proven."""


@dataclass(frozen=True)
class SourceMaterial:
    path: str
    sha256: str


@dataclass(frozen=True)
class MaterializedRunner:
    output_root: Path
    wheelhouse_tar: Path
    context_tar: Path
    receipt: Path


@dataclass(frozen=True)
class BuildCommandResult:
    stdout: bytes
    stderr: bytes
    exit_code: int
    timed_out: bool = False


@dataclass
class _AnchoredDirectory:
    """A real directory held open so bundle I/O cannot be path-redirected."""

    path: Path
    descriptor: int
    device: int
    inode: int
    where: str

    def __enter__(self) -> "_AnchoredDirectory":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1

    def assert_identity(self) -> None:
        if self.descriptor < 0:
            raise RunnerBuildError(f"{self.where}:directory_anchor_closed")
        current = os.fstat(self.descriptor)
        if (
            not stat.S_ISDIR(current.st_mode)
            or current.st_dev != self.device
            or current.st_ino != self.inode
        ):
            raise RunnerBuildError(f"{self.where}:directory_anchor_changed")
        try:
            visible = self.path.lstat()
        except OSError as exc:
            raise RunnerBuildError(f"{self.where}:directory_path_changed") from exc
        if (
            not stat.S_ISDIR(visible.st_mode)
            or stat.S_ISLNK(visible.st_mode)
            or visible.st_dev != self.device
            or visible.st_ino != self.inode
        ):
            raise RunnerBuildError(f"{self.where}:directory_path_changed")


BuildCommandRunner = Callable[
    [Sequence[str], Mapping[str, str], int, bytes], BuildCommandResult
]


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RunnerBuildError(f"duplicate_json_key:{key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise RunnerBuildError(f"nonfinite_json_constant:{value}")


def _strict_json_bytes(content: bytes, where: str) -> dict[str, Any]:
    try:
        value = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except RunnerBuildError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RunnerBuildError(f"{where}:invalid_json") from exc
    if not isinstance(value, dict):
        raise RunnerBuildError(f"{where}:object_required")
    return value


def _exact_keys(
    value: Mapping[str, Any], required: set[str], where: str
) -> None:
    observed = set(value)
    if observed != required:
        missing = ",".join(sorted(required - observed)) or "none"
        extra = ",".join(sorted(observed - required)) or "none"
        raise RunnerBuildError(f"{where}:keys_mismatch:missing={missing}:extra={extra}")


def _string(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise RunnerBuildError(f"{where}:nonempty_string_required")
    return value


def _hash(value: Any, where: str) -> str:
    digest = _string(value, where)
    if not HEX64_RE.fullmatch(digest):
        raise RunnerBuildError(f"{where}:invalid_sha256")
    return digest


def _portable_component(value: str, where: str) -> str:
    if (
        not value
        or value in {".", ".."}
        or value[-1:] in {" ", "."}
        or unicodedata.normalize("NFC", value) != value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or "/" in value
        or "\\" in value
        or ":" in value
        or value.split(".", 1)[0].casefold() in WINDOWS_RESERVED_COMPONENTS
    ):
        raise RunnerBuildError(f"{where}:nonportable_component")
    return value


def _portable_relative(value: str, where: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise RunnerBuildError(f"{where}:safe_relative_path_required")
    pure = PurePosixPath(value)
    if pure.is_absolute() or not pure.parts or pure.as_posix() != value:
        raise RunnerBuildError(f"{where}:safe_relative_path_required")
    for index, part in enumerate(pure.parts):
        _portable_component(part, f"{where}[{index}]")
    return pure.as_posix()


def _normalized_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _git(root: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
    git_env = dict(os.environ)
    for name in list(git_env):
        if name.startswith("GIT_"):
            git_env.pop(name, None)
    git_env["GIT_NO_REPLACE_OBJECTS"] = "1"
    git_env["LC_ALL"] = "C"
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            input=input_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            shell=False,
            env=git_env,
        )
    except OSError as exc:
        raise RunnerBuildError(f"git_unavailable:{type(exc).__name__}") from exc
    if result.returncode != 0:
        raise RunnerBuildError(f"git_failed:{args[0] if args else 'unknown'}")
    return result.stdout


def require_exact_clean_source(root: Path, source_commit: str) -> tuple[str, str]:
    if not COMMIT_RE.fullmatch(source_commit):
        raise RunnerBuildError("source_commit_invalid")
    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        raise RunnerBuildError("source_root_unavailable") from exc
    git_top = Path(
        _git(root, "rev-parse", "--show-toplevel").decode("utf-8").strip()
    ).resolve()
    if git_top != root:
        raise RunnerBuildError("source_root_not_git_toplevel")
    expected_materializer = (root / MATERIALIZER_PATH).resolve()
    expected_lifecycle = (root / "scripts/assurance/docker_lifecycle.py").resolve()
    if (
        ROOT.resolve() != root
        or Path(__file__).resolve() != expected_materializer
        or Path(docker_lifecycle.__file__).resolve() != expected_lifecycle
    ):
        raise RunnerBuildError("source_executing_assurance_code_mismatch")
    head = _git(root, "rev-parse", "HEAD").decode("ascii").strip()
    if head != source_commit:
        raise RunnerBuildError("source_head_mismatch")
    commit = _git(root, "rev-parse", f"{source_commit}^{{commit}}").decode("ascii").strip()
    if commit != source_commit:
        raise RunnerBuildError("source_commit_unresolvable")
    status = _git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    if status:
        raise RunnerBuildError("source_worktree_not_exact_clean")
    tree = _git(root, "rev-parse", f"{source_commit}^{{tree}}").decode("ascii").strip()
    if not COMMIT_RE.fullmatch(tree):
        raise RunnerBuildError("source_tree_invalid")
    return commit, tree


def _bound_source_bytes(root: Path, source_commit: str, relative: str) -> bytes:
    relative = _portable_relative(relative, "source_material.path")
    return _git(root, "show", f"{source_commit}:{relative}")


def _parse_requirements_lock(content: bytes) -> list[tuple[str, str]]:
    try:
        text = content.decode("utf-8")
    except UnicodeError as exc:
        raise RunnerBuildError("requirements_lock:not_utf8") from exc
    pins: list[tuple[str, str]] = []
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(
            r"([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9.!+_-]*)",
            line,
        )
        if match is None:
            raise RunnerBuildError(f"requirements_lock:{line_number}:exact_pin_required")
        name = _normalized_name(match.group(1))
        version = match.group(2)
        if not NORMALIZED_NAME_RE.fullmatch(name):
            raise RunnerBuildError(f"requirements_lock:{line_number}:name_invalid")
        try:
            Version(version)
        except InvalidVersion as exc:
            raise RunnerBuildError(
                f"requirements_lock:{line_number}:version_invalid"
            ) from exc
        pins.append((name, version))
    if not pins:
        raise RunnerBuildError("requirements_lock:empty")
    if pins != sorted(pins) or len(pins) != len(set(pins)):
        raise RunnerBuildError("requirements_lock:pins_must_be_unique_and_sorted")
    return pins


def _tag_is_compatible(tag: str) -> bool:
    if not WHEEL_TAG_RE.fullmatch(tag):
        return False
    python_tag, abi_tag, platform_tag = tag.split("-", 2)
    python_tags = python_tag.split(".")
    platforms = platform_tag.split(".")
    if abi_tag == "none":
        python_ok = bool(set(python_tags) & {"py3", "py312"}) and set(
            python_tags
        ).issubset({"py3", "py312"})
    elif abi_tag == "cp312":
        python_ok = python_tags == ["cp312"]
    elif abi_tag == "abi3":
        python_ok = bool(python_tags) and all(
            item.startswith("cp3")
            and item[2:].isdigit()
            and 30 <= int(item[2:]) <= 312
            for item in python_tags
        )
    else:
        python_ok = False
    if "any" in platforms and platforms != ["any"]:
        return False
    platform_ok = False
    for item in platforms:
        if item == "any":
            if abi_tag == "none" and platforms == ["any"]:
                platform_ok = True
            continue
        if item in {
            "linux_x86_64",
            "manylinux1_x86_64",
            "manylinux2010_x86_64",
            "manylinux2014_x86_64",
        }:
            platform_ok = True
        match = re.fullmatch(r"manylinux_(\d+)_(\d+)_x86_64", item)
        if match and (int(match.group(1)), int(match.group(2))) <= TARGET_GLIBC:
            platform_ok = True
    return python_ok and platform_ok


def _entry_manifest_sha256(entries: Sequence[Mapping[str, Any]]) -> str:
    lines = sorted(
        f"{item['name']}=={item['version']}\t{item['sha256']}\t"
        f"{item['size']}\t{item['filename']}\n"
        for item in entries
    )
    return _sha256_bytes("".join(lines).encode("utf-8"))


def validate_wheel_manifest(
    content: bytes, requirements_content: bytes
) -> dict[str, Any]:
    raw = _strict_json_bytes(content, "wheel_manifest")
    if _canonical_json_bytes(raw) != content:
        raise RunnerBuildError("wheel_manifest:not_canonical_json")
    _exact_keys(
        raw,
        {"schema_version", "target", "requirements_lock_path", "entries", "aggregate"},
        "wheel_manifest",
    )
    if raw["schema_version"] != WHEEL_MANIFEST_SCHEMA_VERSION:
        raise RunnerBuildError("wheel_manifest:schema_version_unsupported")
    if raw["requirements_lock_path"] != REQUIREMENTS_LOCK_PATH:
        raise RunnerBuildError("wheel_manifest:requirements_lock_path_mismatch")
    target = raw["target"]
    if not isinstance(target, dict):
        raise RunnerBuildError("wheel_manifest.target:object_required")
    _exact_keys(
        target,
        {"implementation", "python_version", "abi", "platform", "glibc_max"},
        "wheel_manifest.target",
    )
    expected_target = {
        "implementation": "cp",
        "python_version": "3.12",
        "abi": "cp312",
        "platform": TARGET_PLATFORM,
        "glibc_max": "2.36",
    }
    if target != expected_target:
        raise RunnerBuildError("wheel_manifest.target:mismatch")
    entries = raw["entries"]
    if not isinstance(entries, list) or not entries:
        raise RunnerBuildError("wheel_manifest.entries:nonempty_array_required")
    normalized_entries: list[dict[str, Any]] = []
    for index, item in enumerate(entries):
        where = f"wheel_manifest.entries[{index}]"
        if not isinstance(item, dict):
            raise RunnerBuildError(f"{where}:object_required")
        _exact_keys(
            item,
            {
                "name",
                "version",
                "filename",
                "sha256",
                "size",
                "selected_tag",
                "wheel_tags",
                "requires_python",
            },
            where,
        )
        name = _string(item["name"], f"{where}.name")
        if name != _normalized_name(name) or not NORMALIZED_NAME_RE.fullmatch(name):
            raise RunnerBuildError(f"{where}.name:not_normalized")
        version = _string(item["version"], f"{where}.version")
        try:
            Version(version)
        except InvalidVersion as exc:
            raise RunnerBuildError(f"{where}.version:invalid") from exc
        filename = _portable_component(
            _string(item["filename"], f"{where}.filename"), f"{where}.filename"
        )
        try:
            parsed_name, parsed_version, parsed_build, _ = parse_wheel_filename(filename)
        except InvalidWheelFilename as exc:
            raise RunnerBuildError(f"{where}.filename:invalid_wheel_filename") from exc
        if (
            _normalized_name(str(parsed_name)) != name
            or parsed_version != Version(version)
            or parsed_build
        ):
            raise RunnerBuildError(f"{where}.filename:identity_mismatch")
        digest = _hash(item["sha256"], f"{where}.sha256")
        size = item["size"]
        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            raise RunnerBuildError(f"{where}.size:positive_integer_required")
        selected_tag = _string(item["selected_tag"], f"{where}.selected_tag")
        tags = item["wheel_tags"]
        if (
            not isinstance(tags, list)
            or not tags
            or any(not isinstance(tag, str) or not WHEEL_TAG_RE.fullmatch(tag) for tag in tags)
            or len(tags) != len(set(tags))
        ):
            raise RunnerBuildError(f"{where}.wheel_tags:unique_valid_tags_required")
        if selected_tag not in tags or not _tag_is_compatible(selected_tag):
            raise RunnerBuildError(f"{where}.selected_tag:not_compatible")
        expected_filename = f"{name.replace('-', '_')}-{version}-{selected_tag}.whl"
        if filename != expected_filename:
            raise RunnerBuildError(f"{where}.filename:not_canonical")
        requires_python = item["requires_python"]
        if requires_python is not None:
            if not isinstance(requires_python, str) or not requires_python:
                raise RunnerBuildError(f"{where}.requires_python:invalid")
            try:
                if TARGET_PYTHON not in SpecifierSet(requires_python):
                    raise RunnerBuildError(f"{where}.requires_python:target_excluded")
            except InvalidSpecifier as exc:
                raise RunnerBuildError(f"{where}.requires_python:invalid") from exc
        normalized_entries.append(
            {
                "name": name,
                "version": version,
                "filename": filename,
                "sha256": digest,
                "size": size,
                "selected_tag": selected_tag,
                "wheel_tags": list(tags),
                "requires_python": requires_python,
            }
        )
    names = [item["name"] for item in normalized_entries]
    filenames = [item["filename"] for item in normalized_entries]
    digests = [item["sha256"] for item in normalized_entries]
    if names != sorted(names) or len(names) != len(set(names)):
        raise RunnerBuildError("wheel_manifest.entries:names_must_be_unique_and_sorted")
    if len(filenames) != len({unicodedata.normalize("NFC", item).casefold() for item in filenames}):
        raise RunnerBuildError("wheel_manifest.entries:filename_collision")
    if len(digests) != len(set(digests)):
        raise RunnerBuildError("wheel_manifest.entries:artifact_hash_collision")
    pins = _parse_requirements_lock(requirements_content)
    if [(item["name"], item["version"]) for item in normalized_entries] != pins:
        raise RunnerBuildError("wheel_manifest.entries:requirements_closure_mismatch")
    aggregate = raw["aggregate"]
    if not isinstance(aggregate, dict):
        raise RunnerBuildError("wheel_manifest.aggregate:object_required")
    _exact_keys(
        aggregate,
        {"entry_count", "selected_bytes", "entry_manifest_sha256"},
        "wheel_manifest.aggregate",
    )
    expected_aggregate = {
        "entry_count": len(normalized_entries),
        "selected_bytes": sum(item["size"] for item in normalized_entries),
        "entry_manifest_sha256": _entry_manifest_sha256(normalized_entries),
    }
    if aggregate != expected_aggregate:
        raise RunnerBuildError("wheel_manifest.aggregate:mismatch")
    raw["entries"] = normalized_entries
    return raw


def validate_build_profile(content: bytes) -> dict[str, Any]:
    """Validate the one reviewed offline-runner profile, byte for byte.

    This intentionally is not a caller-extensible profile language.  Changing
    any build meaning requires changing the source-bound profile and this
    validator together, which makes an alternate caller-supplied Dockerfile,
    cache layout, network mode, or installation command fail closed.
    """

    raw = _strict_json_bytes(content, "build_profile")
    if _canonical_json_bytes(raw) != content:
        raise RunnerBuildError("build_profile:not_canonical_json")
    _exact_keys(
        raw,
        {
            "schema_version",
            "id",
            "platform",
            "external_order_submission_enabled",
            "source_materials",
            "wheelhouse",
            "docker",
            "base_images",
            "installation",
            "required_image_labels",
        },
        "build_profile",
    )
    if raw["schema_version"] != BUILD_PROFILE_SCHEMA_VERSION:
        raise RunnerBuildError("build_profile:schema_version_unsupported")
    if raw["id"] != "phase3-python312-node20-offline":
        raise RunnerBuildError("build_profile:id_mismatch")
    if raw["platform"] != {
        "os": "linux",
        "architecture": "amd64",
        "python": "3.12",
        "glibc_max": "2.36",
    }:
        raise RunnerBuildError("build_profile:platform_mismatch")
    if raw["external_order_submission_enabled"] is not False:
        raise RunnerBuildError("build_profile:external_order_submission_must_be_false")
    if raw["source_materials"] != {
        "dockerfile": DOCKERFILE_PATH,
        "requirements_lock": REQUIREMENTS_LOCK_PATH,
        "wheel_manifest": WHEEL_MANIFEST_PATH,
        "materializer": MATERIALIZER_PATH,
    }:
        raise RunnerBuildError("build_profile:source_materials_mismatch")
    if raw["wheelhouse"] != {
        "context_prefix": WHEELHOUSE_PREFIX,
        "hashed_requirements_name": "requirements.hashed.txt",
        "manifest_name": "python-wheel-manifest.lock.json",
    }:
        raise RunnerBuildError("build_profile:wheelhouse_mismatch")
    if raw["docker"] != {
        "network_mode": "none",
        "pull": False,
        "no_cache": True,
        "shell": False,
        "context_transport": "verified_tar_stdin",
    }:
        raise RunnerBuildError("build_profile:docker_boundary_mismatch")

    expected_bases = [
        {
            "argument": "NODE_IMAGE",
            "stage": "node_runtime",
            "reference": "docker.io/library/node@sha256:"
            "2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0",
            "digest": "sha256:"
            "2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0",
        },
        {
            "argument": "PYTHON_IMAGE",
            "stage": "runtime",
            "reference": "docker.io/library/python@sha256:"
            "a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134",
            "digest": "sha256:"
            "a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134",
        },
    ]
    bases = raw["base_images"]
    if bases != expected_bases:
        raise RunnerBuildError("build_profile:base_images_mismatch")
    if raw["installation"] != {
        "argv": [
            "python",
            "-m",
            "pip",
            "install",
            "--no-cache-dir",
            "--no-index",
            "--no-deps",
            "--only-binary=:all:",
            "--require-hashes",
            "--find-links=/opt/qt-assurance/wheelhouse",
            "-r",
            "/opt/qt-assurance/requirements.hashed.txt",
        ],
        "pip_check_argv": ["python", "-m", "pip", "check"],
        "intentionally_unavailable_executables": ["psql"],
    }:
        raise RunnerBuildError("build_profile:installation_mismatch")
    if raw["required_image_labels"] != list(REQUIRED_IMAGE_LABELS):
        raise RunnerBuildError("build_profile:required_image_labels_mismatch")
    return raw


def validate_bound_dockerfile(content: bytes, profile: Mapping[str, Any]) -> None:
    """Cross-check the fixed Dockerfile against the reviewed profile meaning."""

    try:
        text = content.decode("utf-8")
    except UnicodeError as exc:
        raise RunnerBuildError("dockerfile:not_utf8") from exc
    if not text.endswith("\n") or "\r" in text or "\x00" in text:
        raise RunnerBuildError("dockerfile:canonical_text_required")
    if re.search(r"\bpsql\b|postgresql-client|\bapt(?:-get)?\b|\bapk\b", text, re.I):
        raise RunnerBuildError("dockerfile:unapproved_executable_or_package_manager")
    if re.search(r"(?m)^\s*ADD\s", text, re.I):
        raise RunnerBuildError("dockerfile:add_forbidden")

    for base in profile["base_images"]:
        argument = re.escape(base["argument"])
        reference = re.escape(base["reference"])
        stage = re.escape(base["stage"])
        if len(re.findall(rf"(?m)^ARG {argument}={reference}$", text)) != 1:
            raise RunnerBuildError(f"dockerfile:base_argument_mismatch:{base['argument']}")
        if len(re.findall(rf"(?m)^FROM \$\{{{argument}\}} AS {stage}$", text)) != 1:
            raise RunnerBuildError(f"dockerfile:base_stage_mismatch:{base['stage']}")
    if len(re.findall(r"(?m)^FROM ", text)) != len(profile["base_images"]):
        raise RunnerBuildError("dockerfile:unprofiled_base_stage")

    run_lines = [line[4:] for line in text.splitlines() if line.startswith("RUN ")]
    if len(run_lines) != 4 or any(not line.startswith("[") for line in run_lines):
        raise RunnerBuildError("dockerfile:exec_form_run_set_mismatch")
    try:
        run_commands = [json.loads(line) for line in run_lines]
    except json.JSONDecodeError as exc:
        raise RunnerBuildError("dockerfile:run_exec_json_invalid") from exc
    if run_commands != [
        profile["installation"]["argv"],
        profile["installation"]["pip_check_argv"],
        ["python", "--version"],
        ["node", "--version"],
    ]:
        raise RunnerBuildError("dockerfile:installation_commands_mismatch")
    expected_copies = [
        "COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node",
        "COPY .qt-assurance-wheelhouse/python-wheel-manifest.lock.json "
        "/opt/qt-assurance/python-wheel-manifest.lock.json",
        "COPY .qt-assurance-wheelhouse/requirements.hashed.txt "
        "/opt/qt-assurance/requirements.hashed.txt",
        "COPY .qt-assurance-wheelhouse/wheelhouse/ /opt/qt-assurance/wheelhouse/",
    ]
    observed_copies = [line for line in text.splitlines() if line.startswith("COPY ")]
    if observed_copies != expected_copies:
        raise RunnerBuildError("dockerfile:closed_context_copy_set_mismatch")
    expected_text = "\n".join(
        [
            *(
                f"ARG {base['argument']}={base['reference']}"
                for base in profile["base_images"]
            ),
            "",
            "FROM ${NODE_IMAGE} AS node_runtime",
            "",
            "FROM ${PYTHON_IMAGE} AS runtime",
            "",
            "COPY --from=node_runtime /usr/local/bin/node /usr/local/bin/node",
            "",
            "ENV PYTHONDONTWRITEBYTECODE=1 \\",
            "    PYTHONUNBUFFERED=1 \\",
            "    PIP_DISABLE_PIP_VERSION_CHECK=1",
            "",
            "WORKDIR /workspace",
            "",
            "COPY .qt-assurance-wheelhouse/python-wheel-manifest.lock.json "
            "/opt/qt-assurance/python-wheel-manifest.lock.json",
            "COPY .qt-assurance-wheelhouse/requirements.hashed.txt "
            "/opt/qt-assurance/requirements.hashed.txt",
            "COPY .qt-assurance-wheelhouse/wheelhouse/ /opt/qt-assurance/wheelhouse/",
            "",
            *(f"RUN {json.dumps(argv)}" for argv in run_commands),
            "",
            "ENTRYPOINT []",
            'CMD ["node", "--version"]',
            "",
        ]
    )
    if text != expected_text:
        raise RunnerBuildError("dockerfile:canonical_instruction_set_mismatch")


def _safe_zip_member(value: str, where: str, *, directory: bool = False) -> str:
    if directory:
        if not value.endswith("/"):
            raise RunnerBuildError(f"{where}:canonical_directory_marker_required")
        value = value[:-1]
    return _portable_relative(value, where)


def _record_digest(value: bytes) -> str:
    return "sha256=" + base64.urlsafe_b64encode(
        hashlib.sha256(value).digest()
    ).rstrip(b"=").decode("ascii")


def validate_wheel_snapshot(data: bytes, entry: Mapping[str, Any]) -> None:
    where = f"wheel:{entry['name']}=={entry['version']}"
    if len(data) != entry["size"] or _sha256_bytes(data) != entry["sha256"]:
        raise RunnerBuildError(f"{where}:artifact_identity_mismatch")
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except (OSError, zipfile.BadZipFile) as exc:
        raise RunnerBuildError(f"{where}:invalid_zip") from exc
    with archive:
        infos = archive.infolist()
        if not infos:
            raise RunnerBuildError(f"{where}:empty_zip")
        expanded_bytes = sum(info.file_size for info in infos)
        if (
            expanded_bytes > 4 * 1024 * 1024 * 1024
            or any(info.file_size > 1024 * 1024 * 1024 for info in infos)
            or expanded_bytes > max(len(data) * 1000, len(data) + 1024 * 1024)
        ):
            raise RunnerBuildError(f"{where}:unreasonable_zip_expansion")
        if any(info.flag_bits & 0x1 for info in infos):
            raise RunnerBuildError(f"{where}:encrypted_member_forbidden")
        try:
            bad_crc_member = archive.testzip()
        except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
            raise RunnerBuildError(f"{where}:crc_or_read_failure") from exc
        if bad_crc_member is not None:
            raise RunnerBuildError(f"{where}:crc_failure:{bad_crc_member}")
        member_bytes: dict[str, bytes] = {}
        folded: set[str] = set()
        for index, info in enumerate(infos):
            name = _safe_zip_member(
                info.filename,
                f"{where}.members[{index}]",
                directory=info.is_dir(),
            )
            folded_name = unicodedata.normalize("NFC", name).casefold()
            if folded_name in folded:
                raise RunnerBuildError(f"{where}:member_path_collision")
            folded.add(folded_name)
            file_type = (info.external_attr >> 16) & 0o170000
            if info.is_dir():
                if file_type not in {0, stat.S_IFDIR} or info.file_size != 0:
                    raise RunnerBuildError(f"{where}:directory_type_invalid")
                continue
            if file_type not in {0, stat.S_IFREG}:
                raise RunnerBuildError(f"{where}:symlink_or_special_member")
            try:
                payload = archive.read(info)
            except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
                raise RunnerBuildError(f"{where}:crc_or_read_failure") from exc
            if len(payload) != info.file_size:
                raise RunnerBuildError(f"{where}:member_size_mismatch")
            member_bytes[name] = payload
        dist_info_dirs = {
            PurePosixPath(name).parts[0]
            for name in member_bytes
            if PurePosixPath(name).parts[0].endswith(".dist-info")
        }
        if len(dist_info_dirs) != 1:
            raise RunnerBuildError(f"{where}:exactly_one_root_dist_info_required")
        dist_info = next(iter(dist_info_dirs))
        dist_identity = dist_info[: -len(".dist-info")]
        if "-" not in dist_identity:
            raise RunnerBuildError(f"{where}:dist_info_identity_invalid")
        dist_name, dist_version = dist_identity.rsplit("-", 1)
        try:
            normalized_dist_version = Version(dist_version)
        except InvalidVersion as exc:
            raise RunnerBuildError(f"{where}:dist_info_version_invalid") from exc
        if (
            _normalized_name(dist_name) != entry["name"]
            or normalized_dist_version != Version(entry["version"])
        ):
            raise RunnerBuildError(f"{where}:dist_info_identity_mismatch")
        metadata_path = f"{dist_info}/METADATA"
        wheel_path = f"{dist_info}/WHEEL"
        record_path = f"{dist_info}/RECORD"
        if any(
            path not in member_bytes
            for path in (metadata_path, wheel_path, record_path)
        ):
            raise RunnerBuildError(f"{where}:root_metadata_wheel_or_record_missing")
        metadata = email.parser.BytesParser(policy=email.policy.default).parsebytes(
            member_bytes[metadata_path]
        )
        if metadata.defects:
            raise RunnerBuildError(f"{where}:metadata_parse_defect")
        if len(metadata.get_all("Name", [])) != 1 or _normalized_name(
            metadata.get("Name", "")
        ) != entry["name"]:
            raise RunnerBuildError(f"{where}:metadata_name_mismatch")
        if len(metadata.get_all("Version", [])) != 1 or metadata.get(
            "Version"
        ) != entry["version"]:
            raise RunnerBuildError(f"{where}:metadata_version_mismatch")
        requires_values = metadata.get_all("Requires-Python", [])
        if len(requires_values) > 1:
            raise RunnerBuildError(f"{where}:requires_python_duplicated")
        observed_requires = (requires_values[0] if requires_values else None) or None
        if observed_requires != entry["requires_python"]:
            raise RunnerBuildError(f"{where}:requires_python_mismatch")
        wheel_metadata = email.parser.BytesParser(policy=email.policy.default).parsebytes(
            member_bytes[wheel_path]
        )
        if wheel_metadata.defects:
            raise RunnerBuildError(f"{where}:wheel_metadata_parse_defect")
        if len(wheel_metadata.get_all("Wheel-Version", [])) != 1:
            raise RunnerBuildError(f"{where}:wheel_version_missing_or_duplicate")
        wheel_version = wheel_metadata.get("Wheel-Version", "")
        if not re.fullmatch(r"1(?:\.\d+)?", wheel_version):
            raise RunnerBuildError(f"{where}:wheel_version_unsupported")
        if wheel_metadata.get_all("Root-Is-Purelib", []) not in [["true"], ["false"]]:
            raise RunnerBuildError(f"{where}:root_is_purelib_invalid")
        observed_tags = wheel_metadata.get_all("Tag") or []
        if observed_tags != entry["wheel_tags"]:
            raise RunnerBuildError(f"{where}:wheel_tags_mismatch")
        if not any(_tag_is_compatible(tag) for tag in observed_tags):
            raise RunnerBuildError(f"{where}:no_compatible_tag")
        try:
            record_text = member_bytes[record_path].decode("utf-8")
            rows = list(csv.reader(io.StringIO(record_text, newline="")))
        except (UnicodeError, csv.Error) as exc:
            raise RunnerBuildError(f"{where}:record_invalid") from exc
        recorded: dict[str, tuple[str, str]] = {}
        for row_index, row in enumerate(rows):
            if len(row) != 3:
                raise RunnerBuildError(f"{where}:record_row_shape:{row_index}")
            path = _safe_zip_member(row[0], f"{where}.record[{row_index}]")
            folded_path = unicodedata.normalize("NFC", path).casefold()
            if any(
                unicodedata.normalize("NFC", existing).casefold() == folded_path
                for existing in recorded
            ):
                raise RunnerBuildError(f"{where}:record_path_collision")
            recorded[path] = (row[1], row[2])
        if set(recorded) != set(member_bytes):
            raise RunnerBuildError(f"{where}:record_inventory_mismatch")
        for path, payload in member_bytes.items():
            digest, size = recorded[path]
            if path == record_path:
                if digest or size:
                    raise RunnerBuildError(f"{where}:record_self_must_be_unhashed")
                continue
            if digest != _record_digest(payload) or size != str(len(payload)):
                raise RunnerBuildError(f"{where}:record_integrity_mismatch:{path}")


def _is_within(path: Path, anchor: Path) -> bool:
    try:
        path.relative_to(anchor)
        return True
    except ValueError:
        return False


def _resolved_external_directory(
    path: Path, *, root: Path, where: str, private: bool
) -> Path:
    if not path.is_absolute():
        raise RunnerBuildError(f"{where}:absolute_path_required")
    lexical = path.absolute()
    try:
        resolved = path.resolve(strict=True)
        mode = resolved.lstat().st_mode
    except OSError as exc:
        raise RunnerBuildError(f"{where}:unavailable") from exc
    if lexical != resolved:
        raise RunnerBuildError(f"{where}:symlinked_or_noncanonical_path")
    if (
        not stat.S_ISDIR(mode)
        or stat.S_ISLNK(mode)
        or (private and mode & 0o077)
        or _is_within(resolved, root.resolve())
    ):
        raise RunnerBuildError(f"{where}:external_real_directory_required")
    return resolved


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )


def _anchor_external_directory(
    path: Path, *, root: Path, where: str, private: bool
) -> _AnchoredDirectory:
    resolved = _resolved_external_directory(
        path, root=root, where=where, private=private
    )
    before = resolved.lstat()
    try:
        descriptor = os.open(resolved, _directory_flags())
    except OSError as exc:
        raise RunnerBuildError(f"{where}:directory_open_failed") from exc
    try:
        opened = os.fstat(descriptor)
        after = resolved.lstat()
        identity = (before.st_dev, before.st_ino)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or stat.S_ISLNK(after.st_mode)
            or (opened.st_dev, opened.st_ino) != identity
            or (after.st_dev, after.st_ino) != identity
            or (private and opened.st_mode & 0o077)
        ):
            raise RunnerBuildError(f"{where}:directory_changed_while_opening")
    except BaseException:
        os.close(descriptor)
        raise
    return _AnchoredDirectory(
        resolved, descriptor, opened.st_dev, opened.st_ino, where
    )


def _create_external_directory(
    path: Path, *, root: Path, where: str
) -> _AnchoredDirectory:
    if not path.is_absolute():
        raise RunnerBuildError(f"{where}:absolute_path_required")
    name = _portable_component(path.name, f"{where}.name")
    lexical_parent = path.parent.absolute()
    try:
        resolved_parent = path.parent.resolve(strict=True)
        parent_mode = resolved_parent.lstat().st_mode
    except OSError as exc:
        raise RunnerBuildError(f"{where}:parent_unavailable") from exc
    if lexical_parent != resolved_parent:
        raise RunnerBuildError(f"{where}:symlinked_or_noncanonical_parent")
    target = resolved_parent / name
    if (
        not stat.S_ISDIR(parent_mode)
        or stat.S_ISLNK(parent_mode)
        or _is_within(target, root.resolve())
        or target.exists()
        or target.is_symlink()
    ):
        raise RunnerBuildError(f"{where}:unsafe_or_existing_target")
    try:
        parent_descriptor = os.open(resolved_parent, _directory_flags())
    except OSError as exc:
        raise RunnerBuildError(f"{where}:parent_open_failed") from exc
    child_descriptor = -1
    try:
        opened_parent = os.fstat(parent_descriptor)
        visible_parent = resolved_parent.lstat()
        if (
            (opened_parent.st_dev, opened_parent.st_ino)
            != (visible_parent.st_dev, visible_parent.st_ino)
            or not stat.S_ISDIR(opened_parent.st_mode)
            or stat.S_ISLNK(visible_parent.st_mode)
        ):
            raise RunnerBuildError(f"{where}:parent_changed_while_opening")
        try:
            os.mkdir(name, 0o700, dir_fd=parent_descriptor)
        except OSError as exc:
            raise RunnerBuildError(f"{where}:create_failed") from exc
        try:
            child_descriptor = os.open(
                name, _directory_flags(), dir_fd=parent_descriptor
            )
        except OSError as exc:
            raise RunnerBuildError(f"{where}:created_directory_reopen_failed") from exc
        child_stat = os.fstat(child_descriptor)
        visible_child = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if (
            (child_stat.st_dev, child_stat.st_ino)
            != (visible_child.st_dev, visible_child.st_ino)
            or not stat.S_ISDIR(child_stat.st_mode)
            or stat.S_ISLNK(visible_child.st_mode)
            or child_stat.st_mode & 0o077
        ):
            raise RunnerBuildError(f"{where}:created_directory_not_private")
        anchor = _AnchoredDirectory(
            target,
            child_descriptor,
            child_stat.st_dev,
            child_stat.st_ino,
            where,
        )
        anchor.assert_identity()
        child_descriptor = -1
        return anchor
    except BaseException:
        if child_descriptor >= 0:
            os.close(child_descriptor)
        raise
    finally:
        os.close(parent_descriptor)


def _read_regular_once(path: Path, where: str) -> bytes:
    """Read one final path once while rejecting links, mutation, and specials."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RunnerBuildError(f"{where}:open_failed:{type(exc).__name__}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RunnerBuildError(f"{where}:regular_file_required")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RunnerBuildError(f"{where}:short_read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RunnerBuildError(f"{where}:grew_during_read")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after:
        raise RunnerBuildError(f"{where}:changed_during_read")
    return b"".join(chunks)


def _read_regular_at_once(
    directory: _AnchoredDirectory, basename: str, where: str
) -> bytes:
    """Read one regular bundle member through a retained directory descriptor."""

    basename = _portable_component(basename, f"{where}.basename")
    directory.assert_identity()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(basename, flags, dir_fd=directory.descriptor)
    except OSError as exc:
        raise RunnerBuildError(f"{where}:open_failed:{type(exc).__name__}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RunnerBuildError(f"{where}:regular_file_required")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RunnerBuildError(f"{where}:short_read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RunnerBuildError(f"{where}:grew_during_read")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after:
        raise RunnerBuildError(f"{where}:changed_during_read")
    directory.assert_identity()
    return b"".join(chunks)


def _read_beneath_once(root: Path, relative: str, where: str) -> bytes:
    """Open every cache component without following a symlink."""

    relative = _portable_relative(relative, where)
    if not root.is_absolute():
        raise RunnerBuildError(f"{where}:cache_root_must_be_absolute")
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise RunnerBuildError(f"{where}:cache_root_unavailable") from exc
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise RunnerBuildError(f"{where}:cache_root_real_directory_required")
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        descriptor = os.open(root, directory_flags)
    except OSError as exc:
        raise RunnerBuildError(f"{where}:cache_root_open_failed") from exc
    try:
        parts = PurePosixPath(relative).parts
        for part in parts[:-1]:
            try:
                child = os.open(part, directory_flags, dir_fd=descriptor)
            except OSError as exc:
                raise RunnerBuildError(f"{where}:unsafe_cache_ancestor") from exc
            os.close(descriptor)
            descriptor = child
        file_flags = (
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            file_descriptor = os.open(parts[-1], file_flags, dir_fd=descriptor)
        except OSError as exc:
            raise RunnerBuildError(f"{where}:cache_candidate_open_failed") from exc
    finally:
        os.close(descriptor)
    try:
        before = os.fstat(file_descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RunnerBuildError(f"{where}:cache_candidate_not_regular")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(file_descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RunnerBuildError(f"{where}:cache_candidate_short_read")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(file_descriptor, 1):
            raise RunnerBuildError(f"{where}:cache_candidate_grew")
        after = os.fstat(file_descriptor)
    finally:
        os.close(file_descriptor)
    if (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise RunnerBuildError(f"{where}:cache_candidate_changed")
    return b"".join(chunks)


def discover_wheel_snapshots(
    cache_roots: Sequence[Path],
    manifest: Mapping[str, Any],
    *,
    source_root: Path | None = None,
) -> dict[str, bytes]:
    """Discover each hash-selected wheel without retaining cache provenance.

    Candidate bytes are read exactly once from their cache location.  Every
    selected digest must have exactly one matching cache candidate; even a
    byte-identical duplicate is rejected so traversal order never selects the
    physical build input.
    """

    if not cache_roots:
        raise RunnerBuildError("wheel_cache_root_required")
    expected = {item["filename"]: item for item in manifest["entries"]}
    expected_by_digest = {item["sha256"]: item for item in manifest["entries"]}
    expected_sizes = {item["size"] for item in manifest["entries"]}
    source_root = source_root.resolve() if source_root is not None else None
    candidate_relatives: list[tuple[Path, str]] = []
    normalized_roots: list[Path] = []
    candidate_count = 0
    for index, raw_root in enumerate(cache_roots):
        if not raw_root.is_absolute():
            raise RunnerBuildError(f"wheel_cache[{index}]:absolute_path_required")
        try:
            root = raw_root.resolve(strict=True)
            root_stat = root.lstat()
        except OSError as exc:
            raise RunnerBuildError(f"wheel_cache[{index}]:unavailable") from exc
        if raw_root.absolute() != root:
            raise RunnerBuildError(f"wheel_cache[{index}]:symlinked_or_noncanonical")
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise RunnerBuildError(f"wheel_cache[{index}]:real_directory_required")
        if source_root is not None and (
            _is_within(root, source_root) or _is_within(source_root, root)
        ):
            raise RunnerBuildError(f"wheel_cache[{index}]:must_be_external_to_source")
        root_key = os.path.normcase(str(root))
        if root_key in {os.path.normcase(str(item)) for item in normalized_roots}:
            raise RunnerBuildError("wheel_cache:duplicate_root")
        normalized_roots.append(root)
        for current, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
            current_path = Path(current)
            safe_directories: list[str] = []
            for dirname in sorted(dirnames):
                candidate = current_path / dirname
                try:
                    mode = candidate.lstat().st_mode
                except OSError:
                    continue
                if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
                    safe_directories.append(dirname)
            dirnames[:] = safe_directories
            for filename in sorted(filenames):
                if not filename.casefold().endswith((".body", ".whl")):
                    continue
                candidate = current_path / filename
                candidate_count += 1
                if candidate_count > MAX_CACHE_CANDIDATES:
                    raise RunnerBuildError("wheel_cache:candidate_limit_exceeded")
                try:
                    mode = candidate.lstat().st_mode
                except OSError as exc:
                    raise RunnerBuildError("wheel_cache:candidate_lstat_failed") from exc
                if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                    raise RunnerBuildError("wheel_cache:candidate_not_real_regular")
                if candidate.lstat().st_size not in expected_sizes:
                    continue
                relative = candidate.relative_to(root).as_posix()
                candidate_relatives.append((root, relative))

    snapshots: dict[str, bytes] = {}
    for cache_root, relative in sorted(
        candidate_relatives, key=lambda item: (str(item[0]), item[1])
    ):
        data = _read_beneath_once(cache_root, relative, "wheel_cache.candidate")
        entry = expected_by_digest.get(_sha256_bytes(data))
        if entry is None or len(data) != entry["size"]:
            continue
        validate_wheel_snapshot(data, entry)
        filename = entry["filename"]
        previous = snapshots.get(filename)
        if previous is not None:
            raise RunnerBuildError(
                f"wheel_cache:duplicate_matching_candidate:{filename}"
            )
        snapshots[filename] = data
    missing = sorted(set(expected) - set(snapshots))
    if missing:
        raise RunnerBuildError("wheel_cache:artifact_missing:" + ",".join(missing))
    return snapshots


def _hashed_requirements_bytes(entries: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        f"{item['name']}=={item['version']} --hash=sha256:{item['sha256']}\n"
        for item in entries
    ).encode("utf-8")


def _inventory(entries: Mapping[str, bytes]) -> list[dict[str, Any]]:
    return [
        {"path": path, "sha256": _sha256_bytes(entries[path]), "size": len(entries[path])}
        for path in sorted(entries)
    ]


def _deterministic_tar(entries: Mapping[str, bytes]) -> bytes:
    normalized: dict[str, bytes] = {}
    folded: set[str] = set()
    for raw_path, content in entries.items():
        path = _portable_relative(raw_path, "tar.path")
        folded_path = unicodedata.normalize("NFC", path).casefold()
        if folded_path in folded:
            raise RunnerBuildError("tar:path_collision")
        if not isinstance(content, bytes):
            raise RunnerBuildError("tar:bytes_required")
        folded.add(folded_path)
        normalized[path] = content
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for path in sorted(normalized):
            payload = normalized[path]
            info = tarfile.TarInfo(path)
            info.size = len(payload)
            info.mode = 0o444
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue()


def _validate_tar_snapshot(
    content: bytes, expected_inventory: Sequence[Mapping[str, Any]], where: str
) -> dict[str, bytes]:
    expected = {item["path"]: dict(item) for item in expected_inventory}
    if len(expected) != len(expected_inventory):
        raise RunnerBuildError(f"{where}:inventory_duplicate")
    try:
        archive = tarfile.open(fileobj=io.BytesIO(content), mode="r:")
    except (OSError, tarfile.TarError) as exc:
        raise RunnerBuildError(f"{where}:invalid_tar") from exc
    extracted: dict[str, bytes] = {}
    folded: set[str] = set()
    with archive:
        for index, member in enumerate(archive.getmembers()):
            path = _portable_relative(member.name, f"{where}.members[{index}]")
            folded_path = unicodedata.normalize("NFC", path).casefold()
            if folded_path in folded:
                raise RunnerBuildError(f"{where}:member_path_collision")
            folded.add(folded_path)
            if (
                not member.isfile()
                or member.mode != 0o444
                or member.uid != 0
                or member.gid != 0
                or member.uname
                or member.gname
                or member.mtime != 0
            ):
                raise RunnerBuildError(f"{where}:noncanonical_member:{path}")
            handle = archive.extractfile(member)
            if handle is None:
                raise RunnerBuildError(f"{where}:member_unreadable:{path}")
            payload = handle.read()
            if len(payload) != member.size:
                raise RunnerBuildError(f"{where}:member_size_mismatch:{path}")
            extracted[path] = payload
    if set(extracted) != set(expected):
        raise RunnerBuildError(f"{where}:closed_inventory_mismatch")
    for path, payload in extracted.items():
        row = expected[path]
        _exact_keys(row, {"path", "sha256", "size"}, f"{where}.inventory[{path}]")
        if row["sha256"] != _sha256_bytes(payload) or row["size"] != len(payload):
            raise RunnerBuildError(f"{where}:inventory_identity_mismatch:{path}")
    if _deterministic_tar(extracted) != content:
        raise RunnerBuildError(f"{where}:tar_bytes_not_deterministic")
    return extracted


def _write_create_only_at(
    directory: _AnchoredDirectory, basename: str, content: bytes, where: str
) -> None:
    """Create one durable bundle member without re-resolving the directory path."""

    basename = _portable_component(basename, f"{where}.basename")
    directory.assert_identity()
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(
            basename, flags, 0o600, dir_fd=directory.descriptor
        )
    except OSError as exc:
        raise RunnerBuildError(f"{where}:create_only_write_failed") from exc
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            os.unlink(basename, dir_fd=directory.descriptor)
        except OSError:
            pass
        raise
    directory.assert_identity()


def _fsync_anchored_directory(directory: _AnchoredDirectory, where: str) -> None:
    directory.assert_identity()
    try:
        os.fsync(directory.descriptor)
    except OSError as exc:
        raise RunnerBuildError(f"{where}:directory_fsync_failed") from exc
    directory.assert_identity()


def _source_contract(root: Path, source_commit: str) -> dict[str, Any]:
    if not COMMIT_RE.fullmatch(source_commit):
        raise RunnerBuildError("source_commit_invalid")
    resolved = _git(root, "rev-parse", f"{source_commit}^{{commit}}").decode("ascii").strip()
    if resolved != source_commit:
        raise RunnerBuildError("source_commit_unresolvable")
    tree = _git(root, "rev-parse", f"{source_commit}^{{tree}}").decode("ascii").strip()
    if not COMMIT_RE.fullmatch(tree):
        raise RunnerBuildError("source_tree_invalid")
    paths = {
        "build_profile": BUILD_PROFILE_PATH,
        "dockerfile": DOCKERFILE_PATH,
        "requirements_lock": REQUIREMENTS_LOCK_PATH,
        "wheel_manifest": WHEEL_MANIFEST_PATH,
        "materializer": MATERIALIZER_PATH,
    }
    source_bytes = {
        name: _bound_source_bytes(root, source_commit, path) for name, path in paths.items()
    }
    profile = validate_build_profile(source_bytes["build_profile"])
    manifest = validate_wheel_manifest(
        source_bytes["wheel_manifest"], source_bytes["requirements_lock"]
    )
    validate_bound_dockerfile(source_bytes["dockerfile"], profile)
    materials = {
        name: {"path": paths[name], "sha256": _sha256_bytes(source_bytes[name])}
        for name in sorted(paths)
    }
    return {
        "source": {"commit": source_commit, "tree": tree},
        "source_bytes": source_bytes,
        "source_materials": materials,
        "profile": profile,
        "manifest": manifest,
    }


def _materialization_payload(
    *,
    contract: Mapping[str, Any],
    wheelhouse_tar: bytes,
    wheelhouse_inventory: Sequence[Mapping[str, Any]],
    context_tar: bytes,
    context_inventory: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": MATERIALIZATION_SCHEMA_VERSION,
        "source": contract["source"],
        "profile": {
            "id": contract["profile"]["id"],
            "sha256": contract["source_materials"]["build_profile"]["sha256"],
        },
        "source_materials": contract["source_materials"],
        "wheel_artifacts": {
            "entries": contract["manifest"]["entries"],
            "aggregate": contract["manifest"]["aggregate"],
            "tar": {
                "name": WHEELHOUSE_TAR_NAME,
                "sha256": _sha256_bytes(wheelhouse_tar),
                "size": len(wheelhouse_tar),
            },
            "inventory": list(wheelhouse_inventory),
        },
        "build_context": {
            "tar": {
                "name": CONTEXT_TAR_NAME,
                "sha256": _sha256_bytes(context_tar),
                "size": len(context_tar),
            },
            "inventory": list(context_inventory),
        },
        "external_order_submission_enabled": False,
    }


def materialize_runner(
    *,
    root: Path,
    source_commit: str,
    cache_roots: Sequence[Path],
    output_root: Path,
) -> MaterializedRunner:
    root = root.resolve()
    if not output_root.is_absolute():
        raise RunnerBuildError("materialization_output_must_be_external_absolute")
    require_exact_clean_source(root, source_commit)
    contract = _source_contract(root, source_commit)
    wheels = discover_wheel_snapshots(
        cache_roots, contract["manifest"], source_root=root
    )
    require_exact_clean_source(root, source_commit)
    profile = contract["profile"]
    prefix = profile["wheelhouse"]["context_prefix"]
    hashed_requirements = _hashed_requirements_bytes(contract["manifest"]["entries"])
    wheelhouse_entries: dict[str, bytes] = {
        profile["wheelhouse"]["hashed_requirements_name"]: hashed_requirements,
        profile["wheelhouse"]["manifest_name"]: contract["source_bytes"]["wheel_manifest"],
        **{f"wheelhouse/{name}": content for name, content in wheels.items()},
    }
    wheelhouse_inventory = _inventory(wheelhouse_entries)
    wheelhouse_tar = _deterministic_tar(wheelhouse_entries)
    context_entries = {
        DOCKERFILE_PATH: contract["source_bytes"]["dockerfile"],
        **{f"{prefix}/{path}": content for path, content in wheelhouse_entries.items()},
    }
    context_inventory = _inventory(context_entries)
    context_tar = _deterministic_tar(context_entries)
    receipt = _materialization_payload(
        contract=contract,
        wheelhouse_tar=wheelhouse_tar,
        wheelhouse_inventory=wheelhouse_inventory,
        context_tar=context_tar,
        context_inventory=context_inventory,
    )
    require_exact_clean_source(root, source_commit)
    with _create_external_directory(
        output_root, root=root, where="materialization_output"
    ) as output_directory:
        output_path = output_directory.path
        wheelhouse_path = output_path / WHEELHOUSE_TAR_NAME
        context_path = output_path / CONTEXT_TAR_NAME
        receipt_path = output_path / MATERIALIZATION_RECEIPT_NAME
        _write_create_only_at(
            output_directory,
            WHEELHOUSE_TAR_NAME,
            wheelhouse_tar,
            "materialization.wheelhouse_tar",
        )
        _write_create_only_at(
            output_directory,
            CONTEXT_TAR_NAME,
            context_tar,
            "materialization.context_tar",
        )
        _fsync_anchored_directory(output_directory, "materialization.payloads")
        require_exact_clean_source(root, source_commit)
        _write_create_only_at(
            output_directory,
            MATERIALIZATION_RECEIPT_NAME,
            _canonical_json_bytes(receipt),
            "materialization.receipt",
        )
        _fsync_anchored_directory(output_directory, "materialization.receipt")
        require_exact_clean_source(root, source_commit)
        output_directory.assert_identity()
    return MaterializedRunner(output_path, wheelhouse_path, context_path, receipt_path)


def _validate_file_identity(
    value: Any, where: str, *, allowed_keys: set[str]
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RunnerBuildError(f"{where}:object_required")
    _exact_keys(value, allowed_keys, where)
    name_key = "name" if "name" in allowed_keys else "basename"
    name = _portable_component(_string(value[name_key], f"{where}.{name_key}"), where)
    digest = _hash(value["sha256"], f"{where}.sha256")
    size = value["size"]
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise RunnerBuildError(f"{where}.size:nonnegative_integer_required")
    return {name_key: name, "sha256": digest, "size": size}


def _validate_inventory(value: Any, where: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise RunnerBuildError(f"{where}:nonempty_array_required")
    result: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        item_where = f"{where}[{index}]"
        if not isinstance(raw, dict):
            raise RunnerBuildError(f"{item_where}:object_required")
        _exact_keys(raw, {"path", "sha256", "size"}, item_where)
        size = raw["size"]
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise RunnerBuildError(f"{item_where}.size:nonnegative_integer_required")
        result.append(
            {
                "path": _portable_relative(raw["path"], f"{item_where}.path"),
                "sha256": _hash(raw["sha256"], f"{item_where}.sha256"),
                "size": size,
            }
        )
    paths = [item["path"] for item in result]
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise RunnerBuildError(f"{where}:paths_must_be_unique_and_sorted")
    folded = [unicodedata.normalize("NFC", path).casefold() for path in paths]
    if len(folded) != len(set(folded)):
        raise RunnerBuildError(f"{where}:portable_path_collision")
    return result


def _expected_wheelhouse_entries(contract: Mapping[str, Any]) -> dict[str, bytes | None]:
    profile = contract["profile"]
    return {
        profile["wheelhouse"]["hashed_requirements_name"]: _hashed_requirements_bytes(
            contract["manifest"]["entries"]
        ),
        profile["wheelhouse"]["manifest_name"]: contract["source_bytes"][
            "wheel_manifest"
        ],
        **{
            f"wheelhouse/{item['filename']}": None
            for item in contract["manifest"]["entries"]
        },
    }


def _validate_materialization_payload(
    raw: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> dict[str, Any]:
    _exact_keys(
        raw,
        {
            "schema_version",
            "source",
            "profile",
            "source_materials",
            "wheel_artifacts",
            "build_context",
            "external_order_submission_enabled",
        },
        "materialization",
    )
    if raw["schema_version"] != MATERIALIZATION_SCHEMA_VERSION:
        raise RunnerBuildError("materialization:schema_version_unsupported")
    if raw["source"] != contract["source"]:
        raise RunnerBuildError("materialization:source_binding_mismatch")
    if raw["profile"] != {
        "id": contract["profile"]["id"],
        "sha256": contract["source_materials"]["build_profile"]["sha256"],
    }:
        raise RunnerBuildError("materialization:profile_binding_mismatch")
    if raw["source_materials"] != contract["source_materials"]:
        raise RunnerBuildError("materialization:source_materials_mismatch")
    if raw["external_order_submission_enabled"] is not False:
        raise RunnerBuildError("materialization:external_order_submission_must_be_false")

    wheel_artifacts = raw["wheel_artifacts"]
    if not isinstance(wheel_artifacts, dict):
        raise RunnerBuildError("materialization.wheel_artifacts:object_required")
    _exact_keys(
        wheel_artifacts,
        {"entries", "aggregate", "tar", "inventory"},
        "materialization.wheel_artifacts",
    )
    if wheel_artifacts["entries"] != contract["manifest"]["entries"]:
        raise RunnerBuildError("materialization.wheel_artifacts:entries_mismatch")
    if wheel_artifacts["aggregate"] != contract["manifest"]["aggregate"]:
        raise RunnerBuildError("materialization.wheel_artifacts:aggregate_mismatch")
    wheel_tar = _validate_file_identity(
        wheel_artifacts["tar"],
        "materialization.wheel_artifacts.tar",
        allowed_keys={"name", "sha256", "size"},
    )
    if wheel_tar["name"] != WHEELHOUSE_TAR_NAME:
        raise RunnerBuildError("materialization.wheel_artifacts.tar:name_mismatch")
    wheel_inventory = _validate_inventory(
        wheel_artifacts["inventory"], "materialization.wheel_artifacts.inventory"
    )

    build_context = raw["build_context"]
    if not isinstance(build_context, dict):
        raise RunnerBuildError("materialization.build_context:object_required")
    _exact_keys(build_context, {"tar", "inventory"}, "materialization.build_context")
    context_tar = _validate_file_identity(
        build_context["tar"],
        "materialization.build_context.tar",
        allowed_keys={"name", "sha256", "size"},
    )
    if context_tar["name"] != CONTEXT_TAR_NAME:
        raise RunnerBuildError("materialization.build_context.tar:name_mismatch")
    context_inventory = _validate_inventory(
        build_context["inventory"], "materialization.build_context.inventory"
    )

    expected_wheelhouse = _expected_wheelhouse_entries(contract)
    if [item["path"] for item in wheel_inventory] != sorted(expected_wheelhouse):
        raise RunnerBuildError("materialization.wheel_artifacts:closed_inventory_mismatch")
    prefix = contract["profile"]["wheelhouse"]["context_prefix"]
    expected_context_paths = sorted(
        [DOCKERFILE_PATH, *(f"{prefix}/{path}" for path in expected_wheelhouse)]
    )
    if [item["path"] for item in context_inventory] != expected_context_paths:
        raise RunnerBuildError("materialization.build_context:closed_inventory_mismatch")
    return {
        "schema_version": MATERIALIZATION_SCHEMA_VERSION,
        "source": dict(raw["source"]),
        "profile": dict(raw["profile"]),
        "source_materials": json.loads(json.dumps(raw["source_materials"])),
        "wheel_artifacts": {
            "entries": json.loads(json.dumps(wheel_artifacts["entries"])),
            "aggregate": dict(wheel_artifacts["aggregate"]),
            "tar": wheel_tar,
            "inventory": wheel_inventory,
        },
        "build_context": {"tar": context_tar, "inventory": context_inventory},
        "external_order_submission_enabled": False,
    }


def _load_materialization_from_anchor(
    materialized_directory: _AnchoredDirectory,
    *,
    root: Path,
    source_commit: str,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    root = root.resolve()
    materialized_directory.assert_identity()
    try:
        observed_names = sorted(os.listdir(materialized_directory.descriptor))
    except OSError as exc:
        raise RunnerBuildError("materialization_root_inventory_unavailable") from exc
    if observed_names != sorted(
        [WHEELHOUSE_TAR_NAME, CONTEXT_TAR_NAME, MATERIALIZATION_RECEIPT_NAME]
    ):
        raise RunnerBuildError("materialization_root_closed_inventory_mismatch")
    contract = _source_contract(root, source_commit)
    receipt_bytes = _read_regular_at_once(
        materialized_directory,
        MATERIALIZATION_RECEIPT_NAME,
        "materialization.receipt",
    )
    raw = _strict_json_bytes(receipt_bytes, "materialization.receipt")
    if _canonical_json_bytes(raw) != receipt_bytes:
        raise RunnerBuildError("materialization.receipt:not_canonical_json")
    receipt = _validate_materialization_payload(raw, contract=contract)
    wheelhouse_bytes = _read_regular_at_once(
        materialized_directory,
        WHEELHOUSE_TAR_NAME,
        "materialization.wheelhouse_tar",
    )
    context_bytes = _read_regular_at_once(
        materialized_directory,
        CONTEXT_TAR_NAME,
        "materialization.context_tar",
    )
    for name, content, identity in (
        ("wheelhouse", wheelhouse_bytes, receipt["wheel_artifacts"]["tar"]),
        ("context", context_bytes, receipt["build_context"]["tar"]),
    ):
        if len(content) != identity["size"] or _sha256_bytes(content) != identity["sha256"]:
            raise RunnerBuildError(f"materialization.{name}_tar:identity_mismatch")
    wheelhouse_entries = _validate_tar_snapshot(
        wheelhouse_bytes,
        receipt["wheel_artifacts"]["inventory"],
        "materialization.wheelhouse_tar",
    )
    context_entries = _validate_tar_snapshot(
        context_bytes,
        receipt["build_context"]["inventory"],
        "materialization.context_tar",
    )
    expected_wheelhouse = _expected_wheelhouse_entries(contract)
    for path, expected_bytes in expected_wheelhouse.items():
        payload = wheelhouse_entries[path]
        if expected_bytes is not None and payload != expected_bytes:
            raise RunnerBuildError(f"materialization.wheelhouse_tar:bound_file_mismatch:{path}")
    entry_by_filename = {
        item["filename"]: item for item in contract["manifest"]["entries"]
    }
    for filename, entry in entry_by_filename.items():
        validate_wheel_snapshot(wheelhouse_entries[f"wheelhouse/{filename}"], entry)
    prefix = contract["profile"]["wheelhouse"]["context_prefix"]
    if context_entries[DOCKERFILE_PATH] != contract["source_bytes"]["dockerfile"]:
        raise RunnerBuildError("materialization.build_context:dockerfile_mismatch")
    for path, payload in wheelhouse_entries.items():
        if context_entries[f"{prefix}/{path}"] != payload:
            raise RunnerBuildError(f"materialization.build_context:wheelhouse_mismatch:{path}")
    materialized_directory.assert_identity()
    return receipt, {
        "receipt": receipt_bytes,
        "wheelhouse_tar": wheelhouse_bytes,
        "context_tar": context_bytes,
    }


def load_materialization(
    materialized_root: Path, *, root: Path, source_commit: str
) -> tuple[dict[str, Any], dict[str, bytes]]:
    root = root.resolve()
    with _anchor_external_directory(
        materialized_root,
        root=root,
        where="materialization_root",
        private=True,
    ) as materialized_directory:
        return _load_materialization_from_anchor(
            materialized_directory, root=root, source_commit=source_commit
        )


def _default_build_command_runner(
    argv: Sequence[str],
    env: Mapping[str, str],
    timeout_seconds: int,
    stdin_bytes: bytes,
) -> BuildCommandResult:
    """Run exactly one Docker build with a verified tar on standard input."""

    try:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=dict(env),
            shell=False,
            start_new_session=True,
        )
    except OSError as exc:
        raise RunnerBuildError(
            f"docker_build_start_failed:{type(exc).__name__}"
        ) from exc
    try:
        stdout, stderr = process.communicate(input=stdin_bytes, timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (AttributeError, OSError):
            process.kill()
        stdout, stderr = process.communicate()
        if not stdout:
            stdout = exc.stdout or b""
        if not stderr:
            stderr = exc.stderr or b""
        return BuildCommandResult(stdout, stderr, 124, True)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (AttributeError, OSError):
            process.kill()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        raise
    return BuildCommandResult(stdout, stderr, int(process.returncode), False)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise RunnerBuildError("timestamp_must_be_utc")
    return value.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_timestamp(value: Any, where: str) -> datetime:
    text = _string(value, where)
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", text):
        raise RunnerBuildError(f"{where}:canonical_utc_timestamp_required")
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise RunnerBuildError(f"{where}:invalid_timestamp") from exc


def _resolved_docker_identity(docker_path: Path) -> tuple[Path, dict[str, str]]:
    if not docker_path.is_absolute():
        raise RunnerBuildError("docker_tool:absolute_path_required")
    try:
        resolved = docker_path.resolve(strict=True)
    except OSError as exc:
        raise RunnerBuildError("docker_tool:unavailable") from exc
    try:
        mode = resolved.lstat().st_mode
    except OSError as exc:
        raise RunnerBuildError("docker_tool:unavailable") from exc
    if not stat.S_ISREG(mode):
        raise RunnerBuildError("docker_tool:regular_executable_required")
    content = _read_regular_once(resolved, "docker_tool.executable")
    return resolved, {
        "executable_basename": _portable_component(resolved.name, "docker_tool.basename"),
        "executable_sha256": _sha256_bytes(content),
        "resolved_path_sha256": _sha256_bytes(str(resolved).encode("utf-8")),
    }


def _private_external_directory(path: Path, *, root: Path, where: str) -> Path:
    return _resolved_external_directory(
        path, root=root, where=where, private=True
    )


def _controller(
    *,
    docker_path: Path,
    root: Path,
    private_root: Path,
    source_commit: str,
    docker_tool: Mapping[str, Any] | None = None,
) -> docker_lifecycle.DockerController:
    admission_tool: dict[str, Any] = {"resolved_path": str(docker_path)}
    if docker_tool is not None:
        admission_tool.update(
            {
                "executable_sha256": docker_tool["executable_sha256"],
                "daemon_identity_sha256": docker_tool["daemon_identity_sha256"],
                "version": docker_tool["version"],
            }
        )
    return docker_lifecycle.DockerController(
        admission={"docker_tool": admission_tool},
        root=root,
        private_root=private_root,
        source_commit=source_commit,
        attestation_id="QT-ATT-19700101T000000Z-000000000000-runner-build",
        profile_id="runner-build",
        environment_instance_id="qt-runner-build",
    )


def _inspect_base_image(
    controller: docker_lifecycle.DockerController,
    base: Mapping[str, Any],
) -> dict[str, str]:
    try:
        observed = controller._image_inspect(base["reference"])
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"base_image_unavailable:{base['argument']}:{exc}") from exc
    image_id = observed.get("Id")
    repo_digests = observed.get("RepoDigests") or []
    platform = f"{observed.get('Os')}/{observed.get('Architecture')}"
    if (
        not isinstance(image_id, str)
        or not IMAGE_ID_RE.fullmatch(image_id)
        or platform != TARGET_PLATFORM
        or not isinstance(repo_digests, list)
        or not any(
            isinstance(item, str) and item.endswith("@" + base["digest"])
            for item in repo_digests
        )
    ):
        raise RunnerBuildError(f"base_image_identity_mismatch:{base['argument']}")
    return {
        "argument": base["argument"],
        "stage": base["stage"],
        "reference": base["reference"],
        "digest": base["digest"],
        "image_id": image_id,
        "platform": platform,
    }


def _inspect_output_image(
    controller: docker_lifecycle.DockerController,
    token: str,
    expected_labels: Mapping[str, str],
) -> dict[str, Any]:
    try:
        observed = controller._image_inspect(token)
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"output_image_inspection_failed:{exc}") from exc
    image_id = observed.get("Id")
    platform = f"{observed.get('Os')}/{observed.get('Architecture')}"
    labels = (observed.get("Config") or {}).get("Labels") or {}
    if not isinstance(image_id, str) or not IMAGE_ID_RE.fullmatch(image_id):
        raise RunnerBuildError("output_image_id_invalid")
    if platform != TARGET_PLATFORM:
        raise RunnerBuildError("output_image_platform_mismatch")
    if not isinstance(labels, dict) or any(
        labels.get(key) != value for key, value in expected_labels.items()
    ):
        raise RunnerBuildError("output_image_required_labels_mismatch")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()):
        raise RunnerBuildError("output_image_labels_invalid")
    if any(
        any(character in key + value for character in "\x00\r\n")
        for key, value in labels.items()
    ):
        raise RunnerBuildError("output_image_labels_contain_control_character")
    return {"image_id": image_id, "platform": platform, "labels": dict(sorted(labels.items()))}


def _output_tag_absent(
    controller: docker_lifecycle.DockerController, output_tag: str
) -> None:
    try:
        result = controller._call(["image", "inspect", output_tag], check=False)
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"output_tag_preflight_failed:{exc}") from exc
    if type(result.exit_code) is int and result.exit_code == 0:
        raise RunnerBuildError("output_tag_preexisting")
    expected_stderr = (
        b"Error response from daemon: No such image: "
        + output_tag.encode("ascii")
        + b"\n"
    )
    if (
        result.timed_out is not False
        or type(result.exit_code) is not int
        or result.exit_code != 1
        or result.stdout != b"[]\n"
        or result.stderr != expected_stderr
    ):
        raise RunnerBuildError("output_tag_absence_ambiguous")


def _expected_build_labels(
    contract: Mapping[str, Any], materialization: Mapping[str, Any]
) -> dict[str, str]:
    labels = {
        SOURCE_LABEL: contract["source"]["commit"],
        SOURCE_TREE_LABEL: contract["source"]["tree"],
        BUILD_PROFILE_LABEL: contract["source_materials"]["build_profile"]["sha256"],
        BUILD_DEFINITION_LABEL: contract["source_materials"]["dockerfile"]["sha256"],
        WHEEL_MANIFEST_LABEL: contract["source_materials"]["wheel_manifest"]["sha256"],
        WHEEL_ARTIFACT_LABEL: materialization["wheel_artifacts"]["tar"]["sha256"],
        BUILD_CONTEXT_LABEL: materialization["build_context"]["tar"]["sha256"],
    }
    if tuple(labels) != REQUIRED_IMAGE_LABELS:
        raise RunnerBuildError("build_labels_internal_order_mismatch")
    return labels


def _logical_build_argv(
    *,
    executable_basename: str,
    profile: Mapping[str, Any],
    labels: Mapping[str, str],
    output_tag: str,
) -> list[str]:
    argv = [
        executable_basename,
        "build",
        f"--platform={TARGET_PLATFORM}",
        "--network=none",
        "--pull=false",
        "--no-cache",
        "--progress=plain",
    ]
    for base in profile["base_images"]:
        argv.extend(["--build-arg", f"{base['argument']}={base['reference']}"])
    for key in REQUIRED_IMAGE_LABELS:
        argv.extend(["--label", f"{key}={labels[key]}"])
    argv.extend(["--tag", output_tag, "--file", DOCKERFILE_PATH, "-"])
    return argv


def _argv_sha256(argv: Sequence[str]) -> str:
    return _sha256_bytes(_canonical_json_bytes(list(argv)))


def _build_log_bytes(result: BuildCommandResult) -> bytes:
    return (
        b"QT assurance runner Docker build stdout\n"
        + result.stdout
        + (b"" if result.stdout.endswith(b"\n") or not result.stdout else b"\n")
        + b"QT assurance runner Docker build stderr\n"
        + result.stderr
        + (b"" if result.stderr.endswith(b"\n") or not result.stderr else b"\n")
        + f"exit_code={result.exit_code}\ntimed_out={str(result.timed_out).lower()}\n".encode(
            "ascii"
        )
    )


def _recheck_materialization_snapshots(
    materialized_directory: _AnchoredDirectory, snapshots: Mapping[str, bytes]
) -> None:
    for key, basename in (
        ("receipt", MATERIALIZATION_RECEIPT_NAME),
        ("wheelhouse_tar", WHEELHOUSE_TAR_NAME),
        ("context_tar", CONTEXT_TAR_NAME),
    ):
        observed = _read_regular_at_once(
            materialized_directory, basename, f"materialization.recheck.{key}"
        )
        if observed != snapshots[key]:
            raise RunnerBuildError(f"materialization_changed_during_build:{key}")


def _build_runner_image_from_anchor(
    *,
    root: Path,
    source_commit: str,
    materialized_directory: _AnchoredDirectory,
    docker_path: Path,
    private_root: Path,
    output_tag: str,
    build_record_path: Path | None = None,
    command_runner: BuildCommandRunner | None = None,
) -> Path:
    """Build once from the closed context and write an immutable build record."""

    root = root.resolve()
    if not SAFE_TAG_RE.fullmatch(output_tag) or "@" in output_tag:
        raise RunnerBuildError("output_tag_invalid")
    require_exact_clean_source(root, source_commit)
    contract = _source_contract(root, source_commit)
    materialized_root = materialized_directory.path
    materialization, materialization_snapshots = _load_materialization_from_anchor(
        materialized_directory, root=root, source_commit=source_commit
    )
    record_path = (
        materialized_root / DEFAULT_BUILD_RECORD_NAME
        if build_record_path is None
        else build_record_path.absolute()
    )
    if record_path.parent != materialized_root or record_path.name != DEFAULT_BUILD_RECORD_NAME:
        raise RunnerBuildError("build_record_must_use_materialization_bundle_location")
    log_path = materialized_root / DEFAULT_BUILD_LOG_NAME
    for path, where in ((record_path, "build_record"), (log_path, "build_log")):
        if path.exists() or path.is_symlink():
            raise RunnerBuildError(f"{where}:create_only_target_exists")
    private_root = _private_external_directory(
        private_root, root=root, where="docker_private_root"
    )
    resolved_docker, docker_identity = _resolved_docker_identity(docker_path)
    controller = _controller(
        docker_path=resolved_docker,
        root=root,
        private_root=private_root,
        source_commit=source_commit,
    )
    try:
        _, daemon_identity, version = controller.control_plane()
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"docker_control_plane_unavailable:{exc}") from exc
    docker_tool = {
        **docker_identity,
        "version": version,
        "daemon_identity_sha256": daemon_identity,
    }
    base_images = [
        _inspect_base_image(controller, base) for base in contract["profile"]["base_images"]
    ]
    _output_tag_absent(controller, output_tag)
    labels = _expected_build_labels(contract, materialization)
    logical_argv = _logical_build_argv(
        executable_basename=docker_tool["executable_basename"],
        profile=contract["profile"],
        labels=labels,
        output_tag=output_tag,
    )
    executable_argv = [str(resolved_docker), *logical_argv[1:]]
    started = _utc_now()
    build_id = (
        "QT-RUNNER-BUILD-"
        + started.replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + secrets.token_hex(8)
    )

    # The tag and all bases were checked under this daemon.  Rebind the daemon
    # and base identities at the last possible point before the mutation.
    try:
        _, immediate_daemon, immediate_version = controller.control_plane()
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"docker_control_plane_prebuild_failed:{exc}") from exc
    if immediate_daemon != daemon_identity or immediate_version != version:
        raise RunnerBuildError("docker_control_plane_changed_before_build")
    if [
        _inspect_base_image(controller, base) for base in contract["profile"]["base_images"]
    ] != base_images:
        raise RunnerBuildError("base_image_changed_before_build")
    _output_tag_absent(controller, output_tag)
    _, immediate_docker_identity = _resolved_docker_identity(resolved_docker)
    if immediate_docker_identity != docker_identity:
        raise RunnerBuildError("docker_executable_changed_before_build")
    require_exact_clean_source(root, source_commit)
    runner = command_runner or _default_build_command_runner
    result = runner(
        executable_argv,
        controller.process_env(),
        BUILD_TIMEOUT_SECONDS,
        materialization_snapshots["context_tar"],
    )
    if not isinstance(result, BuildCommandResult):
        raise RunnerBuildError("docker_build_runner_result_invalid")
    require_exact_clean_source(root, source_commit)
    try:
        _, final_daemon, final_version = controller.control_plane()
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"docker_control_plane_postbuild_failed:{exc}") from exc
    if final_daemon != daemon_identity or final_version != version:
        raise RunnerBuildError("docker_control_plane_changed_during_build")
    _, final_docker_identity = _resolved_docker_identity(resolved_docker)
    if final_docker_identity != docker_identity:
        raise RunnerBuildError("docker_executable_changed_during_build")
    if [
        _inspect_base_image(controller, base) for base in contract["profile"]["base_images"]
    ] != base_images:
        raise RunnerBuildError("base_image_changed_during_build")

    if result.exit_code == 0 and not result.timed_out:
        output_image: dict[str, Any] | None = _inspect_output_image(
            controller, output_tag, labels
        )
        status_value = "succeeded"
    else:
        # A failed command is recordable only when it did not leave a tag that
        # could later be mistaken for the reviewed output.
        _output_tag_absent(controller, output_tag)
        output_image = None
        status_value = "failed"
    _recheck_materialization_snapshots(materialized_directory, materialization_snapshots)
    try:
        _, acceptance_daemon, acceptance_version = controller.control_plane()
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(
            f"docker_control_plane_preacceptance_failed:{exc}"
        ) from exc
    if acceptance_daemon != daemon_identity or acceptance_version != version:
        raise RunnerBuildError("docker_control_plane_changed_before_image_acceptance")
    _, acceptance_docker_identity = _resolved_docker_identity(resolved_docker)
    if acceptance_docker_identity != docker_identity:
        raise RunnerBuildError("docker_executable_changed_before_image_acceptance")
    if [
        _inspect_base_image(controller, base) for base in contract["profile"]["base_images"]
    ] != base_images:
        raise RunnerBuildError("base_image_changed_before_image_acceptance")
    if output_image is not None:
        final_output = _inspect_output_image(controller, output_tag, labels)
        if final_output != output_image:
            raise RunnerBuildError("output_image_changed_before_record")
    else:
        _output_tag_absent(controller, output_tag)
    finished = _utc_now()
    log_bytes = _build_log_bytes(result)
    invocation = {
        "argv": logical_argv,
        "argv_sha256": _argv_sha256(logical_argv),
        "context_stdin_sha256": materialization["build_context"]["tar"]["sha256"],
        "output_tag": output_tag,
        "timeout_seconds": BUILD_TIMEOUT_SECONDS,
    }
    record = {
        "schema_version": BUILD_RECORD_SCHEMA_VERSION,
        "build_id": build_id,
        "status": status_value,
        "source": contract["source"],
        "source_materials": contract["source_materials"],
        "wheel_artifacts": {
            "entries": materialization["wheel_artifacts"]["entries"],
            "aggregate": materialization["wheel_artifacts"]["aggregate"],
            "tar": {
                "basename": WHEELHOUSE_TAR_NAME,
                "sha256": materialization["wheel_artifacts"]["tar"]["sha256"],
                "size": materialization["wheel_artifacts"]["tar"]["size"],
            },
        },
        "build_context": {
            "tar": {
                "basename": CONTEXT_TAR_NAME,
                "sha256": materialization["build_context"]["tar"]["sha256"],
                "size": materialization["build_context"]["tar"]["size"],
            },
            "inventory": materialization["build_context"]["inventory"],
        },
        "docker_tool": docker_tool,
        "base_images": base_images,
        "invocation": invocation,
        "started_at": _timestamp(started),
        "finished_at": _timestamp(finished),
        "exit_code": result.exit_code,
        "log": {
            "basename": DEFAULT_BUILD_LOG_NAME,
            "sha256": _sha256_bytes(log_bytes),
            "size": len(log_bytes),
        },
        "output_image": output_image,
        "external_order_submission_enabled": False,
    }
    # A final source check separates environment evidence from a build that
    # raced with repository edits.  The log is written first and the canonical
    # record last so no record can point at a missing log.
    require_exact_clean_source(root, source_commit)
    _write_create_only_at(
        materialized_directory, DEFAULT_BUILD_LOG_NAME, log_bytes, "build_log"
    )
    _fsync_anchored_directory(materialized_directory, "build_log")
    _write_create_only_at(
        materialized_directory,
        DEFAULT_BUILD_RECORD_NAME,
        _canonical_json_bytes(record),
        "build_record",
    )
    _fsync_anchored_directory(materialized_directory, "build_record")
    require_exact_clean_source(root, source_commit)
    materialized_directory.assert_identity()
    return record_path


def build_runner_image(
    *,
    root: Path,
    source_commit: str,
    materialized_root: Path,
    docker_path: Path,
    private_root: Path,
    output_tag: str,
    build_record_path: Path | None = None,
    command_runner: BuildCommandRunner | None = None,
) -> Path:
    """Build once from an fd-anchored closed context and publish one record."""

    root = root.resolve()
    with _anchor_external_directory(
        materialized_root,
        root=root,
        where="materialized_build_root",
        private=True,
    ) as materialized_directory:
        return _build_runner_image_from_anchor(
            root=root,
            source_commit=source_commit,
            materialized_directory=materialized_directory,
            docker_path=docker_path,
            private_root=private_root,
            output_tag=output_tag,
            build_record_path=build_record_path,
            command_runner=command_runner,
        )


def _assert_no_absolute_host_paths(value: Any, where: str = "build_record") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_no_absolute_host_paths(item, f"{where}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_absolute_host_paths(item, f"{where}[{index}]")
    elif isinstance(value, str):
        if PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute():
            raise RunnerBuildError(f"{where}:absolute_host_path_forbidden")


def _expected_context_inventory(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    profile = contract["profile"]
    prefix = profile["wheelhouse"]["context_prefix"]
    hashed = _hashed_requirements_bytes(contract["manifest"]["entries"])
    rows = [
        {
            "path": DOCKERFILE_PATH,
            "sha256": contract["source_materials"]["dockerfile"]["sha256"],
            "size": len(contract["source_bytes"]["dockerfile"]),
        },
        {
            "path": f"{prefix}/{profile['wheelhouse']['hashed_requirements_name']}",
            "sha256": _sha256_bytes(hashed),
            "size": len(hashed),
        },
        {
            "path": f"{prefix}/{profile['wheelhouse']['manifest_name']}",
            "sha256": contract["source_materials"]["wheel_manifest"]["sha256"],
            "size": len(contract["source_bytes"]["wheel_manifest"]),
        },
    ]
    rows.extend(
        {
            "path": f"{prefix}/wheelhouse/{item['filename']}",
            "sha256": item["sha256"],
            "size": item["size"],
        }
        for item in contract["manifest"]["entries"]
    )
    return sorted(rows, key=lambda item: item["path"])


def _expected_wheelhouse_inventory(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    profile = contract["profile"]
    hashed = _hashed_requirements_bytes(contract["manifest"]["entries"])
    rows = [
        {
            "path": profile["wheelhouse"]["hashed_requirements_name"],
            "sha256": _sha256_bytes(hashed),
            "size": len(hashed),
        },
        {
            "path": profile["wheelhouse"]["manifest_name"],
            "sha256": contract["source_materials"]["wheel_manifest"]["sha256"],
            "size": len(contract["source_bytes"]["wheel_manifest"]),
        },
    ]
    rows.extend(
        {
            "path": f"wheelhouse/{item['filename']}",
            "sha256": item["sha256"],
            "size": item["size"],
        }
        for item in contract["manifest"]["entries"]
    )
    return sorted(rows, key=lambda item: item["path"])


def _validate_docker_tool(value: Any) -> dict[str, str]:
    where = "build_record.docker_tool"
    if not isinstance(value, dict):
        raise RunnerBuildError(f"{where}:object_required")
    _exact_keys(
        value,
        {
            "executable_basename",
            "executable_sha256",
            "resolved_path_sha256",
            "version",
            "daemon_identity_sha256",
        },
        where,
    )
    basename = _portable_component(
        _string(value["executable_basename"], f"{where}.executable_basename"),
        f"{where}.executable_basename",
    )
    version = _string(value["version"], f"{where}.version")
    if (
        not re.fullmatch(r"client=[^;\x00\r\n]+;server=[^;\x00\r\n]+", version)
        or "/" in version
        or "\\" in version
    ):
        raise RunnerBuildError(f"{where}.version:invalid")
    return {
        "executable_basename": basename,
        "executable_sha256": _hash(
            value["executable_sha256"], f"{where}.executable_sha256"
        ),
        "resolved_path_sha256": _hash(
            value["resolved_path_sha256"], f"{where}.resolved_path_sha256"
        ),
        "version": version,
        "daemon_identity_sha256": _hash(
            value["daemon_identity_sha256"], f"{where}.daemon_identity_sha256"
        ),
    }


def _validate_base_images(
    value: Any, profile: Mapping[str, Any]
) -> list[dict[str, str]]:
    if not isinstance(value, list) or len(value) != len(profile["base_images"]):
        raise RunnerBuildError("build_record.base_images:exact_pair_required")
    result: list[dict[str, str]] = []
    for index, (raw, expected) in enumerate(zip(value, profile["base_images"])):
        where = f"build_record.base_images[{index}]"
        if not isinstance(raw, dict):
            raise RunnerBuildError(f"{where}:object_required")
        _exact_keys(
            raw,
            {"argument", "stage", "reference", "digest", "image_id", "platform"},
            where,
        )
        if any(raw[key] != expected[key] for key in ("argument", "stage", "reference", "digest")):
            raise RunnerBuildError(f"{where}:profile_binding_mismatch")
        image_id = _string(raw["image_id"], f"{where}.image_id")
        if not IMAGE_ID_RE.fullmatch(image_id):
            raise RunnerBuildError(f"{where}.image_id:invalid")
        if raw["platform"] != TARGET_PLATFORM:
            raise RunnerBuildError(f"{where}.platform:mismatch")
        result.append(dict(raw))
    return result


def _validate_output_image(
    value: Any, expected_labels: Mapping[str, str], *, required: bool
) -> dict[str, Any] | None:
    where = "build_record.output_image"
    if value is None:
        if required:
            raise RunnerBuildError(f"{where}:required_for_success")
        return None
    if not isinstance(value, dict):
        raise RunnerBuildError(f"{where}:object_required")
    _exact_keys(value, {"image_id", "platform", "labels"}, where)
    image_id = _string(value["image_id"], f"{where}.image_id")
    if not IMAGE_ID_RE.fullmatch(image_id):
        raise RunnerBuildError(f"{where}.image_id:invalid")
    if value["platform"] != TARGET_PLATFORM:
        raise RunnerBuildError(f"{where}.platform:mismatch")
    labels = value["labels"]
    if (
        not isinstance(labels, dict)
        or any(not isinstance(key, str) or not isinstance(item, str) for key, item in labels.items())
        or any(
            any(character in key + item for character in "\x00\r\n")
            for key, item in labels.items()
        )
        or any(labels.get(key) != item for key, item in expected_labels.items())
    ):
        raise RunnerBuildError(f"{where}.labels:mismatch")
    return {"image_id": image_id, "platform": TARGET_PLATFORM, "labels": dict(labels)}


def _validate_build_record_payload(
    raw: Mapping[str, Any], *, contract: Mapping[str, Any], require_success: bool
) -> dict[str, Any]:
    _exact_keys(
        raw,
        {
            "schema_version",
            "build_id",
            "status",
            "source",
            "source_materials",
            "wheel_artifacts",
            "build_context",
            "docker_tool",
            "base_images",
            "invocation",
            "started_at",
            "finished_at",
            "exit_code",
            "log",
            "output_image",
            "external_order_submission_enabled",
        },
        "build_record",
    )
    if raw["schema_version"] != BUILD_RECORD_SCHEMA_VERSION:
        raise RunnerBuildError("build_record:schema_version_unsupported")
    build_id = _string(raw["build_id"], "build_record.build_id")
    if not BUILD_ID_RE.fullmatch(build_id):
        raise RunnerBuildError("build_record.build_id:invalid")
    status_value = raw["status"]
    if status_value not in {"succeeded", "failed"}:
        raise RunnerBuildError("build_record.status:invalid")
    if require_success and status_value != "succeeded":
        raise RunnerBuildError("build_record:successful_build_required")
    if raw["source"] != contract["source"]:
        raise RunnerBuildError("build_record:source_binding_mismatch")
    if raw["source_materials"] != contract["source_materials"]:
        raise RunnerBuildError("build_record:source_materials_mismatch")
    if raw["external_order_submission_enabled"] is not False:
        raise RunnerBuildError("build_record:external_order_submission_must_be_false")

    wheel_artifacts = raw["wheel_artifacts"]
    if not isinstance(wheel_artifacts, dict):
        raise RunnerBuildError("build_record.wheel_artifacts:object_required")
    _exact_keys(
        wheel_artifacts,
        {"entries", "aggregate", "tar"},
        "build_record.wheel_artifacts",
    )
    if (
        wheel_artifacts["entries"] != contract["manifest"]["entries"]
        or wheel_artifacts["aggregate"] != contract["manifest"]["aggregate"]
    ):
        raise RunnerBuildError("build_record.wheel_artifacts:manifest_binding_mismatch")
    wheel_tar = _validate_file_identity(
        wheel_artifacts["tar"],
        "build_record.wheel_artifacts.tar",
        allowed_keys={"basename", "sha256", "size"},
    )
    if wheel_tar["basename"] != WHEELHOUSE_TAR_NAME:
        raise RunnerBuildError("build_record.wheel_artifacts.tar:basename_mismatch")

    build_context = raw["build_context"]
    if not isinstance(build_context, dict):
        raise RunnerBuildError("build_record.build_context:object_required")
    _exact_keys(build_context, {"tar", "inventory"}, "build_record.build_context")
    context_tar = _validate_file_identity(
        build_context["tar"],
        "build_record.build_context.tar",
        allowed_keys={"basename", "sha256", "size"},
    )
    if context_tar["basename"] != CONTEXT_TAR_NAME:
        raise RunnerBuildError("build_record.build_context.tar:basename_mismatch")
    context_inventory = _validate_inventory(
        build_context["inventory"], "build_record.build_context.inventory"
    )
    if context_inventory != _expected_context_inventory(contract):
        raise RunnerBuildError("build_record.build_context:inventory_mismatch")

    docker_tool = _validate_docker_tool(raw["docker_tool"])
    base_images = _validate_base_images(raw["base_images"], contract["profile"])
    expected_labels = {
        SOURCE_LABEL: contract["source"]["commit"],
        SOURCE_TREE_LABEL: contract["source"]["tree"],
        BUILD_PROFILE_LABEL: contract["source_materials"]["build_profile"]["sha256"],
        BUILD_DEFINITION_LABEL: contract["source_materials"]["dockerfile"]["sha256"],
        WHEEL_MANIFEST_LABEL: contract["source_materials"]["wheel_manifest"]["sha256"],
        WHEEL_ARTIFACT_LABEL: wheel_tar["sha256"],
        BUILD_CONTEXT_LABEL: context_tar["sha256"],
    }

    invocation = raw["invocation"]
    if not isinstance(invocation, dict):
        raise RunnerBuildError("build_record.invocation:object_required")
    _exact_keys(
        invocation,
        {
            "argv",
            "argv_sha256",
            "context_stdin_sha256",
            "output_tag",
            "timeout_seconds",
        },
        "build_record.invocation",
    )
    output_tag = _string(invocation["output_tag"], "build_record.invocation.output_tag")
    if not SAFE_TAG_RE.fullmatch(output_tag) or "@" in output_tag:
        raise RunnerBuildError("build_record.invocation.output_tag:invalid")
    expected_argv = _logical_build_argv(
        executable_basename=docker_tool["executable_basename"],
        profile=contract["profile"],
        labels=expected_labels,
        output_tag=output_tag,
    )
    if invocation["argv"] != expected_argv:
        raise RunnerBuildError("build_record.invocation:argv_mismatch")
    if invocation["argv_sha256"] != _argv_sha256(expected_argv):
        raise RunnerBuildError("build_record.invocation:argv_hash_mismatch")
    if invocation["context_stdin_sha256"] != context_tar["sha256"]:
        raise RunnerBuildError("build_record.invocation:context_hash_mismatch")
    if invocation["timeout_seconds"] != BUILD_TIMEOUT_SECONDS:
        raise RunnerBuildError("build_record.invocation:timeout_mismatch")

    started = _parse_timestamp(raw["started_at"], "build_record.started_at")
    finished = _parse_timestamp(raw["finished_at"], "build_record.finished_at")
    if finished < started:
        raise RunnerBuildError("build_record:time_order_invalid")
    expected_build_prefix = "QT-RUNNER-BUILD-" + started.strftime("%Y%m%dT%H%M%SZ") + "-"
    if not build_id.startswith(expected_build_prefix):
        raise RunnerBuildError("build_record.build_id:timestamp_mismatch")
    exit_code = raw["exit_code"]
    if not isinstance(exit_code, int) or isinstance(exit_code, bool) or not -255 <= exit_code <= 255:
        raise RunnerBuildError("build_record.exit_code:invalid")
    if (status_value == "succeeded") != (exit_code == 0):
        raise RunnerBuildError("build_record:status_exit_code_mismatch")
    log = _validate_file_identity(
        raw["log"], "build_record.log", allowed_keys={"basename", "sha256", "size"}
    )
    if log["basename"] != DEFAULT_BUILD_LOG_NAME:
        raise RunnerBuildError("build_record.log:basename_mismatch")
    output = _validate_output_image(
        raw["output_image"], expected_labels, required=status_value == "succeeded"
    )
    if status_value == "failed" and output is not None:
        raise RunnerBuildError("build_record.output_image:forbidden_for_failure")
    normalized = {
        "schema_version": BUILD_RECORD_SCHEMA_VERSION,
        "build_id": build_id,
        "status": status_value,
        "source": dict(raw["source"]),
        "source_materials": json.loads(json.dumps(raw["source_materials"])),
        "wheel_artifacts": {
            "entries": json.loads(json.dumps(wheel_artifacts["entries"])),
            "aggregate": dict(wheel_artifacts["aggregate"]),
            "tar": wheel_tar,
        },
        "build_context": {"tar": context_tar, "inventory": context_inventory},
        "docker_tool": docker_tool,
        "base_images": base_images,
        "invocation": {
            "argv": list(invocation["argv"]),
            "argv_sha256": invocation["argv_sha256"],
            "context_stdin_sha256": invocation["context_stdin_sha256"],
            "output_tag": output_tag,
            "timeout_seconds": BUILD_TIMEOUT_SECONDS,
        },
        "started_at": raw["started_at"],
        "finished_at": raw["finished_at"],
        "exit_code": exit_code,
        "log": log,
        "output_image": output,
        "external_order_submission_enabled": False,
    }
    _assert_no_absolute_host_paths(normalized)
    return normalized


def _verify_external_build_bundle(
    *,
    record: Mapping[str, Any],
    bundle_directory: _AnchoredDirectory,
    contract: Mapping[str, Any],
) -> None:
    bundle_directory.assert_identity()
    try:
        observed_names = sorted(os.listdir(bundle_directory.descriptor))
    except OSError as exc:
        raise RunnerBuildError("build_record.bundle_inventory_unavailable") from exc
    expected_names = sorted(
        [
            WHEELHOUSE_TAR_NAME,
            CONTEXT_TAR_NAME,
            MATERIALIZATION_RECEIPT_NAME,
            DEFAULT_BUILD_LOG_NAME,
            DEFAULT_BUILD_RECORD_NAME,
        ]
    )
    if observed_names != expected_names:
        raise RunnerBuildError("build_record.bundle_closed_inventory_mismatch")

    receipt_bytes = _read_regular_at_once(
        bundle_directory,
        MATERIALIZATION_RECEIPT_NAME,
        "build_record.materialization_receipt",
    )
    receipt_raw = _strict_json_bytes(receipt_bytes, "build_record.materialization_receipt")
    if _canonical_json_bytes(receipt_raw) != receipt_bytes:
        raise RunnerBuildError("build_record.materialization_receipt:not_canonical_json")
    receipt = _validate_materialization_payload(receipt_raw, contract=contract)
    if (
        receipt["wheel_artifacts"]["entries"] != record["wheel_artifacts"]["entries"]
        or receipt["wheel_artifacts"]["aggregate"]
        != record["wheel_artifacts"]["aggregate"]
        or {
            "basename": receipt["wheel_artifacts"]["tar"]["name"],
            "sha256": receipt["wheel_artifacts"]["tar"]["sha256"],
            "size": receipt["wheel_artifacts"]["tar"]["size"],
        }
        != record["wheel_artifacts"]["tar"]
        or {
            "basename": receipt["build_context"]["tar"]["name"],
            "sha256": receipt["build_context"]["tar"]["sha256"],
            "size": receipt["build_context"]["tar"]["size"],
        }
        != record["build_context"]["tar"]
        or receipt["build_context"]["inventory"] != record["build_context"]["inventory"]
    ):
        raise RunnerBuildError("build_record:materialization_receipt_mismatch")

    wheel_tar = _read_regular_at_once(
        bundle_directory,
        record["wheel_artifacts"]["tar"]["basename"],
        "build_record.wheel_artifacts.tar",
    )
    context_tar = _read_regular_at_once(
        bundle_directory,
        record["build_context"]["tar"]["basename"],
        "build_record.build_context.tar",
    )
    log_bytes = _read_regular_at_once(
        bundle_directory, record["log"]["basename"], "build_record.log"
    )
    for content, identity, where in (
        (wheel_tar, record["wheel_artifacts"]["tar"], "wheel_artifacts.tar"),
        (context_tar, record["build_context"]["tar"], "build_context.tar"),
        (log_bytes, record["log"], "log"),
    ):
        if len(content) != identity["size"] or _sha256_bytes(content) != identity["sha256"]:
            raise RunnerBuildError(f"build_record.{where}:external_identity_mismatch")

    wheelhouse_entries = _validate_tar_snapshot(
        wheel_tar,
        _expected_wheelhouse_inventory(contract),
        "build_record.wheel_artifacts.tar",
    )
    context_entries = _validate_tar_snapshot(
        context_tar,
        record["build_context"]["inventory"],
        "build_record.build_context.tar",
    )
    profile = contract["profile"]
    hashed_path = profile["wheelhouse"]["hashed_requirements_name"]
    manifest_path = profile["wheelhouse"]["manifest_name"]
    if wheelhouse_entries[hashed_path] != _hashed_requirements_bytes(
        contract["manifest"]["entries"]
    ):
        raise RunnerBuildError("build_record.wheel_artifacts:hashed_requirements_mismatch")
    if wheelhouse_entries[manifest_path] != contract["source_bytes"]["wheel_manifest"]:
        raise RunnerBuildError("build_record.wheel_artifacts:manifest_bytes_mismatch")
    for entry in contract["manifest"]["entries"]:
        validate_wheel_snapshot(
            wheelhouse_entries[f"wheelhouse/{entry['filename']}"], entry
        )
    prefix = profile["wheelhouse"]["context_prefix"]
    if context_entries[DOCKERFILE_PATH] != contract["source_bytes"]["dockerfile"]:
        raise RunnerBuildError("build_record.build_context:dockerfile_bytes_mismatch")
    for path, content in wheelhouse_entries.items():
        if context_entries[f"{prefix}/{path}"] != content:
            raise RunnerBuildError(f"build_record.build_context:wheelhouse_mismatch:{path}")
    bundle_directory.assert_identity()


def load_build_record(
    path: Path,
    *,
    root: Path,
    source_commit: str,
    require_success: bool = True,
    verify_external: bool = True,
) -> tuple[dict[str, Any], bytes]:
    """Load a canonical source-bound record without trusting caller definitions."""

    root = root.resolve()
    if not path.is_absolute():
        raise RunnerBuildError("build_record.path:absolute_required")
    lexical = path.absolute()
    if path.name != DEFAULT_BUILD_RECORD_NAME:
        raise RunnerBuildError("build_record.path:fixed_basename_required")
    with _anchor_external_directory(
        lexical.parent,
        root=root,
        where="build_record.bundle_root",
        private=True,
    ) as bundle_directory:
        path = bundle_directory.path / DEFAULT_BUILD_RECORD_NAME
        if lexical != path:
            raise RunnerBuildError("build_record.path:symlinked_or_noncanonical")
        content = _read_regular_at_once(
            bundle_directory, DEFAULT_BUILD_RECORD_NAME, "build_record"
        )
        raw = _strict_json_bytes(content, "build_record")
        if _canonical_json_bytes(raw) != content:
            raise RunnerBuildError("build_record:not_canonical_json")
        contract = _source_contract(root, source_commit)
        record = _validate_build_record_payload(
            raw, contract=contract, require_success=require_success
        )
        if record != raw:
            raise RunnerBuildError("build_record:normalization_mismatch")
        if verify_external:
            _verify_external_build_bundle(
                record=record,
                bundle_directory=bundle_directory,
                contract=contract,
            )
        bundle_directory.assert_identity()
        return record, content


def validate_live_build_record(
    record: Mapping[str, Any],
    *,
    docker_path: Path,
    root: Path,
    private_root: Path,
) -> dict[str, Any]:
    """Read-only verification of the recorded daemon, bases, and output image."""

    root = root.resolve()
    source = record.get("source")
    if not isinstance(source, dict):
        raise RunnerBuildError("live_build_record:source_missing")
    source_commit = source.get("commit")
    if not isinstance(source_commit, str):
        raise RunnerBuildError("live_build_record:source_commit_missing")
    contract = _source_contract(root, source_commit)
    normalized = _validate_build_record_payload(
        record, contract=contract, require_success=True
    )
    if normalized != record:
        raise RunnerBuildError("live_build_record:normalization_mismatch")
    private_root = _private_external_directory(
        private_root, root=root, where="live_docker_private_root"
    )
    resolved_docker, observed_path_identity = _resolved_docker_identity(docker_path)
    docker_tool = normalized["docker_tool"]
    if any(
        observed_path_identity[key] != docker_tool[key]
        for key in (
            "executable_basename",
            "executable_sha256",
            "resolved_path_sha256",
        )
    ):
        raise RunnerBuildError("live_build_record:docker_executable_identity_mismatch")
    controller = _controller(
        docker_path=resolved_docker,
        root=root,
        private_root=private_root,
        source_commit=source_commit,
        docker_tool=docker_tool,
    )
    try:
        controller.verify_admission()
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(f"live_build_record:docker_control_plane_mismatch:{exc}") from exc
    observed_bases = [
        _inspect_base_image(controller, base) for base in contract["profile"]["base_images"]
    ]
    if observed_bases != normalized["base_images"]:
        raise RunnerBuildError("live_build_record:base_images_changed")
    output = normalized["output_image"]
    if not isinstance(output, dict):
        raise RunnerBuildError("live_build_record:successful_output_required")
    observed_output = _inspect_output_image(
        controller, output["image_id"], output["labels"]
    )
    if observed_output != output:
        raise RunnerBuildError("live_build_record:output_image_changed")
    observed_tag = _inspect_output_image(
        controller, normalized["invocation"]["output_tag"], output["labels"]
    )
    if observed_tag != output:
        raise RunnerBuildError("live_build_record:output_tag_changed")
    try:
        controller.verify_admission()
    except docker_lifecycle.DockerLifecycleError as exc:
        raise RunnerBuildError(
            f"live_build_record:docker_control_plane_changed_during_validation:{exc}"
        ) from exc
    return {
        "docker_tool": dict(docker_tool),
        "base_images": json.loads(json.dumps(observed_bases)),
        "output_image": json.loads(json.dumps(observed_output)),
    }


def archivable_build_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Return the already path-free canonical record for immutable archival."""

    archived = json.loads(json.dumps(record))
    _assert_no_absolute_host_paths(archived)
    if _canonical_json_bytes(archived) != _canonical_json_bytes(record):
        raise RunnerBuildError("build_record:archive_copy_mismatch")
    return archived


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize_parser = subparsers.add_parser(
        "materialize", help="create the source-bound closed offline build inputs"
    )
    materialize_parser.add_argument("--source-commit", required=True)
    materialize_parser.add_argument(
        "--cache-root", type=Path, action="append", required=True
    )
    materialize_parser.add_argument("--output-root", type=Path, required=True)

    build_parser = subparsers.add_parser(
        "build", help="perform one no-pull/no-network/no-cache Docker build"
    )
    build_parser.add_argument("--source-commit", required=True)
    build_parser.add_argument("--materialized-root", type=Path, required=True)
    build_parser.add_argument("--docker-path", type=Path, required=True)
    build_parser.add_argument("--private-root", type=Path, required=True)
    build_parser.add_argument("--output-tag", required=True)

    validate_parser = subparsers.add_parser(
        "validate-record", help="validate a build record and optionally its live images"
    )
    validate_parser.add_argument("--source-commit", required=True)
    validate_parser.add_argument("--record", type=Path, required=True)
    validate_parser.add_argument("--docker-path", type=Path)
    validate_parser.add_argument("--private-root", type=Path)

    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.command == "materialize":
            result = materialize_runner(
                root=root,
                source_commit=args.source_commit,
                cache_roots=args.cache_root,
                output_root=args.output_root,
            )
            output = {
                "status": "materialized",
                "output_root": str(result.output_root),
                "receipt": str(result.receipt),
            }
        elif args.command == "build":
            record_path = build_runner_image(
                root=root,
                source_commit=args.source_commit,
                materialized_root=args.materialized_root,
                docker_path=args.docker_path,
                private_root=args.private_root,
                output_tag=args.output_tag,
            )
            record, content = load_build_record(
                record_path,
                root=root,
                source_commit=args.source_commit,
                require_success=False,
                verify_external=True,
            )
            output = {
                "status": record["status"],
                "record": str(record_path),
                "record_sha256": _sha256_bytes(content),
            }
        else:
            if (args.docker_path is None) != (args.private_root is None):
                raise RunnerBuildError(
                    "validate_record:docker_path_and_private_root_must_be_paired"
                )
            record, content = load_build_record(
                args.record,
                root=root,
                source_commit=args.source_commit,
                require_success=True,
                verify_external=True,
            )
            live = None
            if args.docker_path is not None:
                live = validate_live_build_record(
                    record,
                    docker_path=args.docker_path,
                    root=root,
                    private_root=args.private_root,
                )
            output = {
                "status": "validated",
                "record_sha256": _sha256_bytes(content),
                "live_verified": live is not None,
            }
    except RunnerBuildError as exc:
        print(f"runner_build_error:{exc}", file=sys.stderr)
        return 2
    print(_canonical_json_bytes(output).decode("utf-8"), end="")
    if args.command == "build" and output["status"] != "succeeded":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())

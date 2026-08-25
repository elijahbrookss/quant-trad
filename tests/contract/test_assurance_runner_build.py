from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import os
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import warnings
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from scripts.assurance import build_runner
from scripts.assurance import docker_lifecycle


SOURCE_COMMIT = "a" * 40
SOURCE_TREE = "b" * 40
OUTPUT_IMAGE_ID = "sha256:" + "9" * 64


def test_cli_help_is_directly_executable() -> None:
    result = subprocess.run(
        [sys.executable, str(Path(build_runner.__file__).resolve()), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "materialize" in result.stdout
    assert "validate-record" in result.stdout


def _zip_info(name: str, *, kind: int = stat.S_IFREG) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, (2020, 1, 1, 0, 0, 0))
    info.create_system = 3
    info.external_attr = (kind | 0o644) << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    return info


def _wheel_bytes(
    *,
    files_override: Mapping[str, bytes] | None = None,
    extra: list[tuple[zipfile.ZipInfo, bytes]] | None = None,
    include_record: bool = True,
) -> bytes:
    dist = "demo-1.0.dist-info"
    files: dict[str, bytes] = {
        "demo/__init__.py": b"VALUE = 1\n",
        f"{dist}/METADATA": (
            b"Metadata-Version: 2.1\n"
            b"Name: demo\n"
            b"Version: 1.0\n"
            b"Requires-Python: >=3.9\n\n"
        ),
        f"{dist}/WHEEL": (
            b"Wheel-Version: 1.0\n"
            b"Generator: qt-contract-test\n"
            b"Root-Is-Purelib: true\n"
            b"Tag: py3-none-any\n\n"
        ),
    }
    if files_override:
        files.update(files_override)
    record_path = f"{dist}/RECORD"
    if include_record:
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        for name in sorted(files):
            payload = files[name]
            writer.writerow(
                [name, build_runner._record_digest(payload), str(len(payload))]
            )
        writer.writerow([record_path, "", ""])
        files[record_path] = output.getvalue().encode("utf-8")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in sorted(files):
            archive.writestr(_zip_info(name), files[name])
        for info, payload in extra or []:
            archive.writestr(info, payload)
    return buffer.getvalue()


def _entry(wheel: bytes) -> dict[str, Any]:
    return {
        "name": "demo",
        "version": "1.0",
        "filename": "demo-1.0-py3-none-any.whl",
        "sha256": hashlib.sha256(wheel).hexdigest(),
        "size": len(wheel),
        "selected_tag": "py3-none-any",
        "wheel_tags": ["py3-none-any"],
        "requires_python": ">=3.9",
    }


def _manifest_bytes(wheel: bytes) -> bytes:
    entry = _entry(wheel)
    payload = {
        "schema_version": build_runner.WHEEL_MANIFEST_SCHEMA_VERSION,
        "target": {
            "implementation": "cp",
            "python_version": "3.12",
            "abi": "cp312",
            "platform": "linux/amd64",
            "glibc_max": "2.36",
        },
        "requirements_lock_path": build_runner.REQUIREMENTS_LOCK_PATH,
        "entries": [entry],
        "aggregate": {
            "entry_count": 1,
            "selected_bytes": len(wheel),
            "entry_manifest_sha256": build_runner._entry_manifest_sha256([entry]),
        },
    }
    return build_runner._canonical_json_bytes(payload)


def _contract(wheel: bytes) -> dict[str, Any]:
    root = Path(build_runner.ROOT)
    profile_bytes = (root / build_runner.BUILD_PROFILE_PATH).read_bytes()
    dockerfile_bytes = (root / build_runner.DOCKERFILE_PATH).read_bytes()
    requirements = b"demo==1.0\n"
    manifest_bytes = _manifest_bytes(wheel)
    profile = build_runner.validate_build_profile(profile_bytes)
    manifest = build_runner.validate_wheel_manifest(manifest_bytes, requirements)
    build_runner.validate_bound_dockerfile(dockerfile_bytes, profile)
    paths = {
        "build_profile": build_runner.BUILD_PROFILE_PATH,
        "dockerfile": build_runner.DOCKERFILE_PATH,
        "requirements_lock": build_runner.REQUIREMENTS_LOCK_PATH,
        "wheel_manifest": build_runner.WHEEL_MANIFEST_PATH,
        "materializer": build_runner.MATERIALIZER_PATH,
    }
    source_bytes = {
        "build_profile": profile_bytes,
        "dockerfile": dockerfile_bytes,
        "requirements_lock": requirements,
        "wheel_manifest": manifest_bytes,
        "materializer": b"source-bound-test-materializer\n",
    }
    return {
        "source": {"commit": SOURCE_COMMIT, "tree": SOURCE_TREE},
        "source_bytes": source_bytes,
        "source_materials": {
            name: {
                "path": paths[name],
                "sha256": hashlib.sha256(source_bytes[name]).hexdigest(),
            }
            for name in sorted(paths)
        },
        "profile": profile,
        "manifest": manifest,
    }


def _bind_contract(
    monkeypatch: pytest.MonkeyPatch, contract: Mapping[str, Any]
) -> None:
    monkeypatch.setattr(
        build_runner,
        "_source_contract",
        lambda root, source_commit: contract
        if source_commit == SOURCE_COMMIT
        else (_ for _ in ()).throw(build_runner.RunnerBuildError("wrong source")),
    )
    monkeypatch.setattr(
        build_runner,
        "require_exact_clean_source",
        lambda root, source_commit: (source_commit, SOURCE_TREE),
    )


def _materialize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, Any], Path, Path, dict[str, Any]]:
    wheel = _wheel_bytes()
    contract = _contract(wheel)
    _bind_contract(monkeypatch, contract)
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    wheel_path = cache / contract["manifest"]["entries"][0]["filename"]
    wheel_path.write_bytes(wheel)
    output = tmp_path / "materialized"
    result = build_runner.materialize_runner(
        root=tmp_path / "source",
        source_commit=SOURCE_COMMIT,
        cache_roots=[cache],
        output_root=output,
    )
    receipt = json.loads(result.receipt.read_text(encoding="utf-8"))
    return contract, output, wheel_path, receipt


def test_frozen_profile_manifest_and_dockerfile_are_canonical() -> None:
    root = Path(build_runner.ROOT)
    profile = build_runner.validate_build_profile(
        (root / build_runner.BUILD_PROFILE_PATH).read_bytes()
    )
    manifest = build_runner.validate_wheel_manifest(
        (root / build_runner.WHEEL_MANIFEST_PATH).read_bytes(),
        (root / build_runner.REQUIREMENTS_LOCK_PATH).read_bytes(),
    )
    build_runner.validate_bound_dockerfile(
        (root / build_runner.DOCKERFILE_PATH).read_bytes(), profile
    )
    assert profile["installation"]["intentionally_unavailable_executables"] == [
        "psql"
    ]
    assert profile["external_order_submission_enabled"] is False
    assert profile["required_image_labels"] == list(build_runner.REQUIRED_IMAGE_LABELS)
    assert len(manifest["entries"]) == 91


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda value: value.update(id="alternate"), "id_mismatch"),
        (
            lambda value: value["docker"].update(network_mode="default"),
            "docker_boundary_mismatch",
        ),
        (
            lambda value: value["installation"].update(
                intentionally_unavailable_executables=[]
            ),
            "installation_mismatch",
        ),
        (
            lambda value: value["source_materials"].update(
                dockerfile="caller/Dockerfile"
            ),
            "source_materials_mismatch",
        ),
    ],
)
def test_build_profile_rejects_semantic_drift(mutation: Any, match: str) -> None:
    raw = json.loads(
        (Path(build_runner.ROOT) / build_runner.BUILD_PROFILE_PATH).read_text(
            encoding="utf-8"
        )
    )
    mutation(raw)
    with pytest.raises(build_runner.RunnerBuildError, match=match):
        build_runner.validate_build_profile(build_runner._canonical_json_bytes(raw))


def test_dockerfile_rejects_shell_form_and_psql() -> None:
    root = Path(build_runner.ROOT)
    profile = build_runner.validate_build_profile(
        (root / build_runner.BUILD_PROFILE_PATH).read_bytes()
    )
    valid = (root / build_runner.DOCKERFILE_PATH).read_bytes()
    with pytest.raises(build_runner.RunnerBuildError, match="exec_form_run_set"):
        build_runner.validate_bound_dockerfile(
            valid.replace(b'RUN ["node", "--version"]', b"RUN node --version"), profile
        )
    with pytest.raises(build_runner.RunnerBuildError, match="unapproved_executable"):
        build_runner.validate_bound_dockerfile(valid + b"# psql\n", profile)
    with pytest.raises(
        build_runner.RunnerBuildError, match="canonical_instruction_set_mismatch"
    ):
        build_runner.validate_bound_dockerfile(
            valid.replace(b"ENTRYPOINT []", b'ENTRYPOINT ["false"]'), profile
        )
    with pytest.raises(
        build_runner.RunnerBuildError, match="canonical_instruction_set_mismatch"
    ):
        build_runner.validate_bound_dockerfile(
            valid.replace(b"WORKDIR /workspace", b"ENV EXTRA=1\nWORKDIR /workspace"),
            profile,
        )


def test_manifest_requires_canonical_exact_requirements_closure() -> None:
    wheel = _wheel_bytes()
    content = _manifest_bytes(wheel)
    assert build_runner.validate_wheel_manifest(content, b"demo==1.0\n")["entries"]
    with pytest.raises(build_runner.RunnerBuildError, match="not_canonical_json"):
        build_runner.validate_wheel_manifest(
            json.dumps(json.loads(content), indent=2).encode("utf-8"), b"demo==1.0\n"
        )
    with pytest.raises(build_runner.RunnerBuildError, match="closure_mismatch"):
        build_runner.validate_wheel_manifest(content, b"other==1.0\n")


def test_valid_wheel_checks_root_metadata_wheel_and_record() -> None:
    wheel = _wheel_bytes()
    build_runner.validate_wheel_snapshot(wheel, _entry(wheel))


def test_wheel_accepts_one_canonical_explicit_directory_marker() -> None:
    wheel = _wheel_bytes(extra=[(_zip_info("demo/", kind=stat.S_IFDIR), b"")])
    build_runner.validate_wheel_snapshot(wheel, _entry(wheel))


def test_wheel_rejects_nonempty_explicit_directory_member() -> None:
    wheel = _wheel_bytes(
        extra=[(_zip_info("demo/", kind=stat.S_IFDIR), b"hidden")]
    )
    with pytest.raises(build_runner.RunnerBuildError, match="directory_type_invalid"):
        build_runner.validate_wheel_snapshot(wheel, _entry(wheel))


@pytest.mark.parametrize(
    "tag",
    [
        "py3-cp311-any",
        "py312-cp312-any",
        "cp311-cp311-manylinux_2_17_x86_64",
        "cp313-abi3-manylinux_2_17_x86_64",
        "py3-none-any.linux_x86_64",
    ],
)
def test_tag_compatibility_couples_python_abi_and_platform(tag: str) -> None:
    assert build_runner._tag_is_compatible(tag) is False


def _with_zip_extra(
    info: zipfile.ZipInfo, payload: bytes, *, include_record: bool = True
) -> tuple[bytes, dict[str, Any]]:
    wheel = _wheel_bytes(extra=[(info, payload)], include_record=include_record)
    return wheel, _entry(wheel)


@pytest.mark.parametrize(
    ("wheel_factory", "match"),
    [
        (
            lambda: _with_zip_extra(_zip_info("../escape"), b"bad"),
            "nonportable_component|safe_relative_path",
        ),
        (
            lambda: _with_zip_extra(_zip_info("./alias.py"), b"bad"),
            "safe_relative_path",
        ),
        (
            lambda: _with_zip_extra(_zip_info("demo//alias.py"), b"bad"),
            "safe_relative_path",
        ),
        (
            lambda: _with_zip_extra(
                _zip_info("linked", kind=stat.S_IFLNK), b"demo/__init__.py"
            ),
            "symlink_or_special_member",
        ),
        (
            lambda: _with_zip_extra(_zip_info("DEMO/__init__.py"), b"duplicate"),
            "member_path_collision",
        ),
        (
            lambda: (
                (wheel := _wheel_bytes(include_record=False)),
                _entry(wheel),
            ),
            "root_metadata_wheel_or_record_missing",
        ),
    ],
)
def test_wheel_rejects_unsafe_or_incomplete_archives(
    wheel_factory: Any, match: str
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        wheel, entry = wheel_factory()
    with pytest.raises(build_runner.RunnerBuildError, match=match):
        build_runner.validate_wheel_snapshot(wheel, entry)


def test_wheel_rejects_duplicate_member_and_record_tamper() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        duplicate = _wheel_bytes(
            extra=[
                (
                    _zip_info("demo-1.0.dist-info/METADATA"),
                    b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0\n\n",
                )
            ]
        )
    with pytest.raises(build_runner.RunnerBuildError, match="member_path_collision"):
        build_runner.validate_wheel_snapshot(duplicate, _entry(duplicate))

    original = _wheel_bytes()
    with zipfile.ZipFile(io.BytesIO(original)) as archive:
        original_record = archive.read("demo-1.0.dist-info/RECORD")
    tampered = _wheel_bytes(
        files_override={
            "demo/__init__.py": b"VALUE = 2\n",
            "demo-1.0.dist-info/RECORD": original_record,
        },
        include_record=False,
    )
    with pytest.raises(build_runner.RunnerBuildError, match="record_integrity_mismatch"):
        build_runner.validate_wheel_snapshot(tampered, _entry(tampered))


def test_wheel_missing_metadata_is_a_fail_closed_runner_error() -> None:
    original = _wheel_bytes()
    buffer = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(original)) as source, zipfile.ZipFile(
        buffer, "w"
    ) as destination:
        for info in source.infolist():
            if info.filename.endswith("/METADATA"):
                continue
            destination.writestr(_zip_info(info.filename), source.read(info))
    wheel = buffer.getvalue()
    with pytest.raises(
        build_runner.RunnerBuildError, match="root_metadata_wheel_or_record_missing"
    ):
        build_runner.validate_wheel_snapshot(wheel, _entry(wheel))


def test_wheel_rejects_crc_and_encryption_flags() -> None:
    wheel = bytearray(_wheel_bytes())
    with zipfile.ZipFile(io.BytesIO(wheel)) as archive:
        info = archive.getinfo("demo/__init__.py")
        offset = info.header_offset
    name_length, extra_length = struct.unpack_from("<HH", wheel, offset + 26)
    payload_offset = offset + 30 + name_length + extra_length
    wheel[payload_offset + max(1, info.compress_size // 2)] ^= 0x01
    corrupt = bytes(wheel)
    with pytest.raises(build_runner.RunnerBuildError, match="crc"):
        build_runner.validate_wheel_snapshot(corrupt, _entry(corrupt))

    encrypted = bytearray(_wheel_bytes())
    local = encrypted.find(b"PK\x03\x04")
    central = encrypted.find(b"PK\x01\x02")
    struct.pack_into("<H", encrypted, local + 6, struct.unpack_from("<H", encrypted, local + 6)[0] | 1)
    struct.pack_into(
        "<H", encrypted, central + 8, struct.unpack_from("<H", encrypted, central + 8)[0] | 1
    )
    encrypted_bytes = bytes(encrypted)
    with pytest.raises(build_runner.RunnerBuildError, match="encrypted_member"):
        build_runner.validate_wheel_snapshot(encrypted_bytes, _entry(encrypted_bytes))


def test_cache_discovery_is_content_selected_and_reads_opaque_body_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel = _wheel_bytes()
    manifest = _contract(wheel)["manifest"]
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    target = cache / "opaque-http-cache-key.body"
    target.write_bytes(wheel)
    original = build_runner._read_beneath_once
    calls: list[str] = []

    def counted(root: Path, relative: str, where: str) -> bytes:
        calls.append(relative)
        return original(root, relative, where)

    monkeypatch.setattr(build_runner, "_read_beneath_once", counted)
    assert build_runner.discover_wheel_snapshots([cache], manifest) == {
        manifest["entries"][0]["filename"]: wheel
    }
    assert calls == [target.name]

    second = tmp_path / "cache-two"
    second.mkdir(mode=0o700)
    (second / "unrelated.body").write_bytes(b"x" * len(wheel))
    assert build_runner.discover_wheel_snapshots([cache, second], manifest) == {
        manifest["entries"][0]["filename"]: wheel
    }
    duplicate = tmp_path / "cache-three"
    duplicate.mkdir(mode=0o700)
    (duplicate / "another-opaque.body").write_bytes(wheel)
    with pytest.raises(
        build_runner.RunnerBuildError, match="duplicate_matching_candidate"
    ):
        build_runner.discover_wheel_snapshots([cache, duplicate], manifest)


def test_cache_discovery_rejects_symlink_candidate(tmp_path: Path) -> None:
    wheel = _wheel_bytes()
    manifest = _contract(wheel)["manifest"]
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    outside = tmp_path / "outside.whl"
    outside.write_bytes(wheel)
    (cache / "opaque.body").symlink_to(outside)
    with pytest.raises(build_runner.RunnerBuildError, match="candidate_not_real_regular"):
        build_runner.discover_wheel_snapshots([cache], manifest)


def test_materializer_rejects_cache_inside_source_before_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel = _wheel_bytes()
    contract = _contract(wheel)
    _bind_contract(monkeypatch, contract)
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    cache = source / ".ignored-cache"
    cache.mkdir(mode=0o700)
    (cache / "opaque.body").write_bytes(wheel)
    output = tmp_path / "must-not-exist"
    with pytest.raises(build_runner.RunnerBuildError, match="must_be_external_to_source"):
        build_runner.materialize_runner(
            root=source,
            source_commit=SOURCE_COMMIT,
            cache_roots=[cache],
            output_root=output,
        )
    assert not output.exists()
    with pytest.raises(build_runner.RunnerBuildError, match="must_be_external_to_source"):
        build_runner.materialize_runner(
            root=source,
            source_commit=SOURCE_COMMIT,
            cache_roots=[tmp_path],
            output_root=output,
        )
    assert not output.exists()


def test_materialization_is_deterministic_closed_and_path_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract, first, wheel_path, receipt = _materialize(tmp_path, monkeypatch)
    assert set(item.name for item in first.iterdir()) == {
        build_runner.WHEELHOUSE_TAR_NAME,
        build_runner.CONTEXT_TAR_NAME,
        build_runner.MATERIALIZATION_RECEIPT_NAME,
    }
    receipt_text = (first / build_runner.MATERIALIZATION_RECEIPT_NAME).read_text(
        encoding="utf-8"
    )
    assert str(wheel_path.parent) not in receipt_text
    assert not any(Path(value).is_absolute() for value in receipt_text.split('"'))
    loaded, snapshots = build_runner.load_materialization(
        first, root=tmp_path / "source", source_commit=SOURCE_COMMIT
    )
    assert loaded == receipt
    assert snapshots["context_tar"] == (first / build_runner.CONTEXT_TAR_NAME).read_bytes()

    second_cache = tmp_path / "unrelated-cache-location"
    second_cache.mkdir(mode=0o700)
    (second_cache / wheel_path.name).write_bytes(wheel_path.read_bytes())
    second = tmp_path / "materialized-two"
    build_runner.materialize_runner(
        root=tmp_path / "source",
        source_commit=SOURCE_COMMIT,
        cache_roots=[second_cache],
        output_root=second,
    )
    for name in (
        build_runner.WHEELHOUSE_TAR_NAME,
        build_runner.CONTEXT_TAR_NAME,
        build_runner.MATERIALIZATION_RECEIPT_NAME,
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    assert receipt["build_context"]["inventory"] == build_runner._expected_context_inventory(
        contract
    )


def test_deterministic_tar_rejects_links_extras_and_noncanonical_metadata() -> None:
    entries = {"safe/file.txt": b"value\n"}
    content = build_runner._deterministic_tar(entries)
    inventory = build_runner._inventory(entries)
    assert build_runner._validate_tar_snapshot(content, inventory, "test") == entries

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        info = tarfile.TarInfo("safe/file.txt")
        info.type = tarfile.SYMTYPE
        info.linkname = "../../escape"
        archive.addfile(info)
    with pytest.raises(build_runner.RunnerBuildError, match="noncanonical_member"):
        build_runner._validate_tar_snapshot(buffer.getvalue(), inventory, "test")

    with pytest.raises(build_runner.RunnerBuildError, match="closed_inventory"):
        build_runner._validate_tar_snapshot(
            build_runner._deterministic_tar({**entries, "extra": b"x"}), inventory, "test"
        )


def test_materialization_detects_tar_or_receipt_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, output, _, _ = _materialize(tmp_path, monkeypatch)
    context = output / build_runner.CONTEXT_TAR_NAME
    context.write_bytes(context.read_bytes() + b"tamper")
    with pytest.raises(build_runner.RunnerBuildError, match="identity_mismatch"):
        build_runner.load_materialization(
            output, root=tmp_path / "source", source_commit=SOURCE_COMMIT
        )


def test_materialization_rejects_symlinked_parent_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel = _wheel_bytes()
    contract = _contract(wheel)
    _bind_contract(monkeypatch, contract)
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    (cache / "opaque.body").write_bytes(wheel)
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    alias = tmp_path / "alias"
    alias.symlink_to(source, target_is_directory=True)
    target = alias / "must-not-exist"
    with pytest.raises(
        build_runner.RunnerBuildError, match="symlinked_or_noncanonical_parent"
    ):
        build_runner.materialize_runner(
            root=source,
            source_commit=SOURCE_COMMIT,
            cache_roots=[cache],
            output_root=target,
        )
    assert not (source / "must-not-exist").exists()


def test_anchored_bundle_write_rejects_path_replacement_without_redirect(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir(mode=0o700)
    bundle = tmp_path / "bundle"
    bundle.mkdir(mode=0o700)
    attacker = tmp_path / "attacker"
    attacker.mkdir(mode=0o700)
    moved = tmp_path / "moved"
    anchor = build_runner._anchor_external_directory(
        bundle, root=source, where="test_bundle", private=True
    )
    try:
        bundle.rename(moved)
        bundle.symlink_to(attacker, target_is_directory=True)
        with pytest.raises(build_runner.RunnerBuildError, match="directory_path_changed"):
            build_runner._write_create_only_at(anchor, "evidence", b"safe", "test")
    finally:
        anchor.close()
    assert not (attacker / "evidence").exists()
    assert not (moved / "evidence").exists()


def _fake_docker_control(
    *,
    contract: Mapping[str, Any],
    output_tag: str,
    labels: Mapping[str, str],
    daemon_ids: list[str] | None = None,
    preexisting: bool = False,
) -> tuple[dict[str, Any], docker_lifecycle.CommandRunner]:
    state: dict[str, Any] = {
        "built": preexisting,
        "info_calls": 0,
        "commands": [],
    }
    base_by_reference = {
        base["reference"]: {
            "Id": "sha256:" + str(index + 1) * 64,
            "RepoDigests": [base["reference"]],
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {"Labels": {}},
        }
        for index, base in enumerate(contract["profile"]["base_images"])
    }
    daemon_ids = daemon_ids or ["daemon-stable"]

    def runner(
        argv: list[str] | tuple[str, ...], env: Mapping[str, str], timeout: int
    ) -> docker_lifecycle.CommandResult:
        args = list(argv[1:])
        state["commands"].append(args)
        if args == ["version", "--format", "{{json .}}"]:
            payload = {
                "Client": {"Version": "27.0.0"},
                "Server": {"Version": "27.0.0", "ApiVersion": "1.46"},
            }
            return docker_lifecycle.CommandResult(
                json.dumps(payload).encode(), b"", 0
            )
        if args == ["context", "show"]:
            return docker_lifecycle.CommandResult(b"default\n", b"", 0)
        if args == ["info", "--format", "{{json .}}"]:
            index = min(state["info_calls"], len(daemon_ids) - 1)
            state["info_calls"] += 1
            payload = {
                "Architecture": "x86_64",
                "ID": daemon_ids[index],
                "DockerRootDir": "/var/lib/docker",
                "OSType": "linux",
            }
            return docker_lifecycle.CommandResult(
                json.dumps(payload).encode(), b"", 0
            )
        if len(args) == 3 and args[:2] == ["image", "inspect"]:
            token = args[2]
            if token in base_by_reference:
                payload = base_by_reference[token]
            elif state["built"] and token in {output_tag, OUTPUT_IMAGE_ID}:
                payload = {
                    "Id": OUTPUT_IMAGE_ID,
                    "RepoTags": [output_tag],
                    "Os": "linux",
                    "Architecture": "amd64",
                    "Config": {"Labels": dict(labels)},
                }
            else:
                return docker_lifecycle.CommandResult(
                    b"", f"Error: No such image: {token}\n".encode(), 1
                )
            return docker_lifecycle.CommandResult(
                json.dumps([payload]).encode(), b"", 0
            )
        raise AssertionError(f"unexpected Docker control command: {args}")

    return state, runner


def test_default_build_command_is_shell_free_and_streams_only_snapshot_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    class FakeProcess:
        pid = 12345
        returncode = 0

        def communicate(
            self, input: bytes | None = None, timeout: int | None = None
        ) -> tuple[bytes, bytes]:
            observed["input"] = input
            observed["timeout"] = timeout
            return b"stdout", b"stderr"

    def popen(argv: list[str], **kwargs: Any) -> FakeProcess:
        observed["argv"] = argv
        observed["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setattr(build_runner.subprocess, "Popen", popen)
    result = build_runner._default_build_command_runner(
        ["/approved/docker", "build", "--network=none", "-"],
        {"LANG": "C.UTF-8"},
        7200,
        b"verified-context",
    )
    assert result == build_runner.BuildCommandResult(b"stdout", b"stderr", 0)
    assert observed["argv"] == [
        "/approved/docker",
        "build",
        "--network=none",
        "-",
    ]
    assert observed["kwargs"]["shell"] is False
    assert observed["kwargs"]["stdin"] is build_runner.subprocess.PIPE
    assert observed["input"] == b"verified-context"
    assert observed["timeout"] == 7200


def test_git_source_reads_ignore_caller_git_redirection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: dict[str, Any] = {}

    def run(argv: list[str], **kwargs: Any) -> SimpleNamespace:
        observed["argv"] = argv
        observed["env"] = kwargs["env"]
        observed["shell"] = kwargs["shell"]
        return SimpleNamespace(returncode=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setenv("GIT_DIR", "/attacker/repository")
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", "/attacker/objects")
    monkeypatch.setattr(build_runner.subprocess, "run", run)
    assert build_runner._git(tmp_path, "rev-parse", "HEAD") == b"ok\n"
    assert observed["shell"] is False
    assert "GIT_DIR" not in observed["env"]
    assert "GIT_OBJECT_DIRECTORY" not in observed["env"]
    assert observed["env"]["GIT_NO_REPLACE_OBJECTS"] == "1"


def test_source_guard_rejects_execution_from_a_different_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "other-checkout"
    source.mkdir(mode=0o700)

    def fake_git(root: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
        assert args == ("rev-parse", "--show-toplevel")
        return (str(source) + "\n").encode("utf-8")

    monkeypatch.setattr(build_runner, "_git", fake_git)
    with pytest.raises(
        build_runner.RunnerBuildError,
        match="source_executing_assurance_code_mismatch",
    ):
        build_runner.require_exact_clean_source(source, SOURCE_COMMIT)


def _build_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    build_exit: int = 0,
    leave_partial_tag: bool = False,
    preexisting: bool = False,
    daemon_ids: list[str] | None = None,
    mutate_context_during_build: bool = False,
) -> tuple[dict[str, Any], Path, Path, dict[str, Any], dict[str, Any]]:
    contract, materialized, _, receipt = _materialize(tmp_path, monkeypatch)
    labels = build_runner._expected_build_labels(contract, receipt)
    output_tag = "qt-assurance-runtime:test"
    state, control_runner = _fake_docker_control(
        contract=contract,
        output_tag=output_tag,
        labels=labels,
        daemon_ids=daemon_ids,
        preexisting=preexisting,
    )
    monkeypatch.setattr(docker_lifecycle, "_default_command_runner", control_runner)
    docker = tmp_path / "docker"
    docker.write_bytes(b"fake-docker-executable\n")
    docker.chmod(0o700)
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    executed: dict[str, Any] = {"calls": 0}

    def build_command(
        argv: list[str] | tuple[str, ...],
        env: Mapping[str, str],
        timeout: int,
        stdin_bytes: bytes,
    ) -> build_runner.BuildCommandResult:
        executed["calls"] += 1
        executed["argv"] = list(argv)
        executed["timeout"] = timeout
        executed["context_sha256"] = hashlib.sha256(stdin_bytes).hexdigest()
        if mutate_context_during_build:
            context_path = materialized / build_runner.CONTEXT_TAR_NAME
            context_path.write_bytes(context_path.read_bytes() + b"mutated")
        state["built"] = build_exit == 0 or leave_partial_tag
        return build_runner.BuildCommandResult(
            b"build stdout\n", b"build stderr\n", build_exit, False
        )

    record_path = build_runner.build_runner_image(
        root=tmp_path / "source",
        source_commit=SOURCE_COMMIT,
        materialized_root=materialized,
        docker_path=docker,
        private_root=private,
        output_tag=output_tag,
        command_runner=build_command,
    )
    record = json.loads(record_path.read_text(encoding="utf-8"))
    return contract, record_path, docker, state, executed


def test_build_record_is_path_free_exact_and_live_verifiable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract, record_path, docker, _, executed = _build_bundle(tmp_path, monkeypatch)
    record, raw = build_runner.load_build_record(
        record_path,
        root=tmp_path / "source",
        source_commit=SOURCE_COMMIT,
        require_success=True,
        verify_external=True,
    )
    serialized = raw.decode("utf-8")
    assert str(tmp_path) not in serialized
    assert record["status"] == "succeeded"
    assert record["external_order_submission_enabled"] is False
    assert record["docker_tool"]["executable_basename"] == "docker"
    assert set(record["docker_tool"]) == {
        "executable_basename",
        "executable_sha256",
        "resolved_path_sha256",
        "version",
        "daemon_identity_sha256",
    }
    argv = record["invocation"]["argv"]
    assert argv[0] == "docker"
    assert "--network=none" in argv
    assert "--pull=false" in argv
    assert "--no-cache" in argv
    assert argv[-1] == "-"
    assert record["invocation"]["argv_sha256"] == build_runner._argv_sha256(argv)
    assert executed["argv"][0] == str(docker.resolve())
    assert executed["argv"][1:] == argv[1:]
    assert executed["context_sha256"] == record["build_context"]["tar"]["sha256"]
    assert record["source_materials"] == contract["source_materials"]
    assert build_runner.archivable_build_record(record) == record

    live = build_runner.validate_live_build_record(
        record,
        docker_path=docker,
        root=tmp_path / "source",
        private_root=tmp_path / "private",
    )
    assert live["output_image"] == record["output_image"]
    assert live["base_images"] == record["base_images"]


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda record: record["source_materials"]["dockerfile"].update(
                path="caller/Dockerfile"
            ),
            "source_materials_mismatch",
        ),
        (
            lambda record: record["invocation"]["argv"].remove("--network=none"),
            "argv_mismatch",
        ),
        (
            lambda record: record["invocation"].update(argv_sha256="0" * 64),
            "argv_hash_mismatch",
        ),
        (
            lambda record: record["docker_tool"].update(resolved_path="/usr/bin/docker"),
            "keys_mismatch",
        ),
        (
            lambda record: record["output_image"]["labels"].update(
                {build_runner.SOURCE_LABEL: "c" * 40}
            ),
            "labels:mismatch",
        ),
    ],
)
def test_build_record_rejects_tampered_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Any,
    match: str,
) -> None:
    contract, record_path, _, _, _ = _build_bundle(tmp_path, monkeypatch)
    record = json.loads(record_path.read_text(encoding="utf-8"))
    mutate(record)
    with pytest.raises(build_runner.RunnerBuildError, match=match):
        build_runner._validate_build_record_payload(
            record, contract=contract, require_success=True
        )


def test_build_record_external_validation_rejects_tar_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, record_path, _, _, _ = _build_bundle(tmp_path, monkeypatch)
    context = record_path.parent / build_runner.CONTEXT_TAR_NAME
    context.write_bytes(context.read_bytes() + b"tamper")
    with pytest.raises(build_runner.RunnerBuildError, match="external_identity_mismatch"):
        build_runner.load_build_record(
            record_path,
            root=tmp_path / "source",
            source_commit=SOURCE_COMMIT,
            verify_external=True,
        )


def test_build_record_bundle_requires_exact_five_file_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, record_path, _, _, _ = _build_bundle(tmp_path, monkeypatch)
    extra = record_path.parent / "unreviewed-extra"
    extra.write_bytes(b"extra")
    with pytest.raises(build_runner.RunnerBuildError, match="closed_inventory"):
        build_runner.load_build_record(
            record_path,
            root=tmp_path / "source",
            source_commit=SOURCE_COMMIT,
            verify_external=True,
        )
    extra.unlink()
    (record_path.parent / build_runner.DEFAULT_BUILD_LOG_NAME).unlink()
    with pytest.raises(build_runner.RunnerBuildError, match="closed_inventory"):
        build_runner.load_build_record(
            record_path,
            root=tmp_path / "source",
            source_commit=SOURCE_COMMIT,
            verify_external=True,
        )


def test_build_uses_immutable_context_snapshot_and_rejects_bundle_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(
        build_runner.RunnerBuildError, match="materialization_changed_during_build"
    ):
        _build_bundle(
            tmp_path, monkeypatch, mutate_context_during_build=True
        )
    assert not list(tmp_path.rglob(build_runner.DEFAULT_BUILD_RECORD_NAME))
    assert not list(tmp_path.rglob(build_runner.DEFAULT_BUILD_LOG_NAME))


def test_build_rejects_preexisting_output_tag_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(build_runner.RunnerBuildError, match="output_tag_preexisting"):
        _build_bundle(tmp_path, monkeypatch, preexisting=True)


def test_build_rejects_daemon_change_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(build_runner.RunnerBuildError, match="changed_before_build"):
        _build_bundle(
            tmp_path,
            monkeypatch,
            daemon_ids=["daemon-first", "daemon-second"],
        )


def test_build_rejects_daemon_change_before_image_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(
        build_runner.RunnerBuildError, match="changed_before_image_acceptance"
    ):
        _build_bundle(
            tmp_path,
            monkeypatch,
            daemon_ids=[
                "daemon-stable",
                "daemon-stable",
                "daemon-stable",
                "daemon-replaced",
            ],
        )


def test_failed_build_is_recorded_only_without_partial_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, record_path, _, _, _ = _build_bundle(tmp_path, monkeypatch, build_exit=1)
    record, _ = build_runner.load_build_record(
        record_path,
        root=tmp_path / "source",
        source_commit=SOURCE_COMMIT,
        require_success=False,
        verify_external=True,
    )
    assert record["status"] == "failed"
    assert record["output_image"] is None
    with pytest.raises(build_runner.RunnerBuildError, match="successful_build_required"):
        build_runner.load_build_record(
            record_path,
            root=tmp_path / "source",
            source_commit=SOURCE_COMMIT,
            require_success=True,
            verify_external=False,
        )


def test_failed_build_with_partial_tag_is_rejected_without_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(build_runner.RunnerBuildError, match="output_tag_preexisting"):
        _build_bundle(
            tmp_path,
            monkeypatch,
            build_exit=1,
            leave_partial_tag=True,
        )
    assert not list(tmp_path.rglob(build_runner.DEFAULT_BUILD_RECORD_NAME))


def test_source_change_before_record_publication_leaves_no_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel = _wheel_bytes()
    contract = _contract(wheel)
    _bind_contract(monkeypatch, contract)
    cache = tmp_path / "cache"
    cache.mkdir(mode=0o700)
    (cache / contract["manifest"]["entries"][0]["filename"]).write_bytes(wheel)
    materialized = tmp_path / "materialized"
    build_runner.materialize_runner(
        root=tmp_path / "source",
        source_commit=SOURCE_COMMIT,
        cache_roots=[cache],
        output_root=materialized,
    )
    receipt = json.loads(
        (materialized / build_runner.MATERIALIZATION_RECEIPT_NAME).read_text()
    )
    labels = build_runner._expected_build_labels(contract, receipt)
    state, control = _fake_docker_control(
        contract=contract,
        output_tag="qt-assurance-runtime:test",
        labels=labels,
    )
    monkeypatch.setattr(docker_lifecycle, "_default_command_runner", control)
    docker = tmp_path / "docker"
    docker.write_bytes(b"fake\n")
    docker.chmod(0o700)
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    checks = {"count": 0}

    def clean(root: Path, source_commit: str) -> tuple[str, str]:
        checks["count"] += 1
        if checks["count"] == 4:
            raise build_runner.RunnerBuildError("source_worktree_not_exact_clean")
        return source_commit, SOURCE_TREE

    monkeypatch.setattr(build_runner, "require_exact_clean_source", clean)

    def command(
        argv: list[str], env: Mapping[str, str], timeout: int, stdin_bytes: bytes
    ) -> build_runner.BuildCommandResult:
        state["built"] = True
        return build_runner.BuildCommandResult(b"", b"", 0)

    with pytest.raises(build_runner.RunnerBuildError, match="not_exact_clean"):
        build_runner.build_runner_image(
            root=tmp_path / "source",
            source_commit=SOURCE_COMMIT,
            materialized_root=materialized,
            docker_path=docker,
            private_root=private,
            output_tag="qt-assurance-runtime:test",
            command_runner=command,
        )
    assert not (materialized / build_runner.DEFAULT_BUILD_RECORD_NAME).exists()
    assert not (materialized / build_runner.DEFAULT_BUILD_LOG_NAME).exists()

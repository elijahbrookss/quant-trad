"""A runtime consumer must receive the same archive read view as the backend."""
from types import SimpleNamespace
from pathlib import Path
import os

import pytest

from portal.backend.service.bots.runner import DockerBotRunner
from core.settings import clear_settings_cache


@pytest.fixture
def launch(monkeypatch):
    for key in ("QT_MARKET_DATA_ROOT", "QT_MARKET_DATA_EXPECTED_UUID",
                "MARKET_STRUCTURE_STORAGE_ROOT", "QT_STORAGE_UDEV_ROOT", "QT_ARCHIVE_SHARED_GROUP_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("portal.backend.service.bots.runner._SECURITY_SETTINGS",
                        SimpleNamespace(provider_credential_key="disposable-not-a-credential"))
    monkeypatch.setattr(DockerBotRunner, "inspect_bot_container",
                        staticmethod(lambda *args, **kwargs: {"status":"missing", "running":False}))
    monkeypatch.setattr(DockerBotRunner, "_resolve_runtime_network", lambda self: "disposable-internal")
    commands = []
    def run(command):
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout="disposable-container", stderr="")
    monkeypatch.setattr(DockerBotRunner, "_run_docker", staticmethod(run))
    def start():
        clear_settings_cache()
        return DockerBotRunner(image="disposable-image", network="disposable-internal").start_bot(
            bot={"id":"disposable-bot", "snapshot_interval_ms":250}, run_id="disposable-run")
    yield start, commands
    clear_settings_cache()


def values(command, flag):
    return [command[i+1] for i, value in enumerate(command) if value == flag]


def test_runtime_receives_read_only_archive_without_live_or_database_mounts(launch, tmp_path, monkeypatch):
    start, commands = launch
    archive = tmp_path/"container-archive"
    archive.mkdir()
    monkeypatch.setenv("QT_MARKET_DATA_ROOT", "/host/history/archive")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(archive))
    assert start() == "disposable-container"
    command = commands[-1]
    assert values(command, "--mount") == [f"type=bind,src=/host/history/archive,dst={archive},readonly"]
    assert f"MARKET_STRUCTURE_STORAGE_ROOT={archive}" in values(command, "-e")


def test_dedicated_archive_missing_host_mapping_blocks_before_container_start(launch, monkeypatch):
    start, commands = launch
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "disposable-archive")
    with pytest.raises(RuntimeError, match="runtime_archive_host_root_required"):
        start()
    assert commands == []


def test_dedicated_runtime_retains_mount_identity_and_read_only_udev(launch, tmp_path, monkeypatch):
    start, commands = launch
    archive = tmp_path/"container-archive"
    archive.mkdir()
    udev = tmp_path/"udev"
    udev.mkdir()
    device = archive.stat().st_dev
    (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=disposable-archive\n")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(archive))
    monkeypatch.setenv("QT_MARKET_DATA_ROOT", "/host/history/archive")
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "disposable-archive")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    start()
    assert values(commands[-1], "--mount") == [
        f"type=bind,src=/host/history/archive,dst={archive},readonly",
        f"type=bind,src=/run/udev/data,dst={udev},readonly",
    ]
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "wrong-drive")
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        start()
    assert len(commands) == 1


@pytest.mark.parametrize("host", ["/", "//", "/host/..", "relative/archive", "/host,readonly=false"])
def test_unsafe_archive_source_rejected_before_launch(launch, tmp_path, monkeypatch, host):
    start, commands = launch
    monkeypatch.setenv("QT_MARKET_DATA_ROOT", host)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    with pytest.raises(RuntimeError, match="runtime_archive_mount_invalid"):
        start()
    assert commands == []


def test_unconfigured_runtime_keeps_existing_launch_contract(launch):
    start, commands = launch
    start()
    assert values(commands[-1], "--mount") == []


def test_shared_archive_bot_runs_as_application_with_only_archive_group(launch, tmp_path, monkeypatch):
    start, commands = launch
    monkeypatch.setenv("QT_ARCHIVE_SHARED_GROUP_ID", "70")
    monkeypatch.setenv("QT_MARKET_DATA_ROOT", "/host/history/archive")
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    start()
    command = commands[-1]
    assert values(command, "--user") == ["1000:1000"]
    assert values(command, "--group-add") == ["70"]
    assert values(command, "--cap-drop") == ["ALL"]
    assert values(command, "--security-opt") == ["no-new-privileges"]
    assert values(command, "--mount") == [f"type=bind,src=/host/history/archive,dst={tmp_path},readonly"]


def test_shared_group_cannot_start_bot_without_archive_mount(launch, monkeypatch):
    start, commands = launch
    monkeypatch.setenv("QT_ARCHIVE_SHARED_GROUP_ID", "70")
    with pytest.raises(RuntimeError, match="runtime_shared_archive_mount_required"):
        start()
    assert not commands

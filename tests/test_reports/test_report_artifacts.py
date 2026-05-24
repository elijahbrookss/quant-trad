from __future__ import annotations

from types import SimpleNamespace
from zipfile import ZipFile
import io

from portal.backend.service.reports import artifacts


def test_build_run_archive_streams_existing_run_directory(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(artifacts, "_ARTIFACT_SETTINGS", SimpleNamespace(root_dir=str(tmp_path)))
    run_dir = tmp_path / "bot_id=bot-1" / "run_id=run-1"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text('{"run_id":"run-1"}\n', encoding="utf-8")
    (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

    archive_bytes, filename = artifacts.build_run_archive("run-1")

    assert filename == "run_id=run-1.zip"
    with ZipFile(io.BytesIO(archive_bytes)) as archive:
        names = sorted(archive.namelist())
    assert "run_id=run-1/manifest.json" in names
    assert "run_id=run-1/summary.md" in names

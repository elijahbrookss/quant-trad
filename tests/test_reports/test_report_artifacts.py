from __future__ import annotations

from types import SimpleNamespace
from zipfile import ZipFile
import csv
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


def test_list_run_indicator_output_rows_reads_finalized_csv_artifacts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(artifacts, "_ARTIFACT_SETTINGS", SimpleNamespace(root_dir=str(tmp_path)))
    run_dir = tmp_path / "bot_id=bot-1" / "run_id=run-1"
    indicator_path = run_dir / "series" / "symbol=BTC_USD" / "timeframe=1h" / "indicators" / "indicator-1.csv"
    indicator_path.parent.mkdir(parents=True)
    with indicator_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "bot_id",
                "bar_time",
                "known_at",
                "indicator_id",
                "output_name",
                "output_type",
                "ready",
                "value_json",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "run_id": "run-1",
                "bot_id": "bot-1",
                "bar_time": "2026-01-01T00:00:00Z",
                "known_at": "2026-01-01T00:00:00Z",
                "indicator_id": "indicator-1",
                "output_name": "candidate_lifecycle",
                "output_type": "lifecycle",
                "ready": "true",
                "value_json": '{"events":[{"candidate_id":"candidate-1","family":"retest","side":"long","stage":"formed","status":"active","reason":"source_confirmed","known_at":1767225600}]}',
            }
        )
    (run_dir / "manifest.json").write_text(
        '{"status":"completed","files":[{"path":"series/symbol=BTC_USD/timeframe=1h/indicators/indicator-1.csv","kind":"indicator_outputs","rows":1}]}\n',
        encoding="utf-8",
    )

    payload = artifacts.list_run_indicator_output_rows("run-1", bot_id="bot-1", output_type="lifecycle")

    assert payload["available"] is True
    assert len(payload["items"]) == 1
    row = payload["items"][0]
    assert row["symbol"] == "BTC_USD"
    assert row["timeframe"] == "1h"
    assert row["output_type"] == "lifecycle"

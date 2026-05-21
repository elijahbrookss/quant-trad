"""Contract-backed report export bundles."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Dict, List

from .contract import get_candle_dataset, get_run_research_dataset


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, indent=2, default=str).encode("utf-8")


def _csv_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    output = io.StringIO()
    clean_rows = [dict(row) for row in rows]
    if not clean_rows:
        return b""
    fieldnames: List[str] = sorted({str(key) for row in clean_rows for key in row.keys()})
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in clean_rows:
        writer.writerow(
            {
                key: json.dumps(value, sort_keys=True, default=str) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            }
        )
    return output.getvalue().encode("utf-8")


def _metrics_payload(dataset: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "schema_version": "report_metrics.v1",
        "summary": dataset.get("summary") or {},
        "fee_accounting": dataset.get("fee_accounting") or {},
        "wallet_accounting": dataset.get("wallet_accounting") or {},
        "execution": dataset.get("execution") or {},
        "data_quality": dataset.get("candle_gaps") or {},
        "portfolio_metrics": dataset.get("portfolio_metrics") or {},
        "performance": dataset.get("performance") or {},
        "strategy_insights": dataset.get("strategy_insights") or {},
    }


def _file_entry(
    *,
    path: str,
    content_type: str,
    section: str,
    payload: bytes,
    row_count: int | None = None,
) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "path": path,
        "content_type": content_type,
        "section": section,
        "format": "csv" if content_type == "text/csv" else "json",
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    if row_count is not None:
        entry["row_count"] = int(row_count)
    return entry


def _safe_name(value: Any) -> str:
    text = str(value or "unknown").strip() or "unknown"
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in text)


def _section_rows(section: Mapping[str, Any], key: str) -> List[Dict[str, Any]]:
    payload = section.get(key)
    if not isinstance(payload, Mapping):
        return []
    return [dict(row) for row in payload.get("items") or [] if isinstance(row, Mapping)]


def _export_files(
    dataset: Mapping[str, Any],
    *,
    include_json: bool,
    include_csv: bool,
    include_candles: bool,
) -> tuple[List[Dict[str, Any]], Dict[str, bytes]]:
    files: List[Dict[str, Any]] = [
        {"path": "manifest.json", "content_type": "application/json", "section": "manifest", "format": "json"}
    ]
    payloads: Dict[str, bytes] = {}

    def add_json(path: str, section: str, payload: Any, *, row_count: int | None = None) -> None:
        encoded = _json_bytes(payload)
        payloads[path] = encoded
        files.append(_file_entry(path=path, content_type="application/json", section=section, payload=encoded, row_count=row_count))

    def add_csv(path: str, section: str, rows: Sequence[Mapping[str, Any]]) -> None:
        clean_rows = [dict(row) for row in rows]
        encoded = _csv_bytes(clean_rows)
        payloads[path] = encoded
        files.append(_file_entry(path=path, content_type="text/csv", section=section, payload=encoded, row_count=len(clean_rows)))

    diagnostics = dict(dataset.get("diagnostics") or {})
    diagnostics_items = [dict(row) for row in diagnostics.get("items") or [] if isinstance(row, Mapping)]
    trades = [dict(row) for row in dataset.get("trades") or [] if isinstance(row, Mapping)]
    decisions = [dict(row) for row in dataset.get("decisions") or [] if isinstance(row, Mapping)]
    signals = [dict(row) for row in dataset.get("signals") or [] if isinstance(row, Mapping)]
    timeseries = dict(dataset.get("timeseries") or {})
    timeseries_sections = dict(timeseries.get("items") or {})
    context = dict(dataset.get("context") or {})
    candle_catalog = dict(dataset.get("candle_catalog") or {})
    candle_catalog_rows = [dict(row) for row in candle_catalog.get("items") or [] if isinstance(row, Mapping)]

    if include_json:
        add_json("metadata.json", "metadata", dataset.get("metadata") or {})
        add_json("readiness.json", "readiness", dataset.get("readiness") or {})
        add_json("summary.json", "summary", dataset.get("summary") or {})
        add_json("sections.json", "sections", dataset.get("sections") or {})
        add_json("diagnostics.json", "diagnostics", diagnostics, row_count=len(diagnostics_items))
        add_json("trades.json", "trades", trades, row_count=len(trades))
        add_json("decisions.json", "decisions", decisions, row_count=len(decisions))
        add_json("signals.json", "signals", signals, row_count=len(signals))
        add_json("metrics.json", "metrics", _metrics_payload(dataset))
        add_json("candle_catalog.json", "candle_catalog", candle_catalog, row_count=len(candle_catalog_rows))
        add_json("context.json", "context", context)
        add_json("operational_health.json", "operational_health", dataset.get("operational_health") or {})
        for name, payload in timeseries_sections.items():
            if not isinstance(payload, Mapping):
                continue
            rows = [dict(row) for row in payload.get("items") or [] if isinstance(row, Mapping)]
            add_json(f"timeseries/{_safe_name(name)}.json", f"timeseries.{name}", rows, row_count=len(rows))
        for name in ("indicator_snapshots", "decision_context", "trade_context", "market_state"):
            rows = _section_rows(context, name)
            add_json(f"context/{_safe_name(name)}.json", f"context.{name}", rows, row_count=len(rows))
    if include_csv:
        add_csv("trades.csv", "trades", trades)
        add_csv("decisions.csv", "decisions", decisions)
        add_csv("signals.csv", "signals", signals)
        add_csv("diagnostics.csv", "diagnostics", diagnostics_items)
        add_csv("candle_catalog.csv", "candle_catalog", candle_catalog_rows)
        for name, payload in timeseries_sections.items():
            if not isinstance(payload, Mapping):
                continue
            rows = [dict(row) for row in payload.get("items") or [] if isinstance(row, Mapping)]
            add_csv(f"timeseries/{_safe_name(name)}.csv", f"timeseries.{name}", rows)
        for name in ("indicator_snapshots", "decision_context", "trade_context", "market_state"):
            rows = _section_rows(context, name)
            add_csv(f"context/{_safe_name(name)}.csv", f"context.{name}", rows)
    if include_candles:
        run_id = str(dict(dataset.get("metadata") or {}).get("run_id") or "")
        for item in candle_catalog_rows:
            instrument_id = str(item.get("instrument_id") or "").strip()
            timeframe = str(item.get("timeframe") or "").strip()
            start = item.get("start_time")
            end = item.get("end_time")
            if not instrument_id or not timeframe or not start or not end:
                continue
            candle_payload = get_candle_dataset(
                run_id,
                instrument_id=instrument_id,
                timeframe=timeframe,
                start=str(start),
                end=str(end),
                limit=20000,
            )
            candle_rows = [dict(row) for row in candle_payload.get("items") or [] if isinstance(row, Mapping)]
            stem = f"{_safe_name(instrument_id)}_{_safe_name(timeframe)}"
            if include_json:
                add_json(f"candles/{stem}.json", "candles", candle_rows, row_count=len(candle_rows))
            if include_csv:
                add_csv(f"candles/{stem}.csv", "candles", candle_rows)
    return files, payloads


def _manifest(dataset: Mapping[str, Any], *, files: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    metadata = dict(dataset.get("metadata") or {})
    diagnostics = dict(dataset.get("diagnostics") or {})
    run_id = str(metadata.get("run_id") or "")
    sections = dict(dataset.get("sections") or {})
    unavailable_sections: List[Dict[str, Any]] = []
    for section in sections.get("items") or []:
        if not isinstance(section, Mapping):
            continue
        if section.get("available"):
            continue
        unavailable_sections.append(
            {
                "name": section.get("name"),
                "reason": section.get("reason") or "unavailable",
            }
        )

    return {
        "schema_version": "export_manifest.v1",
        "export_manifest_version": "export_manifest.v1",
        "run_id": run_id,
        "dataset_schema_version": str(dataset.get("schema_version") or ""),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": dict(dataset.get("readiness") or {}).get("export_status")
        or ("available" if dict(dataset.get("readiness") or {}).get("dataset_ready") else "unavailable"),
        "filename": f"run_{run_id}_report_export.zip",
        "files": [dict(entry) for entry in files],
        "unavailable_sections": unavailable_sections,
        "diagnostics": {
            "schema_version": str(diagnostics.get("schema_version") or "report_diagnostics.v1"),
            "run_id": run_id,
            "summary": dict(diagnostics.get("summary") or {}),
        },
    }


def build_export_manifest(
    run_id: str,
    *,
    include_json: bool = True,
    include_csv: bool = True,
    include_candles: bool = False,
) -> Dict[str, Any]:
    dataset = get_run_research_dataset(run_id)
    files, _payloads = _export_files(dataset, include_json=include_json, include_csv=include_csv, include_candles=include_candles)
    return _manifest(dataset, files=files)


def build_export_archive(
    run_id: str,
    *,
    include_json: bool = True,
    include_csv: bool = True,
    include_candles: bool = False,
) -> tuple[bytes, str]:
    dataset = get_run_research_dataset(run_id)
    files, payloads = _export_files(dataset, include_json=include_json, include_csv=include_csv, include_candles=include_candles)
    manifest = _manifest(dataset, files=files)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", _json_bytes(manifest))
        for path, payload in payloads.items():
            archive.writestr(path, payload)
    return buffer.getvalue(), str(manifest.get("filename") or f"run_{run_id}_report_export.zip")


__all__ = ["build_export_archive", "build_export_manifest"]

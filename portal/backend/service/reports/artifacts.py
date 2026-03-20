"""Run-scoped report artifact bundle generation."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import io
import json
import logging
from pathlib import Path
import shutil
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from zipfile import ZIP_DEFLATED, ZipFile

from core.settings import get_settings
from utils.log_context import build_log_context, with_log_context


logger = logging.getLogger(__name__)
_SETTINGS = get_settings()
_ARTIFACT_SETTINGS = _SETTINGS.reports.artifacts
_REPO_ROOT = Path(__file__).resolve().parents[4]
_SUPPORTED_OUTPUT_FORMATS = {"csv", "parquet"}
_MANIFEST_VERSION = 1


def _storage():
    from portal.backend.service.storage import storage

    return storage


def _report_helpers():
    from .report_service import _closed_trades, _compute_summary, _parse_iso

    return _closed_trades, _compute_summary, _parse_iso


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _sanitize_segment(value: Any, default: str = "unknown") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    return "".join(char if char.isalnum() or char in {"-", "_", "="} else "_" for char in text)


def _artifact_root() -> Path:
    configured = Path(_ARTIFACT_SETTINGS.root_dir)
    if configured.is_absolute():
        return configured
    return (_REPO_ROOT / configured).resolve()


def _run_directory(bot_id: str, run_id: str) -> Path:
    return _artifact_root() / f"bot_id={_sanitize_segment(bot_id)}" / f"run_id={_sanitize_segment(run_id)}"


def _manifest_path(run_dir: Path) -> Path:
    return run_dir / "manifest.json"


def _artifact_status(runtime_status: str) -> str:
    normalized = str(runtime_status or "").strip().lower()
    if normalized == "completed":
        return "completed"
    if normalized == "stopped":
        return "aborted"
    if normalized == "error":
        return "failed"
    return "in_progress"


def _output_extension(output_format: str) -> str:
    fmt = str(output_format or "").strip().lower()
    if fmt not in _SUPPORTED_OUTPUT_FORMATS:
        raise RuntimeError(f"report_artifacts_invalid_output_format: {fmt}")
    return fmt


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(dict(payload)), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_json_safe(dict(payload)), sort_keys=True) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(dict(json.loads(text)))
    return rows


def _write_tabular(path: Path, rows: Sequence[Mapping[str, Any]], *, output_format: str) -> int:
    clean_rows = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        normalized: dict[str, Any] = {}
        for key, value in dict(row).items():
            safe_value = _json_safe(value)
            if isinstance(safe_value, (dict, list)):
                normalized[str(key)] = json.dumps(safe_value, sort_keys=True)
            else:
                normalized[str(key)] = safe_value
        clean_rows.append(normalized)
    if not clean_rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = _output_extension(output_format)
    if fmt == "csv":
        fieldnames: list[str] = []
        for row in clean_rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(str(key))
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in clean_rows:
                writer.writerow({key: row.get(key) for key in fieldnames})
        return len(clean_rows)
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:  # pragma: no cover - environment specific
        raise RuntimeError("report_artifacts_parquet_requires_pyarrow") from exc
    import pandas as pd

    frame = pd.DataFrame(clean_rows)
    frame.to_parquet(path, index=False)
    return len(clean_rows)


def _stream_zip_bytes(root: Path) -> bytes:
    buffer = io.BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                continue
            archive.write(path, arcname=str(path.relative_to(root.parent)))
    return buffer.getvalue()


def _build_series_snapshot(series: Sequence[Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    snapshots: list[dict[str, Any]] = []
    indicator_meta_by_id: dict[str, dict[str, Any]] = {}
    storage = _storage()
    for entry in series:
        meta = dict(getattr(entry, "meta", {}) or {})
        indicator_links = list(meta.get("indicator_links") or [])
        indicator_ids = []
        for link in indicator_links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id:
                continue
            indicator_ids.append(indicator_id)
            if indicator_id not in indicator_meta_by_id:
                record = storage.get_indicator(indicator_id) or {}
                indicator_meta_by_id[indicator_id] = {
                    "id": indicator_id,
                    "type": record.get("type") or "unknown",
                    "version": record.get("version") or "v1",
                    "name": record.get("name") or indicator_id,
                    "params": dict(record.get("params") or {}),
                    "enabled": bool(record.get("enabled", True)),
                }
        snapshots.append(
            {
                "strategy_id": getattr(entry, "strategy_id", None),
                "name": getattr(entry, "name", None),
                "symbol": getattr(entry, "symbol", None),
                "timeframe": getattr(entry, "timeframe", None),
                "datasource": getattr(entry, "datasource", None),
                "exchange": getattr(entry, "exchange", None),
                "window_start": getattr(entry, "window_start", None),
                "window_end": getattr(entry, "window_end", None),
                "indicator_ids": indicator_ids,
            }
        )
    return snapshots, indicator_meta_by_id


def _build_config_snapshot(config: Mapping[str, Any], series: Sequence[Any]) -> dict[str, Any]:
    symbols: list[str] = []
    strategies: list[dict[str, Any]] = []
    strategy_ids_seen: set[str] = set()
    storage = _storage()
    for entry in series:
        symbol = str(getattr(entry, "symbol", "") or "").strip()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
        strategy_id = str(getattr(entry, "strategy_id", "") or "").strip()
        if not strategy_id or strategy_id in strategy_ids_seen:
            continue
        strategy_ids_seen.add(strategy_id)
        meta = dict(getattr(entry, "meta", {}) or {})
        indicator_links = list(meta.get("indicator_links") or [])
        indicator_params = []
        for link in indicator_links:
            indicator_id = str(link.get("indicator_id") or link.get("id") or "").strip()
            if not indicator_id:
                continue
            record = storage.get_indicator(indicator_id)
            if record:
                indicator_params.append(record)
        strategies.append(
            {
                "id": strategy_id,
                "name": meta.get("name") or getattr(entry, "name", strategy_id),
                "timeframe": meta.get("timeframe") or getattr(entry, "timeframe", None),
                "datasource": meta.get("datasource") or getattr(entry, "datasource", None),
                "exchange": meta.get("exchange") or getattr(entry, "exchange", None),
                "atm_template_id": meta.get("atm_template_id"),
                "atm_template": meta.get("atm_template") or {},
                "rules": meta.get("rules") or {},
                "indicator_ids": [row.get("id") for row in indicator_params],
                "indicator_params": indicator_params,
                "instruments": list(meta.get("instrument_links") or []),
            }
        )
    timeframe = getattr(series[0], "timeframe", None) if series else None
    datasource = getattr(series[0], "datasource", None) if series else None
    exchange = getattr(series[0], "exchange", None) if series else None
    return {
        "wallet_start": dict(config.get("wallet_config") or {}),
        "risk_settings": dict(config.get("risk") or {}),
        "date_range": {
            "start": config.get("backtest_start"),
            "end": config.get("backtest_end"),
        },
        "symbols": symbols,
        "timeframe": timeframe,
        "datasource": datasource,
        "exchange": exchange,
        "fee_model": (config.get("risk") or {}).get("fee_model"),
        "slippage_model": (config.get("risk") or {}).get("slippage_model"),
        "strategies": strategies,
    }


class RunArtifactBundle:
    """Filesystem-backed run artifact bundle with incremental spool semantics."""

    def __init__(
        self,
        *,
        bot_id: str,
        run_id: str,
        config: Mapping[str, Any],
        series: Sequence[Any],
    ) -> None:
        self.bot_id = str(bot_id)
        self.run_id = str(run_id)
        self.config = dict(config or {})
        self.series = list(series or [])
        self.run_type = str(self.config.get("run_type") or "backtest").strip().lower()
        self.settings = get_settings().reports.artifacts
        self.output_format = _output_extension(self.settings.output_format)
        self.run_dir = _run_directory(self.bot_id, self.run_id)
        self.spool_dir = self.run_dir / ".spool"
        self.execution_dir = self.run_dir / "execution"
        self.run_meta_dir = self.run_dir / "run"
        self.summary_dir = self.run_dir / "summary"
        self._started = False
        self._series_snapshot, self._indicator_meta_by_id = _build_series_snapshot(self.series)

    @property
    def enabled(self) -> bool:
        if not self.settings.enabled:
            return False
        if self.run_type == "backtest":
            return bool(self.settings.capture_backtest)
        if self.run_type == "preview":
            return False
        return bool(self.settings.capture_live)

    def start(self, *, started_at: str) -> None:
        if not self.enabled or self._started:
            return
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            _manifest_path(self.run_dir),
            {
                "manifest_version": _MANIFEST_VERSION,
                "bot_id": self.bot_id,
                "run_id": self.run_id,
                "run_type": self.run_type,
                "status": "in_progress",
                "started_at": started_at,
                "ended_at": None,
                "output_format": self.output_format,
                "generated_at": _utcnow_iso(),
                "files": [],
            },
        )
        _write_json(
            self.run_meta_dir / "metadata.json",
            {
                "bot_id": self.bot_id,
                "run_id": self.run_id,
                "run_type": self.run_type,
                "started_at": started_at,
            },
        )
        _write_json(self.run_meta_dir / "config.json", self.config)
        _write_json(self.run_meta_dir / "series.json", {"series": self._series_snapshot})
        _write_json(self.run_meta_dir / "indicators.json", {"indicators": list(self._indicator_meta_by_id.values())})
        if self.settings.include_candles:
            for entry in self.series:
                series_spool = self._series_spool_dir(entry)
                for candle in list(getattr(entry, "candles", []) or []):
                    _append_jsonl(
                        series_spool / "candles.jsonl",
                        {
                            "time": getattr(candle, "time", None),
                            "open": getattr(candle, "open", None),
                            "high": getattr(candle, "high", None),
                            "low": getattr(candle, "low", None),
                            "close": getattr(candle, "close", None),
                            "volume": getattr(candle, "volume", None),
                        },
                    )
        self._started = True

    def record_runtime_event(self, *, serialized: Mapping[str, Any], decision_entry: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        if self.settings.include_runtime_events:
            _append_jsonl(self.execution_dir / "runtime_events.jsonl", serialized)
        if self.settings.include_decision_trace:
            _append_jsonl(self.spool_dir / "execution" / "decision_trace.jsonl", decision_entry)

    def record_indicator_frame(self, *, state: Any, candle: Any) -> None:
        if not self.enabled:
            return
        series = getattr(state, "series", None)
        if series is None:
            return
        series_spool = self._series_spool_dir(series)
        if self.settings.include_indicator_outputs:
            outputs = dict(getattr(state, "indicator_outputs", {}) or {})
            for output_key, runtime_output in outputs.items():
                indicator_id, _, output_name = str(output_key).partition(".")
                indicator_meta = self._indicator_meta_by_id.get(indicator_id, {})
                _append_jsonl(
                    series_spool / "indicators" / f"{_sanitize_segment(indicator_id)}.jsonl",
                    {
                        "bar_time": getattr(runtime_output, "bar_time", None),
                        "known_at": getattr(runtime_output, "bar_time", None),
                        "indicator_id": indicator_id,
                        "indicator_type": indicator_meta.get("type") or "unknown",
                        "indicator_version": indicator_meta.get("version") or "v1",
                        "output_name": output_name,
                        "output_type": (getattr(state, "indicator_output_types", {}) or {}).get(output_key),
                        "ready": bool(getattr(runtime_output, "ready", False)),
                        "value_json": json.dumps(_json_safe(getattr(runtime_output, "value", {})), sort_keys=True),
                    },
                )
        if self.settings.include_overlays:
            overlays = dict(getattr(state, "indicator_overlays", {}) or {})
            for overlay_key, runtime_overlay in overlays.items():
                indicator_id, _, overlay_name = str(overlay_key).partition(".")
                indicator_meta = self._indicator_meta_by_id.get(indicator_id, {})
                _append_jsonl(
                    series_spool / "overlays" / f"{_sanitize_segment(indicator_id)}.jsonl",
                    {
                        "bar_time": getattr(runtime_overlay, "bar_time", None),
                        "known_at": getattr(runtime_overlay, "bar_time", None),
                        "indicator_id": indicator_id,
                        "indicator_type": indicator_meta.get("type") or "unknown",
                        "indicator_version": indicator_meta.get("version") or "v1",
                        "overlay_name": overlay_name,
                        "ready": bool(getattr(runtime_overlay, "ready", False)),
                        "value": _json_safe(getattr(runtime_overlay, "value", {})),
                    },
                )

    def finalize(
        self,
        *,
        runtime_status: str,
        artifact: Mapping[str, Any],
    ) -> None:
        if not self.enabled:
            return
        if not self._started:
            self.start(started_at=str(artifact.get("started_at") or _utcnow_iso()))
        artifact_status = _artifact_status(runtime_status)
        log_context = build_log_context(bot_id=self.bot_id, run_id=self.run_id, status=artifact_status)
        logger.info(with_log_context("report_artifacts_finalize_start", log_context))

        _write_json(self.run_meta_dir / "runtime_artifact.json", artifact)
        config_snapshot = _build_config_snapshot(self.config, self.series)
        storage = _storage()
        _closed_trades, _compute_summary, _parse_iso = _report_helpers()

        files: list[dict[str, Any]] = []
        if self.settings.include_decision_trace:
            rows = _read_jsonl(self.spool_dir / "execution" / "decision_trace.jsonl")
            rows_written = _write_tabular(
                self.execution_dir / f"decision_trace.{self.output_format}",
                rows,
                output_format=self.output_format,
            )
            files.append(
                {
                    "path": f"execution/decision_trace.{self.output_format}",
                    "rows": rows_written,
                    "source": "runtime",
                    "kind": "decision_trace",
                }
            )
        if self.settings.include_candles or self.settings.include_indicator_outputs:
            for entry in self.series:
                final_series_dir = self._series_final_dir(entry)
                series_spool = self._series_spool_dir(entry)
                if self.settings.include_candles:
                    candle_rows = _read_jsonl(series_spool / "candles.jsonl")
                    rows_written = _write_tabular(
                        final_series_dir / f"candles.{self.output_format}",
                        candle_rows,
                        output_format=self.output_format,
                    )
                    files.append(
                        {
                            "path": str((final_series_dir / f"candles.{self.output_format}").relative_to(self.run_dir)),
                            "rows": rows_written,
                            "source": "runtime",
                            "kind": "candles",
                        }
                    )
                if self.settings.include_indicator_outputs:
                    indicators_spool = series_spool / "indicators"
                    indicator_paths = sorted(indicators_spool.glob("*.jsonl")) if indicators_spool.exists() else []
                    for path in indicator_paths:
                        indicator_rows = _read_jsonl(path)
                        output_name = path.stem
                        final_path = final_series_dir / "indicators" / f"{output_name}.{self.output_format}"
                        rows_written = _write_tabular(final_path, indicator_rows, output_format=self.output_format)
                        files.append(
                            {
                                "path": str(final_path.relative_to(self.run_dir)),
                                "rows": rows_written,
                                "source": "runtime",
                                "kind": "indicator_outputs",
                            }
                        )
                if self.settings.include_overlays:
                    overlays_spool = series_spool / "overlays"
                    overlay_paths = sorted(overlays_spool.glob("*.jsonl")) if overlays_spool.exists() else []
                    for path in overlay_paths:
                        final_path = final_series_dir / "overlays" / path.name
                        final_path.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(path, final_path)
                        files.append(
                            {
                                "path": str(final_path.relative_to(self.run_dir)),
                                "rows": len(_read_jsonl(path)),
                                "source": "runtime",
                                "kind": "overlays",
                            }
                        )
        trades = storage.list_bot_trades_for_run(self.run_id) if self.settings.include_trades else []
        closed_trades = _closed_trades(trades)
        if self.settings.include_trades:
            rows_written = _write_tabular(
                self.execution_dir / f"trades.{self.output_format}",
                trades,
                output_format=self.output_format,
            )
            files.append(
                {
                    "path": f"execution/trades.{self.output_format}",
                    "rows": rows_written,
                    "source": "postrun_db",
                    "kind": "trades",
                }
            )
        if self.settings.include_trade_events:
            trade_ids = [str(row.get("id") or "") for row in trades if row.get("id")]
            trade_events = storage.list_bot_trade_events_for_trades(trade_ids)
            rows_written = _write_tabular(
                self.execution_dir / f"trade_events.{self.output_format}",
                trade_events,
                output_format=self.output_format,
            )
            files.append(
                {
                    "path": f"execution/trade_events.{self.output_format}",
                    "rows": rows_written,
                    "source": "postrun_db",
                    "kind": "trade_events",
                }
            )
        summary = _compute_summary(
            closed_trades,
            config_snapshot,
            start_time=_parse_iso(self.config.get("backtest_start") or artifact.get("started_at")),
            end_time=_parse_iso(self.config.get("backtest_end") or artifact.get("ended_at")),
        )
        _write_json(self.summary_dir / "summary.json", {"summary": summary, "status": artifact_status})
        (self.summary_dir / "run_summary.md").write_text(
            self._build_summary_markdown(summary, artifact_status=artifact_status),
            encoding="utf-8",
        )
        if self.settings.include_runtime_events:
            files.append(
                {
                    "path": "execution/runtime_events.jsonl",
                    "rows": len(_read_jsonl(self.execution_dir / "runtime_events.jsonl")),
                    "source": "runtime",
                    "kind": "runtime_events",
                }
            )
        files.extend(
            [
                {"path": "summary/summary.json", "rows": 1, "source": "postrun_derived", "kind": "summary"},
                {"path": "summary/run_summary.md", "rows": 1, "source": "postrun_derived", "kind": "summary_markdown"},
                {"path": "run/config.json", "rows": 1, "source": "runtime", "kind": "config"},
                {"path": "run/metadata.json", "rows": 1, "source": "runtime", "kind": "metadata"},
                {"path": "run/series.json", "rows": len(self._series_snapshot), "source": "runtime", "kind": "series"},
                {"path": "run/indicators.json", "rows": len(self._indicator_meta_by_id), "source": "runtime", "kind": "indicator_index"},
                {"path": "run/runtime_artifact.json", "rows": 1, "source": "runtime", "kind": "runtime_artifact"},
            ]
        )
        self._upsert_run_index(
            config_snapshot=config_snapshot,
            summary=summary,
            status=runtime_status,
            started_at=artifact.get("started_at"),
            ended_at=artifact.get("ended_at"),
            decision_trace=list(artifact.get("decision_trace") or []),
        )
        if self.spool_dir.exists():
            shutil.rmtree(self.spool_dir)
        zip_path = None
        if self.settings.compress_zip_on_finalize:
            zip_path = shutil.make_archive(str(self.run_dir), "zip", root_dir=self.run_dir.parent, base_dir=self.run_dir.name)
            files.append(
                {
                    "path": str(Path(zip_path).name),
                    "rows": 1,
                    "source": "postrun_derived",
                    "kind": "zip_bundle",
                }
            )
        _write_json(
            _manifest_path(self.run_dir),
            {
                "manifest_version": _MANIFEST_VERSION,
                "bot_id": self.bot_id,
                "run_id": self.run_id,
                "run_type": self.run_type,
                "status": artifact_status,
                "started_at": artifact.get("started_at"),
                "ended_at": artifact.get("ended_at"),
                "output_format": self.output_format,
                "generated_at": _utcnow_iso(),
                "files": files,
            },
        )
        logger.info(with_log_context("report_artifacts_finalize_done", log_context | {"files": len(files), "zip": zip_path}))

    def _series_spool_dir(self, entry: Any) -> Path:
        return self.spool_dir / "series" / f"symbol={_sanitize_segment(getattr(entry, 'symbol', None))}" / f"timeframe={_sanitize_segment(getattr(entry, 'timeframe', None))}"

    def _series_final_dir(self, entry: Any) -> Path:
        return self.run_dir / "series" / f"symbol={_sanitize_segment(getattr(entry, 'symbol', None))}" / f"timeframe={_sanitize_segment(getattr(entry, 'timeframe', None))}"

    def _build_summary_markdown(self, summary: Mapping[str, Any], *, artifact_status: str) -> str:
        lines = [
            f"# Run Summary: {self.run_id}",
            "",
            f"- Bot ID: `{self.bot_id}`",
            f"- Status: `{artifact_status}`",
            f"- Net PnL: `{summary.get('net_pnl')}`",
            f"- Total Return: `{summary.get('total_return')}`",
            f"- Sharpe: `{summary.get('sharpe')}`",
            f"- Max Drawdown %: `{summary.get('max_drawdown_pct')}`",
            f"- Total Trades: `{summary.get('total_trades')}`",
        ]
        return "\n".join(lines) + "\n"

    def _upsert_run_index(
        self,
        *,
        config_snapshot: Mapping[str, Any],
        summary: Mapping[str, Any],
        status: str,
        started_at: Any,
        ended_at: Any,
        decision_trace: Sequence[Mapping[str, Any]],
    ) -> None:
        strategy = next(iter(config_snapshot.get("strategies") or []), {})
        _storage().upsert_bot_run(
            {
                "run_id": self.run_id,
                "bot_id": self.bot_id,
                "bot_name": self.config.get("name"),
                "strategy_id": strategy.get("id"),
                "strategy_name": strategy.get("name"),
                "run_type": self.run_type,
                "status": status,
                "timeframe": config_snapshot.get("timeframe"),
                "datasource": config_snapshot.get("datasource"),
                "exchange": config_snapshot.get("exchange"),
                "symbols": list(config_snapshot.get("symbols") or []),
                "backtest_start": self.config.get("backtest_start"),
                "backtest_end": self.config.get("backtest_end"),
                "started_at": started_at,
                "ended_at": ended_at,
                "summary": dict(summary or {}),
                "config_snapshot": dict(config_snapshot or {}),
                "decision_ledger": list(decision_trace or []),
            }
        )


def build_run_artifact_bundle(bot_id: str, run_id: str, config: Mapping[str, Any], series: Sequence[Any]) -> Optional[RunArtifactBundle]:
    bundle = RunArtifactBundle(bot_id=bot_id, run_id=run_id, config=config, series=series)
    if not bundle.enabled:
        return None
    return bundle


def build_run_archive(run_id: str) -> tuple[bytes, str]:
    run_dir = find_run_directory(run_id)
    if run_dir is None:
        raise KeyError(f"Run {run_id} report bundle was not found")
    zip_candidate = Path(str(run_dir) + ".zip")
    if zip_candidate.exists():
        return zip_candidate.read_bytes(), zip_candidate.name
    return _stream_zip_bytes(run_dir), f"{run_dir.name}.zip"


def find_run_directory(run_id: str) -> Optional[Path]:
    root = _artifact_root()
    if not root.exists():
        return None
    matches = sorted(root.glob(f"bot_id=*/run_id={_sanitize_segment(run_id)}"))
    if matches:
        return matches[0]
    fallback_matches = sorted(root.rglob(f"run_id={_sanitize_segment(run_id)}"))
    return fallback_matches[0] if fallback_matches else None


__all__ = [
    "RunArtifactBundle",
    "build_run_archive",
    "build_run_artifact_bundle",
    "find_run_directory",
]

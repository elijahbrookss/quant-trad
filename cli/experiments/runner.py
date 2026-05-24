from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from cli.api import ApiClient, ApiError, filename_from_content_disposition
from cli.audit import safe_path_part, utc_now

from .contracts import COMPARISON_REF_SCHEMA, TERMINAL_RUN_STATUSES, json_safe
from .event_log import ExperimentEventLog
from .notifications import notify_terminal_state
from .pass_gates import evaluate_pass_gates
from .state_store import ExperimentStateStore, experiment_id_for_name, find_experiment_dir


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _terminal_status(payload: dict[str, Any]) -> str:
    for key in ("status", "run_status", "phase"):
        value = payload.get(key)
        if value:
            return str(value).strip().lower()
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in ("run_status", "status", "phase"):
            value = summary.get(key)
            if value:
                return str(value).strip().lower()
    return ""


def _step_by_id(state: dict[str, Any], step_id: str) -> dict[str, Any]:
    for step in state.get("steps") or []:
        if step.get("step_id") == step_id:
            return step
    raise ValueError(f"step not found: {step_id}")


def _set_step_status(
    state: dict[str, Any],
    step_id: str,
    status: str,
    *,
    error: dict[str, Any] | None = None,
    artifact_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    step = _step_by_id(state, step_id)
    step["status"] = status
    state["current_step_id"] = step_id if status not in {"COMPLETED", "FAILED", "SKIPPED", "CANCELLED"} else None
    if status == "RUNNING":
        step["started_at"] = step.get("started_at") or utc_now().isoformat()
    if status in {"COMPLETED", "FAILED", "SKIPPED", "CANCELLED"}:
        step["completed_at"] = utc_now().isoformat()
    if error:
        step["error"] = error
    if artifact_refs:
        step.setdefault("artifact_refs", []).extend(artifact_refs)
    return step


class ExperimentRunner:
    def __init__(self, *, client: ApiClient, log_root: str | Path) -> None:
        self.client = client
        self.log_root = Path(log_root).expanduser()

    def create(self, plan: dict[str, Any], *, experiment_id: str | None = None) -> tuple[ExperimentStateStore, dict[str, Any]]:
        resolved_id = experiment_id or experiment_id_for_name(str(plan.get("name")))
        store = ExperimentStateStore(self.log_root, experiment_id=resolved_id)
        state = store.create_state(plan)
        events = ExperimentEventLog(store.events_path, experiment_id=resolved_id)
        events.append(event_type="state_transition", operation="experiment_created", status="succeeded", target={"status": "CREATED"})
        return store, state

    def resume(self, ref: str) -> tuple[ExperimentStateStore, dict[str, Any], dict[str, Any]]:
        path = find_experiment_dir(self.log_root, ref)
        store = ExperimentStateStore(self.log_root, path=path)
        return store, store.load_plan(), store.load_state()

    def run(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        events = ExperimentEventLog(store.events_path, experiment_id=str(state.get("experiment_id")))
        if state.get("status") == "CREATED":
            state["status"] = "VALIDATED"
            store.write_state(state)
            events.append(event_type="state_transition", operation="plan_validated", status="succeeded", target={"status": "VALIDATED"})
        failures: list[dict[str, Any]] = []
        try:
            failures.extend(self._run_bot_steps(store, plan, state, events))
            failures.extend(self._export_steps(store, plan, state, events))
            failures.extend(self._materialize_steps(store, plan, state, events))
            failures.extend(self._summary_steps(store, plan, state, events))
            failures.extend(self._compare_steps(store, plan, state, events))
            self._evaluate_gates(store, plan, state, events)
            if failures and not bool(dict(plan.get("run_policy") or {}).get("stop_on_first_failure", False)):
                state["status"] = "PARTIALLY_COMPLETED"
                state["terminal_error"] = {"error": "one_or_more_steps_failed", "failures": failures}
            elif str(state.get("status")) not in {"FAILED", "PARTIALLY_COMPLETED"}:
                gate_status = self._load_gate_status(store, state)
                state["status"] = "COMPLETED" if gate_status == "PASSED" else "FAILED"
                if gate_status != "PASSED":
                    state["terminal_error"] = {"error": "pass_gates_failed", "pass_gate_status": gate_status}
            self._notify(store, plan, state, events)
            store.write_state(state)
            return state
        except Exception as exc:
            state["status"] = "FAILED"
            state["terminal_error"] = {"error": str(exc), "type": type(exc).__name__}
            store.write_state(state)
            events.append(
                event_type="error",
                operation="experiment_run",
                status="failed",
                error={"message": str(exc), "type": type(exc).__name__},
            )
            self._notify(store, plan, state, events)
            store.write_state(state)
            raise

    def _variant_by_id(self, plan: dict[str, Any], variant_id: str) -> dict[str, Any]:
        for variant in plan.get("variants") or []:
            if str(variant.get("id")) == variant_id:
                return dict(variant)
        raise ValueError(f"variant not found: {variant_id}")

    def _window_by_id(self, plan: dict[str, Any], window_id: str) -> dict[str, Any]:
        for window in plan.get("windows") or []:
            if str(window.get("id")) == window_id:
                return dict(window)
        raise ValueError(f"window not found: {window_id}")

    def _validate_bot_context(self, *, context: dict[str, Any], variant: dict[str, Any]) -> None:
        strategy = context.get("strategy") if isinstance(context.get("strategy"), dict) else {}
        expected_id = str(variant.get("expected_strategy_variant_id") or "").strip()
        expected_name = str(variant.get("expected_strategy_variant") or variant.get("expected_strategy_variant_name") or "").strip()
        if expected_id and expected_id != str(strategy.get("strategy_variant_id") or "").strip():
            raise ValueError(f"bot {variant.get('bot_id')} selected strategy variant id does not match {expected_id}")
        if expected_name and expected_name != str(strategy.get("strategy_variant_name") or "").strip():
            raise ValueError(f"bot {variant.get('bot_id')} selected strategy variant name does not match {expected_name}")

    def _start_or_wait_run(
        self,
        *,
        store: ExperimentStateStore,
        plan: dict[str, Any],
        state: dict[str, Any],
        events: ExperimentEventLog,
        step: dict[str, Any],
    ) -> dict[str, Any]:
        window_id = str(step.get("window_id"))
        variant_id = str(step.get("variant_id"))
        bot_id = str(step.get("bot_id"))
        window = self._window_by_id(plan, window_id)
        variant = self._variant_by_id(plan, variant_id)
        record_path = store.run_record_path(window_id, variant_id)
        record = json.loads(record_path.read_text(encoding="utf-8")) if record_path.exists() else {}
        run_id = str(record.get("run_id") or "").strip()
        if not run_id:
            context = self.client.request_json("GET", f"/api/bots/{bot_id}/run-context")
            if not isinstance(context, dict):
                raise ApiError("bot run context returned unexpected payload")
            self._validate_bot_context(context=context, variant=variant)
            if bool(dict(plan.get("run_policy") or {}).get("update_bot_window", True)):
                events.append(
                    event_type="intent",
                    operation="update_bot_window",
                    status="started",
                    step_id=str(step.get("step_id")),
                    target={"bot_id": bot_id, "window": window},
                )
                self.client.request_json(
                    "PUT",
                    f"/api/bots/{bot_id}",
                    payload={"backtest_start": window.get("start"), "backtest_end": window.get("end")},
                )
                events.append(
                    event_type="result",
                    operation="update_bot_window",
                    status="succeeded",
                    step_id=str(step.get("step_id")),
                    target={"bot_id": bot_id, "window_id": window_id},
                )
            request_id = f"{state.get('experiment_id')}__{safe_path_part(window_id)}__{safe_path_part(variant_id)}"
            events.append(
                event_type="intent",
                operation="start_bot_run",
                status="started",
                step_id=str(step.get("step_id")),
                target={"bot_id": bot_id, "request_id": request_id},
            )
            start_payload = self.client.request_json("POST", f"/api/bots/{bot_id}/runs/start", payload={"request_id": request_id})
            if not isinstance(start_payload, dict):
                raise ApiError("bot run start returned unexpected payload")
            run_id = str(start_payload.get("run_id") or "").strip()
            if not run_id:
                raise ValueError("start response did not include run_id")
            record = {
                "schema_version": "experiment_window_variant_run.v1",
                "experiment_id": state.get("experiment_id"),
                "window_id": window_id,
                "variant_id": variant_id,
                "bot_id": bot_id,
                "run_id": run_id,
                "request_id": request_id,
                "window": window,
                "variant": variant,
                "start": start_payload,
                "status": start_payload.get("status"),
            }
            store.write_run_record(window_id, variant_id, record)
            state.setdefault("run_refs", []).append({"window_id": window_id, "variant_id": variant_id, "bot_id": bot_id, "run_id": run_id, "path": str(record_path)})
            events.append(
                event_type="result",
                operation="start_bot_run",
                status="succeeded",
                step_id=str(step.get("step_id")),
                target={"bot_id": bot_id, "run_id": run_id},
                artifact_refs=[{"type": "run_record", "path": str(record_path)}],
            )
        timeout = float(dict(plan.get("run_policy") or {}).get("run_timeout_seconds") or 3600.0)
        interval = float(dict(plan.get("run_policy") or {}).get("poll_interval_seconds") or 30.0)
        deadline = time.monotonic() + timeout
        status_payload: dict[str, Any] = {}
        while True:
            status_payload = self.client.request_json("GET", f"/api/bots/{bot_id}/runs/{run_id}/status")
            if not isinstance(status_payload, dict):
                raise ApiError("bot run status returned unexpected payload")
            status = _terminal_status(status_payload)
            if status in TERMINAL_RUN_STATUSES:
                record = {**record, "status": status, "terminal_status": status_payload}
                store.write_run_record(window_id, variant_id, record)
                return record
            if time.monotonic() >= deadline:
                record = {**record, "status": "timeout", "terminal_status": status_payload}
                store.write_run_record(window_id, variant_id, record)
                return record
            time.sleep(interval)

    def _run_bot_steps(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        state["status"] = "RUNNING"
        store.write_state(state)
        for step in state.get("steps") or []:
            if step.get("type") != "RUN_BOT" or step.get("status") == "COMPLETED":
                continue
            step_id = str(step.get("step_id"))
            _set_step_status(state, step_id, "RUNNING")
            store.write_state(state)
            events.append(event_type="intent", operation="run_bot_step", status="started", step_id=step_id, target=step)
            try:
                record = self._start_or_wait_run(store=store, plan=plan, state=state, events=events, step=step)
                terminal = str(record.get("status") or "")
                if terminal == "completed":
                    _set_step_status(state, step_id, "COMPLETED", artifact_refs=[{"type": "run_record", "path": str(store.run_record_path(str(step.get("window_id")), str(step.get("variant_id"))))}])
                    events.append(event_type="result", operation="run_bot_step", status="succeeded", step_id=step_id, target={"run_id": record.get("run_id"), "status": terminal})
                else:
                    error = {"error": "run_not_completed", "status": terminal}
                    _set_step_status(state, step_id, "FAILED", error=error)
                    failures.append({"step_id": step_id, **error})
                    events.append(event_type="result", operation="run_bot_step", status="failed", step_id=step_id, target={"run_id": record.get("run_id")}, error=error)
                    if bool(dict(plan.get("run_policy") or {}).get("stop_on_first_failure", False)):
                        raise RuntimeError(f"{step_id} failed with run status {terminal}")
            finally:
                store.write_state(state)
        return failures

    def _completed_record_for_step(self, store: ExperimentStateStore, step: dict[str, Any]) -> dict[str, Any] | None:
        path = store.run_record_path(str(step.get("window_id")), str(step.get("variant_id")))
        if not path.exists():
            return None
        record = json.loads(path.read_text(encoding="utf-8"))
        return record if str(record.get("status")) == "completed" else None

    def _export_report(self, store: ExperimentStateStore, run_id: str, policy: dict[str, Any]) -> dict[str, Any]:
        response = self.client.request_bytes(
            "POST",
            f"/api/reports/{run_id}/export",
            payload={
                "include_json": bool(policy.get("include_json", True)),
                "include_csv": bool(policy.get("include_csv", True)),
                "include_candles": bool(policy.get("include_candles", False)),
            },
        )
        headers = {key.lower(): value for key, value in response.headers.items()}
        filename = filename_from_content_disposition(headers.get("content-disposition"), f"run_{run_id}_report_export.zip")
        out_dir = store.artifacts_dir / "reports" / f"run_{safe_path_part(run_id)}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / filename
        path.write_bytes(response.body)
        return {"run_id": run_id, "path": str(path), "filename": filename, "size_bytes": len(response.body)}

    def _export_steps(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        state["status"] = "EXPORTING"
        policy = dict(plan.get("export_policy") or {})
        for step in state.get("steps") or []:
            if step.get("type") != "EXPORT_REPORT" or step.get("status") == "COMPLETED":
                continue
            step_id = str(step.get("step_id"))
            record = self._completed_record_for_step(store, step)
            if record is None:
                _set_step_status(state, step_id, "SKIPPED", error={"reason": "run_not_completed"})
                continue
            _set_step_status(state, step_id, "RUNNING")
            store.write_state(state)
            try:
                export_ref = self._export_report(store, str(record.get("run_id")), policy)
                record["export"] = export_ref
                store.write_run_record(str(step.get("window_id")), str(step.get("variant_id")), record)
                _set_step_status(state, step_id, "COMPLETED", artifact_refs=[{"type": "report_export", "path": export_ref["path"], "run_id": record.get("run_id")}])
                events.append(event_type="artifact_written", operation="export_report", status="succeeded", step_id=step_id, artifact_refs=[{"type": "report_export", "path": export_ref["path"]}])
            except Exception as exc:
                error = {"error": str(exc), "type": type(exc).__name__}
                _set_step_status(state, step_id, "FAILED", error=error)
                failures.append({"step_id": step_id, **error})
                events.append(event_type="result", operation="export_report", status="failed", step_id=step_id, error=error)
            store.write_state(state)
        return failures

    def _materialize_steps(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        state["status"] = "MATERIALIZING"
        policy = dict(plan.get("materialization_policy") or {})
        for step in state.get("steps") or []:
            if step.get("type") != "MATERIALIZE_REPORT" or step.get("status") == "COMPLETED":
                continue
            step_id = str(step.get("step_id"))
            record = self._completed_record_for_step(store, step)
            if record is None:
                _set_step_status(state, step_id, "SKIPPED", error={"reason": "run_not_completed"})
                continue
            _set_step_status(state, step_id, "RUNNING")
            store.write_state(state)
            try:
                payload = self.client.request_json(
                    "POST",
                    f"/api/reports/{record.get('run_id')}/run-report/build",
                    params={"async_build": False, "force_rebuild": bool(policy.get("force_rebuild", False))},
                )
                record["materialization"] = payload
                store.write_run_record(str(step.get("window_id")), str(step.get("variant_id")), record)
                _set_step_status(state, step_id, "COMPLETED")
                events.append(event_type="result", operation="materialize_report", status="succeeded", step_id=step_id, target={"run_id": record.get("run_id")})
            except Exception as exc:
                error = {"error": str(exc), "type": type(exc).__name__}
                _set_step_status(state, step_id, "FAILED", error=error)
                failures.append({"step_id": step_id, **error})
                events.append(event_type="result", operation="materialize_report", status="failed", step_id=step_id, error=error)
            store.write_state(state)
        return failures

    def _summary_steps(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        for step in state.get("steps") or []:
            if step.get("type") != "FETCH_SUMMARY" or step.get("status") == "COMPLETED":
                continue
            step_id = str(step.get("step_id"))
            record = self._completed_record_for_step(store, step)
            if record is None:
                _set_step_status(state, step_id, "SKIPPED", error={"reason": "run_not_completed"})
                continue
            _set_step_status(state, step_id, "RUNNING")
            store.write_state(state)
            try:
                summary = self.client.request_json("GET", f"/api/reports/{record.get('run_id')}/research-summary")
                path = store.artifacts_dir / "summaries" / f"{safe_path_part(str(step.get('window_id')))}__{safe_path_part(str(step.get('variant_id')))}__research-summary.json"
                _write_json(path, summary)
                record["research_summary"] = {"path": str(path), "run_id": record.get("run_id")}
                store.write_run_record(str(step.get("window_id")), str(step.get("variant_id")), record)
                _set_step_status(state, step_id, "COMPLETED", artifact_refs=[{"type": "research_summary", "path": str(path), "run_id": record.get("run_id")}])
                events.append(event_type="artifact_written", operation="fetch_research_summary", status="succeeded", step_id=step_id, artifact_refs=[{"type": "research_summary", "path": str(path)}])
            except Exception as exc:
                error = {"error": str(exc), "type": type(exc).__name__}
                _set_step_status(state, step_id, "FAILED", error=error)
                failures.append({"step_id": step_id, **error})
                events.append(event_type="result", operation="fetch_research_summary", status="failed", step_id=step_id, error=error)
            store.write_state(state)
        return failures

    def _comparison_by_id(self, plan: dict[str, Any], comparison_id: str) -> dict[str, Any]:
        for comparison in plan.get("comparisons") or []:
            if str(comparison.get("id")) == comparison_id:
                return dict(comparison)
        raise ValueError(f"comparison not found: {comparison_id}")

    def _compare_steps(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> list[dict[str, Any]]:
        failures: list[dict[str, Any]] = []
        state["status"] = "COMPARING"
        policy = dict(plan.get("comparison_policy") or {})
        for step in state.get("steps") or []:
            if step.get("type") != "COMPARE_REPORTS" or step.get("status") == "COMPLETED":
                continue
            step_id = str(step.get("step_id"))
            comparison = self._comparison_by_id(plan, str(step.get("comparison_id")))
            window_id = str(step.get("window_id"))
            baseline = store.load_run_records().get((window_id, str(comparison.get("baseline_variant_id"))))
            candidate = store.load_run_records().get((window_id, str(comparison.get("candidate_variant_id"))))
            if not baseline or not candidate or baseline.get("status") != "completed" or candidate.get("status") != "completed":
                _set_step_status(state, step_id, "SKIPPED", error={"reason": "comparison_run_missing_or_not_completed"})
                continue
            _set_step_status(state, step_id, "RUNNING")
            store.write_state(state)
            try:
                summary = self.client.request_json(
                    "GET",
                    "/api/reports/compare/summary",
                    params={
                        "left_run_id": baseline.get("run_id"),
                        "right_run_id": candidate.get("run_id"),
                        "include_golden": bool(policy.get("include_golden", True)),
                        "require_golden": bool(policy.get("require_golden", False)),
                    },
                )
                path = store.artifacts_dir / "comparisons" / f"{safe_path_part(window_id)}__{safe_path_part(str(comparison.get('id')))}.json"
                _write_json(path, summary)
                ref = {
                    "schema_version": COMPARISON_REF_SCHEMA,
                    "window_id": window_id,
                    "baseline_variant_id": comparison.get("baseline_variant_id"),
                    "candidate_variant_id": comparison.get("candidate_variant_id"),
                    "baseline_run_id": baseline.get("run_id"),
                    "candidate_run_id": candidate.get("run_id"),
                    "summary_path": str(path),
                    "status": "COMPLETED",
                }
                state.setdefault("comparison_refs", []).append(ref)
                _set_step_status(state, step_id, "COMPLETED", artifact_refs=[{"type": "comparison_summary", "path": str(path)}])
                events.append(event_type="artifact_written", operation="compare_reports", status="succeeded", step_id=step_id, artifact_refs=[{"type": "comparison_summary", "path": str(path)}])
            except Exception as exc:
                error = {"error": str(exc), "type": type(exc).__name__}
                _set_step_status(state, step_id, "FAILED", error=error)
                failures.append({"step_id": step_id, **error})
                events.append(event_type="result", operation="compare_reports", status="failed", step_id=step_id, error=error)
            store.write_state(state)
        return failures

    def _evaluate_gates(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> None:
        state["status"] = "EVALUATING_GATES"
        step_id = "evaluate_gates"
        step = _step_by_id(state, step_id)
        if step.get("status") == "COMPLETED":
            return
        _set_step_status(state, step_id, "RUNNING")
        store.write_state(state)
        summaries: dict[tuple[str, str], dict[str, Any]] = {}
        for key, record in store.load_run_records().items():
            ref = record.get("research_summary")
            path = ref.get("path") if isinstance(ref, dict) else None
            if path and Path(path).exists():
                summaries[key] = json.loads(Path(path).read_text(encoding="utf-8"))
        result = evaluate_pass_gates(plan=plan, summaries=summaries, comparison_refs=list(state.get("comparison_refs") or []))
        path = store.artifacts_dir / "summaries" / "pass_gate_result.json"
        _write_json(path, result)
        state["pass_gate_result_ref"] = str(path)
        _set_step_status(state, step_id, "COMPLETED", artifact_refs=[{"type": "pass_gate_result", "path": str(path)}])
        events.append(event_type="artifact_written", operation="evaluate_pass_gates", status="succeeded", step_id=step_id, artifact_refs=[{"type": "pass_gate_result", "path": str(path)}])
        store.write_state(state)

    def _load_gate_status(self, store: ExperimentStateStore, state: dict[str, Any]) -> str:
        ref = state.get("pass_gate_result_ref")
        if not ref or not Path(str(ref)).exists():
            return "FAILED"
        payload = json.loads(Path(str(ref)).read_text(encoding="utf-8"))
        return str(payload.get("status") or "FAILED")

    def _notify(self, store: ExperimentStateStore, plan: dict[str, Any], state: dict[str, Any], events: ExperimentEventLog) -> None:
        state["status"] = str(state.get("status") or "FAILED")
        step_id = "notify"
        if any(step.get("step_id") == step_id and step.get("status") == "COMPLETED" for step in state.get("steps") or []):
            return
        _set_step_status(state, step_id, "RUNNING")
        store.write_state(state)
        result = notify_terminal_state(
            notifications_path=store.notifications_path,
            state=state,
            policy=dict(plan.get("notification_policy") or {}),
        )
        state["notification_status"] = result
        _set_step_status(state, step_id, "COMPLETED", artifact_refs=[{"type": "notifications", "path": str(store.notifications_path)}])
        events.append(event_type="result", operation="notify_terminal_state", status="succeeded", step_id=step_id, target=result)


import logging
import os
from typing import Mapping

from utils.log_context import build_log_context, format_log_context


class RuntimeContextFormatter(logging.Formatter):
    """Append process-level runtime context to every emitted log line."""

    def __init__(self, fmt: str, *, context: Mapping[str, object] | None = None) -> None:
        super().__init__(fmt)
        self._rendered_context = format_log_context(context or {})

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        if not self._rendered_context:
            return line
        return f"{line} | {self._rendered_context}"


def runtime_log_context_from_env(env: Mapping[str, str] | None = None) -> dict[str, object]:
    source = os.environ if env is None else env
    request_id = str(source.get("QT_BOT_RUNTIME_REQUEST_ID") or source.get("QT_REQUEST_ID") or "").strip()
    bot_id = str(source.get("QT_BOT_RUNTIME_BOT_ID") or "").strip()
    run_id = str(source.get("QT_BOT_RUNTIME_RUN_ID") or "").strip()
    source_revision = str(source.get("SOURCE_REVISION") or "").strip()

    context = build_log_context(request_id=request_id, bot_id=bot_id, run_id=run_id)
    if bot_id or run_id:
        context["runtime"] = "bot"
        context["service"] = "bot-runtime"
    if source_revision and (bot_id or run_id):
        context["source_revision"] = source_revision
    return context


__all__ = ["RuntimeContextFormatter", "runtime_log_context_from_env"]

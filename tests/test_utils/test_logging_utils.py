from __future__ import annotations

import logging

from utils.logging_utils import RuntimeContextFormatter, runtime_log_context_from_env


def test_runtime_log_context_from_env_uses_bot_runtime_ids():
    context = runtime_log_context_from_env(
        {
            "QT_BOT_RUNTIME_BOT_ID": "bot-1",
            "QT_BOT_RUNTIME_RUN_ID": "run-1",
            "QT_BOT_RUNTIME_REQUEST_ID": "req-1",
            "SOURCE_REVISION": "abc123",
        }
    )

    assert context == {
        "request_id": "req-1",
        "bot_id": "bot-1",
        "run_id": "run-1",
        "runtime": "bot",
        "service": "bot-runtime",
        "source_revision": "abc123",
    }


def test_runtime_log_context_does_not_attach_backend_revision_without_runtime_ids():
    assert runtime_log_context_from_env({"SOURCE_REVISION": "abc123"}) == {}


def test_runtime_context_formatter_appends_context_once():
    formatter = RuntimeContextFormatter(
        "%(levelname)s | %(message)s",
        context={"bot_id": "bot-1", "run_id": "run-1", "service": "bot-runtime"},
    )
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="bot_runtime_started",
        args=(),
        exc_info=None,
    )

    assert formatter.format(record) == (
        "INFO | bot_runtime_started | bot_id=bot-1 | run_id=run-1 | service=bot-runtime"
    )

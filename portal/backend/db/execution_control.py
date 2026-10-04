"""Interrupt only SQL owned by the current controlled execution."""
from __future__ import annotations
import math

from core.execution_control import current_execution_control, execution_checkpoint
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


@event.listens_for(Engine, "before_cursor_execute")
def _before_statement(connection, cursor, statement, parameters, context, executemany):
    control = current_execution_control()
    if control is None:
        return
    control.check()
    cancel = getattr(cursor.connection, "cancel", None)
    if cancel is None:
        raise RuntimeError("controlled_execution_requires_interruptible_database_driver")
    control.register(context, cancel)
    context._qt_execution_control = control
    remaining = control.remaining_seconds()
    if remaining is not None:
        # Transaction-local and no longer than the remaining total budget.
        # The normal driver query timeout still applies outside controlled work.
        milliseconds = max(1, math.ceil(remaining * 1000))
        # Streaming SELECTs use a named cursor, which may execute only once.
        # Configure its connection using a separate, short-lived normal cursor.
        with cursor.connection.cursor() as settings_cursor:
            settings_cursor.execute(
                "SELECT set_config('statement_timeout', LEAST(%s, "
                "CASE WHEN setting::bigint = 0 THEN %s ELSE setting::bigint END)::text, true) "
                "FROM pg_settings WHERE name = 'statement_timeout'",
                (milliseconds, milliseconds),
            )


def _release(context) -> None:
    control = getattr(context, "_qt_execution_control", None)
    if control is not None:
        control.unregister(context)
        context._qt_execution_control = None


@event.listens_for(Engine, "after_cursor_execute")
def _after_statement(connection, cursor, statement, parameters, context, executemany):
    _release(context)


@event.listens_for(Engine, "handle_error")
def _statement_error(exception_context):
    _release(exception_context.execution_context)
    control = current_execution_control()
    if control is not None:
        control.check()


@event.listens_for(Session, "before_commit")
def _before_commit(session):
    # Fail while rollback is still possible, including transactions whose last
    # SQL returned just before cancellation or deadline exhaustion.
    execution_checkpoint()

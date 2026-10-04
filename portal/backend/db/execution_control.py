"""Interrupt only SQL owned by the current controlled execution."""
from __future__ import annotations

from core.execution_control import current_execution_control
from sqlalchemy import event
from sqlalchemy.engine import Engine


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

"""Postgres extension bootstrapping and health checks for startup."""

from __future__ import annotations

import logging
import time
from typing import Dict, Iterable, Optional, Set, Tuple

from core.settings import get_settings
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
_DATABASE_SETTINGS = get_settings().database

_EXTENSIONS = ("pg_stat_statements", "pg_buffercache", "system_stats")


def _build_engine(dsn: str, timeout_s: int) -> object:
    return create_engine(
        dsn,
        pool_pre_ping=True,
        connect_args={"connect_timeout": timeout_s},
        future=True,
    )


def _load_settings(conn) -> Tuple[str, str, str]:
    preload = conn.execute(text("SHOW shared_preload_libraries")).scalar() or ""
    logging_collector = conn.execute(text("SHOW logging_collector")).scalar() or ""
    log_directory = conn.execute(text("SHOW log_directory")).scalar() or ""
    return str(preload), str(logging_collector), str(log_directory)


def _load_extensions(conn) -> Set[str]:
    rows = conn.execute(text("SELECT extname FROM pg_extension")).fetchall()
    return {row[0] for row in rows}


def _safe_create_extension(conn, name: str) -> Optional[str]:
    try:
        conn.execute(text(f"CREATE EXTENSION IF NOT EXISTS {name}"))
        return None
    except SQLAlchemyError as exc:
        return str(exc)


def _apply_extensions(conn, extensions: Iterable[str]) -> Dict[str, str]:
    results: Dict[str, str] = {}
    for ext in extensions:
        error = _safe_create_extension(conn, ext)
        if error:
            results[ext] = error
        else:
            results[ext] = "enabled"
    return results


def ensure_postgres_extensions(*, timeout_s: int = 2, retries: int = 1) -> None:
    dsn = _DATABASE_SETTINGS.dsn
    if not dsn:
        logger.warning("postgres_extensions_skipped | reason=missing_pg_dsn")
        return

    attempt = 0
    while attempt <= retries:
        attempt += 1
        try:
            engine = _build_engine(dsn, timeout_s)
            with engine.begin() as conn:
                create_results = _apply_extensions(conn, _EXTENSIONS)
                preload, logging_collector, log_directory = _load_settings(conn)
                installed = _load_extensions(conn)
            _log_extension_results(create_results)
            _log_readiness(
                installed=installed,
                preload=preload,
                logging_collector=logging_collector,
                log_directory=log_directory,
            )
            return
        except SQLAlchemyError as exc:
            logger.warning(
                "postgres_extensions_failed | attempt=%s error=%s",
                attempt,
                exc,
            )
            if attempt > retries:
                return
            time.sleep(1)


def _log_extension_results(results: Dict[str, str]) -> None:
    for name, outcome in results.items():
        if outcome == "enabled":
            logger.info("postgres_extension_enabled | name=%s", name)
            continue
        if name == "system_stats":
            logger.warning(
                "postgres_extension_unavailable | name=%s error=%s",
                name,
                outcome,
            )
            continue
        if name == "pg_stat_statements" and "shared_preload_libraries" in outcome:
            logger.warning(
                "postgres_extension_requires_preload | name=%s error=%s",
                name,
                outcome,
            )
            continue
        logger.warning("postgres_extension_failed | name=%s error=%s", name, outcome)


def _log_readiness(
    *,
    installed: Set[str],
    preload: str,
    logging_collector: str,
    log_directory: str,
) -> None:
    logger.info(
        "postgres_extension_readiness | pg_stat_statements=%s system_stats=%s pg_buffercache=%s "
        "logging_collector=%s log_directory=%s shared_preload_libraries=%s",
        "enabled" if "pg_stat_statements" in installed else "disabled",
        "enabled" if "system_stats" in installed else "disabled",
        "enabled" if "pg_buffercache" in installed else "disabled",
        logging_collector,
        log_directory,
        preload,
    )

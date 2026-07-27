from __future__ import annotations

import pytest
from sqlalchemy import text

from core.settings import get_settings
from portal.backend.service.db import postgres_extensions


pytestmark = pytest.mark.db


def test_optional_extension_failure_does_not_abort_readiness_transaction() -> None:
    dsn = get_settings().database.dsn
    assert dsn
    engine = postgres_extensions._build_engine(dsn, timeout_s=2)
    try:
        with engine.begin() as conn:
            error = postgres_extensions._safe_create_extension(
                conn,
                "quant_trad_intentionally_missing_extension",
            )

            assert error is not None
            assert conn.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        engine.dispose()

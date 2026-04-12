from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import connect
from psycopg.sql import SQL, Identifier

from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.settings import settings


@pytest.fixture(autouse=True)
def restore_api_dependency_overrides() -> None:
    original_overrides = dict(app.dependency_overrides)
    original_data_dir = settings.app_data_dir
    original_sqlite_path = settings.app_sqlite_path
    original_state_backend = settings.app_state_backend
    original_truth_backend = settings.app_truth_backend
    original_postgres_dsn = settings.app_postgres_dsn
    original_postgres_schema = settings.app_postgres_schema
    try:
        yield
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(original_overrides)
        settings.app_data_dir = original_data_dir
        settings.app_sqlite_path = original_sqlite_path
        settings.app_state_backend = original_state_backend
        settings.app_truth_backend = original_truth_backend
        settings.app_postgres_dsn = original_postgres_dsn
        settings.app_postgres_schema = original_postgres_schema


@pytest.fixture
def temp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(settings, "app_data_dir", tmp_path)
    monkeypatch.setattr(settings, "app_sqlite_path", tmp_path / "sourcebound.db")
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    return tmp_path


@pytest.fixture
def postgres_app_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str | Path]:
    dsn = settings.app_postgres_dsn
    try:
        with connect(dsn, autocommit=True) as connection:
            connection.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"Postgres unavailable at {dsn}: {exc}")

    schema = f"test_sourcebound_{uuid4().hex[:12]}"
    monkeypatch.setattr(settings, "app_data_dir", tmp_path)
    monkeypatch.setattr(settings, "app_sqlite_path", tmp_path / "sourcebound.db")
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "app_postgres_schema", schema)

    try:
        yield {"dsn": dsn, "schema": schema, "data_dir": tmp_path}
    finally:
        with connect(dsn, autocommit=True) as connection:
            connection.execute(
                SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema))
            )

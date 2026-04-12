from __future__ import annotations

from pathlib import Path

import pytest

from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.settings import settings


@pytest.fixture(autouse=True)
def restore_api_dependency_overrides() -> None:
    original_overrides = dict(app.dependency_overrides)
    original_data_dir = settings.app_data_dir
    original_sqlite_path = settings.app_sqlite_path
    original_state_backend = settings.app_state_backend
    original_truth_backend = settings.app_truth_backend
    try:
        yield
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(original_overrides)
        settings.app_data_dir = original_data_dir
        settings.app_sqlite_path = original_sqlite_path
        settings.app_state_backend = original_state_backend
        settings.app_truth_backend = original_truth_backend


@pytest.fixture
def temp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(settings, "app_data_dir", tmp_path)
    monkeypatch.setattr(settings, "app_sqlite_path", tmp_path / "sourcebound.db")
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    return tmp_path

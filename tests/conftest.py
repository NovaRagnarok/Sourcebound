from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import connect
from psycopg.sql import SQL, Identifier
from qdrant_client import QdrantClient

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
    original_strict_startup_checks = settings.app_strict_startup_checks
    original_wikibase_base_url = settings.wikibase_base_url
    original_wikibase_api_url = settings.wikibase_api_url
    original_wikibase_username = settings.wikibase_username
    original_wikibase_password = settings.wikibase_password
    original_wikibase_property_map = settings.wikibase_property_map
    original_qdrant_enabled = settings.qdrant_enabled
    original_qdrant_collection = settings.qdrant_collection
    original_graph_rag_enabled = settings.graph_rag_enabled
    original_graph_rag_mode = settings.graph_rag_mode
    original_graph_rag_root = settings.graph_rag_root
    original_graph_rag_artifacts_dir = settings.graph_rag_artifacts_dir
    original_zotero_library_type = settings.zotero_library_type
    original_zotero_library_id = settings.zotero_library_id
    original_zotero_collection_key = settings.zotero_collection_key
    original_zotero_api_key = settings.zotero_api_key
    original_zotero_base_url = settings.zotero_base_url
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
        settings.app_strict_startup_checks = original_strict_startup_checks
        settings.wikibase_base_url = original_wikibase_base_url
        settings.wikibase_api_url = original_wikibase_api_url
        settings.wikibase_username = original_wikibase_username
        settings.wikibase_password = original_wikibase_password
        settings.wikibase_property_map = original_wikibase_property_map
        settings.qdrant_enabled = original_qdrant_enabled
        settings.qdrant_collection = original_qdrant_collection
        settings.graph_rag_enabled = original_graph_rag_enabled
        settings.graph_rag_mode = original_graph_rag_mode
        settings.graph_rag_root = original_graph_rag_root
        settings.graph_rag_artifacts_dir = original_graph_rag_artifacts_dir
        settings.zotero_library_type = original_zotero_library_type
        settings.zotero_library_id = original_zotero_library_id
        settings.zotero_collection_key = original_zotero_collection_key
        settings.zotero_api_key = original_zotero_api_key
        settings.zotero_base_url = original_zotero_base_url


@pytest.fixture
def temp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(settings, "app_data_dir", tmp_path)
    monkeypatch.setattr(settings, "app_sqlite_path", tmp_path / "sourcebound.db")
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "wikibase_base_url", None)
    monkeypatch.setattr(settings, "wikibase_api_url", None)
    monkeypatch.setattr(settings, "wikibase_username", None)
    monkeypatch.setattr(settings, "wikibase_password", None)
    monkeypatch.setattr(settings, "wikibase_property_map", None)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "zotero_collection_key", None)
    monkeypatch.setattr(settings, "zotero_api_key", None)
    return tmp_path


@pytest.fixture
def live_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(settings, "app_data_dir", tmp_path)
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
    monkeypatch.setattr(settings, "wikibase_base_url", None)
    monkeypatch.setattr(settings, "wikibase_api_url", None)
    monkeypatch.setattr(settings, "wikibase_username", None)
    monkeypatch.setattr(settings, "wikibase_password", None)
    monkeypatch.setattr(settings, "wikibase_property_map", None)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "zotero_collection_key", None)
    monkeypatch.setattr(settings, "zotero_api_key", None)

    try:
        yield {"dsn": dsn, "schema": schema, "data_dir": tmp_path}
    finally:
        with connect(dsn, autocommit=True) as connection:
            connection.execute(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema)))


@pytest.fixture
def require_live_zotero() -> None:
    if not settings.zotero_library_id:
        pytest.skip("Live Zotero test requires ZOTERO_LIBRARY_ID.")


@pytest.fixture
def require_live_wikibase(live_cache_dir: Path) -> Path:
    required = {
        "WIKIBASE_API_URL": settings.wikibase_api_url,
        "WIKIBASE_USERNAME": settings.wikibase_username,
        "WIKIBASE_PASSWORD": settings.wikibase_password,
        "WIKIBASE_PROPERTY_MAP": settings.wikibase_property_map,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        pytest.skip(f"Live Wikibase test requires {', '.join(missing)}.")
    return live_cache_dir


@pytest.fixture
def live_qdrant_collection(monkeypatch: pytest.MonkeyPatch) -> str:
    if not settings.qdrant_url:
        pytest.skip("Live Qdrant test requires QDRANT_URL.")

    collection = f"sourcebound_live_test_{uuid4().hex[:12]}"
    client = QdrantClient(url=settings.qdrant_url, check_compatibility=False)
    try:
        client.get_collections()
    except Exception as exc:
        pytest.skip(f"Qdrant unavailable at {settings.qdrant_url}: {exc}")

    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "qdrant_collection", collection)

    try:
        yield collection
    finally:
        try:
            if client.collection_exists(collection):
                client.delete_collection(collection)
        except Exception:
            pass

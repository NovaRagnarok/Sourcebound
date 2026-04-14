from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="source-aware-worldbuilding", alias="APP_NAME")
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    app_state_backend: str = Field(default="postgres", alias="APP_STATE_BACKEND")
    app_truth_backend: Literal["file", "postgres", "wikibase"] = Field(
        default="postgres",
        alias="APP_TRUTH_BACKEND",
    )
    app_data_dir: Path = Field(default=Path("data/dev"), alias="APP_DATA_DIR")
    app_sqlite_path: Path = Field(default=Path("runtime/sourcebound.db"), alias="APP_SQLITE_PATH")
    app_postgres_dsn: str = Field(
        default="postgresql://saw:saw@localhost:5432/saw",
        alias="APP_POSTGRES_DSN",
    )
    app_postgres_schema: str = Field(default="sourcebound", alias="APP_POSTGRES_SCHEMA")
    app_ui_enabled: bool = Field(default=True, alias="APP_UI_ENABLED")

    zotero_library_type: str = Field(default="user", alias="ZOTERO_LIBRARY_TYPE")
    zotero_library_id: str | None = Field(default=None, alias="ZOTERO_LIBRARY_ID")
    zotero_collection_key: str | None = Field(default=None, alias="ZOTERO_COLLECTION_KEY")
    zotero_api_key: str | None = Field(default=None, alias="ZOTERO_API_KEY")
    zotero_base_url: str = Field(default="https://api.zotero.org", alias="ZOTERO_BASE_URL")

    graph_rag_root: Path = Field(default=Path("runtime/graphrag"), alias="GRAPH_RAG_ROOT")
    graph_rag_enabled: bool = Field(default=True, alias="GRAPH_RAG_ENABLED")
    graph_rag_mode: Literal["in_process", "artifact_import"] = Field(
        default="in_process",
        alias="GRAPH_RAG_MODE",
    )
    graph_rag_artifacts_dir: Path | None = Field(
        default=None,
        alias="GRAPH_RAG_ARTIFACTS_DIR",
    )
    llm_api_key: str | None = Field(default=None, alias="LLM_API_KEY")

    wikibase_base_url: str | None = Field(default=None, alias="WIKIBASE_BASE_URL")
    wikibase_api_url: str | None = Field(default=None, alias="WIKIBASE_API_URL")
    wikibase_username: str | None = Field(default=None, alias="WIKIBASE_USERNAME")
    wikibase_password: str | None = Field(default=None, alias="WIKIBASE_PASSWORD")
    wikibase_property_map: str | None = Field(default=None, alias="WIKIBASE_PROPERTY_MAP")

    qdrant_url: str = Field(default="http://localhost:6333", alias="QDRANT_URL")
    qdrant_collection: str = Field(default="approved_claims", alias="QDRANT_COLLECTION")
    qdrant_enabled: bool = Field(default=True, alias="QDRANT_ENABLED")


settings = Settings()

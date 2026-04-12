from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="source-aware-worldbuilding", alias="APP_NAME")
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    app_truth_backend: str = Field(default="file", alias="APP_TRUTH_BACKEND")
    app_data_dir: Path = Field(default=Path("data/dev"), alias="APP_DATA_DIR")

    zotero_library_type: str = Field(default="user", alias="ZOTERO_LIBRARY_TYPE")
    zotero_library_id: str | None = Field(default=None, alias="ZOTERO_LIBRARY_ID")
    zotero_api_key: str | None = Field(default=None, alias="ZOTERO_API_KEY")

    graph_rag_root: Path = Field(default=Path("runtime/graphrag"), alias="GRAPH_RAG_ROOT")
    graph_rag_enabled: bool = Field(default=False, alias="GRAPH_RAG_ENABLED")
    llm_api_key: str | None = Field(default=None, alias="LLM_API_KEY")

    wikibase_base_url: str | None = Field(default=None, alias="WIKIBASE_BASE_URL")
    wikibase_username: str | None = Field(default=None, alias="WIKIBASE_USERNAME")
    wikibase_password: str | None = Field(default=None, alias="WIKIBASE_PASSWORD")

    qdrant_url: str = Field(default="http://localhost:6333", alias="QDRANT_URL")
    qdrant_collection: str = Field(default="approved_claims", alias="QDRANT_COLLECTION")


settings = Settings()

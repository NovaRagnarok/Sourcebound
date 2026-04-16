from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True, slots=True)
class StartupValidationIssue:
    summary: str
    fix: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="source-aware-worldbuilding", alias="APP_NAME")
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    app_state_backend: Literal["file", "sqlite", "postgres"] = Field(
        default="postgres",
        alias="APP_STATE_BACKEND",
    )
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
    app_operator_token: str | None = Field(default=None, alias="APP_OPERATOR_TOKEN")
    app_strict_startup_checks: bool = Field(
        default=False,
        alias="APP_STRICT_STARTUP_CHECKS",
    )
    app_job_worker_enabled: bool = Field(default=True, alias="APP_JOB_WORKER_ENABLED")
    app_job_poll_interval_seconds: float = Field(
        default=0.25,
        alias="APP_JOB_POLL_INTERVAL_SECONDS",
    )
    app_job_stale_timeout_seconds: float = Field(
        default=15.0,
        alias="APP_JOB_STALE_TIMEOUT_SECONDS",
    )
    app_allow_queued_jobs_without_worker: bool = Field(
        default=False,
        alias="APP_ALLOW_QUEUED_JOBS_WITHOUT_WORKER",
    )
    app_research_default_adapter_id: str = Field(
        default="web_open",
        alias="APP_RESEARCH_DEFAULT_ADAPTER_ID",
    )
    app_research_total_fetch_time_seconds: int = Field(
        default=90,
        alias="APP_RESEARCH_TOTAL_FETCH_TIME_SECONDS",
    )
    app_research_per_host_fetch_cap: int = Field(
        default=3,
        alias="APP_RESEARCH_PER_HOST_FETCH_CAP",
    )
    app_research_retry_attempts: int = Field(
        default=3,
        alias="APP_RESEARCH_RETRY_ATTEMPTS",
    )
    app_research_retry_backoff_base_ms: int = Field(
        default=250,
        alias="APP_RESEARCH_RETRY_BACKOFF_BASE_MS",
    )
    app_research_retry_backoff_max_ms: int = Field(
        default=2000,
        alias="APP_RESEARCH_RETRY_BACKOFF_MAX_MS",
    )
    app_research_respect_robots: bool = Field(
        default=True,
        alias="APP_RESEARCH_RESPECT_ROBOTS",
    )
    app_research_user_agent: str = Field(
        default="SourceboundResearchScout/0.2 (+https://github.com/NovaRagnarok/Sourcebound)",
        alias="APP_RESEARCH_USER_AGENT",
    )
    app_research_search_providers: str = Field(
        default="",
        alias="APP_RESEARCH_SEARCH_PROVIDERS",
    )
    brave_search_api_key: str | None = Field(
        default=None,
        alias="BRAVE_SEARCH_API_KEY",
    )
    brave_search_base_url: str = Field(
        default="https://api.search.brave.com",
        alias="BRAVE_SEARCH_BASE_URL",
    )

    zotero_library_type: Literal["user", "group"] = Field(
        default="user",
        alias="ZOTERO_LIBRARY_TYPE",
    )
    zotero_library_id: str | None = Field(default=None, alias="ZOTERO_LIBRARY_ID")
    zotero_collection_key: str | None = Field(default=None, alias="ZOTERO_COLLECTION_KEY")
    zotero_api_key: str | None = Field(default=None, alias="ZOTERO_API_KEY")
    zotero_base_url: str = Field(default="https://api.zotero.org", alias="ZOTERO_BASE_URL")

    graph_rag_root: Path = Field(default=Path("runtime/graphrag"), alias="GRAPH_RAG_ROOT")
    graph_rag_enabled: bool = Field(default=False, alias="GRAPH_RAG_ENABLED")
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
    research_semantic_enabled: bool = Field(
        default=False,
        alias="RESEARCH_SEMANTIC_ENABLED",
    )
    research_qdrant_collection: str = Field(
        default="research_findings",
        alias="RESEARCH_QDRANT_COLLECTION",
    )
    research_semantic_duplicate_threshold: float = Field(
        default=0.9,
        alias="RESEARCH_SEMANTIC_DUPLICATE_THRESHOLD",
    )
    research_semantic_novelty_floor: float = Field(
        default=0.1,
        alias="RESEARCH_SEMANTIC_NOVELTY_FLOOR",
    )
    research_semantic_rerank_weight: float = Field(
        default=0.05,
        alias="RESEARCH_SEMANTIC_RERANK_WEIGHT",
    )

    @property
    def postgres_enabled(self) -> bool:
        return self.app_state_backend == "postgres" or self.app_truth_backend == "postgres"

    @property
    def qdrant_features_enabled(self) -> bool:
        return self.qdrant_enabled or self.research_semantic_enabled

    def startup_validation_issues(self) -> list[StartupValidationIssue]:
        issues: list[StartupValidationIssue] = []

        if self.postgres_enabled and not self.app_postgres_dsn.strip():
            issues.append(
                StartupValidationIssue(
                    summary=(
                        "Postgres is selected for app state or the truth store, "
                        "but APP_POSTGRES_DSN is empty."
                    ),
                    fix=(
                        "Run `cp .env.example .env`, or set "
                        "`APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw`."
                    ),
                )
            )

        if self.app_truth_backend == "wikibase":
            missing = [
                name
                for name, value in (
                    ("WIKIBASE_API_URL", self.wikibase_api_url),
                    ("WIKIBASE_USERNAME", self.wikibase_username),
                    ("WIKIBASE_PASSWORD", self.wikibase_password),
                    ("WIKIBASE_PROPERTY_MAP", self.wikibase_property_map),
                )
                if not value
            ]
            if missing:
                issues.append(
                    StartupValidationIssue(
                        summary=(
                            "APP_TRUTH_BACKEND=wikibase is enabled, but required Wikibase "
                            f"settings are missing: {', '.join(missing)}."
                        ),
                        fix=(
                            "Set those variables, or switch back to "
                            "`APP_TRUTH_BACKEND=postgres` or `APP_TRUTH_BACKEND=file` "
                            "for local startup."
                        ),
                    )
                )

        if self.qdrant_features_enabled and not self.qdrant_url.strip():
            issues.append(
                StartupValidationIssue(
                    summary=("Qdrant-backed features are enabled, but QDRANT_URL is empty."),
                    fix=(
                        "Set `QDRANT_URL=http://localhost:6333`, or disable "
                        "`QDRANT_ENABLED` and `RESEARCH_SEMANTIC_ENABLED` only if you "
                        "intentionally want a non-default degraded local path."
                    ),
                )
            )

        return issues


settings = Settings()

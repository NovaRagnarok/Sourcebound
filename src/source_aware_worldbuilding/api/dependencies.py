from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileResearchFindingStore,
    FileResearchProgramStore,
    FileResearchRunStore,
    FileReviewStore,
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.heuristic_extraction import (
    HeuristicExtractionAdapter,
)
from source_aware_worldbuilding.adapters.postgres_backed import (
    PostgresCandidateStore,
    PostgresEvidenceStore,
    PostgresExtractionRunStore,
    PostgresResearchFindingStore,
    PostgresResearchProgramStore,
    PostgresResearchRunStore,
    PostgresReviewStore,
    PostgresSourceDocumentStore,
    PostgresSourceStore,
    PostgresTextUnitStore,
    PostgresTruthStore,
)
from source_aware_worldbuilding.adapters.qdrant_adapter import (
    QdrantProjectionAdapter,
    QdrantResearchSemanticAdapter,
)
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteResearchFindingStore,
    SqliteResearchProgramStore,
    SqliteResearchRunStore,
    SqliteReviewStore,
    SqliteSourceDocumentStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
)
from source_aware_worldbuilding.adapters.web_research_scout import (
    CuratedInputsResearchScout,
    ResearchScoutRegistry,
    WebOpenResearchScout,
)
from source_aware_worldbuilding.adapters.wikibase_adapter import WikibaseTruthStore
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.intake import IntakeService
from source_aware_worldbuilding.services.lore_packet import LorePacketService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.query import QueryService
from source_aware_worldbuilding.services.research import ResearchService
from source_aware_worldbuilding.services.review import ReviewService
from source_aware_worldbuilding.settings import settings
from source_aware_worldbuilding.domain.models import ResearchExecutionPolicy


def _sqlite_path() -> Path:
    return Path(settings.app_sqlite_path)


def _postgres_args() -> tuple[str, str]:
    return settings.app_postgres_dsn, settings.app_postgres_schema


def _default_research_program_markdown() -> str:
    path = Path(__file__).resolve().parents[3] / "docs" / "research" / "default_program.md"
    return path.read_text(encoding="utf-8")


def _default_research_execution_policy() -> ResearchExecutionPolicy:
    return ResearchExecutionPolicy(
        total_fetch_time_seconds=settings.app_research_total_fetch_time_seconds,
        per_host_fetch_cap=settings.app_research_per_host_fetch_cap,
        retry_attempts=settings.app_research_retry_attempts,
        retry_backoff_base_ms=settings.app_research_retry_backoff_base_ms,
        retry_backoff_max_ms=settings.app_research_retry_backoff_max_ms,
        respect_robots=settings.app_research_respect_robots,
    )


def get_source_store():
    if settings.app_state_backend == "postgres":
        return PostgresSourceStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteSourceStore(_sqlite_path())
    return FileSourceStore(settings.app_data_dir)


def get_text_unit_store():
    if settings.app_state_backend == "postgres":
        return PostgresTextUnitStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteTextUnitStore(_sqlite_path())
    return FileTextUnitStore(settings.app_data_dir)


def get_source_document_store():
    if settings.app_state_backend == "postgres":
        return PostgresSourceDocumentStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteSourceDocumentStore(_sqlite_path())
    return FileSourceDocumentStore(settings.app_data_dir)


def get_run_store():
    if settings.app_state_backend == "postgres":
        return PostgresExtractionRunStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteExtractionRunStore(_sqlite_path())
    return FileExtractionRunStore(settings.app_data_dir)


def get_candidate_store():
    if settings.app_state_backend == "postgres":
        return PostgresCandidateStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteCandidateStore(_sqlite_path())
    return FileCandidateStore(settings.app_data_dir)


def get_evidence_store():
    if settings.app_state_backend == "postgres":
        return PostgresEvidenceStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteEvidenceStore(_sqlite_path())
    return FileEvidenceStore(settings.app_data_dir)


def get_review_store():
    if settings.app_state_backend == "postgres":
        return PostgresReviewStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteReviewStore(_sqlite_path())
    return FileReviewStore(settings.app_data_dir)


def get_research_run_store():
    if settings.app_state_backend == "postgres":
        return PostgresResearchRunStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteResearchRunStore(_sqlite_path())
    return FileResearchRunStore(settings.app_data_dir)


def get_research_finding_store():
    if settings.app_state_backend == "postgres":
        return PostgresResearchFindingStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteResearchFindingStore(_sqlite_path())
    return FileResearchFindingStore(settings.app_data_dir)


def get_research_program_store():
    if settings.app_state_backend == "postgres":
        return PostgresResearchProgramStore(*_postgres_args())
    if settings.app_state_backend == "sqlite":
        return SqliteResearchProgramStore(_sqlite_path())
    return FileResearchProgramStore(settings.app_data_dir)


def get_truth_store():
    if settings.app_truth_backend == "file":
        return FileTruthStore(settings.app_data_dir)
    if settings.app_truth_backend == "postgres":
        return PostgresTruthStore(*_postgres_args())
    return WikibaseTruthStore(
        base_url=settings.wikibase_base_url,
        api_url=settings.wikibase_api_url,
        username=settings.wikibase_username,
        password=settings.wikibase_password,
        property_map_raw=settings.wikibase_property_map,
        cache_dir=settings.app_data_dir,
    )


def get_projection():
    return QdrantProjectionAdapter()


def get_research_semantic():
    return QdrantResearchSemanticAdapter()


def get_extractor():
    if settings.graph_rag_enabled:
        from source_aware_worldbuilding.adapters.graphrag_adapter import GraphRAGExtractionAdapter

        return GraphRAGExtractionAdapter(
            mode=settings.graph_rag_mode,
            graph_rag_root=settings.graph_rag_root,
            artifacts_dir=settings.graph_rag_artifacts_dir,
        )
    return HeuristicExtractionAdapter()


def get_corpus():
    return ZoteroCorpusAdapter()


def get_research_scout_registry() -> ResearchScoutRegistry:
    return ResearchScoutRegistry(
        [
            WebOpenResearchScout(user_agent=settings.app_research_user_agent),
            CuratedInputsResearchScout(user_agent=settings.app_research_user_agent),
        ],
        default_adapter_id=settings.app_research_default_adapter_id,
    )


def get_ingestion_service() -> IngestionService:
    return IngestionService(
        corpus=get_corpus(),
        extractor=get_extractor(),
        source_store=get_source_store(),
        text_unit_store=get_text_unit_store(),
        source_document_store=get_source_document_store(),
        run_store=get_run_store(),
        candidate_store=get_candidate_store(),
        evidence_store=get_evidence_store(),
    )


def get_intake_service() -> IntakeService:
    return IntakeService(
        corpus=get_corpus(),
        source_store=get_source_store(),
        source_document_store=get_source_document_store(),
    )


def get_normalization_service() -> NormalizationService:
    return NormalizationService(
        source_document_store=get_source_document_store(),
        text_unit_store=get_text_unit_store(),
    )


def get_review_service() -> ReviewService:
    return ReviewService(
        candidate_store=get_candidate_store(),
        truth_store=get_truth_store(),
        review_store=get_review_store(),
        evidence_store=get_evidence_store(),
        projection=get_projection(),
    )


def get_query_service() -> QueryService:
    return QueryService(
        truth_store=get_truth_store(),
        evidence_store=get_evidence_store(),
        source_store=get_source_store(),
        projection=get_projection(),
    )


def get_lore_packet_service() -> LorePacketService:
    return LorePacketService(
        truth_store=get_truth_store(),
        evidence_store=get_evidence_store(),
        source_store=get_source_store(),
    )


def get_research_service() -> ResearchService:
    return ResearchService(
        scout_registry=get_research_scout_registry(),
        run_store=get_research_run_store(),
        finding_store=get_research_finding_store(),
        program_store=get_research_program_store(),
        source_store=get_source_store(),
        source_document_store=get_source_document_store(),
        normalization_service=get_normalization_service(),
        ingestion_service=get_ingestion_service(),
        research_semantic=get_research_semantic(),
        default_program_markdown=_default_research_program_markdown(),
        default_execution_policy=_default_research_execution_policy(),
        default_adapter_id=settings.app_research_default_adapter_id,
        research_user_agent=settings.app_research_user_agent,
        semantic_duplicate_threshold=settings.research_semantic_duplicate_threshold,
        semantic_novelty_floor=settings.research_semantic_novelty_floor,
        semantic_rerank_weight=settings.research_semantic_rerank_weight,
    )

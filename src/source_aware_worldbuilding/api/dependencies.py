from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
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
    PostgresReviewStore,
    PostgresSourceDocumentStore,
    PostgresSourceStore,
    PostgresTextUnitStore,
    PostgresTruthStore,
)
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantProjectionAdapter
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteReviewStore,
    SqliteSourceDocumentStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
)
from source_aware_worldbuilding.adapters.wikibase_adapter import WikibaseTruthStore
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.intake import IntakeService
from source_aware_worldbuilding.services.lore_packet import LorePacketService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.query import QueryService
from source_aware_worldbuilding.services.review import ReviewService
from source_aware_worldbuilding.settings import settings


def _sqlite_path() -> Path:
    return Path(settings.app_sqlite_path)


def _postgres_args() -> tuple[str, str]:
    return settings.app_postgres_dsn, settings.app_postgres_schema


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

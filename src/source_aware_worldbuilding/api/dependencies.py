from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileReviewStore,
    FileSourceStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.graphrag_adapter import GraphRAGExtractionAdapter
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantProjectionAdapter
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteReviewStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
    SqliteTruthStore,
)
from source_aware_worldbuilding.adapters.wikibase_adapter import WikibaseTruthStore
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.query import QueryService
from source_aware_worldbuilding.services.review import ReviewService
from source_aware_worldbuilding.settings import settings


def _sqlite_path() -> Path:
    return Path(settings.app_sqlite_path)


def get_source_store():
    if settings.app_state_backend == "sqlite":
        return SqliteSourceStore(_sqlite_path())
    return FileSourceStore(settings.app_data_dir)


def get_text_unit_store():
    if settings.app_state_backend == "sqlite":
        return SqliteTextUnitStore(_sqlite_path())
    return FileTextUnitStore(settings.app_data_dir)


def get_run_store():
    if settings.app_state_backend == "sqlite":
        return SqliteExtractionRunStore(_sqlite_path())
    return FileExtractionRunStore(settings.app_data_dir)


def get_candidate_store():
    if settings.app_state_backend == "sqlite":
        return SqliteCandidateStore(_sqlite_path())
    return FileCandidateStore(settings.app_data_dir)


def get_evidence_store():
    if settings.app_state_backend == "sqlite":
        return SqliteEvidenceStore(_sqlite_path())
    return FileEvidenceStore(settings.app_data_dir)


def get_review_store():
    if settings.app_state_backend == "sqlite":
        return SqliteReviewStore(_sqlite_path())
    return FileReviewStore(settings.app_data_dir)


def get_truth_store():
    if settings.app_truth_backend == "sqlite":
        return SqliteTruthStore(_sqlite_path())
    if settings.app_truth_backend == "wikibase":
        return WikibaseTruthStore(
            base_url=settings.wikibase_base_url,
            cache_dir=settings.app_data_dir,
        )
    return FileTruthStore(settings.app_data_dir)


def get_projection():
    return QdrantProjectionAdapter()


def get_ingestion_service() -> IngestionService:
    return IngestionService(
        corpus=ZoteroCorpusAdapter(),
        extractor=GraphRAGExtractionAdapter(),
        source_store=get_source_store(),
        text_unit_store=get_text_unit_store(),
        run_store=get_run_store(),
        candidate_store=get_candidate_store(),
        evidence_store=get_evidence_store(),
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
    )

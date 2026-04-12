from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.graphrag_adapter import GraphRAGExtractionAdapter
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.query import QueryService
from source_aware_worldbuilding.services.review import ReviewService
from source_aware_worldbuilding.settings import settings


def get_candidate_store() -> FileCandidateStore:
    return FileCandidateStore(settings.app_data_dir)


def get_truth_store() -> FileTruthStore:
    return FileTruthStore(settings.app_data_dir)


def get_evidence_store() -> FileEvidenceStore:
    return FileEvidenceStore(settings.app_data_dir)


def get_ingestion_service() -> IngestionService:
    return IngestionService(
        corpus=ZoteroCorpusAdapter(),
        extractor=GraphRAGExtractionAdapter(),
        candidate_store=get_candidate_store(),
    )


def get_review_service() -> ReviewService:
    return ReviewService(
        candidate_store=get_candidate_store(),
        truth_store=get_truth_store(),
    )


def get_query_service() -> QueryService:
    return QueryService(
        truth_store=get_truth_store(),
        evidence_store=get_evidence_store(),
    )

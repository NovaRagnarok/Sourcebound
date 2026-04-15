from fastapi import APIRouter, Body, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import (
    get_ingestion_service,
    get_normalization_service,
)
from source_aware_worldbuilding.domain.errors import (
    ZoteroAuthError,
    ZoteroConfigError,
    ZoteroError,
    ZoteroNotFoundError,
    ZoteroRateLimitError,
)
from source_aware_worldbuilding.domain.models import (
    ExtractCandidatesRequest,
    NormalizeDocumentsRequest,
    ZoteroPullRequest,
)
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.normalization import NormalizationService

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])


@router.post("/zotero/pull")
def pull_sources(
    payload: ZoteroPullRequest = Body(default_factory=ZoteroPullRequest),
    service: IngestionService = Depends(get_ingestion_service),
) -> dict:
    try:
        return service.pull_sources(payload).model_dump(mode="json")
    except ZoteroError as exc:
        raise _zotero_http_error(exc) from exc


@router.post("/normalize-documents")
def normalize_documents(
    payload: NormalizeDocumentsRequest = Body(default_factory=NormalizeDocumentsRequest),
    service: NormalizationService = Depends(get_normalization_service),
) -> dict:
    return service.normalize_documents(
        document_ids=payload.document_ids,
        source_ids=payload.source_ids,
        retry_failed=payload.retry_failed,
    )


@router.post("/extract-candidates")
def extract_candidates(
    payload: ExtractCandidatesRequest = Body(default_factory=ExtractCandidatesRequest),
    service: IngestionService = Depends(get_ingestion_service),
) -> dict:
    try:
        output = service.extract_candidates(source_ids=payload.source_ids)
    except ZoteroError as exc:
        raise _zotero_http_error(exc) from exc
    return {
        "run": output.run.model_dump(mode="json"),
        "count": len(output.candidates),
        "candidates": [candidate.model_dump(mode="json") for candidate in output.candidates],
        "evidence": [item.model_dump(mode="json") for item in output.evidence],
    }


def _zotero_http_error(exc: ZoteroError) -> HTTPException:
    if isinstance(exc, ZoteroConfigError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, ZoteroAuthError):
        return HTTPException(status_code=401, detail=str(exc))
    if isinstance(exc, ZoteroNotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ZoteroRateLimitError):
        return HTTPException(status_code=429, detail=str(exc))
    return HTTPException(status_code=502, detail=str(exc))

from fastapi import APIRouter, Depends

from source_aware_worldbuilding.api.dependencies import get_ingestion_service
from source_aware_worldbuilding.services.ingestion import IngestionService

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])


@router.post("/zotero/pull")
def pull_sources(service: IngestionService = Depends(get_ingestion_service)) -> dict:
    sources = service.pull_sources()
    return {
        "count": len(sources),
        "sources": [source.model_dump(mode="json") for source in sources],
    }


@router.post("/extract-candidates")
def extract_candidates(service: IngestionService = Depends(get_ingestion_service)) -> dict:
    output = service.extract_candidates()
    return {
        "run": output.run.model_dump(mode="json"),
        "count": len(output.candidates),
        "candidates": [candidate.model_dump(mode="json") for candidate in output.candidates],
        "evidence": [item.model_dump(mode="json") for item in output.evidence],
    }

from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_ingestion_service
from source_aware_worldbuilding.services.ingestion import IngestionService

router = APIRouter(prefix="/v1/sources", tags=["sources"])


@router.get("")
def list_sources(service: IngestionService = Depends(get_ingestion_service)) -> list[dict]:
    return [source.model_dump(mode="json") for source in service.list_sources()]


@router.get("/{source_id}")
def get_source(source_id: str, service: IngestionService = Depends(get_ingestion_service)) -> dict:
    detail = service.get_source_detail(source_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return detail.model_dump(mode="json")

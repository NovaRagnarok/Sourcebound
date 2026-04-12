from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_ingestion_service
from source_aware_worldbuilding.services.ingestion import IngestionService

router = APIRouter(prefix="/v1/sources", tags=["sources"])


@router.get("")
def list_sources(service: IngestionService = Depends(get_ingestion_service)) -> list[dict]:
    return [source.model_dump(mode="json") for source in service.list_sources()]


@router.get("/{source_id}")
def get_source(source_id: str, service: IngestionService = Depends(get_ingestion_service)) -> dict:
    source = next((item for item in service.list_sources() if item.source_id == source_id), None)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return {
        "source": source.model_dump(mode="json"),
        "text_units": [
            item.model_dump(mode="json") for item in service.get_source_text_units(source_id)
        ],
    }

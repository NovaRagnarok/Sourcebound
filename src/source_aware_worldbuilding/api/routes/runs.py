from fastapi import APIRouter, Depends

from source_aware_worldbuilding.api.dependencies import get_ingestion_service
from source_aware_worldbuilding.services.ingestion import IngestionService

router = APIRouter(prefix="/v1/extraction-runs", tags=["extraction-runs"])


@router.get("")
def list_runs(service: IngestionService = Depends(get_ingestion_service)) -> list[dict]:
    return [run.model_dump(mode="json") for run in service.list_runs()]

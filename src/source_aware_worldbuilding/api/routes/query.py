from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_query_service
from source_aware_worldbuilding.domain.errors import CanonUnavailableError, WikibaseSyncError
from source_aware_worldbuilding.domain.models import QueryRequest
from source_aware_worldbuilding.services.query import QueryService

router = APIRouter(prefix="/v1/query", tags=["query"])


@router.post("")
def query(payload: QueryRequest, service: QueryService = Depends(get_query_service)) -> dict:
    try:
        result = service.answer(payload)
    except CanonUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except WikibaseSyncError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result.model_dump(mode="json")

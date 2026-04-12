from fastapi import APIRouter, Depends

from source_aware_worldbuilding.api.dependencies import get_query_service
from source_aware_worldbuilding.domain.models import QueryRequest
from source_aware_worldbuilding.services.query import QueryService

router = APIRouter(prefix="/v1/query", tags=["query"])


@router.post("")
def query(payload: QueryRequest, service: QueryService = Depends(get_query_service)) -> dict:
    result = service.answer(payload)
    return result.model_dump(mode="json")

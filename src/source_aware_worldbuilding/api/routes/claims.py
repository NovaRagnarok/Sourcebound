from fastapi import APIRouter, Depends

from source_aware_worldbuilding.api.dependencies import get_truth_store

router = APIRouter(prefix="/v1/claims", tags=["claims"])


@router.get("")
def list_claims(store=Depends(get_truth_store)) -> list[dict]:
    return [claim.model_dump(mode="json") for claim in store.list_claims()]

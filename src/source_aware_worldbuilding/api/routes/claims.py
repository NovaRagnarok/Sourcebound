from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_truth_store
from source_aware_worldbuilding.domain.errors import CanonUnavailableError, WikibaseSyncError

router = APIRouter(prefix="/v1/claims", tags=["claims"])


@router.get("")
def list_claims(store=Depends(get_truth_store)) -> list[dict]:
    try:
        return [claim.model_dump(mode="json") for claim in store.list_claims()]
    except CanonUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except WikibaseSyncError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/{claim_id}")
def get_claim(claim_id: str, store=Depends(get_truth_store)) -> dict:
    try:
        claim = store.get_claim(claim_id)
    except CanonUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except WikibaseSyncError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if claim is None:
        raise HTTPException(status_code=404, detail="Claim not found")
    return claim.model_dump(mode="json")

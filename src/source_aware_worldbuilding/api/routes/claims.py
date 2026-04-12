from fastapi import APIRouter, Depends, HTTPException

from source_aware_worldbuilding.api.dependencies import get_truth_store

router = APIRouter(prefix="/v1/claims", tags=["claims"])


@router.get("")
def list_claims(store=Depends(get_truth_store)) -> list[dict]:
    return [claim.model_dump(mode="json") for claim in store.list_claims()]


@router.get("/{claim_id}")
def get_claim(claim_id: str, store=Depends(get_truth_store)) -> dict:
    claim = store.get_claim(claim_id)
    if claim is None:
        raise HTTPException(status_code=404, detail="Claim not found")
    return claim.model_dump(mode="json")

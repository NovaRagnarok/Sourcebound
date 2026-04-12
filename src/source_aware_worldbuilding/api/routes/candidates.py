from fastapi import APIRouter, Depends, HTTPException, Query

from source_aware_worldbuilding.api.dependencies import get_candidate_store, get_review_service
from source_aware_worldbuilding.domain.models import ReviewRequest
from source_aware_worldbuilding.services.review import ReviewService

router = APIRouter(prefix="/v1/candidates", tags=["candidates"])


@router.get("")
def list_candidates(
    review_state: str | None = Query(default=None),
    service: ReviewService = Depends(get_review_service),
) -> list[dict]:
    return [
        candidate.model_dump(mode="json") for candidate in service.list_candidates(review_state)
    ]


@router.get("/{candidate_id}")
def get_candidate(candidate_id: str, store=Depends(get_candidate_store)) -> dict:
    candidate = store.get_candidate(candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return candidate.model_dump(mode="json")


@router.post("/{candidate_id}/review")
def review_candidate(
    candidate_id: str,
    payload: ReviewRequest,
    service: ReviewService = Depends(get_review_service),
) -> dict:
    approved = service.review_candidate(candidate_id, payload)
    if payload.decision.value == "reject":
        return {"status": "rejected", "candidate_id": candidate_id}
    if approved is None:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return {"status": "approved", "claim": approved.model_dump(mode="json")}

from fastapi import APIRouter, Depends, HTTPException, Query

from source_aware_worldbuilding.api.dependencies import get_candidate_store, get_review_service
from source_aware_worldbuilding.domain.errors import (
    CanonUnavailableError,
    ReviewConflictError,
    WikibaseSyncError,
)
from source_aware_worldbuilding.domain.models import ReviewQueueCard, ReviewRequest
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


@router.get("/review-queue", response_model=list[ReviewQueueCard])
def get_review_queue(
    service: ReviewService = Depends(get_review_service),
) -> list[ReviewQueueCard]:
    return service.list_review_queue()


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
    try:
        approved = service.review_candidate(candidate_id, payload)
    except CanonUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except WikibaseSyncError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except ReviewConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if payload.decision.value == "reject":
        return {"status": "rejected", "candidate_id": candidate_id}
    if approved is None:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return {"status": "approved", "claim": approved.model_dump(mode="json")}

from fastapi import APIRouter, Depends, HTTPException, Query

from source_aware_worldbuilding.api.dependencies import (
    get_candidate_store,
    get_review_service,
    require_writer_actor,
)
from source_aware_worldbuilding.domain.errors import (
    CanonUnavailableError,
    ReviewConflictError,
    WikibaseSyncError,
)
from source_aware_worldbuilding.domain.models import (
    ReviewEvent,
    ReviewQueueCard,
    ReviewRequest,
)
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


@router.get("/{candidate_id}/reviews", response_model=list[ReviewEvent])
def list_reviews(
    candidate_id: str,
    service: ReviewService = Depends(get_review_service),
    store=Depends(get_candidate_store),
) -> list[ReviewEvent]:
    if store.get_candidate(candidate_id) is None:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return service.list_reviews(candidate_id=candidate_id)


@router.post("/{candidate_id}/review")
def review_candidate(
    candidate_id: str,
    payload: ReviewRequest,
    service: ReviewService = Depends(get_review_service),
    store=Depends(get_candidate_store),
    actor=Depends(require_writer_actor),
) -> dict:
    if store.get_candidate(candidate_id) is None:
        raise HTTPException(status_code=404, detail="Candidate not found")
    try:
        approved = service.review_candidate(candidate_id, payload, actor=actor)
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

from __future__ import annotations

from uuid import uuid4

from source_aware_worldbuilding.domain.enums import ReviewState
from source_aware_worldbuilding.domain.models import ApprovedClaim, ReviewRequest
from source_aware_worldbuilding.ports import CandidateStorePort, TruthStorePort


class ReviewService:
    def __init__(self, candidate_store: CandidateStorePort, truth_store: TruthStorePort):
        self.candidate_store = candidate_store
        self.truth_store = truth_store

    def review_candidate(self, candidate_id: str, request: ReviewRequest) -> ApprovedClaim | None:
        candidate = self.candidate_store.get_candidate(candidate_id)
        if candidate is None:
            return None

        if request.decision.value == "reject":
            candidate.review_state = ReviewState.REJECTED
            self.candidate_store.update_candidate(candidate)
            return None

        candidate.review_state = ReviewState.APPROVED
        self.candidate_store.update_candidate(candidate)

        approved = ApprovedClaim(
            claim_id=f"claim-{uuid4().hex[:12]}",
            subject=candidate.subject,
            predicate=candidate.predicate,
            value=candidate.value,
            claim_kind=candidate.claim_kind,
            status=request.override_status or candidate.status_suggestion,
            place=candidate.place,
            time_start=candidate.time_start,
            time_end=candidate.time_end,
            viewpoint_scope=candidate.viewpoint_scope,
            author_choice=(request.override_status and request.override_status.value == "author_choice")
            or False,
            evidence_ids=candidate.evidence_ids,
            notes=request.notes or candidate.notes,
        )
        self.truth_store.save_claim(approved)
        return approved

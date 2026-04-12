from __future__ import annotations

from uuid import uuid4

from source_aware_worldbuilding.domain.enums import ReviewState
from source_aware_worldbuilding.domain.models import ApprovedClaim, ReviewEvent, ReviewRequest
from source_aware_worldbuilding.ports import (
    CandidateStorePort,
    EvidenceStorePort,
    ProjectionPort,
    ReviewStorePort,
    TruthStorePort,
)


class ReviewService:
    def __init__(
        self,
        candidate_store: CandidateStorePort,
        truth_store: TruthStorePort,
        review_store: ReviewStorePort,
        evidence_store: EvidenceStorePort,
        projection: ProjectionPort | None = None,
    ):
        self.candidate_store = candidate_store
        self.truth_store = truth_store
        self.review_store = review_store
        self.evidence_store = evidence_store
        self.projection = projection

    def list_candidates(self, review_state: str | None = None):
        return self.candidate_store.list_candidates(review_state=review_state)

    def list_reviews(self, candidate_id: str | None = None):
        return self.review_store.list_reviews(candidate_id=candidate_id)

    def review_candidate(self, candidate_id: str, request: ReviewRequest) -> ApprovedClaim | None:
        candidate = self.candidate_store.get_candidate(candidate_id)
        if candidate is None:
            return None

        review = ReviewEvent(
            review_id=f"rev-{uuid4().hex[:12]}",
            candidate_id=candidate_id,
            decision=request.decision,
            override_status=request.override_status,
            notes=request.notes,
        )

        if request.decision.value == "reject":
            candidate.review_state = ReviewState.REJECTED
            self.candidate_store.update_candidate(candidate)
            self.review_store.save_review(review)
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
            author_choice=(
                request.override_status is not None
                and request.override_status.value == "author_choice"
            ),
            evidence_ids=candidate.evidence_ids,
            notes=request.notes or candidate.notes,
        )
        self.truth_store.save_claim(approved)
        review.approved_claim_id = approved.claim_id
        self.review_store.save_review(review)
        if self.projection is not None:
            evidence = [
                snippet
                for evidence_id in approved.evidence_ids
                if (snippet := self.evidence_store.get_evidence(evidence_id)) is not None
            ]
            self.projection.upsert_claims([approved], evidence)
        return approved

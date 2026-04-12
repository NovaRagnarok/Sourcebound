from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileReviewStore,
    FileTruthStore,
)
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ClaimStatus, ReviewDecision, ReviewState
from source_aware_worldbuilding.domain.models import ReviewRequest
from source_aware_worldbuilding.services.review import ReviewService


def build_review_service(
    data_dir: Path,
) -> tuple[FileCandidateStore, FileTruthStore, FileReviewStore, ReviewService]:
    candidate_store = FileCandidateStore(data_dir)
    truth_store = FileTruthStore(data_dir)
    review_store = FileReviewStore(data_dir)
    evidence_store = FileEvidenceStore(data_dir)
    return (
        candidate_store,
        truth_store,
        review_store,
        ReviewService(
            candidate_store=candidate_store,
            truth_store=truth_store,
            review_store=review_store,
            evidence_store=evidence_store,
        ),
    )


def test_review_flow(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    approved = service.review_candidate("cand-1", ReviewRequest(decision=ReviewDecision.APPROVE))

    assert approved is not None
    assert approved.status == ClaimStatus.PROBABLE
    assert approved.author_choice is False
    assert approved.evidence_ids == ["evi-1"]
    updated_candidate = candidate_store.get_candidate("cand-1")
    assert updated_candidate is not None
    assert updated_candidate.review_state == ReviewState.APPROVED
    assert len(truth_store.list_claims()) == 1
    assert review_store.list_reviews("cand-1")[0].approved_claim_id == approved.claim_id


def test_review_reject_marks_candidate_without_creating_claim(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    rejected = service.review_candidate("cand-2", ReviewRequest(decision=ReviewDecision.REJECT))

    assert rejected is None
    rejected_candidate = candidate_store.get_candidate("cand-2")
    assert rejected_candidate is not None
    assert rejected_candidate.review_state == ReviewState.REJECTED
    assert truth_store.list_claims() == []
    assert review_store.list_reviews("cand-2")[0].decision == ReviewDecision.REJECT


def test_review_override_can_mark_author_choice(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    approved = service.review_candidate(
        "cand-2",
        ReviewRequest(
            decision=ReviewDecision.APPROVE,
            override_status=ClaimStatus.AUTHOR_CHOICE,
            notes="Authorial call for the pilot.",
        ),
    )

    assert approved is not None
    assert approved.status == ClaimStatus.AUTHOR_CHOICE
    assert approved.author_choice is True
    assert approved.notes == "Authorial call for the pilot."
    updated_candidate = candidate_store.get_candidate("cand-2")
    assert updated_candidate is not None
    assert updated_candidate.review_state == ReviewState.APPROVED
    assert len(truth_store.list_claims()) == 1
    assert review_store.list_reviews("cand-2")[0].override_status == ClaimStatus.AUTHOR_CHOICE


def test_review_missing_candidate_returns_none(temp_data_dir: Path) -> None:
    seed_dev_data()
    _, truth_store, review_store, service = build_review_service(temp_data_dir)

    assert (
        service.review_candidate("missing", ReviewRequest(decision=ReviewDecision.APPROVE)) is None
    )
    assert truth_store.list_claims() == []
    assert review_store.list_reviews() == []

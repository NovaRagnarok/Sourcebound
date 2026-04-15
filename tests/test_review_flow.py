from pathlib import Path

import pytest

from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileReviewStore,
    FileSourceStore,
    FileTextUnitStore,
)
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ClaimStatus, ReviewDecision, ReviewState
from source_aware_worldbuilding.domain.errors import ReviewConflictError, WikibaseSyncError
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    CandidateClaim,
    ClaimKind,
    ClaimRelationship,
    ReviewClaimPatch,
    ReviewRequest,
)
from source_aware_worldbuilding.services.review import ReviewService


class InMemoryTruthStore:
    def __init__(self) -> None:
        self.claims: dict[str, ApprovedClaim] = {}

    def list_claims(self) -> list[ApprovedClaim]:
        return list(self.claims.values())

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        return self.claims.get(claim_id)

    def list_relationships(self, claim_id: str | None = None) -> list[ClaimRelationship]:
        _ = claim_id
        return []

    def save_claim(self, claim: ApprovedClaim, evidence=None, review=None) -> None:
        _ = evidence, review
        self.claims[claim.claim_id] = claim


def build_review_service(
    data_dir: Path,
) -> tuple[FileCandidateStore, InMemoryTruthStore, FileReviewStore, ReviewService]:
    candidate_store = FileCandidateStore(data_dir)
    truth_store = InMemoryTruthStore()
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
            source_store=FileSourceStore(data_dir),
            text_unit_store=FileTextUnitStore(data_dir),
        ),
    )


def test_review_flow(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    approved = service.review_candidate(
        "cand-grain-bell-beadles",
        ReviewRequest(decision=ReviewDecision.APPROVE),
    )

    assert approved is not None
    assert approved.status == ClaimStatus.PROBABLE
    assert approved.author_choice is False
    assert approved.evidence_ids == ["evi-grain-bell-beadles"]
    assert approved.created_from_run_id == "extract-research-rouen"
    updated_candidate = candidate_store.get_candidate("cand-grain-bell-beadles")
    assert updated_candidate is not None
    assert updated_candidate.review_state == ReviewState.APPROVED
    assert len(truth_store.list_claims()) == 1
    assert (
        review_store.list_reviews("cand-grain-bell-beadles")[0].approved_claim_id
        == approved.claim_id
    )


def test_review_reject_marks_candidate_without_creating_claim(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    rejected = service.review_candidate(
        "cand-blue-lanterns",
        ReviewRequest(decision=ReviewDecision.REJECT),
    )

    assert rejected is None
    rejected_candidate = candidate_store.get_candidate("cand-blue-lanterns")
    assert rejected_candidate is not None
    assert rejected_candidate.review_state == ReviewState.REJECTED
    assert truth_store.list_claims() == []
    assert review_store.list_reviews("cand-blue-lanterns")[0].decision == ReviewDecision.REJECT


def test_review_override_can_mark_author_choice(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    approved = service.review_candidate(
        "cand-blue-lanterns",
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
    assert approved.created_from_run_id == "extract-rouen-core"
    updated_candidate = candidate_store.get_candidate("cand-blue-lanterns")
    assert updated_candidate is not None
    assert updated_candidate.review_state == ReviewState.APPROVED
    assert len(truth_store.list_claims()) == 1
    assert (
        review_store.list_reviews("cand-blue-lanterns")[0].override_status
        == ClaimStatus.AUTHOR_CHOICE
    )


def test_review_approve_can_patch_claim_without_mutating_candidate(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, _, service = build_review_service(temp_data_dir)

    approved = service.review_candidate(
        "cand-grain-bell-beadles",
        ReviewRequest(
            decision=ReviewDecision.APPROVE,
            claim_patch=ReviewClaimPatch(
                value="the grain bell during the January ration roll",
                viewpoint_scope="parish witnesses",
            ),
            notes="Tightened wording before approval.",
        ),
    )

    assert approved is not None
    assert approved.value == "the grain bell during the January ration roll"
    assert approved.viewpoint_scope == "parish witnesses"
    assert approved.notes == "Tightened wording before approval."
    stored_candidate = candidate_store.get_candidate("cand-grain-bell-beadles")
    assert stored_candidate is not None
    assert stored_candidate.value == "the grain bell in 1422"
    assert stored_candidate.review_state == ReviewState.APPROVED
    assert len(truth_store.list_claims()) == 1


def test_review_reject_can_defer_candidate_for_edit(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    rejected = service.review_candidate(
        "cand-blue-lanterns",
        ReviewRequest(
            decision=ReviewDecision.REJECT,
            defer_state="needs_edit",
            notes="Keep the folklore framing explicit.",
        ),
    )

    assert rejected is None
    stored_candidate = candidate_store.get_candidate("cand-blue-lanterns")
    assert stored_candidate is not None
    assert stored_candidate.review_state == ReviewState.NEEDS_EDIT
    assert truth_store.list_claims() == []
    assert review_store.list_reviews("cand-blue-lanterns")[0].decision == ReviewDecision.REJECT


def test_deferred_candidate_requires_meaningful_edit_before_approval(
    temp_data_dir: Path,
) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)
    review_count_before = len(review_store.list_reviews())

    with pytest.raises(ReviewConflictError):
        service.review_candidate(
            "cand-grain-bell-timing",
            ReviewRequest(decision=ReviewDecision.APPROVE),
        )

    stored_candidate = candidate_store.get_candidate("cand-grain-bell-timing")
    assert stored_candidate is not None
    assert stored_candidate.review_state == ReviewState.NEEDS_SPLIT
    assert truth_store.list_claims() == []
    assert len(review_store.list_reviews()) == review_count_before


def test_review_queue_cards_include_context_and_source_title(temp_data_dir: Path) -> None:
    seed_dev_data()
    _, _, _, service = build_review_service(temp_data_dir)

    cards = service.list_review_queue()
    card = next(item for item in cards if item.candidate_id == "cand-grain-bell-beadles")

    assert card.claim_text == "Rouen parish beadles were posted at the grain bell in 1422"
    assert card.evidence_quality == "supported"
    assert card.primary_evidence is not None
    assert card.primary_evidence.source_title == "Curated parish note on grain bell beadles"
    assert card.primary_evidence.context_before.strip().startswith("Witness depositions")
    assert card.primary_evidence.context_after.strip().startswith("Bakers waited")
    assert card.primary_evidence.excerpt.startswith("Rouen parish beadles were posted")
    assert card.location_summary == "curated input · Rouen · 1422-01-01 to 1422-02-28"


def test_review_queue_marks_thin_and_blind_candidates(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, _, _, service = build_review_service(temp_data_dir)
    candidate_store.save_candidates(
        [
            CandidateClaim(
                candidate_id="cand-blind-ledger",
                subject="Rouen market clerk",
                predicate="recorded",
                value="an unsupported side note",
                claim_kind=ClaimKind.PRACTICE,
                status_suggestion=ClaimStatus.PROBABLE,
                review_state=ReviewState.PENDING,
                place="Rouen",
                evidence_ids=["missing-evidence"],
                extractor_run_id="extract-rouen-core",
                notes="Intentionally missing evidence for queue-card fallback coverage.",
            )
        ]
    )

    cards = service.list_review_queue()
    thin_card = next(item for item in cards if item.candidate_id == "cand-shrine-lantern-omen")
    blind_card = next(item for item in cards if item.candidate_id == "cand-blind-ledger")

    assert thin_card.evidence_quality == "thin"
    assert "missing_span_context" in thin_card.weakness_reasons
    assert blind_card.evidence_quality == "blind"
    assert "missing_evidence" in blind_card.weakness_reasons
    assert blind_card.primary_evidence is None


def test_review_queue_keeps_primary_evidence_and_extra_snippets_ordered(
    temp_data_dir: Path,
) -> None:
    seed_dev_data()
    _, _, _, service = build_review_service(temp_data_dir)

    cards = service.list_review_queue()
    card = next(item for item in cards if item.candidate_id == "cand-grain-bell-timing")

    assert card.primary_evidence is not None
    assert card.primary_evidence.evidence_id == "evi-bell-prime"
    assert card.extra_evidence_count == 1
    assert [item.evidence_id for item in card.evidence_items] == [
        "evi-bell-prime",
        "evi-bell-terce",
    ]


def test_review_missing_candidate_returns_none(temp_data_dir: Path) -> None:
    seed_dev_data()
    _, truth_store, review_store, service = build_review_service(temp_data_dir)
    review_count_before = len(review_store.list_reviews())

    assert (
        service.review_candidate("missing", ReviewRequest(decision=ReviewDecision.APPROVE)) is None
    )
    assert truth_store.list_claims() == []
    assert len(review_store.list_reviews()) == review_count_before


def test_review_cannot_approve_same_candidate_twice(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store, truth_store, review_store, service = build_review_service(temp_data_dir)

    first = service.review_candidate(
        "cand-grain-bell-beadles",
        ReviewRequest(decision=ReviewDecision.APPROVE),
    )
    review_count_after_first = len(review_store.list_reviews())

    assert first is not None

    with pytest.raises(ReviewConflictError):
        service.review_candidate(
            "cand-grain-bell-beadles",
            ReviewRequest(decision=ReviewDecision.APPROVE),
        )

    candidate = candidate_store.get_candidate("cand-grain-bell-beadles")
    assert candidate is not None
    assert candidate.review_state == ReviewState.APPROVED
    assert len(truth_store.list_claims()) == 1
    assert len(review_store.list_reviews()) == review_count_after_first


class FailingTruthStore(InMemoryTruthStore):
    def save_claim(self, claim, evidence=None, review=None) -> None:
        _ = claim, evidence, review
        raise WikibaseSyncError("Wikibase sync failed: upstream unavailable")


def test_review_keeps_candidate_pending_when_wikibase_sync_fails(temp_data_dir: Path) -> None:
    seed_dev_data()
    candidate_store = FileCandidateStore(temp_data_dir)
    review_store = FileReviewStore(temp_data_dir)
    review_count_before = len(review_store.list_reviews())
    service = ReviewService(
        candidate_store=candidate_store,
        truth_store=FailingTruthStore(),
        review_store=review_store,
        evidence_store=FileEvidenceStore(temp_data_dir),
    )

    try:
        service.review_candidate(
            "cand-grain-bell-beadles",
            ReviewRequest(decision=ReviewDecision.APPROVE),
        )
    except WikibaseSyncError as exc:
        assert "upstream unavailable" in str(exc)
    else:
        raise AssertionError("Expected WikibaseSyncError")

    candidate = candidate_store.get_candidate("cand-grain-bell-beadles")
    assert candidate is not None
    assert candidate.review_state == ReviewState.PENDING
    assert len(review_store.list_reviews()) == review_count_before

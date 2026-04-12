from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import FileCandidateStore, FileTruthStore
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ReviewDecision
from source_aware_worldbuilding.domain.models import ReviewRequest
from source_aware_worldbuilding.services.review import ReviewService
from source_aware_worldbuilding.settings import settings


def test_review_flow() -> None:
    seed_dev_data()
    candidate_store = FileCandidateStore(Path(settings.app_data_dir))
    truth_store = FileTruthStore(Path(settings.app_data_dir))
    service = ReviewService(candidate_store=candidate_store, truth_store=truth_store)

    approved = service.review_candidate("cand-1", ReviewRequest(decision=ReviewDecision.APPROVE))

    assert approved is not None
    claims = truth_store.list_claims()
    assert len(claims) >= 1

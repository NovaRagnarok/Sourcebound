from __future__ import annotations

from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ReviewState
from source_aware_worldbuilding.domain.models import CandidateClaim, SourceRecord


class GraphRAGExtractionAdapter:
    """Placeholder adapter.

    Real implementation should:
    - normalize input text units
    - call GraphRAG indexing or extraction routines
    - map extracted structures into CandidateClaim objects
    - preserve run identifiers and evidence mapping
    """

    def extract_candidates(self, sources: list[SourceRecord]) -> list[CandidateClaim]:
        if not sources:
            return []
        source = sources[0]
        return [
            CandidateClaim(
                candidate_id="cand-stub-1",
                subject=source.title,
                predicate="has_status",
                value="illustrative only",
                claim_kind=ClaimKind.OBJECT,
                status_suggestion=ClaimStatus.PROBABLE,
                review_state=ReviewState.PENDING,
                evidence_ids=["evi-1"],
                notes="Replace stub extraction with a real GraphRAG pipeline.",
            )
        ]

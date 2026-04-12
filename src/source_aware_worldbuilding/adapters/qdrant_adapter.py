from __future__ import annotations

from source_aware_worldbuilding.domain.models import ApprovedClaim


class QdrantProjectionAdapter:
    """Placeholder projection adapter.

    Real implementation should write only approved claims and linked evidence
    into a retrieval collection with payload filters such as status, place,
    timeframe, and viewpoint scope.
    """

    def upsert_claims(self, claims: list[ApprovedClaim]) -> None:
        _ = claims
        return None

from __future__ import annotations

from source_aware_worldbuilding.domain.models import ApprovedClaim


class WikibaseTruthStore:
    """Placeholder adapter.

    The rest of the code should depend on the TruthStorePort, not on this class.
    This adapter should be responsible for translating ApprovedClaim objects into
    Wikibase entities, statements, qualifiers, and references.
    """

    def __init__(self, base_url: str | None):
        self.base_url = base_url

    def list_claims(self) -> list[ApprovedClaim]:
        raise NotImplementedError("Implement read logic against a Wikibase instance.")

    def save_claim(self, claim: ApprovedClaim) -> None:
        raise NotImplementedError("Implement write logic against a Wikibase instance.")

from __future__ import annotations

from math import sqrt

from source_aware_worldbuilding.domain.models import ApprovedClaim, EvidenceSnippet
from source_aware_worldbuilding.settings import settings


class QdrantProjectionAdapter:
    """Optional Qdrant projection for approved claims."""

    VECTOR_SIZE = 32

    def __init__(self) -> None:
        self.enabled = settings.qdrant_enabled

    def upsert_claims(self, claims: list[ApprovedClaim], evidence: list[EvidenceSnippet]) -> None:
        if not self.enabled or not claims:
            return

        try:
            from qdrant_client import QdrantClient
            from qdrant_client.models import Distance, PointStruct, VectorParams
        except Exception:
            return

        evidence_by_id = {item.evidence_id: item for item in evidence}
        client = QdrantClient(url=settings.qdrant_url)
        client.recreate_collection(
            collection_name=settings.qdrant_collection,
            vectors_config=VectorParams(size=self.VECTOR_SIZE, distance=Distance.COSINE),
        )

        points: list[PointStruct] = []
        for index, claim in enumerate(claims, start=1):
            text = self._claim_text(claim, evidence_by_id)
            points.append(
                PointStruct(
                    id=index,
                    vector=self._embed(text),
                    payload={
                        "claim_id": claim.claim_id,
                        "subject": claim.subject,
                        "predicate": claim.predicate,
                        "value": claim.value,
                        "status": claim.status.value,
                        "claim_kind": claim.claim_kind.value,
                        "place": claim.place,
                        "viewpoint_scope": claim.viewpoint_scope,
                        "text": text,
                    },
                )
            )
        client.upsert(collection_name=settings.qdrant_collection, points=points)

    def _claim_text(self, claim: ApprovedClaim, evidence_by_id: dict[str, EvidenceSnippet]) -> str:
        evidence_text = " ".join(
            evidence_by_id[evidence_id].text
            for evidence_id in claim.evidence_ids
            if evidence_id in evidence_by_id
        )
        return f"{claim.subject} {claim.predicate} {claim.value}. {evidence_text}".strip()

    def _embed(self, text: str) -> list[float]:
        vector = [0.0] * self.VECTOR_SIZE
        for token in text.lower().split():
            slot = hash(token) % self.VECTOR_SIZE
            vector[slot] += 1.0
        norm = sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]

from __future__ import annotations

from math import sqrt

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    ProjectionSearchResult,
)
from source_aware_worldbuilding.settings import settings


class QdrantProjectionAdapter:
    """Optional Qdrant projection for approved claims."""

    VECTOR_SIZE = 32

    def __init__(self) -> None:
        self.enabled = settings.qdrant_enabled
        self.url = settings.qdrant_url
        self.collection = settings.qdrant_collection

    def upsert_claims(self, claims: list[ApprovedClaim], evidence: list[EvidenceSnippet]) -> None:
        if not self.enabled or not claims:
            return

        try:
            from qdrant_client.models import PointStruct
        except Exception:
            return

        evidence_by_id = {item.evidence_id: item for item in evidence}
        try:
            client = self._client()
            self._ensure_collection(client)
        except Exception:
            return

        points: list[PointStruct] = []
        for claim in claims:
            text = self._claim_text(claim, evidence_by_id)
            points.append(
                PointStruct(
                    id=claim.claim_id,
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
                        "evidence_ids": claim.evidence_ids,
                        "text": text,
                    },
                )
            )
        try:
            client.upsert(collection_name=self.collection, points=points)
        except Exception:
            return

    def search_claim_ids(
        self,
        question: str,
        allowed_claim_ids: list[str],
        *,
        limit: int = 10,
    ) -> ProjectionSearchResult:
        if not self.enabled:
            return ProjectionSearchResult(
                fallback_used=True,
                fallback_reason="Qdrant is disabled.",
            )
        if not allowed_claim_ids:
            return ProjectionSearchResult()

        try:
            from qdrant_client.models import FieldCondition, Filter, MatchAny
        except Exception:
            return ProjectionSearchResult(
                fallback_used=True,
                fallback_reason="Qdrant client is unavailable.",
            )

        try:
            client = self._client()
            if not client.collection_exists(self.collection):
                return ProjectionSearchResult(
                    fallback_used=True,
                    fallback_reason="Qdrant collection is not initialized.",
                )
            response = client.query_points(
                collection_name=self.collection,
                query=self._embed(question),
                limit=min(limit, len(allowed_claim_ids)),
                query_filter=Filter(
                    must=[
                        FieldCondition(
                            key="claim_id",
                            match=MatchAny(any=allowed_claim_ids),
                        )
                    ]
                ),
                with_payload=True,
                with_vectors=False,
            )
        except Exception as exc:
            return ProjectionSearchResult(
                fallback_used=True,
                fallback_reason=f"Qdrant query failed: {exc}",
            )

        claim_ids = [
            str(point.payload.get("claim_id"))
            for point in response.points
            if point.payload and point.payload.get("claim_id")
        ]
        if not claim_ids:
            return ProjectionSearchResult(
                fallback_used=True,
                fallback_reason="Qdrant returned no usable hits.",
            )
        return ProjectionSearchResult(claim_ids=claim_ids)

    def _client(self):
        from qdrant_client import QdrantClient

        return QdrantClient(url=self.url)

    def _ensure_collection(self, client) -> None:
        from qdrant_client.models import Distance, VectorParams

        if client.collection_exists(self.collection):
            return
        client.create_collection(
            collection_name=self.collection,
            vectors_config=VectorParams(size=self.VECTOR_SIZE, distance=Distance.COSINE),
        )

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

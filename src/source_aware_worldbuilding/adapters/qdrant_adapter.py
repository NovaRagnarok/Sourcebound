from __future__ import annotations

import re
from collections import defaultdict
from hashlib import sha1
from math import sqrt
from uuid import NAMESPACE_URL, uuid5

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    ProjectionSearchResult,
    ResearchFinding,
    ResearchSemanticMatch,
    ResearchSemanticResult,
)
from source_aware_worldbuilding.settings import settings

_EMBED_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _stable_token_slot(token: str, vector_size: int) -> int:
    digest = sha1(token.encode()).digest()
    return int.from_bytes(digest[:8], "big") % vector_size


def _stable_token_sign(token: str) -> float:
    digest = sha1(f"sign:{token}".encode()).digest()
    return 1.0 if digest[0] % 2 == 0 else -1.0


def _embedding_tokens(text: str) -> list[str]:
    return _EMBED_TOKEN_RE.findall(text.lower())


def _weighted_text_features(
    text: str,
    *,
    unigram_weight: float = 1.0,
    bigram_weight: float = 0.65,
    trigram_weight: float = 0.2,
) -> dict[str, float]:
    tokens = _embedding_tokens(text)
    features: dict[str, float] = defaultdict(float)
    for token in tokens:
        features[f"w:{token}"] += unigram_weight
        if len(token) >= 6:
            for index in range(len(token) - 2):
                features[f"c3:{token[index : index + 3]}"] += trigram_weight
    for left, right in zip(tokens, tokens[1:], strict=False):
        features[f"b:{left}_{right}"] += bigram_weight
    return dict(features)


def _merge_weighted_features(*feature_maps: dict[str, float]) -> dict[str, float]:
    merged: dict[str, float] = defaultdict(float)
    for feature_map in feature_maps:
        for feature, weight in feature_map.items():
            merged[feature] += weight
    return dict(merged)


def _embed_weighted_features(features: dict[str, float], vector_size: int) -> list[float]:
    vector = [0.0] * vector_size
    for feature, weight in features.items():
        slot = _stable_token_slot(feature, vector_size)
        vector[slot] += _stable_token_sign(feature) * weight
    norm = sqrt(sum(value * value for value in vector)) or 1.0
    return [value / norm for value in vector]


class QdrantProjectionAdapter:
    """Qdrant projection for approved claims."""

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
                    id=self._point_id(claim.claim_id),
                    vector=self._embed_features(self._claim_features(claim, evidence_by_id)),
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

    def runtime_probe(self) -> tuple[str, bool | None, bool, str]:
        if not self.enabled:
            return (
                "disabled",
                None,
                True,
                (
                    "Qdrant projection is disabled. This is a non-default local mode; query "
                    "and composition fall back to in-memory ranking until you re-enable it."
                ),
            )
        try:
            client = self._client()
        except Exception:
            return (
                "qdrant:degraded",
                None,
                False,
                (
                    "Qdrant client is unavailable. Install the qdrant client dependency "
                    "and start Qdrant with `docker compose up -d qdrant` so query and "
                    "composition stop falling back to memory ranking."
                ),
            )
        try:
            collection_ready = client.collection_exists(self.collection)
        except Exception as exc:
            return (
                "qdrant:degraded",
                False,
                False,
                f"Qdrant is configured but not queryable: {exc}. Start it with "
                "`docker compose up -d qdrant` and verify QDRANT_URL.",
            )
        if not collection_ready:
            return (
                "qdrant:uninitialized",
                True,
                False,
                (
                    f"Qdrant is reachable, but collection '{self.collection}' is not "
                    "initialized. Run `saw seed-dev-data` to initialize it, or "
                    "`saw qdrant-rebuild` to repair the projection manually."
                ),
            )
        return (
            "qdrant:ready",
            True,
            True,
            f"Qdrant collection '{self.collection}' is queryable.",
        )

    def initialize_collection(self) -> bool:
        if not self.enabled:
            return False
        try:
            client = self._client()
            existed = client.collection_exists(self.collection)
            self._ensure_collection(client)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to initialize Qdrant collection '{self.collection}': {exc}"
            ) from exc
        return not existed

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
        text = " ".join(
            part
            for part in [
                claim.subject,
                claim.predicate.replace("_", " "),
                claim.value,
                claim.place or "",
                claim.viewpoint_scope or "",
                claim.notes or "",
            ]
            if part
        ).strip()
        if evidence_text:
            return f"{text}. Evidence: {evidence_text}".strip()
        return text

    def _claim_features(
        self,
        claim: ApprovedClaim,
        evidence_by_id: dict[str, EvidenceSnippet],
    ) -> dict[str, float]:
        evidence_text = " ".join(
            evidence_by_id[evidence_id].text
            for evidence_id in claim.evidence_ids
            if evidence_id in evidence_by_id
        )
        return _merge_weighted_features(
            _weighted_text_features(
                f"{claim.subject} {claim.value}",
                unigram_weight=1.8,
                bigram_weight=1.05,
                trigram_weight=0.18,
            ),
            _weighted_text_features(
                claim.predicate.replace("_", " "),
                unigram_weight=0.55,
                bigram_weight=0.3,
                trigram_weight=0.05,
            ),
            _weighted_text_features(
                claim.place or "",
                unigram_weight=1.2,
                bigram_weight=0.7,
                trigram_weight=0.12,
            ),
            _weighted_text_features(
                claim.viewpoint_scope or "",
                unigram_weight=1.1,
                bigram_weight=0.65,
                trigram_weight=0.12,
            ),
            _weighted_text_features(
                claim.notes or "",
                unigram_weight=0.85,
                bigram_weight=0.45,
                trigram_weight=0.08,
            ),
            _weighted_text_features(
                evidence_text,
                unigram_weight=0.3,
                bigram_weight=0.12,
                trigram_weight=0.03,
            ),
        )

    def _point_id(self, claim_id: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"sourcebound:{claim_id}"))

    def _embed(self, text: str) -> list[float]:
        return _embed_weighted_features(_weighted_text_features(text), self.VECTOR_SIZE)

    def _embed_features(self, features: dict[str, float]) -> list[float]:
        return _embed_weighted_features(features, self.VECTOR_SIZE)


class QdrantResearchSemanticAdapter:
    VECTOR_SIZE = QdrantProjectionAdapter.VECTOR_SIZE

    def __init__(self) -> None:
        self.enabled = settings.research_semantic_enabled
        self.url = settings.qdrant_url
        self.collection = settings.research_qdrant_collection

    def upsert_findings(self, findings: list[ResearchFinding], *, run_id: str) -> int:
        if not self.enabled or not findings:
            return 0
        try:
            from qdrant_client.models import PointStruct
        except Exception as exc:
            raise RuntimeError("Qdrant client is unavailable for research semantics.") from exc
        client = self._client()
        self._ensure_collection(client)

        points: list[PointStruct] = []
        for finding in findings:
            text = self._finding_text(finding)
            points.append(
                PointStruct(
                    id=self._point_id(finding.finding_id),
                    vector=self._embed_finding(finding),
                    payload={
                        "run_id": run_id,
                        "finding_id": finding.finding_id,
                        "facet_id": finding.facet_id,
                        "title": finding.title,
                        "canonical_url": finding.canonical_url,
                        "decision": finding.decision.value,
                        "text": text,
                    },
                )
            )
        client.upsert(collection_name=self.collection, points=points)
        return len(points)

    def search_similar_findings(
        self,
        finding: ResearchFinding,
        allowed_finding_ids: list[str],
        *,
        run_id: str,
        limit: int = 3,
    ) -> ResearchSemanticResult:
        if not self.enabled:
            return ResearchSemanticResult(
                fallback_used=True,
                fallback_reason="Research semantics are disabled.",
            )
        if not allowed_finding_ids:
            return ResearchSemanticResult()
        try:
            from qdrant_client.models import FieldCondition, Filter, MatchAny, MatchValue
        except Exception:
            return ResearchSemanticResult(
                fallback_used=True,
                fallback_reason="Qdrant client is unavailable.",
            )

        try:
            client = self._client()
            if not client.collection_exists(self.collection):
                return ResearchSemanticResult(
                    fallback_used=True,
                    fallback_reason="Research Qdrant collection is not initialized.",
                )
            response = client.query_points(
                collection_name=self.collection,
                query=self._embed_finding(finding),
                limit=min(limit, len(allowed_finding_ids)),
                query_filter=Filter(
                    must=[
                        FieldCondition(key="run_id", match=MatchValue(value=run_id)),
                        FieldCondition(key="facet_id", match=MatchValue(value=finding.facet_id)),
                        FieldCondition(key="finding_id", match=MatchAny(any=allowed_finding_ids)),
                    ]
                ),
                with_payload=True,
                with_vectors=False,
            )
        except Exception as exc:
            return ResearchSemanticResult(
                fallback_used=True,
                fallback_reason=f"Research Qdrant query failed: {exc}",
            )

        matches: list[ResearchSemanticMatch] = []
        for point in response.points:
            payload = point.payload or {}
            finding_id = payload.get("finding_id")
            if not finding_id:
                continue
            matches.append(
                ResearchSemanticMatch(
                    finding_id=str(finding_id),
                    similarity=round(float(point.score or 0.0), 4),
                    title=str(payload.get("title") or ""),
                    canonical_url=payload.get("canonical_url"),
                    decision=payload.get("decision"),
                )
            )
        if not matches:
            return ResearchSemanticResult(
                fallback_used=True,
                fallback_reason="Research Qdrant returned no usable hits.",
            )
        return ResearchSemanticResult(matches=matches)

    def _client(self):
        from qdrant_client import QdrantClient

        return QdrantClient(url=self.url)

    def runtime_probe(self) -> tuple[str, bool | None, bool, str]:
        if not self.enabled:
            return (
                "disabled",
                None,
                True,
                (
                    "Research semantic matching is disabled by default for local startup. "
                    "Research still works without it, but duplicate detection and reranking "
                    "stay lexical until you enable it."
                ),
            )
        try:
            client = self._client()
        except Exception:
            return (
                "qdrant:degraded",
                None,
                False,
                (
                    "Qdrant client is unavailable. Install the qdrant client dependency "
                    "and start Qdrant with `docker compose up -d qdrant` so research "
                    "semantic matching can run."
                ),
            )
        try:
            collection_ready = client.collection_exists(self.collection)
        except Exception as exc:
            return (
                "qdrant:degraded",
                False,
                False,
                f"Research semantics are configured but Qdrant is not queryable: {exc}. "
                "Start it with `docker compose up -d qdrant` and verify QDRANT_URL.",
            )
        if not collection_ready:
            return (
                "qdrant:uninitialized",
                True,
                False,
                (
                    f"Qdrant is reachable, but research collection '{self.collection}' is not "
                    "initialized. Run `saw seed-dev-data` or `saw qdrant-init` to create it."
                ),
            )
        return (
            "qdrant:ready",
            True,
            True,
            f"Research Qdrant collection '{self.collection}' is queryable.",
        )

    def initialize_collection(self) -> bool:
        if not settings.research_semantic_enabled:
            return False
        try:
            client = self._client()
            existed = client.collection_exists(self.collection)
            self._ensure_collection(client)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to initialize research Qdrant collection '{self.collection}': {exc}"
            ) from exc
        return not existed

    def _ensure_collection(self, client) -> None:
        from qdrant_client.models import Distance, VectorParams

        if client.collection_exists(self.collection):
            return
        client.create_collection(
            collection_name=self.collection,
            vectors_config=VectorParams(size=self.VECTOR_SIZE, distance=Distance.COSINE),
        )

    def _finding_text(self, finding: ResearchFinding) -> str:
        normalized_title = (
            finding.provenance.scoring.normalized_title if finding.provenance else finding.title
        )
        facet = finding.provenance.facet_label if finding.provenance else finding.facet_id
        excerpt = finding.page_excerpt or finding.snippet_text
        return f"{normalized_title}. {facet}. {excerpt}".strip()

    def _point_id(self, finding_id: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"sourcebound:research:{finding_id}"))

    def _embed(self, text: str) -> list[float]:
        return _embed_weighted_features(_weighted_text_features(text), self.VECTOR_SIZE)

    def _embed_finding(self, finding: ResearchFinding) -> list[float]:
        normalized_title = (
            finding.provenance.scoring.normalized_title if finding.provenance else finding.title
        )
        facet = finding.provenance.facet_label if finding.provenance else finding.facet_id
        excerpt = finding.page_excerpt or finding.snippet_text or ""
        source_type = finding.source_type or ""
        features = _merge_weighted_features(
            _weighted_text_features(
                normalized_title or "",
                unigram_weight=2.6,
                bigram_weight=1.2,
                trigram_weight=0.3,
            ),
            _weighted_text_features(
                excerpt,
                unigram_weight=1.2,
                bigram_weight=0.75,
                trigram_weight=0.15,
            ),
            _weighted_text_features(
                facet or "",
                unigram_weight=0.45,
                bigram_weight=0.2,
                trigram_weight=0.0,
            ),
            _weighted_text_features(
                source_type,
                unigram_weight=0.2,
                bigram_weight=0.0,
                trigram_weight=0.0,
            ),
        )
        return _embed_weighted_features(features, self.VECTOR_SIZE)

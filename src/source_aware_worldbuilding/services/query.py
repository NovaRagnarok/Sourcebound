from __future__ import annotations

from collections import defaultdict

from source_aware_worldbuilding.domain.enums import QueryMode
from source_aware_worldbuilding.domain.models import (
    ClaimRelationship,
    ProjectionSearchResult,
    QueryRequest,
    QueryResult,
    QueryResultMetadata,
)
from source_aware_worldbuilding.ports import (
    EvidenceStorePort,
    ProjectionPort,
    SourceStorePort,
    TruthStorePort,
)


class QueryService:
    def __init__(
        self,
        truth_store: TruthStorePort,
        evidence_store: EvidenceStorePort,
        source_store: SourceStorePort,
        projection: ProjectionPort | None = None,
    ):
        self.truth_store = truth_store
        self.evidence_store = evidence_store
        self.source_store = source_store
        self.projection = projection

    def answer(self, request: QueryRequest) -> QueryResult:
        claims = self.truth_store.list_claims()
        relationship_index = self._relationship_index(claims)
        if request.filters:
            if request.filters.status:
                claims = [c for c in claims if c.status == request.filters.status]
            if request.filters.claim_kind:
                claims = [c for c in claims if c.claim_kind == request.filters.claim_kind]
            if request.filters.place:
                claims = [c for c in claims if c.place == request.filters.place]
            if request.filters.viewpoint_scope:
                claims = [c for c in claims if c.viewpoint_scope == request.filters.viewpoint_scope]

        projection_result = self._search_projection(request.question, claims)
        if projection_result is not None and not projection_result.fallback_used:
            matched = self._claims_from_projection(claims, projection_result.claim_ids)
        else:
            matched = self._rank_claims(request.question, claims, relationship_index)
        if not matched:
            matched = claims[:5]

        warnings: list[str] = []
        metadata = QueryResultMetadata(retrieval_backend="memory")
        if projection_result is not None and not projection_result.fallback_used:
            metadata = QueryResultMetadata(retrieval_backend="qdrant")
        elif projection_result is not None and projection_result.fallback_used:
            metadata = QueryResultMetadata(
                retrieval_backend="memory",
                fallback_used=True,
                fallback_reason=projection_result.fallback_reason,
            )
            if projection_result.fallback_reason:
                warnings.append(f"Qdrant fallback: {projection_result.fallback_reason}")

        if request.mode == QueryMode.STRICT_FACTS:
            matched = [c for c in matched if c.status.value in {"verified", "probable"}]
            warnings.append("Strict facts mode hides rumor and legend by design.")
        elif request.mode == QueryMode.CONTESTED_VIEWS:
            matched = [c for c in matched if c.status.value == "contested"] or matched
            warnings.append("Contested views mode prefers disputed claims.")
        elif request.mode == QueryMode.RUMOR_AND_LEGEND:
            matched = [c for c in matched if c.status.value in {"rumor", "legend"}] or matched
            warnings.append("Rumor and legend mode surfaces low-certainty material intentionally.")
        elif request.mode == QueryMode.CHARACTER_KNOWLEDGE:
            warnings.append(
                "Character knowledge mode is a placeholder until viewpoint models are richer."
            )
        else:
            warnings.append("Open exploration mode may include mixed-certainty material.")

        evidence = []
        for claim in matched:
            for evidence_id in claim.evidence_ids:
                snippet = self.evidence_store.get_evidence(evidence_id)
                if snippet is not None:
                    evidence.append(snippet)

        related_claims = self._related_claims_for(matched)
        if any(item.relationship_type == "contradicts" for item in related_claims):
            warnings.append("Some returned claims have explicit contradictions in canon.")
        if any(item.relationship_type == "supersedes" for item in related_claims):
            warnings.append("Some returned claims supersede earlier canonical claims.")

        source_ids = {item.source_id for item in evidence}
        sources = [
            source
            for source_id in source_ids
            if (source := self.source_store.get_source(source_id))
        ]

        if matched:
            answer_lines = [
                (
                    f"- {claim.subject}: {claim.predicate} -> {claim.value} "
                    f"[{claim.status.value}] "
                    f"(evidence: {', '.join(claim.evidence_ids) or 'none'})"
                )
                for claim in matched[:5]
            ]
            answer = "\n".join(answer_lines)
        else:
            answer = (
                "No approved claims matched the request. Treat this as a research gap, "
                "not as permission to guess."
            )

        return QueryResult(
            question=request.question,
            mode=request.mode,
            answer=answer,
            supporting_claims=matched[:5],
            related_claims=related_claims,
            evidence=evidence[:10],
            sources=sources,
            warnings=warnings,
            metadata=metadata,
        )

    def _search_projection(self, question: str, claims) -> ProjectionSearchResult | None:
        if self.projection is None or not claims:
            return None
        return self.projection.search_claim_ids(
            question,
            [claim.claim_id for claim in claims],
            limit=min(10, len(claims)),
        )

    def _claims_from_projection(self, claims, claim_ids: list[str]):
        claim_by_id = {claim.claim_id: claim for claim in claims}
        ordered = [claim_by_id[claim_id] for claim_id in claim_ids if claim_id in claim_by_id]
        if ordered:
            return ordered
        return self._rank_claims("", claims, {})

    def _rank_claims(self, question: str, claims, relationship_index):
        question_lower = question.lower().strip()
        tokens = [token for token in question.lower().split() if token]
        if not tokens:
            return sorted(
                claims,
                key=lambda claim: (
                    self._relationship_score(claim.claim_id, relationship_index),
                    claim.claim_id,
                ),
                reverse=True,
            )

        scores = defaultdict(int)
        for claim in claims:
            haystack = " ".join(
                fragment or ""
                for fragment in [
                    claim.subject,
                    claim.predicate,
                    claim.value,
                    claim.notes,
                    claim.place,
                ]
            ).lower()
            if question_lower and question_lower in haystack:
                scores[claim.claim_id] += max(3, len(tokens))
            for token in tokens:
                if token in haystack:
                    scores[claim.claim_id] += 1
        ranked = sorted(
            claims,
            key=lambda claim: (
                scores[claim.claim_id],
                self._relationship_score(claim.claim_id, relationship_index),
                claim.claim_id,
            ),
            reverse=True,
        )
        strongest_score = max((scores[claim.claim_id] for claim in ranked), default=0)
        if strongest_score <= 0:
            return []
        return [claim for claim in ranked if scores[claim.claim_id] == strongest_score]

    def _relationship_index(self, claims) -> dict[str, list[ClaimRelationship]]:
        if not claims:
            return {}
        allowed = {claim.claim_id for claim in claims}
        index: dict[str, list[ClaimRelationship]] = defaultdict(list)
        for relationship in self.truth_store.list_relationships():
            if relationship.claim_id in allowed:
                index[relationship.claim_id].append(relationship)
        return index

    def _relationship_score(
        self,
        claim_id: str,
        relationship_index: dict[str, list[ClaimRelationship]],
    ) -> int:
        relationships = relationship_index.get(claim_id, [])
        score = 0
        for relationship in relationships:
            if relationship.relationship_type == "supports":
                score += 2
            elif relationship.relationship_type == "supersedes":
                score += 3
            elif relationship.relationship_type == "contradicts":
                score -= 1
            elif relationship.relationship_type == "superseded_by":
                score -= 3
            if relationship.source_kind == "manual":
                score += 1
        return score

    def _related_claims_for(self, claims) -> list[ClaimRelationship]:
        if not claims:
            return []
        claim_ids = {claim.claim_id for claim in claims[:5]}
        relationships: list[ClaimRelationship] = []
        seen: set[tuple[str, str, str]] = set()
        for claim in claims[:5]:
            for relationship in self.truth_store.list_relationships(claim.claim_id):
                key = (
                    relationship.claim_id,
                    relationship.related_claim_id,
                    relationship.relationship_type,
                )
                if key in seen:
                    continue
                if relationship.related_claim_id not in claim_ids:
                    seen.add(key)
                    relationships.append(relationship)
                    continue
                seen.add(key)
                relationships.append(relationship)
        return relationships

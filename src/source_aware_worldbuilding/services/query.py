from __future__ import annotations

from source_aware_worldbuilding.domain.enums import QueryMode
from source_aware_worldbuilding.domain.models import QueryRequest, QueryResult
from source_aware_worldbuilding.ports import EvidenceStorePort, TruthStorePort


class QueryService:
    def __init__(self, truth_store: TruthStorePort, evidence_store: EvidenceStorePort):
        self.truth_store = truth_store
        self.evidence_store = evidence_store

    def answer(self, request: QueryRequest) -> QueryResult:
        claims = self.truth_store.list_claims()
        if request.filters:
            if request.filters.status:
                claims = [c for c in claims if c.status == request.filters.status]
            if request.filters.claim_kind:
                claims = [c for c in claims if c.claim_kind == request.filters.claim_kind]
            if request.filters.place:
                claims = [c for c in claims if c.place == request.filters.place]
            if request.filters.viewpoint_scope:
                claims = [c for c in claims if c.viewpoint_scope == request.filters.viewpoint_scope]

        question_lower = request.question.lower()
        matched = [
            claim
            for claim in claims
            if question_lower in claim.subject.lower()
            or question_lower in claim.predicate.lower()
            or question_lower in claim.value.lower()
        ]
        if not matched:
            matched = claims[:5]

        warnings: list[str] = []
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
            warnings.append("Character knowledge mode is a placeholder until viewpoint models are richer.")
        else:
            warnings.append("Open exploration mode may include mixed-certainty material.")

        evidence = []
        for claim in matched:
            for evidence_id in claim.evidence_ids:
                snippet = self.evidence_store.get_evidence(evidence_id)
                if snippet is not None:
                    evidence.append(snippet)

        if matched:
            answer_lines = [
                f"- {claim.subject} — {claim.predicate} — {claim.value} [{claim.status.value}]"
                for claim in matched[:5]
            ]
            answer = "\n".join(answer_lines)
        else:
            answer = "No approved claims matched the request. Treat this as a research gap, not as permission to guess."

        return QueryResult(
            question=request.question,
            mode=request.mode,
            answer=answer,
            supporting_claims=matched[:5],
            evidence=evidence[:10],
            warnings=warnings,
        )

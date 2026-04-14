from __future__ import annotations

import re
from collections import defaultdict
from hashlib import sha1

from source_aware_worldbuilding.domain.enums import QueryMode
from source_aware_worldbuilding.domain.models import (
    AnswerSection,
    ApprovedClaim,
    ClaimCluster,
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
    _CLUSTER_MATCH_LIMIT = 10
    _CLUSTER_RELATIONSHIP_TYPES = {
        "supports",
        "contradicts",
        "supersedes",
        "superseded_by",
    }
    _CERTAINTY_RANK = {
        "verified": 5,
        "author_choice": 4,
        "probable": 3,
        "contested": 2,
        "rumor": 1,
        "legend": 0,
    }
    _QUESTION_STOPWORDS = {
        "a",
        "an",
        "and",
        "about",
        "do",
        "does",
        "for",
        "in",
        "is",
        "of",
        "on",
        "the",
        "to",
        "what",
        "where",
        "which",
        "who",
    }

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
        if request.filters:
            if request.filters.status:
                claims = [c for c in claims if c.status == request.filters.status]
            if request.filters.claim_kind:
                claims = [c for c in claims if c.claim_kind == request.filters.claim_kind]
            if request.filters.place:
                claims = [c for c in claims if c.place == request.filters.place]
            if request.filters.viewpoint_scope:
                claims = [c for c in claims if c.viewpoint_scope == request.filters.viewpoint_scope]
        relationship_index = self._relationship_index(claims)

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

        cluster_seed_claims = matched[: self._CLUSTER_MATCH_LIMIT]
        related_claims = self._related_claims_for(cluster_seed_claims)
        claim_clusters, answer_sections = self._build_claim_clusters(
            cluster_seed_claims,
            related_claims,
            request.question,
            request.mode,
            relationship_index,
        )
        if any(item.relationship_type == "contradicts" for item in related_claims):
            warnings.append("Some returned claims have explicit contradictions in canon.")
        if any(item.relationship_type == "supersedes" for item in related_claims):
            warnings.append("Some returned claims supersede earlier canonical claims.")

        surfaced_claims = self._claims_for_clusters(cluster_seed_claims, claim_clusters)
        evidence = self._evidence_for_claims(surfaced_claims)
        source_ids = {item.source_id for item in evidence}
        sources = [
            source
            for source_id in source_ids
            if (source := self.source_store.get_source(source_id))
        ]

        if answer_sections:
            answer = "\n\n".join(section.text for section in answer_sections[:3])
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
            claim_clusters=claim_clusters,
            answer_sections=answer_sections,
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
        question_lower = self._normalize_text(question)
        tokens = self._question_tokens(question)
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
            )
            haystack_normalized = self._normalize_text(haystack)
            if question_lower and question_lower in haystack_normalized:
                scores[claim.claim_id] += max(3, len(tokens))
            for token in tokens:
                if token in haystack_normalized:
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

    def _claims_for_clusters(
        self,
        matched_claims: list[ApprovedClaim],
        claim_clusters: list[ClaimCluster],
    ) -> list[ApprovedClaim]:
        claim_by_id = {claim.claim_id: claim for claim in matched_claims}
        for cluster in claim_clusters:
            for claim_id in cluster.claim_ids:
                if claim_id in claim_by_id:
                    continue
                related_claim = self.truth_store.get_claim(claim_id)
                if related_claim is not None:
                    claim_by_id[claim_id] = related_claim
        return list(claim_by_id.values())

    def _evidence_for_claims(self, claims: list[ApprovedClaim]) -> list:
        evidence = []
        seen: set[str] = set()
        for claim in claims:
            for evidence_id in claim.evidence_ids:
                if evidence_id in seen:
                    continue
                snippet = self.evidence_store.get_evidence(evidence_id)
                if snippet is not None:
                    seen.add(evidence_id)
                    evidence.append(snippet)
        return evidence

    def _build_claim_clusters(
        self,
        matched_claims: list[ApprovedClaim],
        related_claims: list[ClaimRelationship],
        question: str,
        mode: QueryMode,
        relationship_index: dict[str, list[ClaimRelationship]],
    ) -> tuple[list[ClaimCluster], list[AnswerSection]]:
        if not matched_claims:
            return [], []

        matched_ids = {claim.claim_id for claim in matched_claims}
        claim_by_id = {claim.claim_id: claim for claim in matched_claims}
        graph: dict[str, set[str]] = defaultdict(set)
        cluster_edges: list[ClaimRelationship] = []

        for claim in matched_claims:
            graph.setdefault(claim.claim_id, set())

        for relationship in related_claims:
            if relationship.relationship_type not in self._CLUSTER_RELATIONSHIP_TYPES:
                continue
            if (
                relationship.claim_id not in matched_ids
                and relationship.related_claim_id not in matched_ids
            ):
                continue
            graph[relationship.claim_id].add(relationship.related_claim_id)
            graph[relationship.related_claim_id].add(relationship.claim_id)
            cluster_edges.append(relationship)
            if relationship.related_claim_id not in claim_by_id:
                related_claim = self.truth_store.get_claim(relationship.related_claim_id)
                if related_claim is not None:
                    claim_by_id[related_claim.claim_id] = related_claim
                    graph.setdefault(related_claim.claim_id, set())
            if relationship.claim_id not in claim_by_id:
                related_claim = self.truth_store.get_claim(relationship.claim_id)
                if related_claim is not None:
                    claim_by_id[related_claim.claim_id] = related_claim
                    graph.setdefault(related_claim.claim_id, set())

        components = self._connected_components(graph)
        clusters: list[ClaimCluster] = []
        sections: list[AnswerSection] = []

        for component in components:
            cluster_claims = [
                claim_by_id[claim_id] for claim_id in component if claim_id in claim_by_id
            ]
            if not cluster_claims:
                continue
            relationships = self._relationships_for_component(
                component,
                cluster_edges,
                relationship_index,
            )
            cluster_kind = self._cluster_kind(relationships)
            lead_claim = self._lead_claim(
                cluster_claims,
                mode,
                relationship_index,
                relationships,
            )
            summary = self._summarize_cluster(
                cluster_kind,
                lead_claim,
                cluster_claims,
                relationships,
            )
            relationship_types = sorted({item.relationship_type for item in relationships})
            cluster_id = self._cluster_id(
                [claim.claim_id for claim in cluster_claims],
                relationship_types,
            )
            clusters.append(
                ClaimCluster(
                    cluster_id=cluster_id,
                    lead_claim_id=lead_claim.claim_id,
                    claim_ids=[claim.claim_id for claim in cluster_claims],
                    relationship_types=relationship_types,
                    cluster_kind=cluster_kind,
                    summary=summary,
                )
            )
            sections.append(
                AnswerSection(
                    cluster_id=cluster_id,
                    heading=self._cluster_heading(cluster_kind),
                    text=summary,
                    claim_ids=[claim.claim_id for claim in cluster_claims],
                    cluster_kind=cluster_kind,
                )
            )

        ordered_pairs = sorted(
            zip(clusters, sections, strict=False),
            key=lambda item: self._cluster_sort_key(
                item[0],
                claim_by_id,
                relationship_index,
                question,
                mode,
            ),
            reverse=True,
        )
        if not ordered_pairs:
            return [], []
        ordered_clusters, ordered_sections = zip(*ordered_pairs, strict=False)
        return list(ordered_clusters), list(ordered_sections)

    def _cluster_id(self, claim_ids: list[str], relationship_types: list[str]) -> str:
        material = "|".join(sorted(claim_ids)) + "::" + "|".join(sorted(relationship_types))
        return f"cluster-{sha1(material.encode('utf-8')).hexdigest()[:12]}"

    def _connected_components(self, graph: dict[str, set[str]]) -> list[list[str]]:
        components: list[list[str]] = []
        seen: set[str] = set()
        for node in graph:
            if node in seen:
                continue
            stack = [node]
            component: list[str] = []
            seen.add(node)
            while stack:
                current = stack.pop()
                component.append(current)
                for neighbor in graph[current]:
                    if neighbor in seen:
                        continue
                    seen.add(neighbor)
                    stack.append(neighbor)
            components.append(sorted(component))
        return components

    def _relationships_for_component(
        self,
        component: list[str],
        cluster_edges: list[ClaimRelationship],
        relationship_index: dict[str, list[ClaimRelationship]],
    ) -> list[ClaimRelationship]:
        component_ids = set(component)
        relationships: list[ClaimRelationship] = []
        seen: set[tuple[str, str, str]] = set()
        for relationship in cluster_edges:
            if (
                relationship.claim_id not in component_ids
                or relationship.related_claim_id not in component_ids
            ):
                continue
            key = (
                relationship.claim_id,
                relationship.related_claim_id,
                relationship.relationship_type,
            )
            if key in seen:
                continue
            seen.add(key)
            relationships.append(relationship)
        for claim_id in component:
            for relationship in relationship_index.get(claim_id, []):
                if (
                    relationship.related_claim_id not in component_ids
                    or relationship.relationship_type not in self._CLUSTER_RELATIONSHIP_TYPES
                ):
                    continue
                key = (
                    relationship.claim_id,
                    relationship.related_claim_id,
                    relationship.relationship_type,
                )
                if key in seen:
                    continue
                seen.add(key)
                relationships.append(relationship)
        return relationships

    def _cluster_kind(self, relationships: list[ClaimRelationship]) -> str:
        relationship_types = {item.relationship_type for item in relationships}
        if "contradicts" in relationship_types:
            return "contested"
        if {"supersedes", "superseded_by"} & relationship_types:
            return "supersession"
        return "reinforcing"

    def _lead_claim(
        self,
        cluster_claims: list[ApprovedClaim],
        mode: QueryMode,
        relationship_index: dict[str, list[ClaimRelationship]],
        relationships: list[ClaimRelationship],
    ) -> ApprovedClaim:
        component_ids = {claim.claim_id for claim in cluster_claims}
        superseded_claim_ids = {
            relationship.claim_id
            for relationship in relationships
            if relationship.relationship_type == "superseded_by"
            and relationship.related_claim_id in component_ids
        }
        superseded_claim_ids.update(
            relationship.related_claim_id
            for relationship in relationships
            if relationship.relationship_type == "supersedes"
            and relationship.claim_id in component_ids
        )
        candidates = [
            claim for claim in cluster_claims if claim.claim_id not in superseded_claim_ids
        ]
        if not candidates:
            candidates = cluster_claims
        return max(
            candidates,
            key=lambda claim: (
                self._certainty_rank_for_mode(claim, mode),
                self._is_active_claim(claim, relationships),
                self._relationship_score(claim.claim_id, relationship_index),
                claim.author_choice,
                claim.claim_id,
            ),
        )

    def _certainty_rank_for_mode(self, claim: ApprovedClaim, mode: QueryMode) -> int:
        score = self._CERTAINTY_RANK.get(claim.status.value, 0)
        if mode == QueryMode.STRICT_FACTS:
            return score * 2
        return score

    def _is_active_claim(
        self,
        claim: ApprovedClaim,
        relationships: list[ClaimRelationship],
    ) -> int:
        for relationship in relationships:
            if (
                relationship.claim_id == claim.claim_id
                and relationship.relationship_type == "superseded_by"
            ):
                return 0
            if (
                relationship.related_claim_id == claim.claim_id
                and relationship.relationship_type == "supersedes"
            ):
                return 0
        return 1

    def _summarize_cluster(
        self,
        cluster_kind: str,
        lead_claim: ApprovedClaim,
        cluster_claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> str:
        evidence_label = self._describe_evidence(lead_claim)
        if cluster_kind == "reinforcing":
            reinforcing_claims = [
                claim for claim in cluster_claims if claim.claim_id != lead_claim.claim_id
            ]
            support_count = len(reinforcing_claims)
            if support_count == 0:
                return (
                    f"Canonical evidence indicates that "
                    f"{self._format_claim_text(lead_claim)} "
                    f"[{lead_claim.status.value}]. "
                    f"This reading is grounded in {evidence_label}."
                )
            return (
                f"Multiple canonical claims reinforce the point that "
                f"{self._format_claim_text(lead_claim)} [{lead_claim.status.value}]. "
                f"This reading is grounded in {evidence_label} and reinforced by "
                f"{support_count} additional linked claim"
                f"{'' if support_count == 1 else 's'}."
            )

        if cluster_kind == "contested":
            alternatives = [
                claim for claim in cluster_claims if claim.claim_id != lead_claim.claim_id
            ]
            alternative_text = "; ".join(
                f"{self._format_claim_text(claim)} [{claim.status.value}]"
                for claim in alternatives[:2]
            )
            summary = (
                f"Canonical claims disagree here. The strongest current line says "
                f"{self._format_claim_text(lead_claim)} [{lead_claim.status.value}]"
            )
            if alternative_text:
                summary += f", while competing canon says {alternative_text}"
            return summary + f". The lead position is grounded in {evidence_label}."

        current_claim = lead_claim
        older_claims = [
            claim
            for claim in cluster_claims
            if claim.claim_id != current_claim.claim_id
            and not self._is_active_claim(claim, relationships)
        ]
        if not older_claims:
            older_claims = [
                claim for claim in cluster_claims if claim.claim_id != current_claim.claim_id
            ]
        older_text = "; ".join(
            f"{self._format_claim_text(claim)} [{claim.status.value}]"
            for claim in older_claims[:2]
        )
        summary = (
            f"The current canonical position is that "
            f"{self._format_claim_text(current_claim)} "
            f"[{current_claim.status.value}]"
        )
        if older_text:
            summary += f". Older superseded canon said {older_text}"
        return summary + f". This current position is grounded in {evidence_label}."

    def _format_claim_text(self, claim: ApprovedClaim) -> str:
        predicate = claim.predicate.replace("_", " ")
        return f"{claim.subject} {predicate} {claim.value}"

    def _describe_evidence(self, claim: ApprovedClaim) -> str:
        if not claim.evidence_ids:
            return "no linked evidence"
        if len(claim.evidence_ids) == 1:
            return f"evidence {claim.evidence_ids[0]}"
        return f"evidence {', '.join(claim.evidence_ids[:2])}"

    def _cluster_heading(self, cluster_kind: str) -> str:
        if cluster_kind == "reinforcing":
            return "Reinforced Canon"
        if cluster_kind == "contested":
            return "Contested Canon"
        return "Superseded Canon"

    def _cluster_sort_key(
        self,
        cluster: ClaimCluster,
        claim_by_id: dict[str, ApprovedClaim],
        relationship_index: dict[str, list[ClaimRelationship]],
        question: str,
        mode: QueryMode,
    ) -> tuple[int, int, int, str]:
        lead_claim = claim_by_id[cluster.lead_claim_id]
        kind_rank = self._cluster_kind_priority(cluster.cluster_kind, question, mode)
        relevance_rank = self._cluster_question_relevance(cluster, claim_by_id, question)
        return (
            kind_rank,
            relevance_rank,
            self._CERTAINTY_RANK.get(lead_claim.status.value, 0),
            self._relationship_score(lead_claim.claim_id, relationship_index),
            lead_claim.claim_id,
        )

    def _cluster_kind_priority(
        self,
        cluster_kind: str,
        question: str,
        mode: QueryMode,
    ) -> int:
        question_lower = question.lower()
        if mode == QueryMode.CONTESTED_VIEWS or any(
            token in question_lower
            for token in {"disagree", "contradict", "conflict", "versus", "vs", "debate"}
        ):
            return {
                "contested": 3,
                "supersession": 2,
                "reinforcing": 1,
            }[cluster_kind]
        if any(
            token in question_lower
            for token in {"current", "latest", "canonical", "active", "now", "superseded"}
        ):
            return {
                "supersession": 3,
                "reinforcing": 2,
                "contested": 1,
            }[cluster_kind]
        if mode == QueryMode.STRICT_FACTS:
            return {
                "reinforcing": 3,
                "supersession": 2,
                "contested": 1,
            }[cluster_kind]
        return {
            "reinforcing": 3,
            "contested": 2,
            "supersession": 1,
        }[cluster_kind]

    def _cluster_question_relevance(
        self,
        cluster: ClaimCluster,
        claim_by_id: dict[str, ApprovedClaim],
        question: str,
    ) -> int:
        tokens = set(self._question_tokens(question))
        if not tokens:
            return 0
        score = 0
        for claim_id in cluster.claim_ids:
            claim = claim_by_id.get(claim_id)
            if claim is None:
                continue
            haystack = " ".join(
                fragment or ""
                for fragment in [
                    claim.subject,
                    claim.predicate,
                    claim.value,
                    claim.notes,
                    claim.place,
                    claim.viewpoint_scope,
                ]
            )
            haystack = self._normalize_text(haystack)
            score += sum(1 for token in tokens if token in haystack)
        return score

    def _question_tokens(self, text: str) -> list[str]:
        return [
            token
            for token in re.findall(r"[a-z0-9]+", text.lower())
            if token not in self._QUESTION_STOPWORDS
        ]

    def _normalize_text(self, text: str) -> str:
        return " ".join(re.findall(r"[a-z0-9]+", text.lower()))

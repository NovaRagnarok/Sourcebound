from __future__ import annotations

import re
from collections import Counter, defaultdict
from hashlib import sha1

from source_aware_worldbuilding.domain.enums import QueryMode
from source_aware_worldbuilding.domain.models import (
    AnswerSection,
    ApprovedClaim,
    BibleProjectProfile,
    ClaimCluster,
    ClaimKind,
    ClaimRelationship,
    ProjectionSearchResult,
    QueryRequest,
    QueryResult,
    QueryResultMetadata,
)
from source_aware_worldbuilding.ports import (
    BibleProjectProfileStorePort,
    EvidenceStorePort,
    ProjectionPort,
    SourceStorePort,
    TruthStorePort,
)


class QueryService:
    _CLUSTER_MATCH_LIMIT = 10
    _CLUSTER_RELATIONSHIP_TYPES = {
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
        "how",
        "were",
        "still",
        "say",
        "handle",
        "handled",
        "record",
        "claims",
        "claim",
        "tell",
        "me",
    }

    def __init__(
        self,
        truth_store: TruthStorePort,
        evidence_store: EvidenceStorePort,
        source_store: SourceStorePort,
        projection: ProjectionPort | None = None,
        profile_store: BibleProjectProfileStorePort | None = None,
    ):
        self.truth_store = truth_store
        self.evidence_store = evidence_store
        self.source_store = source_store
        self.projection = projection
        self.profile_store = profile_store

    def answer(self, request: QueryRequest) -> QueryResult:
        profile = self._profile_for_request(request)
        claims = self.truth_store.list_claims()
        claims = self._apply_filters(claims, request.filters)
        relationship_index = self._relationship_index(claims)
        question_profile = self._question_profile(request)

        projection_result = self._search_projection(request, claims, profile)
        ranked_matches = self._rank_claims(
            request,
            claims,
            relationship_index,
            projection_result,
            profile=profile,
            question_profile=question_profile,
        )

        warnings: list[str] = []
        ranking_strategy = "intent_blended" if profile is not None else "lexical"
        metadata = QueryResultMetadata(
            retrieval_backend="memory",
            ranking_strategy=ranking_strategy,
            retrieval_quality_tier="memory_ranked",
        )
        if (
            projection_result is not None
            and not projection_result.fallback_used
            and profile is None
        ):
            metadata = QueryResultMetadata(
                retrieval_backend="qdrant",
                ranking_strategy="blended",
                retrieval_quality_tier="projection",
            )
        elif (
            projection_result is not None
            and not projection_result.fallback_used
            and profile is not None
        ):
            metadata = QueryResultMetadata(
                retrieval_backend="qdrant",
                ranking_strategy="intent_blended",
                retrieval_quality_tier="projection",
            )
        elif projection_result is not None and projection_result.fallback_used:
            metadata = QueryResultMetadata(
                retrieval_backend="memory",
                fallback_used=True,
                fallback_reason=projection_result.fallback_reason,
                ranking_strategy=ranking_strategy,
                retrieval_quality_tier="memory_ranked",
            )
            if projection_result.fallback_reason:
                warnings.append(f"Qdrant fallback: {projection_result.fallback_reason}")

        direct_matches = self._direct_topic_claims(
            claims,
            request,
            question_profile=question_profile,
        )
        matched, direct_match_ids, adjacent_context_ids, boundary_warnings = (
            self._resolve_answer_claims(
                request,
                ranked_matches,
                direct_matches,
                question_profile=question_profile,
            )
        )
        warnings.extend(boundary_warnings)

        if request.mode == QueryMode.STRICT_FACTS:
            warnings.append("Strict facts mode hides rumor and legend by design.")
        elif request.mode == QueryMode.CONTESTED_VIEWS:
            warnings.append("Contested views mode prefers disputed claims.")
        elif request.mode == QueryMode.RUMOR_AND_LEGEND:
            warnings.append("Rumor and legend mode surfaces low-certainty material intentionally.")
        elif request.mode == QueryMode.CHARACTER_KNOWLEDGE:
            if self._has_viewpoint_grounding(matched, request, profile):
                warnings.append(
                    "Character knowledge mode preferred canon that matches the requested or inferred viewpoint."
                )
            else:
                warnings.append(
                    "Character knowledge mode found only limited viewpoint grounding; treat the answer as adjacent occupational canon."
                )
        else:
            warnings.append("Open exploration mode may include mixed-certainty material.")
        if self._is_disagreement_question(request.question):
            contradictory_ids = {
                relationship.claim_id
                for relationship in self.truth_store.list_relationships()
                if relationship.relationship_type == "contradicts"
            } | {
                relationship.related_claim_id
                for relationship in self.truth_store.list_relationships()
                if relationship.relationship_type == "contradicts"
            }
            contested_pool = [
                c
                for c in claims
                if c.status.value == "contested" or c.claim_id in contradictory_ids
            ]
            contested_matches = self._rank_claims(
                request,
                contested_pool,
                relationship_index,
                profile=profile,
                question_profile=question_profile,
            )
            disagreement_first = contested_matches or [
                c
                for c in matched
                if c.status.value == "contested" or c.claim_id in contradictory_ids
            ]
            if disagreement_first:
                matched = disagreement_first + [
                    c
                    for c in matched
                    if c.claim_id not in {item.claim_id for item in disagreement_first}
                ]
                if not direct_match_ids:
                    adjacent_context_ids = [
                        claim.claim_id
                        for claim in disagreement_first
                        if claim.claim_id not in direct_match_ids
                    ]

        cluster_seed_claims = matched[: self._CLUSTER_MATCH_LIMIT]
        related_claims = self._related_claims_for(cluster_seed_claims)
        claim_clusters, answer_sections = self._build_claim_clusters(
            cluster_seed_claims,
            related_claims,
            request,
            relationship_index,
            profile=profile,
            question_profile=question_profile,
        )
        claim_clusters, answer_sections, related_claims, topical_cap_applied = (
            self._apply_topical_cap(
                claim_clusters,
                answer_sections,
                related_claims,
                request,
                question_profile,
            )
        )
        if topical_cap_applied:
            warnings.append(
                "Answer focus was narrowed to the strongest topical canon cluster; "
                "unrelated canon was left out of the assembled answer."
            )
        if any(item.relationship_type == "contradicts" for item in related_claims):
            warnings.append("Some returned claims have explicit contradictions in canon.")
        if any(item.relationship_type == "supersedes" for item in related_claims):
            warnings.append("Some returned claims supersede earlier canonical claims.")

        if claim_clusters:
            kept_claim_ids = {
                claim_id for cluster in claim_clusters for claim_id in cluster.claim_ids
            }
            matched = [claim for claim in matched if claim.claim_id in kept_claim_ids] or matched
        cluster_seed_claims = matched[: self._CLUSTER_MATCH_LIMIT]
        surfaced_claims = self._claims_for_clusters(cluster_seed_claims, claim_clusters)
        contradiction_flags = [
            f"{claim.subject} contradicts claim {relationship.related_claim_id}"
            for relationship in related_claims
            if relationship.relationship_type == "contradicts"
            for claim in surfaced_claims
            if claim.claim_id == relationship.claim_id
        ]
        evidence = self._evidence_for_claims(surfaced_claims)
        sources = self._sources_for_evidence(evidence)
        nearby_claims = self._nearby_claims(
            request,
            ranked_matches,
            matched,
            direct_match_ids,
            answer_boundary=self._answer_boundary(
                matched,
                direct_match_ids,
                adjacent_context_ids,
            ),
        )

        answer_boundary = self._answer_boundary(
            matched,
            direct_match_ids,
            adjacent_context_ids,
        )
        metadata.answer_boundary = answer_boundary
        metadata.used_nearby_context = bool(adjacent_context_ids)

        if answer_sections and answer_boundary == "adjacent_context":
            answer = (
                "Approved canon does not directly answer this question, but it does offer "
                "nearby context that may help.\n\n"
                + "\n\n".join(section.text for section in answer_sections[:3])
            )
        elif answer_sections:
            answer = "\n\n".join(section.text for section in answer_sections[:3])
        elif answer_boundary == "research_gap":
            answer = (
                "Approved canon does not directly answer this question yet. "
                "Treat the missing detail as a research gap instead of filling it "
                "with nearby canon."
            )
        else:
            answer = (
                "No approved claims matched the request. Treat this as a research gap, "
                "not as permission to guess."
            )
        certainty_summary = dict(Counter(claim.status.value for claim in matched))
        coverage_gaps = self._coverage_gaps(matched, request)
        recommended_next_research = self._recommended_next_research(coverage_gaps, request)
        suggested_follow_ups = self._suggested_follow_ups(
            request,
            nearby_claims,
            coverage_gaps,
        )

        return QueryResult(
            question=request.question,
            mode=request.mode,
            answer=answer,
            supporting_claims=matched[:5],
            nearby_claims=nearby_claims,
            related_claims=related_claims,
            claim_clusters=claim_clusters,
            answer_sections=answer_sections,
            evidence=evidence[:10],
            sources=sources,
            warnings=warnings,
            certainty_summary=certainty_summary,
            coverage_gaps=coverage_gaps,
            contradiction_flags=contradiction_flags,
            recommended_next_research=recommended_next_research,
            suggested_follow_ups=suggested_follow_ups,
            direct_match_claim_ids=direct_match_ids,
            adjacent_context_claim_ids=adjacent_context_ids,
            metadata=metadata,
        )

    def _resolve_answer_claims(
        self,
        request: QueryRequest,
        ranked_matches: list[ApprovedClaim],
        direct_matches: list[ApprovedClaim],
        *,
        question_profile: dict[str, bool | list[str]],
    ) -> tuple[list[ApprovedClaim], list[str], list[str], list[str]]:
        warnings: list[str] = []
        direct_ids = [claim.claim_id for claim in direct_matches]
        direct_id_set = set(direct_ids)
        if request.mode == QueryMode.STRICT_FACTS:
            fact_direct = [
                claim
                for claim in direct_matches
                if claim.status.value in {"verified", "probable"}
            ]
            if fact_direct:
                matched = self._augment_direct_matches_with_linked_claims(
                    fact_direct[:3],
                    ranked_matches,
                )
                direct_ids = [claim.claim_id for claim in fact_direct[:3]]
                adjacent_ids = [
                    claim.claim_id for claim in matched if claim.claim_id not in set(direct_ids)
                ]
                return matched, direct_ids, adjacent_ids, warnings
            warnings.append(
                "Approved canon does not directly answer this narrow question yet; adjacent canon was not substituted."
            )
            return [], [], [], warnings

        if request.mode == QueryMode.CONTESTED_VIEWS:
            contested_matches = [
                claim
                for claim in ranked_matches
                if claim.status.value == "contested"
                or claim.claim_id in {
                    relationship.claim_id
                    for relationship in self.truth_store.list_relationships()
                    if relationship.relationship_type == "contradicts"
                }
                or claim.claim_id in {
                    relationship.related_claim_id
                    for relationship in self.truth_store.list_relationships()
                    if relationship.relationship_type == "contradicts"
                }
            ]
            direct_contested = [
                claim for claim in contested_matches if claim.claim_id in direct_id_set
            ]
            if direct_contested:
                matched = self._augment_direct_matches_with_linked_claims(
                    direct_contested[:3],
                    contested_matches,
                )
                direct_ids = [claim.claim_id for claim in direct_contested[:3]]
                adjacent_ids = [
                    claim.claim_id for claim in matched if claim.claim_id not in set(direct_ids)
                ]
                return matched, direct_ids, adjacent_ids, warnings
            if contested_matches:
                warnings.append(
                    "Approved canon offers contested context here, but not a direct disputed answer."
                )
                nearby = contested_matches[:4]
                return nearby, [], [claim.claim_id for claim in nearby], warnings
            return [], [], [], warnings

        if request.mode == QueryMode.RUMOR_AND_LEGEND:
            low_certainty = [
                claim
                for claim in ranked_matches
                if claim.status.value in {"rumor", "legend", "contested"}
            ]
            direct_low = [claim for claim in low_certainty if claim.claim_id in direct_id_set]
            if direct_low:
                matched = self._augment_direct_matches_with_linked_claims(
                    direct_low[:3],
                    low_certainty,
                )
                direct_ids = [claim.claim_id for claim in direct_low[:3]]
                adjacent_ids = [
                    claim.claim_id for claim in matched if claim.claim_id not in set(direct_ids)
                ]
                return matched, direct_ids, adjacent_ids, warnings
            if low_certainty:
                warnings.append(
                    "Approved canon did not contain a direct rumor/legend hit, so nearby low-certainty canon was surfaced explicitly."
                )
                return low_certainty[:4], [], [claim.claim_id for claim in low_certainty[:4]], warnings
            return [], [], [], warnings

        if direct_matches:
            matched = self._augment_direct_matches_with_linked_claims(
                direct_matches[:3],
                ranked_matches,
            )
            direct_ids = [claim.claim_id for claim in direct_matches[:3]]
            adjacent_ids = [
                claim.claim_id for claim in matched if claim.claim_id not in set(direct_ids)
            ]
            return matched, direct_ids, adjacent_ids, warnings
        if ranked_matches:
            warnings.append(
                "Approved canon did not contain a direct answer, so nearby canon was surfaced explicitly."
            )
            nearby = ranked_matches[:4]
            return nearby, [], [claim.claim_id for claim in nearby], warnings
        return [], [], [], warnings

    def _has_viewpoint_grounding(
        self,
        claims: list[ApprovedClaim],
        request: QueryRequest,
        profile: BibleProjectProfile | None,
    ) -> bool:
        if not claims:
            return False
        requested_scope = self._normalize_text(request.filters.viewpoint_scope) if request.filters and request.filters.viewpoint_scope else ""
        inferred_scope = self._normalize_text(profile.social_lens) if profile and profile.social_lens else ""
        scope_hints = {hint for hint in [requested_scope, inferred_scope] if hint}
        if not scope_hints:
            return any(claim.viewpoint_scope for claim in claims)
        for claim in claims:
            claim_scope = self._normalize_text(claim.viewpoint_scope or "")
            if any(hint in claim_scope or claim_scope in hint for hint in scope_hints if claim_scope):
                return True
        return False

    def _answer_boundary(
        self,
        matched: list[ApprovedClaim],
        direct_match_ids: list[str],
        adjacent_context_ids: list[str],
    ) -> str:
        if not matched:
            return "research_gap"
        if direct_match_ids:
            return "direct_answer"
        if adjacent_context_ids:
            return "adjacent_context"
        return "research_gap"

    def _apply_filters(self, claims, filters):
        if filters is None:
            return claims
        filtered = list(claims)
        if filters.status:
            filtered = [c for c in filtered if c.status == filters.status]
        if filters.include_statuses:
            allowed = set(filters.include_statuses)
            filtered = [c for c in filtered if c.status in allowed]
        if filters.claim_kind:
            filtered = [c for c in filtered if c.claim_kind == filters.claim_kind]
        if filters.place:
            filtered = [c for c in filtered if c.place == filters.place]
        if filters.viewpoint_scope:
            filtered = [c for c in filtered if c.viewpoint_scope == filters.viewpoint_scope]
        if filters.time_start:
            filtered = [c for c in filtered if not c.time_end or c.time_end >= filters.time_start]
        if filters.time_end:
            filtered = [c for c in filtered if not c.time_start or c.time_start <= filters.time_end]
        if filters.relationship_types:
            allowed_ids = {
                item.claim_id
                for item in self.truth_store.list_relationships()
                if item.relationship_type in filters.relationship_types
            }
            filtered = [c for c in filtered if c.claim_id in allowed_ids]
        if filters.source_types:
            filtered = [
                c
                for c in filtered
                if any(
                    (source := self.source_store.get_source(snippet.source_id))
                    and source.source_type in filters.source_types
                    for evidence_id in c.evidence_ids
                    if (snippet := self.evidence_store.get_evidence(evidence_id)) is not None
                )
            ]
        return filtered

    def _coverage_gaps(self, claims, request: QueryRequest) -> list[str]:
        gaps: list[str] = []
        if not claims:
            if self._question_profile(request)["hard_cap"]:
                return ["Approved canon does not directly answer the question yet."]
            return ["No approved claims matched the question."]
        if request.mode == QueryMode.STRICT_FACTS and not any(
            claim.status.value == "verified" for claim in claims
        ):
            gaps.append("No verified claims matched; answer relies on probable canon.")
        if not any(claim.evidence_ids for claim in claims):
            gaps.append("Matching claims lack linked evidence.")
        if not any(claim.time_start or claim.time_end for claim in claims):
            gaps.append("Matching claims are weakly anchored in time.")
        return gaps

    def _recommended_next_research(self, gaps: list[str], request: QueryRequest) -> list[str]:
        prompts: list[str] = []
        for gap in gaps:
            lower = gap.lower()
            if "does not directly answer" in lower:
                prompts.append(f"Find directly documented evidence for: {request.question}")
            elif "verified" in lower:
                prompts.append(
                    f"Find record-like or archival sources that verify: {request.question}"
                )
            elif "time" in lower:
                prompts.append(f"Find dated sources that anchor: {request.question}")
            elif "evidence" in lower:
                prompts.append(f"Find better-cited sources for: {request.question}")
        return prompts

    def _nearby_claims(
        self,
        request: QueryRequest,
        ranked_matches: list[ApprovedClaim],
        matched: list[ApprovedClaim],
        direct_match_ids: list[str],
        *,
        answer_boundary: str,
    ) -> list[ApprovedClaim]:
        direct_ids = set(direct_match_ids)
        if answer_boundary == "adjacent_context":
            nearby = [claim for claim in matched if claim.claim_id not in direct_ids]
            return nearby[:3]
        if answer_boundary == "research_gap":
            if request.mode == QueryMode.STRICT_FACTS:
                return [
                    claim
                    for claim in ranked_matches
                    if claim.status.value in {"verified", "probable"}
                ][:3]
            return ranked_matches[:3]
        return [claim for claim in matched if claim.claim_id not in direct_ids][:3]

    def _suggested_follow_ups(
        self,
        request: QueryRequest,
        nearby_claims: list[ApprovedClaim],
        coverage_gaps: list[str],
    ) -> list[str]:
        suggestions: list[str] = []
        if nearby_claims:
            lead = nearby_claims[0]
            if lead.place:
                suggestions.append(f"What approved canon do we have for {lead.place} that is directly tied to this scene?")
            suggestions.append(f"What directly documented detail would confirm or reject {lead.subject.lower()}?")
            if lead.time_start or lead.time_end:
                anchor = lead.time_start or lead.time_end
                suggestions.append(f"What else is firmly dated around {anchor}?")
        for gap in coverage_gaps:
            lower = gap.lower()
            if "does not directly answer" in lower:
                suggestions.append("What specific detail is missing enough that it should trigger research before drafting?")
            elif "verified" in lower:
                suggestions.append("Which part of this answer still rests on probable rather than verified canon?")
        seen: set[str] = set()
        deduped: list[str] = []
        for item in suggestions:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped[:4]

    def _search_projection(
        self,
        request: QueryRequest,
        claims,
        profile: BibleProjectProfile | None,
    ) -> ProjectionSearchResult | None:
        if self.projection is None or not claims:
            return None
        question = self._projection_query_text(request, profile)
        return self.projection.search_claim_ids(
            question,
            [claim.claim_id for claim in claims],
            limit=min(10, len(claims)),
        )

    def _rank_claims(
        self,
        request: QueryRequest,
        claims,
        relationship_index,
        projection_result=None,
        *,
        profile: BibleProjectProfile | None = None,
        question_profile: dict[str, bool] | None = None,
    ):
        profile_data = question_profile or self._question_profile(request)
        question_lower = self._normalize_text(request.question)
        tokens = profile_data["tokens"]
        bigrams = self._question_bigrams(tokens)
        intent_text = self._projection_query_text(request, profile)
        intent_tokens = self._question_tokens(intent_text)
        mentioned_places = {
            self._normalize_text(claim.place)
            for claim in claims
            if claim.place and self._normalize_text(claim.place) in question_lower
        }
        projection_order = (
            {
                claim_id: len(projection_result.claim_ids) - index
                for index, claim_id in enumerate(projection_result.claim_ids)
            }
            if projection_result is not None and not projection_result.fallback_used
            else {}
        )
        if not tokens:
            return sorted(
                claims,
                key=lambda claim: (
                    projection_order.get(claim.claim_id, 0),
                    self._relationship_score(claim.claim_id, relationship_index),
                    claim.claim_id,
                ),
                reverse=True,
            )

        topic_scores = defaultdict(int)
        intent_scores = defaultdict(int)
        for claim in claims:
            haystack_normalized = self._claim_haystack(claim)
            topic_scores[claim.claim_id] += self._topic_match_score(
                claim,
                question_lower,
                tokens,
                bigrams,
                haystack_normalized,
                mentioned_places,
                profile_data,
            )
            topic_scores[claim.claim_id] += self._project_intent_score(
                claim,
                request,
                profile,
                intent_tokens,
                haystack_normalized,
            )
            topic_scores[claim.claim_id] += self._filter_alignment_score(claim, request)
            intent_scores[claim.claim_id] += self._claim_intent_bonus(
                claim,
                relationship_index,
                profile_data,
            )
        affinity_scores = defaultdict(int)
        overlap_counts = defaultdict(int)
        ranked = sorted(
            claims,
            key=lambda claim: (
                topic_scores[claim.claim_id]
                if profile_data["topic_first"]
                else projection_order.get(claim.claim_id, 0),
                intent_scores[claim.claim_id],
                projection_order.get(claim.claim_id, 0)
                if profile_data["topic_first"]
                else topic_scores[claim.claim_id],
                self._canonical_strength(claim, relationship_index),
                claim.claim_id,
            ),
            reverse=True,
        )
        if profile_data["topic_first"] and ranked:
            anchor_claim = ranked[0]
            for claim in ranked:
                affinity_scores[claim.claim_id] = self._claim_affinity_score(anchor_claim, claim)
                overlap_counts[claim.claim_id] = self._claim_overlap_count(anchor_claim, claim)
            ranked = sorted(
                ranked,
                key=lambda claim: (
                    topic_scores[claim.claim_id],
                    affinity_scores[claim.claim_id],
                    intent_scores[claim.claim_id],
                    projection_order.get(claim.claim_id, 0),
                    self._canonical_strength(claim, relationship_index),
                    claim.claim_id,
                ),
                reverse=True,
            )
        strongest_score = max((topic_scores[claim.claim_id] for claim in ranked), default=0)
        if strongest_score <= 0 and not projection_order:
            return []
        if strongest_score <= 0 and projection_order:
            return ranked
        if profile_data["topic_first"]:
            threshold = max(6, strongest_score - 7)
        elif len(tokens) <= 3:
            threshold = strongest_score
        else:
            threshold = max(2, strongest_score - 3)
        relevant_claims = [
            claim
            for claim in ranked
            if topic_scores[claim.claim_id] >= threshold
            or (
                profile_data["topic_first"]
                and affinity_scores[claim.claim_id] >= 8
                and overlap_counts[claim.claim_id] >= (1 if profile_data["rumor_focus"] else 3)
                and (
                    not mentioned_places
                    or not claim.place
                    or self._normalize_text(claim.place) in mentioned_places
                )
                and topic_scores[claim.claim_id] >= max(3, strongest_score - 30)
            )
        ]
        if len(relevant_claims) < 2:
            if profile_data["topic_first"]:
                return ranked[: min(4, len(ranked))]
            if len(tokens) > 4:
                return ranked[:2]
            return relevant_claims or ranked[:1]
        if profile_data["hard_cap"]:
            return relevant_claims[:6]
        if len(tokens) > 3 and len(relevant_claims) < 5:
            return ranked[:5]
        return relevant_claims[:8]

    def _profile_for_request(self, request: QueryRequest) -> BibleProjectProfile | None:
        if self.profile_store is None or not request.project_id:
            return None
        return self.profile_store.get_profile(request.project_id)

    def _projection_query_text(
        self,
        request: QueryRequest,
        profile: BibleProjectProfile | None,
    ) -> str:
        parts = [request.question]
        filters = request.filters
        if filters and filters.place:
            parts.append(filters.place)
        if filters and filters.viewpoint_scope:
            parts.append(filters.viewpoint_scope)
        if filters and filters.claim_kind is not None:
            parts.append(filters.claim_kind.value.replace("_", " "))
        if profile is not None:
            parts.extend(
                [
                    profile.project_name,
                    profile.narrative_focus or "",
                    profile.social_lens or "",
                    profile.geography or "",
                    profile.era or "",
                    profile.time_start or "",
                    profile.time_end or "",
                    " ".join(profile.desired_facets),
                ]
            )
        return " ".join(part for part in parts if part).strip()

    def _claim_haystack(self, claim: ApprovedClaim) -> str:
        evidence_fragments: list[str] = []
        for evidence_id in claim.evidence_ids[:2]:
            snippet = self.evidence_store.get_evidence(evidence_id)
            if snippet is None:
                continue
            evidence_fragments.extend(
                [
                    snippet.text,
                    snippet.notes or "",
                    snippet.locator or "",
                ]
            )
        return self._normalize_text(
            " ".join(
                fragment or ""
                for fragment in [
                    claim.subject,
                    claim.predicate,
                    claim.value,
                    claim.notes,
                    claim.place,
                    claim.time_start,
                    claim.time_end,
                    claim.viewpoint_scope,
                    claim.claim_kind.value.replace("_", " "),
                    *evidence_fragments,
                ]
            )
        )

    def _direct_topic_claims(
        self,
        claims: list[ApprovedClaim],
        request: QueryRequest,
        *,
        question_profile: dict[str, bool | list[str]],
    ) -> list[ApprovedClaim]:
        direct_matches = [
            claim
            for claim in claims
            if self._is_direct_topic_match(claim, request, question_profile=question_profile)
        ]
        if direct_matches:
            return sorted(
                direct_matches,
                key=lambda claim: (
                    self._direct_topic_score(claim, request, question_profile=question_profile),
                    self._CERTAINTY_RANK.get(claim.status.value, 0),
                    1 if claim.evidence_ids else 0,
                    claim.claim_id,
                ),
                reverse=True,
            )[:5]
        return []

    def _is_direct_topic_match(
        self,
        claim: ApprovedClaim,
        request: QueryRequest,
        *,
        question_profile: dict[str, bool | list[str]],
    ) -> bool:
        core_tokens, core_bigrams = self._direct_topic_terms(
            request, question_profile=question_profile
        )
        if not core_tokens:
            return False
        subject = self._normalize_text(claim.subject)
        value = self._normalize_text(claim.value)
        notes = self._normalize_text(claim.notes or "")
        haystack = " ".join(part for part in [subject, value, notes] if part)
        if not haystack:
            return False
        core_phrase = " ".join(core_tokens)
        if core_phrase and core_phrase in haystack:
            return True
        if core_bigrams:
            if any(bigram in haystack for bigram in core_bigrams):
                return True
            return False
        return any(token in haystack for token in core_tokens)

    def _direct_topic_score(
        self,
        claim: ApprovedClaim,
        request: QueryRequest,
        *,
        question_profile: dict[str, bool | list[str]],
    ) -> int:
        core_tokens, core_bigrams = self._direct_topic_terms(
            request,
            question_profile=question_profile,
        )
        if not core_tokens:
            return 0
        subject = self._normalize_text(claim.subject)
        value = self._normalize_text(claim.value)
        predicate = self._normalize_text(claim.predicate)
        notes = self._normalize_text(claim.notes or "")
        evidence = self._normalize_text(
            " ".join(
                snippet.text
                for evidence_id in claim.evidence_ids[:2]
                if (snippet := self.evidence_store.get_evidence(evidence_id)) is not None
            )
        )
        place = self._normalize_text(claim.place or "")
        score = 0
        phrase = " ".join(core_tokens)
        if phrase and phrase in " ".join(part for part in [subject, value, notes, evidence] if part):
            score += 10
        for bigram in core_bigrams:
            if bigram in " ".join(part for part in [subject, value, evidence] if part):
                score += 6
        for token in core_tokens:
            if token in value:
                score += 5
            if token in subject:
                score += 4
            if token in evidence:
                score += 4
            if token in notes:
                score += 2
            if token in predicate:
                score += 1
            if token in place:
                score += 2
        return score

    def _direct_topic_terms(
        self,
        request: QueryRequest,
        *,
        question_profile: dict[str, bool | list[str]],
    ) -> tuple[list[str], list[str]]:
        question_lower = self._normalize_text(request.question)
        mentioned_places = {
            token
            for claim in self.truth_store.list_claims()
            for token in re.findall(r"[a-z0-9]+", (claim.place or "").lower())
            if token and token in question_lower
        }
        raw_tokens = re.findall(r"[a-z0-9]+", request.question.lower())
        context_breakers = {
            "in",
            "during",
            "before",
            "after",
            "around",
            "at",
            "by",
            "within",
            "amid",
            "under",
        }
        complement_markers = {"about", "regarding", "concerning"}
        segments: list[list[str]] = []
        current_segment: list[str] = []
        for token in raw_tokens:
            if token in complement_markers:
                if current_segment:
                    segments.append(current_segment)
                    current_segment = []
                continue
            if token in context_breakers and current_segment:
                segments.append(current_segment)
                break
            if token in self._QUESTION_STOPWORDS or token in mentioned_places:
                continue
            current_segment.append(token)
        if current_segment:
            segments.append(current_segment)
        if not segments:
            segments = [
                [token for token in question_profile["tokens"] if token not in mentioned_places]
            ]
        core_tokens = [token for segment in segments for token in segment]
        bigrams = [bigram for segment in segments for bigram in self._question_bigrams(segment)]
        return core_tokens, bigrams

    def _augment_direct_matches_with_linked_claims(
        self,
        direct_matches: list[ApprovedClaim],
        ranked_matches: list[ApprovedClaim],
    ) -> list[ApprovedClaim]:
        claim_by_id = {claim.claim_id: claim for claim in direct_matches}
        allowed_relationships = {"supports", "contradicts", "supersedes", "superseded_by"}
        for claim in ranked_matches:
            if claim.claim_id in claim_by_id:
                continue
            if any(
                self._shares_subject_context(claim, direct_claim)
                and claim.place == direct_claim.place
                and claim.claim_kind == direct_claim.claim_kind
                for direct_claim in direct_matches
            ):
                claim_by_id.setdefault(claim.claim_id, claim)
        for claim in direct_matches:
            for relationship in self.truth_store.list_relationships(claim.claim_id):
                if relationship.relationship_type not in allowed_relationships:
                    continue
                linked_claim = self.truth_store.get_claim(relationship.related_claim_id)
                if linked_claim is not None:
                    claim_by_id.setdefault(linked_claim.claim_id, linked_claim)
            for relationship in self.truth_store.list_relationships():
                if (
                    relationship.related_claim_id != claim.claim_id
                    or relationship.relationship_type not in allowed_relationships
                ):
                    continue
                linked_claim = self.truth_store.get_claim(relationship.claim_id)
                if linked_claim is not None:
                    claim_by_id.setdefault(linked_claim.claim_id, linked_claim)
        ordered_matches = [claim for claim in ranked_matches if claim.claim_id in claim_by_id]
        related_only = [
            claim
            for claim_id, claim in claim_by_id.items()
            if claim_id not in {item.claim_id for item in ordered_matches}
        ]
        return ordered_matches + related_only

    def _shares_subject_context(self, claim: ApprovedClaim, direct_claim: ApprovedClaim) -> bool:
        claim_tokens = {
            token
            for token in self._question_tokens(claim.subject)
            if token not in self._question_tokens(claim.place or "")
        }
        direct_tokens = {
            token
            for token in self._question_tokens(direct_claim.subject)
            if token not in self._question_tokens(direct_claim.place or "")
        }
        return bool(claim_tokens & direct_tokens)

    def _sources_for_evidence(self, evidence: list) -> list:
        sources = []
        seen: set[str] = set()
        for snippet in evidence:
            if snippet.source_id in seen:
                continue
            source = self.source_store.get_source(snippet.source_id)
            if source is None:
                continue
            seen.add(snippet.source_id)
            sources.append(source)
        return sources

    def _topic_match_score(
        self,
        claim: ApprovedClaim,
        question_lower: str,
        tokens: list[str],
        bigrams: list[str],
        haystack_normalized: str,
        mentioned_places: set[str],
        question_profile: dict[str, bool],
    ) -> int:
        score = 0
        subject_normalized = self._normalize_text(claim.subject)
        predicate_normalized = self._normalize_text(claim.predicate)
        value_normalized = self._normalize_text(claim.value)
        notes_normalized = self._normalize_text(claim.notes or "")
        place_normalized = self._normalize_text(claim.place or "")
        viewpoint_normalized = self._normalize_text(claim.viewpoint_scope or "")
        place_tokens = {
            part for place in mentioned_places for part in re.findall(r"[a-z0-9]+", place.lower())
        }
        core_tokens = [token for token in tokens if token not in place_tokens]
        if question_lower and question_lower in " ".join(
            part for part in [subject_normalized, value_normalized, notes_normalized] if part
        ):
            score += max(6, len(tokens) * 2)
        for bigram in bigrams:
            if bigram in " ".join(part for part in [subject_normalized, value_normalized] if part):
                score += 6
            elif bigram in " ".join(
                part for part in [notes_normalized, viewpoint_normalized] if part
            ):
                score += 3
            elif bigram in haystack_normalized:
                score += 4
        core_hits = 0
        for token in tokens:
            token_hit = False
            if token in value_normalized:
                score += 5
                token_hit = True
            if token in subject_normalized:
                score += 4
                token_hit = True
            if token in place_normalized:
                score += 3
                token_hit = True
            if token in viewpoint_normalized:
                score += 3
                token_hit = True
            if token in notes_normalized:
                score += 2
                token_hit = True
            if token in predicate_normalized:
                score += 1
                token_hit = True
            elif token in haystack_normalized:
                score += 2
                token_hit = True
            if token_hit and token in core_tokens:
                core_hits += 1
        if claim.place and place_normalized in question_lower:
            score += 2
        elif mentioned_places and claim.place:
            score -= 6 if question_profile["topic_first"] else 3
        if claim.time_start and self._normalize_text(claim.time_start) in question_lower:
            score += 2
        if question_profile["hard_cap"]:
            if core_tokens and core_hits == 0:
                score -= 8
            elif len(core_tokens) >= 2 and core_hits < 2:
                score -= 3
            if not any(
                token
                in " ".join(
                    part
                    for part in [subject_normalized, value_normalized, notes_normalized]
                    if part
                )
                for token in tokens
            ):
                score -= 2
        return score

    def _canonical_strength(
        self,
        claim: ApprovedClaim,
        relationship_index: dict[str, list[ClaimRelationship]],
    ) -> int:
        return (
            self._CERTAINTY_RANK.get(claim.status.value, 0) * 4
            + (2 if claim.evidence_ids else 0)
            + self._relationship_score(claim.claim_id, relationship_index)
            + (1 if claim.author_choice else 0)
        )

    def _question_profile(self, request: QueryRequest) -> dict[str, bool | list[str]]:
        normalized = self._normalize_text(request.question)
        tokens = self._question_tokens(request.question)
        is_disagreement = request.mode == QueryMode.CONTESTED_VIEWS or any(
            token in normalized
            for token in {"disagree", "contradict", "conflict", "versus", "vs", "debate"}
        )
        wants_current = any(
            token in normalized
            for token in {"current", "latest", "canonical", "active", "now", "superseded"}
        )
        rumor_focus = request.mode == QueryMode.RUMOR_AND_LEGEND or any(
            token in normalized
            for token in {
                "rumor",
                "rumour",
                "legend",
                "gossip",
                "whisper",
                "whispered",
                "contested",
            }
        )
        broad = len(tokens) <= 1 or any(
            token in normalized for token in {"overview", "general", "everything", "all canon"}
        )
        narrow_topic = not broad and not is_disagreement and len(tokens) >= 2
        topic_first = narrow_topic or rumor_focus or wants_current
        hard_cap = request.mode == QueryMode.STRICT_FACTS and narrow_topic and not is_disagreement
        return {
            "tokens": tokens,
            "is_disagreement": is_disagreement,
            "wants_current": wants_current,
            "rumor_focus": rumor_focus,
            "broad": broad,
            "narrow_topic": narrow_topic,
            "topic_first": topic_first,
            "hard_cap": hard_cap,
        }

    def _question_bigrams(self, tokens: list[str]) -> list[str]:
        return [f"{left} {right}" for left, right in zip(tokens, tokens[1:], strict=False)]

    def _claim_intent_bonus(
        self,
        claim: ApprovedClaim,
        relationship_index: dict[str, list[ClaimRelationship]],
        question_profile: dict[str, bool],
    ) -> int:
        score = 0
        relationships = relationship_index.get(claim.claim_id, [])
        relationship_types = {relationship.relationship_type for relationship in relationships}
        if question_profile["rumor_focus"] and claim.status.value in {
            "contested",
            "rumor",
            "legend",
        }:
            score += 8
        if question_profile["is_disagreement"] and (
            claim.status.value == "contested" or "contradicts" in relationship_types
        ):
            score += 8
        if question_profile["wants_current"]:
            if "supersedes" in relationship_types:
                score += 6
            if "superseded_by" in relationship_types:
                score -= 6
        return score

    def _project_intent_score(
        self,
        claim: ApprovedClaim,
        request: QueryRequest,
        profile: BibleProjectProfile | None,
        intent_tokens: list[str],
        haystack_normalized: str,
    ) -> int:
        if profile is None:
            return 0
        score = 0
        for token in intent_tokens:
            if token in haystack_normalized:
                score += 1
        if profile.geography and claim.place == profile.geography:
            score += 4
        if profile.social_lens and claim.viewpoint_scope:
            if self._normalize_text(profile.social_lens) in self._normalize_text(
                claim.viewpoint_scope
            ):
                score += 4
        if profile.narrative_focus:
            score += self._partial_overlap_score(claim, profile.narrative_focus) * 2
        if profile.desired_facets and self._facet_label_for_claim(claim) in set(
            profile.desired_facets
        ):
            score += 3
        if profile.time_start or profile.time_end:
            score += self._time_window_score(claim, profile.time_start, profile.time_end)
        if profile.taboo_topics:
            taboo_hits = sum(
                1
                for taboo in profile.taboo_topics
                if taboo and self._normalize_text(taboo) in haystack_normalized
            )
            score -= taboo_hits * 3
        if request.mode == QueryMode.CHARACTER_KNOWLEDGE and claim.viewpoint_scope:
            score += 2
        return score

    def _filter_alignment_score(self, claim: ApprovedClaim, request: QueryRequest) -> int:
        filters = request.filters
        if filters is None:
            return 0
        score = 0
        if filters.claim_kind is not None and claim.claim_kind == filters.claim_kind:
            score += 4
        if filters.place and claim.place == filters.place:
            score += 5
        if filters.viewpoint_scope and claim.viewpoint_scope == filters.viewpoint_scope:
            score += 5
        if filters.time_start or filters.time_end:
            score += self._time_window_score(claim, filters.time_start, filters.time_end)
        if filters.status and claim.status == filters.status:
            score += 3
        if filters.include_statuses and claim.status in set(filters.include_statuses):
            score += 2
        return score

    def _time_window_score(
        self,
        claim: ApprovedClaim,
        time_start: str | None,
        time_end: str | None,
    ) -> int:
        if not (time_start or time_end):
            return 0
        if time_start and claim.time_end and claim.time_end < time_start:
            return -2
        if time_end and claim.time_start and claim.time_start > time_end:
            return -2
        if claim.time_start or claim.time_end:
            return 3
        return 0

    def _partial_overlap_score(self, claim: ApprovedClaim, text: str) -> int:
        haystack = self._claim_haystack(claim)
        return sum(1 for token in self._question_tokens(text) if token in haystack)

    def _claim_affinity_score(self, anchor_claim: ApprovedClaim, claim: ApprovedClaim) -> int:
        if anchor_claim.claim_id == claim.claim_id:
            return 99
        anchor_tokens = set(self._question_tokens(f"{anchor_claim.subject} {anchor_claim.value}"))
        claim_tokens = set(self._question_tokens(f"{claim.subject} {claim.value}"))
        overlap = len(anchor_tokens & claim_tokens)
        score = overlap * 2
        if anchor_claim.place and claim.place:
            score += 4 if anchor_claim.place == claim.place else -3
        if anchor_claim.claim_kind == claim.claim_kind:
            score += 2
        if anchor_claim.subject == claim.subject:
            score += 3
        if anchor_claim.viewpoint_scope and anchor_claim.viewpoint_scope == claim.viewpoint_scope:
            score += 2
        return score

    def _claim_overlap_count(self, anchor_claim: ApprovedClaim, claim: ApprovedClaim) -> int:
        anchor_tokens = set(self._question_tokens(f"{anchor_claim.subject} {anchor_claim.value}"))
        claim_tokens = set(self._question_tokens(f"{claim.subject} {claim.value}"))
        return len(anchor_tokens & claim_tokens)

    def _facet_label_for_claim(self, claim: ApprovedClaim) -> str:
        return {
            ClaimKind.PERSON: "people",
            ClaimKind.PLACE: "places",
            ClaimKind.INSTITUTION: "institutions",
            ClaimKind.EVENT: "events",
            ClaimKind.PRACTICE: "practices",
            ClaimKind.OBJECT: "objects",
            ClaimKind.BELIEF: "beliefs",
            ClaimKind.RELATIONSHIP: "relationships",
        }.get(claim.claim_kind, claim.claim_kind.value)

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
        request: QueryRequest,
        relationship_index: dict[str, list[ClaimRelationship]],
        *,
        profile: BibleProjectProfile | None = None,
        question_profile: dict[str, bool | list[str]] | None = None,
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
        profile_data = question_profile or self._question_profile(request)

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
                request,
                request.mode,
                relationship_index,
                relationships,
                profile_data,
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
                request,
                profile=profile,
                question_profile=profile_data,
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
        request: QueryRequest,
        mode: QueryMode,
        relationship_index: dict[str, list[ClaimRelationship]],
        relationships: list[ClaimRelationship],
        question_profile: dict[str, bool | list[str]],
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
                self._topic_match_score(
                    claim,
                    self._normalize_text(request.question),
                    list(question_profile["tokens"]),
                    self._question_bigrams(list(question_profile["tokens"])),
                    self._claim_haystack(claim),
                    set(),
                    question_profile,
                )
                if question_profile["topic_first"]
                else 0,
                self._claim_intent_bonus(claim, relationship_index, question_profile),
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
            f"{self._format_claim_text(claim)} [{claim.status.value}]" for claim in older_claims[:2]
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
        request: QueryRequest,
        *,
        profile: BibleProjectProfile | None = None,
        question_profile: dict[str, bool | list[str]] | None = None,
    ) -> tuple[int, int, int, int, str]:
        lead_claim = claim_by_id[cluster.lead_claim_id]
        profile_data = question_profile or self._question_profile(request)
        kind_rank = self._cluster_kind_priority(
            cluster.cluster_kind,
            request.question,
            request.mode,
            profile_data,
        )
        relevance_rank = self._cluster_question_relevance(
            cluster,
            claim_by_id,
            request,
            profile=profile,
            question_profile=profile_data,
        )
        if profile_data["topic_first"]:
            return (
                relevance_rank,
                kind_rank,
                self._claim_intent_bonus(lead_claim, relationship_index, profile_data),
                self._CERTAINTY_RANK.get(lead_claim.status.value, 0),
                lead_claim.claim_id,
            )
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
        question_profile: dict[str, bool | list[str]] | None = None,
    ) -> int:
        profile_data = question_profile or self._question_profile(
            QueryRequest(question=question, mode=mode)
        )
        if profile_data["is_disagreement"]:
            return {
                "contested": 3,
                "supersession": 2,
                "reinforcing": 1,
            }[cluster_kind]
        if profile_data["wants_current"]:
            return {
                "supersession": 3,
                "reinforcing": 2,
                "contested": 1,
            }[cluster_kind]
        if profile_data["rumor_focus"]:
            return {
                "contested": 3,
                "reinforcing": 2,
                "supersession": 1,
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
        request: QueryRequest,
        *,
        profile: BibleProjectProfile | None = None,
        question_profile: dict[str, bool | list[str]] | None = None,
    ) -> int:
        profile_data = question_profile or self._question_profile(request)
        projection_tokens = self._question_tokens(self._projection_query_text(request, profile))
        bigrams = self._question_bigrams(projection_tokens)
        if not projection_tokens:
            return 0
        score = 0
        question_lower = self._normalize_text(request.question)
        mentioned_places = {
            self._normalize_text(claim.place)
            for claim in claim_by_id.values()
            if claim.place and self._normalize_text(claim.place) in question_lower
        }
        for claim_id in cluster.claim_ids:
            claim = claim_by_id.get(claim_id)
            if claim is None:
                continue
            haystack = self._claim_haystack(claim)
            score += self._topic_match_score(
                claim,
                question_lower,
                projection_tokens,
                bigrams,
                haystack,
                mentioned_places,
                profile_data,
            )
            if profile is not None:
                score += self._project_intent_score(
                    claim,
                    request,
                    profile,
                    projection_tokens,
                    haystack,
                )
        return score

    def _apply_topical_cap(
        self,
        claim_clusters: list[ClaimCluster],
        answer_sections: list[AnswerSection],
        related_claims: list[ClaimRelationship],
        request: QueryRequest,
        question_profile: dict[str, bool | list[str]],
    ) -> tuple[list[ClaimCluster], list[AnswerSection], list[ClaimRelationship], bool]:
        if not claim_clusters or not question_profile["hard_cap"]:
            return claim_clusters, answer_sections, related_claims, False
        claim_by_id: dict[str, ApprovedClaim] = {}
        for cluster in claim_clusters:
            for claim_id in cluster.claim_ids:
                if claim_id in claim_by_id:
                    continue
                if claim := self.truth_store.get_claim(claim_id):
                    claim_by_id[claim_id] = claim
        cluster_scores = {
            cluster.cluster_id: self._cluster_question_relevance(
                cluster,
                claim_by_id,
                request,
                question_profile=question_profile,
            )
            for cluster in claim_clusters
        }
        ranked_scores = sorted(cluster_scores.values(), reverse=True)
        strongest = ranked_scores[0] if ranked_scores else 0
        runner_up = ranked_scores[1] if len(ranked_scores) > 1 else 0
        if strongest < 4 or strongest < runner_up + 2:
            return claim_clusters, answer_sections, related_claims, False
        ranked_clusters = sorted(
            claim_clusters,
            key=lambda cluster: cluster_scores.get(cluster.cluster_id, 0),
            reverse=True,
        )
        dominant_cluster = ranked_clusters[0]
        dominant_lead = claim_by_id.get(dominant_cluster.lead_claim_id)
        band_floor = max(4, strongest - 30)
        question_lower = self._normalize_text(request.question)
        mentioned_places = {
            self._normalize_text(claim.place)
            for claim in claim_by_id.values()
            if claim.place and self._normalize_text(claim.place) in question_lower
        }
        kept_cluster_ids = {dominant_cluster.cluster_id}
        if dominant_lead is not None:
            required_overlap = 1 if question_profile["rumor_focus"] else 3
            for cluster in ranked_clusters[1:]:
                cluster_lead = claim_by_id.get(cluster.lead_claim_id)
                if cluster_lead is None:
                    continue
                place_matches = (
                    not mentioned_places
                    or not cluster_lead.place
                    or self._normalize_text(cluster_lead.place) in mentioned_places
                )
                if (
                    cluster_scores.get(cluster.cluster_id, 0) >= band_floor
                    and place_matches
                    and self._claim_overlap_count(dominant_lead, cluster_lead) >= required_overlap
                ):
                    kept_cluster_ids.add(cluster.cluster_id)
        dominant_claim_ids = set(dominant_cluster.claim_ids)
        adjacent_claim_ids = {
            related_claim_id
            for relationship in self.truth_store.list_relationships()
            if relationship.relationship_type in {"contradicts", "supersedes", "superseded_by"}
            and relationship.claim_id in dominant_claim_ids
            for related_claim_id in [relationship.related_claim_id]
        } | {
            relationship.claim_id
            for relationship in self.truth_store.list_relationships()
            if relationship.relationship_type in {"contradicts", "supersedes", "superseded_by"}
            and relationship.related_claim_id in dominant_claim_ids
        }
        for cluster in ranked_clusters[1:]:
            if set(cluster.claim_ids) & adjacent_claim_ids:
                kept_cluster_ids.add(cluster.cluster_id)
        if len(kept_cluster_ids) == len(claim_clusters):
            return claim_clusters, answer_sections, related_claims, False
        kept_clusters = [
            cluster for cluster in claim_clusters if cluster.cluster_id in kept_cluster_ids
        ]
        kept_sections = [
            section for section in answer_sections if section.cluster_id in kept_cluster_ids
        ]
        kept_claim_ids = {claim_id for cluster in kept_clusters for claim_id in cluster.claim_ids}
        kept_relationships = [
            relationship
            for relationship in related_claims
            if relationship.claim_id in kept_claim_ids
            and relationship.related_claim_id in kept_claim_ids
        ]
        return kept_clusters, kept_sections, kept_relationships, True

    def _question_tokens(self, text: str) -> list[str]:
        return [
            token
            for token in re.findall(r"[a-z0-9]+", text.lower())
            if token not in self._QUESTION_STOPWORDS
        ]

    def _is_disagreement_question(self, text: str) -> bool:
        normalized = text.lower()
        return any(
            token in normalized
            for token in {"disagree", "contradict", "conflict", "versus", "vs", "debate"}
        )

    def _normalize_text(self, text: str) -> str:
        return " ".join(re.findall(r"[a-z0-9]+", text.lower()))

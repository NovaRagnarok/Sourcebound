from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from hashlib import sha1
from uuid import uuid4

from source_aware_worldbuilding.domain.enums import BibleSectionType, ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleCoverageAnalysis,
    BibleCoverageBucket,
    BibleParagraphProvenance,
    BibleProjectExportResponse,
    BibleProjectProfile,
    BibleProjectProfileUpdateRequest,
    BibleSection,
    BibleSectionCompositionMetrics,
    BibleSectionCreateRequest,
    BibleSectionDraft,
    BibleSectionFilters,
    BibleSectionParagraph,
    BibleSectionProvenanceDetail,
    BibleSectionReference,
    BibleSectionRegenerateRequest,
    BibleSectionUpdateRequest,
    ClaimRelationship,
    EvidenceSnippet,
    SourceRecord,
    utc_now,
)
from source_aware_worldbuilding.ports import (
    BibleProjectProfileStorePort,
    BibleSectionStorePort,
    EvidenceStorePort,
    ProjectionPort,
    SourceStorePort,
    TruthStorePort,
)


@dataclass(frozen=True)
class BeatSpec:
    beat_id: str
    paragraph_role: str
    selection_strategy: str
    min_distinct_claims: int = 2
    min_distinct_evidence: int = 1
    allow_single_strong_claim: bool = False
    allowed_certainties: tuple[ClaimStatus, ...] = (ClaimStatus.VERIFIED, ClaimStatus.PROBABLE)
    contradiction_policy: str = "forbidden"
    allow_counterpart_claims: bool = False
    counterpart_required: bool = False
    require_relationship_context: bool = False
    require_support_context: bool = False


@dataclass
class ClaimPack:
    beat_id: str
    claim_roles: dict[str, str]
    selected_claims: list[ApprovedClaim]
    selected_evidence_ids: list[str]
    selected_source_ids: list[str]


@dataclass
class SkippedBeat:
    beat_id: str
    reason: str


@dataclass
class SectionCoverageMetricsInternal:
    target_beat_count: int = 0
    produced_beat_count: int = 0
    skipped_beat_count: int = 0
    claim_density: float = 0.0
    evidence_density: float = 0.0
    certainty_mix: dict[str, int] = field(default_factory=dict)
    contradiction_presence: bool = False


@dataclass
class SectionCompositionPlan:
    section_type: BibleSectionType
    target_beats: list[str]
    attempted_beats: list[str]
    produced_beats: list[str]
    skipped_beats: list[SkippedBeat]
    beat_to_claim_reservations: dict[str, list[str]]
    section_coverage_metrics: SectionCoverageMetricsInternal


class BibleWorkspaceService:
    _SECTION_TITLES = {
        BibleSectionType.SETTING_OVERVIEW: "Setting Overview",
        BibleSectionType.CHRONOLOGY: "Chronology",
        BibleSectionType.PEOPLE_AND_FACTIONS: "People And Factions",
        BibleSectionType.DAILY_LIFE: "Daily Life And Practices",
        BibleSectionType.INSTITUTIONS_AND_POLITICS: "Institutions And Politics",
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE: "Economics And Material Culture",
        BibleSectionType.RUMORS_AND_CONTESTED: "Rumors And Contested Accounts",
        BibleSectionType.AUTHOR_DECISIONS: "Author Decisions",
    }
    _SECTION_KINDS = {
        BibleSectionType.SETTING_OVERVIEW: {
            ClaimKind.PLACE,
            ClaimKind.EVENT,
            ClaimKind.PERSON,
            ClaimKind.INSTITUTION,
            ClaimKind.RELATIONSHIP,
        },
        BibleSectionType.CHRONOLOGY: {ClaimKind.EVENT},
        BibleSectionType.PEOPLE_AND_FACTIONS: {
            ClaimKind.PERSON,
            ClaimKind.RELATIONSHIP,
            ClaimKind.INSTITUTION,
        },
        BibleSectionType.DAILY_LIFE: {ClaimKind.PRACTICE, ClaimKind.OBJECT, ClaimKind.BELIEF},
        BibleSectionType.INSTITUTIONS_AND_POLITICS: {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP},
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE: {ClaimKind.PRACTICE, ClaimKind.OBJECT},
        BibleSectionType.RUMORS_AND_CONTESTED: {ClaimKind.BELIEF, ClaimKind.EVENT, ClaimKind.PLACE},
        BibleSectionType.AUTHOR_DECISIONS: {
            ClaimKind.PLACE,
            ClaimKind.PERSON,
            ClaimKind.EVENT,
            ClaimKind.PRACTICE,
            ClaimKind.OBJECT,
        },
    }
    _CERTAINTY_WEIGHT = {
        ClaimStatus.VERIFIED: 6,
        ClaimStatus.AUTHOR_CHOICE: 5,
        ClaimStatus.PROBABLE: 4,
        ClaimStatus.CONTESTED: 3,
        ClaimStatus.RUMOR: 2,
        ClaimStatus.LEGEND: 1,
    }
    _FACET_LABELS = {
        ClaimKind.PERSON: "people",
        ClaimKind.PLACE: "places",
        ClaimKind.INSTITUTION: "institutions",
        ClaimKind.EVENT: "events",
        ClaimKind.PRACTICE: "practices",
        ClaimKind.OBJECT: "objects",
        ClaimKind.BELIEF: "beliefs",
        ClaimKind.RELATIONSHIP: "relationships",
    }
    _FACET_DISPLAY_LABELS = {
        "beliefs": "beliefs",
        "daily_life": "daily life",
        "economics": "economics",
        "events": "events",
        "institutions": "institutions",
        "legend": "legend",
        "material_culture": "material culture",
        "objects": "objects",
        "people": "people",
        "places": "places",
        "politics": "politics",
        "practices": "practices",
        "regional_context": "regional context",
        "relationships": "relationships",
        "ritual": "ritual",
        "rumor": "rumor",
    }
    _FACET_ALIASES = {
        "beliefs": "beliefs",
        "daily_life": "daily_life",
        "daily_life_and_practices": "daily_life",
        "dailylife": "daily_life",
        "economics": "economics",
        "economics_and_material_culture": "economics",
        "economics_commercial": "economics",
        "events": "events",
        "institutions": "institutions",
        "legend": "legend",
        "legends": "legend",
        "material_culture": "material_culture",
        "objects": "objects",
        "objects_technology": "material_culture",
        "people": "people",
        "places": "places",
        "politics": "politics",
        "practices": "practices",
        "regional_context": "regional_context",
        "relationships": "relationships",
        "ritual": "ritual",
        "rumor": "rumor",
        "rumors": "rumor",
    }
    _SECTION_COVERAGE_BUCKETS = {
        BibleSectionType.SETTING_OVERVIEW: {
            "places",
            "regional_context",
            "events",
            "people",
            "institutions",
            "relationships",
        },
        BibleSectionType.CHRONOLOGY: {"events"},
        BibleSectionType.PEOPLE_AND_FACTIONS: {
            "people",
            "institutions",
            "politics",
            "relationships",
        },
        BibleSectionType.DAILY_LIFE: {
            "daily_life",
            "practices",
            "objects",
            "material_culture",
            "beliefs",
            "ritual",
        },
        BibleSectionType.INSTITUTIONS_AND_POLITICS: {"institutions", "politics", "relationships"},
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE: {
            "economics",
            "daily_life",
            "practices",
            "objects",
            "material_culture",
        },
        BibleSectionType.RUMORS_AND_CONTESTED: {
            "beliefs",
            "ritual",
            "rumor",
            "legend",
            "events",
            "regional_context",
        },
        BibleSectionType.AUTHOR_DECISIONS: set(),
    }

    def __init__(
        self,
        profile_store: BibleProjectProfileStorePort,
        section_store: BibleSectionStorePort,
        truth_store: TruthStorePort,
        evidence_store: EvidenceStorePort,
        source_store: SourceStorePort,
        projection: ProjectionPort | None = None,
    ) -> None:
        self.profile_store = profile_store
        self.section_store = section_store
        self.truth_store = truth_store
        self.evidence_store = evidence_store
        self.source_store = source_store
        self.projection = projection

    def list_profiles(self) -> list[BibleProjectProfile]:
        return self.profile_store.list_profiles()

    def get_profile(self, project_id: str) -> BibleProjectProfile | None:
        return self.profile_store.get_profile(project_id)

    def save_profile(
        self,
        project_id: str,
        request: BibleProjectProfileUpdateRequest,
    ) -> BibleProjectProfile:
        now = utc_now()
        existing = self.profile_store.get_profile(project_id)
        profile = BibleProjectProfile(
            project_id=project_id,
            project_name=request.project_name,
            era=request.era,
            time_start=request.time_start,
            time_end=request.time_end,
            geography=request.geography,
            social_lens=request.social_lens,
            narrative_focus=request.narrative_focus,
            taboo_topics=request.taboo_topics,
            desired_facets=request.desired_facets,
            tone=request.tone,
            composition_defaults=request.composition_defaults,
            created_at=existing.created_at if existing else now,
            updated_at=now,
        )
        self.profile_store.save_profile(profile)
        return profile

    def list_sections(self, project_id: str) -> list[BibleSection]:
        return self.section_store.list_sections(project_id=project_id)

    def get_section(self, section_id: str) -> BibleSection | None:
        return self.section_store.get_section(section_id)

    def get_section_provenance(self, section_id: str) -> BibleSectionProvenanceDetail | None:
        section = self.get_section(section_id)
        if section is None:
            return None
        claim_ids = {
            claim_id for paragraph in section.paragraphs for claim_id in paragraph.claim_ids
        }
        evidence_ids = {
            evidence_id
            for paragraph in section.paragraphs
            for evidence_id in paragraph.evidence_ids
        }
        claims = {
            claim_id: claim
            for claim_id in claim_ids
            if (claim := self.truth_store.get_claim(claim_id)) is not None
        }
        evidence = {
            evidence_id: item
            for evidence_id in evidence_ids
            if (item := self.evidence_store.get_evidence(evidence_id)) is not None
        }
        sources = {
            item.source_id: source
            for item in evidence.values()
            if (source := self.source_store.get_source(item.source_id)) is not None
        }
        relationship_context = self._relationship_context(list(claims.values()))
        paragraphs: list[BibleParagraphProvenance] = []
        for paragraph in section.paragraphs:
            paragraph_claims = [
                claims[claim_id] for claim_id in paragraph.claim_ids if claim_id in claims
            ]
            paragraph_evidence = [
                evidence[evidence_id]
                for evidence_id in paragraph.evidence_ids
                if evidence_id in evidence
            ]
            paragraph_sources = [
                sources[source_id] for source_id in paragraph.source_ids if source_id in sources
            ]
            contradiction_details, supersession_details = self._relationship_details_for_claim_ids(
                paragraph.claim_ids
            )
            paragraphs.append(
                BibleParagraphProvenance(
                    paragraph=paragraph,
                    claims=paragraph_claims,
                    evidence=paragraph_evidence,
                    sources=paragraph_sources,
                    contradiction_context=paragraph.contradiction_flags,
                    supersession_context=paragraph.supersession_flags,
                    provenance_scope=self._paragraph_scope(section.section_type, paragraph_claims),
                    why_this_paragraph_exists=self._why_this_paragraph_exists(
                        paragraph,
                        paragraph_claims,
                        paragraph_sources,
                    ),
                    claim_details=[self._claim_detail(claim) for claim in paragraph_claims],
                    evidence_details=[
                        self._evidence_detail(item, sources.get(item.source_id))
                        for item in paragraph_evidence
                    ],
                    contradiction_details=contradiction_details,
                    supersession_details=supersession_details,
                )
            )
        return BibleSectionProvenanceDetail(
            section_id=section.section_id,
            title=section.title,
            section_type=section.section_type,
            references=section.references,
            paragraphs=paragraphs,
            relationships=relationship_context,
        )

    def prepare_section(self, request: BibleSectionCreateRequest) -> BibleSection:
        now = utc_now()
        title = request.title or self._SECTION_TITLES[request.section_type]
        section = BibleSection(
            section_id=f"section-{uuid4().hex[:12]}",
            project_id=request.project_id,
            section_type=request.section_type,
            title=title,
            content="Composition queued.",
            generated_markdown="",
            generation_filters=request.filters,
            created_at=now,
            updated_at=now,
        )
        self.section_store.save_section(section)
        return section

    def compose_prepared_section(self, section_id: str) -> BibleSection:
        section = self.section_store.get_section(section_id)
        if section is None:
            raise ValueError("Bible section not found.")
        draft = self._compose_section(
            section.project_id, section.section_type, section.generation_filters
        )
        now = utc_now()
        section.title = section.title or draft.title
        section.generated_markdown = draft.generated_markdown
        section.paragraphs = draft.paragraphs
        section.references = draft.references
        section.certainty_summary = draft.certainty_summary
        section.coverage_gaps = draft.coverage_gaps
        section.contradiction_flags = draft.contradiction_flags
        section.recommended_next_research = draft.recommended_next_research
        section.coverage_analysis = draft.coverage_analysis
        section.retrieval_metadata = draft.retrieval_metadata
        section.composition_metrics = draft.composition_metrics
        if not section.has_manual_edits:
            section.content = draft.generated_markdown
        section.updated_at = now
        section.last_generated_at = now
        self.section_store.save_section(section)
        return section

    def create_section(self, request: BibleSectionCreateRequest) -> BibleSection:
        return self.compose_prepared_section(self.prepare_section(request).section_id)

    def update_section(self, section_id: str, request: BibleSectionUpdateRequest) -> BibleSection:
        section = self.section_store.get_section(section_id)
        if section is None:
            raise ValueError("Bible section not found.")
        now = utc_now()
        section.title = request.title or section.title
        section.content = request.content
        section.manual_markdown = request.content
        section.has_manual_edits = True
        section.updated_at = now
        section.last_edited_at = now
        self.section_store.save_section(section)
        return section

    def regenerate_section(
        self,
        section_id: str,
        request: BibleSectionRegenerateRequest | None = None,
    ) -> BibleSection:
        section = self.section_store.get_section(section_id)
        if section is None:
            raise ValueError("Bible section not found.")
        filters = request.filters if request and request.filters else section.generation_filters
        draft = self._compose_section(section.project_id, section.section_type, filters)
        now = utc_now()
        section.generated_markdown = draft.generated_markdown
        section.paragraphs = draft.paragraphs
        if not section.has_manual_edits:
            section.content = draft.generated_markdown
        section.generation_filters = filters
        section.references = draft.references
        section.certainty_summary = draft.certainty_summary
        section.coverage_gaps = draft.coverage_gaps
        section.contradiction_flags = draft.contradiction_flags
        section.recommended_next_research = draft.recommended_next_research
        section.coverage_analysis = draft.coverage_analysis
        section.retrieval_metadata = draft.retrieval_metadata
        section.composition_metrics = draft.composition_metrics
        section.updated_at = now
        section.last_generated_at = now
        self.section_store.save_section(section)
        return section

    def export_project(self, project_id: str) -> BibleProjectExportResponse:
        profile = self.profile_store.get_profile(project_id)
        if profile is None:
            raise ValueError("Bible project profile not found.")
        return BibleProjectExportResponse(
            profile=profile,
            sections=sorted(
                self.section_store.list_sections(project_id=project_id),
                key=lambda item: (item.section_type.value, item.title.lower()),
            ),
        )

    def _compose_section(
        self,
        project_id: str,
        section_type: BibleSectionType,
        filters: BibleSectionFilters,
    ) -> BibleSectionDraft:
        profile = self.profile_store.get_profile(project_id)
        claims, retrieval_metadata = self._select_claims(section_type, filters, profile)
        evidence_by_id = self._evidence_index(claims)
        source_by_id = self._source_index(evidence_by_id.values())
        relationships = self.truth_store.list_relationships()
        contradiction_flags = self._contradiction_flags(claims, relationships)
        coverage_analysis = self._coverage_analysis(section_type, claims, profile)
        coverage_gaps = self._coverage_gaps(section_type, claims, coverage_analysis)
        recommended_next_research = self._recommended_research(
            coverage_analysis,
            coverage_gaps,
            profile,
            section_type,
        )
        paragraphs, composition_metrics = self._build_paragraphs(
            section_type,
            claims,
            evidence_by_id,
            source_by_id,
            relationships,
            filters,
            profile,
        )
        referenced_claim_ids = sorted(
            {claim_id for paragraph in paragraphs for claim_id in paragraph.claim_ids}
        )
        referenced_claims = [
            claim
            for claim_id in referenced_claim_ids
            if (claim := self.truth_store.get_claim(claim_id)) is not None
        ] or claims
        references = BibleSectionReference(
            claim_ids=[claim.claim_id for claim in referenced_claims],
            evidence_ids=sorted(
                {item for claim in referenced_claims for item in claim.evidence_ids}
            ),
            source_ids=sorted(
                {source_id for paragraph in paragraphs for source_id in paragraph.source_ids}
            ),
            certainty_buckets=sorted(
                {claim.status for claim in referenced_claims},
                key=lambda status: status.value,
            ),
        )
        title = self._SECTION_TITLES[section_type]
        markdown = self._render_markdown(
            title,
            paragraphs,
            coverage_analysis,
            coverage_gaps,
            recommended_next_research,
            retrieval_metadata,
        )
        return BibleSectionDraft(
            section_type=section_type,
            title=title,
            generated_markdown=markdown,
            paragraphs=paragraphs,
            references=references,
            certainty_summary=dict(Counter(claim.status.value for claim in referenced_claims)),
            coverage_gaps=coverage_gaps,
            contradiction_flags=contradiction_flags,
            recommended_next_research=recommended_next_research,
            coverage_analysis=coverage_analysis,
            retrieval_metadata=retrieval_metadata,
            composition_metrics=composition_metrics,
        )

    def _select_claims(
        self,
        section_type: BibleSectionType,
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> tuple[list[ApprovedClaim], dict[str, object]]:
        claims = list(self.truth_store.list_claims())
        allowed_kinds = self._SECTION_KINDS[section_type]
        if section_type != BibleSectionType.AUTHOR_DECISIONS:
            claims = [claim for claim in claims if claim.claim_kind in allowed_kinds]
        if section_type == BibleSectionType.RUMORS_AND_CONTESTED:
            statuses = set(
                filters.statuses or [ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND]
            )
            claims = [claim for claim in claims if claim.status in statuses]
        elif section_type == BibleSectionType.AUTHOR_DECISIONS:
            claims = [
                claim
                for claim in claims
                if claim.status == ClaimStatus.AUTHOR_CHOICE or claim.author_choice
            ]
        else:
            default_statuses = (
                profile.composition_defaults.include_statuses
                if profile
                else [ClaimStatus.VERIFIED, ClaimStatus.PROBABLE]
            )
            statuses = set(filters.statuses or default_statuses)
            claims = [claim for claim in claims if claim.status in statuses]
        if filters.claim_kind is not None:
            claims = [claim for claim in claims if claim.claim_kind == filters.claim_kind]
        if filters.place:
            claims = [claim for claim in claims if claim.place == filters.place]
        elif profile and profile.geography:
            claims = [claim for claim in claims if claim.place in {None, "", profile.geography}]
        if filters.viewpoint_scope:
            claims = [claim for claim in claims if claim.viewpoint_scope == filters.viewpoint_scope]
        if filters.time_start:
            claims = [
                claim
                for claim in claims
                if not claim.time_end or claim.time_end >= filters.time_start
            ]
        elif profile and profile.time_start:
            claims = [
                claim
                for claim in claims
                if not claim.time_end or claim.time_end >= profile.time_start
            ]
        if filters.time_end:
            claims = [
                claim
                for claim in claims
                if not claim.time_start or claim.time_start <= filters.time_end
            ]
        elif profile and profile.time_end:
            claims = [
                claim
                for claim in claims
                if not claim.time_start or claim.time_start <= profile.time_end
            ]
        if filters.relationship_types:
            allowed_relationship_claim_ids = {
                item.claim_id
                for item in self.truth_store.list_relationships()
                if item.relationship_type in filters.relationship_types
            }
            claims = [claim for claim in claims if claim.claim_id in allowed_relationship_claim_ids]
        if filters.source_types or (profile and profile.composition_defaults.source_types):
            source_types = set(filters.source_types or profile.composition_defaults.source_types)
            evidence_by_id = self._evidence_index(claims)
            source_by_id = self._source_index(evidence_by_id.values())
            claims = [
                claim
                for claim in claims
                if any(
                    (
                        source_by_id.get(evidence_by_id[evidence_id].source_id)
                        or SourceRecord(source_id="", title="")
                    ).source_type
                    in source_types
                    for evidence_id in claim.evidence_ids
                    if evidence_id in evidence_by_id
                )
            ]
        return self._rank_claims(claims, filters, profile, section_type)

    def _rank_claims(
        self,
        claims: list[ApprovedClaim],
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
        section_type: BibleSectionType,
    ) -> tuple[list[ApprovedClaim], dict[str, object]]:
        projection_order: dict[str, int] = {}
        retrieval_metadata: dict[str, object] = {
            "retrieval_backend": "memory",
            "fallback_used": False,
            "fallback_reason": None,
            "seed_query": self._retrieval_seed_query(section_type, filters, profile),
            "ranking_strategy": "intent_blended"
            if self._has_intent_context(filters, profile)
            else "lexical",
        }
        seed_query = str(retrieval_metadata["seed_query"] or "")
        if seed_query and self.projection is not None and claims:
            projection = self.projection.search_claim_ids(
                seed_query,
                [claim.claim_id for claim in claims],
                limit=min(25, len(claims)),
            )
            if not projection.fallback_used:
                retrieval_metadata["retrieval_backend"] = projection.retrieval_backend
                retrieval_metadata["ranking_strategy"] = (
                    "intent_blended" if self._has_intent_context(filters, profile) else "blended"
                )
                projection_order = {
                    claim_id: index for index, claim_id in enumerate(projection.claim_ids)
                }
            else:
                retrieval_metadata["fallback_used"] = True
                retrieval_metadata["fallback_reason"] = projection.fallback_reason
        relationships = self.truth_store.list_relationships()
        relationship_counts = Counter(
            item.claim_id for item in relationships if item.relationship_type == "supports"
        )
        ranked = sorted(
            claims,
            key=lambda claim: (
                -self._blended_claim_score(
                    claim,
                    seed_query,
                    projection_order,
                    relationship_counts,
                    filters,
                    profile,
                    section_type,
                ),
                projection_order.get(claim.claim_id, 999),
                -(1 if claim.evidence_ids else 0),
                claim.time_start or "",
                claim.subject.lower(),
                claim.value.lower(),
            ),
        )
        if filters.focus:
            ranked = self._focused_claim_window(ranked, filters.focus, relationships)
        return ranked, retrieval_metadata

    def _focused_claim_window(
        self,
        ranked_claims: list[ApprovedClaim],
        focus: str,
        relationships: list[ClaimRelationship],
    ) -> list[ApprovedClaim]:
        if len(ranked_claims) <= 6:
            return ranked_claims
        focus_scores = {
            claim.claim_id: self._claim_focus_score(claim, focus) for claim in ranked_claims
        }
        strongest_focus = max(focus_scores.values(), default=0)
        if strongest_focus <= 0:
            return ranked_claims[:8]

        seed_claim_ids = {
            claim.claim_id
            for claim in ranked_claims
            if focus_scores[claim.claim_id] >= max(3, strongest_focus - 4)
        }
        connected_ids = set(seed_claim_ids)
        for relationship in relationships:
            if relationship.claim_id in seed_claim_ids:
                connected_ids.add(relationship.related_claim_id)
            if relationship.related_claim_id in seed_claim_ids:
                connected_ids.add(relationship.claim_id)

        focused_window: list[ApprovedClaim] = []
        for claim in ranked_claims:
            if claim.claim_id in connected_ids:
                focused_window.append(claim)
                continue
            if focus_scores[claim.claim_id] > 0:
                focused_window.append(claim)
                continue
            if (
                claim.subject
                and any(
                    seed.subject == claim.subject
                    for seed in ranked_claims
                    if seed.claim_id in seed_claim_ids
                )
                and strongest_focus >= 4
            ):
                focused_window.append(claim)
        if len(focused_window) < 5:
            seen_ids = {claim.claim_id for claim in focused_window}
            for claim in ranked_claims:
                if claim.claim_id in seen_ids:
                    continue
                focused_window.append(claim)
                seen_ids.add(claim.claim_id)
                if len(focused_window) >= 6:
                    break
        return focused_window[:10]

    def _build_paragraphs(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        relationships: list[ClaimRelationship],
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> tuple[list[BibleSectionParagraph], BibleSectionCompositionMetrics]:
        if not claims:
            paragraphs = [
                BibleSectionParagraph(
                    paragraph_id=self._stable_paragraph_id("empty_state", "", []),
                    paragraph_kind="empty_state",
                    text="No approved canon matches this section yet.",
                )
            ]
            return (
                paragraphs,
                BibleSectionCompositionMetrics(
                    thin_section=True,
                    target_beats=0,
                    produced_beats=0,
                    skipped_reasons=["No approved canon matched this section."],
                ),
            )
        if section_type in {
            BibleSectionType.SETTING_OVERVIEW,
            BibleSectionType.CHRONOLOGY,
            BibleSectionType.PEOPLE_AND_FACTIONS,
            BibleSectionType.RUMORS_AND_CONTESTED,
        }:
            paragraphs, plan = self._compose_priority_section(
                section_type,
                claims,
                evidence_by_id,
                source_by_id,
                relationships,
                filters,
                profile,
            )
            return paragraphs, self._summarize_composition_metrics(plan)
        beats = self._section_beats(section_type, claims, relationships, filters, profile)
        if section_type == BibleSectionType.CHRONOLOGY:
            paragraphs = self._chronology_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        elif section_type == BibleSectionType.SETTING_OVERVIEW:
            paragraphs = self._setting_overview_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        elif section_type == BibleSectionType.PEOPLE_AND_FACTIONS:
            paragraphs = self._people_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        elif section_type == BibleSectionType.DAILY_LIFE:
            paragraphs = self._daily_life_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        elif section_type == BibleSectionType.INSTITUTIONS_AND_POLITICS:
            paragraphs = self._institutions_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        elif section_type == BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE:
            paragraphs = self._economics_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        elif section_type == BibleSectionType.RUMORS_AND_CONTESTED:
            paragraphs = self._rumor_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        elif section_type == BibleSectionType.AUTHOR_DECISIONS:
            paragraphs = self._author_decision_paragraphs(
                claims, evidence_by_id, source_by_id, relationships
            )
        else:
            paragraphs = self._grouped_summary_paragraphs(
                claims,
                evidence_by_id,
                source_by_id,
                relationships,
                section_type,
            )
        paragraphs = self._order_paragraphs_by_beats(paragraphs, beats)
        kind_to_beat = {
            "setting_cluster": "scene_anchor",
            "chronology_entry": "chronology_turn",
            "chronology_undated": "uncertainty",
            "actor_cluster": "actor_pressure",
            "routine_cluster": "routine_pressure",
            "material_cluster": "material_texture",
            "institution_cluster": "institution_pressure",
            "economy_cluster": "economy_pressure",
            "material_culture_cluster": "material_cues",
            "economy_notebook": "writer_line",
            "contested_topic": "contested_note",
            "author_guidance": "author_guidance",
        }
        produced_beats = [
            beat
            for beat in dict.fromkeys(
                kind_to_beat.get(paragraph.paragraph_kind)
                for paragraph in paragraphs
                if kind_to_beat.get(paragraph.paragraph_kind)
            )
        ]
        skipped_beat_ids = [beat for beat in beats if beat not in produced_beats]
        paragraph_count = len(paragraphs) or 1
        metrics = BibleSectionCompositionMetrics(
            thin_section=len(paragraphs) <= 1,
            target_beats=len(beats),
            produced_beats=len(produced_beats),
            skipped_beat_ids=skipped_beat_ids,
            skipped_reasons=[
                f"No paragraph produced for beat '{beat}'." for beat in skipped_beat_ids
            ],
            claim_density=sum(len(paragraph.claim_ids) for paragraph in paragraphs)
            / paragraph_count,
            evidence_density=sum(len(paragraph.evidence_ids) for paragraph in paragraphs)
            / paragraph_count,
            contradiction_presence=any(paragraph.contradiction_flags for paragraph in paragraphs),
        )
        return paragraphs, metrics

    def _compose_priority_section(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        relationships: list[ClaimRelationship],
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> tuple[list[BibleSectionParagraph], SectionCompositionPlan]:
        blueprint = self._priority_blueprint(section_type, claims, relationships, filters, profile)
        paragraphs: list[BibleSectionParagraph] = []
        skipped: list[SkippedBeat] = []
        reservations: dict[str, list[str]] = {}
        produced_ids: list[str] = []
        seen_signatures: list[tuple[str, frozenset[str]]] = []

        for beat_instance_id, spec, heading, candidate_claims in blueprint:
            pack = self._select_claim_pack(
                section_type,
                spec,
                candidate_claims,
                claims,
                relationships,
            )
            if pack is None:
                skipped.append(
                    SkippedBeat(
                        beat_id=beat_instance_id,
                        reason=self._initial_skip_reason(spec, candidate_claims),
                    )
                )
                continue
            reason = self._pack_skip_reason(pack, spec, seen_signatures)
            if reason is not None:
                skipped.append(SkippedBeat(beat_id=beat_instance_id, reason=reason))
                continue
            reservations[beat_instance_id] = [claim.claim_id for claim in pack.selected_claims]
            produced_ids.append(beat_instance_id)
            seen_signatures.append(
                (
                    self._claim_fact_signature(self._pack_anchor(pack)),
                    frozenset(self._claim_fact_signature(claim) for claim in pack.selected_claims),
                )
            )
            paragraphs.append(
                self._paragraph_from_claim_pack(
                    heading=heading,
                    kind=spec.beat_id,
                    paragraph_role=spec.paragraph_role,
                    pack=pack,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_priority_paragraph_text(
                        section_type, spec, pack, heading, relationships
                    ),
                )
            )

        produced_claims = [
            claim
            for paragraph in paragraphs
            for claim in claims
            if claim.claim_id in paragraph.claim_ids
        ]
        certainty_mix = dict(Counter(claim.status.value for claim in produced_claims))
        return paragraphs, SectionCompositionPlan(
            section_type=section_type,
            target_beats=[item[0] for item in blueprint],
            attempted_beats=[item[0] for item in blueprint],
            produced_beats=produced_ids,
            skipped_beats=skipped,
            beat_to_claim_reservations=reservations,
            section_coverage_metrics=SectionCoverageMetricsInternal(
                target_beat_count=len(blueprint),
                produced_beat_count=len(paragraphs),
                skipped_beat_count=len(skipped),
                claim_density=round(
                    sum(len(item.claim_ids) for item in paragraphs) / max(len(paragraphs), 1), 2
                ),
                evidence_density=round(
                    sum(len(item.evidence_ids) for item in paragraphs) / max(len(paragraphs), 1), 2
                ),
                certainty_mix=certainty_mix,
                contradiction_presence=any(item.contradiction_flags for item in paragraphs),
            ),
        )

    def _priority_blueprint(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> list[tuple[str, BeatSpec, str, list[ApprovedClaim]]]:
        _ = filters, profile
        if section_type == BibleSectionType.SETTING_OVERVIEW:
            place = next((claim.place for claim in claims if claim.place), "Setting")
            return [
                (
                    "setting_anchor",
                    BeatSpec(
                        beat_id="setting_anchor",
                        paragraph_role="descriptive_synthesis",
                        selection_strategy="setting_anchor",
                        allow_single_strong_claim=True,
                    ),
                    place,
                    claims,
                ),
                (
                    "power_map",
                    BeatSpec(
                        beat_id="power_map",
                        paragraph_role="interpretive_synthesis",
                        selection_strategy="power_map",
                        allow_single_strong_claim=True,
                    ),
                    "Power map",
                    claims,
                ),
                (
                    "active_pressure",
                    BeatSpec(
                        beat_id="active_pressure",
                        paragraph_role="interpretive_synthesis",
                        selection_strategy="active_pressure",
                        allow_single_strong_claim=True,
                    ),
                    "Active pressure",
                    claims,
                ),
            ]
        if section_type == BibleSectionType.CHRONOLOGY:
            blueprint = [
                (
                    f"dated_turn:{label}",
                    BeatSpec(
                        beat_id="dated_turn",
                        paragraph_role="descriptive_synthesis",
                        selection_strategy="dated_turn",
                        allow_single_strong_claim=True,
                    ),
                    label,
                    [claim for claim in claims if claim.time_start == label],
                )
                for label in sorted({claim.time_start for claim in claims if claim.time_start})
            ]
            carryover_claims = [
                claim
                for claim in claims
                if not claim.time_start
                or claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
            ]
            if carryover_claims or self._contradiction_flags(claims, relationships):
                blueprint.append(
                    (
                        "carryover_pressure",
                        BeatSpec(
                            beat_id="carryover_pressure",
                            paragraph_role="uncertainty_framing",
                            selection_strategy="carryover_pressure",
                            min_distinct_claims=1,
                            allow_single_strong_claim=True,
                            allowed_certainties=(
                                ClaimStatus.VERIFIED,
                                ClaimStatus.PROBABLE,
                                ClaimStatus.CONTESTED,
                                ClaimStatus.RUMOR,
                                ClaimStatus.LEGEND,
                            ),
                            contradiction_policy="optional",
                        ),
                        "Carryover pressure",
                        carryover_claims or claims,
                    )
                )
            return blueprint
        if section_type == BibleSectionType.PEOPLE_AND_FACTIONS:
            grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
            for claim in claims:
                grouped[claim.subject].append(claim)
            blueprint = [
                (
                    f"actor_profile:{subject}",
                    BeatSpec(
                        beat_id="actor_profile",
                        paragraph_role="descriptive_synthesis",
                        selection_strategy="actor_profile",
                        min_distinct_claims=1,
                        allow_single_strong_claim=True,
                        require_support_context=True,
                    ),
                    subject,
                    group,
                )
                for subject, group in sorted(
                    grouped.items(),
                    key=lambda item: (
                        -max(self._CERTAINTY_WEIGHT.get(claim.status, 0) for claim in item[1]),
                        -len(item[1]),
                        item[0].lower(),
                    ),
                )[:3]
            ]
            if len(claims) >= 2:
                blueprint.append(
                    (
                        "power_web",
                        BeatSpec(
                            beat_id="power_web",
                            paragraph_role="interpretive_synthesis",
                            selection_strategy="power_web",
                            allow_single_strong_claim=True,
                            require_relationship_context=True,
                        ),
                        "Power web",
                        claims,
                    )
                )
            return blueprint
        if section_type == BibleSectionType.RUMORS_AND_CONTESTED:
            grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
            for claim in claims:
                grouped[claim.subject].append(claim)
            blueprint = [
                (
                    f"contested_record:{subject}",
                    BeatSpec(
                        beat_id="contested_record",
                        paragraph_role="uncertainty_framing",
                        selection_strategy="contested_record",
                        min_distinct_claims=1,
                        allow_single_strong_claim=True,
                        allowed_certainties=(
                            ClaimStatus.CONTESTED,
                            ClaimStatus.RUMOR,
                            ClaimStatus.LEGEND,
                            ClaimStatus.PROBABLE,
                            ClaimStatus.VERIFIED,
                        ),
                        contradiction_policy="optional",
                        allow_counterpart_claims=True,
                    ),
                    subject,
                    group,
                )
                for subject, group in sorted(
                    grouped.items(), key=lambda item: (-len(item[1]), item[0].lower())
                )[:3]
            ]
            if claims:
                blueprint.append(
                    (
                        "circulation",
                        BeatSpec(
                            beat_id="circulation",
                            paragraph_role="descriptive_synthesis",
                            selection_strategy="circulation",
                            min_distinct_claims=1,
                            allow_single_strong_claim=True,
                            allowed_certainties=(
                                ClaimStatus.CONTESTED,
                                ClaimStatus.RUMOR,
                                ClaimStatus.LEGEND,
                            ),
                            contradiction_policy="optional",
                        ),
                        "Circulation",
                        claims,
                    )
                )
            if self._rumor_operational_effect_needed(claims, relationships):
                blueprint.append(
                    (
                        "operational_effect",
                        BeatSpec(
                            beat_id="operational_effect",
                            paragraph_role="writer_guidance",
                            selection_strategy="operational_effect",
                            min_distinct_claims=1,
                            allow_single_strong_claim=True,
                            allowed_certainties=(
                                ClaimStatus.CONTESTED,
                                ClaimStatus.RUMOR,
                                ClaimStatus.LEGEND,
                                ClaimStatus.PROBABLE,
                                ClaimStatus.VERIFIED,
                            ),
                            contradiction_policy="optional",
                            allow_counterpart_claims=True,
                        ),
                        "Operational effect",
                        claims,
                    )
                )
            return blueprint
        return []

    def _initial_skip_reason(self, spec: BeatSpec, claims: list[ApprovedClaim]) -> str:
        if spec.require_relationship_context and not any(
            claim.claim_kind in {ClaimKind.RELATIONSHIP, ClaimKind.INSTITUTION} for claim in claims
        ):
            return "no_relationship_context"
        if not claims:
            return "out_of_section_scope"
        if not any(claim.evidence_ids for claim in claims):
            return "insufficient_evidence"
        return "insufficient_claims"

    def _select_claim_pack(
        self,
        section_type: BibleSectionType,
        spec: BeatSpec,
        candidate_claims: list[ApprovedClaim],
        section_claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> ClaimPack | None:
        _ = section_type
        allowed = [claim for claim in candidate_claims if claim.status in spec.allowed_certainties]
        strategy = spec.selection_strategy
        if strategy == "setting_anchor":
            selected = self._select_setting_anchor_claims(allowed)
        elif strategy == "power_map":
            selected = self._select_power_map_claims(allowed)
        elif strategy == "active_pressure":
            selected = self._select_active_pressure_claims(allowed)
        elif strategy == "dated_turn":
            selected = self._select_dated_turn_claims(allowed)
        elif strategy == "carryover_pressure":
            selected = self._select_carryover_claims(candidate_claims, relationships)
        elif strategy == "actor_profile":
            selected = self._select_actor_profile_claims(allowed)
        elif strategy == "power_web":
            selected = self._select_power_web_claims(allowed)
        elif strategy == "contested_record":
            selected = self._select_contested_record_claims(
                candidate_claims, section_claims, relationships
            )
        elif strategy == "circulation":
            selected = self._select_circulation_claims(allowed)
        elif strategy == "operational_effect":
            selected = self._select_operational_effect_claims(
                candidate_claims, section_claims, relationships
            )
        else:
            selected = []
        if not selected:
            return None
        selected = self._dedupe_claims(selected)
        if spec.contradiction_policy == "forbidden":
            selected = self._prune_contradictory_claims(selected, relationships)
        role_map = self._assign_claim_roles(spec.beat_id, selected)
        if len([role for role in role_map.values() if role == "anchor"]) != 1:
            return None
        if not self._pack_meets_eligibility(spec, selected, relationships):
            return None
        evidence_ids = sorted(
            {evidence_id for claim in selected for evidence_id in claim.evidence_ids}
        )
        source_ids = sorted(
            {
                evidence.source_id
                for evidence_id in evidence_ids
                if (evidence := self.evidence_store.get_evidence(evidence_id)) is not None
            }
        )
        return ClaimPack(
            beat_id=spec.beat_id,
            claim_roles=role_map,
            selected_claims=selected,
            selected_evidence_ids=evidence_ids,
            selected_source_ids=source_ids,
        )

    def _select_setting_anchor_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        anchor = next((claim for claim in claims if claim.claim_kind == ClaimKind.PLACE), None)
        anchor = anchor or next(
            (claim for claim in claims if claim.claim_kind == ClaimKind.EVENT), None
        )
        if anchor is None:
            return []
        support = [
            claim
            for claim in claims
            if claim.claim_id != anchor.claim_id
            and claim.claim_kind in {ClaimKind.EVENT, ClaimKind.PERSON, ClaimKind.INSTITUTION}
        ]
        detail = [
            claim for claim in claims if claim.claim_id != anchor.claim_id and claim not in support
        ]
        return [anchor] + support[:2] + detail[:1]

    def _select_power_map_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        anchor = next(
            (
                claim
                for claim in claims
                if claim.claim_kind
                in {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP, ClaimKind.PERSON}
            ),
            None,
        )
        if anchor is None:
            return []
        support = [
            claim
            for claim in claims
            if claim.claim_id != anchor.claim_id
            and claim.claim_kind
            in {ClaimKind.PERSON, ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP}
        ]
        pressure = [
            claim
            for claim in claims
            if claim.claim_id != anchor.claim_id
            and claim.claim_kind in {ClaimKind.EVENT, ClaimKind.PRACTICE}
        ]
        return [anchor] + support[:2] + pressure[:1]

    def _select_active_pressure_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        anchor = next(
            (
                claim
                for claim in claims
                if claim.claim_kind
                in {
                    ClaimKind.EVENT,
                    ClaimKind.PRACTICE,
                    ClaimKind.RELATIONSHIP,
                    ClaimKind.INSTITUTION,
                }
            ),
            None,
        )
        if anchor is None:
            return []
        support = [
            claim
            for claim in claims
            if claim.claim_id != anchor.claim_id
            and claim.claim_kind in {ClaimKind.PERSON, ClaimKind.INSTITUTION, ClaimKind.EVENT}
        ]
        return [anchor] + support[:3]

    def _select_dated_turn_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        anchor = next((claim for claim in claims if claim.claim_kind == ClaimKind.EVENT), None) or (
            claims[0] if claims else None
        )
        if anchor is None:
            return []
        return [anchor] + [claim for claim in claims if claim.claim_id != anchor.claim_id][:3]

    def _select_carryover_claims(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> list[ApprovedClaim]:
        unsettled = [
            claim
            for claim in claims
            if claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
            or not claim.time_start
        ]
        if unsettled:
            return unsettled[:3]
        related_ids = {
            relationship.claim_id
            for relationship in relationships
            if relationship.relationship_type in {"contradicts", "supersedes", "superseded_by"}
        }
        return [claim for claim in claims if claim.claim_id in related_ids][:2]

    def _select_actor_profile_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        anchor = next(
            (
                claim
                for claim in claims
                if claim.claim_kind in {ClaimKind.PERSON, ClaimKind.INSTITUTION}
            ),
            None,
        ) or (claims[0] if claims else None)
        if anchor is None:
            return []
        support = [
            claim
            for claim in claims
            if claim.claim_id != anchor.claim_id
            and claim.claim_kind in {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP, ClaimKind.EVENT}
        ]
        detail = [
            claim for claim in claims if claim.claim_id != anchor.claim_id and claim not in support
        ]
        return [anchor] + support[:2] + detail[:1]

    def _select_power_web_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        relational = sorted(
            [
                claim
                for claim in claims
                if claim.claim_kind
                in {ClaimKind.RELATIONSHIP, ClaimKind.INSTITUTION, ClaimKind.PERSON}
            ],
            key=lambda claim: (
                0 if claim.claim_kind in {ClaimKind.RELATIONSHIP, ClaimKind.INSTITUTION} else 1,
                claim.subject.lower(),
            ),
        )
        return relational[:4]

    def _select_contested_record_claims(
        self,
        topic_claims: list[ApprovedClaim],
        section_claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> list[ApprovedClaim]:
        contested = [
            claim
            for claim in topic_claims
            if claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
        ]
        if not contested:
            return []
        anchor = contested[0]
        return (
            [anchor]
            + contested[1:2]
            + self._counterpart_claims(anchor, section_claims, relationships)[:2]
        )

    def _select_circulation_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        return [
            claim
            for claim in claims
            if claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
        ][:4]

    def _select_operational_effect_claims(
        self,
        topic_claims: list[ApprovedClaim],
        section_claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> list[ApprovedClaim]:
        contested = self._select_circulation_claims(topic_claims)
        if not contested:
            return []
        return [contested[0]] + self._counterpart_claims(
            contested[0], section_claims, relationships
        )[:2]

    def _counterpart_claims(
        self,
        anchor: ApprovedClaim,
        section_claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> list[ApprovedClaim]:
        related_ids = {
            relationship.related_claim_id
            for relationship in relationships
            if relationship.claim_id == anchor.claim_id
        }
        external_matches = [
            claim
            for claim in self.truth_store.list_claims()
            if claim.claim_id != anchor.claim_id
            and claim.status in {ClaimStatus.PROBABLE, ClaimStatus.VERIFIED}
            and (
                claim.subject == anchor.subject
                or claim.predicate == anchor.predicate
                or claim.claim_id in related_ids
            )
        ]
        if external_matches:
            return external_matches
        return [
            claim
            for claim in section_claims
            if claim.claim_id != anchor.claim_id
            and claim.status in {ClaimStatus.PROBABLE, ClaimStatus.VERIFIED}
            and claim.subject == anchor.subject
        ]

    def _assign_claim_roles(self, beat_id: str, claims: list[ApprovedClaim]) -> dict[str, str]:
        if not claims:
            return {}
        roles = {claims[0].claim_id: "anchor"}
        support_taken = False
        for claim in claims[1:]:
            if beat_id in {"contested_record", "operational_effect"} and claim.status in {
                ClaimStatus.PROBABLE,
                ClaimStatus.VERIFIED,
            }:
                roles[claim.claim_id] = "contrast"
            elif claim.claim_kind in {
                ClaimKind.RELATIONSHIP,
                ClaimKind.INSTITUTION,
                ClaimKind.EVENT,
            }:
                roles[claim.claim_id] = "pressure"
            elif not support_taken:
                roles[claim.claim_id] = "support"
                support_taken = True
            else:
                roles[claim.claim_id] = "detail"
        return roles

    def _pack_meets_eligibility(
        self,
        spec: BeatSpec,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> bool:
        distinct_evidence = {evidence_id for claim in claims for evidence_id in claim.evidence_ids}
        distinct_claim_count = len({claim.claim_id for claim in claims})
        if len(distinct_evidence) < spec.min_distinct_evidence:
            return False
        if distinct_claim_count < spec.min_distinct_claims and not (
            spec.allow_single_strong_claim and self._is_single_strong_claim(claims)
        ):
            return False
        contradiction_present = bool(self._contradiction_flags(claims, relationships))
        if spec.contradiction_policy == "required" and not contradiction_present:
            return False
        if spec.contradiction_policy == "forbidden" and contradiction_present:
            return False
        if spec.require_relationship_context and not any(
            claim.claim_kind in {ClaimKind.RELATIONSHIP, ClaimKind.INSTITUTION} for claim in claims
        ):
            return False
        if spec.beat_id in {
            "contested_record",
            "circulation",
            "operational_effect",
            "carryover_pressure",
        }:
            return (
                any(
                    claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
                    for claim in claims
                )
                or contradiction_present
            )
        return True

    def _is_single_strong_claim(self, claims: list[ApprovedClaim]) -> bool:
        return (
            len(claims) == 1
            and claims[0].status == ClaimStatus.VERIFIED
            and bool(claims[0].evidence_ids)
        )

    def _pack_skip_reason(
        self,
        pack: ClaimPack,
        spec: BeatSpec,
        seen_signatures: list[tuple[str, frozenset[str]]],
    ) -> str | None:
        if len(pack.selected_evidence_ids) < spec.min_distinct_evidence:
            return "insufficient_evidence"
        if any(claim.status not in spec.allowed_certainties for claim in pack.selected_claims):
            return "certainty_policy_blocked"
        if spec.paragraph_role == "writer_guidance":
            return None
        anchor_signature = self._claim_fact_signature(self._pack_anchor(pack))
        pack_signatures = frozenset(
            self._claim_fact_signature(claim) for claim in pack.selected_claims
        )
        for seen_anchor, seen_pack in seen_signatures:
            overlap = len(pack_signatures.intersection(seen_pack)) / max(
                len(pack_signatures.union(seen_pack)), 1
            )
            if anchor_signature == seen_anchor and overlap >= 0.5:
                return "no_distinct_information"
        return None

    def _prune_contradictory_claims(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> list[ApprovedClaim]:
        pruned = list(claims)
        while len(pruned) > 1 and self._contradiction_flags(pruned, relationships):
            pruned.pop()
        return pruned

    def _paragraph_from_claim_pack(
        self,
        *,
        heading: str,
        kind: str,
        paragraph_role: str,
        pack: ClaimPack,
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        relationships: list[ClaimRelationship],
        text: str,
    ) -> BibleSectionParagraph:
        contradiction_flags, supersession_flags = self._relationship_flags_for_claim_ids(
            [claim.claim_id for claim in pack.selected_claims],
            relationships,
        )
        citations = self._compact_citations(
            pack.selected_evidence_ids, evidence_by_id, source_by_id
        )
        suffix = f" Sources: {citations}." if citations else ""
        return BibleSectionParagraph(
            paragraph_id=self._stable_paragraph_id(
                kind,
                heading,
                [claim.claim_id for claim in pack.selected_claims],
            ),
            heading=heading,
            text=f"{text}{suffix}",
            paragraph_kind=kind,
            paragraph_role=paragraph_role,
            claim_ids=[claim.claim_id for claim in pack.selected_claims],
            evidence_ids=pack.selected_evidence_ids,
            source_ids=pack.selected_source_ids,
            contradiction_flags=contradiction_flags,
            supersession_flags=supersession_flags,
        )

    def _compose_priority_paragraph_text(
        self,
        section_type: BibleSectionType,
        spec: BeatSpec,
        pack: ClaimPack,
        heading: str,
        relationships: list[ClaimRelationship],
    ) -> str:
        _ = section_type
        anchor = self._pack_anchor(pack)
        support_note = self._join_claim_notes(self._claims_for_role(pack, "support"), limit=3)
        detail_note = self._join_claim_notes(self._claims_for_role(pack, "detail"), limit=2)
        pressure_note = self._join_claim_notes(self._claims_for_role(pack, "pressure"), limit=2)
        contrast_note = self._join_claim_notes(self._claims_for_role(pack, "contrast"), limit=2)
        anchor_note = self._claim_scene_note(anchor)

        if spec.beat_id == "setting_anchor":
            return self._compose_notebook_paragraph(
                lead=f"{heading} is anchored by {anchor_note}.",
                development=(
                    f"Concrete support comes through {support_note}."
                    if support_note
                    else (
                        f"Concrete detail stays visible through {detail_note}."
                        if detail_note
                        else ""
                    )
                ),
                anchor=(
                    f"The wider setting stays active through {pressure_note}."
                    if pressure_note
                    else ""
                ),
            )
        if spec.beat_id == "power_map":
            return self._compose_notebook_paragraph(
                lead=f"Power in {anchor.place or heading} concentrates around {anchor_note}.",
                development=(
                    f"That map widens through {support_note}."
                    if support_note
                    else (f"Pressure gathers through {pressure_note}." if pressure_note else "")
                ),
                anchor=(f"The consequences stay visible in {detail_note}." if detail_note else ""),
            )
        if spec.beat_id == "active_pressure":
            return self._compose_notebook_paragraph(
                lead=f"The active pressure runs through {anchor_note}.",
                development=(
                    f"It reaches daily movement through {pressure_note}."
                    if pressure_note
                    else (f"It stays concrete through {support_note}." if support_note else "")
                ),
                anchor=(f"Additional detail comes from {detail_note}." if detail_note else ""),
            )
        if spec.beat_id == "dated_turn":
            return self._compose_notebook_paragraph(
                lead=f"By {heading}, {anchor_note}.",
                development=(
                    f"The dated turn also includes {support_note}." if support_note else ""
                ),
                anchor=(f"Carry the timing through {detail_note}." if detail_note else ""),
            )
        if spec.beat_id == "carryover_pressure":
            return self._compose_notebook_paragraph(
                lead=f"Some pressure carries forward without a clean date: {anchor_note}.",
                development=(
                    f"Related unsettled material includes {support_note}." if support_note else ""
                ),
                uncertainty=self._uncertainty_boundary_sentence(
                    pack.selected_claims, relationships
                ),
            )
        if spec.beat_id == "actor_profile":
            return self._compose_notebook_paragraph(
                lead=f"{heading} is anchored by the fact that {anchor_note}.",
                development=(
                    f"Leverage and affiliations show up through {support_note}."
                    if support_note
                    else (
                        f"Pressure around {heading} shows up through {pressure_note}."
                        if pressure_note
                        else ""
                    )
                ),
                anchor=(f"Specific detail comes from {detail_note}." if detail_note else ""),
            )
        if spec.beat_id == "power_web":
            return self._compose_notebook_paragraph(
                lead=f"The wider power web is legible through {anchor_note}.",
                development=(
                    f"It connects outward through {support_note}." if support_note else ""
                ),
                anchor=(
                    f"Pressure points stay visible in {pressure_note}." if pressure_note else ""
                ),
            )
        if spec.beat_id == "contested_record":
            return self._compose_notebook_paragraph(
                lead=f"On {heading}, accounts diverge over {anchor_note}.",
                development=(
                    f"The dispute stays active in {support_note}."
                    if support_note
                    else (
                        f"The stable contrast points toward {contrast_note}."
                        if contrast_note
                        else ""
                    )
                ),
                uncertainty=self._uncertainty_boundary_sentence(
                    pack.selected_claims, relationships
                ),
            )
        if spec.beat_id == "circulation":
            viewpoints = sorted(
                {claim.viewpoint_scope for claim in pack.selected_claims if claim.viewpoint_scope}
            )
            circulation_context = (
                self._human_join(viewpoints) if viewpoints else "different witnesses"
            )
            return self._compose_notebook_paragraph(
                lead=f"These accounts circulate through {circulation_context}.",
                development=f"They cluster around {anchor_note}.",
                anchor=(f"Repeated details include {support_note}." if support_note else ""),
            )
        if spec.beat_id == "operational_effect":
            return self._compose_notebook_paragraph(
                lead=(
                    f"When scenes touch this dispute, treat {anchor_note} as "
                    "contested rather than silently settled."
                ),
                development=(
                    f"If a counter-line is needed on the page, use {contrast_note}."
                    if contrast_note
                    else ""
                ),
            )
        return self._compose_notebook_paragraph(lead=f"{anchor_note}.")

    def _summarize_composition_metrics(
        self,
        plan: SectionCompositionPlan,
    ) -> BibleSectionCompositionMetrics:
        metrics = plan.section_coverage_metrics
        return BibleSectionCompositionMetrics(
            thin_section=(
                metrics.produced_beat_count == 0
                or metrics.produced_beat_count < max(1, min(2, metrics.target_beat_count))
                or metrics.claim_density < 1.5
            ),
            target_beats=metrics.target_beat_count,
            produced_beats=metrics.produced_beat_count,
            skipped_beat_ids=[item.beat_id for item in plan.skipped_beats],
            skipped_reasons=[item.reason for item in plan.skipped_beats],
            claim_density=metrics.claim_density,
            evidence_density=metrics.evidence_density,
            contradiction_presence=metrics.contradiction_presence,
        )

    def _pack_anchor(self, pack: ClaimPack) -> ApprovedClaim:
        anchor_id = next(
            claim_id for claim_id, role in pack.claim_roles.items() if role == "anchor"
        )
        return next(claim for claim in pack.selected_claims if claim.claim_id == anchor_id)

    def _claims_for_role(self, pack: ClaimPack, role: str) -> list[ApprovedClaim]:
        return [
            claim for claim in pack.selected_claims if pack.claim_roles.get(claim.claim_id) == role
        ]

    def _claim_fact_signature(self, claim: ApprovedClaim) -> str:
        return (
            f"{claim.subject.strip().lower()}|"
            f"{claim.predicate.strip().lower()}|"
            f"{claim.value.strip().lower()}"
        )

    def _dedupe_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        seen: set[str] = set()
        deduped: list[ApprovedClaim] = []
        for claim in claims:
            signature = self._claim_fact_signature(claim)
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(claim)
        return deduped

    def _uncertainty_boundary_sentence(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> str:
        contrast = [
            claim
            for claim in claims
            if claim.status in {ClaimStatus.PROBABLE, ClaimStatus.VERIFIED}
        ]
        if contrast:
            return (
                "Keep the stronger line explicit as contrast only: "
                f"{self._join_claim_notes(contrast, limit=2)}."
            )
        contradiction_notes = self._contradiction_flags(claims, relationships)
        if contradiction_notes:
            return (
                "Keep the contradiction visible around " + ", ".join(contradiction_notes[:2]) + "."
            )
        return "Keep the uncertainty explicit rather than folding it into settled description."

    def _rumor_operational_effect_needed(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> bool:
        return bool(self._contradiction_flags(claims, relationships)) or any(
            claim.viewpoint_scope for claim in claims
        )

    def _compose_notebook_paragraph(
        self,
        *,
        lead: str,
        development: str = "",
        anchor: str = "",
        uncertainty: str = "",
    ) -> str:
        parts = [lead.strip(), development.strip(), anchor.strip(), uncertainty.strip()]
        return " ".join(part for part in parts if part)

    def _paragraph_development_sentence(
        self,
        prefix: str,
        claims: list[ApprovedClaim],
        *,
        limit: int = 4,
    ) -> str:
        if not claims:
            return ""
        return prefix + self._join_claim_notes(claims, limit=limit) + "."

    def _merge_development_sentences(self, *sentences: str) -> str:
        clean = [sentence.strip() for sentence in sentences if sentence.strip()]
        if not clean:
            return ""
        if len(clean) == 1:
            return clean[0]
        first = clean[0].rstrip(".")
        tail = [sentence.rstrip(".") for sentence in clean[1:]]
        return first + "; " + "; ".join(tail) + "."

    def _setting_overview_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            key = claim.place or (
                claim.subject if claim.claim_kind == ClaimKind.PLACE else "Wider setting"
            )
            grouped[key].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for place, group in sorted(grouped.items()):
            place_claims = [claim for claim in group if claim.claim_kind == ClaimKind.PLACE]
            institutions = [
                claim
                for claim in group
                if claim.claim_kind in {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP}
            ]
            events = [claim for claim in group if claim.claim_kind == ClaimKind.EVENT]
            people = [claim for claim in group if claim.claim_kind == ClaimKind.PERSON]
            lead = self._writer_surface_sentence(
                place_claims or group,
                context=f"The clearest setting anchor for {place} is that",
                fallback_context="A softer working edge still keeps",
            )
            development = self._merge_development_sentences(
                self._paragraph_development_sentence(
                    "Let public pressure arrive through ", events, limit=3
                ),
                self._paragraph_development_sentence(
                    "Keep the local cast and civic machinery moving through ",
                    institutions + people,
                    limit=4,
                ),
            )
            anchor_sentence = self._scene_anchor_sentence(group)
            uncertainty_sentence = self._bounded_uncertainty_sentence(group, relationships)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=place,
                    kind="setting_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        anchor=anchor_sentence,
                        uncertainty=uncertainty_sentence,
                    ),
                )
            )
        return paragraphs

    def _chronology_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.time_start or "Undated but relevant"].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for label, group in sorted(grouped.items(), key=lambda item: item[0]):
            if label == "Undated but relevant":
                text = self._compose_notebook_paragraph(
                    lead=(
                        "Keep these pressures in the background even when the exact "
                        "date stays unsettled: "
                        + self._join_claim_notes(group, limit=5)
                        + "."
                    ),
                    uncertainty=self._bounded_uncertainty_sentence(group, relationships),
                )
                kind = "chronology_undated"
            else:
                lead = self._writer_surface_sentence(
                    group,
                    context=f"By {label}, the clearest dated turn is that",
                    fallback_context="A softer timing read still leaves room for",
                )
                development = self._paragraph_development_sentence(
                    "Treat the dated shift as something characters have to move through: ",
                    group,
                    limit=3,
                )
                text = self._compose_notebook_paragraph(
                    lead=lead,
                    development=development,
                    anchor=self._scene_anchor_sentence(group),
                    uncertainty=self._bounded_uncertainty_sentence(group, relationships),
                )
                kind = "chronology_entry"
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=label,
                    kind=kind,
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=text,
                )
            )
        return paragraphs

    def _people_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.subject].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for actor, group in sorted(grouped.items()):
            affiliations = [
                claim
                for claim in group
                if claim.claim_kind in {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP}
            ]
            lead = self._writer_surface_sentence(
                group,
                context=f"{actor} comes into focus through the fact that",
                fallback_context="A softer working read still keeps",
            )
            development = self._merge_development_sentences(
                self._paragraph_development_sentence(
                    "Tie leverage and faction pull to ",
                    affiliations,
                    limit=3,
                ),
                self._social_pressure_sentence(group),
            )
            contradiction_notes, _ = self._relationship_flags_for_claim_ids(
                [claim.claim_id for claim in group],
                relationships,
            )
            uncertainty_parts = []
            if contradiction_notes:
                uncertainty_parts.append(
                    f"Accounts diverge around {', '.join(contradiction_notes[:2])}."
                )
            bounded = self._bounded_uncertainty_sentence(group, relationships)
            if bounded:
                uncertainty_parts.append(bounded)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=actor,
                    kind="actor_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        uncertainty=" ".join(uncertainty_parts),
                    ),
                )
            )
        return paragraphs

    def _daily_life_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        routines = [claim for claim in claims if claim.claim_kind == ClaimKind.PRACTICE]
        materials = [
            claim for claim in claims if claim.claim_kind in {ClaimKind.OBJECT, ClaimKind.BELIEF}
        ]
        paragraphs: list[BibleSectionParagraph] = []
        if routines:
            lead = self._writer_surface_sentence(
                routines,
                context="The clearest daily-life anchor is that",
                fallback_context="A softer working detail still keeps",
            )
            development = self._social_pressure_sentence(routines)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Routine and practice",
                    kind="routine_cluster",
                    claims=routines,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        anchor=self._scene_anchor_sentence(routines),
                        uncertainty=self._bounded_uncertainty_sentence(routines, relationships),
                    ),
                )
            )
        if materials:
            lead = self._writer_surface_sentence(
                materials,
                context="Material detail comes through the fact that",
                fallback_context="A softer material cue still keeps",
            )
            development = self._paragraph_development_sentence(
                "Use those cues to make the world feel touched, worn, and specific: ",
                materials,
                limit=3,
            )
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Material and sensory detail",
                    kind="material_cluster",
                    claims=materials,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        uncertainty=self._bounded_uncertainty_sentence(materials, relationships),
                    ),
                )
            )
        return paragraphs

    def _institutions_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.subject].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for institution, group in sorted(grouped.items()):
            lead = self._writer_surface_sentence(
                group,
                context=f"{institution} works best on the page when",
                fallback_context="A softer institutional read still keeps",
            )
            development = self._merge_development_sentences(
                "Use the institution as pressure, process, and gatekeeping rather "
                "than background ornament.",
                self._social_pressure_sentence(group),
            )
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=institution,
                    kind="institution_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        uncertainty=self._bounded_uncertainty_sentence(group, relationships),
                    ),
                )
            )
        return paragraphs

    def _economics_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        trade_claims = [claim for claim in claims if claim.claim_kind == ClaimKind.PRACTICE]
        material_claims = [claim for claim in claims if claim.claim_kind == ClaimKind.OBJECT]
        paragraphs: list[BibleSectionParagraph] = []
        if trade_claims:
            lead = self._writer_surface_sentence(
                trade_claims,
                context="Trade pressure is clearest when",
                fallback_context="A softer economic read still keeps",
            )
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Trade, exchange, and pressure",
                    kind="economy_cluster",
                    claims=trade_claims,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=self._social_pressure_sentence(trade_claims),
                        anchor=self._scene_anchor_sentence(trade_claims),
                        uncertainty=self._bounded_uncertainty_sentence(trade_claims, relationships),
                    ),
                )
            )
        if material_claims:
            lead = self._writer_surface_sentence(
                material_claims,
                context="Material culture stays concrete through the fact that",
                fallback_context="A softer material cue still keeps",
            )
            development = self._paragraph_development_sentence(
                "Keep the material cue sceneable enough to name outright: ",
                material_claims,
                limit=3,
            )
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Sceneable material cues",
                    kind="material_culture_cluster",
                    claims=material_claims,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        uncertainty=self._bounded_uncertainty_sentence(
                            material_claims, relationships
                        ),
                    ),
                )
            )
        if trade_claims or material_claims:
            combined = trade_claims + material_claims
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Writer's working line",
                    kind="economy_notebook",
                    claims=combined,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead="Use the economy as daily pressure rather than abstract background.",
                        development=(
                            "Treat prices, tools, and exchange routines as "
                            "something characters handle in motion."
                        ),
                        anchor=self._scene_anchor_sentence(combined),
                        uncertainty=self._bounded_uncertainty_sentence(combined, relationships),
                    ),
                )
            )
        return paragraphs

    def _writer_surface_sentence(
        self,
        claims: list[ApprovedClaim],
        *,
        context: str,
        fallback_context: str,
        limit: int = 4,
    ) -> str:
        verified_claims = [claim for claim in claims if claim.status == ClaimStatus.VERIFIED]
        probable_claims = [claim for claim in claims if claim.status == ClaimStatus.PROBABLE]
        settled_claims = verified_claims or probable_claims or claims
        sentence = f"{context} {self._join_claim_notes(settled_claims, limit=limit)}."
        if verified_claims and probable_claims:
            sentence += (
                f" {fallback_context} "
                f"{self._join_claim_notes(probable_claims, limit=min(limit, 3))}."
            )
        return sentence

    def _rumor_lead_sentence(
        self,
        topic: str,
        claims: list[ApprovedClaim],
        believers: str,
    ) -> str:
        rumors = [claim for claim in claims if claim.status == ClaimStatus.RUMOR]
        legends = [claim for claim in claims if claim.status == ClaimStatus.LEGEND]
        contested = [claim for claim in claims if claim.status == ClaimStatus.CONTESTED]
        probable = [claim for claim in claims if claim.status == ClaimStatus.PROBABLE]
        verified = [claim for claim in claims if claim.status == ClaimStatus.VERIFIED]
        if rumors:
            return (
                f"Around {topic}, {believers} keep repeating "
                f"{self._join_claim_notes(rumors, limit=3)}."
            )
        if legends:
            return (
                f"Around {topic}, local legend keeps alive "
                f"{self._join_claim_notes(legends, limit=3)}."
            )
        if contested:
            return (
                f"On {topic}, the record splits over {self._join_claim_notes(contested, limit=3)}."
            )
        if probable:
            return (
                f"On {topic}, the strongest working line points toward "
                f"{self._join_claim_notes(probable, limit=3)}."
            )
        return f"On {topic}, canon records {self._join_claim_notes(verified or claims, limit=3)}."

    def _rumor_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.subject].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for topic, group in sorted(grouped.items()):
            believers = (
                ", ".join(
                    sorted({claim.viewpoint_scope for claim in group if claim.viewpoint_scope})
                )
                or "different witnesses"
            )
            lead = self._rumor_lead_sentence(topic, group, believers)
            development = (
                "Use this as suspicion, gossip, or motive, not as settled exposition."
            )
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=topic,
                    kind="contested_topic",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=self._compose_notebook_paragraph(
                        lead=lead,
                        development=development,
                        anchor=self._scene_anchor_sentence(group),
                        uncertainty=self._bounded_uncertainty_sentence(group, relationships),
                    ),
                )
            )
        return paragraphs

    def _author_decision_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        return [
            self._paragraph_from_claim_group(
                heading=claim.subject,
                kind="author_guidance",
                claims=[claim],
                evidence_by_id=evidence_by_id,
                source_by_id=source_by_id,
                relationships=relationships,
                text=(
                    f"Author choice: depict {claim.subject} as {claim.value}. "
                    "Keep it available for drafting decisions, but do not present "
                    "it as settled canon in strict-fact outputs."
                ),
            )
            for claim in claims
        ]

    def _grouped_summary_paragraphs(
        self, claims, evidence_by_id, source_by_id, relationships, section_type
    ):
        grouped: dict[ClaimKind, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.claim_kind].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for claim_kind, group in sorted(grouped.items(), key=lambda item: item[0].value):
            label = self._FACET_LABELS.get(claim_kind, claim_kind.value.replace("_", " "))
            text = (
                f"{self._SECTION_TITLES[section_type]} is anchored by "
                + "; ".join(f"{claim.subject} {claim.value}" for claim in group[:4])
                + "."
            )
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=label.title(),
                    kind=f"{claim_kind.value}_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=text,
                )
            )
        return paragraphs

    def _certainty_sentences(
        self,
        claims: list[ApprovedClaim],
        *,
        settled_intro: str,
        probable_intro: str,
        contested_intro: str = "Accounts differ over",
        rumor_intro: str = "Rumor keeps circling that",
        legend_intro: str = "Legend keeps alive the claim that",
    ) -> list[str]:
        sentences: list[str] = []
        grouped = {
            ClaimStatus.VERIFIED: [
                claim for claim in claims if claim.status == ClaimStatus.VERIFIED
            ],
            ClaimStatus.PROBABLE: [
                claim for claim in claims if claim.status == ClaimStatus.PROBABLE
            ],
            ClaimStatus.CONTESTED: [
                claim for claim in claims if claim.status == ClaimStatus.CONTESTED
            ],
            ClaimStatus.RUMOR: [claim for claim in claims if claim.status == ClaimStatus.RUMOR],
            ClaimStatus.LEGEND: [claim for claim in claims if claim.status == ClaimStatus.LEGEND],
            ClaimStatus.AUTHOR_CHOICE: [
                claim
                for claim in claims
                if claim.status == ClaimStatus.AUTHOR_CHOICE or claim.author_choice
            ],
        }
        if grouped[ClaimStatus.VERIFIED]:
            sentences.append(
                f"{settled_intro} {self._join_claim_notes(grouped[ClaimStatus.VERIFIED])}."
            )
        if grouped[ClaimStatus.PROBABLE]:
            sentences.append(
                f"{probable_intro} {self._join_claim_notes(grouped[ClaimStatus.PROBABLE])}."
            )
        if grouped[ClaimStatus.CONTESTED]:
            sentences.append(
                f"{contested_intro} {self._join_claim_notes(grouped[ClaimStatus.CONTESTED])}."
            )
        if grouped[ClaimStatus.RUMOR]:
            sentences.append(f"{rumor_intro} {self._join_claim_notes(grouped[ClaimStatus.RUMOR])}.")
        if grouped[ClaimStatus.LEGEND]:
            sentences.append(
                f"{legend_intro} {self._join_claim_notes(grouped[ClaimStatus.LEGEND])}."
            )
        if grouped[ClaimStatus.AUTHOR_CHOICE]:
            sentences.append(
                "Author choice keeps the drafting default at "
                + self._join_claim_notes(grouped[ClaimStatus.AUTHOR_CHOICE])
                + "."
            )
        return sentences or [f"{settled_intro} {self._join_claim_notes(claims)}."]

    def _scene_anchor_sentence(self, claims: list[ApprovedClaim]) -> str:
        places = list(dict.fromkeys(claim.place for claim in claims if claim.place))
        times = list(dict.fromkeys(claim.time_start for claim in claims if claim.time_start))
        viewpoints = list(
            dict.fromkeys(claim.viewpoint_scope for claim in claims if claim.viewpoint_scope)
        )
        fragments: list[str] = []
        if places:
            fragments.append(f"in {self._human_join(places[:2])}")
        if times:
            fragments.append(f"mainly around {self._human_join(times[:2])}")
        if viewpoints:
            fragments.append(f"through the perspective of {self._human_join(viewpoints[:2])}")
        if not fragments:
            return ""
        return "Stage scenes " + ", ".join(fragments) + "."

    def _social_pressure_sentence(self, claims: list[ApprovedClaim]) -> str:
        people = [claim for claim in claims if claim.claim_kind == ClaimKind.PERSON]
        institutions = [claim for claim in claims if claim.claim_kind == ClaimKind.INSTITUTION]
        relationships = [claim for claim in claims if claim.claim_kind == ClaimKind.RELATIONSHIP]
        pressure_claims = (people + institutions + relationships)[:4]
        if not pressure_claims:
            return ""
        return (
            "Let the social pressure gather around "
            + self._join_claim_notes(pressure_claims, limit=4)
            + "."
        )

    def _bounded_uncertainty_sentence(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> str:
        contested_claims = [
            claim
            for claim in claims
            if claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
        ]
        if contested_claims:
            return (
                "Keep uncertainty bounded: treat "
                + self._join_claim_notes(contested_claims, limit=3)
                + " as disputed material rather than settled fact."
            )
        contradiction_notes = self._contradiction_flags(claims, relationships)
        if contradiction_notes:
            return (
                "Keep the contradiction visible around " + ", ".join(contradiction_notes[:2]) + "."
            )
        probable_claims = [claim for claim in claims if claim.status == ClaimStatus.PROBABLE]
        if probable_claims:
            return (
                "Keep uncertainty bounded: treat "
                + self._join_claim_notes(probable_claims, limit=3)
                + " as the working read rather than hard fact."
            )
        return ""

    def _join_claim_notes(self, claims: list[ApprovedClaim], *, limit: int = 4) -> str:
        notes: list[str] = []
        for claim in claims[:limit]:
            note = self._claim_scene_note(claim)
            if note not in notes:
                notes.append(note)
        return self._human_join(notes)

    def _claim_scene_note(self, claim: ApprovedClaim) -> str:
        predicate = {
            "has_feature": "has",
            "serves_as": "serves as",
            "works_with": "works with",
            "rings_at": "rings at",
            "rose_during": "rose during",
            "is_reported_in": "is reported in",
            "circulates_in": "circulates in",
            "were_paid_in": "were paid in",
            "posts": "posts",
        }.get(claim.predicate, claim.predicate.replace("_", " ").strip())
        fragments = [claim.subject.strip(), predicate, claim.value.strip()]
        note = " ".join(fragment for fragment in fragments if fragment).strip()
        if (
            claim.place
            and claim.claim_kind != ClaimKind.PLACE
            and claim.place.lower() not in note.lower()
        ):
            note += f" in {claim.place}"
        if (
            claim.time_start
            and claim.claim_kind == ClaimKind.EVENT
            and claim.time_start not in note
        ):
            note += f" by {claim.time_start}"
        return note

    def _retrieval_seed_query(
        self,
        section_type: BibleSectionType,
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> str:
        parts = [self._SECTION_TITLES[section_type]]
        if filters.focus:
            parts.append(filters.focus)
        if profile and profile.narrative_focus:
            parts.append(profile.narrative_focus)
        if profile and profile.social_lens:
            parts.append(profile.social_lens)
        if filters.place or (profile and profile.geography):
            parts.append(filters.place or profile.geography or "")
        if profile and (profile.era or profile.time_start or profile.time_end):
            parts.append(
                profile.era
                or " ".join(item for item in [profile.time_start, profile.time_end] if item)
            )
        if profile and profile.desired_facets:
            parts.append(" ".join(profile.desired_facets))
        return " ".join(part for part in parts if part).strip()

    def _blended_claim_score(
        self,
        claim: ApprovedClaim,
        seed_query: str,
        projection_order: dict[str, int],
        relationship_counts: Counter,
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
        section_type: BibleSectionType,
    ) -> int:
        projection_bonus = max(
            0, len(projection_order) - projection_order.get(claim.claim_id, len(projection_order))
        )
        lexical_bonus = self._claim_focus_score(claim, seed_query)
        narrative_bonus = (
            self._claim_focus_score(claim, profile.narrative_focus or "") if profile else 0
        )
        social_bonus = self._claim_focus_score(claim, profile.social_lens or "") if profile else 0
        requested_buckets = set(self._requested_coverage_buckets(section_type, profile))
        facet_bonus = (
            3
            if requested_buckets.intersection(self._claim_coverage_buckets(claim, section_type))
            else 0
        )
        section_bonus = 2 if claim.claim_kind in self._SECTION_KINDS[section_type] else 0
        viewpoint_bonus = (
            3 if filters.viewpoint_scope and claim.viewpoint_scope == filters.viewpoint_scope else 0
        )
        time_bonus = self._time_alignment_bonus(
            claim,
            filters.time_start or (profile.time_start if profile else None),
            filters.time_end or (profile.time_end if profile else None),
        )
        return (
            projection_bonus * 5
            + lexical_bonus * 3
            + narrative_bonus * 2
            + social_bonus
            + facet_bonus
            + section_bonus
            + (3 if filters.place and claim.place == filters.place else 0)
            + (2 if profile and profile.geography and claim.place == profile.geography else 0)
            + time_bonus
            + viewpoint_bonus
            + self._CERTAINTY_WEIGHT.get(claim.status, 0)
            + relationship_counts.get(claim.claim_id, 0)
            + (1 if claim.evidence_ids else 0)
        )

    def _has_intent_context(
        self,
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> bool:
        return any(
            [
                filters.focus,
                filters.place,
                filters.time_start,
                filters.time_end,
                filters.viewpoint_scope,
                profile
                and (
                    profile.narrative_focus
                    or profile.social_lens
                    or profile.geography
                    or profile.era
                    or profile.time_start
                    or profile.time_end
                    or profile.desired_facets
                ),
            ]
        )

    def _time_alignment_bonus(
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

    def _claim_focus_score(self, claim: ApprovedClaim, seed_query: str) -> int:
        normalized_query = self._normalize_text(seed_query)
        tokens = self._focus_tokens(seed_query)
        bigrams = [
            f"{left} {right}" for left, right in zip(tokens, tokens[1:], strict=False)
        ]
        haystack = " ".join(
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
            ]
        )
        normalized_haystack = self._normalize_text(haystack)
        subject_value = self._normalize_text(
            " ".join(part for part in [claim.subject, claim.value] if part)
        )
        score = 0
        if normalized_query and normalized_query in normalized_haystack:
            score += max(4, len(tokens))
        for bigram in bigrams:
            if bigram in subject_value:
                score += 4
            elif bigram in normalized_haystack:
                score += 2
        for token in tokens:
            if token in self._normalize_text(claim.value):
                score += 2
            if token in self._normalize_text(claim.subject):
                score += 2
            if token in self._normalize_text(claim.notes or ""):
                score += 1
            if token in self._normalize_text(claim.place or ""):
                score += 1
            if token in self._normalize_text(claim.viewpoint_scope or ""):
                score += 1
        return score

    def _focus_tokens(self, text: str) -> list[str]:
        return [token for token in re.findall(r"[a-z0-9]{3,}", text.lower())]

    def _normalize_text(self, text: str) -> str:
        return " ".join(re.findall(r"[a-z0-9]+", text.lower()))

    def _normalize_facet_key(self, facet: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", facet.strip().lower()).strip("_")

    def _canonical_facet(self, facet: str) -> str | None:
        normalized = self._normalize_facet_key(facet)
        if not normalized:
            return None
        return self._FACET_ALIASES.get(
            normalized, normalized if normalized in self._FACET_DISPLAY_LABELS else None
        )

    def _display_facet(self, facet: str) -> str:
        return self._FACET_DISPLAY_LABELS.get(facet, facet.replace("_", " "))

    def _human_join(self, items: list[str]) -> str:
        if not items:
            return ""
        if len(items) == 1:
            return items[0]
        if len(items) == 2:
            return f"{items[0]} and {items[1]}"
        return ", ".join(items[:-1]) + f", and {items[-1]}"

    def _requested_coverage_buckets(
        self,
        section_type: BibleSectionType,
        profile: BibleProjectProfile | None,
    ) -> list[str]:
        if profile is None:
            return []
        allowed = self._SECTION_COVERAGE_BUCKETS.get(section_type, set())
        buckets: list[str] = []
        for facet in profile.desired_facets:
            canonical = self._canonical_facet(facet)
            if canonical is None or canonical not in allowed or canonical in buckets:
                continue
            buckets.append(canonical)
        return buckets

    def _claim_coverage_buckets(
        self,
        claim: ApprovedClaim,
        section_type: BibleSectionType,
    ) -> set[str]:
        buckets: set[str] = set()
        if claim.claim_kind == ClaimKind.PERSON:
            buckets.add("people")
        if claim.claim_kind == ClaimKind.PLACE:
            buckets.update({"places", "regional_context"})
        if claim.claim_kind == ClaimKind.INSTITUTION:
            buckets.update({"institutions", "politics"})
        if claim.claim_kind == ClaimKind.EVENT:
            buckets.add("events")
        if claim.claim_kind == ClaimKind.PRACTICE:
            buckets.add("practices")
            if section_type in {
                BibleSectionType.DAILY_LIFE,
                BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
            }:
                buckets.add("daily_life")
            if section_type == BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE:
                buckets.add("economics")
        if claim.claim_kind == ClaimKind.OBJECT:
            buckets.add("objects")
            if section_type in {
                BibleSectionType.DAILY_LIFE,
                BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
            }:
                buckets.add("material_culture")
        if claim.claim_kind == ClaimKind.BELIEF:
            buckets.add("beliefs")
            if section_type in {BibleSectionType.DAILY_LIFE, BibleSectionType.RUMORS_AND_CONTESTED}:
                buckets.add("ritual")
        if claim.claim_kind == ClaimKind.RELATIONSHIP:
            buckets.update({"relationships", "institutions", "politics"})
        if claim.status == ClaimStatus.RUMOR:
            buckets.add("rumor")
        if claim.status == ClaimStatus.LEGEND:
            buckets.add("legend")
        return buckets.intersection(self._SECTION_COVERAGE_BUCKETS.get(section_type, set()))

    def _section_facet_distribution(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
    ) -> Counter:
        distribution: Counter = Counter()
        for claim in claims:
            for bucket in self._claim_coverage_buckets(claim, section_type):
                distribution[bucket] += 1
        return distribution

    def _coverage_summary_sentence(
        self,
        *,
        covered_facets: list[str],
        missing_facets: list[str],
        missing_named_actors: bool,
        missing_material_detail: bool,
        missing_dated_anchors: bool,
    ) -> str:
        parts: list[str] = []
        if covered_facets:
            parts.append(
                "Coverage currently supports "
                + self._human_join([self._display_facet(item) for item in covered_facets])
            )
        if missing_facets:
            parts.append(
                "Still thin on "
                + self._human_join([self._display_facet(item) for item in missing_facets])
            )
        if missing_named_actors:
            parts.append("named actors remain thin")
        if missing_material_detail:
            parts.append("material detail remains thin")
        if missing_dated_anchors:
            parts.append("dated anchors remain thin")
        if not parts:
            return "Coverage is usable for drafting and stays bounded by approved canon."
        return "; ".join(parts).capitalize() + "."

    def _section_beats(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
        filters: BibleSectionFilters,
        profile: BibleProjectProfile | None,
    ) -> list[str]:
        _ = filters, profile
        if section_type == BibleSectionType.SETTING_OVERVIEW:
            beats = ["scene_anchor", "power_map"]
            if any(
                claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
                for claim in claims
            ):
                beats.append("uncertainty")
            return beats
        if section_type == BibleSectionType.CHRONOLOGY:
            beats = ["chronology_turn"]
            if any(not claim.time_start for claim in claims):
                beats.append("uncertainty")
            return beats
        if section_type == BibleSectionType.PEOPLE_AND_FACTIONS:
            return ["actor_pressure", "uncertainty"]
        if section_type == BibleSectionType.DAILY_LIFE:
            return ["routine_pressure", "material_texture", "uncertainty"]
        if section_type == BibleSectionType.INSTITUTIONS_AND_POLITICS:
            return ["institution_pressure", "uncertainty"]
        if section_type == BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE:
            return ["economy_pressure", "material_cues", "writer_line", "uncertainty"]
        if section_type == BibleSectionType.RUMORS_AND_CONTESTED:
            return ["contested_note", "uncertainty"]
        if section_type == BibleSectionType.AUTHOR_DECISIONS:
            return ["author_guidance"]
        return ["scene_anchor", "uncertainty"] if relationships else ["scene_anchor"]

    def _order_paragraphs_by_beats(
        self,
        paragraphs: list[BibleSectionParagraph],
        beats: list[str],
    ) -> list[BibleSectionParagraph]:
        if not paragraphs or not beats:
            return paragraphs
        kind_to_beat = {
            "setting_cluster": "scene_anchor",
            "chronology_entry": "chronology_turn",
            "chronology_undated": "uncertainty",
            "actor_cluster": "actor_pressure",
            "routine_cluster": "routine_pressure",
            "material_cluster": "material_texture",
            "institution_cluster": "institution_pressure",
            "economy_cluster": "economy_pressure",
            "material_culture_cluster": "material_cues",
            "economy_notebook": "writer_line",
            "contested_topic": "contested_note",
            "author_guidance": "author_guidance",
        }
        beat_rank = {beat: index for index, beat in enumerate(beats)}
        return sorted(
            paragraphs,
            key=lambda paragraph: (
                beat_rank.get(kind_to_beat.get(paragraph.paragraph_kind, ""), len(beats)),
                paragraph.heading or "",
            ),
        )

    def _paragraph_from_claim_group(
        self,
        *,
        heading: str,
        kind: str,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        relationships: list[ClaimRelationship],
        text: str,
    ) -> BibleSectionParagraph:
        claim_ids = [claim.claim_id for claim in claims]
        evidence_ids = sorted(
            {evidence_id for claim in claims for evidence_id in claim.evidence_ids}
        )
        source_ids = sorted(
            {
                evidence_by_id[evidence_id].source_id
                for evidence_id in evidence_ids
                if evidence_id in evidence_by_id
            }
        )
        contradiction_flags, supersession_flags = self._relationship_flags_for_claim_ids(
            claim_ids, relationships
        )
        citations = self._compact_citations(evidence_ids, evidence_by_id, source_by_id)
        suffix = f" Sources: {citations}." if citations else ""
        return BibleSectionParagraph(
            paragraph_id=self._stable_paragraph_id(kind, heading, claim_ids),
            heading=heading,
            text=f"{text}{suffix}",
            paragraph_kind=kind,
            claim_ids=claim_ids,
            evidence_ids=evidence_ids,
            source_ids=source_ids,
            contradiction_flags=contradiction_flags,
            supersession_flags=supersession_flags,
        )

    def _stable_paragraph_id(self, kind: str, heading: str, claim_ids: list[str]) -> str:
        material = f"{kind}::{heading.lower()}::{'|'.join(sorted(claim_ids))}"
        return f"para-{sha1(material.encode('utf-8')).hexdigest()[:10]}"

    def _render_markdown(
        self,
        title: str,
        paragraphs: list[BibleSectionParagraph],
        coverage_analysis: BibleCoverageAnalysis,
        coverage_gaps: list[str],
        recommended_next_research: list[str],
        retrieval_metadata: dict[str, object],
    ) -> str:
        lines = [f"# {title}", ""]
        for paragraph in paragraphs:
            if paragraph.heading:
                lines.append(f"## {paragraph.heading}")
            lines.append(paragraph.text)
            lines.append("")
        lines.append("## Coverage Notes")
        lines.append(coverage_analysis.diagnostic_summary or "Coverage analysis is available.")
        if retrieval_metadata.get("fallback_used"):
            fallback_reason = retrieval_metadata.get("fallback_reason") or "projection unavailable"
            lines.append(f"- Retrieval fallback: {fallback_reason}.")
        if coverage_gaps:
            lines.extend([f"- {gap}" for gap in coverage_gaps])
        if recommended_next_research:
            lines.append("")
            lines.append("## Recommended Next Research")
            lines.extend([f"- {item}" for item in recommended_next_research])
        return "\n".join(lines).strip()

    def _coverage_analysis(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
        profile: BibleProjectProfile | None,
    ) -> BibleCoverageAnalysis:
        facet_distribution = self._section_facet_distribution(section_type, claims)
        desired_facets = list(profile.desired_facets if profile else [])
        requested_buckets = self._requested_coverage_buckets(section_type, profile)
        missing_facets = [
            bucket for bucket in requested_buckets if facet_distribution.get(bucket, 0) <= 0
        ]
        covered_facets = [
            bucket for bucket in requested_buckets if facet_distribution.get(bucket, 0) > 0
        ]
        time_coverage = [
            BibleCoverageBucket(label=label, count=count)
            for label, count in sorted(
                Counter(claim.time_start or "undated" for claim in claims).items()
            )
        ]
        place_coverage = [
            BibleCoverageBucket(label=label, count=count)
            for label, count in sorted(
                Counter(claim.place or "unplaced" for claim in claims).items()
            )
        ]
        missing_named_actors = section_type == BibleSectionType.PEOPLE_AND_FACTIONS and not any(
            claim.claim_kind == ClaimKind.PERSON for claim in claims
        )
        missing_material_detail = section_type in {
            BibleSectionType.DAILY_LIFE,
            BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
        } and not any(
            claim.claim_kind in {ClaimKind.PRACTICE, ClaimKind.OBJECT} for claim in claims
        )
        missing_dated_anchors = section_type == BibleSectionType.CHRONOLOGY and not any(
            claim.time_start for claim in claims
        )
        certainty_mix = dict(Counter(claim.status.value for claim in claims))
        return BibleCoverageAnalysis(
            desired_facets=desired_facets,
            facet_distribution={
                self._display_facet(bucket): count
                for bucket, count in sorted(facet_distribution.items())
            },
            missing_facets=[self._display_facet(bucket) for bucket in missing_facets],
            certainty_mix=certainty_mix,
            time_coverage=time_coverage,
            place_coverage=place_coverage,
            missing_named_actors=missing_named_actors,
            missing_material_detail=missing_material_detail,
            missing_dated_anchors=missing_dated_anchors,
            diagnostic_summary=self._coverage_summary_sentence(
                covered_facets=covered_facets,
                missing_facets=missing_facets,
                missing_named_actors=missing_named_actors,
                missing_material_detail=missing_material_detail,
                missing_dated_anchors=missing_dated_anchors,
            ),
        )

    def _coverage_gaps(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
        coverage_analysis: BibleCoverageAnalysis,
    ) -> list[str]:
        if not claims:
            return [f"No approved canon supports {self._SECTION_TITLES[section_type].lower()} yet."]
        gaps: list[str] = []
        if not any(
            claim.status in {ClaimStatus.VERIFIED, ClaimStatus.PROBABLE} for claim in claims
        ):
            gaps.append("Section lacks high-certainty claims.")
        if coverage_analysis.missing_facets:
            gaps.append(
                "Desired section facets are still thin: "
                + ", ".join(coverage_analysis.missing_facets)
                + "."
            )
        if coverage_analysis.missing_named_actors:
            gaps.append("People section lacks named individuals.")
        if coverage_analysis.missing_material_detail:
            gaps.append("Section lacks material-culture detail.")
        if coverage_analysis.missing_dated_anchors:
            gaps.append("Chronology lacks dated claims.")
        if (
            len(coverage_analysis.place_coverage) <= 1
            and section_type == BibleSectionType.SETTING_OVERVIEW
            and not any(claim.place for claim in claims)
        ):
            gaps.append("Place coverage is narrow.")
        return gaps

    def _recommended_research(
        self,
        coverage_analysis: BibleCoverageAnalysis,
        gaps: list[str],
        profile: BibleProjectProfile | None,
        section_type: BibleSectionType,
    ) -> list[str]:
        locale = profile.geography if profile and profile.geography else "the target locale"
        period = (
            profile.era or profile.time_start or "the target period"
            if profile
            else "the target period"
        )
        prompts: list[str] = []
        for gap in gaps:
            lower = gap.lower()
            if "dated" in lower:
                prompts.append(
                    f"Find date-anchored sources for "
                    f"{self._SECTION_TITLES[section_type].lower()} in {locale} "
                    f"during {period}."
                )
            elif "material" in lower:
                prompts.append(
                    f"Find routine, domestic, and object detail for {locale} during {period}."
                )
            elif "named individuals" in lower:
                prompts.append(
                    f"Find named people, offices, and faction links active in "
                    f"{locale} during {period}."
                )
            elif "section facets" in lower or "desired" in lower:
                prompts.append(
                    f"Research missing section facets for "
                    f"{self._SECTION_TITLES[section_type].lower()}: "
                    + ", ".join(coverage_analysis.missing_facets or ["target facets"])
                    + "."
                )
            elif "high-certainty" in lower:
                prompts.append(
                    f"Find archival or record-like sources to verify "
                    f"{self._SECTION_TITLES[section_type].lower()} in {locale}."
                )
            elif "place coverage" in lower:
                prompts.append(
                    f"Find sources that widen place coverage beyond {locale} while "
                    f"staying within {period}."
                )
        return prompts

    def _paragraph_scope(
        self,
        section_type: BibleSectionType,
        claims: list[ApprovedClaim],
    ) -> str:
        if section_type == BibleSectionType.AUTHOR_DECISIONS or any(
            claim.status == ClaimStatus.AUTHOR_CHOICE or claim.author_choice for claim in claims
        ):
            return "author_guidance"
        if any(
            claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
            for claim in claims
        ):
            return "contested_context"
        return "canon_support"

    def _why_this_paragraph_exists(
        self,
        paragraph: BibleSectionParagraph,
        claims: list[ApprovedClaim],
        sources: list[SourceRecord],
    ) -> str:
        scope = self._paragraph_scope(
            BibleSectionType.AUTHOR_DECISIONS
            if paragraph.paragraph_kind == "author_guidance"
            else BibleSectionType.SETTING_OVERVIEW,
            claims,
        )
        if scope == "author_guidance":
            return (
                f"This paragraph preserves a deliberate drafting choice tied to "
                f"{len(claims)} approved author-decision claim"
                + ("s." if len(claims) != 1 else ".")
            )
        if scope == "contested_context":
            return (
                "This paragraph exists to keep disputed or low-certainty material "
                "visible without promoting it to settled fact. "
                f"It currently draws on {len(claims)} claim(s) across {len(sources)} source(s)."
            )
        return (
            "This paragraph exists because approved canon groups into one usable "
            "drafting note here. "
            f"It currently draws on {len(claims)} claim(s) across {len(sources)} source(s)."
        )

    def _claim_detail(self, claim: ApprovedClaim) -> dict[str, object]:
        return {
            "claim_id": claim.claim_id,
            "status": claim.status.value,
            "claim_kind": claim.claim_kind.value,
            "subject": claim.subject,
            "predicate": claim.predicate,
            "value": claim.value,
            "place": claim.place,
            "time_start": claim.time_start,
            "time_end": claim.time_end,
            "viewpoint_scope": claim.viewpoint_scope,
            "notes": claim.notes,
            "summary": self._claim_scene_note(claim),
        }

    def _evidence_detail(
        self,
        evidence: EvidenceSnippet,
        source: SourceRecord | None,
    ) -> dict[str, object]:
        return {
            "evidence_id": evidence.evidence_id,
            "locator": evidence.locator,
            "snippet": evidence.text,
            "notes": evidence.notes,
            "source_id": evidence.source_id,
            "source_title": source.title if source else evidence.source_id,
            "source_type": source.source_type if source else None,
            "source_url": source.url if source else None,
        }

    def _relationship_details_for_claim_ids(
        self,
        claim_ids: list[str],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        claim_id_set = set(claim_ids)
        contradiction_details: list[dict[str, object]] = []
        supersession_details: list[dict[str, object]] = []
        for relationship in self.truth_store.list_relationships():
            if relationship.claim_id not in claim_id_set:
                continue
            related = self.truth_store.get_claim(relationship.related_claim_id)
            detail = {
                "relationship_id": relationship.relationship_id,
                "relationship_type": relationship.relationship_type,
                "related_claim_id": relationship.related_claim_id,
                "related_claim_summary": self._claim_scene_note(related)
                if related
                else relationship.related_claim_id,
                "notes": relationship.notes,
                "source_kind": relationship.source_kind,
            }
            if relationship.relationship_type == "contradicts":
                contradiction_details.append(detail)
            elif relationship.relationship_type in {"supersedes", "superseded_by"}:
                supersession_details.append(detail)
        return contradiction_details, supersession_details

    def _relationship_context(self, claims: list[ApprovedClaim]) -> list[str]:
        claim_ids = {claim.claim_id for claim in claims}
        context: list[str] = []
        for relationship in self.truth_store.list_relationships():
            if relationship.claim_id not in claim_ids:
                continue
            related = self.truth_store.get_claim(relationship.related_claim_id)
            if related is None:
                continue
            context.append(
                f"{relationship.relationship_type}: {related.subject} "
                f"({relationship.notes or 'canon relationship'})"
            )
        return context

    def _relationship_flags_for_claim_ids(
        self,
        claim_ids: list[str],
        relationships: list[ClaimRelationship],
    ) -> tuple[list[str], list[str]]:
        claim_id_set = set(claim_ids)
        contradiction_flags: list[str] = []
        supersession_flags: list[str] = []
        for relationship in relationships:
            if relationship.claim_id not in claim_id_set:
                continue
            related = self.truth_store.get_claim(relationship.related_claim_id)
            if related is None:
                continue
            message = f"{related.subject} ({relationship.notes or 'canon relationship'})"
            if relationship.relationship_type == "contradicts":
                contradiction_flags.append(message)
            if relationship.relationship_type in {"supersedes", "superseded_by"}:
                supersession_flags.append(f"{relationship.relationship_type}: {message}")
        return contradiction_flags, supersession_flags

    def _contradiction_flags(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship],
    ) -> list[str]:
        claim_ids = [claim.claim_id for claim in claims]
        contradiction_flags, _ = self._relationship_flags_for_claim_ids(claim_ids, relationships)
        return contradiction_flags

    def _compact_citations(
        self,
        evidence_ids: list[str],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
    ) -> str:
        citations: list[str] = []
        for evidence_id in evidence_ids[:3]:
            evidence = evidence_by_id.get(evidence_id)
            if evidence is None:
                continue
            source = source_by_id.get(evidence.source_id)
            source_label = source.title if source else evidence.source_id
            citations.append(f"{source_label} ({evidence.locator})")
        return "; ".join(citations)

    def _place_clause(self, claim: ApprovedClaim) -> str:
        return f" in {claim.place}" if claim.place else ""

    def _evidence_index(self, claims: list[ApprovedClaim]) -> dict[str, EvidenceSnippet]:
        index: dict[str, EvidenceSnippet] = {}
        for claim in claims:
            for evidence_id in claim.evidence_ids:
                snippet = self.evidence_store.get_evidence(evidence_id)
                if snippet is not None:
                    index[evidence_id] = snippet
        return index

    def _source_index(self, evidence: list[EvidenceSnippet]) -> dict[str, SourceRecord]:
        index: dict[str, SourceRecord] = {}
        for item in evidence:
            source = self.source_store.get_source(item.source_id)
            if source is not None:
                index[item.source_id] = source
        return index

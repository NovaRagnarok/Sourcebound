from __future__ import annotations

import re
from collections import Counter, defaultdict
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
        claim_ids = {claim_id for paragraph in section.paragraphs for claim_id in paragraph.claim_ids}
        evidence_ids = {evidence_id for paragraph in section.paragraphs for evidence_id in paragraph.evidence_ids}
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
            paragraph_claims = [claims[claim_id] for claim_id in paragraph.claim_ids if claim_id in claims]
            paragraph_evidence = [
                evidence[evidence_id]
                for evidence_id in paragraph.evidence_ids
                if evidence_id in evidence
            ]
            paragraph_sources = [
                sources[source_id]
                for source_id in paragraph.source_ids
                if source_id in sources
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
        draft = self._compose_section(section.project_id, section.section_type, section.generation_filters)
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
        if retrieval_metadata.get("fallback_used"):
            fallback_reason = retrieval_metadata.get("fallback_reason") or "projection unavailable"
            existing = coverage_analysis.diagnostic_summary or "Coverage analysis is available."
            coverage_analysis.diagnostic_summary = f"{existing.rstrip('.')} Retrieval fallback: {fallback_reason}."
        coverage_gaps = self._coverage_gaps(section_type, claims, coverage_analysis)
        recommended_next_research = self._recommended_research(
            coverage_analysis,
            coverage_gaps,
            profile,
            section_type,
        )
        references = BibleSectionReference(
            claim_ids=[claim.claim_id for claim in claims],
            evidence_ids=sorted({item for claim in claims for item in claim.evidence_ids}),
            source_ids=sorted({snippet.source_id for snippet in evidence_by_id.values()}),
            certainty_buckets=sorted(
                {claim.status for claim in claims},
                key=lambda status: status.value,
            ),
        )
        paragraphs = self._build_paragraphs(
            section_type,
            claims,
            evidence_by_id,
            source_by_id,
            relationships,
        )
        title = self._SECTION_TITLES[section_type]
        markdown = self._render_markdown(title, paragraphs, coverage_analysis, coverage_gaps, recommended_next_research)
        return BibleSectionDraft(
            section_type=section_type,
            title=title,
            generated_markdown=markdown,
            paragraphs=paragraphs,
            references=references,
            certainty_summary=dict(Counter(claim.status.value for claim in claims)),
            coverage_gaps=coverage_gaps,
            contradiction_flags=contradiction_flags,
            recommended_next_research=recommended_next_research,
            coverage_analysis=coverage_analysis,
            retrieval_metadata=retrieval_metadata,
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
            statuses = set(filters.statuses or [ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND])
            claims = [claim for claim in claims if claim.status in statuses]
        elif section_type == BibleSectionType.AUTHOR_DECISIONS:
            claims = [claim for claim in claims if claim.status == ClaimStatus.AUTHOR_CHOICE or claim.author_choice]
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
            claims = [
                claim
                for claim in claims
                if claim.place in {None, "", profile.geography}
            ]
        if filters.viewpoint_scope:
            claims = [claim for claim in claims if claim.viewpoint_scope == filters.viewpoint_scope]
        if filters.time_start:
            claims = [claim for claim in claims if not claim.time_end or claim.time_end >= filters.time_start]
        elif profile and profile.time_start:
            claims = [claim for claim in claims if not claim.time_end or claim.time_end >= profile.time_start]
        if filters.time_end:
            claims = [claim for claim in claims if not claim.time_start or claim.time_start <= filters.time_end]
        elif profile and profile.time_end:
            claims = [claim for claim in claims if not claim.time_start or claim.time_start <= profile.time_end]
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
                    (source_by_id.get(evidence_by_id[evidence_id].source_id) or SourceRecord(source_id="", title="")).source_type
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
            "ranking_strategy": "lexical",
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
                retrieval_metadata["ranking_strategy"] = "blended"
                projection_order = {
                    claim_id: index for index, claim_id in enumerate(projection.claim_ids)
                }
            else:
                retrieval_metadata["fallback_used"] = True
                retrieval_metadata["fallback_reason"] = projection.fallback_reason
        relationships = self.truth_store.list_relationships()
        relationship_counts = Counter(item.claim_id for item in relationships if item.relationship_type == "supports")
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
            claim.claim_id: self._claim_focus_score(claim, focus)
            for claim in ranked_claims
        }
        strongest_focus = max(focus_scores.values(), default=0)
        if strongest_focus <= 0:
            return ranked_claims[:8]

        seed_claim_ids = {
            claim.claim_id
            for claim in ranked_claims
            if focus_scores[claim.claim_id] >= max(2, strongest_focus - 1)
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
            if claim.place and any(seed.place == claim.place for seed in ranked_claims if seed.claim_id in seed_claim_ids):
                focused_window.append(claim)
                continue
            if claim.subject and any(seed.subject == claim.subject for seed in ranked_claims if seed.claim_id in seed_claim_ids):
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
    ) -> list[BibleSectionParagraph]:
        if not claims:
            return [
                BibleSectionParagraph(
                    paragraph_id=f"para-{uuid4().hex[:10]}",
                    paragraph_kind="empty_state",
                    text="No approved canon matches this section yet.",
                )
            ]
        if section_type == BibleSectionType.CHRONOLOGY:
            return self._chronology_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.SETTING_OVERVIEW:
            return self._setting_overview_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.PEOPLE_AND_FACTIONS:
            return self._people_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.DAILY_LIFE:
            return self._daily_life_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.INSTITUTIONS_AND_POLITICS:
            return self._institutions_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE:
            return self._economics_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.RUMORS_AND_CONTESTED:
            return self._rumor_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        if section_type == BibleSectionType.AUTHOR_DECISIONS:
            return self._author_decision_paragraphs(claims, evidence_by_id, source_by_id, relationships)
        return self._grouped_summary_paragraphs(claims, evidence_by_id, source_by_id, relationships, section_type)

    def _setting_overview_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            key = claim.place or (claim.subject if claim.claim_kind == ClaimKind.PLACE else "Wider setting")
            grouped[key].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for place, group in sorted(grouped.items()):
            place_claims = [claim for claim in group if claim.claim_kind == ClaimKind.PLACE]
            institutions = [
                claim for claim in group if claim.claim_kind in {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP}
            ]
            events = [claim for claim in group if claim.claim_kind == ClaimKind.EVENT]
            people = [claim for claim in group if claim.claim_kind == ClaimKind.PERSON]
            sentences = self._certainty_sentences(
                place_claims or group,
                settled_intro=f"Scenes in {place} can safely lean on",
                probable_intro=f"Where the record is thinner, canon still points toward",
            )
            if events:
                sentences.append(
                    f"Let public pressure come from {self._join_claim_notes(events, limit=3)}."
                )
            if institutions or people:
                sentences.append(
                    "Keep nearby actors and structures visible through "
                    + self._join_claim_notes(institutions + people, limit=4)
                    + "."
                )
            anchor_sentence = self._scene_anchor_sentence(group)
            if anchor_sentence:
                sentences.append(anchor_sentence)
            uncertainty_sentence = self._bounded_uncertainty_sentence(group, relationships)
            if uncertainty_sentence:
                sentences.append(uncertainty_sentence)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=place,
                    kind="setting_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=" ".join(sentences),
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
                text = (
                    "Keep these pressures in the background even when the exact date is unsettled: "
                    + self._join_claim_notes(group, limit=5)
                    + "."
                )
                kind = "chronology_undated"
            else:
                sentences = self._certainty_sentences(
                    group,
                    settled_intro=f"By {label}, scenes can safely show",
                    probable_intro=f"By {label}, canon also leans toward",
                )
                anchor_sentence = self._scene_anchor_sentence(group)
                if anchor_sentence:
                    sentences.append(anchor_sentence)
                uncertainty_sentence = self._bounded_uncertainty_sentence(group, relationships)
                if uncertainty_sentence:
                    sentences.append(uncertainty_sentence)
                text = " ".join(sentences)
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
                claim for claim in group if claim.claim_kind in {ClaimKind.INSTITUTION, ClaimKind.RELATIONSHIP}
            ]
            text = " ".join(
                self._certainty_sentences(
                    group,
                    settled_intro=f"For {actor}, scenes can safely play",
                    probable_intro=f"For {actor}, canon also leans toward",
                )
            )
            if affiliations:
                text += f" Ties, leverage, and faction pull run through {self._join_claim_notes(affiliations, limit=3)}."
            social_pressure = self._social_pressure_sentence(group)
            if social_pressure:
                text += f" {social_pressure}"
            contradiction_notes, _ = self._relationship_flags_for_claim_ids(
                [claim.claim_id for claim in group],
                relationships,
            )
            if contradiction_notes:
                text += f" Accounts diverge around {', '.join(contradiction_notes[:2])}."
            uncertainty_sentence = self._bounded_uncertainty_sentence(group, relationships)
            if uncertainty_sentence:
                text += f" {uncertainty_sentence}"
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=actor,
                    kind="actor_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=text,
                )
            )
        return paragraphs

    def _daily_life_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        routines = [claim for claim in claims if claim.claim_kind == ClaimKind.PRACTICE]
        materials = [claim for claim in claims if claim.claim_kind in {ClaimKind.OBJECT, ClaimKind.BELIEF}]
        paragraphs: list[BibleSectionParagraph] = []
        if routines:
            routine_sentences = self._certainty_sentences(
                routines,
                settled_intro="Everyday scenes can safely rest on",
                probable_intro="Where daily detail stays thin, canon still points toward",
            )
            anchor_sentence = self._scene_anchor_sentence(routines)
            if anchor_sentence:
                routine_sentences.append(anchor_sentence)
            social_pressure = self._social_pressure_sentence(routines)
            if social_pressure:
                routine_sentences.append(social_pressure)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Routine and practice",
                    kind="routine_cluster",
                    claims=routines,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=" ".join(routine_sentences),
                )
            )
        if materials:
            material_sentences = self._certainty_sentences(
                materials,
                settled_intro="To make the world feel handled and inhabited, use",
                probable_intro="Where texture stays softer, canon still points toward",
                contested_intro="Accounts differ on whether people handled",
            )
            uncertainty_sentence = self._bounded_uncertainty_sentence(materials, relationships)
            if uncertainty_sentence:
                material_sentences.append(uncertainty_sentence)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Material and sensory detail",
                    kind="material_cluster",
                    claims=materials,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=" ".join(material_sentences),
                )
            )
        return paragraphs

    def _institutions_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.subject].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for institution, group in sorted(grouped.items()):
            text = " ".join(
                self._certainty_sentences(
                    group,
                    settled_intro=f"When {institution} enters a scene, show",
                    probable_intro=f"For {institution}, canon also leans toward",
                )
            )
            text += " Use the institution as pressure, process, and gatekeeping rather than as background ornament."
            social_pressure = self._social_pressure_sentence(group)
            if social_pressure:
                text += f" {social_pressure}"
            uncertainty_sentence = self._bounded_uncertainty_sentence(group, relationships)
            if uncertainty_sentence:
                text += f" {uncertainty_sentence}"
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=institution,
                    kind="institution_cluster",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=text,
                )
            )
        return paragraphs

    def _economics_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        trade_claims = [claim for claim in claims if claim.claim_kind == ClaimKind.PRACTICE]
        material_claims = [claim for claim in claims if claim.claim_kind == ClaimKind.OBJECT]
        paragraphs: list[BibleSectionParagraph] = []
        if trade_claims:
            trade_sentences = self._certainty_sentences(
                trade_claims,
                settled_intro="For market, workshop, or household scenes, show",
                probable_intro="Where prices or routines stay thin, canon still points toward",
            )
            anchor_sentence = self._scene_anchor_sentence(trade_claims)
            if anchor_sentence:
                trade_sentences.append(anchor_sentence)
            social_pressure = self._social_pressure_sentence(trade_claims)
            if social_pressure:
                trade_sentences.append(social_pressure)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Trade, exchange, and pressure",
                    kind="economy_cluster",
                    claims=trade_claims,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=" ".join(trade_sentences),
                )
            )
        if material_claims:
            material_sentences = self._certainty_sentences(
                material_claims,
                settled_intro="Name concrete materials such as",
                probable_intro="Where specifics stay lighter, canon still points toward",
            )
            uncertainty_sentence = self._bounded_uncertainty_sentence(material_claims, relationships)
            if uncertainty_sentence:
                material_sentences.append(uncertainty_sentence)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading="Sceneable material cues",
                    kind="material_culture_cluster",
                    claims=material_claims,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=" ".join(material_sentences),
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
                    text=" ".join(
                        sentence
                        for sentence in [
                            "Use the economy as daily pressure rather than abstract background.",
                            self._scene_anchor_sentence(combined),
                            self._bounded_uncertainty_sentence(combined, relationships),
                        ]
                        if sentence
                    ),
                )
            )
        return paragraphs

    def _rumor_paragraphs(self, claims, evidence_by_id, source_by_id, relationships):
        grouped: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.subject].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for topic, group in sorted(grouped.items()):
            believers = ", ".join(sorted({claim.viewpoint_scope for claim in group if claim.viewpoint_scope})) or "different witnesses"
            sentences = self._certainty_sentences(
                group,
                settled_intro=f"On {topic}, canon records",
                probable_intro=f"On {topic}, canon leans toward",
                contested_intro=f"On {topic}, accounts differ over",
                rumor_intro=f"On {topic}, {believers} repeat that",
                legend_intro=f"On {topic}, local legend keeps alive the claim that",
            )
            sentences.append("Use this as suspicion, gossip, or motive, not as settled exposition.")
            uncertainty_sentence = self._bounded_uncertainty_sentence(group, relationships)
            if uncertainty_sentence:
                sentences.append(uncertainty_sentence)
            paragraphs.append(
                self._paragraph_from_claim_group(
                    heading=topic,
                    kind="contested_topic",
                    claims=group,
                    evidence_by_id=evidence_by_id,
                    source_by_id=source_by_id,
                    relationships=relationships,
                    text=" ".join(sentences),
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
                    "Keep this available for drafting decisions, but do not present it as settled canon in strict-fact outputs."
                ),
            )
            for claim in claims
        ]

    def _grouped_summary_paragraphs(self, claims, evidence_by_id, source_by_id, relationships, section_type):
        grouped: dict[ClaimKind, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            grouped[claim.claim_kind].append(claim)
        paragraphs: list[BibleSectionParagraph] = []
        for claim_kind, group in sorted(grouped.items(), key=lambda item: item[0].value):
            label = self._FACET_LABELS.get(claim_kind, claim_kind.value.replace("_", " "))
            text = f"{self._SECTION_TITLES[section_type]} is anchored by " + "; ".join(
                f"{claim.subject} {claim.value}" for claim in group[:4]
            ) + "."
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
            ClaimStatus.VERIFIED: [claim for claim in claims if claim.status == ClaimStatus.VERIFIED],
            ClaimStatus.PROBABLE: [claim for claim in claims if claim.status == ClaimStatus.PROBABLE],
            ClaimStatus.CONTESTED: [claim for claim in claims if claim.status == ClaimStatus.CONTESTED],
            ClaimStatus.RUMOR: [claim for claim in claims if claim.status == ClaimStatus.RUMOR],
            ClaimStatus.LEGEND: [claim for claim in claims if claim.status == ClaimStatus.LEGEND],
            ClaimStatus.AUTHOR_CHOICE: [
                claim for claim in claims if claim.status == ClaimStatus.AUTHOR_CHOICE or claim.author_choice
            ],
        }
        if grouped[ClaimStatus.VERIFIED]:
            sentences.append(f"{settled_intro} {self._join_claim_notes(grouped[ClaimStatus.VERIFIED])}.")
        if grouped[ClaimStatus.PROBABLE]:
            sentences.append(f"{probable_intro} {self._join_claim_notes(grouped[ClaimStatus.PROBABLE])}.")
        if grouped[ClaimStatus.CONTESTED]:
            sentences.append(f"{contested_intro} {self._join_claim_notes(grouped[ClaimStatus.CONTESTED])}.")
        if grouped[ClaimStatus.RUMOR]:
            sentences.append(f"{rumor_intro} {self._join_claim_notes(grouped[ClaimStatus.RUMOR])}.")
        if grouped[ClaimStatus.LEGEND]:
            sentences.append(f"{legend_intro} {self._join_claim_notes(grouped[ClaimStatus.LEGEND])}.")
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
        viewpoints = list(dict.fromkeys(claim.viewpoint_scope for claim in claims if claim.viewpoint_scope))
        fragments: list[str] = []
        if places:
            fragments.append(f"place the action around {', '.join(places[:2])}")
        if times:
            fragments.append(f"anchor it to {', '.join(times[:2])}")
        if viewpoints:
            fragments.append(f"filter it through {', '.join(viewpoints[:2])}")
        if not fragments:
            return ""
        return "For scene construction, " + ", ".join(fragments) + "."

    def _social_pressure_sentence(self, claims: list[ApprovedClaim]) -> str:
        people = [claim for claim in claims if claim.claim_kind == ClaimKind.PERSON]
        institutions = [claim for claim in claims if claim.claim_kind == ClaimKind.INSTITUTION]
        relationships = [claim for claim in claims if claim.claim_kind == ClaimKind.RELATIONSHIP]
        pressure_claims = (people + institutions + relationships)[:4]
        if not pressure_claims:
            return ""
        return (
            "Let social pressure show through "
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
                "Uncertainty stays bounded: keep "
                + self._join_claim_notes(contested_claims, limit=3)
                + " labeled as disputed rather than settled."
            )
        contradiction_notes = self._contradiction_flags(claims, relationships)
        if contradiction_notes:
            return (
                "Uncertainty stays bounded: note where canon conflicts around "
                + ", ".join(contradiction_notes[:2])
                + "."
            )
        probable_claims = [claim for claim in claims if claim.status == ClaimStatus.PROBABLE]
        if probable_claims:
            return (
                "Where certainty softens, treat "
                + self._join_claim_notes(probable_claims, limit=3)
                + " as the best-supported working read rather than hard fact."
            )
        return ""

    def _join_claim_notes(self, claims: list[ApprovedClaim], *, limit: int = 4) -> str:
        notes: list[str] = []
        for claim in claims[:limit]:
            note = self._claim_scene_note(claim)
            if note not in notes:
                notes.append(note)
        return "; ".join(notes)

    def _claim_scene_note(self, claim: ApprovedClaim) -> str:
        fragments = [claim.subject.strip(), claim.value.strip()]
        note = " ".join(fragment for fragment in fragments if fragment).strip()
        if claim.place and claim.claim_kind != ClaimKind.PLACE and claim.place.lower() not in note.lower():
            note += f" in {claim.place}"
        if claim.time_start and claim.claim_kind == ClaimKind.EVENT and claim.time_start not in note:
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
            parts.append(profile.era or " ".join(item for item in [profile.time_start, profile.time_end] if item))
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
    ) -> int:
        projection_bonus = max(0, len(projection_order) - projection_order.get(claim.claim_id, len(projection_order)))
        lexical_bonus = self._claim_focus_score(claim, seed_query)
        return (
            projection_bonus * 5
            + lexical_bonus * 3
            + (3 if filters.place and claim.place == filters.place else 0)
            + (2 if profile and profile.geography and claim.place == profile.geography else 0)
            + (2 if filters.time_start and claim.time_start else 0)
            + self._CERTAINTY_WEIGHT.get(claim.status, 0)
            + relationship_counts.get(claim.claim_id, 0)
            + (1 if claim.evidence_ids else 0)
        )

    def _claim_focus_score(self, claim: ApprovedClaim, seed_query: str) -> int:
        normalized_query = self._normalize_text(seed_query)
        tokens = self._focus_tokens(seed_query)
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
        score = 0
        if normalized_query and normalized_query in normalized_haystack:
            score += max(4, len(tokens))
        for token in tokens:
            if token in normalized_haystack:
                score += 1
                if token in self._normalize_text(claim.value):
                    score += 1
                if token in self._normalize_text(claim.notes or ""):
                    score += 1
        return score

    def _focus_tokens(self, text: str) -> list[str]:
        return [token for token in re.findall(r"[a-z0-9]{3,}", text.lower())]

    def _normalize_text(self, text: str) -> str:
        return " ".join(re.findall(r"[a-z0-9]+", text.lower()))

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
        evidence_ids = sorted({evidence_id for claim in claims for evidence_id in claim.evidence_ids})
        source_ids = sorted(
            {
                evidence_by_id[evidence_id].source_id
                for evidence_id in evidence_ids
                if evidence_id in evidence_by_id
            }
        )
        contradiction_flags, supersession_flags = self._relationship_flags_for_claim_ids(claim_ids, relationships)
        citations = self._compact_citations(evidence_ids, evidence_by_id, source_by_id)
        suffix = f" Sources: {citations}." if citations else ""
        return BibleSectionParagraph(
            paragraph_id=f"para-{uuid4().hex[:10]}",
            heading=heading,
            text=f"{text}{suffix}",
            paragraph_kind=kind,
            claim_ids=claim_ids,
            evidence_ids=evidence_ids,
            source_ids=source_ids,
            contradiction_flags=contradiction_flags,
            supersession_flags=supersession_flags,
        )

    def _render_markdown(
        self,
        title: str,
        paragraphs: list[BibleSectionParagraph],
        coverage_analysis: BibleCoverageAnalysis,
        coverage_gaps: list[str],
        recommended_next_research: list[str],
    ) -> str:
        lines = [f"# {title}", ""]
        for paragraph in paragraphs:
            if paragraph.heading:
                lines.append(f"## {paragraph.heading}")
            lines.append(paragraph.text)
            lines.append("")
        lines.append("## Coverage Notes")
        lines.append(coverage_analysis.diagnostic_summary or "Coverage analysis is available.")
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
        facet_distribution = Counter(self._FACET_LABELS.get(claim.claim_kind, claim.claim_kind.value) for claim in claims)
        desired_facets = list(profile.desired_facets if profile else [])
        missing_facets = [facet for facet in desired_facets if facet not in facet_distribution]
        time_coverage = [
            BibleCoverageBucket(label=label, count=count)
            for label, count in sorted(Counter(claim.time_start or "undated" for claim in claims).items())
        ]
        place_coverage = [
            BibleCoverageBucket(label=label, count=count)
            for label, count in sorted(Counter(claim.place or "unplaced" for claim in claims).items())
        ]
        missing_named_actors = (
            section_type == BibleSectionType.PEOPLE_AND_FACTIONS
            and not any(claim.claim_kind == ClaimKind.PERSON for claim in claims)
        )
        missing_material_detail = (
            section_type in {BibleSectionType.DAILY_LIFE, BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE}
            and not any(claim.claim_kind in {ClaimKind.PRACTICE, ClaimKind.OBJECT} for claim in claims)
        )
        missing_dated_anchors = (
            section_type == BibleSectionType.CHRONOLOGY
            and not any(claim.time_start for claim in claims)
        )
        certainty_mix = dict(Counter(claim.status.value for claim in claims))
        summary_bits = []
        if missing_facets:
            summary_bits.append(f"missing facets: {', '.join(missing_facets)}")
        if missing_named_actors:
            summary_bits.append("missing named actors")
        if missing_material_detail:
            summary_bits.append("missing material detail")
        if missing_dated_anchors:
            summary_bits.append("missing dated anchors")
        if not summary_bits:
            summary_bits.append("coverage is usable but still bounded by approved canon")
        return BibleCoverageAnalysis(
            desired_facets=desired_facets,
            facet_distribution=dict(facet_distribution),
            missing_facets=missing_facets,
            certainty_mix=certainty_mix,
            time_coverage=time_coverage,
            place_coverage=place_coverage,
            missing_named_actors=missing_named_actors,
            missing_material_detail=missing_material_detail,
            missing_dated_anchors=missing_dated_anchors,
            diagnostic_summary="; ".join(summary_bits).capitalize() + ".",
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
        if not any(claim.status in {ClaimStatus.VERIFIED, ClaimStatus.PROBABLE} for claim in claims):
            gaps.append("Section lacks high-certainty claims.")
        if coverage_analysis.missing_facets:
            gaps.append(
                "Desired profile facets are thin: " + ", ".join(coverage_analysis.missing_facets) + "."
            )
        if coverage_analysis.missing_named_actors:
            gaps.append("People section lacks named individuals.")
        if coverage_analysis.missing_material_detail:
            gaps.append("Section lacks material-culture detail.")
        if coverage_analysis.missing_dated_anchors:
            gaps.append("Chronology lacks dated claims.")
        if len(coverage_analysis.place_coverage) <= 1:
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
        period = profile.era or profile.time_start or "the target period" if profile else "the target period"
        prompts: list[str] = []
        for gap in gaps:
            lower = gap.lower()
            if "dated" in lower:
                prompts.append(f"Find date-anchored sources for {self._SECTION_TITLES[section_type].lower()} in {locale} during {period}.")
            elif "material" in lower:
                prompts.append(f"Find routine, domestic, and object detail for {locale} during {period}.")
            elif "named individuals" in lower:
                prompts.append(f"Find named people, offices, and faction links active in {locale} during {period}.")
            elif "profile facets" in lower or "desired" in lower:
                prompts.append(
                    f"Research missing profile facets for {self._SECTION_TITLES[section_type].lower()}: "
                    + ", ".join(coverage_analysis.missing_facets or ["target facets"])
                    + "."
                )
            elif "high-certainty" in lower:
                prompts.append(f"Find archival or record-like sources to verify {self._SECTION_TITLES[section_type].lower()} in {locale}.")
            elif "place coverage" in lower:
                prompts.append(f"Find sources that widen place coverage beyond {locale} while staying within {period}.")
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
        if any(claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND} for claim in claims):
            return "contested_context"
        return "canon_support"

    def _why_this_paragraph_exists(
        self,
        paragraph: BibleSectionParagraph,
        claims: list[ApprovedClaim],
        sources: list[SourceRecord],
    ) -> str:
        scope = self._paragraph_scope(BibleSectionType.AUTHOR_DECISIONS if paragraph.paragraph_kind == "author_guidance" else BibleSectionType.SETTING_OVERVIEW, claims)
        if scope == "author_guidance":
            return (
                f"This paragraph preserves a deliberate drafting choice tied to {len(claims)} approved author-decision claim"
                + ("s." if len(claims) != 1 else ".")
            )
        if scope == "contested_context":
            return (
                f"This paragraph exists to keep disputed or low-certainty material visible without promoting it to settled fact. "
                f"It currently draws on {len(claims)} claim(s) across {len(sources)} source(s)."
            )
        return (
            f"This paragraph exists because approved canon groups into one usable drafting note here. "
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
                "related_claim_summary": self._claim_scene_note(related) if related else relationship.related_claim_id,
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
                f"{relationship.relationship_type}: {related.subject} ({relationship.notes or 'canon relationship'})"
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

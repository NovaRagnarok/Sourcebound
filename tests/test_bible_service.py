from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileEvidenceStore,
    FileSourceStore,
    FileTruthStore,
)
from source_aware_worldbuilding.domain.enums import (
    BibleSectionGenerationStatus,
    BibleSectionType,
    ClaimKind,
    ClaimStatus,
)
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    BibleSectionFilters,
    ClaimRelationship,
    EvidenceSnippet,
    ProjectionSearchResult,
    SourceRecord,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.storage.json_store import JsonListStore


class FakeProjection:
    def __init__(self, result: ProjectionSearchResult):
        self.result = result

    def upsert_claims(self, claims, evidence) -> None:
        _ = claims, evidence

    def search_claim_ids(self, question: str, allowed_claim_ids: list[str], *, limit: int = 10):
        _ = question, allowed_claim_ids, limit
        return self.result


def populate_bible_service_fixtures(data_dir: Path) -> None:
    JsonListStore(data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-1", title="Greyport town register", source_type="record"),
            SourceRecord(
                source_id="src-2", title="Dock wardens ledger", source_type="account_book"
            ),
            SourceRecord(source_id="src-3", title="Moon well rumors", source_type="oral_history"),
        ]
    )
    JsonListStore(data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 2r",
                text="Greyport kept stone walls and a cramped market gate.",
            ),
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-1",
                locator="folio 4v",
                text="The harbor watch posted curfew at dusk in 1201.",
            ),
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-2",
                locator="entry 8",
                text="Alys served as harbor master and worked with the Dock Wardens.",
            ),
            EvidenceSnippet(
                evidence_id="evi-4",
                source_id="src-2",
                locator="entry 11",
                text="Bakers compared wax-sealed ration scrip at the gate before dawn.",
            ),
            EvidenceSnippet(
                evidence_id="evi-5",
                source_id="src-3",
                locator="entry 3",
                text="Dockhands insisted the grain bell rang at prime and that the moon well sang.",
            ),
            EvidenceSnippet(
                evidence_id="evi-6",
                source_id="src-1",
                locator="folio 7r",
                text="A later ordinance fixed the grain bell at terce.",
            ),
        ]
    )
    JsonListStore(data_dir / "claims.json").write_models(
        [
            ApprovedClaim(
                claim_id="claim-place",
                subject="Greyport",
                predicate="has_feature",
                value="stone walls and a cramped market gate",
                claim_kind=ClaimKind.PLACE,
                status=ClaimStatus.VERIFIED,
                place="Greyport",
                evidence_ids=["evi-1"],
            ),
            ApprovedClaim(
                claim_id="claim-event",
                subject="Harbor watch",
                predicate="posts",
                value="curfew at dusk",
                claim_kind=ClaimKind.EVENT,
                status=ClaimStatus.VERIFIED,
                place="Greyport",
                time_start="1201",
                evidence_ids=["evi-2"],
            ),
            ApprovedClaim(
                claim_id="claim-person",
                subject="Alys",
                predicate="serves_as",
                value="harbor master",
                claim_kind=ClaimKind.PERSON,
                status=ClaimStatus.VERIFIED,
                place="Greyport",
                evidence_ids=["evi-3"],
            ),
            ApprovedClaim(
                claim_id="claim-affiliation",
                subject="Alys",
                predicate="works_with",
                value="the Dock Wardens",
                claim_kind=ClaimKind.INSTITUTION,
                status=ClaimStatus.PROBABLE,
                place="Greyport",
                evidence_ids=["evi-3"],
            ),
            ApprovedClaim(
                claim_id="claim-practice",
                subject="Bakers",
                predicate="compare",
                value="wax-sealed ration scrip at the gate before dawn",
                claim_kind=ClaimKind.PRACTICE,
                status=ClaimStatus.VERIFIED,
                place="Greyport",
                time_start="1202",
                evidence_ids=["evi-4"],
            ),
            ApprovedClaim(
                claim_id="claim-object",
                subject="Ration scrip",
                predicate="is",
                value="wax-sealed and easy to compare in the cold",
                claim_kind=ClaimKind.OBJECT,
                status=ClaimStatus.PROBABLE,
                place="Greyport",
                evidence_ids=["evi-4"],
            ),
            ApprovedClaim(
                claim_id="claim-rumor-prime",
                subject="Grain bell",
                predicate="rings_at",
                value="prime",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.CONTESTED,
                place="Greyport",
                viewpoint_scope="dockhands",
                evidence_ids=["evi-5"],
            ),
            ApprovedClaim(
                claim_id="claim-rumor-terce",
                subject="Grain bell",
                predicate="rings_at",
                value="terce",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.PROBABLE,
                place="Greyport",
                evidence_ids=["evi-6"],
            ),
            ApprovedClaim(
                claim_id="claim-legend",
                subject="Moon well",
                predicate="sings_to",
                value="sailors",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.LEGEND,
                place="Greyport",
                viewpoint_scope="dockhands",
                evidence_ids=["evi-5"],
            ),
            ApprovedClaim(
                claim_id="claim-author",
                subject="Greyport docks",
                predicate="should_be_depicted_as",
                value="crowded, wind-cut, and always half in argument",
                claim_kind=ClaimKind.PLACE,
                status=ClaimStatus.AUTHOR_CHOICE,
                author_choice=True,
                place="Greyport",
                notes="Unify harbor scenes even when the strict canon stays sparse.",
            ),
        ]
    )
    JsonListStore(data_dir / "claim_relationships.json").write_models(
        [
            ClaimRelationship(
                relationship_id="rel-1",
                claim_id="claim-rumor-prime",
                related_claim_id="claim-rumor-terce",
                relationship_type="contradicts",
                notes="Later ordinance fixed the bell at terce.",
            ),
            ClaimRelationship(
                relationship_id="rel-2",
                claim_id="claim-rumor-terce",
                related_claim_id="claim-rumor-prime",
                relationship_type="supersedes",
                notes="The ordinance replaced the earlier practice.",
            ),
        ]
    )


def build_service(data_dir: Path, projection=None) -> BibleWorkspaceService:
    service = BibleWorkspaceService(
        profile_store=FileBibleProjectProfileStore(data_dir),
        section_store=FileBibleSectionStore(data_dir),
        truth_store=FileTruthStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
        projection=projection,
    )
    service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(
            project_name="Greyport Bible",
            geography="Greyport",
            era="1201-1202",
            time_start="1201",
            time_end="1202",
            narrative_focus="market queues, cold mornings, and bell-controlled flow",
            desired_facets=["economics", "daily life", "rumor"],
        ),
    )
    return service


def test_bible_composition_uses_writer_facing_section_strategies(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    setting = service._compose_section(
        "project-greyport",
        BibleSectionType.SETTING_OVERVIEW,
        BibleSectionFilters(place="Greyport"),
    )
    chronology = service._compose_section(
        "project-greyport",
        BibleSectionType.CHRONOLOGY,
        BibleSectionFilters(place="Greyport"),
    )
    people = service._compose_section(
        "project-greyport",
        BibleSectionType.PEOPLE_AND_FACTIONS,
        BibleSectionFilters(place="Greyport"),
    )
    daily = service._compose_section(
        "project-greyport",
        BibleSectionType.DAILY_LIFE,
        BibleSectionFilters(place="Greyport"),
    )
    author = service._compose_section(
        "project-greyport",
        BibleSectionType.AUTHOR_DECISIONS,
        BibleSectionFilters(place="Greyport"),
    )

    assert len(setting.paragraphs) == 3
    assert setting.paragraphs[0].paragraph_kind == "setting_anchor"
    assert setting.paragraphs[0].paragraph_role == "descriptive_synthesis"
    assert "Greyport" in setting.paragraphs[0].text
    assert "Sources:" in setting.paragraphs[0].text
    assert setting.composition_metrics.target_beats == 3
    assert setting.composition_metrics.produced_beats == 3
    assert chronology.paragraphs[0].paragraph_kind == "dated_turn"
    assert chronology.paragraphs[0].paragraph_role == "descriptive_synthesis"
    assert "1201" in chronology.paragraphs[0].text
    assert chronology.paragraphs[0].text.startswith("By 1201,")
    assert people.paragraphs[0].paragraph_kind == "actor_profile"
    assert people.paragraphs[0].paragraph_role == "descriptive_synthesis"
    assert "Alys is anchored by the fact that" in people.paragraphs[0].text
    assert any(paragraph.paragraph_kind == "power_web" for paragraph in people.paragraphs)
    assert people.composition_metrics.produced_beats >= 1
    assert len(daily.paragraphs) == 2
    assert daily.paragraphs[0].paragraph_kind == "routine_cluster"
    assert "The clearest daily-life anchor is that" in daily.paragraphs[0].text
    assert "Stage scenes in Greyport" in daily.paragraphs[0].text
    assert daily.paragraphs[1].paragraph_kind == "material_cluster"
    assert author.paragraphs[0].paragraph_kind == "author_guidance"
    assert author.paragraphs[0].text.startswith("Drafting default: depict Greyport docks")
    assert "strict-fact outputs" in author.paragraphs[0].text
    assert author.generation_status == BibleSectionGenerationStatus.READY
    assert author.ready_for_writer is True
    assert setting.retrieval_metadata["ranking_strategy"] == "intent_blended"


def test_priority_sections_surface_composition_metrics_and_roles(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    rumor = service._compose_section(
        "project-greyport",
        BibleSectionType.RUMORS_AND_CONTESTED,
        BibleSectionFilters(place="Greyport"),
    )

    assert rumor.composition_metrics.target_beats >= 3
    assert rumor.composition_metrics.produced_beats >= 3
    assert rumor.composition_metrics.thin_section is False
    assert any(paragraph.paragraph_role == "uncertainty_framing" for paragraph in rumor.paragraphs)
    assert any(paragraph.paragraph_role == "writer_guidance" for paragraph in rumor.paragraphs)
    assert "claim-rumor-terce" in rumor.references.claim_ids
    contested = next(
        paragraph
        for paragraph in rumor.paragraphs
        if paragraph.paragraph_kind == "contested_record"
    )
    assert "claim-rumor-prime" in contested.claim_ids
    assert "claim-rumor-terce" in contested.claim_ids
    assert "contrast only" in contested.text


def test_bible_focus_pulls_adjacent_claims_into_richer_notebook_output(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    economics = service._compose_section(
        "project-greyport",
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
        BibleSectionFilters(place="Greyport", focus="ration scrip at the gate before dawn"),
    )

    assert len(economics.paragraphs) >= 3
    assert economics.paragraphs[-1].paragraph_kind == "economy_notebook"
    assert (
        "Use the economy as daily pressure rather than abstract background."
        in economics.paragraphs[-1].text
    )
    assert (
        "Treat prices, tools, and exchange routines as something characters handle in motion."
        in economics.paragraphs[-1].text
    )
    assert "claim-practice" in economics.references.claim_ids
    assert "claim-object" in economics.references.claim_ids
    assert "ration scrip" in economics.generated_markdown.lower()


def test_bible_economics_focus_stays_topic_first_and_writer_facing(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    economics = service._compose_section(
        "project-greyport",
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
        BibleSectionFilters(place="Greyport", focus="ration scrip and gate comparison"),
    )

    assert economics.paragraphs[0].paragraph_kind == "economy_cluster"
    assert "Trade pressure is clearest when" in economics.paragraphs[0].text
    assert "Stage scenes in Greyport" in economics.paragraphs[0].text
    assert "claim-practice" in economics.paragraphs[0].claim_ids
    assert "claim-object" in economics.paragraphs[1].claim_ids
    assert "Sources:" in economics.paragraphs[0].text


def test_bible_coverage_uses_section_local_normalized_facets(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)
    service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(
            project_name="Greyport Bible",
            geography="Greyport",
            era="1201-1202",
            time_start="1201",
            time_end="1202",
            narrative_focus="market queues, cold mornings, and bell-controlled flow",
            desired_facets=["economics", "daily life", "institutions", "ritual", "rumor"],
        ),
    )

    economics = service._compose_section(
        "project-greyport",
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
        BibleSectionFilters(place="Greyport"),
    )

    assert economics.coverage_analysis.desired_facets == [
        "economics",
        "daily life",
        "institutions",
        "ritual",
        "rumor",
    ]
    assert economics.coverage_analysis.missing_facets == []
    assert "economics" in economics.coverage_analysis.diagnostic_summary.lower()
    assert "daily life" in economics.coverage_analysis.diagnostic_summary.lower()
    assert "institutions" not in economics.coverage_analysis.diagnostic_summary.lower()
    assert all("institutions" not in gap.lower() for gap in economics.coverage_gaps)
    assert all("ritual" not in gap.lower() for gap in economics.coverage_gaps)
    assert all("rumor" not in gap.lower() for gap in economics.coverage_gaps)


def test_bible_coverage_maps_research_facet_ids_into_section_buckets(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)
    service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(
            project_name="Greyport Bible",
            geography="Greyport",
            era="1201-1202",
            time_start="1201",
            time_end="1202",
            desired_facets=["economics_commercial", "daily_life", "regional_context"],
        ),
    )

    economics = service._compose_section(
        "project-greyport",
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
        BibleSectionFilters(place="Greyport"),
    )
    setting = service._compose_section(
        "project-greyport",
        BibleSectionType.SETTING_OVERVIEW,
        BibleSectionFilters(place="Greyport"),
    )

    assert economics.coverage_analysis.missing_facets == []
    assert setting.coverage_analysis.missing_facets == []
    assert economics.coverage_analysis.facet_distribution["economics"] >= 1
    assert economics.coverage_analysis.facet_distribution["daily life"] >= 1
    assert setting.coverage_analysis.facet_distribution["regional context"] >= 1


def test_bible_provenance_is_enriched_for_paragraph_audits(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)
    section = service.create_section(
        BibleSectionCreateRequest(
            project_id="project-greyport",
            section_type=BibleSectionType.RUMORS_AND_CONTESTED,
            filters=BibleSectionFilters(place="Greyport"),
        )
    )

    provenance = service.get_section_provenance(section.section_id)

    assert provenance is not None
    assert provenance.paragraphs
    paragraph = provenance.paragraphs[0]
    assert paragraph.why_this_paragraph_exists
    assert paragraph.claim_details
    assert paragraph.evidence_details
    assert paragraph.provenance_scope == "contested_context"
    assert paragraph.contradiction_details or paragraph.supersession_details
    assert "source_title" in paragraph.evidence_details[0]
    assert "summary" in paragraph.claim_details[0]
    assert paragraph.paragraph.claim_ids == [item["claim_id"] for item in paragraph.claim_details]
    assert paragraph.paragraph.evidence_ids == [
        item["evidence_id"] for item in paragraph.evidence_details
    ]
    assert paragraph.paragraph.source_ids == [item.source_id for item in paragraph.sources]


def test_bible_retrieval_metadata_surfaces_qdrant_fallback(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(
        temp_data_dir,
        projection=FakeProjection(
            ProjectionSearchResult(
                fallback_used=True,
                fallback_reason="Qdrant collection is not initialized.",
            )
        ),
    )

    draft = service._compose_section(
        "project-greyport",
        BibleSectionType.SETTING_OVERVIEW,
        BibleSectionFilters(place="Greyport", focus="market gate and curfew"),
    )

    assert draft.retrieval_metadata["retrieval_backend"] == "memory"
    assert draft.retrieval_metadata["fallback_used"] is True
    assert draft.retrieval_metadata["fallback_reason"] == "Qdrant collection is not initialized."
    assert "Retrieval fallback" not in (draft.coverage_analysis.diagnostic_summary or "")
    assert "Retrieval fallback: Qdrant collection is not initialized." in draft.generated_markdown


def test_author_decisions_ignore_profile_time_and_source_defaults(temp_data_dir: Path) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    author = service._compose_section(
        "project-greyport",
        BibleSectionType.AUTHOR_DECISIONS,
        BibleSectionFilters(
            place="Greyport",
            time_start="1201",
            time_end="1202",
            source_types=["record"],
        ),
    )

    assert author.references.claim_ids == ["claim-author"]
    assert author.paragraphs[0].paragraph_kind == "author_guidance"
    assert author.generation_status == BibleSectionGenerationStatus.READY
    assert author.ready_for_writer is True


def test_rumor_sections_treat_relationship_filters_as_context_not_hard_gate(
    temp_data_dir: Path,
) -> None:
    populate_bible_service_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    claims = JsonListStore(temp_data_dir / "claims.json").read_models(ApprovedClaim)
    claims.extend(
        [
            ApprovedClaim(
                claim_id="claim-bell-prime-inst",
                subject="Greyport grain bell",
                predicate="rang_after",
                value="prime",
                claim_kind=ClaimKind.INSTITUTION,
                status=ClaimStatus.CONTESTED,
                place="Greyport",
                evidence_ids=["evi-5"],
            ),
            ApprovedClaim(
                claim_id="claim-bell-terce-inst",
                subject="Greyport grain bell",
                predicate="rang_after",
                value="terce",
                claim_kind=ClaimKind.INSTITUTION,
                status=ClaimStatus.PROBABLE,
                place="Greyport",
                evidence_ids=["evi-6"],
            ),
        ]
    )
    JsonListStore(temp_data_dir / "claims.json").write_models(claims)
    relationships = JsonListStore(temp_data_dir / "claim_relationships.json").read_models(
        ClaimRelationship
    )
    relationships.extend(
        [
            ClaimRelationship(
                relationship_id="rel-inst-prime-terce",
                claim_id="claim-bell-prime-inst",
                related_claim_id="claim-bell-terce-inst",
                relationship_type="contradicts",
                notes="The institutional bell record splits across two ordinances.",
            ),
            ClaimRelationship(
                relationship_id="rel-inst-terce-prime",
                claim_id="claim-bell-terce-inst",
                related_claim_id="claim-bell-prime-inst",
                relationship_type="supersedes",
                notes="The later institutional hour supersedes the earlier one.",
            ),
        ]
    )
    JsonListStore(temp_data_dir / "claim_relationships.json").write_models(relationships)

    rumor = service._compose_section(
        "project-greyport",
        BibleSectionType.RUMORS_AND_CONTESTED,
        BibleSectionFilters(
            place="Greyport",
            statuses=[
                ClaimStatus.CONTESTED,
                ClaimStatus.RUMOR,
                ClaimStatus.LEGEND,
                ClaimStatus.PROBABLE,
            ],
            source_types=["oral_history", "record"],
            relationship_types=["contradicts", "supersedes"],
            focus="grain bell disputes and moon well rumors",
        ),
    )

    assert "claim-bell-prime-inst" in rumor.references.claim_ids
    assert "claim-bell-terce-inst" in rumor.references.claim_ids
    assert "claim-legend" in rumor.references.claim_ids
    assert rumor.generation_status == BibleSectionGenerationStatus.READY

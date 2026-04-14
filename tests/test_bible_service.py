from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileEvidenceStore,
    FileSourceStore,
    FileTruthStore,
)
from source_aware_worldbuilding.domain.enums import BibleSectionType, ClaimKind, ClaimStatus
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
            SourceRecord(source_id="src-2", title="Dock wardens ledger", source_type="account_book"),
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
                evidence_ids=["evi-3"],
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

    assert setting.paragraphs[0].paragraph_kind == "setting_cluster"
    assert "Scenes in Greyport can safely lean on" in setting.paragraphs[0].text
    assert "For scene construction" in setting.paragraphs[0].text
    assert chronology.paragraphs[0].paragraph_kind == "chronology_entry"
    assert "By 1201, scenes can safely show" in chronology.paragraphs[0].text
    assert "For scene construction" in chronology.paragraphs[0].text
    assert people.paragraphs[0].paragraph_kind == "actor_cluster"
    assert "For Alys, scenes can safely play" in people.paragraphs[0].text
    assert "Ties, leverage, and faction pull" in people.paragraphs[0].text
    assert daily.paragraphs[0].paragraph_kind == "routine_cluster"
    assert "Everyday scenes can safely rest on" in daily.paragraphs[0].text
    assert "For scene construction" in daily.paragraphs[0].text
    assert author.paragraphs[0].paragraph_kind == "author_guidance"
    assert author.paragraphs[0].text.startswith("Author choice: depict Greyport docks")


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
    assert "Use the economy as daily pressure rather than abstract background." in economics.paragraphs[-1].text
    assert "claim-practice" in economics.references.claim_ids
    assert "claim-object" in economics.references.claim_ids
    assert "ration scrip" in economics.generated_markdown.lower()


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
    assert "Retrieval fallback" in (draft.coverage_analysis.diagnostic_summary or "")
